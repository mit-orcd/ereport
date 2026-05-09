/*
 * ecrawl_analyze.c — Read-only directory-shape stats from uid_shard_*.bin crawl shards.
 *
 * SPDX-License-Identifier: MIT
 *
 * Build: gcc -O2 -Wall -Wextra -pthread -o ecrawl_analyze ecrawl_analyze.c crawl_bin_chunks.o
 *
 * Usage: ecrawl_analyze [--verbose] [--top N] <crawl-output-dir>
 *
 * Scans shards in parallel using checkpoint segment boundaries from each .ckpt when valid;
 * otherwise falls back to one job per shard [header, EOF). Prints parent-directory histograms,
 * depth (slash-count) histograms, and top parents by regular-file count on stdout. Live progress
 * on stderr when stderr is a TTY.
 *
 * Parallelism: ECRAWL_ANALYZE_THREADS (default 16, minimum 1, maximum 4096). If unset,
 * ECRAWL_REPAIR_THREADS is used for compatibility with older workflows.
 */

#define _FILE_OFFSET_BITS 64
#define _DEFAULT_SOURCE

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <limits.h>

#include "crawl_bin_format.h"
#include "crawl_ckpt.h"
#include "crawl_bin_chunks.h"
#include "crawl_bin_catalog.h"
#include "path_canon.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define DEFAULT_ANALYZE_THREADS 16U
#define ANALYZE_THREADS_MAX 4096U

static pthread_mutex_t g_verbose_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_verbose;
static unsigned g_top_n = 32U;

/* Larger table → shorter collision chains; merge does many strcmp walks per insert. */
#define ANALYZE_HASH_BUCKETS 262144U
#define ANALYZE_DEPTH_BINS 64U

typedef struct parent_node {
    struct parent_node *next;
    char *path;
    uint64_t nfile;
    uint64_t ndir;
    uint64_t nsym;
    uint64_t nother;
} parent_node_t;

typedef struct {
    parent_node_t *buckets[ANALYZE_HASH_BUCKETS];
} parent_map_t;

typedef struct {
    const char *dir_path;
    char **names;
    size_t name_count;
    crawl_bin_file_chunk_t *chunks;
    size_t chunk_count;
    uint64_t *shard_file_sizes;
    _Atomic size_t chunk_cursor;
    _Atomic int failures;
    _Atomic unsigned slot_assign;
    parent_map_t **maps;
    uint64_t **depth_hist;
    uint64_t analyze_bytes_total;
    _Atomic uint64_t analyze_bytes_done;
    _Atomic uint64_t analyze_records_done;
    _Atomic uint64_t analyze_chunks_done;
    _Atomic int analyze_stop_stats;
} analyze_pool_t;

typedef struct {
    analyze_pool_t *pool;
    double t0;
} analyze_stats_ctx_t;

static void format_bytes_human(uint64_t n, char *out, size_t out_sz) {
    static const char *const suf[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    double v = (double)n;
    unsigned si = 0;

    while (v >= 1024.0 && si + 1U < sizeof(suf) / sizeof(suf[0])) {
        v /= 1024.0;
        si++;
    }
    if (si == 0) {
        snprintf(out, out_sz, "%" PRIu64 " B", n);
    } else {
        snprintf(out, out_sz, "%.2f %s", v, suf[si]);
    }
}

static double analyze_now_sec(void) {
    struct timeval tv;

    if (gettimeofday(&tv, NULL) != 0) return 0.0;
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static void analyze_fmt_eta(double sec, char *out, size_t out_sz) {
    uint64_t t;

    if (!out || out_sz == 0) return;
    if (sec < 0.0 || sec > 86400000.0 || sec != sec) {
        snprintf(out, out_sz, "?");
        return;
    }
    t = (uint64_t)(sec + 0.5);
    if (t >= 3600ULL) {
        uint64_t h = t / 3600ULL;
        uint64_t m = (t % 3600ULL) / 60ULL;
        snprintf(out, out_sz, "%" PRIu64 "h%02" PRIu64 "m", h, m);
    } else if (t >= 60ULL) {
        uint64_t m = t / 60ULL;
        uint64_t s = t % 60ULL;
        snprintf(out, out_sz, "%" PRIu64 "m%02" PRIu64 "s", m, s);
    } else {
        snprintf(out, out_sz, "<1m");
    }
}

static void analyze_clear_progress_line(void) {
    if (isatty(STDERR_FILENO)) fprintf(stderr, "\r\033[2K\r");
    fflush(stderr);
}

static void *analyze_stats_thread_main(void *arg) {
    analyze_stats_ctx_t *ctx = (analyze_stats_ctx_t *)arg;
    analyze_pool_t *p = ctx->pool;
    int tty = isatty(STDERR_FILENO);

    if (!p) return NULL;

    while (!atomic_load_explicit(&p->analyze_stop_stats, memory_order_relaxed)) {
        double now = analyze_now_sec();
        double el = now - ctx->t0;
        uint64_t bd = atomic_load_explicit(&p->analyze_bytes_done, memory_order_relaxed);
        uint64_t bt = p->analyze_bytes_total;
        uint64_t rec = atomic_load_explicit(&p->analyze_records_done, memory_order_relaxed);
        uint64_t ck = atomic_load_explicit(&p->analyze_chunks_done, memory_order_relaxed);
        double pct = (bt > 0ULL) ? (100.0 * (double)bd / (double)bt) : 0.0;
        double bps = (el > 0.05) ? ((double)bd / el) : 0.0;
        double rps = (el > 0.05) ? ((double)rec / el) : 0.0;
        char bd_h[48], bt_h[48], bps_h[48], eta_buf[32];

        format_bytes_human(bd, bd_h, sizeof(bd_h));
        format_bytes_human(bt, bt_h, sizeof(bt_h));
        if (bps >= 1099511627776.0)
            snprintf(bps_h, sizeof(bps_h), "%.2f TiB/s", bps / 1099511627776.0);
        else if (bps >= 1073741824.0)
            snprintf(bps_h, sizeof(bps_h), "%.2f GiB/s", bps / 1073741824.0);
        else if (bps >= 1048576.0)
            snprintf(bps_h, sizeof(bps_h), "%.2f MiB/s", bps / 1048576.0);
        else if (bps >= 1024.0)
            snprintf(bps_h, sizeof(bps_h), "%.1f KiB/s", bps / 1024.0);
        else
            snprintf(bps_h, sizeof(bps_h), "%.0f B/s", bps);

        if (bd > 0ULL && bt > bd && bps > 0.0)
            analyze_fmt_eta((double)(bt - bd) / bps, eta_buf, sizeof(eta_buf));
        else {
            snprintf(eta_buf, sizeof(eta_buf), bt > 0ULL && bd >= bt ? "done" : "?");
        }

        if (tty) {
            fprintf(stderr,
                    "\recrawl_analyze: %s / %s (%5.1f%%) | chunks %" PRIu64 "/%zu | rec %" PRIu64 " (%6.1fK/s) | scan %s "
                    "| el %4.0fs | ETA %s     ",
                    bd_h, bt_h, pct, ck, p->chunk_count, rec, rps / 1000.0, bps_h, el, eta_buf);
            fflush(stderr);
        }

        sleep(1);
    }

    return NULL;
}

static int is_uid_shard_bin_name(const char *name) {
    const char *p;
    unsigned long v;
    char *end;

    if (strncmp(name, "uid_shard_", 10) != 0) return 0;
    p = name + 10;
    if (*p == '\0' || !isdigit((unsigned char)*p)) return 0;
    errno = 0;
    v = strtoul(p, &end, 10);
    (void)v;
    if (errno || !end || strcmp(end, ".bin") != 0) return 0;
    return 1;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--verbose] [--top N] <crawl-output-dir>\n"
            "  Read-only parallel scan of uid_shard_*.bin shards; directory-shape stats on stdout.\n"
            "  Uses .ckpt segment boundaries when sidecars are valid; else one range per shard.\n"
            "  Parallel threads: ECRAWL_ANALYZE_THREADS (default %u), or ECRAWL_REPAIR_THREADS if unset.\n"
            "  Live bytes/chunks/records + ETA on stderr when stderr is a terminal.\n"
            "  --top N: list top N parents by regular-file count (default %u).\n",
            prog, DEFAULT_ANALYZE_THREADS, g_top_n);
}

static unsigned parse_analyze_threads_env(void) {
    const char *e = getenv("ECRAWL_ANALYZE_THREADS");
    if (!e || e[0] == '\0') e = getenv("ECRAWL_REPAIR_THREADS");
    if (!e || e[0] == '\0') return DEFAULT_ANALYZE_THREADS;
    {
        unsigned long v;
        char *end;
        errno = 0;
        v = strtoul(e, &end, 10);
        if (errno || end == e || *end != '\0' || v < 1UL || v > (unsigned long)ANALYZE_THREADS_MAX)
            return DEFAULT_ANALYZE_THREADS;
        return (unsigned)v;
    }
}

static int cmp_strptr(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static uint32_t analyze_hash_parent(const char *s) {
    uint32_t h = 2166136261u;
    while (*s) {
        h ^= (uint32_t)(unsigned char)*s++;
        h *= 16777619u;
    }
    return h;
}

static parent_map_t *parent_map_new(void) {
    parent_map_t *m = (parent_map_t *)calloc(1, sizeof(*m));
    return m;
}

static size_t parent_map_count_nodes(const parent_map_t *m) {
    size_t c = 0;
    size_t bi;

    if (!m) return 0;
    for (bi = 0; bi < ANALYZE_HASH_BUCKETS; bi++) {
        const parent_node_t *n = m->buckets[bi];
        while (n) {
            c++;
            n = n->next;
        }
    }
    return c;
}

static void parent_map_free(parent_map_t *m) {
    size_t bi;

    if (!m) return;
    for (bi = 0; bi < ANALYZE_HASH_BUCKETS; bi++) {
        parent_node_t *n = m->buckets[bi];
        while (n) {
            parent_node_t *nx = n->next;
            free(n->path);
            free(n);
            n = nx;
        }
    }
    free(m);
}

static void parent_map_add_record(parent_map_t *m, const char *parent, uint8_t typ) {
    uint32_t hx = analyze_hash_parent(parent);
    size_t bi = (size_t)(hx % ANALYZE_HASH_BUCKETS);
    parent_node_t **pp = &m->buckets[bi];

    while (*pp) {
        if (strcmp((*pp)->path, parent) == 0) {
            if (typ == (uint8_t)'f')
                (*pp)->nfile++;
            else if (typ == (uint8_t)'d')
                (*pp)->ndir++;
            else if (typ == (uint8_t)'l')
                (*pp)->nsym++;
            else
                (*pp)->nother++;
            return;
        }
        pp = &(*pp)->next;
    }
    {
        parent_node_t *node = (parent_node_t *)malloc(sizeof(*node));
        if (!node) return;
        node->path = strdup(parent);
        if (!node->path) {
            free(node);
            return;
        }
        node->next = m->buckets[bi];
        m->buckets[bi] = node;
        node->nfile = node->ndir = node->nsym = node->nother = 0;
        if (typ == (uint8_t)'f')
            node->nfile = 1;
        else if (typ == (uint8_t)'d')
            node->ndir = 1;
        else if (typ == (uint8_t)'l')
            node->nsym = 1;
        else
            node->nother = 1;
    }
}

static void parent_map_add_totals(parent_map_t *m, const char *parent, uint64_t nf, uint64_t nd, uint64_t ns,
                                  uint64_t no) {
    uint32_t hx = analyze_hash_parent(parent);
    size_t bi = (size_t)(hx % ANALYZE_HASH_BUCKETS);
    parent_node_t **pp = &m->buckets[bi];

    while (*pp) {
        if (strcmp((*pp)->path, parent) == 0) {
            (*pp)->nfile += nf;
            (*pp)->ndir += nd;
            (*pp)->nsym += ns;
            (*pp)->nother += no;
            return;
        }
        pp = &(*pp)->next;
    }
    {
        parent_node_t *node = (parent_node_t *)malloc(sizeof(*node));
        if (!node) return;
        node->path = strdup(parent);
        if (!node->path) {
            free(node);
            return;
        }
        node->next = m->buckets[bi];
        m->buckets[bi] = node;
        node->nfile = nf;
        node->ndir = nd;
        node->nsym = ns;
        node->nother = no;
    }
}

static void parent_map_merge_into(parent_map_t *dst, parent_map_t *src) {
    size_t bi;

    if (!dst || !src) return;
    for (bi = 0; bi < ANALYZE_HASH_BUCKETS; bi++) {
        parent_node_t *n = src->buckets[bi];
        while (n) {
            parent_map_add_totals(dst, n->path, n->nfile, n->ndir, n->nsym, n->nother);
            n = n->next;
        }
    }
}

static int parent_dir_from_path(const unsigned char *path, uint16_t path_len, char *parent, size_t parent_sz) {
    size_t len = path_len;

    while (len > 0 && path[len - 1] == '/') len--;
    if (len == 0) {
        if (parent_sz < 2) return -1;
        parent[0] = '.';
        parent[1] = '\0';
        return 0;
    }
    {
        size_t last = (size_t)-1;
        size_t j;

        for (j = len; j > 0; j--) {
            if (path[j - 1] == '/') {
                last = j - 1;
                break;
            }
        }
        if (last == (size_t)-1) {
            if (parent_sz < 2) return -1;
            parent[0] = '.';
            parent[1] = '\0';
            return 0;
        }
        if (last == 0) {
            if (parent_sz < 2) return -1;
            parent[0] = '/';
            parent[1] = '\0';
            return 0;
        }
        if (last >= parent_sz) return -1;
        memcpy(parent, path, last);
        parent[last] = '\0';
        return 0;
    }
}

static unsigned analyze_depth_slash_bin(const unsigned char *path, uint16_t path_len) {
    unsigned c = 0;
    uint16_t i;

    for (i = 0; i < path_len; i++)
        if (path[i] == '/') c++;
    if (c >= ANALYZE_DEPTH_BINS) c = ANALYZE_DEPTH_BINS - 1U;
    return c;
}

static size_t analyze_stdio_fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    return fread(ptr, size, nmemb, stream);
}

static const crawl_bin_chunk_stdio_t analyze_chunk_io = { fopen, analyze_stdio_fread, fclose };

static int analyze_append_chunk_job(crawl_bin_file_chunk_t **all, size_t *all_n, size_t *all_cap,
                                    const char *full_path, uint64_t lo, uint64_t hi, size_t file_index,
                                    uint64_t *byte_sum) {
    crawl_bin_file_chunk_t *na;
    char *pdup;

    if (lo >= hi) return -1;
    pdup = strdup(full_path);
    if (!pdup) return -1;
    if (*all_n >= *all_cap) {
        size_t nc = *all_cap ? *all_cap * 2 : 256U;
        while (nc <= *all_n) nc *= 2;
        na = (crawl_bin_file_chunk_t *)realloc(*all, nc * sizeof(*na));
        if (!na) {
            free(pdup);
            return -1;
        }
        *all = na;
        *all_cap = nc;
    }
    (*all)[*all_n].path = pdup;
    (*all)[*all_n].start_offset = lo;
    (*all)[*all_n].end_offset = hi;
    (*all)[*all_n].file_index = file_index;
    (*all_n)++;
    *byte_sum += hi - lo;
    return 0;
}

/*
 * Reorder jobs round-robin by shard index so concurrent workers tend to open different uid_shard_*.bin
 * files (better parallelism on NFS and some local caches). Requires shard-major layout from
 * analyze_build_all_chunks: all chunks of file fi precede fi+1. If layout differs, leaves order unchanged.
 */
static int analyze_interleave_chunks_shard_round_robin(crawl_bin_file_chunk_t **chunks_io, size_t chunk_count,
                                                       size_t name_count) {
    crawl_bin_file_chunk_t *ch = *chunks_io;
    size_t *nc = NULL;
    size_t *base = NULL;
    crawl_bin_file_chunk_t *out = NULL;
    size_t fi, k, r, max_r = 0, kk;

    if (chunk_count == 0U || name_count == 0U) return 0;

    nc = (size_t *)calloc(name_count, sizeof(*nc));
    if (!nc) return -1;

    for (k = 0; k < chunk_count; k++) {
        fi = ch[k].file_index;
        if (fi >= name_count) {
            free(nc);
            return -1;
        }
        nc[fi]++;
    }

    kk = 0;
    for (fi = 0; fi < name_count; fi++) {
        size_t j;
        for (j = 0; j < nc[fi]; j++, kk++) {
            if (kk >= chunk_count || ch[kk].file_index != fi) {
                free(nc);
                return 0;
            }
        }
    }
    if (kk != chunk_count) {
        free(nc);
        return 0;
    }

    base = (size_t *)calloc(name_count + 1U, sizeof(*base));
    if (!base) {
        free(nc);
        return -1;
    }
    for (fi = 0; fi < name_count; fi++) {
        base[fi + 1U] = base[fi] + nc[fi];
        if (nc[fi] > max_r) max_r = nc[fi];
    }

    out = (crawl_bin_file_chunk_t *)malloc(chunk_count * sizeof(*out));
    if (!out) {
        free(nc);
        free(base);
        return -1;
    }

    kk = 0;
    for (r = 0; r < max_r; r++) {
        for (fi = 0; fi < name_count; fi++) {
            if (r < nc[fi]) {
                size_t src = base[fi] + r;
                out[kk++] = ch[src];
            }
        }
    }

    free(ch);
    free(nc);
    free(base);
    *chunks_io = out;
    return 0;
}

typedef struct {
    parent_map_t *dst;
    parent_map_t *src;
} analyze_merge_pair_t;

static void analyze_merge_pair_do(analyze_merge_pair_t *p) {
    parent_map_merge_into(p->dst, p->src);
    parent_map_free(p->src);
    p->src = NULL;
}

static void *analyze_merge_pair_thread(void *arg) {
    analyze_merge_pair_do((analyze_merge_pair_t *)arg);
    return NULL;
}

typedef struct {
    parent_map_t *m;
    size_t cnt;
} analyze_map_sz_t;

static int cmp_analyze_map_sz_asc(const void *a, const void *b) {
    const analyze_map_sz_t *xa = (const analyze_map_sz_t *)a;
    const analyze_map_sz_t *xb = (const analyze_map_sz_t *)b;

    if (xa->cnt < xb->cnt) return -1;
    if (xa->cnt > xb->cnt) return 1;
    return 0;
}

/* Sort maps by distinct-parent count so pairs (2k,2k+1) merge a smaller map into a larger one. */
static void analyze_sort_maps_by_node_count(parent_map_t **maps, unsigned n) {
    analyze_map_sz_t *rows;
    unsigned i;

    if (n <= 1U) return;
    rows = (analyze_map_sz_t *)malloc((size_t)n * sizeof(*rows));
    if (!rows) return;
    for (i = 0; i < n; i++) {
        rows[i].m = maps[i];
        rows[i].cnt = parent_map_count_nodes(maps[i]);
    }
    qsort(rows, (size_t)n, sizeof(*rows), cmp_analyze_map_sz_asc);
    for (i = 0; i < n; i++) maps[i] = rows[i].m;
    free(rows);
}

/*
 * Reduce *n_io maps at maps[0..*n_io-1] into maps[0]. Clears maps[*n_io..maps_cap-1] each round so no slot
 * keeps a dangling pointer after its map was freed as a merge source. Uses up to max_workers threads per batch.
 */
static int analyze_parallel_reduce_parent_maps(parent_map_t **maps, unsigned *n_io, unsigned maps_cap,
                                               unsigned max_workers) {
    analyze_merge_pair_t *jobs = NULL;
    pthread_t *ths = NULL;
    unsigned mw = max_workers;
    unsigned n = *n_io;
    size_t jobs_sz;
    unsigned pass = 0;

    if (n <= 1U) {
        *n_io = n;
        return 0;
    }
    if (mw < 1U) mw = 1U;
    if (mw > 1024U) mw = 1024U;

    jobs_sz = (size_t)(maps_cap / 2U + 1U);
    jobs = (analyze_merge_pair_t *)malloc(jobs_sz * sizeof(*jobs));
    ths = (pthread_t *)malloc((size_t)mw * sizeof(*ths));
    if (!jobs || !ths) {
        free(jobs);
        free(ths);
        return -1;
    }

    while (n > 1U) {
        unsigned np = n / 2U;
        unsigned odd = n % 2U;
        unsigned b;
        unsigned u;

        pass++;
        fprintf(stderr, "ecrawl_analyze: merge pass %u: %u map(s) left (~%u parallel merge(s))\n", pass, n, np);
        fflush(stderr);

        analyze_sort_maps_by_node_count(maps, n);

        if (np > jobs_sz) {
            free(jobs);
            free(ths);
            return -1;
        }
        /* After ascending sort: maps[2b] is no larger than maps[2b+1]; merge smaller src into larger dst. */
        for (b = 0; b < np; b++) {
            jobs[b].src = maps[2U * b];
            jobs[b].dst = maps[2U * b + 1U];
        }

        for (b = 0; b < np; b += mw) {
            unsigned nb = np - b;
            unsigned j;

            if (nb > mw) nb = mw;
            for (j = 0; j < nb; j++) {
                if (pthread_create(&ths[j], NULL, analyze_merge_pair_thread, &jobs[b + j]) != 0) {
                    for (unsigned k = 0; k < j; k++) pthread_join(ths[k], NULL);
                    for (; j < nb; j++) analyze_merge_pair_do(&jobs[b + j]);
                    goto batch_done;
                }
            }
            for (j = 0; j < nb; j++) pthread_join(ths[j], NULL);
        batch_done:;
        }

        for (b = 0; b < np; b++) maps[b] = jobs[b].dst;
        if (odd)
            maps[np] = maps[2U * np];
        n = np + odd;
        for (u = n; u < maps_cap; u++) maps[u] = NULL;
    }

    *n_io = n;
    free(jobs);
    free(ths);
    return 0;
}

/*
 * Build parse jobs from .ckpt segment boundaries only (no prescan of shard bodies).
 * Falls back to one job per shard [sizeof(hdr), file_size) when sidecar is missing or invalid.
 */
static int analyze_build_all_chunks(const char *dir_path, char **names, size_t name_count,
                                    uint64_t **shard_sizes_out, crawl_bin_file_chunk_t **chunks_out,
                                    size_t *chunk_count_out, uint64_t *chunk_bytes_total_out) {
    crawl_bin_file_chunk_t *all = NULL;
    size_t all_n = 0, all_cap = 0;
    uint64_t *sizes = NULL;
    uint64_t byte_sum = 0;
    size_t fi;
    const uint64_t hdr_end = (uint64_t)sizeof(bin_file_header_t);

    *shard_sizes_out = NULL;
    *chunks_out = NULL;
    *chunk_count_out = 0;
    *chunk_bytes_total_out = 0;

    sizes = (uint64_t *)calloc(name_count, sizeof(*sizes));
    if (!sizes) return -1;

    fprintf(stderr, "ecrawl_analyze: building parse jobs from .ckpt segments (%zu shard file(s))...\n", name_count);
    fflush(stderr);

    for (fi = 0; fi < name_count; fi++) {
        char full[PATH_MAX];
        struct stat st;
        bin_file_header_t fh;
        FILE *fp;
        uint64_t *offs = NULL;
        size_t n_off = 0;
        size_t j;

        if (snprintf(full, sizeof(full), "%s/%s", dir_path, names[fi]) >= (int)sizeof(full)) goto fail;
        if (lstat(full, &st) != 0) goto fail;
        if (!S_ISREG(st.st_mode)) continue;

        sizes[fi] = (uint64_t)st.st_size;
        if (sizes[fi] < hdr_end) continue;

        fp = fopen(full, "rb");
        if (!fp) goto fail;
        if (fread(&fh, sizeof(fh), 1, fp) != 1) {
            fclose(fp);
            goto fail;
        }
        fclose(fp);

        {
            uint64_t record_end = sizes[fi];

            if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) continue;
            if (fh.catalog_offset != 0ULL) {
                if (fh.catalog_offset < sizeof(fh) || fh.catalog_offset > sizes[fi]) continue;
                record_end = fh.catalog_offset;
            }

            if (crawl_bin_load_ckpt(&analyze_chunk_io, full, record_end, &offs, &n_off) == 0 && n_off > 0) {
                int bad = 0;

                for (j = 0; j < n_off; j++) {
                    uint64_t lo = offs[j];
                    uint64_t hi = (j + 1 < n_off) ? offs[j + 1] : record_end;
                    if (lo >= hi || hi > record_end) {
                        bad = 1;
                        break;
                    }
                }
                if (!bad) {
                    for (j = 0; j < n_off; j++) {
                        uint64_t lo = offs[j];
                        uint64_t hi = (j + 1 < n_off) ? offs[j + 1] : record_end;
                        if (analyze_append_chunk_job(&all, &all_n, &all_cap, full, lo, hi, fi, &byte_sum) != 0) {
                            free(offs);
                            goto fail;
                        }
                    }
                    free(offs);
                    continue;
                }
                free(offs);
                offs = NULL;
            } else {
                free(offs);
                offs = NULL;
            }

            if (record_end <= hdr_end) continue;
            if (analyze_append_chunk_job(&all, &all_n, &all_cap, full, hdr_end, record_end, fi, &byte_sum) != 0)
                goto fail;
        }
    }

    if (all_n > 0U && analyze_interleave_chunks_shard_round_robin(&all, all_n, name_count) != 0) goto fail;

    fprintf(stderr, "ecrawl_analyze: %zu parse job(s); starting worker scan...\n", all_n);
    fflush(stderr);

    *shard_sizes_out = sizes;
    *chunks_out = all;
    *chunk_count_out = all_n;
    *chunk_bytes_total_out = byte_sum;
    return 0;

fail:
    crawl_bin_free_chunk_array_rows(all, all_n);
    free(sizes);
    return -1;
}

static int analyze_scan_fp_until(FILE *fp, uint64_t scan_end_exclusive, uint64_t file_sz,
                                 const crawl_bin_catalog_t *cat, unsigned char *pathbuf, char *parentbuf,
                                 char *fullpath_buf, size_t fullpath_sz, parent_map_t *map, uint64_t *depth_hist,
                                 uint64_t *nrec_out) {
    uint64_t nrec = 0;

    if (nrec_out) *nrec_out = 0;

    for (;;) {
        off_t rec0 = ftello(fp);
        bin_record_hdr_t rh;
        size_t rec_tot;

        if (rec0 < 0) return -1;
        if ((uint64_t)rec0 >= scan_end_exclusive) break;

        if (fread(&rh, sizeof(rh), 1, fp) != 1) return -1;
        rec_tot = crawl_bin_record_total_bytes(&rh);
        if ((uint64_t)rec0 + rec_tot > scan_end_exclusive) return -1;
        if ((uint64_t)rec0 + rec_tot > file_sz) return -1;

        if (rh.parent_dir_id == 0ULL) return -1;

        if (rh.name_len) {
            if (fread(pathbuf, rh.name_len, 1, fp) != 1) return -1;
        }

        if (crawl_bin_catalog_entry_path(cat, rh.parent_dir_id, (char *)pathbuf, rh.name_len, fullpath_buf,
                                         fullpath_sz) != 0)
            return -1;
        {
            size_t flen = strlen(fullpath_buf);
            uint16_t pl = flen > 65535U ? 65535U : (uint16_t)flen;

            if (flen > 0) {
                unsigned db = analyze_depth_slash_bin((unsigned char *)fullpath_buf, pl);
                depth_hist[db]++;
                if (parent_dir_from_path((unsigned char *)fullpath_buf, pl, parentbuf, 65536U) == 0)
                    parent_map_add_record(map, parentbuf, rh.type);
            }
        }
        nrec++;
    }

    {
        off_t pos = ftello(fp);
        if (pos < 0) return -1;
        if ((uint64_t)pos != scan_end_exclusive) return -1;
    }
    if (nrec_out) *nrec_out = nrec;
    return 0;
}

static int analyze_process_chunk(const char *full_path, uint64_t start_off, uint64_t end_off, uint64_t file_sz,
                                 unsigned char *pathbuf, char *parentbuf, char *fullpath_buf, size_t fullpath_sz,
                                 parent_map_t *map, uint64_t *depth_hist, uint64_t *nrec_out) {
    FILE *fp;
    int rc;
    bin_file_header_t fh;
    crawl_bin_catalog_t cat;

    if (start_off > file_sz || end_off > file_sz || start_off >= end_off) return -1;
    fp = fopen(full_path, "rb");
    if (!fp) {
        perror(full_path);
        return -1;
    }
    if (fread(&fh, sizeof(fh), 1, fp) != 1) {
        fclose(fp);
        return -1;
    }
    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
        fclose(fp);
        return -1;
    }
    if (fh.catalog_offset == 0ULL || fh.catalog_offset > file_sz) {
        fclose(fp);
        return -1;
    }
    crawl_bin_catalog_init_empty(&cat);
    if (crawl_bin_catalog_load(fp, fh.catalog_offset, file_sz, &cat) != 0) {
        crawl_bin_catalog_free(&cat);
        fclose(fp);
        return -1;
    }
    if (fseeko(fp, (off_t)start_off, SEEK_SET) != 0) {
        crawl_bin_catalog_free(&cat);
        fclose(fp);
        return -1;
    }
    rc = analyze_scan_fp_until(fp, end_off, file_sz, &cat, pathbuf, parentbuf, fullpath_buf, fullpath_sz, map,
                               depth_hist, nrec_out);
    crawl_bin_catalog_free(&cat);
    fclose(fp);
    return rc;
}

static void *analyze_worker_main(void *arg) {
    analyze_pool_t *p = (analyze_pool_t *)arg;
    unsigned slot = atomic_fetch_add_explicit(&p->slot_assign, 1U, memory_order_relaxed);
    parent_map_t *map = parent_map_new();
    uint64_t *dh = (uint64_t *)calloc((size_t)ANALYZE_DEPTH_BINS, sizeof(uint64_t));
    unsigned char *pathbuf = (unsigned char *)malloc(65536);
    char *parentbuf = (char *)malloc(65536);
    char *fullpath_buf = (char *)malloc(PATH_MAX);

    if (!map || !dh || !pathbuf || !parentbuf || !fullpath_buf) {
        free(fullpath_buf);
        free(pathbuf);
        free(parentbuf);
        free(dh);
        if (map) parent_map_free(map);
        atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        return NULL;
    }
    p->maps[slot] = map;
    p->depth_hist[slot] = dh;

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->chunk_cursor, 1, memory_order_relaxed);
        crawl_bin_file_chunk_t *c;
        uint64_t fsz;
        uint64_t chunk_bytes;
        uint64_t nrec = 0;
        int ar;

        if (i >= p->chunk_count) break;
        c = &p->chunks[i];
        fsz = p->shard_file_sizes[c->file_index];
        chunk_bytes = c->end_offset - c->start_offset;

        ar = analyze_process_chunk(c->path, c->start_offset, c->end_offset, fsz, pathbuf, parentbuf, fullpath_buf,
                                   PATH_MAX, map, dh, &nrec);

        atomic_fetch_add_explicit(&p->analyze_bytes_done, chunk_bytes, memory_order_relaxed);
        atomic_fetch_add_explicit(&p->analyze_chunks_done, 1ULL, memory_order_relaxed);
        if (ar == 0)
            atomic_fetch_add_explicit(&p->analyze_records_done, nrec, memory_order_relaxed);
        else {
            fprintf(stderr,
                    "%s [%" PRIu64 ",%" PRIu64 "): ecrawl_analyze chunk scan failed (corrupt or ckpt mismatch?)\n",
                    c->path, c->start_offset, c->end_offset);
            atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        }

        if (ar == 0 && g_verbose) {
            pthread_mutex_lock(&g_verbose_mutex);
            printf("%s [%" PRIu64 ",%" PRIu64 "): chunk ok (%" PRIu64 " records)\n", c->path, c->start_offset,
                   c->end_offset, nrec);
            pthread_mutex_unlock(&g_verbose_mutex);
        }
    }

    free(pathbuf);
    free(parentbuf);
    free(fullpath_buf);
    return NULL;
}

typedef struct {
    parent_node_t *node;
} analyze_sort_wrap_t;

static int cmp_analyze_nfile_desc(const void *a, const void *b) {
    const analyze_sort_wrap_t *wa = (const analyze_sort_wrap_t *)a;
    const analyze_sort_wrap_t *wb = (const analyze_sort_wrap_t *)b;
    uint64_t af = wa->node->nfile;
    uint64_t bf = wb->node->nfile;

    if (af < bf) return 1;
    if (af > bf) return -1;
    return strcmp(wa->node->path, wb->node->path);
}

static void analyze_hist_files_per_parent(uint64_t nfile, uint64_t *b1, uint64_t *b2_10, uint64_t *b11_100,
                                          uint64_t *b101_1k, uint64_t *b1001_10k, uint64_t *b10001_100k,
                                          uint64_t *b100001_1m, uint64_t *b1m_plus) {
    if (nfile == 0ULL) return;
    if (nfile == 1ULL)
        (*b1)++;
    else if (nfile <= 10ULL)
        (*b2_10)++;
    else if (nfile <= 100ULL)
        (*b11_100)++;
    else if (nfile <= 1000ULL)
        (*b101_1k)++;
    else if (nfile <= 10000ULL)
        (*b1001_10k)++;
    else if (nfile <= 100000ULL)
        (*b10001_100k)++;
    else if (nfile <= 1000000ULL)
        (*b100001_1m)++;
    else
        (*b1m_plus)++;
}

static void print_analyze_report(parent_map_t *map, const uint64_t *merged_depth, size_t shard_files,
                                 size_t parse_chunk_jobs) {
    size_t bi;
    uint64_t total_records = 0;
    uint64_t distinct_parents = 0;
    uint64_t parents_with_files = 0;
    uint64_t max_nfile = 0;
    uint64_t hb1 = 0, hb2 = 0, hb3 = 0, hb4 = 0, hb5 = 0, hb6 = 0, hb7 = 0, hb8 = 0;
    uint64_t megadir_100k = 0;
    analyze_sort_wrap_t *sort_arr = NULL;
    size_t sort_n = 0;
    size_t sort_cap = 0;
    unsigned di;
    uint64_t depth_records = 0;
    unsigned max_depth_bin = 0;

    for (bi = 0; bi < ANALYZE_HASH_BUCKETS; bi++) {
        parent_node_t *n = map->buckets[bi];
        while (n) {
            uint64_t tot = n->nfile + n->ndir + n->nsym + n->nother;

            total_records += tot;
            distinct_parents++;
            if (n->nfile > 0ULL) {
                parents_with_files++;
                analyze_hist_files_per_parent(n->nfile, &hb1, &hb2, &hb3, &hb4, &hb5, &hb6, &hb7, &hb8);
                if (n->nfile >= 100000ULL) megadir_100k++;
                if (n->nfile > max_nfile) max_nfile = n->nfile;
            }

            if (sort_n == sort_cap) {
                size_t nc = sort_cap ? sort_cap * 2 : 4096;
                analyze_sort_wrap_t *na = (analyze_sort_wrap_t *)realloc(sort_arr, nc * sizeof(*na));
                if (!na) {
                    free(sort_arr);
                    fprintf(stderr, "ecrawl_analyze: realloc failed for sort buffer\n");
                    return;
                }
                sort_arr = na;
                sort_cap = nc;
            }
            sort_arr[sort_n].node = n;
            sort_n++;

            n = n->next;
        }
    }

    if (sort_n > 0U && sort_arr) qsort(sort_arr, sort_n, sizeof(sort_arr[0]), cmp_analyze_nfile_desc);

    printf("ecrawl_analyze\n");
    printf("uid_shard_bin_files=%zu\n", shard_files);
    printf("parse_chunk_jobs=%zu\n", parse_chunk_jobs);
    printf("records_total=%" PRIu64 "\n", total_records);
    printf("distinct_parent_directories=%" PRIu64 "\n", distinct_parents);
    printf("parents_with_at_least_one_regular_file=%" PRIu64 "\n", parents_with_files);
    printf("max_regular_files_under_single_parent=%" PRIu64 "\n", max_nfile);
    printf("parents_with_regular_files_ge_100000=%" PRIu64 "\n", megadir_100k);
    printf("\n");
    printf("histogram_regular_files_per_parent (among parents with nfile>=1)\n");
    printf("  nfile==1: %" PRIu64 "\n", hb1);
    printf("  nfile_2_10: %" PRIu64 "\n", hb2);
    printf("  nfile_11_100: %" PRIu64 "\n", hb3);
    printf("  nfile_101_1000: %" PRIu64 "\n", hb4);
    printf("  nfile_1001_10000: %" PRIu64 "\n", hb5);
    printf("  nfile_10001_100000: %" PRIu64 "\n", hb6);
    printf("  nfile_100001_1000000: %" PRIu64 "\n", hb7);
    printf("  nfile_gt_1000000: %" PRIu64 "\n", hb8);
    printf("\n");

    printf("histogram_slash_count_in_stored_path (bin index = min(slash_count, %u))\n",
           (unsigned)(ANALYZE_DEPTH_BINS - 1U));
    for (di = 0; di < ANALYZE_DEPTH_BINS; di++) {
        depth_records += merged_depth[di];
        if (merged_depth[di] > 0ULL && di > max_depth_bin) max_depth_bin = di;
    }
    printf("records_used_for_depth_hist=%" PRIu64 "\n", depth_records);
    for (di = 0; di <= max_depth_bin && di < ANALYZE_DEPTH_BINS; di++) {
        if (merged_depth[di] > 0ULL) printf("  depth_bin_%u=%" PRIu64 "\n", di, merged_depth[di]);
    }
    printf("\n");

    printf("top_parents_by_regular_file_count (N=%u)\n", g_top_n);
    printf("# nfile ndir nsym nother path\n");
    {
        size_t lim = (size_t)g_top_n;
        size_t k;

        if (lim > sort_n) lim = sort_n;
        for (k = 0; k < lim; k++) {
            parent_node_t *no = sort_arr[k].node;
            printf("%zu %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %s\n", k + 1U, no->nfile, no->ndir, no->nsym,
                   no->nother, no->path);
        }
    }

    free(sort_arr);
}

static int run_analyze(const char *dir_path, char **names, size_t name_count, unsigned nthreads) {
    pthread_t *threads = NULL;
    pthread_t stats_thread;
    int stats_started = 0;
    analyze_stats_ctx_t sctx;
    analyze_pool_t pool;
    parent_map_t *merged = NULL;
    uint64_t merged_depth[ANALYZE_DEPTH_BINS];
    crawl_bin_file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    uint64_t chunk_byte_sum = 0;
    uint64_t *shard_sizes = NULL;
    unsigned ti;
    unsigned dj;
    int rc = 0;

    if (analyze_build_all_chunks(dir_path, names, name_count, &shard_sizes, &chunks, &chunk_count, &chunk_byte_sum) !=
        0) {
        fprintf(stderr, "ecrawl_analyze: failed to build chunk job list\n");
        return 1;
    }
    if (chunk_count == 0U) {
        fprintf(stderr, "ecrawl_analyze: no parse chunks produced\n");
        free(shard_sizes);
        return 1;
    }

    memset(&pool, 0, sizeof(pool));
    memset(merged_depth, 0, sizeof(merged_depth));
    pool.dir_path = dir_path;
    pool.names = names;
    pool.name_count = name_count;
    pool.chunks = chunks;
    pool.chunk_count = chunk_count;
    pool.shard_file_sizes = shard_sizes;
    atomic_init(&pool.chunk_cursor, 0);
    atomic_init(&pool.failures, 0);
    atomic_init(&pool.slot_assign, 0);
    pool.analyze_bytes_total = chunk_byte_sum;
    atomic_init(&pool.analyze_bytes_done, 0);
    atomic_init(&pool.analyze_records_done, 0);
    atomic_init(&pool.analyze_chunks_done, 0);
    atomic_init(&pool.analyze_stop_stats, 0);

    pool.maps = (parent_map_t **)calloc((size_t)nthreads, sizeof(*pool.maps));
    pool.depth_hist = (uint64_t **)calloc((size_t)nthreads, sizeof(*pool.depth_hist));
    threads = (pthread_t *)calloc((size_t)nthreads, sizeof(*threads));
    if (!pool.maps || !pool.depth_hist || !threads) {
        perror("ecrawl_analyze: alloc");
        free(pool.maps);
        free(pool.depth_hist);
        free(threads);
        crawl_bin_free_chunk_array_rows(chunks, chunk_count);
        free(shard_sizes);
        return 1;
    }

    sctx.pool = &pool;
    sctx.t0 = analyze_now_sec();
    if (pthread_create(&stats_thread, NULL, analyze_stats_thread_main, &sctx) == 0)
        stats_started = 1;
    else
        perror("ecrawl_analyze: pthread_create (progress)");

    for (ti = 0; ti < nthreads; ti++) {
        if (pthread_create(&threads[ti], NULL, analyze_worker_main, &pool) != 0) {
            unsigned j;

            perror("pthread_create");
            atomic_store_explicit(&pool.chunk_cursor, pool.chunk_count, memory_order_relaxed);
            if (stats_started) {
                atomic_store_explicit(&pool.analyze_stop_stats, 1, memory_order_relaxed);
                pthread_join(stats_thread, NULL);
                stats_started = 0;
            }
            analyze_clear_progress_line();
            for (j = 0; j < ti; j++) pthread_join(threads[j], NULL);
            rc = 1;
            merged = NULL;
            for (j = 0; j < nthreads; j++) {
                if (pool.maps[j]) parent_map_free(pool.maps[j]);
                free(pool.depth_hist[j]);
                pool.maps[j] = NULL;
                pool.depth_hist[j] = NULL;
            }
            free(pool.maps);
            free(pool.depth_hist);
            free(threads);
            crawl_bin_free_chunk_array_rows(pool.chunks, pool.chunk_count);
            free(pool.shard_file_sizes);
            pool.chunks = NULL;
            pool.shard_file_sizes = NULL;
            return rc;
        }
    }

    for (ti = 0; ti < nthreads; ti++) pthread_join(threads[ti], NULL);
    free(threads);
    threads = NULL;

    if (stats_started) {
        atomic_store_explicit(&pool.analyze_stop_stats, 1, memory_order_relaxed);
        pthread_join(stats_thread, NULL);
    }
    analyze_clear_progress_line();

    merged = NULL;
    {
        unsigned nm = nthreads;

        if (analyze_parallel_reduce_parent_maps(pool.maps, &nm, nthreads, nthreads) != 0 || nm != 1U) {
            fprintf(stderr, "ecrawl_analyze: parallel map merge unavailable; using sequential merge\n");
            merged = parent_map_new();
            if (!merged) {
                perror("parent_map_new");
                rc = 1;
            } else {
                for (ti = 0; ti < nthreads; ti++) {
                    if (pool.maps[ti]) {
                        parent_map_merge_into(merged, pool.maps[ti]);
                        parent_map_free(pool.maps[ti]);
                        pool.maps[ti] = NULL;
                    }
                }
            }
        } else {
            merged = pool.maps[0];
            pool.maps[0] = NULL;
        }
    }

    for (ti = 0; ti < nthreads; ti++) {
        if (pool.maps[ti]) {
            parent_map_free(pool.maps[ti]);
            pool.maps[ti] = NULL;
        }
        if (pool.depth_hist[ti]) {
            if (merged) {
                for (dj = 0; dj < ANALYZE_DEPTH_BINS; dj++) merged_depth[dj] += pool.depth_hist[ti][dj];
            }
            free(pool.depth_hist[ti]);
            pool.depth_hist[ti] = NULL;
        }
    }

    if (merged) {
        print_analyze_report(merged, merged_depth, name_count, chunk_count);
        parent_map_free(merged);
    }

    crawl_bin_free_chunk_array_rows(pool.chunks, pool.chunk_count);
    free(pool.shard_file_sizes);
    pool.chunks = NULL;
    pool.shard_file_sizes = NULL;

    free(pool.maps);
    free(pool.depth_hist);
    if (atomic_load_explicit(&pool.failures, memory_order_relaxed) > 0) rc = 1;
    return rc;
}

int main(int argc, char **argv) {
    const char *dir_path = NULL;
    static char dir_path_abs[PATH_MAX];
    DIR *dp;
    struct dirent *de;
    char **names = NULL;
    size_t name_count = 0, name_cap = 0;
    unsigned nthreads = parse_analyze_threads_env();
    int i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0)
            g_verbose = 1;
        else if (strcmp(argv[i], "--top") == 0) {
            char *end;
            unsigned long v;

            if (i + 1 >= argc) {
                fprintf(stderr, "%s: --top requires N\n", argv[0]);
                usage(argv[0]);
                return 2;
            }
            errno = 0;
            v = strtoul(argv[++i], &end, 10);
            if (errno || end == argv[i] || *end != '\0' || v < 1UL || v > 100000UL) {
                fprintf(stderr, "%s: --top N expects 1 <= N <= 100000\n", argv[0]);
                usage(argv[0]);
                return 2;
            }
            g_top_n = (unsigned)v;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown option: %s\n", argv[i]);
            usage(argv[0]);
            return 2;
        } else if (!dir_path) {
            dir_path = argv[i];
        } else {
            fprintf(stderr, "extra argument: %s\n", argv[i]);
            usage(argv[0]);
            return 2;
        }
    }

    if (!dir_path) {
        usage(argv[0]);
        return 2;
    }

    if (path_resolve_existing(dir_path, dir_path_abs, "ecrawl_analyze: ") != 0) return 2;
    dir_path = dir_path_abs;

    dp = opendir(dir_path);
    if (!dp) {
        perror(dir_path);
        return 1;
    }

    while ((de = readdir(dp)) != NULL) {
        if (!is_uid_shard_bin_name(de->d_name)) continue;
        if (name_count == name_cap) {
            size_t nc = name_cap ? name_cap * 2 : 64;
            char **p = (char **)realloc(names, nc * sizeof(*p));
            if (!p) {
                perror("realloc");
                closedir(dp);
                for (i = 0; i < (int)name_count; i++) free(names[i]);
                free(names);
                return 1;
            }
            names = p;
            name_cap = nc;
        }
        names[name_count] = strdup(de->d_name);
        if (!names[name_count]) {
            perror("strdup");
            closedir(dp);
            for (i = 0; i < (int)name_count; i++) free(names[i]);
            free(names);
            return 1;
        }
        name_count++;
    }
    closedir(dp);

    if (name_count == 0) {
        fprintf(stderr, "%s: no uid_shard_*.bin files found\n", dir_path);
        return 1;
    }

    qsort(names, name_count, sizeof(names[0]), cmp_strptr);

    if ((size_t)nthreads > name_count) nthreads = (unsigned)name_count;
    if (nthreads < 1) nthreads = 1;

    {
        int ar = run_analyze(dir_path, names, name_count, nthreads);

        for (i = 0; i < (int)name_count; i++) free(names[i]);
        free(names);
        return ar ? 1 : 0;
    }
}
