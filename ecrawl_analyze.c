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
 * ECRAWL_REPAIR_THREADS is used for compatibility with older workflows. Work is split
 * by chunk, not by shard, so a single huge single-UID shard still scales across cores;
 * each shard's catalog is loaded once and shared read-only by all of its chunks.
 * Checkpoint segments are 32 MiB apart, which is coarser than the thread budget on a
 * small capture, so segments are subdivided at block boundaries (see
 * parse_split_target_bytes) — without that a capture under 32 MiB ran on one core.
 * The parent-directory map is shared by all workers (stripe-locked inserts, atomic
 * per-parent counters) rather than built per worker and merged; see parent_map_t.
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
#include <time.h>
#include <unistd.h>
#include <limits.h>

#include "crawl_bin_format.h"
#include "alloc_tuning.h"
#include "crawl_bin_block.h"
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

/*
 * Query mode (--subtree / --size-gt / --type / --gid / --perm / --list): the same parallel scan,
 * with a record predicate instead of the directory-shape histograms. This is how
 * the capture answers "files over N bytes", "bytes under a subtree" and "files
 * under a subtree" without a second index — the questions ereport could only
 * answer by rebuilding every path.
 */
typedef struct {
    int active;
    const char *subtree;   /* absolute, no trailing slash; NULL = whole capture */
    size_t subtree_len;
    const char *sub_base;  /* last component of subtree, for the root-record test */
    size_t sub_base_len;
    int subtree_is_root;   /* subtree == "/": every record qualifies */
    uint64_t size_gt;      /* strictly greater, matching find -size +Nc */
    int have_size_gt;
    int type_filter;       /* 0 = any type, else 'f' / 'd' / 'l' / … */
    uint32_t gid;          /* --gid: exact group owner */
    int have_gid;
    uint32_t perm;         /* --perm: permission bits, masked to 07777 */
    int perm_mode;         /* 0 = no filter, else PERM_EXACT / PERM_ALL / PERM_ANY */
    int list_paths;        /* print matching paths instead of counting them */
    int block_skip;        /* use block header summaries to skip blocks (see ECRAWL_ANALYZE_BLOCK_SKIP) */
    int exact;             /* --exact: never answer from catalog rollups, always scan records */
} query_spec_t;

/* --perm forms, mirroring find(1): MODE is exact, -MODE is "all of these bits",
 * /MODE is "any of these bits". */
enum { PERM_NONE = 0, PERM_EXACT, PERM_ALL, PERM_ANY };

static query_spec_t g_query;

/* Permission-bit test against the recorded st_mode. */
static int query_perm_match(uint32_t mode) {
    uint32_t bits = mode & 07777U;

    switch (g_query.perm_mode) {
        case PERM_EXACT: return bits == g_query.perm;
        case PERM_ALL: return (bits & g_query.perm) == g_query.perm;
        case PERM_ANY: return g_query.perm == 0U || (bits & g_query.perm) != 0U;
        default: return 1;
    }
}
/* Top-list dimensions (selected via --top[,dim...]): dense = most regular files; deep = deepest directories. */
static int g_top_dense = 1;
static int g_top_deep = 0;

/* Larger table → shorter collision chains on lookup, which is now the only thing the table does. */
#define ANALYZE_HASH_BUCKETS 262144U
#define ANALYZE_DEPTH_BINS 64U
/*
 * Insert locks per worker thread. Only chain insertion is serialized, and only against the threads that
 * draw the same stripe, so more stripes than threads keeps that collision rare while the array stays small
 * (a mutex per stripe, not per bucket).
 */
#define ANALYZE_INSERT_STRIPES_PER_THREAD 8U
#define ANALYZE_INSERT_STRIPES_MAX 4096U

typedef struct parent_node {
    struct parent_node *next;
    char *path;
    /*
     * Bumped by whichever worker reads a child record under this parent, so they are atomic. Relaxed is
     * enough: nothing is ordered against them and they are only read after every worker has joined.
     */
    _Atomic uint64_t nfile;
    _Atomic uint64_t ndir;
    _Atomic uint64_t nsym;
    _Atomic uint64_t nother;
} parent_node_t;

/*
 * One map for the whole run, shared by every parse worker.
 *
 * Each worker used to fill a private map which were then reduced pairwise. Nearly every directory lives in
 * exactly one chunk, so that reduce re-hashed, re-compared and re-allocated almost every node it touched —
 * it repeated the entire build, and cost more than half the CPU of a shape run (parent_map_merge_into 27.7%
 * plus the merge threads' 23.9%). Sharing the map instead means a parent that spans chunks is simply found
 * by the second worker, and the report reads the map the workers filled.
 */
typedef struct {
    parent_node_t *buckets[ANALYZE_HASH_BUCKETS];
    pthread_mutex_t *stripes;
    size_t stripe_count; /* power of two; stripe = bucket & (stripe_count - 1) */
} parent_map_t;

/*
 * Per-catalog memo indexed by on-disk parent_dir_id: maps a directory id to its resolved parent node and
 * depth bin. Within one shard catalog, parent_dir_id uniquely identifies a directory (and thus a fixed
 * parent path and slash-count), so repeated child records reuse the result instead of rebuilding the path,
 * hashing it, and walking the bucket chain. Allocation is skipped when a shard's dir-id space is very large.
 */
#define ANALYZE_DIR_CACHE_MAX_ENTRIES (4U * 1000U * 1000U)
typedef struct {
    parent_node_t *node; /* resolved parent node; NULL if parent could not be derived */
    unsigned depth_bin;  /* cached depth-histogram bin for entries under this directory */
    unsigned char state; /* 0=unseen, 1=seen with depth, 2=seen without depth */
} analyze_dir_cache_ent_t;

/*
 * Where --subtree lands in one shard's catalog. Membership is the DFS range test
 * dfs_lo <= dfs_index[parent_dir_id] < dfs_hi, which is O(1) per record against
 * an array the catalog already carries. Earlier this was a per-shard byte array
 * painted by walking every directory's parent chain; the permutation makes both
 * the extra pass and the extra allocation unnecessary. Built once per shard,
 * beside the catalog it indexes.
 */
typedef struct {
    const uint64_t *dfs_index; /* borrowed from the catalog; NULL if unavailable */
    uint64_t max_dir_id;
    uint64_t root_id; /* 0 = this shard holds nothing under the subtree */
    uint64_t dfs_lo;
    uint64_t dfs_hi;
    int whole; /* subtree covers the entire catalog */
} shard_subtree_t;

static inline int subtree_contains(const shard_subtree_t *s, uint64_t dir_id) {
    uint64_t p;

    if (!s) return 0;
    if (s->whole) return 1;
    if (!s->dfs_index || s->root_id == 0ULL || dir_id == 0ULL || dir_id > s->max_dir_id) return 0;
    p = s->dfs_index[dir_id];
    return p >= s->dfs_lo && p < s->dfs_hi;
}

/* One inode whose byte credit is deferred until the global hardlink merge. */
typedef struct {
    uint32_t dev_major;
    uint32_t dev_minor;
    uint64_t inode;
    uint64_t size;
} query_hardlink_t;

typedef struct {
    uint64_t entries;
    uint64_t files;
    uint64_t dirs;
    uint64_t symlinks;
    uint64_t other;
    uint64_t bytes; /* apparent bytes of everything except the deferred hardlinks */
    query_hardlink_t *hl;
    size_t hl_count;
    size_t hl_cap;
    char *out; /* path output, flushed to stdout in large batches */
    size_t out_len;
    size_t out_cap;
    uint64_t blocks_decompressed;
    uint64_t blocks_skipped;
    uint64_t records_skipped;
    int oom;
} query_result_t;

typedef struct {
    const char *dir_path;
    char **names;
    size_t name_count;
    crawl_bin_file_chunk_t *chunks;
    size_t chunk_count;
    uint64_t *shard_file_sizes;
    /*
     * One catalog per shard file, loaded once and shared read-only across every
     * chunk of that shard. Previously each chunk reloaded the catalog from the
     * file (nine arrays plus a strdup per directory name) — for a single-UID
     * shard split into many ckpt segments that repeated the full, expensive
     * catalog build for every segment. Loaded lazily under shard_cat_lock and
     * freed as soon as a shard's last chunk completes (shard_chunks_left==0) so
     * peak memory stays bounded for many-shard corpora.
     */
    crawl_bin_catalog_t *shard_cat;       /* array[name_count] */
    shard_subtree_t *shard_sub;           /* array[name_count]; query mode with --subtree only */
    unsigned char *shard_cat_state;       /* 0=unloaded 1=ready 2=failed 3=freed */
    _Atomic uint64_t *shard_chunks_left;  /* per shard; catalog freed when it reaches 0 */
    pthread_mutex_t shard_cat_lock;
    _Atomic size_t chunk_cursor;
    _Atomic int failures;
    _Atomic unsigned slot_assign;
    parent_map_t *map; /* shared by every parse worker; NULL in query mode */
    uint64_t **depth_hist;
    query_result_t *qres; /* array[nthreads]; query mode only */
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

        /*
         * ~1s cadence, but wake promptly when asked to stop. A single sleep(1)
         * here made shutdown block up to a full second on the join, which
         * dominated the wall time of fast (sub-second) analyses and hid the
         * per-shard parallelism win. Poll the stop flag in short slices instead.
         */
        {
            int k;
            for (k = 0; k < 20; k++) {
                struct timespec ts = { 0, 50 * 1000 * 1000L }; /* 50 ms */
                if (atomic_load_explicit(&p->analyze_stop_stats, memory_order_relaxed)) break;
                nanosleep(&ts, NULL);
            }
        }
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

/*
 * Normalise --subtree into the form the catalogs store: absolute, no trailing
 * slash, no "." or "..". The capture holds resolved paths, so a subtree that
 * does not match textually would silently select nothing.
 */
static int query_set_subtree(const char *arg, const char *prog) {
    static char buf[PATH_MAX];
    size_t len;

    if (path_resolve_existing(arg, buf, "ecrawl_analyze: --subtree ") != 0) {
        /* A capture outlives the tree it describes, and can be read on a host
         * that never had it. An absolute path is usable as written. */
        if (arg[0] != '/') return -1;
        if (strlen(arg) >= sizeof(buf)) {
            fprintf(stderr, "%s: --subtree path too long\n", prog);
            return -1;
        }
        strcpy(buf, arg);
        fprintf(stderr, "ecrawl_analyze: --subtree %s does not exist here; matching the capture literally\n", arg);
    }
    len = strlen(buf);
    while (len > 1U && buf[len - 1U] == '/') buf[--len] = '\0';
    if (buf[0] != '/') {
        fprintf(stderr, "%s: --subtree must be absolute\n", prog);
        return -1;
    }
    g_query.subtree = buf;
    g_query.subtree_len = len;
    g_query.subtree_is_root = (len == 1U);
    {
        const char *slash = strrchr(buf, '/');
        g_query.sub_base = slash ? slash + 1 : buf;
        g_query.sub_base_len = strlen(g_query.sub_base);
    }
    g_query.active = 1;
    return 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--verbose] [--top[,dim...] N] <crawl-output-dir>\n"
            "       %s [--subtree DIR] [--size-gt N] [--type f|d|l|c|b|p|s|o] [--gid N] [--perm MODE]\n"
            "          [--list] <crawl-output-dir>\n"
            "  Read-only parallel scan of uid_shard_*.bin shards; directory-shape stats on stdout.\n"
            "  Uses .ckpt segment boundaries when sidecars are valid; else one range per shard.\n"
            "  Parallel threads: ECRAWL_ANALYZE_THREADS (default %u), or ECRAWL_REPAIR_THREADS if unset.\n"
            "  Live bytes/chunks/records + ETA on stderr when stderr is a terminal.\n"
            "  --top N: list top N parents by regular-file count (default %u). Same as --top,dense N.\n"
            "  --top,DIM[,DIM] N: choose one or more top lists (order-independent):\n"
            "      dense = top N parents by regular-file count\n"
            "      deep  = top N deepest parent directories (by path slash count)\n"
            "    e.g. --top,deep N (deepest only) or --top,dense,deep N (both lists).\n"
            "\n"
            "  Query form (any of --subtree/--size-gt/--type/--gid/--perm/--list): selects records\n"
            "  instead of reporting directory shape. Filters combine with AND.\n"
            "    --subtree DIR  only records at or under DIR (DIR itself included, as du counts it)\n"
            "    --size-gt N    only records larger than N bytes (find -size +Nc)\n"
            "    --type C       only records of that type: f d l c b p s o (find -type)\n"
            "    --gid N        only records owned by numeric group N\n"
            "    --perm MODE    permission bits, octal, in the three find -perm forms:\n"
            "                     0644  exactly these bits    e.g. --perm 0777\n"
            "                     -MODE all of these bits     e.g. --perm -0002 (world-writable)\n"
            "                     /MODE any of these bits     e.g. --perm /0022 (group- or world-writable)\n"
            "    --list         print each matching path on stdout; the totals move to stderr\n"
            "    --exact        never answer from catalog rollups; always scan the records\n"
            "  A bare --subtree DIR aggregate is answered from the per-directory rollups the crawl\n"
            "  already computed, reading no records at all, so its cost is O(directories) rather than\n"
            "  O(files). That shortcut is taken only when it provably equals the scan: it is skipped\n"
            "  when any record in the subtree has nlink > 1, since crawl-time hardlink credit is\n"
            "  attributed to the first link seen anywhere in the tree while a scan dedups within the\n"
            "  subtree. --exact forces the scan; answered_from= in the output says which ran.\n"
            "  Row groups whose column zone maps cannot match the size/type/gid filters are skipped\n"
            "  without decompressing; set ECRAWL_ANALYZE_BLOCK_SKIP=0 to decode every row group.\n"
            "  Totals are key=value lines: entries, files, dirs, symlinks, other, bytes,\n"
            "  hardlink_dupes, records_scanned, block skip diagnostics, answered_from, elapsed_sec.\n"
            "  bytes is apparent size with each multiply-linked inode counted once, so it matches du -sb.\n",
            prog, prog, DEFAULT_ANALYZE_THREADS, g_top_n);
}

/*
 * Parse the dimension suffix of a --top flag. spec is the text after "--top": "" selects the default
 * (dense), otherwise it begins with ',' followed by a comma-separated list of dense/deep. Returns 0 on
 * success (and sets g_top_dense/g_top_deep), -1 on a malformed or unknown dimension.
 */
static int parse_top_dims(const char *spec, const char *prog) {
    char buf[64];
    char *tok;
    char *save = NULL;
    int dense = 0;
    int deep = 0;

    if (spec[0] == '\0') {
        g_top_dense = 1;
        g_top_deep = 0;
        return 0;
    }
    spec++; /* skip leading ',' */
    if (spec[0] == '\0') {
        fprintf(stderr, "%s: --top, requires at least one dimension (dense,deep)\n", prog);
        return -1;
    }
    if (strlen(spec) >= sizeof(buf)) {
        fprintf(stderr, "%s: --top dimension list too long\n", prog);
        return -1;
    }
    strcpy(buf, spec);
    for (tok = strtok_r(buf, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
        if (strcmp(tok, "dense") == 0)
            dense = 1;
        else if (strcmp(tok, "deep") == 0)
            deep = 1;
        else {
            fprintf(stderr, "%s: unknown --top dimension '%s' (expected dense or deep)\n", prog, tok);
            return -1;
        }
    }
    if (!dense && !deep) {
        fprintf(stderr, "%s: --top needs a dimension (dense,deep)\n", prog);
        return -1;
    }
    g_top_dense = dense;
    g_top_deep = deep;
    return 0;
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

/* nthreads: workers that will share the map, which sets how finely inserts are striped. */
static parent_map_t *parent_map_new(unsigned nthreads) {
    parent_map_t *m = (parent_map_t *)calloc(1, sizeof(*m));
    size_t want;
    size_t i;

    if (!m) return NULL;

    want = (size_t)(nthreads ? nthreads : 1U) * (size_t)ANALYZE_INSERT_STRIPES_PER_THREAD;
    if (want > (size_t)ANALYZE_INSERT_STRIPES_MAX) want = (size_t)ANALYZE_INSERT_STRIPES_MAX;
    m->stripe_count = 1U;
    while (m->stripe_count < want) m->stripe_count <<= 1;

    m->stripes = (pthread_mutex_t *)calloc(m->stripe_count, sizeof(*m->stripes));
    if (!m->stripes) {
        free(m);
        return NULL;
    }
    for (i = 0; i < m->stripe_count; i++) {
        if (pthread_mutex_init(&m->stripes[i], NULL) != 0) {
            while (i > 0U) pthread_mutex_destroy(&m->stripes[--i]);
            free(m->stripes);
            free(m);
            return NULL;
        }
    }
    return m;
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
    for (bi = 0; bi < m->stripe_count; bi++) pthread_mutex_destroy(&m->stripes[bi]);
    free(m->stripes);
    free(m);
}

static inline void parent_node_bump(parent_node_t *n, uint8_t typ) {
    _Atomic uint64_t *c;

    if (typ == (uint8_t)'f')
        c = &n->nfile;
    else if (typ == (uint8_t)'d')
        c = &n->ndir;
    else if (typ == (uint8_t)'l')
        c = &n->nsym;
    else
        c = &n->nother;
    atomic_fetch_add_explicit(c, 1ULL, memory_order_relaxed);
}

/*
 * Find the node for parent, creating it (counters zeroed) if absent. Returns NULL only on allocation
 * failure. Safe to call from every worker at once: the stripe lock covers both the chain walk and the
 * insert, and the returned node stays valid for the rest of the run (nodes are never moved or freed until
 * the report is done with them), so callers may memo the pointer.
 */
static parent_node_t *parent_map_get_or_add(parent_map_t *m, const char *parent) {
    uint32_t hx = analyze_hash_parent(parent);
    size_t bi = (size_t)(hx % ANALYZE_HASH_BUCKETS);
    pthread_mutex_t *lock = &m->stripes[bi & (m->stripe_count - 1U)];
    parent_node_t *node;

    pthread_mutex_lock(lock);
    for (node = m->buckets[bi]; node; node = node->next) {
        if (strcmp(node->path, parent) == 0) {
            pthread_mutex_unlock(lock);
            return node;
        }
    }
    node = (parent_node_t *)malloc(sizeof(*node));
    if (!node) {
        pthread_mutex_unlock(lock);
        return NULL;
    }
    node->path = strdup(parent);
    if (!node->path) {
        free(node);
        pthread_mutex_unlock(lock);
        return NULL;
    }
    atomic_init(&node->nfile, 0ULL);
    atomic_init(&node->ndir, 0ULL);
    atomic_init(&node->nsym, 0ULL);
    atomic_init(&node->nother, 0ULL);
    node->next = m->buckets[bi];
    m->buckets[bi] = node;
    pthread_mutex_unlock(lock);
    return node;
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

/*
 * Append one [lo, hi) segment, subdividing it at block boundaries when the
 * caller asked for smaller jobs. Checkpoint segments are 32 MiB apart, so a
 * capture of a few hundred MiB yields only a handful of jobs and leaves most of
 * the thread budget idle. Splitting costs one pass over the block headers.
 */
static int analyze_append_segment_jobs(crawl_bin_file_chunk_t **all, size_t *all_n, size_t *all_cap,
                                       const char *path, uint64_t lo, uint64_t hi, size_t file_index,
                                       uint64_t *byte_sum, uint64_t split_target_bytes);

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

static int analyze_append_segment_jobs(crawl_bin_file_chunk_t **all, size_t *all_n, size_t *all_cap,
                                       const char *path, uint64_t lo, uint64_t hi, size_t file_index,
                                       uint64_t *byte_sum, uint64_t split_target_bytes) {
    crawl_bin_file_chunk_t *parts = NULL;
    size_t part_count = 0;
    unsigned int counter = 0;
    size_t i;
    int rc = 0;

    if (lo >= hi) return -1;
    if (split_target_bytes == 0ULL || hi - lo <= split_target_bytes)
        return analyze_append_chunk_job(all, all_n, all_cap, path, lo, hi, file_index, byte_sum);

    if (crawl_bin_build_chunks_for_segment(&analyze_chunk_io, path, file_index, split_target_bytes, lo, hi, &parts,
                                           &part_count, &counter) != 0 ||
        part_count == 0U) {
        /* A segment that will not split is still scannable whole. */
        crawl_bin_free_chunk_array_rows(parts, part_count);
        return analyze_append_chunk_job(all, all_n, all_cap, path, lo, hi, file_index, byte_sum);
    }
    for (i = 0; i < part_count && rc == 0; i++)
        rc = analyze_append_chunk_job(all, all_n, all_cap, parts[i].path, parts[i].start_offset,
                                      parts[i].end_offset, file_index, byte_sum);
    crawl_bin_free_chunk_array_rows(parts, part_count);
    return rc;
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

/*
 * Build parse jobs from .ckpt segment boundaries only (no prescan of shard bodies).
 * Falls back to one job per shard [sizeof(hdr), file_size) when sidecar is missing or invalid.
 */
static int analyze_build_all_chunks(const char *dir_path, char **names, size_t name_count,
                                    uint64_t **shard_sizes_out, crawl_bin_file_chunk_t **chunks_out,
                                    size_t *chunk_count_out, uint64_t *chunk_bytes_total_out,
                                    uint64_t split_target_bytes) {
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

            if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
                fprintf(stderr, "ecrawl_analyze: skipping %s: format version %u, expected %u (re-crawl needed)\n",
                        names[fi], fh.version, FORMAT_VERSION);
                continue;
            }
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
                        if (analyze_append_segment_jobs(&all, &all_n, &all_cap, full, lo, hi, fi, &byte_sum,
                                                        split_target_bytes) != 0) {
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
            if (analyze_append_segment_jobs(&all, &all_n, &all_cap, full, hdr_end, record_end, fi, &byte_sum,
                                            split_target_bytes) != 0)
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

static int analyze_scan_fp_until(FILE *fp, uint64_t start_off, uint64_t scan_end_exclusive, uint64_t file_sz,
                                 const crawl_bin_catalog_t *cat, unsigned char *pathbuf, char *parentbuf,
                                 char *fullpath_buf, size_t fullpath_sz, parent_map_t *map, uint64_t *depth_hist,
                                 analyze_dir_cache_ent_t *dir_cache, uint64_t dir_cache_max, uint64_t *nrec_out) {
    uint64_t nrec = 0;
    crawl_bin_block_reader_t br;
    crawl_bin_chunk_stdio_t bio;

    (void)pathbuf; /* names now come from the block reader's decompression buffer */
    (void)file_sz;
    if (nrec_out) *nrec_out = 0;

    bio.fopen = NULL;
    bio.fread = fread_unlocked; /* fp is owned by one thread; unlocked is safe */
    bio.fclose = NULL;
    if (crawl_bin_block_reader_init(&br, &bio, fp, start_off, scan_end_exclusive) != 0) return -1;
    /* Directory-shape stats need the parent, the type and the name. Sizes,
     * timestamps and ownership are eight more columns this pass never reads, so
     * leaving them out of the projection skips both their I/O and their decode. */
    (void)crawl_bin_block_reader_set_projection(&br, CRAWL_COL_BIT(CRAWL_COL_PARENT_DIR_ID) |
                                                         CRAWL_COL_BIT(CRAWL_COL_TYPE) |
                                                         CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES));

    for (;;) {
        bin_record_hdr_t rh;
        const unsigned char *rec_name = NULL;
        uint64_t pid;
        int got = crawl_bin_block_reader_next(&br, &rh, &rec_name);

        if (got == 0) break;
        if (got < 0) {
            crawl_bin_block_reader_free(&br);
            return -1;
        }

        pid = rh.parent_dir_id;
        if (pid == 0ULL) {
            crawl_bin_block_reader_free(&br);
            return -1;
        }

        if (dir_cache && pid <= dir_cache_max && dir_cache[pid].state != 0) {
            analyze_dir_cache_ent_t *ce = &dir_cache[pid];

            if (ce->state == 1) {
                depth_hist[ce->depth_bin]++;
                if (ce->node) parent_node_bump(ce->node, rh.type);
            }
        } else {
            parent_node_t *node = NULL;
            unsigned db = 0;
            unsigned char st = 2; /* default: no depth recorded */

            if (crawl_bin_catalog_entry_path(cat, pid, (char *)rec_name, rh.name_len, fullpath_buf, fullpath_sz) != 0) {
                crawl_bin_block_reader_free(&br);
                return -1;
            }
            {
                size_t flen = strlen(fullpath_buf);
                uint16_t pl = flen > 65535U ? 65535U : (uint16_t)flen;

                if (flen > 0) {
                    db = analyze_depth_slash_bin((unsigned char *)fullpath_buf, pl);
                    depth_hist[db]++;
                    st = 1;
                    if (parent_dir_from_path((unsigned char *)fullpath_buf, pl, parentbuf, 65536U) == 0) {
                        node = parent_map_get_or_add(map, parentbuf);
                        if (node) parent_node_bump(node, rh.type);
                    }
                }
            }
            if (dir_cache && pid <= dir_cache_max) {
                dir_cache[pid].node = node;
                dir_cache[pid].depth_bin = db;
                dir_cache[pid].state = st;
            }
        }
        nrec++;
    }

    crawl_bin_block_reader_free(&br);
    if (nrec_out) *nrec_out = nrec;
    return 0;
}

/*
 * Locate g_query.subtree in one catalog and record its DFS range.
 *
 * One pass over the directories to find the subtree's own dir_id by name -- a
 * component compare, so only namesakes cost a path rebuild -- and then the
 * range comes straight out of the permutation. Membership for every record is a
 * comparison against that range afterwards.
 */
static int subtree_build(const crawl_bin_catalog_t *cat, shard_subtree_t *out) {
    uint64_t did;
    char pathbuf[PATH_MAX];

    memset(out, 0, sizeof(*out));
    if (!cat) return -1;
    out->max_dir_id = cat->max_dir_id;
    if (g_query.subtree_is_root) {
        out->whole = 1;
        return 0;
    }
    if (!cat->dfs_index || !cat->dfs_subtree_dirs) {
        /* A capture written before the post-pass, or a catalog loaded without
         * the subtree group: refuse rather than silently matching nothing. */
        errno = EINVAL;
        return -1;
    }

    for (did = 1; did <= cat->max_dir_id; did++) {
        if ((size_t)cat->name_len[did] != g_query.sub_base_len) continue;
        if (!cat->name_comp[did]) continue;
        if (memcmp(cat->name_comp[did], g_query.sub_base, g_query.sub_base_len) != 0) continue;
        if (crawl_bin_catalog_dir_path(cat, did, pathbuf, sizeof(pathbuf)) != 0) continue;
        if (strcmp(pathbuf, g_query.subtree) == 0) {
            out->root_id = did;
            break; /* a path is unique within a catalog */
        }
    }
    if (out->root_id == 0) return 0; /* this shard holds nothing under the subtree */

    out->dfs_index = cat->dfs_index;
    out->dfs_lo = cat->dfs_index[out->root_id];
    out->dfs_hi = out->dfs_lo + cat->dfs_subtree_dirs[out->root_id];
    return 0;
}

static void subtree_free(shard_subtree_t *s) {
    if (!s) return;
    /* dfs_index is borrowed from the catalog, which owns it. */
    memset(s, 0, sizeof(*s));
}

static pthread_mutex_t g_query_out_mutex = PTHREAD_MUTEX_INITIALIZER;
#define QUERY_OUT_FLUSH_BYTES (256U * 1024U)

static void query_out_flush(query_result_t *qr) {
    if (!qr->out || qr->out_len == 0U) return;
    pthread_mutex_lock(&g_query_out_mutex);
    if (fwrite(qr->out, 1, qr->out_len, stdout) != qr->out_len) qr->oom = 1;
    pthread_mutex_unlock(&g_query_out_mutex);
    qr->out_len = 0;
}

static int query_out_append(query_result_t *qr, const char *path, size_t len) {
    if (qr->out_len + len + 1U > qr->out_cap) {
        query_out_flush(qr);
        if (len + 1U > qr->out_cap) {
            size_t nc = qr->out_cap ? qr->out_cap : QUERY_OUT_FLUSH_BYTES;
            char *np;

            while (nc < len + 1U) nc <<= 1;
            np = (char *)realloc(qr->out, nc);
            if (!np) return -1;
            qr->out = np;
            qr->out_cap = nc;
        }
    }
    memcpy(qr->out + qr->out_len, path, len);
    qr->out_len += len;
    qr->out[qr->out_len++] = '\n';
    if (qr->out_len >= QUERY_OUT_FLUSH_BYTES) query_out_flush(qr);
    return 0;
}

static int query_hardlink_defer(query_result_t *qr, const bin_record_hdr_t *rh) {
    if (qr->hl_count == qr->hl_cap) {
        size_t nc = qr->hl_cap ? qr->hl_cap * 2U : 1024U;
        query_hardlink_t *np = (query_hardlink_t *)realloc(qr->hl, nc * sizeof(*np));

        if (!np) return -1;
        qr->hl = np;
        qr->hl_cap = nc;
    }
    qr->hl[qr->hl_count].dev_major = rh->dev_major;
    qr->hl[qr->hl_count].dev_minor = rh->dev_minor;
    qr->hl[qr->hl_count].inode = rh->inode;
    qr->hl[qr->hl_count].size = rh->size;
    qr->hl_count++;
    return 0;
}

static const crawl_bin_catalog_t *analyze_get_shard_catalog(analyze_pool_t *p, size_t fi, const char *full_path);

/*
 * Deferred catalog handle for one chunk. A query only needs the catalog to turn a matched
 * record into a path, so loading it up front made a query that matches nothing (or that
 * only counts) pay the full per-shard catalog build for no benefit — 55% of the profile on
 * a 782k-parent shard whose blocks were all skipped. Resolve on first actual use instead.
 */
typedef struct {
    analyze_pool_t *pool;
    size_t fi;
    const char *path;
    const crawl_bin_catalog_t *cat;
    unsigned char resolved;
} query_cat_lazy_t;

static void query_cat_lazy_init(query_cat_lazy_t *lz, analyze_pool_t *p, size_t fi, const char *path) {
    lz->pool = p;
    lz->fi = fi;
    lz->path = path;
    lz->cat = NULL;
    lz->resolved = 0;
}

/* NULL when the shard's catalog is missing or fails to load; callers treat that as an error
 * so a bad catalog still fails loudly rather than yielding pathless results. */
static const crawl_bin_catalog_t *query_cat_lazy_get(query_cat_lazy_t *lz) {
    if (!lz->resolved) {
        lz->cat = analyze_get_shard_catalog(lz->pool, lz->fi, lz->path);
        lz->resolved = 1;
    }
    return lz->cat;
}

static int query_scan_fp_until(FILE *fp, uint64_t start_off, uint64_t scan_end_exclusive,
                               query_cat_lazy_t *lz, const shard_subtree_t *sub,
                               crawl_bin_block_reader_t *br,
                               char *fullpath_buf, size_t fullpath_sz, query_result_t *qr,
                               uint64_t *nrec_out) {
    uint64_t nrec = 0;
    crawl_bin_chunk_stdio_t bio;
    char parent_path[PATH_MAX];
    uint64_t cached_parent_id = 0;
    size_t cached_parent_len = 0;

    if (nrec_out) *nrec_out = 0;
    bio.fopen = NULL;
    bio.fread = fread_unlocked;
    bio.fclose = NULL;
    if (crawl_bin_block_reader_reinit(br, &bio, fp, start_off, scan_end_exclusive) != 0) return -1;
    /*
     * The query predicate reads the parent, the type and the size; the byte
     * total additionally needs nlink plus (dev, inode) to dedup hardlinks the
     * way du does. Timestamps and uid are never consulted here, and gid/mode
     * only when --gid or --perm ask for them. Names are only needed to print
     * paths, and to recognise the subtree's own directory record, whose parent
     * lies outside the subtree by construction.
     */
    {
        uint32_t proj = CRAWL_COL_BIT(CRAWL_COL_PARENT_DIR_ID) | CRAWL_COL_BIT(CRAWL_COL_TYPE) |
                        CRAWL_COL_BIT(CRAWL_COL_SIZE) | CRAWL_COL_BIT(CRAWL_COL_NLINK) |
                        CRAWL_COL_BIT(CRAWL_COL_INODE) | CRAWL_COL_BIT(CRAWL_COL_DEV_MAJOR) |
                        CRAWL_COL_BIT(CRAWL_COL_DEV_MINOR);

        if (g_query.list_paths || g_query.subtree) proj |= CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES);
        if (g_query.have_gid) proj |= CRAWL_COL_BIT(CRAWL_COL_GID);
        if (g_query.perm_mode) proj |= CRAWL_COL_BIT(CRAWL_COL_MODE);
        (void)crawl_bin_block_reader_set_projection(br, proj);
    }
    if (g_query.block_skip) {
        (void)crawl_bin_block_reader_set_filter(br, g_query.have_size_gt, g_query.size_gt, g_query.type_filter);
        /* gid is RLE'd and near-constant within a uid shard, so its zone map is
         * usually a single value: --gid prunes whole row groups. --perm cannot,
         * because a bit test is not a range. */
        if (g_query.have_gid)
            (void)crawl_bin_block_reader_add_range(br, CRAWL_COL_GID, g_query.gid, g_query.gid);
    }

    for (;;) {
        bin_record_hdr_t rh;
        const unsigned char *rec_name = NULL;
        uint64_t pid;
        int got = crawl_bin_block_reader_next(br, &rh, &rec_name);
        int in_scope;

        if (got == 0) break;
        if (got < 0) return -1;
        nrec++;
        pid = rh.parent_dir_id;
        if (pid == 0ULL) return -1;

        in_scope = 1;
        if (g_query.subtree && !(sub && sub->whole)) {
            in_scope = subtree_contains(sub, pid);
            /*
             * The subtree's own directory record hangs off its parent, so the
             * membership array never claims it, yet du counts it. Namesake
             * check first: comparing one component is far cheaper than
             * rebuilding a path for every directory in the capture.
             */
            if (!in_scope && rh.type == (uint8_t)'d' && (size_t)rh.name_len == g_query.sub_base_len &&
                memcmp(rec_name, g_query.sub_base, g_query.sub_base_len) == 0) {
                const crawl_bin_catalog_t *cat = query_cat_lazy_get(lz);

                if (!cat) return -1;
                if (crawl_bin_catalog_entry_path(cat, pid, (const char *)rec_name, rh.name_len, fullpath_buf,
                                                 fullpath_sz) == 0 &&
                    strcmp(fullpath_buf, g_query.subtree) == 0)
                    in_scope = 1;
            }
            if (!in_scope) continue;
        }

        if (g_query.have_size_gt && rh.size <= g_query.size_gt) continue;
        if (g_query.type_filter && rh.type != (uint8_t)g_query.type_filter) continue;
        if (g_query.have_gid && rh.gid != g_query.gid) continue;
        if (g_query.perm_mode && !query_perm_match(rh.mode)) continue;

        qr->entries++;
        if (rh.type == (uint8_t)'f')
            qr->files++;
        else if (rh.type == (uint8_t)'d')
            qr->dirs++;
        else if (rh.type == (uint8_t)'l')
            qr->symlinks++;
        else
            qr->other++;

        /* du credits a multiply-linked inode once; which visit wins is decided
         * globally, after the workers join, because links can span shards. */
        if (rh.type != (uint8_t)'d' && rh.nlink > 1ULL) {
            if (query_hardlink_defer(qr, &rh) != 0) {
                qr->oom = 1;
                return -1;
            }
        } else {
            qr->bytes += rh.size;
        }

        if (g_query.list_paths) {
            size_t fullpath_len = 0;

            if (cached_parent_id != pid) {
                const crawl_bin_catalog_t *cat = query_cat_lazy_get(lz);

                if (!cat) return -1;
                if (crawl_bin_catalog_dir_path_len(cat, pid, parent_path, sizeof(parent_path),
                                                   &cached_parent_len) != 0)
                    return -1;
                cached_parent_id = pid;
            }
            if (cached_parent_len == 0U) {
                if ((size_t)rh.name_len + 2U > fullpath_sz) return -1;
                fullpath_buf[0] = '/';
                if (rh.name_len > 0U) memcpy(fullpath_buf + 1, rec_name, rh.name_len);
                fullpath_len = 1U + (size_t)rh.name_len;
            } else {
                if (cached_parent_len + 1U + (size_t)rh.name_len + 1U > fullpath_sz) return -1;
                memcpy(fullpath_buf, parent_path, cached_parent_len);
                fullpath_buf[cached_parent_len] = '/';
                if (rh.name_len > 0U) memcpy(fullpath_buf + cached_parent_len + 1U, rec_name, rh.name_len);
                fullpath_len = cached_parent_len + 1U + (size_t)rh.name_len;
            }
            fullpath_buf[fullpath_len] = '\0';
            if (query_out_append(qr, fullpath_buf, fullpath_len) != 0) {
                qr->oom = 1;
                return -1;
            }
        }
    }

    nrec += br->records_skipped;
    qr->blocks_decompressed += br->blocks_decompressed;
    qr->blocks_skipped += br->blocks_skipped;
    qr->records_skipped += br->records_skipped;
    if (nrec_out) *nrec_out = nrec;
    return 0;
}

/*
 * Return shard fi's catalog, loading it once on first use (read-only thereafter,
 * shared by every chunk of the shard). Double-checked under shard_cat_lock so the
 * nine catalog arrays and per-directory name strdups are built a single time per
 * shard instead of once per ckpt chunk. Returns NULL if the catalog is missing or
 * fails to load.
 */
static const crawl_bin_catalog_t *analyze_get_shard_catalog(analyze_pool_t *p, size_t fi, const char *full_path) {
    const crawl_bin_catalog_t *res = NULL;

    pthread_mutex_lock(&p->shard_cat_lock);
    if (p->shard_cat_state[fi] == 0) {
        FILE *fp = fopen(full_path, "rb");
        unsigned char st = 2; /* failed unless proven otherwise */

        if (fp) {
            bin_file_header_t fh;
            uint64_t fsz = p->shard_file_sizes[fi];

            if (fread(&fh, sizeof(fh), 1, fp) == 1 && crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION) &&
                fh.catalog_offset != 0ULL && fh.catalog_offset <= fsz) {
                /* Tree fields plus the DFS permutation subtree_build needs; the
                 * imm_child_* rollups are only read by the fast path, which has
                 * its own load below. */
                if (crawl_bin_catalog_load_sel(fp, fh.catalog_offset, fsz, CRAWL_CAT_SUBTREE, &p->shard_cat[fi]) == 0)
                    st = 1;
                /* The loader already frees + re-inits the struct on failure. */
            }
            /* Subtree membership belongs to the catalog: same lifetime, built once. */
            if (st == 1 && p->shard_sub && subtree_build(&p->shard_cat[fi], &p->shard_sub[fi]) != 0) {
                crawl_bin_catalog_free(&p->shard_cat[fi]);
                st = 2;
            }
            fclose(fp);
        }
        p->shard_cat_state[fi] = st;
    }
    if (p->shard_cat_state[fi] == 1) res = &p->shard_cat[fi];
    pthread_mutex_unlock(&p->shard_cat_lock);
    return res;
}

/*
 * Mark one chunk of shard fi done; when the shard's last chunk completes, drop its
 * catalog so peak memory tracks the shards in flight rather than all shards at once.
 * Safe to free here: the count only reaches 0 after the final chunk has finished
 * using the catalog, so no reader is active.
 */
static void analyze_release_shard_chunk(analyze_pool_t *p, size_t fi) {
    if (atomic_fetch_sub_explicit(&p->shard_chunks_left[fi], 1ULL, memory_order_acq_rel) == 1ULL) {
        pthread_mutex_lock(&p->shard_cat_lock);
        if (p->shard_cat_state[fi] == 1) {
            crawl_bin_catalog_free(&p->shard_cat[fi]);
            if (p->shard_sub) subtree_free(&p->shard_sub[fi]);
            p->shard_cat_state[fi] = 3;
        }
        pthread_mutex_unlock(&p->shard_cat_lock);
    }
}

/* Free any catalogs still loaded (e.g. on the early-exit path) and the per-shard arrays. */
static void analyze_free_shard_catalogs(analyze_pool_t *p, size_t name_count) {
    size_t fi;

    if (p->shard_cat && p->shard_cat_state) {
        for (fi = 0; fi < name_count; fi++)
            if (p->shard_cat_state[fi] == 1) {
                crawl_bin_catalog_free(&p->shard_cat[fi]);
                if (p->shard_sub) subtree_free(&p->shard_sub[fi]);
            }
    }
    free(p->shard_sub);
    p->shard_sub = NULL;
    free(p->shard_cat);
    free(p->shard_cat_state);
    free((void *)p->shard_chunks_left);
    p->shard_cat = NULL;
    p->shard_cat_state = NULL;
    p->shard_chunks_left = NULL;
}

static int analyze_process_chunk(const char *full_path, uint64_t start_off, uint64_t end_off, uint64_t file_sz,
                                 const crawl_bin_catalog_t *cat, unsigned char *pathbuf, char *parentbuf,
                                 char *fullpath_buf, size_t fullpath_sz, parent_map_t *map, uint64_t *depth_hist,
                                 char *iobuf, size_t iobuf_sz, uint64_t *nrec_out) {
    FILE *fp;
    int rc;

    if (!cat) return -1;
    if (start_off > file_sz || end_off > file_sz || start_off >= end_off) return -1;
    fp = fopen(full_path, "rb");
    if (!fp) {
        perror(full_path);
        return -1;
    }
    /* Large fully-buffered stdio buffer cuts read() syscalls on big shards / NFS. Must precede any I/O. */
    if (iobuf && iobuf_sz > 0U) setvbuf(fp, iobuf, _IOFBF, iobuf_sz);
    /* Catalog is preloaded and shared; the per-chunk FILE* only walks records. */
    if (fseeko(fp, (off_t)start_off, SEEK_SET) != 0) {
        fclose(fp);
        return -1;
    }
    {
        analyze_dir_cache_ent_t *dir_cache = NULL;
        uint64_t dir_cache_max = cat->max_dir_id;

        /* Memo for repeated parent_dir_id within this chunk; skip when the dir-id space is too large to be worth it. */
        if (dir_cache_max > 0ULL && dir_cache_max < (uint64_t)ANALYZE_DIR_CACHE_MAX_ENTRIES)
            dir_cache = (analyze_dir_cache_ent_t *)calloc((size_t)dir_cache_max + 1U, sizeof(*dir_cache));

        rc = analyze_scan_fp_until(fp, start_off, end_off, file_sz, cat, pathbuf, parentbuf, fullpath_buf,
                                   fullpath_sz, map, depth_hist, dir_cache, dir_cache_max, nrec_out);
        free(dir_cache);
    }
    fclose(fp);
    return rc;
}

static int query_process_chunk(const char *full_path, uint64_t start_off, uint64_t end_off, uint64_t file_sz,
                               query_cat_lazy_t *lz, const shard_subtree_t *sub,
                               crawl_bin_block_reader_t *br,
                               char *fullpath_buf, size_t fullpath_sz, char *iobuf, size_t iobuf_sz,
                               query_result_t *qr, uint64_t *nrec_out) {
    FILE *fp;
    int rc;

    if (start_off > file_sz || end_off > file_sz || start_off >= end_off) return -1;
    fp = fopen(full_path, "rb");
    if (!fp) {
        perror(full_path);
        return -1;
    }
    if (iobuf && iobuf_sz > 0U) setvbuf(fp, iobuf, _IOFBF, iobuf_sz);
    if (fseeko(fp, (off_t)start_off, SEEK_SET) != 0) {
        fclose(fp);
        return -1;
    }
    rc = query_scan_fp_until(fp, start_off, end_off, lz, sub, br, fullpath_buf, fullpath_sz, qr, nrec_out);
    fclose(fp);
    return rc;
}

static void *query_worker_main(void *arg) {
    analyze_pool_t *p = (analyze_pool_t *)arg;
    unsigned slot = atomic_fetch_add_explicit(&p->slot_assign, 1U, memory_order_relaxed);
    query_result_t *qr = &p->qres[slot];
    char *fullpath_buf = (char *)malloc(PATH_MAX);
    size_t iobuf_sz = (size_t)1 << 20;
    char *iobuf = (char *)malloc(iobuf_sz);
    crawl_bin_block_reader_t br;

    memset(&br, 0, sizeof(br));

    if (!fullpath_buf || !iobuf) {
        free(fullpath_buf);
        free(iobuf);
        atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        return NULL;
    }
    if (g_query.list_paths) {
        qr->out = (char *)malloc(QUERY_OUT_FLUSH_BYTES * 2U);
        if (!qr->out) {
            free(fullpath_buf);
            free(iobuf);
            atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
            return NULL;
        }
        qr->out_cap = QUERY_OUT_FLUSH_BYTES * 2U;
    }

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->chunk_cursor, 1, memory_order_relaxed);
        crawl_bin_file_chunk_t *c;
        uint64_t nrec = 0;
        int ar;

        if (i >= p->chunk_count) break;
        c = &p->chunks[i];
        {
            const shard_subtree_t *sub = p->shard_sub ? &p->shard_sub[c->file_index] : NULL;
            query_cat_lazy_t lz;

            query_cat_lazy_init(&lz, p, c->file_index, c->path);
            /* --subtree filters records against membership state that is built from the
             * catalog, so that mode has to resolve it before scanning. Every other query
             * defers until a matched record needs a path. */
            if (g_query.subtree && !query_cat_lazy_get(&lz)) {
                ar = -1;
            } else {
                ar = query_process_chunk(c->path, c->start_offset, c->end_offset,
                                         p->shard_file_sizes[c->file_index], &lz, sub, &br,
                                         fullpath_buf, PATH_MAX, iobuf, iobuf_sz, qr, &nrec);
            }
            analyze_release_shard_chunk(p, c->file_index);
        }

        atomic_fetch_add_explicit(&p->analyze_bytes_done, c->end_offset - c->start_offset, memory_order_relaxed);
        atomic_fetch_add_explicit(&p->analyze_chunks_done, 1ULL, memory_order_relaxed);
        if (ar == 0) {
            atomic_fetch_add_explicit(&p->analyze_records_done, nrec, memory_order_relaxed);
        } else {
            fprintf(stderr, "%s [%" PRIu64 ",%" PRIu64 "): ecrawl_analyze query scan failed\n", c->path,
                    c->start_offset, c->end_offset);
            atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        }
    }

    query_out_flush(qr);
    crawl_bin_block_reader_free(&br);
    free(iobuf);
    free(fullpath_buf);
    return NULL;
}

/*
 * Fold the deferred hardlinks into one byte total: an inode seen on several
 * paths is credited once, which is what du -sb reports and therefore what the
 * answer has to match. Open addressing on (dev, inode); the table holds only
 * multiply-linked entries, which are a small minority of any real capture.
 */
static uint64_t query_hardlink_bytes(query_result_t *res, unsigned n, uint64_t *dupes_out) {
    size_t total = 0;
    size_t cap = 1;
    size_t mask;
    query_hardlink_t **tab;
    uint64_t bytes = 0;
    uint64_t dupes = 0;
    unsigned i;

    if (dupes_out) *dupes_out = 0;
    for (i = 0; i < n; i++) total += res[i].hl_count;
    if (total == 0) return 0;
    while (cap < total * 2U) cap <<= 1;
    mask = cap - 1U;
    tab = (query_hardlink_t **)calloc(cap, sizeof(*tab));
    if (!tab) {
        /* Without the table we cannot dedup; counting every link once each is
         * the lesser error and the summary says the total is unreliable. */
        for (i = 0; i < n; i++) {
            size_t k;
            for (k = 0; k < res[i].hl_count; k++) bytes += res[i].hl[k].size;
            res[i].oom = 1;
        }
        return bytes;
    }

    for (i = 0; i < n; i++) {
        size_t k;

        for (k = 0; k < res[i].hl_count; k++) {
            query_hardlink_t *e = &res[i].hl[k];
            uint64_t h = e->inode * 1099511628211ULL;
            size_t slot;

            h ^= ((uint64_t)e->dev_major << 32) | (uint64_t)e->dev_minor;
            h *= 1099511628211ULL;
            slot = (size_t)(h >> 24) & mask;
            for (;;) {
                query_hardlink_t *cur = tab[slot];

                if (!cur) {
                    tab[slot] = e;
                    bytes += e->size;
                    break;
                }
                if (cur->inode == e->inode && cur->dev_major == e->dev_major && cur->dev_minor == e->dev_minor) {
                    dupes++;
                    break;
                }
                slot = (slot + 1U) & mask;
            }
        }
    }
    free(tab);
    if (dupes_out) *dupes_out = dupes;
    return bytes;
}

/*
 * Answer a plain --subtree aggregate from the catalog rollups alone, touching no
 * records at all. This is the whole point of the DFS post-pass: the cost becomes
 * O(directories) instead of O(files), so it holds at a billion files where a
 * full pass does not.
 *
 * It is only attempted when the answer is provably identical to the scan:
 *
 *   - no --size-gt / --type / --gid / --perm, because the rollups aggregate every record; and no
 *     --list, because printing paths needs the records regardless.
 *   - subtree_nlink_gt1_count == 0 on the matched root in every shard. Crawl-time
 *     hardlink credit lands with the first link visited anywhere in the tree,
 *     while the scan dedups within the queried subtree, so with a multiply-linked
 *     file in scope the two legitimately differ. Zero means there is none.
 *   - the subtree root was found in exactly the shards that have it, with the
 *     post-pass fields present.
 *
 * Returns 1 when it answered (filling *out), 0 when the caller must scan.
 */
typedef struct {
    uint64_t entries, files, dirs, symlinks, other, bytes, dirs_scanned;
} query_rollup_t;

static int query_rollup_eligible(void) {
    return g_query.active && g_query.subtree && !g_query.subtree_is_root && !g_query.exact &&
           !g_query.have_size_gt && !g_query.type_filter && !g_query.have_gid && !g_query.perm_mode &&
           !g_query.list_paths;
}

static int query_try_rollup(const char *dir_path, char **names, size_t name_count, query_rollup_t *out) {
    size_t fi;
    int found_any = 0;

    memset(out, 0, sizeof(*out));

    for (fi = 0; fi < name_count; fi++) {
        char full[PATH_MAX];
        FILE *fp;
        bin_file_header_t fh;
        struct stat stt;
        crawl_bin_catalog_t cat;
        shard_subtree_t sub;
        uint64_t root;
        int ok = 0;

        if (snprintf(full, sizeof(full), "%s/%s", dir_path, names[fi]) >= (int)sizeof(full)) return 0;
        if (stat(full, &stt) != 0) return 0;
        fp = fopen(full, "rb");
        if (!fp) return 0;

        crawl_bin_catalog_init_empty(&cat);
        if (fread(&fh, sizeof(fh), 1, fp) == 1 &&
            crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION) && fh.catalog_offset != 0ULL &&
            fh.catalog_offset <= (uint64_t)stt.st_size &&
            crawl_bin_catalog_load_sel(fp, fh.catalog_offset, (uint64_t)stt.st_size, CRAWL_CAT_SUBTREE, &cat) ==
                0)
            ok = 1;
        fclose(fp);
        if (!ok) {
            crawl_bin_catalog_free(&cat);
            return 0;
        }

        if (subtree_build(&cat, &sub) != 0) {
            crawl_bin_catalog_free(&cat);
            return 0;
        }
        root = sub.root_id;
        out->dirs_scanned += cat.max_dir_id;
        if (root != 0ULL) {
            if (cat.subtree_nlink_gt1_count[root] != 0ULL) {
                /* A multiply-linked file in scope makes the rollup and the scan
                 * legitimately disagree; do not present it as the answer. */
                crawl_bin_catalog_free(&cat);
                return 0;
            }
            found_any = 1;
            out->bytes += cat.subtree_bytes[root];
            out->entries += cat.subtree_count[root];
            out->files += cat.subtree_files[root];
            out->dirs += cat.subtree_dirs[root];
            out->symlinks += cat.subtree_symlinks[root];
            /* The root directory's own record lives in exactly one shard -- the
             * one owning it -- while every shard with a descendant carries a
             * catalog row for the path. Count it where the record actually is. */
            if (cat.self_present[root]) {
                out->bytes += cat.self_bytes[root];
                out->entries++;
                out->dirs++;
            }
        }
        crawl_bin_catalog_free(&cat);
    }

    if (!found_any) return 0;
    out->other = out->entries - out->files - out->dirs - out->symlinks;
    return 1;
}

/* Same key=value shape as query_report, so a caller cannot tell which path
 * produced the answer except by the diagnostics. */
static void query_report_rollup(const query_rollup_t *r, double elapsed) {
    printf("subtree=%s\n", g_query.subtree);
    printf("entries=%" PRIu64 "\n", r->entries);
    printf("files=%" PRIu64 "\n", r->files);
    printf("dirs=%" PRIu64 "\n", r->dirs);
    printf("symlinks=%" PRIu64 "\n", r->symlinks);
    printf("other=%" PRIu64 "\n", r->other);
    printf("bytes=%" PRIu64 "\n", r->bytes);
    printf("hardlink_dupes=0\n");
    printf("records_scanned=0\n");
    printf("blocks_decompressed=0\n");
    printf("blocks_skipped=0\n");
    printf("records_skipped_by_block_filter=0\n");
    printf("answered_from=catalog_rollup\n");
    printf("directories_examined=%" PRIu64 "\n", r->dirs_scanned);
    printf("elapsed_sec=%.3f\n", elapsed);
    fflush(stdout);
}

static void query_report(query_result_t *res, unsigned n, uint64_t records_scanned, double elapsed) {
    FILE *out = g_query.list_paths ? stderr : stdout;
    query_result_t sum;
    uint64_t dupes = 0;
    unsigned i;

    memset(&sum, 0, sizeof(sum));
    for (i = 0; i < n; i++) {
        sum.entries += res[i].entries;
        sum.files += res[i].files;
        sum.dirs += res[i].dirs;
        sum.symlinks += res[i].symlinks;
        sum.other += res[i].other;
        sum.bytes += res[i].bytes;
        sum.blocks_decompressed += res[i].blocks_decompressed;
        sum.blocks_skipped += res[i].blocks_skipped;
        sum.records_skipped += res[i].records_skipped;
    }
    sum.bytes += query_hardlink_bytes(res, n, &dupes);
    for (i = 0; i < n; i++) sum.oom |= res[i].oom;

    fprintf(out, "subtree=%s\n", g_query.subtree ? g_query.subtree : "(whole capture)");
    if (g_query.have_size_gt) fprintf(out, "size_gt=%" PRIu64 "\n", g_query.size_gt);
    if (g_query.have_gid) fprintf(out, "gid=%" PRIu32 "\n", g_query.gid);
    if (g_query.perm_mode)
        fprintf(out, "perm=%s%04o\n",
                g_query.perm_mode == PERM_ALL ? "-" : (g_query.perm_mode == PERM_ANY ? "/" : ""),
                (unsigned)g_query.perm);
    if (g_query.type_filter) fprintf(out, "type=%c\n", (char)g_query.type_filter);
    fprintf(out, "entries=%" PRIu64 "\n", sum.entries);
    fprintf(out, "files=%" PRIu64 "\n", sum.files);
    fprintf(out, "dirs=%" PRIu64 "\n", sum.dirs);
    fprintf(out, "symlinks=%" PRIu64 "\n", sum.symlinks);
    fprintf(out, "other=%" PRIu64 "\n", sum.other);
    fprintf(out, "bytes=%" PRIu64 "\n", sum.bytes);
    fprintf(out, "hardlink_dupes=%" PRIu64 "\n", dupes);
    fprintf(out, "records_scanned=%" PRIu64 "\n", records_scanned);
    fprintf(out, "blocks_decompressed=%" PRIu64 "\n", sum.blocks_decompressed);
    fprintf(out, "blocks_skipped=%" PRIu64 "\n", sum.blocks_skipped);
    fprintf(out, "records_skipped_by_block_filter=%" PRIu64 "\n", sum.records_skipped);
    fprintf(out, "answered_from=record_scan\n");
    fprintf(out, "elapsed_sec=%.3f\n", elapsed);
    if (sum.oom) fprintf(out, "warning=allocation_failed_totals_may_be_wrong\n");
    fflush(out);
}

static void query_results_free(query_result_t *res, unsigned n) {
    unsigned i;

    if (!res) return;
    for (i = 0; i < n; i++) {
        free(res[i].hl);
        free(res[i].out);
    }
    free(res);
}

static void *analyze_worker_main(void *arg) {
    analyze_pool_t *p = (analyze_pool_t *)arg;
    unsigned slot = atomic_fetch_add_explicit(&p->slot_assign, 1U, memory_order_relaxed);
    parent_map_t *map = p->map;
    uint64_t *dh = (uint64_t *)calloc((size_t)ANALYZE_DEPTH_BINS, sizeof(uint64_t));
    unsigned char *pathbuf = (unsigned char *)malloc(65536);
    char *parentbuf = (char *)malloc(65536);
    char *fullpath_buf = (char *)malloc(PATH_MAX);
    size_t iobuf_sz = (size_t)1 << 20;
    char *iobuf = (char *)malloc(iobuf_sz);

    if (!map || !dh || !pathbuf || !parentbuf || !fullpath_buf || !iobuf) {
        free(iobuf);
        free(fullpath_buf);
        free(pathbuf);
        free(parentbuf);
        free(dh);
        atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        return NULL;
    }
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

        {
            const crawl_bin_catalog_t *cat = analyze_get_shard_catalog(p, c->file_index, c->path);

            ar = cat ? analyze_process_chunk(c->path, c->start_offset, c->end_offset, fsz, cat, pathbuf, parentbuf,
                                             fullpath_buf, PATH_MAX, map, dh, iobuf, iobuf_sz, &nrec)
                     : -1;
            analyze_release_shard_chunk(p, c->file_index);
        }

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

    free(iobuf);
    free(pathbuf);
    free(parentbuf);
    free(fullpath_buf);
    return NULL;
}

typedef struct {
    parent_node_t *node;
    uint64_t key; /* ranking key: nfile for the dense list, slash-count depth for the deep list */
} analyze_sort_wrap_t;

static uint64_t analyze_count_slashes(const char *s) {
    uint64_t c = 0;

    for (; *s; s++)
        if (*s == '/') c++;
    return c;
}

/* Final ordering for a top list: key descending, ties broken by path ascending. */
static int cmp_analyze_key_desc(const void *a, const void *b) {
    const analyze_sort_wrap_t *wa = (const analyze_sort_wrap_t *)a;
    const analyze_sort_wrap_t *wb = (const analyze_sort_wrap_t *)b;

    if (wa->key < wb->key) return 1;
    if (wa->key > wb->key) return -1;
    return strcmp(wa->node->path, wb->node->path);
}

/*
 * "Worse" = ranked later in the desired top order (key desc, then path asc), i.e. the element that
 * should be evicted first. Used to keep a bounded min-heap of the top-N parents so the report does not
 * sort every distinct parent (O(M log N) time, O(N) memory instead of a full O(M log M) sort).
 */
static int analyze_topn_worse(const analyze_sort_wrap_t *a, const analyze_sort_wrap_t *b) {
    if (a->key != b->key) return a->key < b->key;
    return strcmp(a->node->path, b->node->path) > 0;
}

static void analyze_topn_sift_down(analyze_sort_wrap_t *h, size_t n, size_t i) {
    for (;;) {
        size_t l = 2U * i + 1U;
        size_t r = 2U * i + 2U;
        size_t s = i;
        analyze_sort_wrap_t tmp;

        if (l < n && analyze_topn_worse(&h[l], &h[s])) s = l;
        if (r < n && analyze_topn_worse(&h[r], &h[s])) s = r;
        if (s == i) break;
        tmp = h[i];
        h[i] = h[s];
        h[s] = tmp;
        i = s;
    }
}

static void analyze_topn_sift_up(analyze_sort_wrap_t *h, size_t i) {
    while (i > 0U) {
        size_t p = (i - 1U) / 2U;
        analyze_sort_wrap_t tmp;

        if (!analyze_topn_worse(&h[i], &h[p])) break;
        tmp = h[i];
        h[i] = h[p];
        h[p] = tmp;
        i = p;
    }
}

/* Offer one (node, key) to the bounded min-heap (root = current worst kept). */
static void analyze_topn_offer(analyze_sort_wrap_t *heap, size_t *heap_n, size_t cap, parent_node_t *node,
                               uint64_t key) {
    analyze_sort_wrap_t cand;

    if (cap == 0U) return;
    cand.node = node;
    cand.key = key;
    if (*heap_n < cap) {
        heap[*heap_n] = cand;
        analyze_topn_sift_up(heap, *heap_n);
        (*heap_n)++;
    } else if (analyze_topn_worse(&heap[0], &cand)) {
        heap[0] = cand;
        analyze_topn_sift_down(heap, *heap_n, 0U);
    }
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
    analyze_sort_wrap_t *dense_heap = NULL;
    analyze_sort_wrap_t *deep_heap = NULL;
    size_t dense_cap = g_top_dense ? (size_t)g_top_n : 0U;
    size_t deep_cap = g_top_deep ? (size_t)g_top_n : 0U;
    size_t dense_n = 0;
    size_t deep_n = 0;
    unsigned di;
    uint64_t depth_records = 0;
    unsigned max_depth_bin = 0;

    if (dense_cap > 0U) {
        dense_heap = (analyze_sort_wrap_t *)malloc(dense_cap * sizeof(*dense_heap));
        if (!dense_heap) {
            /* Stats below are still emitted; only the top-N listing is dropped on OOM. */
            fprintf(stderr, "ecrawl_analyze: alloc failed for dense top-N heap; skipping list\n");
            dense_cap = 0;
        }
    }
    if (deep_cap > 0U) {
        deep_heap = (analyze_sort_wrap_t *)malloc(deep_cap * sizeof(*deep_heap));
        if (!deep_heap) {
            fprintf(stderr, "ecrawl_analyze: alloc failed for deep top-N heap; skipping list\n");
            deep_cap = 0;
        }
    }

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

            if (dense_cap > 0U) analyze_topn_offer(dense_heap, &dense_n, dense_cap, n, n->nfile);
            if (deep_cap > 0U) analyze_topn_offer(deep_heap, &deep_n, deep_cap, n, analyze_count_slashes(n->path));

            n = n->next;
        }
    }

    if (dense_n > 0U) qsort(dense_heap, dense_n, sizeof(dense_heap[0]), cmp_analyze_key_desc);
    if (deep_n > 0U) qsort(deep_heap, deep_n, sizeof(deep_heap[0]), cmp_analyze_key_desc);

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

    if (g_top_dense) {
        size_t k;

        printf("top_parents_by_regular_file_count (N=%u)\n", g_top_n);
        printf("# nfile ndir nsym nother path\n");
        for (k = 0; k < dense_n; k++) {
            parent_node_t *no = dense_heap[k].node;
            printf("%zu %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %s\n", k + 1U, no->nfile, no->ndir, no->nsym,
                   no->nother, no->path);
        }
    }

    if (g_top_deep) {
        size_t k;

        if (g_top_dense) printf("\n");
        printf("top_parents_by_depth (N=%u)\n", g_top_n);
        printf("# depth nfile ndir nsym nother path\n");
        for (k = 0; k < deep_n; k++) {
            parent_node_t *no = deep_heap[k].node;
            printf("%zu %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %s\n", k + 1U, deep_heap[k].key,
                   no->nfile, no->ndir, no->nsym, no->nother, no->path);
        }
    }

    free(dense_heap);
    free(deep_heap);
}

/*
 * Bytes per parse job. Checkpoint segments are 32 MiB apart, so a capture smaller
 * than that yields one job and runs single-threaded no matter what the thread
 * budget says — a 14 MiB shape report measured 94% CPU with 32 threads
 * configured. 4 MiB keeps the header pass short while giving a typical capture
 * enough jobs to fill the pool; ECRAWL_ANALYZE_CHUNK_BYTES overrides it (tests
 * use a tiny value to force many jobs from a small shard).
 *
 * Splitting is safe for both modes: a job is any block-aligned byte range, and
 * both the shape histograms and the query counters are accumulated per thread
 * and merged at the end, so job boundaries cannot change the answer.
 */
static uint64_t parse_split_target_bytes(void) {
    const char *e;

    e = getenv("ECRAWL_ANALYZE_CHUNK_BYTES");
    if (e && e[0]) {
        char *end;
        unsigned long long v;

        errno = 0;
        v = strtoull(e, &end, 10);
        if (!errno && end != e && *end == '\0' && v >= 4096ULL) return (uint64_t)v;
    }
    return 4ULL << 20;
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
    const char *skip_env = getenv("ECRAWL_ANALYZE_BLOCK_SKIP");

    /* Off only for parity testing: skipping must never change query results. */
    g_query.block_skip = !(skip_env && strcmp(skip_env, "0") == 0);

    /*
     * A plain subtree aggregate is already summed in the catalogs, so answer it
     * there and never open the record region: O(directories) instead of
     * O(files). Falls through to the scan whenever the rollup cannot be proven
     * equal to it (see query_try_rollup).
     */
    if (query_rollup_eligible()) {
        query_rollup_t roll;
        double t0 = analyze_now_sec();

        if (query_try_rollup(dir_path, names, name_count, &roll)) {
            query_report_rollup(&roll, analyze_now_sec() - t0);
            return 0;
        }
    }

    if (analyze_build_all_chunks(dir_path, names, name_count, &shard_sizes, &chunks, &chunk_count, &chunk_byte_sum,
                                 parse_split_target_bytes()) != 0) {
        fprintf(stderr, "ecrawl_analyze: failed to build chunk job list\n");
        return 1;
    }
    if (chunk_count == 0U) {
        fprintf(stderr, "ecrawl_analyze: no parse chunks produced\n");
        free(shard_sizes);
        return 1;
    }

    /*
     * The unit of parallel work is a chunk (ckpt segment), not a shard. A single
     * huge single-UID shard is split into many chunks, so cap workers by chunk
     * count rather than shard count — otherwise a one-shard crawl ran on a single
     * core no matter how many chunks (and cores) were available.
     */
    if ((uint64_t)nthreads > (uint64_t)chunk_count) nthreads = (unsigned)chunk_count;
    if (nthreads < 1U) nthreads = 1U;

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
    pthread_mutex_init(&pool.shard_cat_lock, NULL);

    /* Shared per-shard catalogs (loaded once on demand) + remaining-chunk counts. */
    if (g_query.active && g_query.subtree && !g_query.subtree_is_root) {
        pool.shard_sub = (shard_subtree_t *)calloc(name_count, sizeof(*pool.shard_sub));
        if (!pool.shard_sub) {
            perror("ecrawl_analyze: alloc");
            pthread_mutex_destroy(&pool.shard_cat_lock);
            crawl_bin_free_chunk_array_rows(chunks, chunk_count);
            free(shard_sizes);
            return 1;
        }
    }
    pool.shard_cat = (crawl_bin_catalog_t *)calloc(name_count, sizeof(*pool.shard_cat));
    pool.shard_cat_state = (unsigned char *)calloc(name_count, sizeof(*pool.shard_cat_state));
    pool.shard_chunks_left = (_Atomic uint64_t *)calloc(name_count, sizeof(*pool.shard_chunks_left));
    if (!pool.shard_cat || !pool.shard_cat_state || !pool.shard_chunks_left) {
        perror("ecrawl_analyze: alloc");
        free(pool.shard_sub);
        free(pool.shard_cat);
        free(pool.shard_cat_state);
        free(pool.shard_chunks_left);
        pthread_mutex_destroy(&pool.shard_cat_lock);
        crawl_bin_free_chunk_array_rows(chunks, chunk_count);
        free(shard_sizes);
        return 1;
    }
    {
        size_t ci;
        for (ci = 0; ci < name_count; ci++) atomic_init(&pool.shard_chunks_left[ci], 0ULL);
        for (ci = 0; ci < chunk_count; ci++) {
            size_t fi = chunks[ci].file_index;
            if (fi < name_count)
                atomic_fetch_add_explicit(&pool.shard_chunks_left[fi], 1ULL, memory_order_relaxed);
        }
    }

    /* Query mode keeps its counters per thread in pool.qres and never touches the parent map. */
    if (!g_query.active) pool.map = parent_map_new(nthreads);
    pool.depth_hist = (uint64_t **)calloc((size_t)nthreads, sizeof(*pool.depth_hist));
    threads = (pthread_t *)calloc((size_t)nthreads, sizeof(*threads));
    if (g_query.active) {
        pool.qres = (query_result_t *)calloc((size_t)nthreads, sizeof(*pool.qres));
        if (!pool.qres) {
            perror("ecrawl_analyze: alloc");
            free(pool.depth_hist);
            free(threads);
            analyze_free_shard_catalogs(&pool, name_count);
            pthread_mutex_destroy(&pool.shard_cat_lock);
            crawl_bin_free_chunk_array_rows(chunks, chunk_count);
            free(shard_sizes);
            return 1;
        }
    }
    if ((!g_query.active && !pool.map) || !pool.depth_hist || !threads) {
        perror("ecrawl_analyze: alloc");
        parent_map_free(pool.map);
        free(pool.depth_hist);
        free(threads);
        query_results_free(pool.qres, nthreads);
        analyze_free_shard_catalogs(&pool, name_count);
        pthread_mutex_destroy(&pool.shard_cat_lock);
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
        if (pthread_create(&threads[ti], NULL, g_query.active ? query_worker_main : analyze_worker_main, &pool) !=
            0) {
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
            parent_map_free(pool.map);
            pool.map = NULL;
            for (j = 0; j < nthreads; j++) {
                free(pool.depth_hist[j]);
                pool.depth_hist[j] = NULL;
            }
            free(pool.depth_hist);
            query_results_free(pool.qres, nthreads);
            pool.qres = NULL;
            free(threads);
            analyze_free_shard_catalogs(&pool, name_count);
            pthread_mutex_destroy(&pool.shard_cat_lock);
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

    if (g_query.active) {
        query_report(pool.qres, nthreads, atomic_load_explicit(&pool.analyze_records_done, memory_order_relaxed),
                     analyze_now_sec() - sctx.t0);
        query_results_free(pool.qres, nthreads);
        pool.qres = NULL;
        analyze_free_shard_catalogs(&pool, name_count);
        pthread_mutex_destroy(&pool.shard_cat_lock);
        crawl_bin_free_chunk_array_rows(pool.chunks, pool.chunk_count);
        free(pool.shard_file_sizes);
        free(pool.depth_hist);
        if (atomic_load_explicit(&pool.failures, memory_order_relaxed) > 0) rc = 1;
        return rc;
    }

    /* The workers filled it in place; nothing left to reduce but the 64-bin depth histograms. */
    merged = pool.map;
    pool.map = NULL;

    for (ti = 0; ti < nthreads; ti++) {
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

    analyze_free_shard_catalogs(&pool, name_count);
    pthread_mutex_destroy(&pool.shard_cat_lock);

    crawl_bin_free_chunk_array_rows(pool.chunks, pool.chunk_count);
    free(pool.shard_file_sizes);
    pool.chunks = NULL;
    pool.shard_file_sizes = NULL;

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

    tune_allocator();

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0)
            g_verbose = 1;
        else if (strncmp(argv[i], "--top", 5) == 0 && (argv[i][5] == '\0' || argv[i][5] == ',')) {
            char *end;
            unsigned long v;

            if (parse_top_dims(argv[i] + 5, argv[0]) != 0) {
                usage(argv[0]);
                return 2;
            }
            if (i + 1 >= argc) {
                fprintf(stderr, "%s: %s requires N\n", argv[0], argv[i]);
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
        } else if (strcmp(argv[i], "--subtree") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "%s: --subtree requires a directory\n", argv[0]);
                return 2;
            }
            if (query_set_subtree(argv[++i], argv[0]) != 0) return 2;
        } else if (strcmp(argv[i], "--size-gt") == 0) {
            char *end;
            unsigned long long v;

            if (i + 1 >= argc) {
                fprintf(stderr, "%s: --size-gt requires a byte count\n", argv[0]);
                return 2;
            }
            errno = 0;
            v = strtoull(argv[++i], &end, 10);
            if (errno || end == argv[i] || *end != '\0') {
                fprintf(stderr, "%s: --size-gt expects a byte count\n", argv[0]);
                return 2;
            }
            g_query.size_gt = (uint64_t)v;
            g_query.have_size_gt = 1;
            g_query.active = 1;
        } else if (strcmp(argv[i], "--type") == 0) {
            if (i + 1 >= argc || argv[i + 1][0] == '\0' || argv[i + 1][1] != '\0' ||
                !strchr("fdlcbpso", argv[i + 1][0])) {
                fprintf(stderr, "%s: --type expects one of f d l c b p s o\n", argv[0]);
                return 2;
            }
            g_query.type_filter = argv[++i][0];
            g_query.active = 1;
        } else if (strcmp(argv[i], "--gid") == 0) {
            char *end;
            unsigned long long v;

            if (i + 1 >= argc) {
                fprintf(stderr, "%s: --gid requires a numeric group id\n", argv[0]);
                return 2;
            }
            errno = 0;
            v = strtoull(argv[++i], &end, 10);
            if (errno || end == argv[i] || *end != '\0' || v > 0xFFFFFFFFULL) {
                fprintf(stderr, "%s: --gid expects a numeric group id\n", argv[0]);
                return 2;
            }
            g_query.gid = (uint32_t)v;
            g_query.have_gid = 1;
            g_query.active = 1;
        } else if (strcmp(argv[i], "--perm") == 0) {
            const char *s;
            char *end;
            unsigned long v;

            if (i + 1 >= argc) {
                fprintf(stderr, "%s: --perm requires a mode\n", argv[0]);
                return 2;
            }
            s = argv[++i];
            if (*s == '-') {
                g_query.perm_mode = PERM_ALL;
                s++;
            } else if (*s == '/') {
                g_query.perm_mode = PERM_ANY;
                s++;
            } else {
                g_query.perm_mode = PERM_EXACT;
            }
            errno = 0;
            v = strtoul(s, &end, 8);
            if (errno || end == s || *end != '\0' || v > 07777UL) {
                fprintf(stderr, "%s: --perm expects an octal mode, optionally prefixed by - or /\n", argv[0]);
                return 2;
            }
            g_query.perm = (uint32_t)v;
            g_query.active = 1;
        } else if (strcmp(argv[i], "--list") == 0) {
            g_query.list_paths = 1;
            g_query.active = 1;
        } else if (strcmp(argv[i], "--exact") == 0) {
            g_query.exact = 1;
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

    /* Worker count is clamped to the chunk count inside run_analyze (chunks, not
     * shards, are the unit of parallel work), so a single big shard still scales. */
    if (nthreads < 1) nthreads = 1;

    {
        int ar = run_analyze(dir_path, names, name_count, nthreads);

        for (i = 0; i < (int)name_count; i++) free(names[i]);
        free(names);
        return ar ? 1 : 0;
    }
}
