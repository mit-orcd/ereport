/*
 * ecrawl_query.c — Read-only directory-shape stats from uid_shard_*.bin crawl shards.
 *
 * SPDX-License-Identifier: MIT
 *
 * Build: gcc -O2 -Wall -Wextra -pthread -o ecrawl_query ecrawl_query.c crawl_bin_chunks.o
 *
 * Usage: run with --help (or no arguments) for the flag list.
 * Parallelism: ECRAWL_QUERY_THREADS (default 16).
 */

#define _FILE_OFFSET_BITS 64
#define _DEFAULT_SOURCE

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

#if defined(__APPLE__)
/* glibc-only unlocked stdio; macOS has no fread_unlocked, and its plain fread already
 * takes the FILE lock, so it is the functional equivalent. */
#define fread_unlocked fread
#endif

#include "crawl_bin_format.h"
#include "alloc_tuning.h"
#include "crawl_bin_block.h"
#include "crawl_ckpt.h"
#include "crawl_bin_chunks.h"
#include "crawl_bin_catalog.h"
#include "crawl_fpcache.h"
#include "crawl_sidecar.h"
#include "path_canon.h"

#if defined(__x86_64__)
#include <cpuid.h>
#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* A reconstructed directory path is capped by the catalog's component limit, not by PATH_MAX: 128
 * components of up to 255 bytes do not have to have existed on any one filesystem to be in a bin. */
#define ANALYZE_PARENT_PATH_MAX 65536U

#define DEFAULT_ANALYZE_THREADS 16U
#define ANALYZE_THREADS_MAX 4096U

static pthread_mutex_t g_verbose_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_verbose;
static unsigned g_top_n = 32U;

/*
 * Query mode (--subtree / --size-gt / --type / --gid / --uid / --perm / --list [--level N] [--sum]):
 * the same parallel scan, with a record predicate instead of the directory-shape histograms.
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
    const char *sub_parent; /* subtree's parent directory; "" when that is the capture root */
    int subtree_is_root;   /* subtree == "/": every record qualifies */
    uint64_t size_gt;      /* strictly greater, matching find -size +Nc */
    int have_size_gt;
    int type_filter;       /* 0 = any type, else 'f' / 'd' / 'l' / … */
    uint32_t gid;          /* --gid: exact group owner */
    int have_gid;
    uint32_t uid;          /* --uid: exact user owner */
    int have_uid;
    uint32_t perm;         /* --perm: permission bits, masked to 07777 */
    int perm_mode;         /* 0 = no filter, else PERM_EXACT / PERM_ALL / PERM_ANY */
    int list_paths;        /* print matching paths instead of counting them */
    unsigned list_level;   /* --level N with --list: 0 = emit every path; else relative depth */
    int sum;               /* --sum with --list: per-path files/dirs/symlinks/other/bytes columns */
    int block_skip;        /* use block header summaries to skip blocks (see ECRAWL_QUERY_BLOCK_SKIP) */
    int exact;             /* --exact: never answer from catalog rollups, always scan records */
} query_spec_t;

/* --perm forms, mirroring find(1): MODE is exact, -MODE is "all of these bits",
 * /MODE is "any of these bits". */
enum { PERM_NONE = 0, PERM_EXACT, PERM_ALL, PERM_ANY };

static query_spec_t g_query;

/*
 * --index-dir: where ereport_index --make left dirs.idx / rowgroups.idx.
 *
 * Purely an accelerator. Absent, unreadable, or bound to shards that no longer
 * match, it is dropped without a word and every query runs exactly as it did
 * before the sidecars existed — so a stale index dir can never be the reason an
 * answer is wrong, only the reason it is slow.
 */
static const char *g_index_dir;

/*
 * Whether the scan has to decode name bytes, the largest column in a row group.
 *
 * --list obviously needs them. --subtree needed them too, for one record in the whole
 * capture: the subtree's own directory record, which hangs off a parent outside the subtree
 * and so is recognised by its name. The catalog already carries that record (self_present /
 * self_bytes on the directory's own row), so the aggregate takes it from there and leaves
 * the name column compressed — but only when every filter can still be applied, which rules
 * out --gid, --uid and --perm, whose values that row does not carry.
 */
static int query_needs_names(void) {
    if (g_query.list_paths) return 1;
    if (g_query.subtree && !g_query.subtree_is_root &&
        (g_query.have_gid || g_query.have_uid || g_query.perm_mode))
        return 1;
    return 0;
}

/* --level and --sum both have to see every match before printing, so --list stops streaming. */
static int query_list_buffered(void) {
    return g_query.list_paths && (g_query.list_level != 0U || g_query.sum);
}

/* Listing is sparse when a record filter can omit an ancestor of a listed path.
 * Dense listings (no uid/gid/size/type/perm, and no non-root --subtree) have every
 * ancestor listed, so --list --level can root at the shallowest listed path (found
 * while slicing) and skip Pass 1. */
static int query_list_sparse(void) {
    return g_query.have_uid || g_query.have_gid || g_query.have_size_gt || g_query.type_filter ||
           g_query.perm_mode || (g_query.subtree && !g_query.subtree_is_root);
}

/* The subtree's own directory record, contributed by the one shard that holds it. */
static _Atomic uint64_t g_subtree_self_count;
static _Atomic uint64_t g_catalogs_loaded; /* shard catalogs materialized; 0 when the sidecar serves membership */
static _Atomic uint64_t g_subtree_self_bytes;

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

/*
 * Bucket count, chosen per run from the exact directory count the shard catalogs declare.
 *
 * A fixed 262144 was three parents deep per bucket on a flat tree (782 K parents), so the average
 * lookup chased three nodes at random arena addresses before it could answer. The table is sized to
 * about 1.5 buckets per parent instead, which is the difference between a chain walk and a single
 * probe. The ceiling caps the array at 512 MiB of pointers, small beside the nodes themselves.
 */
#define ANALYZE_HASH_BUCKETS_MIN 4096U
#define ANALYZE_HASH_BUCKETS_MAX (1U << 26)
#define ANALYZE_DEPTH_BINS 64U
/*
 * Child records under one directory, split by type.
 *
 * Atomic because more than one worker can charge the same directory: relaxed is enough, since
 * nothing is ordered against them and they are only read once every worker has joined. The same
 * four counters serve both places a directory is counted -- the per-shard dense array the scan
 * bumps, and the parent node the report reads -- so one flush routine feeds both.
 */
typedef struct {
    _Atomic uint64_t nfile;
    _Atomic uint64_t ndir;
    _Atomic uint64_t nsym;
    _Atomic uint64_t nother;
} dircnt_t;

typedef struct parent_node {
    /*
     * Published with a release store on the bucket head and never changed afterwards, so a lookup can
     * walk the chain with no lock: it sees either the old head or a fully constructed new one.
     */
    _Atomic(struct parent_node *) next;
    uint32_t hash; /* full hash of path: skips the compare for the other chain members */
    /* Lets a chain walk reject on length and then memcmp a known span, instead of strcmp hunting
     * for the NUL. Free: it fills the padding the 8-byte-aligned `path` left after `hash`. */
    uint32_t path_len;
    char *path;
    dircnt_t c;
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
/*
 * Nodes and their path bytes are bump-allocated per worker.
 *
 * A node is never freed on its own: it lives until parent_map_free, so the only thing malloc was
 * buying was a per-node header and a trip through the allocator. On a flat tree that trip dominated
 * -- 782 K parents across 32 workers meant ~1.5 M glibc allocations, and parent_map_get_or_add came
 * out at 57.9% of the run with malloc 47.1% of its children and 29.9% self time in osq_lock, which
 * is threads queueing on the arena locks inside glibc. Bumping a per-worker block removes both the
 * contention and the matching frees.
 */
/*
 * The first block is PARENT_ARENA_BLOCK_BYTES and each later one doubles up to
 * PARENT_ARENA_BLOCK_MAX_BYTES. Growing is not about the malloc count, which the bump arena
 * already made negligible; it is what lets a large arena reach a block big enough to contain a
 * 2 MiB-aligned span, which is the floor alloc_hint_hugepages() insists on before it will advise
 * anything. At a flat 1 MiB that hint can never fire, and first-touch faults were 48% of the
 * neutral_flat analyze -- memmove faulting in a page per 4 KiB as it copied path bytes into
 * freshly mapped block memory. Starting small is what keeps a capture with few parents at its
 * present footprint: a worker that fills a single block still asks for only 1 MiB.
 */
#define PARENT_ARENA_BLOCK_BYTES ((size_t)1 << 20)
#define PARENT_ARENA_BLOCK_MAX_BYTES ((size_t)8 << 20)

typedef struct parent_arena_block {
    struct parent_arena_block *next;
    size_t used;
    size_t cap;
    unsigned char data[];
} parent_arena_block_t;

typedef struct parent_arena {
    parent_arena_block_t *cur;
    size_t next_block_bytes;       /* 0 before the first block; thereafter the next size to request */
    struct parent_arena *reg_next; /* registered with the map, which owns the blocks */
} parent_arena_t;

typedef struct {
    _Atomic(parent_node_t *) *buckets;
    size_t bucket_mask;               /* buckets is a power of two; bucket = hash & bucket_mask */
    _Atomic(parent_arena_t *) arenas; /* every worker arena, so free() and the report can walk them */
} parent_map_t;

/*
 * One dircnt_t per directory of a shard, indexed by dir_id: where the scan puts its counts.
 *
 * Within one catalog dir_id -> path is a bijection (ecrawl hands ids out from
 * shard_cat_lookup_dir_id and next_dir_id++), so nothing about a directory's *identity* needs a
 * string, and the scan does not build one. It indexes this array with the record's parent_dir_id
 * and moves on. Paths appear once, afterwards, in the fold -- see analyze_fold_range.
 *
 * The array this replaces memoised dir_id -> parent node, which still meant resolving each
 * directory the first time: build the path, hash it, probe the bucket array, walk a chain, take a
 * CAS to publish. On a flat tree that came to 22% of the run in parent_map_get_or_add and
 * another 25% in pthread_mutex_lock/unlock, all of it inside the scan and all of it contended.
 * Counting into a dense array indexed by the id the record already carries has none of that, and
 * neighbouring records usually land on the same or a neighbouring line.
 *
 * 32 bytes per directory against the memo's 8, and it lives as long as the catalog does.
 */
typedef dircnt_t analyze_dir_counts_t;

typedef struct {
    uint64_t lo, hi;
} query_dfs_range_t;

/*
 * Where --subtree lands in one shard's catalog. Membership is the DFS range test
 * dfs_lo <= dfs_index[parent_dir_id] < dfs_hi, which is O(1) per record against
 * an array the catalog already carries. Earlier this was a per-shard byte array
 * painted by walking every directory's parent chain; the permutation makes both
 * the extra pass and the extra allocation unnecessary. Built once per shard,
 * beside the catalog it indexes.
 *
 * Duplicate catalog paths (several dir_ids reconstructing to the same string)
 * yield several disjoint DFS ranges; subtree_contains tests any of them.
 */
typedef struct {
    const uint64_t *dfs_index; /* borrowed from the catalog; NULL if unavailable */
    uint64_t max_dir_id;
    uint64_t root_id; /* first matching dir_id; 0 = none in this shard */
    uint64_t *root_ids; /* all matching dir_ids; NULL when nroots<=1 (root_id is the one) */
    size_t nroots;
    uint64_t dfs_lo;
    uint64_t dfs_hi;
    query_dfs_range_t *ranges; /* nroots ranges; NULL when nroots<=1 */
    uint64_t *parent_ids;
    size_t nparents;
    /*
     * Every parent_dir_id this shard can match lies in [pid_lo, pid_hi] — the in-subtree
     * directories widened by the subtree's parent, which owns the subtree's own record.
     * dir_ids are handed out in crawl order, so the hull is usually tight; when it is not it
     * is merely loose, never wrong. Fed to the reader as a zone-map range so row groups
     * outside it are skipped without decompressing.
     */
    uint64_t pid_lo;
    uint64_t pid_hi;
    int have_hull;
    int empty; /* nothing in this shard can match: its chunks are retired unread */
    int whole; /* subtree covers the entire catalog */
    int sidecar_mem; /* DFS ranges came from dirs.idx; membership does not need dfs_index[] */
} shard_subtree_t;

static inline uint64_t shard_sub_root_at(const shard_subtree_t *s, size_t i) {
    if (s->nroots <= 1U) return i == 0U ? s->root_id : 0ULL;
    return s->root_ids[i];
}

static inline int subtree_contains(const shard_subtree_t *s, uint64_t dir_id) {
    uint64_t p;
    size_t i;

    if (!s) return 0;
    if (s->whole) return 1;
    if (!s->dfs_index || s->nroots == 0U || dir_id == 0ULL || dir_id > s->max_dir_id) return 0;
    p = s->dfs_index[dir_id];
    if (s->nroots == 1U) return p >= s->dfs_lo && p < s->dfs_hi;
    for (i = 0; i < s->nroots; i++) {
        if (p >= s->ranges[i].lo && p < s->ranges[i].hi) return 1;
    }
    return 0;
}

/* One inode whose byte credit is deferred until the global hardlink merge. */
typedef struct {
    uint32_t dev_major;
    uint32_t dev_minor;
    uint64_t inode;
    uint64_t size;
} query_hardlink_t;

/* Per listed path, the record data --sum aggregates (one per line in out; --sum only). */
typedef struct {
    uint64_t size;
    uint32_t hl_idx; /* index into this worker's hl[], or UINT32_MAX */
    uint8_t type;
} query_listrec_t;

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
    /* Path bytes live in fixed-size segments so a buffered --level/--sum listing
     * never realloc-copies earlier output. Streaming --list flushes and recycles. */
    struct query_out_seg *out_head;
    struct query_out_seg *out_tail;
    size_t out_len;   /* total bytes across every live segment */
    size_t out_lines; /* complete '\n'-terminated lines currently buffered */
    struct query_lrec_seg *lrec_head;
    struct query_lrec_seg *lrec_tail;
    size_t lrec_count;
    uint64_t blocks_decompressed;
    uint64_t blocks_skipped;
    uint64_t records_skipped;
    uint64_t path_hits_last; /* --list: same parent as the previous listed record */
    uint64_t path_hits_hash; /* --list: 8192-slot dir_id cache */
    uint64_t path_misses;    /* --list: catalog chain walk */
    int oom;
} query_result_t;

/* shard_cat_state values. LOADING means one thread owns the slot and is building it. */
#define SHARD_CAT_UNLOADED 0
#define SHARD_CAT_READY 1
#define SHARD_CAT_FAILED 2
#define SHARD_CAT_FREED 3
#define SHARD_CAT_LOADING 4

/*
 * One slice of one shard's dir_id space, handed to a fold worker.
 *
 * Slicing rather than one job per shard so a corpus with a single huge catalog folds on every
 * thread instead of one, which is the shape that most needs it.
 */
typedef struct {
    size_t fi;
    uint64_t lo, hi; /* dir_ids [lo, hi], inclusive */
} analyze_fold_job_t;

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
     * catalog build for every segment. Loaded lazily (one loader per shard, the
     * read itself outside shard_cat_lock — see analyze_get_shard_catalog) and freed
     * as soon as a shard's last chunk completes (shard_chunks_left==0) so peak
     * memory stays bounded for many-shard corpora.
     */
    crawl_bin_catalog_t *shard_cat;        /* array[name_count] */
    analyze_dir_counts_t **shard_cnt;      /* array[name_count]; per-shard dir_id -> child counts */
    shard_subtree_t *shard_sub;            /* array[name_count]; query mode with --subtree only */
    crawl_sidecar_t sidecar;               /* kept open for scan membership when dirs.idx is live */
    crawl_sidecar_scope_t *sidecar_scope;  /* per-shard DFS ranges; borrowed by shard_sub */
    int sidecar_mem;                       /* membership comes from the sidecar, not the catalog */
    unsigned char *shard_cat_state;       /* SHARD_CAT_* */
    _Atomic uint64_t *shard_chunks_left;  /* per shard; catalog freed when it reaches 0 */
    pthread_mutex_t shard_cat_lock;
    pthread_cond_t shard_cat_cond;        /* signalled when a SHARD_CAT_LOADING slot resolves */
    _Atomic size_t chunk_cursor;
    _Atomic int failures;
    _Atomic unsigned slot_assign;
    /* Fold phase: set up once the scan has joined, then drained by the same thread count. */
    analyze_fold_job_t *fold_jobs;
    size_t fold_job_count;
    _Atomic size_t fold_cursor;
    /*
     * Whether the fold has to go through the hash map at all.
     *
     * dir_ids are per shard, so the same path in two shards is two ids and only a string can tell
     * that they are one directory. With a single shard there is nothing to merge: dir_id already
     * identifies the directory, every one of them is distinct, and the fold can hand the report a
     * node directly -- no hash, no bucket, no insert CAS. That is the flat-tree case, and it is
     * where the map was costing 22% in parent_map_get_or_add plus 25% in mutex lock and unlock.
     */
    int fold_use_map;
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

/* The progress thread idles between ticks. Polling a stop flag in fixed sleep slices left
 * pthread_join waiting out the in-flight slice, so every run — including ones that finish in
 * microseconds — paid that slice as pure idle wall time (50 ms on a 402-record capture that
 * spends 5% of its wall on CPU). Wait on a condvar against an absolute deadline instead and
 * broadcast at stop, the same shape ecrawl uses for its helper threads. */
static pthread_mutex_t g_stats_wait_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_stats_wait_cond = PTHREAD_COND_INITIALIZER;

static void analyze_stats_wait(analyze_pool_t *p, double seconds) {
    struct timespec deadline;
    long nsec;
    int rc = 0;

    if (atomic_load_explicit(&p->analyze_stop_stats, memory_order_relaxed)) return;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += (time_t)seconds;
    nsec = deadline.tv_nsec + (long)((seconds - (double)(time_t)seconds) * 1e9);
    if (nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        nsec -= 1000000000L;
    }
    deadline.tv_nsec = nsec;

    pthread_mutex_lock(&g_stats_wait_lock);
    while (!atomic_load_explicit(&p->analyze_stop_stats, memory_order_relaxed) && rc != ETIMEDOUT)
        rc = pthread_cond_timedwait(&g_stats_wait_cond, &g_stats_wait_lock, &deadline);
    pthread_mutex_unlock(&g_stats_wait_lock);
}

static void analyze_stats_stop_request(analyze_pool_t *p) {
    atomic_store_explicit(&p->analyze_stop_stats, 1, memory_order_relaxed);
    pthread_mutex_lock(&g_stats_wait_lock);
    pthread_cond_broadcast(&g_stats_wait_cond);
    pthread_mutex_unlock(&g_stats_wait_lock);
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
                    "\recrawl_query: %s / %s (%5.1f%%) | chunks %" PRIu64 "/%zu | rec %" PRIu64 " (%6.1fK/s) | scan %s "
                    "| el %4.0fs | ETA %s     ",
                    bd_h, bt_h, pct, ck, p->chunk_count, rec, rps / 1000.0, bps_h, el, eta_buf);
            fflush(stderr);
        }

        /* ~1s cadence, but the join at shutdown returns as soon as the stop is broadcast. */
        analyze_stats_wait(p, 1.0);
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

static int uid_shard_index_from_name(const char *name, uint32_t *out) {
    const char *p;
    unsigned long v;
    char *end;

    if (strncmp(name, "uid_shard_", 10) != 0) return -1;
    p = name + 10;
    errno = 0;
    v = strtoul(p, &end, 10);
    if (errno || !end || strcmp(end, ".bin") != 0 || v > 0xFFFFFFFFUL) return -1;
    *out = (uint32_t)v;
    return 0;
}

/* Power-of-two uid_shards from crawl_manifest.txt, or 0 if unknown/unusable. */
static uint32_t query_manifest_uid_shards(const char *dir_path) {
    char path[PATH_MAX];
    FILE *fp;
    char line[256];
    uint32_t shards = 0;

    if (snprintf(path, sizeof(path), "%s/crawl_manifest.txt", dir_path) >= (int)sizeof(path)) return 0;
    fp = fopen(path, "r");
    if (!fp) return 0;
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "uid_shards=", 11) == 0) {
            unsigned long v;
            char *end;

            errno = 0;
            v = strtoul(line + 11, &end, 10);
            if (errno == 0 && end != line + 11 && v > 0UL && v <= 0xFFFFFFFFUL && (v & (v - 1UL)) == 0UL)
                shards = (uint32_t)v;
        }
    }
    fclose(fp);
    return shards;
}

/*
 * Normalise --subtree into the form the catalogs store: absolute, no trailing
 * slash, no "." or "..". The capture holds resolved paths, so a subtree that
 * does not match textually would silently select nothing.
 */
static int query_set_subtree(const char *arg, const char *prog) {
    static char buf[PATH_MAX];
    size_t len;

    if (path_resolve_existing(arg, buf, "ecrawl_query: --subtree ") != 0) {
        /* A capture outlives the tree it describes, and can be read on a host
         * that never had it. An absolute path is usable as written. */
        if (arg[0] != '/') return -1;
        if (strlen(arg) >= sizeof(buf)) {
            fprintf(stderr, "%s: --subtree path too long\n", prog);
            return -1;
        }
        strcpy(buf, arg);
        fprintf(stderr, "ecrawl_query: --subtree %s does not exist here; matching the capture literally\n", arg);
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
        static char parent_buf[PATH_MAX];
        const char *slash = strrchr(buf, '/');

        g_query.sub_base = slash ? slash + 1 : buf;
        g_query.sub_base_len = strlen(g_query.sub_base);
        /* The subtree's own directory record hangs off this parent, which is outside the
         * subtree: a shard is only worth reading when it holds one of the two. Catalog dir
         * paths for direct children of the capture root are "", so use that spelling. */
        if (slash && slash != buf) {
            size_t plen = (size_t)(slash - buf);

            memcpy(parent_buf, buf, plen);
            parent_buf[plen] = '\0';
        } else {
            parent_buf[0] = '\0';
        }
        g_query.sub_parent = parent_buf;
    }
    g_query.active = 1;
    return 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [options] <crawl-output-dir>\n"
            "\n"
            "  Two forms: directory-shape stats (default), or record query (any filter below).\n"
            "  Query filters AND together.  Totals are key=value (bytes match du -sb).\n"
            "\n"
            "Options:\n"
            "  --verbose, -v             per-chunk lines on stderr\n"
            "  --top[,dim...] N          top lists: dense (file count), deep (slash depth); default dense, N=%u\n"
            "  --subtree DIR             records at or under DIR (DIR included)\n"
            "  --size-gt N              larger than N bytes\n"
            "  --type C                  f d l c b p s o\n"
            "  --gid N / --uid N         numeric owner; --uid opens that uid shard only\n"
            "  --perm MODE               find -perm: 0644 exact, -MODE all bits, /MODE any bits\n"
            "  --list                    print matching paths; totals move to stderr\n"
            "  --level N                with --list: unique prefixes at relative depth N\n"
            "  --sum                     with --list: files,dirs,symlinks,other,bytes,path\n"
            "  --exact                   never answer from catalog rollups\n"
            "  --index-dir DIR           from ereport_index --make; speeds --subtree\n"
            "\n"
            "Environment:\n"
            "  ECRAWL_QUERY_THREADS       workers (default %u)\n"
            "  ECRAWL_QUERY_BLOCK_SKIP   0 = decode every row group (default: skip by zone maps)\n"
            "\n"
            "Details: docs/tools.md.\n",
            prog, g_top_n, DEFAULT_ANALYZE_THREADS);
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
    const char *e = getenv("ECRAWL_QUERY_THREADS");
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

/*
 * Hash a parent path whose length the caller already knows.
 *
 * Eight bytes per multiply instead of one. The previous FNV-1a walked the string a byte at a time,
 * so an average path was an ~80-long chain of dependent multiplies, and it had to find the NUL
 * itself even where the caller had just been handed the length. Nothing outside this run ever sees
 * the value -- it picks a bucket and short-circuits a compare -- so the function is free to change.
 */
#define ANALYZE_HASH_M 0x9e3779b97f4a7c15ULL

static inline uint64_t analyze_hash_read8(const char *p) {
    uint64_t v;

    memcpy(&v, p, sizeof(v)); /* compiles to one unaligned load */
    return v;
}

static inline uint64_t analyze_hash_mix(uint64_t h, uint64_t v) {
    h ^= v;
    h *= ANALYZE_HASH_M;
    return h ^ (h >> 29);
}

static uint32_t analyze_hash_parent(const char *s, size_t len) {
    uint64_t h = 0xcbf29ce484222325ULL ^ ((uint64_t)len * ANALYZE_HASH_M);
    size_t i = 0;

    for (; i + 8U <= len; i += 8U) h = analyze_hash_mix(h, analyze_hash_read8(s + i));
    if (i < len) {
        uint64_t tail;

        if (len >= 8U) {
            /* Re-read the final eight bytes rather than loop over the odd ones. The overlap only
             * feeds bytes in twice, and the length is already mixed in, so nothing collapses. */
            tail = analyze_hash_read8(s + len - 8U);
        } else {
            unsigned char buf[8] = {0};

            memcpy(buf, s + i, len - i);
            memcpy(&tail, buf, sizeof(tail));
        }
        h = analyze_hash_mix(h, tail);
    }
    h *= ANALYZE_HASH_M;
    return (uint32_t)(h >> 32) ^ (uint32_t)h;
}

/*
 * nthreads: retained so call sites stay stable; inserts are CAS-published and no
 * longer striped. expect_parents: upper bound on distinct parents, summed from the
 * shard catalogs' entry counts.
 */
static parent_map_t *parent_map_new(unsigned nthreads, uint64_t expect_parents) {
    parent_map_t *m = (parent_map_t *)calloc(1, sizeof(*m));
    uint64_t want_buckets;
    size_t nbuckets;

    (void)nthreads;
    if (!m) return NULL;

    want_buckets = expect_parents + expect_parents / 2ULL; /* ~0.67 load at the bound */
    if (want_buckets < ANALYZE_HASH_BUCKETS_MIN) want_buckets = ANALYZE_HASH_BUCKETS_MIN;
    if (want_buckets > ANALYZE_HASH_BUCKETS_MAX) want_buckets = ANALYZE_HASH_BUCKETS_MAX;
    nbuckets = ANALYZE_HASH_BUCKETS_MIN;
    while (nbuckets < (size_t)want_buckets && nbuckets < (size_t)ANALYZE_HASH_BUCKETS_MAX) nbuckets <<= 1;
    m->bucket_mask = nbuckets - 1U;

    /* calloc, not malloc+memset: a large table comes back as untouched zero pages, so buckets no
     * parent ever lands in are never faulted in. */
    m->buckets = (_Atomic(parent_node_t *) *)calloc(nbuckets, sizeof(*m->buckets));
    if (!m->buckets) {
        free(m);
        return NULL;
    }
    return m;
}

/*
 * One arena per worker, registered with the map so the map can release the blocks at the end. The
 * arena is only ever bumped by its owning thread; the registry push is the single shared step.
 */
static parent_arena_t *parent_map_arena_new(parent_map_t *m) {
    parent_arena_t *a;

    if (!m) return NULL;
    a = (parent_arena_t *)calloc(1, sizeof(*a));
    if (!a) return NULL;
    a->reg_next = atomic_load_explicit(&m->arenas, memory_order_relaxed);
    while (!atomic_compare_exchange_weak_explicit(&m->arenas, &a->reg_next, a, memory_order_release,
                                                  memory_order_relaxed)) {
    }
    return a;
}

/* Bump stride for one node plus its path. Shared with the arena walk so the two cannot drift. */
static inline size_t parent_arena_stride(size_t path_len) {
    return (sizeof(parent_node_t) + path_len + 1U + 7U) & ~(size_t)7U;
}

static void *parent_arena_alloc(parent_arena_t *a, size_t sz) {
    parent_arena_block_t *b = a->cur;
    size_t need = (sz + 7U) & ~(size_t)7U; /* keep the next node 8-aligned for its atomics */
    void *p;

    if (!b || b->cap - b->used < need) {
        size_t want = a->next_block_bytes ? a->next_block_bytes : PARENT_ARENA_BLOCK_BYTES;
        size_t cap = want > need ? want : need;

        b = (parent_arena_block_t *)malloc(sizeof(*b) + cap);
        if (!b) return NULL;
        b->next = a->cur;
        b->used = 0;
        b->cap = cap;
        a->cur = b;
        a->next_block_bytes = want < PARENT_ARENA_BLOCK_MAX_BYTES ? want * 2U : PARENT_ARENA_BLOCK_MAX_BYTES;
        /* Advisory, and self-limiting: a no-op until cap clears the helper's 4 MiB floor, so the
         * early small blocks are left alone and only an arena that kept growing gets huge pages.
         * Prefaulting is deliberately not paired with this. The bucket table above is calloc'd
         * precisely so untouched buckets never fault, and a block is bump-filled front to back,
         * so the only pages advised here are ones this worker goes on to write. */
        alloc_hint_hugepages(b->data, cap);
    }
    p = b->data + b->used;
    b->used += need;
    return p;
}

static void parent_map_free(parent_map_t *m) {
    parent_arena_t *a;

    if (!m) return;
    /* Nodes and path bytes live in the arenas; the bucket chains are just pointers into them. */
    a = atomic_load_explicit(&m->arenas, memory_order_acquire);
    while (a) {
        parent_arena_t *anext = a->reg_next;
        parent_arena_block_t *b = a->cur;

        while (b) {
            parent_arena_block_t *bnext = b->next;

            free(b);
            b = bnext;
        }
        free(a);
        a = anext;
    }
    free(m->buckets);
    free(m);
}

/*
 * Visit every node in the map, in arena order.
 *
 * The report used to reach the nodes through the bucket array, which meant reading every bucket --
 * now sized to the run, so most of them empty -- and then chasing each chain into arena addresses in
 * hash order, one cache miss per parent. The arenas hold exactly the published nodes, laid out in
 * the order they were created, so walking them instead is sequential and touches nothing else.
 */
static void parent_map_for_each(parent_map_t *m, void (*fn)(parent_node_t *, void *), void *ctx) {
    parent_arena_t *a;

    if (!m) return;
    for (a = atomic_load_explicit(&m->arenas, memory_order_acquire); a; a = a->reg_next) {
        parent_arena_block_t *b;

        for (b = a->cur; b; b = b->next) {
            size_t off = 0;

            while (off < b->used) {
                parent_node_t *n = (parent_node_t *)(void *)(b->data + off);

                off += parent_arena_stride(n->path_len);
                fn(n, ctx);
            }
        }
    }
}

/*
 * The run of consecutive records the scan has seen under one parent, not yet added to its node.
 *
 * ecrawl writes a directory's entries together, so a chunk is mostly runs of records sharing a
 * parent, and charging the node per record made each one an atomic read-modify-write on a line every
 * worker under that parent is fighting for -- 2 M of them for the 491 parents of a single huge
 * directory. Counting the run in plain locals and flushing when the parent changes makes that one
 * add per type per run. A run of one costs the extra pointer compare and nothing else.
 */
typedef struct {
    dircnt_t *dst;
    uint64_t nfile, ndir, nsym, nother;
} parent_run_t;

static inline void parent_run_flush(parent_run_t *r) {
    if (!r->dst) return;
    if (r->nfile) atomic_fetch_add_explicit(&r->dst->nfile, r->nfile, memory_order_relaxed);
    if (r->ndir) atomic_fetch_add_explicit(&r->dst->ndir, r->ndir, memory_order_relaxed);
    if (r->nsym) atomic_fetch_add_explicit(&r->dst->nsym, r->nsym, memory_order_relaxed);
    if (r->nother) atomic_fetch_add_explicit(&r->dst->nother, r->nother, memory_order_relaxed);
    r->nfile = r->ndir = r->nsym = r->nother = 0ULL;
    r->dst = NULL;
}

/* Charge one record to `dst`, flushing first if it ends the previous run. */
static inline void parent_run_bump(parent_run_t *r, dircnt_t *dst, uint8_t typ) {
    if (r->dst != dst) {
        parent_run_flush(r);
        r->dst = dst;
    }
    if (typ == (uint8_t)'f')
        r->nfile++;
    else if (typ == (uint8_t)'d')
        r->ndir++;
    else if (typ == (uint8_t)'l')
        r->nsym++;
    else
        r->nother++;
}

/* Walk from `from` to (not including) `stop`, which may be NULL for the whole chain. */
static parent_node_t *parent_chain_find(parent_node_t *from, const parent_node_t *stop, uint32_t hx,
                                        const char *parent, size_t parent_len) {
    parent_node_t *n = from;

    while (n && n != stop) {
        if (n->hash == hx && n->path_len == (uint32_t)parent_len && memcmp(n->path, parent, parent_len) == 0)
            return n;
        n = atomic_load_explicit(&n->next, memory_order_acquire);
    }
    return NULL;
}

/*
 * Find the node for parent, creating it (counters zeroed) if absent. Returns NULL only on allocation
 * failure. Safe to call from every worker at once, and the returned node stays valid for the rest of the
 * run (nodes are never moved or freed until the report is done with them), so callers may memo the pointer.
 *
 * A hit — the common case once a chunk gets going — takes no lock at all: the chain is append-at-head and
 * its nodes are immutable, so a reader either misses a concurrent insert or sees it complete. Publish is
 * a CAS on the bucket head, matching the lock-free emit membership table; the node is built before the
 * CAS, and a lost race rewinds the arena bump.
 *
 * The node comes from the caller's arena in one bump, node and path together. Losing the publish race
 * rewinds that bump, so the bytes are handed to the next insert rather than freed.
 */
static parent_node_t *parent_map_get_or_add(parent_map_t *m, parent_arena_t *arena, const char *parent,
                                            size_t parent_len) {
    uint32_t hx = analyze_hash_parent(parent, parent_len);
    size_t bi = (size_t)hx & m->bucket_mask;
    parent_node_t *cur = atomic_load_explicit(&m->buckets[bi], memory_order_acquire);
    parent_node_t *node = parent_chain_find(cur, NULL, hx, parent, parent_len);
    parent_arena_block_t *mark_blk;
    size_t mark_used;

    if (node) return node;
    if (!arena) return NULL;

    mark_blk = arena->cur;
    mark_used = mark_blk ? mark_blk->used : 0U;

    /* Every caller builds `parent` into a buffer and knows how long it is, so the length arrives
     * with the string instead of being rediscovered by strlen and then again by the hash. */
    node = (parent_node_t *)parent_arena_alloc(arena, parent_arena_stride(parent_len));
    if (!node) return NULL;
    node->path = (char *)node + sizeof(*node);
    memcpy(node->path, parent, parent_len);
    node->path[parent_len] = '\0';
    node->path_len = (uint32_t)parent_len;
    node->hash = hx;
    atomic_init(&node->c.nfile, 0ULL);
    atomic_init(&node->c.ndir, 0ULL);
    atomic_init(&node->c.nsym, 0ULL);
    atomic_init(&node->c.nother, 0ULL);

    for (;;) {
        parent_node_t *raced = parent_chain_find(cur, NULL, hx, parent, parent_len);

        if (raced) {
            /* Give the bytes back. The arena is bumped only by its owner and nothing else allocated
             * from it since the mark, so either we are still in the marked block, or the allocation
             * opened a fresh one in which our node is the only occupant. Rewinding both cases keeps
             * every byte in the arena part of a published node, which is what lets the report walk
             * the arena instead of the bucket array. */
            if (arena->cur == mark_blk) {
                if (mark_blk) mark_blk->used = mark_used;
            } else if (arena->cur) {
                arena->cur->used = 0U;
            }
            return raced;
        }
        atomic_store_explicit(&node->next, cur, memory_order_relaxed);
        if (atomic_compare_exchange_weak_explicit(&m->buckets[bi], &cur, node, memory_order_release,
                                                  memory_order_acquire))
            return node;
    }
}

/* Writes the parent into `parent` and reports its length, which the map wants anyway. */
static int parent_dir_from_path(const unsigned char *path, uint16_t path_len, char *parent, size_t parent_sz,
                                size_t *len_out) {
    size_t len = path_len;

    *len_out = 0;
    while (len > 0 && path[len - 1] == '/') len--;
    if (len == 0) {
        if (parent_sz < 2) return -1;
        parent[0] = '.';
        parent[1] = '\0';
        *len_out = 1U;
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
            *len_out = 1U;
            return 0;
        }
        if (last == 0) {
            if (parent_sz < 2) return -1;
            parent[0] = '/';
            parent[1] = '\0';
            *len_out = 1U;
            return 0;
        }
        if (last >= parent_sz) return -1;
        memcpy(parent, path, last);
        parent[last] = '\0';
        *len_out = last;
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

/*
 * Shard reads go through the handle cache: a shard is opened once for its catalog and
 * again for each of its chunks, and glibc serializes every fopen/fclose in the process
 * on one stdio list lock — 56% of this tool's cycles on a 1019-shard capture.
 */
static const crawl_bin_chunk_stdio_t analyze_chunk_io = { crawl_fpcache_fopen, analyze_stdio_fread,
                                                          crawl_fpcache_fclose };

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
                                    uint64_t *dir_count_total_out, uint64_t split_target_bytes) {
    crawl_bin_file_chunk_t *all = NULL;
    size_t all_n = 0, all_cap = 0;
    uint64_t *sizes = NULL;
    uint64_t byte_sum = 0;
    uint64_t dir_sum = 0;
    size_t fi;
    const uint64_t hdr_end = (uint64_t)sizeof(bin_file_header_t);

    *shard_sizes_out = NULL;
    *chunks_out = NULL;
    *chunk_count_out = 0;
    *chunk_bytes_total_out = 0;
    *dir_count_total_out = 0;

    sizes = (uint64_t *)calloc(name_count, sizeof(*sizes));
    if (!sizes) return -1;

    fprintf(stderr, "ecrawl_query: building parse jobs from .ckpt segments (%zu shard file(s))...\n", name_count);
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

        fp = crawl_fpcache_fopen(full, "rb");
        if (!fp) goto fail;
        if (fread(&fh, sizeof(fh), 1, fp) != 1) {
            crawl_fpcache_fclose(fp);
            goto fail;
        }
        /*
         * A catalog blob opens with its entry count, so one 8-byte read per shard -- while it is
         * already open for the header above -- totals the run's directories exactly. That is an
         * exact upper bound on distinct parents, and it is the only thing the parent map needs to
         * size its bucket array before the first worker starts.
         */
        if (crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION) && fh.catalog_offset >= sizeof(fh) &&
            sizes[fi] >= sizeof(uint64_t) && fh.catalog_offset <= sizes[fi] - sizeof(uint64_t)) {
            uint64_t n_dirs = 0;

            if (fseeko(fp, (off_t)fh.catalog_offset, SEEK_SET) == 0 && fread(&n_dirs, sizeof(n_dirs), 1, fp) == 1 &&
                n_dirs <= UINT64_MAX - dir_sum)
                dir_sum += n_dirs;
        }
        crawl_fpcache_fclose(fp);

        {
            uint64_t record_end = sizes[fi];

            if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
                fprintf(stderr, "ecrawl_query: skipping %s: format version %u, expected %u (re-crawl needed)\n",
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

    fprintf(stderr, "ecrawl_query: %zu parse job(s); starting worker scan...\n", all_n);
    fflush(stderr);

    *shard_sizes_out = sizes;
    *chunks_out = all;
    *chunk_count_out = all_n;
    *chunk_bytes_total_out = byte_sum;
    *dir_count_total_out = dir_sum;
    return 0;

fail:
    crawl_bin_free_chunk_array_rows(all, all_n);
    free(sizes);
    return -1;
}

static int analyze_scan_fp_until(FILE *fp, uint64_t start_off, uint64_t scan_end_exclusive, uint64_t file_sz,
                                 const crawl_bin_catalog_t *cat, unsigned char *pathbuf, char *parentbuf,
                                 char *fullpath_buf, size_t fullpath_sz, parent_map_t *map, parent_arena_t *arena,
                                 uint64_t *depth_hist, analyze_dir_counts_t *cnt, uint64_t *nrec_out) {
    uint64_t nrec = 0;
    crawl_bin_block_reader_t br;
    crawl_bin_chunk_stdio_t bio;
    parent_run_t run;

    (void)pathbuf; /* names now come from the block reader's decompression buffer */
    (void)file_sz;
    if (nrec_out) *nrec_out = 0;
    memset(&run, 0, sizeof(run));

    bio.fopen = NULL;
    bio.fread = fread_unlocked; /* fp is owned by one thread; unlocked is safe */
    bio.fclose = NULL;
    if (crawl_bin_block_reader_init(&br, &bio, fp, start_off, scan_end_exclusive) != 0) return -1;
    /* Directory-shape stats need the parent, the type and whether the name is empty. Sizes,
     * timestamps and ownership are eight more columns this pass never reads, so leaving them
     * out of the projection skips both their I/O and their decode.
     *
     * The name *bytes* are left out too, and they are the largest column on disk. A record's
     * parent is its parent_dir_id's directory whatever the name says -- crawl_bin_format.h
     * states the contract, "name is a single path component (no slashes); root row uses
     * name_len == 0" -- so the only thing the bytes ever decided here was a memchr for a '/'
     * that the format forbids. The lengths alone still separate the root row, and skipping the
     * blob turns its column chunk into part of a coalesced seek: no read, no decode, and no
     * copy into the reader's name buffer. */
    (void)crawl_bin_block_reader_set_projection(
        &br, CRAWL_COL_BIT(CRAWL_COL_PARENT_DIR_ID) | CRAWL_COL_BIT(CRAWL_COL_TYPE) |
                 CRAWL_COL_BIT(CRAWL_COL_NAME_LEN));

    /*
     * A group at a time, reading columns, rather than a record at a time through the
     * row-reconstruction shim. crawl_bin_block_reader_next() rebuilds a whole
     * bin_record_hdr_t per record -- a memset of the struct plus a projection test and an
     * indexed load for every one of the fifteen columns -- to hand back the three this pass
     * actually reads. Taking the group's arrays touches only those three, walking each in
     * sequence, which is the access pattern a columnar format exists to allow: the shim was
     * 59% of this scan's cycles on a 12M-record capture. ereport_index's row-group index
     * build already reads this way.
     *
     * A column pointer is NULL when the group did not carry that column -- a hardlink-only
     * column in a group with no hardlinks, say. The shim reported those as a zero field, and
     * the reads below keep that, so an absent column still means "0" and not a wild read.
     */
    for (;;) {
        uint32_t ngrp = 0;
        const uint64_t *c_pid, *c_type, *c_nlen;
        uint32_t k;
        int got = crawl_bin_block_reader_next_group(&br, &ngrp);

        if (got == 0) break;
        if (got < 0) {
            parent_run_flush(&run);
            crawl_bin_block_reader_free(&br);
            return -1;
        }

        c_pid = crawl_bin_block_reader_column(&br, CRAWL_COL_PARENT_DIR_ID);
        c_type = crawl_bin_block_reader_column(&br, CRAWL_COL_TYPE);
        c_nlen = crawl_bin_block_reader_column(&br, CRAWL_COL_NAME_LEN);

        for (k = 0; k < ngrp; k++) {
            uint64_t pid = c_pid ? c_pid[k] : 0ULL;
            uint16_t nlen = c_nlen ? (uint16_t)c_nlen[k] : 0U;
            uint8_t rtype = c_type ? (uint8_t)c_type[k] : 0U;
            int by_dir_id;

            if (pid == 0ULL) {
                parent_run_flush(&run);
                crawl_bin_block_reader_free(&br);
                return -1;
            }

            /*
             * Which directory to charge, and how deep the record sits, are both answers the catalog
             * already holds under a dir_id. Two shapes of record resolve to one without a string:
             *
             *   name_len > 0   an ordinary entry: its parent is pid's directory and it sits one level
             *                  below, so charge pid and bin at depth[pid] + 1.
             *
             *   name_len == 0  the row for a directory itself. Its own path is pid's directory, so its
             *                  parent is pid's parent and the '/'-count of the path the long route
             *                  builds ("/a/b/") is depth[pid] + 1 -- the same two answers, without
             *                  materialising a path only to strip its last component back off.
             *                  dir 1 is the exception: its path is "/", whose parent is ".", which is
             *                  no directory in this catalog. That row goes the long way.
             */
            by_dir_id = (cnt != NULL && pid <= cat->max_dir_id);
            if (by_dir_id && nlen == 0) by_dir_id = (pid > 1ULL && cat->parent_dir_id[pid] != 0ULL);

            if (by_dir_id) {
                uint64_t did = nlen > 0 ? pid : cat->parent_dir_id[pid];
                unsigned d = (unsigned)cat->depth[pid] + 1U;
                unsigned db = d >= ANALYZE_DEPTH_BINS ? ANALYZE_DEPTH_BINS - 1U : d;

                depth_hist[db]++;
                parent_run_bump(&run, &cnt[did], rtype);
            }

            if (!by_dir_id) {
                size_t flen;
                uint16_t pl;

                /*
                 * What is left is the row for dir 1 itself -- path "/", parent "." -- and rows naming a
                 * dir_id this catalog does not have. Those already failed the chunk when this branch
                 * rebuilt their path, because dir_path_len refuses an unknown id.
                 *
                 * A name is needed to go further, and the projection left the name bytes on disk, so a
                 * row that still has a name here cannot be served: fail rather than invent a path from
                 * a length with no bytes behind it.
                 */
                if (nlen > 0) {
                    parent_run_flush(&run);
                    crawl_bin_block_reader_free(&br);
                    return -1;
                }
                if (crawl_bin_catalog_entry_path(cat, pid, NULL, 0, fullpath_buf, fullpath_sz) != 0) {
                    parent_run_flush(&run);
                    crawl_bin_block_reader_free(&br);
                    return -1;
                }
                flen = strlen(fullpath_buf);
                pl = flen > 65535U ? 65535U : (uint16_t)flen;
                if (flen > 0) {
                    parent_node_t *node;
                    size_t parent_len;

                    depth_hist[analyze_depth_slash_bin((unsigned char *)fullpath_buf, pl)]++;
                    if (parent_dir_from_path((unsigned char *)fullpath_buf, pl, parentbuf,
                                             ANALYZE_PARENT_PATH_MAX, &parent_len) == 0) {
                        node = parent_map_get_or_add(map, arena, parentbuf, parent_len);
                        if (node) parent_run_bump(&run, &node->c, rtype);
                    }
                }
            }
            nrec++;
        }
    }

    parent_run_flush(&run);
    crawl_bin_block_reader_free(&br);
    if (nrec_out) *nrec_out = nrec;
    return 0;
}

static int query_u64_push(uint64_t **a, size_t *n, size_t *cap, uint64_t v) {
    if (*n == *cap) {
        size_t nc = *cap ? *cap * 2U : 4U;
        uint64_t *p = (uint64_t *)realloc(*a, nc * sizeof(*p));

        if (!p) return -1;
        *a = p;
        *cap = nc;
    }
    (*a)[(*n)++] = v;
    return 0;
}

/*
 * dir_ids of the subtree and of its parent directory, in one pass over the catalog.
 * Matched on the last path component first — a memcmp against one name — so only
 * namesakes pay for a path rebuild. A corrupt catalog can store several dir_ids
 * that reconstruct to the same path; every hit is collected (length-aware: a
 * NUL-padded name is not truncated by strcmp).
 */
static int subtree_find_dirs(const crawl_bin_catalog_t *cat, uint64_t **roots_out, size_t *nroots_out,
                             uint64_t **parents_out, size_t *nparents_out) {
    const char *pbase;
    size_t pbase_len, parent_plen;
    uint64_t did;
    char pathbuf[PATH_MAX];
    uint64_t *roots = NULL, *parents = NULL;
    size_t nroots = 0, nparents = 0, rcap = 0, pcap = 0;

    *roots_out = NULL;
    *nroots_out = 0;
    *parents_out = NULL;
    *nparents_out = 0;
    parent_plen = strlen(g_query.sub_parent);
    /* The capture root is dir 1 and its reconstructed path is the empty string. */
    if (g_query.sub_parent[0] == '\0' && cat->max_dir_id >= 1ULL) {
        if (query_u64_push(&parents, &nparents, &pcap, 1ULL) != 0) return -1;
    }
    {
        const char *slash = strrchr(g_query.sub_parent, '/');

        pbase = slash ? slash + 1 : g_query.sub_parent;
        pbase_len = strlen(pbase);
    }

    for (did = 1; did <= cat->max_dir_id; did++) {
        size_t nlen = (size_t)cat->name_len[did];
        size_t plen = 0;
        int maybe_root = (nlen == g_query.sub_base_len);
        int maybe_parent = (nlen == pbase_len);

        if (!maybe_root && !maybe_parent) continue;
        if (!cat->name_comp[did]) continue;
        if (maybe_root && memcmp(cat->name_comp[did], g_query.sub_base, nlen) != 0) maybe_root = 0;
        if (maybe_parent && memcmp(cat->name_comp[did], pbase, nlen) != 0) maybe_parent = 0;
        if (!maybe_root && !maybe_parent) continue;
        if (crawl_bin_catalog_dir_path_len(cat, did, pathbuf, sizeof(pathbuf), &plen) != 0) continue;
        if (maybe_root && plen == g_query.subtree_len && memcmp(pathbuf, g_query.subtree, plen) == 0) {
            if (query_u64_push(&roots, &nroots, &rcap, did) != 0) {
                free(roots);
                free(parents);
                return -1;
            }
        }
        if (maybe_parent && plen == parent_plen && memcmp(pathbuf, g_query.sub_parent, plen) == 0) {
            if (query_u64_push(&parents, &nparents, &pcap, did) != 0) {
                free(roots);
                free(parents);
                return -1;
            }
        }
    }
    *roots_out = roots;
    *nroots_out = nroots;
    *parents_out = parents;
    *nparents_out = nparents;
    return 0;
}

static void query_warn_dup_subtree(size_t nroots, const char *where) {
    if (nroots <= 1U) return;
    fprintf(stderr, "ecrawl_query: %zu duplicate catalog entries for %s in %s; unioning DFS ranges\n", nroots,
            g_query.subtree, where ? where : "(shard)");
}

static void subtree_free(shard_subtree_t *s);

/*
 * Locate g_query.subtree in one catalog and record its DFS range(s) plus the dir_id hull the
 * shard's records must fall inside.
 *
 * One pass over the directories to find every dir_id whose reconstructed path equals
 * the subtree -- a component compare, so only namesakes cost a path rebuild -- and then
 * each range comes straight out of the permutation. Membership for every record is a
 * comparison against those ranges afterwards.
 *
 * A shard whose catalog has no directory at the subtree path holds no record under it, and
 * the whole shard is skipped unless it can hold the subtree's own directory record — which
 * hangs off the subtree's parent, so it is enough to look that one directory up.
 *
 * want_hull asks for the dir_id hull as well. Only the record scan reads it, to hand the
 * block reader a zone-map range; the rollup fast path answers from the root's own row and
 * never looks, so it passes 0 and skips a second O(directories) loop per shard.
 */
static int subtree_build(const crawl_bin_catalog_t *cat, shard_subtree_t *out, int want_hull) {
    uint64_t did;
    uint64_t *roots = NULL, *parents = NULL;
    size_t nroots = 0, nparents = 0, i;

    memset(out, 0, sizeof(*out));
    if (!cat) return -1;
    out->max_dir_id = cat->max_dir_id;
    if (g_query.subtree_is_root) {
        out->whole = 1;
        return 0;
    }
    if (!cat->dfs_index || !cat->dfs_subtree_dirs) {
        errno = EINVAL;
        return -1;
    }

    if (subtree_find_dirs(cat, &roots, &nroots, &parents, &nparents) != 0) return -1;

    out->parent_ids = parents;
    out->nparents = nparents;
    out->nroots = nroots;
    if (nroots == 0) {
        free(roots);
        if (nparents == 0) {
            out->empty = 1;
            return 0;
        }
        if (want_hull) {
            out->pid_lo = out->pid_hi = parents[0];
            for (i = 1; i < nparents; i++) {
                if (parents[i] < out->pid_lo) out->pid_lo = parents[i];
                if (parents[i] > out->pid_hi) out->pid_hi = parents[i];
            }
            out->have_hull = 1;
        }
        return 0;
    }

    out->dfs_index = cat->dfs_index;
    out->root_id = roots[0];
    out->dfs_lo = cat->dfs_index[roots[0]];
    out->dfs_hi = out->dfs_lo + cat->dfs_subtree_dirs[roots[0]];
    if (nroots == 1U) {
        free(roots);
    } else {
        out->root_ids = roots;
        out->ranges = (query_dfs_range_t *)malloc(nroots * sizeof(*out->ranges));
        if (!out->ranges) {
            subtree_free(out);
            return -1;
        }
        for (i = 0; i < nroots; i++) {
            out->ranges[i].lo = cat->dfs_index[roots[i]];
            out->ranges[i].hi = out->ranges[i].lo + cat->dfs_subtree_dirs[roots[i]];
        }
    }

    if (!want_hull) return 0;

    out->pid_lo = nparents ? parents[0] : out->root_id;
    out->pid_hi = out->pid_lo;
    for (i = 0; i < nparents; i++) {
        if (parents[i] < out->pid_lo) out->pid_lo = parents[i];
        if (parents[i] > out->pid_hi) out->pid_hi = parents[i];
    }
    for (did = 1; did <= cat->max_dir_id; did++) {
        if (!subtree_contains(out, did)) continue;
        if (did < out->pid_lo) out->pid_lo = did;
        if (did > out->pid_hi) out->pid_hi = did;
    }
    out->have_hull = 1;
    return 0;
}

/*
 * Credit the subtree's own directory record from the catalog of the shards that hold it,
 * for the scans that no longer decode names and so cannot recognise it among the records.
 * Called once per shard, from the thread that built the catalog. Every matching root is
 * credited: duplicate catalog entries are distinct rows, each with its own self_present.
 */
static void query_note_subtree_self(const crawl_bin_catalog_t *cat, const shard_subtree_t *sub) {
    size_t i;

    if (query_needs_names() || !sub || sub->nroots == 0U || sub->whole) return;
    if (!cat->self_present || !cat->self_bytes) return;
    if (g_query.type_filter && g_query.type_filter != 'd') return;
    for (i = 0; i < sub->nroots; i++) {
        uint64_t root = shard_sub_root_at(sub, i);

        if (root == 0ULL || !cat->self_present[root]) continue;
        if (g_query.have_size_gt && cat->self_bytes[root] <= g_query.size_gt) continue;
        atomic_fetch_add_explicit(&g_subtree_self_count, 1ULL, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_subtree_self_bytes, cat->self_bytes[root], memory_order_relaxed);
    }
}

static void subtree_free(shard_subtree_t *s) {
    if (!s) return;
    if (!s->sidecar_mem) {
        /* Sidecar-backed entries borrow their arrays from the pool's scope. */
        free(s->root_ids);
        free(s->ranges);
        free(s->parent_ids);
    }
    memset(s, 0, sizeof(*s));
}

/*
 * Fill membership state from the dir-index scope instead of the catalog, so the
 * scan never materializes the shard's catalog. root_ids/ranges/parent_ids are
 * borrowed from the scope, which the pool keeps alive until the workers join.
 *
 * The dir_id hull is looser than subtree_build's: parent-before-child id
 * assignment makes every in-subtree id larger than the root's, so
 * [min(root,parent ids), max_dir_id] is a safe superset. Row-group pruning has
 * already done the coarse skipping; the per-record range test stays exact.
 */
static void query_subtree_from_scope(const crawl_sidecar_scope_t *sp, uint64_t max_dir_id, shard_subtree_t *out) {
    size_t i;

    memset(out, 0, sizeof(*out));
    out->max_dir_id = max_dir_id;
    out->sidecar_mem = 1;
    out->nroots = sp->nroots;
    out->root_id = sp->root;
    out->root_ids = sp->roots;
    out->ranges = (query_dfs_range_t *)sp->ranges; /* same {lo,hi} layout */
    out->dfs_lo = sp->dfs_lo;
    out->dfs_hi = sp->dfs_hi;
    out->parent_ids = sp->parents;
    out->nparents = sp->nparents;
    out->empty = !sp->in_shard;
    if (sp->nroots || sp->nparents) {
        uint64_t lo = UINT64_MAX;

        for (i = 0; i < sp->nroots; i++)
            if (sp->roots[i] < lo) lo = sp->roots[i];
        for (i = 0; i < sp->nparents; i++)
            if (sp->parents[i] < lo) lo = sp->parents[i];
        out->pid_lo = lo;
        out->pid_hi = max_dir_id;
        out->have_hull = 1;
    }
}

/* Range test on a DFS position, the sidecar route's half of subtree_contains. */
static int query_subtree_dfs_in(const shard_subtree_t *s, uint64_t p) {
    size_t i;

    if (s->nroots == 1U) return p >= s->dfs_lo && p < s->dfs_hi;
    for (i = 0; i < s->nroots; i++)
        if (p >= s->ranges[i].lo && p < s->ranges[i].hi) return 1;
    return 0;
}

/*
 * The subtree's own directory record, credited without a catalog: the scope
 * carries each root's self-record flag and size. Only for scans that project no
 * names; with names the scan recognises the record itself (see query_needs_names).
 */
static void query_note_subtree_self_scope(const crawl_sidecar_scope_t *sp) {
    size_t i;

    if (query_needs_names() || !sp) return;
    if (g_query.type_filter && g_query.type_filter != 'd') return;
    for (i = 0; i < sp->nroots; i++) {
        if (!sp->self_flags[i]) continue;
        if (g_query.have_size_gt && sp->self_bytes[i] <= g_query.size_gt) continue;
        atomic_fetch_add_explicit(&g_subtree_self_count, 1ULL, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_subtree_self_bytes, sp->self_bytes[i], memory_order_relaxed);
    }
}

static pthread_mutex_t g_query_out_mutex = PTHREAD_MUTEX_INITIALIZER;
/* One batch per worker per flush: bigger batches mean fewer turns on the mutex and fewer
 * write() calls for the same listing. */
#define QUERY_OUT_FLUSH_BYTES (1024U * 1024U)
#define QUERY_OUT_SEG_BYTES (2U * 1024U * 1024U)
#define QUERY_LREC_SEG_CAP 4096U

typedef struct query_out_seg {
    struct query_out_seg *next;
    size_t used;
    size_t lines;
    char data[QUERY_OUT_SEG_BYTES];
} query_out_seg_t;

typedef struct query_lrec_seg {
    struct query_lrec_seg *next;
    size_t count;
    size_t cap;
    query_listrec_t rec[QUERY_LREC_SEG_CAP];
} query_lrec_seg_t;

static void query_out_segs_free(query_out_seg_t *s) {
    while (s) {
        query_out_seg_t *n = s->next;

        free(s);
        s = n;
    }
}

static void query_lrec_segs_free(query_lrec_seg_t *s) {
    while (s) {
        query_lrec_seg_t *n = s->next;

        free(s);
        s = n;
    }
}

static int query_out_seg_push(query_result_t *qr) {
    query_out_seg_t *s = (query_out_seg_t *)malloc(sizeof(*s));

    if (!s) return -1;
    s->next = NULL;
    s->used = 0;
    s->lines = 0;
    if (qr->out_tail)
        qr->out_tail->next = s;
    else
        qr->out_head = s;
    qr->out_tail = s;
    return 0;
}

static int query_lrec_seg_push(query_result_t *qr) {
    query_lrec_seg_t *s = (query_lrec_seg_t *)malloc(sizeof(*s));

    if (!s) return -1;
    s->next = NULL;
    s->count = 0;
    s->cap = QUERY_LREC_SEG_CAP;
    if (qr->lrec_tail)
        qr->lrec_tail->next = s;
    else
        qr->lrec_head = s;
    qr->lrec_tail = s;
    return 0;
}

/*
 * Paths go straight to fd 1: stdio would copy every batch into its own buffer on the way to
 * the same write(). Nothing else writes to stdout while workers run (the summary goes to
 * stderr in --list mode), so the two never interleave.
 */
static void query_out_flush(query_result_t *qr) {
    query_out_seg_t *s;

    if (!qr->out_head || qr->out_len == 0U) return;
    pthread_mutex_lock(&g_query_out_mutex);
    for (s = qr->out_head; s; s = s->next) {
        size_t off = 0;

        while (off < s->used) {
            ssize_t w = write(STDOUT_FILENO, s->data + off, s->used - off);

            if (w < 0) {
                if (errno == EINTR) continue;
                qr->oom = 1;
                break;
            }
            off += (size_t)w;
        }
        if (qr->oom) break;
    }
    pthread_mutex_unlock(&g_query_out_mutex);
    /* Recycle the first segment; drop the rest so a streaming listing stays at 2 MiB. */
    s = qr->out_head;
    if (s) {
        query_out_segs_free(s->next);
        s->next = NULL;
        s->used = 0;
        s->lines = 0;
        qr->out_tail = s;
    }
    qr->out_len = 0;
    qr->out_lines = 0;
}

static int query_out_append(query_result_t *qr, const char *path, size_t len) {
    size_t need = len + 1U;
    query_out_seg_t *s;

    if (need > QUERY_OUT_SEG_BYTES) return -1;
    if (!query_list_buffered() && qr->out_len + need > QUERY_OUT_FLUSH_BYTES) query_out_flush(qr);
    s = qr->out_tail;
    if (!s || s->used + need > QUERY_OUT_SEG_BYTES) {
        if (query_out_seg_push(qr) != 0) return -1;
        s = qr->out_tail;
    }
    memcpy(s->data + s->used, path, len);
    s->used += len;
    s->data[s->used++] = '\n';
    s->lines++;
    qr->out_len += need;
    qr->out_lines++;
    if (!query_list_buffered() && qr->out_len >= QUERY_OUT_FLUSH_BYTES) query_out_flush(qr);
    return 0;
}

static int query_lrec_append(query_result_t *qr, uint64_t size, uint8_t type, uint32_t hl_idx) {
    query_lrec_seg_t *s = qr->lrec_tail;

    if (!s || s->count == s->cap) {
        if (query_lrec_seg_push(qr) != 0) return -1;
        s = qr->lrec_tail;
    }
    s->rec[s->count].size = size;
    s->rec[s->count].type = type;
    s->rec[s->count].hl_idx = hl_idx;
    s->count++;
    qr->lrec_count++;
    return 0;
}

typedef struct {
    query_lrec_seg_t *seg;
    size_t off;
} query_lrec_cur_t;

static int query_lrec_cur_seek(query_result_t *qr, size_t idx, query_lrec_cur_t *c) {
    query_lrec_seg_t *s = qr->lrec_head;

    while (s) {
        if (idx < s->count) {
            c->seg = s;
            c->off = idx;
            return 0;
        }
        idx -= s->count;
        s = s->next;
    }
    c->seg = NULL;
    c->off = 0;
    return -1;
}

static query_listrec_t *query_lrec_cur_get(query_lrec_cur_t *c) {
    if (!c->seg || c->off >= c->seg->count) return NULL;
    return &c->seg->rec[c->off];
}

static void query_lrec_cur_next(query_lrec_cur_t *c) {
    if (!c->seg) return;
    c->off++;
    if (c->off >= c->seg->count) {
        c->seg = c->seg->next;
        c->off = 0;
    }
}

static int query_path_is_under(const char *parent, const char *child) {
    size_t plen = strlen(parent);
    size_t clen = strlen(child);

    if (clen <= plen) return 0;
    if (memcmp(parent, child, plen) != 0) return 0;
    return child[plen] == '/';
}

static int query_list_write_line(const char *s, size_t len) {
    size_t off = 0;
    char nl = '\n';

    while (off < len) {
        ssize_t w = write(STDOUT_FILENO, s + off, len - off);

        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        off += (size_t)w;
    }
    while (write(STDOUT_FILENO, &nl, 1) < 0) {
        if (errno == EINTR) continue;
        return -1;
    }
    return 0;
}

/*
 * --list --level N (with or without --sum), hash-based.
 *
 * Matching paths are a forest: level 1 is each path with no matching ancestor, and level N
 * truncates to N - 1 components below that root. The sorted walk this replaces spent most of
 * the run qsorting every matching path only to find, per record, its level-1 root -- the
 * shortest ancestor-or-self prefix that also matched. That root is a membership question, so
 * instead: insert every matching path into a hash set once, then answer each record with a
 * shallow-to-deep prefix probe. Buffers arrive clustered by directory, and a one-entry memo
 * answers nearly every record with a single memcmp: when the memo root prefixes P at a slash
 * boundary, P's shortest matching prefix cannot be shorter than the memo root (else the memo
 * record itself would have rooted higher), so the memo stays correct. Only the distinct
 * output prefixes are sorted at the end, keeping output byte-identical to the sorted walk.
 */

#define QUERY_SUMOUT_CAP ((4U << 20) + PATH_MAX + 128U)

static void query_sumout_flush(char *buf, size_t *len, int *err) {
    size_t off = 0;

    while (off < *len) {
        ssize_t w = write(STDOUT_FILENO, buf + off, *len - off);

        if (w < 0) {
            if (errno == EINTR) continue;
            *err = 1;
            break;
        }
        off += (size_t)w;
    }
    *len = 0;
}

/* path carries an explicit length: --level rows are prefixes into a longer buffered line. */
static void query_sumout_row(char *buf, size_t *len, int *err, uint64_t f, uint64_t d, uint64_t l,
                             uint64_t o, uint64_t b, const char *path, size_t path_len) {
    int n;

    if (*err) return;
    n = snprintf(buf + *len, QUERY_SUMOUT_CAP - *len,
                 "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%.*s\n", f, d, l, o, b,
                 (int)path_len, path);
    if (n < 0) {
        *err = 1;
        return;
    }
    if ((size_t)n >= QUERY_SUMOUT_CAP - *len) {
        query_sumout_flush(buf, len, err);
        if (*err) return;
        n = snprintf(buf, QUERY_SUMOUT_CAP,
                     "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%.*s\n", f, d, l, o,
                     b, (int)path_len, path);
        if (n < 0 || (size_t)n >= QUERY_SUMOUT_CAP) {
            *err = 1;
            return;
        }
    }
    *len += (size_t)n;
}

/* Borrowed-key string set/map over the worker path buffers. Slots point at lines inside
 * res[].out (which outlives the emit); probes carry an explicit length because a prefix
 * probe is not NUL-terminated. The 32-bit hash is only a pre-filter; the length check and
 * memcmp decide, so collisions cost a compare, never a wrong answer. */
typedef struct {
    const char *s;
    uint32_t h;
    uint32_t len;
    uint32_t pay; /* sum-bucket index, or UINT32_MAX when unused */
} query_hslot_t;

typedef struct {
    query_hslot_t *tab;
    size_t mask;  /* capacity - 1; capacity is a power of two */
    size_t count;
} query_hmap_t;

static uint32_t query_hash_path_fnv(const char *s, size_t len) {
    uint64_t h = 0xcbf29ce484222325ULL ^ ((uint64_t)len * ANALYZE_HASH_M);
    size_t i = 0;

    for (; i + 8U <= len; i += 8U) h = analyze_hash_mix(h, analyze_hash_read8(s + i));
    if (i < len) {
        uint64_t tail;

        if (len >= 8U) {
            /* Re-read the final eight bytes; the overlap only feeds bytes twice. */
            tail = analyze_hash_read8(s + len - 8U);
        } else {
            unsigned char buf[8] = {0};

            memcpy(buf, s + i, len - i);
            memcpy(&tail, buf, sizeof(tail));
        }
        h = analyze_hash_mix(h, tail);
    }
    h *= ANALYZE_HASH_M;
    h ^= h >> 29;
    return (uint32_t)h;
}

#if defined(__x86_64__)
static int query_cpu_has_sse42(void) {
    static int cached = -1;
    unsigned eax, ebx, ecx, edx;

    if (cached >= 0) return cached;
#if defined(__SSE4_2__)
    cached = 1;
    return 1;
#else
    cached = 0;
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) cached = (ecx & bit_SSE4_2) ? 1 : 0;
    return cached;
#endif
}

__attribute__((target("sse4.2")))
static uint32_t query_hash_path_crc(const char *s, size_t len) {
    uint64_t seed = 0xcbf29ce484222325ULL ^ ((uint64_t)len * ANALYZE_HASH_M);
    unsigned long long c = (unsigned long long)(uint32_t)seed;
    size_t i = 0;
    uint64_t h;

    for (; i + 8U <= len; i += 8U) c = __builtin_ia32_crc32di(c, analyze_hash_read8(s + i));
    h = (uint64_t)(uint32_t)c;
    if (i < len) {
        uint64_t tail;

        if (len >= 8U) {
            tail = analyze_hash_read8(s + len - 8U);
        } else {
            unsigned char buf[8] = {0};

            memcpy(buf, s + i, len - i);
            memcpy(&tail, buf, sizeof(tail));
        }
        h = analyze_hash_mix(h, tail);
    }
    h *= ANALYZE_HASH_M;
    h ^= h >> 29;
    return (uint32_t)h;
}
#endif

static uint32_t query_hash_path(const char *s, size_t len) {
#if defined(__x86_64__)
    if (query_cpu_has_sse42()) return query_hash_path_crc(s, len);
#endif
    return query_hash_path_fnv(s, len);
}

static int query_hmap_init(query_hmap_t *m, size_t expect) {
    size_t cap = 16;

    while (cap < expect * 2U) cap <<= 1; /* load stays under 1/2: no grow, short probes */
    m->tab = (query_hslot_t *)calloc(cap, sizeof(*m->tab));
    if (!m->tab) return -1;
    m->mask = cap - 1U;
    m->count = 0;
    return 0;
}

static int query_hmap_grow(query_hmap_t *m) {
    size_t ncap = (m->mask + 1U) * 2U;
    query_hslot_t *nt = (query_hslot_t *)calloc(ncap, sizeof(*nt));
    size_t i;

    if (!nt) return -1;
    for (i = 0; i <= m->mask; i++) {
        if (m->tab[i].s) {
            size_t slot = (size_t)m->tab[i].h & (ncap - 1U);

            while (nt[slot].s) slot = (slot + 1U) & (ncap - 1U);
            nt[slot] = m->tab[i];
        }
    }
    free(m->tab);
    m->tab = nt;
    m->mask = ncap - 1U;
    return 0;
}

/* Insert-or-find; *ins is set on a fresh insert (payload left at UINT32_MAX). */
static query_hslot_t *query_hmap_slot(query_hmap_t *m, const char *s, size_t len, int *ins) {
    uint32_t h;
    size_t slot;

    if ((m->count + 1U) * 3U >= (m->mask + 1U) * 2U && query_hmap_grow(m) != 0) return NULL;
    h = query_hash_path(s, len);
    slot = (size_t)h & m->mask;
    for (;;) {
        query_hslot_t *e = &m->tab[slot];

        if (!e->s) {
            e->s = s;
            e->h = h;
            e->len = (uint32_t)len;
            e->pay = UINT32_MAX;
            m->count++;
            *ins = 1;
            return e;
        }
        if (e->h == h && e->len == (uint32_t)len && memcmp(e->s, s, len) == 0) {
            *ins = 0;
            return e;
        }
        slot = (slot + 1U) & m->mask;
    }
}

/* Read-only probe: NULL on miss. */
static const query_hslot_t *query_hmap_find(const query_hmap_t *m, const char *s, size_t len) {
    uint32_t h = query_hash_path(s, len);
    size_t slot = (size_t)h & m->mask;

    for (;;) {
        const query_hslot_t *e = &m->tab[slot];

        if (!e->s) return NULL;
        if (e->h == h && e->len == (uint32_t)len && memcmp(e->s, s, len) == 0) return e;
        slot = (slot + 1U) & m->mask;
    }
}

static int query_path_is_under_len(const char *parent, size_t plen, const char *child, size_t clen) {
    if (clen <= plen) return 0;
    if (plen > 0U && memcmp(parent, child, plen) != 0) return 0;
    return plen == 0U || child[plen] == '/';
}

/* Shortest component-boundary prefix of (s,len) present in mset, else (s,len) itself.
 * Returns the root's component count; *root_len its byte length. */
static unsigned query_level_root_nc(const query_hmap_t *mset, const char *s, size_t len, size_t *root_len) {
    size_t i = 0;
    unsigned d = 0;

    while (i < len && s[i] == '/') i++;
    if (i < len) {
        d = 1;
        for (; i < len; i++) {
            if (s[i] == '/' && i + 1U < len && s[i + 1U] != '/') {
                if (query_hmap_find(mset, s, i)) {
                    *root_len = i;
                    return d;
                }
                d++;
            }
        }
    }
    *root_len = len;
    return d; /* 0 for an all-slashes path, else the full component count */
}

static int query_path_has_ncomp(const char *s, size_t len, unsigned want) {
    size_t i = 0;
    unsigned n = 0;

    if (want == 0U) return 1;
    while (i < len && s[i] == '/') i++;
    if (i == len) return 0;
    n = 1;
    for (; i < len; i++) {
        if (s[i] == '/' && i + 1U < len && s[i + 1U] != '/') {
            n++;
            if (n >= want) return 1;
        }
    }
    return 0;
}

/* Byte length of the first `want` components of a path of `len` bytes that has at least
 * `want` of them. Length-aware: an embedded NUL is name bytes, not a terminator. */
static size_t query_path_prefix_len(const char *s, size_t len, unsigned want) {
    size_t i = 0;
    unsigned n = 0;

    while (i < len && s[i] == '/') i++;
    for (; i < len; i++) {
        if (s[i] == '/' && i + 1U < len && s[i + 1U] != '/') {
            n++;
            if (n == want) break;
        }
    }
    return i;
}

/*
 * Dense --level skips the membership set, but the level root is still the
 * shallowest *listed* path — in a valid capture the root directory's own record,
 * an ancestor of everything else — not the first component of each path (the
 * capture root's parents are not records). The shallowest listed path is found
 * while slicing (fewest components wins, lexicographically first breaks ties,
 * which only corrupt captures can reach) and Pass 2 roots every line at it.
 */
typedef struct {
    const char *p;
    size_t len;
    unsigned ncomp;
} query_dense_root_t;

static unsigned query_path_ncomp(const char *s, size_t len) {
    size_t i = 0;
    unsigned d = 0;

    while (i < len && s[i] == '/') i++;
    if (i < len) {
        d = 1;
        for (; i < len; i++)
            if (s[i] == '/' && i + 1U < len && s[i + 1U] != '/') d++;
    }
    return d;
}

static void query_dense_root_consider(query_dense_root_t *r, const char *s, size_t len) {
    unsigned nc = query_path_ncomp(s, len);
    size_t n;
    int c;

    if (!r->p || nc < r->ncomp) goto take;
    if (nc > r->ncomp) return;
    n = len < r->len ? len : r->len;
    c = memcmp(s, r->p, n);
    if (c < 0 || (c == 0 && len < r->len)) goto take;
    return;
take:
    r->p = s;
    r->len = len;
    r->ncomp = nc;
}

/* The dense Pass 2 root: the shallowest listed path when it is a component-
 * boundary ancestor of this line. An all-slashes root means the crawl was of "/",
 * and the membership walk never roots at the all-slashes prefix, so the first
 * component wins there. A line the root is not an ancestor of roots at itself,
 * exactly what the membership walk would conclude. */
static unsigned query_level_root_dense(const query_dense_root_t *r, const char *s, size_t len,
                                       size_t *root_len) {
    if (r->ncomp == 0U) {
        size_t i = 0;

        while (i < len && s[i] == '/') i++;
        if (i == len) {
            *root_len = len;
            return 0;
        }
        *root_len = query_path_prefix_len(s, len, 1U);
        return 1;
    }
    if (len >= r->len && memcmp(s, r->p, r->len) == 0 && (len == r->len || s[r->len] == '/')) {
        *root_len = r->len;
        return r->ncomp;
    }
    *root_len = len;
    return query_path_ncomp(s, len);
}

/* Line count of [s, s+n), folding each line into the dense-root tracker when given. */
static size_t query_count_lines(const char *s, size_t n, query_dense_root_t *root) {
    const char *end = s + n;
    size_t lines = 0;

    while (s < end) {
        const char *nl = (const char *)memchr(s, '\n', (size_t)(end - s));

        if (!nl) break;
        if (root) query_dense_root_consider(root, s, (size_t)(nl - s));
        lines++;
        s = nl + 1;
    }
    return lines;
}

static int cmp_hslot_path(const void *pa, const void *pb) {
    const query_hslot_t *a = *(const query_hslot_t *const *)pa;
    const query_hslot_t *b = *(const query_hslot_t *const *)pb;
    size_t n = a->len < b->len ? a->len : b->len;
    int c = memcmp(a->s, b->s, n);

    if (c) return c;
    return (a->len > b->len) - (a->len < b->len); /* shorter first: strcmp order */
}

typedef struct {
    uint64_t f, d, l, o, b;
} query_sumbucket_t;

/* Online first-seen dedup of (inode, dev) for --level --sum. The sorted --sum path credits
 * the lexicographically first link; here the first link in buffer order keeps the bytes.
 * Either way each inode's bytes land in exactly one row and the grand total is unchanged. */
typedef struct {
    uint64_t inode;
    uint64_t dev; /* dev_major << 32 | dev_minor */
    int used;
} query_hlseen_t;

/* Returns 1 when the tuple was already seen (caller zeroes the size), 0 on first sight.
 * On OOM it returns 0 forever: count every link, the oom policy of query_hardlink_bytes. */
static int query_hl_seen(query_hlseen_t **tabp, size_t *maskp, size_t *countp, uint64_t inode, uint64_t dev) {
    uint64_t h;
    size_t slot;

    if (!*tabp) {
        *maskp = (1U << 12) - 1U;
        *tabp = (query_hlseen_t *)calloc(*maskp + 1U, sizeof(**tabp));
        if (!*tabp) return 0;
        *countp = 0;
    }
    if ((*countp + 1U) * 2U >= *maskp + 1U) {
        size_t ncap = (*maskp + 1U) * 2U;
        query_hlseen_t *nt = (query_hlseen_t *)calloc(ncap, sizeof(*nt));
        size_t i;

        if (!nt) return 0;
        for (i = 0; i <= *maskp; i++) {
            if ((*tabp)[i].used) {
                size_t ns = (size_t)((*tabp)[i].inode * 1099511628211ULL ^ (*tabp)[i].dev) & (ncap - 1U);

                while (nt[ns].used) ns = (ns + 1U) & (ncap - 1U);
                nt[ns] = (*tabp)[i];
            }
        }
        free(*tabp);
        *tabp = nt;
        *maskp = ncap - 1U;
    }
    h = inode * 1099511628211ULL ^ dev;
    slot = (size_t)h & *maskp;
    for (;;) {
        query_hlseen_t *e = &(*tabp)[slot];

        if (!e->used) {
            e->used = 1;
            e->inode = inode;
            e->dev = dev;
            (*countp)++;
            return 0;
        }
        if (e->inode == inode && e->dev == dev) return 1;
        slot = (slot + 1U) & *maskp;
    }
}

/* ---- Parallel --level emit --------------------------------------------------
 *
 * The serial emit spent nearly the whole query on one core: pass 1 inserted every
 * buffered line into a giant membership table (a cache miss per insert) and pass 2
 * re-walked every line to root it, truncate it, and aggregate. Both passes partition
 * cleanly over byte ranges of the worker buffers:
 *
 *  - Pass 1 inserts into a shared table pre-sized to twice the exact line count, so
 *    the load factor stays under 1/2 and no grow can happen mid-flight; slots are
 *    claimed with a CAS and published with a release store (lock-free).
 *  - Pass 2 runs against the then read-only table with thread-local key maps and sum
 *    buckets; only the --sum hardlink first-seen set stays global (a mutex; hardlink
 *    records are rare). The per-thread maps are merged before printing.
 *
 * Rows come from the same sorted key set as the serial emit, so output is identical.
 */
typedef struct {
    unsigned buf;      /* index into res[] */
    query_out_seg_t *seg;
    size_t begin, end; /* byte range of seg->data; begin sits on a line boundary */
    size_t lines;      /* pass 1 fills: complete lines in the range */
    size_t line_lo;    /* index of the range's first line within the buffer */
} query_emit_task_t;

typedef struct {
    query_result_t *res;
    query_hmap_t mset; /* shared; written in pass 1, read-only afterwards (count unused) */
    query_emit_task_t *tasks;
    size_t ntasks;
    _Atomic size_t cursor;
    int sum;
    int dense; /* no record filters: skip pass 1 / mset, root at the shallowest listed path */
    query_dense_root_t dense_root; /* filled while slicing when dense */
    /* Pass 2 per-thread outputs, indexed by thread slot. */
    query_hmap_t *keys;
    query_sumbucket_t **buckets;
    size_t *nbuckets, *cap_buckets;
    int *oom;
    /* --sum hardlink dedup is global: an inode's links can land in any thread. */
    pthread_mutex_t hl_lock;
    query_hlseen_t *hl_tab;
    size_t hl_mask, hl_count;
} query_emit_ctx_t;

/* Slot-claim marker for the lock-free pass-1 insert: s goes NULL -> BUSY -> line. */
#define QUERY_MSET_BUSY ((const char *)(uintptr_t)1)

/* Insert-or-ignore into the pre-sized membership table from many threads. The table
 * is sized to twice the exact line count before any thread starts, so the load stays
 * under 1/2 and no grow is possible; that is what makes lock-free claims safe. The
 * payload stays UINT32_MAX: pass 1 only needs membership. */
static void query_mset_insert_par(query_hmap_t *m, const char *s, size_t len) {
    uint32_t h = query_hash_path(s, len);
    size_t slot = (size_t)h & m->mask;

    for (;;) {
        query_hslot_t *e = &m->tab[slot];
        const char *cur = __atomic_load_n(&e->s, __ATOMIC_ACQUIRE);

        if (!cur) {
            const char *want = NULL;

            if (__atomic_compare_exchange_n(&e->s, &want, QUERY_MSET_BUSY, 0, __ATOMIC_ACQ_REL,
                                            __ATOMIC_ACQUIRE)) {
                e->h = h;
                e->len = (uint32_t)len;
                e->pay = UINT32_MAX;
                __atomic_store_n(&e->s, s, __ATOMIC_RELEASE);
                return;
            }
            cur = want; /* lost the claim race; inspect the winner */
        }
        while (cur == QUERY_MSET_BUSY) cur = __atomic_load_n(&e->s, __ATOMIC_ACQUIRE);
        /* cur is a published slot, so its h/len (stored before the release) are visible. */
        if (e->h == h && e->len == (uint32_t)len && memcmp(cur, s, len) == 0) return;
        slot = (slot + 1U) & m->mask;
    }
}

static void query_emit_pass1_run(query_emit_ctx_t *c) {
    size_t t;

    while ((t = atomic_fetch_add_explicit(&c->cursor, 1, memory_order_relaxed)) < c->ntasks) {
        query_emit_task_t *task = &c->tasks[t];
        char *s = c->res[task->buf].out_head ? task->seg->data + task->begin : NULL;
        char *end = s ? task->seg->data + task->end : NULL;
        size_t lines = 0;

        while (s < end) {
            char *nl = (char *)memchr(s, '\n', (size_t)(end - s));

            if (!nl) break;
            query_mset_insert_par(&c->mset, s, (size_t)(nl - s));
            lines++;
            s = nl + 1;
        }
        task->lines = lines;
    }
}

/* Pass 2 worker: root each record of the task range, truncate to the output prefix,
 * aggregate into the thread-local key map. Per-line logic is identical to the old
 * serial walk; the last_root memo is thread-local and stays valid because a line's
 * shortest present prefix does not depend on visitation order. */
static void query_emit_pass2_run(query_emit_ctx_t *c, unsigned me) {
    query_hmap_t *keys;
    const char *last_root = NULL;
    size_t last_root_len = 0;
    unsigned last_root_nc = 0;
    size_t t;

    if (query_hmap_init(&c->keys[me], 1024) != 0) {
        c->oom[me] = 1;
        return;
    }
    keys = &c->keys[me];
    while ((t = atomic_fetch_add_explicit(&c->cursor, 1, memory_order_relaxed)) < c->ntasks) {
        query_emit_task_t *task = &c->tasks[t];
        query_result_t *qr = &c->res[task->buf];
        char *s = task->seg->data + task->begin;
        char *end = task->seg->data + task->end;
        size_t r = task->line_lo;
        size_t rcount = c->sum ? task->line_lo + task->lines : (size_t)-1;
        query_lrec_cur_t lcur;

        memset(&lcur, 0, sizeof(lcur));
        if (c->sum && query_lrec_cur_seek(qr, task->line_lo, &lcur) != 0) rcount = task->line_lo;
        if (c->sum && rcount > qr->lrec_count) rcount = qr->lrec_count;
        while (s < end && r < rcount) {
            char *nl = (char *)memchr(s, '\n', (size_t)(end - s));
            size_t len, root_len;
            unsigned root_nc, keep;

            if (!nl) break;
            len = (size_t)(nl - s);
            if (len == 0U) { /* cannot happen; keeps the walk total */
                s = nl + 1;
                r++;
                if (c->sum) query_lrec_cur_next(&lcur);
                continue;
            }
            if (last_root && query_path_is_under_len(last_root, last_root_len, s, len)) {
                root_len = last_root_len;
                root_nc = last_root_nc;
            } else if (c->dense) {
                root_nc = query_level_root_dense(&c->dense_root, s, len, &root_len);
                last_root = s;
                last_root_len = root_len;
                last_root_nc = root_nc;
            } else {
                root_nc = query_level_root_nc(&c->mset, s, len, &root_len);
                last_root = s;
                last_root_len = root_len;
                last_root_nc = root_nc;
            }
            keep = root_nc + g_query.list_level - 1U;
            /* Exact relative depth: a match shallower than `keep` belongs to an earlier
             * --level, and a longer path is truncated to the prefix at this depth. */
            if (query_path_has_ncomp(s, len, keep)) {
                /* keep == 0 means the level-1 root is "/" itself: it is the prefix. */
                size_t klen = keep == 0U ? root_len : query_path_prefix_len(s, len, keep);
                int ins = 0;
                query_hslot_t *e = query_hmap_slot(keys, s, klen, &ins);

                if (!e) {
                    c->oom[me] = 1;
                    return;
                }
                if (c->sum) {
                    query_sumbucket_t *b;
                    query_listrec_t *lr = query_lrec_cur_get(&lcur);
                    uint64_t size;
                    uint8_t type;

                    if (!lr) {
                        c->oom[me] = 1;
                        return;
                    }
                    size = lr->size;
                    type = lr->type;

                    if (lr->hl_idx != UINT32_MAX) {
                        const query_hardlink_t *hl = &qr->hl[lr->hl_idx];
                        int dup;

                        pthread_mutex_lock(&c->hl_lock);
                        dup = query_hl_seen(&c->hl_tab, &c->hl_mask, &c->hl_count, hl->inode,
                                            ((uint64_t)hl->dev_major << 32) | (uint64_t)hl->dev_minor);
                        pthread_mutex_unlock(&c->hl_lock);
                        if (dup) size = 0;
                    }
                    if (ins) {
                        if (c->nbuckets[me] == c->cap_buckets[me]) {
                            size_t nc = c->cap_buckets[me] ? c->cap_buckets[me] * 2U : 256U;
                            query_sumbucket_t *nb =
                                (query_sumbucket_t *)realloc(c->buckets[me], nc * sizeof(*nb));

                            if (!nb) {
                                c->oom[me] = 1;
                                return;
                            }
                            c->buckets[me] = nb;
                            c->cap_buckets[me] = nc;
                        }
                        e->pay = (uint32_t)c->nbuckets[me];
                        b = &c->buckets[me][c->nbuckets[me]++];
                        memset(b, 0, sizeof(*b));
                    } else {
                        b = &c->buckets[me][e->pay];
                    }
                    if (type == 'f')
                        b->f++;
                    else if (type == 'd')
                        b->d++;
                    else if (type == 'l')
                        b->l++;
                    else
                        b->o++;
                    b->b += size;
                }
            }
            s = nl + 1;
            r++;
            if (c->sum) query_lrec_cur_next(&lcur);
        }
    }
}

typedef struct {
    query_emit_ctx_t *c;
    unsigned me;
    int pass;
} query_emit_arg_t;

static void *query_emit_thread_main(void *vp) {
    const query_emit_arg_t *a = (const query_emit_arg_t *)vp;

    if (a->pass == 1)
        query_emit_pass1_run(a->c);
    else
        query_emit_pass2_run(a->c, a->me);
    return NULL;
}

/* Run one emit pass over T thread slots. When pthread_create falls short, the main
 * thread drains the shared cursor itself with an unused slot, so a thread-poor
 * environment degrades toward the serial walk instead of failing. */
static void query_emit_run_threads(query_emit_ctx_t *c, unsigned T, int pass) {
    pthread_t *th;
    query_emit_arg_t *args;
    unsigned started = 0, i;

    atomic_store_explicit(&c->cursor, 0, memory_order_relaxed);
    if (T < 2U) {
        if (pass == 1)
            query_emit_pass1_run(c);
        else
            query_emit_pass2_run(c, 0);
        return;
    }
    th = (pthread_t *)malloc(T * sizeof(*th));
    args = (query_emit_arg_t *)malloc(T * sizeof(*args));
    if (th && args) {
        for (i = 0; i < T; i++) {
            args[i].c = c;
            args[i].me = i;
            args[i].pass = pass;
            if (pthread_create(&th[i], NULL, query_emit_thread_main, &args[i]) != 0) break;
            started++;
        }
    }
    if (started < T) {
        if (pass == 1)
            query_emit_pass1_run(c);
        else
            query_emit_pass2_run(c, started);
    }
    for (i = 0; i < started; i++) pthread_join(th[i], NULL);
    free(th);
    free(args);
}

/* n = number of worker result buffers; threads = emit thread count (the requested,
 * unclamped-by-chunks value: a single huge buffer still slices into 4*T tasks). */
static void query_list_emit_level_hash(query_result_t *res, unsigned n, unsigned threads) {
    query_emit_ctx_t c;
    size_t total = 0, total_bytes = 0, i;
    unsigned T = threads;
    int oom = 0;

    memset(&c, 0, sizeof(c));
    c.res = res;
    c.sum = g_query.sum ? 1 : 0;
    c.dense = !query_list_sparse();
    for (i = 0; i < n; i++) {
        total += res[i].out_lines;
        total_bytes += res[i].out_len;
    }
    if (!total) return;
    if (T < 1U) T = 1U;

    /* Sparse listings need Pass 1 membership so Pass 2 can find the shallowest
     * listed ancestor. Dense listings skip the table: every ancestor is listed. */
    if (!c.dense && query_hmap_init(&c.mset, total) != 0) {
        fprintf(stderr, "ecrawl_query: --level: out of memory, listing suppressed\n");
        goto done;
    }

    /* Slice the worker buffers into ~4*T byte ranges at line boundaries, so the
     * atomic cursor stays balanced even when one worker buffered much more than
     * another. */
    {
        size_t slice = total_bytes / ((size_t)T * 4U) + 1U;
        size_t cap = (size_t)T * 8U + n + 16U;

        c.tasks = (query_emit_task_t *)malloc(cap * sizeof(*c.tasks));
        if (!c.tasks) {
            fprintf(stderr, "ecrawl_query: --level: out of memory, listing suppressed\n");
            goto done;
        }
        for (i = 0; i < n; i++) {
            query_out_seg_t *seg;

            for (seg = res[i].out_head; seg; seg = seg->next) {
                size_t begin = 0, blen = seg->used;

                while (begin < blen) {
                    size_t want = begin + slice, end;

                    if (c.ntasks == cap) {
                        size_t ncap = cap * 2U;
                        query_emit_task_t *nt = (query_emit_task_t *)realloc(c.tasks, ncap * sizeof(*nt));

                        if (!nt) {
                            fprintf(stderr, "ecrawl_query: --level: out of memory, listing suppressed\n");
                            goto done;
                        }
                        c.tasks = nt;
                        cap = ncap;
                    }
                    if (want >= blen) {
                        end = blen;
                    } else {
                        char *nl = (char *)memchr(seg->data + want, '\n', blen - want);

                        end = nl ? (size_t)(nl - seg->data) + 1U : blen;
                    }
                    c.tasks[c.ntasks].buf = (unsigned)i;
                    c.tasks[c.ntasks].seg = seg;
                    c.tasks[c.ntasks].begin = begin;
                    c.tasks[c.ntasks].end = end;
                    c.tasks[c.ntasks].lines =
                        c.dense ? query_count_lines(seg->data + begin, end - begin, &c.dense_root) : 0;
                    c.tasks[c.ntasks].line_lo = 0;
                    c.ntasks++;
                    begin = end;
                }
            }
        }
    }

    if (!c.dense) {
        /* Pass 1: every matching path into the shared membership set. Lines stay
         * '\n'-terminated: a reconstructed path can contain a NUL byte (corrupt name in
         * the capture), and the whole emit is length-aware so a NUL must never become a
         * line boundary. */
        query_emit_run_threads(&c, T, 1);
    }

    /* Pass 2 walks lrec[] alongside the lines, so each task needs the index of its
     * first line: prefix-sum the line counts (tasks are in buffer order). Dense
     * listings counted while slicing; sparse listings take the Pass 1 counts. */
    {
        size_t run = 0;
        unsigned cur_buf = 0;
        int first = 1;

        for (i = 0; i < c.ntasks; i++) {
            if (first || c.tasks[i].buf != cur_buf) {
                cur_buf = c.tasks[i].buf;
                run = 0;
                first = 0;
            }
            c.tasks[i].line_lo = run;
            run += c.tasks[i].lines;
        }
    }

    c.keys = (query_hmap_t *)calloc(T, sizeof(*c.keys));
    c.buckets = (query_sumbucket_t **)calloc(T, sizeof(*c.buckets));
    c.nbuckets = (size_t *)calloc(T, sizeof(*c.nbuckets));
    c.cap_buckets = (size_t *)calloc(T, sizeof(*c.cap_buckets));
    c.oom = (int *)calloc(T, sizeof(*c.oom));
    if (!c.keys || !c.buckets || !c.nbuckets || !c.cap_buckets || !c.oom) {
        fprintf(stderr, "ecrawl_query: --level: out of memory, listing suppressed\n");
        goto done;
    }
    if (pthread_mutex_init(&c.hl_lock, NULL) != 0) {
        fprintf(stderr, "ecrawl_query: --level: out of memory, listing suppressed\n");
        goto done;
    }
    query_emit_run_threads(&c, T, 2);
    pthread_mutex_destroy(&c.hl_lock);
    for (i = 0; i < T; i++)
        if (c.oom[i]) oom = 1;
    if (oom) goto done;

    /* Merge the per-thread key maps into slot 0; distinct prefixes are few next to
     * the line count, so this stays cheap even summed over threads. */
    for (i = 1; i < T; i++) {
        size_t k;

        if (!c.keys[i].tab) continue;
        for (k = 0; k <= c.keys[i].mask; k++) {
            const query_hslot_t *s = &c.keys[i].tab[k];
            query_hslot_t *g;
            int ins = 0;

            if (!s->s) continue;
            g = query_hmap_slot(&c.keys[0], s->s, s->len, &ins);
            if (!g) {
                oom = 1;
                goto done;
            }
            if (c.sum) {
                const query_sumbucket_t *sb = &c.buckets[i][s->pay];
                query_sumbucket_t *gb;

                if (ins) {
                    if (c.nbuckets[0] == c.cap_buckets[0]) {
                        size_t nc = c.cap_buckets[0] ? c.cap_buckets[0] * 2U : 256U;
                        query_sumbucket_t *nb =
                            (query_sumbucket_t *)realloc(c.buckets[0], nc * sizeof(*nb));

                        if (!nb) {
                            oom = 1;
                            goto done;
                        }
                        c.buckets[0] = nb;
                        c.cap_buckets[0] = nc;
                    }
                    g->pay = (uint32_t)c.nbuckets[0];
                    gb = &c.buckets[0][c.nbuckets[0]++];
                    *gb = *sb;
                } else {
                    gb = &c.buckets[0][g->pay];
                    gb->f += sb->f;
                    gb->d += sb->d;
                    gb->l += sb->l;
                    gb->o += sb->o;
                    gb->b += sb->b;
                }
            }
        }
    }


    /* Only the distinct prefixes are sorted, so row order matches the sorted walk. */
    if (c.keys[0].count) {
        query_hslot_t **ord = (query_hslot_t **)malloc(c.keys[0].count * sizeof(*ord));
        size_t m = 0;

        if (!ord) {
            oom = 1;
            goto done;
        }
        for (i = 0; i <= c.keys[0].mask; i++)
            if (c.keys[0].tab[i].s) ord[m++] = &c.keys[0].tab[i];
        qsort(ord, m, sizeof(*ord), cmp_hslot_path);
        if (c.sum) {
            char *buf = (char *)malloc(QUERY_SUMOUT_CAP);
            size_t blen = 0;
            int berr = 0;

            if (!buf) {
                oom = 1;
                free(ord);
                goto done;
            }
            for (i = 0; i < m; i++) {
                const query_sumbucket_t *b = &c.buckets[0][ord[i]->pay];

                query_sumout_row(buf, &blen, &berr, b->f, b->d, b->l, b->o, b->b, ord[i]->s, ord[i]->len);
            }
            query_sumout_flush(buf, &blen, &berr);
            if (berr) fprintf(stderr, "ecrawl_query: --sum: output failed, listing may be truncated\n");
            free(buf);
        } else {
            for (i = 0; i < m; i++) {
                /* A path holding a NUL byte prints the way the old %s walk printed it:
                 * cut at the NUL. Folding and aggregation used the full bytes. */
                const char *z = (const char *)memchr(ord[i]->s, '\0', ord[i]->len);
                size_t plen = z ? (size_t)(z - ord[i]->s) : ord[i]->len;

                if (query_list_write_line(ord[i]->s, plen) != 0) break;
            }
        }
        free(ord);
    }

done:
    if (oom) fprintf(stderr, "ecrawl_query: --level: out of memory, listing may be suppressed\n");
    free(c.hl_tab);
    if (c.keys)
        for (i = 0; i < T; i++) free(c.keys[i].tab);
    if (c.buckets)
        for (i = 0; i < T; i++) free(c.buckets[i]);
    free(c.keys);
    free(c.buckets);
    free(c.nbuckets);
    free(c.cap_buckets);
    free(c.oom);
    free(c.tasks);
    free(c.mset.tab);
}

/* One buffered match, gathered across workers for the --sum sort. */
typedef struct {
    char *path;
    uint64_t size;   /* zeroed for hardlink duplicates after the sort */
    uint32_t hl_idx; /* into res[worker].hl, or UINT32_MAX */
    uint32_t worker;
    uint8_t type;
} query_sumrec_t;

/* A directory row in flight: its counts are not final until its children have been seen. */
typedef struct {
    const char *path;
    uint64_t f, d, l, o, b;
} query_sumframe_t;

static int cmp_sumrec(const void *pa, const void *pb) {
    return strcmp(((const query_sumrec_t *)pa)->path, ((const query_sumrec_t *)pb)->path);
}

/*
 * Sort slices are pulled off an atomic cursor so a slow qsort (data-dependent)
 * idles nobody: 4xT slices keep every emit thread busy until the last one.
 */
typedef struct {
    query_sumrec_t *a;
    const size_t *lo, *hi;
    size_t nparts;
    _Atomic size_t cursor;
} query_sumsort_ctx_t;

static void *query_sumsort_thread(void *vp) {
    query_sumsort_ctx_t *c = (query_sumsort_ctx_t *)vp;
    size_t t;

    while ((t = atomic_fetch_add_explicit(&c->cursor, 1, memory_order_relaxed)) < c->nparts) {
        if (c->hi[t] > c->lo[t]) qsort(c->a + c->lo[t], c->hi[t] - c->lo[t], sizeof(*c->a), cmp_sumrec);
    }
    return NULL;
}

/* Min-heap of partition indices, keyed by each partition's current head record. */
static int query_sum_part_less(const query_sumrec_t *a, const size_t *cur, size_t x, size_t y) {
    return cmp_sumrec(&a[cur[x]], &a[cur[y]]) < 0;
}

static void query_sum_heap_down(const query_sumrec_t *a, const size_t *cur, size_t *hp, size_t hn, size_t i) {
    for (;;) {
        size_t l = 2U * i + 1U, r = l + 1U, best = i, tmp;

        if (l < hn && query_sum_part_less(a, cur, hp[l], hp[best])) best = l;
        if (r < hn && query_sum_part_less(a, cur, hp[r], hp[best])) best = r;
        if (best == i) return;
        tmp = hp[i];
        hp[i] = hp[best];
        hp[best] = tmp;
        i = best;
    }
}

typedef struct {
    query_sumframe_t *st;
    size_t st_n, st_cap;
    char *buf;
    size_t *blen;
    int *berr;
} query_sumwalk_t;

static void query_sumwalk_feed(query_sumwalk_t *w, const query_sumrec_t *r) {
    uint64_t sf = 0, sd = 0, sl = 0, so = 0;

    if (*w->berr) return;
    if (r->type == 'f')
        sf = 1;
    else if (r->type == 'd')
        sd = 1;
    else if (r->type == 'l')
        sl = 1;
    else
        so = 1;
    while (w->st_n && !query_path_is_under(w->st[w->st_n - 1U].path, r->path)) {
        query_sumframe_t e = w->st[--w->st_n];

        query_sumout_row(w->buf, w->blen, w->berr, e.f, e.d, e.l, e.o, e.b, e.path, strlen(e.path));
        if (w->st_n) {
            w->st[w->st_n - 1U].f += e.f;
            w->st[w->st_n - 1U].d += e.d;
            w->st[w->st_n - 1U].l += e.l;
            w->st[w->st_n - 1U].o += e.o;
            w->st[w->st_n - 1U].b += e.b;
        }
    }
    if (r->type == 'd') {
        if (w->st_n == w->st_cap) {
            size_t nc = w->st_cap ? w->st_cap * 2U : 64U;
            query_sumframe_t *np = (query_sumframe_t *)realloc(w->st, nc * sizeof(*np));

            if (!np) {
                *w->berr = 1;
                return;
            }
            w->st = np;
            w->st_cap = nc;
        }
        w->st[w->st_n].path = r->path;
        w->st[w->st_n].f = sf;
        w->st[w->st_n].d = sd;
        w->st[w->st_n].l = sl;
        w->st[w->st_n].o = so;
        w->st[w->st_n].b = r->size;
        w->st_n++;
    } else {
        if (w->st_n) {
            w->st[w->st_n - 1U].f += sf;
            w->st[w->st_n - 1U].d += sd;
            w->st[w->st_n - 1U].l += sl;
            w->st[w->st_n - 1U].o += so;
            w->st[w->st_n - 1U].b += r->size;
        }
        query_sumout_row(w->buf, w->blen, w->berr, sf, sd, sl, so, r->size, r->path, strlen(r->path));
    }
}

static void query_sumwalk_finish(query_sumwalk_t *w) {
    while (w->st_n) {
        query_sumframe_t e = w->st[--w->st_n];

        query_sumout_row(w->buf, w->blen, w->berr, e.f, e.d, e.l, e.o, e.b, e.path, strlen(e.path));
        if (w->st_n) {
            w->st[w->st_n - 1U].f += e.f;
            w->st[w->st_n - 1U].d += e.d;
            w->st[w->st_n - 1U].l += e.l;
            w->st[w->st_n - 1U].o += e.o;
            w->st[w->st_n - 1U].b += e.b;
        }
    }
}

/*
 * --list --sum without --level: every row is files,dirs,symlinks,other,bytes,path over the
 * matching records at or under path; a directory counts itself, so rows roll up to the
 * stderr totals. Every matching path prints, a dir's row after its children (du order),
 * because its counts are not final until they have been seen. (--level --sum is served by
 * query_list_emit_level_hash, which needs no global sort.)
 *
 * Sort is the wall: partition the gathered records, qsort each slice on emit_threads,
 * then k-way merge in lex order into the du stack walk. Hardlink bytes stay on the
 * lexicographically first path (same as the old serial qsort + query_sum_hardlink_zero).
 */
static void query_list_emit_sum(query_result_t *res, unsigned n, unsigned threads) {
    size_t total = 0, k = 0, i;
    query_sumrec_t *a;
    char *buf;
    size_t blen = 0;
    int berr = 0;
    unsigned T = threads;
    query_sumwalk_t walk;
    query_hlseen_t *hl_tab = NULL;
    size_t hl_mask = 0, hl_count = 0;
    size_t nparts, p;
    size_t *plo = NULL, *pcur = NULL, *phi = NULL;
    size_t *heap = NULL;
    pthread_t *th = NULL;

    for (i = 0; i < n; i++) total += res[i].lrec_count;
    if (!total) return;
    a = (query_sumrec_t *)malloc(total * sizeof(*a));
    buf = (char *)malloc(QUERY_SUMOUT_CAP);
    if (!a || !buf) {
        fprintf(stderr, "ecrawl_query: --sum: out of memory, listing suppressed\n");
        free(a);
        free(buf);
        return;
    }
    for (i = 0; i < n; i++) {
        query_out_seg_t *os;
        query_lrec_cur_t lcur;

        if (query_lrec_cur_seek(&res[i], 0, &lcur) != 0 && res[i].lrec_count) {
            fprintf(stderr, "ecrawl_query: --sum: listing/lrec mismatch, listing suppressed\n");
            goto out;
        }
        for (os = res[i].out_head; os; os = os->next) {
            char *s = os->data;
            char *end = os->data + os->used;

            while (s < end) {
                char *nl = (char *)memchr(s, '\n', (size_t)(end - s));
                query_listrec_t *lr;

                if (!nl) break;
                *nl = '\0';
                lr = query_lrec_cur_get(&lcur);
                if (!lr) break;
                a[k].path = s;
                a[k].size = lr->size;
                a[k].type = lr->type;
                a[k].hl_idx = lr->hl_idx;
                a[k].worker = (uint32_t)i;
                k++;
                query_lrec_cur_next(&lcur);
                s = nl + 1;
            }
        }
    }
    if (!k) {
        free(a);
        free(buf);
        return;
    }
    if (T < 1U) T = 1U;
    nparts = (size_t)T * 4U;
    if (nparts > k) nparts = k;

    plo = (size_t *)malloc(nparts * sizeof(*plo));
    pcur = (size_t *)malloc(nparts * sizeof(*pcur));
    phi = (size_t *)malloc(nparts * sizeof(*phi));
    if (!plo || !pcur || !phi) {
        fprintf(stderr, "ecrawl_query: --sum: out of memory, listing suppressed\n");
        goto out;
    }
    {
        size_t part_sz = k / nparts;

        for (p = 0; p < nparts; p++) {
            plo[p] = p * part_sz;
            phi[p] = (p + 1U == nparts) ? k : (p + 1U) * part_sz;
            pcur[p] = plo[p];
        }
    }

    if (nparts == 1U) {
        qsort(a, k, sizeof(*a), cmp_sumrec);
    } else {
        query_sumsort_ctx_t sctx;
        unsigned started = 0, ti;

        sctx.a = a;
        sctx.lo = plo;
        sctx.hi = phi;
        sctx.nparts = nparts;
        atomic_init(&sctx.cursor, 0);
        th = (pthread_t *)malloc(T * sizeof(*th));
        if (th) {
            for (ti = 0; ti < T; ti++) {
                if (pthread_create(&th[ti], NULL, query_sumsort_thread, &sctx) != 0) break;
                started++;
            }
        }
        /* The main thread sorts alongside the helpers, or alone when none started. */
        query_sumsort_thread(&sctx);
        for (ti = 0; ti < started; ti++) pthread_join(th[ti], NULL);
    }

    memset(&walk, 0, sizeof(walk));
    walk.buf = buf;
    walk.blen = &blen;
    walk.berr = &berr;

    heap = (size_t *)malloc(nparts * sizeof(*heap));
    if (!heap) {
        fprintf(stderr, "ecrawl_query: --sum: out of memory, listing suppressed\n");
        goto out_walk;
    }
    {
        size_t hn = 0;

        for (p = 0; p < nparts; p++)
            if (pcur[p] < phi[p]) heap[hn++] = p;
        for (p = hn / 2U; p-- > 0U;) query_sum_heap_down(a, pcur, heap, hn, p);
        while (hn) {
            size_t best = heap[0];
            query_sumrec_t rec = a[pcur[best]++];
            const query_hardlink_t *hl;

            if (rec.hl_idx != UINT32_MAX) {
                hl = &res[rec.worker].hl[rec.hl_idx];
                if (query_hl_seen(&hl_tab, &hl_mask, &hl_count, hl->inode,
                                  ((uint64_t)hl->dev_major << 32) | (uint64_t)hl->dev_minor))
                    rec.size = 0;
            }
            query_sumwalk_feed(&walk, &rec);
            if (berr) break;
            /* The root's key only grew, so a sift-down restores the heap either way. */
            if (pcur[best] >= phi[best]) heap[0] = heap[--hn];
            if (hn) query_sum_heap_down(a, pcur, heap, hn, 0);
        }
    }
    query_sumwalk_finish(&walk);
    query_sumout_flush(buf, &blen, &berr);
    if (berr) fprintf(stderr, "ecrawl_query: --sum: output failed, listing may be truncated\n");

out_walk:
    free(walk.st);

out:
    free(hl_tab);
    free(heap);
    free(th);
    free(plo);
    free(pcur);
    free(phi);
    free(buf);
    free(a);
}

/*
 * Per-worker cache of reconstructed parent directory paths, keyed by dir_id.
 *
 * Building a path walks the parent chain and copies every component. Row groups
 * are sorted by (parent_dir_id, name), so a last-parent slot hits on sibling
 * runs; the 8192-slot hash covers chunk jumps and group boundaries. Direct-mapped
 * and fixed size: a miss costs the walk that would have happened anyway. The
 * arena is 1 MiB; wrapping bumps a generation so stale slots miss without
 * walking the table. last[] survives the wrap, and memory per worker stays flat.
 */
#define QUERY_PATH_CACHE_SLOTS 8192U /* power of two */
#define QUERY_PATH_CACHE_ARENA (1024U * 1024U)

typedef struct {
    uint64_t key; /* dir_id + 1; 0 = empty slot */
    uint32_t off;
    uint32_t len;
    uint32_t gen;
} query_path_cache_ent_t;

typedef struct {
    query_path_cache_ent_t *ent;
    char *arena;
    size_t arena_len;
    uint32_t gen; /* bumped on wrap; slots with a different gen are empty */
    uint64_t last_id; /* 0 = empty; else dir_id of last[] */
    size_t last_len;
    char last[PATH_MAX];
    uint64_t hits_last;
    uint64_t hits_hash;
    uint64_t misses;
} query_path_cache_t;

static int query_path_cache_init(query_path_cache_t *c) {
    c->ent = (query_path_cache_ent_t *)calloc(QUERY_PATH_CACHE_SLOTS, sizeof(*c->ent));
    c->arena = (char *)malloc(QUERY_PATH_CACHE_ARENA);
    c->arena_len = 0;
    c->gen = 1;
    c->last_id = 0;
    c->last_len = 0;
    if (!c->ent || !c->arena) {
        free(c->ent);
        free(c->arena);
        c->ent = NULL;
        c->arena = NULL;
        return -1;
    }
    return 0;
}

static void query_path_cache_free(query_path_cache_t *c) {
    free(c->ent);
    free(c->arena);
    c->ent = NULL;
    c->arena = NULL;
    c->arena_len = 0;
    c->last_id = 0;
}

/* Catalogs are per shard, so a dir_id means something different in each: start over. */
static void query_path_cache_reset(query_path_cache_t *c) {
    c->arena_len = 0;
    c->last_id = 0;
    c->last_len = 0;
    /* Bump gen rather than memset 8192 slots: lookups see a different gen and miss. */
    if (c->gen == UINT32_MAX) {
        if (c->ent) memset(c->ent, 0, QUERY_PATH_CACHE_SLOTS * sizeof(*c->ent));
        c->gen = 1;
    } else {
        c->gen++;
    }
}

static inline size_t query_path_cache_slot(uint64_t dir_id) {
    return (size_t)((dir_id * 0x9E3779B97F4A7C15ULL) >> 51) & (QUERY_PATH_CACHE_SLOTS - 1U);
}

static void query_path_cache_store(query_path_cache_t *c, uint64_t dir_id, const char *src, size_t len) {
    query_path_cache_ent_t *e;

    if (!c->ent || !c->arena) return;
    if (len > QUERY_PATH_CACHE_ARENA) return;
    if (c->arena_len + len > QUERY_PATH_CACHE_ARENA) {
        /* Drop the hash (not last-parent): a generation bump makes every slot
         * miss without walking 8192 entries per store. */
        c->arena_len = 0;
        if (c->gen == UINT32_MAX) {
            memset(c->ent, 0, QUERY_PATH_CACHE_SLOTS * sizeof(*c->ent));
            c->gen = 1;
        } else {
            c->gen++;
        }
    }
    e = &c->ent[query_path_cache_slot(dir_id)];
    if (len) memcpy(c->arena + c->arena_len, src, len);
    e->key = dir_id + 1ULL;
    e->off = (uint32_t)c->arena_len;
    e->len = (uint32_t)len;
    e->gen = c->gen;
    c->arena_len += len;
}

/*
 * Parent path for dir_id, from last-parent, the hash cache, or the catalog.
 * Returns a pointer into c->last, valid until the next call on this cache.
 */
static const char *query_parent_path(query_path_cache_t *c, const crawl_bin_catalog_t *cat, uint64_t dir_id,
                                     size_t *len_out, char *scratch, size_t scratch_sz) {
    query_path_cache_ent_t *e;
    size_t len = 0;

    if (c->last_id == dir_id) {
        *len_out = c->last_len;
        c->hits_last++;
        return c->last;
    }

    if (c->ent) {
        e = &c->ent[query_path_cache_slot(dir_id)];
        if (e->key == dir_id + 1ULL && e->gen == c->gen && (size_t)e->len < sizeof(c->last)) {
            len = (size_t)e->len;
            if (len) memcpy(c->last, c->arena + e->off, len);
            c->last[len] = '\0';
            c->last_id = dir_id;
            c->last_len = len;
            c->hits_hash++;
            *len_out = len;
            return c->last;
        }
    }

    if (crawl_bin_catalog_dir_path_len(cat, dir_id, scratch, scratch_sz, &len) != 0) return NULL;
    c->misses++;
    if (len >= sizeof(c->last)) return NULL;
    if (len) memcpy(c->last, scratch, len);
    c->last[len] = '\0';
    c->last_id = dir_id;
    c->last_len = len;
    query_path_cache_store(c, dir_id, scratch, len);
    *len_out = len;
    return c->last;
}

/* Takes the four values rather than a bin_record_hdr_t: the caller reads columns and no
 * longer has a header to point at, and these are the only fields this ever wanted. */
static int query_hardlink_defer(query_result_t *qr, uint32_t dev_major, uint32_t dev_minor, uint64_t inode,
                                uint64_t size) {
    if (qr->hl_count == qr->hl_cap) {
        size_t nc = qr->hl_cap ? qr->hl_cap * 2U : 1024U;
        query_hardlink_t *np = (query_hardlink_t *)realloc(qr->hl, nc * sizeof(*np));

        if (!np) return -1;
        qr->hl = np;
        qr->hl_cap = nc;
    }
    qr->hl[qr->hl_count].dev_major = dev_major;
    qr->hl[qr->hl_count].dev_minor = dev_minor;
    qr->hl[qr->hl_count].inode = inode;
    qr->hl[qr->hl_count].size = size;
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

/*
 * Membership scratch for a sidecar-backed subtree scan: one row-read walk per
 * worker plus a last-parent memo. Records arrive sorted by parent_dir_id, so
 * the memo absorbs sibling runs and the walk's cached catalog chunk absorbs the
 * rest -- a catalog chunk is decoded at most once per (task, shard) pair.
 */
typedef struct {
    crawl_dirx_walk_t *walk;
    uint64_t pid;
    int in;
    int live;
} query_memb_t;

/* dir_id -> DFS position via the sidecar's chunk table, then the range test.
 * Returns -1 when the sidecar cannot read a row it promised at open. */
static int query_sidecar_contains(analyze_pool_t *p, size_t fi, const shard_subtree_t *sub, query_memb_t *mb,
                                  uint64_t pid, int *out) {
    bin_dir_catalog_entry_t ent;
    unsigned char namebuf[256];

    if (mb->live && mb->pid == pid) {
        *out = mb->in;
        return 0;
    }
    if (crawl_dirx_read_row(&p->sidecar.dirs[fi], mb->walk, pid, CRAWL_CAT_SUBTREE, &ent, namebuf,
                            sizeof(namebuf), NULL, NULL) != 0)
        return -1;
    *out = query_subtree_dfs_in(sub, ent.dfs_index);
    mb->pid = pid;
    mb->in = *out;
    mb->live = 1;
    return 0;
}

static int query_scan_fp_until(FILE *fp, uint64_t start_off, uint64_t scan_end_exclusive,
                               query_cat_lazy_t *lz, const shard_subtree_t *sub, query_memb_t *mb,
                               crawl_bin_block_reader_t *br, query_path_cache_t *pcache,
                               char *fullpath_buf, size_t fullpath_sz, query_result_t *qr,
                               uint64_t *nrec_out) {
    uint64_t nrec = 0;
    crawl_bin_chunk_stdio_t bio;
    char parent_path[PATH_MAX];
    uint64_t last_list_pid = 0;
    size_t last_list_plen = 0;
    int prefix_live = 0;

    if (nrec_out) *nrec_out = 0;
    bio.fopen = NULL;
    bio.fread = fread_unlocked;
    bio.fclose = NULL;
    if (crawl_bin_block_reader_reinit(br, &bio, fp, start_off, scan_end_exclusive) != 0) return -1;
    /*
     * The query predicate reads the parent, the type and the size; the byte
     * total additionally needs nlink plus (dev, inode) to dedup hardlinks the
     * way du does. Timestamps are never consulted here. uid/gid/mode only when
     * --uid / --gid / --perm ask for them. Names are only needed to print
     * paths, and to recognise the subtree's own directory record, whose parent
     * lies outside the subtree by construction.
     */
    {
        uint32_t proj = CRAWL_COL_BIT(CRAWL_COL_PARENT_DIR_ID) | CRAWL_COL_BIT(CRAWL_COL_TYPE) |
                        CRAWL_COL_BIT(CRAWL_COL_SIZE) | CRAWL_COL_BIT(CRAWL_COL_NLINK) |
                        CRAWL_COL_BIT(CRAWL_COL_INODE) | CRAWL_COL_BIT(CRAWL_COL_DEV_MAJOR) |
                        CRAWL_COL_BIT(CRAWL_COL_DEV_MINOR);

        if (query_needs_names()) proj |= CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES);
        if (g_query.have_gid) proj |= CRAWL_COL_BIT(CRAWL_COL_GID);
        if (g_query.have_uid) proj |= CRAWL_COL_BIT(CRAWL_COL_UID);
        if (g_query.perm_mode) proj |= CRAWL_COL_BIT(CRAWL_COL_MODE);
        (void)crawl_bin_block_reader_set_projection(br, proj);
        /* The dedup triple is consulted only under `nlink > 1` below, so row groups whose NLINK zone
         * map tops out at 1 need none of it -- on a tree without hardlinks, that is every group and
         * three of the seven columns this scan would otherwise decompress. */
        (void)crawl_bin_block_reader_set_hardlink_columns(br, CRAWL_COL_BIT(CRAWL_COL_INODE) |
                                                                  CRAWL_COL_BIT(CRAWL_COL_DEV_MAJOR) |
                                                                  CRAWL_COL_BIT(CRAWL_COL_DEV_MINOR));
    }
    if (g_query.block_skip) {
        (void)crawl_bin_block_reader_set_filter(br, g_query.have_size_gt, g_query.size_gt, g_query.type_filter);
        /* gid is RLE'd and near-constant within a uid shard, so its zone map is
         * usually a single value: --gid prunes whole groups. uid is typically
         * CONST inside a shard, so --uid either keeps the group or skips it.
         * --perm cannot prune, because a bit test is not a range. */
        if (g_query.have_gid)
            (void)crawl_bin_block_reader_add_range(br, CRAWL_COL_GID, g_query.gid, g_query.gid);
        if (g_query.have_uid)
            (void)crawl_bin_block_reader_add_range(br, CRAWL_COL_UID, g_query.uid, g_query.uid);
        /* Records under the subtree all hang off dir_ids in one hull, so groups whose
         * parent_dir_id zone map misses it cannot match. */
        if (sub && sub->have_hull && !sub->whole)
            (void)crawl_bin_block_reader_add_range(br, CRAWL_COL_PARENT_DIR_ID, sub->pid_lo, sub->pid_hi);
    }

    /*
     * Column-at-a-time; see the note in analyze_scan_fp_until. The row shim rebuilt all
     * fifteen fields of a bin_record_hdr_t per record to serve the seven to ten this scan
     * reads, and it sits on the latency path of every query answered by record_scan.
     *
     * The hardlink triple is the case that makes the NULL-column handling matter rather
     * than merely tidy: set_hardlink_columns() clears INODE / DEV_MAJOR / DEV_MINOR from a
     * group whose NLINK zone map tops out at 1, so those pointers are genuinely NULL on a
     * capture without hardlinks -- which is most of them. They are only read under
     * nlink > 1, which such a group cannot produce, so the zero default is never the value
     * that gets used.
     */
    for (;;) {
        uint32_t ngrp = 0;
        const uint64_t *c_pid, *c_type, *c_nlen, *c_size, *c_gid, *c_uid, *c_mode, *c_nlink;
        const uint64_t *c_ino, *c_devmaj, *c_devmin;
        uint32_t k;
        int got = crawl_bin_block_reader_next_group(br, &ngrp);

        if (got == 0) break;
        if (got < 0) return -1;

        c_pid = crawl_bin_block_reader_column(br, CRAWL_COL_PARENT_DIR_ID);
        c_type = crawl_bin_block_reader_column(br, CRAWL_COL_TYPE);
        c_nlen = crawl_bin_block_reader_column(br, CRAWL_COL_NAME_LEN);
        c_size = crawl_bin_block_reader_column(br, CRAWL_COL_SIZE);
        c_gid = crawl_bin_block_reader_column(br, CRAWL_COL_GID);
        c_uid = crawl_bin_block_reader_column(br, CRAWL_COL_UID);
        c_mode = crawl_bin_block_reader_column(br, CRAWL_COL_MODE);
        c_nlink = crawl_bin_block_reader_column(br, CRAWL_COL_NLINK);
        c_ino = crawl_bin_block_reader_column(br, CRAWL_COL_INODE);
        c_devmaj = crawl_bin_block_reader_column(br, CRAWL_COL_DEV_MAJOR);
        c_devmin = crawl_bin_block_reader_column(br, CRAWL_COL_DEV_MINOR);

        for (k = 0; k < ngrp; k++) {
            const unsigned char *rec_name;
            uint64_t pid = c_pid ? c_pid[k] : 0ULL;
            uint64_t rsize = c_size ? c_size[k] : 0ULL;
            uint64_t rnlink = c_nlink ? c_nlink[k] : 0ULL;
            uint64_t rgid = c_gid ? c_gid[k] : 0ULL;
            uint64_t ruid = c_uid ? c_uid[k] : 0ULL;
            uint32_t rmode = c_mode ? (uint32_t)c_mode[k] : 0U;
            uint16_t nlen = c_nlen ? (uint16_t)c_nlen[k] : 0U;
            uint8_t rtype = c_type ? (uint8_t)c_type[k] : 0U;
            int in_scope;

            /* NULL whenever the name bytes are not projected, exactly as the shim left it.
             * The length comes from the NAME_LEN column, as it did there. */
            rec_name = crawl_bin_block_reader_name(br, k, NULL);

            nrec++;
            if (pid == 0ULL) return -1;

            in_scope = 1;
            if (g_query.subtree && !(sub && sub->whole)) {
                if (sub && sub->sidecar_mem) {
                    if (query_sidecar_contains(lz->pool, lz->fi, sub, mb, pid, &in_scope) != 0) return -1;
                } else {
                    in_scope = subtree_contains(sub, pid);
                }
                /*
                 * The subtree's own directory record hangs off its parent, so the
                 * membership array never claims it, yet du counts it. Namesake
                 * check first: comparing one component is far cheaper than
                 * rebuilding a path for every directory in the capture. When names
                 * are not projected the catalog supplies that record instead (see
                 * query_needs_names), so there is nothing to recognise here.
                 */
                if (!in_scope && rec_name && rtype == (uint8_t)'d' &&
                    (size_t)nlen == g_query.sub_base_len &&
                    memcmp(rec_name, g_query.sub_base, g_query.sub_base_len) == 0) {
                    if (sub && sub->sidecar_mem) {
                        /* pid's path is the subtree's parent exactly when the
                         * sidecar resolved it into the scope's parent set. */
                        size_t pi;

                        for (pi = 0; pi < sub->nparents; pi++)
                            if (sub->parent_ids[pi] == pid) {
                                in_scope = 1;
                                break;
                            }
                    } else {
                        const crawl_bin_catalog_t *cat = query_cat_lazy_get(lz);

                        if (!cat) return -1;
                        {
                            size_t plen = 0;

                            prefix_live = 0;
                            if (crawl_bin_catalog_entry_path_len(cat, pid, (const char *)rec_name, (size_t)nlen,
                                                                 fullpath_buf, fullpath_sz, &plen) == 0 &&
                                plen == g_query.subtree_len && memcmp(fullpath_buf, g_query.subtree, plen) == 0)
                                in_scope = 1;
                        }
                    }
                }
                if (!in_scope) continue;
            }

            if (g_query.have_size_gt && rsize <= g_query.size_gt) continue;
            if (g_query.type_filter && rtype != (uint8_t)g_query.type_filter) continue;
            if (g_query.have_gid && rgid != g_query.gid) continue;
            if (g_query.have_uid && ruid != (uint64_t)g_query.uid) continue;
            if (g_query.perm_mode && !query_perm_match(rmode)) continue;

            qr->entries++;
            if (rtype == (uint8_t)'f')
                qr->files++;
            else if (rtype == (uint8_t)'d')
                qr->dirs++;
            else if (rtype == (uint8_t)'l')
                qr->symlinks++;
            else
                qr->other++;

            /* du credits a multiply-linked inode once; which visit wins is decided
             * globally, after the workers join, because links can span shards. */
            uint32_t hl_idx = UINT32_MAX;
            if (rtype != (uint8_t)'d' && rnlink > 1ULL) {
                uint32_t devmaj = c_devmaj ? (uint32_t)c_devmaj[k] : 0U;
                uint32_t devmin = c_devmin ? (uint32_t)c_devmin[k] : 0U;

                if (query_hardlink_defer(qr, devmaj, devmin, c_ino ? c_ino[k] : 0ULL, rsize) != 0) {
                    qr->oom = 1;
                    return -1;
                }
                hl_idx = (uint32_t)(qr->hl_count - 1U);
            } else {
                qr->bytes += rsize;
            }

            if (g_query.list_paths) {
                size_t fullpath_len = 0;
                size_t plen = 0;

                if (prefix_live && last_list_pid == pid) {
                    /* Same parent as the previous listed record: only the leaf changes. */
                    plen = last_list_plen;
                    pcache->hits_last++;
                    if (plen == 0U) {
                        if ((size_t)nlen + 2U > fullpath_sz) return -1;
                        if (nlen > 0U) memcpy(fullpath_buf + 1, rec_name, nlen);
                        fullpath_len = 1U + (size_t)nlen;
                    } else {
                        if (plen + 1U + (size_t)nlen + 1U > fullpath_sz) return -1;
                        if (nlen > 0U) memcpy(fullpath_buf + plen + 1U, rec_name, nlen);
                        fullpath_len = plen + 1U + (size_t)nlen;
                    }
                } else {
                    const char *ppath;
                    const crawl_bin_catalog_t *cat = query_cat_lazy_get(lz);

                    if (!cat) return -1;
                    ppath = query_parent_path(pcache, cat, pid, &plen, parent_path, sizeof(parent_path));
                    if (!ppath) return -1;
                    if (plen == 0U) {
                        if ((size_t)nlen + 2U > fullpath_sz) return -1;
                        fullpath_buf[0] = '/';
                        if (nlen > 0U) memcpy(fullpath_buf + 1, rec_name, nlen);
                        fullpath_len = 1U + (size_t)nlen;
                    } else {
                        if (plen + 1U + (size_t)nlen + 1U > fullpath_sz) return -1;
                        memcpy(fullpath_buf, ppath, plen);
                        fullpath_buf[plen] = '/';
                        if (nlen > 0U) memcpy(fullpath_buf + plen + 1U, rec_name, nlen);
                        fullpath_len = plen + 1U + (size_t)nlen;
                    }
                    last_list_pid = pid;
                    last_list_plen = plen;
                }
                prefix_live = 1;
                fullpath_buf[fullpath_len] = '\0';
                if (query_out_append(qr, fullpath_buf, fullpath_len) != 0) {
                    qr->oom = 1;
                    return -1;
                }
                if (g_query.sum && query_lrec_append(qr, rsize, rtype, hl_idx) != 0) {
                    qr->oom = 1;
                    return -1;
                }
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

static void analyze_shard_cat_sync_init(analyze_pool_t *p) {
    pthread_mutex_init(&p->shard_cat_lock, NULL);
    pthread_cond_init(&p->shard_cat_cond, NULL);
}

static void analyze_shard_cat_sync_destroy(analyze_pool_t *p) {
    pthread_cond_destroy(&p->shard_cat_cond);
    pthread_mutex_destroy(&p->shard_cat_lock);
}

/*
 * Return shard fi's catalog, loading it once on first use (read-only thereafter,
 * shared by every chunk of the shard). The load itself runs outside shard_cat_lock:
 * the slot is claimed by flipping its state to SHARD_CAT_LOADING, which both keeps a
 * second thread from building the same catalog and lets catalogs for *different*
 * shards load concurrently. Holding the lock across the open + nine array reads +
 * per-directory strdups serialized the whole pool on many-shard captures — a
 * 1019-shard capture ran at 40% CPU with 32 threads, nearly all of it one thread
 * loading a catalog while the rest queued behind the mutex. Returns NULL if the
 * catalog is missing or fails to load.
 */
static const crawl_bin_catalog_t *analyze_get_shard_catalog(analyze_pool_t *p, size_t fi, const char *full_path) {
    const crawl_bin_catalog_t *res = NULL;

    pthread_mutex_lock(&p->shard_cat_lock);
    while (p->shard_cat_state[fi] == SHARD_CAT_LOADING)
        pthread_cond_wait(&p->shard_cat_cond, &p->shard_cat_lock);
    if (p->shard_cat_state[fi] == SHARD_CAT_UNLOADED) {
        FILE *fp;
        unsigned char st = SHARD_CAT_FAILED; /* failed unless proven otherwise */

        p->shard_cat_state[fi] = SHARD_CAT_LOADING;
        pthread_mutex_unlock(&p->shard_cat_lock);

        fp = crawl_fpcache_fopen(full_path, "rb");
        if (fp) {
            bin_file_header_t fh;
            uint64_t fsz = p->shard_file_sizes[fi];

            if (fread(&fh, sizeof(fh), 1, fp) == 1 && crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION) &&
                fh.catalog_offset != 0ULL && fh.catalog_offset <= fsz) {
                /* Tree fields always; the DFS permutation only when subtree_build below will read it.
                 * CRAWL_CAT_SUBTREE is ten more arrays -- nine uint64 plus self_present -- so it costs
                 * 73 bytes per directory to allocate, read and fill, and a shape run never looks at any
                 * of them. p->shard_sub is non-NULL exactly when --subtree asked for a real subtree.
                 * The imm_child_* rollups are only read by the rollup fast path, which loads its own. */
                unsigned cat_fields = p->shard_sub ? CRAWL_CAT_SUBTREE : 0U;

                if (crawl_bin_catalog_load_sel(fp, fh.catalog_offset, fsz, cat_fields, &p->shard_cat[fi]) == 0) {
                    st = SHARD_CAT_READY;
                    atomic_fetch_add_explicit(&g_catalogs_loaded, 1ULL, memory_order_relaxed);
                }
                /* The loader already frees + re-inits the struct on failure. */
            }
            /* The counts share the catalog's lifetime and its dir_id space. Unlike the memo they
             * replace, the scan has nowhere else to put a count, so failing to allocate them fails
             * the shard rather than merely slowing it down. */
            if (st == SHARD_CAT_READY && p->shard_cnt) {
                p->shard_cnt[fi] = (analyze_dir_counts_t *)calloc((size_t)p->shard_cat[fi].max_dir_id + 1U,
                                                                  sizeof(analyze_dir_counts_t));
                if (!p->shard_cnt[fi]) {
                    crawl_bin_catalog_free(&p->shard_cat[fi]);
                    st = SHARD_CAT_FAILED;
                }
            }
            /* Subtree membership belongs to the catalog: same lifetime, built once.
             * A sidecar-backed shard already has its membership from the scope. */
            if (st == SHARD_CAT_READY && p->shard_sub && !p->shard_sub[fi].sidecar_mem) {
                if (subtree_build(&p->shard_cat[fi], &p->shard_sub[fi], 1) != 0) {
                    crawl_bin_catalog_free(&p->shard_cat[fi]);
                    st = SHARD_CAT_FAILED;
                } else {
                    query_warn_dup_subtree(p->shard_sub[fi].nroots, full_path);
                    query_note_subtree_self(&p->shard_cat[fi], &p->shard_sub[fi]);
                }
            }
            crawl_fpcache_fclose(fp);
        }

        pthread_mutex_lock(&p->shard_cat_lock);
        p->shard_cat_state[fi] = st;
        pthread_cond_broadcast(&p->shard_cat_cond);
    }
    if (p->shard_cat_state[fi] == SHARD_CAT_READY) res = &p->shard_cat[fi];
    pthread_mutex_unlock(&p->shard_cat_lock);
    return res;
}

/*
 * The previous directory's parent path, kept so a run of siblings costs one chain walk.
 *
 * crawl_bin_catalog_dir_path_len rebuilds a path by walking parent_dir_id to the root, which is a
 * random load per level and was 8.3% of the run. The fold visits dir_ids in ascending order and
 * ecrawl hands them out as it meets entries, so a directory's neighbours are usually its siblings:
 * cache the parent's path and the next directory is a memcpy plus its own name.
 */
typedef struct {
    uint64_t prefix_dir; /* dir_id whose path fills buf; 0 when there is none */
    size_t prefix_len;
    char buf[ANALYZE_PARENT_PATH_MAX];
} analyze_path_cache_t;

static int analyze_dir_path_cached(const crawl_bin_catalog_t *cat, analyze_path_cache_t *pc, uint64_t d, char *out,
                                   size_t out_sz, size_t *len_out) {
    uint64_t par;
    size_t nl, end;

    if (d == 0ULL || d > cat->max_dir_id) return -1;
    if (d == 1ULL) {
        /* The synthetic root reconstructs to the empty string, exactly as dir_path_len has it. */
        if (out_sz < 1U) return -1;
        out[0] = '\0';
        *len_out = 0;
        return 0;
    }

    /*
     * Past the walk's component limit the two routes disagree, and not by a rounding error:
     * dir_path_len drops the leading components it did not reach and says nothing about it, so
     * "parent's path plus one name" is a 129-component path where the direct walk gives 128. A
     * 200-deep chain is legal input, so match the walk rather than improve on it.
     */
    if (cat->depth[d] > CRAWL_BIN_CATALOG_MAX_PATH_PARTS)
        return crawl_bin_catalog_dir_path_len(cat, d, out, out_sz, len_out);

    par = cat->parent_dir_id[d];
    if (par == 0ULL) return -1;
    if (par != pc->prefix_dir) {
        if (crawl_bin_catalog_dir_path_len(cat, par, pc->buf, sizeof(pc->buf), &pc->prefix_len) != 0) {
            pc->prefix_dir = 0;
            return -1;
        }
        pc->prefix_dir = par;
    }

    nl = (size_t)cat->name_len[d];
    if (nl > 0 && !cat->name_comp[d]) return -1;
    end = pc->prefix_len + 1U + nl;
    if (end + 1U > out_sz) return -1;
    memcpy(out, pc->buf, pc->prefix_len);
    out[pc->prefix_len] = '/'; /* paths are absolute, so every component gets a leading slash */
    if (nl > 0) memcpy(out + pc->prefix_len + 1U, cat->name_comp[d], nl);
    out[end] = '\0';
    *len_out = end;
    return 0;
}

/* Publish a node nobody else can be holding a duplicate of: no hash, no bucket, no lock. */
static parent_node_t *parent_arena_add_unique(parent_arena_t *arena, const char *path, size_t path_len) {
    parent_node_t *node = (parent_node_t *)parent_arena_alloc(arena, parent_arena_stride(path_len));

    if (!node) return NULL;
    node->path = (char *)node + sizeof(*node);
    memcpy(node->path, path, path_len);
    node->path[path_len] = '\0';
    node->path_len = (uint32_t)path_len;
    node->hash = 0; /* never chained, so nothing reads it */
    atomic_init(&node->next, NULL);
    return node;
}

/*
 * Turn one slice of a shard's counters into parent nodes.
 *
 * This is where paths finally get built, once per directory that actually holds records, instead of
 * once per directory per worker that happened to meet it first. Directories nothing was counted
 * under are skipped, which is what the map did implicitly by only ever inserting a parent it saw.
 */
static int analyze_fold_range(const crawl_bin_catalog_t *cat, const analyze_dir_counts_t *cnt, uint64_t lo,
                              uint64_t hi, parent_map_t *map, parent_arena_t *arena, int use_map, char *pathbuf,
                              size_t pathbuf_sz, analyze_path_cache_t *pc) {
    uint64_t d;

    for (d = lo; d <= hi; d++) {
        const analyze_dir_counts_t *c = &cnt[d];
        uint64_t nfile = atomic_load_explicit(&c->nfile, memory_order_relaxed);
        uint64_t ndir = atomic_load_explicit(&c->ndir, memory_order_relaxed);
        uint64_t nsym = atomic_load_explicit(&c->nsym, memory_order_relaxed);
        uint64_t nother = atomic_load_explicit(&c->nother, memory_order_relaxed);
        parent_node_t *node;
        size_t plen = 0;

        if ((nfile | ndir | nsym | nother) == 0ULL) continue;

        if (analyze_dir_path_cached(cat, pc, d, pathbuf, pathbuf_sz, &plen) != 0) return -1;
        if (plen == 0) {
            /* dir 1: its children are /<name>, so the directory they hang off is "/". */
            pathbuf[0] = '/';
            pathbuf[1] = '\0';
            plen = 1U;
        }

        if (use_map) {
            node = parent_map_get_or_add(map, arena, pathbuf, plen);
            if (!node) return -1;
            /* Another shard may be folding the same path right now. */
            atomic_fetch_add_explicit(&node->c.nfile, nfile, memory_order_relaxed);
            atomic_fetch_add_explicit(&node->c.ndir, ndir, memory_order_relaxed);
            atomic_fetch_add_explicit(&node->c.nsym, nsym, memory_order_relaxed);
            atomic_fetch_add_explicit(&node->c.nother, nother, memory_order_relaxed);
        } else {
            node = parent_arena_add_unique(arena, pathbuf, plen);
            if (!node) return -1;
            atomic_init(&node->c.nfile, nfile);
            atomic_init(&node->c.ndir, ndir);
            atomic_init(&node->c.nsym, nsym);
            atomic_init(&node->c.nother, nother);
        }
    }
    return 0;
}

static void *analyze_fold_worker(void *arg) {
    analyze_pool_t *p = (analyze_pool_t *)arg;
    parent_map_t *map = p->map;
    char *pathbuf = (char *)malloc(ANALYZE_PARENT_PATH_MAX);
    analyze_path_cache_t *pc = (analyze_path_cache_t *)malloc(sizeof(*pc));
    parent_arena_t *arena = parent_map_arena_new(map);

    if (!pathbuf || !pc || !arena) {
        free(pathbuf);
        free(pc);
        atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        return NULL;
    }
    pc->prefix_dir = 0;
    pc->prefix_len = 0;

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->fold_cursor, 1, memory_order_relaxed);
        const analyze_fold_job_t *j;

        if (i >= p->fold_job_count) break;
        j = &p->fold_jobs[i];
        /* Slices of one shard run concurrently, so the cached prefix from another shard is stale. */
        pc->prefix_dir = 0;
        if (analyze_fold_range(&p->shard_cat[j->fi], p->shard_cnt[j->fi], j->lo, j->hi, map, arena, p->fold_use_map,
                               pathbuf, ANALYZE_PARENT_PATH_MAX, pc) != 0) {
            fprintf(stderr, "%s: ecrawl_query could not name directories %" PRIu64 "-%" PRIu64 " of this shard\n",
                    p->names[j->fi], j->lo, j->hi);
            atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        }
    }

    free(pathbuf);
    free(pc);
    return NULL;
}

/* Dir_ids per fold job: enough that claiming one is negligible, small enough to keep threads fed. */
#define ANALYZE_FOLD_JOB_DIRS 8192ULL
/* Directories a fold thread has to be worth: below this, spawning it costs more than it saves. */
#define ANALYZE_FOLD_DIRS_PER_THREAD 32768ULL

static int analyze_fold_build_jobs(analyze_pool_t *p, size_t name_count) {
    size_t cap = 0, n = 0, fi;
    analyze_fold_job_t *jobs = NULL;

    for (fi = 0; fi < name_count; fi++) {
        uint64_t max_id, lo;

        if (p->shard_cat_state[fi] != SHARD_CAT_READY || !p->shard_cnt[fi]) continue;
        max_id = p->shard_cat[fi].max_dir_id;
        for (lo = 1ULL; lo <= max_id; lo += ANALYZE_FOLD_JOB_DIRS) {
            uint64_t hi = lo + ANALYZE_FOLD_JOB_DIRS - 1ULL;

            if (hi > max_id) hi = max_id;
            if (n == cap) {
                size_t ncap = cap ? cap * 2U : 64U;
                analyze_fold_job_t *nj = (analyze_fold_job_t *)realloc(jobs, ncap * sizeof(*nj));

                if (!nj) {
                    free(jobs);
                    return -1;
                }
                jobs = nj;
                cap = ncap;
            }
            jobs[n].fi = fi;
            jobs[n].lo = lo;
            jobs[n].hi = hi;
            n++;
        }
    }
    p->fold_jobs = jobs;
    p->fold_job_count = n;
    atomic_store_explicit(&p->fold_cursor, 0, memory_order_relaxed);
    return 0;
}

/*
 * Mark one chunk of shard fi done; when the shard's last chunk completes, drop its
 * catalog so peak memory tracks the shards in flight rather than all shards at once.
 * Safe to free here: the count only reaches 0 after the final chunk has finished
 * using the catalog, so no reader is active.
 *
 * The shape pass has to fold the shard's counts into paths before it can let the catalog go, since
 * a dir_id means nothing without it. A shard small enough to fold in one go is folded right here,
 * by the worker that finished it, which keeps that work overlapped with the shards still being
 * scanned and returns the catalog to the allocator immediately -- a corpus of a thousand small
 * shards would otherwise pay for all of them at once and then fold them in a phase of its own. A
 * shard too large for that is left for the post-scan fold, which can split it across threads.
 */
static void analyze_release_shard_chunk(analyze_pool_t *p, size_t fi, parent_arena_t *arena, char *pathbuf,
                                        analyze_path_cache_t *pc) {
    if (atomic_fetch_sub_explicit(&p->shard_chunks_left[fi], 1ULL, memory_order_acq_rel) != 1ULL) return;

    if (p->shard_cnt) {
        analyze_dir_counts_t *cnt = p->shard_cnt[fi];

        if (!cnt || !arena || !pathbuf || !pc) return; /* the post-scan fold will take it */
        if (p->shard_cat_state[fi] != SHARD_CAT_READY) return;
        if (p->shard_cat[fi].max_dir_id > ANALYZE_FOLD_JOB_DIRS) return;

        pc->prefix_dir = 0;
        if (analyze_fold_range(&p->shard_cat[fi], cnt, 1ULL, p->shard_cat[fi].max_dir_id, p->map, arena,
                               p->fold_use_map, pathbuf, ANALYZE_PARENT_PATH_MAX, pc) != 0) {
            fprintf(stderr, "%s: ecrawl_query could not name this shard's directories\n", p->names[fi]);
            atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        }
        /* Cleared before the catalog goes, so the post-scan pass knows this shard is spoken for. */
        p->shard_cnt[fi] = NULL;
        free(cnt);
    }

    pthread_mutex_lock(&p->shard_cat_lock);
    if (p->shard_cat_state[fi] == SHARD_CAT_READY) {
        crawl_bin_catalog_free(&p->shard_cat[fi]);
        if (p->shard_sub) subtree_free(&p->shard_sub[fi]);
        p->shard_cat_state[fi] = SHARD_CAT_FREED;
    }
    pthread_mutex_unlock(&p->shard_cat_lock);
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
    if (p->shard_cnt)
        for (fi = 0; fi < name_count; fi++) free(p->shard_cnt[fi]);
    free(p->shard_sub);
    p->shard_sub = NULL;
    free(p->shard_cnt);
    p->shard_cnt = NULL;
    free(p->shard_cat);
    free(p->shard_cat_state);
    free((void *)p->shard_chunks_left);
    p->shard_cat = NULL;
    p->shard_cat_state = NULL;
    p->shard_chunks_left = NULL;
}

static int analyze_process_chunk(const char *full_path, uint64_t start_off, uint64_t end_off, uint64_t file_sz,
                                 const crawl_bin_catalog_t *cat, unsigned char *pathbuf, char *parentbuf,
                                 char *fullpath_buf, size_t fullpath_sz, parent_map_t *map, parent_arena_t *arena,
                                 uint64_t *depth_hist, analyze_dir_counts_t *cnt, uint64_t *nrec_out) {
    FILE *fp;
    int rc;

    if (!cat) return -1;
    if (start_off > file_sz || end_off > file_sz || start_off >= end_off) return -1;
    /* The handle cache applies the large read buffer when it really opens the shard, and hands the
     * same stream back for the next chunk of it; a reused stream must not be setvbuf'd again. */
    fp = crawl_fpcache_fopen(full_path, "rb");
    if (!fp) {
        perror(full_path);
        return -1;
    }
    /* Catalog is preloaded and shared; the per-chunk FILE* only walks records. */
    if (fseeko(fp, (off_t)start_off, SEEK_SET) != 0) {
        crawl_fpcache_fclose(fp);
        return -1;
    }
    rc = analyze_scan_fp_until(fp, start_off, end_off, file_sz, cat, pathbuf, parentbuf, fullpath_buf, fullpath_sz,
                               map, arena, depth_hist, cnt, nrec_out);
    crawl_fpcache_fclose(fp);
    return rc;
}

static int query_process_chunk(const char *full_path, uint64_t start_off, uint64_t end_off, uint64_t file_sz,
                               query_cat_lazy_t *lz, const shard_subtree_t *sub, query_memb_t *mb,
                               crawl_bin_block_reader_t *br, query_path_cache_t *pcache,
                               char *fullpath_buf, size_t fullpath_sz,
                               query_result_t *qr, uint64_t *nrec_out) {
    FILE *fp;
    int rc;

    if (start_off > file_sz || end_off > file_sz || start_off >= end_off) return -1;
    fp = crawl_fpcache_fopen(full_path, "rb");
    if (!fp) {
        perror(full_path);
        return -1;
    }
    if (fseeko(fp, (off_t)start_off, SEEK_SET) != 0) {
        crawl_fpcache_fclose(fp);
        return -1;
    }
    rc = query_scan_fp_until(fp, start_off, end_off, lz, sub, mb, br, pcache, fullpath_buf, fullpath_sz, qr, nrec_out);
    crawl_fpcache_fclose(fp);
    return rc;
}

static void *query_worker_main(void *arg) {
    analyze_pool_t *p = (analyze_pool_t *)arg;
    unsigned slot = atomic_fetch_add_explicit(&p->slot_assign, 1U, memory_order_relaxed);
    query_result_t *qr = &p->qres[slot];
    char *fullpath_buf = (char *)malloc(PATH_MAX);
    crawl_bin_block_reader_t br;
    query_path_cache_t pcache;
    query_memb_t mb;
    size_t last_shard = (size_t)-1;

    memset(&br, 0, sizeof(br));
    memset(&pcache, 0, sizeof(pcache));
    memset(&mb, 0, sizeof(mb));

    if (!fullpath_buf) {
        atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        return NULL;
    }
    if (p->sidecar_mem) {
        mb.walk = crawl_dirx_walk_new();
        if (!mb.walk) {
            free(fullpath_buf);
            atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
            return NULL;
        }
    }
    if (g_query.list_paths) {
        /* A miss just rebuilds the path, so a failed allocation costs speed, not answers. */
        (void)query_path_cache_init(&pcache);
    }

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->chunk_cursor, 1, memory_order_relaxed);
        crawl_bin_file_chunk_t *c;
        uint64_t nrec = 0;
        int ar;

        if (i >= p->chunk_count) break;
        c = &p->chunks[i];
        if (c->file_index != last_shard) {
            /* dir_ids are per shard: a cached path from the previous one would be wrong. */
            query_path_cache_reset(&pcache);
            mb.live = 0;
            last_shard = c->file_index;
        }
        {
            const shard_subtree_t *sub = p->shard_sub ? &p->shard_sub[c->file_index] : NULL;
            query_cat_lazy_t lz;

            query_cat_lazy_init(&lz, p, c->file_index, c->path);
            /* --subtree filters records against membership state, so that mode has to
             * resolve it before scanning: from the catalog normally, from the dir-index
             * scope when the sidecar is live. Every other query defers the catalog until
             * a matched record needs a path. */
            if (g_query.subtree && !(sub && sub->sidecar_mem) && !query_cat_lazy_get(&lz)) {
                ar = -1;
            } else if (sub && sub->empty) {
                /* The subtree is not in this shard's catalog, and neither is its parent, so
                 * no record here can match: retire the chunk without reading it. */
                ar = 0;
            } else {
                ar = query_process_chunk(c->path, c->start_offset, c->end_offset,
                                         p->shard_file_sizes[c->file_index], &lz, sub, &mb, &br, &pcache,
                                         fullpath_buf, PATH_MAX, qr, &nrec);
            }
            analyze_release_shard_chunk(p, c->file_index, NULL, NULL, NULL);
        }

        atomic_fetch_add_explicit(&p->analyze_bytes_done, c->end_offset - c->start_offset, memory_order_relaxed);
        atomic_fetch_add_explicit(&p->analyze_chunks_done, 1ULL, memory_order_relaxed);
        if (ar == 0) {
            atomic_fetch_add_explicit(&p->analyze_records_done, nrec, memory_order_relaxed);
        } else {
            fprintf(stderr, "%s [%" PRIu64 ",%" PRIu64 "): ecrawl_query query scan failed\n", c->path,
                    c->start_offset, c->end_offset);
            atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        }
    }

    if (!query_list_buffered()) query_out_flush(qr);
    crawl_bin_block_reader_free(&br);
    qr->path_hits_last += pcache.hits_last;
    qr->path_hits_hash += pcache.hits_hash;
    qr->path_misses += pcache.misses;
    query_path_cache_free(&pcache);
    crawl_dirx_walk_free(mb.walk);
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
 *   - no --size-gt / --type / --gid / --uid / --perm, because the rollups aggregate every record; and no
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
           !g_query.have_size_gt && !g_query.type_filter && !g_query.have_gid && !g_query.have_uid &&
           !g_query.perm_mode && !g_query.list_paths;
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
        int ok = 0;

        if (snprintf(full, sizeof(full), "%s/%s", dir_path, names[fi]) >= (int)sizeof(full)) return 0;
        if (stat(full, &stt) != 0) return 0;
        fp = crawl_fpcache_fopen(full, "rb");
        if (!fp) return 0;

        crawl_bin_catalog_init_empty(&cat);
        if (fread(&fh, sizeof(fh), 1, fp) == 1 &&
            crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION) && fh.catalog_offset != 0ULL &&
            fh.catalog_offset <= (uint64_t)stt.st_size &&
            crawl_bin_catalog_load_sel(fp, fh.catalog_offset, (uint64_t)stt.st_size, CRAWL_CAT_SUBTREE, &cat) ==
                0)
            ok = 1;
        crawl_fpcache_fclose(fp);
        if (!ok) {
            crawl_bin_catalog_free(&cat);
            return 0;
        }

        if (subtree_build(&cat, &sub, 0) != 0) {
            subtree_free(&sub);
            crawl_bin_catalog_free(&cat);
            return 0;
        }
        query_warn_dup_subtree(sub.nroots, names[fi]);
        out->dirs_scanned += cat.max_dir_id;
        {
            size_t ri;

            for (ri = 0; ri < sub.nroots; ri++) {
                uint64_t root = shard_sub_root_at(&sub, ri);

                if (root == 0ULL) continue;
                if (cat.subtree_nlink_gt1_count[root] != 0ULL) {
                    /* A multiply-linked file in scope makes the rollup and the scan
                     * legitimately disagree; do not present it as the answer. */
                    subtree_free(&sub);
                    crawl_bin_catalog_free(&cat);
                    return 0;
                }
                found_any = 1;
                out->bytes += cat.subtree_bytes[root];
                out->entries += cat.subtree_count[root];
                out->files += cat.subtree_files[root];
                out->dirs += cat.subtree_dirs[root];
                out->symlinks += cat.subtree_symlinks[root];
                /* Each matching root's own record is counted where the record actually is. */
                if (cat.self_present[root]) {
                    out->bytes += cat.self_bytes[root];
                    out->entries++;
                    out->dirs++;
                }
            }
        }
        subtree_free(&sub);
        crawl_bin_catalog_free(&cat);
    }

    if (!found_any) return 0;
    out->other = out->entries - out->files - out->dirs - out->symlinks;
    return 1;
}

/* ---------------------------------------------------------------------------
 * dirs.idx / rowgroups.idx
 *
 * The rollup above is already O(directories) rather than O(files), but it still
 * has to materialise every directory row in every shard to find the one row it
 * needs — 880513 directories examined to answer with 2011, flat in the size of
 * the subtree. These sidecars remove that: dirs.idx turns the path into a
 * dir_id and a byte offset, so the answer is a handful of preads, and
 * rowgroups.idx says which row groups a scan can skip outright.
 *
 * The reader itself lives in crawl_sidecar.c, shared with ereport --subtree;
 * what follows is only how a query uses it.
 * ------------------------------------------------------------------------- */

/* Aim for a few jobs per thread so a slow job cannot leave the pool idle at the tail. */
#define ANALYZE_JOBS_PER_THREAD 4ULL
#define ANALYZE_SPLIT_MIN_BYTES (256ULL << 10)
#define ANALYZE_SPLIT_MAX_BYTES (4ULL << 20)

/*
 * Q4 through the sidecar: the same aggregate query_try_rollup computes, but each
 * shard costs a binary search and a short chain of preads instead of a full
 * catalog parse. Every guard the catalog route applies applies here too — most
 * importantly the nlink>1 bail-out, which is what keeps the rollup honest.
 *
 * Returns 1 when it answered, 0 when the caller must take another route.
 */
static int query_try_rollup_sidecar(const crawl_sidecar_t *sc, query_rollup_t *out) {
    crawl_dirx_walk_t *walk;
    unsigned char namebuf[256];
    uint64_t rows_read = 0;
    size_t fi;
    int found_any = 0;
    int rc = 0;

    memset(out, 0, sizeof(*out));
    if (!sc->have_dirs) return 0;

    walk = crawl_dirx_walk_new();
    if (!walk) goto done;

    for (fi = 0; fi < sc->shard_count; fi++) {
        const crawl_dirx_view_t *v = &sc->dirs[fi];
        uint64_t *ids = NULL;
        size_t n = 0, i;
        char where[32];

        if (crawl_dirx_lookup_all(v, g_query.subtree, g_query.subtree_len, walk, &ids, &n, &rows_read) != 0)
            goto done;
        if (n == 0) {
            free(ids);
            continue;
        }
        snprintf(where, sizeof(where), "shard %zu", fi);
        query_warn_dup_subtree(n, where);
        for (i = 0; i < n; i++) {
            bin_dir_catalog_entry_t ent;

            if (crawl_dirx_read_row(v, walk, ids[i], CRAWL_CAT_SUBTREE, &ent, namebuf, sizeof(namebuf), NULL,
                                    &rows_read) != 0) {
                free(ids);
                goto done;
            }
            if (ent.subtree_nlink_gt1_count != 0ULL) {
                /* Crawl-time hardlink credit lands with the first link seen anywhere in
                 * the tree; a scan dedups within the subtree. With one in scope the two
                 * legitimately differ, so this is not an answer. */
                free(ids);
                goto done;
            }
            found_any = 1;
            out->bytes += ent.subtree_bytes;
            out->entries += ent.subtree_count;
            out->files += ent.subtree_files;
            out->dirs += ent.subtree_dirs;
            out->symlinks += ent.subtree_symlinks;
            if (ent.flags & CRAWL_DIR_FLAG_SELF_RECORD) {
                out->bytes += ent.self_bytes;
                out->entries++;
                out->dirs++;
            }
        }
        free(ids);
    }

    if (found_any) {
        out->other = out->entries - out->files - out->dirs - out->symlinks;
        out->dirs_scanned = rows_read;
        rc = 1;
    }

done:
    crawl_dirx_walk_free(walk);
    return rc;
}

static crawl_rgix_prune_stats_t g_rgix_stats;

/* Jobs the scan was split into. The non-query summary prints its own count; a
 * query prints this one so pruning's effect on parallelism stays visible. */
static size_t g_parse_chunk_jobs;

/* The shared reader takes shard paths; a query has the directory and the basenames. */
static int analyze_sidecar_open_named(const char *index_dir, const char *dir_path, char **names,
                                      size_t name_count, crawl_sidecar_t *sc) {
    const char **paths;
    size_t fi;
    int rc = -1;

    memset(sc, 0, sizeof(*sc));
    paths = (const char **)calloc(name_count ? name_count : 1U, sizeof(*paths));
    if (!paths) return -1;
    for (fi = 0; fi < name_count; fi++) {
        char full[PATH_MAX];

        if (snprintf(full, sizeof(full), "%s/%s", dir_path, names[fi]) >= (int)sizeof(full)) goto out;
        paths[fi] = strdup(full);
        if (!paths[fi]) goto out;
    }
    rc = crawl_sidecar_open(index_dir, paths, name_count, sc);

out:
    for (fi = 0; fi < name_count; fi++) free((void *)paths[fi]);
    free(paths);
    return rc;
}

/*
 * Build the scan job list from the row groups that survive pruning, in place of
 * the .ckpt segment boundaries analyze_build_all_chunks would use.
 *
 * The pruning itself is shared (crawl_rgix_build_chunks); what is analyze's own
 * is the split policy and the two by-products the scan wants anyway -- the
 * shard sizes and the capture's directory count. The caller computes the scope
 * (and keeps it alive when the scan will test membership against it).
 */
static int analyze_build_chunks_from_rowgroups(const crawl_sidecar_t *sc, const char *dir_path, char **names,
                                               size_t name_count, const crawl_sidecar_scope_t *scope,
                                               uint64_t **shard_sizes_out,
                                               crawl_bin_file_chunk_t **chunks_out, size_t *chunk_count_out,
                                               uint64_t *chunk_bytes_total_out, uint64_t *dir_count_total_out,
                                               unsigned nthreads, crawl_rgix_prune_stats_t *st) {
    const char **paths = NULL;
    uint64_t *sizes = NULL;
    uint64_t dir_sum = 0;
    size_t fi;

    memset(st, 0, sizeof(*st));
    *shard_sizes_out = NULL;
    *chunks_out = NULL;
    *chunk_count_out = 0;
    *chunk_bytes_total_out = 0;
    *dir_count_total_out = 0;

    if (!sc->have_dirs || !sc->have_groups) return -1;

    sizes = (uint64_t *)calloc(name_count, sizeof(*sizes));
    paths = (const char **)calloc(name_count, sizeof(*paths));
    if (!sizes || !paths) goto fail;

    for (fi = 0; fi < name_count; fi++) {
        char full[PATH_MAX];

        if (snprintf(full, sizeof(full), "%s/%s", dir_path, names[fi]) >= (int)sizeof(full)) goto fail;
        paths[fi] = strdup(full);
        if (!paths[fi]) goto fail;
        sizes[fi] = sc->dirs[fi].shard_size;
        dir_sum += sc->dirs[fi].catalog_entries;
    }

    if (crawl_rgix_build_chunks(sc, paths, name_count, scope, nthreads, (unsigned)ANALYZE_JOBS_PER_THREAD,
                                ANALYZE_SPLIT_MIN_BYTES, ANALYZE_SPLIT_MAX_BYTES, chunks_out, chunk_count_out,
                                chunk_bytes_total_out, st) != 0)
        goto fail;
    if (analyze_interleave_chunks_shard_round_robin(chunks_out, *chunk_count_out, name_count) != 0) {
        crawl_bin_free_chunk_array_rows(*chunks_out, *chunk_count_out);
        *chunks_out = NULL;
        *chunk_count_out = 0;
        goto fail;
    }

    for (fi = 0; fi < name_count; fi++) free((void *)paths[fi]);
    free(paths);
    *shard_sizes_out = sizes;
    *dir_count_total_out = dir_sum;
    return 0;

fail:
    if (paths) {
        for (fi = 0; fi < name_count; fi++) free((void *)paths[fi]);
        free(paths);
    }
    free(sizes);
    memset(st, 0, sizeof(*st));
    *chunks_out = NULL;
    *chunk_count_out = 0;
    *chunk_bytes_total_out = 0;
    return -1;
}

/* Same key=value shape as query_report, so a caller cannot tell which path
 * produced the answer except by the diagnostics. */
static void query_report_rollup(const query_rollup_t *r, const char *source, double elapsed) {
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
    printf("answered_from=%s\n", source);
    printf("directories_examined=%" PRIu64 "\n", r->dirs_scanned);
    /* Microseconds, not milliseconds: a dirs.idx answer lands in 2-3 ms, which %.3f rounds to
     * 0.000 and the tool loses the ability to report its own speed. */
    printf("elapsed_sec=%.6f\n", elapsed);
    fflush(stdout);
}

static void query_report(query_result_t *res, unsigned n, uint64_t records_scanned, double elapsed,
                         double chunk_prep_sec) {
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
        sum.path_hits_last += res[i].path_hits_last;
        sum.path_hits_hash += res[i].path_hits_hash;
        sum.path_misses += res[i].path_misses;
    }
    sum.bytes += query_hardlink_bytes(res, n, &dupes);
    for (i = 0; i < n; i++) sum.oom |= res[i].oom;
    {
        /* The subtree's own directory record, taken from the catalog when the scan ran
         * without the name column (see query_note_subtree_self). */
        uint64_t self_n = atomic_load_explicit(&g_subtree_self_count, memory_order_relaxed);

        if (self_n) {
            sum.entries += self_n;
            sum.dirs += self_n;
            sum.bytes += atomic_load_explicit(&g_subtree_self_bytes, memory_order_relaxed);
        }
    }

    fprintf(out, "subtree=%s\n", g_query.subtree ? g_query.subtree : "(whole capture)");
    if (g_query.have_size_gt) fprintf(out, "size_gt=%" PRIu64 "\n", g_query.size_gt);
    if (g_query.have_gid) fprintf(out, "gid=%" PRIu32 "\n", g_query.gid);
    if (g_query.have_uid) fprintf(out, "uid=%" PRIu32 "\n", g_query.uid);
    if (g_query.list_level) fprintf(out, "list_level=%u\n", g_query.list_level);
    if (g_query.sum) fprintf(out, "sum=1\n");
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
    fprintf(out, "catalogs_loaded=%" PRIu64 "\n", atomic_load_explicit(&g_catalogs_loaded, memory_order_relaxed));
    fprintf(out, "blocks_decompressed=%" PRIu64 "\n", sum.blocks_decompressed);
    fprintf(out, "blocks_skipped=%" PRIu64 "\n", sum.blocks_skipped);
    fprintf(out, "records_skipped_by_block_filter=%" PRIu64 "\n", sum.records_skipped);
    if (g_query.list_paths) {
        fprintf(out, "path_cache_last_hits=%" PRIu64 "\n", sum.path_hits_last);
        fprintf(out, "path_cache_hash_hits=%" PRIu64 "\n", sum.path_hits_hash);
        fprintf(out, "path_cache_misses=%" PRIu64 "\n", sum.path_misses);
    }
    fprintf(out, "parse_chunk_jobs=%zu\n", g_parse_chunk_jobs);
    fprintf(out, "chunk_prep_sec=%.6f\n", chunk_prep_sec);
    if (g_rgix_stats.used) {
        /* kept is the intersection the scan actually used; the two single-sketch
         * counts are what the interval or the bitmap would have kept on its own,
         * which is how the two encodings get compared on real data. */
        fprintf(out, "rowgroups_total=%" PRIu64 "\n", g_rgix_stats.total);
        fprintf(out, "rowgroups_kept=%" PRIu64 "\n", g_rgix_stats.kept);
        fprintf(out, "rowgroups_kept_interval=%" PRIu64 "\n", g_rgix_stats.kept_interval);
        fprintf(out, "rowgroups_kept_bitmap=%" PRIu64 "\n", g_rgix_stats.kept_bitmap);
        fprintf(out, "rowgroup_bytes_total=%" PRIu64 "\n", g_rgix_stats.bytes_total);
        fprintf(out, "rowgroup_bytes_kept=%" PRIu64 "\n", g_rgix_stats.bytes_kept);
    }
    fprintf(out, "answered_from=record_scan\n");
    fprintf(out, "elapsed_sec=%.6f\n", elapsed);
    if (sum.oom) fprintf(out, "warning=allocation_failed_totals_may_be_wrong\n");
    fflush(out);
}

static void query_results_free(query_result_t *res, unsigned n) {
    unsigned i;

    if (!res) return;
    for (i = 0; i < n; i++) {
        free(res[i].hl);
        query_out_segs_free(res[i].out_head);
        query_lrec_segs_free(res[i].lrec_head);
    }
    free(res);
}

static void *analyze_worker_main(void *arg) {
    analyze_pool_t *p = (analyze_pool_t *)arg;
    unsigned slot = atomic_fetch_add_explicit(&p->slot_assign, 1U, memory_order_relaxed);
    parent_map_t *map = p->map;
    uint64_t *dh = (uint64_t *)calloc((size_t)ANALYZE_DEPTH_BINS, sizeof(uint64_t));
    unsigned char *pathbuf = (unsigned char *)malloc(ANALYZE_PARENT_PATH_MAX);
    char *parentbuf = (char *)malloc(ANALYZE_PARENT_PATH_MAX);
    char *fullpath_buf = (char *)malloc(PATH_MAX);
    /* For folding a shard this worker finishes; parentbuf is free again by then, so it holds the path. */
    analyze_path_cache_t *pc = (analyze_path_cache_t *)malloc(sizeof(*pc));
    /* Owned by the map, which frees its blocks once the report has read the nodes out of them. */
    parent_arena_t *arena = parent_map_arena_new(map);

    if (!map || !dh || !pathbuf || !parentbuf || !fullpath_buf || !pc || !arena) {
        free(fullpath_buf);
        free(pathbuf);
        free(parentbuf);
        free(pc);
        free(dh);
        atomic_fetch_add_explicit(&p->failures, 1, memory_order_relaxed);
        return NULL;
    }
    pc->prefix_dir = 0;
    pc->prefix_len = 0;
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

            analyze_dir_counts_t *cnt = p->shard_cnt ? p->shard_cnt[c->file_index] : NULL;

            ar = cat ? analyze_process_chunk(c->path, c->start_offset, c->end_offset, fsz, cat, pathbuf, parentbuf,
                                             fullpath_buf, PATH_MAX, map, arena, dh, cnt, &nrec)
                     : -1;
            analyze_release_shard_chunk(p, c->file_index, arena, parentbuf, pc);
        }

        atomic_fetch_add_explicit(&p->analyze_bytes_done, chunk_bytes, memory_order_relaxed);
        atomic_fetch_add_explicit(&p->analyze_chunks_done, 1ULL, memory_order_relaxed);
        if (ar == 0)
            atomic_fetch_add_explicit(&p->analyze_records_done, nrec, memory_order_relaxed);
        else {
            fprintf(stderr,
                    "%s [%" PRIu64 ",%" PRIu64 "): ecrawl_query chunk scan failed (corrupt or ckpt mismatch?)\n",
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
    free(pc);
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

/*
 * Order two parents by path, the tie-break for both top lists.
 *
 * Same answer as strcmp, reached without hunting for the NUL: the nodes carry their lengths, so
 * this is a memcmp over a known span and then a length compare. Equivalent because when one path is
 * a prefix of the other, strcmp is deciding between the shorter one's NUL and a real byte, and the
 * NUL always loses -- which is what the length compare says. Worth doing because a flat tree offers
 * every parent with the same key, so the heap tie-breaks on the path 782 K times: 2.2% of the run
 * sat in __strcmp_avx2.
 */
static inline int analyze_path_cmp(const parent_node_t *a, const parent_node_t *b) {
    size_t la = a->path_len, lb = b->path_len;
    size_t n = la < lb ? la : lb;
    int c = memcmp(a->path, b->path, n);

    if (c != 0) return c;
    if (la == lb) return 0;
    return la < lb ? -1 : 1;
}

/* Final ordering for a top list: key descending, ties broken by path ascending. */
static int cmp_analyze_key_desc(const void *a, const void *b) {
    const analyze_sort_wrap_t *wa = (const analyze_sort_wrap_t *)a;
    const analyze_sort_wrap_t *wb = (const analyze_sort_wrap_t *)b;

    if (wa->key < wb->key) return 1;
    if (wa->key > wb->key) return -1;
    return analyze_path_cmp(wa->node, wb->node);
}

/*
 * "Worse" = ranked later in the desired top order (key desc, then path asc), i.e. the element that
 * should be evicted first. Used to keep a bounded min-heap of the top-N parents so the report does not
 * sort every distinct parent (O(M log N) time, O(N) memory instead of a full O(M log M) sort).
 */
static int analyze_topn_worse(const analyze_sort_wrap_t *a, const analyze_sort_wrap_t *b) {
    if (a->key != b->key) return a->key < b->key;
    return analyze_path_cmp(a->node, b->node) > 0;
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

/* Everything the report accumulates in its one pass over the parents. */
typedef struct {
    uint64_t total_records;
    uint64_t distinct_parents;
    uint64_t parents_with_files;
    uint64_t max_nfile;
    uint64_t hb1, hb2, hb3, hb4, hb5, hb6, hb7, hb8;
    uint64_t megadir_100k;
    analyze_sort_wrap_t *dense_heap;
    analyze_sort_wrap_t *deep_heap;
    size_t dense_cap, deep_cap;
    size_t dense_n, deep_n;
} analyze_rollup_t;

static void analyze_rollup_node(parent_node_t *n, void *ctx) {
    analyze_rollup_t *r = (analyze_rollup_t *)ctx;
    uint64_t nfile = n->c.nfile;
    uint64_t tot = nfile + n->c.ndir + n->c.nsym + n->c.nother;

    r->total_records += tot;
    r->distinct_parents++;
    if (nfile > 0ULL) {
        r->parents_with_files++;
        analyze_hist_files_per_parent(nfile, &r->hb1, &r->hb2, &r->hb3, &r->hb4, &r->hb5, &r->hb6, &r->hb7, &r->hb8);
        if (nfile >= 100000ULL) r->megadir_100k++;
        if (nfile > r->max_nfile) r->max_nfile = nfile;
    }

    if (r->dense_cap > 0U) analyze_topn_offer(r->dense_heap, &r->dense_n, r->dense_cap, n, nfile);
    if (r->deep_cap > 0U)
        analyze_topn_offer(r->deep_heap, &r->deep_n, r->deep_cap, n, analyze_count_slashes(n->path));
}

static void print_analyze_report(parent_map_t *map, const uint64_t *merged_depth, size_t shard_files,
                                 size_t parse_chunk_jobs, double chunk_prep_sec) {
    analyze_rollup_t roll;
    unsigned di;
    uint64_t depth_records = 0;
    unsigned max_depth_bin = 0;

    memset(&roll, 0, sizeof(roll));
    roll.dense_cap = g_top_dense ? (size_t)g_top_n : 0U;
    roll.deep_cap = g_top_deep ? (size_t)g_top_n : 0U;

    if (roll.dense_cap > 0U) {
        roll.dense_heap = (analyze_sort_wrap_t *)malloc(roll.dense_cap * sizeof(*roll.dense_heap));
        if (!roll.dense_heap) {
            /* Stats below are still emitted; only the top-N listing is dropped on OOM. */
            fprintf(stderr, "ecrawl_query: alloc failed for dense top-N heap; skipping list\n");
            roll.dense_cap = 0;
        }
    }
    if (roll.deep_cap > 0U) {
        roll.deep_heap = (analyze_sort_wrap_t *)malloc(roll.deep_cap * sizeof(*roll.deep_heap));
        if (!roll.deep_heap) {
            fprintf(stderr, "ecrawl_query: alloc failed for deep top-N heap; skipping list\n");
            roll.deep_cap = 0;
        }
    }

    parent_map_for_each(map, analyze_rollup_node, &roll);

    if (roll.dense_n > 0U) qsort(roll.dense_heap, roll.dense_n, sizeof(roll.dense_heap[0]), cmp_analyze_key_desc);
    if (roll.deep_n > 0U) qsort(roll.deep_heap, roll.deep_n, sizeof(roll.deep_heap[0]), cmp_analyze_key_desc);

    printf("ecrawl_query\n");
    printf("uid_shard_bin_files=%zu\n", shard_files);
    printf("parse_chunk_jobs=%zu\n", parse_chunk_jobs);
    printf("chunk_prep_sec=%.6f\n", chunk_prep_sec);
    printf("records_total=%" PRIu64 "\n", roll.total_records);
    printf("distinct_parent_directories=%" PRIu64 "\n", roll.distinct_parents);
    printf("parents_with_at_least_one_regular_file=%" PRIu64 "\n", roll.parents_with_files);
    printf("max_regular_files_under_single_parent=%" PRIu64 "\n", roll.max_nfile);
    printf("parents_with_regular_files_ge_100000=%" PRIu64 "\n", roll.megadir_100k);
    printf("\n");
    printf("histogram_regular_files_per_parent (among parents with nfile>=1)\n");
    printf("  nfile==1: %" PRIu64 "\n", roll.hb1);
    printf("  nfile_2_10: %" PRIu64 "\n", roll.hb2);
    printf("  nfile_11_100: %" PRIu64 "\n", roll.hb3);
    printf("  nfile_101_1000: %" PRIu64 "\n", roll.hb4);
    printf("  nfile_1001_10000: %" PRIu64 "\n", roll.hb5);
    printf("  nfile_10001_100000: %" PRIu64 "\n", roll.hb6);
    printf("  nfile_100001_1000000: %" PRIu64 "\n", roll.hb7);
    printf("  nfile_gt_1000000: %" PRIu64 "\n", roll.hb8);
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
        for (k = 0; k < roll.dense_n; k++) {
            parent_node_t *no = roll.dense_heap[k].node;
            printf("%zu %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %s\n", k + 1U, no->c.nfile, no->c.ndir,
                   no->c.nsym, no->c.nother, no->path);
        }
    }

    if (g_top_deep) {
        size_t k;

        if (g_top_dense) printf("\n");
        printf("top_parents_by_depth (N=%u)\n", g_top_n);
        printf("# depth nfile ndir nsym nother path\n");
        for (k = 0; k < roll.deep_n; k++) {
            parent_node_t *no = roll.deep_heap[k].node;
            printf("%zu %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %s\n", k + 1U,
                   roll.deep_heap[k].key, no->c.nfile, no->c.ndir, no->c.nsym, no->c.nother, no->path);
        }
    }

    free(roll.dense_heap);
    free(roll.deep_heap);
}

/*
 * Bytes per parse job. Checkpoint segments are 32 MiB apart, so a capture smaller
 * than that yields one job and runs single-threaded no matter what the thread
 * budget says — a 14 MiB shape report measured 94% CPU with 32 threads
 * configured. A fixed 4 MiB was not enough of a fix: columnar shards compress so
 * well that the same capture still only split into 4 jobs for 32 threads (175%
 * CPU). Size the job from the bytes actually on disk and the thread budget
 * instead, so the pool fills on small captures, and keep 4 MiB as the ceiling so
 * large captures keep jobs cache-friendly. ECRAWL_QUERY_CHUNK_BYTES overrides
 * it (tests use a tiny value to force many jobs from a small shard).
 *
 * Splitting is safe for both modes: a job is any block-aligned byte range, and
 * both the shape histograms and the query counters are accumulated per thread
 * and merged at the end, so job boundaries cannot change the answer.
 */
static uint64_t parse_split_target_bytes(const char *dir_path, char **names, size_t name_count,
                                         unsigned nthreads) {
    const char *e;
    uint64_t total = 0;
    uint64_t budget;
    uint64_t target;
    size_t fi;

    e = getenv("ECRAWL_QUERY_CHUNK_BYTES");
    if (e && e[0]) {
        char *end;
        unsigned long long v;

        errno = 0;
        v = strtoull(e, &end, 10);
        if (!errno && end != e && *end == '\0' && v >= 4096ULL) return (uint64_t)v;
    }

    for (fi = 0; fi < name_count; fi++) {
        char full[PATH_MAX];
        struct stat st;

        if (snprintf(full, sizeof(full), "%s/%s", dir_path, names[fi]) >= (int)sizeof(full)) continue;
        if (lstat(full, &st) == 0 && S_ISREG(st.st_mode)) total += (uint64_t)st.st_size;
    }

    budget = (uint64_t)(nthreads ? nthreads : 1U) * ANALYZE_JOBS_PER_THREAD;
    target = total / budget;
    if (target < ANALYZE_SPLIT_MIN_BYTES) target = ANALYZE_SPLIT_MIN_BYTES;
    if (target > ANALYZE_SPLIT_MAX_BYTES) target = ANALYZE_SPLIT_MAX_BYTES;
    return target;
}

/* Drop the sidecar and scope the pool may own; both are zeroed when it never took them. */
static void query_pool_sidecar_drop(analyze_pool_t *p) {
    crawl_sidecar_close(&p->sidecar);
    crawl_sidecar_scope_release_n(p->sidecar_scope, p->name_count);
    free(p->sidecar_scope);
    p->sidecar_scope = NULL;
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
    uint64_t dir_count_total = 0;
    uint64_t *shard_sizes = NULL;
    unsigned emit_threads;
    unsigned ti;
    unsigned dj;
    int rc = 0;
    crawl_sidecar_t sidecar;
    crawl_sidecar_scope_t *sidecar_scope = NULL;
    int have_sidecar = 0;
    const char *skip_env = getenv("ECRAWL_QUERY_BLOCK_SKIP");

    /* Off only for parity testing: skipping must never change query results. */
    g_query.block_skip = !(skip_env && strcmp(skip_env, "0") == 0);

    memset(&sidecar, 0, sizeof(sidecar));
    /* --index-dir names the sidecars explicitly; without it, probe the crawl output
     * dir, which is where a capture-local index lives. Absent files just degrade to
     * the catalog path. */
    if (g_query.active && g_query.subtree && !g_query.subtree_is_root)
        have_sidecar = (analyze_sidecar_open_named(g_index_dir ? g_index_dir : dir_path, dir_path, names,
                                                   name_count, &sidecar) == 0);

    /*
     * A plain subtree aggregate is already summed in the catalogs, so answer it
     * there and never open the record region: O(directories) instead of
     * O(files). Falls through to the scan whenever the rollup cannot be proven
     * equal to it (see query_try_rollup).
     *
     * With dirs.idx the same aggregate needs no catalog parse either — a hash
     * lookup and a short chain of preads locate the one row that holds it.
     */
    if (query_rollup_eligible()) {
        query_rollup_t roll;
        double t0 = analyze_now_sec();

        if (have_sidecar && query_try_rollup_sidecar(&sidecar, &roll)) {
            crawl_sidecar_close(&sidecar);
            query_report_rollup(&roll, "dir_index", analyze_now_sec() - t0);
            return 0;
        }
        if (query_try_rollup(dir_path, names, name_count, &roll)) {
            crawl_sidecar_close(&sidecar);
            query_report_rollup(&roll, "catalog_rollup", analyze_now_sec() - t0);
            return 0;
        }
    }

    double chunk_prep_sec = 0.0;
    /* The scope locates the subtree in every shard once, up front: the row-group
     * pruning reads it, and when no listing needs paths the scan keeps it for
     * per-record membership instead of materializing any catalog. A sidecar that
     * cannot place the subtree is dropped, not fatal. */
    if (have_sidecar) {
        uint64_t scope_rows = 0;
        size_t fi;

        sidecar_scope = (crawl_sidecar_scope_t *)calloc(name_count, sizeof(*sidecar_scope));
        if (!sidecar_scope ||
            crawl_sidecar_scope_subtree(&sidecar, g_query.subtree, g_query.subtree_len, g_query.sub_parent,
                                        sidecar_scope, &scope_rows) != 0) {
            crawl_sidecar_scope_release_n(sidecar_scope, name_count);
            free(sidecar_scope);
            sidecar_scope = NULL;
            crawl_sidecar_close(&sidecar);
            have_sidecar = 0;
        } else {
            for (fi = 0; fi < name_count; fi++) query_warn_dup_subtree(sidecar_scope[fi].nroots, names[fi]);
        }
    }
    {
        double t_prep0 = analyze_now_sec();
        uint64_t split_target = parse_split_target_bytes(dir_path, names, name_count, nthreads);
        int built = -1;

        /* Row groups whose DFS sketch cannot reach the subtree hold nothing this
         * query can match, so the scan list is built from the survivors instead of
         * from every .ckpt segment. Purely a reduction of the same byte ranges. */
        if (have_sidecar)
            built = analyze_build_chunks_from_rowgroups(&sidecar, dir_path, names, name_count, sidecar_scope,
                                                        &shard_sizes,
                                                        &chunks, &chunk_count, &chunk_byte_sum, &dir_count_total,
                                                        nthreads, &g_rgix_stats);
        if (built != 0 &&
            analyze_build_all_chunks(dir_path, names, name_count, &shard_sizes, &chunks, &chunk_count,
                                     &chunk_byte_sum, &dir_count_total, split_target) != 0) {
            crawl_sidecar_close(&sidecar);
            crawl_sidecar_scope_release_n(sidecar_scope, name_count);
            free(sidecar_scope);
            fprintf(stderr, "ecrawl_query: failed to build chunk job list\n");
            return 1;
        }
        /* The chunk-list build is a serial per-shard prologue; at high shard counts it
         * is the one phase no worker helps with, so report it separately. */
        chunk_prep_sec = analyze_now_sec() - t_prep0;
    }
    g_parse_chunk_jobs = chunk_count;
    if (chunk_count == 0U) {
        fprintf(stderr, "ecrawl_query: no parse chunks produced\n");
        crawl_sidecar_close(&sidecar);
        crawl_sidecar_scope_release_n(sidecar_scope, name_count);
        free(sidecar_scope);
        free(shard_sizes);
        return 1;
    }

    /*
     * The unit of parallel work is a chunk (ckpt segment), not a shard. A single
     * huge single-UID shard is split into many chunks, so cap workers by chunk
     * count rather than shard count — otherwise a one-shard crawl ran on a single
     * core no matter how many chunks (and cores) were available.
     *
     * The --level emit's work unit is a byte-range task sliced from the result
     * buffers, not a chunk, so it keeps the full requested thread count even when
     * few chunks (hence few workers and few buffers) limited the scan.
     */
    emit_threads = nthreads;
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
    atomic_init(&pool.fold_cursor, 0);
    /* Workers fold their own shard as they finish it, so this has to be settled before they start.
     * Two shards can hold the same path under different dir_ids and only the map can tell; one
     * cannot, so its dir_id is already the key and every node it folds is unique by construction. */
    pool.fold_use_map = (name_count > 1U);
    pool.analyze_bytes_total = chunk_byte_sum;
    atomic_init(&pool.analyze_bytes_done, 0);
    atomic_init(&pool.analyze_records_done, 0);
    atomic_init(&pool.analyze_chunks_done, 0);
    atomic_init(&pool.analyze_stop_stats, 0);
    analyze_shard_cat_sync_init(&pool);

    /* A live sidecar serves subtree membership when no listing needs paths; the
     * pool then owns it (and the scope shard_sub borrows from) until the workers
     * have joined. --list still loads catalogs for path reconstruction, so the
     * sidecar's job ends with the chunk list there. */
    pool.sidecar_mem = (have_sidecar && !g_query.list_paths);
    if (pool.sidecar_mem) {
        pool.sidecar = sidecar;
        pool.sidecar_scope = sidecar_scope;
        memset(&sidecar, 0, sizeof(sidecar));
        sidecar_scope = NULL;
    } else {
        crawl_sidecar_close(&sidecar);
        crawl_sidecar_scope_release_n(sidecar_scope, name_count);
        free(sidecar_scope);
        sidecar_scope = NULL;
    }

    /* Shared per-shard catalogs (loaded once on demand) + remaining-chunk counts. */
    if (g_query.active && g_query.subtree && !g_query.subtree_is_root) {
        pool.shard_sub = (shard_subtree_t *)calloc(name_count, sizeof(*pool.shard_sub));
        if (!pool.shard_sub) {
            perror("ecrawl_query: alloc");
            crawl_sidecar_close(&pool.sidecar);
            crawl_sidecar_scope_release_n(pool.sidecar_scope, name_count);
            free(pool.sidecar_scope);
            analyze_shard_cat_sync_destroy(&pool);
            crawl_bin_free_chunk_array_rows(chunks, chunk_count);
            free(shard_sizes);
            return 1;
        }
        if (pool.sidecar_mem) {
            size_t fi2;

            for (fi2 = 0; fi2 < name_count; fi2++) {
                query_subtree_from_scope(&pool.sidecar_scope[fi2], pool.sidecar.dirs[fi2].max_dir_id,
                                         &pool.shard_sub[fi2]);
                query_note_subtree_self_scope(&pool.sidecar_scope[fi2]);
            }
        }
    }
    pool.shard_cat = (crawl_bin_catalog_t *)calloc(name_count, sizeof(*pool.shard_cat));
    pool.shard_cat_state = (unsigned char *)calloc(name_count, sizeof(*pool.shard_cat_state));
    pool.shard_chunks_left = (_Atomic uint64_t *)calloc(name_count, sizeof(*pool.shard_chunks_left));
    /* Only the shape pass counts directories; query mode has its own per-thread counters. Its
     * presence is also what tells the release path to keep catalogs alive for the fold. */
    if (!g_query.active) pool.shard_cnt = (analyze_dir_counts_t **)calloc(name_count, sizeof(*pool.shard_cnt));
    if (!pool.shard_cat || !pool.shard_cat_state || !pool.shard_chunks_left ||
        (!g_query.active && !pool.shard_cnt)) {
        perror("ecrawl_query: alloc");
        free(pool.shard_sub);
        free(pool.shard_cat);
        free(pool.shard_cat_state);
        free(pool.shard_cnt);
        free(pool.shard_chunks_left);
        query_pool_sidecar_drop(&pool);
        analyze_shard_cat_sync_destroy(&pool);
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
    if (!g_query.active) pool.map = parent_map_new(nthreads, dir_count_total);
    pool.depth_hist = (uint64_t **)calloc((size_t)nthreads, sizeof(*pool.depth_hist));
    threads = (pthread_t *)calloc((size_t)nthreads, sizeof(*threads));
    if (g_query.active) {
        pool.qres = (query_result_t *)calloc((size_t)nthreads, sizeof(*pool.qres));
        if (!pool.qres) {
            perror("ecrawl_query: alloc");
            free(pool.depth_hist);
            free(threads);
            query_pool_sidecar_drop(&pool);
            analyze_free_shard_catalogs(&pool, name_count);
            analyze_shard_cat_sync_destroy(&pool);
            crawl_bin_free_chunk_array_rows(chunks, chunk_count);
            free(shard_sizes);
            return 1;
        }
    }
    if ((!g_query.active && !pool.map) || !pool.depth_hist || !threads) {
        perror("ecrawl_query: alloc");
        parent_map_free(pool.map);
        free(pool.depth_hist);
        free(threads);
        query_results_free(pool.qres, nthreads);
        query_pool_sidecar_drop(&pool);
        analyze_free_shard_catalogs(&pool, name_count);
        analyze_shard_cat_sync_destroy(&pool);
        crawl_bin_free_chunk_array_rows(chunks, chunk_count);
        free(shard_sizes);
        return 1;
    }

    sctx.pool = &pool;
    sctx.t0 = analyze_now_sec();
    /* Workers write listings straight to fd 1; drain anything stdio still holds first. */
    if (g_query.list_paths) fflush(stdout);
    /* The tick only ever writes to a terminal, so a redirected stderr gets a thread that
     * produces nothing; don't start it. */
    if (isatty(STDERR_FILENO) || g_verbose) {
        if (pthread_create(&stats_thread, NULL, analyze_stats_thread_main, &sctx) == 0)
            stats_started = 1;
        else
            perror("ecrawl_query: pthread_create (progress)");
    }

    for (ti = 0; ti < nthreads; ti++) {
        if (pthread_create(&threads[ti], NULL, g_query.active ? query_worker_main : analyze_worker_main, &pool) !=
            0) {
            unsigned j;

            perror("pthread_create");
            atomic_store_explicit(&pool.chunk_cursor, pool.chunk_count, memory_order_relaxed);
            if (stats_started) {
                analyze_stats_stop_request(&pool);
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
            query_pool_sidecar_drop(&pool);
            analyze_free_shard_catalogs(&pool, name_count);
            analyze_shard_cat_sync_destroy(&pool);
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
        analyze_stats_stop_request(&pool);
        pthread_join(stats_thread, NULL);
    }
    analyze_clear_progress_line();

    if (g_query.active) {
        if (g_query.list_level)
            query_list_emit_level_hash(pool.qres, nthreads, emit_threads);
        else if (g_query.sum)
            query_list_emit_sum(pool.qres, nthreads, emit_threads);
        query_report(pool.qres, nthreads, atomic_load_explicit(&pool.analyze_records_done, memory_order_relaxed),
                     analyze_now_sec() - sctx.t0, chunk_prep_sec);
        query_results_free(pool.qres, nthreads);
        pool.qres = NULL;
        query_pool_sidecar_drop(&pool);
        analyze_free_shard_catalogs(&pool, name_count);
        analyze_shard_cat_sync_destroy(&pool);
        crawl_bin_free_chunk_array_rows(pool.chunks, chunk_count);
        free(pool.shard_file_sizes);
        free(pool.depth_hist);
        if (atomic_load_explicit(&pool.failures, memory_order_relaxed) > 0) rc = 1;
        return rc;
    }

    /*
     * Fold: turn the per-shard dir_id counters into named parents.
     *
     * The scan left every count in a dense array indexed by dir_id, which says nothing about which
     * directory it belongs to without the catalog beside it, so this is the first and only point at
     * which paths get built -- once per directory that holds records, on every thread. Deferring it
     * this far is what let the scan run without touching a hash table or a lock.
     */
    if (pool.map && pool.shard_cnt) {
        size_t fi;
        uint64_t fold_dirs = 0;

        for (fi = 0; fi < name_count; fi++)
            if (pool.shard_cat_state[fi] == SHARD_CAT_READY && pool.shard_cnt[fi])
                fold_dirs += pool.shard_cat[fi].max_dir_id;

        if (analyze_fold_build_jobs(&pool, name_count) != 0) {
            perror("ecrawl_query: alloc");
            atomic_fetch_add_explicit(&pool.failures, 1, memory_order_relaxed);
        } else if (pool.fold_job_count > 0U) {
            /*
             * Only shards too big to have been folded by the worker that finished them are left, so
             * there is real work here; still scale the helper count to it, because spawning a thread
             * costs more than folding a few thousand directories.
             */
            uint64_t want = fold_dirs / ANALYZE_FOLD_DIRS_PER_THREAD;
            unsigned nfold = (want > (uint64_t)nthreads) ? nthreads : (unsigned)want;
            pthread_t *fthreads = NULL;
            unsigned started = 0;

            if ((uint64_t)nfold > (uint64_t)pool.fold_job_count) nfold = (unsigned)pool.fold_job_count;
            if (nfold > 1U) fthreads = (pthread_t *)calloc(nfold, sizeof(*fthreads));
            if (fthreads) {
                for (ti = 0; ti < nfold; ti++) {
                    if (pthread_create(&fthreads[ti], NULL, analyze_fold_worker, &pool) != 0) break;
                    started++;
                }
            }
            /* Whether or not helpers started, this thread drains what is left of the job list. */
            analyze_fold_worker(&pool);
            for (ti = 0; ti < started; ti++) pthread_join(fthreads[ti], NULL);
            free(fthreads);
        }
        free(pool.fold_jobs);
        pool.fold_jobs = NULL;
        pool.fold_job_count = 0;
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
        print_analyze_report(merged, merged_depth, name_count, chunk_count, chunk_prep_sec);
        parent_map_free(merged);
    }

    analyze_free_shard_catalogs(&pool, name_count);
    analyze_shard_cat_sync_destroy(&pool);

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
    /* Large fully-buffered stdio on shard reads cuts read() syscalls on big shards / NFS. */
    crawl_fpcache_set_bufsz((size_t)1 << 20);

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
        } else if (strcmp(argv[i], "--uid") == 0) {
            char *end;
            unsigned long long v;

            if (i + 1 >= argc) {
                fprintf(stderr, "%s: --uid requires a numeric user id\n", argv[0]);
                return 2;
            }
            errno = 0;
            v = strtoull(argv[++i], &end, 10);
            if (errno || end == argv[i] || *end != '\0' || v > 0xFFFFFFFFULL) {
                fprintf(stderr, "%s: --uid expects a numeric user id\n", argv[0]);
                return 2;
            }
            g_query.uid = (uint32_t)v;
            g_query.have_uid = 1;
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
        } else if (strcmp(argv[i], "--level") == 0) {
            char *end;
            unsigned long v;

            if (i + 1 >= argc) {
                fprintf(stderr, "%s: --level requires a depth (N >= 1)\n", argv[0]);
                return 2;
            }
            errno = 0;
            v = strtoul(argv[++i], &end, 10);
            if (errno || end == argv[i] || *end != '\0' || v < 1UL || v > 1000UL) {
                fprintf(stderr, "%s: --level expects 1 <= N <= 1000\n", argv[0]);
                return 2;
            }
            g_query.list_level = (unsigned)v;
        } else if (strcmp(argv[i], "--sum") == 0) {
            g_query.sum = 1;
            g_query.active = 1;
        } else if (strcmp(argv[i], "--index-dir") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "%s: --index-dir requires a directory\n", argv[0]);
                return 2;
            }
            g_index_dir = argv[++i];
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

    if (g_query.list_level && !g_query.list_paths) {
        fprintf(stderr, "%s: --level requires --list\n", argv[0]);
        return 2;
    }

    if (g_query.sum && !g_query.list_paths) {
        fprintf(stderr, "%s: --sum requires --list\n", argv[0]);
        return 2;
    }

    if (path_resolve_existing(dir_path, dir_path_abs, "ecrawl_query: ") != 0) return 2;
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

    if (g_query.have_uid) {
        uint32_t shards = query_manifest_uid_shards(dir_path);

        if (shards) {
            uint32_t want = g_query.uid & (shards - 1U);
            size_t w = 0, r;

            for (r = 0; r < name_count; r++) {
                uint32_t idx;

                if (uid_shard_index_from_name(names[r], &idx) == 0 && idx == want)
                    names[w++] = names[r];
                else
                    free(names[r]);
            }
            name_count = w;
        }
    }

    if (name_count == 0) {
        if (g_query.have_uid && g_query.active) {
            query_result_t empty;

            memset(&empty, 0, sizeof(empty));
            query_report(&empty, 1U, 0, 0.0, 0.0);
            return 0;
        }
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
