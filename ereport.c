/*
 * ereport.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Parallel reader for crawl bin files produced by ecrawl.
 * Emits the original HTML summary plus per-bucket drilldown pages with
 * dense level-1/level-2 directory summaries. Path search in index.html uses
 * GET /<user>/search on eserve (ereport_index trigram index under ./index/);
 * the search box is hidden unless the page URL carries a ?search parameter.
 *
 * Build:
 *   gcc -O2 -Wall -Wextra -pthread -o ereport ereport.c
 *
 * Usage:
 *   ./ereport [--bucket-details N] [--report-dir DIR] [--subtree PATH] [--index-dir DIR] [--path-rewrite OLD=NEW] [--verbose] <username|uid> [<atime|mtime|ctime|effective>] [bin_dir ...]
 *   ./ereport [--bucket-details N] [--report-dir DIR] [--subtree PATH] [--index-dir DIR] [--path-rewrite OLD=NEW] [--verbose] [<atime|mtime|ctime|effective>] [bin_dir ...]
 *   When the time argument is omitted (single-user form), age buckets use effective time: max(atime,mtime,ctime).
 *     --bucket-details N (optional): emit N levels of per-bucket directory tables (1…32); if omitted,
 *     bucket pages are brief summaries only.
 *     --report-dir DIR (optional): write reports under DIR/(sanitized user or all_users)/ instead of cwd.
 *     --index-dir DIR (optional): dirs.idx / rowgroups.idx from `ereport_index --make`. Only --subtree uses them,
 *     and only as a shortcut to the same answer; anything unusable about them falls back silently.
 *     --verbose (optional): per-call I/O counters and rolling throughput samples (default: quiet progress,
 *     no I/O counter atomics on hot paths). While verbose, stderr prints ecrawl-style `key=value` progress
 *     about every 30s (idle counters omitted). Thread count remains EREPORT_THREADS.
 *     Purple C-led heat-map badge threshold: EREPORT_HEAT_CTIME_LED_MIN_SHARE (optional float in (0,1], default 0.30).
 *     Flags must appear first (in any order), before username/time basis.
 *     (omit username: aggregate report for all UIDs in the crawl; output under ./all_users/)
 * Parallel thread count: EREPORT_THREADS (default 32); see worker_main / stats_thread / bucket HTML emit.
 * EREPORT_BUCKET_CELL_CONCURRENCY (optional 1..1024): how many bucket cells emit at once in the
 *   bucket-HTML phase. Each cell aggregates with its own inner thread pool, so with a large
 *   EREPORT_THREADS, running all 36 cells at once oversubscribes early and starves the slow-cell
 *   tail. Default: ~EREPORT_THREADS/16 (min 4) when EREPORT_THREADS>=64, else unchanged. Lower it
 *   (e.g. 4 or 8) to give the heaviest cells more inner threads; raise it for many small cells.
 * Multiple bin_dir values merge shard files from each crawl output directory (one user’s shards,
 * or every shard when aggregating all users).
 *
 * Writes outputs under ./<resolved_username>/ or ./all_users/ for aggregate mode (cwd).
 * Falls back to ./tmp/ only if the directory name is empty or unusably long after sanitization.
 */

#define _GNU_SOURCE /* qsort_r for thread-safe index sorts (parallel path-order merge sort). */
#define _XOPEN_SOURCE 700

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <dirent.h>
#include <errno.h>
#include <pwd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/time.h>

#include "alloc_tuning.h"
#include "crawl_bin_block.h"
#include "crawl_bin_catalog.h"
#include "crawl_bin_chunks.h"
#include "crawl_ckpt.h"
#include "crawl_fpcache.h"
#include "crawl_sidecar.h"
#include "path_canon.h"
#include "path_utils.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define DEFAULT_THREADS 32
/* Matched-record finalize: slice huge per-worker shards across many memcpy tasks so one core cannot dominate. */
#define MR_FIN_RECORDS_PER_TASK (1u << 19)
#define WINDOW_SECONDS 10
#define PROGRESS_FLUSH_INTERVAL 1024U
#define PARSE_CHUNK_BYTES CRAWL_BIN_PARSE_CHUNK_BYTES
#define PARSE_CHUNK_MIN_BYTES (1ULL << 20)
#define BUCKET_DETAIL_LEVELS_MAX 32
#define BUCKET_PATH_TABLE_MAX_ROWS 200
#define BUCKET_SHAPE_DRILL_MAX_ROWS 10
/* C-led: ctime must exceed max(atime,mtime) by at least this many seconds ("substantially newer"; hardcoded). */
#define CTIME_LED_MIN_DELTA_SEC (180ULL * 86400ULL)
/* Purple C-led badges (heat map, bucket pill) only when C-led bytes are at least this fraction of the slice. */
#define CTIME_LED_BADGE_MIN_SHARE_FRAC (0.30)
static double g_ctime_led_badge_min_share_frac = CTIME_LED_BADGE_MIN_SHARE_FRAC;

/* Path-shape badges (heat map): deep paths, dense parent directories, skew = both. */
#define PATH_SHAPE_DEEP_MIN_SLASHES 12U
#define PATH_SHAPE_BADGE_MIN_SHARE_FRAC (0.30)
/* Dense heat-map badge: parent dir must have at least this many immediate children (all types) in the crawl. */
#define PATH_SHAPE_DENSE_MIN_CHILDREN (8u * 1024u)
#define PATH_SHAPE_MIN_BUCKET_FILES 20ULL
/*
 * Per-(age,size)-cell parent map buckets. The bucket array is now per-map dynamic
 * (see dense_cell_map_t / dense_cell_reserve): it starts tiny and doubles as the map
 * fills, keeping the average collision chain ~<=1 regardless of how many distinct
 * parents land in the map. This is critical at scale — on a whole-corpus all-users run
 * the crawl-wide parent_fanout map holds tens of millions of distinct directories; a
 * fixed 4096-bucket table degraded to ~10k-long chains and ~80% of CPU in the strcmp
 * walk (measured). Sparse per-cell maps stay small instead of each pre-allocating 32 KiB.
 */
#define DENSE_PARENT_BUCKETS_INIT 64U   /* initial bucket count for a freshly-populated map (power of two) */
/*
 * The three per-worker tables whose key is "every parent directory this worker sees" (the two parent
 * interns and the crawl-wide fanout maps) know their size in advance: it is the capture's directory count
 * spread over the workers. Starting them at DENSE_PARENT_BUCKETS_INIT made each worker walk ten doublings
 * to get there, and a doubling is a calloc + rehash + free of a multi-megabyte array — read_one_chunk spent
 * a third of its cycles in the fault handler, a tenth of that in TLB-shootdown IPIs to all cores.
 *
 * The estimate is an average, so it is only a starting point (the tables still double if a worker draws
 * more chunks than its share) and it is capped: guessing high must cost a bounded amount of memory per
 * table, not hundreds of megabytes. Per-(age,size)-cell maps are deliberately left tiny, since their keys
 * are one bucket's worth of parents rather than all of them.
 */
#define PARENT_TABLE_RESERVE_MAX_SLOTS (1U << 18)
/* The merged crawl-wide maps exist once per run, not once per worker, and their key count is exactly the
 * capture's directory count, so they can be sized to it outright — growing into the same size costs the
 * rehashes this is here to avoid. */
#define MERGED_TABLE_RESERVE_MAX_BUCKETS (1U << 24)
#define SUMMARY_REDUCE_MIN_CHUNK 4
/* Parent-directory map merge: use threads inside each merge when the tournament has few pairs (late rounds). */
#define DENSE_CELL_MERGE_PARALLEL_MIN_NODES 4096u
/*
 * dense_cell_max_fanout_among_parents: inner bucket-range workers. Capped so path_shape’s first phase
 * (up to 36 cell workers) does not multiply into hundreds of simultaneous pthreads.
 */
#define FANOUT_MAX_INNER_NT 32
#define FANOUT_MAX_PAR_MIN_NODES 2048u
#define PATH_ORD_QSORT_THRESH (65536u)
/* Deeper merge-sort split → more pthread spawns for huge matched-record corpora (bucket path order). */
#define PATH_ORD_MAX_MS_DEPTH 12
/*
 * Bucket HTML: aggregate_totals_for_page_n walks matched records under base_prefix. Parallel path shards the
 * record range; each worker accumulates into a private open-addressed table holding only the rows its slice
 * touches (about R/nw of them), then adds those counts into the shared rows with relaxed atomics. Total table
 * memory is O(R) and is capped by AGG_TOTALS_PAR_MATRIX_BUDGET_BYTES; rows past a table's load limit are added
 * atomically as they are seen, so a badly skewed slice costs contention rather than memory.
 *
 * A dense nw×R×3×uint64 matrix was the earlier shape. It is not viable at scale: a row's index is its slot in
 * the path_row_map, so a worker's rows are spread over the whole vector and every worker faults in and memsets
 * all R rows — 27% of an ereport run was in the page-fault handler under that memset.
 */
#define AGG_TOTALS_PAR_MIN_RECORDS (8192ULL)
#define AGG_TOTALS_PAR_MATRIX_BUDGET_BYTES (1536ULL * 1024ULL * 1024ULL)
#define AGG_BKT_PAR_MIN_DETAILS 4096u
#define AGG_BKT_PAR_MAX_THREADS 24

static int parse_ereport_thread_count(void);
static int parse_bucket_cell_concurrency(void);

static void ereport_free_bin_dirs_list(const char **dirs, size_t n) {
    size_t k;
    if (!dirs) return;
    for (k = 0; k < n; k++) free((void *)dirs[k]);
    free((void *)dirs);
}

typedef enum {
    TIME_ATIME = 0,
    TIME_MTIME = 1,
    TIME_CTIME = 2,
    TIME_EFFECTIVE = 3
} time_basis_t;

enum { SIZE_BUCKETS = 6 };
enum { AGE_BUCKETS = 6 };

static const char *size_bucket_names[SIZE_BUCKETS] = {
    "< 4K",
    "4K – 1M",
    "1M – 100M",
    "100M – 1G",
    "1G – 10G",
    "10G+"
};

static const char *age_bucket_names[AGE_BUCKETS] = {
    "< 30 days",
    "30–90 days",
    "90–180 days",
    "180 days – 1 yr",
    "1–3 years",
    "3+ years"
};

/* crawl_manifest.txt crawl_*epoch / crawl_elapsed_sec (written by ecrawl); aggregated for merged reports. */
typedef struct {
    int valid;
    time_t wall_start;
    time_t wall_end;
    double elapsed_sec;
    int merged;
} ereport_crawl_timing_t;

/*
 * crawl_manifest.txt disk fields (written by ecrawl): summed when multiple bin_dir inputs are merged.
 * Interpretation: totals describe the full ecrawl run that produced each manifest (all UIDs in that crawl),
 * unlike heat-map byte totals which honor the report’s UID filter.
 */
typedef struct {
    int valid;
    uint32_t st_blocks_bytes_unit;
    uint64_t total_allocated_bytes;
    uint64_t files_sparse_heuristic;
    int unit_mismatch;
} ereport_manifest_disk_t;

typedef struct {
    uint64_t bytes[AGE_BUCKETS][SIZE_BUCKETS];
    uint64_t files[AGE_BUCKETS][SIZE_BUCKETS];
    /* Regular files whose ctime exceeds max(atime,mtime) by at least CTIME_LED_MIN_DELTA_SEC (inode/metadata recency). */
    uint64_t ctime_led_bytes[AGE_BUCKETS][SIZE_BUCKETS];
    uint64_t ctime_led_files[AGE_BUCKETS][SIZE_BUCKETS];
    uint64_t total_bytes;
    uint64_t total_capacity_bytes;
    uint64_t total_files;
    uint64_t total_dirs;
    uint64_t total_links;
    uint64_t total_others;
    uint64_t total_other_bytes;
    uint64_t scanned_records;
    uint64_t matched_records;
    uint64_t matched_files;
    uint64_t matched_dirs;
    uint64_t matched_links;
    uint64_t matched_others;
    uint64_t scanned_input_files;
    uint64_t bad_input_files;
    uint64_t total_ctime_led_bytes;
    uint64_t total_ctime_led_files;
    /* Regular files whose stored path has at least PATH_SHAPE_DEEP_MIN_SLASHES slashes (byte-weighted like heat cells). */
    uint64_t shape_deep_bytes[AGE_BUCKETS][SIZE_BUCKETS];
    uint64_t total_shape_deep_bytes;
} summary_t;

static void summary_merge(summary_t *dst, const summary_t *src);

/*
 * Per-thread bump arena for matched-record / bucket-detail path strings.
 * Replaces hundreds of millions of per-string strdup()/free() pairs with a
 * handful of block allocations and one bulk free per worker, removing the
 * cross-thread allocator (malloc-arena) contention this used to generate.
 * Strings handed out stay valid until path_arena_destroy().
 */
#define PATH_ARENA_BLOCK_BYTES (1u << 20)

typedef struct path_arena_block {
    struct path_arena_block *next;
    size_t used;
    size_t cap;
    char data[];
} path_arena_block_t;

typedef struct {
    path_arena_block_t *head;
} path_arena_t;

static char *path_arena_dup(path_arena_t *a, const char *s) {
    size_t len = s ? strlen(s) : 0;
    size_t need = len + 1;
    path_arena_block_t *b = a->head;
    char *out;

    if (!b || (b->cap - b->used) < need) {
        size_t cap = (need > PATH_ARENA_BLOCK_BYTES) ? need : PATH_ARENA_BLOCK_BYTES;
        path_arena_block_t *nb = (path_arena_block_t *)malloc(sizeof(path_arena_block_t) + cap);
        if (!nb) return NULL;
        nb->next = a->head;
        nb->used = 0;
        nb->cap = cap;
        a->head = nb;
        b = nb;
    }
    out = b->data + b->used;
    if (len) memcpy(out, s, len);
    out[len] = '\0';
    b->used += need;
    return out;
}

/* Like path_arena_dup but for a (possibly non-NUL-terminated) length-delimited slice. Used to
 * intern parent-directory prefixes (the path up to and including the trailing '/') so that all
 * records under one directory share a single copy of the parent bytes. */
static char *path_arena_dup_n(path_arena_t *a, const char *s, size_t len) {
    size_t need = len + 1;
    path_arena_block_t *b = a->head;
    char *out;

    if (!b || (b->cap - b->used) < need) {
        size_t cap = (need > PATH_ARENA_BLOCK_BYTES) ? need : PATH_ARENA_BLOCK_BYTES;
        path_arena_block_t *nb = (path_arena_block_t *)malloc(sizeof(path_arena_block_t) + cap);
        if (!nb) return NULL;
        nb->next = a->head;
        nb->used = 0;
        nb->cap = cap;
        a->head = nb;
        b = nb;
    }
    out = b->data + b->used;
    if (len && s) memcpy(out, s, len);
    out[len] = '\0';
    b->used += need;
    return out;
}

static void path_arena_destroy(path_arena_t *a) {
    path_arena_block_t *b;
    if (!a) return;
    b = a->head;
    while (b) {
        path_arena_block_t *n = b->next;
        free(b);
        b = n;
    }
    a->head = NULL;
}

/* Sum of bytes the arena actually holds (block headers + capacity). Diagnostic only. */
static void path_arena_bytes(const path_arena_t *a, size_t *alloc_bytes, size_t *used_bytes) {
    const path_arena_block_t *b;
    size_t al = 0, us = 0;

    if (a) {
        for (b = a->head; b; b = b->next) {
            al += sizeof(*b) + b->cap;
            us += b->used;
        }
    }
    if (alloc_bytes) *alloc_bytes = al;
    if (used_bytes) *used_bytes = us;
}

/*
 * A retained path is stored split as parent + leaf rather than one contiguous string:
 *   - parent: the directory prefix INCLUDING its trailing '/', interned once per directory
 *             and shared by every record under it (and shared between the matched_records and
 *             per-cell details lists). This removes the dominant per-record cost on huge
 *             --bucket-details runs (the parent bytes were previously copied for every file).
 *   - leaf:   the final path component (basename), arena-duped once per record.
 * parent + leaf concatenated reproduces the original absolute path byte-for-byte, so every
 * consumer that needs the full path simply reconstructs it into a scratch buffer.
 */
typedef struct {
    const char *parent;
    const char *leaf;
    uint64_t size;
    uint8_t ctime_led;
} detail_record_t;

typedef struct {
    detail_record_t *items;
    size_t count;
    size_t cap;
    /* When set, path strings are owned by an external per-thread arena and must
     * not be individually freed by bucket_details_free. */
    path_arena_t *arena;     /* non-NULL: append copies into this arena */
    int paths_external;      /* non-zero: free() must not touch item paths */
} bucket_details_t;

typedef struct {
    const char *parent; /* directory prefix incl trailing '/', interned/shared (see detail_record_t) */
    const char *leaf;   /* final path component (basename) */
    uint8_t type;
    uint64_t size;
} matched_record_t;

typedef struct {
    matched_record_t *items;
    size_t count;
    size_t cap;
    path_arena_t *arena;     /* non-NULL: append copies into this arena */
    int paths_external;      /* non-zero: free() must not touch item paths */
} matched_records_t;

/*
 * Reconstruct the full path of a split (parent, leaf) record into caller scratch. parent already
 * carries its trailing '/', so the join is a plain concatenation that byte-exactly reproduces the
 * original absolute path. Returns buf (always NUL-terminated). Cheap: two memcpy of ~path length.
 */
static inline const char *path_join_pl(const char *parent, const char *leaf, char *buf, size_t bufsz) {
    size_t pl = parent ? strlen(parent) : 0;
    size_t ll = leaf ? strlen(leaf) : 0;
    if (bufsz == 0) return "";
    if (pl + ll + 1 > bufsz) {
        /* Defensive truncation; reconstructed paths fit PATH_MAX by construction. */
        if (pl >= bufsz) pl = bufsz - 1;
        if (pl) memcpy(buf, parent, pl);
        ll = (pl + ll + 1 > bufsz) ? (bufsz - 1 - pl) : ll;
        if (ll) memcpy(buf + pl, leaf, ll);
        buf[pl + ll] = '\0';
        return buf;
    }
    if (pl) memcpy(buf, parent, pl);
    if (ll) memcpy(buf + pl, leaf, ll);
    buf[pl + ll] = '\0';
    return buf;
}

/*
 * Split an absolute path into (parent-incl-slash, leaf). parent_len counts up to and including the
 * last '/'; leaf points at the basename within the same buffer. For "/c" -> parent "/" + leaf "c";
 * for "/a/b/c" -> parent "/a/b/" + leaf "c". Absolute crawl paths always contain a '/'.
 */
static inline void path_split_pl(const char *path, size_t *parent_len, const char **leaf) {
    const char *slash = path ? strrchr(path, '/') : NULL;
    if (slash) {
        *parent_len = (size_t)(slash - path) + 1; /* include the slash */
        *leaf = slash + 1;
    } else {
        /* No slash (degenerate / relative): empty parent, whole string is the leaf. */
        *parent_len = 0;
        *leaf = path ? path : "";
    }
}

/*
 * Per-chunk parent-directory interner. Maps a parent prefix (the bytes up to and including the last
 * '/') to a single arena-owned copy, so every record under a directory shares one parent string
 * (and that string is shared between the matched_records and details lists). This is the core of the
 * large-dataset memory reduction: parent bytes used to be re-stored for every file.
 *
 * A last-hit cache keyed by parent_dir_id short-circuits contiguous same-directory runs (the common
 * case; mirrors the existing shape/fanout/dir caches, which likewise trust parent_dir_id as a stable
 * per-chunk directory key). Non-contiguous repeats fall back to an open-addressing hash probe.
 */
typedef struct {
    const char *parent; /* arena-owned, NUL-terminated, includes trailing '/' */
    uint32_t hash;
    uint32_t len;
} pintern_slot_t;

typedef struct {
    pintern_slot_t *slots;
    size_t nslots; /* power of two, 0 until first insert */
    size_t count;
    size_t reserve_slots; /* size of the first allocation; 0 = start at DENSE_PARENT_BUCKETS_INIT */
    path_arena_t *arena;
    uint64_t last_id;
    const char *last_parent;
    int last_ok;
} parent_intern_t;

static inline uint32_t pintern_hash(const char *s, size_t n) {
    uint32_t h = 2166136261u;
    size_t i;
    for (i = 0; i < n; i++) { h ^= (unsigned char)s[i]; h *= 16777619u; }
    return h;
}

/*
 * Power-of-two table size that holds expect_keys without a rehash, for a table that grows once it is
 * per_key_num/per_key_den full, clamped to max_slots. Returns 0 for an unknown count, meaning "start
 * small and double". See PARENT_TABLE_RESERVE_MAX_SLOTS for why the answer is clamped at all.
 */
static size_t parent_table_size_for(size_t expect_keys, size_t per_key_num, size_t per_key_den,
                                    size_t max_slots) {
    size_t need;
    size_t n = DENSE_PARENT_BUCKETS_INIT;

    if (expect_keys == 0) return 0;
    need = expect_keys * per_key_num / per_key_den + 1U;
    while (n < need && n < max_slots) n <<= 1;
    return n;
}

/* expect_parents: distinct parent directories this interner is expected to hold (0 if unknown). */
static void pintern_init(parent_intern_t *t, path_arena_t *arena, size_t expect_parents) {
    t->slots = NULL;
    t->nslots = 0;
    t->count = 0;
    /* pintern_get grows at 3/4 load. */
    t->reserve_slots = parent_table_size_for(expect_parents, 4, 3, PARENT_TABLE_RESERVE_MAX_SLOTS);
    t->arena = arena;
    t->last_id = 0;
    t->last_parent = NULL;
    t->last_ok = 0;
}

static void pintern_free(parent_intern_t *t) {
    free(t->slots);
    t->slots = NULL;
    t->nslots = 0;
    t->count = 0;
}

static int pintern_grow(parent_intern_t *t, size_t want) {
    size_t newn = t->nslots ? t->nslots : 64;
    pintern_slot_t *ns;
    size_t i;

    while (newn < want) newn <<= 1;
    ns = (pintern_slot_t *)calloc(newn, sizeof(*ns));
    if (!ns) return -1;
    alloc_hint_hugepages(ns, newn * sizeof(*ns));
    alloc_prefault(ns, newn * sizeof(*ns));
    for (i = 0; i < t->nslots; i++) {
        size_t idx;
        if (!t->slots[i].parent) continue;
        idx = t->slots[i].hash & (newn - 1);
        while (ns[idx].parent) idx = (idx + 1) & (newn - 1);
        ns[idx] = t->slots[i];
    }
    free(t->slots);
    t->slots = ns;
    t->nslots = newn;
    return 0;
}

static const char *pintern_get(parent_intern_t *t, uint64_t parent_id, const char *src, size_t plen) {
    size_t mask;
    size_t idx;
    char *dup;
    uint32_t h;

    /* Contiguous same-directory runs (the common crawl-bin layout) short-circuit here without
     * hashing the prefix at all — the prefix FNV was a top read-phase cost at 950M+ records. */
    if (t->last_ok && t->last_id == parent_id) return t->last_parent;

    h = pintern_hash(src, plen);

    if (t->nslots == 0) {
        if (pintern_grow(t, t->reserve_slots ? t->reserve_slots : 64) != 0) return NULL;
    } else if ((t->count + 1) * 4 >= t->nslots * 3) {
        if (pintern_grow(t, t->nslots * 2) != 0) return NULL;
    }

    mask = t->nslots - 1;
    idx = h & mask;
    while (t->slots[idx].parent) {
        if (t->slots[idx].hash == h && t->slots[idx].len == plen &&
            memcmp(t->slots[idx].parent, src, plen) == 0) {
            t->last_ok = 1;
            t->last_id = parent_id;
            t->last_parent = t->slots[idx].parent;
            return t->slots[idx].parent;
        }
        idx = (idx + 1) & mask;
    }

    dup = path_arena_dup_n(t->arena, src, plen);
    if (!dup) return NULL;
    t->slots[idx].parent = dup;
    t->slots[idx].hash = h;
    t->slots[idx].len = (uint32_t)plen;
    t->count++;
    t->last_ok = 1;
    t->last_id = parent_id;
    t->last_parent = dup;
    return dup;
}

typedef crawl_bin_file_chunk_t file_chunk_t;

typedef struct {
    file_chunk_t *chunks;
    size_t count;
    size_t next_index;
    pthread_mutex_t mutex;
} work_queue_t;

typedef struct {
    atomic_uint remaining_chunks;
    crawl_bin_catalog_t *catalog;
    /*
     * --subtree via dirs.idx. subtree_bits has one bit per dir_id, set when that directory is at
     * or under the subtree root *in this shard*; the scan then tests a record's parent_dir_id
     * against it instead of rebuilding the record's path. NULL when the sidecar route is not in
     * use, which is what the scan reads to decide between the two.
     *
     * subtree_root_parent is the root directory's own parent_dir_id here, because the root's own
     * record hangs off that parent -- outside the bitmap -- and `path == prefix` is in scope.
     */
    unsigned char *subtree_bits;
    uint64_t subtree_root_parent;
} file_state_t;

typedef struct {
    uint32_t dev_major;
    uint32_t dev_minor;
    uint64_t inode;
} inode_key_t;

#define INODE_SET_SHARDS 64

typedef struct {
    inode_key_t *keys;
    unsigned char *used;
    size_t cap;
    size_t count;
    pthread_mutex_t mutex;
} inode_set_shard_t;

typedef struct {
    inode_set_shard_t shard[INODE_SET_SHARDS];
} inode_set_t;

typedef struct {
    uint64_t *keys;
    unsigned char *used;
    size_t cap;
    size_t count;
} uid_accum_t;

typedef struct {
    uint64_t scanned_input_files;
    uint64_t scanned_records;
    uint64_t matched_records;
    uint64_t bad_input_files;
} progress_local_t;

/* Per-run stats for HTML report generation (stack in `main`; no file-scope progress atomics). */
typedef struct ereport_run_stats {
    atomic_ullong scanned_input_files;
    atomic_ullong scanned_records;
    atomic_ullong matched_records;
    atomic_ullong bad_input_files;
    atomic_int stop_stats;
    /* Set after all parse worker threads finish — merges/HTML emit run with no new scanned_records. */
    atomic_int parse_workers_done;
    /* 1=merging worker summaries, 2=writing bucket_*.html, 3=writing index.html (0=unused). */
    atomic_int finalize_phase;
    /* Phase 1: 1=fold summaries 2=matched paths 3=directory maps 4=UID merge (all_users). */
    atomic_int finalize_merge_substep;
    /* Phase 2: 1=corpus totals + path ordering before workers; 0=emitting bucket_*.html cells. */
    atomic_int finalize_bucket_prep;
    /* Phase 3: 1..6 coarse checkpoints while writing index.html (see ereport_index_prog). */
    atomic_uint finalize_index_step;
    /* Phase 1 substep 3: parallel dense merge; 0..AGE*SIZE while bucket_dense_cells_finalize_parallel runs. */
    atomic_uint finalize_dense_cells_done;
    /*
     * After dense cells reach 36/36: 1=repartition merged crawl-wide fanout into 65k lookup (steal);
     * 2=path_shape per-cell fanout scan; 3=path_shape row/column/all margins. 0=earlier sub-steps.
     */
    atomic_uint finalize_lookup_stage;
    atomic_uint finalize_bucket_done; /* cells written toward 36 */
    /*
     * Verbose runtime only: substep 3 prelude — merging each parse worker's parent_fanout (done/total workers).
     * Cleared when dense cell merge starts.
     */
    atomic_int finalize_fanout_workers_total;
    atomic_int finalize_fanout_workers_done;
    /*
     * Verbose runtime only: during finalize_merge_substep==2 parallel matched-record memcpy (slice done/total).
     * Cleared when matched finalize returns.
     */
    atomic_int finalize_matched_slices_total;
    atomic_int finalize_matched_slices_done;
    uint64_t input_files_total;
    atomic_ullong chunk_prep_files_done;
    uint64_t chunk_prep_files_total;
    /* During chunk-map: each prep thread sets its slot to the .bin path it is scanning (else NULL). */
    volatile const char **chunk_map_worker_paths;
    int chunk_map_path_slots;
    double run_start_sec;
    double records_rate_sum;
    double records_rate_min;
    double records_rate_max;
    uint64_t records_rate_samples;
    /*
     * --verbose machine-readable phase timing (emit_run_stats). Wall times are main-thread elapsed where noted;
     * corpus vs path-index prep may overlap — see phase_bucket_prep_note in emit_run_stats.
     */
    double vt_chunk_map_sec;
    double vt_parse_workers_sec;
    double vt_fini_summaries_sec;
    double vt_fini_matched_paths_sec;
    double vt_fini_directory_maps_dense_sec;
    double vt_fini_path_shape_sec;
    double vt_fini_uid_sec;
    double vt_bucket_prep_wall_sec;
    double vt_bucket_prep_corpus_sec;
    double vt_bucket_cells_wall_sec;
    double vt_index_html_sec;
    atomic_uint_fast64_t vt_ns_path_sort;
    atomic_uint_fast64_t vt_ns_bucket_emit_sort;
} ereport_run_stats_t;

static void ereport_run_stats_reset(ereport_run_stats_t *s) {
    atomic_store(&s->scanned_input_files, 0);
    atomic_store(&s->scanned_records, 0);
    atomic_store(&s->matched_records, 0);
    atomic_store(&s->bad_input_files, 0);
    atomic_store(&s->stop_stats, 0);
    atomic_store(&s->parse_workers_done, 0);
    atomic_store(&s->finalize_phase, 0);
    atomic_store(&s->finalize_merge_substep, 0);
    atomic_store(&s->finalize_bucket_prep, 0);
    atomic_store(&s->finalize_index_step, 0);
    atomic_store(&s->finalize_dense_cells_done, 0);
    atomic_store(&s->finalize_lookup_stage, 0);
    atomic_store(&s->finalize_bucket_done, 0);
    atomic_store(&s->finalize_fanout_workers_total, 0);
    atomic_store(&s->finalize_fanout_workers_done, 0);
    atomic_store(&s->finalize_matched_slices_total, 0);
    atomic_store(&s->finalize_matched_slices_done, 0);
    s->input_files_total = 0;
    atomic_store(&s->chunk_prep_files_done, 0);
    s->chunk_prep_files_total = 0;
    s->chunk_map_worker_paths = NULL;
    s->chunk_map_path_slots = 0;
    s->run_start_sec = 0.0;
    s->records_rate_sum = 0.0;
    s->records_rate_min = 0.0;
    s->records_rate_max = 0.0;
    s->records_rate_samples = 0;
    s->vt_chunk_map_sec = 0.0;
    s->vt_parse_workers_sec = 0.0;
    s->vt_fini_summaries_sec = 0.0;
    s->vt_fini_matched_paths_sec = 0.0;
    s->vt_fini_directory_maps_dense_sec = 0.0;
    s->vt_fini_path_shape_sec = 0.0;
    s->vt_fini_uid_sec = 0.0;
    s->vt_bucket_prep_wall_sec = 0.0;
    s->vt_bucket_prep_corpus_sec = 0.0;
    s->vt_bucket_cells_wall_sec = 0.0;
    s->vt_index_html_sec = 0.0;
    atomic_init(&s->vt_ns_path_sort, 0);
    atomic_init(&s->vt_ns_bucket_emit_sort, 0);
}

#define EREPORT_INDEX_PROGRESS_STEPS 6u

static void ereport_index_prog(ereport_run_stats_t *rs, unsigned step) {
    if (rs && step >= 1u && step <= EREPORT_INDEX_PROGRESS_STEPS)
        atomic_store(&rs->finalize_index_step, step);
}

static const char *ereport_finalize_merge_substep_cstr(int m) {
    switch (m) {
        case 1:
            return "worker summaries";
        case 2:
            return "matched paths";
        case 3:
            return "directory maps";
        case 4:
            return "UID tallies";
        default:
            return "";
    }
}

static const char *ereport_finalize_index_step_cstr(unsigned step) {
    static const char *const lab[6] = {"path search", "heatmap", "bucket help", "stats", "drawer", "finishing"};
    if (step >= 1u && step <= 6u) return lab[step - 1u];
    return "";
}

typedef struct dense_node {
    struct dense_node *next;
    /* For an "owning" node (map->parents_external == 0) this points into the trailing bytes
     * of this same malloc block (see dense_node_new) and is freed with the node. For an
     * "external" node (map->parents_external == 1) it instead references a parent string
     * interned in a per-worker arena that outlives the map (see dense_pintern); the node is a
     * bare struct (no trailing bytes) and free(node) must not free the parent. Either way
     * free(node) is correct because the owned copy lives inside the node's own allocation. */
    char *parent;
    uint64_t n;
    uint32_t hash; /* full 32-bit FNV of parent; bucket = hash & (nbuckets-1) */
} dense_node_t;

typedef struct {
    /* Dynamically grown bucket array (power-of-two size in nbuckets; NULL/0 until the
     * first insert). Index is hash & (nbuckets-1). dense_cell_reserve doubles + rehashes
     * when the load factor reaches ~1 so chains stay short on huge many-parent maps. */
    dense_node_t **buckets;
    size_t nbuckets;
    /* Size of the first bucket allocation when the expected parent count is known (crawl-wide maps);
     * 0 = start at DENSE_PARENT_BUCKETS_INIT and double, which is what the per-cell maps want. */
    size_t reserve_buckets;
    /* Last-hit cache keyed by parent_dir_id: consecutive records under the same
     * directory (the common crawl-bin layout, esp. mega-dirs) then skip hashing
     * and strcmp entirely. Only used by the *_cached accumulate path; merge/insert
     * helpers leave it NULL. */
    dense_node_t *last;
    uint64_t last_id;
    /* Live count of distinct nodes (parents) in this map. Maintained by the add
     * helpers (incremented only when a brand-new node is created). Node moves during
     * merge/steal never create nodes, so this stays exact for any populated map; the
     * parallel-vs-serial decisions read it (O(1)) instead of walking every chain. */
    size_t count;
    /* When 1, newly created nodes reference an externally-owned (interned) parent string
     * instead of copying it into trailing bytes. Set on the per-worker per-cell dense maps,
     * whose parents are interned once per worker (dense_pintern) and shared across all 36
     * age/size cells + fanout, eliminating the dominant per-cell parent-string duplication.
     * Maps that are fed ephemeral (stack) parent strings (e.g. the deep-parents drilldown
     * temporaries) must leave this 0 so they keep owning their own copy. */
    int parents_external;
} dense_cell_map_t;

typedef struct {
    /* Crawl-wide parent -> immediate-child-count lookup. Dynamically sized to the parent count
     * (power-of-two, mask indexing) so chains stay ~1 even with hundreds of millions of
     * directories; a fixed bucket array degraded to thousands-deep strcmp chains per probe and
     * dominated finalize CPU on large datasets. NULL/0 until built. */
    dense_node_t **buckets;
    size_t nbuckets;
} dense_cell_fanout_lookup_t;

/*
 * Crawl-wide immediate-child breakdown under each parent directory (matched records only).
 * Used for Dense drill-down: files vs dirs vs other types, min/max of the report time basis across children.
 */
typedef struct fanout_parent_stat_node {
    struct fanout_parent_stat_node *next;
    char *parent;
    uint64_t n_files;
    uint64_t n_dirs;
    uint64_t n_others;
    uint64_t t_min;
    uint64_t t_max;
    uint32_t hash; /* full 32-bit FNV of parent */
    unsigned char have_time;
} fanout_parent_stat_node_t;

typedef struct {
    /* Dynamically grown bucket array (power-of-two nbuckets; NULL/0 until first insert),
     * indexed by hash & (nbuckets-1). Grows + rehashes at load factor ~1 so chains stay
     * short even at tens of millions of distinct parents (a fixed table degraded to long
     * chains and a large share of CPU in the accumulate strcmp walk). */
    fanout_parent_stat_node_t **buckets;
    size_t nbuckets;
    size_t reserve_buckets; /* as in dense_cell_map_t */
    fanout_parent_stat_node_t *last; /* last-hit cache keyed by parent_dir_id */
    uint64_t last_id;
    size_t count;
    /* When 1, node->parent references an externally-owned (per-worker interned) string instead of
     * a strdup'd copy: the accumulator stores the interned pointer it is handed, and neither the
     * merge nor the free touches it. The fanout-stat parent is the same per-worker interned
     * no-slash prefix the dense maps share, so this drops a full duplicate copy of every parent
     * path (~40 GiB on the production crawl). Requires the caller to always pass that interned
     * pointer (true whenever the worker arena exists). */
    int parents_external;
} fanout_parent_stat_map_t;

typedef struct {
    uint64_t deep_bytes;
    /* Max immediate-child count (crawl-wide) among parent dirs of regular files represented in this slice. */
    uint64_t dense_fanout_max;
} path_shape_slice_t;

typedef struct {
    path_shape_slice_t cell[AGE_BUCKETS][SIZE_BUCKETS];
    path_shape_slice_t row[AGE_BUCKETS];
    path_shape_slice_t col[SIZE_BUCKETS];
    path_shape_slice_t all;
} path_shape_view_t;

static void emit_heat_map_badges(FILE *out,
                                 int heat_badges,
                                 int show_ctime_led,
                                 const char *ctime_led_label,
                                 uint64_t ctime_led_bytes,
                                 uint64_t deep_bytes,
                                 uint64_t bucket_bytes,
                                 uint64_t bucket_files,
                                 uint64_t dense_fanout_max,
                                 const char *basis_str,
                                 const char *bucket_scope);

typedef struct {
    work_queue_t *queue;
    file_state_t *file_states;
    uid_t target_uid;
    int all_users;
    int bucket_detail_levels;
    time_basis_t basis;
    time_t now;
    inode_set_t *seen_inodes;
    uid_accum_t uid_distinct;
    summary_t summary;
    bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS];
    matched_records_t matched_records;
    /* Backing storage for all path strings appended by this worker (matched
     * records + bucket details). Bulk-freed once via path_arena_destroy. */
    path_arena_t path_arena;
    ereport_run_stats_t *run_stats;
    /* Distinct parent directories this worker is expected to see: the capture's directory count over the
     * worker count. Sizes the parent interns and the two crawl-wide maps below; 0 leaves them growing. */
    size_t expect_parents;
    dense_cell_map_t dense_maps[AGE_BUCKETS][SIZE_BUCKETS];
    /* Crawl-wide: immediate children per parent dir (all matched types with paths). */
    dense_cell_map_t parent_fanout;
    fanout_parent_stat_map_t parent_fanout_stats;
} worker_arg_t;

static atomic_ullong g_io_opendir_calls = 0;
static atomic_ullong g_io_readdir_calls = 0;
static atomic_ullong g_io_closedir_calls = 0;
static atomic_ullong g_io_fopen_calls = 0;
static atomic_ullong g_io_fclose_calls = 0;
static atomic_ullong g_io_fread_calls = 0;
static atomic_ullong g_bucket_records[WINDOW_SECONDS];
static atomic_ullong g_window_records = 0;
static atomic_int g_bucket_index = 0;
static atomic_uint g_seconds_seen = 0;
/* Like ecrawl: default quiet (no per-read I/O atomics; sparse progress). --verbose enables full accounting + stderr progress. */
static int g_ereport_verbose = 0;

/* --subtree <abs-path>: when non-NULL, restrict the whole analysis to records whose reconstructed full
 * path is at or under this directory, as if only that subtree had been crawled. Full absolute paths are
 * retained in the report (records are filtered, not rewritten). Forces per-record path reconstruction
 * even when --bucket-details is off (the skip_paths fast path needs the path to test the prefix). */
static const char *g_subtree_prefix = NULL;
static size_t g_subtree_prefix_len = 0;
/* Derived once from the prefix: the last component (the subtree's own directory name) and the
 * directory holding it ("" when that is the capture root). Both are what the dirs.idx route needs
 * and neither depends on any shard, so they are computed where the option is parsed. */
static const char *g_subtree_base = NULL;
static size_t g_subtree_base_len = 0;
static char g_subtree_parent[PATH_MAX];
/* --subtree "/" matches every absolute path, so there is nothing for an index to narrow. */
static int g_subtree_is_root = 0;

/*
 * --index-dir DIR: where `ereport_index --make` left dirs.idx / rowgroups.idx.
 *
 * Only --subtree uses them, and only as a shortcut to what the scan already computes: the root is
 * resolved once per shard, row groups whose DFS sketch cannot reach it are never opened, and
 * membership becomes a bit test on the record's parent_dir_id instead of a rebuilt path and a
 * string compare. A missing, stale or truncated sidecar, or one that does not name every shard
 * being read, is ignored outright -- the run then does exactly what it did before sidecars
 * existed, to the byte.
 */
static const char *g_index_dir = NULL;
/* What rowgroups.idx removed from this run's scan. Reported, never acted on twice. */
static crawl_rgix_prune_stats_t g_rgix_stats;
/* Set once the per-shard bitmaps are in place, which is the point of no return: from here the scan
 * answers --subtree from parent_dir_id. Separate from g_rgix_stats.used, since a capture small
 * enough that every row group survives still takes this route. */
static int g_subtree_from_index = 0;

/* --path-rewrite OLD=NEW: when g_rewrite_from is non-NULL, every reconstructed path at or under OLD has its
 * OLD prefix replaced with NEW (directory-boundary match), purely at read time — the on-disk bins are never
 * modified. Lets a crawl stored under one root be reported/heat-mapped as if it lived elsewhere
 * (e.g. relabel /data1/group as /orcd/data). Applied before --subtree, so --subtree is given in NEW terms. */
static const char *g_rewrite_from = NULL;
static size_t g_rewrite_from_len = 0;
static const char *g_rewrite_to = NULL;
static size_t g_rewrite_to_len = 0;

/* If path is at/under g_rewrite_from (directory boundary), rewrite that prefix to g_rewrite_to in place.
 * Both prefixes are normalized absolute dirs without trailing '/'. Returns 0 (no-op when no rewrite is set
 * or the path doesn't match); -1 if the result would not fit in bufsz (path left unchanged). */
static int starts_with_dir_prefix(const char *path, const char *prefix);
static int rewrite_path_prefix(char *path, size_t bufsz) {
    size_t plen, suffix_len, newlen;

    if (!g_rewrite_from || !path) return 0;
    if (!starts_with_dir_prefix(path, g_rewrite_from)) return 0;
    plen = strlen(path);
    /* g_rewrite_from has no trailing slash, so the remainder is "" (exact match) or "/...". */
    suffix_len = plen - g_rewrite_from_len;
    newlen = g_rewrite_to_len + suffix_len;
    if (newlen + 1 > bufsz) return -1;
    memmove(path + g_rewrite_to_len, path + g_rewrite_from_len, suffix_len + 1); /* include NUL */
    memcpy(path, g_rewrite_to, g_rewrite_to_len);
    return 0;
}

/* Manifest-driven crawl directory layout: "uid_shards" or "unsharded" (no uid-shard manifest). */
static const char *g_input_layout = "unsharded";
static uint32_t g_input_uid_shards = 0;
/* Set once from resolved login name via set_bucket_output_dir(); see main(). Not "." / not "tmp" unless fallback. */
static char g_bucket_output_dir[PATH_MAX];

typedef struct {
    char *path;
    uint64_t hash; /* cached full-path FNV-1a; reused by rehash + map merge so paths aren't re-hashed */
    uint32_t path_len; /* strlen(path); lets probes memcmp a known span instead of scanning for NUL */
    uint64_t bucket_files;
    uint64_t bucket_bytes;
    uint64_t bucket_ctime_led_files;
    uint64_t bucket_ctime_led_bytes;
    uint64_t total_files;
    uint64_t total_dirs;
    uint64_t total_bytes;
} path_row_t;

/*
 * Row keys live in the map's own bump arena rather than one strdup per row. A bucket page
 * inserts a row per distinct path prefix per level, so the old scheme meant a malloc per row
 * on the way in and a free per row on the way out, every page — and the maps are rebuilt for
 * every page by every aggregation worker. Blocks are kept when the map is cleared, so the
 * next page writes over memory that is already resident.
 */
#define PATH_KEY_BLK_BYTES (256U * 1024U)

typedef struct path_key_blk {
    struct path_key_blk *next;
    size_t len;
    size_t cap;
    char data[];
} path_key_blk_t;

typedef struct {
    path_row_t *rows;
    unsigned char *used;
    size_t cap;
    size_t count;
    path_key_blk_t *keys;     /* all blocks, in allocation order */
    path_key_blk_t *keys_cur; /* block currently being filled */
} path_row_map_t;

static uint64_t inode_key_hash(uint32_t dev_major, uint32_t dev_minor, uint64_t inode) {
    uint64_t x = inode;
    x ^= ((uint64_t)dev_major << 32) ^ (uint64_t)dev_minor;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static int inode_shard_rehash_locked(inode_set_shard_t *sh, size_t new_cap) {
    inode_key_t *new_keys = (inode_key_t *)calloc(new_cap, sizeof(*new_keys));
    unsigned char *new_used = (unsigned char *)calloc(new_cap, sizeof(*new_used));
    size_t i;

    if (!new_keys || !new_used) {
        free(new_keys);
        free(new_used);
        return -1;
    }

    for (i = 0; i < sh->cap; i++) {
        if (sh->used[i]) {
            inode_key_t key = sh->keys[i];
            size_t idx = (size_t)(inode_key_hash(key.dev_major, key.dev_minor, key.inode) & (new_cap - 1));
            while (new_used[idx]) idx = (idx + 1) & (new_cap - 1);
            new_keys[idx] = key;
            new_used[idx] = 1;
        }
    }

    free(sh->keys);
    free(sh->used);
    sh->keys = new_keys;
    sh->used = new_used;
    sh->cap = new_cap;
    return 0;
}

static int inode_set_init(inode_set_t *s, size_t initial_cap) {
    size_t per_shard_target;
    size_t cap;
    int si;

    if (!s) return -1;
    per_shard_target = (initial_cap + (size_t)INODE_SET_SHARDS) / (size_t)INODE_SET_SHARDS;
    if (per_shard_target < 32) per_shard_target = 32;
    cap = 1;
    while (cap < per_shard_target) cap <<= 1;

    for (si = 0; si < INODE_SET_SHARDS; si++) {
        inode_set_shard_t *sh = &s->shard[si];

        sh->keys = (inode_key_t *)calloc(cap, sizeof(*sh->keys));
        sh->used = (unsigned char *)calloc(cap, sizeof(*sh->used));
        if (!sh->keys || !sh->used) {
            int j;
            for (j = 0; j < si; j++) {
                free(s->shard[j].keys);
                free(s->shard[j].used);
                s->shard[j].keys = NULL;
                s->shard[j].used = NULL;
                s->shard[j].cap = 0;
                s->shard[j].count = 0;
                pthread_mutex_destroy(&s->shard[j].mutex);
            }
            free(sh->keys);
            free(sh->used);
            return -1;
        }
        sh->cap = cap;
        sh->count = 0;
        pthread_mutex_init(&sh->mutex, NULL);
    }
    return 0;
}

static void inode_set_destroy(inode_set_t *s) {
    int si;

    if (!s) return;
    for (si = 0; si < INODE_SET_SHARDS; si++) {
        inode_set_shard_t *sh = &s->shard[si];

        free(sh->keys);
        free(sh->used);
        sh->keys = NULL;
        sh->used = NULL;
        sh->cap = 0;
        sh->count = 0;
        pthread_mutex_destroy(&sh->mutex);
    }
}

static int inode_set_insert_if_new(inode_set_t *s, uint32_t dev_major, uint32_t dev_minor, uint64_t inode) {
    uint64_t hh;
    size_t si;
    inode_set_shard_t *sh;
    size_t idx;

    if (!s || inode == 0) return 1;

    hh = inode_key_hash(dev_major, dev_minor, inode);
    si = (size_t)(hh & ((uint64_t)INODE_SET_SHARDS - 1U));
    sh = &s->shard[si];

    pthread_mutex_lock(&sh->mutex);

    if (sh->cap > 0 && (sh->count + 1) * 10 >= sh->cap * 7) {
        if (inode_shard_rehash_locked(sh, sh->cap << 1) != 0) {
            pthread_mutex_unlock(&sh->mutex);
            return -1;
        }
    }

    idx = (size_t)(hh & (sh->cap - 1));
    while (sh->used[idx]) {
        inode_key_t *k = &sh->keys[idx];
        if (k->dev_major == dev_major && k->dev_minor == dev_minor && k->inode == inode) {
            pthread_mutex_unlock(&sh->mutex);
            return 0;
        }
        idx = (idx + 1) & (sh->cap - 1);
    }

    sh->used[idx] = 1;
    sh->keys[idx].dev_major = dev_major;
    sh->keys[idx].dev_minor = dev_minor;
    sh->keys[idx].inode = inode;
    sh->count++;

    pthread_mutex_unlock(&sh->mutex);
    return 1;
}

static uint64_t uid_hash64(uint64_t uid) {
    uint64_t x = uid;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static int uid_accum_init(uid_accum_t *s, size_t initial_cap) {
    size_t cap = 1;
    while (cap < initial_cap) cap <<= 1;

    s->keys = (uint64_t *)calloc(cap, sizeof(*s->keys));
    s->used = (unsigned char *)calloc(cap, sizeof(*s->used));
    if (!s->keys || !s->used) {
        free(s->keys);
        free(s->used);
        s->keys = NULL;
        s->used = NULL;
        s->cap = 0;
        s->count = 0;
        return -1;
    }

    s->cap = cap;
    s->count = 0;
    return 0;
}

static void uid_accum_destroy(uid_accum_t *s) {
    if (!s) return;
    free(s->keys);
    free(s->used);
    s->keys = NULL;
    s->used = NULL;
    s->cap = 0;
    s->count = 0;
}

static int uid_accum_rehash(uid_accum_t *s, size_t new_cap) {
    uint64_t *new_keys = (uint64_t *)calloc(new_cap, sizeof(*new_keys));
    unsigned char *new_used = (unsigned char *)calloc(new_cap, sizeof(*new_used));
    size_t i;

    if (!new_keys || !new_used) {
        free(new_keys);
        free(new_used);
        return -1;
    }

    for (i = 0; i < s->cap; i++) {
        if (s->used[i]) {
            uint64_t key = s->keys[i];
            size_t idx = (size_t)(uid_hash64(key) & (new_cap - 1));
            while (new_used[idx]) idx = (idx + 1) & (new_cap - 1);
            new_keys[idx] = key;
            new_used[idx] = 1;
        }
    }

    free(s->keys);
    free(s->used);
    s->keys = new_keys;
    s->used = new_used;
    s->cap = new_cap;
    return 0;
}

/* Single-threaded only. Returns 1 if newly inserted, 0 if already present, -1 on error. */
static int uid_accum_insert_if_new(uid_accum_t *s, uint64_t uid) {
    size_t idx;

    if ((s->count + 1) * 10 >= s->cap * 7) {
        if (uid_accum_rehash(s, s->cap << 1) != 0) return -1;
    }

    idx = (size_t)(uid_hash64(uid) & (s->cap - 1));
    while (s->used[idx]) {
        if (s->keys[idx] == uid) return 0;
        idx = (idx + 1) & (s->cap - 1);
    }

    s->used[idx] = 1;
    s->keys[idx] = uid;
    s->count++;
    return 1;
}

static size_t uid_accum_size(const uid_accum_t *s) {
    return s ? s->count : 0;
}

/* dst must be distinct from src; single-threaded; leaves src intact (caller frees src). */
static int uid_accum_merge_into(uid_accum_t *dst, const uid_accum_t *src) {
    size_t i;

    for (i = 0; i < src->cap; i++) {
        if (src->used[i]) {
            if (uid_accum_insert_if_new(dst, src->keys[i]) < 0) return -1;
        }
    }
    return 0;
}

/*
 * Tournament step: fold odd into even (merge uid sets). On OOM mid-merge, even may be partial and odd unchanged;
 * recover by merging even into odd, move survivor to the even slot (required for the next reduction round).
 */
static int uid_accum_merge_pair_fold(uid_accum_t *even, uid_accum_t *odd) {
    if (!even || !odd) return -1;
    if (uid_accum_merge_into(even, odd) == 0) {
        uid_accum_destroy(odd);
        memset(odd, 0, sizeof(*odd));
        return 0;
    }
    if (uid_accum_merge_into(odd, even) != 0) return -1;
    uid_accum_destroy(even);
    *even = *odd;
    memset(odd, 0, sizeof(*odd));
    return 0;
}

typedef struct {
    uid_accum_t *ubuf;
    atomic_int *next_pair;
    atomic_int *fail;
    int pairs;
} uid_pair_merge_ctx_t;

static void *uid_pair_merge_worker(void *vp) {
    uid_pair_merge_ctx_t *c = (uid_pair_merge_ctx_t *)vp;

    for (;;) {
        int p = atomic_fetch_add_explicit(c->next_pair, 1, memory_order_relaxed);

        if (p >= c->pairs) break;
        if (atomic_load_explicit(c->fail, memory_order_relaxed)) return NULL;
        if (uid_accum_merge_pair_fold(&c->ubuf[2 * p], &c->ubuf[2 * p + 1]) != 0) {
            atomic_store_explicit(c->fail, 1, memory_order_relaxed);
            return NULL;
        }
    }
    return NULL;
}

static void uid_accum_ubuf_destroy_slots(uid_accum_t *ubuf, int n) {
    int k;

    if (!ubuf || n < 1) return;
    for (k = 0; k < n; k++) {
        uid_accum_destroy(&ubuf[k]);
        memset(&ubuf[k], 0, sizeof(ubuf[k]));
    }
}

/*
 * Parallel pairwise tournament of per-worker uid_distinct maps (same steal pattern as fanout_shard_summaries_reduce_parallel).
 * Moves args[0..threads_used).uid_distinct into a temp buffer; on success args slots are cleared; on failure destroys ubuf
 * and returns -1 (merged left unchanged).
 */
static int uid_accum_reduce_workers_into(uid_accum_t *merged, worker_arg_t *args, int threads_used) {
    uid_accum_t *ubuf = NULL;
    int n;
    int pairs;
    int j;
    int k;
    int nw;
    int ti;
    int n_join;
    pthread_t *tids = NULL;
    uid_pair_merge_ctx_t *targs = NULL;
    atomic_int next_pair;
    atomic_int fail;
    int i;

    if (!merged || !args || threads_used < 1) return -1;

    if (threads_used == 1) {
        if (uid_accum_merge_into(merged, &args[0].uid_distinct) != 0) return -1;
        uid_accum_destroy(&args[0].uid_distinct);
        memset(&args[0].uid_distinct, 0, sizeof(args[0].uid_distinct));
        return 0;
    }

    ubuf = (uid_accum_t *)calloc((size_t)threads_used, sizeof(*ubuf));
    if (!ubuf) return -1;

    for (i = 0; i < threads_used; i++) {
        ubuf[i] = args[i].uid_distinct;
        memset(&args[i].uid_distinct, 0, sizeof(args[i].uid_distinct));
    }

    atomic_init(&fail, 0);
    n = threads_used;
    while (n > 1) {
        pairs = n / 2;
        if (pairs <= 0) break;

        nw = parse_ereport_thread_count();
        if (nw < 1) nw = 1;
        if (nw > pairs) nw = pairs;

        if (pairs < 2 || nw < 2) {
            for (j = 0; j < pairs; j++) {
                if (atomic_load_explicit(&fail, memory_order_relaxed)) {
                    uid_accum_ubuf_destroy_slots(ubuf, n);
                    free(ubuf);
                    return -1;
                }
                if (uid_accum_merge_pair_fold(&ubuf[2 * j], &ubuf[2 * j + 1]) != 0) {
                    atomic_store_explicit(&fail, 1, memory_order_relaxed);
                    uid_accum_ubuf_destroy_slots(ubuf, n);
                    free(ubuf);
                    return -1;
                }
            }
        } else {
            atomic_store_explicit(&next_pair, 0, memory_order_relaxed);
            tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
            targs = (uid_pair_merge_ctx_t *)calloc((size_t)nw, sizeof(*targs));
            if (!tids || !targs) {
                free(tids);
                free(targs);
                tids = NULL;
                targs = NULL;
                for (j = 0; j < pairs; j++) {
                    if (atomic_load_explicit(&fail, memory_order_relaxed)) {
                        uid_accum_ubuf_destroy_slots(ubuf, n);
                        free(ubuf);
                        return -1;
                    }
                    if (uid_accum_merge_pair_fold(&ubuf[2 * j], &ubuf[2 * j + 1]) != 0) {
                        atomic_store_explicit(&fail, 1, memory_order_relaxed);
                        uid_accum_ubuf_destroy_slots(ubuf, n);
                        free(ubuf);
                        return -1;
                    }
                }
            } else {
                n_join = 0;
                for (ti = 0; ti < nw; ti++) {
                    targs[ti].ubuf = ubuf;
                    targs[ti].next_pair = &next_pair;
                    targs[ti].fail = &fail;
                    targs[ti].pairs = pairs;
                    if (pthread_create(&tids[ti], NULL, uid_pair_merge_worker, &targs[ti]) != 0) break;
                    n_join++;
                }
                for (ti = 0; ti < n_join; ti++) pthread_join(tids[ti], NULL);
                if (!atomic_load_explicit(&fail, memory_order_relaxed)) {
                    for (;;) {
                        int p = atomic_fetch_add_explicit(&next_pair, 1, memory_order_relaxed);

                        if (p >= pairs) break;
                        if (uid_accum_merge_pair_fold(&ubuf[2 * p], &ubuf[2 * p + 1]) != 0) {
                            atomic_store_explicit(&fail, 1, memory_order_relaxed);
                            break;
                        }
                    }
                }
                free(tids);
                free(targs);
                tids = NULL;
                targs = NULL;
            }
        }

        if (atomic_load_explicit(&fail, memory_order_relaxed)) {
            uid_accum_ubuf_destroy_slots(ubuf, n);
            free(ubuf);
            return -1;
        }

        k = 0;
        for (j = 0; j < pairs; j++) {
            ubuf[k] = ubuf[2 * j];
            k++;
        }
        if (n % 2 == 1) {
            ubuf[k] = ubuf[n - 1];
            k++;
        }
        n = k;
    }

    if (uid_accum_merge_into(merged, &ubuf[0]) != 0) {
        uid_accum_ubuf_destroy_slots(ubuf, 1);
        free(ubuf);
        return -1;
    }
    uid_accum_destroy(&ubuf[0]);
    memset(&ubuf[0], 0, sizeof(ubuf[0]));
    free(ubuf);
    return 0;
}

static int bucket_details_append(bucket_details_t *b, const char *path, uint64_t size, int ctime_led) {
    detail_record_t *tmp;

    if (b->count == b->cap) {
        size_t new_cap = (b->cap == 0) ? 256 : b->cap * 2;
        tmp = (detail_record_t *)realloc(b->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        b->items = tmp;
        b->cap = new_cap;
    }

    {
        const char *src = path ? path : "";
        size_t plen;
        const char *leaf;
        char *dp;
        char *dl;
        path_split_pl(src, &plen, &leaf);
        if (b->arena) {
            dp = path_arena_dup_n(b->arena, src, plen);
            dl = path_arena_dup(b->arena, leaf);
        } else {
            dp = (char *)malloc(plen + 1);
            if (dp) { if (plen) memcpy(dp, src, plen); dp[plen] = '\0'; }
            dl = strdup(leaf);
        }
        if (!dp || !dl) { if (!b->arena) { free(dp); free(dl); } return -1; }
        b->items[b->count].parent = dp;
        b->items[b->count].leaf = dl;
    }
    b->items[b->count].size = size;
    b->items[b->count].ctime_led = (uint8_t)(ctime_led ? 1 : 0);
    b->count++;
    return 0;
}

/* Append a detail record whose parent + leaf strings are already owned by a shared arena (the same
 * pointers the matched_records entry points at). No copy is made; caller must guarantee paths_external
 * so the shared pointers are never individually freed. Lets one arena-dup back both lists. */
static int bucket_details_append_ptr(bucket_details_t *b, const char *parent, const char *leaf,
                                     uint64_t size, int ctime_led) {
    detail_record_t *tmp;

    if (b->count == b->cap) {
        size_t new_cap = (b->cap == 0) ? 256 : b->cap * 2;
        tmp = (detail_record_t *)realloc(b->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        b->items = tmp;
        b->cap = new_cap;
    }
    b->items[b->count].parent = parent;
    b->items[b->count].leaf = leaf;
    b->items[b->count].size = size;
    b->items[b->count].ctime_led = (uint8_t)(ctime_led ? 1 : 0);
    b->count++;
    return 0;
}

static void bucket_details_free(bucket_details_t *b) {
    size_t i;
    if (!b->paths_external) {
        for (i = 0; i < b->count; i++) { free((void *)b->items[i].parent); free((void *)b->items[i].leaf); }
    }
    free(b->items);
    b->items = NULL;
    b->count = 0;
    b->cap = 0;
}

static int matched_records_append(matched_records_t *m, const char *path, uint8_t type, uint64_t size) {
    matched_record_t *tmp;

    if (m->count == m->cap) {
        size_t new_cap = (m->cap == 0) ? 1024 : m->cap * 2;
        tmp = (matched_record_t *)realloc(m->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        m->items = tmp;
        m->cap = new_cap;
    }

    {
        const char *src = path ? path : "";
        size_t plen;
        const char *leaf;
        char *dp;
        char *dl;
        path_split_pl(src, &plen, &leaf);
        if (m->arena) {
            dp = path_arena_dup_n(m->arena, src, plen);
            dl = path_arena_dup(m->arena, leaf);
        } else {
            dp = (char *)malloc(plen + 1);
            if (dp) { if (plen) memcpy(dp, src, plen); dp[plen] = '\0'; }
            dl = strdup(leaf);
        }
        if (!dp || !dl) { if (!m->arena) { free(dp); free(dl); } return -1; }
        m->items[m->count].parent = dp;
        m->items[m->count].leaf = dl;
    }
    m->items[m->count].type = type;
    m->items[m->count].size = size;
    m->count++;
    return 0;
}

/* Append a matched record whose parent + leaf strings are already owned by a shared arena. No copy
 * is made; caller must guarantee paths_external so the shared pointers are never individually freed. */
static int matched_records_append_ptr(matched_records_t *m, const char *parent, const char *leaf,
                                      uint8_t type, uint64_t size) {
    matched_record_t *tmp;

    if (m->count == m->cap) {
        size_t new_cap = (m->cap == 0) ? 1024 : m->cap * 2;
        tmp = (matched_record_t *)realloc(m->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        m->items = tmp;
        m->cap = new_cap;
    }
    m->items[m->count].parent = parent;
    m->items[m->count].leaf = leaf;
    m->items[m->count].type = type;
    m->items[m->count].size = size;
    m->count++;
    return 0;
}

static void matched_records_free(matched_records_t *m) {
    size_t i;
    if (!m->paths_external) {
        for (i = 0; i < m->count; i++) { free((void *)m->items[i].parent); free((void *)m->items[i].leaf); }
    }
    free(m->items);
    m->items = NULL;
    m->count = 0;
    m->cap = 0;
}

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static uint64_t vt_mono_ns(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void vt_path_sort_commit(ereport_run_stats_t *rs, uint64_t t0_ns) {
    uint64_t d;

    if (!rs || !g_ereport_verbose || t0_ns == 0ULL) return;
    d = vt_mono_ns() - t0_ns;
    if (d > 0ULL)
        atomic_fetch_add_explicit(&rs->vt_ns_path_sort, (uint_fast64_t)d, memory_order_relaxed);
}

static void vt_bucket_qsort_ns(ereport_run_stats_t *rs, uint64_t t0_ns) {
    uint64_t d;

    if (!rs || !g_ereport_verbose || t0_ns == 0ULL) return;
    d = vt_mono_ns() - t0_ns;
    if (d > 0ULL)
        atomic_fetch_add_explicit(&rs->vt_ns_bucket_emit_sort, (uint_fast64_t)d, memory_order_relaxed);
}

#define VT_QSORT_BUCKET(rs, arr, nmemb, sz, cmp) \
    do { \
        if ((rs) && g_ereport_verbose) { \
            uint64_t _vtq0 = vt_mono_ns(); \
            qsort((arr), (nmemb), (sz), (cmp)); \
            vt_bucket_qsort_ns((rs), _vtq0); \
        } else { \
            qsort((arr), (nmemb), (sz), (cmp)); \
        } \
    } while (0)

/* Key=value lines for --verbose sort vs wall (uses ereport_run_stats_t fields updated during the run). */
static void ereport_verbose_fprint_timing_kv(const ereport_run_stats_t *rs, int merge_sub, int fft) {
    double ps, bs, wcells, other;

    ps = (double)atomic_load_explicit(&rs->vt_ns_path_sort, memory_order_relaxed) * 1e-9;
    bs = (double)atomic_load_explicit(&rs->vt_ns_bucket_emit_sort, memory_order_relaxed) * 1e-9;
    if (ps > 1e-12) fprintf(stderr, "verbose_sort_path_index_sec=%.6f\n", ps);
    if (bs > 1e-12) fprintf(stderr, "verbose_sort_bucket_emit_sec=%.6f\n", bs);

#define EREP_VT_WALL(tag, fld) \
    do { \
        if ((rs)->fld > 1e-9) fprintf(stderr, "verbose_wall_" tag "=%.6f\n", (rs)->fld); \
    } while (0)

    EREP_VT_WALL("chunk_map_sec", vt_chunk_map_sec);
    EREP_VT_WALL("parse_workers_sec", vt_parse_workers_sec);
    EREP_VT_WALL("fini_summaries_sec", vt_fini_summaries_sec);
    EREP_VT_WALL("fini_matched_paths_sec", vt_fini_matched_paths_sec);
    EREP_VT_WALL("fini_directory_maps_dense_sec", vt_fini_directory_maps_dense_sec);
    EREP_VT_WALL("fini_path_shape_sec", vt_fini_path_shape_sec);
    EREP_VT_WALL("fini_uid_sec", vt_fini_uid_sec);
    EREP_VT_WALL("bucket_prep_wall_sec", vt_bucket_prep_wall_sec);
    EREP_VT_WALL("bucket_prep_corpus_scan_sec", vt_bucket_prep_corpus_sec);
    EREP_VT_WALL("bucket_html_cells_wall_sec", vt_bucket_cells_wall_sec);
    EREP_VT_WALL("index_html_sec", vt_index_html_sec);
#undef EREP_VT_WALL

    wcells = rs->vt_bucket_cells_wall_sec;
    if (wcells > 1e-9 && bs > 1e-12) {
        other = wcells > bs ? wcells - bs : 0.0;
        fprintf(stderr, "verbose_bucket_html_cells_non_sort_est_sec=%.6f\n", other);
    }

    if (rs->vt_bucket_prep_wall_sec > 1e-9 && rs->vt_bucket_prep_corpus_sec > 1e-9 && ps > 1e-12)
        fprintf(stderr,
                "verbose_note=bucket_prep_corpus_scan_and_path_index_sort_can_overlap_parallel_threads\n");

    if (merge_sub == 3 && fft > 0 && ps <= 1e-12 && bs <= 1e-12)
        fprintf(stderr,
                "verbose_note=fanout_parent_map_merge_is_not_measured_as_sort_cpu_heavy_hash_merge\n");
}

/* stderr: --verbose progress (ecrawl-style key=value; omit idle zero counters). */
static void ereport_verbose_fprint_runtime_peek(const ereport_run_stats_t *rs) {
    uint64_t prep_tot;
    unsigned long long prep_done;
    int fin;
    double el;
    int phase;
    int merge_sub;
    int mst, msd, fft, ffd, bprep;
    unsigned int dcells, bdone, idx_step;
    const int nbucket = AGE_BUCKETS * SIZE_BUCKETS;

    if (!rs || !g_ereport_verbose) return;

    prep_tot = rs->chunk_prep_files_total;
    prep_done = atomic_load_explicit(&rs->chunk_prep_files_done, memory_order_relaxed);
    fin = atomic_load_explicit(&rs->parse_workers_done, memory_order_relaxed);
    el = rs->run_start_sec > 0.0 ? now_sec() - rs->run_start_sec : 0.0;

    fprintf(stderr, "\nereport: verbose progress\n");
    fprintf(stderr, "elapsed_sec=%.1f\n", el);

    if (prep_tot > 0ULL && prep_done < prep_tot && !fin) {
        int nslots = rs->chunk_map_path_slots;
        int active = 0;
        int si;
        volatile const char **wpaths = rs->chunk_map_worker_paths;

        if (wpaths && nslots > 0) {
            for (si = 0; si < nslots; si++) {
                if (wpaths[si]) active++;
            }
        }
        fprintf(stderr, "stage=chunk_map\n");
        fprintf(stderr, "chunk_prep_files_done=%llu\n", prep_done);
        fprintf(stderr, "chunk_prep_files_total=%" PRIu64 "\n", prep_tot);
        fprintf(stderr, "chunk_map_readers_busy=%d\n", active);
        fprintf(stderr, "chunk_map_reader_slots=%d\n", nslots > 0 ? nslots : 0);
        ereport_verbose_fprint_timing_kv(rs, 0, 0);
        fflush(stderr);
        return;
    }

    if (!fin) {
        fprintf(stderr, "stage=parse\n");
        fprintf(stderr, "scanned_input_files=%llu\n",
                (unsigned long long)atomic_load_explicit(&rs->scanned_input_files, memory_order_relaxed));
        fprintf(stderr, "input_files_total=%" PRIu64 "\n", (uint64_t)rs->input_files_total);
        fprintf(stderr, "scanned_records=%llu\n",
                (unsigned long long)atomic_load_explicit(&rs->scanned_records, memory_order_relaxed));
        fprintf(stderr, "matched_records=%llu\n",
                (unsigned long long)atomic_load_explicit(&rs->matched_records, memory_order_relaxed));
        {
            unsigned long long bad = atomic_load_explicit(&rs->bad_input_files, memory_order_relaxed);
            if (bad > 0ULL) fprintf(stderr, "bad_input_files=%llu\n", bad);
        }
        ereport_verbose_fprint_timing_kv(rs, 0, 0);
        fflush(stderr);
        return;
    }

    phase = atomic_load_explicit(&rs->finalize_phase, memory_order_relaxed);
    merge_sub = atomic_load_explicit(&rs->finalize_merge_substep, memory_order_relaxed);
    mst = atomic_load_explicit(&rs->finalize_matched_slices_total, memory_order_relaxed);
    msd = atomic_load_explicit(&rs->finalize_matched_slices_done, memory_order_relaxed);
    fft = atomic_load_explicit(&rs->finalize_fanout_workers_total, memory_order_relaxed);
    ffd = atomic_load_explicit(&rs->finalize_fanout_workers_done, memory_order_relaxed);
    dcells = atomic_load_explicit(&rs->finalize_dense_cells_done, memory_order_relaxed);
    bprep = atomic_load_explicit(&rs->finalize_bucket_prep, memory_order_relaxed);
    bdone = atomic_load_explicit(&rs->finalize_bucket_done, memory_order_relaxed);
    idx_step = atomic_load_explicit(&rs->finalize_index_step, memory_order_relaxed);

    fprintf(stderr, "stage=post_parse\n");
    fprintf(stderr, "finalize_phase=%d\n", phase);
    if (merge_sub != 0) fprintf(stderr, "finalize_merge_substep=%d\n", merge_sub);

    if (mst > 0) {
        fprintf(stderr, "finalize_matched_copy_slices_done=%d\n", msd);
        fprintf(stderr, "finalize_matched_copy_slices_total=%d\n", mst);
    }

    if (fft > 0) {
        fprintf(stderr, "finalize_fanout_workers_done=%d\n", ffd);
        fprintf(stderr, "finalize_fanout_workers_total=%d\n", fft);
    }

    if (dcells > 0U) {
        fprintf(stderr, "finalize_dense_cells_done=%u\n", dcells);
        fprintf(stderr, "finalize_dense_cells_total=%d\n", nbucket);
    }

    if (bprep != 0) fprintf(stderr, "finalize_bucket_prep=%d\n", bprep);
    if (bdone > 0U) {
        fprintf(stderr, "finalize_bucket_html_done=%u\n", bdone);
        fprintf(stderr, "finalize_bucket_html_total=%d\n", nbucket);
    }
    if (idx_step > 0U) fprintf(stderr, "finalize_index_step=%u\n", idx_step);

    ereport_verbose_fprint_timing_kv(rs, merge_sub, fft);

    fprintf(stderr, "scanned_input_files=%llu\n",
            (unsigned long long)atomic_load_explicit(&rs->scanned_input_files, memory_order_relaxed));
    fprintf(stderr, "input_files_total=%" PRIu64 "\n", (uint64_t)rs->input_files_total);
    fprintf(stderr, "scanned_records=%llu\n",
            (unsigned long long)atomic_load_explicit(&rs->scanned_records, memory_order_relaxed));
    fprintf(stderr, "matched_records=%llu\n",
            (unsigned long long)atomic_load_explicit(&rs->matched_records, memory_order_relaxed));
    {
        unsigned long long bad = atomic_load_explicit(&rs->bad_input_files, memory_order_relaxed);
        if (bad > 0ULL) fprintf(stderr, "bad_input_files=%llu\n", bad);
    }
    fflush(stderr);
}

static void clear_status_line(void) {
    printf("\r%160s\r", "");
    fflush(stdout);
}

static void human_decimal(double v, char *buf, size_t sz) {
    const char *units[] = {"", "K", "M", "G", "T", "P", "E"};
    int i = 0;

    while (v >= 1000.0 && i < 6) {
        v /= 1000.0;
        i++;
    }

    if (v >= 100.0 || i == 0) snprintf(buf, sz, "%.0f%s", v, units[i]);
    else if (v >= 10.0) snprintf(buf, sz, "%.1f%s", v, units[i]);
    else snprintf(buf, sz, "%.2f%s", v, units[i]);
}

/* Thousands separators for HTML (locale-independent). First group has 1–3 digits, then groups of 3. */
static void format_uint_commas(uint64_t n, char *buf, size_t sz) {
    char raw[40];
    size_t raw_len;
    size_t i, pos;
    size_t first_len;
    unsigned g;

    if (sz == 0) return;
    snprintf(raw, sizeof(raw), "%" PRIu64, n);
    raw_len = strlen(raw);
    first_len = raw_len % 3U;
    if (first_len == 0) first_len = 3U;
    pos = 0;
    for (i = 0; i < first_len && pos + 1 < sz; i++) buf[pos++] = raw[i];
    while (i < raw_len) {
        if (pos + 1 >= sz) break;
        buf[pos++] = ',';
        for (g = 0; g < 3U && i < raw_len && pos + 1 < sz; g++, i++) buf[pos++] = raw[i];
    }
    buf[pos] = '\0';
}

/* Abbreviated count in parentheses only for 7+ digit totals (>= 1,000,000): (111M), (5T). */
static void format_count_paren_round(uint64_t n, char *buf, size_t sz) {
    int v;

    if (sz == 0) return;
    if (n < 1000000ULL) {
        buf[0] = '\0';
        return;
    }
    if (n >= 1000000000000ULL) {
        v = (int)((n + 500000000000ULL) / 1000000000000ULL);
        snprintf(buf, sz, "(%dT)", v);
    } else if (n >= 1000000000ULL) {
        v = (int)((n + 500000000ULL) / 1000000000ULL);
        snprintf(buf, sz, "(%dB)", v);
    } else {
        v = (int)((n + 500000ULL) / 1000000ULL);
        snprintf(buf, sz, "(%dM)", v);
    }
}

/* Comma-separated count; optional abbreviated suffix in parentheses only when >= 1e6. */
static void format_count_pretty_inline(uint64_t n, char *buf, size_t sz) {
    char c[64];
    char p[16];

    format_uint_commas(n, c, sizeof(c));
    format_count_paren_round(n, p, sizeof(p));
    if (p[0])
        snprintf(buf, sz, "%s %s", c, p);
    else
        snprintf(buf, sz, "%s", c);
}

/* Stats cards: comma abs + optional (rounded) for large counts. */
static void emit_stats_count_dd(FILE *out, const char *dt_label, uint64_t v) {
    char comma_buf[48];
    char paren_buf[16];

    format_uint_commas(v, comma_buf, sizeof(comma_buf));
    format_count_paren_round(v, paren_buf, sizeof(paren_buf));
    if (paren_buf[0])
        fprintf(out,
                "<dt>%s</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num-short\">%s</span></dd>\n",
                dt_label,
                comma_buf,
                paren_buf);
    else
        fprintf(out, "<dt>%s</dt><dd><span class=\"stats-num\">%s</span></dd>\n", dt_label, comma_buf);
}

static void format_duration(double sec, char *buf, size_t sz) {
    long total = sec > 0.0 ? (long)(sec + 0.5) : 0;
    long h = total / 3600;
    long m = (total % 3600) / 60;
    long s = total % 60;
    snprintf(buf, sz, "%02ld:%02ld:%02ld", h, m, s);
}

static void progress_flush_local(progress_local_t *progress, ereport_run_stats_t *rs) {
    int idx;

    if (!progress || !rs) return;
    if (progress->scanned_input_files == 0 &&
        progress->scanned_records == 0 &&
        progress->matched_records == 0 &&
        progress->bad_input_files == 0) return;

    idx = atomic_load(&g_bucket_index);
    if (progress->scanned_input_files > 0) {
        atomic_fetch_add(&rs->scanned_input_files, progress->scanned_input_files);
    }
    if (progress->scanned_records > 0) {
        atomic_fetch_add(&rs->scanned_records, progress->scanned_records);
        atomic_fetch_add(&g_window_records, progress->scanned_records);
        atomic_fetch_add(&g_bucket_records[idx], progress->scanned_records);
    }
    if (progress->matched_records > 0) {
        atomic_fetch_add(&rs->matched_records, progress->matched_records);
    }
    if (progress->bad_input_files > 0) {
        atomic_fetch_add(&rs->bad_input_files, progress->bad_input_files);
    }

    memset(progress, 0, sizeof(*progress));
}

static void progress_maybe_flush(progress_local_t *progress, ereport_run_stats_t *rs) {
    if (!progress || !rs) return;
    if (progress->scanned_records >= PROGRESS_FLUSH_INTERVAL) progress_flush_local(progress, rs);
}

/*
 * Read-only opens go through the shard handle cache: a shard is opened once for its
 * catalog and again for every chunk of it, and glibc serializes all of that on one
 * process-wide stdio list lock. Write opens (the report pages) fall straight through.
 */
static FILE *counted_fopen(const char *path, const char *mode) {
    if (g_ereport_verbose) atomic_fetch_add_explicit(&g_io_fopen_calls, 1, memory_order_relaxed);
    return crawl_fpcache_fopen(path, mode);
}

static int counted_fclose(FILE *fp) {
    if (g_ereport_verbose) atomic_fetch_add_explicit(&g_io_fclose_calls, 1, memory_order_relaxed);
    return crawl_fpcache_fclose(fp);
}

static size_t counted_fread(void *ptr, size_t size, size_t nmemb, FILE *fp) {
    if (g_ereport_verbose) atomic_fetch_add_explicit(&g_io_fread_calls, 1, memory_order_relaxed);
    return fread(ptr, size, nmemb, fp);
}

/* Unlocked variant for streams owned by a single thread (the parse path: each
 * worker opens its own chunk FILE*). Avoids the per-call stdio lock taken
 * twice per record (header + name). */
static size_t counted_fread_unlocked(void *ptr, size_t size, size_t nmemb, FILE *fp) {
    if (g_ereport_verbose) atomic_fetch_add_explicit(&g_io_fread_calls, 1, memory_order_relaxed);
    return fread_unlocked(ptr, size, nmemb, fp);
}

#define EREPORT_PARSE_STDIO_BUFSZ (1u << 20) /* 1 MiB explicit read buffer per chunk FILE* */

static const crawl_bin_chunk_stdio_t ereport_chunk_io = {counted_fopen, counted_fread, counted_fclose};

static DIR *counted_opendir(const char *path) {
    if (g_ereport_verbose) atomic_fetch_add_explicit(&g_io_opendir_calls, 1, memory_order_relaxed);
    return opendir(path);
}

static struct dirent *counted_readdir(DIR *dir) {
    if (g_ereport_verbose) atomic_fetch_add_explicit(&g_io_readdir_calls, 1, memory_order_relaxed);
    return readdir(dir);
}

static int counted_closedir(DIR *dir) {
    if (g_ereport_verbose) atomic_fetch_add_explicit(&g_io_closedir_calls, 1, memory_order_relaxed);
    return closedir(dir);
}

static void die(const char *msg) {
    fprintf(stderr, "%s\n", msg);
    exit(1);
}

static int has_bin_suffix(const char *name) {
    size_t n = strlen(name);
    return n > 4 && strcmp(name + n - 4, ".bin") == 0;
}

static int starts_with_thread(const char *name) {
    return strncmp(name, "thread_", 7) == 0;
}

static int starts_with_uid_shard(const char *name) {
    return strncmp(name, "uid_shard_", 10) == 0;
}

static int is_power_of_two_u32(uint32_t v) {
    return v && ((v & (v - 1U)) == 0U);
}

static int parse_uid_shard_number(const char *name, uint32_t *out) {
    const char *p = name + 10;
    char *end = NULL;
    unsigned long v;

    if (!starts_with_uid_shard(name) || !has_bin_suffix(name)) return -1;

    errno = 0;
    v = strtoul(p, &end, 10);
    if (errno != 0 || end == p || strcmp(end, ".bin") != 0 || v > UINT32_MAX) return -1;

    *out = (uint32_t)v;
    return 0;
}

static int read_uid_shard_layout(const char *dirpath, uint32_t *uid_shards_out) {
    char manifest_path[PATH_MAX];
    FILE *fp;
    char line[256];
    int saw_layout = 0;
    uint32_t uid_shards = 0;

    if (snprintf(manifest_path, sizeof(manifest_path), "%s/crawl_manifest.txt", dirpath) >= (int)sizeof(manifest_path)) return -1;

    fp = counted_fopen(manifest_path, "r");
    if (!fp) {
        if (errno == ENOENT) return 0;
        return -1;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *nl = strchr(line, '\n');
        if (nl) *nl = '\0';

        if (strcmp(line, "layout=uid_shards") == 0) {
            saw_layout = 1;
        } else if (strncmp(line, "uid_shards=", 11) == 0) {
            unsigned long v = strtoul(line + 11, NULL, 10);
            if (v > 0 && v <= UINT32_MAX) uid_shards = (uint32_t)v;
        }
    }

    counted_fclose(fp);
    if (!saw_layout) return 0;
    if (uid_shards == 0 || !is_power_of_two_u32(uid_shards)) {
        fprintf(stderr, "invalid uid_shards value in %s\n", manifest_path);
        return -1;
    }

    *uid_shards_out = uid_shards;
    return 1;
}

/*
 * Prefer record_root (logical storage id, written by crawls from before the
 * ecrawl --record-root removal) when present in crawl_manifest.txt, else
 * start_path (filesystem crawl root). Returns 0 when a path was filled.
 */
static int read_manifest_storage_display_path(const char *bin_dir, char *out, size_t out_sz) {
    char manifest_path[PATH_MAX];
    FILE *fp;
    char line[4096];
    char record_root[4096];
    char start_path[4096];

    if (!out || out_sz == 0) return -1;
    record_root[0] = '\0';
    start_path[0] = '\0';

    if (snprintf(manifest_path, sizeof(manifest_path), "%s/crawl_manifest.txt", bin_dir) >= (int)sizeof(manifest_path)) return -1;

    fp = counted_fopen(manifest_path, "r");
    if (!fp) return -1;

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *nl = strchr(line, '\n');
        if (nl) *nl = '\0';

        if (strncmp(line, "record_root=", 12) == 0) {
            if (snprintf(record_root, sizeof(record_root), "%s", line + 12) >= (int)sizeof(record_root)) record_root[0] = '\0';
        } else if (strncmp(line, "start_path=", 11) == 0) {
            if (snprintf(start_path, sizeof(start_path), "%s", line + 11) >= (int)sizeof(start_path)) start_path[0] = '\0';
        }
    }

    counted_fclose(fp);

    if (record_root[0] != '\0') {
        int r = snprintf(out, out_sz, "%s", record_root);
        if (r >= 0 && (size_t)r < out_sz) return 0;
        return -1;
    }
    if (start_path[0] != '\0') {
        int r = snprintf(out, out_sz, "%s", start_path);
        if (r >= 0 && (size_t)r < out_sz) return 0;
        return -1;
    }
    return -1;
}

static int read_manifest_crawl_timing(const char *bin_dir, time_t *start_out, time_t *end_out, double *elapsed_out) {
    char manifest_path[PATH_MAX];
    FILE *fp;
    char line[4096];
    time_t st = 0, en = 0;
    double el = 0.0;
    int have_el = 0;

    if (!bin_dir || !start_out || !end_out || !elapsed_out) return 0;
    *start_out = *end_out = 0;
    *elapsed_out = 0.0;

    if (snprintf(manifest_path, sizeof(manifest_path), "%s/crawl_manifest.txt", bin_dir) >= (int)sizeof(manifest_path)) return 0;

    fp = counted_fopen(manifest_path, "r");
    if (!fp) return 0;

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *nl = strchr(line, '\n');
        if (nl) *nl = '\0';

        if (strncmp(line, "crawl_started_epoch=", 20) == 0) {
            errno = 0;
            st = (time_t)strtoull(line + 20, NULL, 10);
            if (errno) st = 0;
        } else if (strncmp(line, "crawl_finished_epoch=", 21) == 0) {
            errno = 0;
            en = (time_t)strtoull(line + 21, NULL, 10);
            if (errno) en = 0;
        } else if (strncmp(line, "crawl_elapsed_sec=", 18) == 0) {
            char *endp = NULL;
            el = strtod(line + 18, &endp);
            if (endp != line + 18 && el >= 0.0) have_el = 1;
        }
    }

    counted_fclose(fp);

    if (st <= 0 || en < st) return 0;
    *start_out = st;
    *end_out = en;
    if (have_el) *elapsed_out = el;
    return 1;
}

static void aggregate_crawl_timing_from_manifests(const char **bin_dirs, size_t n, ereport_crawl_timing_t *out) {
    size_t i;
    int got = 0;
    time_t mn = 0, mx = 0;
    double single_el = 0.0;
    double max_el = 0.0;

    if (!out) return;
    memset(out, 0, sizeof(*out));
    if (!bin_dirs || n == 0) return;

    for (i = 0; i < n; i++) {
        time_t st, en;
        double el = 0.0;

        if (!read_manifest_crawl_timing(bin_dirs[i], &st, &en, &el)) continue;
        got++;
        if (got == 1 || st < mn) mn = st;
        if (got == 1 || en > mx) mx = en;
        if (el > max_el) max_el = el;
        if (n == 1U) single_el = el;
    }

    if (got == 0 || mn <= 0 || mx < mn) return;

    out->valid = 1;
    out->wall_start = mn;
    out->wall_end = mx;
    out->merged = (n > 1U);

    if (n == 1U && single_el > 0.0)
        out->elapsed_sec = single_el;
    else {
        double span = difftime(mx, mn);
        out->elapsed_sec = span > 0.0 ? span : max_el;
        if (out->elapsed_sec <= 0.0 && max_el > 0.0) out->elapsed_sec = max_el;
    }
}

#define EREPORT_MANIFEST_DEFAULT_ST_BLOCKS_UNIT 512U

static int read_manifest_disk_stats(const char *bin_dir, uint32_t *unit_out, uint64_t *alloc_out, uint64_t *sparse_out) {
    char manifest_path[PATH_MAX];
    FILE *fp;
    char line[4096];
    int saw_alloc = 0;
    int saw_sparse = 0;
    uint32_t u = 0;
    uint64_t al = 0ULL;
    uint64_t sp = 0ULL;

    if (!bin_dir || !unit_out || !alloc_out || !sparse_out) return 0;
    *unit_out = 0;
    *alloc_out = 0ULL;
    *sparse_out = 0ULL;

    if (snprintf(manifest_path, sizeof(manifest_path), "%s/crawl_manifest.txt", bin_dir) >= (int)sizeof(manifest_path))
        return 0;

    fp = counted_fopen(manifest_path, "r");
    if (!fp) return 0;

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *nl = strchr(line, '\n');

        if (nl) *nl = '\0';

        if (strncmp(line, "st_blocks_bytes_unit=", 21) == 0) {
            unsigned long v = strtoul(line + 21, NULL, 10);

            if (v > 0UL && v <= (unsigned long)UINT32_MAX) u = (uint32_t)v;
        } else if (strncmp(line, "total_allocated_bytes=", 22) == 0) {
            errno = 0;
            al = strtoull(line + 22, NULL, 10);
            if (!errno) saw_alloc = 1;
        } else if (strncmp(line, "files_sparse_heuristic=", 23) == 0) {
            errno = 0;
            sp = strtoull(line + 23, NULL, 10);
            if (!errno) saw_sparse = 1;
        }
    }

    counted_fclose(fp);

    if (!saw_alloc) return 0;
    *unit_out = u;
    *alloc_out = al;
    if (saw_sparse) *sparse_out = sp;
    return 1;
}

static void aggregate_manifest_disk_from_manifests(const char **bin_dirs, size_t n, ereport_manifest_disk_t *out) {
    size_t i;
    int got = 0;
    uint32_t chosen_unit = 0;

    if (!out) return;
    memset(out, 0, sizeof(*out));
    if (!bin_dirs || n == 0) return;

    for (i = 0; i < n; i++) {
        uint32_t u;
        uint64_t al, sp;

        if (!read_manifest_disk_stats(bin_dirs[i], &u, &al, &sp)) continue;
        got = 1;
        out->total_allocated_bytes += al;
        out->files_sparse_heuristic += sp;
        if (u != 0U) {
            if (chosen_unit == 0U)
                chosen_unit = u;
            else if (u != chosen_unit) {
                if (!out->unit_mismatch)
                    fprintf(stderr,
                            "ereport: warning: mixed st_blocks_bytes_unit= across merged crawl_manifest.txt "
                            "inputs\n");
                out->unit_mismatch = 1;
            }
        }
    }

    if (!got) return;
    out->valid = 1;
    out->st_blocks_bytes_unit = chosen_unit != 0U ? chosen_unit : EREPORT_MANIFEST_DEFAULT_ST_BLOCKS_UNIT;
}

static void format_wall_clock_local(time_t t, char *buf, size_t sz) {
    struct tm tm_local;

    if (!buf || sz == 0) return;
    buf[0] = '\0';
    if (!localtime_r(&t, &tm_local)) {
        snprintf(buf, sz, "(unknown)");
        return;
    }
    if (!strftime(buf, sz, "%Y-%m-%d %H:%M:%S %Z", &tm_local)) snprintf(buf, sz, "(unknown)");
}

static void format_duration_approx(double sec, char *buf, size_t sz) {
    if (!buf || sz == 0) return;
    if (sec < 0.0) sec = 0.0;
    if (sec < 60.0)
        snprintf(buf, sz, "%.1f s", sec);
    else if (sec < 3600.0)
        snprintf(buf, sz, "%.1f min", sec / 60.0);
    else if (sec < 86400.0)
        snprintf(buf, sz, "%.2f h", sec / 3600.0);
    else
        snprintf(buf, sz, "%.2f days", sec / 86400.0);
}

static void format_storage_base_paths_label(const char **dirs, size_t n, char *buf, size_t buf_sz) {
    size_t pos = 0;
    size_t i;
    char one[4096];

    if (!buf || buf_sz == 0) return;
    buf[0] = '\0';
    if (n == 0 || !dirs) return;

    if (n == 1) {
        if (read_manifest_storage_display_path(dirs[0], one, sizeof(one)) == 0)
            snprintf(buf, buf_sz, "%s", one);
        else
            snprintf(buf, buf_sz, "%s", dirs[0]);
        return;
    }

    for (i = 0; i < n && pos + 1 < buf_sz; i++) {
        const char *show = dirs[i];
        if (read_manifest_storage_display_path(dirs[i], one, sizeof(one)) == 0) show = one;
        {
            int w = snprintf(buf + pos, buf_sz - pos, "%s%s", i ? ";" : "", show);
            if (w < 0 || (size_t)w >= buf_sz - pos) {
                snprintf(buf, buf_sz, "%s;… (%zu locations)", dirs[0], n);
                return;
            }
            pos += (size_t)w;
        }
    }
}

/*
 * Create directory path (mkdir -p semantics). Mutates path with temporary NULs.
 */
static int mkdir_p_path(char *path) {
    size_t i;
    size_t len;
    struct stat st;

    if (!path || path[0] == '\0') {
        errno = EINVAL;
        return -1;
    }
    len = strlen(path);
    for (i = 1U; i < len; i++) {
        if (path[i] != '/')
            continue;
        path[i] = '\0';
        if (path[0] != '\0') {
            if (stat(path, &st) != 0) {
                if (mkdir(path, 0777) != 0 && errno != EEXIST) {
                    path[i] = '/';
                    return -1;
                }
            } else if (!S_ISDIR(st.st_mode)) {
                path[i] = '/';
                errno = ENOTDIR;
                return -1;
            }
        }
        path[i] = '/';
    }
    if (stat(path, &st) != 0) {
        if (mkdir(path, 0777) != 0 && errno != EEXIST)
            return -1;
    } else if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }
    return 0;
}

static void set_bucket_output_dir(const char *username) {
    size_t i;
    int n;

    /* Default output directory is the resolved username (sanitized). Only empty/unusable names fall back to "tmp". */
    if (!username || username[0] == '\0') username = "tmp";

    n = snprintf(g_bucket_output_dir, sizeof(g_bucket_output_dir), "%s", username);
    if (n < 0 || (size_t)n >= sizeof(g_bucket_output_dir)) {
        snprintf(g_bucket_output_dir, sizeof(g_bucket_output_dir), "%s", "tmp");
        return;
    }

    for (i = 0; g_bucket_output_dir[i] != '\0'; i++) {
        unsigned char c = (unsigned char)g_bucket_output_dir[i];
        if (c == '/' || c == '\\' || c == ':' || c == '\t' || c == '\n' || c == '\r') {
            g_bucket_output_dir[i] = '_';
        }
    }
}

static int parse_uid_arg(const char *s, uid_t *out) {
    unsigned long long v;
    char *end = NULL;

    if (!s || *s == '\0') return -1;

    errno = 0;
    v = strtoull(s, &end, 10);
    if (errno != 0 || !end || *end != '\0') return -1;
    if ((unsigned long long)((uid_t)v) != v) return -1;

    *out = (uid_t)v;
    return 0;
}

static int resolve_target_user(const char *spec, uid_t *out_uid, char *display_name, size_t display_name_sz) {
    struct passwd *pw;
    uid_t parsed_uid;

    if (parse_uid_arg(spec, &parsed_uid) == 0) {
        *out_uid = parsed_uid;
        pw = getpwuid(parsed_uid);
        if (pw && pw->pw_name && pw->pw_name[0] != '\0') {
            snprintf(display_name, display_name_sz, "%s", pw->pw_name);
        } else {
            snprintf(display_name, display_name_sz, "%s", spec);
        }
        return 0;
    }

    pw = getpwnam(spec);
    if (!pw) return -1;

    *out_uid = pw->pw_uid;
    snprintf(display_name, display_name_sz, "%s", (pw->pw_name && pw->pw_name[0] != '\0') ? pw->pw_name : spec);
    return 0;
}

static int parse_time_basis(const char *s, time_basis_t *out) {
    if (strcmp(s, "atime") == 0) {
        *out = TIME_ATIME;
        return 0;
    }
    if (strcmp(s, "mtime") == 0) {
        *out = TIME_MTIME;
        return 0;
    }
    if (strcmp(s, "ctime") == 0) {
        *out = TIME_CTIME;
        return 0;
    }
    if (strcmp(s, "effective") == 0) {
        *out = TIME_EFFECTIVE;
        return 0;
    }
    return -1;
}

static uint64_t pick_time(const bin_record_hdr_t *r, time_basis_t basis) {
    switch (basis) {
        case TIME_ATIME:
            return r->atime;
        case TIME_MTIME:
            return r->mtime;
        case TIME_CTIME:
            return r->ctime;
        case TIME_EFFECTIVE: {
            uint64_t t = r->atime;
            if (r->mtime > t) t = r->mtime;
            if (r->ctime > t) t = r->ctime;
            return t;
        }
        default:
            return r->mtime;
    }
}

static int record_ctime_led(const bin_record_hdr_t *r) {
    uint64_t tam = r->atime;

    if (r->mtime > tam) tam = r->mtime;
    if (r->ctime <= tam) return 0;
    return (r->ctime - tam) >= CTIME_LED_MIN_DELTA_SEC;
}

static int ctime_led_badge_visible(uint64_t led_bytes, uint64_t total_bytes) {
    if (total_bytes == 0 || led_bytes == 0) return 0;
    return (double)led_bytes / (double)total_bytes >= g_ctime_led_badge_min_share_frac;
}

static int shape_deep_badge_visible(uint64_t deep_bytes, uint64_t bucket_bytes, uint64_t bucket_files) {
    if (bucket_bytes == 0 || deep_bytes == 0 || bucket_files < PATH_SHAPE_MIN_BUCKET_FILES) return 0;
    return (double)deep_bytes / (double)bucket_bytes >= PATH_SHAPE_BADGE_MIN_SHARE_FRAC;
}

static int shape_dense_badge_visible(uint64_t max_fanout_among_slice_parents) {
    return max_fanout_among_slice_parents >= PATH_SHAPE_DENSE_MIN_CHILDREN;
}

static int heat_cell_skew_visible(const summary_t *sum, const path_shape_view_t *shape, int ab, int sb) {
    uint64_t cb;
    uint64_t cf;
    uint64_t db;
    uint64_t dmax;

    if (!sum || !shape) return 0;
    cb = sum->bytes[ab][sb];
    cf = sum->files[ab][sb];
    db = shape->cell[ab][sb].deep_bytes;
    dmax = shape->cell[ab][sb].dense_fanout_max;
    return shape_deep_badge_visible(db, cb, cf) && shape_dense_badge_visible(dmax);
}

static unsigned path_slash_count_str(const char *path) {
    unsigned c = 0;
    if (!path) return 0;
    for (; *path; path++) {
        if (*path == '/') c++;
    }
    return c;
}

/*
 * Parent directory of a filesystem path (UTF-8 safe: byte-oriented like crawl paths).
 * Returns 0 on success; -1 if parent does not fit in parent_sz.
 */
static int parent_dir_to_buf(const char *path, char *parent, size_t parent_sz) {
    size_t len;

    if (!path || !parent || parent_sz == 0) return -1;
    len = strlen(path);
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

static uint32_t dense_hash_full(const char *parent) {
    uint32_t h = 2166136261u;

    for (; *parent; parent++) {
        h ^= (uint32_t)(unsigned char)*parent;
        h *= 16777619u;
    }
    return h;
}

/* Single allocation: node header immediately followed by the NUL-terminated parent
 * string. node->parent points into the same block, so it is never freed separately
 * (all free sites just free(node)) and it shares cache lines with the node header,
 * which speeds the collision-chain strcmp walks that dominate flat/many-parent corpora. */
static dense_node_t *dense_node_new(const char *parent, uint32_t h, uint64_t n) {
    size_t len = strlen(parent);
    dense_node_t *node = (dense_node_t *)malloc(sizeof(*node) + len + 1U);

    if (!node) return NULL;
    node->parent = (char *)(node + 1);
    memcpy(node->parent, parent, len + 1U);
    node->n = n;
    node->hash = h;
    node->next = NULL;
    return node;
}

/* External variant: the parent string is interned elsewhere (per-worker arena) and outlives
 * this node, so the node is a bare struct that merely references it. free(node) frees only the
 * node. Used by maps with parents_external == 1 to avoid copying the parent into every cell. */
static dense_node_t *dense_node_new_ref(const char *parent, uint32_t h, uint64_t n) {
    dense_node_t *node = (dense_node_t *)malloc(sizeof(*node));

    if (!node) return NULL;
    node->parent = (char *)parent; /* not owned; interned in a longer-lived arena */
    node->n = n;
    node->hash = h;
    node->next = NULL;
    return node;
}

static inline dense_node_t *dense_node_make(const dense_cell_map_t *m, const char *parent,
                                            uint32_t h, uint64_t n) {
    return m->parents_external ? dense_node_new_ref(parent, h, n)
                               : dense_node_new(parent, h, n);
}

/* Ensure m->buckets exists and has room (load factor < 1). On growth, doubles the table
 * and rehashes every node by its stored full hash. Nodes keep their addresses (only relinked),
 * so m->last stays valid across a grow. Single-threaded use only (per-worker accumulation and
 * post-join merges; never called while other threads touch this map). Returns -1 on OOM. */
static int dense_cell_reserve(dense_cell_map_t *m) {
    size_t newn;
    dense_node_t **nb;
    size_t i;

    if (m->buckets && m->count < m->nbuckets) return 0;
    newn = m->nbuckets ? (m->nbuckets << 1)
                       : (m->reserve_buckets ? m->reserve_buckets : (size_t)DENSE_PARENT_BUCKETS_INIT);
    nb = (dense_node_t **)calloc(newn, sizeof(*nb));
    if (!nb) return -1;
    alloc_hint_hugepages(nb, newn * sizeof(*nb));
    alloc_prefault(nb, newn * sizeof(*nb));
    for (i = 0; i < m->nbuckets; i++) {
        dense_node_t *n = m->buckets[i];

        while (n) {
            dense_node_t *nx = n->next;
            size_t bi = (size_t)n->hash & (newn - 1);

            n->next = nb[bi];
            nb[bi] = n;
            n = nx;
        }
    }
    free(m->buckets);
    m->buckets = nb;
    m->nbuckets = newn;
    return 0;
}

/* Insert/accumulate with a precomputed full hash; compares hash before strcmp so
 * collision-chain walks skip the (expensive) full path strcmp on hash mismatches. */
static int dense_cell_add_h(dense_cell_map_t *m, const char *parent, uint32_t h, uint64_t delta,
                            summary_t *sum) {
    size_t bi;
    dense_node_t **pp;
    dense_node_t *node;

    if (m->nbuckets) {
        bi = (size_t)h & (m->nbuckets - 1);
        pp = &m->buckets[bi];
        while (*pp) {
            if ((*pp)->hash == h && strcmp((*pp)->parent, parent) == 0) {
                (*pp)->n += delta;
                return 0;
            }
            pp = &(*pp)->next;
        }
    }
    if (dense_cell_reserve(m) != 0) {
        if (sum) sum->bad_input_files++;
        return -1;
    }
    node = dense_node_make(m, parent, h, delta);
    if (!node) {
        if (sum) sum->bad_input_files++;
        return -1;
    }
    bi = (size_t)h & (m->nbuckets - 1);
    node->next = m->buckets[bi];
    m->buckets[bi] = node;
    m->count++;
    return 0;
}

/* Accumulate keyed by parent_dir_id with a last-hit fast path. parent_id must be a
 * stable identifier for the directory whose path is *parent (here: the crawl-bin
 * parent_dir_id, which is bijective with the parent path). Repeated records under the
 * same directory then increment the cached node directly, skipping hash + strcmp. */
static int dense_cell_add_cached_h(dense_cell_map_t *m, const char *parent, uint64_t parent_id,
                                   uint32_t h, uint64_t delta, summary_t *sum) {
    size_t bi;
    dense_node_t **pp;
    dense_node_t *node;

    if (m->last && m->last_id == parent_id) {
        m->last->n += delta;
        return 0;
    }
    if (m->nbuckets) {
        bi = (size_t)h & (m->nbuckets - 1);
        pp = &m->buckets[bi];
        while (*pp) {
            if ((*pp)->hash == h && strcmp((*pp)->parent, parent) == 0) {
                (*pp)->n += delta;
                m->last = *pp;
                m->last_id = parent_id;
                return 0;
            }
            pp = &(*pp)->next;
        }
    }
    if (dense_cell_reserve(m) != 0) {
        if (sum) sum->bad_input_files++;
        return -1;
    }
    node = dense_node_make(m, parent, h, delta);
    if (!node) {
        if (sum) sum->bad_input_files++;
        return -1;
    }
    bi = (size_t)h & (m->nbuckets - 1);
    node->next = m->buckets[bi];
    m->buckets[bi] = node;
    m->count++;
    m->last = node;
    m->last_id = parent_id;
    return 0;
}

static int dense_cell_add_cached(dense_cell_map_t *m, const char *parent, uint64_t parent_id,
                                 uint64_t delta, summary_t *sum) {
    if (m->last && m->last_id == parent_id) {
        m->last->n += delta;
        return 0;
    }
    return dense_cell_add_cached_h(m, parent, parent_id, dense_hash_full(parent), delta, sum);
}

static size_t dense_cell_total_nodes(const dense_cell_map_t *m) {
    if (!m) return 0;
    return m->count;
}

typedef struct {
    atomic_int gate; /* 0 spin, 1 run, 2 abort (pthread_create failure) */
    pthread_barrier_t mid;
} dmerge_sync_t;

/* Merge src's bucket range into dst by STEALING (relinking) src nodes rather than allocating a
 * fresh dst node and copying the parent for each. On a duplicate parent the counts are folded and
 * the now-redundant src node is freed; otherwise the src node is relinked straight into dst,
 * carrying its parent (owned trailing bytes for an internal node, or the shared interned pointer
 * for an external node) unchanged. This removes the per-node malloc/free + parent memcpy storm
 * (the dominant allocator/TLB churn during finalize) and the transient peak of holding both the
 * src node and a freshly-copied dst node at once. dst->count stays exact (only relinks increment
 * it). Single-threaded use (post-join merges); dst->last is not maintained here. */
static void dense_cell_merge_bucket_range(dense_cell_map_t *dst, dense_cell_map_t *src, summary_t *sum, size_t bi_lo, size_t bi_hi) {
    size_t bi;

    if (!dst || !src) return;
    for (bi = bi_lo; bi < bi_hi; bi++) {
        dense_node_t *n = src->buckets[bi];

        src->buckets[bi] = NULL;
        while (n) {
            dense_node_t *nx = n->next;
            dense_node_t **pp = NULL;
            dense_node_t *hit = NULL;
            size_t dbi;

            if (dst->nbuckets) {
                dbi = (size_t)n->hash & (dst->nbuckets - 1);
                pp = &dst->buckets[dbi];
                while (*pp) {
                    if ((*pp)->hash == n->hash && strcmp((*pp)->parent, n->parent) == 0) {
                        hit = *pp;
                        break;
                    }
                    pp = &(*pp)->next;
                }
            }
            if (hit) {
                hit->n += n->n;
                free(n); /* redundant src node; parent (if owned) lives in its trailing bytes */
                n = nx;
                continue;
            }
            if (dense_cell_reserve(dst) != 0) {
                /* OOM: cannot grow dst. Drop this node and the rest of the src chain. */
                if (sum) sum->bad_input_files++;
                free(n);
                n = nx;
                while (n) {
                    nx = n->next;
                    free(n);
                    n = nx;
                }
                break;
            }
            dbi = (size_t)n->hash & (dst->nbuckets - 1);
            n->next = dst->buckets[dbi];
            dst->buckets[dbi] = n;
            dst->count++;
            n = nx;
        }
    }
}

/*
 * Merge src into dst by re-inserting every src node (dst grows its own dynamic hash as needed).
 * Sequential: with per-map dynamic bucket counts the old bucket-index-parallel two-phase merge no
 * longer holds (a parent need not map to the same bucket index in src and dst), so we use the
 * hash-based re-insert path. This runs post-join (no concurrent writers); coarser parallelism is
 * preserved by the caller's tournament (distinct cells/pairs merged on distinct threads).
 */
static void dense_cell_merge_into_ex(dense_cell_map_t *dst, dense_cell_map_t *src, summary_t *sum, int merge_threads) {
    (void)merge_threads;
    if (!dst || !src) return;
    dense_cell_merge_bucket_range(dst, src, sum, 0, src->nbuckets);
}

static void dense_cell_merge_add(dense_cell_map_t *dst, const dense_cell_map_t *src, summary_t *sum) {
    size_t bi;

    if (!dst || !src) return;
    for (bi = 0; bi < src->nbuckets; bi++) {
        const dense_node_t *n;

        for (n = src->buckets[bi]; n; n = n->next) {
            if (dense_cell_add_h(dst, n->parent, n->hash, n->n, sum) != 0) return;
        }
    }
}

/* Allocate the lookup bucket array sized to the parent count (power-of-two, load factor ~1) so
 * probes stay O(1). No-op if already built. Returns 0 on success, -1 on OOM. */
static int dense_cell_fanout_lookup_reserve(dense_cell_fanout_lookup_t *lk, size_t want_nodes) {
    size_t nb = 1024;

    if (!lk || lk->buckets) return 0;
    while (nb < want_nodes) nb <<= 1;
    lk->buckets = (dense_node_t **)calloc(nb, sizeof(*lk->buckets));
    if (!lk->buckets) return -1;
    lk->nbuckets = nb;
    return 0;
}

/*
 * Move every node of the merged narrow fanout map into the lookup. This is a one-time O(nodes)
 * relink (the heavy cost on large datasets is the *probes* afterward, which the dynamic sizing
 * makes O(1)); a serial pass avoids the per-thread nt*nbuckets staging that a parallel partition
 * would need and that does not scale to a large dynamic bucket count.
 */
static void dense_cell_steal_into_fanout_lookup(dense_cell_map_t *narrow, dense_cell_fanout_lookup_t *lk) {
    size_t bi;
    size_t mask;

    if (!narrow || !lk) return;
    if (!lk->buckets) {
        size_t want = narrow->count ? narrow->count : 1024;
        if (dense_cell_fanout_lookup_reserve(lk, want) != 0) return; /* OOM: leave empty (probes return 0) */
    }
    mask = lk->nbuckets - 1;
    for (bi = 0; bi < narrow->nbuckets; bi++) {
        dense_node_t *n = narrow->buckets[bi];

        narrow->buckets[bi] = NULL;
        while (n) {
            dense_node_t *nx = n->next;
            size_t biw = (size_t)n->hash & mask;

            n->next = lk->buckets[biw];
            lk->buckets[biw] = n;
            n = nx;
        }
    }
}

static void dense_cell_steal_into_fanout_lookup_ex(dense_cell_map_t *narrow, dense_cell_fanout_lookup_t *lk, int merge_threads) {
    (void)merge_threads;
    dense_cell_steal_into_fanout_lookup(narrow, lk);
}

/* _h variants take the parent's full dense hash (callers iterate dense nodes that
 * already carry ->hash), so emission lookups don't re-run dense_hash_full per row.
 * All dense_node_t are created with ->hash set, so the hash pre-check is exact. */
static uint64_t dense_cell_fanout_lookup_get_n_h(const dense_cell_fanout_lookup_t *lk, const char *parent, uint32_t h) {
    size_t biw;
    const dense_node_t *n;

    if (!lk || !parent || lk->nbuckets == 0) return 0;
    biw = (size_t)h & (lk->nbuckets - 1);
    for (n = lk->buckets[biw]; n; n = n->next) {
        if (n->hash == h && strcmp(n->parent, parent) == 0) return n->n;
    }
    return 0;
}

static uint64_t dense_cell_map_get_n_h(const dense_cell_map_t *m, const char *parent, uint32_t h) {
    size_t bi;
    const dense_node_t *n;

    if (!m || !parent || m->nbuckets == 0) return 0;
    bi = (size_t)h & (m->nbuckets - 1);
    for (n = m->buckets[bi]; n; n = n->next) {
        if (n->hash == h && strcmp(n->parent, parent) == 0) return n->n;
    }
    return 0;
}

static void dense_cell_fanout_lookup_free(dense_cell_fanout_lookup_t *lk) {
    size_t bi;

    if (!lk || !lk->buckets) return;
    for (bi = 0; bi < lk->nbuckets; bi++) {
        dense_node_t *n = lk->buckets[bi];

        lk->buckets[bi] = NULL;
        while (n) {
            dense_node_t *nx = n->next;

            free(n);
            n = nx;
        }
    }
    free(lk->buckets);
    lk->buckets = NULL;
    lk->nbuckets = 0;
}

/*
 * slice_parents: per-cell map keyed by parent dir, counts regular files in that heat-map cell under each parent.
 * fanout_lookup: merged crawl-wide immediate-child counts per parent directory (65k buckets for fast lookup).
 * Returns max global_fanout[P] over parents P appearing in the slice map.
 */
typedef struct {
    const dense_cell_map_t *slice;
    const dense_cell_fanout_lookup_t *lk;
    size_t bi_lo;
    size_t bi_hi;
    uint64_t mx;
} fanout_max_wctx_t;

static void *fanout_max_worker(void *vp) {
    fanout_max_wctx_t *c = (fanout_max_wctx_t *)vp;
    size_t bi;
    uint64_t mx = 0;

    for (bi = c->bi_lo; bi < c->bi_hi; bi++) {
        const dense_node_t *n;

        for (n = c->slice->buckets[bi]; n; n = n->next) {
            uint64_t g = dense_cell_fanout_lookup_get_n_h(c->lk, n->parent, n->hash);

            if (g > mx) mx = g;
        }
    }
    c->mx = mx;
    return NULL;
}

static uint64_t fanout_max_among_parents_serial(const dense_cell_map_t *slice_parents,
                                                const dense_cell_fanout_lookup_t *fanout_lookup) {
    size_t bi;
    uint64_t mx = 0;

    if (!slice_parents || !fanout_lookup) return 0;
    for (bi = 0; bi < slice_parents->nbuckets; bi++) {
        const dense_node_t *n;

        for (n = slice_parents->buckets[bi]; n; n = n->next) {
            uint64_t g = dense_cell_fanout_lookup_get_n_h(fanout_lookup, n->parent, n->hash);

            if (g > mx) mx = g;
        }
    }
    return mx;
}

static uint64_t dense_cell_max_fanout_among_parents(const dense_cell_map_t *slice_parents,
                                                    const dense_cell_fanout_lookup_t *fanout_lookup) {
    int ninner;
    int ninner_cap;
    int ti;
    int k;
    uint64_t mx;
    fanout_max_wctx_t *ctx = NULL;
    pthread_t *tp = NULL;

    if (!slice_parents || !fanout_lookup) return 0;

    ninner_cap = parse_ereport_thread_count();
    if (ninner_cap < 1) ninner_cap = 1;
    if (ninner_cap > FANOUT_MAX_INNER_NT) ninner_cap = FANOUT_MAX_INNER_NT;

    if (ninner_cap < 2 || dense_cell_total_nodes(slice_parents) < FANOUT_MAX_PAR_MIN_NODES)
        return fanout_max_among_parents_serial(slice_parents, fanout_lookup);

    for (ninner = ninner_cap; ninner >= 2;) {
        ctx = (fanout_max_wctx_t *)calloc((size_t)ninner, sizeof(*ctx));
        tp = (pthread_t *)calloc((size_t)(ninner - 1), sizeof(*tp));
        if (ctx && tp) break;
        free(ctx);
        ctx = NULL;
        free(tp);
        tp = NULL;
        if (ninner <= 2) return fanout_max_among_parents_serial(slice_parents, fanout_lookup);
        ninner = (ninner > 4 ? ninner / 2 : ninner - 1);
    }

    for (ti = 0; ti < ninner; ti++) {
        ctx[ti].slice = slice_parents;
        ctx[ti].lk = fanout_lookup;
        ctx[ti].bi_lo = (size_t)ti * slice_parents->nbuckets / (size_t)ninner;
        ctx[ti].bi_hi = ((size_t)ti + 1) * slice_parents->nbuckets / (size_t)ninner;
        ctx[ti].mx = 0;
    }

    for (ti = 1; ti < ninner; ti++) {
        if (pthread_create(&tp[ti - 1], NULL, fanout_max_worker, &ctx[ti]) != 0) {
            for (k = 0; k < ti - 1; k++) pthread_join(tp[k], NULL);
            free(ctx);
            free(tp);
            return fanout_max_among_parents_serial(slice_parents, fanout_lookup);
        }
    }

    fanout_max_worker(&ctx[0]);
    for (k = 0; k < ninner - 1; k++) pthread_join(tp[k], NULL);

    mx = ctx[0].mx;
    for (ti = 1; ti < ninner; ti++) {
        if (ctx[ti].mx > mx) mx = ctx[ti].mx;
    }
    free(ctx);
    free(tp);
    return mx;
}

static void dense_cell_free(dense_cell_map_t *m) {
    size_t bi;

    if (!m) return;
    for (bi = 0; bi < m->nbuckets; bi++) {
        dense_node_t *n = m->buckets[bi];

        m->buckets[bi] = NULL;
        while (n) {
            dense_node_t *nx = n->next;
            free(n);
            n = nx;
        }
    }
    free(m->buckets);
    m->buckets = NULL;
    m->nbuckets = 0;
    m->count = 0;
    m->last = NULL;
    m->last_id = 0;
}

static void fanout_parent_stat_map_free(fanout_parent_stat_map_t *m) {
    size_t bi;

    if (!m) return;
    for (bi = 0; bi < m->nbuckets; bi++) {
        fanout_parent_stat_node_t *n = m->buckets[bi];

        m->buckets[bi] = NULL;
        while (n) {
            fanout_parent_stat_node_t *nx = n->next;

            if (!m->parents_external) free(n->parent);
            free(n);
            n = nx;
        }
    }
    free(m->buckets);
    m->buckets = NULL;
    m->nbuckets = 0;
    m->count = 0;
    m->last = NULL;
    m->last_id = 0;
}

/* Lazily create / double + rehash the bucket array so the load factor stays ~<=1. Nodes are only
 * relinked (addresses preserved, so m->last stays valid). Single-threaded use (per-worker accumulate
 * and post-join merges). Returns -1 on OOM (map unchanged). */
static int fanout_parent_stat_reserve(fanout_parent_stat_map_t *m) {
    size_t newn;
    fanout_parent_stat_node_t **nb;
    size_t i;

    if (m->buckets && m->count < m->nbuckets) return 0;
    newn = m->nbuckets ? (m->nbuckets << 1)
                       : (m->reserve_buckets ? m->reserve_buckets : (size_t)DENSE_PARENT_BUCKETS_INIT);
    nb = (fanout_parent_stat_node_t **)calloc(newn, sizeof(*nb));
    if (!nb) return -1;
    alloc_hint_hugepages(nb, newn * sizeof(*nb));
    alloc_prefault(nb, newn * sizeof(*nb));
    for (i = 0; i < m->nbuckets; i++) {
        fanout_parent_stat_node_t *n = m->buckets[i];

        while (n) {
            fanout_parent_stat_node_t *nx = n->next;
            size_t bi = (size_t)n->hash & (newn - 1);

            n->next = nb[bi];
            nb[bi] = n;
            n = nx;
        }
    }
    free(m->buckets);
    m->buckets = nb;
    m->nbuckets = newn;
    return 0;
}

static void fanout_parent_stat_absorb_one(fanout_parent_stat_map_t *dst, fanout_parent_stat_node_t *src_node, summary_t *sum) {
    size_t biw;
    fanout_parent_stat_node_t **pp;

    (void)sum;
    if (dst->nbuckets) {
        biw = (size_t)src_node->hash & (dst->nbuckets - 1); /* node carries its full hash */
        pp = &dst->buckets[biw];
        while (*pp) {
            if ((*pp)->hash == src_node->hash && strcmp((*pp)->parent, src_node->parent) == 0) {
                (*pp)->n_files += src_node->n_files;
                (*pp)->n_dirs += src_node->n_dirs;
                (*pp)->n_others += src_node->n_others;
                if (src_node->have_time) {
                    if (!(*pp)->have_time) {
                        (*pp)->t_min = src_node->t_min;
                        (*pp)->t_max = src_node->t_max;
                        (*pp)->have_time = 1;
                    } else {
                        if (src_node->t_min < (*pp)->t_min) (*pp)->t_min = src_node->t_min;
                        if (src_node->t_max > (*pp)->t_max) (*pp)->t_max = src_node->t_max;
                    }
                }
                if (!dst->parents_external) free(src_node->parent);
                free(src_node);
                return;
            }
            pp = &(*pp)->next;
        }
    }
    if (fanout_parent_stat_reserve(dst) != 0) {
        /* OOM: cannot grow dst; drop this node rather than corrupt the table. */
        if (!dst->parents_external) free(src_node->parent);
        free(src_node);
        return;
    }
    biw = (size_t)src_node->hash & (dst->nbuckets - 1);
    src_node->next = dst->buckets[biw];
    dst->buckets[biw] = src_node;
    dst->count++;
}

static void fanout_parent_stat_merge_bucket_range_serial(fanout_parent_stat_map_t *dst, fanout_parent_stat_map_t *src, summary_t *sum) {
    size_t bi;

    if (!dst || !src) return;
    for (bi = 0; bi < src->nbuckets; bi++) {
        fanout_parent_stat_node_t *n = src->buckets[bi];

        src->buckets[bi] = NULL;
        while (n) {
            fanout_parent_stat_node_t *nx = n->next;

            n->next = NULL;
            fanout_parent_stat_absorb_one(dst, n, sum);
            n = nx;
        }
    }
}

/*
 * Merge src into dst by absorbing every src node (dst grows its own dynamic hash). Sequential:
 * with per-map dynamic bucket counts a parent need not map to the same bucket index in src and dst,
 * so the old bucket-index-parallel two-phase merge is invalid. Runs post-join (no concurrent writers);
 * coarse parallelism across worker maps is preserved by the caller's tournament reduce.
 */
static void fanout_parent_stat_merge_into_ex(fanout_parent_stat_map_t *dst, fanout_parent_stat_map_t *src, summary_t *sum, int merge_threads) {
    (void)merge_threads;
    if (!dst || !src) return;
    fanout_parent_stat_merge_bucket_range_serial(dst, src, sum);
}

static void fanout_parent_stat_node_apply(fanout_parent_stat_node_t *node,
                                          uint64_t nf, uint64_t nd, uint64_t no,
                                          uint64_t pick_ts) {
    node->n_files += nf;
    node->n_dirs += nd;
    node->n_others += no;
    if (!node->have_time) {
        node->t_min = pick_ts;
        node->t_max = pick_ts;
        node->have_time = 1;
    } else {
        if (pick_ts < node->t_min) node->t_min = pick_ts;
        if (pick_ts > node->t_max) node->t_max = pick_ts;
    }
}

/* parent_id is a stable, path-bijective identifier (crawl-bin parent_dir_id) used for
 * the last-hit fast path; the parent string is still derived for insertion/strcmp. */
static void fanout_parent_stat_accumulate(fanout_parent_stat_map_t *m,
                                          const char *child_path,
                                          uint64_t parent_id,
                                          uint8_t type,
                                          uint64_t pick_ts,
                                          summary_t *sum,
                                          const char *pre_parent,
                                          uint32_t pre_phash) {
    char parent_buf[PATH_MAX];
    const char *parent;
    uint32_t h;
    size_t biw;
    fanout_parent_stat_node_t **pp;
    fanout_parent_stat_node_t *node;
    uint64_t nf = 0;
    uint64_t nd = 0;
    uint64_t no = 0;

    if (!m || !child_path) return;
    if (type == 'f')
        nf = 1;
    else if (type == 'd')
        nd = 1;
    else
        no = 1;

    if (m->last && m->last_id == parent_id) {
        fanout_parent_stat_node_apply(m->last, nf, nd, no, pick_ts);
        return;
    }
    if (pre_parent) {
        parent = pre_parent;
        h = pre_phash;
    } else {
        if (parent_dir_to_buf(child_path, parent_buf, sizeof(parent_buf)) != 0) return;
        parent = parent_buf;
        h = dense_hash_full(parent);
    }

    if (m->nbuckets) {
        biw = (size_t)h & (m->nbuckets - 1);
        pp = &m->buckets[biw];
        while (*pp) {
            if ((*pp)->hash == h && strcmp((*pp)->parent, parent) == 0) {
                fanout_parent_stat_node_apply(*pp, nf, nd, no, pick_ts);
                m->last = *pp;
                m->last_id = parent_id;
                return;
            }
            pp = &(*pp)->next;
        }
    }
    if (fanout_parent_stat_reserve(m) != 0) {
        if (sum) sum->bad_input_files++;
        return;
    }
    node = (fanout_parent_stat_node_t *)malloc(sizeof(*node));
    if (!node) {
        if (sum) sum->bad_input_files++;
        return;
    }
    if (m->parents_external) {
        node->parent = (char *)parent; /* per-worker interned; not owned by this node */
    } else {
        node->parent = strdup(parent);
        if (!node->parent) {
            free(node);
            if (sum) sum->bad_input_files++;
            return;
        }
    }
    node->n_files = nf;
    node->n_dirs = nd;
    node->n_others = no;
    node->t_min = pick_ts;
    node->t_max = pick_ts;
    node->have_time = 1;
    node->hash = h;
    biw = (size_t)h & (m->nbuckets - 1);
    node->next = m->buckets[biw];
    m->buckets[biw] = node;
    m->count++;
    m->last = node;
    m->last_id = parent_id;
}

static const fanout_parent_stat_node_t *fanout_parent_stat_lookup(const fanout_parent_stat_map_t *m, const char *parent) {
    uint32_t h;
    size_t biw;
    const fanout_parent_stat_node_t *n;

    if (!m || !parent || m->nbuckets == 0) return NULL;
    h = dense_hash_full(parent);
    biw = (size_t)h & (m->nbuckets - 1);
    for (n = m->buckets[biw]; n; n = n->next) {
        if (n->hash == h && strcmp(n->parent, parent) == 0) return n;
    }
    return NULL;
}

static void bucket_shape_maps_destroy(dense_cell_fanout_lookup_t *lk,
                                      dense_cell_map_t merged_dense[AGE_BUCKETS][SIZE_BUCKETS],
                                      fanout_parent_stat_map_t *fanout_stats) {
    int ab, sb;

    dense_cell_fanout_lookup_free(lk);
    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) dense_cell_free(&merged_dense[ab][sb]);
    }
    fanout_parent_stat_map_free(fanout_stats);
}

static void worker_dense_maps_free(worker_arg_t *args, int nthreads) {
    int ti, ab, sb;

    if (!args || nthreads < 1) return;
    for (ti = 0; ti < nthreads; ti++) {
        dense_cell_free(&args[ti].parent_fanout);
        fanout_parent_stat_map_free(&args[ti].parent_fanout_stats);
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            for (sb = 0; sb < SIZE_BUCKETS; sb++) dense_cell_free(&args[ti].dense_maps[ab][sb]);
        }
    }
}

/*
 * Release every worker's path arena. Safe to call only after all consumers of
 * the matched-record / bucket-detail path strings (report emission) are done.
 * Arenas are zero-initialized by calloc, so calling on unused slots is a no-op.
 */
static void worker_path_arenas_destroy(worker_arg_t *args, int nthreads) {
    int ti;

    if (!args || nthreads < 1) return;
    for (ti = 0; ti < nthreads; ti++) path_arena_destroy(&args[ti].path_arena);
}

/* Opt-in (EREPORT_MEMSTATS=1) breakdown of the resident per-worker structures right after the
 * read phase — the data is fully built but the finalize merges haven't allocated yet, so this is
 * the closest printable point to peak RSS. Sums across workers; walks the fanout maps to total
 * their inline/strdup parent bytes. Diagnostic only; prints to stderr and flushes. */
static size_t ereport_catalog_bytes(const file_state_t *fs, size_t n);

static void ereport_print_memstats(const worker_arg_t *args, int nthreads, const inode_set_t *inodes,
                                   const file_state_t *file_states, size_t path_count) {
    size_t matched_cnt = 0, details_cnt = 0;
    size_t dense_nodes = 0, fanout_nodes = 0, fstat_nodes = 0;
    size_t arena_alloc = 0, arena_used = 0;
    size_t fanout_parent_bytes = 0, fstat_parent_bytes = 0;
    size_t inode_cnt = 0, inode_bytes = 0;
    size_t catalog_bytes = 0;
    int ti, ab, sb;
    double gib = 1024.0 * 1024.0 * 1024.0;

    if (!args || nthreads < 1) return;
    for (ti = 0; ti < nthreads; ti++) {
        size_t aa = 0, au = 0;
        size_t bi;

        matched_cnt += args[ti].matched_records.count;
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            for (sb = 0; sb < SIZE_BUCKETS; sb++) {
                details_cnt += args[ti].details[ab][sb].count;
                dense_nodes += args[ti].dense_maps[ab][sb].count;
            }
        }
        path_arena_bytes(&args[ti].path_arena, &aa, &au);
        arena_alloc += aa;
        arena_used += au;

        fanout_nodes += args[ti].parent_fanout.count;
        if (!args[ti].parent_fanout.parents_external) {
            for (bi = 0; bi < args[ti].parent_fanout.nbuckets; bi++) {
                const dense_node_t *n;
                for (n = args[ti].parent_fanout.buckets[bi]; n; n = n->next)
                    fanout_parent_bytes += (n->parent ? strlen(n->parent) : 0) + 1;
            }
        }
        fstat_nodes += args[ti].parent_fanout_stats.count;
        if (!args[ti].parent_fanout_stats.parents_external) {
            for (bi = 0; bi < args[ti].parent_fanout_stats.nbuckets; bi++) {
                const fanout_parent_stat_node_t *n;
                for (n = args[ti].parent_fanout_stats.buckets[bi]; n; n = n->next)
                    fstat_parent_bytes += (n->parent ? strlen(n->parent) : 0) + 1;
            }
        }
    }
    if (inodes) {
        int s;
        for (s = 0; s < INODE_SET_SHARDS; s++) {
            inode_cnt += inodes->shard[s].count;
            inode_bytes += inodes->shard[s].cap * (sizeof(inode_key_t) + 1);
        }
    }
    catalog_bytes = ereport_catalog_bytes(file_states, path_count);

    {
        size_t matched_b = matched_cnt * sizeof(matched_record_t);
        size_t details_b = details_cnt * sizeof(detail_record_t);
        size_t dense_b = dense_nodes * sizeof(dense_node_t);
        size_t fanout_b = fanout_nodes * sizeof(dense_node_t) + fanout_parent_bytes;
        size_t fstat_b = fstat_nodes * sizeof(fanout_parent_stat_node_t) + fstat_parent_bytes;
        size_t total = matched_b + details_b + arena_alloc + dense_b + fanout_b + fstat_b
                       + inode_bytes + catalog_bytes;

        fprintf(stderr,
            "\n=== ereport memstats (post-read, per-worker sums) ===\n"
            "  matched_records : %10.2f GiB  (%zu items x %zuB)\n"
            "  bucket details  : %10.2f GiB  (%zu items x %zuB)\n"
            "  path arena      : %10.2f GiB  (used %.2f GiB; parents+leaves)\n"
            "  dense maps      : %10.2f GiB  (%zu nodes x %zuB, parents interned in arena)\n"
            "  parent_fanout   : %10.2f GiB  (%zu nodes; own parents %.2f GiB, 0 if interned)\n"
            "  fanout_stats    : %10.2f GiB  (%zu nodes; own parents %.2f GiB, 0 if interned)\n"
            "  inode set       : %10.2f GiB  (%zu inodes)\n"
            "  shard catalogs  : %10.2f GiB  (freed right after this point)\n"
            "  ------------------------------------------------\n"
            "  tracked total   : %10.2f GiB\n\n",
            matched_b / gib, matched_cnt, sizeof(matched_record_t),
            details_b / gib, details_cnt, sizeof(detail_record_t),
            arena_alloc / gib, arena_used / gib,
            dense_b / gib, dense_nodes, sizeof(dense_node_t),
            fanout_b / gib, fanout_nodes, fanout_parent_bytes / gib,
            fstat_b / gib, fstat_nodes, fstat_parent_bytes / gib,
            inode_bytes / gib, inode_cnt,
            catalog_bytes / gib,
            total / gib);
        fflush(stderr);
    }
}

/* Count one immediate child under its parent (used for crawl-wide megadir / Dense badge).
 * parent_id is a stable path-bijective key (crawl-bin parent_dir_id) for the last-hit cache. */
static void path_fanout_accumulate(dense_cell_map_t *fanout, const char *child_path, uint64_t parent_id,
                                   const char *pre_parent, uint32_t pre_phash) {
    char parent[PATH_MAX];

    if (!fanout || !child_path) return;
    if (fanout->last && fanout->last_id == parent_id) {
        fanout->last->n += 1ULL;
        return;
    }
    if (pre_parent) {
        (void)dense_cell_add_cached_h(fanout, pre_parent, parent_id, pre_phash, 1ULL, NULL);
        return;
    }
    if (parent_dir_to_buf(child_path, parent, sizeof(parent)) != 0) return;
    (void)dense_cell_add_cached(fanout, parent, parent_id, 1ULL, NULL);
}

static void path_shape_accumulate_file(summary_t *sum,
                                       dense_cell_map_t *cell_dense,
                                       int ab,
                                       int sb,
                                       const char *path,
                                       uint64_t parent_id,
                                       uint64_t accounted_bytes,
                                       const char *pre_parent,
                                       uint32_t pre_phash) {
    char parent[PATH_MAX];
    unsigned slashes;

    if (!sum || !cell_dense || !path) return;
    slashes = path_slash_count_str(path);
    if (slashes >= PATH_SHAPE_DEEP_MIN_SLASHES && accounted_bytes > 0ULL) {
        sum->shape_deep_bytes[ab][sb] += accounted_bytes;
        sum->total_shape_deep_bytes += accounted_bytes;
    }
    if (cell_dense->last && cell_dense->last_id == parent_id) {
        cell_dense->last->n += 1ULL;
        return;
    }
    if (pre_parent) {
        (void)dense_cell_add_cached_h(cell_dense, pre_parent, parent_id, pre_phash, 1ULL, sum);
        return;
    }
    if (parent_dir_to_buf(path, parent, sizeof(parent)) != 0) return;
    if (dense_cell_add_cached(cell_dense, parent, parent_id, 1ULL, sum) != 0) return;
}

static int size_bucket_for(uint64_t size) {
    if (size < 4ULL * 1024ULL) return 0;
    if (size < 1ULL * 1024ULL * 1024ULL) return 1;
    if (size < 100ULL * 1024ULL * 1024ULL) return 2;
    if (size < 1024ULL * 1024ULL * 1024ULL) return 3;
    if (size < 10ULL * 1024ULL * 1024ULL * 1024ULL) return 4;
    return 5;
}

static int age_bucket_for(uint64_t ts, time_t now) {
    uint64_t age_sec;
    uint64_t days;

    if (ts == 0 || ts > (uint64_t)now) return 0;

    age_sec = (uint64_t)now - ts;
    days = age_sec / 86400ULL;

    if (days < 30ULL) return 0;
    if (days < 90ULL) return 1;
    if (days < 180ULL) return 2;
    if (days < 365ULL) return 3;
    if (days < 3ULL * 365ULL) return 4;
    return 5;
}

static void human_bytes(uint64_t v, char *buf, size_t sz) {
    const char *units[] = {"B","K","M","G","T","P"};
    double d = (double)v;
    int i = 0;

    while (d >= 1024.0 && i < 5) {
        d /= 1024.0;
        i++;
    }

    if (d >= 100.0) snprintf(buf, sz, "%.0f%s", d, units[i]);
    else if (d >= 10.0) snprintf(buf, sz, "%.1f%s", d, units[i]);
    else snprintf(buf, sz, "%.2f%s", d, units[i]);
}

static void html_escape(FILE *out, const char *s) {
    for (; *s; s++) {
        switch (*s) {
            case '&': fputs("&amp;", out); break;
            case '<': fputs("&lt;", out); break;
            case '>': fputs("&gt;", out); break;
            case '"': fputs("&quot;", out); break;
            default: fputc(*s, out); break;
        }
    }
}

static void html_escape_segment(FILE *out, const char *s, size_t len) {
    size_t i;
    for (i = 0; i < len; i++) {
        switch (s[i]) {
            case '&': fputs("&amp;", out); break;
            case '<': fputs("&lt;", out); break;
            case '>': fputs("&gt;", out); break;
            case '"': fputs("&quot;", out); break;
            default: fputc(s[i], out); break;
        }
    }
}

static void emit_heat_badge_tip_shell_css(FILE *out);
static void emit_heat_badge_tip_install_js(FILE *out);

#define PATH_HASH_FNV_OFFSET 1469598103934665603ULL
#define PATH_HASH_FNV_PRIME 1099511628211ULL

/* FNV-1a is a left-to-right fold with no finalization, so the hash of a path
 * built up component-by-component can be carried forward: fold only the newly
 * appended bytes instead of re-hashing the whole prefix each depth level. */
static inline uint64_t path_hash_fold(uint64_t h, const char *s, size_t n) {
    size_t i;
    for (i = 0; i < n; i++) {
        h ^= (unsigned char)s[i];
        h *= PATH_HASH_FNV_PRIME;
    }
    return h;
}

static uint64_t path_hash(const char *s) {
    uint64_t h = PATH_HASH_FNV_OFFSET;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= PATH_HASH_FNV_PRIME;
    }
    return h;
}

/* Open-hash capacity for `record_count` inserts at trie depth `depth` (0 = first path
 * component after base_prefix). Reduces rehash + page-fault churn during bucket-detail
 * aggregation; capped so flat megadirs do not allocate huge tables for few unique keys. */
static size_t path_row_map_est_initial_cap(size_t record_count, int depth) {
    size_t est;

    if (record_count == 0) return 4096;
    /*
     * Distinct keys at this depth are bounded by record_count. Assume the trie roughly
     * halves the number of distinct prefixes per level (branching ~2): depth 0 uses the
     * full count (shift 0) so flat megadirs — where every record is a distinct depth-0
     * key — presize correctly instead of rehashing up from a tiny table, while deeper
     * levels shrink geometrically. Summed across depths this stays ~2× record_count.
     */
    if (depth <= 0) {
        est = record_count;
    } else if (depth >= (int)(sizeof(size_t) * 8)) {
        est = 0;
    } else {
        est = record_count >> (unsigned)depth;
    }
    if (est < 4096) est = 4096;
    if (est > record_count) est = record_count;
    if (est > (1u << 26)) est = (1u << 26); /* 64M-slot safety cap (~5 GB table) before load-factor scaling */
    /* Match get_or_insert_h load factor 7/10 so the first growth rehash is avoided. */
    return (est * 10 + 6) / 7;
}

/*
 * Slot count for a page that will insert `record_count` records at `depth`, when the previous
 * page on this worker is available to learn from. The branching-2 guess above is only a guess;
 * the distinct-row count the last page actually produced at this level, scaled by how much
 * bigger this page is, is a far better one — it stops flat megadirs from rehashing their way
 * up at every level and stops deep levels from reserving room they never use.
 */
static size_t path_row_map_seed_cap(size_t record_count, int depth, size_t prev_distinct, size_t prev_records) {
    size_t est;

    if (record_count == 0 || prev_records == 0) return path_row_map_est_initial_cap(record_count, depth);

    est = (size_t)((double)prev_distinct * (double)record_count / (double)prev_records * 1.25) + 64;
    if (est > record_count) est = record_count;
    if (est < 4096) est = 4096;
    if (est > (1u << 26)) est = (1u << 26);
    return (est * 10 + 6) / 7;
}

static void path_row_map_keys_free(path_row_map_t *m) {
    path_key_blk_t *b = m->keys;

    while (b) {
        path_key_blk_t *next = b->next;

        free(b);
        b = next;
    }
    m->keys = NULL;
    m->keys_cur = NULL;
}

static char *path_row_map_key_intern(path_row_map_t *m, const char *s, size_t n) {
    size_t need = n + 1U;
    path_key_blk_t *b = m->keys_cur;
    char *out;

    if (b && b->cap - b->len < need) b = b->next; /* blocks past the cursor are empty */
    if (b && b->cap - b->len < need) b = NULL;
    if (!b) {
        size_t cap = need > PATH_KEY_BLK_BYTES ? need : PATH_KEY_BLK_BYTES;

        b = (path_key_blk_t *)malloc(sizeof(*b) + cap);
        if (!b) return NULL;
        b->cap = cap;
        b->len = 0;
        b->next = NULL;
        if (m->keys_cur) {
            b->next = m->keys_cur->next;
            m->keys_cur->next = b;
        } else {
            b->next = m->keys;
            m->keys = b;
        }
    }
    out = b->data + b->len;
    memcpy(out, s, need);
    b->len += need;
    m->keys_cur = b;
    return out;
}

static int path_row_map_init(path_row_map_t *m, size_t initial_cap) {
    size_t cap = 1;
    while (cap < initial_cap) cap <<= 1;

    m->rows = (path_row_t *)calloc(cap, sizeof(*m->rows));
    m->used = (unsigned char *)calloc(cap, sizeof(*m->used));
    m->keys = NULL;
    m->keys_cur = NULL;
    if (!m->rows || !m->used) {
        free(m->rows);
        free(m->used);
        m->rows = NULL;
        m->used = NULL;
        m->cap = 0;
        m->count = 0;
        return -1;
    }

    /* Deliberately not alloc_prefault()ed, unlike the parent tables: initial_cap here is an upper
     * bound (record count shifted by trie depth), and the deeper levels stay nearly empty, so
     * populating them made resident what calloc was right to leave untouched — 1.3 GB -> 2.6 GB peak
     * on a 1.3M-record capture. Faulting these in on demand is the cheaper trade. */
    alloc_hint_hugepages(m->rows, cap * sizeof(*m->rows));

    m->cap = cap;
    m->count = 0;
    return 0;
}

static void path_row_map_destroy(path_row_map_t *m) {
    path_row_map_keys_free(m);
    free(m->rows);
    free(m->used);
    m->rows = NULL;
    m->used = NULL;
    m->cap = 0;
    m->count = 0;
}

/*
 * Ready a map for the next page. Only the slots the last page occupied are touched, so a page
 * that filled a corner of a large table does not fault in the rest of it; the key arena keeps
 * its blocks and rewinds. A table far larger than the next page needs is dropped instead, so
 * one huge page does not pin its capacity for the rest of the run.
 */
static int path_row_map_reset_or_init(path_row_map_t *m, size_t want_cap) {
    size_t i;
    path_key_blk_t *b;

    if (!m->rows || m->cap < want_cap || m->cap > want_cap * 4U) {
        path_row_map_destroy(m);
        return path_row_map_init(m, want_cap);
    }

    for (i = 0; i < m->cap; i++) {
        if (!m->used[i]) continue;
        memset(&m->rows[i], 0, sizeof(m->rows[i]));
        m->used[i] = 0;
    }
    m->count = 0;
    for (b = m->keys; b; b = b->next) b->len = 0;
    m->keys_cur = m->keys;
    return 0;
}

static int path_row_map_rehash(path_row_map_t *m, size_t new_cap) {
    path_row_t *new_rows = (path_row_t *)calloc(new_cap, sizeof(*new_rows));
    unsigned char *new_used = (unsigned char *)calloc(new_cap, sizeof(*new_used));
    size_t i;

    if (!new_rows || !new_used) {
        free(new_rows);
        free(new_used);
        return -1;
    }

    for (i = 0; i < m->cap; i++) {
        if (m->used[i]) {
            path_row_t row = m->rows[i];
            size_t idx = (size_t)(row.hash & (new_cap - 1));
            while (new_used[idx]) idx = (idx + 1) & (new_cap - 1);
            new_rows[idx] = row;
            new_used[idx] = 1;
        }
    }

    free(m->rows);
    free(m->used);
    m->rows = new_rows;
    m->used = new_used;
    m->cap = new_cap;
    return 0;
}

/*
 * Both take the full-path FNV hash precomputed by the caller, built incrementally across depth
 * levels so a path is hashed once rather than once per level, and the path's length, which the
 * callers are already carrying to build the path.
 *
 * The stored hash settles almost every probe. Without it each collision walked two paths byte by
 * byte, and directory paths share long prefixes, so strcmp read most of the string before finding
 * the difference. What is left after the hash check is mostly the true match, which has to read
 * the whole key either way -- so compare the lengths first (a mismatch ends it on a register) and
 * memcmp a known span rather than have strcmp look for a NUL it already knows the position of.
 */
static inline int path_row_key_eq(const path_row_t *row, const char *path, size_t len, uint64_t hash) {
    return row->hash == hash && row->path_len == (uint32_t)len && memcmp(row->path, path, len) == 0;
}

static path_row_t *path_row_map_get_or_insert_h(path_row_map_t *m, const char *path, size_t path_len,
                                                uint64_t hash) {
    size_t idx;

    if ((m->count + 1) * 10 >= m->cap * 7) {
        if (path_row_map_rehash(m, m->cap << 1) != 0) return NULL;
    }

    idx = (size_t)(hash & (m->cap - 1));
    while (m->used[idx]) {
        if (path_row_key_eq(&m->rows[idx], path, path_len, hash)) return &m->rows[idx];
        idx = (idx + 1) & (m->cap - 1);
    }

    m->rows[idx].path = path_row_map_key_intern(m, path, path_len);
    if (!m->rows[idx].path) return NULL;
    m->rows[idx].hash = hash;
    m->rows[idx].path_len = (uint32_t)path_len;
    m->used[idx] = 1;
    m->count++;
    return &m->rows[idx];
}

static path_row_t *path_row_map_find_h(path_row_map_t *m, const char *path, size_t path_len, uint64_t hash) {
    size_t idx;

    if (m->cap == 0) return NULL;

    idx = (size_t)(hash & (m->cap - 1));
    while (m->used[idx]) {
        if (path_row_key_eq(&m->rows[idx], path, path_len, hash)) return &m->rows[idx];
        idx = (idx + 1) & (m->cap - 1);
    }

    return NULL;
}

static int path_row_map_collect(path_row_map_t *m, path_row_t ***out_rows, size_t *out_count) {
    path_row_t **rows;
    size_t i;
    size_t j = 0;

    rows = (path_row_t **)calloc(m->count ? m->count : 1, sizeof(*rows));
    if (!rows) return -1;

    for (i = 0; i < m->cap; i++) {
        if (m->used[i]) rows[j++] = &m->rows[i];
    }

    *out_rows = rows;
    *out_count = j;
    return 0;
}

/* Add src row bucket counters into dst (same path keys). total_* fields are merged additively for callers that pre-fill them. */
static int path_row_map_merge_accumulate(path_row_map_t *dst, const path_row_map_t *src) {
    size_t i;

    if (!dst || !src) return 0;
    for (i = 0; i < src->cap; i++) {
        if (!src->used[i]) continue;
        {
            const path_row_t *sr = &src->rows[i];
            path_row_t *dr = path_row_map_get_or_insert_h(dst, sr->path, sr->path_len, sr->hash);

            if (!dr) return -1;
            dr->bucket_files += sr->bucket_files;
            dr->bucket_bytes += sr->bucket_bytes;
            dr->bucket_ctime_led_files += sr->bucket_ctime_led_files;
            dr->bucket_ctime_led_bytes += sr->bucket_ctime_led_bytes;
            dr->total_files += sr->total_files;
            dr->total_dirs += sr->total_dirs;
            dr->total_bytes += sr->total_bytes;
        }
    }
    return 0;
}

static int cmp_row_bucket_bytes_desc(const void *a, const void *b) {
    const path_row_t *ra = *(const path_row_t * const *)a;
    const path_row_t *rb = *(const path_row_t * const *)b;
    if (ra->bucket_bytes < rb->bucket_bytes) return 1;
    if (ra->bucket_bytes > rb->bucket_bytes) return -1;
    if (ra->bucket_files < rb->bucket_files) return 1;
    if (ra->bucket_files > rb->bucket_files) return -1;
    return strcmp(ra->path, rb->path);
}

/* Max-heap by cmp_row_bucket_bytes_desc "badness" (root = worst among kept rows). Used to pick top BUCKET_PATH_TABLE_MAX_ROWS without sorting millions of paths. */
static void path_row_topk_heap_sift_down(path_row_t **h, size_t n, size_t i) {
    for (;;) {
        size_t l = 2 * i + 1, r = l + 1, mx = i;

        if (l < n && cmp_row_bucket_bytes_desc(&h[l], &h[mx]) > 0) mx = l;
        if (r < n && cmp_row_bucket_bytes_desc(&h[r], &h[mx]) > 0) mx = r;
        if (mx == i) break;
        {
            path_row_t *t = h[i];
            h[i] = h[mx];
            h[mx] = t;
        }
        i = mx;
    }
}

static void path_row_topk_heap_push(path_row_t **h, size_t *hsz, size_t kcap, path_row_t *row) {
    size_t i;

    if (*hsz < kcap) {
        i = *hsz;
        h[i] = row;
        *hsz = i + 1;
        while (i > 0) {
            size_t p = (i - 1) / 2;

            if (cmp_row_bucket_bytes_desc(&h[i], &h[p]) <= 0) break;
            {
                path_row_t *t = h[i];
                h[i] = h[p];
                h[p] = t;
            }
            i = p;
        }
        return;
    }
    if (cmp_row_bucket_bytes_desc(&row, &h[0]) >= 0) return;
    h[0] = row;
    path_row_topk_heap_sift_down(h, kcap, 0);
}

static int starts_with_dir_prefix(const char *path, const char *prefix);

static char *dup_common_dir_prefix(const bucket_details_t *details) {
    char *prefix;
    size_t i;
    size_t len;

    char pbuf0[PATH_MAX];
    char pbufi[PATH_MAX];

    if (details->count == 0) return strdup("");

    prefix = strdup(path_join_pl(details->items[0].parent, details->items[0].leaf, pbuf0, sizeof(pbuf0)));
    if (!prefix) return NULL;
    len = strlen(prefix);

    for (i = 1; i < details->count; i++) {
        const char *p = path_join_pl(details->items[i].parent, details->items[i].leaf, pbufi, sizeof(pbufi));
        size_t j = 0;
        while (j < len && prefix[j] && p[j] && prefix[j] == p[j]) j++;
        len = j;
        prefix[len] = '\0';
        if (len == 0) break;
    }

    while (len > 1 && prefix[len - 1] == '/') prefix[--len] = '\0';

    /*
     * If every path is exactly this prefix or continues as prefix/..., the prefix is already a
     * directory boundary (e.g. shared crawl root /a/b/001). Do not strip the last segment — that
     * incorrectly turns /orcd/.../001 into /orcd/... and shifts all level tables by one.
     * Strip one component only when the character-wise LCP ends inside a filename segment
     * (e.g. /a/b/file1 vs /a/b/file2 → /a/b/fi → parent /a/b).
     */
    {
        int all_dir_boundary = 1;

        for (i = 0; i < details->count; i++) {
            const char *pth = path_join_pl(details->items[i].parent, details->items[i].leaf, pbufi, sizeof(pbufi));
            if (!starts_with_dir_prefix(pth, prefix)) {
                all_dir_boundary = 0;
                break;
            }
        }

        if (!all_dir_boundary) {
            char *slash = strrchr(prefix, '/');
            if (slash) {
                if (slash == prefix) prefix[1] = '\0';
                else *slash = '\0';
            } else if (pbuf0[0] == '/') {
                free(prefix);
                prefix = strdup("/");
                if (!prefix) return NULL;
            } else {
                prefix[0] = '\0';
            }
        }
    }

    return prefix;
}

static int starts_with_dir_prefix(const char *path, const char *prefix) {
    size_t plen;

    if (!prefix || prefix[0] == '\0') return 1;
    if (strcmp(prefix, "/") == 0) return path[0] == '/';

    plen = strlen(prefix);
    if (strncmp(path, prefix, plen) != 0) return 0;
    return path[plen] == '\0' || path[plen] == '/';
}

/* Same test against a split (parent-incl-slash, leaf) record, without materialising the join. */
static int starts_with_dir_prefix_pl(const char *parent, const char *leaf, const char *prefix) {
    size_t plen;
    size_t parlen;

    if (!prefix || prefix[0] == '\0') return 1;
    parent = parent ? parent : "";
    leaf = leaf ? leaf : "";
    if (strcmp(prefix, "/") == 0) return (parent[0] ? parent[0] : leaf[0]) == '/';

    plen = strlen(prefix);
    parlen = strlen(parent);
    if (plen <= parlen) {
        if (memcmp(parent, prefix, plen) != 0) return 0;
        /* prefix ends inside the parent: the following byte of the join is the parent's or the leaf's */
        return parent[plen] == '\0' ? (leaf[0] == '\0' || leaf[0] == '/') : parent[plen] == '/';
    }
    if (memcmp(parent, prefix, parlen) != 0) return 0;
    {
        const char *rest = prefix + parlen;
        size_t restlen = plen - parlen;

        if (strncmp(leaf, rest, restlen) != 0) return 0;
        return leaf[restlen] == '\0' || leaf[restlen] == '/';
    }
}

/*
 * Directory-aware sort: all paths under a POSIX directory prefix are contiguous (unlike raw strcmp),
 * so we can binary-search a slice of matched records per bucket page instead of scanning the full corpus.
 */
static int path_cmp_seg(const char *a, const char *b) {
    if (!a) a = "";
    if (!b) b = "";
    for (;;) {
        while (*a == '/') a++;
        while (*b == '/') b++;
        if (*a == '\0' && *b == '\0') return 0;
        if (*a == '\0') return -1;
        if (*b == '\0') return 1;
        {
            const char *na = strchr(a, '/');
            const char *nb = strchr(b, '/');
            size_t la = na ? (size_t)(na - a) : strlen(a);
            size_t lb = nb ? (size_t)(nb - b) : strlen(b);
            int c = memcmp(a, b, la < lb ? la : lb);
            if (c != 0) return c;
            if (la != lb) return la < lb ? -1 : 1;
            a = na ? na + 1 : a + la;
            b = nb ? nb + 1 : b + lb;
        }
    }
}

/*
 * path_cmp_seg over two split (parent-incl-slash, leaf) records, without materialising either path.
 * Defined next to the transform cursor it walks; declared here because the comparators below predate it.
 */
static int path_cmp_seg_pl(const char *pa, const char *la, const char *pb, const char *lb);

static size_t matched_ord_lower_bound_seg(const matched_records_t *rec, const size_t *ord, size_t n, const char *key) {
    size_t lo = 0;
    size_t hi = n;

    if (!key) key = "";
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const matched_record_t *r = &rec->items[ord[mid]];

        if (path_cmp_seg_pl(r->parent, r->leaf, NULL, key) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

/*
 * Smallest i in [lo, n) where path for ord[i] is not under prefix (directory-aware). Returns n if all
 * indices in [lo, n) are still under prefix. Replaces O(n) linear extension of hi in aggregate_totals.
 */
static size_t matched_ord_first_not_under_prefix(const matched_records_t *rec,
                                                 const size_t *ord,
                                                 size_t n,
                                                 size_t lo,
                                                 const char *prefix) {
    size_t a = lo;
    size_t b = n;

    if (!rec || !ord || lo > n) return n;
    while (a < b) {
        size_t mid = a + (b - a) / 2;
        const matched_record_t *r = &rec->items[ord[mid]];

        if (starts_with_dir_prefix_pl(r->parent, r->leaf, prefix))
            a = mid + 1;
        else
            b = mid;
    }
    return a;
}

/* Set only around qsort of matched-record indices (single-threaded). */
static const matched_record_t *g_bucket_path_sort_items;

static int matched_path_ord_cmp(const void *aa, const void *bb) {
    const matched_record_t *ra = &g_bucket_path_sort_items[*(const size_t *)aa];
    const matched_record_t *rb = &g_bucket_path_sort_items[*(const size_t *)bb];

    return path_cmp_seg_pl(ra->parent, ra->leaf, rb->parent, rb->leaf);
}

/* Thread-safe comparator for parallel merge-sort leaves (items via qsort_r context, not globals). */
static int matched_path_ord_cmp_r(const void *aa, const void *bb, void *ctx) {
    const matched_record_t *items = (const matched_record_t *)ctx;
    const matched_record_t *ra = &items[*(const size_t *)aa];
    const matched_record_t *rb = &items[*(const size_t *)bb];

    return path_cmp_seg_pl(ra->parent, ra->leaf, rb->parent, rb->leaf);
}

static void merge_ord_path_slice(const matched_record_t *items,
                                 size_t *ord,
                                 size_t *tmp,
                                 size_t lo,
                                 size_t mid,
                                 size_t hi) {
    size_t i = lo, j = mid, k = lo;

    while (i < mid && j < hi) {
        const matched_record_t *ra = &items[ord[i]];
        const matched_record_t *rb = &items[ord[j]];

        if (path_cmp_seg_pl(ra->parent, ra->leaf, rb->parent, rb->leaf) <= 0)
            tmp[k++] = ord[i++];
        else
            tmp[k++] = ord[j++];
    }
    while (i < mid) tmp[k++] = ord[i++];
    while (j < hi) tmp[k++] = ord[j++];
    memcpy(ord + lo, tmp + lo, (hi - lo) * sizeof(size_t));
}

typedef struct {
    const matched_record_t *items;
    size_t *ord;
    size_t *tmp;
    size_t lo, hi;
    int depth;
    int max_depth;
} ms_heap_t;

static void matched_records_ms_parallel(const matched_record_t *items,
                                        size_t *ord,
                                        size_t *tmp,
                                        size_t lo,
                                        size_t hi,
                                        int depth,
                                        int max_depth);

static void *ms_par_left_entry(void *vp) {
    ms_heap_t *h = (ms_heap_t *)vp;
    matched_records_ms_parallel(h->items, h->ord, h->tmp, h->lo, h->hi, h->depth, h->max_depth);
    free(h);
    return NULL;
}

static void matched_records_ms_parallel(const matched_record_t *items,
                                        size_t *ord,
                                        size_t *tmp,
                                        size_t lo,
                                        size_t hi,
                                        int depth,
                                        int max_depth) {
    size_t n = hi - lo;
    size_t mid;
    pthread_t th;
    ms_heap_t *hp;

    if (n <= PATH_ORD_QSORT_THRESH) {
        if (n <= 1) return;
        qsort_r(ord + lo, n, sizeof(size_t), matched_path_ord_cmp_r, (void *)items);
        return;
    }

    mid = lo + (n >> 1);
    if (depth < max_depth) {
        ms_heap_t *hl;
        ms_heap_t *hr;
        pthread_t thl, thr;
        int rc_l;
        int rc_r;

        /*
         * Sort both halves concurrently when possible. The previous "left in a thread, right on
         * this stack" pattern kept merge-sort peak concurrency near ~2 even with a large
         * EREPORT_THREADS — bucket prep then looked "stuck" at low CPU while path order dominated.
         */
        hl = (ms_heap_t *)malloc(sizeof(*hl));
        hr = (ms_heap_t *)malloc(sizeof(*hr));
        if (hl && hr) {
            hl->items = items;
            hl->ord = ord;
            hl->tmp = tmp;
            hl->lo = lo;
            hl->hi = mid;
            hl->depth = depth + 1;
            hl->max_depth = max_depth;
            hr->items = items;
            hr->ord = ord;
            hr->tmp = tmp;
            hr->lo = mid;
            hr->hi = hi;
            hr->depth = depth + 1;
            hr->max_depth = max_depth;

            rc_l = pthread_create(&thl, NULL, ms_par_left_entry, hl);
            rc_r = pthread_create(&thr, NULL, ms_par_left_entry, hr);
            if (rc_l == 0 && rc_r == 0) {
                pthread_join(thl, NULL);
                pthread_join(thr, NULL);
                merge_ord_path_slice(items, ord, tmp, lo, mid, hi);
                return;
            }
            if (rc_l == 0) pthread_join(thl, NULL);
            else free(hl);
            if (rc_r == 0) pthread_join(thr, NULL);
            else free(hr);
            if (rc_l == 0 && rc_r != 0) {
                matched_records_ms_parallel(items, ord, tmp, mid, hi, depth + 1, max_depth);
                merge_ord_path_slice(items, ord, tmp, lo, mid, hi);
                return;
            }
            if (rc_l != 0 && rc_r == 0) {
                matched_records_ms_parallel(items, ord, tmp, lo, mid, depth + 1, max_depth);
                merge_ord_path_slice(items, ord, tmp, lo, mid, hi);
                return;
            }
            /* both pthread_create failed — fall through to single-helper or full serial */
        } else {
            free(hl);
            free(hr);
        }

        hp = (ms_heap_t *)malloc(sizeof(*hp));
        if (hp) {
            hp->items = items;
            hp->ord = ord;
            hp->tmp = tmp;
            hp->lo = lo;
            hp->hi = mid;
            hp->depth = depth + 1;
            hp->max_depth = max_depth;
            if (pthread_create(&th, NULL, ms_par_left_entry, hp) == 0) {
                matched_records_ms_parallel(items, ord, tmp, mid, hi, depth + 1, max_depth);
                pthread_join(th, NULL);
                merge_ord_path_slice(items, ord, tmp, lo, mid, hi);
                return;
            }
            free(hp);
        }
    }

    matched_records_ms_parallel(items, ord, tmp, lo, mid, depth + 1, max_depth);
    matched_records_ms_parallel(items, ord, tmp, mid, hi, depth + 1, max_depth);
    merge_ord_path_slice(items, ord, tmp, lo, mid, hi);
}

static int path_ord_merge_max_depth(void) {
    int nw = parse_ereport_thread_count();
    int d = 1;

    if (nw < 2) return 0;
    while ((1 << d) < nw && d < PATH_ORD_MAX_MS_DEPTH) d++;
    return d;
}

/*
 * MSD radix path-order sort. Produces exactly the same permutation as a
 * path_cmp_seg comparison sort (validated byte-identical over deep/flat/
 * duplicate/tricky-byte corpora) but touches each path byte O(1) times instead
 * of re-scanning shared prefixes on every comparison — the dominant cost of the
 * old merge sort on large single-shard inputs (mega-dir path index).
 *
 * Each path is treated as the byte sequence T(path): every '/'-delimited
 * component's bytes followed by a 0x00 separator, with leading/duplicate/
 * trailing slashes collapsed. memcmp over T() with shorter-first equals
 * path_cmp_seg (the separator 0x00 sorts below any component byte, which never
 * contains NUL). A per-record cursor walks one transform "class" per radix
 * level so the prefix is never re-read.
 *
 * class: 0 = ENDED (key exhausted, sorts first), 1 = SEP (the 0x00 after a
 * component), 2..257 = component byte value + 1 (byte >= 1, no NUL in names).
 */
#define PATH_RADIX_NCLASS 258
#define PATH_RADIX_SEP 1
#ifndef PATH_RADIX_LEAF
#define PATH_RADIX_LEAF 64u
#endif
#ifndef PATH_RADIX_SPAWN_MIN
#define PATH_RADIX_SPAWN_MIN (1u << 18) /* only hand a sub-bucket to a thread above this size */
#endif

/*
 * Two-part cursor over a split (parent-incl-slash, leaf) path. It produces exactly the same
 * transform-byte stream T() that the old single-string cursor produced for the joined path, so the
 * radix sort yields a byte-identical permutation. The trick: walk the parent's components, but when
 * the parent sub-cursor would emit its terminal END, instead switch to the leaf sub-cursor (whose
 * stream ends with the real END). The SEP the parent emits just before ending is exactly the
 * separator between the parent's last component and the leaf; an empty parent ("/") emits no
 * leading SEP, matching the joined "/leaf" transform.
 */
typedef struct {
    const char *p;     /* read pointer within the active sub-segment */
    const char *leaf;  /* leaf segment, walked after the parent ends */
    uint8_t st;        /* 0 = reading component, 1 = owe SEP, 2 = sub-segment ended */
    uint8_t phase;     /* 0 = walking parent, 1 = walking leaf */
} path_tcur_t;

static inline void path_tcur_init(path_tcur_t *c, const char *parent, const char *leaf) {
    const char *s = parent ? parent : "";
    while (*s == '/') s++;
    c->leaf = leaf ? leaf : "";
    c->phase = 0u;
    c->p = s;
    c->st = (*s == '\0') ? 2u : 0u;
}

static inline int path_tcur_next(path_tcur_t *c) {
    for (;;) {
        if (c->st == 2u) {
            if (c->phase == 0u) {
                /* Parent exhausted: switch to leaf instead of emitting the parent's END. */
                const char *s = c->leaf;
                while (*s == '/') s++;
                c->p = s;
                c->phase = 1u;
                c->st = (*s == '\0') ? 2u : 0u;
                continue;
            }
            return 0; /* leaf END = real END */
        }
        if (c->st == 1u) {
            c->st = 0u;
            while (*c->p == '/') c->p++;
            if (*c->p == '\0') c->st = 2u;
            return PATH_RADIX_SEP;
        }
        {
            unsigned char ch = (unsigned char)*c->p;
            c->p++;
            if (*c->p == '/' || *c->p == '\0') c->st = 1u;
            return (int)ch + 1;
        }
    }
}

/*
 * Compare the transform streams of two split paths from scratch. Same order as path_cmp_seg over the
 * joined paths (see the T() description above), but nothing is copied: a comparison that resolves in
 * the first component never reads the rest of either path. Records under one directory share their
 * interned parent pointer, so that case skips the parent entirely.
 */
static int path_cmp_seg_pl(const char *pa, const char *la, const char *pb, const char *lb) {
    path_tcur_t ca, cb;

    if (pa == pb) return path_cmp_seg(la ? la : "", lb ? lb : "");

    path_tcur_init(&ca, pa, la);
    path_tcur_init(&cb, pb, lb);
    for (;;) {
        int xa = path_tcur_next(&ca);
        int xb = path_tcur_next(&cb);

        if (xa != xb) return xa < xb ? -1 : 1;
        if (xa == 0) return 0;
    }
}

/*
 * Small-bucket leaf: comparator-sort, stable by original index for tie safety. Every record in this
 * bucket has emitted the same transform prefix, so comparing copies of their live cursors resumes at
 * the first byte that can differ instead of re-walking (and re-joining) the shared prefix.
 */
static int matched_path_radix_leaf_cmp(const void *aa, const void *bb, void *ctx) {
    const path_tcur_t *curs = (const path_tcur_t *)ctx;
    size_t ia = *(const size_t *)aa;
    size_t ib = *(const size_t *)bb;
    path_tcur_t ca = curs[ia];
    path_tcur_t cb = curs[ib];

    for (;;) {
        int xa = path_tcur_next(&ca);
        int xb = path_tcur_next(&cb);

        if (xa != xb) return xa < xb ? -1 : 1;
        if (xa == 0) break;
    }
    return ia < ib ? -1 : (ia > ib ? 1 : 0);
}

static atomic_int g_path_radix_active;
static int g_path_radix_max;
static __thread unsigned short *g_path_radix_cls;
static __thread size_t g_path_radix_cls_cap;

static void path_radix_sort(size_t *idx, size_t *tmp, path_tcur_t *curs, size_t n);

typedef struct {
    size_t *idx;
    size_t *tmp;
    path_tcur_t *curs;
    size_t n;
} path_radix_task_t;

static void *path_radix_entry(void *vp) {
    path_radix_task_t *t = (path_radix_task_t *)vp;
    path_radix_sort(t->idx, t->tmp, t->curs, t->n);
    free(t);
    /* release this worker's class scratch (thread-local; not auto-freed on join) */
    free(g_path_radix_cls);
    g_path_radix_cls = NULL;
    g_path_radix_cls_cap = 0;
    return NULL;
}

static void path_radix_sort(size_t *idx, size_t *tmp, path_tcur_t *curs, size_t n) {
    size_t count[PATH_RADIX_NCLASS];
    size_t off[PATH_RADIX_NCLASS];
    size_t cur[PATH_RADIX_NCLASS];

descend:
    if (n <= 1) return;
    if (n <= PATH_RADIX_LEAF) {
        qsort_r(idx, n, sizeof(size_t), matched_path_radix_leaf_cmp, (void *)curs);
        return;
    }
    if (g_path_radix_cls_cap < n) {
        unsigned short *nb = (unsigned short *)realloc(g_path_radix_cls, n * sizeof(*nb));
        if (!nb) { /* no scratch: fall back to comparator sort for this bucket */
            qsort_r(idx, n, sizeof(size_t), matched_path_radix_leaf_cmp, (void *)curs);
            return;
        }
        g_path_radix_cls = nb;
        g_path_radix_cls_cap = n;
    }

    memset(count, 0, sizeof(count));
    {
        size_t i;
        for (i = 0; i < n; i++) {
            int c = path_tcur_next(&curs[idx[i]]);
            g_path_radix_cls[i] = (unsigned short)c;
            count[c]++;
        }
    }

    /*
     * Shared-prefix fast path: if every key has the same class at this level the
     * permutation is unchanged, so skip the distribute + memcpy and descend in
     * place. This is what keeps deep common prefixes cheap (no per-level shuffle).
     */
    {
        int nonempty = 0;
        int single = -1;
        int c;
        for (c = 0; c < PATH_RADIX_NCLASS && nonempty < 2; c++)
            if (count[c]) { nonempty++; single = c; }
        if (nonempty == 1) {
            if (single == 0) return; /* all keys ended here: fully sorted */
            goto descend;            /* same idx range, cursors already advanced */
        }
    }

    {
        size_t acc = 0;
        int c;
        for (c = 0; c < PATH_RADIX_NCLASS; c++) { off[c] = acc; cur[c] = acc; acc += count[c]; }
    }
    {
        size_t i;
        for (i = 0; i < n; i++) { int c = g_path_radix_cls[i]; tmp[cur[c]++] = idx[i]; }
    }
    memcpy(idx, tmp, n * sizeof(size_t));

    /* Class 0 (ENDED) is fully placed (all equal). Recurse on 1..257; hand large
     * sub-buckets to worker threads while a global budget (g_path_radix_max) lasts. */
    {
        pthread_t th[PATH_RADIX_NCLASS];
        int nth = 0;
        int c;
        for (c = 1; c < PATH_RADIX_NCLASS; c++) {
            if (count[c] <= 1) continue;
            if (count[c] > PATH_RADIX_SPAWN_MIN) {
                int prev = atomic_fetch_add(&g_path_radix_active, 1);
                if (prev < g_path_radix_max) {
                    path_radix_task_t *t = (path_radix_task_t *)malloc(sizeof(*t));
                    if (t) {
                        t->idx = idx + off[c];
                        t->tmp = tmp + off[c];
                        t->curs = curs;
                        t->n = count[c];
                        if (pthread_create(&th[nth], NULL, path_radix_entry, t) == 0) { nth++; continue; }
                        free(t);
                    }
                    atomic_fetch_sub(&g_path_radix_active, 1);
                } else {
                    atomic_fetch_sub(&g_path_radix_active, 1);
                }
            }
            path_radix_sort(idx + off[c], tmp + off[c], curs, count[c]);
        }
        {
            int i;
            for (i = 0; i < nth; i++) { pthread_join(th[i], NULL); atomic_fetch_sub(&g_path_radix_active, 1); }
        }
    }
}

static size_t *matched_records_build_path_order(const matched_records_t *rec, ereport_run_stats_t *run_rs) {
    size_t *ord;
    size_t *tmp;
    size_t i;
    size_t n;
    int md;
    int nw;
    uint64_t t0_ns;

    if (!rec || rec->count == 0) return NULL;
    n = rec->count;
    ord = (size_t *)malloc(n * sizeof(size_t));
    if (!ord) return NULL;
    for (i = 0; i < n; i++) ord[i] = i;

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    t0_ns = (run_rs && g_ereport_verbose) ? vt_mono_ns() : 0ULL;
    if (n <= PATH_ORD_QSORT_THRESH || nw < 2) {
        g_bucket_path_sort_items = rec->items;
        qsort(ord, n, sizeof(size_t), matched_path_ord_cmp);
        g_bucket_path_sort_items = NULL;
        vt_path_sort_commit(run_rs, t0_ns);
        return ord;
    }

    tmp = (size_t *)malloc(n * sizeof(size_t));
    if (!tmp) {
        g_bucket_path_sort_items = rec->items;
        qsort(ord, n, sizeof(size_t), matched_path_ord_cmp);
        g_bucket_path_sort_items = NULL;
        vt_path_sort_commit(run_rs, t0_ns);
        return ord;
    }

    /*
     * Primary path: MSD radix (no shared-prefix re-comparison). Needs an n-entry
     * cursor array; if that allocation fails, fall back to the parallel merge sort
     * below (which only needs tmp). Both produce identical path order.
     */
    {
        path_tcur_t *curs = (path_tcur_t *)malloc(n * sizeof(*curs));
        if (curs) {
            for (i = 0; i < n; i++) path_tcur_init(&curs[i], rec->items[i].parent, rec->items[i].leaf);
            g_path_radix_max = nw;
            atomic_store(&g_path_radix_active, 1);
            path_radix_sort(ord, tmp, curs, n);
            free(g_path_radix_cls);
            g_path_radix_cls = NULL;
            g_path_radix_cls_cap = 0;
            free(curs);
            free(tmp);
            vt_path_sort_commit(run_rs, t0_ns);
            return ord;
        }
    }

    md = path_ord_merge_max_depth();
    if (md < 1) {
        free(tmp);
        g_bucket_path_sort_items = rec->items;
        qsort(ord, n, sizeof(size_t), matched_path_ord_cmp);
        g_bucket_path_sort_items = NULL;
        vt_path_sort_commit(run_rs, t0_ns);
        return ord;
    }

    matched_records_ms_parallel(rec->items, ord, tmp, 0, n, 0, md);
    free(tmp);
    vt_path_sort_commit(run_rs, t0_ns);
    return ord;
}

/*
 * Append one path component in place to a buffer that already holds `len`
 * NUL-terminated bytes, using the same separator rules the old
 * join_path_component used (empty prefix -> "comp"; root "/" -> "/comp";
 * otherwise "prefix/comp"). Produces byte-identical strings to the previous
 * join+recopy approach but only touches the new tail, turning the per-level
 * cost from O(path_len) into O(comp_len). Returns the new length, or
 * (size_t)-1 on overflow.
 */
static size_t path_append_component(char *buf, size_t bufsz, size_t len, const char *comp, size_t comp_len) {
    size_t base;
    int need_sep = 0;

    if (len == 0) base = 0;
    else if (len == 1 && buf[0] == '/') base = 1;
    else {
        base = len + 1;
        need_sep = 1;
    }
    if (base + comp_len + 1 > bufsz) return (size_t)-1;
    if (need_sep) buf[len] = '/';
    memcpy(buf + base, comp, comp_len);
    buf[base + comp_len] = '\0';
    return base + comp_len;
}

/*
 * The base prefix is fixed for a whole aggregation phase, but the per-record test used to re-derive
 * its length and re-run the "/" comparison for every path -- once per record in loops that run over
 * every detail record in the capture. Resolve it once and the per-record work is a strncmp and a
 * pointer bump.
 */
typedef struct {
    const char *s; /* NULL when the prefix matches everything */
    size_t len;
    int is_root; /* prefix is exactly "/" */
} base_prefix_t;

static void base_prefix_init(base_prefix_t *bp, const char *prefix) {
    bp->s = (prefix && prefix[0] != '\0') ? prefix : NULL;
    bp->len = bp->s ? strlen(bp->s) : 0;
    bp->is_root = bp->s && bp->len == 1U && bp->s[0] == '/';
}

/* Same result as path_after_base_prefix, with the prefix already measured. */
static inline const char *path_after_base_prefix_bp(const char *path, const base_prefix_t *bp) {
    if (!bp->s) return path;
    if (bp->is_root) return path[0] == '/' ? path + 1 : NULL;
    /* strncmp, not memcmp: path may be shorter than the prefix. */
    if (strncmp(path, bp->s, bp->len) != 0) return NULL;
    if (path[bp->len] == '\0') return path + bp->len;
    if (path[bp->len] != '/') return NULL;
    return path + bp->len + 1U;
}

typedef struct {
    path_row_map_t maps[BUCKET_DETAIL_LEVELS_MAX];
    int nlevels;
    const bucket_details_t *details;
    const char *base_prefix;
    size_t lo, hi;
    size_t seed_distinct[BUCKET_DETAIL_LEVELS_MAX]; /* what this slice's last page produced */
    size_t seed_records;
    int err;
} agg_bkt_worker_t;

/*
 * Row-map scratch owned by one bucket-page worker, reused for every page that worker emits.
 *
 * A page used to calloc (1 + aggregation workers) x levels tables, fill a fraction of them,
 * strdup a key per row, then free the lot — for each of the 36 pages. Faulting in those fresh
 * tables was 25-29% of a bucket-heavy run. Holding them here means a page starts by clearing
 * the slots the previous page used and sizing itself from what that page actually produced.
 */
typedef struct {
    path_row_map_t page_maps[BUCKET_DETAIL_LEVELS_MAX];
    size_t page_distinct[BUCKET_DETAIL_LEVELS_MAX];
    size_t page_records;
    agg_bkt_worker_t *workers;
    unsigned workers_cap;
} bucket_page_scratch_t;

static void bucket_page_scratch_free(bucket_page_scratch_t *s) {
    int d;
    unsigned i;

    if (!s) return;
    for (d = 0; d < BUCKET_DETAIL_LEVELS_MAX; d++) path_row_map_destroy(&s->page_maps[d]);
    for (i = 0; i < s->workers_cap; i++) {
        for (d = 0; d < BUCKET_DETAIL_LEVELS_MAX; d++) path_row_map_destroy(&s->workers[i].maps[d]);
    }
    free(s->workers);
    s->workers = NULL;
    s->workers_cap = 0;
}

static int aggregate_bucket_for_page_range(path_row_map_t *maps,
                                           int nlevels,
                                           const bucket_details_t *details,
                                           const char *base_prefix,
                                           size_t lo,
                                           size_t hi) {
    size_t i;
    uint64_t base_hash = base_prefix ? path_hash(base_prefix) : PATH_HASH_FNV_OFFSET;
    base_prefix_t bp;
    size_t base_len;

    base_prefix_init(&bp, base_prefix);
    base_len = base_prefix ? strlen(base_prefix) : 0;
    if (base_len >= PATH_MAX) return -1;

    for (i = lo; i < hi; i++) {
        const detail_record_t *r = &details->items[i];
        char rowpath[PATH_MAX];
        char fbuf[PATH_MAX];
        size_t rowlen = base_len;
        uint64_t h = base_hash;
        const char *p;
        int depth;

        p = path_after_base_prefix_bp(path_join_pl(r->parent, r->leaf, fbuf, sizeof(fbuf)), &bp);
        if (!p || *p == '\0') continue;

        if (base_prefix)
            memcpy(rowpath, base_prefix, base_len + 1);
        else
            rowpath[0] = '\0';

        for (depth = 0; depth < nlevels; depth++) {
            const char *start;
            size_t comp_len;
            size_t prev_len = rowlen;
            path_row_t *row;

            while (*p == '/') p++;
            if (*p == '\0') break;

            start = p;
            while (*p && *p != '/') p++;
            comp_len = (size_t)(p - start);
            if (comp_len == 0) break;

            rowlen = path_append_component(rowpath, sizeof(rowpath), rowlen, start, comp_len);
            if (rowlen == (size_t)-1) return -1;
            h = path_hash_fold(h, rowpath + prev_len, rowlen - prev_len);

            row = path_row_map_get_or_insert_h(&maps[depth], rowpath, rowlen, h);
            if (!row) return -1;
            row->bucket_files++;
            row->bucket_bytes += r->size;
            if (r->ctime_led) {
                row->bucket_ctime_led_files++;
                row->bucket_ctime_led_bytes += r->size;
            }
        }
    }

    return 0;
}

static void *agg_bkt_worker_thread(void *vp) {
    agg_bkt_worker_t *w = (agg_bkt_worker_t *)vp;
    int d;

    if (!w) return NULL;
    if (w->lo >= w->hi) return NULL;

    for (d = 0; d < w->nlevels; d++) {
        size_t chunk_records = (w->hi > w->lo) ? (w->hi - w->lo) : 0;
        size_t want = path_row_map_seed_cap(chunk_records, d, w->seed_distinct[d], w->seed_records);

        if (path_row_map_reset_or_init(&w->maps[d], want) != 0) {
            w->err = 1;
            return NULL;
        }
    }

    if (aggregate_bucket_for_page_range(w->maps, w->nlevels, w->details, w->base_prefix, w->lo, w->hi) != 0)
        w->err = 1;

    /* What this slice produced sizes the same slice on the next page. */
    w->seed_records = (w->hi > w->lo) ? (w->hi - w->lo) : 0;
    for (d = 0; d < w->nlevels; d++) w->seed_distinct[d] = w->maps[d].count;

    return NULL;
}

/*
 * Merge every parse-worker's level-`level` map into dst. Distinct levels write to
 * distinct destination maps, so one worker per level runs without locking.
 */
typedef struct {
    path_row_map_t *dst;
    agg_bkt_worker_t *workers;
    int nw;
    int level;
    int err;
} agg_bkt_merge_ctx_t;

static void *agg_bkt_merge_level_worker(void *vp) {
    agg_bkt_merge_ctx_t *c = (agg_bkt_merge_ctx_t *)vp;
    int i;

    for (i = 0; i < c->nw; i++) {
        agg_bkt_worker_t *w = &c->workers[i];

        if (w->lo >= w->hi) continue;
        if (path_row_map_merge_accumulate(c->dst, &w->maps[c->level]) != 0) c->err = 1;
    }
    return NULL;
}

static int aggregate_bucket_for_page_n(path_row_map_t *maps,
                                       int nlevels,
                                       const bucket_details_t *details,
                                       const char *base_prefix,
                                       bucket_page_scratch_t *scratch) {
    unsigned thr;
    unsigned nw;
    size_t chunk;
    size_t i;
    agg_bkt_worker_t *workers = NULL;
    pthread_t *tids = NULL;
    int any_err = 0;

    if (!maps || nlevels < 1 || !details || !base_prefix || !scratch) return -1;
    if (nlevels > BUCKET_DETAIL_LEVELS_MAX) return -1;

    thr = parse_ereport_thread_count();
    if (details->count < AGG_BKT_PAR_MIN_DETAILS || thr < 2)
        return aggregate_bucket_for_page_range(maps, nlevels, details, base_prefix, 0, details->count);

    nw = thr;
    if (nw > AGG_BKT_PAR_MAX_THREADS) nw = AGG_BKT_PAR_MAX_THREADS;
    if (nw > details->count) nw = (unsigned)details->count;
    if (nw < 2)
        return aggregate_bucket_for_page_range(maps, nlevels, details, base_prefix, 0, details->count);

    /* The worker slices — and the maps they carry from page to page — belong to this page
     * worker, so a slice keeps facing comparable work across pages. */
    if (scratch->workers_cap < nw) {
        agg_bkt_worker_t *nwk = (agg_bkt_worker_t *)realloc(scratch->workers, (size_t)nw * sizeof(*nwk));

        if (!nwk) return -1;
        memset(nwk + scratch->workers_cap, 0, ((size_t)nw - scratch->workers_cap) * sizeof(*nwk));
        scratch->workers = nwk;
        scratch->workers_cap = nw;
    }
    workers = scratch->workers;
    tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
    if (!tids) return -1;

    chunk = (details->count + (size_t)nw - 1u) / (size_t)nw;
    for (i = 0; i < (size_t)nw; i++) {
        agg_bkt_worker_t *w = &workers[i];

        w->nlevels = nlevels;
        w->details = details;
        w->base_prefix = base_prefix;
        w->lo = i * chunk;
        w->hi = w->lo + chunk;
        if (w->lo >= details->count) {
            w->lo = w->hi = details->count;
            continue;
        }
        if (w->hi > details->count) w->hi = details->count;
        w->err = 0;
        if (pthread_create(&tids[i], NULL, agg_bkt_worker_thread, w) != 0) {
            size_t j;

            for (j = 0; j < i; j++) (void)pthread_join(tids[j], NULL);
            free(tids);
            return -1;
        }
    }

    for (i = 0; i < (size_t)nw; i++) {
        (void)pthread_join(tids[i], NULL);
        if (workers[i].err) any_err = 1;
    }

    if (!any_err) {
        /* Merge per-level (each level -> its own dst map), one thread per level. */
        agg_bkt_merge_ctx_t *mctx = (agg_bkt_merge_ctx_t *)calloc((size_t)nlevels, sizeof(*mctx));
        pthread_t *mtids = (pthread_t *)calloc((size_t)nlevels, sizeof(*mtids));

        if (!mctx || !mtids) {
            int d;
            free(mctx);
            free(mtids);
            for (i = 0; i < (size_t)nw; i++) {
                agg_bkt_worker_t *w = &workers[i];

                if (w->lo >= w->hi) continue;
                for (d = 0; d < nlevels; d++) {
                    if (path_row_map_merge_accumulate(&maps[d], &w->maps[d]) != 0) any_err = 1;
                }
            }
        } else {
            int d;
            int mspawn = 0;

            for (d = 0; d < nlevels; d++) {
                mctx[d].dst = &maps[d];
                mctx[d].workers = workers;
                mctx[d].nw = (int)nw;
                mctx[d].level = d;
                mctx[d].err = 0;
                if (pthread_create(&mtids[mspawn], NULL, agg_bkt_merge_level_worker, &mctx[d]) != 0)
                    agg_bkt_merge_level_worker(&mctx[d]); /* inline this level */
                else
                    mspawn++;
            }
            for (d = 0; d < mspawn; d++) pthread_join(mtids[d], NULL);
            for (d = 0; d < nlevels; d++) {
                if (mctx[d].err) any_err = 1;
            }
            free(mctx);
            free(mtids);
        }
    }

    /* The worker maps stay allocated for the next page; see bucket_page_scratch_t. */
    free(tids);

    return any_err ? -1 : 0;
}

static size_t path_row_maps_total_used_rows(const path_row_map_t *maps, int nlevels) {
    size_t d;
    size_t t = 0;

    if (!maps || nlevels < 1) return 0;
    for (d = 0; d < (size_t)nlevels; d++) t += maps[d].count;
    return t;
}

static void zero_map_row_corpus_totals(path_row_map_t *maps, int nlevels) {
    size_t d, i;

    if (!maps || nlevels < 1) return;
    for (d = 0; d < (size_t)nlevels; d++) {
        for (i = 0; i < maps[d].cap; i++) {
            if (!maps[d].used[i]) continue;
            maps[d].rows[i].total_files = 0;
            maps[d].rows[i].total_dirs = 0;
            maps[d].rows[i].total_bytes = 0;
        }
    }
}

/*
 * One accumulation slot in a worker's private table. Keyed by row pointer: rows never move once their
 * map stops rehashing, and the aggregation pass only reads the maps.
 */
typedef struct {
    path_row_t *key;
    uint64_t files;
    uint64_t dirs;
    uint64_t bytes;
} agg_tot_slot_t;

typedef struct {
    path_row_map_t *maps;
    int nlevels;
    const matched_records_t *records;
    const char *base_prefix;
    const size_t *path_ord;
    int use_ord_slice;
    size_t c_lo;
    size_t c_hi;
    agg_tot_slot_t *tbl; /* NULL in atomic_mode */
    size_t tbl_mask;     /* slot count - 1, slot count a power of two */
    size_t tbl_used;
    size_t tbl_limit; /* stop inserting past this load; further rows go straight to the shared totals */
    int atomic_mode;  /* 1: accumulate directly into shared row totals via relaxed atomics (no table) */
    atomic_int *fatal_atom;
} agg_tot_par_wctx_t;

/*
 * Returns this row's slot, or NULL when the table is full and the caller must fall back to atomics.
 * Liveness is a nonzero key: the worker memsets its table before each cell, so a NULL key is empty.
 */
static inline agg_tot_slot_t *agg_tot_slot_for(agg_tot_par_wctx_t *w, path_row_t *row) {
    uint64_t h = ((uint64_t)(uintptr_t)row >> 3) * 0x9E3779B97F4A7C15ULL;
    size_t i = (size_t)(h >> 32) & w->tbl_mask;

    for (;;) {
        agg_tot_slot_t *s = &w->tbl[i];

        if (s->key == row) return s;
        if (!s->key) {
            if (w->tbl_used >= w->tbl_limit) return NULL;
            s->key = row;
            w->tbl_used++;
            return s;
        }
        i = (i + 1) & w->tbl_mask;
    }
}

/* Add a worker's private counts into the shared rows. Callers zero the rows before any worker starts. */
static void agg_tot_flush_table(agg_tot_par_wctx_t *w) {
    size_t i;

    if (!w->tbl) return;
    for (i = 0; i <= w->tbl_mask; i++) {
        agg_tot_slot_t *s = &w->tbl[i];

        if (!s->key) continue;
        if (s->files) __atomic_fetch_add(&s->key->total_files, s->files, __ATOMIC_RELAXED);
        if (s->dirs) __atomic_fetch_add(&s->key->total_dirs, s->dirs, __ATOMIC_RELAXED);
        if (s->bytes) __atomic_fetch_add(&s->key->total_bytes, s->bytes, __ATOMIC_RELAXED);
    }
}

static void *agg_totals_par_worker(void *vp) {
    agg_tot_par_wctx_t *w = (agg_tot_par_wctx_t *)vp;
    size_t ii;
    uint64_t base_hash = w->base_prefix ? path_hash(w->base_prefix) : PATH_HASH_FNV_OFFSET;
    base_prefix_t bp;
    size_t base_len = w->base_prefix ? strlen(w->base_prefix) : 0;

    base_prefix_init(&bp, w->base_prefix);
    if (base_len >= PATH_MAX) {
        atomic_store_explicit(w->fatal_atom, 1, memory_order_relaxed);
        return NULL;
    }

    /* Zero the worker's own table for this cell. The scratch pages stay resident across cells
     * (one reused buffer per emit thread), so this is a plain rewrite, not a fault storm. */
    if (!w->atomic_mode) memset(w->tbl, 0, (w->tbl_mask + 1) * sizeof(*w->tbl));

    for (ii = w->c_lo; ii < w->c_hi; ii++) {
        size_t i = w->use_ord_slice ? w->path_ord[ii] : ii;
        const matched_record_t *r = &w->records->items[i];
        char rowpath[PATH_MAX];
        char fbuf[PATH_MAX];
        size_t rowlen = base_len;
        uint64_t h = base_hash;
        const char *p;
        int depth;

        if (atomic_load_explicit(w->fatal_atom, memory_order_relaxed)) break;

        p = path_after_base_prefix_bp(path_join_pl(r->parent, r->leaf, fbuf, sizeof(fbuf)), &bp);
        if (!p || *p == '\0') continue;

        if (w->base_prefix)
            memcpy(rowpath, w->base_prefix, base_len + 1);
        else
            rowpath[0] = '\0';

        for (depth = 0; depth < w->nlevels; depth++) {
            const char *start;
            size_t comp_len;
            size_t prev_len = rowlen;
            path_row_t *row;

            while (*p == '/') p++;
            if (*p == '\0') break;

            start = p;
            while (*p && *p != '/') p++;
            comp_len = (size_t)(p - start);
            if (comp_len == 0) break;

            rowlen = path_append_component(rowpath, sizeof(rowpath), rowlen, start, comp_len);
            if (rowlen == (size_t)-1) {
                atomic_store_explicit(w->fatal_atom, 1, memory_order_relaxed);
                goto done;
            }
            h = path_hash_fold(h, rowpath + prev_len, rowlen - prev_len);

            row = path_row_map_find_h(&w->maps[depth], rowpath, rowlen, h);
            if (row) {
                agg_tot_slot_t *s = w->atomic_mode ? NULL : agg_tot_slot_for(w, row);

                if (s) {
                    if (r->type == 'f')
                        s->files++;
                    else if (r->type == 'd')
                        s->dirs++;
                    s->bytes += r->size;
                } else {
                    if (r->type == 'f')
                        __atomic_fetch_add(&row->total_files, 1ULL, __ATOMIC_RELAXED);
                    else if (r->type == 'd')
                        __atomic_fetch_add(&row->total_dirs, 1ULL, __ATOMIC_RELAXED);
                    __atomic_fetch_add(&row->total_bytes, r->size, __ATOMIC_RELAXED);
                }
            }
        }
    }

done:
    agg_tot_flush_table(w);
    return NULL;
}

/*
 * Atomic-accumulation variant: shard records across all nw threads and add straight into the shared row
 * totals (relaxed atomics). Used when the per-thread partial matrix (O(nw·R)) cannot afford enough threads
 * within the memory budget for a very large R. Memory is O(R); the only contention is on the hottest rows.
 * On any worker fatal we re-zero the row totals so the caller's serial fallback recomputes from a clean state.
 */
static int aggregate_totals_for_page_n_atomic(path_row_map_t *maps,
                                               int nlevels,
                                               const matched_records_t *records,
                                               const char *base_prefix,
                                               const size_t *path_ord,
                                               int use_ord_slice,
                                               size_t lo,
                                               size_t hi,
                                               int nw) {
    size_t nscan = hi - lo;
    pthread_t *tp = NULL;
    agg_tot_par_wctx_t *ctxs = NULL;
    int j;
    int started;
    size_t per;
    size_t t;
    atomic_int shared_fatal;

    if (nw < 1) return -1;
    atomic_init(&shared_fatal, 0);

    tp = (pthread_t *)malloc((size_t)nw * sizeof(pthread_t));
    ctxs = (agg_tot_par_wctx_t *)calloc((size_t)nw, sizeof(*ctxs));
    if (!tp || !ctxs) {
        free(tp);
        free(ctxs);
        return -1;
    }

    zero_map_row_corpus_totals(maps, nlevels);

    started = 0;
    per = (nscan + (size_t)nw - 1) / (size_t)nw;
    for (t = 0; t < (size_t)nw; t++) {
        size_t clo = lo + t * per;
        size_t chi = clo + per;

        if (clo >= hi) break;
        if (chi > hi) chi = hi;

        ctxs[started].maps = maps;
        ctxs[started].nlevels = nlevels;
        ctxs[started].records = records;
        ctxs[started].base_prefix = base_prefix;
        ctxs[started].path_ord = path_ord;
        ctxs[started].use_ord_slice = use_ord_slice;
        ctxs[started].c_lo = clo;
        ctxs[started].c_hi = chi;
        ctxs[started].tbl = NULL;
        ctxs[started].atomic_mode = 1;
        ctxs[started].fatal_atom = &shared_fatal;

        if (pthread_create(&tp[started], NULL, agg_totals_par_worker, &ctxs[started]) != 0) break;
        started++;
    }

    if (started < 1) {
        free(tp);
        free(ctxs);
        return -1;
    }

    for (j = 0; j < started; j++) pthread_join(tp[j], NULL);

    free(tp);
    free(ctxs);

    if (atomic_load_explicit(&shared_fatal, memory_order_relaxed)) {
        zero_map_row_corpus_totals(maps, nlevels);
        return -1;
    }
    return 0;
}

/*
 * Accumulation-table scratch, owned by the thread that emits the bucket cell. One page emits dozens of
 * cells, so a fresh allocation per cell maps and unmaps the tables every time and faults them back in
 * from scratch. One buffer per emit thread, only ever growing, keeps those pages resident across cells.
 * It is malloc and not calloc because the workers zero their own tables in parallel.
 */
typedef struct {
    void *buf;
    size_t cap; /* bytes */
} agg_parts_scratch_t;

static pthread_key_t g_agg_parts_key;
static pthread_once_t g_agg_parts_once = PTHREAD_ONCE_INIT;
static int g_agg_parts_key_ok;

static void agg_parts_scratch_destroy(void *p) {
    agg_parts_scratch_t *s = (agg_parts_scratch_t *)p;

    if (!s) return;
    free(s->buf);
    free(s);
}

static void agg_parts_key_init(void) {
    g_agg_parts_key_ok = (pthread_key_create(&g_agg_parts_key, agg_parts_scratch_destroy) == 0);
}

/* At least need bytes with undefined contents, or NULL, which asks the caller to allocate its own. */
static void *agg_parts_scratch_get(size_t need) {
    agg_parts_scratch_t *s;

    pthread_once(&g_agg_parts_once, agg_parts_key_init);
    if (!g_agg_parts_key_ok) return NULL;

    s = (agg_parts_scratch_t *)pthread_getspecific(g_agg_parts_key);
    if (!s) {
        s = (agg_parts_scratch_t *)calloc(1, sizeof(*s));
        if (!s) return NULL;
        if (pthread_setspecific(g_agg_parts_key, s) != 0) {
            free(s);
            return NULL;
        }
    }
    if (s->cap < need) {
        /* Not realloc: every byte is rewritten, so copying the old contents would be wasted work. */
        void *nb = malloc(need);

        if (!nb) return NULL;
        free(s->buf);
        s->buf = nb;
        s->cap = need;
    }
    return s->buf;
}

static int aggregate_totals_for_page_n_parallel(path_row_map_t *maps,
                                              int nlevels,
                                              const matched_records_t *records,
                                              const char *base_prefix,
                                              const size_t *path_ord,
                                              int use_ord_slice,
                                              size_t lo,
                                              size_t hi) {
    size_t nscan = hi - lo;
    size_t R;
    agg_tot_slot_t *tables = NULL;
    pthread_t *tp = NULL;
    agg_tot_par_wctx_t *ctxs = NULL;
    int nw;
    int j;
    int started;
    int tables_owned; /* 1: tables is a private allocation, not the emit thread's reused scratch */
    size_t slots;
    size_t want;
    size_t per;
    size_t t;
    atomic_int shared_fatal;

    if (!maps || nlevels < 1 || !records || lo >= hi) return -1;
    if (nscan < AGG_TOTALS_PAR_MIN_RECORDS) return -1;

    R = path_row_maps_total_used_rows(maps, nlevels);
    if (R == 0) return -1;

    nw = parse_ereport_thread_count();
    if (nw < 2) return -1;

    /*
     * A worker walks a contiguous slice of the path-ordered records, and rows are ancestors of the records
     * that produce them, so it only ever touches about R/nw distinct rows — scattered across the map tables,
     * but few. Give it a table sized for twice that and let anything past the load limit fall through to the
     * shared totals: the whole matrix is O(R) instead of the O(nw*R) a dense per-worker row vector needs, and
     * a worker's working set is its own table rather than all R rows.
     */
    want = R / (size_t)nw * 2 + 128;
    if (want > R + 128) want = R + 128;
    slots = 256;
    while (slots < want && slots < (SIZE_MAX >> 1)) slots <<= 1;
    while (slots > 256 && (size_t)nw * slots * sizeof(agg_tot_slot_t) > AGG_TOTALS_PAR_MATRIX_BUDGET_BYTES)
        slots >>= 1;

    atomic_init(&shared_fatal, 0);

    tables_owned = 0;
    tables = (agg_tot_slot_t *)agg_parts_scratch_get((size_t)nw * slots * sizeof(*tables));
    if (!tables) {
        tables = (agg_tot_slot_t *)calloc((size_t)nw * slots, sizeof(*tables));
        tables_owned = 1;
    }
    tp = (pthread_t *)malloc((size_t)nw * sizeof(pthread_t));
    ctxs = (agg_tot_par_wctx_t *)calloc((size_t)nw, sizeof(*ctxs));
    if (!tables || !tp || !ctxs) {
        if (tables_owned) free(tables);
        free(tp);
        free(ctxs);
        /* No memory for the tables; the atomic variant needs none beyond the rows themselves. */
        return aggregate_totals_for_page_n_atomic(maps, nlevels, records, base_prefix, path_ord, use_ord_slice, lo,
                                                  hi, nw);
    }

    /* Workers add into the rows, so they have to start from zero rather than be assigned at the end. */
    zero_map_row_corpus_totals(maps, nlevels);

    started = 0;
    per = (nscan + (size_t)nw - 1) / (size_t)nw;
    for (t = 0; t < (size_t)nw; t++) {
        size_t clo = lo + t * per;
        size_t chi = clo + per;

        if (clo >= hi) break;
        if (chi > hi) chi = hi;

        ctxs[started].maps = maps;
        ctxs[started].nlevels = nlevels;
        ctxs[started].records = records;
        ctxs[started].base_prefix = base_prefix;
        ctxs[started].path_ord = path_ord;
        ctxs[started].use_ord_slice = use_ord_slice;
        ctxs[started].c_lo = clo;
        ctxs[started].c_hi = chi;
        ctxs[started].tbl = tables + (size_t)started * slots;
        ctxs[started].tbl_mask = slots - 1;
        ctxs[started].tbl_used = 0;
        ctxs[started].tbl_limit = slots - slots / 4;
        ctxs[started].atomic_mode = 0;
        ctxs[started].fatal_atom = &shared_fatal;

        if (pthread_create(&tp[started], NULL, agg_totals_par_worker, &ctxs[started]) != 0) break;
        started++;
    }

    if (started < 1) {
        if (tables_owned) free(tables);
        free(tp);
        free(ctxs);
        return -1;
    }

    for (j = 0; j < started; j++) pthread_join(tp[j], NULL);

    if (tables_owned) free(tables);
    free(tp);
    free(ctxs);

    if (atomic_load_explicit(&shared_fatal, memory_order_relaxed)) {
        zero_map_row_corpus_totals(maps, nlevels);
        return -1;
    }
    return 0;
}

static int aggregate_totals_for_page_n(path_row_map_t *maps,
                                       int nlevels,
                                       const matched_records_t *records,
                                       const char *base_prefix,
                                       const size_t *path_ord,
                                       size_t pre_lo,
                                       size_t pre_hi,
                                       int pre_slice_valid) {
    size_t n = records ? records->count : 0;
    size_t lo = 0;
    size_t hi = n;
    size_t ii;
    int use_ord_slice = (base_prefix && base_prefix[0] != '\0' && path_ord);

    if (use_ord_slice) {
        if (pre_slice_valid) {
            lo = pre_lo;
            hi = pre_hi;
        } else {
            lo = matched_ord_lower_bound_seg(records, path_ord, n, base_prefix);
            hi = matched_ord_first_not_under_prefix(records, path_ord, n, lo, base_prefix);
        }
    }

    if (aggregate_totals_for_page_n_parallel(maps, nlevels, records, base_prefix, path_ord, use_ord_slice, lo, hi) == 0)
        return 0;

    {
    uint64_t base_hash = base_prefix ? path_hash(base_prefix) : PATH_HASH_FNV_OFFSET;
    base_prefix_t bp;
    size_t base_len = base_prefix ? strlen(base_prefix) : 0;

    base_prefix_init(&bp, base_prefix);
    if (base_len >= PATH_MAX) return -1;
    for (ii = lo; ii < hi; ii++) {
        size_t i = use_ord_slice ? path_ord[ii] : ii;
        const matched_record_t *r = &records->items[i];
        char rowpath[PATH_MAX];
        char fbuf[PATH_MAX];
        size_t rowlen = base_len;
        uint64_t h = base_hash;
        const char *p;
        int depth;

        p = path_after_base_prefix_bp(path_join_pl(r->parent, r->leaf, fbuf, sizeof(fbuf)), &bp);
        if (!p || *p == '\0') continue;

        if (base_prefix)
            memcpy(rowpath, base_prefix, base_len + 1);
        else
            rowpath[0] = '\0';

        for (depth = 0; depth < nlevels; depth++) {
            const char *start;
            size_t comp_len;
            size_t prev_len = rowlen;
            path_row_t *row;

            while (*p == '/') p++;
            if (*p == '\0') break;

            start = p;
            while (*p && *p != '/') p++;
            comp_len = (size_t)(p - start);
            if (comp_len == 0) break;

            rowlen = path_append_component(rowpath, sizeof(rowpath), rowlen, start, comp_len);
            if (rowlen == (size_t)-1) return -1;
            h = path_hash_fold(h, rowpath + prev_len, rowlen - prev_len);

            row = path_row_map_find_h(&maps[depth], rowpath, rowlen, h);
            if (row) {
                if (r->type == 'f') row->total_files++;
                else if (r->type == 'd') row->total_dirs++;
                row->total_bytes += r->size;
            }
        }
    }
    }

    return 0;
}

/*
 * Map corpus_pct (share of corpus, 0..100) to color intensity 0..100.
 * ref_max_pct is full saturation: inner bucket cells use max share among age×size buckets;
 * row totals, column totals, and the corner use 100 (whole corpus).
 */
static double heatmap_norm_pct(double corpus_pct, double ref_max_pct) {
    double x;

    if (ref_max_pct <= 0.0) return 0.0;
    x = 100.0 * corpus_pct / ref_max_pct;
    if (x > 100.0) x = 100.0;
    return x;
}

/* Label for “ctime-led” share of bytes within a bucket (heat map cell or path row). */
static void format_ctime_led_share_label(uint64_t ctime_led_bytes, uint64_t bucket_bytes, char *buf, size_t sz) {
    double p;

    if (bucket_bytes == 0 || ctime_led_bytes == 0) {
        snprintf(buf, sz, "0%%");
        return;
    }
    p = 100.0 * (double)ctime_led_bytes / (double)bucket_bytes;
    if (p < 1.0)
        snprintf(buf, sz, "<1%%");
    else if (p < 10.0)
        snprintf(buf, sz, "%.1f%%", p);
    else
        snprintf(buf, sz, "%.0f%%", p);
}

static void contribution_cell_color(double pct, char *buf, size_t sz) {
    const int low_r = 248, low_g = 244, low_b = 238;
    const int high_r = 245, high_g = 214, high_b = 214;
    double t = pct / 100.0;
    int r, g, b;

    if (t < 0.0) t = 0.0;
    if (t > 1.0) t = 1.0;

    r = (int)(low_r + (high_r - low_r) * t + 0.5);
    g = (int)(low_g + (high_g - low_g) * t + 0.5);
    b = (int)(low_b + (high_b - low_b) * t + 0.5);
    snprintf(buf, sz, "rgb(%d,%d,%d)", r, g, b);
}

/* Heat-map diagonal (bytes triangle): light blue → deeper blue by share of total bytes. */
static void bytes_share_cell_color(double pct, char *buf, size_t sz) {
    const int low_r = 244, low_g = 249, low_b = 252;
    const int high_r = 165, high_g = 198, high_b = 242;
    double t = pct / 100.0;
    int r, g, b;

    if (t < 0.0) t = 0.0;
    if (t > 1.0) t = 1.0;

    r = (int)(low_r + (high_r - low_r) * t + 0.5);
    g = (int)(low_g + (high_g - low_g) * t + 0.5);
    b = (int)(low_b + (high_b - low_b) * t + 0.5);
    snprintf(buf, sz, "rgb(%d,%d,%d)", r, g, b);
}

/*
 * Heat-map file triangle: comma main line + parenthetical "(2M, 12%)" or "(12%)" on a separate badge line.
 */
static void format_file_count_main_and_paren(uint64_t f,
                                           double pct_files,
                                           char *main_out,
                                           size_t main_sz,
                                           char *paren_out,
                                           size_t paren_sz) {
    int v;

    format_uint_commas(f, main_out, main_sz);
    if (f >= 1000000ULL) {
        if (f >= 1000000000000ULL) {
            v = (int)((f + 500000000000ULL) / 1000000000000ULL);
            snprintf(paren_out, paren_sz, "(%dT, %.0f%%)", v, pct_files);
        } else if (f >= 1000000000ULL) {
            v = (int)((f + 500000000ULL) / 1000000000ULL);
            snprintf(paren_out, paren_sz, "(%dB, %.0f%%)", v, pct_files);
        } else {
            v = (int)((f + 500000ULL) / 1000000ULL);
            snprintf(paren_out, paren_sz, "(%dM, %.0f%%)", v, pct_files);
        }
    } else {
        snprintf(paren_out, paren_sz, "(%.0f%%)", pct_files);
    }
}

static const char *path_tail_component(const char *path) {
    const char *slash = strrchr(path, '/');
    if (slash && slash[1] != '\0') return slash + 1;
    return path;
}

/*
 * Directory prefix for display: path through the '/' before the last path component.
 * Level 1 (level_idx == 0): no anchor — show the full parent prefix, or generic ".../tail" if long.
 * Level 2+ (level_idx >= 1): path is under shared base `anchor`; show the part below the base.
 *   The crawl base is omitted from the muted line (shown as "...") so rows are not dominated by
 *   repeating it; long middle segments still collapse with ".../tail" within that remainder.
 */
static void compact_path_prefix(const char *path, char *buf, size_t sz, const char *anchor, int level_idx) {
    const char *slash = strrchr(path, '/');
    size_t prefix_len;
    const size_t keep = 28;

    if (!slash) {
        buf[0] = '\0';
        return;
    }

    prefix_len = (size_t)(slash - path + 1);

    if (level_idx >= 1 && anchor && anchor[0] != '\0') {
        size_t alen = strlen(anchor);
        if (prefix_len >= alen && strncmp(path, anchor, alen) == 0 &&
            (path[alen] == '/' || alen == prefix_len)) {
            size_t ext_len = prefix_len > alen ? prefix_len - alen : 0;
            const char *ext = path + alen;

            if (ext_len == 0 || (ext_len == 1U && ext[0] == '/')) {
                int n = snprintf(buf, sz, ".../");
                if (n < 0 || (size_t)n >= sz) buf[0] = '\0';
                return;
            }

            if (ext_len <= keep) {
                int n = snprintf(buf, sz, "...%.*s", (int)ext_len, ext);
                if (n < 0 || (size_t)n >= sz) buf[0] = '\0';
                return;
            }

            {
                const char *start = ext + (ext_len - keep);
                while (start > ext && *(start - 1) != '/') start--;
                int n = snprintf(buf, sz, ".../%s", start);
                if (n < 0 || (size_t)n >= sz) buf[0] = '\0';
                return;
            }
        }
    }

    if (prefix_len < sz && prefix_len <= keep) {
        memcpy(buf, path, prefix_len);
        buf[prefix_len] = '\0';
        return;
    }

    if (prefix_len <= 1) {
        snprintf(buf, sz, "/");
        return;
    }

    if (prefix_len > keep) {
        const char *start = path + (prefix_len - keep);
        while (start > path && *(start - 1) != '/') start--;
        snprintf(buf, sz, ".../%s", start);
        return;
    }

    snprintf(buf, sz, "%.*s", (int)prefix_len, path);
}

static void emit_compact_path_cell(FILE *out, const char *path, const char *base_prefix, int level_idx) {
    char prefix[96];
    const char *tail = path_tail_component(path);

    compact_path_prefix(path, prefix, sizeof(prefix), base_prefix, level_idx);

    fprintf(out, "<td class=\"path-cell\" data-sort-s=\"");
    html_escape(out, path);
    fprintf(out, "\" title=\"");
    html_escape(out, path);
    fprintf(out, "\">");
    fprintf(out, "<div class=\"path-line\">");
    fprintf(out, "<button type=\"button\" class=\"path-toggle\" aria-expanded=\"false\" title=\"");
    html_escape(out, path);
    fprintf(out, "\">");
    if (prefix[0] != '\0') {
        fprintf(out, "<span class=\"path-prefix\">");
        html_escape(out, prefix);
        fprintf(out, "</span>");
    }
    fprintf(out, "<span class=\"path-tail\">");
    html_escape(out, tail);
    fprintf(out, "</span></button>");
    fprintf(out, "<button type=\"button\" class=\"copy-path\" data-copy=\"");
    html_escape(out, path);
    fprintf(out, "\" title=\"Copy full path\">Copy</button>");
    fprintf(out, "</div><div class=\"path-full\" hidden>");
    html_escape(out, path);
    fprintf(out, "</div></td>");
}

static void emit_path_summary_table(FILE *out,
                                    const char *title,
                                    path_row_map_t *map,
                                    uint64_t total_bucket_files,
                                    uint64_t total_bucket_bytes,
                                    uint64_t total_user_files,
                                    uint64_t total_user_bytes,
                                    const char *base_prefix,
                                    int level_idx,
                                    ereport_run_stats_t *run_rs) {
    path_row_t **rows = NULL;
    size_t count = 0;
    size_t i;

    fprintf(out, "<h2>");
    html_escape(out, title);
    fprintf(out, "</h2>\n");

    if (path_row_map_collect(map, &rows, &count) != 0) {
        fprintf(out, "<p>Allocation failed while building this view.</p>\n");
        return;
    }

    if (count == 0) {
        fprintf(out, "<p>No directories at this depth contain files from this bucket.</p>\n");
        free(rows);
        return;
    }

    {
        size_t full_count = count;

        if (full_count > (size_t)BUCKET_PATH_TABLE_MAX_ROWS) {
            path_row_t **h =
                (path_row_t **)malloc((size_t)BUCKET_PATH_TABLE_MAX_ROWS * sizeof(path_row_t *));
            size_t hsz = 0;
            size_t j;

            if (!h) {
                fprintf(out, "<p>Allocation failed while building this view.</p>\n");
                free(rows);
                return;
            }
            for (j = 0; j < full_count; j++) path_row_topk_heap_push(h, &hsz, (size_t)BUCKET_PATH_TABLE_MAX_ROWS, rows[j]);
            free(rows);
            rows = h;
            count = hsz;
        }

        VT_QSORT_BUCKET(run_rs, rows, count, sizeof(*rows), cmp_row_bucket_bytes_desc);

        {
            size_t shown = full_count;
            if (shown > (size_t)BUCKET_PATH_TABLE_MAX_ROWS) shown = (size_t)BUCKET_PATH_TABLE_MAX_ROWS;

            fprintf(out,
                    "<!-- bucket path table: %zu director%s at this level; %zu rows in HTML (cap %d; default sort bucket "
                    "bytes desc; headers clickable) -->\n",
                    full_count,
                    full_count == 1 ? "y" : "ies",
                    shown,
                    BUCKET_PATH_TABLE_MAX_ROWS);

            if (full_count > (size_t)BUCKET_PATH_TABLE_MAX_ROWS) {
                fprintf(out,
                        "<p class=\"table-trunc-note\">Showing the <strong>top %d</strong> of <strong>%zu</strong> "
                        "directories at this depth (default sort: bucket bytes, largest first). Omitted rows are lower "
                        "in that ranking; heat-map and bucket summary totals above still include the full bucket. "
                        "Use column headers to sort the visible rows.</p>\n",
                        BUCKET_PATH_TABLE_MAX_ROWS,
                        full_count);
            }
        }
    }

    fprintf(out,
            "<div class=\"bucket-table-wrap\"><table>\n<thead><tr>"
            "<th class=\"path-cell sort-h\" data-i=\"0\" data-t=\"s\"><span class=\"th-text\">Path</span></th>"
            "<th class=\"r num sort-h\" data-i=\"1\" data-t=\"n\"><span class=\"th-text\">Bucket Files</span></th>"
            "<th class=\"r num sort-h\" data-i=\"2\" data-t=\"n\"><span class=\"th-text\">Share of Bucket Files</span></th>"
            "<th class=\"r num sort-h sort-desc\" data-i=\"3\" data-t=\"n\"><span class=\"th-text\">Bucket Bytes</span></th>"
            "<th class=\"r num sort-h\" data-i=\"4\" data-t=\"n\"><span class=\"th-text\">Share of Bucket Bytes</span></th>"
            "<th class=\"r num sort-h\" data-i=\"5\" data-t=\"n\" title=\"Percent of bucket bytes whose ctime is substantially "
            "newer than both atime and mtime (>=180 days). Purple C-led pill only when this percent is at least 30. High "
            "values: metadata-led recency; chmod, chown, ACL, rsync attrs, migration.\"><span class=\"th-text\">C-led "
            "bytes</span></th>"
            "<th class=\"r num sort-h\" data-i=\"6\" data-t=\"n\"><span class=\"th-text\">Total Files</span></th>"
            "<th class=\"r num sort-h\" data-i=\"7\" data-t=\"n\"><span class=\"th-text\">Total Dirs</span></th>"
            "<th class=\"r num sort-h\" data-i=\"8\" data-t=\"n\"><span class=\"th-text\">Total Bytes</span></th>"
            "<th class=\"r num sort-h\" data-i=\"9\" data-t=\"n\"><span class=\"th-text\">Share of User Bytes</span></th>"
            "<th class=\"r num sort-h\" data-i=\"10\" data-t=\"n\"><span class=\"th-text\">Share of User Files</span></th>"
            "</tr></thead>\n<tbody>\n");

    {
        size_t shown_n = count > (size_t)BUCKET_PATH_TABLE_MAX_ROWS ? (size_t)BUCKET_PATH_TABLE_MAX_ROWS : count;
        uint64_t max_total_files = 0;
        uint64_t max_total_dirs = 0;
        uint64_t max_total_bytes = 0;
        double max_ctime_led_pct = 0.0;
        for (i = 0; i < shown_n; i++) {
            double row_cl_pct = rows[i]->bucket_bytes
                                  ? (100.0 * (double)rows[i]->bucket_ctime_led_bytes / (double)rows[i]->bucket_bytes)
                                  : 0.0;
            if (rows[i]->total_files > max_total_files) max_total_files = rows[i]->total_files;
            if (rows[i]->total_dirs > max_total_dirs) max_total_dirs = rows[i]->total_dirs;
            if (rows[i]->total_bytes > max_total_bytes) max_total_bytes = rows[i]->total_bytes;
            if (row_cl_pct > max_ctime_led_pct) max_ctime_led_pct = row_cl_pct;
        }

        for (i = 0; i < count && i < (size_t)BUCKET_PATH_TABLE_MAX_ROWS; i++) {
            char bb[32];
            char tb[32];
            char cl_label[24];
            char file_bg[32];
            char byte_bg[32];
            char cled_bg[32];
            char total_files_bg[32];
            char total_dirs_bg[32];
            char total_bytes_bg[32];
            char user_byte_bg[32];
            char user_file_bg[32];
            double share_bytes =
                total_bucket_bytes ? (100.0 * (double)rows[i]->bucket_bytes / (double)total_bucket_bytes) : 0.0;
            double share_files =
                total_bucket_files ? (100.0 * (double)rows[i]->bucket_files / (double)total_bucket_files) : 0.0;
            double user_bytes_pct =
                total_user_bytes ? (100.0 * (double)rows[i]->bucket_bytes / (double)total_user_bytes) : 0.0;
            double user_files_pct =
                total_user_files ? (100.0 * (double)rows[i]->bucket_files / (double)total_user_files) : 0.0;
            double total_files_heat =
                max_total_files ? (100.0 * (double)rows[i]->total_files / (double)max_total_files) : 0.0;
            double total_dirs_heat =
                max_total_dirs ? (100.0 * (double)rows[i]->total_dirs / (double)max_total_dirs) : 0.0;
            double total_bytes_heat =
                max_total_bytes ? (100.0 * (double)rows[i]->total_bytes / (double)max_total_bytes) : 0.0;
            double ctime_led_pct = rows[i]->bucket_bytes
                                       ? (100.0 * (double)rows[i]->bucket_ctime_led_bytes / (double)rows[i]->bucket_bytes)
                                       : 0.0;
            double ctime_led_heat =
                max_ctime_led_pct > 0.0 ? (100.0 * ctime_led_pct / max_ctime_led_pct) : 0.0;

            char bpf[96];
            char tpf[96];

            human_bytes(rows[i]->bucket_bytes, bb, sizeof(bb));
            human_bytes(rows[i]->total_bytes, tb, sizeof(tb));
            format_ctime_led_share_label(rows[i]->bucket_ctime_led_bytes, rows[i]->bucket_bytes, cl_label,
                                         sizeof(cl_label));
            format_count_pretty_inline(rows[i]->bucket_files, bpf, sizeof(bpf));
            format_count_pretty_inline(rows[i]->total_files, tpf, sizeof(tpf));
            contribution_cell_color(share_files, file_bg, sizeof(file_bg));
            bytes_share_cell_color(share_bytes, byte_bg, sizeof(byte_bg));
            contribution_cell_color(ctime_led_heat, cled_bg, sizeof(cled_bg));
            contribution_cell_color(total_files_heat, total_files_bg, sizeof(total_files_bg));
            contribution_cell_color(total_dirs_heat, total_dirs_bg, sizeof(total_dirs_bg));
            bytes_share_cell_color(total_bytes_heat, total_bytes_bg, sizeof(total_bytes_bg));
            bytes_share_cell_color(user_bytes_pct, user_byte_bg, sizeof(user_byte_bg));
            contribution_cell_color(user_files_pct, user_file_bg, sizeof(user_file_bg));

            fprintf(out, "<tr>");
            emit_compact_path_cell(out, rows[i]->path, base_prefix, level_idx);
            {
                char clin_title[384];
                snprintf(clin_title,
                         sizeof(clin_title),
                         "This directory within the open bucket: C-led %% = fraction of bytes where ctime is at least "
                         "180 days newer than both atime and mtime (Linux inode/metadata vs reads or content edits). "
                         "Purple pill when >= %.0f%% (same as heat map; EREPORT_HEAT_CTIME_LED_MIN_SHARE).",
                         g_ctime_led_badge_min_share_frac * 100.0);
                fprintf(out,
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%" PRIu64 "\">%s</td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%.17g\">%.1f</td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%" PRIu64 "\">%s</td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%.17g\">%.1f</td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%.17g\">"
                        "<span class=\"heat-badge-tip\" tabindex=\"0\" data-tip=\"",
                        file_bg,
                        rows[i]->bucket_files,
                        bpf,
                        file_bg,
                        share_files,
                        share_files,
                        byte_bg,
                        rows[i]->bucket_bytes,
                        bb,
                        byte_bg,
                        share_bytes,
                        share_bytes,
                        cled_bg,
                        ctime_led_pct);
                html_escape(out, clin_title);
                fputs("\">", out);
                if (ctime_led_badge_visible(rows[i]->bucket_ctime_led_bytes, rows[i]->bucket_bytes))
                    fputs("<span class=\"path-ctime-led-badge\">C-led</span> ", out);
                fprintf(out, "<span class=\"path-ctime-led-pct\">");
                html_escape(out, cl_label);
                fprintf(out,
                        "</span></span></td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%" PRIu64 "\">%s</td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%" PRIu64 "\">%" PRIu64 "</td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%" PRIu64 "\">%s</td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%.17g\">%.1f</td>"
                        "<td class=\"r num\" style=\"background:%s\" data-sort-n=\"%.17g\">%.1f</td></tr>\n",
                        total_files_bg,
                        rows[i]->total_files,
                        tpf,
                        total_dirs_bg,
                        rows[i]->total_dirs,
                        rows[i]->total_dirs,
                        total_bytes_bg,
                        rows[i]->total_bytes,
                        tb,
                        user_byte_bg,
                        user_bytes_pct,
                        user_bytes_pct,
                        user_file_bg,
                        user_files_pct,
                        user_files_pct);
            }
        }
    }
    fprintf(out, "</tbody></table></div>\n");
    free(rows);
}

/* Age×size heat map: row total for ab, column total for sb, and full-matrix total (regular files). */
static void emit_heat_map_margin_summary(FILE *out, const summary_t *sum, int ab, int sb) {
    uint64_t row_b = 0, row_f = 0, col_b = 0, col_f = 0;
    int i;
    char rb[32], cb[32], gb[32];
    char rf[128], cf[128], gf[128];

    if (!sum) return;
    for (i = 0; i < SIZE_BUCKETS; i++) {
        row_b += sum->bytes[ab][i];
        row_f += sum->files[ab][i];
    }
    for (i = 0; i < AGE_BUCKETS; i++) {
        col_b += sum->bytes[i][sb];
        col_f += sum->files[i][sb];
    }
    human_bytes(row_b, rb, sizeof(rb));
    human_bytes(col_b, cb, sizeof(cb));
    human_bytes(sum->total_bytes, gb, sizeof(gb));
    format_count_pretty_inline(row_f, rf, sizeof(rf));
    format_count_pretty_inline(col_f, cf, sizeof(cf));
    format_count_pretty_inline(sum->total_files, gf, sizeof(gf));

    fprintf(out, "<section class=\"heat-map-margins\" aria-label=\"Heat map row, column, and full totals\">\n");
    fprintf(out,
            "<p><strong>Age row total</strong> (this age band, all size buckets): <strong>%s</strong> in <strong>%s</strong> "
            "regular files.</p>\n",
            rb,
            rf);
    fprintf(out,
            "<p><strong>Size column total</strong> (this size band, all age buckets): <strong>%s</strong> in <strong>%s</strong> "
            "regular files.</p>\n",
            cb,
            cf);
    fprintf(out,
            "<p><strong>All buckets</strong> (full heat map, all age×size cells): <strong>%s</strong> in <strong>%s</strong> "
            "regular files.</p>\n",
            gb,
            gf);
    fprintf(out, "</section>\n");
}

typedef struct {
    const char *parent;
    uint64_t slice_files;
    uint64_t global_fanout;
} dense_drill_row_t;

typedef struct {
    const char *parent;
    uint64_t bytes;
    uint64_t files;
} deep_drill_row_t;

static int cmp_dense_drill_desc(const void *a, const void *b) {
    const dense_drill_row_t *ra = (const dense_drill_row_t *)a;
    const dense_drill_row_t *rb = (const dense_drill_row_t *)b;

    if (ra->slice_files > rb->slice_files) return -1;
    if (ra->slice_files < rb->slice_files) return 1;
    return strcmp(ra->parent, rb->parent);
}

static void dense_drill_topk_heap_sift_down(dense_drill_row_t *h, size_t n, size_t i) {
    for (;;) {
        size_t l = 2 * i + 1, r = l + 1, mx = i;

        if (l < n && cmp_dense_drill_desc(&h[l], &h[mx]) > 0) mx = l;
        if (r < n && cmp_dense_drill_desc(&h[r], &h[mx]) > 0) mx = r;
        if (mx == i) break;
        {
            dense_drill_row_t t = h[i];
            h[i] = h[mx];
            h[mx] = t;
        }
        i = mx;
    }
}

static void dense_drill_topk_heap_push(dense_drill_row_t *h, size_t *hsz, size_t kcap, dense_drill_row_t row) {
    size_t i;

    if (*hsz < kcap) {
        i = *hsz;
        h[i] = row;
        *hsz = i + 1;
        while (i > 0) {
            size_t p = (i - 1) / 2;

            if (cmp_dense_drill_desc(&h[i], &h[p]) <= 0) break;
            {
                dense_drill_row_t t = h[i];
                h[i] = h[p];
                h[p] = t;
            }
            i = p;
        }
        return;
    }
    if (cmp_dense_drill_desc(&row, &h[0]) >= 0) return;
    h[0] = row;
    dense_drill_topk_heap_sift_down(h, kcap, 0);
}

static int cmp_deep_drill_desc(const void *a, const void *b) {
    const deep_drill_row_t *ra = (const deep_drill_row_t *)a;
    const deep_drill_row_t *rb = (const deep_drill_row_t *)b;

    if (ra->files > rb->files) return -1;
    if (ra->files < rb->files) return 1;
    if (ra->bytes > rb->bytes) return -1;
    if (ra->bytes < rb->bytes) return 1;
    return strcmp(ra->parent, rb->parent);
}

static void deep_drill_topk_heap_sift_down(deep_drill_row_t *h, size_t n, size_t i) {
    for (;;) {
        size_t l = 2 * i + 1, r = l + 1, mx = i;

        if (l < n && cmp_deep_drill_desc(&h[l], &h[mx]) > 0) mx = l;
        if (r < n && cmp_deep_drill_desc(&h[r], &h[mx]) > 0) mx = r;
        if (mx == i) break;
        {
            deep_drill_row_t t = h[i];
            h[i] = h[mx];
            h[mx] = t;
        }
        i = mx;
    }
}

static void deep_drill_topk_heap_push(deep_drill_row_t *h, size_t *hsz, size_t kcap, deep_drill_row_t row) {
    size_t i;

    if (*hsz < kcap) {
        i = *hsz;
        h[i] = row;
        *hsz = i + 1;
        while (i > 0) {
            size_t p = (i - 1) / 2;

            if (cmp_deep_drill_desc(&h[i], &h[p]) <= 0) break;
            {
                deep_drill_row_t t = h[i];
                h[i] = h[p];
                h[p] = t;
            }
            i = p;
        }
        return;
    }
    if (cmp_deep_drill_desc(&row, &h[0]) >= 0) return;
    h[0] = row;
    deep_drill_topk_heap_sift_down(h, kcap, 0);
}

/*
 * Tables tying heat-map Deep/Dense/Skew badges to concrete parent directories for one bucket page.
 */
static void emit_drill_iso_time_cell(FILE *out, int have_t, uint64_t ts) {
    char buf[72];

    if (!have_t) {
        fputs("<td class=\"num r\" data-sort-n=\"-1\">&mdash;</td>\n", out);
        return;
    }
    format_wall_clock_local((time_t)ts, buf, sizeof(buf));
    fprintf(out, "<td class=\"num r\" data-sort-n=\"%.15g\">", (double)ts);
    html_escape(out, buf);
    fputs("</td>\n", out);
}

static void emit_bucket_shape_drill_section(FILE *out,
                                            int ab,
                                            int sb,
                                            const bucket_details_t *details,
                                            const dense_cell_map_t *slice_dense_parents,
                                            const dense_cell_fanout_lookup_t *fanout_lookup,
                                            const fanout_parent_stat_map_t *fanout_stats,
                                            const summary_t *heat_sum,
                                            const path_shape_view_t *shape,
                                            const char *basis_str,
                                            ereport_run_stats_t *run_rs) {
    uint64_t cell_bytes = heat_sum->bytes[ab][sb];
    uint64_t cell_files = heat_sum->files[ab][sb];
    uint64_t deep_b = shape->cell[ab][sb].deep_bytes;
    uint64_t dense_max = shape->cell[ab][sb].dense_fanout_max;
    char hb[32], hf[96], db[32];
    double deep_pct = 0.0;
    size_t i;
    uint32_t bi;
    const dense_node_t *n;

    if (cell_bytes > 0ULL && deep_b > 0ULL) deep_pct = 100.0 * (double)deep_b / (double)cell_bytes;

    human_bytes(cell_bytes, hb, sizeof(hb));
    format_count_pretty_inline(cell_files, hf, sizeof(hf));
    human_bytes(deep_b, db, sizeof(db));

    fputs("<h2 class=\"shape-drill-h2\">Path-shape drill-down</h2>\n", out);
    fprintf(out,
            "<p class=\"note\">This age&times;size slice has <strong>%s</strong> files totaling <strong>%s</strong> "
            "(same basis as the heat map). Deep-labelled bytes here are <strong>%s</strong> (~<strong>%.2f%%</strong> "
            "of slice bytes). Among parents represented in this slice, the largest crawl-wide immediate-child fan-out "
            "seen is <strong>%llu</strong> (the Dense badge uses parents with at least <strong>%u</strong> crawl-wide "
            "children).</p>\n",
            hf, hb, db, deep_pct, (unsigned long long)dense_max, (unsigned)PATH_SHAPE_DENSE_MIN_CHILDREN);

    /* Dense parents: slice grouping × crawl-wide fan-out */
    {
        size_t dense_cnt = 0;
        size_t show_n;

        for (bi = 0; bi < slice_dense_parents->nbuckets; bi++)
            for (n = slice_dense_parents->buckets[bi]; n; n = n->next) dense_cnt++;

        if (dense_cnt > 0) {
            size_t kcap = (size_t)BUCKET_SHAPE_DRILL_MAX_ROWS;
            dense_drill_row_t *rows = NULL;
            size_t row_n = 0;

            if (dense_cnt > kcap) {
                rows = (dense_drill_row_t *)malloc(kcap * sizeof(*rows));
                if (rows) {
                    size_t hsz = 0;
                    for (bi = 0; bi < slice_dense_parents->nbuckets; bi++) {
                        for (n = slice_dense_parents->buckets[bi]; n; n = n->next) {
                            dense_drill_row_t row;

                            row.parent = n->parent;
                            row.slice_files = n->n;
                            row.global_fanout = dense_cell_fanout_lookup_get_n_h(fanout_lookup, n->parent, n->hash);
                            dense_drill_topk_heap_push(rows, &hsz, kcap, row);
                        }
                    }
                    row_n = hsz;
                }
            } else {
                rows = (dense_drill_row_t *)calloc(dense_cnt, sizeof(*rows));
                if (rows) {
                    size_t k = 0;
                    for (bi = 0; bi < slice_dense_parents->nbuckets; bi++) {
                        for (n = slice_dense_parents->buckets[bi]; n; n = n->next) {
                            rows[k].parent = n->parent;
                            rows[k].slice_files = n->n;
                            rows[k].global_fanout = dense_cell_fanout_lookup_get_n_h(fanout_lookup, n->parent, n->hash);
                            k++;
                        }
                    }
                    row_n = dense_cnt;
                }
            }

            if (rows) {
                VT_QSORT_BUCKET(run_rs, rows, row_n, sizeof(*rows), cmp_dense_drill_desc);
                show_n = row_n;
                if (show_n > kcap) show_n = kcap;

                fprintf(out,
                        "<details class=\"bucket-help shape-drill-details\"><summary>Dense parents &mdash; up to "
                        "%u rows by slice files</summary>\n"
                        "<div class=\"bucket-help-body\">\n",
                        (unsigned)BUCKET_SHAPE_DRILL_MAX_ROWS);
                fprintf(out,
                        "<p class=\"table-trunc-note\">Immediate parent directories of files in this slice (each row is one "
                        "parent), ranked by <strong>slice files</strong> (most in this bucket first; click headers to sort). "
                        "At most %u unique parents are shown. <strong>Slice files</strong> counts regular files in this bucket "
                        "only. <strong>Child files / dirs / other</strong> count immediate crawl entries under that parent "
                        "(matched records only; crawl-wide fan-out drives the Dense badge). <strong>Min / max</strong> use this "
                        "report&rsquo;s time basis across those children.</p>\n",
                        (unsigned)BUCKET_SHAPE_DRILL_MAX_ROWS);
                fputs("<div class=\"bucket-table-wrap\"><table data-sort-col=\"0\" data-sort-dir=\"d\">\n", out);
                fputs("<thead><tr>"
                      "<th class=\"num r sort-h sort-desc\" data-i=\"0\" data-t=\"n\"><span class=\"th-text\">Slice files</span></th>"
                      "<th class=\"num r sort-h\" data-i=\"1\" data-t=\"n\"><span class=\"th-text\">Child files</span></th>"
                      "<th class=\"num r sort-h\" data-i=\"2\" data-t=\"n\"><span class=\"th-text\">Child dirs</span></th>"
                      "<th class=\"num r sort-h\" data-i=\"3\" data-t=\"n\"><span class=\"th-text\">Child other</span></th>"
                      "<th class=\"num r sort-h\" data-i=\"4\" data-t=\"n\"><span class=\"th-text\">Min",
                      out);
                if (basis_str && basis_str[0]) {
                    fputs(" <span class=\"meta-sub\">(", out);
                    html_escape(out, basis_str);
                    fputs(")</span>", out);
                }
                fputs("</span></th>"
                      "<th class=\"num r sort-h\" data-i=\"5\" data-t=\"n\"><span class=\"th-text\">Max",
                      out);
                if (basis_str && basis_str[0]) {
                    fputs(" <span class=\"meta-sub\">(", out);
                    html_escape(out, basis_str);
                    fputs(")</span>", out);
                }
                fputs("</span></th>"
                      "<th class=\"path-cell sort-h\" data-i=\"6\" data-t=\"s\"><span class=\"th-text\">Parent directory</span></th>"
                      "</tr></thead>\n<tbody>\n",
                      out);
                for (i = 0; i < show_n; i++) {
                    char sf[96], cf[96], cd[96], co[96];
                    const fanout_parent_stat_node_t *st =
                        (fanout_stats && rows[i].parent) ? fanout_parent_stat_lookup(fanout_stats, rows[i].parent) : NULL;
                    uint64_t ch_f = st ? st->n_files : 0ULL;
                    uint64_t ch_d = st ? st->n_dirs : 0ULL;
                    uint64_t ch_o = st ? st->n_others : 0ULL;

                    format_count_pretty_inline(rows[i].slice_files, sf, sizeof(sf));
                    format_count_pretty_inline(ch_f, cf, sizeof(cf));
                    format_count_pretty_inline(ch_d, cd, sizeof(cd));
                    format_count_pretty_inline(ch_o, co, sizeof(co));
                    fputs("<tr>", out);
                    fprintf(out, "<td class=\"num r\" data-sort-n=\"%.15g\">%s</td>", (double)rows[i].slice_files, sf);
                    fprintf(out, "<td class=\"num r\" data-sort-n=\"%.15g\">%s</td>", (double)ch_f, cf);
                    fprintf(out, "<td class=\"num r\" data-sort-n=\"%.15g\">%s</td>", (double)ch_d, cd);
                    fprintf(out, "<td class=\"num r\" data-sort-n=\"%.15g\">%s</td>", (double)ch_o, co);
                    emit_drill_iso_time_cell(out, st ? (int)st->have_time : 0, st ? st->t_min : 0ULL);
                    emit_drill_iso_time_cell(out, st ? (int)st->have_time : 0, st ? st->t_max : 0ULL);
                    fputs("<td class=\"path-cell\" data-sort-s=\"", out);
                    html_escape(out, rows[i].parent);
                    fputs("\">", out);
                    html_escape(out, rows[i].parent);
                    fputs("</td></tr>\n", out);
                }
                fputs("</tbody></table></div>\n", out);
                if (dense_cnt > show_n) {
                    fprintf(out,
                            "<p class=\"table-trunc-note\">Showing %zu of %zu unique parents ranked by slice files (most first).</p>\n",
                            show_n, dense_cnt);
                }
                fputs("</div></details>\n", out);
            }
            free(rows);
        }
    }

    /* Deep parents: bucket files with deep paths */
    {
        dense_cell_map_t dm_bytes;
        dense_cell_map_t dm_files;
        size_t deep_cnt = 0;
        size_t show_n;
        int deep_ok = 1;

        memset(&dm_bytes, 0, sizeof(dm_bytes));
        memset(&dm_files, 0, sizeof(dm_files));

        if (details && details->count > 0) {
            for (i = 0; i < details->count; i++) {
                char parent[PATH_MAX];
                char fbuf[PATH_MAX];
                const detail_record_t *rec = &details->items[i];
                const char *full = path_join_pl(rec->parent, rec->leaf, fbuf, sizeof(fbuf));

                if (!full || !*full) continue;
                if (path_slash_count_str(full) < PATH_SHAPE_DEEP_MIN_SLASHES) continue;
                uint32_t ph;
                if (parent_dir_to_buf(full, parent, sizeof(parent)) != 0) continue;
                ph = dense_hash_full(parent);
                if (dense_cell_add_h(&dm_bytes, parent, ph, rec->size, NULL) != 0 ||
                    dense_cell_add_h(&dm_files, parent, ph, 1ULL, NULL) != 0) {
                    deep_ok = 0;
                    break;
                }
            }
        }

        if (deep_ok) {
            for (bi = 0; bi < dm_bytes.nbuckets; bi++)
                for (n = dm_bytes.buckets[bi]; n; n = n->next) deep_cnt++;

            if (deep_cnt > 0) {
                size_t kcap = (size_t)BUCKET_SHAPE_DRILL_MAX_ROWS;
                deep_drill_row_t *rows = NULL;
                size_t row_n = 0;

                if (deep_cnt > kcap) {
                    rows = (deep_drill_row_t *)malloc(kcap * sizeof(*rows));
                    if (rows) {
                        size_t hsz = 0;
                        for (bi = 0; bi < dm_bytes.nbuckets; bi++) {
                            for (n = dm_bytes.buckets[bi]; n; n = n->next) {
                                deep_drill_row_t row;

                                row.parent = n->parent;
                                row.bytes = n->n;
                                row.files = dense_cell_map_get_n_h(&dm_files, n->parent, n->hash);
                                deep_drill_topk_heap_push(rows, &hsz, kcap, row);
                            }
                        }
                        row_n = hsz;
                    }
                } else {
                    rows = (deep_drill_row_t *)calloc(deep_cnt, sizeof(*rows));
                    if (rows) {
                        size_t k = 0;
                        for (bi = 0; bi < dm_bytes.nbuckets; bi++) {
                            for (n = dm_bytes.buckets[bi]; n; n = n->next) {
                                rows[k].parent = n->parent;
                                rows[k].bytes = n->n;
                                rows[k].files = dense_cell_map_get_n_h(&dm_files, n->parent, n->hash);
                                k++;
                            }
                        }
                        row_n = deep_cnt;
                    }
                }

                if (rows) {
                    VT_QSORT_BUCKET(run_rs, rows, row_n, sizeof(*rows), cmp_deep_drill_desc);
                    show_n = row_n;
                    if (show_n > kcap) show_n = kcap;

                    fprintf(out,
                            "<details class=\"bucket-help shape-drill-details\"><summary>Deep parents &mdash; up to "
                            "%u rows by deep slice files</summary>\n"
                            "<div class=\"bucket-help-body\">\n",
                            (unsigned)BUCKET_SHAPE_DRILL_MAX_ROWS);
                    fprintf(out,
                            "<p class=\"table-trunc-note\">Immediate parents of bucket files whose full path has at least "
                            "<strong>%u</strong> &lsquo;<strong>/</strong>&rsquo; characters, ranked by <strong>deep slice "
                            "files</strong> (most first; click headers to sort). At most %u unique parents are shown.</p>\n",
                            (unsigned)PATH_SHAPE_DEEP_MIN_SLASHES,
                            (unsigned)BUCKET_SHAPE_DRILL_MAX_ROWS);
                    fputs("<div class=\"bucket-table-wrap\"><table data-sort-col=\"1\" data-sort-dir=\"d\">\n", out);
                    fputs("<thead><tr>"
                          "<th class=\"path-cell sort-h\" data-i=\"0\" data-t=\"s\"><span class=\"th-text\">Parent directory</span></th>"
                          "<th class=\"num r sort-h sort-desc\" data-i=\"1\" data-t=\"n\"><span class=\"th-text\">Deep slice "
                          "files</span></th>"
                          "<th class=\"num r sort-h\" data-i=\"2\" data-t=\"n\"><span class=\"th-text\">Deep slice bytes</span></th>"
                          "</tr></thead>\n<tbody>\n",
                          out);
                    for (i = 0; i < show_n; i++) {
                        char sf[96], sbytes[32];
                        format_count_pretty_inline(rows[i].files, sf, sizeof(sf));
                        human_bytes(rows[i].bytes, sbytes, sizeof(sbytes));
                        fputs("<tr><td class=\"path-cell\" data-sort-s=\"", out);
                        html_escape(out, rows[i].parent);
                        fputs("\">", out);
                        html_escape(out, rows[i].parent);
                        fputs("</td>", out);
                        fprintf(out, "<td class=\"num r\" data-sort-n=\"%.15g\">%s</td>", (double)rows[i].files, sf);
                        fprintf(out, "<td class=\"num r\" data-sort-n=\"%.15g\">%s</td>", (double)rows[i].bytes, sbytes);
                        fputs("</tr>\n", out);
                    }
                    fputs("</tbody></table></div>\n", out);
                    if (deep_cnt > show_n) {
                        fprintf(out,
                                "<p class=\"table-trunc-note\">Showing %zu of %zu unique parents ranked by deep slice files "
                                "(most first).</p>\n",
                                show_n, deep_cnt);
                    }
                    fputs("</div></details>\n", out);
                }
                free(rows);
            }
        }

        dense_cell_free(&dm_bytes);
        dense_cell_free(&dm_files);
    }

    if (heat_cell_skew_visible(heat_sum, shape, ab, sb)) {
        fputs("<details class=\"bucket-help shape-drill-details\"><summary>Skew &mdash; how Deep and Dense overlap</summary>\n"
              "<div class=\"bucket-help-body\">\n"
              "<p class=\"note\"><strong>Skew.</strong> The heat-map <strong>Skew</strong> pill applies when both Deep and Dense "
              "would badge this slice: substantial bytes under unusually deep paths while some parents here also sit under "
              "very large crawl-wide fan-out. Compare parent directories that appear in both tables above.</p>\n"
              "</div></details>\n",
              out);
    }
}

typedef struct {
    char *base_prefix;
    size_t ord_lo;
    size_t ord_hi;
} bucket_page_slice_t;

typedef struct {
    bucket_details_t (*details)[SIZE_BUCKETS];
    const matched_records_t *mr;
    const size_t *ord;
    bucket_page_slice_t (*slices)[SIZE_BUCKETS];
    atomic_int next_cell;
} bucket_slice_prep_ctx_t;

static void bucket_page_slices_free(bucket_page_slice_t slices[AGE_BUCKETS][SIZE_BUCKETS]);

static void bucket_fill_one_page_slice(bucket_details_t *bd,
                                       const matched_records_t *mr,
                                       const size_t *ord,
                                       bucket_page_slice_t *sl) {
    size_t n;

    sl->base_prefix = NULL;
    sl->ord_lo = 0;
    sl->ord_hi = 0;
    if (!bd || bd->count == 0 || !mr || !ord) return;
    n = mr->count;
    sl->base_prefix = dup_common_dir_prefix(bd);
    if (!sl->base_prefix) return;
    if (n > 0 && sl->base_prefix[0] != '\0') {
        sl->ord_lo = matched_ord_lower_bound_seg(mr, ord, n, sl->base_prefix);
        sl->ord_hi = matched_ord_first_not_under_prefix(mr, ord, n, sl->ord_lo, sl->base_prefix);
    } else {
        sl->ord_lo = 0;
        sl->ord_hi = n;
    }
}

static void *bucket_slice_prep_worker(void *vp) {
    bucket_slice_prep_ctx_t *c = (bucket_slice_prep_ctx_t *)vp;

    for (;;) {
        int k = atomic_fetch_add_explicit(&c->next_cell, 1, memory_order_relaxed);
        int ab;
        int sb;

        if (k >= AGE_BUCKETS * SIZE_BUCKETS) break;
        ab = k / SIZE_BUCKETS;
        sb = k % SIZE_BUCKETS;
        bucket_fill_one_page_slice(&c->details[ab][sb], c->mr, c->ord, &c->slices[ab][sb]);
    }
    return NULL;
}

static void bucket_prepare_page_slices(bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS],
                                       const matched_records_t *mr,
                                       const size_t *ord,
                                       bucket_page_slice_t slices[AGE_BUCKETS][SIZE_BUCKETS]) {
    int nw_l;
    int i;
    int ab;
    int sb;
    pthread_t *tp = NULL;
    bucket_slice_prep_ctx_t ctx;

    memset(slices, 0, sizeof(bucket_page_slice_t) * (size_t)AGE_BUCKETS * (size_t)SIZE_BUCKETS);
    if (!mr || !ord || mr->count == 0) return;

    memset(&ctx, 0, sizeof(ctx));
    ctx.details = details;
    ctx.mr = mr;
    ctx.ord = ord;
    ctx.slices = slices;
    atomic_init(&ctx.next_cell, 0);

    nw_l = parse_ereport_thread_count();
    if (nw_l < 1) nw_l = 1;
    if (nw_l > AGE_BUCKETS * SIZE_BUCKETS) nw_l = AGE_BUCKETS * SIZE_BUCKETS;

    tp = (pthread_t *)malloc((size_t)nw_l * sizeof(pthread_t));
    if (!tp) {
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_fill_one_page_slice(&details[ab][sb], mr, ord, &slices[ab][sb]);
        }
        return;
    }

    for (i = 0; i < nw_l; i++) {
        if (pthread_create(&tp[i], NULL, bucket_slice_prep_worker, &ctx) != 0) {
            int j;
            for (j = 0; j < i; j++) pthread_join(tp[j], NULL);
            free(tp);
            bucket_page_slices_free(slices);
            memset(slices, 0, sizeof(bucket_page_slice_t) * (size_t)AGE_BUCKETS * (size_t)SIZE_BUCKETS);
            for (ab = 0; ab < AGE_BUCKETS; ab++) {
                for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_fill_one_page_slice(&details[ab][sb], mr, ord, &slices[ab][sb]);
            }
            return;
        }
    }
    for (i = 0; i < nw_l; i++) pthread_join(tp[i], NULL);
    free(tp);
}

static void bucket_page_slices_free(bucket_page_slice_t slices[AGE_BUCKETS][SIZE_BUCKETS]) {
    int ab;
    int sb;

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            free(slices[ab][sb].base_prefix);
            slices[ab][sb].base_prefix = NULL;
        }
    }
}

static const char BUCKET_DETAIL_PAGE_HEAD[] =
    "<!DOCTYPE html>\n"
    "<html lang=\"en\">\n"
    "<head>\n"
    "<meta charset=\"utf-8\">\n"
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
    "<title>Bucket Details</title>\n"
    "<style>\n"
    "html{box-sizing:border-box}\n"
    "body{font-family:\"DejaVu Sans Mono\",\"Consolas\",monospace;margin:0;padding:10px clamp(10px,2.2vw,28px) 28px;width:100%;max-width:none;box-sizing:border-box;color:#1f2328;background:#fcfcf8}\n"
    ".bucket-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%;max-width:none;margin-bottom:22px}\n"
    "h1,h2{margin:0 0 12px 0;font-weight:600}\n"
    "h3{margin:18px 0 10px;font-weight:600;font-size:14px;color:#3d362c}\n"
    ".shape-drill-h2{margin-top:22px}\n"
    ".shape-drill-details{margin-top:12px}\n"
    ".meta{margin:0 0 14px 0;color:#555;line-height:1.5;font-size:13px}\n"
    ".meta-sub{font-weight:400;color:#666}\n"
    ".note{font-size:12px;color:#666;margin-bottom:14px;max-width:none;width:100%}\n"
    ".table-trunc-note{font-size:12px;color:#555;margin:0 0 12px;max-width:none;width:100%;line-height:1.45}\n"
    ".bucket-help{margin:0 0 18px;border:1px solid #ddd2c8;border-radius:8px;background:#faf8f4;max-width:none;width:100%;font-size:13px;line-height:1.55;color:#555}\n"
    ".bucket-help summary{cursor:pointer;padding:10px 12px;font-weight:600;color:#4a4034;list-style-position:outside}\n"
    ".bucket-help summary::-webkit-details-marker{color:#8b7355}\n"
    ".bucket-help .bucket-help-body{padding:0 12px 12px}\n"
    ".bucket-help .bucket-help-body p{margin:0 0 10px}\n"
    ".bucket-help .bucket-help-body p:last-child{margin-bottom:0}\n"
    "table{border-collapse:collapse;font-size:13px;table-layout:auto;width:max-content;max-width:none}\n"
    "th,td{border:1px solid #d5d0c5;padding:5px 12px;vertical-align:top}\n"
    "thead th{background:#ece6da;position:sticky;top:0;z-index:3}\n"
    "thead th:first-child,tbody td:first-child{position:sticky;left:0;z-index:4}\n"
    "thead th:first-child{background:#ece6da;z-index:5}\n"
    "tbody td:first-child{background:#f8f5ee;z-index:1}\n"
    "td.r,th.r{text-align:right}\n"
    ".bucket-table-wrap td.num{white-space:nowrap;box-sizing:border-box}\n"
    ".bucket-table-wrap th.num{white-space:normal;box-sizing:border-box}\n"
    ".bucket-table-wrap th.num .th-text{overflow-wrap:break-word;word-break:normal}\n"
    ".bucket-table-wrap th.sort-h{cursor:pointer;user-select:none}\n"
    ".bucket-table-wrap th.sort-h:hover{background:#ddd4c4}\n"
    ".bucket-table-wrap th.sort-asc .th-text::after{content:\" \\25b2\";font-size:0.72em;opacity:0.85}\n"
    ".bucket-table-wrap th.sort-desc .th-text::after{content:\" \\25bc\";font-size:0.72em;opacity:0.85}\n"
    "th:first-child{min-width:max(260px,min(28vw,560px));max-width:none;box-sizing:border-box}\n"
    ".path-cell{min-width:max(260px,min(28vw,560px));max-width:none;overflow-x:hidden;overflow-y:visible;box-sizing:border-box}\n"
    ".path-line{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start;gap:8px;min-width:0}\n"
    ".path-toggle{min-width:0;max-width:100%;display:block;border:0;background:none;padding:0;margin:0;color:inherit;font:inherit;text-align:left;cursor:pointer}\n"
    ".path-prefix{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#8b8172;font-size:11px;line-height:1.15;margin-bottom:1px}\n"
    ".path-tail{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700;color:#1f2328;line-height:1.2}\n"
    ".copy-path{opacity:0;pointer-events:none;align-self:start;flex-shrink:0;border:1px solid #d8ccb8;background:#f4ede0;color:#6b4c16;border-radius:999px;padding:1px 5px;font:inherit;font-size:9px;line-height:1.2;cursor:pointer;transition:opacity 0.15s ease,background-color 0.15s ease}\n"
    ".path-cell:hover .copy-path,.path-cell:focus-within .copy-path,.path-cell.expanded .copy-path{opacity:1;pointer-events:auto}\n"
    ".copy-path:hover{background:#eadfc9}\n"
    ".path-toggle:hover .path-tail,.path-toggle:focus .path-tail{text-decoration:underline}\n"
    ".path-full{margin-top:6px;padding:6px 8px;max-width:100%;box-sizing:border-box;background:#f4efe4;border:1px solid #ddd2bf;border-radius:4px;white-space:normal;word-break:break-all;overflow-wrap:anywhere;font-size:10px;line-height:1.35;color:#4e4538;user-select:all;overflow-x:auto}\n"
    ".path-cell.expanded .path-prefix{white-space:normal;overflow:hidden;text-overflow:clip;word-break:break-all;overflow-wrap:anywhere}\n"
    ".heat-map-margins{font-size:12px;color:#555;margin:0 0 16px;line-height:1.5;max-width:none;width:100%}\n"
    ".heat-map-margins p{margin:6px 0 0}\n"
    "a{color:#6b4c16;text-decoration:none}\n"
    ".path-ctime-led-badge{font-size:9px;font-weight:700;line-height:1;padding:2px 6px;border-radius:999px;background:rgba(109,40,217,0.92);color:#faf5ff;border:1px solid rgba(76,29,149,0.85);margin-right:6px;vertical-align:middle;white-space:nowrap;display:inline-block}\n"
    ".path-ctime-led-pct{font-variant-numeric:tabular-nums}\n"
    ".bucket-heat-shape{font-size:12px;color:#444;margin:0 0 14px;line-height:1.5;display:flex;flex-wrap:wrap;align-items:center;gap:6px;max-width:none;width:100%}\n"
    ".bucket-heat-shape-label{font-weight:600;color:#555;margin-right:2px}\n"
    ".bucket-heat-shape .heat-map-badges{position:static;display:flex;flex-direction:row;flex-wrap:wrap;align-items:center;gap:4px;pointer-events:auto}\n"
    ".bucket-heat-shape .heat-ctime-led-badge,.bucket-heat-shape .heat-shape-badge{font-size:8px;pointer-events:auto;cursor:help}\n"
    ".bucket-heat-shape .heat-ctime-led-badge{font-size:8px;font-weight:700;line-height:1;padding:2px 5px;border-radius:4px;background:rgba(109,40,217,0.94);color:#faf5ff;border:1px solid rgba(76,29,149,0.88);letter-spacing:0.02em;white-space:nowrap;margin-right:0}\n"
    ".heat-shape-badge{font-size:8px;font-weight:700;line-height:1;padding:2px 5px;border-radius:4px;letter-spacing:0.02em;white-space:nowrap;border:1px solid rgba(0,0,0,0.14)}\n"
    ".heat-shape-deep{background:rgba(14,116,144,0.92);color:#ecfeff}\n"
    ".heat-shape-dense{background:rgba(180,83,9,0.92);color:#fffbeb}\n"
    ".heat-shape-skew{background:rgba(127,29,29,0.92);color:#fef2f2}\n";

static int emit_bucket_detail_page(const char *filename,
                                   const char *username,
                                   int all_users,
                                   uint64_t distinct_uids,
                                   const char *basis_str,
                                   int ab,
                                   int sb,
                                   int detail_levels,
                                   const bucket_details_t *details,
                                   const matched_records_t *matched_records,
                                   size_t *matched_path_ord,
                                   uint64_t corpus_total_user_files,
                                   uint64_t corpus_total_user_bytes,
                                   const summary_t *heat_sum,
                                   const path_shape_view_t *shape,
                                   const dense_cell_map_t (*merged_dense_matrix)[SIZE_BUCKETS],
                                   const dense_cell_fanout_lookup_t *fanout_lookup_shape,
                                   const fanout_parent_stat_map_t *fanout_parent_stats,
                                   ereport_run_stats_t *run_rs,
                                   const bucket_page_slice_t *pre_slice,
                                   bucket_page_scratch_t *scratch) {
    FILE *out = counted_fopen(filename, "w");
    char *base_prefix = NULL;
    int base_prefix_owned = 0;
    path_row_map_t *maps;
    size_t i;
    uint64_t bucket_files = 0;
    uint64_t bucket_bytes = 0;
    uint64_t total_user_files = corpus_total_user_files;
    uint64_t total_user_bytes = corpus_total_user_bytes;
    int d;
    int init_fail_at = -1;

    if (!out) return -1;
    (void)setvbuf(out, NULL, _IOFBF, 1 << 20);

    if (detail_levels < 1 || detail_levels > BUCKET_DETAIL_LEVELS_MAX || !scratch) {
        counted_fclose(out);
        return -1;
    }

    maps = scratch->page_maps;
    for (d = 0; d < detail_levels; d++) {
        size_t want = path_row_map_seed_cap(details->count, d, scratch->page_distinct[d], scratch->page_records);

        if (path_row_map_reset_or_init(&maps[d], want) != 0) {
            init_fail_at = d;
            break;
        }
    }
    if (init_fail_at >= 0) {
        counted_fclose(out);
        return -1;
    }

    /* One fwrite of the shared head+CSS; rewriting it with dozens of fprintfs
     * per age×size page was a measurable slice of bucket_html_cells wall. */
    if (fwrite(BUCKET_DETAIL_PAGE_HEAD, 1, sizeof(BUCKET_DETAIL_PAGE_HEAD) - 1, out) !=
        sizeof(BUCKET_DETAIL_PAGE_HEAD) - 1) {
        counted_fclose(out);
        return -1;
    }
    emit_heat_badge_tip_shell_css(out);
    fprintf(out, "</style>\n</head>\n<body>\n");

    fprintf(out, "<h1>Bucket Details</h1>\n<div class=\"meta\">");
    if (all_users) {
        fprintf(out, "Scope: <strong>all crawled users</strong> <span class=\"meta-sub\">(%" PRIu64 " distinct UIDs)</span> | ",
                distinct_uids);
    } else {
        fprintf(out, "User: <strong>");
        html_escape(out, username);
        fprintf(out, "</strong> | ");
    }
    fprintf(out, "Basis: <strong>");
    html_escape(out, basis_str);
    fprintf(out, "</strong> | Age: <strong>");
    html_escape(out, age_bucket_names[ab]);
    fprintf(out, "</strong> | Size: <strong>");
    html_escape(out, size_bucket_names[sb]);
    fprintf(out, "</strong></div>\n");

    if (shape && heat_sum) {
        uint64_t cell_b = heat_sum->bytes[ab][sb];
        uint64_t cell_f = heat_sum->files[ab][sb];
        uint64_t cl_b = heat_sum->ctime_led_bytes[ab][sb];
        int show_cl = ctime_led_badge_visible(cl_b, cell_b);
        int d = shape_deep_badge_visible(shape->cell[ab][sb].deep_bytes, cell_b, cell_f);
        int dn = shape_dense_badge_visible(shape->cell[ab][sb].dense_fanout_max);

        if (show_cl || d || dn) {
            char led_lbl[24];
            char scope_line[160];
            format_ctime_led_share_label(cl_b, cell_b, led_lbl, sizeof(led_lbl));
            snprintf(scope_line,
                     sizeof(scope_line),
                     "%s × %s",
                     age_bucket_names[ab],
                     size_bucket_names[sb]);
            fprintf(out, "<div class=\"bucket-heat-shape\"><span class=\"bucket-heat-shape-label\">Heat map slice:</span> ");
            emit_heat_map_badges(out,
                                 1,
                                 show_cl,
                                 led_lbl,
                                 cl_b,
                                 shape->cell[ab][sb].deep_bytes,
                                 cell_b,
                                 cell_f,
                                 shape->cell[ab][sb].dense_fanout_max,
                                 basis_str,
                                 scope_line);
            fprintf(out, "</div>\n");
        }
    }

    if (heat_sum) emit_heat_map_margin_summary(out, heat_sum, ab, sb);

    if (details->count == 0) {
        fprintf(out, "<div class=\"note\">This bucket has no matching files.</div>\n</body>\n</html>\n");
        counted_fclose(out);
        return 0;
    }

    if (pre_slice && pre_slice->base_prefix) {
        base_prefix = pre_slice->base_prefix;
        base_prefix_owned = 0;
    } else {
        base_prefix = dup_common_dir_prefix(details);
        base_prefix_owned = 1;
    }
    if (!base_prefix) {
        counted_fclose(out);
        return -1;
    }

    for (i = 0; i < details->count; i++) {
        bucket_files++;
        bucket_bytes += details->items[i].size;
    }

    {
        int pre_tot = (pre_slice && pre_slice->base_prefix && pre_slice->base_prefix[0] != '\0' && matched_path_ord);

        if (aggregate_bucket_for_page_n(maps, detail_levels, details, base_prefix, scratch) != 0 ||
            aggregate_totals_for_page_n(maps, detail_levels, matched_records, base_prefix, matched_path_ord,
                                        pre_tot ? pre_slice->ord_lo : 0, pre_tot ? pre_slice->ord_hi : 0, pre_tot) !=
                0) {
            if (base_prefix_owned) free(base_prefix);
            counted_fclose(out);
            return -1;
        }
    }

    {
        char bb[32];
        char bfiles_p[96];

        human_bytes(bucket_bytes, bb, sizeof(bb));
        format_count_pretty_inline(bucket_files, bfiles_p, sizeof(bfiles_p));
        fprintf(out, "<div class=\"meta\">Base path: <strong>");
        html_escape(out, base_prefix[0] ? base_prefix : ".");
        fprintf(out, "</strong> | Bucket files: <strong>%s</strong> | Bucket bytes: <strong>%s</strong></div>\n", bfiles_p, bb);
    }
    fputs("<details class=\"bucket-help\"><summary>How to read these tables</summary>\n"
          "<div class=\"bucket-help-body\">\n"
          "<p><strong>Sorting.</strong> Click a column header to sort; click again to reverse. "
          "The default is bucket bytes (largest first). The path column sorts by full path (A&ndash;Z).</p>\n"
          "<p><strong>Heat map slice badges.</strong> When shown below the title, the pills match the corresponding "
          "<a href=\"index.html\">index.html</a> heat-map cell for this age&times;size slice (purple <strong>C</strong>; "
          "<strong>Deep</strong> or <strong>Dense</strong> alone; or <strong>Skew</strong> when both apply&mdash;Skew replaces "
          "the separate Deep and Dense pills) under the same rules as the main report.</p>\n",
          out);
    fprintf(out,
            "<p><strong>Path-shape drill-down.</strong> Below this box, collapsible sections list up to <strong>%u</strong> "
            "worst unique parent directories for <strong>Dense</strong> fan-out (with child type counts and min/max times) "
            "and <strong>Deep</strong> paths (many slashes). If this slice earns a <strong>Skew</strong> badge, a third "
            "collapsible section explains how Deep and Dense overlap.</p>\n",
            (unsigned)BUCKET_SHAPE_DRILL_MAX_ROWS);
    fputs("<p><strong>Bucket columns.</strong> &ldquo;Bucket files&rdquo; and &ldquo;Bucket bytes&rdquo; count only files that fall in this age/size bucket. "
          "&ldquo;Share of bucket files/bytes&rdquo; is the fraction of <em>this bucket&rsquo;s</em> total that sits under each path. "
          "&ldquo;C-led bytes&rdquo; is the percent of those bucket bytes whose <em>ctime</em> is substantially newer than "
          "both <em>atime</em> and <em>mtime</em> (here: at least <strong>180 days</strong> newer than the newer of the "
          "two), suggesting apparent recency is metadata-led rather than usage- or content-led. High values may indicate "
          "stale data artificially refreshed by metadata-only changes (e.g. <strong>chmod</strong>, <strong>chown</strong>, "
          "<strong>ACL</strong> updates, <strong>rsync</strong> attribute updates, migration). The purple "
          "<strong>C-led</strong> pill appears only when that share is at least <strong>30%</strong> of bucket bytes.</p>\n"
          "<p><strong>User share columns.</strong> &ldquo;Share of user bytes/files&rdquo; compares this path to <em>all</em> of the user&rsquo;s files across every bucket (overall footprint).</p>\n"
          "<p><strong>Totals under each path.</strong> Total files, dirs, and bytes include everything recorded below that directory prefix.</p>\n",
          out);
    fprintf(out,
            "<p><strong>Levels 1&ndash;%d.</strong> Each table groups paths by cumulative directory depth below the shared base path "
            "(one segment per level). A row appears at level <em>n</em> only when there is at least one path segment at that depth.</p>\n",
            detail_levels);
    fputs(
          "<p><strong>Heat colors.</strong> <strong>Blue</strong> tints mark data volume (bucket bytes, share of bucket bytes, "
          "total bytes, share of user bytes). <strong>Red</strong> tints mark file-style counts (bucket files, share of bucket files, "
          "total files, total dirs, share of user files). Row-relative columns scale strongest vs other rows in that table.</p>\n"
          "</div></details>\n",
          out);

    if (merged_dense_matrix && fanout_lookup_shape && heat_sum && shape && details) {
        emit_bucket_shape_drill_section(out, ab, sb, details, &merged_dense_matrix[ab][sb], fanout_lookup_shape,
                                        fanout_parent_stats, heat_sum, shape, basis_str, run_rs);
    }

    for (d = 0; d < detail_levels; d++) {
        char title[64];
        snprintf(title, sizeof(title), "Level %d Directories", d + 1);
        emit_path_summary_table(out,
                                title,
                                &maps[d],
                                bucket_files,
                                bucket_bytes,
                                total_user_files,
                                total_user_bytes,
                                base_prefix,
                                d,
                                run_rs);
    }

    fputs("<script>\n"
          "(function(){\n"
          "function copyText(text){if(navigator.clipboard&&window.isSecureContext){return navigator.clipboard.writeText(text);}return new Promise(function(resolve,reject){var ta=document.createElement('textarea');ta.value=text;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.focus();ta.select();try{document.execCommand('copy');resolve();}catch(err){reject(err);}document.body.removeChild(ta);});}\n"
          "function sortTable(table,th){var col=parseInt(th.getAttribute('data-i'),10);var typ=th.getAttribute('data-t');var lco=table.getAttribute('data-sort-col');var ldi=table.getAttribute('data-sort-dir');var dir;if(String(col)===lco&&ldi){dir=ldi==='d'?'a':'d';}else{dir=typ==='s'?'a':'d';}table.setAttribute('data-sort-col',String(col));table.setAttribute('data-sort-dir',dir);table.querySelectorAll('thead th.sort-h').forEach(function(x){x.classList.remove('sort-asc','sort-desc');});th.classList.add(dir==='d'?'sort-desc':'sort-asc');var tb=table.querySelector('tbody');if(!tb)return;var rows=Array.prototype.slice.call(tb.rows);rows.sort(function(ra,rb){var ca=ra.cells[col];var cb=rb.cells[col];if(!ca||!cb)return 0;var cmp;if(typ==='s'){var sa=ca.getAttribute('data-sort-s')||'';var sb=cb.getAttribute('data-sort-s')||'';cmp=sa.localeCompare(sb);}else{var va=parseFloat(ca.getAttribute('data-sort-n'));var vb=parseFloat(cb.getAttribute('data-sort-n'));if(isNaN(va))va=0;if(isNaN(vb))vb=0;cmp=va-vb;}if(dir==='d')cmp=-cmp;return cmp||0;});rows.forEach(function(r){tb.appendChild(r);});}\n"
          "document.querySelectorAll('.bucket-table-wrap thead th.sort-h').forEach(function(th){th.addEventListener('click',function(){sortTable(th.closest('table'),th);});});\n"
          "document.querySelectorAll('.copy-path').forEach(function(btn){btn.addEventListener('click',function(ev){var text=btn.getAttribute('data-copy');var old=btn.textContent;ev.preventDefault();ev.stopPropagation();copyText(text).then(function(){btn.textContent='Copied';setTimeout(function(){btn.textContent=old;},900);}).catch(function(){btn.textContent='Copy?';setTimeout(function(){btn.textContent=old;},1200);});});});\n"
          "document.querySelectorAll('.path-toggle').forEach(function(btn){btn.addEventListener('click',function(ev){var cell=btn.closest('.path-cell');var full=cell.querySelector('.path-full');var expanded=cell.classList.toggle('expanded');full.hidden=!expanded;btn.setAttribute('aria-expanded',expanded?'true':'false');ev.preventDefault();});});\n"
          "})();\n", out);
    emit_heat_badge_tip_install_js(out);
    fputs("</script>\n", out);
    fprintf(out, "</body>\n</html>\n");

    if (base_prefix_owned) free(base_prefix);
    /* Hand this page's shape to the next one and leave the tables allocated. */
    scratch->page_records = details->count;
    for (d = 0; d < detail_levels; d++) scratch->page_distinct[d] = maps[d].count;
    counted_fclose(out);
    return 0;
}

static int build_bucket_page_path(char *out, size_t out_sz, int ab, int sb) {
    char suffix[32];
    size_t dir_len = strlen(g_bucket_output_dir);
    int suffix_len;

    suffix_len = snprintf(suffix, sizeof(suffix), "/bucket_a%d_s%d.html", ab, sb);
    if (suffix_len < 0 || (size_t)suffix_len >= sizeof(suffix)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    if (dir_len + (size_t)suffix_len >= out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }

    memcpy(out, g_bucket_output_dir, dir_len);
    memcpy(out + dir_len, suffix, (size_t)suffix_len + 1);
    return 0;
}

static int ensure_bucket_output_dir_exists(void) {
    char buf[PATH_MAX];
    size_t len;

    if (g_bucket_output_dir[0] == '\0') return -1;
    len = strlen(g_bucket_output_dir);
    if (len >= sizeof(buf)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(buf, g_bucket_output_dir, len + 1U);
    return mkdir_p_path(buf);
}

static int emit_bucket_detail_stub_fast(const char *filename,
                                        const char *username,
                                        int all_users,
                                        uint64_t distinct_uids,
                                        const char *basis_str,
                                        int ab,
                                        int sb,
                                        const summary_t *sum) {
    FILE *out = counted_fopen(filename, "w");
    char hb[32];

    if (!out) return -1;
    human_bytes(sum->bytes[ab][sb], hb, sizeof(hb));

    fprintf(out, "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    fprintf(out, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    fprintf(out, "<title>Bucket summary</title>\n<style>\n");
    fprintf(out, "body{font-family:Arial,sans-serif;margin:24px;color:#222;line-height:1.45}\n");
    fprintf(out, ".meta{margin:0 0 14px 0;color:#555;font-size:13px}\n");
    fprintf(out, ".note{margin:16px 0;padding:12px 14px;background:#faf8f0;border:1px solid #e8e4dc;border-radius:8px;font-size:13px}\n");
    fprintf(out, ".heat-map-margins{font-size:13px;color:#444;margin:14px 0;line-height:1.5;max-width:900px}\n");
    fprintf(out, ".heat-map-margins p{margin:8px 0 0}\n");
    fprintf(out, "</style>\n</head>\n<body>\n<h1>Bucket summary</h1>\n");

    fprintf(out, "<div class=\"meta\">");
    if (all_users) {
        fprintf(out, "Scope: <strong>all crawled users</strong> (%" PRIu64 " distinct UIDs) | ", distinct_uids);
    } else {
        fprintf(out, "User: <strong>");
        html_escape(out, username);
        fprintf(out, "</strong> | ");
    }
    fprintf(out, "Basis: <strong>");
    html_escape(out, basis_str);
    fprintf(out, "</strong> | Age: <strong>");
    html_escape(out, age_bucket_names[ab]);
    fprintf(out, "</strong> | Size: <strong>");
    html_escape(out, size_bucket_names[sb]);
    fprintf(out, "</strong></div>\n");

    fprintf(out,
            "<p class=\"note\">Per-path drill-down tables were omitted because <strong>--bucket-details N</strong> was "
            "not used. The heat map on <code>index.html</code> uses full totals; only directory/path breakdowns inside "
            "each bucket are skipped. Re-run with <strong>--bucket-details</strong> (before other arguments) for full "
            "tables.</p>\n");

    {
        char cell_files[128];

        format_count_pretty_inline(sum->files[ab][sb], cell_files, sizeof(cell_files));
        fprintf(out,
                "<p>This age/size cell: <strong>%s</strong> total bytes in <strong>%s</strong> regular files (hard-link "
                "dedup).</p>\n",
                hb,
                cell_files);
    }
    emit_heat_map_margin_summary(out, sum, ab, sb);

    fprintf(out, "</body>\n</html>\n");
    if (counted_fclose(out) != 0) return -1;
    return 0;
}

typedef struct {
    const char *username;
    int all_users;
    uint64_t distinct_uids;
    const char *basis_str;
    int bucket_detail_levels;
    bucket_details_t (*details)[SIZE_BUCKETS];
    const matched_records_t *matched_records;
    size_t *matched_path_ord;
    uint64_t corpus_total_user_files;
    uint64_t corpus_total_user_bytes;
    const summary_t *sum_ref;
    const path_shape_view_t *path_shape;
    const dense_cell_map_t (*merged_dense_matrix)[SIZE_BUCKETS];
    const dense_cell_fanout_lookup_t *fanout_lookup_shape;
    const fanout_parent_stat_map_t *fanout_parent_stats;
    int stub_mode;
    atomic_size_t next_task;
    atomic_int any_fail;
    ereport_run_stats_t *run_stats;
    bucket_page_slice_t (*page_slices)[SIZE_BUCKETS];
    /* Cell dispatch order (indices into the 36 AGE×SIZE cells), heaviest first, to minimize the tail. */
    int order[AGE_BUCKETS * SIZE_BUCKETS];
} bucket_emit_ctx_t;

typedef struct {
    bucket_emit_ctx_t *ctx;
    bucket_page_scratch_t scratch; /* reused by every page this worker emits */
} bucket_emit_thread_arg_t;

typedef struct {
    const matched_records_t *mr;
    size_t lo, hi;
    uint64_t files;
    uint64_t bytes;
} corp_scan_worker_ctx_t;

static void *corp_scan_worker(void *vp) {
    corp_scan_worker_ctx_t *c = (corp_scan_worker_ctx_t *)vp;
    size_t i;

    c->files = 0;
    c->bytes = 0;
    for (i = c->lo; i < c->hi; i++) {
        if (c->mr->items[i].type == 'f') {
            c->files++;
            c->bytes += c->mr->items[i].size;
        }
    }
    return NULL;
}

/*
 * Sum regular-file count and byte total over matched_records (for bucket page corpus lines).
 */
static void corpus_file_byte_totals_parallel(const matched_records_t *mr,
                                             uint64_t *out_files,
                                             uint64_t *out_bytes,
                                             ereport_run_stats_t *run_rs) {
    size_t n;
    int nw;
    size_t per;
    int slot;
    int n_join;
    int j;
    corp_scan_worker_ctx_t *ctxs = NULL;
    pthread_t *tp = NULL;
    double tc0 = 0.0;

    *out_files = 0;
    *out_bytes = 0;
    if (!mr || mr->count == 0) return;
    if (run_rs && g_ereport_verbose) tc0 = now_sec();
    n = mr->count;

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if ((size_t)nw > n) nw = (int)n;

    if (nw <= 1 || n < 4096) {
        corp_scan_worker_ctx_t one;
        one.mr = mr;
        one.lo = 0;
        one.hi = n;
        corp_scan_worker(&one);
        *out_files = one.files;
        *out_bytes = one.bytes;
        if (run_rs && g_ereport_verbose && tc0 > 0.0) run_rs->vt_bucket_prep_corpus_sec += now_sec() - tc0;
        return;
    }

    ctxs = (corp_scan_worker_ctx_t *)calloc((size_t)nw, sizeof(*ctxs));
    tp = (pthread_t *)malloc((size_t)nw * sizeof(pthread_t));
    if (!ctxs || !tp) {
        free(ctxs);
        free(tp);
        corp_scan_worker_ctx_t one;
        one.mr = mr;
        one.lo = 0;
        one.hi = n;
        corp_scan_worker(&one);
        *out_files = one.files;
        *out_bytes = one.bytes;
        if (run_rs && g_ereport_verbose && tc0 > 0.0) run_rs->vt_bucket_prep_corpus_sec += now_sec() - tc0;
        return;
    }

    per = (n + (size_t)nw - 1) / (size_t)nw;
    slot = 0;
    n_join = 0;
    for (size_t loz = 0; loz < n; loz += per) {
        size_t hiz = loz + per;
        if (hiz > n) hiz = n;

        ctxs[slot].mr = mr;
        ctxs[slot].lo = loz;
        ctxs[slot].hi = hiz;
        ctxs[slot].files = 0;
        ctxs[slot].bytes = 0;
        if (pthread_create(&tp[n_join], NULL, corp_scan_worker, &ctxs[slot]) != 0)
            corp_scan_worker(&ctxs[slot]);
        else
            n_join++;
        slot++;
    }

    for (j = 0; j < n_join; j++) pthread_join(tp[j], NULL);

    for (j = 0; j < slot; j++) {
        *out_files += ctxs[j].files;
        *out_bytes += ctxs[j].bytes;
    }

    free(ctxs);
    free(tp);
    if (run_rs && g_ereport_verbose && tc0 > 0.0) run_rs->vt_bucket_prep_corpus_sec += now_sec() - tc0;
}

typedef struct {
    const matched_records_t *mr;
    size_t **out_ord;
    ereport_run_stats_t *run_rs;
} path_ord_async_ctx_t;

static void *path_ord_async_worker(void *vp) {
    path_ord_async_ctx_t *c = (path_ord_async_ctx_t *)vp;

    *c->out_ord = matched_records_build_path_order(c->mr, c->run_rs);
    return NULL;
}

typedef struct {
    int cell;
    uint64_t cost;
} bucket_cell_cost_t;

static int cmp_bucket_cell_cost_desc(const void *a, const void *b) {
    const bucket_cell_cost_t *xa = (const bucket_cell_cost_t *)a;
    const bucket_cell_cost_t *xb = (const bucket_cell_cost_t *)b;

    if (xa->cost > xb->cost) return -1;
    if (xa->cost < xb->cost) return 1;
    return (xa->cell > xb->cell) - (xa->cell < xb->cell); /* stable-ish: ascending cell index on ties */
}

/*
 * Order the 36 AGE×SIZE cells heaviest-first so the work-stealing pool starts the most expensive bucket
 * immediately; this shrinks the single-cell tail that otherwise leaves one thread finishing for a long time.
 * Cost is the per-cell matched-record slice span when page slices are available, else 0 (identity order).
 */
static void bucket_compute_cell_order(bucket_emit_ctx_t *ctx) {
    const int ntasks = AGE_BUCKETS * SIZE_BUCKETS;
    bucket_cell_cost_t cc[AGE_BUCKETS * SIZE_BUCKETS];
    int k;

    for (k = 0; k < ntasks; k++) {
        int ab = k / SIZE_BUCKETS;
        int sb = k % SIZE_BUCKETS;

        cc[k].cell = k;
        cc[k].cost = 0;
        if (ctx->page_slices) {
            const bucket_page_slice_t *sl = &ctx->page_slices[ab][sb];

            if (sl->ord_hi > sl->ord_lo) cc[k].cost = (uint64_t)(sl->ord_hi - sl->ord_lo);
        }
    }

    qsort(cc, (size_t)ntasks, sizeof(cc[0]), cmp_bucket_cell_cost_desc);
    for (k = 0; k < ntasks; k++) ctx->order[k] = cc[k].cell;
}

static void *bucket_page_emit_worker(void *arg) {
    bucket_emit_thread_arg_t *ta = (bucket_emit_thread_arg_t *)arg;
    bucket_emit_ctx_t *c = ta->ctx;
    const size_t ntasks = (size_t)AGE_BUCKETS * (size_t)SIZE_BUCKETS;

    for (;;) {
        size_t k = atomic_fetch_add_explicit(&c->next_task, 1U, memory_order_relaxed);
        int ab;
        int sb;
        char fn[PATH_MAX];
        int page_rc;

        if (k >= ntasks) break;

        {
            int cell = c->order[k];

            ab = cell / SIZE_BUCKETS;
            sb = cell % SIZE_BUCKETS;
        }

        if (build_bucket_page_path(fn, sizeof(fn), ab, sb) != 0) {
            atomic_store(&c->any_fail, 1);
            if (c->run_stats) atomic_fetch_add_explicit(&c->run_stats->finalize_bucket_done, 1U, memory_order_relaxed);
            continue;
        }

        if (c->stub_mode) {
            if (!c->sum_ref) {
                atomic_store(&c->any_fail, 1);
                if (c->run_stats) atomic_fetch_add_explicit(&c->run_stats->finalize_bucket_done, 1U, memory_order_relaxed);
                continue;
            }
            page_rc =
                emit_bucket_detail_stub_fast(fn, c->username, c->all_users, c->distinct_uids, c->basis_str, ab, sb, c->sum_ref);
        } else {
            page_rc =
                emit_bucket_detail_page(fn, c->username, c->all_users, c->distinct_uids, c->basis_str, ab, sb,
                                        c->bucket_detail_levels, &c->details[ab][sb], c->matched_records,
                                        c->matched_path_ord, c->corpus_total_user_files, c->corpus_total_user_bytes,
                                        c->sum_ref, c->path_shape, c->merged_dense_matrix, c->fanout_lookup_shape,
                                        c->fanout_parent_stats, c->run_stats,
                                        c->page_slices ? &c->page_slices[ab][sb] : NULL, &ta->scratch);
        }
        if (page_rc != 0) atomic_store(&c->any_fail, 1);
        if (c->run_stats) atomic_fetch_add_explicit(&c->run_stats->finalize_bucket_done, 1U, memory_order_relaxed);
    }

    return NULL;
}

static int emit_all_bucket_detail_pages(const char *username,
                                        int all_users,
                                        uint64_t distinct_uids,
                                        const char *basis_str,
                                        int bucket_detail_levels,
                                        const summary_t *sum_ref,
                                        const path_shape_view_t *path_shape,
                                        bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS],
                                        const matched_records_t *matched_records,
                                        ereport_run_stats_t *run_stats,
                                        const dense_cell_map_t (*merged_dense_matrix)[SIZE_BUCKETS],
                                        const dense_cell_fanout_lookup_t *fanout_lookup_shape,
                                        const fanout_parent_stat_map_t *fanout_parent_stats) {
    const size_t ntasks = (size_t)AGE_BUCKETS * (size_t)SIZE_BUCKETS;
    bucket_emit_ctx_t ctx;
    bucket_page_slice_t page_slices[AGE_BUCKETS][SIZE_BUCKETS];
    pthread_t *tids = NULL;
    bucket_emit_thread_arg_t *args = NULL;
    int nw;
    size_t ti;

    if (ensure_bucket_output_dir_exists() != 0) return -1;

    memset(page_slices, 0, sizeof(page_slices));
    memset(&ctx, 0, sizeof(ctx));
    ctx.username = username;
    ctx.all_users = all_users;
    ctx.distinct_uids = distinct_uids;
    ctx.basis_str = basis_str;
    ctx.bucket_detail_levels = bucket_detail_levels;
    ctx.details = details;
    ctx.matched_records = matched_records;
    ctx.matched_path_ord = NULL;
    ctx.corpus_total_user_files = 0;
    ctx.corpus_total_user_bytes = 0;
    ctx.sum_ref = sum_ref;
    ctx.path_shape = path_shape;
    ctx.merged_dense_matrix = merged_dense_matrix;
    ctx.fanout_lookup_shape = fanout_lookup_shape;
    ctx.fanout_parent_stats = fanout_parent_stats;
    ctx.stub_mode = (bucket_detail_levels == 0 && sum_ref != NULL) ? 1 : 0;
    ctx.run_stats = run_stats;
    ctx.page_slices = page_slices;

    if (run_stats) {
        atomic_store(&run_stats->finalize_merge_substep, 0);
        atomic_store(&run_stats->finalize_phase, 2);
        atomic_store(&run_stats->finalize_bucket_prep, 1);
        atomic_store(&run_stats->finalize_bucket_done, 0);
    }

    if (!ctx.stub_mode && matched_records && matched_records->count > 0) {
        pthread_t ord_th;
        path_ord_async_ctx_t pac;
        size_t *ord_out = NULL;
        double prep_t0 = 0.0;

        if (run_stats && g_ereport_verbose) prep_t0 = now_sec();

        pac.mr = matched_records;
        pac.out_ord = &ord_out;
        pac.run_rs = run_stats;
        if (pthread_create(&ord_th, NULL, path_ord_async_worker, &pac) != 0) {
            corpus_file_byte_totals_parallel(matched_records, &ctx.corpus_total_user_files,
                                             &ctx.corpus_total_user_bytes, run_stats);
            ctx.matched_path_ord = matched_records_build_path_order(matched_records, run_stats);
        } else {
            corpus_file_byte_totals_parallel(matched_records, &ctx.corpus_total_user_files,
                                             &ctx.corpus_total_user_bytes, run_stats);
            pthread_join(ord_th, NULL);
            ctx.matched_path_ord = ord_out;
        }

        if (run_stats && g_ereport_verbose && prep_t0 > 0.0)
            run_stats->vt_bucket_prep_wall_sec += now_sec() - prep_t0;
    }

    if (!ctx.stub_mode && matched_records && matched_records->count > 0 && ctx.matched_path_ord) {
        bucket_prepare_page_slices(details, matched_records, ctx.matched_path_ord, page_slices);
    }

    if (run_stats) atomic_store(&run_stats->finalize_bucket_prep, 0);

    bucket_compute_cell_order(&ctx);

    atomic_init(&ctx.next_task, 0);
    atomic_init(&ctx.any_fail, 0);

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if ((size_t)nw > ntasks) nw = (int)ntasks;

    /*
     * Cap how many cells run concurrently. Each running cell launches its own
     * internally-parallel aggregation, so emitting all cells at once with a large
     * thread count oversubscribes the machine early and leaves the slow-cell tail
     * with idle cores. Capping the outer pool (cells are dispatched largest-first)
     * hands the heaviest cells a real share of inner threads. An explicit
     * EREPORT_BUCKET_CELL_CONCURRENCY overrides; otherwise, when EREPORT_THREADS is
     * large relative to the cell count, give each concurrent cell ~16 inner threads.
     */
    {
        int cell_conc = parse_bucket_cell_concurrency();

        if (cell_conc <= 0) {
            int threads_now = parse_ereport_thread_count();
            if (threads_now >= 64) {
                cell_conc = threads_now / 16; /* e.g. 128 threads -> 8 concurrent cells */
                if (cell_conc < 4) cell_conc = 4;
            } else {
                cell_conc = nw; /* small machines: keep existing behavior */
            }
        }
        if (cell_conc < 1) cell_conc = 1;
        if (cell_conc < nw) nw = cell_conc;
    }

    tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
    args = (bucket_emit_thread_arg_t *)calloc((size_t)nw, sizeof(*args));
    if (!tids || !args) {
        fprintf(stderr, "ereport: allocation failed (bucket page emit pool)\n");
        free(tids);
        free(args);
        bucket_page_slices_free(page_slices);
        free(ctx.matched_path_ord);
        return -1;
    }

    for (ti = 0; ti < (size_t)nw; ti++) args[ti].ctx = &ctx;

    /*
     * The inner aggregation pools below each ask for the full EREPORT_THREADS, so nw concurrent
     * cells request nw × that many threads. Do not "fix" this by dividing the budget across cells:
     * measured on a 3.9 GiB medium tree at EREPORT_THREADS=32 on 64 CPUs, dividing cost 9% of wall
     * (median 3.29 s against 3.01 s over three reps each). Cells finish at very different times, and
     * oversubscription is what lets a cell still working use the cores the finished ones left.
     */
    {
        double cell_t0 = 0.0;

        if (run_stats && g_ereport_verbose) cell_t0 = now_sec();

        for (ti = 0; ti < (size_t)nw; ti++) {
            if (pthread_create(&tids[ti], NULL, bucket_page_emit_worker, &args[ti]) != 0) {
                size_t j;
                fprintf(stderr, "ereport: pthread_create failed (bucket page worker); retrying sequentially\n");
                for (j = 0; j < ti; j++) pthread_join(tids[j], NULL);
                free(tids);
                atomic_store(&ctx.next_task, 0);
                atomic_store(&ctx.any_fail, 0);
                bucket_page_emit_worker(&args[0]);
                for (j = 0; j < (size_t)nw; j++) bucket_page_scratch_free(&args[j].scratch);
                free(args);
                bucket_page_slices_free(page_slices);
                free(ctx.matched_path_ord);
                if (run_stats && g_ereport_verbose && cell_t0 > 0.0)
                    run_stats->vt_bucket_cells_wall_sec += now_sec() - cell_t0;
                return atomic_load(&ctx.any_fail) ? -1 : 0;
            }
        }

        for (ti = 0; ti < (size_t)nw; ti++) pthread_join(tids[ti], NULL);
        if (run_stats && g_ereport_verbose && cell_t0 > 0.0)
            run_stats->vt_bucket_cells_wall_sec += now_sec() - cell_t0;
    }
    free(tids);
    for (ti = 0; ti < (size_t)nw; ti++) bucket_page_scratch_free(&args[ti].scratch);
    free(args);
    bucket_page_slices_free(page_slices);
    free(ctx.matched_path_ord);
    return atomic_load(&ctx.any_fail) ? -1 : 0;
}

typedef struct {
    worker_arg_t *args;
    int lo, hi;
    summary_t *partial;
} summary_chunk_ctx_t;

static void *summary_chunk_worker(void *vp) {
    summary_chunk_ctx_t *c = (summary_chunk_ctx_t *)vp;
    int i;

    memset(c->partial, 0, sizeof(summary_t));
    for (i = c->lo; i < c->hi; i++) summary_merge(c->partial, &c->args[i].summary);
    return NULL;
}

/*
 * Fold per-parse-worker summaries into dst (parallel chunk reduce when beneficial).
 */
static void summary_reduce_from_worker_args(summary_t *dst, worker_arg_t *args, int threads_used) {
    int i;
    int nw;
    size_t per;
    int lo, hi;
    int slot;
    int n_join;
    int j;
    summary_t *partials = NULL;
    pthread_t *tp = NULL;
    summary_chunk_ctx_t *ctxs = NULL;

    memset(dst, 0, sizeof(*dst));
    if (!args || threads_used < 1) return;
    if (threads_used == 1) {
        summary_merge(dst, &args[0].summary);
        return;
    }

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if (nw > threads_used) nw = threads_used;
    while (nw > 1 && (threads_used + nw - 1) / nw < SUMMARY_REDUCE_MIN_CHUNK) nw /= 2;

    if (nw <= 1) {
        for (i = 0; i < threads_used; i++) summary_merge(dst, &args[i].summary);
        return;
    }

    partials = (summary_t *)calloc((size_t)nw, sizeof(summary_t));
    tp = (pthread_t *)malloc((size_t)nw * sizeof(pthread_t));
    ctxs = (summary_chunk_ctx_t *)malloc((size_t)nw * sizeof(summary_chunk_ctx_t));
    if (!partials || !tp || !ctxs) {
        free(partials);
        free(tp);
        free(ctxs);
        for (i = 0; i < threads_used; i++) summary_merge(dst, &args[i].summary);
        return;
    }

    per = (size_t)((threads_used + nw - 1) / nw);
    slot = 0;
    n_join = 0;
    for (lo = 0; lo < threads_used; lo = hi) {
        hi = lo + (int)per;
        if (hi > threads_used) hi = threads_used;

        ctxs[slot].args = args;
        ctxs[slot].lo = lo;
        ctxs[slot].hi = hi;
        ctxs[slot].partial = &partials[slot];

        if (pthread_create(&tp[n_join], NULL, summary_chunk_worker, &ctxs[slot]) != 0)
            summary_chunk_worker(&ctxs[slot]);
        else
            n_join++;
        slot++;
    }

    for (j = 0; j < n_join; j++) pthread_join(tp[j], NULL);

    for (j = 0; j < slot; j++) summary_merge(dst, &partials[j]);

    free(partials);
    free(tp);
    free(ctxs);
}

static void summary_merge(summary_t *dst, const summary_t *src) {
    int ab, sb;

    dst->total_bytes += src->total_bytes;
    dst->total_capacity_bytes += src->total_capacity_bytes;
    dst->total_files += src->total_files;
    dst->total_dirs += src->total_dirs;
    dst->total_links += src->total_links;
    dst->total_others += src->total_others;
    dst->total_other_bytes += src->total_other_bytes;
    dst->scanned_records += src->scanned_records;
    dst->matched_records += src->matched_records;
    dst->matched_files += src->matched_files;
    dst->matched_dirs += src->matched_dirs;
    dst->matched_links += src->matched_links;
    dst->matched_others += src->matched_others;
    dst->scanned_input_files += src->scanned_input_files;
    dst->bad_input_files += src->bad_input_files;

    dst->total_ctime_led_bytes += src->total_ctime_led_bytes;
    dst->total_ctime_led_files += src->total_ctime_led_files;
    dst->total_shape_deep_bytes += src->total_shape_deep_bytes;

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            dst->bytes[ab][sb] += src->bytes[ab][sb];
            dst->files[ab][sb] += src->files[ab][sb];
            dst->ctime_led_bytes[ab][sb] += src->ctime_led_bytes[ab][sb];
            dst->ctime_led_files[ab][sb] += src->ctime_led_files[ab][sb];
            dst->shape_deep_bytes[ab][sb] += src->shape_deep_bytes[ab][sb];
        }
    }
}

typedef struct {
    worker_arg_t *args;
    int threads_used;
    detail_record_t *dst_items;
    size_t *prefix;
    int ab;
    int sb;
    atomic_int next_ti;
} bd_fin_copy_ctx_t;

static void *bd_fin_copy_worker(void *vp) {
    bd_fin_copy_ctx_t *c = (bd_fin_copy_ctx_t *)vp;

    for (;;) {
        int ti = atomic_fetch_add_explicit(&c->next_ti, 1, memory_order_relaxed);
        if (ti >= c->threads_used) break;
        {
            size_t cnt = c->args[ti].details[c->ab][c->sb].count;
            if (cnt) {
                memcpy(c->dst_items + c->prefix[ti], c->args[ti].details[c->ab][c->sb].items,
                       cnt * sizeof(detail_record_t));
                free(c->args[ti].details[c->ab][c->sb].items);
                c->args[ti].details[c->ab][c->sb].items = NULL;
                c->args[ti].details[c->ab][c->sb].count = 0;
                c->args[ti].details[c->ab][c->sb].cap = 0;
            }
        }
    }
    return NULL;
}

/*
 * Build final per-cell bucket detail list in one allocation (moves records out of worker buffers).
 * Detail rows are copied from parse workers in parallel when possible.
 */
static int bucket_details_finalize_cell(worker_arg_t *args, int threads_used, bucket_details_t *dst, int ab, int sb) {
    size_t *prefix;
    size_t total;
    int ti;

    prefix = (size_t *)malloc((size_t)(threads_used + 1) * sizeof(size_t));
    if (!prefix) return -1;
    prefix[0] = 0;
    for (ti = 0; ti < threads_used; ti++) prefix[ti + 1] = prefix[ti] + args[ti].details[ab][sb].count;
    total = prefix[threads_used];
    if (total == 0) {
        free(prefix);
        dst->items = NULL;
        dst->count = 0;
        dst->cap = 0;
        return 0;
    }

    dst->items = (detail_record_t *)malloc(total * sizeof(detail_record_t));
    if (!dst->items) {
        free(prefix);
        return -1;
    }
    dst->count = total;
    dst->cap = total;

    if (threads_used > 1 && parse_ereport_thread_count() > 1) {
        int nwbc = parse_ereport_thread_count();
        pthread_t *tp = NULL;
        bd_fin_copy_ctx_t bctx;
        int j, started;

        if (nwbc > threads_used) nwbc = threads_used;
        atomic_init(&bctx.next_ti, 0);
        bctx.args = args;
        bctx.threads_used = threads_used;
        bctx.dst_items = dst->items;
        bctx.prefix = prefix;
        bctx.ab = ab;
        bctx.sb = sb;
        tp = (pthread_t *)malloc((size_t)nwbc * sizeof(pthread_t));
        if (tp) {
            started = 0;
            for (j = 0; j < nwbc; j++) {
                if (pthread_create(&tp[j], NULL, bd_fin_copy_worker, &bctx) != 0) break;
                started++;
            }
            if (started > 0) {
                for (j = 0; j < started; j++) pthread_join(tp[j], NULL);
            }
            if (started == 0) {
                for (ti = 0; ti < threads_used; ti++) {
                    size_t cnt = args[ti].details[ab][sb].count;
                    if (cnt) {
                        memcpy(dst->items + prefix[ti], args[ti].details[ab][sb].items, cnt * sizeof(detail_record_t));
                        free(args[ti].details[ab][sb].items);
                        args[ti].details[ab][sb].items = NULL;
                        args[ti].details[ab][sb].count = 0;
                        args[ti].details[ab][sb].cap = 0;
                    }
                }
            }
            free(tp);
        } else {
            for (ti = 0; ti < threads_used; ti++) {
                size_t cnt = args[ti].details[ab][sb].count;
                if (cnt) {
                    memcpy(dst->items + prefix[ti], args[ti].details[ab][sb].items, cnt * sizeof(detail_record_t));
                    free(args[ti].details[ab][sb].items);
                    args[ti].details[ab][sb].items = NULL;
                    args[ti].details[ab][sb].count = 0;
                    args[ti].details[ab][sb].cap = 0;
                }
            }
        }
    } else {
        for (ti = 0; ti < threads_used; ti++) {
            size_t cnt = args[ti].details[ab][sb].count;
            if (cnt) {
                memcpy(dst->items + prefix[ti], args[ti].details[ab][sb].items, cnt * sizeof(detail_record_t));
                free(args[ti].details[ab][sb].items);
                args[ti].details[ab][sb].items = NULL;
                args[ti].details[ab][sb].count = 0;
                args[ti].details[ab][sb].cap = 0;
            }
        }
    }
    free(prefix);
    return 0;
}

typedef struct {
    matched_record_t *dst;
    matched_record_t *src;
    size_t n;
} mr_fin_slice_t;

typedef struct {
    const mr_fin_slice_t *slices;
    int n_slices;
    atomic_int next;
    ereport_run_stats_t *run_rs;
} mr_fin_slice_ctx_t;

static void *mr_fin_slice_worker(void *vp) {
    mr_fin_slice_ctx_t *c = (mr_fin_slice_ctx_t *)vp;

    for (;;) {
        int s = atomic_fetch_add_explicit(&c->next, 1, memory_order_relaxed);
        if (s >= c->n_slices) break;
        {
            const mr_fin_slice_t *sl = &c->slices[s];
            if (sl->n) memcpy(sl->dst, sl->src, sl->n * sizeof(matched_record_t));
        }
        if (c->run_rs)
            atomic_fetch_add_explicit(&c->run_rs->finalize_matched_slices_done, 1, memory_order_relaxed);
    }
    return NULL;
}

/*
 * Single malloc + parallel memcpy into contiguous matched-record array.
 * Splits each parse-worker shard into MR_FIN_RECORDS_PER_TASK slices so imbalanced shards use all cores.
 */
static int matched_records_finalize_parallel(worker_arg_t *args,
                                             int threads_used,
                                             matched_records_t *dst,
                                             ereport_run_stats_t *run_rs) {
    size_t *prefix;
    size_t total;
    int nw, i, nw_started;
    int ns;
    int si;
    pthread_t *pool = NULL;
    mr_fin_slice_t *slices = NULL;
    mr_fin_slice_ctx_t sctx;

    if (run_rs) {
        atomic_store(&run_rs->finalize_matched_slices_total, 0);
        atomic_store(&run_rs->finalize_matched_slices_done, 0);
    }

    prefix = (size_t *)malloc((size_t)(threads_used + 1) * sizeof(size_t));
    if (!prefix) return -1;
    prefix[0] = 0;
    for (i = 0; i < threads_used; i++) prefix[i + 1] = prefix[i] + args[i].matched_records.count;
    total = prefix[threads_used];

    dst->items = total ? (matched_record_t *)malloc(total * sizeof(matched_record_t)) : NULL;
    if (total && !dst->items) {
        free(prefix);
        return -1;
    }
    dst->count = total;
    dst->cap = total;

    ns = 0;
    for (i = 0; i < threads_used; i++) {
        size_t c = args[i].matched_records.count;
        size_t off;
        for (off = 0; off < c; off += MR_FIN_RECORDS_PER_TASK) ns++;
    }

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;

    if (nw <= 1 || threads_used <= 1 || ns <= 1 || total == 0) {
        for (i = 0; i < threads_used; i++) {
            size_t cnt = args[i].matched_records.count;
            if (cnt) {
                memcpy(dst->items + prefix[i], args[i].matched_records.items, cnt * sizeof(matched_record_t));
                free(args[i].matched_records.items);
                args[i].matched_records.items = NULL;
                args[i].matched_records.count = 0;
                args[i].matched_records.cap = 0;
            }
        }
        free(prefix);
        return 0;
    }

    slices = (mr_fin_slice_t *)malloc((size_t)ns * sizeof(*slices));
    if (!slices) {
        for (i = 0; i < threads_used; i++) {
            size_t cnt = args[i].matched_records.count;
            if (cnt) {
                memcpy(dst->items + prefix[i], args[i].matched_records.items, cnt * sizeof(matched_record_t));
                free(args[i].matched_records.items);
                args[i].matched_records.items = NULL;
                args[i].matched_records.count = 0;
                args[i].matched_records.cap = 0;
            }
        }
        free(prefix);
        return 0;
    }

    si = 0;
    for (i = 0; i < threads_used; i++) {
        matched_record_t *srcbase = args[i].matched_records.items;
        size_t c = args[i].matched_records.count;
        size_t base = prefix[i];
        size_t off;
        for (off = 0; off < c; off += MR_FIN_RECORDS_PER_TASK) {
            size_t len = c - off;
            if (len > MR_FIN_RECORDS_PER_TASK) len = MR_FIN_RECORDS_PER_TASK;
            slices[si].dst = dst->items + base + off;
            slices[si].src = srcbase + off;
            slices[si].n = len;
            si++;
        }
    }

    if (nw > ns) nw = ns;

    pool = (pthread_t *)malloc((size_t)nw * sizeof(pthread_t));
    if (!pool) {
        for (si = 0; si < ns; si++) {
            if (slices[si].n) memcpy(slices[si].dst, slices[si].src, slices[si].n * sizeof(matched_record_t));
        }
        free(slices);
        for (i = 0; i < threads_used; i++) {
            free(args[i].matched_records.items);
            args[i].matched_records.items = NULL;
            args[i].matched_records.count = 0;
            args[i].matched_records.cap = 0;
        }
        free(prefix);
        return 0;
    }

    sctx.slices = slices;
    sctx.n_slices = ns;
    atomic_init(&sctx.next, 0);
    sctx.run_rs = run_rs;

    if (run_rs) {
        atomic_store(&run_rs->finalize_matched_slices_total, ns);
        atomic_store(&run_rs->finalize_matched_slices_done, 0);
    }

    nw_started = 0;
    for (i = 0; i < nw; i++) {
        if (pthread_create(&pool[i], NULL, mr_fin_slice_worker, &sctx) != 0) break;
        nw_started++;
    }

    if (nw_started > 0) {
        for (i = 0; i < nw_started; i++) pthread_join(pool[i], NULL);
    } else {
        for (si = 0; si < ns; si++) {
            if (slices[si].n) memcpy(slices[si].dst, slices[si].src, slices[si].n * sizeof(matched_record_t));
        }
        if (run_rs) {
            atomic_store(&run_rs->finalize_matched_slices_total, 0);
            atomic_store(&run_rs->finalize_matched_slices_done, 0);
        }
    }

    free(pool);
    free(slices);

    for (i = 0; i < threads_used; i++) {
        free(args[i].matched_records.items);
        args[i].matched_records.items = NULL;
        args[i].matched_records.count = 0;
        args[i].matched_records.cap = 0;
    }
    free(prefix);
    if (run_rs) {
        atomic_store(&run_rs->finalize_matched_slices_total, 0);
        atomic_store(&run_rs->finalize_matched_slices_done, 0);
    }
    return 0;
}

/*
 * Parallel pairwise merge of dense_cell_map_t buffers (same invariants as fanout_pair_merge_worker).
 */
typedef struct {
    dense_cell_map_t *dbuf;
    atomic_int *next_pair;
    int pairs;
    int merge_budget;
} dense_pair_merge_ctx_t;

static void *dense_pair_merge_worker(void *vp) {
    dense_pair_merge_ctx_t *c = (dense_pair_merge_ctx_t *)vp;

    for (;;) {
        int p = atomic_fetch_add_explicit(c->next_pair, 1, memory_order_relaxed);
        if (p >= c->pairs) break;
        dense_cell_merge_into_ex(&c->dbuf[2 * p], &c->dbuf[2 * p + 1], NULL, c->merge_budget);
    }
    return NULL;
}

/*
 * Merge parse-worker dense maps for one age×size cell: parallel pairwise tournament when many workers,
 * parallel inner merges (dense_cell_merge_into_ex); final fold into dst uses all threads.
 */
static void dense_cell_reduce_workers_into(dense_cell_map_t *dst,
                                           worker_arg_t *args,
                                           int threads_used,
                                           int ab,
                                           int sb) {
    dense_cell_map_t *buf = NULL;
    int n;
    int ti;
    int pairs;
    int j;
    int k;
    int nw;
    int n_join;
    int conc;
    int merge_budget;
    pthread_t *tids = NULL;
    dense_pair_merge_ctx_t *targs = NULL;
    atomic_int next_pair;

    if (!dst || !args || threads_used < 1) return;

    if (threads_used == 1) {
        dense_cell_merge_into_ex(dst, &args[0].dense_maps[ab][sb], NULL, parse_ereport_thread_count());
        return;
    }

    buf = (dense_cell_map_t *)calloc((size_t)threads_used, sizeof(*buf));
    if (!buf) {
        for (ti = 0; ti < threads_used; ti++)
            dense_cell_merge_into_ex(dst, &args[ti].dense_maps[ab][sb], NULL, parse_ereport_thread_count());
        return;
    }

    for (ti = 0; ti < threads_used; ti++) {
        buf[ti] = args[ti].dense_maps[ab][sb];
        memset(&args[ti].dense_maps[ab][sb], 0, sizeof(args[ti].dense_maps[ab][sb]));
    }

    n = threads_used;
    while (n > 1) {
        pairs = n / 2;
        if (pairs <= 0) break;

        nw = parse_ereport_thread_count();
        if (nw < 1) nw = 1;
        if (nw > pairs) nw = pairs;

        if (pairs < 2 || nw < 2) {
            for (j = 0; j < pairs; j++)
                dense_cell_merge_into_ex(&buf[2 * j], &buf[2 * j + 1], NULL, parse_ereport_thread_count());
        } else {
            atomic_store_explicit(&next_pair, 0, memory_order_relaxed);
            tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
            targs = (dense_pair_merge_ctx_t *)calloc((size_t)nw, sizeof(*targs));
            if (!tids || !targs) {
                free(tids);
                free(targs);
                tids = NULL;
                targs = NULL;
                for (j = 0; j < pairs; j++)
                    dense_cell_merge_into_ex(&buf[2 * j], &buf[2 * j + 1], NULL, parse_ereport_thread_count());
            } else {
                conc = pairs < nw ? pairs : nw;
                merge_budget = parse_ereport_thread_count();
                if (merge_budget < 1) merge_budget = 1;
                merge_budget = (merge_budget + conc - 1) / conc;
                if (merge_budget < 1) merge_budget = 1;
                n_join = 0;
                for (ti = 0; ti < nw; ti++) {
                    targs[ti].dbuf = buf;
                    targs[ti].next_pair = &next_pair;
                    targs[ti].pairs = pairs;
                    targs[ti].merge_budget = merge_budget;
                    if (pthread_create(&tids[ti], NULL, dense_pair_merge_worker, &targs[ti]) != 0) break;
                    n_join++;
                }
                for (ti = 0; ti < n_join; ti++) pthread_join(tids[ti], NULL);
                for (;;) {
                    int p = atomic_fetch_add_explicit(&next_pair, 1, memory_order_relaxed);
                    if (p >= pairs) break;
                    dense_cell_merge_into_ex(&buf[2 * p], &buf[2 * p + 1], NULL, merge_budget);
                }
                free(tids);
                free(targs);
                tids = NULL;
                targs = NULL;
            }
        }

        k = 0;
        for (j = 0; j < pairs; j++) {
            buf[k] = buf[2 * j];
            k++;
        }
        if (n % 2 == 1) {
            buf[k] = buf[n - 1];
            k++;
        }
        n = k;
    }

    dense_cell_merge_into_ex(dst, &buf[0], NULL, parse_ereport_thread_count());
    memset(&buf[0], 0, sizeof(buf[0]));
    free(buf);
}

/*
 * Parallel tournament merge of per-parse-worker parent_fanout maps into one (disjoint dst per pair).
 * Each thread merges buf[2p+1] into buf[2p] for distinct p — safe (disjoint dst per pair; see dense_cell_merge_into_ex).
 */
typedef struct {
    dense_cell_map_t *dbuf;
    fanout_parent_stat_map_t *sbuf;
    atomic_int *next_pair;
    int pairs;
    int merge_budget;
} fanout_pair_merge_ctx_t;

static void *fanout_pair_merge_worker(void *vp) {
    fanout_pair_merge_ctx_t *c = (fanout_pair_merge_ctx_t *)vp;

    for (;;) {
        int p = atomic_fetch_add_explicit(c->next_pair, 1, memory_order_relaxed);
        if (p >= c->pairs) break;
        dense_cell_merge_into_ex(&c->dbuf[2 * p], &c->dbuf[2 * p + 1], NULL, c->merge_budget);
        fanout_parent_stat_merge_into_ex(&c->sbuf[2 * p], &c->sbuf[2 * p + 1], NULL, c->merge_budget);
    }
    return NULL;
}

/*
 * Fold args[0..threads_used).parent_fanout (+ stats) into merged_* using parallel pairwise rounds when beneficial.
 * On failure returns -1 (caller falls back to sequential merge from args — args unchanged on -1).
 */
static int fanout_shard_summaries_reduce_parallel(dense_cell_map_t *merged_fanout,
                                                  fanout_parent_stat_map_t *merged_fanout_stats,
                                                  worker_arg_t *args,
                                                  int threads_used,
                                                  ereport_run_stats_t *run_rs) {
    dense_cell_map_t *dbuf = NULL;
    fanout_parent_stat_map_t *sbuf = NULL;
    int n;
    int i;
    int pairs;
    int j;
    int k;
    int nw;
    int ti;
    int n_join;
    int conc;
    int merge_budget;
    pthread_t *tids = NULL;
    fanout_pair_merge_ctx_t *targs = NULL;
    atomic_int next_pair;

    if (!merged_fanout || !merged_fanout_stats || !args || threads_used < 1) return 0;

    if (threads_used == 1) {
        dense_cell_merge_into_ex(merged_fanout, &args[0].parent_fanout, NULL, parse_ereport_thread_count());
        fanout_parent_stat_merge_into_ex(merged_fanout_stats, &args[0].parent_fanout_stats, NULL, parse_ereport_thread_count());
        memset(&args[0].parent_fanout, 0, sizeof(args[0].parent_fanout));
        memset(&args[0].parent_fanout_stats, 0, sizeof(args[0].parent_fanout_stats));
        if (run_rs) atomic_store(&run_rs->finalize_fanout_workers_done, 1);
        return 0;
    }

    dbuf = (dense_cell_map_t *)calloc((size_t)threads_used, sizeof(*dbuf));
    sbuf = (fanout_parent_stat_map_t *)calloc((size_t)threads_used, sizeof(*sbuf));
    if (!dbuf || !sbuf) {
        free(dbuf);
        free(sbuf);
        return -1;
    }

    for (i = 0; i < threads_used; i++) {
        dbuf[i] = args[i].parent_fanout;
        sbuf[i] = args[i].parent_fanout_stats;
        memset(&args[i].parent_fanout, 0, sizeof(args[i].parent_fanout));
        memset(&args[i].parent_fanout_stats, 0, sizeof(args[i].parent_fanout_stats));
    }

    n = threads_used;
    while (n > 1) {
        pairs = n / 2;
        if (pairs <= 0) break;

        nw = parse_ereport_thread_count();
        if (nw < 1) nw = 1;
        if (nw > pairs) nw = pairs;

        if (pairs < 2 || nw < 2) {
            /* pairs==1 (2 survivors → 1): still merge one huge map; use parallel dense merge, not single-threaded. */
            for (j = 0; j < pairs; j++) {
                dense_cell_merge_into_ex(&dbuf[2 * j], &dbuf[2 * j + 1], NULL, parse_ereport_thread_count());
                fanout_parent_stat_merge_into_ex(&sbuf[2 * j], &sbuf[2 * j + 1], NULL, parse_ereport_thread_count());
            }
        } else {
            atomic_store_explicit(&next_pair, 0, memory_order_relaxed);
            tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
            targs = (fanout_pair_merge_ctx_t *)calloc((size_t)nw, sizeof(*targs));
            if (!tids || !targs) {
                free(tids);
                free(targs);
                tids = NULL;
                targs = NULL;
                for (j = 0; j < pairs; j++) {
                    dense_cell_merge_into_ex(&dbuf[2 * j], &dbuf[2 * j + 1], NULL, parse_ereport_thread_count());
                    fanout_parent_stat_merge_into_ex(&sbuf[2 * j], &sbuf[2 * j + 1], NULL, parse_ereport_thread_count());
                }
            } else {
                conc = pairs < nw ? pairs : nw;
                merge_budget = parse_ereport_thread_count();
                if (merge_budget < 1) merge_budget = 1;
                merge_budget = (merge_budget + conc - 1) / conc;
                if (merge_budget < 1) merge_budget = 1;
                n_join = 0;
                for (ti = 0; ti < nw; ti++) {
                    targs[ti].dbuf = dbuf;
                    targs[ti].sbuf = sbuf;
                    targs[ti].next_pair = &next_pair;
                    targs[ti].pairs = pairs;
                    targs[ti].merge_budget = merge_budget;
                    if (pthread_create(&tids[ti], NULL, fanout_pair_merge_worker, &targs[ti]) != 0) break;
                    n_join++;
                }
                for (ti = 0; ti < n_join; ti++) pthread_join(tids[ti], NULL);
                for (;;) {
                    int p = atomic_fetch_add_explicit(&next_pair, 1, memory_order_relaxed);
                    if (p >= pairs) break;
                    dense_cell_merge_into_ex(&dbuf[2 * p], &dbuf[2 * p + 1], NULL, merge_budget);
                    fanout_parent_stat_merge_into_ex(&sbuf[2 * p], &sbuf[2 * p + 1], NULL, merge_budget);
                }
                free(tids);
                free(targs);
                tids = NULL;
                targs = NULL;
            }
        }

        k = 0;
        for (j = 0; j < pairs; j++) {
            dbuf[k] = dbuf[2 * j];
            sbuf[k] = sbuf[2 * j];
            k++;
        }
        if (n % 2 == 1) {
            dbuf[k] = dbuf[n - 1];
            sbuf[k] = sbuf[n - 1];
            k++;
        }
        n = k;
        if (run_rs) atomic_store(&run_rs->finalize_fanout_workers_done, threads_used - n);
    }

    dense_cell_merge_into_ex(merged_fanout, &dbuf[0], NULL, parse_ereport_thread_count());
    fanout_parent_stat_merge_into_ex(merged_fanout_stats, &sbuf[0], NULL, parse_ereport_thread_count());
    if (run_rs) atomic_store(&run_rs->finalize_fanout_workers_done, threads_used);

    memset(&dbuf[0], 0, sizeof(dbuf[0]));
    memset(&sbuf[0], 0, sizeof(sbuf[0]));
    free(dbuf);
    free(sbuf);
    return 0;
}

typedef struct {
    worker_arg_t *args;
    int threads_used;
    bucket_details_t (*final_details)[SIZE_BUCKETS];
    dense_cell_map_t (*merged_dense)[SIZE_BUCKETS];
    atomic_int next_cell;
    atomic_int fail;
    ereport_run_stats_t *run_stats;
} cell_fin_ctx_t;

static void *cell_fin_worker(void *vp) {
    cell_fin_ctx_t *c = (cell_fin_ctx_t *)vp;

    for (;;) {
        int task = atomic_fetch_add_explicit(&c->next_cell, 1, memory_order_relaxed);
        int ab, sb;

        if (task >= AGE_BUCKETS * SIZE_BUCKETS) break;

        if (atomic_load_explicit(&c->fail, memory_order_acquire)) continue;

        ab = task / SIZE_BUCKETS;
        sb = task % SIZE_BUCKETS;

        if (bucket_details_finalize_cell(c->args, c->threads_used, &c->final_details[ab][sb], ab, sb) != 0) {
            atomic_store_explicit(&c->fail, 1, memory_order_release);
            continue;
        }

        dense_cell_reduce_workers_into(&c->merged_dense[ab][sb], c->args, c->threads_used, ab, sb);
        if (c->run_stats)
            atomic_fetch_add_explicit(&c->run_stats->finalize_dense_cells_done, 1U, memory_order_relaxed);
    }
    return NULL;
}

/*
 * Per age×size cell: finalize bucket details + merge dense parent maps. Cells are independent (parallel).
 */
static int bucket_dense_cells_finalize_parallel(worker_arg_t *args,
                                                int threads_used,
                                                bucket_details_t final_details[AGE_BUCKETS][SIZE_BUCKETS],
                                                dense_cell_map_t merged_dense[AGE_BUCKETS][SIZE_BUCKETS],
                                                ereport_run_stats_t *run_rs) {
    const int ncells = AGE_BUCKETS * SIZE_BUCKETS;
    int nw, i, nw_started, task, ab, sb;
    pthread_t *pool = NULL;
    cell_fin_ctx_t ctx;

    if (run_rs) {
        atomic_store(&run_rs->finalize_fanout_workers_total, 0);
        atomic_store(&run_rs->finalize_fanout_workers_done, 0);
        atomic_store(&run_rs->finalize_dense_cells_done, 0U);
    }

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if (nw > ncells) nw = ncells;

    ctx.args = args;
    ctx.threads_used = threads_used;
    ctx.final_details = final_details;
    ctx.merged_dense = merged_dense;
    ctx.run_stats = run_rs;
    atomic_init(&ctx.fail, 0);
    atomic_init(&ctx.next_cell, 0);

    if (nw <= 1) {
        for (task = 0; task < ncells; task++) {
            ab = task / SIZE_BUCKETS;
            sb = task % SIZE_BUCKETS;
            if (bucket_details_finalize_cell(args, threads_used, &final_details[ab][sb], ab, sb) != 0) return -1;
            dense_cell_reduce_workers_into(&merged_dense[ab][sb], args, threads_used, ab, sb);
            if (run_rs) atomic_fetch_add_explicit(&run_rs->finalize_dense_cells_done, 1U, memory_order_relaxed);
        }
        return 0;
    }

    pool = (pthread_t *)malloc((size_t)nw * sizeof(pthread_t));
    if (!pool) {
        for (task = 0; task < ncells; task++) {
            ab = task / SIZE_BUCKETS;
            sb = task % SIZE_BUCKETS;
            if (bucket_details_finalize_cell(args, threads_used, &final_details[ab][sb], ab, sb) != 0) return -1;
            dense_cell_reduce_workers_into(&merged_dense[ab][sb], args, threads_used, ab, sb);
            if (run_rs) atomic_fetch_add_explicit(&run_rs->finalize_dense_cells_done, 1U, memory_order_relaxed);
        }
        return 0;
    }

    nw_started = 0;
    for (i = 0; i < nw; i++) {
        if (pthread_create(&pool[i], NULL, cell_fin_worker, &ctx) != 0) break;
        nw_started++;
    }
    if (nw_started > 0) {
        for (i = 0; i < nw_started; i++) pthread_join(pool[i], NULL);
    }
    if (nw_started == 0) {
        for (task = 0; task < ncells; task++) {
            ab = task / SIZE_BUCKETS;
            sb = task % SIZE_BUCKETS;
            if (bucket_details_finalize_cell(args, threads_used, &final_details[ab][sb], ab, sb) != 0) {
                free(pool);
                return -1;
            }
            dense_cell_reduce_workers_into(&merged_dense[ab][sb], args, threads_used, ab, sb);
            if (run_rs) atomic_fetch_add_explicit(&run_rs->finalize_dense_cells_done, 1U, memory_order_relaxed);
        }
        free(pool);
        return 0;
    }

    free(pool);
    return atomic_load_explicit(&ctx.fail, memory_order_acquire) ? -1 : 0;
}

typedef struct {
    const summary_t *sum;
    dense_cell_map_t (*merged_dense)[SIZE_BUCKETS];
    const dense_cell_fanout_lookup_t *fanout_lookup;
    path_shape_view_t *path_shape;
    atomic_int next_cell;
} ps_cell_ctx_t;

static void *ps_cell_worker(void *vp) {
    ps_cell_ctx_t *c = (ps_cell_ctx_t *)vp;
    const int ncells = AGE_BUCKETS * SIZE_BUCKETS;

    for (;;) {
        int t = atomic_fetch_add_explicit(&c->next_cell, 1, memory_order_relaxed);
        int ab;
        int sb;

        if (t >= ncells) break;

        ab = t / SIZE_BUCKETS;
        sb = t % SIZE_BUCKETS;
        c->path_shape->cell[ab][sb].deep_bytes = c->sum->shape_deep_bytes[ab][sb];
        c->path_shape->cell[ab][sb].dense_fanout_max =
            dense_cell_max_fanout_among_parents(&c->merged_dense[ab][sb], c->fanout_lookup);
    }
    return NULL;
}

typedef struct {
    const summary_t *sum;
    dense_cell_map_t (*merged_dense)[SIZE_BUCKETS];
    const dense_cell_fanout_lookup_t *fanout_lookup;
    path_shape_view_t *path_shape;
    dense_cell_map_t *row_merged;
    atomic_int next_task;
} shape_margin_ctx_t;

/*
 * Merge n source maps into rm. With per-map dynamic bucket counts a parent no longer hashes to the
 * same bucket index across maps, so the old bucket-strip-parallel merge is invalid; re-insert by hash
 * instead. This runs once per row/column strip post-join and the enclosing shape_margin worker pool
 * already parallelizes across the AGE_BUCKETS+SIZE_BUCKETS strips, so each strip merging serially is fine.
 */
static void shape_margin_merge_strip(dense_cell_map_t *rm, const dense_cell_map_t *const *srcv, int n) {
    int k;

    if (!rm || !srcv || n < 1) return;
    for (k = 0; k < n; k++) dense_cell_merge_add(rm, srcv[k], NULL);
}

static void shape_margin_merge_bucket_strip_parallel(dense_cell_map_t *rm,
                                                     dense_cell_map_t (*merged_dense)[SIZE_BUCKETS],
                                                     int strip_idx,
                                                     int row_strip) {
    const dense_cell_map_t *srcv[SIZE_BUCKETS];
    int k;

    for (k = 0; k < SIZE_BUCKETS; k++)
        srcv[k] = row_strip ? &merged_dense[strip_idx][k] : &merged_dense[k][strip_idx];
    shape_margin_merge_strip(rm, srcv, SIZE_BUCKETS);
}

static void shape_margin_run_task(shape_margin_ctx_t *c, int t) {
    if (t < AGE_BUCKETS) {
        int ab = t;
        uint64_t db = 0;
        int sb;
        dense_cell_map_t *rm = &c->row_merged[ab];

        for (sb = 0; sb < SIZE_BUCKETS; sb++) db += c->sum->shape_deep_bytes[ab][sb];

        shape_margin_merge_bucket_strip_parallel(rm, c->merged_dense, ab, 1);
        c->path_shape->row[ab].deep_bytes = db;
        c->path_shape->row[ab].dense_fanout_max = dense_cell_max_fanout_among_parents(rm, c->fanout_lookup);
    } else {
        int sb = t - AGE_BUCKETS;
        dense_cell_map_t col_acc;
        uint64_t db = 0;
        int ab;

        memset(&col_acc, 0, sizeof(col_acc));
        for (ab = 0; ab < AGE_BUCKETS; ab++) db += c->sum->shape_deep_bytes[ab][sb];

        shape_margin_merge_bucket_strip_parallel(&col_acc, c->merged_dense, sb, 0);
        c->path_shape->col[sb].deep_bytes = db;
        c->path_shape->col[sb].dense_fanout_max =
            dense_cell_max_fanout_among_parents(&col_acc, c->fanout_lookup);
        dense_cell_free(&col_acc);
    }
}

static void *shape_margin_worker(void *vp) {
    shape_margin_ctx_t *c = (shape_margin_ctx_t *)vp;
    const int ntasks = AGE_BUCKETS + SIZE_BUCKETS;

    for (;;) {
        int t = atomic_fetch_add_explicit(&c->next_task, 1, memory_order_relaxed);
        if (t >= ntasks) break;
        shape_margin_run_task(c, t);
    }
    return NULL;
}

/*
 * Heat-map Deep from summary bytes; Dense megadir signal from crawl-wide fanout lookup vs per-cell maps.
 */
static void path_shape_fill_from_merged_dense(const summary_t *sum,
                                              dense_cell_map_t merged_dense[AGE_BUCKETS][SIZE_BUCKETS],
                                              const dense_cell_fanout_lookup_t *fanout_lookup,
                                              path_shape_view_t *path_shape,
                                              ereport_run_stats_t *run_rs) {
    dense_cell_map_t row_merged[AGE_BUCKETS];
    shape_margin_ctx_t ctx;
    ps_cell_ctx_t ps_ctx;
    int nw, i, nw_started, t, ab;
    pthread_t *pool = NULL;
    pthread_t *ps_pool = NULL;
    const int ntasks = AGE_BUCKETS + SIZE_BUCKETS;
    const int ncells = AGE_BUCKETS * SIZE_BUCKETS;

    ps_ctx.sum = sum;
    ps_ctx.merged_dense = merged_dense;
    ps_ctx.fanout_lookup = fanout_lookup;
    ps_ctx.path_shape = path_shape;
    atomic_init(&ps_ctx.next_cell, 0);

    if (run_rs) atomic_store(&run_rs->finalize_lookup_stage, 2U);

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if (nw > ncells) nw = ncells;

    if (nw <= 1) {
        int ab, sb;
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            for (sb = 0; sb < SIZE_BUCKETS; sb++) {
                path_shape->cell[ab][sb].deep_bytes = sum->shape_deep_bytes[ab][sb];
                path_shape->cell[ab][sb].dense_fanout_max =
                    dense_cell_max_fanout_among_parents(&merged_dense[ab][sb], fanout_lookup);
            }
        }
    } else {
        ps_pool = (pthread_t *)malloc((size_t)nw * sizeof(pthread_t));
        if (!ps_pool) {
            int ab, sb;
            for (ab = 0; ab < AGE_BUCKETS; ab++) {
                for (sb = 0; sb < SIZE_BUCKETS; sb++) {
                    path_shape->cell[ab][sb].deep_bytes = sum->shape_deep_bytes[ab][sb];
                    path_shape->cell[ab][sb].dense_fanout_max =
                        dense_cell_max_fanout_among_parents(&merged_dense[ab][sb], fanout_lookup);
                }
            }
        } else {
            nw_started = 0;
            for (i = 0; i < nw; i++) {
                if (pthread_create(&ps_pool[i], NULL, ps_cell_worker, &ps_ctx) != 0) break;
                nw_started++;
            }
            if (nw_started > 0) {
                for (i = 0; i < nw_started; i++) pthread_join(ps_pool[i], NULL);
            }
            if (nw_started == 0) {
                int ab, sb;
                for (ab = 0; ab < AGE_BUCKETS; ab++) {
                    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
                        path_shape->cell[ab][sb].deep_bytes = sum->shape_deep_bytes[ab][sb];
                        path_shape->cell[ab][sb].dense_fanout_max =
                            dense_cell_max_fanout_among_parents(&merged_dense[ab][sb], fanout_lookup);
                    }
                }
            }
            free(ps_pool);
        }
    }

    if (run_rs) atomic_store(&run_rs->finalize_lookup_stage, 3U);

    memset(row_merged, 0, sizeof(row_merged));

    ctx.sum = sum;
    ctx.merged_dense = merged_dense;
    ctx.fanout_lookup = fanout_lookup;
    ctx.path_shape = path_shape;
    ctx.row_merged = row_merged;
    atomic_init(&ctx.next_task, 0);

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if (nw > ntasks) nw = ntasks;

    if (nw <= 1) {
        for (t = 0; t < ntasks; t++) shape_margin_run_task(&ctx, t);
    } else {
        pool = (pthread_t *)malloc((size_t)nw * sizeof(pthread_t));
        if (!pool) {
            for (t = 0; t < ntasks; t++) shape_margin_run_task(&ctx, t);
        } else {
            nw_started = 0;
            for (i = 0; i < nw; i++) {
                if (pthread_create(&pool[i], NULL, shape_margin_worker, &ctx) != 0) break;
                nw_started++;
            }
            if (nw_started > 0) {
                for (i = 0; i < nw_started; i++) pthread_join(pool[i], NULL);
            }
            if (nw_started == 0) {
                for (t = 0; t < ntasks; t++) shape_margin_run_task(&ctx, t);
            }
            free(pool);
        }
    }

    /*
     * Overall dense-fanout badge: max global child-count among parents that appear in any heat-map cell.
     * Each row aggregate already scanned all parents in that age strip across size buckets; taking the max
     * over rows (and columns, same set) avoids merging six huge maps then re-walking them with fanout lookups
     * (O(nodes * chain) in the 65k table), which could stall huge crawls for tens of minutes.
     */
    path_shape->all.deep_bytes = sum->total_shape_deep_bytes;
    {
        uint64_t all_mx = 0;

        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            if (path_shape->row[ab].dense_fanout_max > all_mx) all_mx = path_shape->row[ab].dense_fanout_max;
        }
        for (ab = 0; ab < SIZE_BUCKETS; ab++) {
            if (path_shape->col[ab].dense_fanout_max > all_mx) all_mx = path_shape->col[ab].dense_fanout_max;
        }
        path_shape->all.dense_fanout_max = all_mx;
    }
    for (ab = 0; ab < AGE_BUCKETS; ab++) dense_cell_free(&row_merged[ab]);
    if (run_rs) atomic_store(&run_rs->finalize_lookup_stage, 0U);
}

static file_chunk_t *queue_pop(work_queue_t *q) {
    file_chunk_t *chunk = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->next_index < q->count) chunk = &q->chunks[q->next_index++];
    pthread_mutex_unlock(&q->mutex);

    return chunk;
}

static int finalize_chunk_file_progress(file_state_t *file_states,
                                        size_t file_index,
                                        progress_local_t *progress) {
    unsigned int old_remaining;

    if (!file_states || !progress) return 0;
    old_remaining = atomic_fetch_sub(&file_states[file_index].remaining_chunks, 1U);
    if (old_remaining == 1U) progress->scanned_input_files++;
    return 0;
}

static void ereport_free_file_states(file_state_t *fs, size_t n) {
    size_t i;

    if (!fs) return;
    for (i = 0; i < n; i++) {
        if (fs[i].catalog) {
            crawl_bin_catalog_free(fs[i].catalog);
            free(fs[i].catalog);
            fs[i].catalog = NULL;
        }
        free(fs[i].subtree_bits);
        fs[i].subtree_bits = NULL;
    }
    free(fs);
}

/* Free only the shard catalogs (keep the file_state_t array). The catalogs back path
 * reconstruction during the read phase; nothing in finalize/emission touches them, so freeing
 * them right after the workers join returns their footprint (per-dir arrays + name strdups,
 * tens of GiB on a large crawl) before the finalize merges and the matched/details gather
 * allocate. Idempotent: catalogs are NULLed, so the later ereport_free_file_states is a no-op
 * for them and still frees the array exactly once. */
static void ereport_free_shard_catalogs(file_state_t *fs, size_t n) {
    size_t i;

    if (!fs) return;
    for (i = 0; i < n; i++) {
        if (fs[i].catalog) {
            crawl_bin_catalog_free(fs[i].catalog);
            free(fs[i].catalog);
            fs[i].catalog = NULL;
        }
        /* Same lifetime, same reason: the subtree bitmaps are read only by the scan. */
        free(fs[i].subtree_bits);
        fs[i].subtree_bits = NULL;
    }
}

/* Approximate resident bytes held by all loaded shard catalogs (diagnostic only). Sums the
 * per-dir parallel arrays (sized cap+1) plus the owned per-component name strings. */
static size_t ereport_catalog_bytes(const file_state_t *fs, size_t n) {
    size_t i;
    size_t total = 0;
    /* per-slot fixed cost: parent_dir_id(8) depth(4) name_len(2) name_comp ptr(8).
     * ereport loads only the tree fields, so the optional rollup arrays are not
     * allocated and must not be counted here. */
    const size_t per_slot = 8 + 4 + 2 + 8;

    if (!fs) return 0;
    for (i = 0; i < n; i++) {
        const crawl_bin_catalog_t *c = fs[i].catalog;
        uint64_t slots, d;
        if (!c) continue;
        slots = c->cap + 1;
        total += (size_t)slots * per_slot;
        /* owned name strings (strdup'd per directory component) */
        if (c->name_len) {
            for (d = 1; d <= c->max_dir_id; d++)
                if (c->name_len[d]) total += (size_t)c->name_len[d] + 1;
        }
    }
    return total;
}

static int ereport_attach_shard_catalog(file_state_t *fs, const char *path) {
    FILE *fp;
    bin_file_header_t fh;
    struct stat st;
    crawl_bin_catalog_t *cat;

    if (!fs || fs->catalog) return -1;
    cat = (crawl_bin_catalog_t *)malloc(sizeof(*cat));
    if (!cat) return -1;
    crawl_bin_catalog_init_empty(cat);

    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) goto fail;
    fp = counted_fopen(path, "rb");
    if (!fp) goto fail;
    if (counted_fread(&fh, sizeof(fh), 1, fp) != 1) {
        counted_fclose(fp);
        goto fail;
    }
    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
        counted_fclose(fp);
        errno = EINVAL;
        goto fail;
    }
    if (fh.catalog_offset == 0ULL || fh.catalog_offset > (uint64_t)st.st_size) {
        counted_fclose(fp);
        errno = EINVAL;
        goto fail;
    }
    /* Only the tree fields: ereport reconstructs paths and reads records, and
     * consults no per-directory rollup. */
    if (crawl_bin_catalog_load_sel(fp, fh.catalog_offset, (uint64_t)st.st_size, 0U, cat) != 0) {
        counted_fclose(fp);
        goto fail;
    }
    counted_fclose(fp);
    fs->catalog = cat;
    return 0;

fail:
    crawl_bin_catalog_free(cat);
    free(cat);
    return -1;
}

/*
 * One bit per dir_id, set when that directory is at or under `root`.
 *
 * The catalog ereport loads has the parent pointers but not the DFS permutation
 * (CRAWL_CAT_SUBTREE is ten more per-directory arrays, and this route exists to
 * spend less, not more), so descent is closed here instead: dir_ids are handed
 * out parent-first -- crawl_bin_format's writer interns a directory's parent
 * before the directory -- so one forward pass settles every descendant, and a
 * catalog that does not hold to that is reported as unusable rather than
 * half-believed. root == 0 means the subtree is not in this shard at all, which
 * is an all-zero bitmap: every record here is out of scope.
 *
 * 1 bit per directory, so ~85 KB for a shard with 680k of them.
 */
static unsigned char *ereport_subtree_bits(const crawl_bin_catalog_t *cat, uint64_t root) {
    uint64_t n = cat->max_dir_id;
    unsigned char *bits;
    uint64_t d;

    if (root > n) return NULL;
    bits = (unsigned char *)calloc((size_t)(n / 8ULL) + 1U, 1U);
    if (!bits) return NULL;
    if (root == 0ULL) return bits;
    bits[root >> 3] |= (unsigned char)(1U << (root & 7U));

    for (d = 2ULL; d <= n; d++) {
        uint64_t p = cat->parent_dir_id[d];

        if (p >= d) {
            free(bits);
            return NULL;
        }
        if (p != 0ULL && ((bits[p >> 3] >> (p & 7U)) & 1U)) bits[d >> 3] |= (unsigned char)(1U << (d & 7U));
    }
    return bits;
}

/*
 * Per-chunk cache of the most recently resolved parent directory path. Records under
 * the same directory are contiguous in crawl-bin output, so caching by parent_dir_id
 * lets the common case skip the O(depth) catalog chain walk in
 * crawl_bin_catalog_dir_path() and just append the entry name.
 */
typedef struct {
    uint64_t id; /* cached parent_dir_id (>1); 0 = empty */
    size_t len;
    char dir[PATH_MAX];
} catalog_dir_cache_t;

static int catalog_entry_path_cached(const crawl_bin_catalog_t *cat, uint64_t parent_dir_id,
                                     const char *name, size_t name_len, char *out, size_t out_sz,
                                     catalog_dir_cache_t *cache) {
    size_t plen;

    if (!cat || !out || out_sz == 0) return -1;
    if (parent_dir_id == 0ULL) {
        errno = EINVAL;
        return -1;
    }

    if (parent_dir_id == 1ULL) {
        /* Direct child of the synthetic root: path is /<name> (paths are absolute). */
        if (name_len + 2 > out_sz) return -1;
        out[0] = '/';
        if (name_len > 0 && name) memcpy(out + 1, name, name_len);
        out[1 + name_len] = '\0';
        return 0;
    }

    if (cache->id != parent_dir_id) {
        if (crawl_bin_catalog_dir_path_len(cat, parent_dir_id, cache->dir, sizeof(cache->dir), &cache->len) !=
            0) {
            cache->id = 0;
            return -1;
        }
        cache->id = parent_dir_id;
    }

    plen = cache->len;
    if (plen + 1 + name_len + 1 > out_sz) return -1;
    memcpy(out, cache->dir, plen);
    if (plen > 0) {
        out[plen] = '/';
        plen++;
    }
    if (name_len > 0 && name) memcpy(out + plen, name, name_len);
    out[plen + name_len] = '\0';
    return 0;
}

static int read_one_chunk(const file_chunk_t *chunk,
                          file_state_t *file_states,
                          uid_t target_uid,
                          int all_users,
                          int bucket_detail_levels,
                          time_basis_t basis,
                          time_t now,
                          inode_set_t *seen_inodes,
                          uid_accum_t *uid_distinct,
                          progress_local_t *progress,
                          summary_t *sum,
                          bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS],
                          matched_records_t *matched_records,
                          dense_cell_map_t dense_maps[AGE_BUCKETS][SIZE_BUCKETS],
                          dense_cell_map_t *parent_fanout,
                          fanout_parent_stat_map_t *parent_fanout_stats,
                          parent_intern_t *pintern,
                          parent_intern_t *dense_pintern,
                          ereport_run_stats_t *run_stats) {
    FILE *fp = NULL;
    int rc = -1;
    /* Reused across every record in this chunk to avoid per-record malloc/free
     * of a 4 KiB path buffer and a name buffer (name_len is uint16). */
    char *pathbuf_store = NULL;
    crawl_bin_block_reader_t br;
    /* The parent interners are owned by the worker and reused across every chunk it processes:
     * the hash table is grown a handful of times per worker instead of being rebuilt (calloc +
     * rehash, zero-page COW + cross-CPU TLB shootdowns) for every chunk, and a directory that
     * recurs across chunk boundaries is stored once per worker rather than once per chunk. */
    int dense_intern = (matched_records->arena != NULL); /* matches dense_maps' parents_external */
    memset(&br, 0, sizeof(br));

    fp = counted_fopen(chunk->path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", chunk->path, strerror(errno));
        sum->bad_input_files++;
        if (progress) progress->bad_input_files++;
        finalize_chunk_file_progress(file_states, chunk->file_index, progress);
        return -1;
    }

    /* Buffering (EREPORT_PARSE_STDIO_BUFSZ) is applied by the handle cache when the
     * stream is really opened; a reused stream already has it and must not be setvbuf'd. */

    pathbuf_store = (char *)malloc(PATH_MAX);
    if (!pathbuf_store) {
        fprintf(stderr, "warn: scratch alloc failed in %s\n", chunk->path);
        sum->bad_input_files++;
        if (progress) progress->bad_input_files++;
        goto out;
    }

    /* Records live in compressed blocks; the shared block reader seeks to the
     * chunk start and decompresses block-by-block, returning each record header
     * plus a pointer to its name bytes (valid until the next call). */
    {
        crawl_bin_chunk_stdio_t bio;
        bio.fopen = NULL;
        bio.fread = counted_fread_unlocked;
        bio.fclose = NULL;
        if (crawl_bin_block_reader_init(&br, &bio, fp, chunk->start_offset, chunk->end_offset) != 0) {
            fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
            sum->bad_input_files++;
            if (progress) progress->bad_input_files++;
            goto out;
        }
        /* Age buckets, capacity totals and the hardlink dedup between them touch
         * every column except gid and mode, which nothing in the report reads. */
        (void)crawl_bin_block_reader_set_projection(
            &br, CRAWL_PROJECTION_ALL & ~(CRAWL_COL_BIT(CRAWL_COL_GID) | CRAWL_COL_BIT(CRAWL_COL_MODE)));
        /* inode and dev are read only under `if (r.nlink > 1)` below, so row groups whose NLINK zone
         * map tops out at 1 -- most of them on a tree without hardlinks -- can skip all three. */
        (void)crawl_bin_block_reader_set_hardlink_columns(&br, CRAWL_COL_BIT(CRAWL_COL_INODE) |
                                                                   CRAWL_COL_BIT(CRAWL_COL_DEV_MAJOR) |
                                                                   CRAWL_COL_BIT(CRAWL_COL_DEV_MINOR));
    }

    /* Memoizes the last parent directory path so contiguous same-directory records
     * skip the catalog parent-chain walk. */
    catalog_dir_cache_t dir_cache;
    dir_cache.id = 0;
    dir_cache.len = 0;

    /* Record-level parent cache: the three shape/fanout accumulators all need the
     * parent dir string and its dense hash. Derive both once per distinct parent_id
     * here and share them, instead of each accumulator recomputing on its own miss. */
    char shape_parent[PATH_MAX];
    uint32_t shape_phash = 0;
    uint64_t shape_parent_id = 0; /* 0 never appears as a real parent_dir_id */
    int shape_parent_ok = 0;
    const char *shape_parent_interned = NULL; /* interned form of shape_parent, recomputed per distinct parent_id */

    /* --subtree through dirs.idx: membership is a bit test on parent_dir_id, so no path is built
     * for a record the subtree filter is going to drop. NULL means the sidecar was absent, stale
     * or not applicable and the prefix compare below is the filter. */
    const unsigned char *sub_bits = file_states[chunk->file_index].subtree_bits;
    const uint64_t sub_root_parent = file_states[chunk->file_index].subtree_root_parent;
    const uint64_t sub_max_dir_id =
        file_states[chunk->file_index].catalog ? file_states[chunk->file_index].catalog->max_dir_id : 0ULL;

    /*
     * Key for the per-worker last-hit caches below (pintern, the dense and fanout maps). Those
     * caches take a parent_id as a stand-in for the parent path and do not re-check the string, so
     * the key has to be unique across every record the *worker* sees -- and parent_dir_id is only
     * unique within one shard, so shard 3's directory 7 and shard 9's directory 7 would otherwise
     * be the same directory as far as the cache is concerned. Chunk order decides whether that
     * ever lands two such records next to each other, which makes it the kind of wrong answer that
     * appears when something unrelated changes how the file is cut up.
     *
     * Salted with the input file index. Directory counts stay far below 2^40 (a capture with a
     * trillion directories is not a thing) and captures have tens of shards, not millions.
     */
    const uint64_t parent_key_salt = (uint64_t)chunk->file_index << 40;
#define PARENT_KEY(pid) (parent_key_salt | ((pid) & ((1ULL << 40) - 1ULL)))

    for (;;) {
        bin_record_hdr_t r;
        const unsigned char *rec_name = NULL;
        char *pathbuf = NULL;
        uint64_t accounted_size = 0;
        int record_match;
        int skip_paths;
        int got = crawl_bin_block_reader_next(&br, &r, &rec_name);

        if (got == 0) {
            rc = 0;
            break;
        }
        if (got < 0) {
            fprintf(stderr, "warn: read error in %s\n", chunk->path);
            sum->bad_input_files++;
            if (progress) progress->bad_input_files++;
            break;
        }

        sum->scanned_records++;
        if (progress) {
            progress->scanned_records++;
            progress_maybe_flush(progress, run_stats);
        }

        record_match = all_users || ((uid_t)r.uid == target_uid);
        skip_paths = record_match && bucket_detail_levels == 0;

        if (!file_states[chunk->file_index].catalog) {
            fprintf(stderr, "warn: shard catalog not loaded for %s\n", chunk->path);
            sum->bad_input_files++;
            if (progress) progress->bad_input_files++;
            break;
        }

        if (!record_match) {
            /* Name bytes were already decompressed with the record; nothing to skip. */
            continue;
        }

        if (r.parent_dir_id == 0ULL) {
            fprintf(stderr, "warn: incomplete/wire-format record in %s\n", chunk->path);
            sum->bad_input_files++;
            if (progress) progress->bad_input_files++;
            break;
        }

        /* --subtree via dirs.idx: decide membership from the record's parent_dir_id, before any path
         * exists. Same set of records as the prefix compare below -- the bitmap is the descendants of
         * the directory the sidecar resolved, and the subtree's own record, which starts_with_dir_prefix
         * accepts because path == prefix. That record lives in exactly one shard, hanging off the
         * parent outside the bitmap, and is recognised there by its name. */
        if (sub_bits) {
            uint64_t p = r.parent_dir_id;
            int in_scope = (p <= sub_max_dir_id) && ((sub_bits[p >> 3] >> (p & 7U)) & 1U);

            if (!in_scope && p == sub_root_parent && sub_root_parent != 0ULL &&
                (size_t)r.name_len == g_subtree_base_len && rec_name &&
                memcmp(rec_name, g_subtree_base, (size_t)r.name_len) == 0)
                in_scope = 1;
            if (!in_scope) continue;
        }

        if (skip_paths && (!g_subtree_prefix || sub_bits)) {
            pathbuf = NULL;
        } else {
            /* --subtree forces reconstruction even on the skip_paths fast path: we need the full path to
             * test the prefix. skip_paths still gates the heavier per-record appends below. */
            const unsigned char *name_bytes = (r.name_len > 0) ? rec_name : NULL;

            pathbuf = pathbuf_store;
            if (catalog_entry_path_cached(file_states[chunk->file_index].catalog, r.parent_dir_id,
                                          (char *)name_bytes, r.name_len, pathbuf, PATH_MAX,
                                          &dir_cache) != 0) {
                fprintf(stderr, "warn: path reconstruct failed in %s\n", chunk->path);
                sum->bad_input_files++;
                if (progress) progress->bad_input_files++;
                break;
            }
        }

        /* --path-rewrite: relabel the stored prefix before any path use, so the report, heat map, and the
         * --subtree filter all operate in the rewritten namespace. */
        if (g_rewrite_from && pathbuf) {
            (void)rewrite_path_prefix(pathbuf, PATH_MAX);
        }

        /* --subtree: keep only records at or under the requested directory (full absolute path retained).
         * Filter before any accounting so totals/heat-map/distinct-uids reflect just the subtree. */
        if (g_subtree_prefix && !sub_bits && !(pathbuf && starts_with_dir_prefix(pathbuf, g_subtree_prefix))) {
            continue;
        }

        if (all_users && uid_distinct && uid_accum_insert_if_new(uid_distinct, r.uid) < 0) {
            fprintf(stderr, "warn: uid set error in %s\n", chunk->path);
            sum->bad_input_files++;
            if (progress) progress->bad_input_files++;
            break;
        }

        sum->matched_records++;
        if (progress) progress->matched_records++;

        /* When retaining paths, store each path split into an interned parent prefix (shared by every
         * record under the directory) plus a per-record leaf, and share both pointers between the
         * matched_records and per-cell details lists. This is the dominant memory win on huge
         * --bucket-details runs: parent bytes are kept once per directory instead of once per file.
         * Only valid when both lists are arena-backed + paths_external (set in main); otherwise fall
         * back to the per-list dup so ownership/free stays correct. */
        const char *stored_parent = NULL;
        const char *stored_leaf = NULL;
        int share_paths = (!skip_paths) && matched_records->arena != NULL;
        if (share_paths) {
            const char *src = pathbuf ? pathbuf : "";
            size_t plen;
            const char *leaf;
            path_split_pl(src, &plen, &leaf);
            stored_parent = pintern_get(pintern, PARENT_KEY(r.parent_dir_id), src, plen);
            stored_leaf = path_arena_dup(matched_records->arena, leaf);
            if (!stored_parent || !stored_leaf) {
                fprintf(stderr, "warn: path arena alloc failed in %s\n", chunk->path);
                sum->bad_input_files++;
                if (progress) progress->bad_input_files++;
                break;
            }
        }

        time_t rec_time = pick_time(&r, basis); /* computed once; reused for age bucket + fanout stats */

        /* Derive parent dir + dense hash once per distinct parent_id; the shape/fanout
         * accumulators below reuse it (they still last-hit on parent_id internally). */
        const char *shp = NULL;
        uint32_t shp_h = 0;
        if (pathbuf && bucket_detail_levels > 0) {
            if (shape_parent_id != r.parent_dir_id || !shape_parent_ok) {
                if (parent_dir_to_buf(pathbuf, shape_parent, sizeof(shape_parent)) == 0) {
                    shape_phash = dense_hash_full(shape_parent);
                    shape_parent_id = r.parent_dir_id;
                    shape_parent_ok = 1;
                    shape_parent_interned = NULL; /* recompute the interned copy for this new parent */
                } else {
                    shape_parent_ok = 0;
                }
            }
            if (shape_parent_ok) {
                if (dense_intern) {
                    /* Intern once per distinct parent_id; the dense maps store this pointer
                     * (parents_external), so it must be stable for the run, not the stack buffer.
                     * fanout/fanout-stat copy it on their own and are unaffected. */
                    if (!shape_parent_interned) {
                        size_t sl = strlen(shape_parent);
                        shape_parent_interned =
                            pintern_get(dense_pintern, PARENT_KEY(r.parent_dir_id), shape_parent, sl);
                        if (!shape_parent_interned) {
                            fprintf(stderr, "warn: dense parent arena alloc failed in %s\n", chunk->path);
                            sum->bad_input_files++;
                            if (progress) progress->bad_input_files++;
                            break;
                        }
                    }
                    shp = shape_parent_interned;
                } else {
                    shp = shape_parent;
                }
                shp_h = shape_phash;
            }
        }

        if (r.type == 'f') {
            int sb = size_bucket_for(r.size);
            int ab = age_bucket_for(rec_time, now);
            int count_bytes = 1;
            int ctime_led = (bucket_detail_levels > 0) ? record_ctime_led(&r) : 0;

            if (r.nlink > 1) {
                int ins = inode_set_insert_if_new(seen_inodes, r.dev_major, r.dev_minor, r.inode);
                if (ins < 0) {
                    fprintf(stderr, "warn: inode dedup set error in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    break;
                }
                if (ins == 0) count_bytes = 0;
            }

            sum->matched_files++;
            sum->total_files++;
            sum->files[ab][sb] += 1;
            accounted_size = count_bytes ? r.size : 0;

            if (bucket_detail_levels > 0 && ctime_led) {
                sum->ctime_led_files[ab][sb] += 1;
                sum->total_ctime_led_files++;
            }
            if (count_bytes) {
                sum->total_capacity_bytes += r.size;
                sum->total_bytes += r.size;
                sum->bytes[ab][sb] += r.size;
                if (bucket_detail_levels > 0 && ctime_led) {
                    sum->ctime_led_bytes[ab][sb] += r.size;
                    sum->total_ctime_led_bytes += r.size;
                }
            }

            if (pathbuf && bucket_detail_levels > 0) {
                path_shape_accumulate_file(sum, &dense_maps[ab][sb], ab, sb, pathbuf,
                                           PARENT_KEY(r.parent_dir_id), accounted_size, shp, shp_h);
            }

            if (!skip_paths) {
                int drc = share_paths
                    ? bucket_details_append_ptr(&details[ab][sb], stored_parent, stored_leaf, accounted_size, ctime_led)
                    : bucket_details_append(&details[ab][sb], pathbuf, accounted_size, ctime_led);
                if (drc != 0) {
                    fprintf(stderr, "warn: detail append failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    break;
                }
            }
        } else if (r.type == 'd') {
            sum->matched_dirs++;
            sum->total_dirs++;
            sum->total_capacity_bytes += r.size;
            accounted_size = r.size;
        } else if (r.type == 'l') {
            sum->matched_links++;
            sum->total_links++;
            sum->total_capacity_bytes += r.size;
            accounted_size = r.size;
        } else {
            sum->matched_others++;
            sum->total_others++;
            sum->total_other_bytes += r.size;
            sum->total_capacity_bytes += r.size;
            accounted_size = r.size;
        }

        if (!skip_paths) {
            int mrc = share_paths
                ? matched_records_append_ptr(matched_records, stored_parent, stored_leaf, r.type, accounted_size)
                : matched_records_append(matched_records, pathbuf, r.type, accounted_size);
            if (mrc != 0) {
                fprintf(stderr, "warn: matched record append failed in %s\n", chunk->path);
                sum->bad_input_files++;
                if (progress) progress->bad_input_files++;
                break;
            }
        }

        if (pathbuf && bucket_detail_levels > 0 && parent_fanout) {
            path_fanout_accumulate(parent_fanout, pathbuf, PARENT_KEY(r.parent_dir_id), shp, shp_h);
            if (parent_fanout_stats)
                fanout_parent_stat_accumulate(parent_fanout_stats, pathbuf, PARENT_KEY(r.parent_dir_id),
                                              r.type, rec_time, sum, shp, shp_h);
        }
    }
#undef PARENT_KEY

out:
    crawl_bin_block_reader_free(&br);
    free(pathbuf_store);
    counted_fclose(fp);
    finalize_chunk_file_progress(file_states, chunk->file_index, progress);
    progress_flush_local(progress, run_stats);
    return rc;
}

static void *worker_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;
    progress_local_t progress;
    /* Owned by this worker and reused across all chunks it pulls: avoids per-chunk hash rebuilds
     * (the dominant read-phase cost — calloc/rehash zero-page COW + TLB shootdowns) and dedups
     * directories that span chunk boundaries to a single arena copy per worker. The interned
     * strings live in this worker's path arena (freed only after report emission). */
    parent_intern_t pintern;
    parent_intern_t dense_pintern;

    memset(&progress, 0, sizeof(progress));
    pintern_init(&pintern, arg->matched_records.arena, arg->expect_parents);
    pintern_init(&dense_pintern, arg->matched_records.arena, arg->expect_parents);

    for (;;) {
        file_chunk_t *chunk = queue_pop(arg->queue);
        if (!chunk) break;

        read_one_chunk(chunk,
                       arg->file_states,
                       arg->target_uid,
                       arg->all_users,
                       arg->bucket_detail_levels,
                       arg->basis,
                       arg->now,
                       arg->seen_inodes,
                       arg->all_users ? &arg->uid_distinct : NULL,
                       &progress,
                       &arg->summary,
                       arg->details,
                       &arg->matched_records,
                       arg->dense_maps,
                       &arg->parent_fanout,
                       &arg->parent_fanout_stats,
                       &pintern,
                       &dense_pintern,
                       arg->run_stats);
    }

    pintern_free(&pintern);
    pintern_free(&dense_pintern);
    progress_flush_local(&progress, arg->run_stats);

    return NULL;
}

/* The stats tick thread idles between wakeups. A plain sleep(1) is not interruptible, so
 * pthread_join at shutdown had to wait out the in-flight sleep: every run paid up to a full
 * second of dead time after the report finished, which quantized total wall time to whole
 * seconds. Wait on a condvar instead, broadcast when a stop is requested. */
static pthread_mutex_t g_stats_wait_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_stats_wait_cond = PTHREAD_COND_INITIALIZER;

/* Idle for `seconds` unless a stop is requested. Returns 1 when the full interval elapsed,
 * 0 when stopped. Waiting on an absolute deadline makes spurious wakeups resume the
 * remaining time rather than tick early. */
static int stats_wait_or_stop(ereport_run_stats_t *rs, double seconds) {
    struct timespec deadline;
    long nsec;
    int rc = 0;

    if (atomic_load(&rs->stop_stats)) return 0;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += (time_t)seconds;
    nsec = deadline.tv_nsec + (long)((seconds - (double)(time_t)seconds) * 1e9);
    if (nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        nsec -= 1000000000L;
    }
    deadline.tv_nsec = nsec;

    pthread_mutex_lock(&g_stats_wait_lock);
    while (!atomic_load(&rs->stop_stats) && rc != ETIMEDOUT) {
        rc = pthread_cond_timedwait(&g_stats_wait_cond, &g_stats_wait_lock, &deadline);
    }
    pthread_mutex_unlock(&g_stats_wait_lock);

    return atomic_load(&rs->stop_stats) ? 0 : 1;
}

/* Request shutdown and wake the tick thread so its join returns without waiting out the
 * current interval. Every exit path must go through here, not a bare atomic_store. */
static void ereport_stats_stop_request(ereport_run_stats_t *rs) {
    atomic_store(&rs->stop_stats, 1);
    pthread_mutex_lock(&g_stats_wait_lock);
    pthread_cond_broadcast(&g_stats_wait_cond);
    pthread_mutex_unlock(&g_stats_wait_lock);
}

static void *stats_thread_main(void *arg) {
    ereport_run_stats_t *rs = (ereport_run_stats_t *)arg;

    while (!atomic_load(&rs->stop_stats)) {
        unsigned long long scanned_files;
        unsigned long long scanned_records;
        unsigned long long matched_records;
        unsigned long long bad_input_files;
        unsigned long long window_records;
        double records_rate;
        double elapsed_sec;
        char sf[32], tf[32], sr[32], mr[32], rr[32], elapsed_buf[32];

        /* A partial final interval would skew the per-second rolling window, so a stop
         * request ends the loop instead of recording a short tick. */
        if (!stats_wait_or_stop(rs, 1.0)) break;

        {
            int next = (atomic_load(&g_bucket_index) + 1) % WINDOW_SECONDS;
            unsigned long long expired_records = atomic_exchange(&g_bucket_records[next], 0);
            atomic_fetch_sub(&g_window_records, expired_records);
            atomic_store(&g_bucket_index, next);
        }

        {
            unsigned int seen = atomic_load(&g_seconds_seen);
            if (seen < WINDOW_SECONDS) atomic_store(&g_seconds_seen, seen + 1U);
        }

        scanned_files = atomic_load(&rs->scanned_input_files);
        scanned_records = atomic_load(&rs->scanned_records);
        matched_records = atomic_load(&rs->matched_records);
        bad_input_files = atomic_load(&rs->bad_input_files);
        window_records = atomic_load(&g_window_records);
        elapsed_sec = rs->run_start_sec > 0.0 ? now_sec() - rs->run_start_sec : 0.0;

        if (g_ereport_verbose && rs->run_start_sec > 0.0) {
            static double s_last_verbose_runtime_peek;
            double npeek = now_sec();

            if (s_last_verbose_runtime_peek == 0.0) s_last_verbose_runtime_peek = rs->run_start_sec;
            if (npeek - s_last_verbose_runtime_peek >= 30.0) {
                s_last_verbose_runtime_peek = npeek;
                fprintf(stderr, "\n");
                ereport_verbose_fprint_runtime_peek(rs);
            }
        }

        {
            unsigned int divisor = atomic_load(&g_seconds_seen);
            if (divisor == 0) divisor = 1;
            records_rate = (double)window_records / (double)divisor;
        }

        if (g_ereport_verbose) {
            rs->records_rate_sum += records_rate;
            if (rs->records_rate_samples == 0 || records_rate < rs->records_rate_min) rs->records_rate_min = records_rate;
            if (rs->records_rate_samples == 0 || records_rate > rs->records_rate_max) rs->records_rate_max = records_rate;
            rs->records_rate_samples++;
        }

        {
            uint64_t prep_tot_u = rs->chunk_prep_files_total;
            unsigned long long prep_done_u = atomic_load(&rs->chunk_prep_files_done);
            int fin = atomic_load(&rs->parse_workers_done);
            static unsigned int s_quiet_line_tick;
            int do_status_line;

            s_quiet_line_tick++;
            do_status_line = g_ereport_verbose || fin || (s_quiet_line_tick % 5u == 0u);

            if (!do_status_line) continue;

            human_decimal((double)scanned_files, sf, sizeof(sf));
            human_decimal((double)rs->input_files_total, tf, sizeof(tf));
            human_decimal((double)scanned_records, sr, sizeof(sr));
            human_decimal((double)matched_records, mr, sizeof(mr));
            human_decimal(records_rate, rr, sizeof(rr));
            format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

            if (prep_tot_u > 0ULL && prep_done_u < prep_tot_u) {
                char pdone[32], ptot[32];
                human_decimal((double)prep_done_u, pdone, sizeof(pdone));
                human_decimal((double)prep_tot_u, ptot, sizeof(ptot));
                printf("\rchunk-map files:%s/%s | scanning bin headers for parallel parse | el:%s            ", pdone, ptot,
                       elapsed_buf);
                fflush(stdout);
                if (g_ereport_verbose) {
                    static double last_chunk_map_path_log;
                    double now_cp = now_sec();
                    volatile const char **wpaths = rs->chunk_map_worker_paths;
                    int nslots = rs->chunk_map_path_slots;
                    int si, active;

                    if (wpaths && nslots > 0 && (now_cp - last_chunk_map_path_log) >= 8.0) {
                        last_chunk_map_path_log = now_cp;
                        active = 0;
                        for (si = 0; si < nslots; si++) {
                            if (wpaths[si]) active++;
                        }
                        if (active > 0) {
                            printf("\n");
                            fflush(stdout);
                            fprintf(stderr,
                                    "ereport: chunk-map still scanning (%d/%d parallel shard readers busy; large "
                                    "shards or slow storage)\n",
                                    active,
                                    nslots);
                            fflush(stderr);
                        }
                    }
                }
            } else if (atomic_load(&rs->parse_workers_done)) {
                int phase = atomic_load(&rs->finalize_phase);
                unsigned int bdone = atomic_load(&rs->finalize_bucket_done);
                int merge_sub = atomic_load(&rs->finalize_merge_substep);
                int bucket_prep = atomic_load(&rs->finalize_bucket_prep);
                unsigned int idx_step = atomic_load(&rs->finalize_index_step);
                const char *step;
                const char *msub;

                if (phase == 1)
                    step = "merging shard summaries";
                else if (phase == 2)
                    step = "writing bucket HTML";
                else if (phase == 3)
                    step = "writing index.html";
                else
                    step = "finalizing";

                msub = ereport_finalize_merge_substep_cstr(merge_sub);

                if (phase == 2) {
                    if (bucket_prep) {
                        printf(
                            "\rfinalizing: %s (prep: corpus scan + path order) | files:%s/%s rec:%s match:%s bad:%llu | "
                            "el:%s            ",
                            step,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    } else {
                        printf(
                            "\rfinalizing: %s (cells %u/%d) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                            step,
                            bdone,
                            AGE_BUCKETS * SIZE_BUCKETS,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    }
                } else if (phase == 3) {
                    const char *ilab = ereport_finalize_index_step_cstr(idx_step);
                    if (idx_step > 0U && idx_step <= EREPORT_INDEX_PROGRESS_STEPS && ilab[0]) {
                        printf(
                            "\rfinalizing: %s (%u/%u %s) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                            step,
                            idx_step,
                            EREPORT_INDEX_PROGRESS_STEPS,
                            ilab,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    } else {
                        printf(
                            "\rfinalizing: %s | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                            step,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    }
                } else if (phase == 1 && merge_sub == 2) {
                    int mst = atomic_load(&rs->finalize_matched_slices_total);
                    int msd = atomic_load(&rs->finalize_matched_slices_done);

                    if (mst > 0) {
                        printf(
                            "\rfinalizing: %s (matched paths memcpy %d/%d) | files:%s/%s rec:%s match:%s bad:%llu | "
                            "el:%s            ",
                            step,
                            msd,
                            mst,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    } else {
                        printf(
                            "\rfinalizing: %s (%s) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                            step,
                            msub,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    }
                } else if (phase == 1 && merge_sub == 3) {
                    unsigned int dcells = atomic_load(&rs->finalize_dense_cells_done);
                    const int ndc = AGE_BUCKETS * SIZE_BUCKETS;
                    int fft = atomic_load(&rs->finalize_fanout_workers_total);
                    int ffd = atomic_load(&rs->finalize_fanout_workers_done);
                    unsigned int lk = atomic_load(&rs->finalize_lookup_stage);

                    if (fft > 0 && ffd < fft) {
                        printf(
                            "\rfinalizing: %s (fanout merge workers %d/%d) | files:%s/%s rec:%s match:%s bad:%llu | "
                            "el:%s            ",
                            step,
                            ffd,
                            fft,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    } else if (dcells < (unsigned int)ndc) {
                        printf(
                            "\rfinalizing: %s (dense cells %u/%d) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                            step,
                            dcells,
                            ndc,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    } else if (lk == 1U) {
                        printf(
                            "\rfinalizing: %s (fanout lookup repartition) | files:%s/%s rec:%s match:%s bad:%llu | "
                            "el:%s            ",
                            step,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    } else if (lk == 2U) {
                        printf(
                            "\rfinalizing: %s (path-shape heatmap cells) | files:%s/%s rec:%s match:%s bad:%llu | "
                            "el:%s            ",
                            step,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    } else if (lk == 3U) {
                        printf(
                            "\rfinalizing: %s (path-shape margins) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                            step,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    } else {
                        printf(
                            "\rfinalizing: %s (dense cells %u/%d) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                            step,
                            dcells,
                            ndc,
                            sf,
                            tf,
                            sr,
                            mr,
                            bad_input_files,
                            elapsed_buf);
                    }
                } else if (phase == 1 && msub[0]) {
                    printf(
                        "\rfinalizing: %s (%s) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                        step,
                        msub,
                        sf,
                        tf,
                        sr,
                        mr,
                        bad_input_files,
                        elapsed_buf);
                } else {
                    printf(
                        "\rfinalizing: %s | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                        step,
                        sf,
                        tf,
                        sr,
                        mr,
                        bad_input_files,
                        elapsed_buf);
                }
                fflush(stdout);
            } else {
                printf("\r%s rec/s(10s) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                       rr, sf, tf, sr, mr, bad_input_files, elapsed_buf);
                fflush(stdout);
            }
        }
    }

    return NULL;
}

static int scan_dirs_collect_files(const char **dirpaths,
                                   size_t dir_count,
                                   uid_t target_uid,
                                   int all_users,
                                   char ***out_paths,
                                   size_t *out_count) {
    DIR *dir = NULL;
    struct dirent *de;
    char **paths = NULL;
    size_t count = 0;
    size_t cap = 0;
    size_t di;
    uint32_t first_shards = 0;
    int first_layout = -2;
    int use_uid_shards = 0;
    uint32_t wanted_shard = 0;

    if (dir_count == 0) {
        fprintf(stderr, "no crawl bin directories specified\n");
        return -1;
    }

    for (di = 0; di < dir_count; di++) {
        uint32_t shards = 0;
        int layout_rc = read_uid_shard_layout(dirpaths[di], &shards);
        if (layout_rc < 0) {
            fprintf(stderr, "cannot read crawl manifest in %s\n", dirpaths[di]);
            return -1;
        }
        if (di == 0) {
            first_layout = layout_rc;
            first_shards = shards;
        } else {
            if (layout_rc != first_layout) {
                fprintf(stderr,
                        "incompatible crawl directory layouts between %s and %s (uid-sharded vs unsharded)\n",
                        dirpaths[0], dirpaths[di]);
                return -1;
            }
            if (layout_rc > 0 && shards != first_shards) {
                fprintf(stderr, "uid_shards mismatch: %s has %u, expected %u (from %s)\n",
                        dirpaths[di], shards, first_shards, dirpaths[0]);
                return -1;
            }
        }
    }

    if (first_layout > 0) {
        use_uid_shards = 1;
        wanted_shard = ((uint32_t)target_uid) & (first_shards - 1U);
        g_input_layout = "uid_shards";
        g_input_uid_shards = first_shards;
    } else {
        g_input_layout = "unsharded";
        g_input_uid_shards = 0;
    }

    for (di = 0; di < dir_count; di++) {
        const char *dirpath = dirpaths[di];

        dir = counted_opendir(dirpath);
        if (!dir) {
            fprintf(stderr, "cannot open directory %s: %s\n", dirpath, strerror(errno));
            goto fail_partial;
        }

        while ((de = counted_readdir(dir)) != NULL) {
            char full[PATH_MAX];
            char *copy;

            if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
            if (!has_bin_suffix(de->d_name)) continue;

            if (use_uid_shards) {
                uint32_t shard = 0;
                if (parse_uid_shard_number(de->d_name, &shard) != 0) continue;
                if (!all_users && shard != wanted_shard) continue;
            } else {
                if (!starts_with_thread(de->d_name) && !starts_with_uid_shard(de->d_name)) continue;
            }

            if (snprintf(full, sizeof(full), "%s/%s", dirpath, de->d_name) >= (int)sizeof(full)) {
                fprintf(stderr, "warn: path too long: %s/%s\n", dirpath, de->d_name);
                continue;
            }

            if (count == cap) {
                size_t new_cap = (cap == 0) ? 64 : cap * 2;
                char **tmp = (char **)realloc(paths, new_cap * sizeof(*paths));
                if (!tmp) {
                    size_t i;
                    counted_closedir(dir);
                    for (i = 0; i < count; i++) free(paths[i]);
                    free(paths);
                    return -1;
                }
                paths = tmp;
                cap = new_cap;
            }

            copy = strdup(full);
            if (!copy) {
                size_t i;
                counted_closedir(dir);
                for (i = 0; i < count; i++) free(paths[i]);
                free(paths);
                return -1;
            }

            paths[count++] = copy;
        }

        counted_closedir(dir);
        dir = NULL;
    }

    *out_paths = paths;
    *out_count = count;
    return 0;

fail_partial:
    {
        size_t i;
        for (i = 0; i < count; i++) free(paths[i]);
        free(paths);
    }
    return -1;
}

static int parse_ereport_thread_count(void) {
    const char *e = getenv("EREPORT_THREADS");
    long t;
    char *end;

    if (!e || !*e) return DEFAULT_THREADS;
    errno = 0;
    t = strtol(e, &end, 10);
    if (errno || end == e || *end || t < 1 || t > 4096) return DEFAULT_THREADS;
    return (int)t;
}

/*
 * How many bucket cells (age×size pages) to emit concurrently in the bucket-HTML phase.
 * Each concurrent cell runs its own internally-parallel aggregation, so running all 36 at
 * once with a large EREPORT_THREADS oversubscribes the machine early and starves the
 * slow-cell tail. EREPORT_BUCKET_CELL_CONCURRENCY caps the outer pool so the heaviest
 * cells (scheduled largest-first) get a real share of inner threads. Returns 0 when unset
 * (caller picks a default). Valid override range: 1..1024.
 */
static int parse_bucket_cell_concurrency(void) {
    const char *e = getenv("EREPORT_BUCKET_CELL_CONCURRENCY");
    long t;
    char *end;

    if (!e || !*e) return 0;
    errno = 0;
    t = strtol(e, &end, 10);
    if (errno || end == e || *end || t < 1 || t > 1024) return 0;
    return (int)t;
}

/* Aim for ~(4 * threads) chunks per input .bin so work units are not capped by PARSE_CHUNK_BYTES alone. */
static uint64_t compute_parse_chunk_target(uint64_t file_size_bytes, int threads) {
    uint64_t denom;
    uint64_t target;

    if (threads < 1) threads = DEFAULT_THREADS;
    denom = (uint64_t)threads * 4ULL;
    target = file_size_bytes / denom;
    if (target < PARSE_CHUNK_MIN_BYTES) target = PARSE_CHUNK_MIN_BYTES;
    if (target > PARSE_CHUNK_BYTES) target = PARSE_CHUNK_BYTES;
    return target;
}

typedef struct {
    char **paths;
    uint64_t *chunk_targets;
    size_t path_count;
    int *prep_rc;
    file_chunk_t **prep_chunks;
    size_t *prep_chunk_counts;
    atomic_size_t next_path_index;
    ereport_run_stats_t *run_stats;
    volatile const char **worker_cur_path;
    int worker_cur_path_slots;
    /* Threads each scanner may use inside one file (its .ckpt segments are split
     * across them), so the pool and the per-file split share one budget. */
    int seg_threads;
} chunk_prep_pool_t;

typedef struct {
    chunk_prep_pool_t *pool;
    int slot;
} chunk_prep_thread_arg_t;

static void *chunk_prep_worker_main(void *arg) {
    chunk_prep_thread_arg_t *ta = (chunk_prep_thread_arg_t *)arg;
    chunk_prep_pool_t *pool = ta->pool;
    int slot = ta->slot;

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&pool->next_path_index, 1, memory_order_relaxed);
        file_chunk_t *local_chunks = NULL;
        size_t local_count = 0;
        unsigned int fc = 0;
        int r;

        if (i >= pool->path_count) break;

        if (pool->worker_cur_path && slot >= 0 && slot < pool->worker_cur_path_slots) pool->worker_cur_path[slot] = pool->paths[i];

        r = crawl_bin_build_chunks_for_file(&ereport_chunk_io, NULL, pool->paths[i], i, pool->chunk_targets[i],
                                            pool->seg_threads, &local_chunks, &local_count, &fc);

        if (pool->worker_cur_path && slot >= 0 && slot < pool->worker_cur_path_slots) pool->worker_cur_path[slot] = NULL;

        pool->prep_rc[(int)i] = r;
        pool->prep_chunks[(int)i] = local_chunks;
        pool->prep_chunk_counts[(int)i] = local_count;
        (void)fc;
        atomic_fetch_add_explicit(&pool->run_stats->chunk_prep_files_done, 1ULL, memory_order_relaxed);
    }

    if (pool->worker_cur_path && slot >= 0 && slot < pool->worker_cur_path_slots) pool->worker_cur_path[slot] = NULL;

    return NULL;
}

/* Emit one ';'-separated source segment as an <li>, applying --path-rewrite so the provenance line
 * matches the rewritten paths shown elsewhere in the report. seg is not NUL-terminated; seg_len bytes. */
static void emit_storage_source_segment(FILE *out, const char *seg, size_t seg_len) {
    fprintf(out, "<li>");
    if (g_rewrite_from && seg_len < PATH_MAX) {
        char buf[PATH_MAX];
        memcpy(buf, seg, seg_len);
        buf[seg_len] = '\0';
        (void)rewrite_path_prefix(buf, sizeof(buf));
        html_escape(out, buf);
    } else {
        html_escape_segment(out, seg, seg_len);
    }
    fprintf(out, "</li>\n");
}

static void emit_storage_sources_html(FILE *out, size_t crawl_source_count, const char *label) {
    fprintf(out, "<div class=\"report-sources-section\"><h3>Crawl sources</h3>\n");
    fprintf(out, "<p class=\"lead\">Data merged from <strong>%zu</strong> crawl location%s.</p>\n", crawl_source_count,
            crawl_source_count == 1U ? "" : "s");
    fprintf(out, "<ul class=\"report-sources-list\">\n");
    if (!label || label[0] == '\0') {
        fprintf(out, "<li><em>Not listed</em></li>\n");
    } else {
        const char *p = label;
        while (*p) {
            const char *semi = strchr(p, ';');
            if (!semi) {
                emit_storage_source_segment(out, p, strlen(p));
                break;
            }
            if (semi > p) {
                emit_storage_source_segment(out, p, (size_t)(semi - p));
            }
            p = semi + 1;
        }
    }
    fprintf(out, "</ul></div>\n");
}

/* Floating tooltip for heat-map / bucket badges: native `title` is unreliable inside nested links / clipped layouts. */
static void emit_heat_badge_tip_shell_css(FILE *out) {
    fprintf(out,
            "#ereport-badge-tip{position:fixed;display:none;margin:0;padding:8px 10px;"
            "max-width:min(380px,calc(100vw - 24px));box-sizing:border-box;"
            "background:rgba(22,22,24,0.96);color:#f5f5f5;font-size:11px;font-weight:400;line-height:1.4;"
            "border-radius:6px;box-shadow:0 4px 18px rgba(0,0,0,0.35);z-index:100;pointer-events:none;"
            "white-space:normal;word-break:break-word;font-family:Arial,sans-serif}\n"
            "#ereport-badge-tip.open{display:block}\n"
            ".heat-badge-tip{cursor:help}\n");
}

static void emit_heat_badge_tip_install_js(FILE *out) {
    fputs("(function(){\n"
          "'use strict';\n"
          "var tip=document.getElementById('ereport-badge-tip');\n"
          "if(!tip){tip=document.createElement('div');tip.id='ereport-badge-tip';tip.setAttribute('aria-live','polite');"
          "document.body.appendChild(tip);}\n"
          "function hideTip(){tip.classList.remove('open');tip.textContent='';tip.style.left='';tip.style.top='';}\n"
          "function showTip(el){\n"
          "var txt=el.getAttribute('data-tip');if(!txt)return;\n"
          "tip.textContent=txt;var r=el.getBoundingClientRect();\n"
          "var x=r.left,y=r.bottom+6,p=8;\n"
          "tip.classList.add('open');\n"
          "var w=tip.offsetWidth,h=tip.offsetHeight;\n"
          "if(x+w+p>innerWidth)x=Math.max(p,innerWidth-w-p);\n"
          "if(y+h+p>innerHeight)y=Math.max(p,r.top-h-6);\n"
          "tip.style.left=x+'px';tip.style.top=y+'px';\n"
          "}\n"
          "document.body.addEventListener('mouseover',function(ev){\n"
          "var el=ev.target.closest('.heat-badge-tip');if(el)showTip(el);\n"
          "});\n"
          "document.body.addEventListener('mouseout',function(ev){\n"
          "var el=ev.target.closest('.heat-badge-tip');if(!el)return;\n"
          "var rel=ev.relatedTarget;if(rel&&el.contains(rel))return;\n"
          "hideTip();\n"
          "});\n"
          "document.body.addEventListener('focusin',function(ev){\n"
          "var el=ev.target.closest('.heat-badge-tip');if(el)showTip(el);\n"
          "});\n"
          "document.body.addEventListener('focusout',function(ev){\n"
          "if(ev.target.closest('.heat-badge-tip'))hideTip();\n"
          "});\n"
          "window.addEventListener('scroll',hideTip,true);\n"
          "window.addEventListener('resize',hideTip);\n"
          "})();\n",
          out);
}

static void emit_heat_map_badges(FILE *out,
                                 int heat_badges,
                                 int show_ctime_led,
                                 const char *ctime_led_label,
                                 uint64_t ctime_led_bytes,
                                 uint64_t deep_bytes,
                                 uint64_t bucket_bytes,
                                 uint64_t bucket_files,
                                 uint64_t dense_fanout_max,
                                 const char *basis_str,
                                 const char *bucket_scope) {
    int d, dn, sk;
    char tbuf[1024];
    double cl_pct;
    double dp_pct;

    if (!heat_badges) return;

    d = shape_deep_badge_visible(deep_bytes, bucket_bytes, bucket_files);
    dn = shape_dense_badge_visible(dense_fanout_max);
    sk = d && dn;

    if (!show_ctime_led && !d && !dn) return;

    cl_pct = bucket_bytes ? 100.0 * (double)ctime_led_bytes / (double)bucket_bytes : 0.0;
    dp_pct = bucket_bytes ? 100.0 * (double)deep_bytes / (double)bucket_bytes : 0.0;

    fprintf(out, "<span class=\"heat-map-badges\">");
    if (show_ctime_led && ctime_led_label) {
        snprintf(tbuf,
                 sizeof(tbuf),
                 "%s — %s basis. C-led: %.0f%% of bytes (ctime ≥180d newer than both atime and mtime). "
                 "Badge if ≥%.0f%% (Heat: EREPORT_HEAT_CTIME_LED_MIN_SHARE).",
                 bucket_scope,
                 basis_str,
                 cl_pct,
                 g_ctime_led_badge_min_share_frac * 100.0);
        fprintf(out, "<span class=\"heat-badge-tip heat-ctime-led-badge\" tabindex=\"0\" data-tip=\"");
        html_escape(out, tbuf);
        fprintf(out, "\">C %s</span>", ctime_led_label);
    }
    if (sk) {
        snprintf(tbuf,
                 sizeof(tbuf),
                 "%s — %s basis. Skew: ~%.0f%% bytes from deep paths (≥%u '/') plus a parent with ≥%u children.",
                 bucket_scope,
                 basis_str,
                 dp_pct,
                 (unsigned)PATH_SHAPE_DEEP_MIN_SLASHES,
                 (unsigned)PATH_SHAPE_DENSE_MIN_CHILDREN);
        fprintf(out, "<span class=\"heat-badge-tip heat-shape-badge heat-shape-skew\" tabindex=\"0\" data-tip=\"");
        html_escape(out, tbuf);
        fputs("\">Skew</span>", out);
    } else {
        if (d) {
            snprintf(tbuf,
                     sizeof(tbuf),
                     "%s — %s basis. Deep: %.0f%% of bytes from paths with ≥%u slashes (badge if ≥%.0f%%).",
                     bucket_scope,
                     basis_str,
                     dp_pct,
                     (unsigned)PATH_SHAPE_DEEP_MIN_SLASHES,
                     PATH_SHAPE_BADGE_MIN_SHARE_FRAC * 100.0);
            fprintf(out, "<span class=\"heat-badge-tip heat-shape-badge heat-shape-deep\" tabindex=\"0\" data-tip=\"");
            html_escape(out, tbuf);
            fputs("\">Deep</span>", out);
        }
        if (dn) {
            snprintf(tbuf,
                     sizeof(tbuf),
                     "%s — %s basis. Dense: max parent fan-out here is %" PRIu64 " entries (badge if ≥%u).",
                     bucket_scope,
                     basis_str,
                     dense_fanout_max,
                     (unsigned)PATH_SHAPE_DENSE_MIN_CHILDREN);
            fprintf(out, "<span class=\"heat-badge-tip heat-shape-badge heat-shape-dense\" tabindex=\"0\" data-tip=\"");
            html_escape(out, tbuf);
            fputs("\">Dense</span>", out);
        }
    }
    fprintf(out, "</span>");
}

static int emit_html(const char *report_path,
                     const char *username,
                     int all_users,
                     uint64_t distinct_uids,
                     int bucket_detail_levels,
                     uid_t uid,
                     const char *basis_str,
                     const summary_t *sum,
                     const path_shape_view_t *shape,
                     size_t input_files,
                     int threads_used,
                     size_t crawl_source_count,
                     const char *crawl_sources_label,
                     const ereport_crawl_timing_t *crawl_timing,
                     const ereport_manifest_disk_t *manifest_disk,
                     ereport_run_stats_t *prog_rs) {
    FILE *out = counted_fopen(report_path, "w");
    int ab, sb;
    double max_inner_pct_b = 0.0;
    double max_inner_pct_f = 0.0;

    if (!out) return -1;
    if (!shape) return -1;

    fprintf(out, "<!DOCTYPE html>\n");
    if (all_users)
        fprintf(out, "<!-- Generated by ereport (aggregate / all_users). Regenerate by re-running ereport without a username. -->\n");
    fprintf(out, "<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    fprintf(out, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    fprintf(out, "<title>Data storage report — ");
    if (all_users)
        fputs("all users", out);
    else
        html_escape(out, username);
    fprintf(out, "</title>\n");
    fprintf(out, "<style>\n");
    fprintf(out, "body{font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,\"Helvetica Neue\",Arial,sans-serif;"
            "margin:28px auto;padding:0 24px;max-width:1280px;box-sizing:border-box;color:#1f2328;line-height:1.5;"
            "-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}\n");
    fprintf(out, "a{color:#175cd3}\n");
    fprintf(out, "a:focus-visible,button:focus-visible,input:focus-visible,summary:focus-visible{outline:2px solid #2d6a9f;outline-offset:2px;border-radius:4px}\n");
    fprintf(out, "body.drawer-open{overflow:hidden}\n");
    fprintf(out, "h1{margin-bottom:8px}\n");
    fprintf(out, ".report-title{font-size:1.5rem;margin:0 0 10px;letter-spacing:-0.01em;color:#111827}\n");
    fprintf(out, ".report-aggregate-note{margin:0 0 18px;font-size:13px;color:#4b5563;line-height:1.45;max-width:720px}\n");
    fprintf(out, ".report-stats{margin-top:28px;padding-top:22px;border-top:1px solid #ddd}\n");
    fprintf(out, ".report-stats>h2{font-size:1.15rem;margin:0 0 14px}\n");
    fprintf(out, ".report-sources-section{margin-bottom:20px;padding:16px 18px;background:#fff;border:1px solid #e7e5e0;border-radius:10px;box-shadow:0 1px 2px rgba(16,24,40,.05)}\n");
    fprintf(out, ".report-sources-section h3{font-size:0.95rem;margin:0 0 8px;color:#444;font-weight:600}\n");
    fprintf(out, ".report-sources-section .lead{margin:0 0 10px;font-size:13px;color:#555;line-height:1.45}\n");
    fprintf(out, ".report-sources-list{margin:8px 0 0;padding-left:22px;line-height:1.55;color:#333;font-size:13px}\n");
    fprintf(out,
            ".stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%%,220px),1fr));gap:14px;"
            "margin-top:4px}\n");
    fprintf(out, ".stats-foot{margin:12px 0 0;font-size:12px;color:#666;line-height:1.45;max-width:920px}\n");
    fprintf(out, ".stats-card{margin:0;padding:16px 18px;background:#fff;border:1px solid #e7e7e7;border-radius:10px;"
                "box-shadow:0 1px 2px rgba(16,24,40,.05);min-width:0}\n");
    fprintf(out, ".stats-card h3{font-size:0.92rem;margin:0 0 12px;color:#1f2937;font-weight:600}\n");
    fprintf(out, ".stats-dl{margin:0;display:grid;grid-template-columns:auto minmax(0,1fr);gap:8px 14px;font-size:13px;align-items:baseline}\n");
    fprintf(out, ".stats-dl dt{margin:0;color:#666;font-weight:500}\n");
    fprintf(out, ".stats-dl dd{margin:0;color:#222;text-align:right;word-break:break-word}\n");
    fprintf(out, ".stats-num{font-variant-numeric:tabular-nums;font-weight:600;color:#1a1a1a}\n");
    fprintf(out, ".stats-num-short{color:#555;font-size:12px;font-weight:500}\n");
    fprintf(out, ".stats-timing-foot{margin:10px 0 0;font-size:12px;color:#666;line-height:1.45}\n");
    fprintf(out, "table{border-collapse:collapse;margin-top:16px;min-width:900px}\n");
    fprintf(out,
            ".bucket-help{margin:12px 0 18px;border:1px solid #ddd2c8;border-radius:8px;background:#faf8f4;max-width:none;"
            "width:min(900px,100%%);font-size:13px;line-height:1.55;color:#555;box-sizing:border-box}\n");
    fprintf(out, ".bucket-help summary{cursor:pointer;padding:10px 12px;font-weight:600;color:#4a4034;list-style-position:outside}\n");
    fprintf(out, ".bucket-help summary::-webkit-details-marker{color:#8b7355}\n");
    fprintf(out, ".bucket-help .bucket-help-body{padding:0 12px 12px}\n");
    fprintf(out, ".bucket-help .bucket-help-body ul{margin:0;padding-left:1.35em}\n");
    fprintf(out, ".bucket-help .bucket-help-body li{margin:0 0 8px;line-height:1.5}\n");
    fprintf(out, ".bucket-help .bucket-help-body li:last-child{margin-bottom:0}\n");
    fprintf(out, "th,td{border:1px solid #d4d4d4;padding:4px 6px;text-align:right}\n");
    fprintf(out, "th:first-child,td:first-child{text-align:left}\n");
    fprintf(out, "th{background:#f6f7f7}\n");
    fprintf(out, "table.heatmap th.heatmap-corner{font-weight:600;background-color:transparent;background-image:none}\n");
    fprintf(out, "table.heatmap th.heatmap-th-neutral{font-weight:600;background-color:transparent;background-image:none}\n");
    fprintf(out, "table.heatmap th.heatmap-th-neutral[scope=row]{min-width:9em;vertical-align:middle}\n");
    fprintf(out, "table.heatmap th.heatmap-th-x{background-color:rgb(236,244,252);font-weight:600}\n");
    fprintf(out, "table.heatmap th.heatmap-th-y{background-color:rgb(252,244,240);font-weight:600;min-width:9em;"
                "vertical-align:middle}\n");
    fprintf(out, "tr.tot{font-weight:600;background:transparent}\n");
    fprintf(out, "td.tot.tot-cell{background-color:#fafafa}\n");
    fprintf(out, ".cell,.tot-cell{transition:background 0.2s ease}\n");
    fprintf(out, ".cell a.bucket-link,.tot-block.cell-split{display:block;color:inherit;text-decoration:none;position:relative;"
                "overflow:hidden;min-height:62px;padding:0}\n");
    fprintf(out, "td.cell,td.tot-cell{min-width:7.2em;vertical-align:middle}\n");
    fprintf(out, ".cell-split-bg{position:absolute;inset:0;z-index:0;pointer-events:none}\n");
    fprintf(out, ".cell-split-part{position:absolute;inset:0}\n");
    fprintf(out, ".cell-split-bytes{clip-path:polygon(100%% 0,100%% 100%%,0 0)}\n");
    fprintf(out, ".cell-split-files{clip-path:polygon(0 100%%,100%% 100%%,0 0)}\n");
    fprintf(out, ".cell-split-text{position:absolute;inset:0;z-index:1;line-height:1.15;box-sizing:border-box;"
                "pointer-events:none}\n");
    fprintf(out, ".cell-split-text-bytes{clip-path:polygon(100%% 0,100%% 100%%,0 0);display:flex;flex-direction:column;"
                "align-items:flex-end;justify-content:flex-start;padding:5px 5px 36%% 36%%}\n");
    fprintf(out, ".cell-split-text-files{clip-path:polygon(0 100%%,100%% 100%%,0 0);display:flex;flex-direction:column;"
                "align-items:flex-start;justify-content:flex-end;padding:36%% 36%% 5px 5px}\n");
    fprintf(out, ".cell-vol-row{display:flex;align-items:baseline;justify-content:flex-end;gap:3px;flex-wrap:wrap;"
                "max-width:100%%}\n");
    fprintf(out, ".cell-bytes{font-size:12px;font-weight:700;letter-spacing:-0.02em}\n");
    fprintf(out, ".cell-pct{font-size:8px;font-weight:700;color:#163a7a;background:rgba(255,255,255,0.9);padding:1px 3px;"
                "border-radius:999px;line-height:1;white-space:nowrap}\n");
    fprintf(out, ".cell-pct-files{color:#6b2a2a}\n");
    fprintf(out, ".cell-files-stack{display:flex;flex-direction:column;align-items:flex-start;gap:3px;max-width:100%%}\n");
    fprintf(out, ".cell-files-main{font-size:11px;font-weight:700;color:#1a1a1a;line-height:1.15;letter-spacing:-0.02em;"
                "text-shadow:0 0 4px #fff,0 0 8px rgba(255,255,255,0.92);word-break:break-all}\n");
    fprintf(out, ".cell.active{outline:3px solid #2d6a9f;outline-offset:-3px}\n");
    fprintf(out,
            ".heat-map-badges{position:absolute;top:3px;left:4px;z-index:2;display:flex;flex-direction:column;"
            "align-items:flex-start;gap:2px;pointer-events:none}\n"
            ".heat-map-badges>.heat-ctime-led-badge,.heat-map-badges>.heat-shape-badge{pointer-events:auto;cursor:help}\n"
            ".heat-ctime-led-badge{font-size:7px;font-weight:700;line-height:1;"
            "padding:2px 5px;border-radius:4px;background:rgba(109,40,217,0.94);color:#faf5ff;border:1px solid rgba(76,29,149,0.88);"
            "letter-spacing:0.02em;white-space:nowrap}\n"
            ".heat-shape-badge{font-size:7px;font-weight:700;line-height:1;padding:2px 5px;border-radius:4px;"
            "letter-spacing:0.02em;white-space:nowrap;border:1px solid rgba(0,0,0,0.14)}\n"
            ".heat-shape-deep{background:rgba(14,116,144,0.92);color:#ecfeff}\n"
            ".heat-shape-dense{background:rgba(180,83,9,0.92);color:#fffbeb}\n"
            ".heat-shape-skew{background:rgba(127,29,29,0.92);color:#fef2f2}\n");
    emit_heat_badge_tip_shell_css(out);
    fprintf(out, ".drawer-backdrop{position:fixed;inset:0;background:rgba(0,0,0,0.28);opacity:0;pointer-events:none;transition:opacity 0.2s ease;z-index:20}\n");
    fprintf(out, ".drawer-backdrop.open{opacity:1;pointer-events:auto}\n");
    fprintf(out,
            ".drawer{position:fixed;top:0;right:0;width:85vw;height:100vh;background:#fff;"
            "box-shadow:-8px 0 24px rgba(0,0,0,0.18);transform:translateX(100%%);transition:transform 0.22s ease;"
            "z-index:21;display:flex;flex-direction:column}\n");
    fprintf(out, ".drawer.open{transform:translateX(0)}\n");
    fprintf(out, ".drawer-head{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:14px 18px;border-bottom:1px solid #ddd;background:#faf7ef}\n");
    fprintf(out, ".drawer-title{font-size:18px;font-weight:600;color:#222}\n");
    fprintf(out, ".drawer-sub{font-size:12px;color:#666;margin-top:4px}\n");
    fprintf(out, ".drawer-actions{display:flex;align-items:center;gap:10px}\n");
    fprintf(out, ".drawer-actions a,.drawer-actions button{font:inherit;font-size:13px;border:1px solid #c9b991;background:#fff8e8;color:#5a4214;padding:7px 10px;border-radius:6px;text-decoration:none;cursor:pointer}\n");
    fprintf(out, ".drawer-actions button{background:#fff}\n");
    fprintf(out, ".drawer-frame{border:0;width:100%%;flex:1;background:#fff}\n");
    fprintf(out, "@media (max-width:900px){body{margin:14px auto;padding:0 14px}.drawer{width:100vw}.drawer-head{padding:12px 14px}}\n");
    fprintf(out, ".path-search{margin:0 0 22px}\n");
    fprintf(out, ".path-search[hidden]{display:none!important}\n");
    fprintf(out, ".path-search label{display:block;font-weight:600;margin-bottom:6px}\n");
    fprintf(out,
            ".path-search-field-wrap{position:relative;display:inline-block;width:min(520px,95vw);vertical-align:top}\n");
    fprintf(out,
            ".path-search-field-wrap input[type=text]{width:100%%;box-sizing:border-box;padding:9px 36px 9px "
            "12px;font-size:14px;border-radius:8px;transition:background-color .22s ease,border-color .22s ease,box-shadow .15s ease}\n");
    fprintf(out,
            ".path-search-field-wrap input[type=text]:focus{outline:none;box-shadow:0 0 0 3px rgba(45,106,159,.18)}\n");
    fprintf(out, ".path-search-input--neutral{background:#fff;border:1px solid #ccc}\n");
    fprintf(out, ".path-search-input--waiting{background:#fffde7;border:1px solid #ffe082}\n");
    fprintf(out, ".path-search-input--ok{background:#e8f5e9;border:1px solid #a5d6a7}\n");
    fprintf(out, ".path-search-input--empty,.path-search-input--error{background:#ffebee;border:1px solid #ef9a9a}\n");
    fprintf(out,
            ".path-search-spinner{position:absolute;right:8px;top:50%%;width:18px;height:18px;margin-top:-9px;"
            "display:inline-block;border:2px solid #e0d59a;border-top-color:#c9a227;border-radius:50%%;box-sizing:border-box;"
            "animation:pathSearchSpin .65s linear infinite;pointer-events:none;vertical-align:middle}\n");
    fprintf(out, ".path-search-spinner[hidden]{display:none!important}\n");
    fprintf(out, "@keyframes pathSearchSpin{to{transform:rotate(360deg)}}\n");
    fprintf(out, ".path-search-panel{margin-top:12px;padding:14px 16px;border:1px solid #e5e5e5;border-radius:10px;background:#fafafa;box-shadow:0 1px 2px rgba(16,24,40,.05);max-height:min(55vh,480px);overflow:auto;font-size:13px}\n");
    fprintf(out, ".path-search-panel[hidden]{display:none!important}\n");
    fprintf(out, ".path-search-panel-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:8px}\n");
    fprintf(out, ".path-search-panel-head button{font:inherit;font-size:12px;padding:4px 10px;border:1px solid #d0d5dd;border-radius:6px;background:#fff;color:#344054;cursor:pointer}\n");
    fprintf(out, ".path-search-panel-head button:hover{background:#f9fafb}\n");
    fprintf(out, ".path-search-caption{font-size:12px;color:#555;margin:0 0 8px}\n");
    fprintf(out, ".path-search-preview{font-size:13px}\n");
    fprintf(out, ".path-search-preview ul{margin:0;padding-left:20px}\n");
    fprintf(out, ".path-search-muted{color:#777;font-size:13px}\n");
    fprintf(out, ".path-search-hit{background:#fff3cd;padding:0 2px;border-radius:2px;font-weight:600}\n");
    fprintf(out, ".path-search-results{margin-top:18px;padding-top:12px;border-top:1px solid #eee}\n");
    fprintf(out, ".path-search-results-list{margin:8px 0;padding-left:22px;font-size:13px}\n");
    fprintf(out, ".path-search-results-list li{margin:4px 0;word-break:break-all}\n");
    fprintf(out, ".path-search-pager{display:flex;gap:12px;margin-top:12px;align-items:center;flex-wrap:wrap}\n");
    fprintf(out, ".path-search-pager button{padding:6px 14px;font-size:14px;cursor:pointer;border:1px solid #d0d5dd;border-radius:6px;background:#fff;color:#344054}\n");
    fprintf(out, ".path-search-pager button:hover:not(:disabled){background:#f9fafb}\n");
    fprintf(out, ".path-search-pager button:disabled{opacity:0.45;cursor:not-allowed}\n");
    fprintf(out, ".path-search-hint{font-size:12px;color:#666;margin:6px 0 12px;line-height:1.4;max-width:min(720px,100%%)}\n");
    fprintf(out, "</style>\n");
    fprintf(out, "</head>\n<body>\n");

    fprintf(out, "<h1 class=\"report-title\">Data storage report for <strong>");
    if (all_users) {
        fputs("all crawled users", out);
    } else {
        html_escape(out, username);
    }
    fprintf(out, "</strong></h1>\n");
    if (all_users) {
        fprintf(out, "<p class=\"report-aggregate-note\">Includes filesystem entries from <strong>%" PRIu64
                     "</strong> distinct Unix users (UIDs); not filtered to a single account.",
                distinct_uids);
        if (bucket_detail_levels == 0)
            fprintf(out,
                    " Bucket drill-down pages list summary totals only; pass <strong>--bucket-details N</strong> "
                    "(before other arguments) for directory tables. Heat-map totals are unchanged.");
        fprintf(out, "</p>\n");
    } else if (bucket_detail_levels == 0) {
        fprintf(out,
                "<p class=\"report-aggregate-note\">Bucket drill-down pages list summary totals only. Pass "
                "<strong>--bucket-details N</strong> (before the username and time basis, 1&ndash;%d directory levels) "
                "for full per-cell directory tables.</p>\n",
                BUCKET_DETAIL_LEVELS_MAX);
    }

    /* The search box is opt-in: it only works when the report is served alongside a
     * trigram index, which is not always built. Hidden unless the URL carries
     * ?search (the inline script below unhides it), so index-less deployments never
     * show a control that cannot answer. */
    fprintf(out, "<section class=\"path-search\" id=\"path-search\" aria-label=\"Path search\" hidden>\n");
    fprintf(out, "<label for=\"path-search-input\">Find a file or folder</label>\n");
    fprintf(out, "<p class=\"path-search-hint\">Type at least 3 characters of a name. Matches appear as you type; press Enter for full pages of results. Use Hide to close the results panel.</p>\n");
    fprintf(out,
            "<div class=\"path-search-field-wrap\" id=\"path-search-field-wrap\">"
            "<span class=\"path-search-spinner\" id=\"path-search-spinner\" hidden aria-hidden=\"true\"></span>"
            "<input type=\"text\" id=\"path-search-input\" class=\"path-search-input path-search-input--neutral\" "
            "autocomplete=\"off\" placeholder=\"Example: project name or folder\" /></div>\n");
    fprintf(out, "<div id=\"path-search-panel\" class=\"path-search-panel\" hidden aria-live=\"polite\">\n");
    fprintf(out, "<div class=\"path-search-panel-head\"><strong id=\"path-search-panel-title\">Search results</strong>\n");
    fprintf(out, "<button type=\"button\" id=\"path-search-panel-hide\" aria-label=\"Hide results\">Hide</button></div>\n");
    fprintf(out, "<p id=\"path-search-caption\" class=\"path-search-caption\"></p>\n");
    fprintf(out, "<div id=\"path-search-preview\" class=\"path-search-preview\"></div>\n");
    fprintf(out, "<div id=\"path-search-results\" class=\"path-search-results\" hidden>\n");
    fprintf(out, "<div id=\"path-search-results-meta\" class=\"path-search-muted\"></div>\n");
    fprintf(out, "<ol id=\"path-search-results-list\" class=\"path-search-results-list\"></ol>\n");
    fprintf(out, "<div class=\"path-search-pager\">\n");
    fprintf(out, "<button type=\"button\" id=\"path-search-prev\">Previous</button>\n");
    fprintf(out, "<button type=\"button\" id=\"path-search-next\">Next</button>\n");
    fprintf(out, "</div>\n</div>\n</div>\n");
    fprintf(out, "</section>\n");

    ereport_index_prog(prog_rs, 1u);

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            double pb, pf;

            pb = sum->total_bytes ? 100.0 * (double)sum->bytes[ab][sb] / (double)sum->total_bytes : 0.0;
            pf = sum->total_files ? 100.0 * (double)sum->files[ab][sb] / (double)sum->total_files : 0.0;
            if (pb > max_inner_pct_b) max_inner_pct_b = pb;
            if (pf > max_inner_pct_f) max_inner_pct_f = pf;
        }
    }

    fprintf(out, "<table class=\"heatmap\" aria-label=\"File age by size heat map\">\n");
    fprintf(out, "<tr><th scope=\"col\" class=\"heatmap-corner\">Age \xc3\x97 Size</th>");
    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
        fprintf(out, "<th scope=\"col\" class=\"heatmap-th-x\">");
        html_escape(out, size_bucket_names[sb]);
        fprintf(out, "</th>");
    }
    fprintf(out, "<th scope=\"col\" class=\"heatmap-th-neutral\">Total</th></tr>\n");

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        uint64_t row_total = 0;
        uint64_t row_files = 0;

        fprintf(out, "<tr><th scope=\"row\" class=\"heatmap-th-y\">");
        html_escape(out, age_bucket_names[ab]);
        fprintf(out, "</th>");

        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            char hb[32];
            char bg_b[32];
            char bg_f[32];
            char f_main[48];
            char f_paren[80];
            char led_lbl[24];
            double pct_b = 0.0;
            double pct_f = 0.0;
            uint64_t b = sum->bytes[ab][sb];
            uint64_t f = sum->files[ab][sb];
            uint64_t cl_b = sum->ctime_led_bytes[ab][sb];

            row_total += b;
            row_files += f;

            if (sum->total_bytes) pct_b = 100.0 * (double)b / (double)sum->total_bytes;
            if (sum->total_files) pct_f = 100.0 * (double)f / (double)sum->total_files;
            human_bytes(b, hb, sizeof(hb));
            bytes_share_cell_color(heatmap_norm_pct(pct_b, max_inner_pct_b), bg_b, sizeof(bg_b));
            contribution_cell_color(heatmap_norm_pct(pct_f, max_inner_pct_f), bg_f, sizeof(bg_f));
            format_file_count_main_and_paren(f, pct_f, f_main, sizeof(f_main), f_paren, sizeof(f_paren));
            format_ctime_led_share_label(cl_b, b, led_lbl, sizeof(led_lbl));

            fprintf(out,
                    "<td class=\"cell\"><a class=\"bucket-link cell-split\" data-age=\"%d\" data-size=\"%d\" "
                    "href=\"bucket_a%d_s%d.html\">",
                    ab,
                    sb,
                    ab,
                    sb);
            {
                char scope_line[192];
                snprintf(scope_line, sizeof(scope_line), "%s × %s", age_bucket_names[ab], size_bucket_names[sb]);
                emit_heat_map_badges(out,
                                     bucket_detail_levels > 0,
                                     ctime_led_badge_visible(cl_b, b),
                                     led_lbl,
                                     cl_b,
                                     shape->cell[ab][sb].deep_bytes,
                                     b,
                                     f,
                                     shape->cell[ab][sb].dense_fanout_max,
                                     basis_str,
                                     scope_line);
            }
            fprintf(out,
                    "<span class=\"cell-split-bg\" aria-hidden=\"true\">"
                    "<span class=\"cell-split-part cell-split-bytes\" style=\"background:%s\"></span>"
                    "<span class=\"cell-split-part cell-split-files\" style=\"background:%s\"></span>"
                    "</span>"
                    "<span class=\"cell-split-text cell-split-text-bytes\"><span class=\"cell-vol-row\"><span class=\"cell-bytes\">%s</span>"
                    "<span class=\"cell-pct\">%.0f%%</span></span></span>"
                    "<span class=\"cell-split-text cell-split-text-files\"><span class=\"cell-files-stack\"><span "
                    "class=\"cell-files-main\">%s</span><span class=\"cell-pct cell-pct-files\">%s</span></span></span>"
                    "</a></td>",
                    bg_b,
                    bg_f,
                    hb,
                    pct_b,
                    f_main,
                    f_paren);
        }

        {
            char hr[32];
            char bg_b[32];
            char bg_f[32];
            char rf_main[48];
            char rf_paren[80];
            double pct_b = 0.0;
            double pct_f = 0.0;

            if (sum->total_bytes) pct_b = 100.0 * (double)row_total / (double)sum->total_bytes;
            if (sum->total_files) pct_f = 100.0 * (double)row_files / (double)sum->total_files;
            human_bytes(row_total, hr, sizeof(hr));
            bytes_share_cell_color(heatmap_norm_pct(pct_b, 100.0), bg_b, sizeof(bg_b));
            contribution_cell_color(heatmap_norm_pct(pct_f, 100.0), bg_f, sizeof(bg_f));
            format_file_count_main_and_paren(row_files, pct_f, rf_main, sizeof(rf_main), rf_paren, sizeof(rf_paren));
            fprintf(out, "<td class=\"tot tot-cell\"><div class=\"tot-block cell-split\">");
            fprintf(out,
                    "<span class=\"cell-split-bg\" aria-hidden=\"true\">"
                    "<span class=\"cell-split-part cell-split-bytes\" style=\"background:%s\"></span>"
                    "<span class=\"cell-split-part cell-split-files\" style=\"background:%s\"></span>"
                    "</span>"
                    "<span class=\"cell-split-text cell-split-text-bytes\"><span class=\"cell-vol-row\"><span class=\"cell-bytes\">%s</span>"
                    "<span class=\"cell-pct\">%.0f%%</span></span></span>"
                    "<span class=\"cell-split-text cell-split-text-files\"><span class=\"cell-files-stack\"><span "
                    "class=\"cell-files-main\">%s</span><span class=\"cell-pct cell-pct-files\">%s</span></span></span>"
                    "</div></td>",
                    bg_b,
                    bg_f,
                    hr,
                    pct_b,
                    rf_main,
                    rf_paren);
        }

        fprintf(out, "</tr>\n");
    }

    fprintf(out, "<tr class=\"tot\"><th scope=\"row\" class=\"heatmap-th-neutral\">Total</th>");
    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
        uint64_t col_total = 0;
        uint64_t col_files = 0;
        char hc[32];
        char bg_b[32];
        char bg_f[32];
        char cf_main[48];
        char cf_paren[80];
        double pct_b = 0.0;
        double pct_f = 0.0;

        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            col_total += sum->bytes[ab][sb];
            col_files += sum->files[ab][sb];
        }
        if (sum->total_bytes) pct_b = 100.0 * (double)col_total / (double)sum->total_bytes;
        if (sum->total_files) pct_f = 100.0 * (double)col_files / (double)sum->total_files;
        human_bytes(col_total, hc, sizeof(hc));
        bytes_share_cell_color(heatmap_norm_pct(pct_b, 100.0), bg_b, sizeof(bg_b));
        contribution_cell_color(heatmap_norm_pct(pct_f, 100.0), bg_f, sizeof(bg_f));
        format_file_count_main_and_paren(col_files, pct_f, cf_main, sizeof(cf_main), cf_paren, sizeof(cf_paren));
        fprintf(out, "<td class=\"tot tot-cell\"><div class=\"tot-block cell-split\">");
        fprintf(out,
                "<span class=\"cell-split-bg\" aria-hidden=\"true\">"
                "<span class=\"cell-split-part cell-split-bytes\" style=\"background:%s\"></span>"
                "<span class=\"cell-split-part cell-split-files\" style=\"background:%s\"></span>"
                "</span>"
                "<span class=\"cell-split-text cell-split-text-bytes\"><span class=\"cell-vol-row\"><span class=\"cell-bytes\">%s</span>"
                "<span class=\"cell-pct\">%.0f%%</span></span></span>"
                "<span class=\"cell-split-text cell-split-text-files\"><span class=\"cell-files-stack\"><span "
                "class=\"cell-files-main\">%s</span><span class=\"cell-pct cell-pct-files\">%s</span></span></span>"
                "</div></td>",
                bg_b,
                bg_f,
                hc,
                pct_b,
                cf_main,
                cf_paren);
    }
    {
        char ht[32];
        char bg_b[32];
        char bg_f[32];
        char tf_main[48];
        char tf_paren[80];

        human_bytes(sum->total_bytes, ht, sizeof(ht));
        bytes_share_cell_color(heatmap_norm_pct(100.0, 100.0), bg_b, sizeof(bg_b));
        contribution_cell_color(heatmap_norm_pct(100.0, 100.0), bg_f, sizeof(bg_f));
        format_file_count_main_and_paren(sum->total_files, 100.0, tf_main, sizeof(tf_main), tf_paren, sizeof(tf_paren));
        fprintf(out, "<td class=\"tot tot-cell\"><div class=\"tot-block cell-split\">");
        fprintf(out,
                "<span class=\"cell-split-bg\" aria-hidden=\"true\">"
                "<span class=\"cell-split-part cell-split-bytes\" style=\"background:%s\"></span>"
                "<span class=\"cell-split-part cell-split-files\" style=\"background:%s\"></span>"
                "</span>"
                "<span class=\"cell-split-text cell-split-text-bytes\"><span class=\"cell-vol-row\"><span class=\"cell-bytes\">%s</span>"
                "<span class=\"cell-pct\">100%%</span></span></span>"
                "<span class=\"cell-split-text cell-split-text-files\"><span class=\"cell-files-stack\"><span "
                "class=\"cell-files-main\">%s</span><span class=\"cell-pct cell-pct-files\">%s</span></span></span>"
                "</div></td>",
                bg_b,
                bg_f,
                ht,
                tf_main,
                tf_paren);
    }
    fprintf(out, "</tr>\n");

    fprintf(out, "</table>\n");

    ereport_index_prog(prog_rs, 2u);

    fputs("<details class=\"bucket-help\"><summary>How to read this heat map</summary>\n"
          "<div class=\"bucket-help-body\"><ul>\n"
          "<li><strong>Axes.</strong> Rows are file age from the report time basis (<em>atime</em>, <em>mtime</em>, "
          "<em>ctime</em>, or <em>effective</em>: newest of the three). Columns are file size bands.</li>\n"
          "<li><strong>Split cells.</strong> Each cell is divided on the diagonal: upper-right is data volume and share of "
          "total bytes (blue intensity); lower-left is file count (rounded millions/billions when large) and share of "
          "total files (rose intensity).</li>\n"
          "<li><strong>Color scale.</strong> Inner age&times;size cells peak at the largest bucket in the grid; row "
          "totals, column totals, and the corner cell use the full corpus (100%) as the reference so their hues reflect "
          "share of everything reported.</li>\n"
          "<li><strong>Percentages and scope.</strong> Shown percentages are corpus shares. Only regular files; "
          "device/inode dedup applies as elsewhere in the report.</li>\n"
          "<li><strong>Timestamps (Linux).</strong> <em>atime</em> &mdash; last time file data was read (often not "
          "updated on every read: <em>relatime</em>, mount options). <em>mtime</em> &mdash; last time file "
          "<strong>contents</strong> were modified. <em>ctime</em> &mdash; last time <strong>inode metadata</strong> "
          "changed (mode, owner, link count; not file creation time on Linux).</li>\n",
          out);
    if (bucket_detail_levels > 0) {
        fputs("<li><strong>C-led badge.</strong> The purple <strong>C</strong> shows the <strong>percentage of bytes</strong> "
              "in that slice where <em>ctime</em> is <strong>substantially</strong> newer than both <em>atime</em> and "
              "<em>mtime</em>, suggesting the data&rsquo;s apparent recency is <strong>metadata-led</strong> rather than "
              "<strong>usage-</strong> or <strong>content-led</strong>. Here &ldquo;substantially&rdquo; is defined as at "
              "least <strong>180 days</strong> newer than the newer of <em>atime</em> and <em>mtime</em>. The badge is shown "
              "only when that share is at least <strong>30%</strong>.</li>\n"
              "<li><strong>C-led % (interpretation).</strong> This is the percent of bytes whose <em>ctime</em> is much "
              "newer than both <em>atime</em> and <em>mtime</em> (same 180-day rule). <strong>High values</strong> suggest "
              "<strong>stale data</strong> that may have been artificially refreshed by metadata-only changes such as "
              "<strong>chmod</strong>, <strong>chown</strong>, <strong>ACL</strong> updates, <strong>rsync</strong> attribute "
              "updates, or <strong>migration</strong> activity.</li>\n"
              "<li><strong>Deep.</strong> Teal badge when at least <strong>30%</strong> of <strong>bytes</strong> in the slice "
              "are regular files whose paths contain at least <strong>12</strong> slashes (deep trees). Not shown if Dense also "
              "applies in that slice (see Skew).</li>\n",
              out);
        fprintf(out,
                "<li><strong>Dense.</strong> Amber badge when at least one <strong>regular file</strong> in the slice has a "
                "<strong>parent directory</strong> that contains at least <strong>%u</strong> immediate children "
                "(files, directories, symlinks, or other inode types) among <strong>matched crawl records</strong> "
                "(megadir-style fan-out). Not shown if Deep also applies in that slice (see Skew).</li>\n",
                (unsigned)PATH_SHAPE_DENSE_MIN_CHILDREN);
        fputs("<li><strong>Skew.</strong> Dark red badge when <strong>both</strong> Deep and Dense conditions hold in the same "
              "slice; this pill replaces the separate Deep and Dense badges.</li>\n",
              out);
    }
    fputs("<li><strong>Margins.</strong> The rightmost column totals each age row (sum over size buckets). The bottom row "
          "totals each size column (sum over age buckets). The bottom-right cell is the full heat-map total and matches "
          "both the sum of row totals and the sum of column totals. <strong>C-led / Deep / Dense / Skew</strong> badges "
          "appear only on inner age&times;size cells, not on these margin totals.</li>\n"
          "</ul></div></details>\n",
          out);

    ereport_index_prog(prog_rs, 3u);

    {
        char totalb[32];
        char total_non_file_b[32];
        char total_other_b[32];
        uint64_t non_file_count = sum->matched_records - sum->matched_files;
        uint64_t non_file_bytes = sum->total_capacity_bytes - sum->total_bytes;
        uint64_t other_count = sum->matched_others;
        human_bytes(sum->total_bytes, totalb, sizeof(totalb));
        human_bytes(non_file_bytes, total_non_file_b, sizeof(total_non_file_b));
        human_bytes(sum->total_other_bytes, total_other_b, sizeof(total_other_b));

        fprintf(out, "<section class=\"report-stats\" aria-label=\"Summary statistics\">\n");
        fprintf(out, "<h2>Report summary</h2>\n");
        emit_storage_sources_html(out, crawl_source_count, crawl_sources_label);

        fprintf(out, "<div class=\"stats-grid\">\n");

        fprintf(out, "<article class=\"stats-card\"><h3>Run</h3><dl class=\"stats-dl\">\n");
        if (all_users) {
            emit_stats_count_dd(out, "Distinct UIDs (users)", distinct_uids);
        } else {
            fprintf(out, "<dt>Unix UID</dt><dd class=\"stats-num\">%lu</dd>\n", (unsigned long)uid);
        }
        fprintf(out, "<dt>Time basis</dt><dd>");
        html_escape(out, basis_str);
        fprintf(out, "</dd>\n");
        emit_stats_count_dd(out, "Input .bin files", (uint64_t)input_files);
        emit_stats_count_dd(out, "Threads used", (uint64_t)threads_used);
        if (crawl_timing && crawl_timing->valid) {
            char ws[96], we[96], dhms[32], dapprox[48];

            format_wall_clock_local(crawl_timing->wall_start, ws, sizeof(ws));
            format_wall_clock_local(crawl_timing->wall_end, we, sizeof(we));
            format_duration(crawl_timing->elapsed_sec, dhms, sizeof(dhms));
            format_duration_approx(crawl_timing->elapsed_sec, dapprox, sizeof(dapprox));

            fprintf(out, "<dt>Crawl started</dt><dd><span class=\"stats-num\">");
            html_escape(out, ws);
            fprintf(out, "</span></dd>\n");
            fprintf(out, "<dt>Crawl finished</dt><dd><span class=\"stats-num\">");
            html_escape(out, we);
            fprintf(out, "</span></dd>\n");
            fprintf(out,
                    "<dt>Crawl runtime</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num-short\">(%s)</span></dd>\n",
                    dhms,
                    dapprox);
        }
        fprintf(out, "</dl>\n");
        if (crawl_timing && crawl_timing->valid && crawl_timing->merged) {
            fprintf(out,
                    "<p class=\"stats-timing-foot\">Merged report: wall-clock span from earliest crawl start to latest crawl finish "
                    "across inputs.</p>\n");
        }
        fprintf(out, "</article>\n");

        fprintf(out, "<article class=\"stats-card\"><h3>Records processed</h3><dl class=\"stats-dl\">\n");
        emit_stats_count_dd(out, "Scanned records", sum->scanned_records);
        emit_stats_count_dd(out, "Matched records", sum->matched_records);
        emit_stats_count_dd(out, "Bad input files", sum->bad_input_files);
        fprintf(out, "</dl></article>\n");

        fprintf(out, "<article class=\"stats-card\"><h3>Filesystem snapshot</h3><dl class=\"stats-dl\">\n");
        emit_stats_count_dd(out, "Regular files", sum->total_files);
        if (manifest_disk && manifest_disk->valid)
            emit_stats_count_dd(out, "Sparse regular files (est.)", manifest_disk->files_sparse_heuristic);
        emit_stats_count_dd(out, "Directories", sum->total_dirs);
        emit_stats_count_dd(out, "Symbolic links", sum->total_links);
        emit_stats_count_dd(out, "Other types", other_count);
        emit_stats_count_dd(out, "Non-regular entries", non_file_count);
        fprintf(out, "</dl></article>\n");

        fprintf(out, "<article class=\"stats-card\"><h3>Capacity</h3><dl class=\"stats-dl\">\n");
        fprintf(out,
                "<dt>Logical size (regular files)</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num\">(%" PRIu64
                " B)</span></dd>\n",
                totalb,
                sum->total_bytes);
        if (manifest_disk && manifest_disk->valid) {
            char alloc_h[32];

            human_bytes(manifest_disk->total_allocated_bytes, alloc_h, sizeof(alloc_h));
            fprintf(out,
                    "<dt>On disk (regular files)</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num\">(%" PRIu64
                    " B)</span> <span class=\"stats-num-short\">(st_blocks×%" PRIu32 ")</span></dd>\n",
                    alloc_h,
                    manifest_disk->total_allocated_bytes,
                    manifest_disk->st_blocks_bytes_unit);
        }
        fprintf(out, "<dt>In symlinks / non-files</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num\">(%" PRIu64 " B)</span></dd>\n",
                total_non_file_b, non_file_bytes);
        fprintf(out, "<dt>Other file types</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num\">(%" PRIu64 " B)</span></dd>\n",
                total_other_b, sum->total_other_bytes);
        fprintf(out, "</dl></article>\n");

        fprintf(out, "</div>\n");
        if (manifest_disk && manifest_disk->valid && (!all_users || manifest_disk->unit_mismatch)) {
            fprintf(out, "<p class=\"stats-foot\">");
            if (!all_users) {
                fputs(
                    "Sparse count and on-disk total are taken from each input directory’s <code>crawl_manifest.txt</code> "
                    "and describe the <strong>entire ecrawl run</strong> (all UIDs in that crawl). Logical size and regular "
                    "file counts above follow this report’s matched shard records only. ",
                    out);
            }
            if (manifest_disk->unit_mismatch)
                fputs("Merged manifests use different <code>st_blocks_bytes_unit</code> values; combined on-disk bytes "
                      "assume compatible definitions. ",
                      out);
            fprintf(out, "</p>\n");
        }
        fprintf(out, "</section>\n");
    }

    ereport_index_prog(prog_rs, 4u);

    fprintf(out, "<div id=\"drawer-backdrop\" class=\"drawer-backdrop\"></div>\n");
    fprintf(out, "<aside id=\"bucket-drawer\" class=\"drawer\" aria-hidden=\"true\">\n");
    fprintf(out, "<div class=\"drawer-head\"><div><div id=\"bucket-title\" class=\"drawer-title\">Bucket Details</div><div class=\"drawer-sub\">Click a heatmap cell to inspect that bucket.</div></div><div class=\"drawer-actions\"><a id=\"bucket-open\" href=\"#\" target=\"_blank\" rel=\"noopener\">Open page</a><button type=\"button\" id=\"bucket-close\">Close</button></div></div>\n");
    fprintf(out, "<iframe id=\"bucket-frame\" class=\"drawer-frame\" title=\"Bucket details\" loading=\"lazy\"></iframe>\n");
    fprintf(out, "</aside>\n");

    ereport_index_prog(prog_rs, 5u);

    fprintf(out, "<script>\n");
    fputs("(function(){\n", out);
    fputs("'use strict';\n", out);
    /* Search is opt-in via the URL (?search or ?search=1): the box stays hidden
     * unless the report publisher linked it that way (see the section comment). */
    fputs("var pathSearchSection=document.getElementById('path-search');\n", out);
    fputs("var pathSearchEnabled=false;\n", out);
    fputs("try{pathSearchEnabled=new URLSearchParams(window.location.search).has('search');}\n", out);
    fputs("catch(e){pathSearchEnabled=/[?&]search([=&]|$)/.test(window.location.search);}\n", out);
    fputs("if(pathSearchEnabled&&pathSearchSection)pathSearchSection.hidden=false;\n", out);
    fputs("var ageNames=['<30d','30-90d','90-180d','180-365d','1-3y','3y+'];\n", out);
    fputs("var sizeNames=['<4K','4K-1M','1M-100M','100M-1G','1G-10G','10G+'];\n", out);
    fputs("var bucketDrawer=document.getElementById('bucket-drawer');\n", out);
    fputs("var backdrop=document.getElementById('drawer-backdrop');\n", out);
    fputs("var bucketFrame=document.getElementById('bucket-frame');\n", out);
    fputs("var bucketTitleEl=document.getElementById('bucket-title');\n", out);
    fputs("var bucketOpenEl=document.getElementById('bucket-open');\n", out);
    fputs("var activeCell=null;\n", out);
    fputs("function syncBackdrop(){\n", out);
    fputs("var o=bucketDrawer&&bucketDrawer.classList.contains('open');\n", out);
    fputs("backdrop.classList.toggle('open',o);document.body.classList.toggle('drawer-open',o);\n}\n", out);
    fputs("function hideSearchPanel(){\n", out);
    fputs("var p=document.getElementById('path-search-panel');if(p)p.hidden=true;\n", out);
    fputs("var c=document.getElementById('path-search-caption');if(c)c.textContent='';\n", out);
    fputs("setPathSearchFieldState('neutral');\n", out);
    fputs("}\n", out);
    fputs("function showSearchPanel(){\n", out);
    fputs("var p=document.getElementById('path-search-panel');if(p)p.hidden=false;\n", out);
    fputs("}\n", out);
    fputs("function closeBucketDrawer(){\n", out);
    fputs("if(!bucketDrawer)return;\n", out);
    fputs("bucketDrawer.classList.remove('open');bucketDrawer.setAttribute('aria-hidden','true');\n", out);
    fputs("if(activeCell){activeCell.classList.remove('active');activeCell=null;}syncBackdrop();\n}\n", out);
    fputs("function openBucketFromLink(link){\n", out);
    fputs("hideSearchPanel();\n", out);
    fputs("var age=Number(link.dataset.age);var size=Number(link.dataset.size);\n", out);
    fputs("bucketTitleEl.textContent='Bucket Details: '+ageNames[age]+' / '+sizeNames[size];\n", out);
    fputs("bucketFrame.src=link.href;bucketOpenEl.href=link.href;\n", out);
    fputs("bucketDrawer.classList.add('open');bucketDrawer.setAttribute('aria-hidden','false');if(activeCell){activeCell.classList.remove('active');}\n", out);
    fputs("activeCell=link.closest('.cell');if(activeCell){activeCell.classList.add('active');}syncBackdrop();\n}\n", out);
    fputs("document.querySelectorAll('.bucket-link').forEach(function(link){link.addEventListener('click',function(ev){\n", out);
    fputs("if(ev.defaultPrevented||ev.button!==0||ev.metaKey||ev.ctrlKey||ev.shiftKey||ev.altKey)return;\n", out);
    fputs("ev.preventDefault();openBucketFromLink(link);});});\n", out);
    fputs("document.getElementById('bucket-close').addEventListener('click',closeBucketDrawer);\n", out);
    fputs("backdrop.addEventListener('click',closeBucketDrawer);\n", out);
    fputs("document.addEventListener('keydown',function(ev){if(ev.key==='Escape')closeBucketDrawer();});\n", out);
    fputs("var PREVIEW_MAX=20;var PAGE_SIZE=50;var fullTerm='';var pageNum=1;var lastTotal=0;var previewFetchCtl=null;\n", out);
    fputs("var previewGen=0;var pageGen=0;\n", out);
    fputs("function setPathSearchFieldState(st){\n", out);
    fputs("var inp=document.getElementById('path-search-input');\n", out);
    fputs("var sp=document.getElementById('path-search-spinner');\n", out);
    fputs("if(!inp)return;\n", out);
    fputs("inp.classList.remove('path-search-input--neutral','path-search-input--waiting','path-search-input--ok','path-search-input--empty','path-search-input--error');\n", out);
    fputs("if(sp)sp.hidden=true;\n", out);
    fputs("if(st==='waiting'){inp.classList.add('path-search-input--waiting');if(sp)sp.hidden=false;inp.setAttribute('aria-busy','true');return;}\n", out);
    fputs("inp.removeAttribute('aria-busy');\n", out);
    fputs("if(st==='ok')inp.classList.add('path-search-input--ok');\n", out);
    fputs("else if(st==='empty')inp.classList.add('path-search-input--empty');\n", out);
    fputs("else if(st==='error')inp.classList.add('path-search-input--error');\n", out);
    fputs("else inp.classList.add('path-search-input--neutral');\n", out);
    fputs("}\n", out);
    fputs("function fmtSearchMs(ms){\n", out);
    fputs("if(ms==null||!isFinite(ms))return'';\n", out);
    fputs("ms=Number(ms);if(ms<1000)return Math.round(ms)+'ms';\n", out);
    fputs("var s=ms/1000;if(s<1000)return Math.round(s)+'s';\n", out);
    fputs("return Math.round(s/60)+'min';\n", out);
    fputs("}\n", out);
    fputs("function fmtIndexKeys(n){\n", out);
    fputs("if(n==null||!isFinite(n))return'';\n", out);
    fputs("n=Math.round(Number(n));if(n<1000)return String(n);\n", out);
    fputs("if(n<1000000)return Math.round(n/1000)+'K';\n", out);
    fputs("if(n<1000000000)return (n/1e6).toFixed(1).replace(/\\.0$/,'')+'M';\n", out);
    fputs("return (n/1e9).toFixed(1).replace(/\\.0$/,'')+'G';\n", out);
    fputs("}\n", out);
    fputs("function corpusMeta(j){\n", out);
    fputs("if(!j)return'';\n", out);
    fputs("var ip=j.indexed_paths;\n", out);
    fputs("if(ip!=null&&isFinite(ip)&&Math.round(Number(ip))>0)return '~'+fmtIndexKeys(ip)+' paths indexed';\n", out);
    fputs("if(j.index_keys!=null&&fmtIndexKeys(j.index_keys))return '~'+fmtIndexKeys(j.index_keys)+' trigrams';\n", out);
    fputs("return'';\n", out);
    fputs("}\n", out);
    fputs("function escHtml(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');}\n", out);
    fputs("function highlightPathHtml(path,term){\n", out);
    fputs("var q=String(term).trim();\n", out);
    fputs("if(q.length<3)return escHtml(path);\n", out);
    fputs("var lp=path.toLowerCase();\n", out);
    fputs("var lq=q.toLowerCase();\n", out);
    fputs("var out='';var pos=0;\n", out);
    fputs("while(pos<path.length){\n", out);
    fputs("var idx=lp.indexOf(lq,pos);\n", out);
    fputs("if(idx<0){out+=escHtml(path.slice(pos));break;}\n", out);
    fputs("out+=escHtml(path.slice(pos,idx));\n", out);
    fputs("var len=lq.length;\n", out);
    fputs("out+='<mark class=\"path-search-hit\">'+escHtml(path.slice(idx,idx+len))+'</mark>';\n", out);
    fputs("pos=idx+len;\n", out);
    fputs("}\n", out);
    fputs("return out;\n", out);
    fputs("}\n", out);
    fputs("function fetchSearch(term,skip,lim,signal){\n", out);
    fputs("var u=new URL('search',window.location.href);\n", out);
    fputs("u.searchParams.set('q',term);\n", out);
    fputs("u.searchParams.set('skip',String(skip));\n", out);
    fputs("u.searchParams.set('limit',String(lim));\n", out);
    fputs("var fo={};if(signal)fo.signal=signal;\n", out);
    fputs("return fetch(u,fo).then(function(r){\n", out);
    fputs("if(!r.ok){\n", out);
    fputs("var ct=(r.headers.get('Content-Type')||'').toLowerCase();\n", out);
    fputs("if(ct.indexOf('application/json')>=0){\n", out);
    fputs("return r.json().then(function(j){\n", out);
    fputs("var m=(j&&j.error)?String(j.error):('HTTP '+r.status);\n", out);
    fputs("if(j&&j.hint)m=m+' '+String(j.hint);\n", out);
    fputs("throw new Error(m);\n", out);
    fputs("});}\n", out);
    fputs("return r.text().then(function(t){throw new Error(t||String(r.status));});}\n", out);
    fputs("return r.json();\n", out);
    fputs("});}\n", out);
    fputs("function renderPreview(raw){\n", out);
    fputs("var box=document.getElementById('path-search-preview');\n", out);
    fputs("var cap=document.getElementById('path-search-caption');\n", out);
    fputs("var t=raw.trim();\n", out);
    fputs("if(t.length<3){if(previewFetchCtl)previewFetchCtl.abort();box.innerHTML='';document.getElementById('path-search-results').hidden=true;setPathSearchFieldState('neutral');hideSearchPanel();return;}\n", out);
    fputs("showSearchPanel();\n", out);
    fputs("document.getElementById('path-search-panel-title').textContent='Preview';\n", out);
    fputs("if(cap)cap.textContent='Keep typing in the box above—same field—to refine. Press Enter for paged results.';\n", out);
    fputs("document.getElementById('path-search-results').hidden=true;\n", out);
    fputs("if(previewFetchCtl)previewFetchCtl.abort();\n", out);
    fputs("previewFetchCtl=new AbortController();\n", out);
    fputs("var pvSig=previewFetchCtl.signal;\n", out);
    fputs("previewGen++;var prvG=previewGen;\n", out);
    fputs("setPathSearchFieldState('neutral');\n", out);
    fputs("var spinT=setTimeout(function(){\n", out);
    fputs("if(prvG!==previewGen)return;\n", out);
    fputs("var ix=document.getElementById('path-search-input');\n", out);
    fputs("if(ix&&ix.value.trim()===t&&!pvSig.aborted)setPathSearchFieldState('waiting');\n", out);
    fputs("},1000);\n", out);
    fputs("fetchSearch(t,0,PREVIEW_MAX,pvSig).then(function(j){\n", out);
    fputs("clearTimeout(spinT);\n", out);
    fputs("if(prvG!==previewGen)return;\n", out);
    fputs("var inpEl=document.getElementById('path-search-input');\n", out);
    fputs("if(!inpEl||inpEl.value.trim()!==t)return;\n", out);
    fputs("var paths=j.paths||[];\n", out);
    fputs("if(paths.length===0){setPathSearchFieldState('empty');box.innerHTML='<span class=\"path-search-muted\">No matches.</span>';return;}\n", out);
    fputs("setPathSearchFieldState('ok');\n", out);
    fputs("var h='<ul>';for(var i=0;i<paths.length;i++){h+='<li>'+highlightPathHtml(paths[i],t)+'</li>';}h+='</ul>';\n", out);
    fputs("var pv=[];if(j.search_ms!=null&&fmtSearchMs(j.search_ms))pv.push(fmtSearchMs(j.search_ms));\n", out);
    fputs("var cm=corpusMeta(j);if(cm)pv.push(cm);\n", out);
    fputs("var pvs=pv.length?' \\u00b7 '+pv.join(' \\u00b7 '):'';\n", out);
    fputs("if((j.total||0)>paths.length){h+='<div class=\"path-search-muted\">Showing '+paths.length+' of '+j.total+pvs+' \\u2014 press Enter for full paging.</div>';}\n", out);
    fputs("box.innerHTML=h;\n}).catch(function(e){\n", out);
    fputs("clearTimeout(spinT);\n", out);
    fputs("if(prvG!==previewGen)return;\n", out);
    fputs("if(e&&(e.name==='AbortError'||(pvSig&&pvSig.aborted))){setPathSearchFieldState('neutral');return;}\n", out);
    fputs("var caperr=document.getElementById('path-search-caption');if(caperr)caperr.textContent='';setPathSearchFieldState('error');box.innerHTML='<span class=\"path-search-muted\">'+escHtml(e.message)+'</span>';});\n}\n", out);
    fputs("function renderFullPage(){\n", out);
    fputs("var meta=document.getElementById('path-search-results-meta');\n", out);
    fputs("var list=document.getElementById('path-search-results-list');\n", out);
    fputs("var prev=document.getElementById('path-search-prev');\n", out);
    fputs("var next=document.getElementById('path-search-next');\n", out);
    fputs("var cap=document.getElementById('path-search-caption');\n", out);
    fputs("if(!fullTerm){meta.textContent='';list.innerHTML='';if(cap)cap.textContent='';setPathSearchFieldState('neutral');return;}\n", out);
    fputs("showSearchPanel();\n", out);
    fputs("document.getElementById('path-search-panel-title').textContent='Paged results';\n", out);
    fputs("if(cap)cap.textContent='Edit the search box above to change the query; use Prev/Next below.';\n", out);
    fputs("var ftSnap=fullTerm;\n", out);
    fputs("pageGen++;var pg=pageGen;\n", out);
    fputs("setPathSearchFieldState('neutral');\n", out);
    fputs("var spinF=setTimeout(function(){\n", out);
    fputs("if(pg!==pageGen)return;\n", out);
    fputs("var ix=document.getElementById('path-search-input');\n", out);
    fputs("if(ix&&ix.value.trim()===ftSnap)setPathSearchFieldState('waiting');\n", out);
    fputs("},1000);\n", out);
    fputs("fetchSearch(fullTerm,(pageNum-1)*PAGE_SIZE,PAGE_SIZE).then(function(j){\n", out);
    fputs("clearTimeout(spinF);\n", out);
    fputs("if(pg!==pageGen)return;\n", out);
    fputs("var ix2=document.getElementById('path-search-input');\n", out);
    fputs("if(!ix2||ix2.value.trim()!==ftSnap)return;\n", out);
    fputs("lastTotal=j.total||0;var total=lastTotal;var pages=Math.max(1,Math.ceil(total/PAGE_SIZE));\n", out);
    fputs("if(pageNum>pages)pageNum=pages;if(pageNum<1)pageNum=1;\n", out);
    fputs("var pm=[];if(j.search_ms!=null&&fmtSearchMs(j.search_ms))pm.push(fmtSearchMs(j.search_ms));\n", out);
    fputs("var cm2=corpusMeta(j);if(cm2)pm.push(cm2);\n", out);
    fputs("var pms=pm.length?' \\u00b7 '+pm.join(' \\u00b7 '):'';\n", out);
    fputs("meta.textContent=total+' match'+(total===1?'':'es')+pms+' \\u2014 page '+pageNum+' of '+pages;\n", out);
    fputs("var paths=j.paths||[];var h='';for(var i=0;i<paths.length;i++){h+='<li>'+highlightPathHtml(paths[i],fullTerm)+'</li>';}list.innerHTML=h;\n", out);
    fputs("setPathSearchFieldState(total>0?'ok':'empty');\n", out);
    fputs("prev.disabled=pageNum<=1;next.disabled=pageNum>=pages;\n", out);
    fputs("}).catch(function(e){\n", out);
    fputs("clearTimeout(spinF);\n", out);
    fputs("if(pg!==pageGen)return;\n", out);
    fputs("var capfp=document.getElementById('path-search-caption');if(capfp)capfp.textContent='';setPathSearchFieldState('error');meta.textContent='';list.innerHTML='<li class=\"path-search-muted\">'+escHtml(e.message)+'</li>';prev.disabled=true;next.disabled=true;});\n}\n", out);
    fputs("function runFullSearch(term){\n", out);
    fputs("if(previewFetchCtl){previewFetchCtl.abort();previewFetchCtl=null;}\n", out);
    fputs("previewGen++;\n", out);
    fputs("fullTerm=term.trim();if(fullTerm.length<3)return;\n", out);
    fputs("pageNum=1;showSearchPanel();\n", out);
    fputs("document.getElementById('path-search-results').hidden=false;\n", out);
    fputs("renderFullPage();\n}\n", out);
    fputs("document.getElementById('path-search-panel-hide').addEventListener('click',hideSearchPanel);\n", out);
    fputs("var inp=document.getElementById('path-search-input');\n", out);
    fputs("inp.addEventListener('input',function(){renderPreview(inp.value);});\n", out);
    fputs("inp.addEventListener('keydown',function(ev){if(ev.key==='Enter'){ev.preventDefault();runFullSearch(inp.value);}});\n", out);
    fputs("document.getElementById('path-search-prev').addEventListener('click',function(){\n", out);
    fputs("if(pageNum>1){pageNum--;renderFullPage();}});\n", out);
    fputs("document.getElementById('path-search-next').addEventListener('click',function(){\n", out);
    fputs("var pages=Math.max(1,Math.ceil(lastTotal/PAGE_SIZE));\n", out);
    fputs("if(pageNum<pages){pageNum++;renderFullPage();}\n", out);
    fputs("});\n", out);
    fputs("})();\n", out);
    emit_heat_badge_tip_install_js(out);
    fprintf(out, "</script>\n");

    ereport_index_prog(prog_rs, 6u);

    fprintf(out, "</body>\n</html>\n");
    if (counted_fclose(out) != 0) return -1;
    return 0;
}

static void format_input_dirs_label(const char **dirs, size_t n, char *buf, size_t buf_sz) {
    size_t pos = 0;
    size_t i;

    if (!buf || buf_sz == 0) return;
    buf[0] = '\0';
    if (n == 0 || !dirs) return;
    if (n == 1) {
        snprintf(buf, buf_sz, "%s", dirs[0]);
        return;
    }
    for (i = 0; i < n && pos + 1 < buf_sz; i++) {
        int w = snprintf(buf + pos, buf_sz - pos, "%s%s", i ? ";" : "", dirs[i]);
        if (w < 0 || (size_t)w >= buf_sz - pos) {
            snprintf(buf, buf_sz, "%s;… (%zu directories)", dirs[0], n);
            return;
        }
        pos += (size_t)w;
    }
}

static void emit_run_stats(const char *username,
                           int all_users,
                           uint64_t distinct_uids,
                           int bucket_detail_levels,
                           uid_t uid,
                           const char *basis_str,
                           const char *dirpath,
                           const char *report_path,
                           size_t input_files,
                           int threads_requested,
                           int threads_used,
                           const summary_t *sum,
                           ereport_run_stats_t *run_rs,
                           int bucket_pages_written,
                           double elapsed_sec,
                           const ereport_manifest_disk_t *manifest_disk) {
    char avg_records_buf[32], mean_records_buf[32], max_records_buf[32], min_records_buf[32];
    double avg_records = elapsed_sec > 0.0 ? (double)sum->scanned_records / elapsed_sec : 0.0;
    double mean_records =
        run_rs && run_rs->records_rate_samples ? run_rs->records_rate_sum / (double)run_rs->records_rate_samples : avg_records;
    double max_records = run_rs && run_rs->records_rate_samples ? run_rs->records_rate_max : avg_records;
    double min_records = run_rs && run_rs->records_rate_samples ? run_rs->records_rate_min : avg_records;

    human_decimal(avg_records, avg_records_buf, sizeof(avg_records_buf));
    human_decimal(mean_records, mean_records_buf, sizeof(mean_records_buf));
    human_decimal(max_records, max_records_buf, sizeof(max_records_buf));
    human_decimal(min_records, min_records_buf, sizeof(min_records_buf));

    printf("report_type=ereport\n");
    printf("verbose=%d\n", g_ereport_verbose ? 1 : 0);
    printf("user=%s\n", username);
    printf("aggregate_all_users=%d\n", all_users ? 1 : 0);
    printf("bucket_detail_levels=%d\n", bucket_detail_levels);
    if (all_users) printf("distinct_uids=%" PRIu64 "\n", distinct_uids);
    printf("uid=%lu\n", (unsigned long)uid);
    printf("time_basis=%s\n", basis_str);
    printf("input_dir=%s\n", dirpath);
    printf("input_layout=%s\n", g_input_layout);
    if (g_input_uid_shards) printf("input_uid_shards=%u\n", g_input_uid_shards);
    printf("input_files=%zu\n", input_files);
    printf("threads_requested=%d\n", threads_requested);
    printf("threads_used=%d\n", threads_used);
    printf("report_path=%s\n", report_path);
    printf("bucket_pages_dir=%s\n", g_bucket_output_dir);
    printf("bucket_pages_written=%d\n", bucket_pages_written);
    printf("scanned_input_files=%" PRIu64 "\n", sum->scanned_input_files);
    printf("scanned_records=%" PRIu64 "\n", sum->scanned_records);
    if (g_subtree_from_index) printf("subtree_from=dir_index\n");
    if (g_rgix_stats.used) {
        printf("rowgroups_kept=%" PRIu64 "\n", g_rgix_stats.kept);
        printf("rowgroups_total=%" PRIu64 "\n", g_rgix_stats.total);
        printf("rowgroup_records_kept=%" PRIu64 "\n", g_rgix_stats.records_kept);
    }
    printf("matched_records=%" PRIu64 "\n", sum->matched_records);
    printf("files=%" PRIu64 "\n", sum->total_files);
    printf("directories=%" PRIu64 "\n", sum->total_dirs);
    printf("links=%" PRIu64 "\n", sum->total_links);
    printf("others=%" PRIu64 "\n", sum->matched_others);
    printf("non_files=%" PRIu64 "\n", (sum->matched_records - sum->matched_files));
    printf("total_capacity_in_files=%" PRIu64 "\n", sum->total_bytes);
    printf("total_capacity_in_others=%" PRIu64 "\n", sum->total_other_bytes);
    printf("total_capacity_in_non_files=%" PRIu64 "\n", (sum->total_capacity_bytes - sum->total_bytes));
    printf("bad_input_files=%" PRIu64 "\n", sum->bad_input_files);
    if (manifest_disk && manifest_disk->valid) {
        printf("manifest_st_blocks_bytes_unit=%" PRIu32 "\n", manifest_disk->st_blocks_bytes_unit);
        printf("manifest_total_allocated_bytes=%" PRIu64 "\n", manifest_disk->total_allocated_bytes);
        printf("manifest_files_sparse_heuristic=%" PRIu64 "\n", manifest_disk->files_sparse_heuristic);
        printf("manifest_unit_mismatch=%d\n", manifest_disk->unit_mismatch ? 1 : 0);
    }
    if (g_ereport_verbose) {
        printf("io_opendir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_opendir_calls));
        printf("io_readdir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_readdir_calls));
        printf("io_closedir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_closedir_calls));
        printf("io_fopen_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fopen_calls));
        /* Of those calls, the ones that reached the kernel vs. the ones a parked shard handle served. */
        printf("io_fopen_real=%" PRIu64 "\n", crawl_fpcache_real_opens());
        printf("io_fopen_cached=%" PRIu64 "\n", crawl_fpcache_hits());
        printf("io_fclose_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fclose_calls));
        printf("io_fread_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fread_calls));
        printf("mean_records_per_sec=%s\n", mean_records_buf);
        printf("max_records_per_sec=%s\n", max_records_buf);
        printf("min_records_per_sec=%s\n", min_records_buf);
        if (run_rs) {
            double path_sort_sec =
                (double)atomic_load_explicit(&run_rs->vt_ns_path_sort, memory_order_relaxed) / 1e9;
            double bucket_emit_sort_sec =
                (double)atomic_load_explicit(&run_rs->vt_ns_bucket_emit_sort, memory_order_relaxed) / 1e9;
            double w;
            double other;

            w = run_rs->vt_chunk_map_sec;
            printf("phase_chunk_map_wall_sec=%.6f\n", w);
            printf("phase_chunk_map_sort_sec=0.000000\n");
            printf("phase_chunk_map_other_sec=%.6f\n", w);

            w = run_rs->vt_parse_workers_sec;
            printf("phase_parse_workers_wall_sec=%.6f\n", w);
            printf("phase_parse_workers_sort_sec=0.000000\n");
            printf("phase_parse_workers_other_sec=%.6f\n", w);

            w = run_rs->vt_fini_summaries_sec;
            printf("phase_finalize_summaries_wall_sec=%.6f\n", w);
            printf("phase_finalize_summaries_sort_sec=0.000000\n");
            printf("phase_finalize_summaries_other_sec=%.6f\n", w);

            w = run_rs->vt_fini_matched_paths_sec;
            printf("phase_finalize_matched_paths_wall_sec=%.6f\n", w);
            printf("phase_finalize_matched_paths_sort_sec=0.000000\n");
            printf("phase_finalize_matched_paths_other_sec=%.6f\n", w);

            w = run_rs->vt_fini_directory_maps_dense_sec;
            printf("phase_finalize_directory_maps_dense_wall_sec=%.6f\n", w);
            printf("phase_finalize_directory_maps_dense_sort_sec=0.000000\n");
            printf("phase_finalize_directory_maps_dense_other_sec=%.6f\n", w);

            w = run_rs->vt_fini_path_shape_sec;
            printf("phase_finalize_path_shape_wall_sec=%.6f\n", w);
            printf("phase_finalize_path_shape_sort_sec=0.000000\n");
            printf("phase_finalize_path_shape_other_sec=%.6f\n", w);

            w = run_rs->vt_fini_uid_sec;
            printf("phase_finalize_uid_wall_sec=%.6f\n", w);
            printf("phase_finalize_uid_sort_sec=0.000000\n");
            printf("phase_finalize_uid_other_sec=%.6f\n", w);

            w = run_rs->vt_bucket_prep_wall_sec;
            printf("phase_bucket_prep_wall_sec=%.6f\n", w);
            printf("phase_bucket_prep_corpus_scan_wall_sec=%.6f\n", run_rs->vt_bucket_prep_corpus_sec);
            printf("phase_bucket_prep_path_index_sort_sec=%.6f\n", path_sort_sec);
            printf("phase_bucket_prep_note=corpus_scan_and_path_index_sort_may_overlap_when_path_order_thread_used\n");

            w = run_rs->vt_bucket_cells_wall_sec;
            other = w > bucket_emit_sort_sec ? w - bucket_emit_sort_sec : 0.0;
            printf("phase_bucket_html_cells_wall_sec=%.6f\n", w);
            printf("phase_bucket_html_cells_sort_sec=%.6f\n", bucket_emit_sort_sec);
            printf("phase_bucket_html_cells_other_sec=%.6f\n", other);

            w = run_rs->vt_index_html_sec;
            printf("phase_index_html_wall_sec=%.6f\n", w);
            printf("phase_index_html_sort_sec=0.000000\n");
            printf("phase_index_html_other_sec=%.6f\n", w);
        }
    }
    printf("avg_records_per_sec=%s\n", avg_records_buf);
    printf("elapsed_sec=%.3f\n", elapsed_sec);

    /* The verbose_* phase timings otherwise only reach stderr from the ~30s progress peek,
     * so any run shorter than that reported no phase breakdown at all. Emit them once here
     * too; merge_sub/fft are 0 because they only annotate an in-flight fanout merge. */
    if (g_ereport_verbose && run_rs) {
        fflush(stdout);
        ereport_verbose_fprint_timing_kv(run_rs, 0, 0);
        fflush(stderr);
    }
}

int main(int argc, char **argv) {
    const char *user_spec;
    const char *basis_str;
    const char **bin_dirs = NULL;
    size_t bin_dir_count = 0;
    char input_dirs_label[4096];
    char storage_base_paths_label[4096];
    time_basis_t basis;
    uid_t target_uid;
    char display_name[256];
    char **paths = NULL;
    size_t path_count = 0;
    file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    file_state_t *file_states = NULL;
    int threads = DEFAULT_THREADS;
    int threads_used;
    work_queue_t queue;
    pthread_t *tids = NULL;
    worker_arg_t *args = NULL;
    pthread_t stats_thread;
    summary_t final_sum;
    bucket_details_t final_details[AGE_BUCKETS][SIZE_BUCKETS];
    matched_records_t final_matched_records;
    inode_set_t seen_inodes;
    time_t now;
    char report_path[PATH_MAX];
    int i, ab, sb;
    int bucket_pages_written = 0;
    int stats_thread_started = 0;
    int all_users_mode = 0;
    int bucket_detail_levels = 0;
    char report_dir_opt[PATH_MAX];
    static char subtree_buf[PATH_MAX]; /* backs g_subtree_prefix; static so it outlives main()'s frame for worker threads */
    static char rewrite_from_buf[PATH_MAX]; /* backs g_rewrite_from; static for the same reason */
    static char rewrite_to_buf[PATH_MAX];   /* backs g_rewrite_to */
    uint64_t distinct_uid_count = 0;
    uint64_t catalog_dirs_total = 0;
    size_t expect_parents_per_worker = 0;
    double t0, t1;
    ereport_run_stats_t run_stats;
    ereport_crawl_timing_t crawl_timing;
    ereport_manifest_disk_t manifest_disk;
    path_shape_view_t path_shape;
    crawl_sidecar_t sidecar;
    crawl_sidecar_scope_t *sidecar_scope = NULL; /* one per shard; NULL when --index-dir is not in play */

    memset(&sidecar, 0, sizeof(sidecar));

    tune_allocator();
    crawl_fpcache_set_bufsz(EREPORT_PARSE_STDIO_BUFSZ);

    atomic_store(&g_io_opendir_calls, 0);
    atomic_store(&g_io_readdir_calls, 0);
    atomic_store(&g_io_closedir_calls, 0);
    atomic_store(&g_io_fopen_calls, 0);
    atomic_store(&g_io_fclose_calls, 0);
    atomic_store(&g_io_fread_calls, 0);
    ereport_run_stats_reset(&run_stats);
    atomic_store(&g_window_records, 0);
    atomic_store(&g_bucket_index, 0);
    atomic_store(&g_seconds_seen, 0);
    for (i = 0; i < WINDOW_SECONDS; i++) atomic_store(&g_bucket_records[i], 0);
    t0 = now_sec();
    run_stats.run_start_sec = t0;

    report_dir_opt[0] = '\0';

    {
        const char *ehs = getenv("EREPORT_HEAT_CTIME_LED_MIN_SHARE");
        if (ehs && ehs[0]) {
            char *end = NULL;
            errno = 0;
            double v = strtod(ehs, &end);
            if (!errno && end != ehs && end && *end == '\0' && v > 0.0 && v <= 1.0)
                g_ctime_led_badge_min_share_frac = v;
            else
                fprintf(stderr,
                        "ereport: ignoring invalid EREPORT_HEAT_CTIME_LED_MIN_SHARE=%s (want float in (0,1])\n",
                        ehs);
        }
    }

    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s [--bucket-details N] [--report-dir DIR] [--subtree PATH] [--index-dir DIR] [--path-rewrite OLD=NEW] [--verbose] "
                "<username|uid> [<atime|mtime|ctime|effective>] [bin_dir ...]\n",
                argv[0]);
        fprintf(stderr,
                "       %s [--bucket-details N] [--report-dir DIR] [--subtree PATH] [--index-dir DIR] [--path-rewrite OLD=NEW] [--verbose] "
                "[<atime|mtime|ctime|effective>] [bin_dir ...]  (all users → ./all_users/)\n",
                argv[0]);
        fprintf(stderr,
                "Default when the time argument is omitted (single-user form): effective = max(atime,mtime,ctime) "
                "for age buckets. First arg matches a time keyword only when exact.\n");
        fprintf(stderr, "Optional --bucket-details N (1…%d): full per-bucket directory tables; omit for brief buckets.\n",
                BUCKET_DETAIL_LEVELS_MAX);
        fprintf(stderr,
                "Optional --report-dir DIR: write reports under DIR/(user or all_users)/; omit for current directory.\n");
        fprintf(stderr,
                "Optional --subtree PATH (absolute): analyze only records at or under PATH, as if just that "
                "directory had been crawled (full absolute paths kept; totals/heat-map/badges scoped to the subtree).\n");
        fprintf(stderr,
                "Optional --path-rewrite OLD=NEW (both absolute dirs): relabel the OLD path prefix as NEW in the "
                "report/heat-map at read time (bins unchanged), e.g. /data1/group=/orcd/data. Applied before --subtree.\n");
        fprintf(stderr,
                "Optional --index-dir DIR: use the dirs.idx / rowgroups.idx sidecars `ereport_index --make` wrote "
                "there to speed up --subtree — the root is resolved once per shard, row groups that cannot hold a "
                "descendant are never read, and membership becomes a bit test on the record's parent directory "
                "instead of a rebuilt path. The report is identical either way; a sidecar that is absent, stale, or "
                "does not name every shard is ignored.\n");
        fprintf(stderr,
                "Optional --verbose: I/O counters + rolling throughput stats (default quiet: sparse "
                "progress, no per-read I/O atomics); stderr prints ecrawl-style `key=value` progress about "
                "every 30s (idle counters omitted).\n");
        fprintf(stderr,
                "C-led badge threshold: EREPORT_HEAT_CTIME_LED_MIN_SHARE (optional float in (0,1], default 0.30).\n");
        fprintf(stderr,
                "Flags must appear first (any order). Thread count: EREPORT_THREADS (default %d), not argv.\n",
                DEFAULT_THREADS);
        return 2;
    }

    {
        int ac = argc;
        char **av = argv;

        for (;;) {
            if (ac > 1 && strcmp(av[1], "--bucket-details") == 0) {
                char *end;
                long lv;

                if (bucket_detail_levels != 0) {
                    fprintf(stderr, "ereport: duplicate --bucket-details\n");
                    return 2;
                }
                if (ac < 3) {
                    fprintf(stderr, "ereport: --bucket-details requires a number\n");
                    return 2;
                }
                errno = 0;
                lv = strtol(av[2], &end, 10);
                if (errno || end == av[2] || *end || lv < 1 || lv > BUCKET_DETAIL_LEVELS_MAX) {
                    fprintf(stderr, "ereport: --bucket-details must be between 1 and %d\n", BUCKET_DETAIL_LEVELS_MAX);
                    return 2;
                }
                bucket_detail_levels = (int)lv;
                memmove(av + 1, av + 3, (size_t)(ac - 2) * sizeof(char *));
                ac -= 2;
                argc = ac;
                continue;
            }
            if (ac > 1 && strcmp(av[1], "--report-dir") == 0) {
                int nr;

                if (report_dir_opt[0] != '\0') {
                    fprintf(stderr, "ereport: duplicate --report-dir\n");
                    return 2;
                }
                if (ac < 3) {
                    fprintf(stderr, "ereport: --report-dir requires a directory path\n");
                    return 2;
                }
                if (av[2][0] == '\0') {
                    fprintf(stderr, "ereport: --report-dir path is empty\n");
                    return 2;
                }
                nr = snprintf(report_dir_opt, sizeof(report_dir_opt), "%s", av[2]);
                if (nr < 0 || (size_t)nr >= sizeof(report_dir_opt)) {
                    fprintf(stderr, "ereport: --report-dir path too long\n");
                    return 2;
                }
                path_rstrip_path_separators(report_dir_opt);
                if (report_dir_opt[0] == '\0') {
                    fprintf(stderr, "ereport: --report-dir path is invalid\n");
                    return 2;
                }
                memmove(av + 1, av + 3, (size_t)(ac - 2) * sizeof(char *));
                ac -= 2;
                argc = ac;
                continue;
            }
            if (ac > 1 && strcmp(av[1], "--verbose") == 0) {
                if (g_ereport_verbose) {
                    fprintf(stderr, "ereport: duplicate --verbose\n");
                    return 2;
                }
                g_ereport_verbose = 1;
                memmove(av + 1, av + 2, (size_t)(ac - 1) * sizeof(char *));
                ac -= 1;
                argc = ac;
                continue;
            }
            if (ac > 1 && strcmp(av[1], "--subtree") == 0) {
                if (g_subtree_prefix != NULL) {
                    fprintf(stderr, "ereport: duplicate --subtree\n");
                    return 2;
                }
                if (ac < 3) {
                    fprintf(stderr, "ereport: --subtree requires an absolute directory path\n");
                    return 2;
                }
                if (av[2][0] != '/') {
                    fprintf(stderr, "ereport: --subtree path must be absolute (got '%s')\n", av[2]);
                    return 2;
                }
                if (snprintf(subtree_buf, sizeof(subtree_buf), "%s", av[2]) >= (int)sizeof(subtree_buf)) {
                    fprintf(stderr, "ereport: --subtree path too long\n");
                    return 2;
                }
                /* Strip trailing '/' (but keep a lone "/") so prefix matching is on a directory boundary. */
                {
                    size_t sl = strlen(subtree_buf);
                    const char *slash;

                    while (sl > 1 && subtree_buf[sl - 1] == '/') subtree_buf[--sl] = '\0';
                    g_subtree_prefix = subtree_buf;
                    g_subtree_prefix_len = sl;
                    g_subtree_is_root = (sl == 1U);
                    /* Split once: the directory's own name, and the directory holding it. Both are
                     * what the dirs.idx route looks up; the capture root's parent is the empty
                     * string, which is the path catalogs give dir_id 1. */
                    slash = strrchr(subtree_buf, '/');
                    g_subtree_base = slash ? slash + 1 : subtree_buf;
                    g_subtree_base_len = strlen(g_subtree_base);
                    if (slash && slash != subtree_buf) {
                        size_t pl = (size_t)(slash - subtree_buf);

                        memcpy(g_subtree_parent, subtree_buf, pl);
                        g_subtree_parent[pl] = '\0';
                    } else {
                        g_subtree_parent[0] = '\0';
                    }
                }
                memmove(av + 1, av + 3, (size_t)(ac - 2) * sizeof(char *));
                ac -= 2;
                argc = ac;
                continue;
            }
            if (ac > 1 && strcmp(av[1], "--index-dir") == 0) {
                if (g_index_dir != NULL) {
                    fprintf(stderr, "ereport: duplicate --index-dir\n");
                    return 2;
                }
                if (ac < 3 || av[2][0] == '\0') {
                    fprintf(stderr, "ereport: --index-dir requires a directory path\n");
                    return 2;
                }
                g_index_dir = av[2];
                memmove(av + 1, av + 3, (size_t)(ac - 2) * sizeof(char *));
                ac -= 2;
                argc = ac;
                continue;
            }
            if (ac > 1 && strcmp(av[1], "--path-rewrite") == 0) {
                const char *eq;
                size_t fl, tl;

                if (g_rewrite_from != NULL) {
                    fprintf(stderr, "ereport: duplicate --path-rewrite\n");
                    return 2;
                }
                if (ac < 3) {
                    fprintf(stderr, "ereport: --path-rewrite requires OLD=NEW (two absolute directory paths)\n");
                    return 2;
                }
                eq = strchr(av[2], '=');
                if (!eq || eq == av[2] || eq[1] == '\0') {
                    fprintf(stderr, "ereport: --path-rewrite must be OLD=NEW (got '%s')\n", av[2]);
                    return 2;
                }
                fl = (size_t)(eq - av[2]);
                if (av[2][0] != '/' || eq[1] != '/') {
                    fprintf(stderr, "ereport: --path-rewrite OLD and NEW must both be absolute\n");
                    return 2;
                }
                if (fl >= sizeof(rewrite_from_buf) ||
                    snprintf(rewrite_to_buf, sizeof(rewrite_to_buf), "%s", eq + 1) >= (int)sizeof(rewrite_to_buf)) {
                    fprintf(stderr, "ereport: --path-rewrite path too long\n");
                    return 2;
                }
                memcpy(rewrite_from_buf, av[2], fl);
                rewrite_from_buf[fl] = '\0';
                /* Normalize: strip trailing '/'. Both sides must name a directory below root (not a bare "/"). */
                while (fl > 1 && rewrite_from_buf[fl - 1] == '/') rewrite_from_buf[--fl] = '\0';
                tl = strlen(rewrite_to_buf);
                while (tl > 1 && rewrite_to_buf[tl - 1] == '/') rewrite_to_buf[--tl] = '\0';
                if (fl < 2 || tl < 2) {
                    fprintf(stderr, "ereport: --path-rewrite OLD and NEW must name a directory below root (not '/')\n");
                    return 2;
                }
                g_rewrite_from = rewrite_from_buf;
                g_rewrite_from_len = fl;
                g_rewrite_to = rewrite_to_buf;
                g_rewrite_to_len = tl;
                memmove(av + 1, av + 3, (size_t)(ac - 2) * sizeof(char *));
                ac -= 2;
                argc = ac;
                continue;
            }
            break;
        }
    }

    if (argc < 2) {
        fprintf(stderr,
                "ereport: missing arguments (pass a crawl directory, or a time basis, or user plus optional time "
                "basis)\n");
        return 2;
    }

    threads = parse_ereport_thread_count();

    if (g_ereport_verbose) {
        fprintf(stderr,
                "ereport: verbose on (I/O counters + rolling throughput; ecrawl-style key=value progress ~30s on "
                "stderr)\n");
        fflush(stderr);
    }

    if (parse_time_basis(argv[1], &basis) == 0) {
        all_users_mode = 1;
        basis_str = argv[1];
        target_uid = (uid_t)0;
        if (snprintf(display_name, sizeof(display_name), "all_users") >= (int)sizeof(display_name)) {
            fprintf(stderr, "ereport: output name too long\n");
            return 2;
        }

        {
            int ai = 2;
            bin_dirs = (const char **)calloc((size_t)(argc > 2 ? (size_t)(argc - 2) : 1), sizeof(char *));
            if (!bin_dirs) die("allocation failed");
            while (ai < argc) {
                if (argv[ai][0] == '-') {
                    fprintf(stderr,
                            "ereport: unknown option %s (use --verbose for diagnostics; thread count: "
                            "EREPORT_THREADS)\n",
                            argv[ai]);
                    free((void *)bin_dirs);
                    return 2;
                }
                bin_dirs[bin_dir_count++] = argv[ai];
                ai++;
            }
            if (bin_dir_count == 0) {
                free((void *)bin_dirs);
                bin_dirs = (const char **)malloc(sizeof(char *));
                if (!bin_dirs) die("allocation failed");
                bin_dirs[0] = ".";
                bin_dir_count = 1;
            }
        }
    } else {
        user_spec = argv[1];

        if (argc >= 3 && parse_time_basis(argv[2], &basis) == 0) {
            basis_str = argv[2];
            all_users_mode = 0;
            if (resolve_target_user(user_spec, &target_uid, display_name, sizeof(display_name)) != 0) {
                fprintf(stderr, "unknown user or uid: %s\n", user_spec);
                return 1;
            }
            {
                int ai = 3;
                bin_dirs = (const char **)calloc((size_t)(argc > 3 ? (size_t)(argc - 3) : 1), sizeof(char *));
                if (!bin_dirs) die("allocation failed");
                while (ai < argc) {
                    if (argv[ai][0] == '-') {
                        fprintf(stderr,
                            "ereport: unknown option %s (use --verbose for diagnostics; thread count: "
                            "EREPORT_THREADS)\n",
                            argv[ai]);
                        free((void *)bin_dirs);
                        return 2;
                    }
                    bin_dirs[bin_dir_count++] = argv[ai];
                    ai++;
                }
                if (bin_dir_count == 0) {
                    free((void *)bin_dirs);
                    bin_dirs = (const char **)malloc(sizeof(char *));
                    if (!bin_dirs) die("allocation failed");
                    bin_dirs[0] = ".";
                    bin_dir_count = 1;
                }
            }
        } else {
            if (resolve_target_user(user_spec, &target_uid, display_name, sizeof(display_name)) == 0) {
                all_users_mode = 0;
                basis = TIME_EFFECTIVE;
                basis_str = "effective";
                {
                    int ai = 2;
                    bin_dirs = (const char **)calloc((size_t)(argc > 2 ? (size_t)(argc - 2) : 1), sizeof(char *));
                    if (!bin_dirs) die("allocation failed");
                    while (ai < argc) {
                        if (argv[ai][0] == '-') {
                            fprintf(stderr,
                                    "ereport: unknown option %s (use --verbose for diagnostics; thread count: "
                                    "EREPORT_THREADS)\n",
                                    argv[ai]);
                            free((void *)bin_dirs);
                            return 2;
                        }
                        bin_dirs[bin_dir_count++] = argv[ai];
                        ai++;
                    }
                    if (bin_dir_count == 0) {
                        free((void *)bin_dirs);
                        bin_dirs = (const char **)malloc(sizeof(char *));
                        if (!bin_dirs) die("allocation failed");
                        bin_dirs[0] = ".";
                        bin_dir_count = 1;
                    }
                }
            } else {
                all_users_mode = 1;
                basis = TIME_EFFECTIVE;
                basis_str = "effective";
                target_uid = (uid_t)0;
                if (snprintf(display_name, sizeof(display_name), "all_users") >= (int)sizeof(display_name)) {
                    fprintf(stderr, "ereport: output name too long\n");
                    return 2;
                }
                {
                    int ai = 1;
                    bin_dirs = (const char **)calloc((size_t)(argc > 1 ? (size_t)(argc - 1) : 1), sizeof(char *));
                    if (!bin_dirs) die("allocation failed");
                    while (ai < argc) {
                        if (argv[ai][0] == '-') {
                            fprintf(stderr,
                                    "ereport: unknown option %s (use --verbose for diagnostics; thread count: "
                                    "EREPORT_THREADS)\n",
                                    argv[ai]);
                            free((void *)bin_dirs);
                            return 2;
                        }
                        bin_dirs[bin_dir_count++] = argv[ai];
                        ai++;
                    }
                    if (bin_dir_count == 0) {
                        free((void *)bin_dirs);
                        bin_dirs = (const char **)malloc(sizeof(char *));
                        if (!bin_dirs) die("allocation failed");
                        bin_dirs[0] = ".";
                        bin_dir_count = 1;
                    }
                }
            }
        }
    }

    {
        char **resolved;
        size_t j;

        resolved = (char **)malloc(bin_dir_count * sizeof(char *));
        if (!resolved) {
            fprintf(stderr, "ereport: allocation failed\n");
            free((void *)bin_dirs);
            return 1;
        }
        for (j = 0; j < bin_dir_count; j++) {
            char tb[PATH_MAX];

            if (path_resolve_existing(bin_dirs[j], tb, "ereport: ") != 0) {
                while (j > 0) free(resolved[--j]);
                free(resolved);
                free((void *)bin_dirs);
                return 1;
            }
            resolved[j] = strdup(tb);
            if (!resolved[j]) {
                while (j > 0) free(resolved[--j]);
                free(resolved);
                free((void *)bin_dirs);
                return 1;
            }
        }
        free((void *)bin_dirs);
        bin_dirs = (const char **)resolved;
    }

    format_input_dirs_label(bin_dirs, bin_dir_count, input_dirs_label, sizeof(input_dirs_label));
    format_storage_base_paths_label(bin_dirs, bin_dir_count, storage_base_paths_label, sizeof(storage_base_paths_label));

    memset(&crawl_timing, 0, sizeof(crawl_timing));
    aggregate_crawl_timing_from_manifests(bin_dirs, bin_dir_count, &crawl_timing);
    memset(&manifest_disk, 0, sizeof(manifest_disk));
    aggregate_manifest_disk_from_manifests(bin_dirs, bin_dir_count, &manifest_disk);

    set_bucket_output_dir(display_name);
    if (report_dir_opt[0] != '\0') {
        char joined[PATH_MAX];
        int nj;

        nj = snprintf(joined, sizeof(joined), "%s/%s", report_dir_opt, g_bucket_output_dir);
        if (nj < 0 || (size_t)nj >= sizeof(joined)) {
            fprintf(stderr, "ereport: report output path too long under --report-dir\n");
            ereport_free_bin_dirs_list(bin_dirs, bin_dir_count);
            return 1;
        }
        memcpy(g_bucket_output_dir, joined, (size_t)nj + 1U);
    }
    if (snprintf(report_path, sizeof(report_path), "%s/index.html", g_bucket_output_dir) >= (int)sizeof(report_path)) {
        fprintf(stderr, "report path too long for %s\n", g_bucket_output_dir);
        ereport_free_bin_dirs_list(bin_dirs, bin_dir_count);
        return 1;
    }

    if (scan_dirs_collect_files(bin_dirs, bin_dir_count, target_uid, all_users_mode, &paths, &path_count) != 0) {
        ereport_free_bin_dirs_list(bin_dirs, bin_dir_count);
        return 1;
    }
    ereport_free_bin_dirs_list(bin_dirs, bin_dir_count);
    bin_dirs = NULL;
    if (path_count == 0) {
        if (!all_users_mode && strcmp(g_input_layout, "uid_shards") == 0 && g_input_uid_shards > 0U) {
            uint32_t shard = ((uint32_t)target_uid) & (g_input_uid_shards - 1U);
            fprintf(stderr,
                    "ereport: no uid-shard .bin for this user (uid %" PRIuMAX " → shard %u of %u); "
                    "ecrawl only writes non-empty shards. Crawl dir: %s\n",
                    (uintmax_t)(unsigned)target_uid, shard, g_input_uid_shards, input_dirs_label);
        } else {
            fprintf(stderr, "ereport: no matching input .bin files in %s\n", input_dirs_label);
        }
        free(paths);
        return 1;
    }

    fprintf(stderr, "ereport: found %zu matching bin file(s).\n", path_count);
    fflush(stderr);

    /*
     * --index-dir: resolve the subtree in each shard before anything is read.
     *
     * Declined outright for --subtree / (every absolute path matches, so there is nothing to
     * narrow) and under --path-rewrite (the filter then runs in the rewritten namespace, which the
     * catalogs and therefore the sidecar know nothing about). Anything the sidecar cannot account
     * for -- a shard it does not name, an identity that has moved, a subtree it cannot place in any
     * shard -- leaves sidecar_scope NULL and the run takes the path-prefix route unchanged.
     */
    if (g_index_dir && g_subtree_prefix && !g_subtree_is_root && !g_rewrite_from) {
        uint64_t rows_read = 0;

        if (crawl_sidecar_open(g_index_dir, (const char *const *)paths, path_count, &sidecar) == 0) {
            int placed = 0;

            sidecar_scope = (crawl_sidecar_scope_t *)calloc(path_count, sizeof(*sidecar_scope));
            if (sidecar_scope &&
                crawl_sidecar_scope_subtree(&sidecar, g_subtree_prefix, g_subtree_prefix_len, g_subtree_parent,
                                            sidecar_scope, &rows_read) == 0) {
                size_t k;

                /* dirs.idx holds directories only. A --subtree naming a file or a symlink resolves
                 * nowhere here, yet starts_with_dir_prefix would still match that one record on
                 * path == prefix, so an unresolved name is a reason to fall back rather than to
                 * report an empty subtree. */
                for (k = 0; k < path_count; k++)
                    if (sidecar_scope[k].root != 0ULL) placed = 1;
            }
            if (!placed) {
                free(sidecar_scope);
                sidecar_scope = NULL;
                crawl_sidecar_close(&sidecar);
            } else if (g_ereport_verbose) {
                fprintf(stderr, "ereport: dirs.idx resolved %s from %" PRIu64 " directory row(s)\n",
                        g_subtree_prefix, rows_read);
            }
        }
    }

    file_states = (file_state_t *)calloc(path_count, sizeof(*file_states));
    if (!file_states) {
        size_t k;
        fprintf(stderr, "allocation failed\n");
        for (k = 0; k < path_count; k++) free(paths[k]);
        free(paths);
        return 1;
    }

    {
        uint64_t *chunk_targets = (uint64_t *)calloc(path_count, sizeof(uint64_t));
        int *prep_rc = (int *)calloc(path_count, sizeof(int));
        file_chunk_t **prep_chunks = (file_chunk_t **)calloc(path_count, sizeof(*prep_chunks));
        size_t *prep_chunk_counts = (size_t *)calloc(path_count, sizeof(size_t));
        chunk_prep_pool_t pool;
        pthread_t *prep_tids = NULL;
        chunk_prep_thread_arg_t *prep_args = NULL;
        volatile const char **chunk_wpaths = NULL;
        int prep_threads;
        int prep_seg_threads;
        size_t merge_off;
        double vt_chunk0 = 0.0;

        if (!chunk_targets || !prep_rc || !prep_chunks || !prep_chunk_counts) {
            fprintf(stderr, "allocation failed\n");
            free(chunk_targets);
            free(prep_rc);
            free(prep_chunks);
            free(prep_chunk_counts);
            for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
            free(paths);
            ereport_free_file_states(file_states, path_count);
            return 1;
        }

        for (i = 0; (size_t)i < path_count; i++) {
            struct stat st;

            chunk_targets[i] = PARSE_CHUNK_BYTES;
            if (stat(paths[i], &st) == 0 && S_ISREG(st.st_mode))
                chunk_targets[i] = compute_parse_chunk_target((uint64_t)st.st_size, threads);
        }

        run_stats.input_files_total = (uint64_t)path_count;
        run_stats.chunk_prep_files_total = (uint64_t)path_count;
        atomic_store(&run_stats.chunk_prep_files_done, 0ULL);

        if (pthread_create(&stats_thread, NULL, stats_thread_main, &run_stats) != 0) {
            fprintf(stderr, "failed to create stats thread\n");
            free(chunk_targets);
            free(prep_rc);
            free(prep_chunks);
            free(prep_chunk_counts);
            for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
            free(paths);
            ereport_free_file_states(file_states, path_count);
            return 1;
        }
        stats_thread_started = 1;

        if (g_ereport_verbose) vt_chunk0 = now_sec();

        /*
         * --index-dir: rowgroups.idx already knows which row groups can hold a descendant of the
         * subtree, and a group boundary is a legal chunk boundary, so the survivors are the job
         * list. That also retires the boundary-mapping pass below, whose whole job is to find those
         * same boundaries by walking every group header in the record region.
         *
         * Purely a reduction of the same byte ranges: both sketches are conservative supersets, so
         * no record the scan would have matched can be in a group that was dropped.
         */
        if (sidecar_scope) {
            uint64_t pruned_bytes = 0;

            if (crawl_rgix_build_chunks(&sidecar, (const char *const *)paths, path_count, sidecar_scope, threads,
                                        4U, PARSE_CHUNK_MIN_BYTES, PARSE_CHUNK_BYTES, &chunks, &chunk_count,
                                        &pruned_bytes, &g_rgix_stats) == 0) {
                size_t ci;

                for (ci = 0; ci < chunk_count; ci++)
                    atomic_fetch_add(&file_states[chunks[ci].file_index].remaining_chunks, 1U);
                if (g_ereport_verbose) {
                    fprintf(stderr,
                            "rowgroups_kept=%" PRIu64 "\nrowgroups_total=%" PRIu64
                            "\nrowgroup_records_kept=%" PRIu64 "\nrowgroup_records_total=%" PRIu64
                            "\nrowgroup_bytes_kept=%" PRIu64 "\nrowgroup_chunks=%zu\n",
                            g_rgix_stats.kept, g_rgix_stats.total, g_rgix_stats.records_kept,
                            g_rgix_stats.records_total, pruned_bytes, chunk_count);
                    fflush(stderr);
                }
                goto chunks_ready;
            }
        }

        /*
         * Two levels: one scanner per input file, and inside each file the .ckpt
         * segments are split again (crawl_bin_build_chunks_for_file). A single-file
         * capture therefore still uses the whole budget, and N files do not each
         * spawn `threads` helpers on top of the pool.
         */
        prep_threads = threads;
        if ((size_t)prep_threads > path_count) prep_threads = (int)path_count;
        if (prep_threads < 1) prep_threads = 1;
        prep_seg_threads = threads / prep_threads;
        if (prep_seg_threads < 1) prep_seg_threads = 1;

        fprintf(stderr, "ereport: mapping chunk boundaries using %d file scanner(s) x %d segment thread(s)...\n",
                prep_threads, prep_seg_threads);
        fflush(stderr);

        chunk_wpaths = (volatile const char **)calloc((size_t)prep_threads, sizeof(*chunk_wpaths));
        prep_args = (chunk_prep_thread_arg_t *)calloc((size_t)prep_threads, sizeof(*prep_args));
        if (!chunk_wpaths || !prep_args) {
            fprintf(stderr, "allocation failed\n");
            free((void *)chunk_wpaths);
            free(prep_args);
            ereport_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
            free(chunk_targets);
            free(prep_rc);
            free(prep_chunks);
            free(prep_chunk_counts);
            for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
            free(paths);
            ereport_free_file_states(file_states, path_count);
            return 1;
        }

        run_stats.chunk_map_worker_paths = chunk_wpaths;
        run_stats.chunk_map_path_slots = prep_threads;

        memset(&pool, 0, sizeof(pool));
        pool.paths = paths;
        pool.chunk_targets = chunk_targets;
        pool.path_count = path_count;
        pool.prep_rc = prep_rc;
        pool.prep_chunks = prep_chunks;
        pool.prep_chunk_counts = prep_chunk_counts;
        pool.run_stats = &run_stats;
        pool.worker_cur_path = chunk_wpaths;
        pool.worker_cur_path_slots = prep_threads;
        pool.seg_threads = prep_seg_threads;
        atomic_store(&pool.next_path_index, 0);

        prep_tids = (pthread_t *)calloc((size_t)prep_threads, sizeof(*prep_tids));
        if (!prep_tids) {
            fprintf(stderr, "allocation failed\n");
            free((void *)chunk_wpaths);
            free(prep_args);
            run_stats.chunk_map_worker_paths = NULL;
            run_stats.chunk_map_path_slots = 0;
            ereport_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
            free(chunk_targets);
            free(prep_rc);
            free(prep_chunks);
            free(prep_chunk_counts);
            for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
            free(paths);
            ereport_free_file_states(file_states, path_count);
            return 1;
        }

        for (i = 0; i < prep_threads; i++) {
            prep_args[i].pool = &pool;
            prep_args[i].slot = (int)i;
            if (pthread_create(&prep_tids[i], NULL, chunk_prep_worker_main, &prep_args[i]) != 0) {
                int j;
                fprintf(stderr, "failed to create chunk-prep thread\n");
                for (j = 0; j < i; j++) pthread_join(prep_tids[j], NULL);
                free(prep_tids);
                prep_tids = NULL;
                free((void *)chunk_wpaths);
                free(prep_args);
                run_stats.chunk_map_worker_paths = NULL;
                run_stats.chunk_map_path_slots = 0;
                ereport_stats_stop_request(&run_stats);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
                free(chunk_targets);
                free(prep_rc);
                free(prep_chunks);
                free(prep_chunk_counts);
                for (j = 0; (size_t)j < path_count; j++) free(paths[j]);
                free(paths);
                ereport_free_file_states(file_states, path_count);
                return 1;
            }
        }

        for (i = 0; i < prep_threads; i++) pthread_join(prep_tids[i], NULL);
        free(prep_tids);
        prep_tids = NULL;
        free(prep_args);
        prep_args = NULL;
        free((void *)chunk_wpaths);
        chunk_wpaths = NULL;
        run_stats.chunk_map_worker_paths = NULL;
        run_stats.chunk_map_path_slots = 0;

        chunk_count = 0;
        for (i = 0; (size_t)i < path_count; i++) {
            if (prep_rc[i] != 0) {
                atomic_store(&file_states[i].remaining_chunks, 0U);
                if (prep_chunks[i]) {
                    crawl_bin_free_chunk_array_rows(prep_chunks[i], prep_chunk_counts[i]);
                    prep_chunks[i] = NULL;
                }
                continue;
            }
            chunk_count += prep_chunk_counts[i];
        }

        if (chunk_count > 0) {
            file_chunk_t *merged;

            merged = (file_chunk_t *)malloc(chunk_count * sizeof(file_chunk_t));
            if (!merged) {
                fprintf(stderr, "allocation failed\n");
                ereport_stats_stop_request(&run_stats);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
                for (i = 0; (size_t)i < path_count; i++) {
                    if (prep_rc[i] == 0 && prep_chunks[i]) crawl_bin_free_chunk_array_rows(prep_chunks[i], prep_chunk_counts[i]);
                }
                free(chunk_targets);
                free(prep_rc);
                free(prep_chunks);
                free(prep_chunk_counts);
                for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                free(paths);
                ereport_free_file_states(file_states, path_count);
                return 1;
            }

            merge_off = 0;
            for (i = 0; (size_t)i < path_count; i++) {
                if (prep_rc[i] != 0) continue;
                memcpy(merged + merge_off, prep_chunks[i], prep_chunk_counts[i] * sizeof(file_chunk_t));
                merge_off += prep_chunk_counts[i];
                free(prep_chunks[i]);
                prep_chunks[i] = NULL;
                atomic_store(&file_states[i].remaining_chunks,
                             prep_chunk_counts[i] > (size_t)UINT_MAX ? UINT_MAX : (unsigned int)prep_chunk_counts[i]);
            }
            chunks = merged;
        }

chunks_ready:
        free(chunk_targets);
        free(prep_rc);
        free(prep_chunks);
        free(prep_chunk_counts);
        if (g_ereport_verbose && vt_chunk0 > 0.0) run_stats.vt_chunk_map_sec += now_sec() - vt_chunk0;

        run_stats.chunk_prep_files_total = 0;
        atomic_store(&run_stats.chunk_prep_files_done, 0ULL);
    }

    /* The mappings and shard fds have served their purpose; the resolved per-shard roots in
     * sidecar_scope are all the scan still needs. */
    crawl_sidecar_close(&sidecar);

    if (chunk_count == 0) {
        fprintf(stderr, "no readable chunk work found in %s\n", input_dirs_label);
        if (stats_thread_started) {
            ereport_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
        free(paths);
        ereport_free_file_states(file_states, path_count);
        return 1;
    }

    for (i = 0; (size_t)i < path_count; i++) {
        if (atomic_load(&file_states[i].remaining_chunks) == 0U) continue;
        if (ereport_attach_shard_catalog(&file_states[i], paths[i]) != 0) {
            fprintf(stderr, "ereport: cannot load directory catalog from %s\n", paths[i]);
            if (stats_thread_started) {
                ereport_stats_stop_request(&run_stats);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
            }
            for (i = 0; (size_t)i < chunk_count; i++) free(chunks[i].path);
            free(chunks);
            chunks = NULL;
            for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
            free(paths);
            ereport_free_file_states(file_states, path_count);
            return 1;
        }
    }

    /*
     * --index-dir: close the subtree over each shard's parent pointers, once, so the scan can
     * answer "is this record's parent inside?" with a bit test.
     *
     * All or nothing. A shard whose catalog will not yield a bitmap leaves every shard on the
     * path-prefix route, which is the same answer by a slower road -- and one already-pruned chunk
     * list stays correct either way, because a pruned group held no matching record to begin with.
     */
    if (sidecar_scope) {
        size_t fi;
        int ok = 1;

        for (fi = 0; fi < path_count && ok; fi++) {
            if (!file_states[fi].catalog) continue;
            file_states[fi].subtree_bits = ereport_subtree_bits(file_states[fi].catalog, sidecar_scope[fi].root);
            file_states[fi].subtree_root_parent = sidecar_scope[fi].root_parent;
            if (!file_states[fi].subtree_bits) ok = 0;
        }
        if (!ok) {
            fprintf(stderr, "ereport: dir index unusable for this capture; --subtree falls back to path matching\n");
            for (fi = 0; fi < path_count; fi++) {
                free(file_states[fi].subtree_bits);
                file_states[fi].subtree_bits = NULL;
                file_states[fi].subtree_root_parent = 0ULL;
            }
            /* g_rgix_stats stays: the chunk list is already pruned, and the record credit below is
             * what keeps "Scanned records" meaning the same thing it did. */
        } else {
            g_subtree_from_index = 1;
        }
        /* Everything the scan needs is now in the per-shard bitmaps. */
        free(sidecar_scope);
        sidecar_scope = NULL;
    }

    /* Now that the catalogs are in, the number of directories across the shards being read is known, and
     * that is the bound on distinct parents. Chunks are handed out on demand, so the per-worker share is
     * the even split; a worker that draws more than its share still grows its tables from there.
     *
     * Deliberately not divided by the *chunk* count instead, even though with fewer chunks than threads
     * only a few workers run and each does see a larger slice. Sizing the first table for that measured
     * ~45% slower in the parse phase (0.50 s -> 0.73 s on a 1.3M-record capture, identical fault counts,
     * with or without the huge-page hint): a table that starts small stays in cache while it fills and
     * the doubling copies are cheap, whereas one that starts at its final size makes every insert a
     * cache miss from the first record. Under-reserving and growing is the faster mistake to make. */
    {
        size_t fi;

        for (fi = 0; fi < path_count; fi++)
            if (file_states[fi].catalog) catalog_dirs_total += file_states[fi].catalog->max_dir_id;
        if (threads > 0) expect_parents_per_worker = (size_t)(catalog_dirs_total / (uint64_t)threads) + 1U;
    }

    threads_used = threads;
    now = time(NULL);

    memset(&queue, 0, sizeof(queue));
    queue.chunks = chunks;
    queue.count = chunk_count;
    queue.next_index = 0;
    pthread_mutex_init(&queue.mutex, NULL);

    if (inode_set_init(&seen_inodes, 65536) != 0) {
        size_t k;
        fprintf(stderr, "allocation failed\n");
        for (k = 0; k < chunk_count; k++) free(chunks[k].path);
        free(chunks);
        chunks = NULL;
        if (stats_thread_started) {
            ereport_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (k = 0; k < path_count; k++) free(paths[k]);
        free(paths);
        ereport_free_file_states(file_states, path_count);
        pthread_mutex_destroy(&queue.mutex);
        return 1;
    }

    tids = (pthread_t *)calloc((size_t)threads, sizeof(*tids));
    args = (worker_arg_t *)calloc((size_t)threads, sizeof(*args));
    if (!tids || !args) {
        size_t k;
        fprintf(stderr, "allocation failed\n");
        free(tids);
        worker_path_arenas_destroy(args, threads);
        free(args);
        for (k = 0; k < path_count; k++) free(paths[k]);
        free(paths);
        pthread_mutex_destroy(&queue.mutex);
        inode_set_destroy(&seen_inodes);
        ereport_free_file_states(file_states, path_count);
        return 1;
    }

    for (i = 0; i < threads; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].queue = &queue;
        args[i].file_states = file_states;
        args[i].target_uid = target_uid;
        args[i].all_users = all_users_mode;
        args[i].bucket_detail_levels = bucket_detail_levels;
        args[i].basis = basis;
        args[i].now = now;
        args[i].seen_inodes = &seen_inodes;
        args[i].run_stats = &run_stats;
        args[i].expect_parents = expect_parents_per_worker;

        /* Route this worker's path strings into its own arena; its append-side
         * structures must never individually free arena-owned strings. */
        args[i].matched_records.arena = &args[i].path_arena;
        args[i].matched_records.paths_external = 1;
        {
            int ab2, sb2;
            for (ab2 = 0; ab2 < AGE_BUCKETS; ab2++) {
                for (sb2 = 0; sb2 < SIZE_BUCKETS; sb2++) {
                    args[i].details[ab2][sb2].arena = &args[i].path_arena;
                    args[i].details[ab2][sb2].paths_external = 1;
                    /* Per-cell dense parents are interned once per worker (read_one_chunk's
                     * dense_pintern, backed by this same arena) and shared across all 36
                     * age/size cells, so each node only references the interned string. This
                     * removes the dominant per-cell parent-string duplication at peak. */
                    args[i].dense_maps[ab2][sb2].parents_external = 1;
                }
            }
        }
        /* parent_fanout and parent_fanout_stats are fed the same per-worker interned no-slash
         * parent (shp) the dense maps use, so they reference it instead of keeping their own
         * inline/strdup copy — dropping two further full copies of every parent path (~80 GiB on
         * the production crawl). */
        args[i].parent_fanout.parents_external = 1;
        args[i].parent_fanout_stats.parents_external = 1;
        /* Both are keyed by every parent this worker sees, the same cardinality as its interns. */
        args[i].parent_fanout.reserve_buckets =
            parent_table_size_for(expect_parents_per_worker, 1, 1, PARENT_TABLE_RESERVE_MAX_SLOTS);
        args[i].parent_fanout_stats.reserve_buckets = args[i].parent_fanout.reserve_buckets;

        if (all_users_mode) {
            if (uid_accum_init(&args[i].uid_distinct, 8192) != 0) {
                int j;
                fprintf(stderr, "allocation failed\n");
                threads_used = i;
                for (j = 0; j < i; j++) pthread_join(tids[j], NULL);
                for (j = 0; j < i; j++) uid_accum_destroy(&args[j].uid_distinct);
                if (stats_thread_started) {
                    ereport_stats_stop_request(&run_stats);
                    pthread_join(stats_thread, NULL);
                    clear_status_line();
                }
                worker_dense_maps_free(args, i);
                free(tids);
                worker_path_arenas_destroy(args, threads);
                free(args);
                for (j = 0; j < (int)chunk_count; j++) free(chunks[j].path);
                free(chunks);
                for (j = 0; (size_t)j < path_count; j++) free(paths[j]);
                free(paths);
                ereport_free_file_states(file_states, path_count);
                pthread_mutex_destroy(&queue.mutex);
                inode_set_destroy(&seen_inodes);
                return 1;
            }
        }

        if (pthread_create(&tids[i], NULL, worker_main, &args[i]) != 0) {
            fprintf(stderr, "failed to create thread %d\n", i);
            threads_used = i;
            break;
        }
    }

    memset(&final_sum, 0, sizeof(final_sum));
    memset(final_details, 0, sizeof(final_details));
    memset(&final_matched_records, 0, sizeof(final_matched_records));
    /* Final structures only ever receive path pointers transferred out of the
     * per-worker arenas, so they must never individually free those strings. */
    final_matched_records.paths_external = 1;
    {
        int ab2, sb2;
        for (ab2 = 0; ab2 < AGE_BUCKETS; ab2++) {
            for (sb2 = 0; sb2 < SIZE_BUCKETS; sb2++) final_details[ab2][sb2].paths_external = 1;
        }
    }

    {
        double vt_parse0 = 0.0;

        if (g_ereport_verbose) vt_parse0 = now_sec();
        for (i = 0; i < threads_used; i++) pthread_join(tids[i], NULL);
        if (g_ereport_verbose && vt_parse0 > 0.0) run_stats.vt_parse_workers_sec += now_sec() - vt_parse0;

        if (getenv("EREPORT_MEMSTATS")) {
            clear_status_line();
            ereport_print_memstats(args, threads_used, &seen_inodes, file_states, path_count);
        }

        /* The shard catalogs back path reconstruction during the read phase only; nothing in
         * finalize/emission touches them. Free them now (workers have joined) so their footprint
         * is reclaimed before the finalize merges and the matched/details gather allocate, cutting
         * the finalize-phase peak RSS. The array itself is freed later by ereport_free_file_states. */
        ereport_free_shard_catalogs(file_states, path_count);

        /* Progress thread: keep parsing status (rec/s line) until workers exit; then finalize banner. */
        atomic_store(&run_stats.parse_workers_done, 1);
        atomic_store(&run_stats.finalize_merge_substep, 1);
        atomic_store(&run_stats.finalize_phase, 1);

        {
            double __vt = 0.0;

            if (g_ereport_verbose) __vt = now_sec();
            summary_reduce_from_worker_args(&final_sum, args, threads_used);
            /*
             * Credit the records in the row groups pruning removed. "Scanned records" is the size
             * of what the run had to account for, not how much of it happened to be decoded; a
             * --subtree report has to read the same whether or not an index dir was handed to it,
             * and the pruned groups are exactly the ones proven to hold nothing in scope. What the
             * pruning actually saved is reported on stderr and under --verbose.
             */
            if (g_rgix_stats.used && g_rgix_stats.records_total > g_rgix_stats.records_kept)
                final_sum.scanned_records += g_rgix_stats.records_total - g_rgix_stats.records_kept;
            if (g_ereport_verbose && __vt > 0.0) run_stats.vt_fini_summaries_sec += now_sec() - __vt;
        }
    }

    memset(&path_shape, 0, sizeof(path_shape));

    dense_cell_map_t merged_dense_shape[AGE_BUCKETS][SIZE_BUCKETS];
    dense_cell_fanout_lookup_t fanout_lookup_shape;
    fanout_parent_stat_map_t merged_fanout_stats;
    int bucket_shape_maps_live = 0;

    memset(&merged_dense_shape, 0, sizeof(merged_dense_shape));
    memset(&fanout_lookup_shape, 0, sizeof(fanout_lookup_shape));
    memset(&merged_fanout_stats, 0, sizeof(merged_fanout_stats));
    /* The merged maps receive the per-worker nodes by relink/absorb, which reference interned
     * arena parents — mark them external so neither merge nor free touches those strings. */
    merged_fanout_stats.parents_external = 1;
    merged_fanout_stats.reserve_buckets =
        parent_table_size_for((size_t)catalog_dirs_total, 1, 1, MERGED_TABLE_RESERVE_MAX_BUCKETS);
    {
        int ab2, sb2;
        for (ab2 = 0; ab2 < AGE_BUCKETS; ab2++) {
            for (sb2 = 0; sb2 < SIZE_BUCKETS; sb2++)
                merged_dense_shape[ab2][sb2].parents_external = 1;
        }
    }

    if (bucket_detail_levels > 0) {
        atomic_store(&run_stats.finalize_merge_substep, 2);
        {
            double __vt = 0.0;

            if (g_ereport_verbose) __vt = now_sec();
            if (matched_records_finalize_parallel(args, threads_used, &final_matched_records, &run_stats) != 0) {
                fprintf(stderr, "allocation failed merging matched records\n");
                if (stats_thread_started) {
                    ereport_stats_stop_request(&run_stats);
                    pthread_join(stats_thread, NULL);
                    clear_status_line();
                }
                matched_records_free(&final_matched_records);
                for (ab = 0; ab < AGE_BUCKETS; ab++) {
                    for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
                }
                worker_dense_maps_free(args, threads_used);
                free(tids);
                if (all_users_mode) {
                    for (i = 0; i < threads; i++) uid_accum_destroy(&args[i].uid_distinct);
                }
                worker_path_arenas_destroy(args, threads);
                free(args);
                for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
                free(chunks);
                for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                free(paths);
                ereport_free_file_states(file_states, path_count);
                pthread_mutex_destroy(&queue.mutex);
                inode_set_destroy(&seen_inodes);
                return 1;
            }
            if (g_ereport_verbose && __vt > 0.0) run_stats.vt_fini_matched_paths_sec += now_sec() - __vt;
        }

        {
            dense_cell_map_t merged_fanout;

            atomic_store(&run_stats.finalize_fanout_workers_total, threads_used);
            atomic_store(&run_stats.finalize_fanout_workers_done, 0);
            atomic_store(&run_stats.finalize_merge_substep, 3);
            {
                double __vt_dense = 0.0;

                if (g_ereport_verbose) __vt_dense = now_sec();
                memset(&merged_fanout, 0, sizeof(merged_fanout));
                merged_fanout.parents_external = 1; /* receives interned per-worker fanout nodes by relink */
                merged_fanout.reserve_buckets =
                    parent_table_size_for((size_t)catalog_dirs_total, 1, 1, MERGED_TABLE_RESERVE_MAX_BUCKETS);
                if (fanout_shard_summaries_reduce_parallel(&merged_fanout, &merged_fanout_stats, args, threads_used,
                                                          &run_stats) != 0) {
                    for (i = 0; i < threads_used; i++) {
                        dense_cell_merge_into_ex(&merged_fanout, &args[i].parent_fanout, NULL,
                                                 parse_ereport_thread_count());
                        fanout_parent_stat_merge_into_ex(&merged_fanout_stats, &args[i].parent_fanout_stats, NULL,
                                                         parse_ereport_thread_count());
                        atomic_store(&run_stats.finalize_fanout_workers_done, i + 1);
                    }
                }

                if (bucket_dense_cells_finalize_parallel(args, threads_used, final_details, merged_dense_shape,
                                                         &run_stats) != 0) {
                    fprintf(stderr, "allocation failed merging bucket details\n");
                    dense_cell_free(&merged_fanout);
                    fanout_parent_stat_map_free(&merged_fanout_stats);
                    memset(&merged_fanout_stats, 0, sizeof(merged_fanout_stats));
                    for (ab = 0; ab < AGE_BUCKETS; ab++) {
                        for (sb = 0; sb < SIZE_BUCKETS; sb++) dense_cell_free(&merged_dense_shape[ab][sb]);
                    }
                    if (stats_thread_started) {
                        ereport_stats_stop_request(&run_stats);
                        pthread_join(stats_thread, NULL);
                        clear_status_line();
                    }
                    matched_records_free(&final_matched_records);
                    for (ab = 0; ab < AGE_BUCKETS; ab++) {
                        for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
                    }
                    worker_dense_maps_free(args, threads_used);
                    free(tids);
                    if (all_users_mode) {
                        for (i = 0; i < threads; i++) uid_accum_destroy(&args[i].uid_distinct);
                    }
                    worker_path_arenas_destroy(args, threads);
                    free(args);
                    for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
                    free(chunks);
                    for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                    free(paths);
                    ereport_free_file_states(file_states, path_count);
                    pthread_mutex_destroy(&queue.mutex);
                    inode_set_destroy(&seen_inodes);
                    return 1;
                }
                atomic_store(&run_stats.finalize_lookup_stage, 1U);
                dense_cell_steal_into_fanout_lookup_ex(&merged_fanout, &fanout_lookup_shape, parse_ereport_thread_count());
                dense_cell_free(&merged_fanout);
                if (g_ereport_verbose && __vt_dense > 0.0)
                    run_stats.vt_fini_directory_maps_dense_sec += now_sec() - __vt_dense;
            }

            {
                double __vt_ps = 0.0;

                if (g_ereport_verbose) __vt_ps = now_sec();
                path_shape_fill_from_merged_dense(&final_sum, merged_dense_shape, &fanout_lookup_shape, &path_shape,
                                                  &run_stats);
                atomic_store(&run_stats.finalize_lookup_stage, 0U);
                if (g_ereport_verbose && __vt_ps > 0.0) run_stats.vt_fini_path_shape_sec += now_sec() - __vt_ps;
            }
            bucket_shape_maps_live = 1;
        }
    }

    if (all_users_mode) {
        double __vt_uid = 0.0;

        if (g_ereport_verbose) __vt_uid = now_sec();
        atomic_store(&run_stats.finalize_merge_substep, 4);
        uid_accum_t merged_uids;
        if (uid_accum_init(&merged_uids, 65536) != 0) {
            fprintf(stderr, "allocation failed merging uid tallies\n");
            if (bucket_shape_maps_live) {
                bucket_shape_maps_destroy(&fanout_lookup_shape, merged_dense_shape, &merged_fanout_stats);
                bucket_shape_maps_live = 0;
            }
            for (i = 0; i < threads; i++) uid_accum_destroy(&args[i].uid_distinct);
            worker_dense_maps_free(args, threads_used);
            free(tids);
            worker_path_arenas_destroy(args, threads);
            free(args);
            for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
            free(chunks);
            for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
            free(paths);
            ereport_free_file_states(file_states, path_count);
            pthread_mutex_destroy(&queue.mutex);
            inode_set_destroy(&seen_inodes);
            matched_records_free(&final_matched_records);
            for (ab = 0; ab < AGE_BUCKETS; ab++) {
                for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
            }
            if (g_ereport_verbose && __vt_uid > 0.0) run_stats.vt_fini_uid_sec += now_sec() - __vt_uid;
            return 1;
        }
        if (uid_accum_reduce_workers_into(&merged_uids, args, threads_used) != 0) {
                int j;
                fprintf(stderr, "allocation failed merging uid tallies\n");
                if (bucket_shape_maps_live) {
                    bucket_shape_maps_destroy(&fanout_lookup_shape, merged_dense_shape, &merged_fanout_stats);
                    bucket_shape_maps_live = 0;
                }
                uid_accum_destroy(&merged_uids);
                for (j = 0; j < threads; j++) uid_accum_destroy(&args[j].uid_distinct);
                worker_dense_maps_free(args, threads_used);
                free(tids);
                worker_path_arenas_destroy(args, threads);
                free(args);
                for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
                free(chunks);
                for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                free(paths);
                ereport_free_file_states(file_states, path_count);
                pthread_mutex_destroy(&queue.mutex);
                inode_set_destroy(&seen_inodes);
                matched_records_free(&final_matched_records);
                for (ab = 0; ab < AGE_BUCKETS; ab++) {
                    for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
                }
                if (g_ereport_verbose && __vt_uid > 0.0) run_stats.vt_fini_uid_sec += now_sec() - __vt_uid;
                return 1;
        }
        for (i = threads_used; i < threads; i++) uid_accum_destroy(&args[i].uid_distinct);
        distinct_uid_count = (uint64_t)uid_accum_size(&merged_uids);
        uid_accum_destroy(&merged_uids);
        if (g_ereport_verbose && __vt_uid > 0.0) run_stats.vt_fini_uid_sec += now_sec() - __vt_uid;
    }

    atomic_store(&run_stats.finalize_merge_substep, 0);

    if (ensure_bucket_output_dir_exists() != 0) {
        fprintf(stderr, "failed to create report output directory %s: %s\n", g_bucket_output_dir, strerror(errno));
        if (bucket_shape_maps_live) {
            bucket_shape_maps_destroy(&fanout_lookup_shape, merged_dense_shape, &merged_fanout_stats);
            bucket_shape_maps_live = 0;
        }
        if (stats_thread_started) {
            ereport_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
        }
        worker_dense_maps_free(args, threads_used);
        free(tids);
        worker_path_arenas_destroy(args, threads);
        free(args);
        for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
        free(chunks);
        for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
        free(paths);
        ereport_free_file_states(file_states, path_count);
        pthread_mutex_destroy(&queue.mutex);
        inode_set_destroy(&seen_inodes);
        matched_records_free(&final_matched_records);
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
        }
        return 1;
    }
    (void)path_try_resolve_inplace(g_bucket_output_dir, sizeof(g_bucket_output_dir));

    if (emit_all_bucket_detail_pages(display_name, all_users_mode, distinct_uid_count, basis_str,
                                     bucket_detail_levels,
                                     &final_sum,
                                     &path_shape,
                                     final_details,
                                     &final_matched_records,
                                     &run_stats,
                                     bucket_detail_levels > 0 ? merged_dense_shape : NULL,
                                     bucket_detail_levels > 0 ? &fanout_lookup_shape : NULL,
                                     bucket_detail_levels > 0 ? &merged_fanout_stats : NULL) != 0) {
        fprintf(stderr, "failed to write bucket detail pages\n");
    } else {
        bucket_pages_written = AGE_BUCKETS * SIZE_BUCKETS;
    }

    if (bucket_shape_maps_live) {
        bucket_shape_maps_destroy(&fanout_lookup_shape, merged_dense_shape, &merged_fanout_stats);
        bucket_shape_maps_live = 0;
    }

    atomic_store(&run_stats.finalize_index_step, 0);
    atomic_store(&run_stats.finalize_phase, 3);

    {
        double __vt_idx = 0.0;

        if (g_ereport_verbose) __vt_idx = now_sec();
        if (emit_html(report_path, display_name, all_users_mode, distinct_uid_count, bucket_detail_levels, target_uid,
                       basis_str, &final_sum, &path_shape, path_count, threads_used, bin_dir_count,
                       storage_base_paths_label, &crawl_timing, &manifest_disk, &run_stats) != 0) {
            fprintf(stderr, "failed to write main report %s\n", report_path);
        }
        if (g_ereport_verbose && __vt_idx > 0.0) run_stats.vt_index_html_sec += now_sec() - __vt_idx;
    }
    final_sum.scanned_input_files = (uint64_t)path_count;
    t1 = now_sec();
    if (stats_thread_started) {
        ereport_stats_stop_request(&run_stats);
        pthread_join(stats_thread, NULL);
        clear_status_line();
    }
    emit_run_stats(display_name, all_users_mode, distinct_uid_count, bucket_detail_levels, target_uid, basis_str,
                   input_dirs_label, report_path, path_count, threads, threads_used, &final_sum, &run_stats,
                   bucket_pages_written, t1 - t0, &manifest_disk);

    free(tids);
    /* Report emission is complete; the final structures no longer dereference
     * arena-backed path strings (their frees below are path-external no-ops). */
    worker_path_arenas_destroy(args, threads);
    free(args);
    for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
    free(chunks);
    for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
    free(paths);
    ereport_free_file_states(file_states, path_count);
    pthread_mutex_destroy(&queue.mutex);
    inode_set_destroy(&seen_inodes);
    matched_records_free(&final_matched_records);
    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
    }

    return 0;
}
