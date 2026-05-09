/*
 * ereport.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Parallel reader for crawl bin files produced by ecrawl.
 * Emits the original HTML summary plus per-bucket drilldown pages with
 * dense level-1/level-2 directory summaries. Path search in index.html uses
 * GET /<user>/search on eserve (ereport_index trigram index under ./index/).
 *
 * Build:
 *   gcc -O2 -Wall -Wextra -pthread -o ereport ereport.c
 *
 * Usage:
 *   ./ereport [--bucket-details N] [--report-dir DIR] [--heat-ctime-led-min-share F] <username|uid> [<atime|mtime|ctime|effective>] [bin_dir ...]
 *   ./ereport [--bucket-details N] [--report-dir DIR] [--heat-ctime-led-min-share F] [<atime|mtime|ctime|effective>] [bin_dir ...]
 *   When the time argument is omitted (single-user form), age buckets use effective time: max(atime,mtime,ctime).
 *     --bucket-details N (optional): emit N levels of per-bucket directory tables (1…32); if omitted,
 *     bucket pages are brief summaries only.
 *     --report-dir DIR (optional): write reports under DIR/(sanitized user or all_users)/ instead of cwd.
 *     --heat-ctime-led-min-share F (optional): purple C-led badge/pill only when C-led bytes are ≥ F of slice (0<F≤1;
 *     default 0.30). Override env EREPORT_HEAT_CTIME_LED_MIN_SHARE; CLI wins when both are set.
 *     Flags must appear first (in any order), before username/time basis.
 *     (omit username: aggregate report for all UIDs in the crawl; output under ./all_users/)
 * Parallel thread count: EREPORT_THREADS (default 32); see worker_main / stats_thread / bucket HTML emit.
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

#include "crawl_bin_catalog.h"
#include "crawl_bin_chunks.h"
#include "crawl_ckpt.h"
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
#define DENSE_PARENT_BUCKETS 512U
/* Wider hash only for merged crawl-wide fanout — shortens chains for path-shape lookups (stack ~512 KiB). */
#define DENSE_PARENT_BUCKETS_FANOUT_LOOKUP 65536U
#define SUMMARY_REDUCE_MIN_CHUNK 4
#define PATH_ORD_QSORT_THRESH (65536u)
#define PATH_ORD_MAX_MS_DEPTH 7

static int parse_ereport_thread_count(void);

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

typedef struct {
    char *path;
    uint64_t size;
    uint8_t ctime_led;
} detail_record_t;

typedef struct {
    detail_record_t *items;
    size_t count;
    size_t cap;
} bucket_details_t;

typedef struct {
    char *path;
    uint8_t type;
    uint64_t size;
} matched_record_t;

typedef struct {
    matched_record_t *items;
    size_t count;
    size_t cap;
} matched_records_t;

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
} file_state_t;

typedef struct {
    uint32_t dev_major;
    uint32_t dev_minor;
    uint64_t inode;
} inode_key_t;

typedef struct {
    inode_key_t *keys;
    unsigned char *used;
    size_t cap;
    size_t count;
    pthread_mutex_t mutex;
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
    atomic_uint finalize_bucket_done; /* cells written toward 36 */
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
} ereport_run_stats_t;

static void ereport_run_stats_reset(ereport_run_stats_t *s) {
    atomic_store(&s->scanned_input_files, 0);
    atomic_store(&s->scanned_records, 0);
    atomic_store(&s->matched_records, 0);
    atomic_store(&s->bad_input_files, 0);
    atomic_store(&s->stop_stats, 0);
    atomic_store(&s->parse_workers_done, 0);
    atomic_store(&s->finalize_phase, 0);
    atomic_store(&s->finalize_bucket_done, 0);
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
}

typedef struct dense_node {
    struct dense_node *next;
    char *parent;
    uint64_t n;
} dense_node_t;

typedef struct {
    dense_node_t *buckets[DENSE_PARENT_BUCKETS];
} dense_cell_map_t;

typedef struct {
    dense_node_t *buckets[DENSE_PARENT_BUCKETS_FANOUT_LOOKUP];
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
    unsigned char have_time;
} fanout_parent_stat_node_t;

typedef struct {
    fanout_parent_stat_node_t *buckets[DENSE_PARENT_BUCKETS_FANOUT_LOOKUP];
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
    ereport_run_stats_t *run_stats;
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

/* Manifest-driven crawl directory layout: "uid_shards" or "unsharded" (no uid-shard manifest). */
static const char *g_input_layout = "unsharded";
static uint32_t g_input_uid_shards = 0;
/* Set once from resolved login name via set_bucket_output_dir(); see main(). Not "." / not "tmp" unless fallback. */
static char g_bucket_output_dir[PATH_MAX];

typedef struct {
    char *path;
    uint64_t bucket_files;
    uint64_t bucket_bytes;
    uint64_t bucket_ctime_led_files;
    uint64_t bucket_ctime_led_bytes;
    uint64_t total_files;
    uint64_t total_dirs;
    uint64_t total_bytes;
} path_row_t;

typedef struct {
    path_row_t *rows;
    unsigned char *used;
    size_t cap;
    size_t count;
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

static int inode_set_init(inode_set_t *s, size_t initial_cap) {
    size_t cap = 1;
    while (cap < initial_cap) cap <<= 1;

    s->keys = (inode_key_t *)calloc(cap, sizeof(*s->keys));
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
    pthread_mutex_init(&s->mutex, NULL);
    return 0;
}

static void inode_set_destroy(inode_set_t *s) {
    free(s->keys);
    free(s->used);
    s->keys = NULL;
    s->used = NULL;
    s->cap = 0;
    s->count = 0;
    pthread_mutex_destroy(&s->mutex);
}

static int inode_set_rehash_locked(inode_set_t *s, size_t new_cap) {
    inode_key_t *new_keys = (inode_key_t *)calloc(new_cap, sizeof(*new_keys));
    unsigned char *new_used = (unsigned char *)calloc(new_cap, sizeof(*new_used));
    size_t i;

    if (!new_keys || !new_used) {
        free(new_keys);
        free(new_used);
        return -1;
    }

    for (i = 0; i < s->cap; i++) {
        if (s->used[i]) {
            inode_key_t key = s->keys[i];
            size_t idx = (size_t)(inode_key_hash(key.dev_major, key.dev_minor, key.inode) & (new_cap - 1));
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

static int inode_set_insert_if_new(inode_set_t *s, uint32_t dev_major, uint32_t dev_minor, uint64_t inode) {
    size_t idx;

    if (inode == 0) return 1;

    pthread_mutex_lock(&s->mutex);

    if ((s->count + 1) * 10 >= s->cap * 7) {
        if (inode_set_rehash_locked(s, s->cap << 1) != 0) {
            pthread_mutex_unlock(&s->mutex);
            return -1;
        }
    }

    idx = (size_t)(inode_key_hash(dev_major, dev_minor, inode) & (s->cap - 1));
    while (s->used[idx]) {
        inode_key_t *k = &s->keys[idx];
        if (k->dev_major == dev_major && k->dev_minor == dev_minor && k->inode == inode) {
            pthread_mutex_unlock(&s->mutex);
            return 0;
        }
        idx = (idx + 1) & (s->cap - 1);
    }

    s->used[idx] = 1;
    s->keys[idx].dev_major = dev_major;
    s->keys[idx].dev_minor = dev_minor;
    s->keys[idx].inode = inode;
    s->count++;

    pthread_mutex_unlock(&s->mutex);
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

static int bucket_details_append(bucket_details_t *b, const char *path, uint64_t size, int ctime_led) {
    detail_record_t *tmp;

    if (b->count == b->cap) {
        size_t new_cap = (b->cap == 0) ? 256 : b->cap * 2;
        tmp = (detail_record_t *)realloc(b->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        b->items = tmp;
        b->cap = new_cap;
    }

    b->items[b->count].path = strdup(path ? path : "");
    if (!b->items[b->count].path) return -1;
    b->items[b->count].size = size;
    b->items[b->count].ctime_led = (uint8_t)(ctime_led ? 1 : 0);
    b->count++;
    return 0;
}

static void bucket_details_free(bucket_details_t *b) {
    size_t i;
    for (i = 0; i < b->count; i++) free(b->items[i].path);
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

    m->items[m->count].path = strdup(path ? path : "");
    if (!m->items[m->count].path) return -1;
    m->items[m->count].type = type;
    m->items[m->count].size = size;
    m->count++;
    return 0;
}

static void matched_records_free(matched_records_t *m) {
    size_t i;
    for (i = 0; i < m->count; i++) free(m->items[i].path);
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

static FILE *counted_fopen(const char *path, const char *mode) {
    atomic_fetch_add(&g_io_fopen_calls, 1);
    return fopen(path, mode);
}

static int counted_fclose(FILE *fp) {
    atomic_fetch_add(&g_io_fclose_calls, 1);
    return fclose(fp);
}

static size_t counted_fread(void *ptr, size_t size, size_t nmemb, FILE *fp) {
    atomic_fetch_add(&g_io_fread_calls, 1);
    return fread(ptr, size, nmemb, fp);
}

static const crawl_bin_chunk_stdio_t ereport_chunk_io = {counted_fopen, counted_fread, counted_fclose};

static DIR *counted_opendir(const char *path) {
    atomic_fetch_add(&g_io_opendir_calls, 1);
    return opendir(path);
}

static struct dirent *counted_readdir(DIR *dir) {
    atomic_fetch_add(&g_io_readdir_calls, 1);
    return readdir(dir);
}

static int counted_closedir(DIR *dir) {
    atomic_fetch_add(&g_io_closedir_calls, 1);
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
 * Prefer record_root (logical storage id) when present in crawl_manifest.txt,
 * else start_path (filesystem crawl root). Returns 0 when a path was filled.
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

static uint32_t dense_parent_bucket(const char *parent) {
    uint32_t h = 2166136261u;

    for (; *parent; parent++) {
        h ^= (uint32_t)(unsigned char)*parent;
        h *= 16777619u;
    }
    return h % (uint32_t)DENSE_PARENT_BUCKETS;
}

static uint32_t dense_parent_bucket_fanout_lookup(const char *parent) {
    uint32_t h = 2166136261u;

    for (; *parent; parent++) {
        h ^= (uint32_t)(unsigned char)*parent;
        h *= 16777619u;
    }
    return h % (uint32_t)DENSE_PARENT_BUCKETS_FANOUT_LOOKUP;
}

static int dense_cell_add(dense_cell_map_t *m, const char *parent, uint64_t delta, summary_t *sum) {
    uint32_t bi = dense_parent_bucket(parent);
    dense_node_t **pp = &m->buckets[bi];
    dense_node_t *node;

    while (*pp) {
        if (strcmp((*pp)->parent, parent) == 0) {
            (*pp)->n += delta;
            return 0;
        }
        pp = &(*pp)->next;
    }
    node = (dense_node_t *)malloc(sizeof(*node));
    if (!node) {
        if (sum) sum->bad_input_files++;
        return -1;
    }
    node->parent = strdup(parent);
    if (!node->parent) {
        free(node);
        if (sum) sum->bad_input_files++;
        return -1;
    }
    node->n = delta;
    node->next = m->buckets[bi];
    m->buckets[bi] = node;
    return 0;
}

static void dense_cell_merge_bucket_range(dense_cell_map_t *dst, dense_cell_map_t *src, summary_t *sum, size_t bi_lo, size_t bi_hi) {
    size_t bi;

    if (!dst || !src) return;
    for (bi = bi_lo; bi < bi_hi; bi++) {
        dense_node_t *n = src->buckets[bi];

        src->buckets[bi] = NULL;
        while (n) {
            dense_node_t *nx = n->next;
            if (dense_cell_add(dst, n->parent, n->n, sum) != 0) {
                /* Leak avoidance on OOM: drop remaining src chain. */
                free(n->parent);
                free(n);
                n = nx;
                while (n) {
                    nx = n->next;
                    free(n->parent);
                    free(n);
                    n = nx;
                }
                break;
            }
            free(n->parent);
            free(n);
            n = nx;
        }
    }
}

/*
 * Merge src into dst (single-threaded over buckets). Parallel bucket slicing was removed after field
 * reports of crashes under high thread counts; dense_cell_add walks singly-linked chains and must not
 * race with peers even when bucket indices partition src.
 */
static void dense_cell_merge_into(dense_cell_map_t *dst, dense_cell_map_t *src, summary_t *sum) {
    if (!dst || !src) return;
    dense_cell_merge_bucket_range(dst, src, sum, 0, DENSE_PARENT_BUCKETS);
}

static void dense_cell_merge_add(dense_cell_map_t *dst, const dense_cell_map_t *src, summary_t *sum) {
    size_t bi;

    if (!dst || !src) return;
    for (bi = 0; bi < DENSE_PARENT_BUCKETS; bi++) {
        const dense_node_t *n;

        for (n = src->buckets[bi]; n; n = n->next) {
            if (dense_cell_add(dst, n->parent, n->n, sum) != 0) return;
        }
    }
}

static void dense_cell_steal_into_fanout_lookup(dense_cell_map_t *narrow, dense_cell_fanout_lookup_t *lk) {
    size_t bi;

    if (!narrow || !lk) return;
    for (bi = 0; bi < DENSE_PARENT_BUCKETS; bi++) {
        dense_node_t *n = narrow->buckets[bi];

        narrow->buckets[bi] = NULL;
        while (n) {
            dense_node_t *nx = n->next;
            uint32_t biw = dense_parent_bucket_fanout_lookup(n->parent);

            n->next = lk->buckets[biw];
            lk->buckets[biw] = n;
            n = nx;
        }
    }
}

static uint64_t dense_cell_fanout_lookup_get_n(const dense_cell_fanout_lookup_t *lk, const char *parent) {
    uint32_t biw;
    const dense_node_t *n;

    if (!lk || !parent) return 0;
    biw = dense_parent_bucket_fanout_lookup(parent);
    for (n = lk->buckets[biw]; n; n = n->next) {
        if (strcmp(n->parent, parent) == 0) return n->n;
    }
    return 0;
}

static uint64_t dense_cell_map_get_n(const dense_cell_map_t *m, const char *parent) {
    uint32_t bi;
    const dense_node_t *n;

    if (!m || !parent) return 0;
    bi = dense_parent_bucket(parent);
    for (n = m->buckets[bi]; n; n = n->next) {
        if (strcmp(n->parent, parent) == 0) return n->n;
    }
    return 0;
}

static void dense_cell_fanout_lookup_free(dense_cell_fanout_lookup_t *lk) {
    size_t bi;

    if (!lk) return;
    for (bi = 0; bi < DENSE_PARENT_BUCKETS_FANOUT_LOOKUP; bi++) {
        dense_node_t *n = lk->buckets[bi];

        lk->buckets[bi] = NULL;
        while (n) {
            dense_node_t *nx = n->next;

            free(n->parent);
            free(n);
            n = nx;
        }
    }
}

/*
 * slice_parents: per-cell map keyed by parent dir, counts regular files in that heat-map cell under each parent.
 * fanout_lookup: merged crawl-wide immediate-child counts per parent directory (65k buckets for fast lookup).
 * Returns max global_fanout[P] over parents P appearing in the slice map.
 */
static uint64_t dense_cell_max_fanout_among_parents(const dense_cell_map_t *slice_parents,
                                                    const dense_cell_fanout_lookup_t *fanout_lookup) {
    size_t bi;
    uint64_t mx = 0;

    if (!slice_parents || !fanout_lookup) return 0;
    for (bi = 0; bi < DENSE_PARENT_BUCKETS; bi++) {
        const dense_node_t *n;

        for (n = slice_parents->buckets[bi]; n; n = n->next) {
            uint64_t g = dense_cell_fanout_lookup_get_n(fanout_lookup, n->parent);

            if (g > mx) mx = g;
        }
    }
    return mx;
}

static void dense_cell_free(dense_cell_map_t *m) {
    size_t bi;

    if (!m) return;
    for (bi = 0; bi < DENSE_PARENT_BUCKETS; bi++) {
        dense_node_t *n = m->buckets[bi];

        m->buckets[bi] = NULL;
        while (n) {
            dense_node_t *nx = n->next;
            free(n->parent);
            free(n);
            n = nx;
        }
    }
}

static void fanout_parent_stat_map_free(fanout_parent_stat_map_t *m) {
    size_t bi;

    if (!m) return;
    for (bi = 0; bi < DENSE_PARENT_BUCKETS_FANOUT_LOOKUP; bi++) {
        fanout_parent_stat_node_t *n = m->buckets[bi];

        m->buckets[bi] = NULL;
        while (n) {
            fanout_parent_stat_node_t *nx = n->next;

            free(n->parent);
            free(n);
            n = nx;
        }
    }
}

static void fanout_parent_stat_absorb_one(fanout_parent_stat_map_t *dst, fanout_parent_stat_node_t *src_node, summary_t *sum) {
    uint32_t biw;
    fanout_parent_stat_node_t **pp;

    (void)sum;
    biw = dense_parent_bucket_fanout_lookup(src_node->parent);
    pp = &dst->buckets[biw];
    while (*pp) {
        if (strcmp((*pp)->parent, src_node->parent) == 0) {
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
            free(src_node->parent);
            free(src_node);
            return;
        }
        pp = &(*pp)->next;
    }
    src_node->next = dst->buckets[biw];
    dst->buckets[biw] = src_node;
}

static void fanout_parent_stat_merge_into(fanout_parent_stat_map_t *dst, fanout_parent_stat_map_t *src, summary_t *sum) {
    size_t bi;

    if (!dst || !src) return;
    for (bi = 0; bi < DENSE_PARENT_BUCKETS_FANOUT_LOOKUP; bi++) {
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

static void fanout_parent_stat_accumulate(fanout_parent_stat_map_t *m,
                                          const char *child_path,
                                          uint8_t type,
                                          uint64_t pick_ts,
                                          summary_t *sum) {
    char parent[PATH_MAX];
    uint32_t biw;
    fanout_parent_stat_node_t **pp;
    fanout_parent_stat_node_t *node;
    uint64_t nf = 0;
    uint64_t nd = 0;
    uint64_t no = 0;

    if (!m || !child_path) return;
    if (parent_dir_to_buf(child_path, parent, sizeof(parent)) != 0) return;
    if (type == 'f')
        nf = 1;
    else if (type == 'd')
        nd = 1;
    else
        no = 1;

    biw = dense_parent_bucket_fanout_lookup(parent);
    pp = &m->buckets[biw];
    while (*pp) {
        if (strcmp((*pp)->parent, parent) == 0) {
            (*pp)->n_files += nf;
            (*pp)->n_dirs += nd;
            (*pp)->n_others += no;
            if (!(*pp)->have_time) {
                (*pp)->t_min = pick_ts;
                (*pp)->t_max = pick_ts;
                (*pp)->have_time = 1;
            } else {
                if (pick_ts < (*pp)->t_min) (*pp)->t_min = pick_ts;
                if (pick_ts > (*pp)->t_max) (*pp)->t_max = pick_ts;
            }
            return;
        }
        pp = &(*pp)->next;
    }
    node = (fanout_parent_stat_node_t *)malloc(sizeof(*node));
    if (!node) {
        if (sum) sum->bad_input_files++;
        return;
    }
    node->parent = strdup(parent);
    if (!node->parent) {
        free(node);
        if (sum) sum->bad_input_files++;
        return;
    }
    node->n_files = nf;
    node->n_dirs = nd;
    node->n_others = no;
    node->t_min = pick_ts;
    node->t_max = pick_ts;
    node->have_time = 1;
    node->next = m->buckets[biw];
    m->buckets[biw] = node;
}

static const fanout_parent_stat_node_t *fanout_parent_stat_lookup(const fanout_parent_stat_map_t *m, const char *parent) {
    uint32_t biw;
    const fanout_parent_stat_node_t *n;

    if (!m || !parent) return NULL;
    biw = dense_parent_bucket_fanout_lookup(parent);
    for (n = m->buckets[biw]; n; n = n->next) {
        if (strcmp(n->parent, parent) == 0) return n;
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

/* Count one immediate child under its parent (used for crawl-wide megadir / Dense badge). */
static void path_fanout_accumulate(dense_cell_map_t *fanout, const char *child_path) {
    char parent[PATH_MAX];

    if (!fanout || !child_path) return;
    if (parent_dir_to_buf(child_path, parent, sizeof(parent)) != 0) return;
    (void)dense_cell_add(fanout, parent, 1ULL, NULL);
}

static void path_shape_accumulate_file(summary_t *sum,
                                       dense_cell_map_t *cell_dense,
                                       int ab,
                                       int sb,
                                       const char *path,
                                       uint64_t accounted_bytes) {
    char parent[PATH_MAX];
    unsigned slashes;

    if (!sum || !cell_dense || !path) return;
    slashes = path_slash_count_str(path);
    if (slashes >= PATH_SHAPE_DEEP_MIN_SLASHES && accounted_bytes > 0ULL) {
        sum->shape_deep_bytes[ab][sb] += accounted_bytes;
        sum->total_shape_deep_bytes += accounted_bytes;
    }
    if (parent_dir_to_buf(path, parent, sizeof(parent)) != 0) return;
    if (dense_cell_add(cell_dense, parent, 1ULL, sum) != 0) return;
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

static uint64_t path_hash(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211ULL;
    }
    return h;
}

static int path_row_map_init(path_row_map_t *m, size_t initial_cap) {
    size_t cap = 1;
    while (cap < initial_cap) cap <<= 1;

    m->rows = (path_row_t *)calloc(cap, sizeof(*m->rows));
    m->used = (unsigned char *)calloc(cap, sizeof(*m->used));
    if (!m->rows || !m->used) {
        free(m->rows);
        free(m->used);
        m->rows = NULL;
        m->used = NULL;
        m->cap = 0;
        m->count = 0;
        return -1;
    }

    m->cap = cap;
    m->count = 0;
    return 0;
}

static void path_row_map_destroy(path_row_map_t *m) {
    size_t i;
    for (i = 0; i < m->cap; i++) {
        if (m->used[i]) free(m->rows[i].path);
    }
    free(m->rows);
    free(m->used);
    m->rows = NULL;
    m->used = NULL;
    m->cap = 0;
    m->count = 0;
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
            size_t idx = (size_t)(path_hash(row.path) & (new_cap - 1));
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

static path_row_t *path_row_map_get_or_insert(path_row_map_t *m, const char *path) {
    size_t idx;

    if ((m->count + 1) * 10 >= m->cap * 7) {
        if (path_row_map_rehash(m, m->cap << 1) != 0) return NULL;
    }

    idx = (size_t)(path_hash(path) & (m->cap - 1));
    while (m->used[idx]) {
        if (strcmp(m->rows[idx].path, path) == 0) return &m->rows[idx];
        idx = (idx + 1) & (m->cap - 1);
    }

    m->rows[idx].path = strdup(path);
    if (!m->rows[idx].path) return NULL;
    m->used[idx] = 1;
    m->count++;
    return &m->rows[idx];
}

static path_row_t *path_row_map_find(path_row_map_t *m, const char *path) {
    size_t idx;

    if (m->cap == 0) return NULL;

    idx = (size_t)(path_hash(path) & (m->cap - 1));
    while (m->used[idx]) {
        if (strcmp(m->rows[idx].path, path) == 0) return &m->rows[idx];
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

static int cmp_row_bucket_bytes_desc(const void *a, const void *b) {
    const path_row_t *ra = *(const path_row_t * const *)a;
    const path_row_t *rb = *(const path_row_t * const *)b;
    if (ra->bucket_bytes < rb->bucket_bytes) return 1;
    if (ra->bucket_bytes > rb->bucket_bytes) return -1;
    if (ra->bucket_files < rb->bucket_files) return 1;
    if (ra->bucket_files > rb->bucket_files) return -1;
    return strcmp(ra->path, rb->path);
}

static int starts_with_dir_prefix(const char *path, const char *prefix);

static char *dup_common_dir_prefix(const bucket_details_t *details) {
    char *prefix;
    size_t i;
    size_t len;

    if (details->count == 0) return strdup("");

    prefix = strdup(details->items[0].path ? details->items[0].path : "");
    if (!prefix) return NULL;
    len = strlen(prefix);

    for (i = 1; i < details->count; i++) {
        const char *p = details->items[i].path ? details->items[i].path : "";
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
            const char *pth = details->items[i].path ? details->items[i].path : "";
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
            } else if (details->items[0].path && details->items[0].path[0] == '/') {
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

static size_t matched_ord_lower_bound_seg(const matched_records_t *rec, const size_t *ord, size_t n, const char *key) {
    size_t lo = 0;
    size_t hi = n;

    if (!key) key = "";
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const char *p = rec->items[ord[mid]].path ? rec->items[ord[mid]].path : "";
        if (path_cmp_seg(p, key) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

/* Set only around qsort of matched-record indices (single-threaded). */
static const matched_record_t *g_bucket_path_sort_items;

static int matched_path_ord_cmp(const void *aa, const void *bb) {
    const size_t ia = *(const size_t *)aa;
    const size_t ib = *(const size_t *)bb;
    const char *pa = g_bucket_path_sort_items[ia].path ? g_bucket_path_sort_items[ia].path : "";
    const char *pb = g_bucket_path_sort_items[ib].path ? g_bucket_path_sort_items[ib].path : "";
    return path_cmp_seg(pa, pb);
}

/* Thread-safe comparator for parallel merge-sort leaves (items via qsort_r context, not globals). */
static int matched_path_ord_cmp_r(const void *aa, const void *bb, void *ctx) {
    const matched_record_t *items = (const matched_record_t *)ctx;
    const size_t ia = *(const size_t *)aa;
    const size_t ib = *(const size_t *)bb;
    const char *pa = items[ia].path ? items[ia].path : "";
    const char *pb = items[ib].path ? items[ib].path : "";
    return path_cmp_seg(pa, pb);
}

static void merge_ord_path_slice(const matched_record_t *items,
                                 size_t *ord,
                                 size_t *tmp,
                                 size_t lo,
                                 size_t mid,
                                 size_t hi) {
    size_t i = lo, j = mid, k = lo;

    while (i < mid && j < hi) {
        const char *pa = items[ord[i]].path ? items[ord[i]].path : "";
        const char *pb = items[ord[j]].path ? items[ord[j]].path : "";
        if (path_cmp_seg(pa, pb) <= 0)
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

static size_t *matched_records_build_path_order(const matched_records_t *rec) {
    size_t *ord;
    size_t *tmp;
    size_t i;
    size_t n;
    int md;
    int nw;

    if (!rec || rec->count == 0) return NULL;
    n = rec->count;
    ord = (size_t *)malloc(n * sizeof(size_t));
    if (!ord) return NULL;
    for (i = 0; i < n; i++) ord[i] = i;

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if (n <= PATH_ORD_QSORT_THRESH || nw < 2) {
        g_bucket_path_sort_items = rec->items;
        qsort(ord, n, sizeof(size_t), matched_path_ord_cmp);
        g_bucket_path_sort_items = NULL;
        return ord;
    }

    tmp = (size_t *)malloc(n * sizeof(size_t));
    if (!tmp) {
        g_bucket_path_sort_items = rec->items;
        qsort(ord, n, sizeof(size_t), matched_path_ord_cmp);
        g_bucket_path_sort_items = NULL;
        return ord;
    }

    md = path_ord_merge_max_depth();
    if (md < 1) {
        free(tmp);
        g_bucket_path_sort_items = rec->items;
        qsort(ord, n, sizeof(size_t), matched_path_ord_cmp);
        g_bucket_path_sort_items = NULL;
        return ord;
    }

    matched_records_ms_parallel(rec->items, ord, tmp, 0, n, 0, md);
    free(tmp);
    return ord;
}

static int join_path_component(char *dst, size_t dst_sz, const char *base, const char *comp, size_t comp_len) {
    int n;

    if (base[0] == '\0') n = snprintf(dst, dst_sz, "%.*s", (int)comp_len, comp);
    else if (strcmp(base, "/") == 0) n = snprintf(dst, dst_sz, "/%.*s", (int)comp_len, comp);
    else n = snprintf(dst, dst_sz, "%s/%.*s", base, (int)comp_len, comp);

    return n >= 0 && (size_t)n < dst_sz;
}

static const char *path_after_base_prefix(const char *path, const char *base_prefix) {
    const char *p = path;
    size_t plen;

    if (!starts_with_dir_prefix(path, base_prefix)) return NULL;

    plen = base_prefix ? strlen(base_prefix) : 0;
    if (base_prefix && base_prefix[0] != '\0' && strcmp(base_prefix, "/") != 0) {
        p += plen;
        if (*p == '/') p++;
    } else if (base_prefix && strcmp(base_prefix, "/") == 0 && *p == '/') {
        p++;
    }

    return p;
}

static int aggregate_bucket_for_page_n(path_row_map_t *maps,
                                       int nlevels,
                                       const bucket_details_t *details,
                                       const char *base_prefix) {
    size_t i;

    for (i = 0; i < details->count; i++) {
        const detail_record_t *r = &details->items[i];
        char prev[PATH_MAX];
        char rowpath[PATH_MAX];
        const char *p;
        int depth;

        p = path_after_base_prefix(r->path, base_prefix);
        if (!p || *p == '\0') continue;

        prev[0] = '\0';
        if (base_prefix) {
            if (snprintf(prev, sizeof(prev), "%s", base_prefix) >= (int)sizeof(prev)) return -1;
        }

        for (depth = 0; depth < nlevels; depth++) {
            const char *start;
            size_t comp_len;
            path_row_t *row;

            while (*p == '/') p++;
            if (*p == '\0') break;

            start = p;
            while (*p && *p != '/') p++;
            comp_len = (size_t)(p - start);
            if (comp_len == 0) break;

            if (!join_path_component(rowpath, sizeof(rowpath), prev, start, comp_len)) return -1;

            row = path_row_map_get_or_insert(&maps[depth], rowpath);
            if (!row) return -1;
            row->bucket_files++;
            row->bucket_bytes += r->size;
            if (r->ctime_led) {
                row->bucket_ctime_led_files++;
                row->bucket_ctime_led_bytes += r->size;
            }

            if (snprintf(prev, sizeof(prev), "%s", rowpath) >= (int)sizeof(prev)) return -1;
        }
    }

    return 0;
}

static int aggregate_totals_for_page_n(path_row_map_t *maps,
                                       int nlevels,
                                       const matched_records_t *records,
                                       const char *base_prefix,
                                       const size_t *path_ord) {
    size_t n = records ? records->count : 0;
    size_t lo = 0;
    size_t hi = n;
    size_t ii;
    int use_ord_slice = (base_prefix && base_prefix[0] != '\0' && path_ord);

    if (use_ord_slice) {
        lo = matched_ord_lower_bound_seg(records, path_ord, n, base_prefix);
        hi = lo;
        while (hi < n) {
            const char *pp = records->items[path_ord[hi]].path ? records->items[path_ord[hi]].path : "";
            if (!starts_with_dir_prefix(pp, base_prefix)) break;
            hi++;
        }
    }

    for (ii = lo; ii < hi; ii++) {
        size_t i = use_ord_slice ? path_ord[ii] : ii;
        const matched_record_t *r = &records->items[i];
        char prev[PATH_MAX];
        char rowpath[PATH_MAX];
        const char *p;
        int depth;

        p = path_after_base_prefix(r->path, base_prefix);
        if (!p || *p == '\0') continue;

        prev[0] = '\0';
        if (base_prefix) {
            if (snprintf(prev, sizeof(prev), "%s", base_prefix) >= (int)sizeof(prev)) return -1;
        }

        for (depth = 0; depth < nlevels; depth++) {
            const char *start;
            size_t comp_len;
            path_row_t *row;

            while (*p == '/') p++;
            if (*p == '\0') break;

            start = p;
            while (*p && *p != '/') p++;
            comp_len = (size_t)(p - start);
            if (comp_len == 0) break;

            if (!join_path_component(rowpath, sizeof(rowpath), prev, start, comp_len)) return -1;

            row = path_row_map_find(&maps[depth], rowpath);
            if (row) {
                if (r->type == 'f') row->total_files++;
                else if (r->type == 'd') row->total_dirs++;
                row->total_bytes += r->size;
            }

            if (snprintf(prev, sizeof(prev), "%s", rowpath) >= (int)sizeof(prev)) return -1;
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
                                    int level_idx) {
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

    qsort(rows, count, sizeof(*rows), cmp_row_bucket_bytes_desc);

    {
        size_t shown = count;
        if (shown > (size_t)BUCKET_PATH_TABLE_MAX_ROWS) shown = (size_t)BUCKET_PATH_TABLE_MAX_ROWS;

        fprintf(out,
                "<!-- bucket path table: %zu director%s at this level; %zu rows in HTML (cap %d; default sort bucket "
                "bytes desc; headers clickable) -->\n",
                count,
                count == 1 ? "y" : "ies",
                shown,
                BUCKET_PATH_TABLE_MAX_ROWS);

        if (count > (size_t)BUCKET_PATH_TABLE_MAX_ROWS) {
            fprintf(out,
                    "<p class=\"table-trunc-note\">Showing the <strong>top %d</strong> of <strong>%zu</strong> "
                    "directories at this depth (default sort: bucket bytes, largest first). Omitted rows are lower "
                    "in that ranking; heat-map and bucket summary totals above still include the full bucket. "
                    "Use column headers to sort the visible rows.</p>\n",
                    BUCKET_PATH_TABLE_MAX_ROWS,
                    count);
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
                         "Purple pill when >= %.0f%% (same as heat map; --heat-ctime-led-min-share).",
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

static int cmp_deep_drill_desc(const void *a, const void *b) {
    const deep_drill_row_t *ra = (const deep_drill_row_t *)a;
    const deep_drill_row_t *rb = (const deep_drill_row_t *)b;

    if (ra->files > rb->files) return -1;
    if (ra->files < rb->files) return 1;
    if (ra->bytes > rb->bytes) return -1;
    if (ra->bytes < rb->bytes) return 1;
    return strcmp(ra->parent, rb->parent);
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
                                            const char *basis_str) {
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
        dense_drill_row_t *rows = NULL;
        size_t show_n;

        for (bi = 0; bi < DENSE_PARENT_BUCKETS; bi++)
            for (n = slice_dense_parents->buckets[bi]; n; n = n->next) dense_cnt++;

        if (dense_cnt > 0) {
            rows = (dense_drill_row_t *)calloc(dense_cnt, sizeof(*rows));
            if (rows) {
                size_t k = 0;
                for (bi = 0; bi < DENSE_PARENT_BUCKETS; bi++) {
                    for (n = slice_dense_parents->buckets[bi]; n; n = n->next) {
                        rows[k].parent = n->parent;
                        rows[k].slice_files = n->n;
                        rows[k].global_fanout = dense_cell_fanout_lookup_get_n(fanout_lookup, n->parent);
                        k++;
                    }
                }
                qsort(rows, dense_cnt, sizeof(*rows), cmp_dense_drill_desc);
                show_n = dense_cnt;
                if (show_n > (size_t)BUCKET_SHAPE_DRILL_MAX_ROWS) show_n = (size_t)BUCKET_SHAPE_DRILL_MAX_ROWS;

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
        deep_drill_row_t *rows = NULL;
        size_t deep_cnt = 0;
        size_t show_n;
        int deep_ok = 1;

        memset(&dm_bytes, 0, sizeof(dm_bytes));
        memset(&dm_files, 0, sizeof(dm_files));

        if (details && details->count > 0) {
            for (i = 0; i < details->count; i++) {
                char parent[PATH_MAX];
                const detail_record_t *rec = &details->items[i];

                if (!rec->path) continue;
                if (path_slash_count_str(rec->path) < PATH_SHAPE_DEEP_MIN_SLASHES) continue;
                if (parent_dir_to_buf(rec->path, parent, sizeof(parent)) != 0) continue;
                if (dense_cell_add(&dm_bytes, parent, rec->size, NULL) != 0 ||
                    dense_cell_add(&dm_files, parent, 1ULL, NULL) != 0) {
                    deep_ok = 0;
                    break;
                }
            }
        }

        if (deep_ok) {
            for (bi = 0; bi < DENSE_PARENT_BUCKETS; bi++)
                for (n = dm_bytes.buckets[bi]; n; n = n->next) deep_cnt++;

            if (deep_cnt > 0) {
                rows = (deep_drill_row_t *)calloc(deep_cnt, sizeof(*rows));
                if (rows) {
                    size_t k = 0;
                    for (bi = 0; bi < DENSE_PARENT_BUCKETS; bi++) {
                        for (n = dm_bytes.buckets[bi]; n; n = n->next) {
                            rows[k].parent = n->parent;
                            rows[k].bytes = n->n;
                            rows[k].files = dense_cell_map_get_n(&dm_files, n->parent);
                            k++;
                        }
                    }
                    qsort(rows, deep_cnt, sizeof(*rows), cmp_deep_drill_desc);
                    show_n = deep_cnt;
                    if (show_n > (size_t)BUCKET_SHAPE_DRILL_MAX_ROWS) show_n = (size_t)BUCKET_SHAPE_DRILL_MAX_ROWS;

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
                                   const fanout_parent_stat_map_t *fanout_parent_stats) {
    FILE *out = counted_fopen(filename, "w");
    char *base_prefix = NULL;
    path_row_map_t maps[BUCKET_DETAIL_LEVELS_MAX];
    size_t i;
    uint64_t bucket_files = 0;
    uint64_t bucket_bytes = 0;
    uint64_t total_user_files = corpus_total_user_files;
    uint64_t total_user_bytes = corpus_total_user_bytes;
    int d;
    int init_fail_at = -1;

    if (!out) return -1;

    if (detail_levels < 1 || detail_levels > BUCKET_DETAIL_LEVELS_MAX) {
        counted_fclose(out);
        return -1;
    }

    for (d = 0; d < detail_levels; d++) {
        if (path_row_map_init(&maps[d], 1024 + (size_t)d * 512) != 0) {
            init_fail_at = d;
            break;
        }
    }
    if (init_fail_at >= 0) {
        for (d = 0; d < init_fail_at; d++) path_row_map_destroy(&maps[d]);
        counted_fclose(out);
        return -1;
    }

    fprintf(out, "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    fprintf(out, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    fprintf(out, "<title>Bucket Details</title>\n<style>\n");
    fprintf(out, "html{box-sizing:border-box}\n");
    fprintf(out,
            "body{font-family:\"DejaVu Sans Mono\",\"Consolas\",monospace;margin:0;padding:"
            "10px clamp(10px,2.2vw,28px) 28px;width:100%%;max-width:none;box-sizing:border-box;color:#1f2328;"
            "background:#fcfcf8}\n");
    fprintf(out,
            ".bucket-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%%;max-width:none;"
            "margin-bottom:22px}\n");
    fprintf(out, "h1,h2{margin:0 0 12px 0;font-weight:600}\n");
    fprintf(out, "h3{margin:18px 0 10px;font-weight:600;font-size:14px;color:#3d362c}\n");
    fprintf(out, ".shape-drill-h2{margin-top:22px}\n");
    fprintf(out, ".shape-drill-details{margin-top:12px}\n");
    fprintf(out, ".meta{margin:0 0 14px 0;color:#555;line-height:1.5;font-size:13px}\n");
    fprintf(out, ".meta-sub{font-weight:400;color:#666}\n");
    fprintf(out, ".note{font-size:12px;color:#666;margin-bottom:14px;max-width:none;width:100%%}\n");
    fprintf(out,
            ".table-trunc-note{font-size:12px;color:#555;margin:0 0 12px;max-width:none;width:100%%;line-height:1.45}\n");
    fprintf(out,
            ".bucket-help{margin:0 0 18px;border:1px solid #ddd2c8;border-radius:8px;background:#faf8f4;max-width:none;"
            "width:100%%;font-size:13px;line-height:1.55;color:#555}\n");
    fprintf(out, ".bucket-help summary{cursor:pointer;padding:10px 12px;font-weight:600;color:#4a4034;list-style-position:outside}\n");
    fprintf(out, ".bucket-help summary::-webkit-details-marker{color:#8b7355}\n");
    fprintf(out, ".bucket-help .bucket-help-body{padding:0 12px 12px}\n");
    fprintf(out, ".bucket-help .bucket-help-body p{margin:0 0 10px}\n");
    fprintf(out, ".bucket-help .bucket-help-body p:last-child{margin-bottom:0}\n");
    fprintf(out,
            "table{border-collapse:collapse;font-size:13px;table-layout:auto;width:max-content;max-width:none}\n");
    fprintf(out, "th,td{border:1px solid #d5d0c5;padding:5px 12px;vertical-align:top}\n");
    fprintf(out, "thead th{background:#ece6da;position:sticky;top:0;z-index:3}\n");
    fprintf(out, "thead th:first-child,tbody td:first-child{position:sticky;left:0;z-index:4}\n");
    fprintf(out, "thead th:first-child{background:#ece6da;z-index:5}\n");
    fprintf(out, "tbody td:first-child{background:#f8f5ee;z-index:1}\n");
    fprintf(out, "td.r,th.r{text-align:right}\n");
    fprintf(out,
            ".bucket-table-wrap td.num,.bucket-table-wrap th.num{white-space:nowrap;box-sizing:border-box}\n");
    fprintf(out,
            ".bucket-table-wrap th.sort-h{cursor:pointer;user-select:none}\n"
            ".bucket-table-wrap th.sort-h:hover{background:#ddd4c4}\n"
            ".bucket-table-wrap th.sort-asc .th-text::after{content:\" \\25b2\";font-size:0.72em;opacity:0.85}\n"
            ".bucket-table-wrap th.sort-desc .th-text::after{content:\" \\25bc\";font-size:0.72em;opacity:0.85}\n");
    fprintf(out,
            "th:first-child{min-width:max(260px,min(28vw,560px));max-width:none;box-sizing:border-box}\n");
    fprintf(out,
            ".path-cell{min-width:max(260px,min(28vw,560px));max-width:none;overflow-x:hidden;overflow-y:visible;"
            "box-sizing:border-box}\n");
    fprintf(out, ".path-line{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start;gap:8px;min-width:0}\n");
    fprintf(out, ".path-toggle{min-width:0;max-width:100%%;display:block;border:0;background:none;padding:0;margin:0;color:inherit;font:inherit;text-align:left;cursor:pointer}\n");
    fprintf(out,
            ".path-prefix{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#8b8172;"
            "font-size:11px;line-height:1.15;margin-bottom:1px}\n");
    fprintf(out,
            ".path-tail{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700;"
            "color:#1f2328;line-height:1.2}\n");
    fprintf(out, ".copy-path{opacity:0;pointer-events:none;align-self:start;flex-shrink:0;border:1px solid #d8ccb8;background:#f4ede0;color:#6b4c16;border-radius:999px;padding:1px 5px;font:inherit;font-size:9px;line-height:1.2;cursor:pointer;transition:opacity 0.15s ease,background-color 0.15s ease}\n");
    fprintf(out, ".path-cell:hover .copy-path,.path-cell:focus-within .copy-path,.path-cell.expanded .copy-path{opacity:1;pointer-events:auto}\n");
    fprintf(out, ".copy-path:hover{background:#eadfc9}\n");
    fprintf(out, ".path-toggle:hover .path-tail,.path-toggle:focus .path-tail{text-decoration:underline}\n");
    fprintf(out, ".path-full{margin-top:6px;padding:6px 8px;max-width:100%%;box-sizing:border-box;background:#f4efe4;border:1px solid #ddd2bf;border-radius:4px;white-space:normal;word-break:break-all;overflow-wrap:anywhere;font-size:10px;line-height:1.35;color:#4e4538;user-select:all;overflow-x:auto}\n");
    fprintf(out, ".path-cell.expanded .path-prefix{white-space:normal;overflow:hidden;text-overflow:clip;word-break:break-all;overflow-wrap:anywhere}\n");
    fprintf(out,
            ".heat-map-margins{font-size:12px;color:#555;margin:0 0 16px;line-height:1.5;max-width:none;width:100%%}\n");
    fprintf(out, ".heat-map-margins p{margin:6px 0 0}\n");
    fprintf(out, "a{color:#6b4c16;text-decoration:none}\n");
    fprintf(out,
            ".path-ctime-led-badge{font-size:9px;font-weight:700;line-height:1;padding:2px 6px;border-radius:999px;"
            "background:rgba(109,40,217,0.92);color:#faf5ff;border:1px solid rgba(76,29,149,0.85);margin-right:6px;"
            "vertical-align:middle;white-space:nowrap;display:inline-block}\n");
    fprintf(out, ".path-ctime-led-pct{font-variant-numeric:tabular-nums}\n");
    fprintf(out,
            ".bucket-heat-shape{font-size:12px;color:#444;margin:0 0 14px;line-height:1.5;display:flex;flex-wrap:wrap;"
            "align-items:center;gap:6px;max-width:none;width:100%%}\n"
            ".bucket-heat-shape-label{font-weight:600;color:#555;margin-right:2px}\n"
            ".bucket-heat-shape .heat-map-badges{position:static;display:flex;flex-direction:row;flex-wrap:wrap;"
            "align-items:center;gap:4px;pointer-events:auto}\n"
            ".bucket-heat-shape .heat-ctime-led-badge,.bucket-heat-shape .heat-shape-badge{font-size:8px;"
            "pointer-events:auto;cursor:help}\n"
            ".bucket-heat-shape .heat-ctime-led-badge{font-size:8px;font-weight:700;line-height:1;padding:2px 5px;"
            "border-radius:4px;background:rgba(109,40,217,0.94);color:#faf5ff;border:1px solid rgba(76,29,149,0.88);"
            "letter-spacing:0.02em;white-space:nowrap;margin-right:0}\n"
            ".heat-shape-badge{font-size:8px;font-weight:700;line-height:1;padding:2px 5px;border-radius:4px;"
            "letter-spacing:0.02em;white-space:nowrap;border:1px solid rgba(0,0,0,0.14)}\n"
            ".heat-shape-deep{background:rgba(14,116,144,0.92);color:#ecfeff}\n"
            ".heat-shape-dense{background:rgba(180,83,9,0.92);color:#fffbeb}\n"
            ".heat-shape-skew{background:rgba(127,29,29,0.92);color:#fef2f2}\n");
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
        for (d = 0; d < detail_levels; d++) path_row_map_destroy(&maps[d]);
        counted_fclose(out);
        return 0;
    }

    base_prefix = dup_common_dir_prefix(details);
    if (!base_prefix) {
        for (d = 0; d < detail_levels; d++) path_row_map_destroy(&maps[d]);
        counted_fclose(out);
        return -1;
    }

    for (i = 0; i < details->count; i++) {
        bucket_files++;
        bucket_bytes += details->items[i].size;
    }

    if (aggregate_bucket_for_page_n(maps, detail_levels, details, base_prefix) != 0 ||
        aggregate_totals_for_page_n(maps, detail_levels, matched_records, base_prefix, matched_path_ord) != 0) {
        free(base_prefix);
        for (d = 0; d < detail_levels; d++) path_row_map_destroy(&maps[d]);
        counted_fclose(out);
        return -1;
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
                                        fanout_parent_stats, heat_sum, shape, basis_str);
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
                                d);
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

    free(base_prefix);
    for (d = 0; d < detail_levels; d++) path_row_map_destroy(&maps[d]);
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
} bucket_emit_ctx_t;

typedef struct {
    bucket_emit_ctx_t *ctx;
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
static void corpus_file_byte_totals_parallel(const matched_records_t *mr, uint64_t *out_files, uint64_t *out_bytes) {
    size_t n;
    int nw;
    size_t per;
    int slot;
    int n_join;
    int j;
    corp_scan_worker_ctx_t *ctxs = NULL;
    pthread_t *tp = NULL;

    *out_files = 0;
    *out_bytes = 0;
    if (!mr || mr->count == 0) return;
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

        ab = (int)(k / (size_t)SIZE_BUCKETS);
        sb = (int)(k % (size_t)SIZE_BUCKETS);

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
                                        c->fanout_parent_stats);
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
    pthread_t *tids = NULL;
    bucket_emit_thread_arg_t *args = NULL;
    int nw;
    size_t ti;

    if (ensure_bucket_output_dir_exists() != 0) return -1;

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

    if (!ctx.stub_mode && matched_records && matched_records->count > 0) {
        corpus_file_byte_totals_parallel(matched_records, &ctx.corpus_total_user_files, &ctx.corpus_total_user_bytes);
        ctx.matched_path_ord = matched_records_build_path_order(matched_records);
    }

    atomic_init(&ctx.next_task, 0);
    atomic_init(&ctx.any_fail, 0);
    if (run_stats) {
        atomic_store(&run_stats->finalize_phase, 2);
        atomic_store(&run_stats->finalize_bucket_done, 0);
    }

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if ((size_t)nw > ntasks) nw = (int)ntasks;

    tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
    args = (bucket_emit_thread_arg_t *)calloc((size_t)nw, sizeof(*args));
    if (!tids || !args) {
        fprintf(stderr, "ereport: allocation failed (bucket page emit pool)\n");
        free(tids);
        free(args);
        free(ctx.matched_path_ord);
        return -1;
    }

    for (ti = 0; ti < (size_t)nw; ti++) args[ti].ctx = &ctx;

    for (ti = 0; ti < (size_t)nw; ti++) {
        if (pthread_create(&tids[ti], NULL, bucket_page_emit_worker, &args[ti]) != 0) {
            size_t j;
            fprintf(stderr, "ereport: pthread_create failed (bucket page worker); retrying sequentially\n");
            for (j = 0; j < ti; j++) pthread_join(tids[j], NULL);
            free(tids);
            atomic_store(&ctx.next_task, 0);
            atomic_store(&ctx.any_fail, 0);
            bucket_page_emit_worker(&args[0]);
            free(args);
            free(ctx.matched_path_ord);
            return atomic_load(&ctx.any_fail) ? -1 : 0;
        }
    }

    for (ti = 0; ti < (size_t)nw; ti++) pthread_join(tids[ti], NULL);
    free(tids);
    free(args);
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
    }
    return NULL;
}

/*
 * Single malloc + parallel memcpy into contiguous matched-record array.
 * Splits each parse-worker shard into MR_FIN_RECORDS_PER_TASK slices so imbalanced shards use all cores.
 */
static int matched_records_finalize_parallel(worker_arg_t *args, int threads_used, matched_records_t *dst) {
    size_t *prefix;
    size_t total;
    int nw, i, nw_started;
    int ns;
    int si;
    pthread_t *pool = NULL;
    mr_fin_slice_t *slices = NULL;
    mr_fin_slice_ctx_t sctx;

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

    nw_started = 0;
    for (i = 0; i < nw; i++) {
        if (pthread_create(&pool[i], NULL, mr_fin_slice_worker, &sctx) != 0) break;
        nw_started++;
    }

    if (nw_started > 0) {
        for (i = 0; i < nw_started; i++) pthread_join(pool[i], NULL);
    }
    if (nw_started == 0) {
        for (si = 0; si < ns; si++) {
            if (slices[si].n) memcpy(slices[si].dst, slices[si].src, slices[si].n * sizeof(matched_record_t));
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
    return 0;
}

/*
 * Merge parse-worker dense maps for one age×size cell (sequential pairwise rounds;
 * each merge fans out over hash buckets inside dense_cell_merge_into).
 */
static void dense_cell_reduce_workers_into(dense_cell_map_t *dst,
                                           worker_arg_t *args,
                                           int threads_used,
                                           int ab,
                                           int sb) {
    dense_cell_map_t *buf;
    int n;
    int ti;

    if (!dst || !args || threads_used < 1) return;

    if (threads_used == 1) {
        dense_cell_merge_into(dst, &args[0].dense_maps[ab][sb], NULL);
        return;
    }

    buf = (dense_cell_map_t *)malloc((size_t)threads_used * sizeof(*buf));
    if (!buf) {
        for (ti = 0; ti < threads_used; ti++) dense_cell_merge_into(dst, &args[ti].dense_maps[ab][sb], NULL);
        return;
    }

    for (ti = 0; ti < threads_used; ti++) {
        buf[ti] = args[ti].dense_maps[ab][sb];
        memset(&args[ti].dense_maps[ab][sb], 0, sizeof(args[ti].dense_maps[ab][sb]));
    }

    n = threads_used;
    while (n > 1) {
        int pairs = n / 2;
        int ii;

        if (pairs > 0) {
            for (ii = 0; ii < pairs; ii++) dense_cell_merge_into(&buf[2 * ii], &buf[2 * ii + 1], NULL);
        }

        {
            int k = 0;
            for (ii = 0; ii < pairs; ii++) buf[k++] = buf[2 * ii];
            if (n % 2 == 1) buf[k++] = buf[n - 1];
            n = k;
        }
    }

    dense_cell_merge_into(dst, &buf[0], NULL);
    free(buf);
}

typedef struct {
    worker_arg_t *args;
    int threads_used;
    bucket_details_t (*final_details)[SIZE_BUCKETS];
    dense_cell_map_t (*merged_dense)[SIZE_BUCKETS];
    atomic_int next_cell;
    atomic_int fail;
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
    }
    return NULL;
}

/*
 * Per age×size cell: finalize bucket details + merge dense parent maps. Cells are independent (parallel).
 */
static int bucket_dense_cells_finalize_parallel(worker_arg_t *args,
                                                int threads_used,
                                                bucket_details_t final_details[AGE_BUCKETS][SIZE_BUCKETS],
                                                dense_cell_map_t merged_dense[AGE_BUCKETS][SIZE_BUCKETS]) {
    const int ncells = AGE_BUCKETS * SIZE_BUCKETS;
    int nw, i, nw_started, task, ab, sb;
    pthread_t *pool = NULL;
    cell_fin_ctx_t ctx;

    nw = parse_ereport_thread_count();
    if (nw < 1) nw = 1;
    if (nw > ncells) nw = ncells;

    ctx.args = args;
    ctx.threads_used = threads_used;
    ctx.final_details = final_details;
    ctx.merged_dense = merged_dense;
    atomic_init(&ctx.fail, 0);
    atomic_init(&ctx.next_cell, 0);

    if (nw <= 1) {
        for (task = 0; task < ncells; task++) {
            ab = task / SIZE_BUCKETS;
            sb = task % SIZE_BUCKETS;
            if (bucket_details_finalize_cell(args, threads_used, &final_details[ab][sb], ab, sb) != 0) return -1;
            dense_cell_reduce_workers_into(&merged_dense[ab][sb], args, threads_used, ab, sb);
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

static void shape_margin_run_task(shape_margin_ctx_t *c, int t) {
    if (t < AGE_BUCKETS) {
        int ab = t;
        uint64_t db = 0;
        int sb;
        dense_cell_map_t *rm = &c->row_merged[ab];

        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            db += c->sum->shape_deep_bytes[ab][sb];
            dense_cell_merge_add(rm, &c->merged_dense[ab][sb], NULL);
        }
        c->path_shape->row[ab].deep_bytes = db;
        c->path_shape->row[ab].dense_fanout_max = dense_cell_max_fanout_among_parents(rm, c->fanout_lookup);
    } else {
        int sb = t - AGE_BUCKETS;
        dense_cell_map_t col_acc;
        uint64_t db = 0;
        int ab;

        memset(&col_acc, 0, sizeof(col_acc));
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            db += c->sum->shape_deep_bytes[ab][sb];
            dense_cell_merge_add(&col_acc, &c->merged_dense[ab][sb], NULL);
        }
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
                                              path_shape_view_t *path_shape) {
    dense_cell_map_t row_merged[AGE_BUCKETS];
    shape_margin_ctx_t ctx;
    ps_cell_ctx_t ps_ctx;
    int nw, i, nw_started, t, ab;
    pthread_t *pool = NULL;
    pthread_t *ps_pool = NULL;
    const int ntasks = AGE_BUCKETS + SIZE_BUCKETS;
    const int ncells = AGE_BUCKETS * SIZE_BUCKETS;
    dense_cell_map_t all_acc;

    ps_ctx.sum = sum;
    ps_ctx.merged_dense = merged_dense;
    ps_ctx.fanout_lookup = fanout_lookup;
    ps_ctx.path_shape = path_shape;
    atomic_init(&ps_ctx.next_cell, 0);

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

    memset(&all_acc, 0, sizeof(all_acc));
    path_shape->all.deep_bytes = sum->total_shape_deep_bytes;
    for (ab = 0; ab < AGE_BUCKETS; ab++) dense_cell_merge_add(&all_acc, &row_merged[ab], NULL);
    path_shape->all.dense_fanout_max = dense_cell_max_fanout_among_parents(&all_acc, fanout_lookup);
    dense_cell_free(&all_acc);
    for (ab = 0; ab < AGE_BUCKETS; ab++) dense_cell_free(&row_merged[ab]);
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
    }
    free(fs);
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
    if (crawl_bin_catalog_load(fp, fh.catalog_offset, (uint64_t)st.st_size, cat) != 0) {
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
                          ereport_run_stats_t *run_stats) {
    FILE *fp = NULL;
    int rc = -1;

    fp = counted_fopen(chunk->path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", chunk->path, strerror(errno));
        sum->bad_input_files++;
        if (progress) progress->bad_input_files++;
        finalize_chunk_file_progress(file_states, chunk->file_index, progress);
        return -1;
    }

    if (fseeko(fp, (off_t)chunk->start_offset, SEEK_SET) != 0) {
        fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
        sum->bad_input_files++;
        if (progress) progress->bad_input_files++;
        goto out;
    }

    for (;;) {
        bin_record_hdr_t r;
        size_t n;
        char *pathbuf = NULL;
        uint64_t accounted_size = 0;
        off_t record_offset = ftello(fp);
        int record_match;
        int skip_paths;

        if (record_offset < 0 || (uint64_t)record_offset >= chunk->end_offset) {
            rc = 0;
            break;
        }

        memset(&r, 0, sizeof(r));

        n = counted_fread(&r, sizeof(r), 1, fp);
        if (n != 1) {
            if (feof(fp)) rc = 0;
            else {
                fprintf(stderr, "warn: read error in %s\n", chunk->path);
                sum->bad_input_files++;
            }
            break;
        }

        {
            size_t rec_total = crawl_bin_record_total_bytes(&r);
            if ((uint64_t)record_offset + rec_total > chunk->end_offset) {
                fprintf(stderr, "warn: truncated record in %s\n", chunk->path);
                sum->bad_input_files++;
                if (progress) progress->bad_input_files++;
                break;
            }
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
            if (r.name_len > 0) {
                if (fseeko(fp, (off_t)r.name_len, SEEK_CUR) != 0) {
                    fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    break;
                }
            }
            continue;
        }

        if (r.parent_dir_id == 0ULL) {
            fprintf(stderr, "warn: incomplete/wire-format record in %s\n", chunk->path);
            sum->bad_input_files++;
            if (progress) progress->bad_input_files++;
            break;
        }

        if (skip_paths) {
            if (r.name_len > 0) {
                if (fseeko(fp, (off_t)r.name_len, SEEK_CUR) != 0) {
                    fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    break;
                }
            }
            pathbuf = NULL;
        } else {
            unsigned char *name_bytes = NULL;

            pathbuf = (char *)malloc(PATH_MAX);
            if (!pathbuf) {
                fprintf(stderr, "warn: path alloc failed in %s\n", chunk->path);
                sum->bad_input_files++;
                if (progress) progress->bad_input_files++;
                break;
            }
            if (r.name_len > 0) {
                name_bytes = (unsigned char *)malloc((size_t)r.name_len);
                if (!name_bytes) {
                    fprintf(stderr, "warn: path alloc failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    free(pathbuf);
                    break;
                }
                if (counted_fread(name_bytes, 1, r.name_len, fp) != r.name_len) {
                    fprintf(stderr, "warn: path read failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    free(name_bytes);
                    free(pathbuf);
                    break;
                }
            }
            if (crawl_bin_catalog_entry_path(file_states[chunk->file_index].catalog, r.parent_dir_id,
                                             (char *)name_bytes, r.name_len, pathbuf, PATH_MAX) != 0) {
                fprintf(stderr, "warn: path reconstruct failed in %s\n", chunk->path);
                sum->bad_input_files++;
                if (progress) progress->bad_input_files++;
                free(name_bytes);
                free(pathbuf);
                break;
            }
            free(name_bytes);
        }

        if (all_users && uid_distinct && uid_accum_insert_if_new(uid_distinct, r.uid) < 0) {
            fprintf(stderr, "warn: uid set error in %s\n", chunk->path);
            sum->bad_input_files++;
            if (progress) progress->bad_input_files++;
            if (pathbuf) free(pathbuf);
            break;
        }

        sum->matched_records++;
        if (progress) progress->matched_records++;

        if (r.type == 'f') {
            int sb = size_bucket_for(r.size);
            int ab = age_bucket_for(pick_time(&r, basis), now);
            int count_bytes = 1;
            int ctime_led = (bucket_detail_levels > 0) ? record_ctime_led(&r) : 0;

            if (r.nlink > 1) {
                int ins = inode_set_insert_if_new(seen_inodes, r.dev_major, r.dev_minor, r.inode);
                if (ins < 0) {
                    fprintf(stderr, "warn: inode dedup set error in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    if (pathbuf) free(pathbuf);
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
                path_shape_accumulate_file(sum, &dense_maps[ab][sb], ab, sb, pathbuf, accounted_size);
            }

            if (!skip_paths) {
                if (bucket_details_append(&details[ab][sb], pathbuf, accounted_size, ctime_led) != 0) {
                    fprintf(stderr, "warn: detail append failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    if (pathbuf) free(pathbuf);
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
            if (matched_records_append(matched_records, pathbuf, r.type, accounted_size) != 0) {
                fprintf(stderr, "warn: matched record append failed in %s\n", chunk->path);
                sum->bad_input_files++;
                if (progress) progress->bad_input_files++;
                if (pathbuf) free(pathbuf);
                break;
            }
        }

        if (pathbuf && bucket_detail_levels > 0 && parent_fanout) {
            path_fanout_accumulate(parent_fanout, pathbuf);
            if (parent_fanout_stats)
                fanout_parent_stat_accumulate(parent_fanout_stats, pathbuf, r.type, pick_time(&r, basis), sum);
        }

        if (pathbuf) free(pathbuf);
    }

out:
    counted_fclose(fp);
    finalize_chunk_file_progress(file_states, chunk->file_index, progress);
    progress_flush_local(progress, run_stats);
    return rc;
}

static void *worker_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;
    progress_local_t progress;

    memset(&progress, 0, sizeof(progress));

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
                       &                       arg->matched_records,
                       arg->dense_maps,
                       &arg->parent_fanout,
                       &arg->parent_fanout_stats,
                       arg->run_stats);
    }

    progress_flush_local(&progress, arg->run_stats);

    return NULL;
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

        sleep(1);

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

        {
            unsigned int divisor = atomic_load(&g_seconds_seen);
            if (divisor == 0) divisor = 1;
            records_rate = (double)window_records / (double)divisor;
        }

        rs->records_rate_sum += records_rate;
        if (rs->records_rate_samples == 0 || records_rate < rs->records_rate_min) rs->records_rate_min = records_rate;
        if (rs->records_rate_samples == 0 || records_rate > rs->records_rate_max) rs->records_rate_max = records_rate;
        rs->records_rate_samples++;

        human_decimal((double)scanned_files, sf, sizeof(sf));
        human_decimal((double)rs->input_files_total, tf, sizeof(tf));
        human_decimal((double)scanned_records, sr, sizeof(sr));
        human_decimal((double)matched_records, mr, sizeof(mr));
        human_decimal(records_rate, rr, sizeof(rr));
        format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

        {
            uint64_t prep_tot_u = rs->chunk_prep_files_total;
            unsigned long long prep_done_u = atomic_load(&rs->chunk_prep_files_done);

            if (prep_tot_u > 0ULL && prep_done_u < prep_tot_u) {
                char pdone[32], ptot[32];
                human_decimal((double)prep_done_u, pdone, sizeof(pdone));
                human_decimal((double)prep_tot_u, ptot, sizeof(ptot));
                printf("\rchunk-map files:%s/%s | scanning bin headers for parallel parse | el:%s            ", pdone, ptot,
                       elapsed_buf);
                fflush(stdout);
                {
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
                const char *step;

                if (phase == 1)
                    step = "merging shard summaries";
                else if (phase == 2)
                    step = "writing bucket HTML";
                else if (phase == 3)
                    step = "writing index.html";
                else
                    step = "finalizing";

                if (phase == 2) {
                    printf(
                        "\rfinalizing: %s (%u/%d) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
                        step,
                        bdone,
                        AGE_BUCKETS * SIZE_BUCKETS,
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
                                            parse_ereport_thread_count(), &local_chunks, &local_count, &fc);

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
                fprintf(out, "<li>");
                html_escape(out, p);
                fprintf(out, "</li>\n");
                break;
            }
            if (semi > p) {
                fprintf(out, "<li>");
                html_escape_segment(out, p, (size_t)(semi - p));
                fprintf(out, "</li>\n");
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
                 "Badge if ≥%.0f%% (Heat: --heat-ctime-led-min-share / EREPORT_HEAT_CTIME_LED_MIN_SHARE).",
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
                     const ereport_crawl_timing_t *crawl_timing) {
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
    fprintf(out, "body{font-family:Arial,sans-serif;margin:24px;color:#222}\n");
    fprintf(out, "body.drawer-open{overflow:hidden}\n");
    fprintf(out, "h1{margin-bottom:8px}\n");
    fprintf(out, ".report-title{font-size:1.35rem;margin:0 0 18px}\n");
    fprintf(out, ".report-aggregate-note{margin:0 0 18px;font-size:13px;color:#444;line-height:1.45;max-width:720px}\n");
    fprintf(out, ".report-stats{margin-top:28px;padding-top:22px;border-top:1px solid #ddd}\n");
    fprintf(out, ".report-stats>h2{font-size:1.15rem;margin:0 0 14px}\n");
    fprintf(out, ".report-sources-section{margin-bottom:20px;padding:14px 16px;background:#f9f9f7;border:1px solid #e8e4dc;border-radius:8px}\n");
    fprintf(out, ".report-sources-section h3{font-size:0.95rem;margin:0 0 8px;color:#444;font-weight:600}\n");
    fprintf(out, ".report-sources-section .lead{margin:0 0 10px;font-size:13px;color:#555;line-height:1.45}\n");
    fprintf(out, ".report-sources-list{margin:8px 0 0;padding-left:22px;line-height:1.55;color:#333;font-size:13px}\n");
    fprintf(out, ".stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-top:4px}\n");
    fprintf(out, ".stats-card{margin:0;padding:14px 16px;background:#fafafa;border:1px solid #e5e5e5;border-radius:8px}\n");
    fprintf(out, ".stats-card h3{font-size:0.92rem;margin:0 0 12px;color:#333;font-weight:600}\n");
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
    fprintf(out, "th,td{border:1px solid #ccc;padding:4px 6px;text-align:right}\n");
    fprintf(out, "th:first-child,td:first-child{text-align:left}\n");
    fprintf(out, "th{background:#f4f4f4}\n");
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
    fprintf(out, "@media (max-width:900px){body{margin:14px}.drawer{width:100vw}.drawer-head{padding:12px 14px}}\n");
    fprintf(out, ".path-search{margin:0 0 22px}\n");
    fprintf(out, ".path-search label{display:block;font-weight:600;margin-bottom:6px}\n");
    fprintf(out, ".path-search input[type=text]{width:min(520px,95vw);padding:8px;font-size:14px;border:1px solid #ccc;border-radius:4px}\n");
    fprintf(out, ".path-search-panel{margin-top:12px;padding:12px 14px;border:1px solid #dadada;border-radius:8px;background:#f9f9f9;max-height:min(55vh,480px);overflow:auto;font-size:13px}\n");
    fprintf(out, ".path-search-panel[hidden]{display:none!important}\n");
    fprintf(out, ".path-search-panel-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:8px}\n");
    fprintf(out, ".path-search-panel-head button{font:inherit;font-size:12px;padding:4px 10px;border:1px solid #bbb;border-radius:4px;background:#fff;cursor:pointer}\n");
    fprintf(out, ".path-search-caption{font-size:12px;color:#555;margin:0 0 8px}\n");
    fprintf(out, ".path-search-preview{font-size:13px}\n");
    fprintf(out, ".path-search-preview ul{margin:0;padding-left:20px}\n");
    fprintf(out, ".path-search-muted{color:#777;font-size:13px}\n");
    fprintf(out, ".path-search-hit{background:#fff3cd;padding:0 2px;border-radius:2px;font-weight:600}\n");
    fprintf(out, ".path-search-results{margin-top:18px;padding-top:12px;border-top:1px solid #eee}\n");
    fprintf(out, ".path-search-results-list{margin:8px 0;padding-left:22px;font-size:13px}\n");
    fprintf(out, ".path-search-results-list li{margin:4px 0;word-break:break-all}\n");
    fprintf(out, ".path-search-pager{display:flex;gap:12px;margin-top:12px;align-items:center;flex-wrap:wrap}\n");
    fprintf(out, ".path-search-pager button{padding:6px 14px;font-size:14px;cursor:pointer;border:1px solid #bbb;border-radius:4px;background:#f8f8f8}\n");
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

    fprintf(out, "<section class=\"path-search\" aria-label=\"Path search\">\n");
    fprintf(out, "<label for=\"path-search-input\">Search paths</label>\n");
    fprintf(out, "<p class=\"path-search-hint\">Type at least three characters. Results appear below as you type; press Enter for full pages of matches. Use Hide to close the results panel.</p>\n");
    fprintf(out,
            "<input type=\"text\" id=\"path-search-input\" autocomplete=\"off\" "
            "placeholder=\"Example: project name or folder\" />\n");
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
        emit_stats_count_dd(out, "Directories", sum->total_dirs);
        emit_stats_count_dd(out, "Symbolic links", sum->total_links);
        emit_stats_count_dd(out, "Other types", other_count);
        emit_stats_count_dd(out, "Non-regular entries", non_file_count);
        fprintf(out, "</dl></article>\n");

        fprintf(out, "<article class=\"stats-card\"><h3>Capacity</h3><dl class=\"stats-dl\">\n");
        fprintf(out, "<dt>In regular files</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num\">(%" PRIu64 " B)</span></dd>\n",
                totalb, sum->total_bytes);
        fprintf(out, "<dt>In symlinks / non-files</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num\">(%" PRIu64 " B)</span></dd>\n",
                total_non_file_b, non_file_bytes);
        fprintf(out, "<dt>Other file types</dt><dd><span class=\"stats-num\">%s</span> <span class=\"stats-num\">(%" PRIu64 " B)</span></dd>\n",
                total_other_b, sum->total_other_bytes);
        fprintf(out, "</dl></article>\n");

        fprintf(out, "</div>\n");
        fprintf(out, "</section>\n");
    }

    fprintf(out, "<div id=\"drawer-backdrop\" class=\"drawer-backdrop\"></div>\n");
    fprintf(out, "<aside id=\"bucket-drawer\" class=\"drawer\" aria-hidden=\"true\">\n");
    fprintf(out, "<div class=\"drawer-head\"><div><div id=\"bucket-title\" class=\"drawer-title\">Bucket Details</div><div class=\"drawer-sub\">Click a heatmap cell to inspect that bucket.</div></div><div class=\"drawer-actions\"><a id=\"bucket-open\" href=\"#\" target=\"_blank\" rel=\"noopener\">Open page</a><button type=\"button\" id=\"bucket-close\">Close</button></div></div>\n");
    fprintf(out, "<iframe id=\"bucket-frame\" class=\"drawer-frame\" title=\"Bucket details\" loading=\"lazy\"></iframe>\n");
    fprintf(out, "</aside>\n");
    fprintf(out, "<script>\n");
    fputs("(function(){\n", out);
    fputs("'use strict';\n", out);
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
    fputs("if(n<1000000000)return Math.round(n/1e6)+'M';\n", out);
    fputs("return Math.round(n/1e9)+'G';\n", out);
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
    fputs("if(t.length<3){if(previewFetchCtl)previewFetchCtl.abort();box.innerHTML='';document.getElementById('path-search-results').hidden=true;hideSearchPanel();return;}\n", out);
    fputs("showSearchPanel();\n", out);
    fputs("document.getElementById('path-search-panel-title').textContent='Preview';\n", out);
    fputs("if(cap)cap.textContent='Keep typing in the box above—same field—to refine. Press Enter for paged results.';\n", out);
    fputs("document.getElementById('path-search-results').hidden=true;\n", out);
    fputs("if(previewFetchCtl)previewFetchCtl.abort();\n", out);
    fputs("previewFetchCtl=new AbortController();\n", out);
    fputs("var pvSig=previewFetchCtl.signal;\n", out);
    fputs("fetchSearch(t,0,PREVIEW_MAX,pvSig).then(function(j){\n", out);
    fputs("var inpEl=document.getElementById('path-search-input');\n", out);
    fputs("if(!inpEl||inpEl.value.trim()!==t)return;\n", out);
    fputs("var paths=j.paths||[];\n", out);
    fputs("if(paths.length===0){box.innerHTML='<span class=\"path-search-muted\">No matches.</span>';return;}\n", out);
    fputs("var h='<ul>';for(var i=0;i<paths.length;i++){h+='<li>'+highlightPathHtml(paths[i],t)+'</li>';}h+='</ul>';\n", out);
    fputs("var pv=[];if(j.search_ms!=null&&fmtSearchMs(j.search_ms))pv.push(fmtSearchMs(j.search_ms));\n", out);
    fputs("var cm=corpusMeta(j);if(cm)pv.push(cm);\n", out);
    fputs("var pvs=pv.length?' \\u00b7 '+pv.join(' \\u00b7 '):'';\n", out);
    fputs("if((j.total||0)>paths.length){h+='<div class=\"path-search-muted\">Showing '+paths.length+' of '+j.total+pvs+' \\u2014 press Enter for full paging.</div>';}\n", out);
    fputs("box.innerHTML=h;\n}).catch(function(e){\n", out);
    fputs("if(e&&(e.name==='AbortError'||(pvSig&&pvSig.aborted)))return;\n", out);
    fputs("var caperr=document.getElementById('path-search-caption');if(caperr)caperr.textContent='';box.innerHTML='<span class=\"path-search-muted\">'+escHtml(e.message)+'</span>';});\n}\n", out);
    fputs("function renderFullPage(){\n", out);
    fputs("var meta=document.getElementById('path-search-results-meta');\n", out);
    fputs("var list=document.getElementById('path-search-results-list');\n", out);
    fputs("var prev=document.getElementById('path-search-prev');\n", out);
    fputs("var next=document.getElementById('path-search-next');\n", out);
    fputs("var cap=document.getElementById('path-search-caption');\n", out);
    fputs("if(!fullTerm){meta.textContent='';list.innerHTML='';if(cap)cap.textContent='';return;}\n", out);
    fputs("showSearchPanel();\n", out);
    fputs("document.getElementById('path-search-panel-title').textContent='Paged results';\n", out);
    fputs("if(cap)cap.textContent='Edit the search box above to change the query; use Prev/Next below.';\n", out);
    fputs("fetchSearch(fullTerm,(pageNum-1)*PAGE_SIZE,PAGE_SIZE).then(function(j){\n", out);
    fputs("lastTotal=j.total||0;var total=lastTotal;var pages=Math.max(1,Math.ceil(total/PAGE_SIZE));\n", out);
    fputs("if(pageNum>pages)pageNum=pages;if(pageNum<1)pageNum=1;\n", out);
    fputs("var pm=[];if(j.search_ms!=null&&fmtSearchMs(j.search_ms))pm.push(fmtSearchMs(j.search_ms));\n", out);
    fputs("var cm2=corpusMeta(j);if(cm2)pm.push(cm2);\n", out);
    fputs("var pms=pm.length?' \\u00b7 '+pm.join(' \\u00b7 '):'';\n", out);
    fputs("meta.textContent=total+' match'+(total===1?'':'es')+pms+' \\u2014 page '+pageNum+' of '+pages;\n", out);
    fputs("var paths=j.paths||[];var h='';for(var i=0;i<paths.length;i++){h+='<li>'+highlightPathHtml(paths[i],fullTerm)+'</li>';}list.innerHTML=h;\n", out);
    fputs("prev.disabled=pageNum<=1;next.disabled=pageNum>=pages;\n", out);
    fputs("}).catch(function(e){var capfp=document.getElementById('path-search-caption');if(capfp)capfp.textContent='';meta.textContent='';list.innerHTML='<li class=\"path-search-muted\">'+escHtml(e.message)+'</li>';prev.disabled=true;next.disabled=true;});\n}\n", out);
    fputs("function runFullSearch(term){\n", out);
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
                           double elapsed_sec) {
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
    printf("io_opendir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_opendir_calls));
    printf("io_readdir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_readdir_calls));
    printf("io_closedir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_closedir_calls));
    printf("io_fopen_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fopen_calls));
    printf("io_fclose_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fclose_calls));
    printf("io_fread_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fread_calls));
    printf("avg_records_per_sec=%s\n", avg_records_buf);
    printf("mean_records_per_sec=%s\n", mean_records_buf);
    printf("max_records_per_sec=%s\n", max_records_buf);
    printf("min_records_per_sec=%s\n", min_records_buf);
    printf("elapsed_sec=%.3f\n", elapsed_sec);
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
    uint64_t distinct_uid_count = 0;
    double t0, t1;
    ereport_run_stats_t run_stats;
    ereport_crawl_timing_t crawl_timing;
    path_shape_view_t path_shape;

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
                "Usage: %s [--bucket-details N] [--report-dir DIR] [--heat-ctime-led-min-share F] "
                "<username|uid> [<atime|mtime|ctime|effective>] [bin_dir ...]\n",
                argv[0]);
        fprintf(stderr,
                "       %s [--bucket-details N] [--report-dir DIR] [--heat-ctime-led-min-share F] "
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
                "Optional --heat-ctime-led-min-share F (0<F≤1): min fraction of bytes for purple C-led badges/pills "
                "(default 0.30; env EREPORT_HEAT_CTIME_LED_MIN_SHARE).\n");
        fprintf(stderr, "Flags must appear first (any order). Thread count: EREPORT_THREADS (default %d).\n",
                DEFAULT_THREADS);
        return 2;
    }

    {
        int ac = argc;
        char **av = argv;
        int heat_ctime_led_share_opt = 0;

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
            if (ac > 1 && strcmp(av[1], "--heat-ctime-led-min-share") == 0) {
                char *end;
                double v;

                if (heat_ctime_led_share_opt) {
                    fprintf(stderr, "ereport: duplicate --heat-ctime-led-min-share\n");
                    return 2;
                }
                if (ac < 3) {
                    fprintf(stderr, "ereport: --heat-ctime-led-min-share requires a float in (0,1]\n");
                    return 2;
                }
                errno = 0;
                v = strtod(av[2], &end);
                if (errno || end == av[2] || (end && *end) || v <= 0.0 || v > 1.0) {
                    fprintf(stderr, "ereport: --heat-ctime-led-min-share must be a float in (0,1]\n");
                    return 2;
                }
                g_ctime_led_badge_min_share_frac = v;
                heat_ctime_led_share_opt = 1;
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
                    fprintf(stderr, "unknown option: %s (thread count is set with EREPORT_THREADS)\n", argv[ai]);
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
                        fprintf(stderr, "unknown option: %s (thread count is set with EREPORT_THREADS)\n", argv[ai]);
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
                            fprintf(stderr, "unknown option: %s (thread count is set with EREPORT_THREADS)\n",
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
                            fprintf(stderr, "unknown option: %s (thread count is set with EREPORT_THREADS)\n",
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
        size_t merge_off;

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

        prep_threads = threads;
        if ((size_t)prep_threads > path_count) prep_threads = (int)path_count;

        fprintf(stderr, "ereport: mapping chunk boundaries using %d parallel scanner(s)...\n", prep_threads);
        fflush(stderr);

        chunk_wpaths = (volatile const char **)calloc((size_t)prep_threads, sizeof(*chunk_wpaths));
        prep_args = (chunk_prep_thread_arg_t *)calloc((size_t)prep_threads, sizeof(*prep_args));
        if (!chunk_wpaths || !prep_args) {
            fprintf(stderr, "allocation failed\n");
            free((void *)chunk_wpaths);
            free(prep_args);
            atomic_store(&run_stats.stop_stats, 1);
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
        atomic_store(&pool.next_path_index, 0);

        prep_tids = (pthread_t *)calloc((size_t)prep_threads, sizeof(*prep_tids));
        if (!prep_tids) {
            fprintf(stderr, "allocation failed\n");
            free((void *)chunk_wpaths);
            free(prep_args);
            run_stats.chunk_map_worker_paths = NULL;
            run_stats.chunk_map_path_slots = 0;
            atomic_store(&run_stats.stop_stats, 1);
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
                atomic_store(&run_stats.stop_stats, 1);
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
                atomic_store(&run_stats.stop_stats, 1);
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

        free(chunk_targets);
        free(prep_rc);
        free(prep_chunks);
        free(prep_chunk_counts);

        run_stats.chunk_prep_files_total = 0;
        atomic_store(&run_stats.chunk_prep_files_done, 0ULL);
    }

    if (chunk_count == 0) {
        fprintf(stderr, "no readable chunk work found in %s\n", input_dirs_label);
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
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
                atomic_store(&run_stats.stop_stats, 1);
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
            atomic_store(&run_stats.stop_stats, 1);
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

        if (all_users_mode) {
            if (uid_accum_init(&args[i].uid_distinct, 8192) != 0) {
                int j;
                fprintf(stderr, "allocation failed\n");
                threads_used = i;
                for (j = 0; j < i; j++) pthread_join(tids[j], NULL);
                for (j = 0; j < i; j++) uid_accum_destroy(&args[j].uid_distinct);
                if (stats_thread_started) {
                    atomic_store(&run_stats.stop_stats, 1);
                    pthread_join(stats_thread, NULL);
                    clear_status_line();
                }
                worker_dense_maps_free(args, i);
                free(tids);
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

    {
        for (i = 0; i < threads_used; i++) pthread_join(tids[i], NULL);

        /* Progress thread: keep parsing status (rec/s line) until workers exit; then finalize banner. */
        atomic_store(&run_stats.parse_workers_done, 1);
        atomic_store(&run_stats.finalize_phase, 1);

        summary_reduce_from_worker_args(&final_sum, args, threads_used);
    }

    memset(&path_shape, 0, sizeof(path_shape));

    dense_cell_map_t merged_dense_shape[AGE_BUCKETS][SIZE_BUCKETS];
    dense_cell_fanout_lookup_t fanout_lookup_shape;
    fanout_parent_stat_map_t merged_fanout_stats;
    int bucket_shape_maps_live = 0;

    memset(&merged_dense_shape, 0, sizeof(merged_dense_shape));
    memset(&fanout_lookup_shape, 0, sizeof(fanout_lookup_shape));
    memset(&merged_fanout_stats, 0, sizeof(merged_fanout_stats));

    if (bucket_detail_levels > 0) {
        if (matched_records_finalize_parallel(args, threads_used, &final_matched_records) != 0) {
            fprintf(stderr, "allocation failed merging matched records\n");
            if (stats_thread_started) {
                atomic_store(&run_stats.stop_stats, 1);
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

        {
            dense_cell_map_t merged_fanout;

            memset(&merged_fanout, 0, sizeof(merged_fanout));
            for (i = 0; i < threads_used; i++) {
                dense_cell_merge_into(&merged_fanout, &args[i].parent_fanout, NULL);
                fanout_parent_stat_merge_into(&merged_fanout_stats, &args[i].parent_fanout_stats, NULL);
            }

            if (bucket_dense_cells_finalize_parallel(args, threads_used, final_details, merged_dense_shape) != 0) {
                fprintf(stderr, "allocation failed merging bucket details\n");
                dense_cell_free(&merged_fanout);
                fanout_parent_stat_map_free(&merged_fanout_stats);
                memset(&merged_fanout_stats, 0, sizeof(merged_fanout_stats));
                for (ab = 0; ab < AGE_BUCKETS; ab++) {
                    for (sb = 0; sb < SIZE_BUCKETS; sb++) dense_cell_free(&merged_dense_shape[ab][sb]);
                }
                if (stats_thread_started) {
                    atomic_store(&run_stats.stop_stats, 1);
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
            dense_cell_steal_into_fanout_lookup(&merged_fanout, &fanout_lookup_shape);
            dense_cell_free(&merged_fanout);

            path_shape_fill_from_merged_dense(&final_sum, merged_dense_shape, &fanout_lookup_shape, &path_shape);
            bucket_shape_maps_live = 1;
        }
    }

    if (all_users_mode) {
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
        for (i = 0; i < threads_used; i++) {
            if (uid_accum_merge_into(&merged_uids, &args[i].uid_distinct) != 0) {
                int j;
                fprintf(stderr, "allocation failed merging uid tallies\n");
                if (bucket_shape_maps_live) {
                    bucket_shape_maps_destroy(&fanout_lookup_shape, merged_dense_shape, &merged_fanout_stats);
                    bucket_shape_maps_live = 0;
                }
                uid_accum_destroy(&merged_uids);
                for (j = i; j < threads; j++) uid_accum_destroy(&args[j].uid_distinct);
                worker_dense_maps_free(args, threads_used);
                free(tids);
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
            uid_accum_destroy(&args[i].uid_distinct);
        }
        for (; i < threads; i++) uid_accum_destroy(&args[i].uid_distinct);
        distinct_uid_count = (uint64_t)uid_accum_size(&merged_uids);
        uid_accum_destroy(&merged_uids);
    }

    if (ensure_bucket_output_dir_exists() != 0) {
        fprintf(stderr, "failed to create report output directory %s: %s\n", g_bucket_output_dir, strerror(errno));
        if (bucket_shape_maps_live) {
            bucket_shape_maps_destroy(&fanout_lookup_shape, merged_dense_shape, &merged_fanout_stats);
            bucket_shape_maps_live = 0;
        }
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
        }
        worker_dense_maps_free(args, threads_used);
        free(tids);
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

    atomic_store(&run_stats.finalize_phase, 3);

    if (emit_html(report_path, display_name, all_users_mode, distinct_uid_count, bucket_detail_levels, target_uid,
                   basis_str, &final_sum, &path_shape, path_count, threads_used, bin_dir_count,
                   storage_base_paths_label, &crawl_timing) != 0) {
        fprintf(stderr, "failed to write main report %s\n", report_path);
    }
    final_sum.scanned_input_files = (uint64_t)path_count;
    t1 = now_sec();
    if (stats_thread_started) {
        atomic_store(&run_stats.stop_stats, 1);
        pthread_join(stats_thread, NULL);
        clear_status_line();
    }
    emit_run_stats(display_name, all_users_mode, distinct_uid_count, bucket_detail_levels, target_uid, basis_str,
                   input_dirs_label, report_path, path_count, threads, threads_used, &final_sum, &run_stats,
                   bucket_pages_written, t1 - t0);

    free(tids);
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
