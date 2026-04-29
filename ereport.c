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
 *   ./ereport [--bucket-details N] <username|uid> <atime|mtime|ctime> [bin_dir ...]
 *   ./ereport [--bucket-details N] <atime|mtime|ctime> [bin_dir ...]
 *     --bucket-details N (optional): emit N levels of per-bucket directory tables (1…32); if omitted,
 *     bucket pages are brief summaries only. Flags must appear first, before username/time basis.
 *     (omit username: aggregate report for all UIDs in the crawl; output under ./all_users/)
 * Parallel worker count: EREPORT_THREADS (default 32); see worker_main / stats_thread / bucket HTML emit.
 * Multiple bin_dir values merge shard files from each crawl output directory (one user’s shards,
 * or every shard when aggregating all users).
 *
 * Writes outputs under ./<resolved_username>/ or ./all_users/ for aggregate mode (cwd).
 * Falls back to ./tmp/ only if the directory name is empty or unusably long after sanitization.
 */

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

#include "crawl_ckpt.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define FILE_MAGIC_LEN 8
#define FORMAT_VERSION 3
#define DEFAULT_THREADS 32
#define WINDOW_SECONDS 10
#define PROGRESS_FLUSH_INTERVAL 1024U
#define PARSE_CHUNK_BYTES (32ULL << 20)
#define PARSE_CHUNK_MIN_BYTES (1ULL << 20)
#define BUCKET_DETAIL_LEVELS_MAX 32
#define BUCKET_PATH_TABLE_MAX_ROWS 200

typedef struct __attribute__((packed)) {
    char magic[FILE_MAGIC_LEN];
    uint32_t version;
    uint32_t reserved;
} bin_file_header_t;

typedef struct __attribute__((packed)) {
    uint16_t path_len;
    uint8_t  type;
    uint8_t  reserved8;
    uint32_t mode;
    uint64_t uid;
    uint64_t gid;
    uint64_t size;
    uint64_t inode;
    uint32_t dev_major;
    uint32_t dev_minor;
    uint64_t nlink;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
} bin_record_hdr_t;

typedef enum {
    TIME_ATIME = 0,
    TIME_MTIME = 1,
    TIME_CTIME = 2
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

typedef struct {
    uint64_t bytes[AGE_BUCKETS][SIZE_BUCKETS];
    uint64_t files[AGE_BUCKETS][SIZE_BUCKETS];
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
} summary_t;

typedef struct {
    char *path;
    uint64_t size;
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

typedef struct {
    struct file_chunk *chunks;
    size_t count;
    size_t next_index;
    pthread_mutex_t mutex;
} work_queue_t;

typedef struct file_chunk {
    char *path;
    uint64_t start_offset;
    uint64_t end_offset;
    size_t file_index;
} file_chunk_t;

typedef struct {
    atomic_uint remaining_chunks;
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

static int bucket_details_append(bucket_details_t *b, const char *path, uint64_t size) {
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
    b->count++;
    return 0;
}

static int bucket_details_merge(bucket_details_t *dst, bucket_details_t *src) {
    size_t i;

    if (src->count == 0) return 0;

    if (dst->count + src->count > dst->cap) {
        size_t new_cap = dst->cap ? dst->cap : 256;
        detail_record_t *tmp;
        while (new_cap < dst->count + src->count) new_cap *= 2;
        tmp = (detail_record_t *)realloc(dst->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        dst->items = tmp;
        dst->cap = new_cap;
    }

    for (i = 0; i < src->count; i++) dst->items[dst->count++] = src->items[i];

    free(src->items);
    src->items = NULL;
    src->count = 0;
    src->cap = 0;
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

static int matched_records_merge(matched_records_t *dst, matched_records_t *src) {
    size_t i;

    if (src->count == 0) return 0;

    if (dst->count + src->count > dst->cap) {
        size_t new_cap = dst->cap ? dst->cap : 1024;
        matched_record_t *tmp;
        while (new_cap < dst->count + src->count) new_cap *= 2;
        tmp = (matched_record_t *)realloc(dst->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        dst->items = tmp;
        dst->cap = new_cap;
    }

    for (i = 0; i < src->count; i++) dst->items[dst->count++] = src->items[i];

    free(src->items);
    src->items = NULL;
    src->count = 0;
    src->cap = 0;
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

/* Thousands separators for HTML (locale-independent). */
static void format_uint_commas(uint64_t n, char *buf, size_t sz) {
    char raw[40];
    size_t raw_len;
    size_t i, j;

    if (sz == 0) return;
    snprintf(raw, sizeof(raw), "%" PRIu64, n);
    raw_len = strlen(raw);
    j = 0;
    for (i = 0; i < raw_len && j + 1 < sz; i++) {
        if (i > 0 && (raw_len - i) % 3 == 0) buf[j++] = ',';
        buf[j++] = raw[i];
    }
    buf[j] = '\0';
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
    char c[48];
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
    return -1;
}

static uint64_t pick_time(const bin_record_hdr_t *r, time_basis_t basis) {
    switch (basis) {
        case TIME_ATIME: return r->atime;
        case TIME_MTIME: return r->mtime;
        case TIME_CTIME: return r->ctime;
        default: return r->mtime;
    }
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

            if (snprintf(prev, sizeof(prev), "%s", rowpath) >= (int)sizeof(prev)) return -1;
        }
    }

    return 0;
}

static int aggregate_totals_for_page_n(path_row_map_t *maps,
                                       int nlevels,
                                       const matched_records_t *records,
                                       const char *base_prefix) {
    size_t i;

    for (i = 0; i < records->count; i++) {
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

    fprintf(out, "<td class=\"path-cell\" title=\"");
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
                "<!-- bucket path table: %zu director%s at this level; %zu rows in HTML (cap %d; sort bucket bytes "
                "desc) -->\n",
                count,
                count == 1 ? "y" : "ies",
                shown,
                BUCKET_PATH_TABLE_MAX_ROWS);

        if (count > (size_t)BUCKET_PATH_TABLE_MAX_ROWS) {
            fprintf(out,
                    "<p class=\"table-trunc-note\">Showing the <strong>top %d</strong> of <strong>%zu</strong> "
                    "directories at this depth (sorted by bucket bytes, largest first). Omitted rows are lower "
                    "in that ranking; heat-map and bucket summary totals above still include the full bucket.</p>\n",
                    BUCKET_PATH_TABLE_MAX_ROWS,
                    count);
        }
    }

    fprintf(out, "<div class=\"bucket-table-wrap\"><table>\n<thead><tr><th>Path</th><th class=\"r\">Bucket Files</th><th class=\"r\">Share of Bucket Files</th><th class=\"r\">Bucket Bytes</th><th class=\"r\">Share of Bucket Bytes</th><th class=\"r\">Total Files</th><th class=\"r\">Total Dirs</th><th class=\"r\">Total Bytes</th><th class=\"r\">Share of User Bytes</th><th class=\"r\">Share of User Files</th></tr></thead>\n<tbody>\n");
    for (i = 0; i < count && i < (size_t)BUCKET_PATH_TABLE_MAX_ROWS; i++) {
        char bb[32];
        char tb[32];
        char file_bg[32];
        char byte_bg[32];
        double share_bytes = total_bucket_bytes ? (100.0 * (double)rows[i]->bucket_bytes / (double)total_bucket_bytes) : 0.0;
        double share_files = total_bucket_files ? (100.0 * (double)rows[i]->bucket_files / (double)total_bucket_files) : 0.0;
        double user_bytes_pct = total_user_bytes ? (100.0 * (double)rows[i]->bucket_bytes / (double)total_user_bytes) : 0.0;
        double user_files_pct = total_user_files ? (100.0 * (double)rows[i]->bucket_files / (double)total_user_files) : 0.0;

        char bpf[96];
        char tpf[96];

        human_bytes(rows[i]->bucket_bytes, bb, sizeof(bb));
        human_bytes(rows[i]->total_bytes, tb, sizeof(tb));
        format_count_pretty_inline(rows[i]->bucket_files, bpf, sizeof(bpf));
        format_count_pretty_inline(rows[i]->total_files, tpf, sizeof(tpf));
        contribution_cell_color(share_files, file_bg, sizeof(file_bg));
        contribution_cell_color(share_bytes, byte_bg, sizeof(byte_bg));

        fprintf(out, "<tr>");
        emit_compact_path_cell(out, rows[i]->path, base_prefix, level_idx);
        fprintf(out,
                "<td class=\"r\" style=\"background:%s\">%s</td><td class=\"r\" style=\"background:%s\">%.1f</td><td class=\"r\" style=\"background:%s\">%s</td><td class=\"r\" style=\"background:%s\">%.1f</td><td class=\"r\">%s</td><td class=\"r\">%" PRIu64 "</td><td class=\"r\">%s</td><td class=\"r\">%.1f</td><td class=\"r\">%.1f</td></tr>\n",
                file_bg,
                bpf,
                file_bg,
                share_files,
                byte_bg,
                bb,
                byte_bg,
                share_bytes,
                tpf,
                rows[i]->total_dirs,
                tb,
                user_bytes_pct,
                user_files_pct);
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
                                   const summary_t *heat_sum) {
    FILE *out = counted_fopen(filename, "w");
    char *base_prefix = NULL;
    path_row_map_t maps[BUCKET_DETAIL_LEVELS_MAX];
    size_t i;
    uint64_t bucket_files = 0;
    uint64_t bucket_bytes = 0;
    uint64_t total_user_files = 0;
    uint64_t total_user_bytes = 0;
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
    fprintf(out, "body{font-family:\"DejaVu Sans Mono\",\"Consolas\",monospace;margin:18px;color:#1f2328;background:#fcfcf8}\n");
    fprintf(out, ".bucket-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%%;margin-bottom:18px}\n");
    fprintf(out, "h1,h2{margin:0 0 10px 0;font-weight:600}\n");
    fprintf(out, ".meta{margin:0 0 14px 0;color:#555;line-height:1.5;font-size:12px}\n");
    fprintf(out, ".meta-sub{font-weight:400;color:#666}\n");
    fprintf(out, ".note{font-size:11px;color:#666;margin-bottom:14px;max-width:1200px}\n");
    fprintf(out, ".table-trunc-note{font-size:11px;color:#555;margin:0 0 10px;max-width:1200px;line-height:1.45}\n");
    fprintf(out, ".bucket-help{margin:0 0 16px;border:1px solid #ddd2c8;border-radius:8px;background:#faf8f4;max-width:1200px;font-size:12px;line-height:1.55;color:#555}\n");
    fprintf(out, ".bucket-help summary{cursor:pointer;padding:10px 12px;font-weight:600;color:#4a4034;list-style-position:outside}\n");
    fprintf(out, ".bucket-help summary::-webkit-details-marker{color:#8b7355}\n");
    fprintf(out, ".bucket-help .bucket-help-body{padding:0 12px 12px}\n");
    fprintf(out, ".bucket-help .bucket-help-body p{margin:0 0 10px}\n");
    fprintf(out, ".bucket-help .bucket-help-body p:last-child{margin-bottom:0}\n");
    fprintf(out, "table{border-collapse:collapse;width:100%%;font-size:11px;table-layout:fixed}\n");
    fprintf(out, "th,td{border:1px solid #d5d0c5;padding:3px 6px;vertical-align:top}\n");
    fprintf(out, "th{background:#ece6da;position:sticky;top:0;z-index:2}\n");
    fprintf(out, "th:first-child,td:first-child{position:sticky;left:0;background:#f8f5ee;z-index:1}\n");
    fprintf(out, "td.r,th.r{text-align:right}\n");
    fprintf(out, "th:first-child{width:40%%;min-width:min(560px,78vw);max-width:720px;box-sizing:border-box}\n");
    fprintf(out, ".path-cell{width:40%%;min-width:min(560px,78vw);max-width:720px;overflow-x:hidden;overflow-y:visible;box-sizing:border-box}\n");
    fprintf(out, ".path-line{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start;gap:8px;min-width:0}\n");
    fprintf(out, ".path-toggle{min-width:0;max-width:100%%;display:block;border:0;background:none;padding:0;margin:0;color:inherit;font:inherit;text-align:left;cursor:pointer}\n");
    fprintf(out, ".path-prefix{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#8b8172;font-size:10px;line-height:1.15;margin-bottom:1px}\n");
    fprintf(out, ".path-tail{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700;color:#1f2328;line-height:1.15}\n");
    fprintf(out, ".copy-path{opacity:0;pointer-events:none;align-self:start;flex-shrink:0;border:1px solid #d8ccb8;background:#f4ede0;color:#6b4c16;border-radius:999px;padding:1px 5px;font:inherit;font-size:9px;line-height:1.2;cursor:pointer;transition:opacity 0.15s ease,background-color 0.15s ease}\n");
    fprintf(out, ".path-cell:hover .copy-path,.path-cell:focus-within .copy-path,.path-cell.expanded .copy-path{opacity:1;pointer-events:auto}\n");
    fprintf(out, ".copy-path:hover{background:#eadfc9}\n");
    fprintf(out, ".path-toggle:hover .path-tail,.path-toggle:focus .path-tail{text-decoration:underline}\n");
    fprintf(out, ".path-full{margin-top:6px;padding:6px 8px;max-width:100%%;box-sizing:border-box;background:#f4efe4;border:1px solid #ddd2bf;border-radius:4px;white-space:normal;word-break:break-all;overflow-wrap:anywhere;font-size:10px;line-height:1.35;color:#4e4538;user-select:all;overflow-x:auto}\n");
    fprintf(out, ".path-cell.expanded .path-prefix{white-space:normal;overflow:hidden;text-overflow:clip;word-break:break-all;overflow-wrap:anywhere}\n");
    fprintf(out, ".heat-map-margins{font-size:11px;color:#555;margin:0 0 14px;line-height:1.5;max-width:1200px}\n");
    fprintf(out, ".heat-map-margins p{margin:6px 0 0}\n");
    fprintf(out, "a{color:#6b4c16;text-decoration:none}\n");
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

    for (i = 0; i < matched_records->count; i++) {
        if (matched_records->items[i].type == 'f') {
            total_user_files++;
            total_user_bytes += matched_records->items[i].size;
        }
    }

    if (aggregate_bucket_for_page_n(maps, detail_levels, details, base_prefix) != 0 ||
        aggregate_totals_for_page_n(maps, detail_levels, matched_records, base_prefix) != 0) {
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
          "<p><strong>Sorting.</strong> Rows are ordered by bucket bytes (largest first).</p>\n"
          "<p><strong>Bucket columns.</strong> &ldquo;Bucket files&rdquo; and &ldquo;Bucket bytes&rdquo; count only files that fall in this age/size bucket. "
          "&ldquo;Share of bucket files/bytes&rdquo; is the fraction of <em>this bucket&rsquo;s</em> total that sits under each path.</p>\n"
          "<p><strong>User share columns.</strong> &ldquo;Share of user bytes/files&rdquo; compares this path to <em>all</em> of the user&rsquo;s files across every bucket (overall footprint).</p>\n"
          "<p><strong>Totals under each path.</strong> Total files, dirs, and bytes include everything recorded below that directory prefix.</p>\n",
          out);
    fprintf(out,
            "<p><strong>Levels 1&ndash;%d.</strong> Each table groups paths by cumulative directory depth below the shared base path "
            "(one segment per level). A row appears at level <em>n</em> only when there is at least one path segment at that depth.</p>\n",
            detail_levels);
    fputs(
          "<p><strong>Heat colors.</strong> Stronger red in the bucket-share cells means a larger share of this bucket&rsquo;s bytes or files.</p>\n"
          "</div></details>\n",
          out);

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
          "document.querySelectorAll('.copy-path').forEach(function(btn){btn.addEventListener('click',function(ev){var text=btn.getAttribute('data-copy');var old=btn.textContent;ev.preventDefault();ev.stopPropagation();copyText(text).then(function(){btn.textContent='Copied';setTimeout(function(){btn.textContent=old;},900);}).catch(function(){btn.textContent='Copy?';setTimeout(function(){btn.textContent=old;},1200);});});});\n"
          "document.querySelectorAll('.path-toggle').forEach(function(btn){btn.addEventListener('click',function(ev){var cell=btn.closest('.path-cell');var full=cell.querySelector('.path-full');var expanded=cell.classList.toggle('expanded');full.hidden=!expanded;btn.setAttribute('aria-expanded',expanded?'true':'false');ev.preventDefault();});});\n"
          "})();\n"
          "</script>\n", out);
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
    struct stat st;

    if (g_bucket_output_dir[0] == '\0') return -1;

    if (stat(g_bucket_output_dir, &st) != 0) {
        if (mkdir(g_bucket_output_dir, 0777) != 0) return -1;
    } else if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }

    return 0;
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

static int parse_ereport_thread_count(void);

typedef struct {
    const char *username;
    int all_users;
    uint64_t distinct_uids;
    const char *basis_str;
    int bucket_detail_levels;
    bucket_details_t (*details)[SIZE_BUCKETS];
    const matched_records_t *matched_records;
    const summary_t *sum_ref;
    int stub_mode;
    atomic_size_t next_task;
    atomic_int any_fail;
    ereport_run_stats_t *run_stats;
} bucket_emit_ctx_t;

typedef struct {
    bucket_emit_ctx_t *ctx;
} bucket_emit_thread_arg_t;

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
            page_rc = emit_bucket_detail_page(fn, c->username, c->all_users, c->distinct_uids, c->basis_str, ab, sb,
                                              c->bucket_detail_levels, &c->details[ab][sb], c->matched_records,
                                              c->sum_ref);
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
                                        bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS],
                                        const matched_records_t *matched_records,
                                        ereport_run_stats_t *run_stats) {
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
    ctx.sum_ref = sum_ref;
    ctx.stub_mode = (bucket_detail_levels == 0 && sum_ref != NULL) ? 1 : 0;
    ctx.run_stats = run_stats;

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
            return atomic_load(&ctx.any_fail) ? -1 : 0;
        }
    }

    for (ti = 0; ti < (size_t)nw; ti++) pthread_join(tids[ti], NULL);
    free(tids);
    free(args);
    return atomic_load(&ctx.any_fail) ? -1 : 0;
}

static double clamp01(double x) {
    if (x < 0.0) return 0.0;
    if (x > 1.0) return 1.0;
    return x;
}

static void heatmap_color(uint64_t cell_bytes,
                          int age_bucket,
                          uint64_t max_cell_bytes,
                          char *buf,
                          size_t sz) {
    const int empty_r = 230, empty_g = 244, empty_b = 234;
    const int low_r   = 228, low_g   = 244, low_b   = 223;
    const int high_r  = 245, high_g  = 214, high_b  = 214;
    double volume_score;
    double age_score;
    double score;
    int r, g, b;

    if (cell_bytes == 0 || max_cell_bytes == 0) {
        snprintf(buf, sz, "rgb(%d,%d,%d)", empty_r, empty_g, empty_b);
        return;
    }

    volume_score = (double)cell_bytes / (double)max_cell_bytes;
    volume_score = clamp01(volume_score);
    age_score = (double)age_bucket / (double)(AGE_BUCKETS - 1);
    score = 0.30 * volume_score + 0.55 * age_score + 0.15 * (volume_score * age_score);
    score = clamp01(score);

    r = (int)(low_r + (high_r - low_r) * score + 0.5);
    g = (int)(low_g + (high_g - low_g) * score + 0.5);
    b = (int)(low_b + (high_b - low_b) * score + 0.5);

    snprintf(buf, sz, "rgb(%d,%d,%d)", r, g, b);
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

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            dst->bytes[ab][sb] += src->bytes[ab][sb];
            dst->files[ab][sb] += src->files[ab][sb];
        }
    }
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

        sum->scanned_records++;
        if (progress) {
            progress->scanned_records++;
            progress_maybe_flush(progress, run_stats);
        }

        record_match = all_users || ((uid_t)r.uid == target_uid);
        skip_paths = record_match && bucket_detail_levels == 0;

        if (!record_match) {
            if (r.path_len > 0) {
                if (fseeko(fp, (off_t)r.path_len, SEEK_CUR) != 0) {
                    fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    break;
                }
            }
            continue;
        }

        if (skip_paths) {
            if (r.path_len > 0) {
                if (fseeko(fp, (off_t)r.path_len, SEEK_CUR) != 0) {
                    fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    break;
                }
            }
            pathbuf = NULL;
        } else {
            if (r.path_len > 0) {
                pathbuf = (char *)malloc((size_t)r.path_len + 1);
                if (!pathbuf) {
                    fprintf(stderr, "warn: path alloc failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    break;
                }
                if (counted_fread(pathbuf, 1, r.path_len, fp) != r.path_len) {
                    fprintf(stderr, "warn: path read failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    free(pathbuf);
                    break;
                }
                pathbuf[r.path_len] = '\0';
            } else {
                pathbuf = strdup("");
                if (!pathbuf) {
                    fprintf(stderr, "warn: path alloc failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    break;
                }
            }
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

            if (count_bytes) {
                sum->total_capacity_bytes += r.size;
                sum->total_bytes += r.size;
                sum->bytes[ab][sb] += r.size;
            }

            if (!skip_paths) {
                if (bucket_details_append(&details[ab][sb], pathbuf, accounted_size) != 0) {
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
                       &arg->matched_records,
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

static void free_chunk_array_rows(file_chunk_t *chunks, size_t count) {
    size_t j;
    for (j = 0; j < count; j++) free(chunks[j].path);
    free(chunks);
}

static int append_chunk(file_chunk_t **chunks,
                        size_t *count,
                        size_t *cap,
                        const char *path,
                        uint64_t start_offset,
                        uint64_t end_offset,
                        size_t file_index) {
    file_chunk_t *tmp;

    if (*count == *cap) {
        size_t new_cap = (*cap == 0) ? 64 : (*cap * 2);
        tmp = (file_chunk_t *)realloc(*chunks, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        *chunks = tmp;
        *cap = new_cap;
    }

    (*chunks)[*count].path = strdup(path);
    if (!(*chunks)[*count].path) return -1;
    (*chunks)[*count].start_offset = start_offset;
    (*chunks)[*count].end_offset = end_offset;
    (*chunks)[*count].file_index = file_index;
    (*count)++;
    return 0;
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

static int bin_ckpt_sidecar_path(const char *bin_path, char *out, size_t out_sz) {
    int n = snprintf(out, out_sz, "%s.ckpt", bin_path);
    return (n < 0 || (size_t)n >= out_sz) ? -1 : 0;
}

static int load_bin_ckpt(const char *bin_path, uint64_t file_sz, uint64_t **offs_out, size_t *n_out) {
    char ckpath[PATH_MAX];
    crawl_ckpt_file_hdr_t ch;
    uint64_t *buf = NULL;
    size_t i;
    FILE *fp;

    *offs_out = NULL;
    *n_out = 0;
    if (bin_ckpt_sidecar_path(bin_path, ckpath, sizeof(ckpath)) != 0) return -1;
    fp = counted_fopen(ckpath, "rb");
    if (!fp) return -1;
    if (counted_fread(&ch, sizeof(ch), 1, fp) != 1) {
        counted_fclose(fp);
        errno = EINVAL;
        return -1;
    }
    if (memcmp(ch.magic, CRAWL_CKPT_MAGIC, CRAWL_CKPT_MAGIC_LEN) != 0 || ch.version != CRAWL_CKPT_ONDISK_VERSION ||
        ch.stride_bytes != CRAWL_CKPT_STRIDE_BYTES || ch.num_offsets == 0 || ch.num_offsets > (uint64_t)(SIZE_MAX / sizeof(uint64_t))) {
        counted_fclose(fp);
        errno = EINVAL;
        return -1;
    }
    buf = (uint64_t *)malloc((size_t)ch.num_offsets * sizeof(*buf));
    if (!buf) {
        counted_fclose(fp);
        return -1;
    }
    if (counted_fread(buf, sizeof(uint64_t), (size_t)ch.num_offsets, fp) != (size_t)ch.num_offsets || counted_fclose(fp) != 0) {
        free(buf);
        errno = EINVAL;
        return -1;
    }
    if (buf[0] != sizeof(bin_file_header_t)) {
        free(buf);
        errno = EINVAL;
        return -1;
    }
    for (i = 1; i < (size_t)ch.num_offsets; i++) {
        if (buf[i] <= buf[i - 1] || buf[i] > file_sz) {
            free(buf);
            errno = EINVAL;
            return -1;
        }
    }
    *offs_out = buf;
    *n_out = (size_t)ch.num_offsets;
    return 0;
}

static int build_chunks_for_segment(const char *path,
                                    size_t file_index,
                                    uint64_t chunk_target_bytes,
                                    uint64_t seg_start,
                                    uint64_t seg_end,
                                    file_chunk_t **chunks_out,
                                    size_t *chunk_count_out,
                                    unsigned int *file_chunk_counter_out) {
    FILE *fp = NULL;
    uint64_t chunk_start;
    uint64_t next_target;
    file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    size_t chunk_cap = 0;
    unsigned int fc = 0;
    int rc = -1;

    *chunks_out = NULL;
    *chunk_count_out = 0;
    *file_chunk_counter_out = 0;

    if (seg_start > seg_end) return -1;

    fp = counted_fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }

    if (fseeko(fp, (off_t)seg_start, SEEK_SET) != 0) goto out;

    if (chunk_target_bytes == 0) chunk_target_bytes = PARSE_CHUNK_BYTES;
    chunk_start = seg_start;
    next_target = chunk_start + chunk_target_bytes;

    for (;;) {
        bin_record_hdr_t r;
        off_t record_start = ftello(fp);
        off_t record_end;

        if (record_start < 0) goto out;

        if ((uint64_t)record_start >= seg_end) {
            if ((uint64_t)record_start > seg_end) goto out;
            if ((uint64_t)record_start > chunk_start) {
                if (append_chunk(&chunks, &chunk_count, &chunk_cap, path, chunk_start, (uint64_t)record_start, file_index) != 0) goto out;
                fc++;
            }
            rc = 0;
            goto out;
        }

        if (counted_fread(&r, sizeof(r), 1, fp) != 1) {
            if (feof(fp)) {
                fprintf(stderr, "warn: unexpected EOF in segment of %s\n", path);
            }
            goto out;
        }

        if (fseeko(fp, (off_t)r.path_len, SEEK_CUR) != 0) goto out;
        record_end = ftello(fp);
        if (record_end < 0) goto out;
        if ((uint64_t)record_end > seg_end) goto out;

        while ((uint64_t)record_end >= next_target) {
            if ((uint64_t)record_end > chunk_start) {
                if (append_chunk(&chunks, &chunk_count, &chunk_cap, path, chunk_start, (uint64_t)record_end, file_index) != 0) goto out;
                fc++;
            }
            chunk_start = (uint64_t)record_end;
            next_target = chunk_start + chunk_target_bytes;
        }
    }

out:
    if (fp) counted_fclose(fp);
    if (rc != 0) {
        free_chunk_array_rows(chunks, chunk_count);
        return -1;
    }
    *chunks_out = chunks;
    *chunk_count_out = chunk_count;
    *file_chunk_counter_out = fc;
    return 0;
}

typedef struct {
    const char *path;
    size_t file_index;
    uint64_t chunk_target_bytes;
    const uint64_t *offs;
    size_t n_offs;
    uint64_t file_size;
    int seg_a;
    int seg_b;
    file_chunk_t *chunks;
    size_t chunk_count;
    size_t chunk_cap;
    unsigned int fc;
    int rc;
} chunk_bundle_t;

static int chunk_list_take_all(file_chunk_t **dst, size_t *dn, size_t *dcap, file_chunk_t *src, size_t sn) {
    size_t j;

    for (j = 0; j < sn; j++) {
        if (append_chunk(dst, dn, dcap, src[j].path, src[j].start_offset, src[j].end_offset, src[j].file_index) != 0) {
            for (; j < sn; j++) free(src[j].path);
            free(src);
            return -1;
        }
        free(src[j].path);
    }
    free(src);
    return 0;
}

static void *chunk_bundle_worker_main(void *arg) {
    chunk_bundle_t *b = (chunk_bundle_t *)arg;
    int si;

    b->rc = 0;
    b->chunks = NULL;
    b->chunk_count = 0;
    b->chunk_cap = 0;
    b->fc = 0;

    for (si = b->seg_a; si < b->seg_b; si++) {
        file_chunk_t *seg_chunks = NULL;
        size_t seg_count = 0;
        unsigned int seg_fc = 0;
        uint64_t lo = b->offs[si];
        uint64_t hi = ((size_t)si + 1U < b->n_offs) ? b->offs[(size_t)si + 1U] : b->file_size;

        if (build_chunks_for_segment(b->path, b->file_index, b->chunk_target_bytes, lo, hi, &seg_chunks, &seg_count, &seg_fc) != 0) {
            free_chunk_array_rows(b->chunks, b->chunk_count);
            b->chunks = NULL;
            b->chunk_count = 0;
            b->rc = -1;
            return NULL;
        }
        if (chunk_list_take_all(&b->chunks, &b->chunk_count, &b->chunk_cap, seg_chunks, seg_count) != 0) {
            free_chunk_array_rows(b->chunks, b->chunk_count);
            b->chunks = NULL;
            b->chunk_count = 0;
            b->rc = -1;
            return NULL;
        }
        b->fc += seg_fc;
    }
    return NULL;
}

static int build_chunks_for_file(const char *path,
                                 size_t file_index,
                                 uint64_t chunk_target_bytes,
                                 file_chunk_t **chunks_out,
                                 size_t *chunk_count_out,
                                 unsigned int *file_chunk_counter_out) {
    struct stat st;
    bin_file_header_t fh;
    uint64_t *offs = NULL;
    size_t n_off = 0;
    uint64_t fsz;
    FILE *fp = NULL;
    int rc = -1;
    file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    unsigned int fc = 0;

    *chunks_out = NULL;
    *chunk_count_out = 0;
    *file_chunk_counter_out = 0;

    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
        fprintf(stderr, "warn: cannot stat %s: %s\n", path, strerror(errno));
        return -1;
    }
    fsz = (uint64_t)st.st_size;

    fp = counted_fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }
    if (counted_fread(&fh, sizeof(fh), 1, fp) != 1) {
        fprintf(stderr, "warn: short read on header: %s\n", path);
        counted_fclose(fp);
        return -1;
    }
    counted_fclose(fp);
    fp = NULL;

    if (memcmp(fh.magic, "NFSCBIN", 7) != 0 || fh.version != FORMAT_VERSION) {
        fprintf(stderr, "warn: bad format/version in %s\n", path);
        return -1;
    }

    if (load_bin_ckpt(path, fsz, &offs, &n_off) != 0) {
        fprintf(stderr, "warn: missing or invalid checkpoint sidecar (.ckpt) for %s\n", path);
        return -1;
    }
    if (n_off == 0 || offs[0] != (uint64_t)sizeof(fh)) {
        fprintf(stderr, "warn: bad checkpoint offsets in %s\n", path);
        free(offs);
        return -1;
    }

    if (chunk_target_bytes == 0) chunk_target_bytes = PARSE_CHUNK_BYTES;

    {
        int nw = parse_ereport_thread_count();
        size_t n_seg = n_off;

        if (nw > (int)n_seg) nw = (int)n_seg;

        if (n_seg <= 1 || nw <= 1) {
            rc = build_chunks_for_segment(path, file_index, chunk_target_bytes, offs[0], fsz, &chunks, &chunk_count, &fc);
            free(offs);
            if (rc != 0) return -1;
            *chunks_out = chunks;
            *chunk_count_out = chunk_count;
            *file_chunk_counter_out = fc;
            return 0;
        }

        {
            chunk_bundle_t *bundles = (chunk_bundle_t *)calloc((size_t)nw, sizeof(*bundles));
            pthread_t *tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
            int w, lo, base, rem;

            if (!bundles || !tids) {
                free(bundles);
                free(tids);
                free(offs);
                return -1;
            }
            lo = 0;
            base = (int)n_seg / nw;
            rem = (int)n_seg % nw;
            for (w = 0; w < nw; w++) {
                int cnt = base + (w < rem ? 1 : 0);
                bundles[w].path = path;
                bundles[w].file_index = file_index;
                bundles[w].chunk_target_bytes = chunk_target_bytes;
                bundles[w].offs = offs;
                bundles[w].n_offs = n_off;
                bundles[w].file_size = fsz;
                bundles[w].seg_a = lo;
                bundles[w].seg_b = lo + cnt;
                lo += cnt;
            }
            for (w = 0; w < nw; w++) {
                if (pthread_create(&tids[w], NULL, chunk_bundle_worker_main, &bundles[w]) != 0) {
                    int j;
                    for (j = 0; j < w; j++) pthread_join(tids[j], NULL);
                    for (j = w; j < nw; j++) {
                        free_chunk_array_rows(bundles[j].chunks, bundles[j].chunk_count);
                        bundles[j].chunks = NULL;
                    }
                    free(bundles);
                    free(tids);
                    free(offs);
                    return -1;
                }
            }
            for (w = 0; w < nw; w++) pthread_join(tids[w], NULL);

            chunks = NULL;
            chunk_count = 0;
            {
                size_t cap = 0;
                fc = 0;
                rc = 0;
                for (w = 0; w < nw; w++) {
                    if (bundles[w].rc != 0) {
                        rc = -1;
                        break;
                    }
                    if (chunk_list_take_all(&chunks, &chunk_count, &cap, bundles[w].chunks, bundles[w].chunk_count) != 0) {
                        rc = -1;
                        break;
                    }
                    bundles[w].chunks = NULL;
                    bundles[w].chunk_count = 0;
                    fc += bundles[w].fc;
                }
                if (rc != 0) {
                    free_chunk_array_rows(chunks, chunk_count);
                    for (w = 0; w < nw; w++) free_chunk_array_rows(bundles[w].chunks, bundles[w].chunk_count);
                    free(bundles);
                    free(tids);
                    free(offs);
                    return -1;
                }
            }
            free(bundles);
            free(tids);
        }
    }

    free(offs);
    *chunks_out = chunks;
    *chunk_count_out = chunk_count;
    *file_chunk_counter_out = fc;
    return 0;
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

        r = build_chunks_for_file(pool->paths[i], i, pool->chunk_targets[i], &local_chunks, &local_count, &fc);

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

static int emit_html(const char *report_path,
                     const char *username,
                     int all_users,
                     uint64_t distinct_uids,
                     int bucket_detail_levels,
                     uid_t uid,
                     const char *basis_str,
                     const summary_t *sum,
                     size_t input_files,
                     int threads_used,
                     size_t crawl_source_count,
                     const char *crawl_sources_label) {
    FILE *out = counted_fopen(report_path, "w");
    int ab, sb;

    if (!out) return -1;
    uint64_t max_cell_bytes = 0;

    fprintf(out, "<!DOCTYPE html>\n");
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
    fprintf(out, "table{border-collapse:collapse;margin-top:16px;min-width:900px}\n");
    fprintf(out, ".heatmap-caption{caption-side:top;text-align:left;font-size:12px;color:#555;line-height:1.45;max-width:720px;margin:10px 0 12px;padding:0}\n");
    fprintf(out, ".heatmap-totals-note{font-size:12px;color:#555;max-width:min(900px,100%%);margin:12px 0 18px;line-height:1.5;padding:0}\n");
    fprintf(out, ".heatmap-corner{font-weight:600}\n");
    fprintf(out, "th,td{border:1px solid #ccc;padding:4px 6px;text-align:right}\n");
    fprintf(out, "th:first-child,td:first-child{text-align:left}\n");
    fprintf(out, "th{background:#f4f4f4}\n");
    fprintf(out, ".tot{font-weight:600;background:#fafafa}\n");
    fprintf(out, ".cell,.tot-cell{transition:background-color 0.2s ease}\n");
    fprintf(out, ".cell a,.tot-block{display:block;color:inherit;text-decoration:none;text-align:center;min-height:42px;padding:1px 0}\n");
    fprintf(out, ".cell-main{display:flex;align-items:center;justify-content:center;gap:4px;flex-wrap:wrap;line-height:1}\n");
    fprintf(out, ".cell-bytes{font-size:14px;font-weight:600}\n");
    fprintf(out, ".cell-pct{font-size:10px;font-weight:700;color:#6a4d1a;background:rgba(255,255,255,0.78);padding:1px 4px;border-radius:999px;line-height:1}\n");
    fprintf(out, ".cell-sub{color:#666;font-size:10px;margin-top:2px;line-height:1.05}\n");
    fprintf(out, ".cell.active{outline:3px solid #8a6a2a;outline-offset:-3px}\n");
    fprintf(out, ".drawer-backdrop{position:fixed;inset:0;background:rgba(0,0,0,0.28);opacity:0;pointer-events:none;transition:opacity 0.2s ease;z-index:20}\n");
    fprintf(out, ".drawer-backdrop.open{opacity:1;pointer-events:auto}\n");
    fprintf(out, ".drawer{position:fixed;top:0;right:0;width:min(980px,92vw);height:100vh;background:#fff;box-shadow:-8px 0 24px rgba(0,0,0,0.18);transform:translateX(100%%);transition:transform 0.22s ease;z-index:21;display:flex;flex-direction:column}\n");
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
            if (sum->bytes[ab][sb] > max_cell_bytes) max_cell_bytes = sum->bytes[ab][sb];
        }
    }

    fprintf(out, "<table>\n");
    fprintf(out,
            "<caption class=\"heatmap-caption\">Rows: file age (relative to the crawl basis). Columns: file size. Second-line "
            "values are file counts (regular files; device/inode dedup).</caption>\n");
    fprintf(out, "<tr><th scope=\"col\" class=\"heatmap-corner\">Age \xc3\x97 Size</th>");
    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
        fprintf(out, "<th>");
        html_escape(out, size_bucket_names[sb]);
        fprintf(out, "</th>");
    }
    fprintf(out, "<th>Total</th></tr>\n");

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        uint64_t row_total = 0;
        uint64_t row_files = 0;

        fprintf(out, "<tr><td>");
        html_escape(out, age_bucket_names[ab]);
        fprintf(out, "</td>");

        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            char hb[32];
            char bg[32];
            char fline[160];
            double pct = 0.0;
            uint64_t b = sum->bytes[ab][sb];
            uint64_t f = sum->files[ab][sb];

            row_total += b;
            row_files += f;

            if (sum->total_bytes) pct = 100.0 * (double)b / (double)sum->total_bytes;
            human_bytes(b, hb, sizeof(hb));
            heatmap_color(b, ab, max_cell_bytes, bg, sizeof(bg));
            format_count_pretty_inline(f, fline, sizeof(fline));
            fprintf(out,
                    "<td class=\"cell\" style=\"background:%s\"><a class=\"bucket-link\" data-age=\"%d\" data-size=\"%d\" "
                    "href=\"bucket_a%d_s%d.html\"><div class=\"cell-main\"><span class=\"cell-bytes\">%s</span><span "
                    "class=\"cell-pct\">%.0f%%</span></div><div class=\"cell-sub\">%s</div></a></td>",
                    bg,
                    ab,
                    sb,
                    ab,
                    sb,
                    hb,
                    pct,
                    fline);
        }

        {
            char hr[32];
            char bg[32];
            char rf[160];
            double pct = 0.0;

            if (sum->total_bytes) pct = 100.0 * (double)row_total / (double)sum->total_bytes;
            human_bytes(row_total, hr, sizeof(hr));
            format_count_pretty_inline(row_files, rf, sizeof(rf));
            contribution_cell_color(pct, bg, sizeof(bg));
            fprintf(out,
                    "<td class=\"tot tot-cell\" style=\"background:%s\"><div class=\"tot-block\"><div "
                    "class=\"cell-main\"><span class=\"cell-bytes\">%s</span><span class=\"cell-pct\">%.0f%%</span></div><div "
                    "class=\"cell-sub\">%s</div></div></td>",
                    bg,
                    hr,
                    pct,
                    rf);
        }

        fprintf(out, "</tr>\n");
    }

    fprintf(out, "<tr class=\"tot\"><td>Total</td>");
    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
        uint64_t col_total = 0;
        uint64_t col_files = 0;
        char hc[32];
        char bg[32];
        char cf[160];
        double pct = 0.0;

        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            col_total += sum->bytes[ab][sb];
            col_files += sum->files[ab][sb];
        }
        if (sum->total_bytes) pct = 100.0 * (double)col_total / (double)sum->total_bytes;
        human_bytes(col_total, hc, sizeof(hc));
        format_count_pretty_inline(col_files, cf, sizeof(cf));
        contribution_cell_color(pct, bg, sizeof(bg));
        fprintf(out,
                "<td class=\"tot tot-cell\" style=\"background:%s\"><div class=\"tot-block\"><div "
                "class=\"cell-main\"><span class=\"cell-bytes\">%s</span><span class=\"cell-pct\">%.0f%%</span></div><div "
                "class=\"cell-sub\">%s</div></div></td>",
                bg,
                hc,
                pct,
                cf);
    }
    {
        char ht[32];
        char tf[160];

        human_bytes(sum->total_bytes, ht, sizeof(ht));
        format_count_pretty_inline(sum->total_files, tf, sizeof(tf));
        fprintf(out,
                "<td class=\"tot tot-cell\"><div class=\"tot-block\"><div class=\"cell-main\"><span "
                "class=\"cell-bytes\">%s</span><span class=\"cell-pct\">100%%</span></div><div class=\"cell-sub\">%s</div></div></td>",
                ht,
                tf);
    }
    fprintf(out, "</tr>\n");

    fprintf(out, "</table>\n");

    fprintf(out,
            "<p class=\"heatmap-totals-note\"><strong>Row and column totals.</strong> The rightmost column is the total for "
            "each age <em>row</em> (sum over all size buckets). The bottom row is the total for each size <em>column</em> "
            "(sum over all age buckets). The bottom-right cell is the total across the full heat map; it matches the sum "
            "of the row totals and the sum of the column totals.</p>\n");

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
        fprintf(out, "</dl></article>\n");

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
    uint64_t distinct_uid_count = 0;
    double t0, t1;
    ereport_run_stats_t run_stats;

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

    if (argc < 2) {
        fprintf(stderr, "Usage: %s [--bucket-details N] <username|uid> <atime|mtime|ctime> [bin_dir ...]\n", argv[0]);
        fprintf(stderr, "       %s [--bucket-details N] <atime|mtime|ctime> [bin_dir ...]  (all users → ./all_users/)\n",
                argv[0]);
        fprintf(stderr, "Optional --bucket-details N (1…%d): full per-bucket directory tables; omit for brief buckets.\n",
                BUCKET_DETAIL_LEVELS_MAX);
        fprintf(stderr, "Flags must appear first. Thread count: EREPORT_THREADS (default %d).\n", DEFAULT_THREADS);
        return 2;
    }

    {
        int ac = argc;
        char **av = argv;
        while (ac > 1 && strcmp(av[1], "--bucket-details") == 0) {
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
        }
    }

    if (argc < 2) {
        fprintf(stderr, "ereport: missing arguments (need a time basis, or username and time basis)\n");
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
        if (argc < 3) {
            fprintf(stderr, "Usage: %s [--bucket-details N] <username|uid> <atime|mtime|ctime> [bin_dir ...]\n", argv[0]);
            fprintf(stderr, "       %s [--bucket-details N] <atime|mtime|ctime> [bin_dir ...]  (all users → ./all_users/)\n",
                    argv[0]);
            fprintf(stderr, "Optional --bucket-details N (1…%d); omit for brief bucket pages. Thread count: EREPORT_THREADS "
                           "(default %d).\n",
                    BUCKET_DETAIL_LEVELS_MAX, DEFAULT_THREADS);
            return 2;
        }

        user_spec = argv[1];
        basis_str = argv[2];

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

        if (parse_time_basis(basis_str, &basis) != 0) {
            free((void *)bin_dirs);
            die("time basis must be one of: atime, mtime, ctime");
        }

        if (resolve_target_user(user_spec, &target_uid, display_name, sizeof(display_name)) != 0) {
            fprintf(stderr, "unknown user or uid: %s\n", user_spec);
            free((void *)bin_dirs);
            return 1;
        }
    }

    format_input_dirs_label(bin_dirs, bin_dir_count, input_dirs_label, sizeof(input_dirs_label));
    format_storage_base_paths_label(bin_dirs, bin_dir_count, storage_base_paths_label, sizeof(storage_base_paths_label));

    set_bucket_output_dir(display_name);
    if (snprintf(report_path, sizeof(report_path), "%s/index.html", g_bucket_output_dir) >= (int)sizeof(report_path)) {
        fprintf(stderr, "report path too long for %s\n", g_bucket_output_dir);
        free((void *)bin_dirs);
        return 1;
    }

    if (scan_dirs_collect_files(bin_dirs, bin_dir_count, target_uid, all_users_mode, &paths, &path_count) != 0) {
        free((void *)bin_dirs);
        return 1;
    }
    free((void *)bin_dirs);
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
            free(file_states);
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
            free(file_states);
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
            free(file_states);
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
            free(file_states);
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
                free(file_states);
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
                    free_chunk_array_rows(prep_chunks[i], prep_chunk_counts[i]);
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
                    if (prep_rc[i] == 0 && prep_chunks[i]) free_chunk_array_rows(prep_chunks[i], prep_chunk_counts[i]);
                }
                free(chunk_targets);
                free(prep_rc);
                free(prep_chunks);
                free(prep_chunk_counts);
                for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                free(paths);
                free(file_states);
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
        free(file_states);
        return 1;
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
        free(file_states);
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
        free(file_states);
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
                free(tids);
                free(args);
                for (j = 0; j < (int)chunk_count; j++) free(chunks[j].path);
                free(chunks);
                for (j = 0; (size_t)j < path_count; j++) free(paths[j]);
                free(paths);
                free(file_states);
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

    atomic_store(&run_stats.parse_workers_done, 1);
    atomic_store(&run_stats.finalize_phase, 1);

    for (i = 0; i < threads_used; i++) {
        pthread_join(tids[i], NULL);
        summary_merge(&final_sum, &args[i].summary);
        if (bucket_detail_levels > 0) {
            if (matched_records_merge(&final_matched_records, &args[i].matched_records) != 0) {
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
                free(tids);
                if (all_users_mode) {
                    for (i = 0; i < threads; i++) uid_accum_destroy(&args[i].uid_distinct);
                }
                free(args);
                for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
                free(chunks);
                for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                free(paths);
                free(file_states);
                pthread_mutex_destroy(&queue.mutex);
                inode_set_destroy(&seen_inodes);
                return 1;
            }
            for (ab = 0; ab < AGE_BUCKETS; ab++) {
                for (sb = 0; sb < SIZE_BUCKETS; sb++) {
                    if (bucket_details_merge(&final_details[ab][sb], &args[i].details[ab][sb]) != 0) {
                        fprintf(stderr, "allocation failed merging bucket details\n");
                        if (stats_thread_started) {
                            atomic_store(&run_stats.stop_stats, 1);
                            pthread_join(stats_thread, NULL);
                            clear_status_line();
                        }
                        matched_records_free(&final_matched_records);
                        for (ab = 0; ab < AGE_BUCKETS; ab++) {
                            for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
                        }
                        free(tids);
                        if (all_users_mode) {
                            for (i = 0; i < threads; i++) uid_accum_destroy(&args[i].uid_distinct);
                        }
                        free(args);
                        for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
                        free(chunks);
                        for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                        free(paths);
                        free(file_states);
                        pthread_mutex_destroy(&queue.mutex);
                        inode_set_destroy(&seen_inodes);
                        return 1;
                    }
                }
            }
        }
    }

    if (all_users_mode) {
        uid_accum_t merged_uids;
        if (uid_accum_init(&merged_uids, 65536) != 0) {
            fprintf(stderr, "allocation failed merging uid tallies\n");
            for (i = 0; i < threads; i++) uid_accum_destroy(&args[i].uid_distinct);
            free(tids);
            free(args);
            for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
            free(chunks);
            for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
            free(paths);
            free(file_states);
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
                uid_accum_destroy(&merged_uids);
                for (j = i; j < threads; j++) uid_accum_destroy(&args[j].uid_distinct);
                free(tids);
                free(args);
                for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
                free(chunks);
                for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                free(paths);
                free(file_states);
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
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
        }
        free(tids);
        free(args);
        for (i = 0; i < (int)chunk_count; i++) free(chunks[i].path);
        free(chunks);
        for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
        free(paths);
        free(file_states);
        pthread_mutex_destroy(&queue.mutex);
        inode_set_destroy(&seen_inodes);
        matched_records_free(&final_matched_records);
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
        }
        return 1;
    }

    if (emit_all_bucket_detail_pages(display_name, all_users_mode, distinct_uid_count, basis_str,
                                     bucket_detail_levels,
                                     &final_sum,
                                     final_details,
                                     &final_matched_records,
                                     &run_stats) != 0) {
        fprintf(stderr, "failed to write bucket detail pages\n");
    } else {
        bucket_pages_written = AGE_BUCKETS * SIZE_BUCKETS;
    }

    atomic_store(&run_stats.finalize_phase, 3);

    if (emit_html(report_path, display_name, all_users_mode, distinct_uid_count, bucket_detail_levels, target_uid,
                   basis_str, &final_sum, path_count, threads_used, bin_dir_count,
                   storage_base_paths_label) != 0) {
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
    free(file_states);
    pthread_mutex_destroy(&queue.mutex);
    inode_set_destroy(&seen_inodes);
    matched_records_free(&final_matched_records);
    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
    }

    return 0;
}
