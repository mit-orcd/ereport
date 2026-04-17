/*
 * ereport.c
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
 *   ./ereport <username|uid> <atime|mtime|ctime> [bin_dir] [threads]
 * Optional thread count: last arg overrides EREPORT_THREADS (else default 32).
 *
 * Writes outputs under ./<resolved_username>/ (cwd). Falls back to ./tmp/ only if
 * the username is empty or unusably long after sanitization.
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

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define FILE_MAGIC_LEN 8
#define FORMAT_VERSION 2
#define DEFAULT_THREADS 32
#define WINDOW_SECONDS 10
#define PROGRESS_FLUSH_INTERVAL 1024U
#define PARSE_CHUNK_BYTES (32ULL << 20)
#define PARSE_CHUNK_MIN_BYTES (1ULL << 20)


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
    "<4K",
    "4K-1M",
    "1M-100M",
    "100M-1G",
    "1G-10G",
    "10G+"
};

static const char *age_bucket_names[AGE_BUCKETS] = {
    "<30d",
    "30-90d",
    "90-180d",
    "180-365d",
    "1-3y",
    "3y+"
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
    work_queue_t *queue;
    file_state_t *file_states;
    uid_t target_uid;
    time_basis_t basis;
    time_t now;
    inode_set_t *seen_inodes;
    summary_t summary;
    bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS];
    matched_records_t matched_records;
} worker_arg_t;

typedef struct {
    uint64_t scanned_input_files;
    uint64_t scanned_records;
    uint64_t matched_records;
    uint64_t bad_input_files;
} progress_local_t;

static atomic_ullong g_io_opendir_calls = 0;
static atomic_ullong g_io_readdir_calls = 0;
static atomic_ullong g_io_closedir_calls = 0;
static atomic_ullong g_io_fopen_calls = 0;
static atomic_ullong g_io_fclose_calls = 0;
static atomic_ullong g_io_fread_calls = 0;
static atomic_ullong g_progress_scanned_input_files = 0;
static atomic_ullong g_progress_scanned_records = 0;
static atomic_ullong g_progress_matched_records = 0;
static atomic_ullong g_progress_bad_input_files = 0;
static atomic_ullong g_bucket_records[WINDOW_SECONDS];
static atomic_ullong g_window_records = 0;
static atomic_int g_bucket_index = 0;
static atomic_uint g_seconds_seen = 0;
static atomic_int g_stop_stats = 0;

static uint64_t g_progress_input_files_total = 0;
static double g_run_start_sec = 0.0;
static double g_records_rate_sum = 0.0;
static double g_records_rate_min = 0.0;
static double g_records_rate_max = 0.0;
static uint64_t g_records_rate_samples = 0;

static const char *g_input_layout = "legacy";
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

static void format_duration(double sec, char *buf, size_t sz) {
    long total = sec > 0.0 ? (long)(sec + 0.5) : 0;
    long h = total / 3600;
    long m = (total % 3600) / 60;
    long s = total % 60;
    snprintf(buf, sz, "%02ld:%02ld:%02ld", h, m, s);
}

static void progress_flush_local(progress_local_t *progress) {
    int idx;

    if (!progress) return;
    if (progress->scanned_input_files == 0 &&
        progress->scanned_records == 0 &&
        progress->matched_records == 0 &&
        progress->bad_input_files == 0) return;

    idx = atomic_load(&g_bucket_index);
    if (progress->scanned_input_files > 0) {
        atomic_fetch_add(&g_progress_scanned_input_files, progress->scanned_input_files);
    }
    if (progress->scanned_records > 0) {
        atomic_fetch_add(&g_progress_scanned_records, progress->scanned_records);
        atomic_fetch_add(&g_window_records, progress->scanned_records);
        atomic_fetch_add(&g_bucket_records[idx], progress->scanned_records);
    }
    if (progress->matched_records > 0) {
        atomic_fetch_add(&g_progress_matched_records, progress->matched_records);
    }
    if (progress->bad_input_files > 0) {
        atomic_fetch_add(&g_progress_bad_input_files, progress->bad_input_files);
    }

    memset(progress, 0, sizeof(*progress));
}

static void progress_maybe_flush(progress_local_t *progress) {
    if (!progress) return;
    if (progress->scanned_records >= PROGRESS_FLUSH_INTERVAL) progress_flush_local(progress);
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

    {
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

static int extract_row_paths(const char *path,
                             const char *base_prefix,
                             char *row1,
                             size_t row1_sz,
                             char *row2,
                             size_t row2_sz,
                             int *has_row2) {
    const char *p = path;
    const char *c1;
    const char *c2;
    size_t comp1_len;
    size_t comp2_len;
    size_t plen = base_prefix ? strlen(base_prefix) : 0;

    *has_row2 = 0;
    if (!starts_with_dir_prefix(path, base_prefix)) return 0;

    if (base_prefix && base_prefix[0] != '\0' && strcmp(base_prefix, "/") != 0) {
        p += plen;
        if (*p == '/') p++;
    } else if (base_prefix && strcmp(base_prefix, "/") == 0 && *p == '/') {
        p++;
    }

    if (*p == '\0') return 0;

    c1 = p;
    while (*p && *p != '/') p++;
    if (*p != '/') return 0;
    comp1_len = (size_t)(p - c1);

    if (!join_path_component(row1, row1_sz, base_prefix ? base_prefix : "", c1, comp1_len)) return 0;

    p++;
    if (*p == '\0') return 1;

    c2 = p;
    while (*p && *p != '/') p++;
    if (*p != '/') return 1;
    comp2_len = (size_t)(p - c2);

    if (!join_path_component(row2, row2_sz, row1, c2, comp2_len)) return 1;
    *has_row2 = 1;
    return 1;
}

static int aggregate_totals_for_page(path_row_map_t *level1,
                                     path_row_map_t *level2,
                                     const matched_records_t *records,
                                     const char *base_prefix) {
    size_t i;

    for (i = 0; i < records->count; i++) {
        char row1[PATH_MAX];
        char row2[PATH_MAX];
        int has_row2;
        const matched_record_t *r = &records->items[i];
        path_row_t *row;

        if (!extract_row_paths(r->path, base_prefix, row1, sizeof(row1), row2, sizeof(row2), &has_row2)) continue;

        row = path_row_map_find(level1, row1);
        if (row) {
            if (r->type == 'f') row->total_files++;
            else if (r->type == 'd') row->total_dirs++;
            row->total_bytes += r->size;
        }

        if (has_row2) {
            row = path_row_map_find(level2, row2);
            if (row) {
                if (r->type == 'f') row->total_files++;
                else if (r->type == 'd') row->total_dirs++;
                row->total_bytes += r->size;
            }
        }
    }

    return 0;
}

static int aggregate_bucket_for_page(path_row_map_t *level1,
                                     path_row_map_t *level2,
                                     const bucket_details_t *details,
                                     const char *base_prefix) {
    size_t i;

    for (i = 0; i < details->count; i++) {
        char row1[PATH_MAX];
        char row2[PATH_MAX];
        int has_row2;
        const detail_record_t *r = &details->items[i];
        path_row_t *row;

        if (!extract_row_paths(r->path, base_prefix, row1, sizeof(row1), row2, sizeof(row2), &has_row2)) continue;

        row = path_row_map_get_or_insert(level1, row1);
        if (!row) return -1;
        row->bucket_files++;
        row->bucket_bytes += r->size;

        if (has_row2) {
            row = path_row_map_get_or_insert(level2, row2);
            if (!row) return -1;
            row->bucket_files++;
            row->bucket_bytes += r->size;
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

static void compact_path_prefix(const char *path, char *buf, size_t sz) {
    const char *slash = strrchr(path, '/');
    size_t prefix_len;
    const size_t keep = 28;

    if (!slash) {
        buf[0] = '\0';
        return;
    }

    prefix_len = (size_t)(slash - path + 1);
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

static void emit_compact_path_cell(FILE *out, const char *path) {
    char prefix[96];
    const char *tail = path_tail_component(path);

    compact_path_prefix(path, prefix, sizeof(prefix));

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
                                    uint64_t total_user_bytes) {
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

    fprintf(out, "<table>\n<thead><tr><th>Path</th><th class=\"r\">Bucket Files</th><th class=\"r\">Share of Bucket Files</th><th class=\"r\">Bucket Bytes</th><th class=\"r\">Share of Bucket Bytes</th><th class=\"r\">Total Files</th><th class=\"r\">Total Dirs</th><th class=\"r\">Total Bytes</th><th class=\"r\">Share of User Bytes</th><th class=\"r\">Share of User Files</th></tr></thead>\n<tbody>\n");
    for (i = 0; i < count; i++) {
        char bb[32];
        char tb[32];
        char file_bg[32];
        char byte_bg[32];
        double share_bytes = total_bucket_bytes ? (100.0 * (double)rows[i]->bucket_bytes / (double)total_bucket_bytes) : 0.0;
        double share_files = total_bucket_files ? (100.0 * (double)rows[i]->bucket_files / (double)total_bucket_files) : 0.0;
        double user_bytes_pct = total_user_bytes ? (100.0 * (double)rows[i]->bucket_bytes / (double)total_user_bytes) : 0.0;
        double user_files_pct = total_user_files ? (100.0 * (double)rows[i]->bucket_files / (double)total_user_files) : 0.0;

        human_bytes(rows[i]->bucket_bytes, bb, sizeof(bb));
        human_bytes(rows[i]->total_bytes, tb, sizeof(tb));
        contribution_cell_color(share_files, file_bg, sizeof(file_bg));
        contribution_cell_color(share_bytes, byte_bg, sizeof(byte_bg));

        fprintf(out, "<tr>");
        emit_compact_path_cell(out, rows[i]->path);
        fprintf(out, "<td class=\"r\" style=\"background:%s\">%" PRIu64 "</td><td class=\"r\" style=\"background:%s\">%.1f</td><td class=\"r\" style=\"background:%s\">%s</td><td class=\"r\" style=\"background:%s\">%.1f</td><td class=\"r\">%" PRIu64 "</td><td class=\"r\">%" PRIu64 "</td><td class=\"r\">%s</td><td class=\"r\">%.1f</td><td class=\"r\">%.1f</td></tr>\n",
                file_bg,
                rows[i]->bucket_files,
                file_bg,
                share_files,
                byte_bg,
                bb,
                byte_bg,
                share_bytes,
                rows[i]->total_files,
                rows[i]->total_dirs,
                tb,
                user_bytes_pct,
                user_files_pct);
    }
    fprintf(out, "</tbody></table>\n");
    free(rows);
}

static int emit_bucket_detail_page(const char *filename,
                                   const char *username,
                                   const char *basis_str,
                                   int ab,
                                   int sb,
                                   const bucket_details_t *details,
                                   const matched_records_t *matched_records) {
    FILE *out = counted_fopen(filename, "w");
    char *base_prefix = NULL;
    path_row_map_t level1;
    path_row_map_t level2;
    size_t i;
    uint64_t bucket_files = 0;
    uint64_t bucket_bytes = 0;
    uint64_t total_user_files = 0;
    uint64_t total_user_bytes = 0;

    if (!out) return -1;

    if (path_row_map_init(&level1, 1024) != 0 || path_row_map_init(&level2, 2048) != 0) {
        counted_fclose(out);
        return -1;
    }

    fprintf(out, "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    fprintf(out, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    fprintf(out, "<title>Bucket Details</title>\n<style>\n");
    fprintf(out, "body{font-family:\"DejaVu Sans Mono\",\"Consolas\",monospace;margin:18px;color:#1f2328;background:#fcfcf8}\n");
    fprintf(out, "h1,h2{margin:0 0 10px 0;font-weight:600}\n");
    fprintf(out, ".meta{margin:0 0 14px 0;color:#555;line-height:1.5;font-size:12px}\n");
    fprintf(out, ".note{font-size:11px;color:#666;margin-bottom:14px;max-width:1200px}\n");
    fprintf(out, "table{border-collapse:collapse;width:100%%;font-size:11px;table-layout:fixed;margin-bottom:18px}\n");
    fprintf(out, "th,td{border:1px solid #d5d0c5;padding:3px 6px;vertical-align:top}\n");
    fprintf(out, "th{background:#ece6da;position:sticky;top:0;z-index:2}\n");
    fprintf(out, "th:first-child,td:first-child{position:sticky;left:0;background:#f8f5ee;z-index:1}\n");
    fprintf(out, "td.r,th.r{text-align:right}\n");
    fprintf(out, ".path-cell{width:320px;max-width:320px;min-width:320px}\n");
    fprintf(out, ".path-line{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:start;gap:8px}\n");
    fprintf(out, ".path-toggle{min-width:0;display:block;border:0;background:none;padding:0;margin:0;color:inherit;font:inherit;text-align:left;cursor:pointer}\n");
    fprintf(out, ".path-prefix{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#8b8172;font-size:10px;line-height:1.1;margin-bottom:1px}\n");
    fprintf(out, ".path-tail{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700;color:#1f2328;line-height:1.15}\n");
    fprintf(out, ".copy-path{opacity:0;pointer-events:none;align-self:start;border:1px solid #d8ccb8;background:#f4ede0;color:#6b4c16;border-radius:999px;padding:1px 5px;font:inherit;font-size:9px;line-height:1.2;cursor:pointer;transition:opacity 0.15s ease,background-color 0.15s ease}\n");
    fprintf(out, ".path-cell:hover .copy-path,.path-cell:focus-within .copy-path,.path-cell.expanded .copy-path{opacity:1;pointer-events:auto}\n");
    fprintf(out, ".copy-path:hover{background:#eadfc9}\n");
    fprintf(out, ".path-toggle:hover .path-tail,.path-toggle:focus .path-tail{text-decoration:underline}\n");
    fprintf(out, ".path-full{margin-top:4px;padding:4px 6px;background:#f4efe4;border:1px solid #ddd2bf;border-radius:4px;white-space:normal;word-break:break-all;font-size:10px;color:#4e4538;user-select:all}\n");
    fprintf(out, ".path-cell.expanded .path-prefix{white-space:normal;overflow:visible;text-overflow:clip}\n");
    fprintf(out, "a{color:#6b4c16;text-decoration:none}\n");
    fprintf(out, "</style>\n</head>\n<body>\n");

    fprintf(out, "<h1>Bucket Details</h1>\n<div class=\"meta\">User: <strong>");
    html_escape(out, username);
    fprintf(out, "</strong> | Basis: <strong>");
    html_escape(out, basis_str);
    fprintf(out, "</strong> | Age: <strong>");
    html_escape(out, age_bucket_names[ab]);
    fprintf(out, "</strong> | Size: <strong>");
    html_escape(out, size_bucket_names[sb]);
    fprintf(out, "</strong></div>\n");

    if (details->count == 0) {
        fprintf(out, "<div class=\"note\">This bucket has no matching files.</div>\n</body>\n</html>\n");
        path_row_map_destroy(&level1);
        path_row_map_destroy(&level2);
        counted_fclose(out);
        return 0;
    }

    base_prefix = dup_common_dir_prefix(details);
    if (!base_prefix) {
        path_row_map_destroy(&level1);
        path_row_map_destroy(&level2);
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

    if (aggregate_bucket_for_page(&level1, &level2, details, base_prefix) != 0 ||
        aggregate_totals_for_page(&level1, &level2, matched_records, base_prefix) != 0) {
        free(base_prefix);
        path_row_map_destroy(&level1);
        path_row_map_destroy(&level2);
        counted_fclose(out);
        return -1;
    }

    {
        char bb[32];
        human_bytes(bucket_bytes, bb, sizeof(bb));
        fprintf(out, "<div class=\"meta\">Base path: <strong>");
        html_escape(out, base_prefix[0] ? base_prefix : ".");
        fprintf(out, "</strong> | Bucket files: <strong>%" PRIu64 "</strong> | Bucket bytes: <strong>%s</strong></div>\n",
                bucket_files,
                bb);
    }
    fputs("<div class=\"note\">Rows are sorted by bucket bytes descending. Bucket Files and Bucket Bytes describe the files that match the clicked bucket. Share of Bucket Files and Share of Bucket Bytes show how much of the selected bucket lives under that path. Share of User Bytes and Share of User Files use the user's total files across all buckets as the denominator, so they show how much this path contributes to the user's overall footprint. Total Files, Total Dirs, and Total Bytes describe everything below that path. Redder bucket-share cells contribute more of the selected bucket.</div>\n", out);

    emit_path_summary_table(out, "Level 1 Directories", &level1, bucket_files, bucket_bytes, total_user_files, total_user_bytes);
    emit_path_summary_table(out, "Level 2 Directories", &level2, bucket_files, bucket_bytes, total_user_files, total_user_bytes);

    fputs("<script>\n"
          "(function(){\n"
          "function copyText(text){if(navigator.clipboard&&window.isSecureContext){return navigator.clipboard.writeText(text);}return new Promise(function(resolve,reject){var ta=document.createElement('textarea');ta.value=text;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.focus();ta.select();try{document.execCommand('copy');resolve();}catch(err){reject(err);}document.body.removeChild(ta);});}\n"
          "document.querySelectorAll('.copy-path').forEach(function(btn){btn.addEventListener('click',function(ev){var text=btn.getAttribute('data-copy');var old=btn.textContent;ev.preventDefault();ev.stopPropagation();copyText(text).then(function(){btn.textContent='Copied';setTimeout(function(){btn.textContent=old;},900);}).catch(function(){btn.textContent='Copy?';setTimeout(function(){btn.textContent=old;},1200);});});});\n"
          "document.querySelectorAll('.path-toggle').forEach(function(btn){btn.addEventListener('click',function(ev){var cell=btn.closest('.path-cell');var full=cell.querySelector('.path-full');var expanded=cell.classList.toggle('expanded');full.hidden=!expanded;btn.setAttribute('aria-expanded',expanded?'true':'false');ev.preventDefault();});});\n"
          "})();\n"
          "</script>\n", out);
    fprintf(out, "</body>\n</html>\n");

    free(base_prefix);
    path_row_map_destroy(&level1);
    path_row_map_destroy(&level2);
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

static int emit_all_bucket_detail_pages(const char *username,
                                        const char *basis_str,
                                        bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS],
                                        const matched_records_t *matched_records) {
    int ab, sb;

    if (ensure_bucket_output_dir_exists() != 0) return -1;

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            char fn[PATH_MAX];
            if (build_bucket_page_path(fn, sizeof(fn), ab, sb) != 0) return -1;
            if (emit_bucket_detail_page(fn, username, basis_str, ab, sb, &details[ab][sb], matched_records) != 0) {
                return -1;
            }
        }
    }
    return 0;
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
                          time_basis_t basis,
                          time_t now,
                          inode_set_t *seen_inodes,
                          progress_local_t *progress,
                          summary_t *sum,
                          bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS],
                          matched_records_t *matched_records) {
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
            progress_maybe_flush(progress);
        }

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

        if ((uid_t)r.uid == target_uid) {
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
                        free(pathbuf);
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

                if (bucket_details_append(&details[ab][sb], pathbuf, accounted_size) != 0) {
                    fprintf(stderr, "warn: detail append failed in %s\n", chunk->path);
                    sum->bad_input_files++;
                    if (progress) progress->bad_input_files++;
                    free(pathbuf);
                    break;
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

            if (matched_records_append(matched_records, pathbuf, r.type, accounted_size) != 0) {
                fprintf(stderr, "warn: matched record append failed in %s\n", chunk->path);
                sum->bad_input_files++;
                if (progress) progress->bad_input_files++;
                free(pathbuf);
                break;
            }
        }

        free(pathbuf);
    }

out:
    counted_fclose(fp);
    finalize_chunk_file_progress(file_states, chunk->file_index, progress);
    progress_flush_local(progress);
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
                       arg->basis,
                       arg->now,
                       arg->seen_inodes,
                       &progress,
                       &arg->summary,
                       arg->details,
                       &arg->matched_records);
    }

    progress_flush_local(&progress);

    return NULL;
}

static void *stats_thread_main(void *arg) {
    (void)arg;

    while (!atomic_load(&g_stop_stats)) {
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

        scanned_files = atomic_load(&g_progress_scanned_input_files);
        scanned_records = atomic_load(&g_progress_scanned_records);
        matched_records = atomic_load(&g_progress_matched_records);
        bad_input_files = atomic_load(&g_progress_bad_input_files);
        window_records = atomic_load(&g_window_records);
        elapsed_sec = g_run_start_sec > 0.0 ? now_sec() - g_run_start_sec : 0.0;

        {
            unsigned int divisor = atomic_load(&g_seconds_seen);
            if (divisor == 0) divisor = 1;
            records_rate = (double)window_records / (double)divisor;
        }

        g_records_rate_sum += records_rate;
        if (g_records_rate_samples == 0 || records_rate < g_records_rate_min) g_records_rate_min = records_rate;
        if (g_records_rate_samples == 0 || records_rate > g_records_rate_max) g_records_rate_max = records_rate;
        g_records_rate_samples++;

        human_decimal((double)scanned_files, sf, sizeof(sf));
        human_decimal((double)g_progress_input_files_total, tf, sizeof(tf));
        human_decimal((double)scanned_records, sr, sizeof(sr));
        human_decimal((double)matched_records, mr, sizeof(mr));
        human_decimal(records_rate, rr, sizeof(rr));
        format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

        printf("\r%s rec/s(10s) | files:%s/%s rec:%s match:%s bad:%llu | el:%s            ",
               rr, sf, tf, sr, mr, bad_input_files, elapsed_buf);
        fflush(stdout);
    }

    return NULL;
}

static int scan_dir_collect_files(const char *dirpath,
                                  uid_t target_uid,
                                  char ***out_paths,
                                  size_t *out_count) {
    DIR *dir = NULL;
    struct dirent *de;
    char **paths = NULL;
    size_t count = 0;
    size_t cap = 0;
    uint32_t uid_shards = 0;
    int layout_rc;
    int use_uid_shards = 0;
    uint32_t wanted_shard = 0;

    layout_rc = read_uid_shard_layout(dirpath, &uid_shards);
    if (layout_rc < 0) {
        fprintf(stderr, "cannot read crawl manifest in %s\n", dirpath);
        return -1;
    }
    if (layout_rc > 0) {
        use_uid_shards = 1;
        wanted_shard = ((uint32_t)target_uid) & (uid_shards - 1U);
        g_input_layout = "uid_shards";
        g_input_uid_shards = uid_shards;
    } else {
        g_input_layout = "legacy";
        g_input_uid_shards = 0;
    }

    dir = counted_opendir(dirpath);
    if (!dir) {
        fprintf(stderr, "cannot open directory %s: %s\n", dirpath, strerror(errno));
        return -1;
    }

    while ((de = counted_readdir(dir)) != NULL) {
        char full[PATH_MAX];
        char *copy;

        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        if (!has_bin_suffix(de->d_name)) continue;

        if (use_uid_shards) {
            uint32_t shard = 0;
            if (parse_uid_shard_number(de->d_name, &shard) != 0) continue;
            if (shard != wanted_shard) continue;
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
    *out_paths = paths;
    *out_count = count;
    return 0;
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

static int build_chunks_for_file(const char *path,
                                 size_t file_index,
                                 uint64_t chunk_target_bytes,
                                 file_chunk_t **chunks,
                                 size_t *chunk_count,
                                 size_t *chunk_cap,
                                 unsigned int *file_chunk_counter) {
    FILE *fp = NULL;
    bin_file_header_t fh;
    uint64_t chunk_start;
    uint64_t next_target;
    int rc = -1;

    fp = counted_fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }

    if (counted_fread(&fh, sizeof(fh), 1, fp) != 1) {
        fprintf(stderr, "warn: short read on header: %s\n", path);
        goto out;
    }
    if (memcmp(fh.magic, "NFSCBIN", 7) != 0 || fh.version != FORMAT_VERSION) {
        fprintf(stderr, "warn: bad format/version in %s\n", path);
        goto out;
    }

    if (chunk_target_bytes == 0) chunk_target_bytes = PARSE_CHUNK_BYTES;
    chunk_start = (uint64_t)sizeof(fh);
    next_target = chunk_start + chunk_target_bytes;

    for (;;) {
        bin_record_hdr_t r;
        off_t record_start = ftello(fp);
        off_t record_end;

        if (record_start < 0) goto out;

        if (counted_fread(&r, sizeof(r), 1, fp) != 1) {
            if (feof(fp)) {
                uint64_t file_end = (uint64_t)record_start;
                if (file_end > chunk_start) {
                    if (append_chunk(chunks, chunk_count, chunk_cap, path, chunk_start, file_end, file_index) != 0) goto out;
                    (*file_chunk_counter)++;
                }
                rc = 0;
            }
            goto out;
        }

        if (fseeko(fp, (off_t)r.path_len, SEEK_CUR) != 0) goto out;
        record_end = ftello(fp);
        if (record_end < 0) goto out;

        while ((uint64_t)record_end >= next_target) {
            if ((uint64_t)record_end > chunk_start) {
                if (append_chunk(chunks, chunk_count, chunk_cap, path, chunk_start, (uint64_t)record_end, file_index) != 0) goto out;
                (*file_chunk_counter)++;
            }
            chunk_start = (uint64_t)record_end;
            next_target = chunk_start + chunk_target_bytes;
        }
    }

out:
    if (fp) counted_fclose(fp);
    return rc;
}

static int emit_html(const char *report_path,
                     const char *username,
                     uid_t uid,
                     const char *basis_str,
                     const summary_t *sum,
                     size_t input_files,
                     int threads_used) {
    FILE *out = counted_fopen(report_path, "w");
    int ab, sb;

    if (!out) return -1;
    uint64_t max_cell_bytes = 0;

    fprintf(out, "<!DOCTYPE html>\n");
    fprintf(out, "<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    fprintf(out, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    fprintf(out, "<title>Storage Report</title>\n");
    fprintf(out, "<style>\n");
    fprintf(out, "body{font-family:Arial,sans-serif;margin:24px;color:#222}\n");
    fprintf(out, "body.drawer-open{overflow:hidden}\n");
    fprintf(out, "h1{margin-bottom:8px}\n");
    fprintf(out, ".report-title{font-size:1.35rem;margin:0 0 18px}\n");
    fprintf(out, ".report-stats{margin-top:28px;padding-top:18px;border-top:1px solid #ddd}\n");
    fprintf(out, ".report-stats h2{font-size:1.1rem;margin:0 0 10px}\n");
    fprintf(out, "table{border-collapse:collapse;margin-top:16px;min-width:900px}\n");
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
    fprintf(out, ".path-search-results{margin-top:18px;padding-top:12px;border-top:1px solid #eee}\n");
    fprintf(out, ".path-search-results-list{margin:8px 0;padding-left:22px;font-size:13px}\n");
    fprintf(out, ".path-search-results-list li{margin:4px 0;word-break:break-all}\n");
    fprintf(out, ".path-search-pager{display:flex;gap:12px;margin-top:12px;align-items:center;flex-wrap:wrap}\n");
    fprintf(out, ".path-search-pager button{padding:6px 14px;font-size:14px;cursor:pointer;border:1px solid #bbb;border-radius:4px;background:#f8f8f8}\n");
    fprintf(out, ".path-search-pager button:disabled{opacity:0.45;cursor:not-allowed}\n");
    fprintf(out, "</style>\n");
    fprintf(out, "</head>\n<body>\n");

    fprintf(out, "<h1 class=\"report-title\">Report for <strong>");
    html_escape(out, username);
    fprintf(out, "</strong></h1>\n");

    fprintf(out, "<section class=\"path-search\" aria-label=\"Path search\">\n");
    fprintf(out, "<label for=\"path-search-input\">Search paths</label>\n");
    fprintf(out,
            "<input type=\"text\" id=\"path-search-input\" autocomplete=\"off\" "
            "placeholder=\"At least 3 characters — uses ereport_index via server (./index/ + eserve)\" />\n");
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
    fprintf(out, "<tr><th>Age \\ Size</th>");
    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
        fprintf(out, "<th>");
        html_escape(out, size_bucket_names[sb]);
        fprintf(out, "</th>");
    }
    fprintf(out, "<th>Total</th></tr>\n");

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        uint64_t row_total = 0;
        fprintf(out, "<tr><td>");
        html_escape(out, age_bucket_names[ab]);
        fprintf(out, "</td>");

        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            char hb[32];
            char bg[32];
            double pct = 0.0;
            uint64_t b = sum->bytes[ab][sb];
            uint64_t f = sum->files[ab][sb];
            row_total += b;

            if (sum->total_bytes) pct = 100.0 * (double)b / (double)sum->total_bytes;
            human_bytes(b, hb, sizeof(hb));
            heatmap_color(b, ab, max_cell_bytes, bg, sizeof(bg));
            fprintf(out, "<td class=\"cell\" style=\"background:%s\"><a class=\"bucket-link\" data-age=\"%d\" data-size=\"%d\" href=\"bucket_a%d_s%d.html\"><div class=\"cell-main\"><span class=\"cell-bytes\">%s</span><span class=\"cell-pct\">%.0f%%</span></div><div class=\"cell-sub\">%" PRIu64 " files</div></a></td>", bg, ab, sb, ab, sb, hb, pct, f);
        }

        {
            char hr[32];
            char bg[32];
            double pct = 0.0;
            if (sum->total_bytes) pct = 100.0 * (double)row_total / (double)sum->total_bytes;
            human_bytes(row_total, hr, sizeof(hr));
            contribution_cell_color(pct, bg, sizeof(bg));
            fprintf(out, "<td class=\"tot tot-cell\" style=\"background:%s\"><div class=\"tot-block\"><div class=\"cell-main\"><span class=\"cell-bytes\">%s</span><span class=\"cell-pct\">%.0f%%</span></div></div></td>", bg, hr, pct);
        }

        fprintf(out, "</tr>\n");
    }

    fprintf(out, "<tr class=\"tot\"><td>Total</td>");
    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
        uint64_t col_total = 0;
        char hc[32];
        char bg[32];
        double pct = 0.0;
        for (ab = 0; ab < AGE_BUCKETS; ab++) col_total += sum->bytes[ab][sb];
        if (sum->total_bytes) pct = 100.0 * (double)col_total / (double)sum->total_bytes;
        human_bytes(col_total, hc, sizeof(hc));
        contribution_cell_color(pct, bg, sizeof(bg));
        fprintf(out, "<td class=\"tot tot-cell\" style=\"background:%s\"><div class=\"tot-block\"><div class=\"cell-main\"><span class=\"cell-bytes\">%s</span><span class=\"cell-pct\">%.0f%%</span></div></div></td>", bg, hc, pct);
    }
    {
        char ht[32];
        human_bytes(sum->total_bytes, ht, sizeof(ht));
        fprintf(out, "<td class=\"tot tot-cell\"><div class=\"tot-block\"><div class=\"cell-main\"><span class=\"cell-bytes\">%s</span><span class=\"cell-pct\">100%%</span></div></div></td>", ht);
    }
    fprintf(out, "</tr>\n");

    fprintf(out, "</table>\n");

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
        fprintf(out, "<h2>Statistics</h2>\n");
        fprintf(out, "<p>");
        fprintf(out, "<strong>Uid:</strong> %lu &nbsp;|&nbsp; <strong>Time basis:</strong> ", (unsigned long)uid);
        html_escape(out, basis_str);
        fprintf(out, " &nbsp;|&nbsp; <strong>Input files:</strong> %zu &nbsp;|&nbsp; <strong>Threads:</strong> %d<br>\n",
               input_files, threads_used);
        fprintf(out, "Scanned records: %" PRIu64 "<br>\n", sum->scanned_records);
        fprintf(out, "Matched records: %" PRIu64 "<br>\n", sum->matched_records);
        fprintf(out, "Files: %" PRIu64 "<br>\n", sum->total_files);
        fprintf(out, "Directories: %" PRIu64 "<br>\n", sum->total_dirs);
        fprintf(out, "Links: %" PRIu64 "<br>\n", sum->total_links);
        fprintf(out, "Other file types: %" PRIu64 "<br>\n", other_count);
        fprintf(out, "Non-files: %" PRIu64 "<br>\n", non_file_count);
        fprintf(out, "Total capacity in files: %s (%" PRIu64 " bytes)<br>\n", totalb, sum->total_bytes);
        fprintf(out, "Total capacity in non-files: %s (%" PRIu64 " bytes)<br>\n", total_non_file_b, non_file_bytes);
        fprintf(out, "Total capacity in other file types: %s (%" PRIu64 " bytes)<br>\n", total_other_b, sum->total_other_bytes);
        fprintf(out, "Bad input files: %" PRIu64, sum->bad_input_files);
        fprintf(out, "</p>\n");
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
    fputs("var PREVIEW_MAX=20;var PAGE_SIZE=50;var fullTerm='';var pageNum=1;var lastTotal=0;\n", out);
    fputs("function escHtml(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');}\n", out);
    fputs("function fetchSearch(term,skip,lim){\n", out);
    fputs("var u='search?q='+encodeURIComponent(term)+'&skip='+String(skip)+'&limit='+String(lim);\n", out);
    fputs("return fetch(u).then(function(r){\n", out);
    fputs("if(!r.ok)return r.text().then(function(t){throw new Error(t||String(r.status));});return r.json();\n", out);
    fputs("});}\n", out);
    fputs("function renderPreview(raw){\n", out);
    fputs("var box=document.getElementById('path-search-preview');\n", out);
    fputs("var cap=document.getElementById('path-search-caption');\n", out);
    fputs("var t=raw.trim();\n", out);
    fputs("if(t.length<3){box.innerHTML='';document.getElementById('path-search-results').hidden=true;hideSearchPanel();return;}\n", out);
    fputs("showSearchPanel();\n", out);
    fputs("document.getElementById('path-search-panel-title').textContent='Preview';\n", out);
    fputs("if(cap)cap.textContent='Keep typing in the box above—same field—to refine. Press Enter for paged results.';\n", out);
    fputs("document.getElementById('path-search-results').hidden=true;\n", out);
    fputs("fetchSearch(t,0,PREVIEW_MAX).then(function(j){\n", out);
    fputs("var paths=j.paths||[];\n", out);
    fputs("if(paths.length===0){box.innerHTML='<span class=\"path-search-muted\">No matches.</span>';return;}\n", out);
    fputs("var h='<ul>';for(var i=0;i<paths.length;i++){h+='<li>'+escHtml(paths[i])+'</li>';}h+='</ul>';\n", out);
    fputs("if((j.total||0)>paths.length){h+='<div class=\"path-search-muted\">Showing '+paths.length+' of '+j.total+' — press Enter for full paging.</div>';}\n", out);
    fputs("box.innerHTML=h;\n}).catch(function(e){box.innerHTML='<span class=\"path-search-muted\">'+escHtml(e.message)+'</span>';});\n}\n", out);
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
    fputs("meta.textContent=total+' match'+(total===1?'':'es')+' — page '+pageNum+' of '+pages;\n", out);
    fputs("var paths=j.paths||[];var h='';for(var i=0;i<paths.length;i++){h+='<li>'+escHtml(paths[i])+'</li>';}list.innerHTML=h;\n", out);
    fputs("prev.disabled=pageNum<=1;next.disabled=pageNum>=pages;\n", out);
    fputs("}).catch(function(e){meta.textContent='';list.innerHTML='<li class=\"path-search-muted\">'+escHtml(e.message)+'</li>';prev.disabled=true;next.disabled=true;});\n}\n", out);
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

static void emit_run_stats(const char *username,
                           uid_t uid,
                           const char *basis_str,
                           const char *dirpath,
                           const char *report_path,
                           size_t input_files,
                           int threads_requested,
                           int threads_used,
                           const summary_t *sum,
                           int bucket_pages_written,
                           double elapsed_sec) {
    char avg_records_buf[32], mean_records_buf[32], max_records_buf[32], min_records_buf[32];
    double avg_records = elapsed_sec > 0.0 ? (double)sum->scanned_records / elapsed_sec : 0.0;
    double mean_records = g_records_rate_samples ? g_records_rate_sum / (double)g_records_rate_samples : avg_records;
    double max_records = g_records_rate_samples ? g_records_rate_max : avg_records;
    double min_records = g_records_rate_samples ? g_records_rate_min : avg_records;

    human_decimal(avg_records, avg_records_buf, sizeof(avg_records_buf));
    human_decimal(mean_records, mean_records_buf, sizeof(mean_records_buf));
    human_decimal(max_records, max_records_buf, sizeof(max_records_buf));
    human_decimal(min_records, min_records_buf, sizeof(min_records_buf));

    printf("report_type=ereport\n");
    printf("user=%s\n", username);
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
    const char *dirpath = ".";
    time_basis_t basis;
    uid_t target_uid;
    char display_name[256];
    char **paths = NULL;
    size_t path_count = 0;
    file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    size_t chunk_cap = 0;
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
    double t0, t1;

    atomic_store(&g_io_opendir_calls, 0);
    atomic_store(&g_io_readdir_calls, 0);
    atomic_store(&g_io_closedir_calls, 0);
    atomic_store(&g_io_fopen_calls, 0);
    atomic_store(&g_io_fclose_calls, 0);
    atomic_store(&g_io_fread_calls, 0);
    atomic_store(&g_progress_scanned_input_files, 0);
    atomic_store(&g_progress_scanned_records, 0);
    atomic_store(&g_progress_matched_records, 0);
    atomic_store(&g_progress_bad_input_files, 0);
    atomic_store(&g_window_records, 0);
    atomic_store(&g_bucket_index, 0);
    atomic_store(&g_seconds_seen, 0);
    atomic_store(&g_stop_stats, 0);
    for (i = 0; i < WINDOW_SECONDS; i++) atomic_store(&g_bucket_records[i], 0);
    g_progress_input_files_total = 0;
    g_records_rate_sum = 0.0;
    g_records_rate_min = 0.0;
    g_records_rate_max = 0.0;
    g_records_rate_samples = 0;
    t0 = now_sec();
    g_run_start_sec = t0;

    if (argc < 3 || argc > 5) {
        fprintf(stderr, "Usage: %s <username|uid> <atime|mtime|ctime> [bin_dir] [threads]\n", argv[0]);
        return 2;
    }

    user_spec = argv[1];
    basis_str = argv[2];
    if (argc >= 4) dirpath = argv[3];
    if (argc == 5) {
        threads = atoi(argv[4]);
        if (threads <= 0) die("threads must be > 0");
    } else {
        threads = parse_ereport_thread_count();
    }

    if (parse_time_basis(basis_str, &basis) != 0) die("time basis must be one of: atime, mtime, ctime");

    if (resolve_target_user(user_spec, &target_uid, display_name, sizeof(display_name)) != 0) {
        fprintf(stderr, "unknown user or uid: %s\n", user_spec);
        return 1;
    }

    set_bucket_output_dir(display_name);
    if (snprintf(report_path, sizeof(report_path), "%s/index.html", g_bucket_output_dir) >= (int)sizeof(report_path)) {
        fprintf(stderr, "report path too long for %s\n", g_bucket_output_dir);
        return 1;
    }

    if (scan_dir_collect_files(dirpath, target_uid, &paths, &path_count) != 0) return 1;
    if (path_count == 0) {
        fprintf(stderr, "no matching input .bin files found in %s\n", dirpath);
        free(paths);
        return 1;
    }

    file_states = (file_state_t *)calloc(path_count, sizeof(*file_states));
    if (!file_states) {
        size_t k;
        fprintf(stderr, "allocation failed\n");
        for (k = 0; k < path_count; k++) free(paths[k]);
        free(paths);
        return 1;
    }

    for (i = 0; (size_t)i < path_count; i++) {
        struct stat st;
        unsigned int file_chunk_counter = 0;
        uint64_t chunk_target = PARSE_CHUNK_BYTES;
        if (stat(paths[i], &st) == 0 && S_ISREG(st.st_mode))
            chunk_target = compute_parse_chunk_target((uint64_t)st.st_size, threads);
        if (build_chunks_for_file(paths[i], (size_t)i, chunk_target, &chunks, &chunk_count, &chunk_cap,
                                   &file_chunk_counter) != 0) {
            atomic_store(&file_states[i].remaining_chunks, 0);
        } else {
            atomic_store(&file_states[i].remaining_chunks, file_chunk_counter);
        }
    }

    if (chunk_count == 0) {
        fprintf(stderr, "no readable chunk work found in %s\n", dirpath);
        for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
        free(paths);
        free(file_states);
        return 1;
    }

    threads_used = threads;
    now = time(NULL);
    g_progress_input_files_total = (uint64_t)path_count;

    memset(&queue, 0, sizeof(queue));
    queue.chunks = chunks;
    queue.count = chunk_count;
    queue.next_index = 0;
    pthread_mutex_init(&queue.mutex, NULL);

    if (inode_set_init(&seen_inodes, 65536) != 0) {
        size_t k;
        fprintf(stderr, "allocation failed\n");
        for (k = 0; k < path_count; k++) free(paths[k]);
        free(paths);
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
        return 1;
    }

    if (pthread_create(&stats_thread, NULL, stats_thread_main, NULL) != 0) {
        fprintf(stderr, "failed to create stats thread\n");
        free(tids);
        free(args);
        for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
        free(paths);
        pthread_mutex_destroy(&queue.mutex);
        inode_set_destroy(&seen_inodes);
        return 1;
    }
    stats_thread_started = 1;

    for (i = 0; i < threads; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].queue = &queue;
        args[i].file_states = file_states;
        args[i].target_uid = target_uid;
        args[i].basis = basis;
        args[i].now = now;
        args[i].seen_inodes = &seen_inodes;

        if (pthread_create(&tids[i], NULL, worker_main, &args[i]) != 0) {
            fprintf(stderr, "failed to create thread %d\n", i);
            threads_used = i;
            break;
        }
    }

    memset(&final_sum, 0, sizeof(final_sum));
    memset(final_details, 0, sizeof(final_details));
    memset(&final_matched_records, 0, sizeof(final_matched_records));

    for (i = 0; i < threads_used; i++) {
        pthread_join(tids[i], NULL);
        summary_merge(&final_sum, &args[i].summary);
        if (matched_records_merge(&final_matched_records, &args[i].matched_records) != 0) {
            fprintf(stderr, "allocation failed merging matched records\n");
            if (stats_thread_started) {
                atomic_store(&g_stop_stats, 1);
                pthread_join(stats_thread, NULL);
                clear_status_line();
            }
            matched_records_free(&final_matched_records);
            for (ab = 0; ab < AGE_BUCKETS; ab++) {
                for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
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
            return 1;
        }
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            for (sb = 0; sb < SIZE_BUCKETS; sb++) {
                if (bucket_details_merge(&final_details[ab][sb], &args[i].details[ab][sb]) != 0) {
                    fprintf(stderr, "allocation failed merging bucket details\n");
                    if (stats_thread_started) {
                        atomic_store(&g_stop_stats, 1);
                        pthread_join(stats_thread, NULL);
                        clear_status_line();
                    }
                    matched_records_free(&final_matched_records);
                    for (ab = 0; ab < AGE_BUCKETS; ab++) {
                        for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
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
                    return 1;
                }
            }
        }
    }

    if (ensure_bucket_output_dir_exists() != 0) {
        fprintf(stderr, "failed to create report output directory %s: %s\n", g_bucket_output_dir, strerror(errno));
        if (stats_thread_started) {
            atomic_store(&g_stop_stats, 1);
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

    if (emit_all_bucket_detail_pages(display_name, basis_str, final_details, &final_matched_records) != 0) {
        fprintf(stderr, "failed to write bucket detail pages\n");
    } else {
        bucket_pages_written = AGE_BUCKETS * SIZE_BUCKETS;
    }

    if (emit_html(report_path, display_name, target_uid, basis_str, &final_sum, path_count, threads_used) != 0) {
        fprintf(stderr, "failed to write main report %s\n", report_path);
    }
    final_sum.scanned_input_files = (uint64_t)path_count;
    t1 = now_sec();
    if (stats_thread_started) {
        atomic_store(&g_stop_stats, 1);
        pthread_join(stats_thread, NULL);
        clear_status_line();
    }
    emit_run_stats(display_name, target_uid, basis_str, dirpath, report_path, path_count, threads, threads_used, &final_sum, bucket_pages_written, t1 - t0);

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
