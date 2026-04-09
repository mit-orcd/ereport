/*
 * ereport.c
 *
 * Parallel reader for thread_*.bin files produced by ecrawl/ecrawl_nfs.
 * Emits the original HTML summary plus per-bucket drilldown pages with
 * dense level-1/level-2 directory summaries.
 *
 * Build:
 *   gcc -O2 -Wall -Wextra -pthread -o ereport ereport.c
 *
 * Usage:
 *   ./ereport <username> <atime|mtime|ctime> [bin_dir] [threads] > report.html
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

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define FILE_MAGIC_LEN 8
#define FORMAT_VERSION 2
#define DEFAULT_THREADS 32
#define BUCKET_OUTPUT_DIR "tmp"

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
    char **paths;
    size_t count;
    size_t next_index;
    pthread_mutex_t mutex;
} work_queue_t;

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
    uid_t target_uid;
    time_basis_t basis;
    time_t now;
    inode_set_t *seen_inodes;
    summary_t summary;
    bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS];
    matched_records_t matched_records;
} worker_arg_t;

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

static void emit_path_summary_table(FILE *out, const char *title, path_row_map_t *map) {
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

    fprintf(out, "<table>\n<thead><tr><th>Path</th><th class=\"r\">Bucket Files</th><th class=\"r\">Bucket Bytes</th><th class=\"r\">Total Files</th><th class=\"r\">Total Dirs</th><th class=\"r\">Total Bytes</th><th class=\"r\">Bucket %% of Bytes</th><th class=\"r\">Bucket %% of Files</th></tr></thead>\n<tbody>\n");
    for (i = 0; i < count; i++) {
        char bb[32];
        char tb[32];
        double bp = rows[i]->total_bytes ? (100.0 * (double)rows[i]->bucket_bytes / (double)rows[i]->total_bytes) : 0.0;
        double fp = rows[i]->total_files ? (100.0 * (double)rows[i]->bucket_files / (double)rows[i]->total_files) : 0.0;
        human_bytes(rows[i]->bucket_bytes, bb, sizeof(bb));
        human_bytes(rows[i]->total_bytes, tb, sizeof(tb));
        fprintf(out, "<tr><td class=\"path\" title=\"");
        html_escape(out, rows[i]->path);
        fprintf(out, "\">");
        html_escape(out, rows[i]->path);
        fprintf(out, "</td><td class=\"r\">%" PRIu64 "</td><td class=\"r\">%s</td><td class=\"r\">%" PRIu64 "</td><td class=\"r\">%" PRIu64 "</td><td class=\"r\">%s</td><td class=\"r\">%.1f</td><td class=\"r\">%.1f</td></tr>\n",
                rows[i]->bucket_files,
                bb,
                rows[i]->total_files,
                rows[i]->total_dirs,
                tb,
                bp,
                fp);
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
    FILE *out = fopen(filename, "w");
    char *base_prefix = NULL;
    path_row_map_t level1;
    path_row_map_t level2;
    size_t i;
    uint64_t bucket_files = 0;
    uint64_t bucket_bytes = 0;

    if (!out) return -1;

    if (path_row_map_init(&level1, 1024) != 0 || path_row_map_init(&level2, 2048) != 0) {
        fclose(out);
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
    fprintf(out, ".path{max-width:720px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n");
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
        fclose(out);
        return 0;
    }

    base_prefix = dup_common_dir_prefix(details);
    if (!base_prefix) {
        path_row_map_destroy(&level1);
        path_row_map_destroy(&level2);
        fclose(out);
        return -1;
    }

    for (i = 0; i < details->count; i++) {
        bucket_files++;
        bucket_bytes += details->items[i].size;
    }

    if (aggregate_bucket_for_page(&level1, &level2, details, base_prefix) != 0 ||
        aggregate_totals_for_page(&level1, &level2, matched_records, base_prefix) != 0) {
        free(base_prefix);
        path_row_map_destroy(&level1);
        path_row_map_destroy(&level2);
        fclose(out);
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
    fputs("<div class=\"note\">Rows are sorted by bucket bytes descending. Bucket Files and Bucket Bytes describe the files that match the clicked bucket. Total Files, Total Dirs, and Total Bytes describe everything below that path. Bucket % of Bytes shows how much of the path's total bytes come from this bucket, and Bucket % of Files shows the same share for file counts.</div>\n", out);

    emit_path_summary_table(out, "Level 1 Directories", &level1);
    emit_path_summary_table(out, "Level 2 Directories", &level2);

    fprintf(out, "</body>\n</html>\n");

    free(base_prefix);
    path_row_map_destroy(&level1);
    path_row_map_destroy(&level2);
    fclose(out);
    return 0;
}

static int emit_all_bucket_detail_pages(const char *username,
                                        const char *basis_str,
                                        bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS],
                                        const matched_records_t *matched_records) {
    int ab, sb;
    struct stat st;

    if (stat(BUCKET_OUTPUT_DIR, &st) != 0) {
        if (mkdir(BUCKET_OUTPUT_DIR, 0777) != 0) {
            return -1;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        return -1;
    }

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            char fn[128];
            snprintf(fn, sizeof(fn), BUCKET_OUTPUT_DIR "/bucket_a%d_s%d.html", ab, sb);
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

static char *queue_pop(work_queue_t *q) {
    char *path = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->next_index < q->count) path = q->paths[q->next_index++];
    pthread_mutex_unlock(&q->mutex);

    return path;
}

static int read_one_file(const char *filepath,
                         uid_t target_uid,
                         time_basis_t basis,
                         time_t now,
                         inode_set_t *seen_inodes,
                         summary_t *sum,
                         bucket_details_t details[AGE_BUCKETS][SIZE_BUCKETS],
                         matched_records_t *matched_records) {
    FILE *fp = NULL;
    bin_file_header_t fh;
    int rc = -1;

    fp = fopen(filepath, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", filepath, strerror(errno));
        sum->bad_input_files++;
        return -1;
    }

    sum->scanned_input_files++;

    if (fread(&fh, sizeof(fh), 1, fp) != 1) {
        fprintf(stderr, "warn: short read on header: %s\n", filepath);
        sum->bad_input_files++;
        goto out;
    }

    if (memcmp(fh.magic, "NFSCBIN", 7) != 0 || fh.version != FORMAT_VERSION) {
        fprintf(stderr, "warn: bad format/version in %s\n", filepath);
        sum->bad_input_files++;
        goto out;
    }

    for (;;) {
        bin_record_hdr_t r;
        size_t n;
        char *pathbuf = NULL;
        uint64_t accounted_size = 0;

        memset(&r, 0, sizeof(r));

        n = fread(&r, sizeof(r), 1, fp);
        if (n != 1) {
            if (feof(fp)) rc = 0;
            else {
                fprintf(stderr, "warn: read error in %s\n", filepath);
                sum->bad_input_files++;
            }
            break;
        }

        sum->scanned_records++;

        if (r.path_len > 0) {
            pathbuf = (char *)malloc((size_t)r.path_len + 1);
            if (!pathbuf) {
                fprintf(stderr, "warn: path alloc failed in %s\n", filepath);
                sum->bad_input_files++;
                break;
            }
            if (fread(pathbuf, 1, r.path_len, fp) != r.path_len) {
                fprintf(stderr, "warn: path read failed in %s\n", filepath);
                sum->bad_input_files++;
                free(pathbuf);
                break;
            }
            pathbuf[r.path_len] = '\0';
        } else {
            pathbuf = strdup("");
            if (!pathbuf) {
                fprintf(stderr, "warn: path alloc failed in %s\n", filepath);
                sum->bad_input_files++;
                break;
            }
        }

        if ((uid_t)r.uid == target_uid) {
            sum->matched_records++;

            if (r.type == 'f') {
                int sb = size_bucket_for(r.size);
                int ab = age_bucket_for(pick_time(&r, basis), now);
                int count_bytes = 1;

                if (r.nlink > 1) {
                    int ins = inode_set_insert_if_new(seen_inodes, r.dev_major, r.dev_minor, r.inode);
                    if (ins < 0) {
                        fprintf(stderr, "warn: inode dedup set error in %s\n", filepath);
                        sum->bad_input_files++;
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
                    fprintf(stderr, "warn: detail append failed in %s\n", filepath);
                    sum->bad_input_files++;
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
                fprintf(stderr, "warn: matched record append failed in %s\n", filepath);
                sum->bad_input_files++;
                free(pathbuf);
                break;
            }
        }

        free(pathbuf);
    }

out:
    fclose(fp);
    return rc;
}

static void *worker_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;

    for (;;) {
        char *path = queue_pop(arg->queue);
        if (!path) break;

        read_one_file(path,
                      arg->target_uid,
                      arg->basis,
                      arg->now,
                      arg->seen_inodes,
                      &arg->summary,
                      arg->details,
                      &arg->matched_records);
    }

    return NULL;
}

static int scan_dir_collect_files(const char *dirpath, char ***out_paths, size_t *out_count) {
    DIR *dir = NULL;
    struct dirent *de;
    char **paths = NULL;
    size_t count = 0;
    size_t cap = 0;

    dir = opendir(dirpath);
    if (!dir) {
        fprintf(stderr, "cannot open directory %s: %s\n", dirpath, strerror(errno));
        return -1;
    }

    while ((de = readdir(dir)) != NULL) {
        char full[PATH_MAX];
        char *copy;

        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        if (!starts_with_thread(de->d_name) || !has_bin_suffix(de->d_name)) continue;

        if (snprintf(full, sizeof(full), "%s/%s", dirpath, de->d_name) >= (int)sizeof(full)) {
            fprintf(stderr, "warn: path too long: %s/%s\n", dirpath, de->d_name);
            continue;
        }

        if (count == cap) {
            size_t new_cap = (cap == 0) ? 64 : cap * 2;
            char **tmp = (char **)realloc(paths, new_cap * sizeof(*paths));
            if (!tmp) {
                size_t i;
                closedir(dir);
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
            closedir(dir);
            for (i = 0; i < count; i++) free(paths[i]);
            free(paths);
            return -1;
        }

        paths[count++] = copy;
    }

    closedir(dir);
    *out_paths = paths;
    *out_count = count;
    return 0;
}

static void emit_html(const char *username,
                      uid_t uid,
                      const char *basis_str,
                      const summary_t *sum,
                      size_t input_files,
                      int threads_used) {
    int ab, sb;
    uint64_t max_cell_bytes = 0;

    printf("<!DOCTYPE html>\n");
    printf("<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    printf("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    printf("<title>Storage Report</title>\n");
    printf("<style>\n");
    printf("body{font-family:Arial,sans-serif;margin:24px;color:#222}\n");
    printf("h1{margin-bottom:8px}\n");
    printf(".meta{margin-bottom:20px;color:#555}\n");
    printf("table{border-collapse:collapse;margin-top:16px;min-width:900px}\n");
    printf("th,td{border:1px solid #ccc;padding:8px 10px;text-align:right}\n");
    printf("th:first-child,td:first-child{text-align:left}\n");
    printf("th{background:#f4f4f4}\n");
    printf(".tot{font-weight:bold;background:#fafafa}\n");
    printf(".cell{transition:background-color 0.2s ease}\n");
    printf(".cell a{display:block;color:inherit;text-decoration:none}\n");
    printf("</style>\n");
    printf("</head>\n<body>\n");

    printf("<h1>Storage Report</h1>\n");
    printf("<div class=\"meta\">User: <strong>");
    html_escape(stdout, username);
    printf("</strong> (uid=%lu) &nbsp; | &nbsp; Time basis: <strong>", (unsigned long)uid);
    html_escape(stdout, basis_str);
    printf("</strong> &nbsp; | &nbsp; Input files: <strong>%zu</strong> &nbsp; | &nbsp; Threads: <strong>%d</strong></div>\n",
           input_files, threads_used);

    {
        char totalb[32];
        char total_other_b[32];
        uint64_t non_file_count = sum->matched_records - sum->matched_files;
        uint64_t non_file_bytes = sum->total_capacity_bytes - sum->total_bytes;
        human_bytes(sum->total_bytes, totalb, sizeof(totalb));
        human_bytes(non_file_bytes, total_other_b, sizeof(total_other_b));

        printf("<p>");
        printf("Scanned records: %" PRIu64 "<br>\n", sum->scanned_records);
        printf("Matched records: %" PRIu64 "<br>\n", sum->matched_records);
        printf("Files: %" PRIu64 "<br>\n", sum->total_files);
        printf("Directories: %" PRIu64 "<br>\n", sum->total_dirs);
        printf("Links: %" PRIu64 "<br>\n", sum->total_links);
        printf("Others: %" PRIu64 "<br>\n", non_file_count);
        printf("Total capacity in files: %s (%" PRIu64 " bytes)<br>\n", totalb, sum->total_bytes);
        printf("Total capacity in others: %s (%" PRIu64 " bytes)<br>\n", total_other_b, non_file_bytes);
        printf("Bad input files: %" PRIu64, sum->bad_input_files);
        printf("</p>\n");
    }

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            if (sum->bytes[ab][sb] > max_cell_bytes) max_cell_bytes = sum->bytes[ab][sb];
        }
    }

    printf("<table>\n");
    printf("<tr><th>Age \\ Size</th>");
    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
        printf("<th>");
        html_escape(stdout, size_bucket_names[sb]);
        printf("</th>");
    }
    printf("<th>Total</th></tr>\n");

    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        uint64_t row_total = 0;
        printf("<tr><td>");
        html_escape(stdout, age_bucket_names[ab]);
        printf("</td>");

        for (sb = 0; sb < SIZE_BUCKETS; sb++) {
            char hb[32];
            char bg[32];
            uint64_t b = sum->bytes[ab][sb];
            uint64_t f = sum->files[ab][sb];
            row_total += b;

            human_bytes(b, hb, sizeof(hb));
            heatmap_color(b, ab, max_cell_bytes, bg, sizeof(bg));
            printf("<td class=\"cell\" style=\"background:%s\"><a href=\"" BUCKET_OUTPUT_DIR "/bucket_a%d_s%d.html\">%s<br><span style=\"color:#666;font-size:12px\">%" PRIu64 " files</span></a></td>", bg, ab, sb, hb, f);
        }

        {
            char hr[32];
            human_bytes(row_total, hr, sizeof(hr));
            printf("<td class=\"tot\">%s</td>", hr);
        }

        printf("</tr>\n");
    }

    printf("<tr class=\"tot\"><td>Total</td>");
    for (sb = 0; sb < SIZE_BUCKETS; sb++) {
        uint64_t col_total = 0;
        char hc[32];
        for (ab = 0; ab < AGE_BUCKETS; ab++) col_total += sum->bytes[ab][sb];
        human_bytes(col_total, hc, sizeof(hc));
        printf("<td>%s</td>", hc);
    }
    {
        char ht[32];
        human_bytes(sum->total_bytes, ht, sizeof(ht));
        printf("<td>%s</td>", ht);
    }
    printf("</tr>\n");

    printf("</table>\n");
    printf("</body>\n</html>\n");
}

int main(int argc, char **argv) {
    const char *username;
    const char *basis_str;
    const char *dirpath = ".";
    time_basis_t basis;
    struct passwd *pw;
    uid_t target_uid;
    char **paths = NULL;
    size_t path_count = 0;
    int threads = DEFAULT_THREADS;
    int threads_used;
    work_queue_t queue;
    pthread_t *tids = NULL;
    worker_arg_t *args = NULL;
    summary_t final_sum;
    bucket_details_t final_details[AGE_BUCKETS][SIZE_BUCKETS];
    matched_records_t final_matched_records;
    inode_set_t seen_inodes;
    time_t now;
    int i, ab, sb;

    if (argc < 3 || argc > 5) {
        fprintf(stderr, "Usage: %s <username> <atime|mtime|ctime> [bin_dir] [threads]\n", argv[0]);
        return 2;
    }

    username = argv[1];
    basis_str = argv[2];
    if (argc >= 4) dirpath = argv[3];
    if (argc == 5) {
        threads = atoi(argv[4]);
        if (threads <= 0) die("threads must be > 0");
    }

    if (parse_time_basis(basis_str, &basis) != 0) die("time basis must be one of: atime, mtime, ctime");

    pw = getpwnam(username);
    if (!pw) {
        fprintf(stderr, "unknown user: %s\n", username);
        return 1;
    }
    target_uid = pw->pw_uid;

    if (scan_dir_collect_files(dirpath, &paths, &path_count) != 0) return 1;
    if (path_count == 0) {
        fprintf(stderr, "no thread_*.bin files found in %s\n", dirpath);
        free(paths);
        return 1;
    }

    if ((size_t)threads > path_count) threads = (int)path_count;
    threads_used = threads;
    now = time(NULL);

    memset(&queue, 0, sizeof(queue));
    queue.paths = paths;
    queue.count = path_count;
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

    for (i = 0; i < threads; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].queue = &queue;
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
            matched_records_free(&final_matched_records);
            for (ab = 0; ab < AGE_BUCKETS; ab++) {
                for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
            }
            free(tids);
            free(args);
            for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
            free(paths);
            pthread_mutex_destroy(&queue.mutex);
            inode_set_destroy(&seen_inodes);
            return 1;
        }
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            for (sb = 0; sb < SIZE_BUCKETS; sb++) {
                if (bucket_details_merge(&final_details[ab][sb], &args[i].details[ab][sb]) != 0) {
                    fprintf(stderr, "allocation failed merging bucket details\n");
                    matched_records_free(&final_matched_records);
                    for (ab = 0; ab < AGE_BUCKETS; ab++) {
                        for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
                    }
                    free(tids);
                    free(args);
                    for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
                    free(paths);
                    pthread_mutex_destroy(&queue.mutex);
                    inode_set_destroy(&seen_inodes);
                    return 1;
                }
            }
        }
    }

    if (emit_all_bucket_detail_pages(username, basis_str, final_details, &final_matched_records) != 0) {
        fprintf(stderr, "failed to write bucket detail pages\n");
    }

    emit_html(username, target_uid, basis_str, &final_sum, path_count, threads_used);

    free(tids);
    free(args);
    for (i = 0; (size_t)i < path_count; i++) free(paths[i]);
    free(paths);
    pthread_mutex_destroy(&queue.mutex);
    inode_set_destroy(&seen_inodes);
    matched_records_free(&final_matched_records);
    for (ab = 0; ab < AGE_BUCKETS; ab++) {
        for (sb = 0; sb < SIZE_BUCKETS; sb++) bucket_details_free(&final_details[ab][sb]);
    }

    return 0;
}
