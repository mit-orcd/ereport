/*
 * ereport_simple.c
 *
 * Parallel reader for thread_*.bin files produced by ecrawl_nfs.
 *
 * What it does:
 *   - Scans a directory for thread_*.bin files
 *   - Uses N reader threads (default 32)
 *   - Resolves the requested username once to a numeric uid
 *   - Reads .bin files in parallel
 *   - Aggregates file capacity into:
 *       age buckets x size buckets
 *   - Emits one HTML report to stdout
 *
 * Build:
 *   gcc -O2 -Wall -Wextra -pthread -o ereport_simple ereport_simple.c
 *
 * Usage:
 *   ./ereport_simple <username> <atime|mtime|ctime> [bin_dir] [threads] > report.html
 *
 * Examples:
 *   ./ereport_simple erbmi1 mtime > report.html
 *   ./ereport_simple erbmi1 atime /data/crawl_bins 32 > report.html
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

typedef struct __attribute__((packed)) {
    char magic[FILE_MAGIC_LEN];   /* "NFSCBIN\0" */
    uint32_t version;
    uint32_t reserved;
} bin_file_header_t;

typedef struct __attribute__((packed)) {
    uint16_t path_len;
    uint8_t  type;       /* 'f', 'd', 'l', ... */
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
} worker_arg_t;

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
            while (new_used[idx]) {
                idx = (idx + 1) & (new_cap - 1);
            }
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
    if (ts == 0 || ts > (uint64_t)now) {
        return 0;
    }

    uint64_t age_sec = (uint64_t)now - ts;
    uint64_t days = age_sec / 86400ULL;

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

    if (d >= 100.0) {
        snprintf(buf, sz, "%.0f%s", d, units[i]);
    } else if (d >= 10.0) {
        snprintf(buf, sz, "%.1f%s", d, units[i]);
    } else {
        snprintf(buf, sz, "%.2f%s", d, units[i]);
    }
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
    const int empty_r = 230, empty_g = 244, empty_b = 234; /* pastel green */
    const int low_r   = 228, low_g   = 244, low_b   = 223; /* young/small */
    const int high_r  = 245, high_g  = 214, high_b  = 214; /* old/large */
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

    /* Older data should push the cell red faster than pure size alone. */
    score = 0.30 * volume_score + 0.55 * age_score + 0.15 * (volume_score * age_score);
    score = clamp01(score);

    r = (int)(low_r  + (high_r - low_r) * score + 0.5);
    g = (int)(low_g  + (high_g - low_g) * score + 0.5);
    b = (int)(low_b  + (high_b - low_b) * score + 0.5);

    snprintf(buf, sz, "rgb(%d,%d,%d)", r, g, b);
}

static void summary_merge(summary_t *dst, const summary_t *src) {
    int ab, sb;

    dst->total_bytes      += src->total_bytes;
    dst->total_capacity_bytes += src->total_capacity_bytes;
    dst->total_files      += src->total_files;
    dst->total_dirs       += src->total_dirs;
    dst->total_links      += src->total_links;
    dst->total_others     += src->total_others;
    dst->total_other_bytes += src->total_other_bytes;
    dst->scanned_records  += src->scanned_records;
    dst->matched_records  += src->matched_records;
    dst->matched_files    += src->matched_files;
    dst->matched_dirs     += src->matched_dirs;
    dst->matched_links    += src->matched_links;
    dst->matched_others   += src->matched_others;
    dst->scanned_input_files += src->scanned_input_files;
    dst->bad_input_files     += src->bad_input_files;

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
    if (q->next_index < q->count) {
        path = q->paths[q->next_index];
        q->next_index++;
    }
    pthread_mutex_unlock(&q->mutex);

    return path;
}

static int read_one_file(const char *filepath,
                         uid_t target_uid,
                         time_basis_t basis,
                         time_t now,
                         inode_set_t *seen_inodes,
                         summary_t *sum) {
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

        memset(&r, 0, sizeof(r));

        n = fread(&r, sizeof(r), 1, fp);
        if (n != 1) {
            if (feof(fp)) {
                rc = 0;
            } else {
                fprintf(stderr, "warn: read error in %s\n", filepath);
                sum->bad_input_files++;
            }
            break;
        }

        sum->scanned_records++;

        if (r.path_len > 0) {
            if (fseek(fp, (long)r.path_len, SEEK_CUR) != 0) {
                fprintf(stderr, "warn: path skip failed in %s\n", filepath);
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
                        break;
                    }
                    if (ins == 0) {
                        count_bytes = 0;
                    }
                }

                sum->matched_files++;
                sum->total_files++;

                if (count_bytes) {
                    sum->total_capacity_bytes += r.size;
                    sum->total_bytes += r.size;
                    sum->bytes[ab][sb] += r.size;
                    sum->files[ab][sb] += 1;
                }
            } else if (r.type == 'd') {
                sum->matched_dirs++;
                sum->total_dirs++;
                sum->total_capacity_bytes += r.size;
            } else if (r.type == 'l') {
                sum->matched_links++;
                sum->total_links++;
                sum->total_capacity_bytes += r.size;
            } else {
                sum->matched_others++;
                sum->total_others++;
                sum->total_other_bytes += r.size;
                sum->total_capacity_bytes += r.size;
            }
        }
    }

out:
    fclose(fp);
    return rc;
}

static void *worker_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;

    for (;;) {
        char *path = queue_pop(arg->queue);
        if (!path) {
            break;
        }

        read_one_file(path, arg->target_uid, arg->basis, arg->now, arg->seen_inodes, &arg->summary);
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

        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) {
            continue;
        }
        if (!starts_with_thread(de->d_name) || !has_bin_suffix(de->d_name)) {
            continue;
        }

        if (snprintf(full, sizeof(full), "%s/%s", dirpath, de->d_name) >= (int)sizeof(full)) {
            fprintf(stderr, "warn: path too long: %s/%s\n", dirpath, de->d_name);
            continue;
        }

        if (count == cap) {
            size_t new_cap = (cap == 0) ? 64 : cap * 2;
            char **tmp = (char **)realloc(paths, new_cap * sizeof(*paths));
            if (!tmp) {
                closedir(dir);
                for (size_t i = 0; i < count; i++) {
                    free(paths[i]);
                }
                free(paths);
                return -1;
            }
            paths = tmp;
            cap = new_cap;
        }

        copy = strdup(full);
        if (!copy) {
            closedir(dir);
            for (size_t i = 0; i < count; i++) {
                free(paths[i]);
            }
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
            if (sum->bytes[ab][sb] > max_cell_bytes) {
                max_cell_bytes = sum->bytes[ab][sb];
            }
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
            printf("<td class=\"cell\" style=\"background:%s\">%s<br><span style=\"color:#666;font-size:12px\">%" PRIu64 " files</span></td>", bg, hb, f);
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
        for (ab = 0; ab < AGE_BUCKETS; ab++) {
            col_total += sum->bytes[ab][sb];
        }
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
    inode_set_t seen_inodes;
    time_t now;
    int i;

    if (argc < 3 || argc > 5) {
        fprintf(stderr, "Usage: %s <username> <atime|mtime|ctime> [bin_dir] [threads]\n", argv[0]);
        return 2;
    }

    username = argv[1];
    basis_str = argv[2];
    if (argc >= 4) {
        dirpath = argv[3];
    }
    if (argc == 5) {
        threads = atoi(argv[4]);
        if (threads <= 0) {
            die("threads must be > 0");
        }
    }

    if (parse_time_basis(basis_str, &basis) != 0) {
        die("time basis must be one of: atime, mtime, ctime");
    }

    pw = getpwnam(username);
    if (!pw) {
        fprintf(stderr, "unknown user: %s\n", username);
        return 1;
    }
    target_uid = pw->pw_uid;

    if (scan_dir_collect_files(dirpath, &paths, &path_count) != 0) {
        return 1;
    }

    if (path_count == 0) {
        fprintf(stderr, "no thread_*.bin files found in %s\n", dirpath);
        free(paths);
        return 1;
    }

    if ((size_t)threads > path_count) {
        threads = (int)path_count;
    }
    threads_used = threads;
    now = time(NULL);

    memset(&queue, 0, sizeof(queue));
    queue.paths = paths;
    queue.count = path_count;
    queue.next_index = 0;
    pthread_mutex_init(&queue.mutex, NULL);

    if (inode_set_init(&seen_inodes, 65536) != 0) {
        fprintf(stderr, "allocation failed\n");
        for (size_t k = 0; k < path_count; k++) {
            free(paths[k]);
        }
        free(paths);
        pthread_mutex_destroy(&queue.mutex);
        return 1;
    }

    tids = (pthread_t *)calloc((size_t)threads, sizeof(*tids));
    args = (worker_arg_t *)calloc((size_t)threads, sizeof(*args));
    if (!tids || !args) {
        fprintf(stderr, "allocation failed\n");
        free(tids);
        free(args);
        for (size_t k = 0; k < path_count; k++) {
            free(paths[k]);
        }
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

    for (i = 0; i < threads_used; i++) {
        pthread_join(tids[i], NULL);
        summary_merge(&final_sum, &args[i].summary);
    }

    emit_html(username, target_uid, basis_str, &final_sum, path_count, threads_used);

    free(tids);
    free(args);
    for (size_t k = 0; k < path_count; k++) {
        free(paths[k]);
    }
    free(paths);
    pthread_mutex_destroy(&queue.mutex);
    inode_set_destroy(&seen_inodes);

    return 0;
}

