#define _XOPEN_SOURCE 700

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define FILE_MAGIC_LEN 8
#define FORMAT_VERSION 2
#define INDEX_VERSION 1
#define TRIGRAM_BUCKET_BITS 12
#define TRIGRAM_BUCKET_COUNT (1U << TRIGRAM_BUCKET_BITS)
#define BUCKET_CACHE_SLOTS 64
#define DEFAULT_THREADS 32
#define PARSE_CHUNK_BYTES (32ULL << 20)
#define PARSE_CHUNK_MIN_BYTES (1ULL << 20)
#define WRITE_BATCH_PATHS 4096
#define MERGE_IO_BUFSIZE (1U << 20)
#define MERGE_MAX_WORKERS 16
#define MERGE_PARALLEL_MIN 4

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

typedef struct __attribute__((packed)) {
    uint32_t trigram;
    uint64_t path_id;
} trigram_record_t;

typedef struct __attribute__((packed)) {
    uint32_t trigram;
    uint32_t reserved;
    uint64_t postings_offset;
    uint64_t postings_bytes;
} trigram_key_t;

typedef struct {
    int bucket;
    FILE *fp;
    uint64_t tick;
} bucket_cache_slot_t;

typedef struct {
    char dir[PATH_MAX];
    bucket_cache_slot_t slots[BUCKET_CACHE_SLOTS];
    uint64_t tick;
} bucket_cache_t;

typedef struct {
    char *path;
    uint64_t start_offset;
    uint64_t end_offset;
    size_t file_index;
} file_chunk_t;

typedef struct {
    file_chunk_t *chunks;
    size_t count;
    size_t next_index;
    pthread_mutex_t mutex;
} work_queue_t;

typedef struct {
    char *path;
    uint32_t *codes;
    size_t code_count;
} parsed_path_t;

typedef struct write_batch {
    parsed_path_t *items;
    size_t count;
    size_t cap;
    struct write_batch *next;
} write_batch_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    write_batch_t *head;
    write_batch_t *tail;
    int closed;
} write_queue_t;

typedef struct {
    atomic_uint remaining_chunks;
} file_state_t;

typedef struct {
    uid_t target_uid;
    char display_name[256];
    char index_dir[PATH_MAX];
    FILE *paths_fp;
    FILE *path_offsets_fp;
    bucket_cache_t bucket_cache;
    uint64_t input_files;
    uint64_t scanned_records;
    uint64_t indexed_paths;
    uint64_t trigram_records;
    uint64_t bad_input_files;
    uint64_t unique_trigrams;
    double start_sec;
    double last_status_sec;
    double last_rate_sec;
    uint64_t last_rate_indexed_paths;
    uint64_t last_rate_merge_units;
    /* process_trigram_buckets (single-threaded merge) */
    double merge_phase_sec;
    uint32_t merge_buckets_nonempty;
    uint32_t merge_buckets_skipped;
    uint64_t merge_trigram_records_read;
    uint64_t merge_bytes_temp_read;
    uint64_t merge_bytes_tri_keys_written;
    uint64_t merge_bytes_tri_postings_written;
    uint8_t bucket_nonempty[TRIGRAM_BUCKET_COUNT];
    int merge_workers_used;
    double index_phase_sec;
    int index_workers_used;
} build_ctx_t;

typedef struct {
    work_queue_t *queue;
    write_queue_t *write_queue;
    file_state_t *file_states;
    build_ctx_t *ctx;
    char *lower_seg_buf;
    size_t lower_seg_cap;
} worker_arg_t;

typedef struct {
    uint64_t *ids;
    size_t count;
    size_t cap;
} u64_vec_t;

static const char *g_input_layout = "legacy";
static uint32_t g_input_uid_shards = 0;
static atomic_ullong g_progress_scanned_input_files = 0;
static atomic_ullong g_progress_scanned_records = 0;
static atomic_ullong g_progress_indexed_paths = 0;
static atomic_ullong g_progress_trigram_records = 0;
static atomic_ullong g_progress_bad_input_files = 0;
static atomic_int g_stop_stats = 0;
static atomic_int g_writer_failed = 0;
static uint64_t g_progress_input_files_total = 0;
static double g_run_start_sec = 0.0;

/* Relaxed counter: pthread_cond_wait wakeups while write queue empty (writer idle vs producers). */
static atomic_ullong g_writeq_writer_waits = 0;

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

static void maybe_emit_status(build_ctx_t *ctx,
                              const char *phase,
                              uint64_t done_units,
                              uint64_t total_units) {
    double now;
    double elapsed_sec;
    double rate_sec;
    double rate;
    const char *rate_label;
    char rate_buf[32], done_buf[32], total_buf[32], rec_buf[32], idx_buf[32], tri_buf[32], elapsed_buf[32];

    if (!ctx) return;

    now = now_sec();
    if (ctx->last_status_sec > 0.0 && (now - ctx->last_status_sec) < 1.0) return;

    elapsed_sec = ctx->start_sec > 0.0 ? (now - ctx->start_sec) : 0.0;
    rate_sec = ctx->last_rate_sec > 0.0 ? (now - ctx->last_rate_sec) : 0.0;
    if (strcmp(phase, "merge") == 0) {
        rate_label = "buckets/s";
        if (rate_sec > 0.0) {
            rate = (double)(done_units - ctx->last_rate_merge_units) / rate_sec;
        } else {
            rate = 0.0;
        }
    } else {
        rate_label = "paths/s";
        if (rate_sec > 0.0) {
            rate = (double)(ctx->indexed_paths - ctx->last_rate_indexed_paths) / rate_sec;
        } else {
            rate = elapsed_sec > 0.0 ? (double)ctx->indexed_paths / elapsed_sec : 0.0;
        }
    }

    human_decimal(rate, rate_buf, sizeof(rate_buf));
    human_decimal((double)done_units, done_buf, sizeof(done_buf));
    human_decimal((double)total_units, total_buf, sizeof(total_buf));
    human_decimal((double)ctx->scanned_records, rec_buf, sizeof(rec_buf));
    human_decimal((double)ctx->indexed_paths, idx_buf, sizeof(idx_buf));
    human_decimal((double)ctx->trigram_records, tri_buf, sizeof(tri_buf));
    format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

    printf("\r%s %s | phase:%s unit:%s/%s rec:%s idx:%s tri:%s bad:%" PRIu64 " | el:%s            ",
           rate_buf,
           rate_label,
           phase,
           done_buf,
           total_buf,
           rec_buf,
           idx_buf,
           tri_buf,
           ctx->bad_input_files,
           elapsed_buf);
    fflush(stdout);

    ctx->last_status_sec = now;
    ctx->last_rate_sec = now;
    if (strcmp(phase, "merge") == 0) {
        ctx->last_rate_merge_units = done_units;
    } else {
        ctx->last_rate_indexed_paths = ctx->indexed_paths;
    }
}

static file_chunk_t *queue_pop(work_queue_t *q) {
    file_chunk_t *chunk = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->next_index < q->count) chunk = &q->chunks[q->next_index++];
    pthread_mutex_unlock(&q->mutex);

    return chunk;
}

static int write_batch_append(write_batch_t *batch, char *path, uint32_t *codes, size_t code_count) {
    parsed_path_t *tmp;

    if (batch->count == batch->cap) {
        size_t new_cap = batch->cap ? batch->cap * 2 : 256;
        tmp = (parsed_path_t *)realloc(batch->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        batch->items = tmp;
        batch->cap = new_cap;
    }

    batch->items[batch->count].path = path;
    batch->items[batch->count].codes = codes;
    batch->items[batch->count].code_count = code_count;
    batch->count++;
    return 0;
}

static void write_batch_destroy(write_batch_t *batch) {
    if (!batch) return;
    for (size_t i = 0; i < batch->count; i++) {
        free(batch->items[i].path);
        free(batch->items[i].codes);
    }
    free(batch->items);
    free(batch);
}

static int write_queue_push(write_queue_t *q, write_batch_t *batch) {
    pthread_mutex_lock(&q->mutex);
    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    batch->next = NULL;
    if (q->tail) q->tail->next = batch;
    else q->head = batch;
    q->tail = batch;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static write_batch_t *write_queue_pop_wait(write_queue_t *q) {
    write_batch_t *batch;

    pthread_mutex_lock(&q->mutex);
    while (!q->head && !q->closed) {
        pthread_cond_wait(&q->cond, &q->mutex);
        atomic_fetch_add_explicit(&g_writeq_writer_waits, 1ULL, memory_order_relaxed);
    }
    batch = q->head;
    if (batch) {
        q->head = batch->next;
        if (!q->head) q->tail = NULL;
    }
    pthread_mutex_unlock(&q->mutex);
    return batch;
}

static void write_queue_close(write_queue_t *q) {
    pthread_mutex_lock(&q->mutex);
    q->closed = 1;
    pthread_cond_broadcast(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

static void *stats_thread_main(void *arg) {
    (void)arg;

    while (!atomic_load(&g_stop_stats)) {
        unsigned long long scanned_files;
        unsigned long long scanned_records;
        unsigned long long indexed_paths;
        unsigned long long trigram_records;
        unsigned long long bad_input_files;
        double elapsed_sec;
        char sf[32], tf[32], sr[32], ip[32], tr[32], rate_buf[32], elapsed_buf[32];
        static unsigned long long prev_indexed_paths = 0;
        double rate;

        sleep(1);

        scanned_files = atomic_load(&g_progress_scanned_input_files);
        scanned_records = atomic_load(&g_progress_scanned_records);
        indexed_paths = atomic_load(&g_progress_indexed_paths);
        trigram_records = atomic_load(&g_progress_trigram_records);
        bad_input_files = atomic_load(&g_progress_bad_input_files);
        elapsed_sec = g_run_start_sec > 0.0 ? now_sec() - g_run_start_sec : 0.0;
        rate = (double)(indexed_paths - prev_indexed_paths);
        prev_indexed_paths = indexed_paths;

        human_decimal((double)scanned_files, sf, sizeof(sf));
        human_decimal((double)g_progress_input_files_total, tf, sizeof(tf));
        human_decimal((double)scanned_records, sr, sizeof(sr));
        human_decimal((double)indexed_paths, ip, sizeof(ip));
        human_decimal((double)trigram_records, tr, sizeof(tr));
        human_decimal(rate, rate_buf, sizeof(rate_buf));
        format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

        printf("\r%s paths/s | files:%s/%s rec:%s idx:%s tri:%s bad:%llu | el:%s            ",
               rate_buf, sf, tf, sr, ip, tr, bad_input_files, elapsed_buf);
        fflush(stdout);
    }

    return NULL;
}

static void die_usage(const char *argv0) {
    fprintf(stderr,
            "Usage:\n"
            "  %s --make <username|uid> [bin_dir ...]\n"
            "  %s --search <term> [index_dir] [--json] [--skip N] [--limit M]\n",
            argv0,
            argv0);
    fprintf(stderr,
            "    Default index_dir is ./index (relative to cwd). Plain search prints paths.\n"
            "    With --json: UTF-8 JSON object {\"total\",\"skip\",\"limit\",\"paths\":[...]}\n");
    exit(2);
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

    fp = fopen(manifest_path, "r");
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

    fclose(fp);
    if (!saw_layout) return 0;
    if (uid_shards == 0 || !is_power_of_two_u32(uid_shards)) {
        fprintf(stderr, "invalid uid_shards value in %s\n", manifest_path);
        return -1;
    }

    *uid_shards_out = uid_shards;
    return 1;
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

static void sanitize_name(char *s) {
    size_t i;
    for (i = 0; s[i] != '\0'; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '/' || c == '\\' || c == ':' || c == '\t' || c == '\n' || c == '\r') s[i] = '_';
    }
}

static int ensure_dir_recursive(const char *path) {
    char buf[PATH_MAX];
    char *p;

    if (snprintf(buf, sizeof(buf), "%s", path) >= (int)sizeof(buf)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    if (buf[0] == '\0') return 0;

    for (p = buf + 1; *p != '\0'; p++) {
        if (*p != '/') continue;
        *p = '\0';
        if (mkdir(buf, 0777) != 0 && errno != EEXIST) return -1;
        *p = '/';
    }

    if (mkdir(buf, 0777) != 0 && errno != EEXIST) return -1;
    return 0;
}

static int build_path(char *out, size_t out_sz, const char *dir, const char *name) {
    if (snprintf(out, out_sz, "%s/%s", dir, name) >= (int)out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int scan_dirs_collect_files(const char **dirpaths,
                                   size_t dir_count,
                                   uid_t target_uid,
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
                fprintf(stderr, "incompatible crawl layouts between %s and %s\n", dirpaths[0], dirpaths[di]);
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
        g_input_layout = "legacy";
        g_input_uid_shards = 0;
    }

    for (di = 0; di < dir_count; di++) {
        const char *dirpath = dirpaths[di];

        dir = opendir(dirpath);
        if (!dir) {
            fprintf(stderr, "cannot open directory %s: %s\n", dirpath, strerror(errno));
            goto fail_partial;
        }

        while ((de = readdir(dir)) != NULL) {
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

static int parse_index_thread_count(void) {
    const char *e = getenv("EREPORT_INDEX_THREADS");
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

    fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }

    if (fread(&fh, sizeof(fh), 1, fp) != 1) {
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

        if (fread(&r, sizeof(r), 1, fp) != 1) {
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
    if (fp) fclose(fp);
    return rc;
}

static int u64_vec_push(u64_vec_t *v, uint64_t id) {
    if (v->count == v->cap) {
        size_t new_cap = v->cap ? v->cap * 2 : 1024;
        uint64_t *tmp = (uint64_t *)realloc(v->ids, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        v->ids = tmp;
        v->cap = new_cap;
    }
    v->ids[v->count++] = id;
    return 0;
}

static int cmp_u32(const void *a, const void *b) {
    uint32_t aa = *(const uint32_t *)a;
    uint32_t bb = *(const uint32_t *)b;
    return (aa > bb) - (aa < bb);
}

static int write_varint_u64(FILE *fp, uint64_t value);

/* LSD radix: 96-bit key as trigram<<64|path_id (big-endian); passes 0..7 = path_id LE, 8..11 = trigram LE. */
static unsigned char trigram_record_radix_byte(const trigram_record_t *r, int pass) {
    if (pass < 8) return ((const unsigned char *)&r->path_id)[pass];
    return ((const unsigned char *)&r->trigram)[pass - 8];
}

static void radix_sort_trigram_records(trigram_record_t *records, size_t n, trigram_record_t *aux) {
    trigram_record_t *in;
    trigram_record_t *out;
    int pass;

    if (n < 2) return;
    in = records;
    out = aux;
    for (pass = 0; pass < 12; pass++) {
        size_t c[256];
        size_t pos[256];
        size_t i;
        memset(c, 0, sizeof(c));
        for (i = 0; i < n; i++) c[trigram_record_radix_byte(&in[i], pass)]++;
        pos[0] = 0;
        for (i = 1; i < 256; i++) pos[i] = pos[i - 1] + c[i - 1];
        for (i = 0; i < n; i++) {
            unsigned k = trigram_record_radix_byte(&in[i], pass);
            out[pos[k]++] = in[i];
        }
        {
            trigram_record_t *t = in;
            in = out;
            out = t;
        }
    }
    if (in != records) memcpy(records, in, n * sizeof(*records));
}

static int write_sorted_bucket_records(FILE *keys_fp, FILE *postings_fp, trigram_record_t *records, size_t record_count,
                                       uint64_t *unique_out) {
    size_t i = 0;

    *unique_out = 0;
    while (i < record_count) {
        trigram_key_t key;
        uint64_t prev = 0;
        size_t j;

        key.trigram = records[i].trigram;
        key.reserved = 0;
        key.postings_offset = (uint64_t)ftello(postings_fp);

        j = i;
        while (j < record_count && records[j].trigram == key.trigram) j++;

        {
            uint64_t last_path = UINT64_MAX;
            for (size_t k = i; k < j; k++) {
                uint64_t path_id = records[k].path_id;
                uint64_t delta;

                if (path_id == last_path) continue;
                delta = (prev == 0) ? (path_id + 1U) : (path_id - prev);
                if (write_varint_u64(postings_fp, delta) != 0) return -1;
                prev = path_id;
                last_path = path_id;
            }
        }

        key.postings_bytes = (uint64_t)ftello(postings_fp) - key.postings_offset;
        if (fwrite(&key, sizeof(key), 1, keys_fp) != 1) return -1;
        (*unique_out)++;
        i = j;
    }
    return 0;
}

static char *ascii_lower_dup(const char *s) {
    size_t len = strlen(s);
    char *out = (char *)malloc(len + 1);
    size_t i;

    if (!out) return NULL;
    for (i = 0; i < len; i++) out[i] = (char)tolower((unsigned char)s[i]);
    out[len] = '\0';
    return out;
}

static int path_element_contains(const char *lower_path, const char *lower_term) {
    const char *seg = lower_path;
    size_t term_len = strlen(lower_term);

    if (term_len == 0) return 1;

    while (*seg != '\0') {
        const char *next = strchr(seg, '/');
        size_t seg_len = next ? (size_t)(next - seg) : strlen(seg);

        if (seg_len > 0) {
            size_t i;
            for (i = 0; i + term_len <= seg_len; i++) {
                if (memcmp(seg + i, lower_term, term_len) == 0) return 1;
            }
        }

        if (!next) break;
        seg = next + 1;
    }

    return 0;
}

static int path_matches_term(const char *path, const char *lower_term) {
    char *lower = ascii_lower_dup(path);
    int matched;
    if (!lower) return 0;
    matched = path_element_contains(lower, lower_term);
    free(lower);
    return matched;
}

static int ensure_worker_buf(char **buf, size_t *cap, size_t need) {
    char *p;
    size_t nc;

    if (*cap >= need) return 0;
    nc = *cap ? *cap : 4096U;
    while (nc < need) nc <<= 1;
    p = (char *)realloc(*buf, nc);
    if (!p) return -1;
    *buf = p;
    *cap = nc;
    return 0;
}

static void insertion_sort_u32(uint32_t *a, size_t n) {
    size_t i;
    for (i = 1; i < n; i++) {
        uint32_t key = a[i];
        size_t j = i;
        while (j > 0 && a[j - 1] > key) {
            a[j] = a[j - 1];
            j--;
        }
        a[j] = key;
    }
}

static void sort_codes_unique(uint32_t *codes, size_t *count) {
    size_t n = *count;
    size_t w;
    size_t i;

    if (n <= 1) return;
    if (n <= 64U)
        insertion_sort_u32(codes, n);
    else
        qsort(codes, n, sizeof(*codes), cmp_u32);

    w = 1;
    for (i = 1; i < n; i++) {
        if (codes[i] != codes[w - 1]) codes[w++] = codes[i];
    }
    *count = w;
}

static int append_unique_trigram(uint32_t **codes, size_t *count, size_t *cap, uint32_t trigram) {
    if (*count == *cap) {
        size_t new_cap = *cap ? (*cap * 2) : 32;
        uint32_t *tmp = (uint32_t *)realloc(*codes, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        *codes = tmp;
        *cap = new_cap;
    }
    (*codes)[(*count)++] = trigram;
    return 0;
}

static int extract_path_trigrams(const char *path, uint32_t **out_codes, size_t *out_count, worker_arg_t *scratch) {
    const char *seg = path;
    uint32_t *codes = NULL;
    size_t count = 0;
    size_t cap = 0;

    while (*seg != '\0') {
        const char *next = strchr(seg, '/');
        size_t seg_len = next ? (size_t)(next - seg) : strlen(seg);

        if (seg_len >= 3) {
            size_t i;
            char *lower_seg;

            if (scratch) {
                if (ensure_worker_buf(&scratch->lower_seg_buf, &scratch->lower_seg_cap, seg_len + 1U) != 0) {
                    free(codes);
                    return -1;
                }
                lower_seg = scratch->lower_seg_buf;
            } else {
                lower_seg = (char *)malloc(seg_len + 1);
                if (!lower_seg) {
                    free(codes);
                    return -1;
                }
            }

            for (i = 0; i < seg_len; i++) lower_seg[i] = (char)tolower((unsigned char)seg[i]);
            lower_seg[seg_len] = '\0';

            for (i = 0; i + 3 <= seg_len; i++) {
                uint32_t trigram = ((uint32_t)(unsigned char)lower_seg[i] << 16) |
                                   ((uint32_t)(unsigned char)lower_seg[i + 1] << 8) |
                                   (uint32_t)(unsigned char)lower_seg[i + 2];
                if (append_unique_trigram(&codes, &count, &cap, trigram) != 0) {
                    if (!scratch) free(lower_seg);
                    free(codes);
                    return -1;
                }
            }
            if (!scratch) free(lower_seg);
        }

        if (!next) break;
        seg = next + 1;
    }

    if (count > 1) sort_codes_unique(codes, &count);

    *out_codes = codes;
    *out_count = count;
    return 0;
}

static int bucket_cache_init(bucket_cache_t *cache, const char *dir) {
    memset(cache, 0, sizeof(*cache));
    if (snprintf(cache->dir, sizeof(cache->dir), "%s", dir) >= (int)sizeof(cache->dir)) return -1;
    for (size_t i = 0; i < BUCKET_CACHE_SLOTS; i++) cache->slots[i].bucket = -1;
    return 0;
}

static void bucket_cache_close_all(bucket_cache_t *cache) {
    for (size_t i = 0; i < BUCKET_CACHE_SLOTS; i++) {
        if (cache->slots[i].fp) fclose(cache->slots[i].fp);
        cache->slots[i].fp = NULL;
        cache->slots[i].bucket = -1;
        cache->slots[i].tick = 0;
    }
}

static FILE *bucket_cache_get_fp(bucket_cache_t *cache, uint32_t bucket, build_ctx_t *ctx) {
    size_t i;
    size_t victim = 0;
    uint64_t victim_tick = UINT64_MAX;
    char path[PATH_MAX];
    FILE *fp;

    for (i = 0; i < BUCKET_CACHE_SLOTS; i++) {
        if (cache->slots[i].bucket == (int)bucket && cache->slots[i].fp) {
            cache->slots[i].tick = ++cache->tick;
            return cache->slots[i].fp;
        }
    }

    for (i = 0; i < BUCKET_CACHE_SLOTS; i++) {
        if (!cache->slots[i].fp) {
            victim = i;
            victim_tick = 0;
            break;
        }
        if (cache->slots[i].tick < victim_tick) {
            victim_tick = cache->slots[i].tick;
            victim = i;
        }
    }

    if (cache->slots[victim].fp) fclose(cache->slots[victim].fp);

    if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u.bin", cache->dir, bucket) >= (int)sizeof(path)) return NULL;
    fp = fopen(path, "ab");
    if (!fp) return NULL;
    if (setvbuf(fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (ctx) ctx->bucket_nonempty[bucket] = 1;

    cache->slots[victim].bucket = (int)bucket;
    cache->slots[victim].fp = fp;
    cache->slots[victim].tick = ++cache->tick;
    return fp;
}

static int write_varint_u64(FILE *fp, uint64_t value) {
    unsigned char buf[10];
    size_t n = 0;

    while (value >= 0x80U) {
        buf[n++] = (unsigned char)((value & 0x7FU) | 0x80U);
        value >>= 7;
    }
    buf[n++] = (unsigned char)value;
    return fwrite(buf, 1, n, fp) == n ? 0 : -1;
}

static int decode_varint_u64_buf(const unsigned char *buf, size_t len, size_t *pos, uint64_t *out) {
    uint64_t value = 0;
    unsigned shift = 0;

    while (*pos < len) {
        unsigned char byte = buf[(*pos)++];
        value |= (uint64_t)(byte & 0x7FU) << shift;
        if ((byte & 0x80U) == 0) {
            *out = value;
            return 0;
        }
        shift += 7;
        if (shift >= 64) return -1;
    }

    return -1;
}

static int append_path_and_trigrams_locked(build_ctx_t *ctx,
                                           const char *path,
                                           const uint32_t *codes,
                                           size_t code_count) {
    uint64_t path_id = ctx->indexed_paths;
    uint64_t offset;

    offset = (uint64_t)ftello(ctx->paths_fp);
    if (fwrite(&offset, sizeof(offset), 1, ctx->path_offsets_fp) != 1) return -1;
    if (fwrite(path, 1, strlen(path) + 1, ctx->paths_fp) != strlen(path) + 1) return -1;

    for (size_t i = 0; i < code_count; i++) {
        trigram_record_t rec;
        uint32_t bucket = codes[i] >> (24 - TRIGRAM_BUCKET_BITS);
        FILE *bucket_fp = bucket_cache_get_fp(&ctx->bucket_cache, bucket, ctx);
        if (!bucket_fp) {
            return -1;
        }
        rec.trigram = codes[i];
        rec.path_id = path_id;
        if (fwrite(&rec, sizeof(rec), 1, bucket_fp) != 1) {
            return -1;
        }
    }

    ctx->trigram_records += (uint64_t)code_count;
    ctx->indexed_paths++;
    return 0;
}

static void *writer_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;

    for (;;) {
        write_batch_t *batch = write_queue_pop_wait(arg->write_queue);
        if (!batch) break;

        for (size_t i = 0; i < batch->count; i++) {
            parsed_path_t *item = &batch->items[i];
            if (append_path_and_trigrams_locked(arg->ctx, item->path, item->codes, item->code_count) != 0) {
                atomic_store(&g_writer_failed, 1);
                write_batch_destroy(batch);
                return NULL;
            }
            atomic_fetch_add(&g_progress_indexed_paths, 1U);
            atomic_fetch_add(&g_progress_trigram_records, (unsigned long long)item->code_count);
        }

        write_batch_destroy(batch);
    }

    return NULL;
}

static void finalize_chunk_file_progress(file_state_t *file_states, size_t file_index) {
    unsigned int old_remaining;
    if (!file_states) return;
    old_remaining = atomic_fetch_sub(&file_states[file_index].remaining_chunks, 1U);
    if (old_remaining == 1U) atomic_fetch_add(&g_progress_scanned_input_files, 1U);
}

static int process_chunk_make(worker_arg_t *worker, const file_chunk_t *chunk) {
    build_ctx_t *ctx = worker->ctx;
    write_queue_t *write_queue = worker->write_queue;
    file_state_t *file_states = worker->file_states;
    FILE *fp = NULL;
    int rc = -1;
    write_batch_t *batch = NULL;

    fp = fopen(chunk->path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", chunk->path, strerror(errno));
        atomic_fetch_add(&g_progress_bad_input_files, 1U);
        return -1;
    }

    if (setvbuf(fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    if (fseeko(fp, (off_t)chunk->start_offset, SEEK_SET) != 0) {
        fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
        atomic_fetch_add(&g_progress_bad_input_files, 1U);
        goto out;
    }

    for (;;) {
        bin_record_hdr_t r;
        char *pathbuf = NULL;
        size_t n;
        off_t record_offset = ftello(fp);
        uint64_t bytes_left_after_hdr;

        if (record_offset < 0 || (uint64_t)record_offset >= chunk->end_offset) {
            rc = 0;
            break;
        }

        memset(&r, 0, sizeof(r));
        n = fread(&r, sizeof(r), 1, fp);
        if (n != 1) {
            if (feof(fp)) rc = 0;
            else {
                fprintf(stderr, "warn: read error in %s\n", chunk->path);
                atomic_fetch_add(&g_progress_bad_input_files, 1U);
            }
            break;
        }

        atomic_fetch_add(&g_progress_scanned_records, 1U);

        bytes_left_after_hdr = chunk->end_offset - (uint64_t)ftello(fp);
        if ((uint64_t)r.path_len > bytes_left_after_hdr) {
            fprintf(stderr, "warn: truncated record in %s\n", chunk->path);
            atomic_fetch_add(&g_progress_bad_input_files, 1U);
            break;
        }

        if ((uid_t)r.uid != ctx->target_uid) {
            if (r.path_len > 0 && fseeko(fp, (off_t)r.path_len, SEEK_CUR) != 0) {
                fprintf(stderr, "warn: seek failed skipping path in %s\n", chunk->path);
                atomic_fetch_add(&g_progress_bad_input_files, 1U);
                break;
            }
            continue;
        }

        pathbuf = (char *)malloc((size_t)r.path_len + 1);
        if (!pathbuf) {
            fprintf(stderr, "warn: path alloc failed in %s\n", chunk->path);
            atomic_fetch_add(&g_progress_bad_input_files, 1U);
            break;
        }
        if (r.path_len > 0 && fread(pathbuf, 1, r.path_len, fp) != r.path_len) {
            fprintf(stderr, "warn: path read failed in %s\n", chunk->path);
            atomic_fetch_add(&g_progress_bad_input_files, 1U);
            free(pathbuf);
            break;
        }
        pathbuf[r.path_len] = '\0';

        {
            uint32_t *codes = NULL;
            size_t code_count = 0;
            if (extract_path_trigrams(pathbuf, &codes, &code_count, worker) != 0) {
                fprintf(stderr, "warn: failed to extract trigrams from %s\n", chunk->path);
                atomic_fetch_add(&g_progress_bad_input_files, 1U);
                free(pathbuf);
                break;
            }

            if (!batch) {
                batch = (write_batch_t *)calloc(1, sizeof(*batch));
                if (!batch) {
                    fprintf(stderr, "warn: failed to allocate write batch for %s\n", chunk->path);
                    atomic_fetch_add(&g_progress_bad_input_files, 1U);
                    free(codes);
                    free(pathbuf);
                    break;
                }
            }
            if (write_batch_append(batch, pathbuf, codes, code_count) != 0) {
                fprintf(stderr, "warn: failed to append write batch for %s\n", chunk->path);
                atomic_fetch_add(&g_progress_bad_input_files, 1U);
                free(codes);
                free(pathbuf);
                break;
            }
            pathbuf = NULL;
            codes = NULL;

            if (batch->count >= WRITE_BATCH_PATHS) {
                if (write_queue_push(write_queue, batch) != 0) {
                    fprintf(stderr, "warn: failed to queue write batch for %s\n", chunk->path);
                    atomic_fetch_add(&g_progress_bad_input_files, 1U);
                    write_batch_destroy(batch);
                    batch = NULL;
                    break;
                }
                batch = NULL;
            }
        }

        free(pathbuf);
    }

out:
    fclose(fp);
    if (batch) {
        if (write_queue_push(write_queue, batch) != 0) {
            atomic_fetch_add(&g_progress_bad_input_files, 1U);
            write_batch_destroy(batch);
        }
    }
    finalize_chunk_file_progress(file_states, chunk->file_index);
    return rc;
}

static void *worker_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;

    for (;;) {
        file_chunk_t *chunk = queue_pop(arg->queue);
        if (!chunk) break;
        if (atomic_load(&g_writer_failed)) break;
        process_chunk_make(arg, chunk);
    }

    free(arg->lower_seg_buf);
    arg->lower_seg_buf = NULL;
    arg->lower_seg_cap = 0;

    return NULL;
}

static int write_final_path_offset(build_ctx_t *ctx) {
    uint64_t final_offset = (uint64_t)ftello(ctx->paths_fp);
    return fwrite(&final_offset, sizeof(final_offset), 1, ctx->path_offsets_fp) == 1 ? 0 : -1;
}

static int write_meta_file(const build_ctx_t *ctx) {
    char path[PATH_MAX];
    FILE *fp;

    if (build_path(path, sizeof(path), ctx->index_dir, "meta.txt") != 0) return -1;
    fp = fopen(path, "w");
    if (!fp) return -1;

    fprintf(fp, "ereport_index_version=%d\n", INDEX_VERSION);
    fprintf(fp, "user=%s\n", ctx->display_name);
    fprintf(fp, "uid=%lu\n", (unsigned long)ctx->target_uid);
    fprintf(fp, "input_layout=%s\n", g_input_layout);
    if (g_input_uid_shards) fprintf(fp, "input_uid_shards=%u\n", g_input_uid_shards);
    fprintf(fp, "input_files=%" PRIu64 "\n", ctx->input_files);
    fprintf(fp, "indexed_paths=%" PRIu64 "\n", ctx->indexed_paths);
    fprintf(fp, "unique_trigrams=%" PRIu64 "\n", ctx->unique_trigrams);
    fprintf(fp, "bucket_bits=%d\n", TRIGRAM_BUCKET_BITS);
    fprintf(fp, "bucket_count=%u\n", TRIGRAM_BUCKET_COUNT);

    if (fclose(fp) != 0) return -1;
    return 0;
}

typedef struct {
    trigram_record_t *records;
    size_t n;
    uint64_t bytes;
    void *mmap_base;
    size_t mmap_len;
    int malloc_copy;
} merge_loaded_bucket_t;

static void merge_loaded_bucket_destroy(merge_loaded_bucket_t *L) {
    if (L->mmap_base && L->mmap_len) {
        munmap(L->mmap_base, L->mmap_len);
    } else if (L->malloc_copy && L->records) {
        free(L->records);
    }
    memset(L, 0, sizeof(*L));
}

static int merge_read_bucket_file(const char *bucket_path, merge_loaded_bucket_t *out) {
    struct stat st;
    int fd;
    void *p;
    trigram_record_t *buf;

    memset(out, 0, sizeof(*out));
    fd = open(bucket_path, O_RDONLY);
    if (fd < 0) return -1;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return -1;
    }
    if (st.st_size == 0 || (st.st_size % (off_t)sizeof(trigram_record_t)) != 0) {
        close(fd);
        return -1;
    }
    out->n = (size_t)(st.st_size / (off_t)sizeof(trigram_record_t));
    out->bytes = (uint64_t)st.st_size;
    p = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p != MAP_FAILED) {
        out->mmap_base = p;
        out->mmap_len = (size_t)st.st_size;
        out->records = (trigram_record_t *)p;
        return 0;
    }
    buf = (trigram_record_t *)malloc((size_t)st.st_size);
    if (!buf) return -1;
    fd = open(bucket_path, O_RDONLY);
    if (fd < 0) {
        free(buf);
        return -1;
    }
    if (read(fd, buf, (size_t)st.st_size) != (ssize_t)st.st_size) {
        close(fd);
        free(buf);
        return -1;
    }
    close(fd);
    out->records = buf;
    out->malloc_copy = 1;
    out->n = (size_t)(st.st_size / (off_t)sizeof(trigram_record_t));
    out->bytes = (uint64_t)st.st_size;
    return 0;
}

static size_t merge_collect_nonempty_from_bitset(build_ctx_t *ctx, uint32_t **out_list) {
    size_t nb = 0;
    uint32_t i;

    for (i = 0; i < TRIGRAM_BUCKET_COUNT; i++) {
        if (ctx->bucket_nonempty[i]) nb++;
    }
    if (nb == 0) return 0;
    *out_list = (uint32_t *)malloc(nb * sizeof(uint32_t));
    if (!*out_list) return 0;
    nb = 0;
    for (i = 0; i < TRIGRAM_BUCKET_COUNT; i++) {
        if (ctx->bucket_nonempty[i]) (*out_list)[nb++] = i;
    }
    return nb;
}

static size_t merge_collect_nonempty_legacy_scan(build_ctx_t *ctx, uint32_t **out_list) {
    char path[PATH_MAX];
    struct stat st;
    size_t nb = 0;
    uint32_t i;

    for (i = 0; i < TRIGRAM_BUCKET_COUNT; i++) {
        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, i) >= (int)sizeof(path)) return 0;
        if (stat(path, &st) != 0) {
            if (errno == ENOENT) continue;
            return 0;
        }
        if (st.st_size == 0) {
            unlink(path);
            continue;
        }
        nb++;
    }
    *out_list = (uint32_t *)malloc(nb * sizeof(uint32_t));
    if (!*out_list) return 0;
    nb = 0;
    for (i = 0; i < TRIGRAM_BUCKET_COUNT; i++) {
        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, i) >= (int)sizeof(path)) {
            free(*out_list);
            *out_list = NULL;
            return 0;
        }
        if (stat(path, &st) != 0) {
            if (errno == ENOENT) continue;
            free(*out_list);
            *out_list = NULL;
            return 0;
        }
        if (st.st_size == 0) {
            unlink(path);
            continue;
        }
        (*out_list)[nb++] = i;
    }
    return nb;
}

typedef struct {
    build_ctx_t *ctx;
    const uint32_t *buckets;
    size_t bucket_count;
    atomic_size_t next;
    atomic_int failed;
    atomic_ullong bytes_in;
    atomic_ullong records_in;
} merge_parallel_arg_t;

static void merge_unlink_segment_pair(build_ctx_t *ctx, uint32_t bucket) {
    char p[PATH_MAX];
    if (snprintf(p, sizeof(p), "%s/merge_seg_k_%04u.bin", ctx->index_dir, bucket) < (int)sizeof(p)) unlink(p);
    if (snprintf(p, sizeof(p), "%s/merge_seg_p_%04u.bin", ctx->index_dir, bucket) < (int)sizeof(p)) unlink(p);
}

static int merge_bucket_to_segment_files(build_ctx_t *ctx, uint32_t bucket, merge_parallel_arg_t *accum) {
    char bucket_path[PATH_MAX];
    char kseg[PATH_MAX];
    char pseg[PATH_MAX];
    merge_loaded_bucket_t L;
    trigram_record_t *aux = NULL;
    FILE *kf = NULL;
    FILE *pf = NULL;
    uint64_t u = 0;
    int rc = -1;

    if (snprintf(bucket_path, sizeof(bucket_path), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(bucket_path)) return -1;
    if (snprintf(kseg, sizeof(kseg), "%s/merge_seg_k_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(kseg)) return -1;
    if (snprintf(pseg, sizeof(pseg), "%s/merge_seg_p_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(pseg)) return -1;

    if (merge_read_bucket_file(bucket_path, &L) != 0) return -1;
    aux = (trigram_record_t *)malloc(L.n * sizeof(*aux));
    if (!aux) {
        merge_loaded_bucket_destroy(&L);
        return -1;
    }
    radix_sort_trigram_records(L.records, L.n, aux);
    free(aux);

    kf = fopen(kseg, "wb");
    pf = fopen(pseg, "wb");
    if (!kf || !pf) goto err;
    if (setvbuf(kf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (setvbuf(pf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    if (write_sorted_bucket_records(kf, pf, L.records, L.n, &u) != 0) goto err;
    if (fclose(kf) != 0) {
        kf = NULL;
        goto err;
    }
    kf = NULL;
    if (fclose(pf) != 0) {
        pf = NULL;
        goto err;
    }
    pf = NULL;
    if (accum) {
        atomic_fetch_add_explicit(&accum->bytes_in, L.bytes, memory_order_relaxed);
        atomic_fetch_add_explicit(&accum->records_in, (unsigned long long)L.n, memory_order_relaxed);
    }
    merge_loaded_bucket_destroy(&L);
    if (unlink(bucket_path) != 0 && errno != ENOENT) return -1;
    return 0;

err:
    if (kf) fclose(kf);
    if (pf) fclose(pf);
    merge_unlink_segment_pair(ctx, bucket);
    merge_loaded_bucket_destroy(&L);
    return rc;
}

static void *merge_parallel_worker(void *arg) {
    merge_parallel_arg_t *p = (merge_parallel_arg_t *)arg;

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->next, 1U, memory_order_relaxed);
        if (i >= p->bucket_count) break;
        if (merge_bucket_to_segment_files(p->ctx, p->buckets[i], p) != 0) atomic_store_explicit(&p->failed, 1, memory_order_relaxed);
    }
    return NULL;
}

static int merge_stitch_segments(build_ctx_t *ctx, const uint32_t *buckets, size_t nb, FILE *keys_fp, FILE *postings_fp,
                                 uint64_t *unique_total) {
    char kpath[PATH_MAX];
    char ppath[PATH_MAX];
    unsigned char *iobuf = NULL;
    uint64_t post_base = 0;
    size_t bi;

    iobuf = (unsigned char *)malloc(MERGE_IO_BUFSIZE);
    if (!iobuf) return -1;
    *unique_total = 0;

    for (bi = 0; bi < nb; bi++) {
        uint32_t b = buckets[bi];
        struct stat sk, sp;
        FILE *kf;
        FILE *pf;
        size_t nk;
        size_t k;

        if (snprintf(kpath, sizeof(kpath), "%s/merge_seg_k_%04u.bin", ctx->index_dir, b) >= (int)sizeof(kpath)) {
            free(iobuf);
            return -1;
        }
        if (snprintf(ppath, sizeof(ppath), "%s/merge_seg_p_%04u.bin", ctx->index_dir, b) >= (int)sizeof(ppath)) {
            free(iobuf);
            return -1;
        }
        if (stat(kpath, &sk) != 0 || stat(ppath, &sp) != 0) {
            free(iobuf);
            return -1;
        }
        if (sk.st_size % (off_t)sizeof(trigram_key_t) != 0) {
            free(iobuf);
            return -1;
        }
        nk = (size_t)(sk.st_size / (off_t)sizeof(trigram_key_t));

        kf = fopen(kpath, "rb");
        pf = fopen(ppath, "rb");
        if (!kf || !pf) {
            if (kf) fclose(kf);
            if (pf) fclose(pf);
            free(iobuf);
            return -1;
        }
        if (setvbuf(kf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
        }
        if (setvbuf(pf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
        }

        for (k = 0; k < nk; k++) {
            trigram_key_t key;
            if (fread(&key, sizeof(key), 1, kf) != 1) {
                fclose(kf);
                fclose(pf);
                free(iobuf);
                return -1;
            }
            key.postings_offset += post_base;
            if (fwrite(&key, sizeof(key), 1, keys_fp) != 1) {
                fclose(kf);
                fclose(pf);
                free(iobuf);
                return -1;
            }
            (*unique_total)++;
        }
        fclose(kf);

        {
            uint64_t psz = (uint64_t)sp.st_size;
            size_t nread;

            for (;;) {
                nread = fread(iobuf, 1, MERGE_IO_BUFSIZE, pf);
                if (nread == 0) break;
                if (fwrite(iobuf, 1, nread, postings_fp) != nread) {
                    fclose(pf);
                    free(iobuf);
                    return -1;
                }
            }
            if (ferror(pf)) {
                fclose(pf);
                free(iobuf);
                return -1;
            }
            post_base += psz;
        }
        fclose(pf);
        unlink(kpath);
        unlink(ppath);
    }

    free(iobuf);
    return 0;
}

static int process_trigram_buckets(build_ctx_t *ctx) {
    char key_path[PATH_MAX], postings_path[PATH_MAX], bucket_path[PATH_MAX];
    FILE *keys_fp = NULL;
    FILE *postings_fp = NULL;
    uint64_t unique_trigrams = 0;
    int rc = -1;
    double merge_wall_start;
    uint32_t merge_skipped = 0;
    uint64_t merge_records_in = 0;
    uint64_t merge_bytes_temp = 0;
    uint32_t *bucket_list = NULL;
    size_t nbuckets = 0;
    size_t bi;
    long ncpu;
    int merge_workers = 1;

    merge_wall_start = now_sec();
    ctx->last_rate_sec = merge_wall_start;
    ctx->last_rate_merge_units = 0;
    ctx->last_status_sec = 0.0;
    ctx->merge_workers_used = 1;

    nbuckets = merge_collect_nonempty_from_bitset(ctx, &bucket_list);
    if (nbuckets == 0) nbuckets = merge_collect_nonempty_legacy_scan(ctx, &bucket_list);
    merge_skipped = (uint32_t)(TRIGRAM_BUCKET_COUNT - nbuckets);

    if (build_path(key_path, sizeof(key_path), ctx->index_dir, "tri_keys.bin") != 0 ||
        build_path(postings_path, sizeof(postings_path), ctx->index_dir, "tri_postings.bin") != 0) {
        free(bucket_list);
        return -1;
    }

    keys_fp = fopen(key_path, "wb");
    postings_fp = fopen(postings_path, "wb");
    if (!keys_fp || !postings_fp) goto out;
    if (setvbuf(keys_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (setvbuf(postings_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (ncpu < 1) ncpu = 4;
    if (ncpu > MERGE_MAX_WORKERS) ncpu = MERGE_MAX_WORKERS;
    merge_workers = (int)ncpu;
    if ((size_t)merge_workers > nbuckets) merge_workers = (int)nbuckets;
    if (nbuckets < (size_t)MERGE_PARALLEL_MIN) merge_workers = 1;
    ctx->merge_workers_used = merge_workers;

    if (nbuckets == 0) {
        unique_trigrams = 0;
        rc = 0;
        goto out;
    }

    if (merge_workers <= 1) {
        for (bi = 0; bi < nbuckets; bi++) {
            uint32_t bucket = bucket_list[bi];
            merge_loaded_bucket_t L;
            trigram_record_t *aux;
            uint64_t u = 0;

            if (snprintf(bucket_path, sizeof(bucket_path), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(bucket_path)) goto out;
            if (merge_read_bucket_file(bucket_path, &L) != 0) goto out;
            merge_bytes_temp += L.bytes;
            merge_records_in += (uint64_t)L.n;
            aux = (trigram_record_t *)malloc(L.n * sizeof(*aux));
            if (!aux) {
                merge_loaded_bucket_destroy(&L);
                goto out;
            }
            radix_sort_trigram_records(L.records, L.n, aux);
            free(aux);
            if (write_sorted_bucket_records(keys_fp, postings_fp, L.records, L.n, &u) != 0) {
                merge_loaded_bucket_destroy(&L);
                goto out;
            }
            unique_trigrams += u;
            merge_loaded_bucket_destroy(&L);
            if (unlink(bucket_path) != 0 && errno != ENOENT) goto out;
            maybe_emit_status(ctx, "merge", (uint64_t)bi + 1U, (uint64_t)nbuckets);
        }
        rc = 0;
    } else {
        merge_parallel_arg_t mp;
        pthread_t *threads = NULL;
        int ti;

        memset(&mp, 0, sizeof(mp));
        mp.ctx = ctx;
        mp.buckets = bucket_list;
        mp.bucket_count = nbuckets;
        atomic_init(&mp.next, 0);
        atomic_init(&mp.failed, 0);
        atomic_init(&mp.bytes_in, 0);
        atomic_init(&mp.records_in, 0);

        threads = (pthread_t *)calloc((size_t)merge_workers, sizeof(*threads));
        if (!threads) goto out;
        for (ti = 0; ti < merge_workers; ti++) {
            if (pthread_create(&threads[ti], NULL, merge_parallel_worker, &mp) != 0) {
                atomic_store(&mp.failed, 1);
                break;
            }
        }
        ctx->merge_workers_used = ti;
        for (ti = 0; ti < ctx->merge_workers_used; ti++) pthread_join(threads[ti], NULL);
        free(threads);

        if (atomic_load(&mp.failed)) {
            for (bi = 0; bi < nbuckets; bi++) merge_unlink_segment_pair(ctx, bucket_list[bi]);
            goto out;
        }

        merge_bytes_temp = (uint64_t)atomic_load(&mp.bytes_in);
        merge_records_in = (uint64_t)atomic_load(&mp.records_in);

        if (merge_stitch_segments(ctx, bucket_list, nbuckets, keys_fp, postings_fp, &unique_trigrams) != 0) goto out;
        rc = 0;
    }

out:
    ctx->unique_trigrams = unique_trigrams;
    ctx->merge_phase_sec = now_sec() - merge_wall_start;
    ctx->merge_buckets_nonempty = (uint32_t)nbuckets;
    ctx->merge_buckets_skipped = merge_skipped;
    ctx->merge_trigram_records_read = merge_records_in;
    ctx->merge_bytes_temp_read = merge_bytes_temp;
    ctx->merge_bytes_tri_keys_written = keys_fp ? (uint64_t)ftello(keys_fp) : 0U;
    ctx->merge_bytes_tri_postings_written = postings_fp ? (uint64_t)ftello(postings_fp) : 0U;
    free(bucket_list);
    if (keys_fp) fclose(keys_fp);
    if (postings_fp) fclose(postings_fp);
    if (rc == 0) return 0;
    return -1;
}

static int build_index_dir(const char *user_spec, const char **dirpaths, size_t dirpath_count) {
    uid_t target_uid;
    char display_name[256];
    char sanitized_name[256];
    char dirs_label[4096];
    char paths_path[PATH_MAX], offsets_path[PATH_MAX];
    char **paths = NULL;
    size_t path_count = 0;
    file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    size_t chunk_cap = 0;
    file_state_t *file_states = NULL;
    work_queue_t queue;
    write_queue_t write_queue;
    pthread_t *tids = NULL;
    worker_arg_t *args = NULL;
    worker_arg_t writer_arg;
    pthread_t stats_thread;
    pthread_t writer_thread;
    build_ctx_t ctx;
    double t0 = now_sec();
    double t1;
    int stats_thread_started = 0;
    int threads = parse_index_thread_count();
    int threads_used = 0;
    size_t i;

    memset(&ctx, 0, sizeof(ctx));
    memset(&queue, 0, sizeof(queue));
    memset(&write_queue, 0, sizeof(write_queue));
    memset(&writer_arg, 0, sizeof(writer_arg));

    if (resolve_target_user(user_spec, &target_uid, display_name, sizeof(display_name)) != 0) {
        fprintf(stderr, "unknown user or uid: %s\n", user_spec);
        return 1;
    }

    snprintf(sanitized_name, sizeof(sanitized_name), "%s", display_name);
    sanitize_name(sanitized_name);
    if (snprintf(ctx.index_dir, sizeof(ctx.index_dir), "%s/index", sanitized_name) >= (int)sizeof(ctx.index_dir)) {
        fprintf(stderr, "index directory path too long\n");
        return 1;
    }
    ctx.target_uid = target_uid;
    snprintf(ctx.display_name, sizeof(ctx.display_name), "%s", display_name);
    ctx.start_sec = t0;
    ctx.last_status_sec = 0.0;
    ctx.last_rate_sec = t0;
    ctx.last_rate_indexed_paths = 0;
    ctx.last_rate_merge_units = 0;
    atomic_store(&g_progress_scanned_input_files, 0);
    atomic_store(&g_progress_scanned_records, 0);
    atomic_store(&g_progress_indexed_paths, 0);
    atomic_store(&g_progress_trigram_records, 0);
    atomic_store(&g_progress_bad_input_files, 0);
    atomic_store(&g_stop_stats, 0);
    atomic_store(&g_writer_failed, 0);
    atomic_store(&g_writeq_writer_waits, 0);
    g_run_start_sec = t0;

    if (scan_dirs_collect_files(dirpaths, dirpath_count, target_uid, &paths, &path_count) != 0) return 1;

    dirs_label[0] = '\0';
    if (dirpath_count == 1) {
        snprintf(dirs_label, sizeof(dirs_label), "%s", dirpaths[0]);
    } else {
        size_t k, pos = 0;
        for (k = 0; k < dirpath_count && pos + 1 < sizeof(dirs_label); k++) {
            int w = snprintf(dirs_label + pos, sizeof(dirs_label) - pos, "%s%s", k ? ";" : "", dirpaths[k]);
            if (w < 0 || (size_t)w >= sizeof(dirs_label) - pos) {
                snprintf(dirs_label, sizeof(dirs_label), "%s;… (%zu dirs)", dirpaths[0], dirpath_count);
                break;
            }
            pos += (size_t)w;
        }
    }

    if (path_count == 0) {
        fprintf(stderr, "no matching input .bin files under %s\n", dirs_label);
        free(paths);
        return 1;
    }

    file_states = (file_state_t *)calloc(path_count, sizeof(*file_states));
    if (!file_states) {
        fprintf(stderr, "allocation failed\n");
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        return 1;
    }

    for (i = 0; i < path_count; i++) {
        struct stat st;
        unsigned int file_chunk_counter = 0;
        uint64_t chunk_target = PARSE_CHUNK_BYTES;
        if (stat(paths[i], &st) == 0 && S_ISREG(st.st_mode))
            chunk_target = compute_parse_chunk_target((uint64_t)st.st_size, threads);
        if (build_chunks_for_file(paths[i], i, chunk_target, &chunks, &chunk_count, &chunk_cap, &file_chunk_counter) == 0) {
            atomic_store(&file_states[i].remaining_chunks, file_chunk_counter);
            ctx.input_files++;
        } else {
            atomic_store(&file_states[i].remaining_chunks, 0);
            ctx.bad_input_files++;
        }
    }
    atomic_store(&g_progress_bad_input_files, ctx.bad_input_files);
    g_progress_input_files_total = ctx.input_files;

    if (chunk_count == 0) {
        fprintf(stderr, "no readable chunk work found in %s\n", dirs_label);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        free(file_states);
        free(chunks);
        return 1;
    }

    if (ensure_dir_recursive(sanitized_name) != 0 || ensure_dir_recursive(ctx.index_dir) != 0) {
        fprintf(stderr, "failed to create %s: %s\n", ctx.index_dir, strerror(errno));
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    if (build_path(paths_path, sizeof(paths_path), ctx.index_dir, "paths.bin") != 0 ||
        build_path(offsets_path, sizeof(offsets_path), ctx.index_dir, "path_offsets.bin") != 0) {
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    ctx.paths_fp = fopen(paths_path, "wb");
    ctx.path_offsets_fp = fopen(offsets_path, "wb");
    if (ctx.paths_fp && setvbuf(ctx.paths_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (ctx.path_offsets_fp && setvbuf(ctx.path_offsets_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (!ctx.paths_fp || !ctx.path_offsets_fp || bucket_cache_init(&ctx.bucket_cache, ctx.index_dir) != 0) {
        fprintf(stderr, "failed to initialize index outputs in %s\n", ctx.index_dir);
        if (ctx.paths_fp) fclose(ctx.paths_fp);
        if (ctx.path_offsets_fp) fclose(ctx.path_offsets_fp);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    queue.chunks = chunks;
    queue.count = chunk_count;
    queue.next_index = 0;
    pthread_mutex_init(&queue.mutex, NULL);
    pthread_mutex_init(&write_queue.mutex, NULL);
    pthread_cond_init(&write_queue.cond, NULL);

    threads_used = threads;
    tids = (pthread_t *)calloc((size_t)threads, sizeof(*tids));
    args = (worker_arg_t *)calloc((size_t)threads, sizeof(*args));
    if (!tids || !args) {
        fprintf(stderr, "allocation failed\n");
        free(tids);
        free(args);
        bucket_cache_close_all(&ctx.bucket_cache);
        fclose(ctx.paths_fp);
        fclose(ctx.path_offsets_fp);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.cond);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    writer_arg.write_queue = &write_queue;
    writer_arg.ctx = &ctx;
    if (pthread_create(&writer_thread, NULL, writer_main, &writer_arg) != 0) {
        fprintf(stderr, "failed to create writer thread\n");
        free(tids);
        free(args);
        bucket_cache_close_all(&ctx.bucket_cache);
        fclose(ctx.paths_fp);
        fclose(ctx.path_offsets_fp);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.cond);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    if (pthread_create(&stats_thread, NULL, stats_thread_main, NULL) != 0) {
        fprintf(stderr, "failed to create stats thread\n");
        write_queue_close(&write_queue);
        pthread_join(writer_thread, NULL);
        free(tids);
        free(args);
        bucket_cache_close_all(&ctx.bucket_cache);
        fclose(ctx.paths_fp);
        fclose(ctx.path_offsets_fp);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.cond);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }
    stats_thread_started = 1;

    for (i = 0; i < (size_t)threads; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].queue = &queue;
        args[i].write_queue = &write_queue;
        args[i].file_states = file_states;
        args[i].ctx = &ctx;
        if (pthread_create(&tids[i], NULL, worker_main, &args[i]) != 0) {
            fprintf(stderr, "failed to create worker %zu\n", i);
            threads_used = (int)i;
            break;
        }
    }

    for (i = 0; i < (size_t)threads_used; i++) {
        pthread_join(tids[i], NULL);
    }
    write_queue_close(&write_queue);
    pthread_join(writer_thread, NULL);

    if (stats_thread_started) {
        atomic_store(&g_stop_stats, 1);
        pthread_join(stats_thread, NULL);
        clear_status_line();
    }

    ctx.scanned_records = atomic_load(&g_progress_scanned_records);
    ctx.indexed_paths = atomic_load(&g_progress_indexed_paths);
    ctx.trigram_records = atomic_load(&g_progress_trigram_records);
    ctx.bad_input_files = atomic_load(&g_progress_bad_input_files);
    if (atomic_load(&g_writer_failed)) {
        fprintf(stderr, "writer thread failed while building %s\n", ctx.index_dir);
        free(tids);
        free(args);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.cond);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        bucket_cache_close_all(&ctx.bucket_cache);
        fclose(ctx.paths_fp);
        fclose(ctx.path_offsets_fp);
        return 1;
    }

    bucket_cache_close_all(&ctx.bucket_cache);

    if (write_final_path_offset(&ctx) != 0 || fclose(ctx.paths_fp) != 0 || fclose(ctx.path_offsets_fp) != 0) {
        fprintf(stderr, "failed to finalize paths in %s\n", ctx.index_dir);
        free(tids);
        free(args);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.cond);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    ctx.index_phase_sec = now_sec() - t0;
    ctx.index_workers_used = threads_used;

    if (process_trigram_buckets(&ctx) != 0 || write_meta_file(&ctx) != 0) {
        fprintf(stderr, "failed to finalize index in %s\n", ctx.index_dir);
        free(tids);
        free(args);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.cond);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    t1 = now_sec();
    clear_status_line();
    printf("mode=make\n");
    printf("user=%s\n", ctx.display_name);
    printf("uid=%lu\n", (unsigned long)ctx.target_uid);
    printf("input_dir=%s\n", dirs_label);
    printf("input_layout=%s\n", g_input_layout);
    if (g_input_uid_shards) printf("input_uid_shards=%u\n", g_input_uid_shards);
    printf("input_files=%" PRIu64 "\n", ctx.input_files);
    printf("scanned_records=%" PRIu64 "\n", ctx.scanned_records);
    printf("index_dir=%s\n", ctx.index_dir);
    printf("indexed_paths=%" PRIu64 "\n", ctx.indexed_paths);
    printf("trigram_records=%" PRIu64 "\n", ctx.trigram_records);
    printf("unique_trigrams=%" PRIu64 "\n", ctx.unique_trigrams);
    printf("bad_input_files=%" PRIu64 "\n", ctx.bad_input_files);
    printf("index_phase_sec=%.3f\n", ctx.index_phase_sec);
    printf("index_workers=%d\n", ctx.index_workers_used);
    {
        char ips_buf[32];
        double ips = ctx.index_phase_sec > 0.0 ? (double)ctx.indexed_paths / ctx.index_phase_sec : 0.0;
        human_decimal(ips, ips_buf, sizeof(ips_buf));
        printf("index_paths_per_sec=%s\n", ips_buf);
    }
    printf("merge_phase_sec=%.3f\n", ctx.merge_phase_sec);
    printf("merge_buckets_nonempty=%u\n", ctx.merge_buckets_nonempty);
    printf("merge_buckets_skipped=%u\n", ctx.merge_buckets_skipped);
    printf("merge_trigram_records_read=%" PRIu64 "\n", ctx.merge_trigram_records_read);
    printf("merge_workers=%d\n", ctx.merge_workers_used);
    printf("merge_bytes_temp_read=%" PRIu64 "\n", ctx.merge_bytes_temp_read);
    printf("merge_bytes_tri_keys_written=%" PRIu64 "\n", ctx.merge_bytes_tri_keys_written);
    printf("merge_bytes_tri_postings_written=%" PRIu64 "\n", ctx.merge_bytes_tri_postings_written);
    {
        char merge_bkt_buf[32], merge_in_buf[32], merge_out_buf[32];
        double merge_bkt_s = ctx.merge_phase_sec > 0.0 ? (double)ctx.merge_buckets_nonempty / ctx.merge_phase_sec : 0.0;
        double merge_in_Bps = ctx.merge_phase_sec > 0.0 ? (double)ctx.merge_bytes_temp_read / ctx.merge_phase_sec : 0.0;
        uint64_t merge_out_bytes = ctx.merge_bytes_tri_keys_written + ctx.merge_bytes_tri_postings_written;
        double merge_out_Bps = ctx.merge_phase_sec > 0.0 ? (double)merge_out_bytes / ctx.merge_phase_sec : 0.0;
        human_decimal(merge_bkt_s, merge_bkt_buf, sizeof(merge_bkt_buf));
        human_decimal(merge_in_Bps, merge_in_buf, sizeof(merge_in_buf));
        human_decimal(merge_out_Bps, merge_out_buf, sizeof(merge_out_buf));
        printf("merge_nonempty_buckets_per_sec=%s\n", merge_bkt_buf);
        printf("merge_temp_read_bytes_per_sec=%s\n", merge_in_buf);
        printf("merge_output_bytes_per_sec=%s\n", merge_out_buf);
    }
    {
        char avg_paths_buf[32];
        double avg_paths = (t1 > t0) ? (double)ctx.indexed_paths / (t1 - t0) : 0.0;
        human_decimal(avg_paths, avg_paths_buf, sizeof(avg_paths_buf));
    printf("avg_paths_per_sec=%s\n", avg_paths_buf);
    }
    printf("writeq_writer_waits=%" PRIu64 "\n", (uint64_t)atomic_load(&g_writeq_writer_waits));
    printf("wall_after_index_sec=%.3f\n", (t1 - t0) - ctx.index_phase_sec);
    printf("elapsed_sec=%.3f\n", t1 - t0);

    free(tids);
    free(args);
    pthread_mutex_destroy(&queue.mutex);
    pthread_mutex_destroy(&write_queue.mutex);
    pthread_cond_destroy(&write_queue.cond);
    for (i = 0; i < path_count; i++) free(paths[i]);
    free(paths);
    for (i = 0; i < chunk_count; i++) free(chunks[i].path);
    free(chunks);
    free(file_states);
    return 0;
}

static int load_key_at(FILE *fp, uint64_t idx, trigram_key_t *out) {
    if (fseeko(fp, (off_t)(idx * (uint64_t)sizeof(*out)), SEEK_SET) != 0) return -1;
    return fread(out, sizeof(*out), 1, fp) == 1 ? 0 : -1;
}

static int find_trigram_key(FILE *fp, uint64_t key_count, uint32_t trigram, trigram_key_t *out) {
    uint64_t lo = 0, hi = key_count;
    while (lo < hi) {
        uint64_t mid = lo + (hi - lo) / 2;
        trigram_key_t key;
        if (load_key_at(fp, mid, &key) != 0) return -1;
        if (key.trigram < trigram) lo = mid + 1;
        else hi = mid;
    }
    if (lo >= key_count) return 1;
    if (load_key_at(fp, lo, out) != 0) return -1;
    return out->trigram == trigram ? 0 : 1;
}

static int load_postings_list(FILE *postings_fp, const trigram_key_t *key, u64_vec_t *vec) {
    unsigned char *buf;
    size_t pos = 0;
    uint64_t prev = 0;

    memset(vec, 0, sizeof(*vec));
    if (key->postings_bytes == 0) return 0;

    buf = (unsigned char *)malloc((size_t)key->postings_bytes);
    if (!buf) return -1;
    if (fseeko(postings_fp, (off_t)key->postings_offset, SEEK_SET) != 0 ||
        fread(buf, 1, (size_t)key->postings_bytes, postings_fp) != (size_t)key->postings_bytes) {
        free(buf);
        return -1;
    }

    while (pos < (size_t)key->postings_bytes) {
        uint64_t delta;
        uint64_t path_id;
        if (decode_varint_u64_buf(buf, (size_t)key->postings_bytes, &pos, &delta) != 0) {
            free(buf);
            free(vec->ids);
            vec->ids = NULL;
            vec->count = vec->cap = 0;
            return -1;
        }
        path_id = (prev == 0) ? (delta - 1U) : (prev + delta);
        if (u64_vec_push(vec, path_id) != 0) {
            free(buf);
            free(vec->ids);
            vec->ids = NULL;
            vec->count = vec->cap = 0;
            return -1;
        }
        prev = path_id;
    }

    free(buf);
    return 0;
}

static int intersect_postings(const u64_vec_t *a, const u64_vec_t *b, u64_vec_t *out) {
    size_t i = 0, j = 0;
    memset(out, 0, sizeof(*out));

    while (i < a->count && j < b->count) {
        if (a->ids[i] == b->ids[j]) {
            if (u64_vec_push(out, a->ids[i]) != 0) {
                free(out->ids);
                out->ids = NULL;
                out->count = out->cap = 0;
                return -1;
            }
            i++;
            j++;
        } else if (a->ids[i] < b->ids[j]) {
            i++;
        } else {
            j++;
        }
    }

    return 0;
}

static int read_path_offsets(FILE *fp, uint64_t path_id, uint64_t *off0, uint64_t *off1) {
    if (fseeko(fp, (off_t)(path_id * sizeof(uint64_t)), SEEK_SET) != 0) return -1;
    if (fread(off0, sizeof(*off0), 1, fp) != 1) return -1;
    if (fread(off1, sizeof(*off1), 1, fp) != 1) return -1;
    return 0;
}

static char *read_path_by_id(FILE *paths_fp, FILE *offsets_fp, uint64_t path_id) {
    uint64_t off0, off1;
    size_t len;
    char *buf;

    if (read_path_offsets(offsets_fp, path_id, &off0, &off1) != 0) return NULL;
    if (off1 < off0) return NULL;
    len = (size_t)(off1 - off0);
    if (len == 0) return NULL;

    buf = (char *)malloc(len);
    if (!buf) return NULL;
    if (fseeko(paths_fp, (off_t)off0, SEEK_SET) != 0 ||
        fread(buf, 1, len, paths_fp) != len) {
        free(buf);
        return NULL;
    }

    return buf;
}

static int collect_query_trigrams(const char *term, uint32_t **out_codes, size_t *out_count) {
    char *lower = ascii_lower_dup(term);
    uint32_t *codes = NULL;
    size_t count = 0, cap = 0;
    size_t len, i;

    if (!lower) return -1;
    len = strlen(lower);
    if (len < 3) {
        free(lower);
        fprintf(stderr, "search term must be at least 3 characters\n");
        return -1;
    }

    for (i = 0; i + 3 <= len; i++) {
        uint32_t trigram = ((uint32_t)(unsigned char)lower[i] << 16) |
                           ((uint32_t)(unsigned char)lower[i + 1] << 8) |
                           (uint32_t)(unsigned char)lower[i + 2];
        if (append_unique_trigram(&codes, &count, &cap, trigram) != 0) {
            free(lower);
            free(codes);
            return -1;
        }
    }
    free(lower);

    qsort(codes, count, sizeof(*codes), cmp_u32);
    if (count > 1) {
        size_t out_i = 1;
        for (i = 1; i < count; i++) {
            if (codes[i] != codes[out_i - 1]) codes[out_i++] = codes[i];
        }
        count = out_i;
    }

    *out_codes = codes;
    *out_count = count;
    return 0;
}

static int cmp_vec_count_asc(const void *a, const void *b) {
    const u64_vec_t *aa = (const u64_vec_t *)a;
    const u64_vec_t *bb = (const u64_vec_t *)b;
    if (aa->count < bb->count) return -1;
    if (aa->count > bb->count) return 1;
    return 0;
}

static void json_escape_stdout(FILE *out, const char *s) {
    fputc('"', out);
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        switch (c) {
            case '"':
                fputs("\\\"", out);
                break;
            case '\\':
                fputs("\\\\", out);
                break;
            case '\b':
                fputs("\\b", out);
                break;
            case '\f':
                fputs("\\f", out);
                break;
            case '\n':
                fputs("\\n", out);
                break;
            case '\r':
                fputs("\\r", out);
                break;
            case '\t':
                fputs("\\t", out);
                break;
            default:
                if (c < 0x20u)
                    fprintf(out, "\\u%04x", c);
                else
                    fputc((char)c, out);
                break;
        }
    }
    fputc('"', out);
}

static int search_index_dir(const char *term, const char *index_dir, uint64_t skip_req, uint64_t limit_req,
                            int json_output) {
    char keys_path[PATH_MAX], postings_path[PATH_MAX], paths_path[PATH_MAX], offsets_path[PATH_MAX];
    FILE *keys_fp = NULL, *postings_fp = NULL, *paths_fp = NULL, *offsets_fp = NULL;
    struct stat st;
    uint64_t key_count;
    uint32_t *query_trigrams = NULL;
    size_t query_trigram_count = 0;
    u64_vec_t *lists = NULL;
    u64_vec_t current, next;
    char *lower_term = NULL;
    int rc = 1;

    if (build_path(keys_path, sizeof(keys_path), index_dir, "tri_keys.bin") != 0 ||
        build_path(postings_path, sizeof(postings_path), index_dir, "tri_postings.bin") != 0 ||
        build_path(paths_path, sizeof(paths_path), index_dir, "paths.bin") != 0 ||
        build_path(offsets_path, sizeof(offsets_path), index_dir, "path_offsets.bin") != 0) {
        return 1;
    }

    keys_fp = fopen(keys_path, "rb");
    postings_fp = fopen(postings_path, "rb");
    paths_fp = fopen(paths_path, "rb");
    offsets_fp = fopen(offsets_path, "rb");
    if (!keys_fp || !postings_fp || !paths_fp || !offsets_fp) {
        fprintf(stderr, "cannot open index under %s\n", index_dir);
        goto out;
    }

    if (stat(keys_path, &st) != 0 || (st.st_size % (off_t)sizeof(trigram_key_t)) != 0) {
        fprintf(stderr, "invalid tri_keys.bin in %s\n", index_dir);
        goto out;
    }
    key_count = (uint64_t)(st.st_size / (off_t)sizeof(trigram_key_t));

    if (collect_query_trigrams(term, &query_trigrams, &query_trigram_count) != 0) goto out;
    lower_term = ascii_lower_dup(term);
    if (!lower_term) goto out;

    lists = (u64_vec_t *)calloc(query_trigram_count, sizeof(*lists));
    if (!lists) goto out;

    for (size_t i = 0; i < query_trigram_count; i++) {
        trigram_key_t key;
        int found = find_trigram_key(keys_fp, key_count, query_trigrams[i], &key);
        if (found != 0) {
            rc = found < 0 ? 1 : 0;
            goto out;
        }
        if (load_postings_list(postings_fp, &key, &lists[i]) != 0) goto out;
    }

    qsort(lists, query_trigram_count, sizeof(*lists), cmp_vec_count_asc);
    memset(&current, 0, sizeof(current));
    memset(&next, 0, sizeof(next));

    if (query_trigram_count == 0) {
        rc = 0;
        goto out;
    }

    current.ids = lists[0].ids;
    current.count = lists[0].count;
    current.cap = lists[0].count;
    lists[0].ids = NULL;
    lists[0].count = lists[0].cap = 0;

    for (size_t i = 1; i < query_trigram_count; i++) {
        if (intersect_postings(&current, &lists[i], &next) != 0) {
            free(current.ids);
            current.ids = NULL;
            goto out;
        }
        free(current.ids);
        current = next;
        memset(&next, 0, sizeof(next));
        if (current.count == 0) break;
    }

    {
        uint64_t max_emit = json_output ? limit_req : UINT64_MAX;
        uint64_t match_idx = 0;
        uint64_t page_emitted = 0;
        char **json_paths = NULL;
        size_t json_cap = 0;
        size_t ki;

        for (size_t i = 0; i < current.count; i++) {
            char *path = read_path_by_id(paths_fp, offsets_fp, current.ids[i]);
            if (!path) continue;
            if (!path_matches_term(path, lower_term)) {
                free(path);
                continue;
            }
            if (match_idx >= skip_req && page_emitted < max_emit) {
                if (json_output) {
                    if (page_emitted >= json_cap) {
                        size_t nc = json_cap ? json_cap * 2 : 64;
                        char **np = (char **)realloc(json_paths, nc * sizeof(char *));
                        if (!np) {
                            free(path);
                            for (ki = 0; ki < page_emitted; ki++) free(json_paths[ki]);
                            free(json_paths);
                            free(current.ids);
                            current.ids = NULL;
                            goto out;
                        }
                        json_paths = np;
                        json_cap = nc;
                    }
                    json_paths[page_emitted++] = path;
                } else {
                    printf("%s\n", path);
                    free(path);
                    page_emitted++;
                }
            } else {
                free(path);
            }
            match_idx++;
        }

        if (json_output) {
            fprintf(stdout,
                    "{\"total\":%" PRIu64 ",\"skip\":%" PRIu64 ",\"limit\":%" PRIu64 ",\"paths\":[",
                    match_idx,
                    skip_req,
                    limit_req);
            for (ki = 0; ki < page_emitted; ki++) {
                if (ki) fputc(',', stdout);
                json_escape_stdout(stdout, json_paths[ki]);
                free(json_paths[ki]);
            }
            fputs("]}\n", stdout);
            free(json_paths);
        }
    }

    free(current.ids);
    current.ids = NULL;
    rc = 0;

out:
    if (lists) {
        for (size_t i = 0; i < query_trigram_count; i++) free(lists[i].ids);
        free(lists);
    }
    free(query_trigrams);
    free(lower_term);
    if (keys_fp) fclose(keys_fp);
    if (postings_fp) fclose(postings_fp);
    if (paths_fp) fclose(paths_fp);
    if (offsets_fp) fclose(offsets_fp);
    return rc;
}

int main(int argc, char **argv) {
    if (argc < 2) die_usage(argv[0]);

    if (strcmp(argv[1], "--make") == 0) {
        const char **dirpaths;
        size_t dirpath_count;
        if (argc < 3) die_usage(argv[0]);
        if (argc == 3) {
            static const char *dot = ".";
            dirpaths = &dot;
            dirpath_count = 1;
        } else {
            dirpaths = (const char **)(argv + 3);
            dirpath_count = (size_t)(argc - 3);
        }
        return build_index_dir(argv[2], dirpaths, dirpath_count);
    }

    if (strcmp(argv[1], "--search") == 0) {
        const char *index_dir = "index";
        uint64_t skip_req = 0;
        uint64_t limit_req = UINT64_MAX;
        int json_output = 0;
        int ai;

        if (argc < 3) die_usage(argv[0]);

        ai = 3;
        if (argc > 3 && argv[3][0] != '-') {
            index_dir = argv[3];
            ai = 4;
        }

        while (ai < argc) {
            if (strcmp(argv[ai], "--json") == 0) {
                json_output = 1;
                ai++;
                continue;
            }
            if (strcmp(argv[ai], "--skip") == 0 && ai + 1 < argc) {
                skip_req = strtoull(argv[ai + 1], NULL, 10);
                ai += 2;
                continue;
            }
            if (strcmp(argv[ai], "--limit") == 0 && ai + 1 < argc) {
                limit_req = strtoull(argv[ai + 1], NULL, 10);
                ai += 2;
                continue;
            }
            die_usage(argv[0]);
        }

        if (json_output) {
            if (limit_req == UINT64_MAX) limit_req = 50;
            if (limit_req == 0) {
                fprintf(stderr, "ereport_index: --limit must be > 0 with --json\n");
                return 2;
            }
        }

        return search_index_dir(argv[2], index_dir, skip_req, limit_req, json_output);
    }

    die_usage(argv[0]);
    return 2;
}
