/*
 * ereport_index — trigram path index for ereport HTML search (eserve).
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

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
#include <sys/resource.h>
#include <time.h>

#include "crawl_ckpt.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define FILE_MAGIC_LEN 8
#define FORMAT_VERSION 3
#define INDEX_VERSION 1
#define TRIGRAM_BUCKET_BITS 12
#define TRIGRAM_BUCKET_COUNT (1U << TRIGRAM_BUCKET_BITS)
#define DEFAULT_THREADS 32
/* LRU ceiling per trigram worker when EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS is unset. */
#define DEFAULT_MAX_OPEN_TRIGRAM_BUCKET_FP 4096
/*
 * Per-process descriptor budget assumed when splitting tmp_trigram FILE*s across workers (no runtime probe).
 * Large `--make` runs should use `ulimit -n 65535` or higher — see README.
 */
#define EREPORT_INDEX_ASSUMED_ULIMIT_NOFILE 65535U
#define EREPORT_INDEX_RESERVED_FD_NON_TRIGRAM 1536U
#define PARSE_CHUNK_BYTES (32ULL << 20)
#define PARSE_CHUNK_MIN_BYTES (1ULL << 20)
#define WRITE_BATCH_PATHS 4096
/* Default pending trigram jobs when EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH is unset: scales with trigram workers. */
#define TRIGRAM_JOB_QUEUE_DEPTH_MIN 4096
#define TRIGRAM_JOB_QUEUE_DEPTH_MAX_DEFAULT 16384
#define TRIGRAM_JOB_QUEUE_DEPTH_PER_WORKER 64

static size_t g_write_batch_paths_base = WRITE_BATCH_PATHS;
/* Like ecrawl/ereport: default output is a concise key=value summary; --verbose enables live detail and extra metrics. */
static int g_verbose = 0;
#define MEMLOG_INTERVAL_SEC 8
#define MERGE_IO_BUFSIZE (1U << 20)
#define MERGE_MAX_WORKERS 16
#define MERGE_PARALLEL_MIN 4
/* Each parallel merge worker holds ~2× bucket file bytes (mmap + radix aux) plus stdio buffers — cap workers to avoid OOM. */
#define MERGE_PER_WORKER_OVERHEAD_BYTES (16ULL << 20)
/* Fraction of min(MemAvailable, cgroup memory.max) used as merge parallelism budget. 55% lets ~2 workers
 * run when max bucket ≈50 GiB and the host has ~360+ GiB MemAvailable; 45% often capped to 1 worker. */
#define MERGE_RAM_FRAC_NUM 55U
#define MERGE_RAM_FRAC_DEN 100U
/*
 * Publish scanned_records to the stats thread this often while a chunk is in flight.
 * Without this, a single large chunk can run for a long time with no visible rec: progress.
 */
#define SCANNED_RECORDS_PUBLISH_STRIDE 65536U

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
    size_t approx_body_bytes; /* path strings + trigram codes; excludes batch struct overhead */
    struct write_batch *next;
} write_batch_t;

/* Per-`--make` run: live on the stack in `build_index_dir`, passed by pointer (no file-scope atomics). */
typedef struct index_run_stats {
    atomic_ullong scanned_input_files;
    atomic_ullong scanned_records;
    atomic_ullong indexed_paths;
    atomic_ullong trigram_records;
    atomic_ullong bad_input_files;
    atomic_int stop_stats;
    atomic_int writer_failed;
    uint64_t input_files_total;
    uint64_t chunk_prep_files_total;
    atomic_ullong chunk_prep_files_done;
    atomic_ullong writeq_writer_waits;
    atomic_ullong writeq_parse_waits;
    atomic_ullong trigramq_paths_waits;
    atomic_ullong trigramq_worker_waits;
    double run_start_sec;
    uint64_t stats_prev_indexed_paths; /* paths/s line in status thread only */
    atomic_ullong chunks_index_done; /* parse workers: completed chunk tasks */
    uint64_t index_chunks_total;     /* set when index phase starts; 0 before that */
    atomic_int stats_wake;           /* main sets 1 so stats thread prints without waiting full 1s */
} index_run_stats_t;

static void index_run_stats_reset(index_run_stats_t *s) {
    atomic_store(&s->scanned_input_files, 0);
    atomic_store(&s->scanned_records, 0);
    atomic_store(&s->indexed_paths, 0);
    atomic_store(&s->trigram_records, 0);
    atomic_store(&s->bad_input_files, 0);
    atomic_store(&s->stop_stats, 0);
    atomic_store(&s->writer_failed, 0);
    s->input_files_total = 0;
    s->chunk_prep_files_total = 0;
    atomic_store(&s->chunk_prep_files_done, 0);
    atomic_store(&s->writeq_writer_waits, 0);
    atomic_store(&s->writeq_parse_waits, 0);
    atomic_store(&s->trigramq_paths_waits, 0);
    atomic_store(&s->trigramq_worker_waits, 0);
    s->run_start_sec = 0.0;
    s->stats_prev_indexed_paths = 0;
    atomic_store(&s->chunks_index_done, 0);
    s->index_chunks_total = 0;
    atomic_store(&s->stats_wake, 0);
}

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t has_batch; /* writer waits for work */
    pthread_cond_t has_space; /* producers wait when queue is full */
    write_batch_t *head;
    write_batch_t *tail;
    size_t depth;
    size_t max_depth;
    uint64_t queued_body_bytes; /* sum of batch->approx_body_bytes in queue */
    int closed;
    index_run_stats_t *run_stats;
} write_queue_t;

typedef struct trigram_job {
    uint64_t path_id;
    uint32_t *codes;
    size_t code_count;
    size_t approx_body_bytes; /* sizeof job + codes array */
    struct trigram_job *next;
} trigram_job_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t has_job;
    pthread_cond_t has_space;
    trigram_job_t *head;
    trigram_job_t *tail;
    size_t depth;
    size_t max_depth;
    uint64_t queued_body_bytes;
    int closed;
    index_run_stats_t *run_stats;
} trigram_job_queue_t;

typedef struct {
    atomic_uint remaining_chunks;
} file_state_t;

typedef struct {
    uid_t target_uid;
    int aggregate_all_users;
    char display_name[256];
    char index_dir[PATH_MAX];
    FILE *paths_fp;
    FILE *path_offsets_fp;
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
    /* Parallel trigram writers use disjoint tmp files per (bucket, worker) — no cross-thread FILE/mutex. */
    uint32_t trigram_tmp_shard_count;
    FILE **tw_worker_fp; /* [trigram_tmp_shard_count * TRIGRAM_BUCKET_COUNT] */
    uint64_t *tw_worker_lru_age;
    uint32_t *tw_worker_open_count; /* [trigram_tmp_shard_count] */
    uint32_t tw_worker_max_open;
    uint64_t *tw_worker_lru_next_tick; /* [trigram_tmp_shard_count] — per-worker LRU clock (no atomics). */
    index_run_stats_t *run_stats;
    int merge_workers_used;
    int merge_workers_cpu; /* before memory cap */
    uint64_t merge_max_bucket_bytes;
    uint64_t merge_parallel_budget_bytes;
    double index_phase_sec;
    int index_workers_used;
    int trigram_writer_workers_used;
    uint64_t chunk_path_bytes_total; /* sum strlen(chunk.path); for backlog estimate */
    size_t chunk_total_count;
    atomic_uint_fast64_t merge_bucket_ram_peak; /* ~2× largest bucket file during merge */
} build_ctx_t;

typedef struct {
    work_queue_t *queue;
    write_queue_t *write_queue;
    file_state_t *file_states;
    build_ctx_t *ctx;
    size_t write_batch_flush_at;
    char *lower_seg_buf;
    size_t lower_seg_cap;
} worker_arg_t;

typedef struct {
    write_queue_t *write_queue;
    trigram_job_queue_t *trigram_queue;
    build_ctx_t *ctx;
} paths_writer_arg_t;

typedef struct {
    trigram_job_queue_t *trigram_queue;
    build_ctx_t *ctx;
    uint32_t worker_index;
} trigram_worker_arg_t;

typedef struct {
    uint64_t *ids;
    size_t count;
    size_t cap;
} u64_vec_t;

/* Manifest-driven crawl directory layout: "uid_shards" or "unsharded" (no uid-shard manifest). */
static const char *g_input_layout = "unsharded";
static uint32_t g_input_uid_shards = 0;

static double rusage_timeval_sec(const struct timeval *tv) {
    return (double)tv->tv_sec + (double)tv->tv_usec / 1000000.0;
}

/* Phase CPU from getrusage(RUSAGE_SELF): sums all threads (Linux). No hot-path cost — only called at phase boundaries. */
static void rusage_print_delta(const char *prefix, const struct rusage *later, const struct rusage *earlier) {
    double u = rusage_timeval_sec(&later->ru_utime) - rusage_timeval_sec(&earlier->ru_utime);
    double s = rusage_timeval_sec(&later->ru_stime) - rusage_timeval_sec(&earlier->ru_stime);
    double tot = u + s;
    long nvc = (long)((long)later->ru_nvcsw - (long)earlier->ru_nvcsw);
    long niv = (long)((long)later->ru_nivcsw - (long)earlier->ru_nivcsw);
    long minf = (long)((long)later->ru_minflt - (long)earlier->ru_minflt);
    long majf = (long)((long)later->ru_majflt - (long)earlier->ru_majflt);

    printf("%s_cpu_user_sec=%.3f\n", prefix, u);
    printf("%s_cpu_sys_sec=%.3f\n", prefix, s);
    if (tot > 1e-9) printf("%s_cpu_sys_frac=%.4f\n", prefix, s / tot);
    printf("%s_ctx_sw_vol=%ld\n", prefix, nvc);
    printf("%s_ctx_sw_inv=%ld\n", prefix, niv);
    printf("%s_pf_minor=%ld\n", prefix, minf);
    printf("%s_pf_major=%ld\n", prefix, majf);
}

/* Per-`--make` stdio/POSIX I/O + bucket-lock stats (tune by reducing calls, batching writes, spreading buckets). */
static atomic_ullong g_mk_fread_calls;
static atomic_ullong g_mk_fread_bytes;
static atomic_ullong g_mk_fwrite_calls;
static atomic_ullong g_mk_fwrite_bytes;
static atomic_ullong g_mk_fopen_calls;
static atomic_ullong g_mk_fclose_calls;
static atomic_ullong g_mk_open_calls;
static atomic_ullong g_mk_read_calls;
static atomic_ullong g_mk_read_bytes;
static atomic_ullong g_mk_mmap_calls;
static atomic_ullong g_mk_munmap_calls;
static atomic_ullong g_mk_trigram_append_batches;

typedef struct {
    unsigned long long fread_calls;
    unsigned long long fread_bytes;
    unsigned long long fwrite_calls;
    unsigned long long fwrite_bytes;
    unsigned long long fopen_calls;
    unsigned long long fclose_calls;
    unsigned long long open_calls;
    unsigned long long read_calls;
    unsigned long long read_bytes;
    unsigned long long mmap_calls;
    unsigned long long munmap_calls;
    unsigned long long trigram_append_batches;
} mk_io_tls_t;

static _Thread_local mk_io_tls_t mk_io_tls;

static void make_io_reset(void) {
    atomic_store_explicit(&g_mk_fread_calls, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_fread_bytes, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_fwrite_calls, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_fwrite_bytes, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_fopen_calls, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_fclose_calls, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_open_calls, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_read_calls, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_read_bytes, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_mmap_calls, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_munmap_calls, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_mk_trigram_append_batches, 0ULL, memory_order_relaxed);
}

/* Fold thread-local make I/O counters into globals (call at thread exit and on main before printing stats). */
static void mk_io_tls_flush(void) {
    if (!g_verbose) {
        memset(&mk_io_tls, 0, sizeof(mk_io_tls));
        return;
    }
    if (mk_io_tls.fread_calls)
        atomic_fetch_add_explicit(&g_mk_fread_calls, mk_io_tls.fread_calls, memory_order_relaxed);
    if (mk_io_tls.fread_bytes)
        atomic_fetch_add_explicit(&g_mk_fread_bytes, mk_io_tls.fread_bytes, memory_order_relaxed);
    if (mk_io_tls.fwrite_calls)
        atomic_fetch_add_explicit(&g_mk_fwrite_calls, mk_io_tls.fwrite_calls, memory_order_relaxed);
    if (mk_io_tls.fwrite_bytes)
        atomic_fetch_add_explicit(&g_mk_fwrite_bytes, mk_io_tls.fwrite_bytes, memory_order_relaxed);
    if (mk_io_tls.fopen_calls)
        atomic_fetch_add_explicit(&g_mk_fopen_calls, mk_io_tls.fopen_calls, memory_order_relaxed);
    if (mk_io_tls.fclose_calls)
        atomic_fetch_add_explicit(&g_mk_fclose_calls, mk_io_tls.fclose_calls, memory_order_relaxed);
    if (mk_io_tls.open_calls)
        atomic_fetch_add_explicit(&g_mk_open_calls, mk_io_tls.open_calls, memory_order_relaxed);
    if (mk_io_tls.read_calls)
        atomic_fetch_add_explicit(&g_mk_read_calls, mk_io_tls.read_calls, memory_order_relaxed);
    if (mk_io_tls.read_bytes)
        atomic_fetch_add_explicit(&g_mk_read_bytes, mk_io_tls.read_bytes, memory_order_relaxed);
    if (mk_io_tls.mmap_calls)
        atomic_fetch_add_explicit(&g_mk_mmap_calls, mk_io_tls.mmap_calls, memory_order_relaxed);
    if (mk_io_tls.munmap_calls)
        atomic_fetch_add_explicit(&g_mk_munmap_calls, mk_io_tls.munmap_calls, memory_order_relaxed);
    if (mk_io_tls.trigram_append_batches)
        atomic_fetch_add_explicit(&g_mk_trigram_append_batches, mk_io_tls.trigram_append_batches, memory_order_relaxed);
    memset(&mk_io_tls, 0, sizeof(mk_io_tls));
}

static size_t mk_fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t r = fread(ptr, size, nmemb, stream);
    if (g_verbose) {
        mk_io_tls.fread_calls++;
        mk_io_tls.fread_bytes += (unsigned long long)(r * size);
    }
    return r;
}

static size_t mk_fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t w = fwrite(ptr, size, nmemb, stream);
    if (g_verbose) {
        mk_io_tls.fwrite_calls++;
        mk_io_tls.fwrite_bytes += (unsigned long long)(w * size);
    }
    return w;
}

static FILE *mk_fopen(const char *path, const char *mode) {
    if (g_verbose) mk_io_tls.fopen_calls++;
    return fopen(path, mode);
}

static int mk_fclose(FILE *stream) {
    if (g_verbose) mk_io_tls.fclose_calls++;
    return fclose(stream);
}

static int mk_open(const char *pathname, int flags) {
    if (g_verbose) mk_io_tls.open_calls++;
    return open(pathname, flags);
}

static ssize_t mk_read(int fd, void *buf, size_t count) {
    ssize_t n = read(fd, buf, count);
    if (g_verbose) {
        mk_io_tls.read_calls++;
        if (n > 0) mk_io_tls.read_bytes += (unsigned long long)n;
    }
    return n;
}

static void *mk_mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset) {
    if (g_verbose) mk_io_tls.mmap_calls++;
    return mmap(addr, length, prot, flags, fd, offset);
}

static int mk_munmap(void *addr, size_t length) {
    if (g_verbose) mk_io_tls.munmap_calls++;
    return munmap(addr, length);
}

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

/*
 * Live progress uses stderr so it stays visible with ereport_index's diagnostic lines (which also use stderr).
 * Printing \\r updates on stdout while logging on stderr leaves no visible status when stdout is not the tty
 * (piped job output, some IDE terminals, etc.).
 */
static void status_line_tty_flush(void) {
    if (!isatty(STDERR_FILENO)) (void)fputc('\n', stderr);
    fflush(stderr);
}

static void clear_status_line(void) {
    if (isatty(STDERR_FILENO))
        fprintf(stderr, "\r%160s\r", "");
    else
        (void)fputc('\n', stderr);
    fflush(stderr);
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
    format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

    if (!g_verbose) {
        fprintf(stderr, "\r%s %s | %s/%s | el:%s            ", rate_buf, rate_label, done_buf, total_buf, elapsed_buf);
        status_line_tty_flush();
        ctx->last_status_sec = now;
        ctx->last_rate_sec = now;
        if (strcmp(phase, "merge") == 0)
            ctx->last_rate_merge_units = done_units;
        else
            ctx->last_rate_indexed_paths = ctx->indexed_paths;
        return;
    }

    human_decimal((double)ctx->scanned_records, rec_buf, sizeof(rec_buf));
    human_decimal((double)ctx->indexed_paths, idx_buf, sizeof(idx_buf));
    human_decimal((double)ctx->trigram_records, tri_buf, sizeof(tri_buf));

    fprintf(stderr,
            "\r%s %s | phase:%s unit:%s/%s rec:%s idx:%s tri:%s bad:%" PRIu64 " | el:%s            ",
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
    status_line_tty_flush();

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
    batch->approx_body_bytes += sizeof(parsed_path_t) + strlen(path) + 1U +
                                code_count * sizeof(uint32_t);
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

/* Cap batches waiting on the writer so N parallel workers cannot enqueue without bound (OOM). */
static size_t compute_write_queue_max_batches(int threads) {
    const char *e;
    unsigned long v;
    char *end;
    size_t w;

    if (threads < 1) threads = DEFAULT_THREADS;
    e = getenv("EREPORT_INDEX_WRITEQ_MAX_BATCHES");
    if (e && *e) {
        errno = 0;
        v = strtoul(e, &end, 10);
        if (!errno && end != e && *end == '\0' && v >= 4UL && v <= 4096UL) return (size_t)v;
    }
    /* Slightly deeper queue than threads/4 reduces parse stalls when one paths writer drains batches. */
    w = (size_t)((threads + 2) / 3);
    if (w < 6U) w = 6U;
    if (w > 96U) w = 96U;
    return w;
}

/* Fewer paths per batch when index_workers is high (one writer drains the queue). */
static size_t batch_paths_flush_limit(int threads) {
    size_t t;
    size_t base = g_write_batch_paths_base;

    if (threads < 1) threads = DEFAULT_THREADS;
    if (threads <= 32) return base;
    /* Softer than base×32/N — reduces batch churn and queue pressure at 64+ workers. */
    t = (size_t)(((unsigned long)base * 48UL) / (unsigned long)threads);
    if (t < 384U) t = 384U;
    return t;
}

static int write_queue_push(write_queue_t *q, write_batch_t *batch) {
    pthread_mutex_lock(&q->mutex);
    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    while (q->depth >= q->max_depth && !q->closed) {
        if (g_verbose)
            atomic_fetch_add_explicit(&q->run_stats->writeq_parse_waits, 1ULL, memory_order_relaxed);
        pthread_cond_wait(&q->has_space, &q->mutex);
    }
    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    batch->next = NULL;
    if (q->tail) q->tail->next = batch;
    else q->head = batch;
    q->tail = batch;
    q->depth++;
    q->queued_body_bytes += (uint64_t)batch->approx_body_bytes;
    pthread_cond_signal(&q->has_batch);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static write_batch_t *write_queue_pop_wait(write_queue_t *q) {
    write_batch_t *batch;

    pthread_mutex_lock(&q->mutex);
    while (!q->head && !q->closed) {
        pthread_cond_wait(&q->has_batch, &q->mutex);
        if (g_verbose)
            atomic_fetch_add_explicit(&q->run_stats->writeq_writer_waits, 1ULL, memory_order_relaxed);
    }
    batch = q->head;
    if (batch) {
        q->head = batch->next;
        if (!q->head) q->tail = NULL;
        q->depth--;
        q->queued_body_bytes -= (uint64_t)batch->approx_body_bytes;
        pthread_cond_signal(&q->has_space);
    }
    pthread_mutex_unlock(&q->mutex);
    return batch;
}

static void write_queue_close(write_queue_t *q) {
    pthread_mutex_lock(&q->mutex);
    q->closed = 1;
    pthread_cond_broadcast(&q->has_batch);
    pthread_cond_broadcast(&q->has_space);
    pthread_mutex_unlock(&q->mutex);
}

static void trigram_job_chain_free(trigram_job_t *head) {
    while (head) {
        trigram_job_t *next = head->next;
        free(head->codes);
        free(head);
        head = next;
    }
}

/* Move up to `space` jobs from the front of *remaining to a detached chain; *remaining is the rest (or NULL). */
static trigram_job_t *trigram_chain_peel_front(trigram_job_t **remaining, size_t space, size_t *n_jobs_out, uint64_t *body_out) {
    trigram_job_t *seg_h = *remaining;
    trigram_job_t *seg_t;
    size_t n;
    uint64_t b;

    if (!seg_h || space == 0) {
        *n_jobs_out = 0;
        *body_out = 0;
        return NULL;
    }
    seg_t = seg_h;
    b = (uint64_t)seg_t->approx_body_bytes;
    n = 1;
    while (n < space && seg_t->next) {
        seg_t = seg_t->next;
        b += (uint64_t)seg_t->approx_body_bytes;
        n++;
    }
    *remaining = seg_t->next;
    seg_t->next = NULL;
    *n_jobs_out = n;
    *body_out = b;
    return seg_h;
}

/*
 * Enqueue a full paths-writer batch chain in slices of up to (max_depth - depth) jobs.
 * Unlike an atomic batch enqueue, uses partial queue capacity — avoids stalling when e.g. depth 3500
 * and the batch has 1024 jobs but only 596 slots would suffice for partial progress.
 */
static int trigram_job_queue_push_chain_slices(trigram_job_queue_t *q, trigram_job_t *head) {
    trigram_job_t *rem = head;

    while (rem) {
        pthread_mutex_lock(&q->mutex);
        if (q->closed) {
            pthread_mutex_unlock(&q->mutex);
            trigram_job_chain_free(rem);
            return -1;
        }
        while (q->depth >= q->max_depth && !q->closed) {
            if (g_verbose)
                atomic_fetch_add_explicit(&q->run_stats->trigramq_paths_waits, 1ULL, memory_order_relaxed);
            pthread_cond_wait(&q->has_space, &q->mutex);
        }
        if (q->closed) {
            pthread_mutex_unlock(&q->mutex);
            trigram_job_chain_free(rem);
            return -1;
        }
        {
            size_t space = q->max_depth - q->depth;
            size_t take_n;
            uint64_t seg_body;
            trigram_job_t *seg_h = trigram_chain_peel_front(&rem, space, &take_n, &seg_body);
            trigram_job_t *seg_t;

            for (seg_t = seg_h; seg_t->next; seg_t = seg_t->next) {
            }
            if (q->tail) q->tail->next = seg_h;
            else q->head = seg_h;
            q->tail = seg_t;
            q->depth += take_n;
            q->queued_body_bytes += seg_body;
            pthread_cond_broadcast(&q->has_job);
        }
        pthread_mutex_unlock(&q->mutex);
    }
    return 0;
}

static trigram_job_t *trigram_job_queue_pop_wait(trigram_job_queue_t *q) {
    trigram_job_t *job;

    pthread_mutex_lock(&q->mutex);
    while (!q->head && !q->closed) {
        if (g_verbose)
            atomic_fetch_add_explicit(&q->run_stats->trigramq_worker_waits, 1ULL, memory_order_relaxed);
        pthread_cond_wait(&q->has_job, &q->mutex);
    }
    job = q->head;
    if (job) {
        q->head = job->next;
        if (!q->head) q->tail = NULL;
        q->depth--;
        q->queued_body_bytes -= (uint64_t)job->approx_body_bytes;
        /* One slot freed; wake one blocked producer (broadcast is unnecessary and costly at depth). */
        pthread_cond_signal(&q->has_space);
    }
    pthread_mutex_unlock(&q->mutex);
    return job;
}

static void trigram_job_queue_close(trigram_job_queue_t *q) {
    pthread_mutex_lock(&q->mutex);
    q->closed = 1;
    pthread_cond_broadcast(&q->has_job);
    pthread_cond_broadcast(&q->has_space);
    pthread_mutex_unlock(&q->mutex);
}

typedef struct {
    atomic_int stop;
    write_queue_t *wq;
    trigram_job_queue_t *tq;
    work_queue_t *chunkq;
    build_ctx_t *ctx;
    FILE *fp;
} memlog_shared_t;

static uint64_t chunk_queue_est_bytes(work_queue_t *q, const build_ctx_t *ctx) {
    size_t pending;
    uint64_t avg_path;

    if (!q || !ctx || ctx->chunk_total_count == 0) return 0;
    pthread_mutex_lock(&q->mutex);
    pending = (q->count > q->next_index) ? (q->count - q->next_index) : 0;
    pthread_mutex_unlock(&q->mutex);
    if (pending == 0) return 0;
    avg_path = ctx->chunk_path_bytes_total / ctx->chunk_total_count;
    return (uint64_t)pending * ((uint64_t)sizeof(file_chunk_t) + avg_path);
}

static void memlog_sort_top3(uint64_t wq, uint64_t tq, uint64_t cq, uint64_t mq, const char **n1, const char **n2,
                             const char **n3) {
    struct {
        const char *n;
        uint64_t v;
    } a[4];
    size_t i, j;

    a[0].n = "WB";
    a[0].v = wq;
    a[1].n = "TJ";
    a[1].v = tq;
    a[2].n = "CQ";
    a[2].v = cq;
    a[3].n = "MP";
    a[3].v = mq;
    for (i = 0; i < 4; i++) {
        for (j = i + 1; j < 4; j++) {
            if (a[j].v > a[i].v) {
                const char *tn;
                uint64_t tv;

                tn = a[i].n;
                tv = a[i].v;
                a[i].n = a[j].n;
                a[i].v = a[j].v;
                a[j].n = tn;
                a[j].v = tv;
            }
        }
    }
    *n1 = a[0].n;
    *n2 = a[1].n;
    *n3 = a[2].n;
}

static void memlog_shutdown(memlog_shared_t *ml, pthread_t *tid, int *started) {
    if (!started || !*started || !ml) return;
    atomic_store_explicit(&ml->stop, 1, memory_order_release);
    pthread_join(*tid, NULL);
    if (ml->fp) {
        fclose(ml->fp);
        ml->fp = NULL;
    }
    *started = 0;
}

static void *memlog_thread_main(void *arg) {
    memlog_shared_t *ml = (memlog_shared_t *)arg;

    for (;;) {
        struct tm tmb;
        time_t nowt;
        uint64_t wq_b = 0, tq_b = 0;
        uint64_t cq_est;
        uint64_t mq_peak;
        const char *nm1, *nm2, *nm3;
        char ts[32];
        char hw[32], ht[32], hc[32], hm[32];

        if (atomic_load_explicit(&ml->stop, memory_order_acquire)) break;
        if (!ml->fp || !ml->ctx) {
            sleep(MEMLOG_INTERVAL_SEC);
            continue;
        }

        nowt = time(NULL);
        gmtime_r(&nowt, &tmb);
        strftime(ts, sizeof(ts), "%y%m%d %H:%M:%S", &tmb);

        if (ml->wq) {
            pthread_mutex_lock(&ml->wq->mutex);
            wq_b = ml->wq->queued_body_bytes;
            pthread_mutex_unlock(&ml->wq->mutex);
        }
        if (ml->tq) {
            pthread_mutex_lock(&ml->tq->mutex);
            tq_b = ml->tq->queued_body_bytes;
            pthread_mutex_unlock(&ml->tq->mutex);
        }
        cq_est = chunk_queue_est_bytes(ml->chunkq, ml->ctx);
        mq_peak = atomic_load_explicit(&ml->ctx->merge_bucket_ram_peak, memory_order_relaxed);

        memlog_sort_top3(wq_b, tq_b, cq_est, mq_peak, &nm1, &nm2, &nm3);
        human_decimal((double)wq_b, hw, sizeof(hw));
        human_decimal((double)tq_b, ht, sizeof(ht));
        human_decimal((double)cq_est, hc, sizeof(hc));
        human_decimal((double)mq_peak, hm, sizeof(hm));

        fprintf(ml->fp, "%s wb=%s tj=%s cq=%s mp=%s | top:%s>%s>%s\n", ts, hw, ht, hc, hm, nm1, nm2, nm3);
        fflush(ml->fp);

        sleep(MEMLOG_INTERVAL_SEC);
    }
    return NULL;
}

/* ~1s between status lines, but wake early when main signals (e.g. index phase just started). */
static void stats_wait_cycle(index_run_stats_t *rs) {
    int i;
    struct timespec sl;

    sl.tv_sec = 0;
    sl.tv_nsec = 100000000L; /* 100ms slices so stats_wake is seen within ~100ms */

    for (i = 0; i < 10 && !atomic_load(&rs->stop_stats); i++) {
        if (atomic_exchange_explicit(&rs->stats_wake, 0, memory_order_relaxed)) return;
        (void)nanosleep(&sl, NULL);
    }
}

static void *stats_thread_main(void *arg) {
    index_run_stats_t *rs = (index_run_stats_t *)arg;

    while (!atomic_load(&rs->stop_stats)) {
        unsigned long long scanned_files;
        unsigned long long scanned_records;
        unsigned long long indexed_paths;
        unsigned long long trigram_records;
        unsigned long long bad_input_files;
        double elapsed_sec;
        char sf[32], tf[32], sr[32], ip[32], tr[32], rate_buf[32], elapsed_buf[32];
        double rate;

        scanned_records = atomic_load(&rs->scanned_records);
        indexed_paths = atomic_load(&rs->indexed_paths);
        bad_input_files = atomic_load(&rs->bad_input_files);
        elapsed_sec = rs->run_start_sec > 0.0 ? now_sec() - rs->run_start_sec : 0.0;
        rate = (double)(indexed_paths - rs->stats_prev_indexed_paths);
        rs->stats_prev_indexed_paths = indexed_paths;
        human_decimal(rate, rate_buf, sizeof(rate_buf));
        human_decimal((double)indexed_paths, ip, sizeof(ip));
        human_decimal((double)scanned_records, sr, sizeof(sr));
        format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

        if (!g_verbose) {
            uint64_t prep_tot_u = rs->chunk_prep_files_total;
            unsigned long long prep_done_u = atomic_load(&rs->chunk_prep_files_done);

            if (prep_tot_u > 0ULL && prep_done_u < prep_tot_u) {
                char pdone[32], ptot[32];
                human_decimal((double)prep_done_u, pdone, sizeof(pdone));
                human_decimal((double)prep_tot_u, ptot, sizeof(ptot));
                fprintf(stderr, "\rmapping %s/%s files | el:%s            ", pdone, ptot, elapsed_buf);
            } else {
                fprintf(stderr,
                        "\r%s paths/s | idx:%s rec:%s bad:%llu | el:%s            ",
                        rate_buf, ip, sr, bad_input_files, elapsed_buf);
            }
            status_line_tty_flush();
            stats_wait_cycle(rs);
            continue;
        }

        scanned_files = atomic_load(&rs->scanned_input_files);
        trigram_records = atomic_load(&rs->trigram_records);
        human_decimal((double)scanned_files, sf, sizeof(sf));
        human_decimal((double)rs->input_files_total, tf, sizeof(tf));
        human_decimal((double)trigram_records, tr, sizeof(tr));

        {
            uint64_t prep_tot_u = rs->chunk_prep_files_total;
            unsigned long long prep_done_u = atomic_load(&rs->chunk_prep_files_done);

            if (prep_tot_u > 0ULL && prep_done_u < prep_tot_u) {
                char pdone[32], ptot[32];
                human_decimal((double)prep_done_u, pdone, sizeof(pdone));
                human_decimal((double)prep_tot_u, ptot, sizeof(ptot));
                fprintf(stderr,
                        "\rchunk-map files:%s/%s | scanning bin boundaries for parallel parse | el:%s            ",
                        pdone, ptot, elapsed_buf);
                status_line_tty_flush();
            } else {
                uint64_t ix_tot = rs->index_chunks_total;
                unsigned long long ix_done = atomic_load(&rs->chunks_index_done);

                if (ix_tot > 0ULL) {
                    char ck_done[32], ck_tot[32];
                    human_decimal((double)ix_done, ck_done, sizeof(ck_done));
                    human_decimal((double)ix_tot, ck_tot, sizeof(ck_tot));
                    fprintf(stderr,
                            "\r%s paths/s | chunks:%s/%s | files:%s/%s rec:%s idx:%s tri:%s bad:%llu | el:%s            ",
                            rate_buf, ck_done, ck_tot, sf, tf, sr, ip, tr, bad_input_files, elapsed_buf);
                } else {
                    fprintf(stderr,
                            "\r%s paths/s | files:%s/%s rec:%s idx:%s tri:%s bad:%llu | el:%s            ",
                            rate_buf, sf, tf, sr, ip, tr, bad_input_files, elapsed_buf);
                }
                status_line_tty_flush();
            }
        }

        stats_wait_cycle(rs);
    }

    return NULL;
}

static int arg_is_verbose(const char *s) {
    return s && strcmp(s, "--verbose") == 0;
}

/* Skip leading "--verbose" tokens (may appear before or after the subcommand). */
static int argv_skip_verbose_prefix(int argc, char **argv) {
    int i = 1;
    while (i < argc && arg_is_verbose(argv[i])) i++;
    return i;
}

static void die_usage(const char *argv0) {
    fprintf(stderr,
            "Usage:\n"
            "  %s --make [--index-dir <path>] [username|uid] [bin_dir ...]\n"
            "  %s --resume-merge --index-dir <path>\n"
            "  %s --search [--index-dir <path>] <term> [--json] [--skip N] [--limit M]\n"
            "  Optional --verbose anywhere: detailed stderr progress, queue-wait stats, rusage, and I/O counters\n"
            "  for --make / --resume-merge; plain --search prints timing to stderr. Default output is a short summary.\n",
            argv0,
            argv0,
            argv0);
    fprintf(stderr,
            "  --make: Optional --index-dir <path> (must follow --make) writes index files directly under\n"
            "    <path> (paths.bin, tri_keys.bin, …). Default is ./<username>/index/ or ./all_users/index/.\n"
            "    Multiple bin_dir arguments are merged like ereport. If the first token after flags is a valid\n"
            "    login or numeric uid, it selects that user; remaining arguments are crawl directories (default .).\n"
            "    If that token is not a known user, every argument is a crawl directory (all-users index).\n"
            "    With no user/bin arguments after flags, the index is all-users for ./\n"
            "  --resume-merge: After paths.bin and path_offsets.bin exist, rebuild tri_keys.bin and tri_postings.bin\n"
            "    from remaining tmp_trigrams_*.bin and merge_seg_* files (e.g. after OOM during merge). Requires\n"
            "    --index-dir. Deletes partial tri_keys.bin / tri_postings.bin first.\n"
            "  --search: Optional --index-dir <path> (same flag as --make); default index dir is ./index.\n"
            "    Plain search prints paths.\n"
            "    With --json: UTF-8 JSON object {\"total\",\"skip\",\"limit\",\"search_ms\",\"index_keys\",\"indexed_paths\",\"paths\":[...]}\n"
            "      (indexed_paths from meta.txt = corpus size; index_keys = distinct trigrams in tri_keys.bin.)\n"
            "    Parallelism: EREPORT_INDEX_THREADS (default 32) loads postings lists and filters paths in parallel\n"
            "    when the query has multiple trigrams and the candidate set is large.\n"
            "    --make tuning: EREPORT_INDEX_TRIGRAM_THREADS (default: same as EREPORT_INDEX_THREADS) parallel\n"
            "    writers to tmp_trigram bucket files; EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH (default scales with\n"
            "    EREPORT_INDEX_TRIGRAM_THREADS, max 16384 unless set; range 512…262144)\n"
            "    bounded queue from paths writer to trigram workers; EREPORT_INDEX_WRITE_BATCH_PATHS (default 4096,\n"
            "    512…65536) paths per batch to the writer. Also EREPORT_INDEX_WRITEQ_MAX_BATCHES, EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS.\n"
            "    Large --make: run `ulimit -n 65535` (or higher) first — see README.\n");
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

static void free_chunk_array_rows(file_chunk_t *chunks, size_t count) {
    size_t j;

    for (j = 0; j < count; j++) free(chunks[j].path);
    free(chunks);
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

/* Trigram writers (default: same as EREPORT_INDEX_THREADS). */
static int parse_trigram_thread_count(int index_threads) {
    const char *e = getenv("EREPORT_INDEX_TRIGRAM_THREADS");
    long t;
    char *end;

    if (!e || !*e) return index_threads;
    errno = 0;
    t = strtol(e, &end, 10);
    if (errno || end == e || *end || t < 1 || t > 4096) return index_threads;
    return (int)t;
}

/* Depth of bounded queue between paths writer and trigram workers (larger → less producer blocking, more RAM). */
static size_t default_trigram_queue_depth(int trigram_workers) {
    size_t d;

    if (trigram_workers < 1) trigram_workers = DEFAULT_THREADS;
    d = (size_t)trigram_workers * (size_t)TRIGRAM_JOB_QUEUE_DEPTH_PER_WORKER;
    if (d < (size_t)TRIGRAM_JOB_QUEUE_DEPTH_MIN) d = (size_t)TRIGRAM_JOB_QUEUE_DEPTH_MIN;
    if (d > (size_t)TRIGRAM_JOB_QUEUE_DEPTH_MAX_DEFAULT) d = (size_t)TRIGRAM_JOB_QUEUE_DEPTH_MAX_DEFAULT;
    return d;
}

static size_t parse_trigram_queue_depth(int trigram_workers) {
    const char *e = getenv("EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH");
    unsigned long v;
    char *end;

    if (!e || !*e) return default_trigram_queue_depth(trigram_workers);
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 512UL || v > 262144UL) return default_trigram_queue_depth(trigram_workers);
    return (size_t)v;
}

/* Target paths per write batch before flushing to the writer thread (scaled down when many parse workers). */
static size_t parse_write_batch_paths(void) {
    const char *e = getenv("EREPORT_INDEX_WRITE_BATCH_PATHS");
    unsigned long v;
    char *end;

    if (!e || !*e) return WRITE_BATCH_PATHS;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 512UL || v > 65536UL) return WRITE_BATCH_PATHS;
    return (size_t)v;
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
    fp = mk_fopen(ckpath, "rb");
    if (!fp) return -1;
    if (mk_fread(&ch, sizeof(ch), 1, fp) != 1) {
        mk_fclose(fp);
        errno = EINVAL;
        return -1;
    }
    if (memcmp(ch.magic, CRAWL_CKPT_MAGIC, CRAWL_CKPT_MAGIC_LEN) != 0 || ch.version != CRAWL_CKPT_ONDISK_VERSION ||
        ch.stride_bytes != CRAWL_CKPT_STRIDE_BYTES || ch.num_offsets == 0 || ch.num_offsets > (uint64_t)(SIZE_MAX / sizeof(uint64_t))) {
        mk_fclose(fp);
        errno = EINVAL;
        return -1;
    }
    buf = (uint64_t *)malloc((size_t)ch.num_offsets * sizeof(*buf));
    if (!buf) {
        mk_fclose(fp);
        return -1;
    }
    if (mk_fread(buf, sizeof(uint64_t), (size_t)ch.num_offsets, fp) != (size_t)ch.num_offsets || mk_fclose(fp) != 0) {
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

    fp = mk_fopen(path, "rb");
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

        if (mk_fread(&r, sizeof(r), 1, fp) != 1) {
            if (feof(fp)) fprintf(stderr, "warn: unexpected EOF in segment of %s\n", path);
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
    if (fp) mk_fclose(fp);
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
            mk_io_tls_flush();
            return NULL;
        }
        if (chunk_list_take_all(&b->chunks, &b->chunk_count, &b->chunk_cap, seg_chunks, seg_count) != 0) {
            free_chunk_array_rows(b->chunks, b->chunk_count);
            b->chunks = NULL;
            b->chunk_count = 0;
            b->rc = -1;
            mk_io_tls_flush();
            return NULL;
        }
        b->fc += seg_fc;
    }
    mk_io_tls_flush();
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

    fp = mk_fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }
    if (mk_fread(&fh, sizeof(fh), 1, fp) != 1) {
        fprintf(stderr, "warn: short read on header: %s\n", path);
        mk_fclose(fp);
        return -1;
    }
    mk_fclose(fp);
    fp = NULL;

    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
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
        int nw = parse_index_thread_count();
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
    index_run_stats_t *run_stats;
} chunk_prep_pool_t;

static void *chunk_prep_worker_main(void *arg) {
    chunk_prep_pool_t *pool = (chunk_prep_pool_t *)arg;

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&pool->next_path_index, 1, memory_order_relaxed);
        file_chunk_t *local_chunks = NULL;
        size_t local_count = 0;
        unsigned int fc = 0;
        int r;

        if (i >= pool->path_count) break;

        r = build_chunks_for_file(pool->paths[i], i, pool->chunk_targets[i], &local_chunks, &local_count, &fc);
        pool->prep_rc[(int)i] = r;
        pool->prep_chunks[(int)i] = local_chunks;
        pool->prep_chunk_counts[(int)i] = local_count;
        (void)fc;
        atomic_fetch_add_explicit(&pool->run_stats->chunk_prep_files_done, 1ULL, memory_order_relaxed);
    }

    mk_io_tls_flush();
    return NULL;
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
        if (mk_fwrite(&key, sizeof(key), 1, keys_fp) != 1) return -1;
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

/* After trigram intersection, require the full query substring to appear contiguously in the path.
 * (Trigrams are extracted per path segment during --make, but the query term is contiguous; otherwise
 * unrelated paths can match every trigram in different segments — e.g. …/micro/…/iche/…/hel… without "michel".) */
static int path_matches_term(const char *path, const char *lower_term) {
    char *lower;
    int matched;

    if (!lower_term || !lower_term[0]) return 0;
    lower = ascii_lower_dup(path);
    if (!lower) return 0;
    matched = strstr(lower, lower_term) != NULL;
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

/* Count sliding 3-byte windows (one trigram each) across path segments (length >= 3). */
static size_t count_path_trigram_windows(const char *path) {
    const char *seg = path;
    size_t total = 0;

    while (*seg != '\0') {
        const char *next = strchr(seg, '/');
        size_t seg_len = next ? (size_t)(next - seg) : strlen(seg);

        if (seg_len >= 3) total += seg_len - 2;
        if (!next) break;
        seg = next + 1;
    }
    return total;
}

static int extract_path_trigrams(const char *path, uint32_t **out_codes, size_t *out_count, worker_arg_t *scratch) {
    const char *seg = path;
    size_t nraw = count_path_trigram_windows(path);
    uint32_t *codes;
    size_t pos = 0;

    if (nraw == 0) {
        *out_codes = NULL;
        *out_count = 0;
        return 0;
    }

    codes = (uint32_t *)malloc(nraw * sizeof(uint32_t));
    if (!codes) return -1;

    seg = path;
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

            for (i = 0; i < seg_len; i++) {
                unsigned char b = (unsigned char)seg[i];
                lower_seg[i] = (char)((b >= (unsigned char)'A' && b <= (unsigned char)'Z') ? (b + 32U) : b);
            }
            lower_seg[seg_len] = '\0';

            for (i = 0; i + 3 <= seg_len; i++) {
                uint32_t trigram = ((uint32_t)(unsigned char)lower_seg[i] << 16) |
                                   ((uint32_t)(unsigned char)lower_seg[i + 1] << 8) |
                                   (uint32_t)(unsigned char)lower_seg[i + 2];
                codes[pos++] = trigram;
            }
            if (!scratch) free(lower_seg);
        }

        if (!next) break;
        seg = next + 1;
    }

    if (pos != nraw) {
        free(codes);
        return -1;
    }

    sort_codes_unique(codes, &nraw);
    *out_codes = codes;
    *out_count = nraw;
    return 0;
}

/*
 * LRU ceiling for tmp_trigrams_* shard FILE*s per worker (lazy-open). Override with
 * EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS (32…4096); unset uses DEFAULT_MAX_OPEN_TRIGRAM_BUCKET_FP.
 */
static uint32_t compute_max_open_trigram_buckets(void) {
    const char *e = getenv("EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS");
    long v;
    char *end;

    if (e && *e) {
        errno = 0;
        v = strtol(e, &end, 10);
        if (!errno && end != e && !*end && v >= 32 && v <= (long)TRIGRAM_BUCKET_COUNT) return (uint32_t)v;
    }
    return (uint32_t)DEFAULT_MAX_OPEN_TRIGRAM_BUCKET_FP;
}

static size_t tw_worker_fp_ix(uint32_t wid, uint32_t bucket) {
    return (size_t)wid * (size_t)TRIGRAM_BUCKET_COUNT + (size_t)bucket;
}

static uint32_t tw_worker_pick_lru_victim(const build_ctx_t *ctx, uint32_t wid, uint32_t exclude) {
    uint32_t b, best = UINT32_MAX;
    uint64_t best_age = 0;
    int found = 0;

    for (b = 0; b < TRIGRAM_BUCKET_COUNT; b++) {
        size_t ix;

        if (b == exclude) continue;
        ix = tw_worker_fp_ix(wid, b);
        if (!ctx->tw_worker_fp[ix]) continue;
        if (!found || ctx->tw_worker_lru_age[ix] < best_age ||
            (ctx->tw_worker_lru_age[ix] == best_age && b < best)) {
            best_age = ctx->tw_worker_lru_age[ix];
            best = b;
            found = 1;
        }
    }
    return found ? best : UINT32_MAX;
}

/*
 * Per-worker LRU cap for tmp_trigrams_*_w*.bin FILE*s. Worst-case open fds ≈ trigram_workers × cap — keep under
 * ulimit (README: use ulimit -n 65535 for large --make). We split EREPORT_INDEX_ASSUMED_ULIMIT_NOFILE across
 * workers so defaults stay safe without probing the kernel.
 */
static uint32_t compute_tw_worker_max_open(uint32_t shard_count, uint32_t gmax) {
    uint64_t budget, per;

    if (shard_count < 1U) shard_count = 1U;

    if (EREPORT_INDEX_ASSUMED_ULIMIT_NOFILE <= EREPORT_INDEX_RESERVED_FD_NON_TRIGRAM)
        return gmax < 32U ? gmax : 32U;

    budget = (uint64_t)EREPORT_INDEX_ASSUMED_ULIMIT_NOFILE - (uint64_t)EREPORT_INDEX_RESERVED_FD_NON_TRIGRAM;
    per = budget / (uint64_t)shard_count;
    if (per > (uint64_t)gmax) per = (uint64_t)gmax;
    if (per < 32ULL) per = 32ULL;
    return (uint32_t)per;
}

static int parallel_bucket_io_init(build_ctx_t *ctx, uint32_t shard_count) {
    uint32_t gmax;

    if (shard_count < 1U) shard_count = 1U;
    ctx->trigram_tmp_shard_count = shard_count;

    ctx->tw_worker_fp = (FILE **)calloc((size_t)shard_count * (size_t)TRIGRAM_BUCKET_COUNT, sizeof(FILE *));
    ctx->tw_worker_lru_age = (uint64_t *)calloc((size_t)shard_count * (size_t)TRIGRAM_BUCKET_COUNT, sizeof(uint64_t));
    ctx->tw_worker_open_count = (uint32_t *)calloc((size_t)shard_count, sizeof(uint32_t));
    ctx->tw_worker_lru_next_tick = (uint64_t *)calloc((size_t)shard_count, sizeof(uint64_t));
    if (!ctx->tw_worker_fp || !ctx->tw_worker_lru_age || !ctx->tw_worker_open_count || !ctx->tw_worker_lru_next_tick) {
        free(ctx->tw_worker_fp);
        free(ctx->tw_worker_lru_age);
        free(ctx->tw_worker_open_count);
        free(ctx->tw_worker_lru_next_tick);
        ctx->tw_worker_fp = NULL;
        ctx->tw_worker_lru_age = NULL;
        ctx->tw_worker_open_count = NULL;
        ctx->tw_worker_lru_next_tick = NULL;
        ctx->trigram_tmp_shard_count = 0;
        return -1;
    }

    gmax = compute_max_open_trigram_buckets();
    ctx->tw_worker_max_open = compute_tw_worker_max_open(shard_count, gmax);
    if (g_verbose) {
        fprintf(stderr,
                "ereport_index: tmp_trigram shard writers=%u max_open_per_shard=%u (EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS=%u)\n",
                shard_count, ctx->tw_worker_max_open, gmax);
    }
    return 0;
}

/* Per trigram worker: sort buffer for batching writes to tmp_trigrams (avoid malloc per path). */
static __thread trigram_record_t *tw_sort_buf;
static __thread size_t tw_sort_cap;

static trigram_record_t *tw_ensure_sort_buf(size_t n) {
    if (n <= tw_sort_cap) return tw_sort_buf;
    {
        void *p = realloc(tw_sort_buf, n * sizeof(trigram_record_t));
        if (!p) return NULL;
        tw_sort_buf = (trigram_record_t *)p;
        tw_sort_cap = n;
    }
    return tw_sort_buf;
}

static int trigram_rec_cmp_bucket(const void *aa, const void *bb) {
    const trigram_record_t *a = (const trigram_record_t *)aa;
    const trigram_record_t *b = (const trigram_record_t *)bb;
    uint32_t ba = a->trigram >> (24 - TRIGRAM_BUCKET_BITS);
    uint32_t bbk = b->trigram >> (24 - TRIGRAM_BUCKET_BITS);
    if (ba < bbk) return -1;
    if (ba > bbk) return 1;
    return 0;
}

static void parallel_bucket_io_shutdown(build_ctx_t *ctx) {
    uint32_t wid, b;

    if (!ctx->tw_worker_fp) return;
    for (wid = 0; wid < ctx->trigram_tmp_shard_count; wid++) {
        for (b = 0; b < TRIGRAM_BUCKET_COUNT; b++) {
            size_t ix = tw_worker_fp_ix(wid, b);

            if (ctx->tw_worker_fp[ix]) {
                mk_fclose(ctx->tw_worker_fp[ix]);
                ctx->tw_worker_fp[ix] = NULL;
            }
        }
        if (ctx->tw_worker_open_count) ctx->tw_worker_open_count[wid] = 0;
    }
    free(ctx->tw_worker_fp);
    ctx->tw_worker_fp = NULL;
    free(ctx->tw_worker_lru_age);
    ctx->tw_worker_lru_age = NULL;
    free(ctx->tw_worker_open_count);
    ctx->tw_worker_open_count = NULL;
    free(ctx->tw_worker_lru_next_tick);
    ctx->tw_worker_lru_next_tick = NULL;
    /* trigram_tmp_shard_count kept for merge phase (merge_load uses worker shard paths). */
}

/*
 * Write n trigram records for one bucket on one trigram worker's shard file (caller groups by bucket).
 * Each worker_id maps to tmp_trigrams_%04u_w%04u.bin — no mutex (exclusive FILE* per worker × bucket).
 */
static int append_trigram_records_batch_parallel(build_ctx_t *ctx, uint32_t worker_id, uint32_t bucket,
                                                 const trigram_record_t *recs, size_t n) {
    FILE *fp;
    char path[PATH_MAX];
    uint64_t tick;
    size_t ix;

    if (n == 0) return 0;
    if (worker_id >= ctx->trigram_tmp_shard_count) return -1;

    if (g_verbose) mk_io_tls.trigram_append_batches++;
    ix = tw_worker_fp_ix(worker_id, bucket);
    fp = ctx->tw_worker_fp[ix];
    if (fp) {
        tick = ++ctx->tw_worker_lru_next_tick[worker_id];
        ctx->tw_worker_lru_age[ix] = tick;
        if (mk_fwrite(recs, sizeof(*recs), n, fp) != n) {
            fprintf(stderr, "ereport_index: fwrite tmp_trigrams bucket %u worker %u: %s\n", bucket, worker_id,
                    strerror(errno));
            return -1;
        }
        return 0;
    }

    while (ctx->tw_worker_open_count[worker_id] >= ctx->tw_worker_max_open) {
        uint32_t victim = tw_worker_pick_lru_victim(ctx, worker_id, bucket);
        size_t vix;

        if (victim == UINT32_MAX) {
            fprintf(stderr,
                    "ereport_index: internal: no LRU victim (worker %u open_count=%u max=%u)\n",
                    worker_id, ctx->tw_worker_open_count[worker_id], ctx->tw_worker_max_open);
            return -1;
        }
        vix = tw_worker_fp_ix(worker_id, victim);
        if (ctx->tw_worker_fp[vix]) {
            if (mk_fclose(ctx->tw_worker_fp[vix]) != 0) {
                fprintf(stderr, "ereport_index: fclose tmp_trigrams bucket %u worker %u: %s\n", victim, worker_id,
                        strerror(errno));
            }
            ctx->tw_worker_fp[vix] = NULL;
            ctx->tw_worker_open_count[worker_id]--;
        }
        fp = ctx->tw_worker_fp[ix];
        if (fp) {
            tick = ++ctx->tw_worker_lru_next_tick[worker_id];
            ctx->tw_worker_lru_age[ix] = tick;
            if (mk_fwrite(recs, sizeof(*recs), n, fp) != n) {
                fprintf(stderr, "ereport_index: fwrite tmp_trigrams bucket %u worker %u: %s\n", bucket, worker_id,
                        strerror(errno));
                return -1;
            }
            return 0;
        }
    }

    fp = ctx->tw_worker_fp[ix];
    if (fp) {
        tick = ++ctx->tw_worker_lru_next_tick[worker_id];
        ctx->tw_worker_lru_age[ix] = tick;
        if (mk_fwrite(recs, sizeof(*recs), n, fp) != n) {
            fprintf(stderr, "ereport_index: fwrite tmp_trigrams bucket %u worker %u: %s\n", bucket, worker_id,
                    strerror(errno));
            return -1;
        }
        return 0;
    }

    if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, worker_id) >=
        (int)sizeof(path)) {
        fprintf(stderr, "ereport_index: tmp_trigrams path too long (bucket %u worker %u)\n", bucket, worker_id);
        return -1;
    }
    fp = mk_fopen(path, "ab");
    if (!fp) {
        fprintf(stderr, "ereport_index: fopen %s: %s\n", path, strerror(errno));
        return -1;
    }
    if (setvbuf(fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    ctx->bucket_nonempty[bucket] = 1;
    ctx->tw_worker_fp[ix] = fp;
    ctx->tw_worker_open_count[worker_id]++;
    tick = ++ctx->tw_worker_lru_next_tick[worker_id];
    ctx->tw_worker_lru_age[ix] = tick;
    if (mk_fwrite(recs, sizeof(*recs), n, fp) != n) {
        fprintf(stderr, "ereport_index: fwrite tmp_trigrams bucket %u worker %u: %s\n", bucket, worker_id, strerror(errno));
        return -1;
    }
    return 0;
}

static int write_varint_u64(FILE *fp, uint64_t value) {
    unsigned char buf[10];
    size_t n = 0;

    while (value >= 0x80U) {
        buf[n++] = (unsigned char)((value & 0x7FU) | 0x80U);
        value >>= 7;
    }
    buf[n++] = (unsigned char)value;
    return mk_fwrite(buf, 1, n, fp) == n ? 0 : -1;
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

static int append_paths_only(build_ctx_t *ctx, const char *path) {
    uint64_t offset;

    offset = (uint64_t)ftello(ctx->paths_fp);
    if (mk_fwrite(&offset, sizeof(offset), 1, ctx->path_offsets_fp) != 1) {
        fprintf(stderr, "ereport_index: fwrite path_offsets.bin: %s\n", strerror(errno));
        return -1;
    }
    if (mk_fwrite(path, 1, strlen(path) + 1, ctx->paths_fp) != strlen(path) + 1) {
        fprintf(stderr, "ereport_index: fwrite paths.bin: %s\n", strerror(errno));
        return -1;
    }
    return 0;
}

static void *paths_writer_main(void *arg_void) {
    paths_writer_arg_t *pa = (paths_writer_arg_t *)arg_void;
    index_run_stats_t *rs = pa->ctx->run_stats;

    for (;;) {
        write_batch_t *batch = write_queue_pop_wait(pa->write_queue);
        if (!batch) break;

        {
            trigram_job_t *tj_head = NULL;
            trigram_job_t *tj_tail = NULL;
            size_t tj_n = 0;
            uint64_t base = pa->ctx->indexed_paths;

            for (size_t i = 0; i < batch->count; i++) {
                parsed_path_t *item = &batch->items[i];
                uint64_t path_id = base + (uint64_t)i;
                trigram_job_t *job;

                if (append_paths_only(pa->ctx, item->path) != 0) {
                    pa->ctx->indexed_paths = base + (uint64_t)i;
                    atomic_fetch_add_explicit(&rs->indexed_paths, (unsigned long long)i, memory_order_relaxed);
                    atomic_store(&rs->writer_failed, 1);
                    trigram_job_chain_free(tj_head);
                    write_batch_destroy(batch);
                    mk_io_tls_flush();
                    return NULL;
                }

                job = (trigram_job_t *)malloc(sizeof(*job));
                if (!job) {
                    fprintf(stderr, "ereport_index: malloc(trigram_job): %s\n", strerror(errno));
                    pa->ctx->indexed_paths = base + (uint64_t)i + 1ULL;
                    atomic_fetch_add_explicit(&rs->indexed_paths, (unsigned long long)(i + 1U), memory_order_relaxed);
                    atomic_store(&rs->writer_failed, 1);
                    trigram_job_chain_free(tj_head);
                    write_batch_destroy(batch);
                    mk_io_tls_flush();
                    return NULL;
                }
                job->path_id = path_id;
                job->codes = item->codes;
                job->code_count = item->code_count;
                job->approx_body_bytes = sizeof(trigram_job_t) + item->code_count * sizeof(uint32_t);
                job->next = NULL;
                item->codes = NULL;

                if (!tj_head) {
                    tj_head = tj_tail = job;
                } else {
                    tj_tail->next = job;
                    tj_tail = job;
                }
                tj_n++;
            }

            pa->ctx->indexed_paths = base + (uint64_t)batch->count;
            atomic_fetch_add_explicit(&rs->indexed_paths, (unsigned long long)batch->count, memory_order_relaxed);

            if (tj_n != 0 && trigram_job_queue_push_chain_slices(pa->trigram_queue, tj_head) != 0) {
                fprintf(stderr, "ereport_index: trigram job queue push failed (queue closed)\n");
                trigram_job_chain_free(tj_head);
                atomic_store(&rs->writer_failed, 1);
                write_batch_destroy(batch);
                mk_io_tls_flush();
                return NULL;
            }
        }

        write_batch_destroy(batch);
    }

    mk_io_tls_flush();
    return NULL;
}

static void *trigram_worker_main(void *arg_void) {
    trigram_worker_arg_t *tw = (trigram_worker_arg_t *)arg_void;
    index_run_stats_t *rs = tw->ctx->run_stats;

    for (;;) {
        trigram_job_t *job = trigram_job_queue_pop_wait(tw->trigram_queue);
        if (!job) break;

        if (atomic_load(&rs->writer_failed)) {
            free(job->codes);
            free(job);
            continue;
        }

        if (job->code_count > 0) {
            trigram_record_t *buf = tw_ensure_sort_buf(job->code_count);
            size_t i;
            size_t run_i;

            if (!buf) {
                fprintf(stderr, "ereport_index: realloc trigram batch buf (%zu): %s\n", job->code_count, strerror(errno));
                atomic_store(&rs->writer_failed, 1);
                free(job->codes);
                free(job);
                mk_io_tls_flush();
                return NULL;
            }
            for (i = 0; i < job->code_count; i++) {
                buf[i].trigram = job->codes[i];
                buf[i].path_id = job->path_id;
            }
            if (job->code_count > 1) qsort(buf, job->code_count, sizeof(*buf), trigram_rec_cmp_bucket);

            run_i = 0;
            while (run_i < job->code_count) {
                uint32_t b = buf[run_i].trigram >> (24 - TRIGRAM_BUCKET_BITS);
                size_t run_j = run_i + 1;
                while (run_j < job->code_count) {
                    uint32_t bj = buf[run_j].trigram >> (24 - TRIGRAM_BUCKET_BITS);
                    if (bj != b) break;
                    run_j++;
                }
                if (append_trigram_records_batch_parallel(tw->ctx, tw->worker_index, b, buf + run_i, run_j - run_i) != 0) {
                    atomic_store(&rs->writer_failed, 1);
                    free(job->codes);
                    free(job);
                    mk_io_tls_flush();
                    return NULL;
                }
                run_i = run_j;
            }
        }

        atomic_fetch_add(&rs->trigram_records, (unsigned long long)job->code_count);
        free(job->codes);
        free(job);
    }

    mk_io_tls_flush();
    return NULL;
}

static void finalize_chunk_file_progress(index_run_stats_t *rs, file_state_t *file_states, size_t file_index) {
    unsigned int old_remaining;
    if (!file_states || !rs) return;
    if (g_verbose) atomic_fetch_add_explicit(&rs->chunks_index_done, 1ULL, memory_order_relaxed);
    old_remaining = atomic_fetch_sub(&file_states[file_index].remaining_chunks, 1U);
    if (old_remaining == 1U && g_verbose) atomic_fetch_add(&rs->scanned_input_files, 1U);
}

static int process_chunk_make(worker_arg_t *worker, const file_chunk_t *chunk) {
    build_ctx_t *ctx = worker->ctx;
    index_run_stats_t *rs = ctx->run_stats;
    write_queue_t *write_queue = worker->write_queue;
    file_state_t *file_states = worker->file_states;
    FILE *fp = NULL;
    int rc = -1;
    write_batch_t *batch = NULL;
    uint64_t scanned_local = 0;
    uint64_t scanned_published = 0;

    fp = mk_fopen(chunk->path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", chunk->path, strerror(errno));
        atomic_fetch_add(&rs->bad_input_files, 1U);
        return -1;
    }

    if (setvbuf(fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    if (fseeko(fp, (off_t)chunk->start_offset, SEEK_SET) != 0) {
        fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
        atomic_fetch_add(&rs->bad_input_files, 1U);
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
        n = mk_fread(&r, sizeof(r), 1, fp);
        if (n != 1) {
            if (feof(fp)) rc = 0;
            else {
                fprintf(stderr, "warn: read error in %s\n", chunk->path);
                atomic_fetch_add(&rs->bad_input_files, 1U);
            }
            break;
        }

        scanned_local++;
        if (g_verbose && scanned_local - scanned_published >= (uint64_t)SCANNED_RECORDS_PUBLISH_STRIDE) {
            uint64_t delta =
                ((scanned_local - scanned_published) / (uint64_t)SCANNED_RECORDS_PUBLISH_STRIDE) *
                (uint64_t)SCANNED_RECORDS_PUBLISH_STRIDE;
            atomic_fetch_add_explicit(&rs->scanned_records, delta, memory_order_relaxed);
            scanned_published += delta;
        }

        bytes_left_after_hdr = chunk->end_offset - (uint64_t)ftello(fp);
        if ((uint64_t)r.path_len > bytes_left_after_hdr) {
            fprintf(stderr, "warn: truncated record in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            break;
        }

        if (!ctx->aggregate_all_users && (uid_t)r.uid != ctx->target_uid) {
            if (r.path_len > 0 && fseeko(fp, (off_t)r.path_len, SEEK_CUR) != 0) {
                fprintf(stderr, "warn: seek failed skipping path in %s\n", chunk->path);
                atomic_fetch_add(&rs->bad_input_files, 1U);
                break;
            }
            continue;
        }

        pathbuf = (char *)malloc((size_t)r.path_len + 1);
        if (!pathbuf) {
            fprintf(stderr, "warn: path alloc failed in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            break;
        }
        if (r.path_len > 0 && mk_fread(pathbuf, 1, r.path_len, fp) != r.path_len) {
            fprintf(stderr, "warn: path read failed in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            free(pathbuf);
            break;
        }
        pathbuf[r.path_len] = '\0';

        {
            uint32_t *codes = NULL;
            size_t code_count = 0;
            if (extract_path_trigrams(pathbuf, &codes, &code_count, worker) != 0) {
                fprintf(stderr, "warn: failed to extract trigrams from %s\n", chunk->path);
                atomic_fetch_add(&rs->bad_input_files, 1U);
                free(pathbuf);
                break;
            }

            if (!batch) {
                batch = (write_batch_t *)calloc(1, sizeof(*batch));
                if (!batch) {
                    fprintf(stderr, "warn: failed to allocate write batch for %s\n", chunk->path);
                    atomic_fetch_add(&rs->bad_input_files, 1U);
                    free(codes);
                    free(pathbuf);
                    break;
                }
            }
            if (write_batch_append(batch, pathbuf, codes, code_count) != 0) {
                fprintf(stderr, "warn: failed to append write batch for %s\n", chunk->path);
                atomic_fetch_add(&rs->bad_input_files, 1U);
                free(codes);
                free(pathbuf);
                break;
            }
            pathbuf = NULL;
            codes = NULL;

            if (batch->count >= worker->write_batch_flush_at) {
                if (write_queue_push(write_queue, batch) != 0) {
                    fprintf(stderr, "warn: failed to queue write batch for %s\n", chunk->path);
                    atomic_fetch_add(&rs->bad_input_files, 1U);
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
    if (scanned_local > scanned_published)
        atomic_fetch_add_explicit(&rs->scanned_records, scanned_local - scanned_published, memory_order_relaxed);
    mk_fclose(fp);
    if (batch) {
        if (write_queue_push(write_queue, batch) != 0) {
            atomic_fetch_add(&rs->bad_input_files, 1U);
            write_batch_destroy(batch);
        }
    }
    finalize_chunk_file_progress(rs, file_states, chunk->file_index);
    return rc;
}

static void *worker_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;

    for (;;) {
        file_chunk_t *chunk = queue_pop(arg->queue);
        if (!chunk) break;
        if (atomic_load(&arg->ctx->run_stats->writer_failed)) break;
        process_chunk_make(arg, chunk);
    }

    free(arg->lower_seg_buf);
    arg->lower_seg_buf = NULL;
    arg->lower_seg_cap = 0;

    mk_io_tls_flush();
    return NULL;
}

static int write_final_path_offset(build_ctx_t *ctx) {
    uint64_t final_offset = (uint64_t)ftello(ctx->paths_fp);
    return mk_fwrite(&final_offset, sizeof(final_offset), 1, ctx->path_offsets_fp) == 1 ? 0 : -1;
}

static int write_meta_file(const build_ctx_t *ctx) {
    char path[PATH_MAX];
    FILE *fp;

    if (build_path(path, sizeof(path), ctx->index_dir, "meta.txt") != 0) return -1;
    fp = mk_fopen(path, "w");
    if (!fp) return -1;

    fprintf(fp, "ereport_index_version=%d\n", INDEX_VERSION);
    fprintf(fp, "user=%s\n", ctx->display_name);
    fprintf(fp, "aggregate_all_users=%d\n", ctx->aggregate_all_users ? 1 : 0);
    fprintf(fp, "uid=%lu\n", (unsigned long)ctx->target_uid);
    fprintf(fp, "input_layout=%s\n", g_input_layout);
    if (g_input_uid_shards) fprintf(fp, "input_uid_shards=%u\n", g_input_uid_shards);
    fprintf(fp, "input_files=%" PRIu64 "\n", ctx->input_files);
    fprintf(fp, "indexed_paths=%" PRIu64 "\n", ctx->indexed_paths);
    fprintf(fp, "unique_trigrams=%" PRIu64 "\n", ctx->unique_trigrams);
    fprintf(fp, "bucket_bits=%d\n", TRIGRAM_BUCKET_BITS);
    fprintf(fp, "bucket_count=%u\n", TRIGRAM_BUCKET_COUNT);

    if (mk_fclose(fp) != 0) return -1;
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
        mk_munmap(L->mmap_base, L->mmap_len);
    } else if (L->malloc_copy && L->records) {
        free(L->records);
    }
    memset(L, 0, sizeof(*L));
}

/* Max worker id + 1 from tmp_trigrams_*_w%04u.bin names (0 if only legacy tmp_trigrams_%04u.bin exists). */
static uint32_t discover_max_trigram_worker_shard(const char *index_dir) {
    DIR *d;
    struct dirent *e;
    uint32_t max_w = 0;

    d = opendir(index_dir);
    if (!d) return 0;
    while ((e = readdir(d)) != NULL) {
        unsigned bkt, wid;

        if (strncmp(e->d_name, "tmp_trigrams_", 13) != 0) continue;
        if (sscanf(e->d_name, "tmp_trigrams_%u_w%u.bin", &bkt, &wid) != 2) continue;
        if (wid + 1U > max_w) max_w = wid + 1U;
    }
    closedir(d);
    return max_w;
}

static void merge_ctx_ensure_trigram_shard_count(build_ctx_t *ctx) {
    if (ctx->trigram_tmp_shard_count > 0U) return;
    ctx->trigram_tmp_shard_count = discover_max_trigram_worker_shard(ctx->index_dir);
}

static void unlink_bucket_tmp_sources(build_ctx_t *ctx, uint32_t bucket) {
    char path[PATH_MAX], leg[PATH_MAX];
    uint32_t w, max_w;

    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;
    if (snprintf(leg, sizeof(leg), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(leg))
        goto shards;
    (void)unlink(leg);
shards:
    for (w = 0; w < max_w; w++) {
        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, w) >= (int)sizeof(path))
            continue;
        (void)unlink(path);
    }
}

/*
 * Load legacy tmp_trigrams_%04u.bin (if present) plus all tmp_trigrams_%04u_w%04u.bin shards into one buffer.
 * Record order is preserved per file; radix sort fixes global order.
 */
static int merge_load_bucket_tmp_files(build_ctx_t *ctx, uint32_t bucket, merge_loaded_bucket_t *out) {
    char path[PATH_MAX], leg[PATH_MAX];
    struct stat st;
    uint64_t total_bytes = 0;
    size_t total_n = 0;
    uint32_t w, max_w;
    unsigned char *buf = NULL;
    size_t pos = 0;
    int n_nonempty = 0;
    int leg_nonempty = 0;
    uint32_t lone_shard_w = 0;

    memset(out, 0, sizeof(*out));
    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;

    if (snprintf(leg, sizeof(leg), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(leg)) return -1;
    if (stat(leg, &st) == 0 && st.st_size > 0) {
        if ((st.st_size % (off_t)sizeof(trigram_record_t)) != 0) return -1;
        total_bytes += (uint64_t)st.st_size;
        total_n += (size_t)((uint64_t)st.st_size / sizeof(trigram_record_t));
        leg_nonempty = 1;
        n_nonempty++;
    }
    for (w = 0; w < max_w; w++) {
        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, w) >= (int)sizeof(path))
            return -1;
        if (stat(path, &st) != 0 || st.st_size == 0) continue;
        if ((st.st_size % (off_t)sizeof(trigram_record_t)) != 0) return -1;
        total_bytes += (uint64_t)st.st_size;
        total_n += (size_t)((uint64_t)st.st_size / sizeof(trigram_record_t));
        lone_shard_w = w;
        n_nonempty++;
    }

    if (total_n == 0) return -1;

    /* Single-file mmap (legacy-only or one shard only). */
    if (n_nonempty == 1 && leg_nonempty) {
        int fd;
        void *p;

        fd = mk_open(leg, O_RDONLY);
        if (fd < 0) return -1;
        if (fstat(fd, &st) != 0 || st.st_size <= 0) {
            close(fd);
            return -1;
        }
        p = mk_mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
        close(fd);
        if (p != MAP_FAILED) {
            out->mmap_base = p;
            out->mmap_len = (size_t)st.st_size;
            out->records = (trigram_record_t *)p;
            out->n = total_n;
            out->bytes = total_bytes;
            return 0;
        }
    } else if (n_nonempty == 1 && !leg_nonempty) {
        int fd;
        void *p;

        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, lone_shard_w) >=
            (int)sizeof(path))
            return -1;
        fd = mk_open(path, O_RDONLY);
        if (fd < 0) return -1;
        if (fstat(fd, &st) != 0 || st.st_size <= 0) {
            close(fd);
            return -1;
        }
        p = mk_mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
        close(fd);
        if (p != MAP_FAILED) {
            out->mmap_base = p;
            out->mmap_len = (size_t)st.st_size;
            out->records = (trigram_record_t *)p;
            out->n = total_n;
            out->bytes = total_bytes;
            return 0;
        }
    }

    buf = (unsigned char *)malloc((size_t)total_bytes);
    if (!buf) return -1;

    if (stat(leg, &st) == 0 && st.st_size > 0) {
        int fd = mk_open(leg, O_RDONLY);

        if (fd < 0) {
            free(buf);
            return -1;
        }
        if (mk_read(fd, buf + pos, (size_t)st.st_size) != (ssize_t)st.st_size) {
            close(fd);
            free(buf);
            return -1;
        }
        close(fd);
        pos += (size_t)st.st_size;
    }
    for (w = 0; w < max_w; w++) {
        int fd;

        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, w) >= (int)sizeof(path)) {
            free(buf);
            return -1;
        }
        if (stat(path, &st) != 0 || st.st_size == 0) continue;
        fd = mk_open(path, O_RDONLY);
        if (fd < 0) {
            free(buf);
            return -1;
        }
        if (mk_read(fd, buf + pos, (size_t)st.st_size) != (ssize_t)st.st_size) {
            close(fd);
            free(buf);
            return -1;
        }
        close(fd);
        pos += (size_t)st.st_size;
    }

    out->records = (trigram_record_t *)buf;
    out->malloc_copy = 1;
    out->n = total_n;
    out->bytes = total_bytes;
    return 0;
}

static uint64_t bucket_tmp_files_total_bytes(build_ctx_t *ctx, uint32_t bucket) {
    char path[PATH_MAX], leg[PATH_MAX];
    struct stat st;
    uint64_t sum = 0;
    uint32_t w, max_w;

    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;
    if (snprintf(leg, sizeof(leg), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(leg)) return 0;
    if (stat(leg, &st) == 0) sum += (uint64_t)st.st_size;
    for (w = 0; w < max_w; w++) {
        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, w) >= (int)sizeof(path))
            continue;
        if (stat(path, &st) == 0) sum += (uint64_t)st.st_size;
    }
    return sum;
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

/* When bucket_nonempty[] was not populated, discover non-empty merge buckets by stat()-ing tmp_trigrams files. */
static size_t merge_collect_nonempty_buckets_stat_scan(build_ctx_t *ctx, uint32_t **out_list) {
    char path[PATH_MAX], leg[PATH_MAX];
    struct stat st;
    size_t nb = 0;
    uint32_t i, w, max_w;

    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;

    for (i = 0; i < TRIGRAM_BUCKET_COUNT; i++) {
        uint64_t sz = 0;

        if (snprintf(leg, sizeof(leg), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, i) >= (int)sizeof(leg)) return 0;
        if (stat(leg, &st) == 0) sz += (uint64_t)st.st_size;
        for (w = 0; w < max_w; w++) {
            if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, i, w) >= (int)sizeof(path))
                return 0;
            if (stat(path, &st) == 0) sz += (uint64_t)st.st_size;
        }
        if (sz > 0) nb++;
    }
    *out_list = (uint32_t *)malloc(nb * sizeof(uint32_t));
    if (!*out_list) return 0;
    nb = 0;
    for (i = 0; i < TRIGRAM_BUCKET_COUNT; i++) {
        uint64_t sz = 0;

        if (snprintf(leg, sizeof(leg), "%s/tmp_trigrams_%04u.bin", ctx->index_dir, i) >= (int)sizeof(leg)) {
            free(*out_list);
            *out_list = NULL;
            return 0;
        }
        if (stat(leg, &st) == 0) sz += (uint64_t)st.st_size;
        for (w = 0; w < max_w; w++) {
            if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, i, w) >= (int)sizeof(path)) {
                free(*out_list);
                *out_list = NULL;
                return 0;
            }
            if (stat(path, &st) == 0) sz += (uint64_t)st.st_size;
        }
        if (sz == 0) continue;
        (*out_list)[nb++] = i;
    }
    return nb;
}

static uint64_t merge_largest_bucket_file_bytes(build_ctx_t *ctx, const uint32_t *bucket_list, size_t nbuckets) {
    size_t bi;
    uint64_t maxb = 0;

    merge_ctx_ensure_trigram_shard_count(ctx);
    for (bi = 0; bi < nbuckets; bi++) {
        uint64_t sz = bucket_tmp_files_total_bytes(ctx, bucket_list[bi]);

        if (sz > maxb) maxb = sz;
    }
    return maxb;
}

static uint64_t read_proc_memavailable_kib(void) {
    FILE *fp;
    char line[256];
    unsigned long kb = 0;

    fp = fopen("/proc/meminfo", "r");
    if (!fp) return 0;
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "MemAvailable:", 13) == 0) {
            if (sscanf(line + 13, " %lu", &kb) == 1) break;
        }
    }
    fclose(fp);
    return (uint64_t)kb;
}

/* cgroup v2 memory.max for this process, or 0 if unlimited / unreadable. */
static uint64_t cgroup_v2_memory_max_bytes(void) {
    FILE *f;
    char cgroup_line[4096];
    char maxpath[PATH_MAX + 64];
    char buf[80];
    char *rel;
    size_t len;

    f = fopen("/proc/self/cgroup", "r");
    if (!f) return 0;
    while (fgets(cgroup_line, sizeof(cgroup_line), f)) {
        if (strncmp(cgroup_line, "0::", 3) != 0) continue;
        rel = cgroup_line + 3;
        len = strlen(rel);
        while (len && (rel[len - 1] == '\n' || rel[len - 1] == '\r')) rel[--len] = '\0';
        fclose(f);
        if (len == 0) return 0;
        if (snprintf(maxpath, sizeof(maxpath), "/sys/fs/cgroup/%s/memory.max", rel) >= (int)sizeof(maxpath)) return 0;
        f = fopen(maxpath, "r");
        if (!f) return 0;
        if (!fgets(buf, sizeof(buf), f)) {
            fclose(f);
            return 0;
        }
        fclose(f);
        if (strncmp(buf, "max", 3) == 0) return 0;
        {
            unsigned long long v = strtoull(buf, NULL, 10);
            return (uint64_t)v;
        }
    }
    fclose(f);
    return 0;
}

/*
 * Conservative RAM budget for concurrent parallel merge workers.
 * Respects the smaller of MemAvailable and cgroup memory.max (user sessions often have a low cap).
 * Override: EREPORT_INDEX_MERGE_MEMORY_MB=<MiB> sets an explicit budget.
 *           EREPORT_INDEX_MERGE_RAM_FRAC=0.55 (fraction of min(mem,cgroup) to use; default 55%).
 */
static uint64_t merge_parallel_ram_budget_bytes(void) {
    const char *e;
    char *end;
    uint64_t mem_kib;
    uint64_t host_b;
    uint64_t cg_b;
    uint64_t base_b;
    uint64_t cap_b;
    unsigned long num = MERGE_RAM_FRAC_NUM;
    unsigned long den = MERGE_RAM_FRAC_DEN;

    e = getenv("EREPORT_INDEX_MERGE_MEMORY_MB");
    if (e && *e) {
        unsigned long mb = strtoul(e, &end, 10);
        if (end != e && mb > 0 && mb < (ULONG_MAX / (1024UL * 1024UL))) return (uint64_t)mb * 1024ULL * 1024ULL;
    }
    e = getenv("EREPORT_INDEX_MERGE_RAM_FRAC");
    if (e && *e) {
        double d = strtod(e, &end);
        if (end != e && d >= 0.05 && d <= 1.0) {
            num = (unsigned long)(d * 100.0 + 0.5);
            den = 100;
            if (num == 0 || num > den) {
                num = MERGE_RAM_FRAC_NUM;
                den = MERGE_RAM_FRAC_DEN;
            }
        }
    }

    mem_kib = read_proc_memavailable_kib();
    host_b = mem_kib * 1024ULL;
    cg_b = cgroup_v2_memory_max_bytes();

    if (cg_b > 0 && host_b > 0)
        base_b = host_b < cg_b ? host_b : cg_b;
    else if (cg_b > 0)
        base_b = cg_b;
    else
        base_b = host_b;

    cap_b = (base_b / den) * num;
    if (cap_b < 32ULL * 1024 * 1024) cap_b = 32ULL * 1024 * 1024;
    return cap_b;
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

/* Drop lone merge_seg_k / merge_seg_p left by a crash between creating the two files. */
static void merge_unlink_orphan_segment_halves(const build_ctx_t *ctx) {
    uint32_t b;
    char kpath[PATH_MAX], ppath[PATH_MAX];
    int hk, hp;

    for (b = 0; b < TRIGRAM_BUCKET_COUNT; b++) {
        if (snprintf(kpath, sizeof(kpath), "%s/merge_seg_k_%04u.bin", ctx->index_dir, b) >= (int)sizeof(kpath)) continue;
        if (snprintf(ppath, sizeof(ppath), "%s/merge_seg_p_%04u.bin", ctx->index_dir, b) >= (int)sizeof(ppath)) continue;
        hk = access(kpath, F_OK) == 0;
        hp = access(ppath, F_OK) == 0;
        if (hk && !hp) unlink(kpath);
        else if (!hk && hp) unlink(ppath);
    }
}

static int merge_bucket_to_segment_files(build_ctx_t *ctx, uint32_t bucket, merge_parallel_arg_t *accum) {
    char kseg[PATH_MAX];
    char pseg[PATH_MAX];
    merge_loaded_bucket_t L;
    trigram_record_t *aux = NULL;
    FILE *kf = NULL;
    FILE *pf = NULL;
    uint64_t u = 0;
    int rc = -1;

    if (snprintf(kseg, sizeof(kseg), "%s/merge_seg_k_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(kseg)) return -1;
    if (snprintf(pseg, sizeof(pseg), "%s/merge_seg_p_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(pseg)) return -1;

    if (merge_load_bucket_tmp_files(ctx, bucket, &L) != 0) return -1;
    {
        uint64_t peak = L.bytes * 2ULL;
        uint64_t cur = atomic_load_explicit(&ctx->merge_bucket_ram_peak, memory_order_relaxed);

        while (peak > cur) {
            if (atomic_compare_exchange_weak_explicit(&ctx->merge_bucket_ram_peak, &cur, peak, memory_order_relaxed,
                                                      memory_order_relaxed))
                break;
        }
    }
    aux = (trigram_record_t *)malloc(L.n * sizeof(*aux));
    if (!aux) {
        merge_loaded_bucket_destroy(&L);
        return -1;
    }
    radix_sort_trigram_records(L.records, L.n, aux);
    free(aux);

    kf = mk_fopen(kseg, "wb");
    pf = mk_fopen(pseg, "wb");
    if (!kf || !pf) goto err;
    if (setvbuf(kf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (setvbuf(pf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    if (write_sorted_bucket_records(kf, pf, L.records, L.n, &u) != 0) goto err;
    if (mk_fclose(kf) != 0) {
        kf = NULL;
        goto err;
    }
    kf = NULL;
    if (mk_fclose(pf) != 0) {
        pf = NULL;
        goto err;
    }
    pf = NULL;
    if (accum) {
        atomic_fetch_add_explicit(&accum->bytes_in, L.bytes, memory_order_relaxed);
        atomic_fetch_add_explicit(&accum->records_in, (unsigned long long)L.n, memory_order_relaxed);
    }
    merge_loaded_bucket_destroy(&L);
    unlink_bucket_tmp_sources(ctx, bucket);
    return 0;

err:
    if (kf) mk_fclose(kf);
    if (pf) mk_fclose(pf);
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
    mk_io_tls_flush();
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

        kf = mk_fopen(kpath, "rb");
        pf = mk_fopen(ppath, "rb");
        if (!kf || !pf) {
            if (kf) mk_fclose(kf);
            if (pf) mk_fclose(pf);
            free(iobuf);
            return -1;
        }
        if (setvbuf(kf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
        }
        if (setvbuf(pf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
        }

        for (k = 0; k < nk; k++) {
            trigram_key_t key;
            if (mk_fread(&key, sizeof(key), 1, kf) != 1) {
                mk_fclose(kf);
                mk_fclose(pf);
                free(iobuf);
                return -1;
            }
            key.postings_offset += post_base;
            if (mk_fwrite(&key, sizeof(key), 1, keys_fp) != 1) {
                mk_fclose(kf);
                mk_fclose(pf);
                free(iobuf);
                return -1;
            }
            (*unique_total)++;
        }
        mk_fclose(kf);

        {
            uint64_t psz = (uint64_t)sp.st_size;
            size_t nread;

            for (;;) {
                nread = mk_fread(iobuf, 1, MERGE_IO_BUFSIZE, pf);
                if (nread == 0) break;
                if (mk_fwrite(iobuf, 1, nread, postings_fp) != nread) {
                    mk_fclose(pf);
                    free(iobuf);
                    return -1;
                }
            }
            if (ferror(pf)) {
                mk_fclose(pf);
                free(iobuf);
                return -1;
            }
            post_base += psz;
        }
        mk_fclose(pf);
        unlink(kpath);
        unlink(ppath);
    }

    free(iobuf);
    return 0;
}

static uint64_t path_offsets_indexed_path_count(const char *index_dir) {
    char path[PATH_MAX];
    struct stat st;
    uint64_t n;

    if (build_path(path, sizeof(path), index_dir, "path_offsets.bin") != 0) return 0;
    if (stat(path, &st) != 0) return 0;
    if (st.st_size < (off_t)sizeof(uint64_t) || (st.st_size % (off_t)sizeof(uint64_t)) != 0) return 0;
    n = (uint64_t)(st.st_size / (off_t)sizeof(uint64_t));
    if (n == 0) return 0;
    return n - 1ULL;
}

/* Buckets that still have tmp_trigrams data (must run tmp → merge_seg). */
static size_t merge_resume_list_tmp_buckets(build_ctx_t *ctx, uint32_t *out, size_t out_cap) {
    uint32_t b;
    size_t n = 0;

    merge_ctx_ensure_trigram_shard_count(ctx);
    for (b = 0; b < TRIGRAM_BUCKET_COUNT; b++) {
        uint64_t sz = bucket_tmp_files_total_bytes(ctx, b);

        if (sz == 0) continue;
        if (n < out_cap) out[n++] = b;
    }
    if (n > 1) qsort(out, n, sizeof(uint32_t), cmp_u32);
    return n;
}

static int merge_any_merge_seg_k_nonempty(const build_ctx_t *ctx) {
    uint32_t b;
    char kpath[PATH_MAX];
    struct stat st;

    for (b = 0; b < TRIGRAM_BUCKET_COUNT; b++) {
        if (snprintf(kpath, sizeof(kpath), "%s/merge_seg_k_%04u.bin", ctx->index_dir, b) >= (int)sizeof(kpath)) continue;
        if (stat(kpath, &st) == 0 && st.st_size > 0) return 1;
    }
    return 0;
}

/* Buckets with completed merge_seg pair (ready for stitch). */
static int merge_list_segment_bucket_ids(const build_ctx_t *ctx, uint32_t *out, size_t *out_n, size_t out_cap) {
    uint32_t b;
    char kpath[PATH_MAX], ppath[PATH_MAX];
    struct stat sk, sp;
    size_t n = 0;

    for (b = 0; b < TRIGRAM_BUCKET_COUNT; b++) {
        if (snprintf(kpath, sizeof(kpath), "%s/merge_seg_k_%04u.bin", ctx->index_dir, b) >= (int)sizeof(kpath)) return -1;
        if (snprintf(ppath, sizeof(ppath), "%s/merge_seg_p_%04u.bin", ctx->index_dir, b) >= (int)sizeof(ppath)) return -1;
        if (stat(kpath, &sk) != 0 || stat(ppath, &sp) != 0) continue;
        if (sk.st_size == 0 && sp.st_size == 0) continue;
        if (sk.st_size % (off_t)sizeof(trigram_key_t) != 0) {
            fprintf(stderr, "ereport_index: invalid merge_seg_k_%04u.bin size in %s\n", b, ctx->index_dir);
            return -1;
        }
        if (n >= out_cap) return -1;
        out[n++] = b;
    }
    if (n > 1) qsort(out, n, sizeof(uint32_t), cmp_u32);
    *out_n = n;
    return 0;
}

/*
 * Finish merge after OOM/interrupt: paths.bin + path_offsets.bin must exist.
 * Deletes tri_keys.bin / tri_postings.bin and rebuilds them from tmp_trigrams_*.bin (if any)
 * and merge_seg_* segment files.
 */
static int process_trigram_buckets_resume(build_ctx_t *ctx) {
    char key_path[PATH_MAX], postings_path[PATH_MAX];
    FILE *keys_fp = NULL;
    FILE *postings_fp = NULL;
    uint32_t need_merge[TRIGRAM_BUCKET_COUNT];
    uint32_t stitch_buckets[TRIGRAM_BUCKET_COUNT];
    uint32_t *parallel_list = NULL;
    size_t n_need;
    size_t n_stitch = 0;
    uint64_t unique_trigrams = 0;
    int rc = -1;
    double merge_wall_start;
    uint64_t merge_bytes_temp = 0;
    uint64_t merge_records_in = 0;
    long ncpu;
    int merge_workers = 1;
    size_t bi;

    merge_wall_start = now_sec();
    ctx->last_rate_sec = merge_wall_start;
    ctx->last_rate_merge_units = 0;
    ctx->last_status_sec = 0.0;
    ctx->merge_workers_used = 1;
    ctx->merge_workers_cpu = 1;

    if (g_verbose) fprintf(stderr, "ereport_index: resuming merge in %s\n", ctx->index_dir);

    if (build_path(key_path, sizeof(key_path), ctx->index_dir, "tri_keys.bin") != 0 ||
        build_path(postings_path, sizeof(postings_path), ctx->index_dir, "tri_postings.bin") != 0)
        return -1;

    n_need = merge_resume_list_tmp_buckets(ctx, need_merge, TRIGRAM_BUCKET_COUNT);
    {
        struct stat st_tri;
        int has_tri = (stat(key_path, &st_tri) == 0 && st_tri.st_size > 0);

        if (n_need > 0 && has_tri && !merge_any_merge_seg_k_nonempty(ctx)) {
            fprintf(stderr,
                    "ereport_index: cannot resume-merge: tmp_trigrams_*.bin remain but tri_keys.bin exists and there are "
                    "no merge_seg_*.bin files (merge used the single-thread path). Re-run a full `ereport_index --make`.\n");
            return -1;
        }
    }

    unlink(key_path);
    unlink(postings_path);
    merge_unlink_orphan_segment_halves(ctx);

    ctx->merge_max_bucket_bytes = merge_largest_bucket_file_bytes(ctx, need_merge, n_need);
    ctx->merge_parallel_budget_bytes = merge_parallel_ram_budget_bytes();

    ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (ncpu < 1) ncpu = 4;
    if (ncpu > MERGE_MAX_WORKERS) ncpu = MERGE_MAX_WORKERS;
    if (n_need == 0) {
        merge_workers = 1;
    } else {
        merge_workers = (int)ncpu;
        if ((size_t)merge_workers > n_need) merge_workers = (int)n_need;
        if (n_need < (size_t)MERGE_PARALLEL_MIN) merge_workers = 1;
    }

    ctx->merge_workers_cpu = merge_workers;

    if (merge_workers > 1 && ctx->merge_max_bucket_bytes > 0) {
        uint64_t per_worker = ctx->merge_max_bucket_bytes * 2ULL + MERGE_PER_WORKER_OVERHEAD_BYTES;
        int mw = (int)(ctx->merge_parallel_budget_bytes / per_worker);
        if (mw < 1) mw = 1;
        if (mw < merge_workers) {
            if (g_verbose) {
                fprintf(stderr,
                        "ereport_index: parallel merge workers %d -> %d "
                        "(max bucket %.2f GiB, merge RAM budget ~%.2f GiB — a fraction of MemAvailable/cgroup, not full RAM; resume)\n"
                        "  Each worker needs ~2× max bucket size in RAM. EREPORT_INDEX_THREADS only affects `--make` "
                        "indexing. For more workers: EREPORT_INDEX_MERGE_RAM_FRAC or EREPORT_INDEX_MERGE_MEMORY_MB.\n",
                        merge_workers, mw, (double)ctx->merge_max_bucket_bytes / (1024.0 * 1024.0 * 1024.0),
                        (double)ctx->merge_parallel_budget_bytes / (1024.0 * 1024.0 * 1024.0));
            }
            merge_workers = mw;
        }
    }

    ctx->merge_workers_used = merge_workers;

    if (n_need > 0) {
        if (merge_workers <= 1) {
            for (bi = 0; bi < n_need; bi++) {
                uint32_t bkt = need_merge[bi];
                uint64_t tbs = bucket_tmp_files_total_bytes(ctx, bkt);

                if (g_verbose) {
                    if (tbs > 0) {
                        fprintf(stderr,
                                "ereport_index: tmp→seg bucket %04u (%zu/%zu) %.2f GiB — mmap, radix sort, write "
                                "(one CPU busy; first huge bucket can take tens of minutes)…\n",
                                bkt, bi + 1, n_need, (double)tbs / (1024.0 * 1024.0 * 1024.0));
                    } else {
                        fprintf(stderr, "ereport_index: tmp→seg bucket %04u (%zu/%zu)…\n", bkt, bi + 1, n_need);
                    }
                    fflush(stderr);
                }
                if (merge_bucket_to_segment_files(ctx, bkt, NULL) != 0) goto out;
                if (g_verbose) {
                    fprintf(stderr, "ereport_index: bucket %04u tmp→seg finished.\n", bkt);
                    fflush(stderr);
                }
                maybe_emit_status(ctx, "merge", (uint64_t)bi + 1U, (uint64_t)n_need);
            }
        } else {
            merge_parallel_arg_t mp;
            pthread_t *threads = NULL;
            int ti;

            parallel_list = (uint32_t *)malloc(n_need * sizeof(uint32_t));
            if (!parallel_list) goto out;
            memcpy(parallel_list, need_merge, n_need * sizeof(uint32_t));

            memset(&mp, 0, sizeof(mp));
            mp.ctx = ctx;
            mp.buckets = parallel_list;
            mp.bucket_count = n_need;
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
                for (bi = 0; bi < n_need; bi++) merge_unlink_segment_pair(ctx, need_merge[bi]);
                goto out;
            }
            merge_bytes_temp = (uint64_t)atomic_load(&mp.bytes_in);
            merge_records_in = (uint64_t)atomic_load(&mp.records_in);
        }
    }

    if (merge_list_segment_bucket_ids(ctx, stitch_buckets, &n_stitch, TRIGRAM_BUCKET_COUNT) != 0) goto out;

    keys_fp = mk_fopen(key_path, "wb");
    postings_fp = mk_fopen(postings_path, "wb");
    if (!keys_fp || !postings_fp) goto out;
    if (setvbuf(keys_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (setvbuf(postings_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    if (merge_stitch_segments(ctx, stitch_buckets, n_stitch, keys_fp, postings_fp, &unique_trigrams) != 0) goto out;
    rc = 0;

out:
    ctx->unique_trigrams = unique_trigrams;
    ctx->merge_phase_sec = now_sec() - merge_wall_start;
    ctx->merge_buckets_nonempty = (uint32_t)n_stitch;
    ctx->merge_buckets_skipped = (uint32_t)(TRIGRAM_BUCKET_COUNT - n_stitch);
    ctx->merge_trigram_records_read = merge_records_in;
    ctx->merge_bytes_temp_read = merge_bytes_temp;
    ctx->merge_bytes_tri_keys_written = keys_fp ? (uint64_t)ftello(keys_fp) : 0U;
    ctx->merge_bytes_tri_postings_written = postings_fp ? (uint64_t)ftello(postings_fp) : 0U;
    free(parallel_list);
    parallel_list = NULL;
    if (keys_fp) mk_fclose(keys_fp);
    if (postings_fp) mk_fclose(postings_fp);
    if (rc == 0) return 0;
    return -1;
}

static int resume_merge_index_dir(const char *index_dir) {
    build_ctx_t ctx;
    char paths_p[PATH_MAX], off_p[PATH_MAX];
    struct stat st;
    uint64_t ipc;
    double t0 = now_sec();
    memlog_shared_t ml_storage;
    pthread_t memlog_tid;
    int memlog_started = 0;
    char logpath[PATH_MAX];

    memset(&ctx, 0, sizeof(ctx));
    atomic_init(&ctx.merge_bucket_ram_peak, 0);
    if (snprintf(ctx.index_dir, sizeof(ctx.index_dir), "%s", index_dir) >= (int)sizeof(ctx.index_dir)) {
        fprintf(stderr, "ereport_index: index directory path too long\n");
        return 1;
    }
    if (build_path(paths_p, sizeof(paths_p), index_dir, "paths.bin") != 0 ||
        build_path(off_p, sizeof(off_p), index_dir, "path_offsets.bin") != 0) {
        return 1;
    }
    if (stat(paths_p, &st) != 0 || !S_ISREG(st.st_mode)) {
        fprintf(stderr, "ereport_index: missing %s (need completed index phase before resume-merge)\n", paths_p);
        return 1;
    }
    if (stat(off_p, &st) != 0 || !S_ISREG(st.st_mode)) {
        fprintf(stderr, "ereport_index: missing %s (need completed index phase before resume-merge)\n", off_p);
        return 1;
    }

    ipc = path_offsets_indexed_path_count(index_dir);
    ctx.indexed_paths = ipc;
    ctx.input_files = ipc > 0 ? ipc : 1ULL;
    ctx.scanned_records = 0;
    ctx.trigram_records = 0;
    ctx.bad_input_files = 0;
    ctx.start_sec = t0;
    ctx.last_status_sec = 0.0;
    ctx.last_rate_sec = t0;
    ctx.last_rate_indexed_paths = 0;
    ctx.last_rate_merge_units = 0;
    snprintf(ctx.display_name, sizeof(ctx.display_name), "%s", "resume");
    ctx.target_uid = (uid_t)0;
    ctx.aggregate_all_users = 0;
    ctx.run_stats = NULL;

    memset(&ml_storage, 0, sizeof(ml_storage));
    ml_storage.ctx = &ctx;
    ml_storage.wq = NULL;
    ml_storage.tq = NULL;
    ml_storage.chunkq = NULL;
    atomic_init(&ml_storage.stop, 0);
    if (g_verbose && build_path(logpath, sizeof(logpath), index_dir, "ereport_index.log") == 0) {
        ml_storage.fp = fopen(logpath, "a");
        if (ml_storage.fp) {
            if (pthread_create(&memlog_tid, NULL, memlog_thread_main, &ml_storage) != 0) {
                fclose(ml_storage.fp);
                ml_storage.fp = NULL;
            } else {
                memlog_started = 1;
            }
        }
    }

    if (process_trigram_buckets_resume(&ctx) != 0) {
        memlog_shutdown(&ml_storage, &memlog_tid, &memlog_started);
        fprintf(stderr, "ereport_index: resume-merge failed in %s\n", index_dir);
        return 1;
    }
    memlog_shutdown(&ml_storage, &memlog_tid, &memlog_started);
    if (write_meta_file(&ctx) != 0) {
        fprintf(stderr, "ereport_index: could not write meta.txt in %s\n", index_dir);
        return 1;
    }

    printf("mode=resume_merge\n");
    printf("index_dir=%s\n", ctx.index_dir);
    printf("indexed_paths=%" PRIu64 "\n", ctx.indexed_paths);
    printf("unique_trigrams=%" PRIu64 "\n", ctx.unique_trigrams);
    printf("merge_phase_sec=%.3f\n", ctx.merge_phase_sec);
    printf("merge_buckets_nonempty=%u\n", ctx.merge_buckets_nonempty);
    printf("merge_workers=%d\n", ctx.merge_workers_used);
    printf("elapsed_sec=%.3f\n", now_sec() - t0);
    if (g_verbose) {
        printf("merge_bytes_tri_keys_written=%" PRIu64 "\n", ctx.merge_bytes_tri_keys_written);
        printf("merge_bytes_tri_postings_written=%" PRIu64 "\n", ctx.merge_bytes_tri_postings_written);
    }
    return 0;
}

static int process_trigram_buckets(build_ctx_t *ctx) {
    char key_path[PATH_MAX], postings_path[PATH_MAX];
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
    if (nbuckets == 0) nbuckets = merge_collect_nonempty_buckets_stat_scan(ctx, &bucket_list);
    merge_skipped = (uint32_t)(TRIGRAM_BUCKET_COUNT - nbuckets);

    if (build_path(key_path, sizeof(key_path), ctx->index_dir, "tri_keys.bin") != 0 ||
        build_path(postings_path, sizeof(postings_path), ctx->index_dir, "tri_postings.bin") != 0) {
        free(bucket_list);
        return -1;
    }

    keys_fp = mk_fopen(key_path, "wb");
    postings_fp = mk_fopen(postings_path, "wb");
    if (!keys_fp || !postings_fp) goto out;
    if (setvbuf(keys_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (setvbuf(postings_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    ctx->merge_max_bucket_bytes = merge_largest_bucket_file_bytes(ctx, bucket_list, nbuckets);
    ctx->merge_parallel_budget_bytes = merge_parallel_ram_budget_bytes();

    ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (ncpu < 1) ncpu = 4;
    if (ncpu > MERGE_MAX_WORKERS) ncpu = MERGE_MAX_WORKERS;
    merge_workers = (int)ncpu;
    if ((size_t)merge_workers > nbuckets) merge_workers = (int)nbuckets;
    if (nbuckets < (size_t)MERGE_PARALLEL_MIN) merge_workers = 1;

    ctx->merge_workers_cpu = merge_workers;

    if (merge_workers > 1 && ctx->merge_max_bucket_bytes > 0) {
        uint64_t per_worker = ctx->merge_max_bucket_bytes * 2ULL + MERGE_PER_WORKER_OVERHEAD_BYTES;
        int mw = (int)(ctx->merge_parallel_budget_bytes / per_worker);
        if (mw < 1) mw = 1;
        if (mw < merge_workers) {
            if (g_verbose) {
                fprintf(stderr,
                        "ereport_index: parallel merge workers %d -> %d "
                        "(max bucket %.2f GiB, merge RAM budget ~%.2f GiB; "
                        "EREPORT_INDEX_MERGE_MEMORY_MB / EREPORT_INDEX_MERGE_RAM_FRAC to adjust)\n"
                        "  EREPORT_INDEX_THREADS applies to `--make` indexing only, not merge.\n",
                        merge_workers, mw, (double)ctx->merge_max_bucket_bytes / (1024.0 * 1024.0 * 1024.0),
                        (double)ctx->merge_parallel_budget_bytes / (1024.0 * 1024.0 * 1024.0));
            }
            merge_workers = mw;
        }
    }

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

            if (merge_load_bucket_tmp_files(ctx, bucket, &L) != 0) goto out;
            {
                uint64_t peak = L.bytes * 2ULL;
                uint64_t cur = atomic_load_explicit(&ctx->merge_bucket_ram_peak, memory_order_relaxed);

                while (peak > cur) {
                    if (atomic_compare_exchange_weak_explicit(&ctx->merge_bucket_ram_peak, &cur, peak, memory_order_relaxed,
                                                              memory_order_relaxed))
                        break;
                }
            }
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
            unlink_bucket_tmp_sources(ctx, bucket);
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
    if (keys_fp) mk_fclose(keys_fp);
    if (postings_fp) mk_fclose(postings_fp);
    if (rc == 0) return 0;
    return -1;
}

static int build_index_dir(const char *user_spec,
                           const char **dirpaths,
                           size_t dirpath_count,
                           int all_users_mode,
                           const char *index_dir_override) {
    uid_t target_uid;
    char display_name[256];
    char sanitized_name[256];
    char dirs_label[4096];
    char paths_path[PATH_MAX], offsets_path[PATH_MAX];
    char **paths = NULL;
    size_t path_count = 0;
    file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    file_state_t *file_states = NULL;
    work_queue_t queue;
    write_queue_t write_queue;
    trigram_job_queue_t trigram_queue;
    pthread_t *tids = NULL;
    worker_arg_t *args = NULL;
    paths_writer_arg_t paths_writer_arg;
    trigram_worker_arg_t *tw_worker_args = NULL;
    pthread_t stats_thread;
    pthread_t paths_writer_thread;
    pthread_t memlog_tid;
    memlog_shared_t ml_storage;
    int memlog_started = 0;
    pthread_t *trigram_tids = NULL;
    build_ctx_t ctx;
    index_run_stats_t run_stats;
    double t0 = now_sec();
    double t1;
    double chunk_prep_sec = 0.0;
    int stats_thread_started = 0;
    int threads = parse_index_thread_count();
    int trigram_threads = parse_trigram_thread_count(threads);
    size_t trigram_queue_depth_cfg = parse_trigram_queue_depth(trigram_threads);
    int threads_used = 0;
    int trigram_threads_used = 0;
    size_t i;
    struct rusage ru_make_start, ru_after_prep, ru_after_index, ru_after_merge;
    int ru_have_start = 0, ru_have_prep = 0, ru_have_index = 0, ru_have_merge = 0;

    memset(&ctx, 0, sizeof(ctx));
    atomic_init(&ctx.merge_bucket_ram_peak, 0);
    memset(&queue, 0, sizeof(queue));
    memset(&write_queue, 0, sizeof(write_queue));
    memset(&trigram_queue, 0, sizeof(trigram_queue));
    write_queue.run_stats = &run_stats;
    trigram_queue.run_stats = &run_stats;
    memset(&paths_writer_arg, 0, sizeof(paths_writer_arg));

    g_write_batch_paths_base = parse_write_batch_paths();

    if (!all_users_mode) {
        if (!user_spec) {
            fprintf(stderr, "ereport_index: internal error: missing user_spec\n");
            return 1;
        }
        if (resolve_target_user(user_spec, &target_uid, display_name, sizeof(display_name)) != 0) {
            fprintf(stderr, "unknown user or uid: %s\n", user_spec);
            return 1;
        }
    } else {
        target_uid = (uid_t)0;
        if (snprintf(display_name, sizeof(display_name), "all_users") >= (int)sizeof(display_name)) {
            fprintf(stderr, "ereport_index: display name buffer\n");
            return 1;
        }
    }

    snprintf(sanitized_name, sizeof(sanitized_name), "%s", display_name);
    sanitize_name(sanitized_name);
    if (index_dir_override && index_dir_override[0] != '\0') {
        if (snprintf(ctx.index_dir, sizeof(ctx.index_dir), "%s", index_dir_override) >= (int)sizeof(ctx.index_dir)) {
            fprintf(stderr, "index directory path too long\n");
            return 1;
        }
    } else if (snprintf(ctx.index_dir, sizeof(ctx.index_dir), "%s/index", sanitized_name) >= (int)sizeof(ctx.index_dir)) {
        fprintf(stderr, "index directory path too long\n");
        return 1;
    }
    ctx.target_uid = target_uid;
    ctx.aggregate_all_users = all_users_mode ? 1 : 0;
    snprintf(ctx.display_name, sizeof(ctx.display_name), "%s", display_name);
    ctx.start_sec = t0;
    ctx.last_status_sec = 0.0;
    ctx.last_rate_sec = t0;
    ctx.last_rate_indexed_paths = 0;
    ctx.last_rate_merge_units = 0;
    index_run_stats_reset(&run_stats);
    run_stats.run_start_sec = t0;
    ctx.run_stats = &run_stats;

    if (scan_dirs_collect_files(dirpaths, dirpath_count, target_uid, all_users_mode, &paths, &path_count) != 0)
        return 1;

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

    if (getrusage(RUSAGE_SELF, &ru_make_start) == 0) ru_have_start = 1;
    make_io_reset();

    {
        double t_chunk_prep0 = now_sec();
        uint64_t *chunk_targets = (uint64_t *)calloc(path_count, sizeof(uint64_t));
        int *prep_rc = (int *)calloc(path_count, sizeof(int));
        file_chunk_t **prep_chunks = (file_chunk_t **)calloc(path_count, sizeof(*prep_chunks));
        size_t *prep_chunk_counts = (size_t *)calloc(path_count, sizeof(size_t));
        chunk_prep_pool_t pool;
        pthread_t *prep_tids = NULL;
        int prep_threads;
        size_t merge_off;

        if (!chunk_targets || !prep_rc || !prep_chunks || !prep_chunk_counts) {
            fprintf(stderr, "allocation failed\n");
            free(chunk_targets);
            free(prep_rc);
            free(prep_chunks);
            free(prep_chunk_counts);
            for (i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            free(file_states);
            return 1;
        }

        for (i = 0; i < path_count; i++) {
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
            for (i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            free(file_states);
            return 1;
        }
        stats_thread_started = 1;

        prep_threads = threads;
        if ((size_t)prep_threads > path_count) prep_threads = (int)path_count;

        if (g_verbose) {
            fprintf(stderr, "ereport_index: mapping chunk boundaries using %d parallel scanner(s)...\n", prep_threads);
            fflush(stderr);
        }

        memset(&pool, 0, sizeof(pool));
        pool.paths = paths;
        pool.chunk_targets = chunk_targets;
        pool.path_count = path_count;
        pool.prep_rc = prep_rc;
        pool.prep_chunks = prep_chunks;
        pool.prep_chunk_counts = prep_chunk_counts;
        pool.run_stats = &run_stats;
        atomic_store(&pool.next_path_index, 0);

        prep_tids = (pthread_t *)calloc((size_t)prep_threads, sizeof(*prep_tids));
        if (!prep_tids) {
            fprintf(stderr, "allocation failed\n");
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
            free(chunk_targets);
            free(prep_rc);
            free(prep_chunks);
            free(prep_chunk_counts);
            for (i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            free(file_states);
            return 1;
        }

        for (i = 0; i < (size_t)prep_threads; i++) {
            if (pthread_create(&prep_tids[i], NULL, chunk_prep_worker_main, &pool) != 0) {
                size_t j;
                fprintf(stderr, "failed to create chunk-prep thread\n");
                atomic_store(&run_stats.stop_stats, 1);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
                for (j = 0; j < i; j++) pthread_join(prep_tids[j], NULL);
                free(prep_tids);
                free(chunk_targets);
                free(prep_rc);
                free(prep_chunks);
                free(prep_chunk_counts);
                for (j = 0; j < path_count; j++) free(paths[j]);
                free(paths);
                free(file_states);
                return 1;
            }
        }

        for (i = 0; i < (size_t)prep_threads; i++) pthread_join(prep_tids[i], NULL);
        free(prep_tids);

        ctx.input_files = 0;
        ctx.bad_input_files = 0;
        for (i = 0; i < path_count; i++) {
            if (prep_rc[i] != 0) {
                atomic_store(&file_states[i].remaining_chunks, 0);
                ctx.bad_input_files++;
                if (prep_chunks[i]) {
                    free_chunk_array_rows(prep_chunks[i], prep_chunk_counts[i]);
                    prep_chunks[i] = NULL;
                }
            } else {
                ctx.input_files++;
            }
        }

        chunk_count = 0;
        for (i = 0; i < path_count; i++) {
            if (prep_rc[i] == 0) chunk_count += prep_chunk_counts[i];
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
                for (i = 0; i < path_count; i++) {
                    if (prep_rc[i] == 0 && prep_chunks[i]) free_chunk_array_rows(prep_chunks[i], prep_chunk_counts[i]);
                }
                free(chunk_targets);
                free(prep_rc);
                free(prep_chunks);
                free(prep_chunk_counts);
                for (i = 0; i < path_count; i++) free(paths[i]);
                free(paths);
                free(file_states);
                return 1;
            }

            merge_off = 0;
            for (i = 0; i < path_count; i++) {
                if (prep_rc[i] != 0) continue;
                memcpy(merged + merge_off, prep_chunks[i], prep_chunk_counts[i] * sizeof(file_chunk_t));
                merge_off += prep_chunk_counts[i];
                free(prep_chunks[i]);
                prep_chunks[i] = NULL;
                atomic_store(&file_states[i].remaining_chunks,
                             prep_chunk_counts[i] > (size_t)UINT_MAX ? UINT_MAX : (unsigned int)prep_chunk_counts[i]);
            }
            chunks = merged;
            {
                uint64_t pt = 0;

                for (i = 0; i < chunk_count; i++) pt += strlen(chunks[i].path);
                ctx.chunk_path_bytes_total = pt;
                ctx.chunk_total_count = chunk_count;
            }
        }

        free(chunk_targets);
        free(prep_rc);
        free(prep_chunks);
        free(prep_chunk_counts);

        run_stats.chunk_prep_files_total = 0;
        atomic_store(&run_stats.chunk_prep_files_done, 0ULL);
        chunk_prep_sec = now_sec() - t_chunk_prep0;
        if (getrusage(RUSAGE_SELF, &ru_after_prep) == 0) ru_have_prep = 1;
    }

    atomic_store(&run_stats.bad_input_files, (unsigned long long)ctx.bad_input_files);

    if (chunk_count == 0) {
        fprintf(stderr, "no readable chunk work found in %s\n", dirs_label);
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        free(file_states);
        free(chunks);
        return 1;
    }

    if ((!index_dir_override || index_dir_override[0] == '\0') && ensure_dir_recursive(sanitized_name) != 0) {
        fprintf(stderr, "failed to create %s: %s\n", sanitized_name, strerror(errno));
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }
    if (ensure_dir_recursive(ctx.index_dir) != 0) {
        fprintf(stderr, "failed to create %s: %s\n", ctx.index_dir, strerror(errno));
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    if (build_path(paths_path, sizeof(paths_path), ctx.index_dir, "paths.bin") != 0 ||
        build_path(offsets_path, sizeof(offsets_path), ctx.index_dir, "path_offsets.bin") != 0) {
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    ctx.paths_fp = mk_fopen(paths_path, "wb");
    ctx.path_offsets_fp = mk_fopen(offsets_path, "wb");
    if (ctx.paths_fp && setvbuf(ctx.paths_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (ctx.path_offsets_fp && setvbuf(ctx.path_offsets_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (!ctx.paths_fp || !ctx.path_offsets_fp) {
        fprintf(stderr, "failed to initialize index outputs in %s\n", ctx.index_dir);
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        if (ctx.paths_fp) mk_fclose(ctx.paths_fp);
        if (ctx.path_offsets_fp) mk_fclose(ctx.path_offsets_fp);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    if (parallel_bucket_io_init(&ctx, (uint32_t)trigram_threads) != 0) {
        fprintf(stderr, "ereport_index: failed to initialize parallel tmp_trigram I/O\n");
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        mk_fclose(ctx.paths_fp);
        mk_fclose(ctx.path_offsets_fp);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    if (g_verbose) {
        fprintf(stderr,
                "ereport_index: make: parse_workers=%d trigram_workers=%d trigram_job_queue_depth=%zu write_batch_paths=%zu\n",
                threads,
                trigram_threads,
                trigram_queue_depth_cfg,
                g_write_batch_paths_base);
        fflush(stderr);
    }
    atomic_store_explicit(&run_stats.chunks_index_done, 0ULL, memory_order_relaxed);
    run_stats.index_chunks_total = (uint64_t)chunk_count;
    if (g_verbose) {
        fprintf(stderr,
                "ereport_index: indexing %zu parallel chunk task(s); stderr updates every ~1s (chunks, rec, idx, tri, ...)\n",
                chunk_count);
        fflush(stderr);
    }
    atomic_store_explicit(&run_stats.stats_wake, 1, memory_order_relaxed);

    queue.chunks = chunks;
    queue.count = chunk_count;
    queue.next_index = 0;
    pthread_mutex_init(&queue.mutex, NULL);
    write_queue.max_depth = compute_write_queue_max_batches(threads);
    pthread_mutex_init(&write_queue.mutex, NULL);
    pthread_cond_init(&write_queue.has_batch, NULL);
    pthread_cond_init(&write_queue.has_space, NULL);

    trigram_queue.max_depth = trigram_queue_depth_cfg;
    pthread_mutex_init(&trigram_queue.mutex, NULL);
    pthread_cond_init(&trigram_queue.has_job, NULL);
    pthread_cond_init(&trigram_queue.has_space, NULL);

    threads_used = threads;
    tids = (pthread_t *)calloc((size_t)threads, sizeof(*tids));
    args = (worker_arg_t *)calloc((size_t)threads, sizeof(*args));
    if (!tids || !args) {
        fprintf(stderr, "allocation failed\n");
        free(tids);
        free(args);
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        parallel_bucket_io_shutdown(&ctx);
        mk_fclose(ctx.paths_fp);
        mk_fclose(ctx.path_offsets_fp);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.has_batch);
        pthread_cond_destroy(&write_queue.has_space);
        pthread_mutex_destroy(&trigram_queue.mutex);
        pthread_cond_destroy(&trigram_queue.has_job);
        pthread_cond_destroy(&trigram_queue.has_space);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    trigram_tids = (pthread_t *)calloc((size_t)trigram_threads, sizeof(*trigram_tids));
    if (!trigram_tids) {
        fprintf(stderr, "allocation failed\n");
        free(tids);
        free(args);
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        parallel_bucket_io_shutdown(&ctx);
        mk_fclose(ctx.paths_fp);
        mk_fclose(ctx.path_offsets_fp);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.has_batch);
        pthread_cond_destroy(&write_queue.has_space);
        pthread_mutex_destroy(&trigram_queue.mutex);
        pthread_cond_destroy(&trigram_queue.has_job);
        pthread_cond_destroy(&trigram_queue.has_space);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    tw_worker_args = (trigram_worker_arg_t *)calloc((size_t)trigram_threads, sizeof(*tw_worker_args));
    if (!tw_worker_args) {
        fprintf(stderr, "allocation failed\n");
        free(trigram_tids);
        trigram_tids = NULL;
        free(tids);
        free(args);
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        parallel_bucket_io_shutdown(&ctx);
        mk_fclose(ctx.paths_fp);
        mk_fclose(ctx.path_offsets_fp);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.has_batch);
        pthread_cond_destroy(&write_queue.has_space);
        pthread_mutex_destroy(&trigram_queue.mutex);
        pthread_cond_destroy(&trigram_queue.has_job);
        pthread_cond_destroy(&trigram_queue.has_space);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    for (i = 0; i < (size_t)trigram_threads; i++) {
        tw_worker_args[i].trigram_queue = &trigram_queue;
        tw_worker_args[i].ctx = &ctx;
        tw_worker_args[i].worker_index = (uint32_t)i;
        if (pthread_create(&trigram_tids[i], NULL, trigram_worker_main, &tw_worker_args[i]) != 0) {
            size_t j;
            fprintf(stderr, "failed to create trigram worker\n");
            atomic_store(&run_stats.writer_failed, 1);
            trigram_job_queue_close(&trigram_queue);
            for (j = 0; j < i; j++) pthread_join(trigram_tids[j], NULL);
            free(trigram_tids);
            trigram_tids = NULL;
            free(tw_worker_args);
            tw_worker_args = NULL;
            free(tids);
            free(args);
            if (stats_thread_started) {
                atomic_store(&run_stats.stop_stats, 1);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
            }
            parallel_bucket_io_shutdown(&ctx);
            mk_fclose(ctx.paths_fp);
            mk_fclose(ctx.path_offsets_fp);
            pthread_mutex_destroy(&queue.mutex);
            pthread_mutex_destroy(&write_queue.mutex);
            pthread_cond_destroy(&write_queue.has_batch);
            pthread_cond_destroy(&write_queue.has_space);
            pthread_mutex_destroy(&trigram_queue.mutex);
            pthread_cond_destroy(&trigram_queue.has_job);
            pthread_cond_destroy(&trigram_queue.has_space);
            for (j = 0; j < path_count; j++) free(paths[j]);
            free(paths);
            for (j = 0; j < chunk_count; j++) free(chunks[j].path);
            free(chunks);
            free(file_states);
            return 1;
        }
    }
    trigram_threads_used = trigram_threads;

    paths_writer_arg.write_queue = &write_queue;
    paths_writer_arg.trigram_queue = &trigram_queue;
    paths_writer_arg.ctx = &ctx;
    if (pthread_create(&paths_writer_thread, NULL, paths_writer_main, &paths_writer_arg) != 0) {
        fprintf(stderr, "failed to create paths writer thread\n");
        atomic_store(&run_stats.writer_failed, 1);
        trigram_job_queue_close(&trigram_queue);
        for (i = 0; i < (size_t)trigram_threads; i++) pthread_join(trigram_tids[i], NULL);
        free(trigram_tids);
        trigram_tids = NULL;
        free(tw_worker_args);
        tw_worker_args = NULL;
        free(tids);
        free(args);
        if (stats_thread_started) {
            atomic_store(&run_stats.stop_stats, 1);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        parallel_bucket_io_shutdown(&ctx);
        mk_fclose(ctx.paths_fp);
        mk_fclose(ctx.path_offsets_fp);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.has_batch);
        pthread_cond_destroy(&write_queue.has_space);
        pthread_mutex_destroy(&trigram_queue.mutex);
        pthread_cond_destroy(&trigram_queue.has_job);
        pthread_cond_destroy(&trigram_queue.has_space);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        return 1;
    }

    memset(&ml_storage, 0, sizeof(ml_storage));
    if (g_verbose) {
        char logpath[PATH_MAX];

        if (build_path(logpath, sizeof(logpath), ctx.index_dir, "ereport_index.log") == 0) {
            ml_storage.fp = fopen(logpath, "a");
            if (ml_storage.fp) {
                ml_storage.wq = &write_queue;
                ml_storage.tq = &trigram_queue;
                ml_storage.chunkq = &queue;
                ml_storage.ctx = &ctx;
                atomic_init(&ml_storage.stop, 0);
                if (pthread_create(&memlog_tid, NULL, memlog_thread_main, &ml_storage) != 0) {
                    fclose(ml_storage.fp);
                    ml_storage.fp = NULL;
                } else {
                    memlog_started = 1;
                }
            }
        }
    }

    for (i = 0; i < (size_t)threads; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].queue = &queue;
        args[i].write_queue = &write_queue;
        args[i].file_states = file_states;
        args[i].ctx = &ctx;
        args[i].write_batch_flush_at = batch_paths_flush_limit(threads);
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
    pthread_join(paths_writer_thread, NULL);
    trigram_job_queue_close(&trigram_queue);
    for (i = 0; i < (size_t)trigram_threads_used; i++) {
        pthread_join(trigram_tids[i], NULL);
    }

    if (stats_thread_started) {
        atomic_store(&run_stats.stop_stats, 1);
        pthread_join(stats_thread, NULL);
        clear_status_line();
    }

    if (getrusage(RUSAGE_SELF, &ru_after_index) == 0) ru_have_index = 1;

    ctx.scanned_records = atomic_load(&run_stats.scanned_records);
    ctx.indexed_paths = atomic_load(&run_stats.indexed_paths);
    ctx.trigram_records = atomic_load(&run_stats.trigram_records);
    ctx.bad_input_files = atomic_load(&run_stats.bad_input_files);
    if (atomic_load(&run_stats.writer_failed)) {
        fprintf(stderr, "ereport_index: indexing writer failed while building %s\n", ctx.index_dir);
        memlog_shutdown(&ml_storage, &memlog_tid, &memlog_started);
        free(tids);
        free(args);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.has_batch);
        pthread_cond_destroy(&write_queue.has_space);
        pthread_mutex_destroy(&trigram_queue.mutex);
        pthread_cond_destroy(&trigram_queue.has_job);
        pthread_cond_destroy(&trigram_queue.has_space);
        free(trigram_tids);
        trigram_tids = NULL;
        free(tw_worker_args);
        tw_worker_args = NULL;
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        parallel_bucket_io_shutdown(&ctx);
        mk_fclose(ctx.paths_fp);
        mk_fclose(ctx.path_offsets_fp);
        return 1;
    }

    if (write_final_path_offset(&ctx) != 0 || mk_fclose(ctx.paths_fp) != 0 || mk_fclose(ctx.path_offsets_fp) != 0) {
        fprintf(stderr, "failed to finalize paths in %s\n", ctx.index_dir);
        memlog_shutdown(&ml_storage, &memlog_tid, &memlog_started);
        free(tids);
        free(args);
        pthread_mutex_destroy(&queue.mutex);
        pthread_mutex_destroy(&write_queue.mutex);
        pthread_cond_destroy(&write_queue.has_batch);
        pthread_cond_destroy(&write_queue.has_space);
        pthread_mutex_destroy(&trigram_queue.mutex);
        pthread_cond_destroy(&trigram_queue.has_job);
        pthread_cond_destroy(&trigram_queue.has_space);
        free(trigram_tids);
        trigram_tids = NULL;
        free(tw_worker_args);
        tw_worker_args = NULL;
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        free(file_states);
        parallel_bucket_io_shutdown(&ctx);
        return 1;
    }

    ctx.index_phase_sec = now_sec() - t0;
    ctx.index_workers_used = threads_used;
    ctx.trigram_writer_workers_used = trigram_threads_used;

    parallel_bucket_io_shutdown(&ctx);

    {
        int merge_rc = process_trigram_buckets(&ctx);

        if (getrusage(RUSAGE_SELF, &ru_after_merge) == 0) ru_have_merge = 1;

        memlog_shutdown(&ml_storage, &memlog_tid, &memlog_started);
        if (merge_rc != 0 || write_meta_file(&ctx) != 0) {
            fprintf(stderr, "failed to finalize index in %s\n", ctx.index_dir);
            free(tids);
            free(args);
            pthread_mutex_destroy(&queue.mutex);
            pthread_mutex_destroy(&write_queue.mutex);
            pthread_cond_destroy(&write_queue.has_batch);
            pthread_cond_destroy(&write_queue.has_space);
            pthread_mutex_destroy(&trigram_queue.mutex);
            pthread_cond_destroy(&trigram_queue.has_job);
            pthread_cond_destroy(&trigram_queue.has_space);
            free(trigram_tids);
            trigram_tids = NULL;
            free(tw_worker_args);
            tw_worker_args = NULL;
            for (i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            for (i = 0; i < chunk_count; i++) free(chunks[i].path);
            free(chunks);
            free(file_states);
            return 1;
        }
    }

    t1 = now_sec();
    clear_status_line();
    printf("mode=make\n");
    printf("user=%s\n", ctx.display_name);
    printf("aggregate_all_users=%d\n", ctx.aggregate_all_users ? 1 : 0);
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
    printf("chunk_prep_sec=%.3f\n", chunk_prep_sec);
    printf("index_phase_sec=%.3f\n", ctx.index_phase_sec);
    printf("index_workers=%d\n", ctx.index_workers_used);
    printf("index_trigram_workers=%d\n", ctx.trigram_writer_workers_used);
    printf("merge_phase_sec=%.3f\n", ctx.merge_phase_sec);
    printf("merge_buckets_nonempty=%u\n", ctx.merge_buckets_nonempty);
    printf("merge_workers=%d\n", ctx.merge_workers_used);
    printf("elapsed_sec=%.3f\n", t1 - t0);
    if (g_verbose) {
        printf("trigram_queue_depth=%zu\n", trigram_queue_depth_cfg);
        printf("write_batch_paths=%zu\n", g_write_batch_paths_base);
        {
            char ips_buf[32];
            double ips = ctx.index_phase_sec > 0.0 ? (double)ctx.indexed_paths / ctx.index_phase_sec : 0.0;
            human_decimal(ips, ips_buf, sizeof(ips_buf));
            printf("index_paths_per_sec=%s\n", ips_buf);
        }
        printf("merge_buckets_skipped=%u\n", ctx.merge_buckets_skipped);
        printf("merge_trigram_records_read=%" PRIu64 "\n", ctx.merge_trigram_records_read);
        printf("merge_workers_cpu=%d\n", ctx.merge_workers_cpu);
        printf("merge_max_bucket_mib=%.1f\n", (double)ctx.merge_max_bucket_bytes / (1024.0 * 1024.0));
        printf("merge_parallel_ram_budget_mib=%.1f\n", (double)ctx.merge_parallel_budget_bytes / (1024.0 * 1024.0));
        printf("merge_bytes_temp_read=%" PRIu64 "\n", ctx.merge_bytes_temp_read);
        printf("merge_bucket_ram_peak_est=%" PRIu64 "\n",
               (uint64_t)atomic_load_explicit(&ctx.merge_bucket_ram_peak, memory_order_relaxed));
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
        printf("writeq_max_batches=%zu\n", write_queue.max_depth);
        printf("write_batch_flush_at=%zu\n", batch_paths_flush_limit(threads));
        printf("writeq_writer_waits=%" PRIu64 "\n", (uint64_t)atomic_load(&run_stats.writeq_writer_waits));
        printf("writeq_parse_waits=%" PRIu64 "\n", (uint64_t)atomic_load(&run_stats.writeq_parse_waits));
        printf("trigramq_paths_waits=%" PRIu64 "\n", (uint64_t)atomic_load(&run_stats.trigramq_paths_waits));
        printf("trigramq_worker_waits=%" PRIu64 "\n", (uint64_t)atomic_load(&run_stats.trigramq_worker_waits));
        printf("wall_after_index_sec=%.3f\n", (t1 - t0) - ctx.index_phase_sec);
        if (ru_have_start && ru_have_prep) rusage_print_delta("cpu_prep", &ru_after_prep, &ru_make_start);
        if (ru_have_prep && ru_have_index) rusage_print_delta("cpu_idx", &ru_after_index, &ru_after_prep);
        if (ru_have_index && ru_have_merge) rusage_print_delta("cpu_mrg", &ru_after_merge, &ru_after_index);
        if (ru_have_start && ru_have_merge) rusage_print_delta("cpu_make", &ru_after_merge, &ru_make_start);

        mk_io_tls_flush();
        printf("make_fread_calls=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_fread_calls, memory_order_relaxed));
        printf("make_fread_bytes=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_fread_bytes, memory_order_relaxed));
        printf("make_fwrite_calls=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_fwrite_calls, memory_order_relaxed));
        printf("make_fwrite_bytes=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_fwrite_bytes, memory_order_relaxed));
        printf("make_fopen_calls=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_fopen_calls, memory_order_relaxed));
        printf("make_fclose_calls=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_fclose_calls, memory_order_relaxed));
        printf("make_open_calls=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_open_calls, memory_order_relaxed));
        printf("make_read_calls=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_read_calls, memory_order_relaxed));
        printf("make_read_bytes=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_read_bytes, memory_order_relaxed));
        printf("make_mmap_calls=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_mmap_calls, memory_order_relaxed));
        printf("make_munmap_calls=%llu\n", (unsigned long long)atomic_load_explicit(&g_mk_munmap_calls, memory_order_relaxed));
        printf("make_trigram_append_batches=%llu\n",
               (unsigned long long)atomic_load_explicit(&g_mk_trigram_append_batches, memory_order_relaxed));
    } else {
        mk_io_tls_flush();
    }

    free(tids);
    free(args);
    free(trigram_tids);
    trigram_tids = NULL;
    free(tw_worker_args);
    tw_worker_args = NULL;
    pthread_mutex_destroy(&queue.mutex);
    pthread_mutex_destroy(&write_queue.mutex);
    pthread_cond_destroy(&write_queue.has_batch);
    pthread_cond_destroy(&write_queue.has_space);
    pthread_mutex_destroy(&trigram_queue.mutex);
    pthread_cond_destroy(&trigram_queue.has_job);
    pthread_cond_destroy(&trigram_queue.has_space);
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

/* Same value as indexed_paths= in meta.txt from --make (total paths in paths.bin). */
static uint64_t read_meta_indexed_paths(const char *index_dir) {
    char path[PATH_MAX];
    FILE *fp;
    char buf[512];
    uint64_t v = 0;

    if (build_path(path, sizeof(path), index_dir, "meta.txt") != 0) return 0;
    fp = fopen(path, "r");
    if (!fp) return 0;
    while (fgets(buf, sizeof(buf), fp)) {
        uint64_t tmp;
        if (sscanf(buf, "indexed_paths=%" SCNu64, &tmp) == 1) {
            v = tmp;
            break;
        }
    }
    fclose(fp);
    return v;
}

static uint64_t monotonic_ms_elapsed(const struct timespec *t0) {
    struct timespec t1;
    int64_t sec, nsec;

    if (clock_gettime(CLOCK_MONOTONIC, &t1) != 0) return 0;
    sec = (int64_t)(t1.tv_sec - t0->tv_sec);
    nsec = (int64_t)(t1.tv_nsec - t0->tv_nsec);
    if (nsec < 0) {
        sec--;
        nsec += 1000000000L;
    }
    return (uint64_t)(sec * 1000 + nsec / 1000000);
}

static void json_print_empty_search(uint64_t skip_req, uint64_t limit_req, uint64_t key_count,
                                    uint64_t indexed_paths_corpus, const struct timespec *t_search_start) {
    uint64_t ms = t_search_start ? monotonic_ms_elapsed(t_search_start) : 0;
    fprintf(stdout,
            "{\"total\":0,\"skip\":%" PRIu64 ",\"limit\":%" PRIu64 ",\"search_ms\":%" PRIu64 ",\"index_keys\":%" PRIu64
            ",\"indexed_paths\":%" PRIu64 ",\"paths\":[]}\n",
            skip_req, limit_req, ms, key_count, indexed_paths_corpus);
    fflush(stdout);
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

enum {
    SEARCH_TRIGRAM_PAR_MIN = 2,
    SEARCH_PATH_PAR_MIN = 64
};

typedef struct {
    const char *keys_path;
    const char *postings_path;
    uint64_t key_count;
    const uint32_t *query_trigrams;
    u64_vec_t *lists;
    size_t lo;
    size_t hi;
    atomic_int *err;
} search_trigram_load_arg_t;

static void *search_trigram_load_worker(void *v) {
    search_trigram_load_arg_t *a = (search_trigram_load_arg_t *)v;
    FILE *kf;
    FILE *pf;
    size_t i;

    if (a->lo >= a->hi) return NULL;
    if (atomic_load_explicit(a->err, memory_order_relaxed)) return NULL;
    kf = fopen(a->keys_path, "rb");
    pf = fopen(a->postings_path, "rb");
    if (!kf || !pf) {
        atomic_store_explicit(a->err, 1, memory_order_relaxed);
        if (kf) fclose(kf);
        if (pf) fclose(pf);
        return NULL;
    }

    for (i = a->lo; i < a->hi; i++) {
        trigram_key_t key;
        int found;

        if (atomic_load_explicit(a->err, memory_order_relaxed)) break;
        found = find_trigram_key(kf, a->key_count, a->query_trigrams[i], &key);
        if (found < 0) {
            atomic_store_explicit(a->err, 1, memory_order_relaxed);
            break;
        }
        if (found > 0) continue;
        if (load_postings_list(pf, &key, &a->lists[i]) != 0) {
            atomic_store_explicit(a->err, 1, memory_order_relaxed);
            break;
        }
    }
    fclose(kf);
    fclose(pf);
    return NULL;
}

typedef struct {
    const char *paths_path;
    const char *offsets_path;
    const uint64_t *ids;
    char **out_paths;
    const char *lower_term;
    size_t lo;
    size_t hi;
    atomic_int *err;
} search_path_filter_arg_t;

static void *search_path_filter_worker(void *v) {
    search_path_filter_arg_t *a = (search_path_filter_arg_t *)v;
    FILE *paths_fp;
    FILE *offsets_fp;
    size_t i;

    if (a->lo >= a->hi) return NULL;
    if (atomic_load_explicit(a->err, memory_order_relaxed)) return NULL;
    paths_fp = fopen(a->paths_path, "rb");
    offsets_fp = fopen(a->offsets_path, "rb");
    if (!paths_fp || !offsets_fp) {
        atomic_store_explicit(a->err, 1, memory_order_relaxed);
        if (paths_fp) fclose(paths_fp);
        if (offsets_fp) fclose(offsets_fp);
        return NULL;
    }

    for (i = a->lo; i < a->hi; i++) {
        char *p;

        if (atomic_load_explicit(a->err, memory_order_relaxed)) break;
        p = read_path_by_id(paths_fp, offsets_fp, a->ids[i]);
        if (!p) {
            a->out_paths[i] = NULL;
            continue;
        }
        if (!path_matches_term(p, a->lower_term)) {
            free(p);
            a->out_paths[i] = NULL;
        } else {
            a->out_paths[i] = p;
        }
    }
    fclose(paths_fp);
    fclose(offsets_fp);
    return NULL;
}

static int search_index_dir(const char *term, const char *index_dir, uint64_t skip_req, uint64_t limit_req,
                            int json_output) {
    char keys_path[PATH_MAX], postings_path[PATH_MAX], paths_path[PATH_MAX], offsets_path[PATH_MAX];
    FILE *keys_fp = NULL, *postings_fp = NULL, *paths_fp = NULL, *offsets_fp = NULL;
    struct stat st;
    struct timespec t_search_start;
    uint64_t key_count;
    uint64_t indexed_paths_corpus = 0;
    uint32_t *query_trigrams = NULL;
    size_t query_trigram_count = 0;
    u64_vec_t *lists = NULL;
    u64_vec_t current, next;
    char *lower_term = NULL;
    int rc = 1;
    int search_threads = parse_index_thread_count();

    /* Avoid full buffering when stdout is a pipe (eserve subprocess): empty reads → 502 */
    if (json_output) setvbuf(stdout, NULL, _IONBF, 0);

    if (build_path(keys_path, sizeof(keys_path), index_dir, "tri_keys.bin") != 0 ||
        build_path(postings_path, sizeof(postings_path), index_dir, "tri_postings.bin") != 0 ||
        build_path(paths_path, sizeof(paths_path), index_dir, "paths.bin") != 0 ||
        build_path(offsets_path, sizeof(offsets_path), index_dir, "path_offsets.bin") != 0) {
        return 1;
    }

    if (stat(keys_path, &st) != 0 || (st.st_size % (off_t)sizeof(trigram_key_t)) != 0) {
        fprintf(stderr, "invalid tri_keys.bin in %s\n", index_dir);
        return 1;
    }
    key_count = (uint64_t)(st.st_size / (off_t)sizeof(trigram_key_t));
    if (clock_gettime(CLOCK_MONOTONIC, &t_search_start) != 0) memset(&t_search_start, 0, sizeof(t_search_start));
    indexed_paths_corpus = read_meta_indexed_paths(index_dir);

    if (collect_query_trigrams(term, &query_trigrams, &query_trigram_count) != 0) goto out;
    lower_term = ascii_lower_dup(term);
    if (!lower_term) goto out;

    lists = (u64_vec_t *)calloc(query_trigram_count, sizeof(*lists));
    if (!lists) goto out;

    if (query_trigram_count >= (size_t)SEARCH_TRIGRAM_PAR_MIN && search_threads > 1) {
        int nw = search_threads;
        int w;

        if (nw > (int)query_trigram_count) nw = (int)query_trigram_count;

        {
            pthread_t *tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
            search_trigram_load_arg_t *args = (search_trigram_load_arg_t *)calloc((size_t)nw, sizeof(*args));
            atomic_int load_err = 0;

            if (!tids || !args) {
                free(tids);
                free(args);
                goto out;
            }
            for (w = 0; w < nw; w++) {
                size_t span = (query_trigram_count + (size_t)nw - 1) / (size_t)nw;
                size_t a_lo = (size_t)w * span;
                size_t a_hi = a_lo + span;
                if (a_hi > query_trigram_count) a_hi = query_trigram_count;
                args[w].keys_path = keys_path;
                args[w].postings_path = postings_path;
                args[w].key_count = key_count;
                args[w].query_trigrams = query_trigrams;
                args[w].lists = lists;
                args[w].lo = a_lo;
                args[w].hi = a_hi;
                args[w].err = &load_err;
            }
            for (w = 0; w < nw; w++) {
                if (pthread_create(&tids[w], NULL, search_trigram_load_worker, &args[w]) != 0) {
                    int j;
                    atomic_store_explicit(&load_err, 1, memory_order_relaxed);
                    for (j = 0; j < w; j++) pthread_join(tids[j], NULL);
                    free(tids);
                    free(args);
                    goto out;
                }
            }
            for (w = 0; w < nw; w++) pthread_join(tids[w], NULL);
            free(tids);
            free(args);
            if (atomic_load_explicit(&load_err, memory_order_relaxed)) goto out;
        }
    } else {
        keys_fp = fopen(keys_path, "rb");
        postings_fp = fopen(postings_path, "rb");
        if (!keys_fp || !postings_fp) {
            fprintf(stderr, "cannot open index under %s\n", index_dir);
            goto out;
        }
        for (size_t i = 0; i < query_trigram_count; i++) {
            trigram_key_t key;
            int found = find_trigram_key(keys_fp, key_count, query_trigrams[i], &key);
            if (found != 0) {
                if (found < 0) {
                    rc = 1;
                } else if (json_output) {
                    json_print_empty_search(skip_req, limit_req, key_count, indexed_paths_corpus, &t_search_start);
                    rc = 0;
                } else {
                    rc = 0;
                }
                goto out;
            }
            if (load_postings_list(postings_fp, &key, &lists[i]) != 0) goto out;
        }
    }

    if (keys_fp) {
        fclose(keys_fp);
        keys_fp = NULL;
    }
    if (postings_fp) {
        fclose(postings_fp);
        postings_fp = NULL;
    }

    qsort(lists, query_trigram_count, sizeof(*lists), cmp_vec_count_asc);
    memset(&current, 0, sizeof(current));
    memset(&next, 0, sizeof(next));

    if (query_trigram_count == 0) {
        if (json_output) json_print_empty_search(skip_req, limit_req, key_count, indexed_paths_corpus, &t_search_start);
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
        char **pref_paths = NULL;

        if (current.count >= (size_t)SEARCH_PATH_PAR_MIN && search_threads > 1) {
            int pnw = search_threads;
            int pw;
            pthread_t *ptids = NULL;
            search_path_filter_arg_t *pargs = NULL;
            atomic_int path_err = 0;

            if (pnw > (int)current.count) pnw = (int)current.count;
            pref_paths = (char **)calloc(current.count, sizeof(char *));
            ptids = (pthread_t *)calloc((size_t)pnw, sizeof(*ptids));
            pargs = (search_path_filter_arg_t *)calloc((size_t)pnw, sizeof(*pargs));
            if (!pref_paths || !ptids || !pargs) {
                free(pref_paths);
                free(ptids);
                free(pargs);
                free(current.ids);
                current.ids = NULL;
                goto out;
            }
            for (pw = 0; pw < pnw; pw++) {
                size_t span = (current.count + (size_t)pnw - 1) / (size_t)pnw;
                size_t a_lo = (size_t)pw * span;
                size_t a_hi = a_lo + span;
                if (a_hi > current.count) a_hi = current.count;
                pargs[pw].paths_path = paths_path;
                pargs[pw].offsets_path = offsets_path;
                pargs[pw].ids = current.ids;
                pargs[pw].out_paths = pref_paths;
                pargs[pw].lower_term = lower_term;
                pargs[pw].lo = a_lo;
                pargs[pw].hi = a_hi;
                pargs[pw].err = &path_err;
            }
            for (pw = 0; pw < pnw; pw++) {
                if (pthread_create(&ptids[pw], NULL, search_path_filter_worker, &pargs[pw]) != 0) {
                    int j;
                    atomic_store_explicit(&path_err, 1, memory_order_relaxed);
                    for (j = 0; j < pw; j++) pthread_join(ptids[j], NULL);
                    for (ki = 0; ki < current.count; ki++) free(pref_paths[ki]);
                    free(pref_paths);
                    free(ptids);
                    free(pargs);
                    free(current.ids);
                    current.ids = NULL;
                    goto out;
                }
            }
            for (pw = 0; pw < pnw; pw++) pthread_join(ptids[pw], NULL);
            free(ptids);
            free(pargs);
            if (atomic_load_explicit(&path_err, memory_order_relaxed)) {
                for (ki = 0; ki < current.count; ki++) free(pref_paths[ki]);
                free(pref_paths);
                free(current.ids);
                current.ids = NULL;
                goto out;
            }
        } else if (current.count > 0) {
            paths_fp = fopen(paths_path, "rb");
            offsets_fp = fopen(offsets_path, "rb");
            if (!paths_fp || !offsets_fp) {
                fprintf(stderr, "cannot open paths.bin under %s\n", index_dir);
                if (paths_fp) fclose(paths_fp);
                if (offsets_fp) fclose(offsets_fp);
                paths_fp = NULL;
                offsets_fp = NULL;
                free(current.ids);
                current.ids = NULL;
                goto out;
            }
        }

        for (size_t i = 0; i < current.count; i++) {
            char *path;

            if (pref_paths) {
                path = pref_paths[i];
            } else {
                path = read_path_by_id(paths_fp, offsets_fp, current.ids[i]);
                if (!path) continue;
                if (!path_matches_term(path, lower_term)) {
                    free(path);
                    continue;
                }
            }
            if (!path) continue;
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

        if (paths_fp) {
            fclose(paths_fp);
            paths_fp = NULL;
        }
        if (offsets_fp) {
            fclose(offsets_fp);
            offsets_fp = NULL;
        }
        if (pref_paths) free(pref_paths);

        if (json_output) {
            uint64_t search_ms_done = monotonic_ms_elapsed(&t_search_start);
            fprintf(stdout,
                    "{\"total\":%" PRIu64 ",\"skip\":%" PRIu64 ",\"limit\":%" PRIu64 ",\"search_ms\":%" PRIu64
                    ",\"index_keys\":%" PRIu64 ",\"indexed_paths\":%" PRIu64 ",\"paths\":[",
                    match_idx,
                    skip_req,
                    limit_req,
                    search_ms_done,
                    key_count,
                    indexed_paths_corpus);
            for (ki = 0; ki < page_emitted; ki++) {
                if (ki) fputc(',', stdout);
                json_escape_stdout(stdout, json_paths[ki]);
                free(json_paths[ki]);
            }
            fputs("]}\n", stdout);
            fflush(stdout);
            free(json_paths);
        }
    }

    free(current.ids);
    current.ids = NULL;
    rc = 0;

out:
    if (rc == 0 && g_verbose && !json_output)
        fprintf(stderr,
                "ereport_index: search_ms=%" PRIu64 " (EREPORT_INDEX_THREADS=%d)\n",
                monotonic_ms_elapsed(&t_search_start),
                search_threads);
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
    int vi;
    int cmd0;

    if (argc < 2) die_usage(argv[0]);
    for (vi = 1; vi < argc; vi++) {
        if (arg_is_verbose(argv[vi])) g_verbose = 1;
    }

    cmd0 = argv_skip_verbose_prefix(argc, argv);
    if (cmd0 >= argc) die_usage(argv[0]);

    if (strcmp(argv[cmd0], "--resume-merge") == 0) {
        if (cmd0 + 2 < argc && strcmp(argv[cmd0 + 1], "--index-dir") == 0 && argv[cmd0 + 2][0] != '\0')
            return resume_merge_index_dir(argv[cmd0 + 2]);
        fprintf(stderr, "ereport_index: --resume-merge requires --index-dir <path>\n");
        return 2;
    }

    if (strcmp(argv[cmd0], "--make") == 0) {
        const char **dirpaths;
        size_t dirpath_count;
        int all_users_mode;
        const char *user_spec = NULL;
        const char *index_dir_override = NULL;
        int ai = cmd0 + 1;
        uid_t probe_uid;
        char probe_disp[256];

        while (ai < argc && arg_is_verbose(argv[ai])) ai++;

        if (ai < argc && strcmp(argv[ai], "--index-dir") == 0) {
            if (ai + 2 > argc) {
                fprintf(stderr, "ereport_index: --index-dir requires a path\n");
                return 2;
            }
            index_dir_override = argv[ai + 1];
            ai += 2;
        }

        while (ai < argc && arg_is_verbose(argv[ai])) ai++;

        if (argc == ai) {
            static const char *dot = ".";
            all_users_mode = 1;
            dirpaths = &dot;
            dirpath_count = 1;
            return build_index_dir(NULL, dirpaths, dirpath_count, all_users_mode, index_dir_override);
        }

        if (resolve_target_user(argv[ai], &probe_uid, probe_disp, sizeof(probe_disp)) == 0) {
            user_spec = argv[ai];
            all_users_mode = 0;
            ai++;
            if (argc == ai) {
                static const char *dot = ".";
                dirpaths = &dot;
                dirpath_count = 1;
            } else {
                dirpaths = (const char **)(argv + ai);
                dirpath_count = (size_t)(argc - ai);
            }
        } else {
            all_users_mode = 1;
            dirpaths = (const char **)(argv + ai);
            dirpath_count = (size_t)(argc - ai);
        }
        while (dirpath_count > 0 && arg_is_verbose(dirpaths[dirpath_count - 1])) dirpath_count--;
        return build_index_dir(user_spec, dirpaths, dirpath_count, all_users_mode, index_dir_override);
    }

    if (strcmp(argv[cmd0], "--search") == 0) {
        const char *index_dir = "index";
        uint64_t skip_req = 0;
        uint64_t limit_req = UINT64_MAX;
        int json_output = 0;
        int ai;
        const char *term;

        ai = cmd0 + 1;
        while (ai < argc && arg_is_verbose(argv[ai])) ai++;
        if (ai < argc && strcmp(argv[ai], "--index-dir") == 0) {
            if (ai + 2 > argc || argv[ai + 1][0] == '\0') {
                fprintf(stderr, "ereport_index: --search --index-dir requires a path\n");
                return 2;
            }
            index_dir = argv[ai + 1];
            ai += 2;
        }

        if (argc <= ai) die_usage(argv[0]);
        term = argv[ai];
        ai++;

        while (ai < argc) {
            if (arg_is_verbose(argv[ai])) {
                ai++;
                continue;
            }
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

        return search_index_dir(term, index_dir, skip_req, limit_req, json_output);
    }

    die_usage(argv[0]);
    return 2;
}
