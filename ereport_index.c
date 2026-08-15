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
#include <stdarg.h>
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
#include <zstd.h>

#include "alloc_tuning.h"
#include "crawl_bin_block.h"
#include "crawl_bin_catalog.h"
#include "crawl_bin_chunks.h"
#include "crawl_ckpt.h"
#include "crawl_fpcache.h"
#include "path_canon.h"
#include "trigram_extract.h"
#include "crawl_trijournal.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/*
 * 3: trigrams are taken from each path's basename only (segment-once). path_isdir.bin
 *    records which path_ids are directories so --search can expand a directory hit to its
 *    descendants. Readers reject older versions — rebuild with `--make`.
 * 2: durable paths.bin and tri_postings.bin are zstd-compressed (EPATH002 / chunked postings).
 */
#define INDEX_VERSION 3
/*
 * Top bits of the 24-bit trigram used to partition records into tmp_trigrams_<bucket>_w<shard>.bin
 * during the index phase, and into one merge segment per bucket during the merge phase.
 *
 * Finer partitions (more bits) would shrink the hottest merge bucket, but they are NOT viable with the
 * current per-(worker × bucket) tmp-file design: each trigram worker keeps at most ~499 shard FILE*s
 * open (fd budget) as an LRU cache. At 12 bits (4096 buckets) consecutive paths' trigrams reuse an
 * overlapping subset of buckets, so the cache hits and open/close churn stays low. At 16 bits (65536
 * buckets) each path's ~150 trigrams scatter across buckets with almost no cross-path reuse, so the
 * cache thrashes: ~150 cold fopen+fclose per path × billions of paths, serialized on glibc's global
 * open-file-list lock (_IO_list_lock). A 16-bit experiment spent 81% of cycles in fclose/__IO_un_link
 * and was ~13× slower in the index phase. So keep this at 12; pursue merge parallelism by splitting
 * hot buckets WITHIN the merge phase instead of by finer global bucketing.
 * Overridable at compile time (e.g. -DTRIGRAM_BUCKET_BITS=14) only for experiments.
 */
#ifndef TRIGRAM_BUCKET_BITS
#define TRIGRAM_BUCKET_BITS 12
#endif
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
#define PARSE_CHUNK_BYTES CRAWL_BIN_PARSE_CHUNK_BYTES
#define PARSE_CHUNK_MIN_BYTES (1ULL << 20)
#define WRITE_BATCH_PATHS 4096
/* Default pending trigram jobs when EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH is unset: scales with trigram workers. */
#define TRIGRAM_JOB_QUEUE_DEPTH_MIN 4096
#define TRIGRAM_JOB_QUEUE_DEPTH_MAX_DEFAULT 16384
#define TRIGRAM_JOB_QUEUE_DEPTH_PER_WORKER 64

static size_t g_write_batch_paths_base = WRITE_BATCH_PATHS;
/* Like ecrawl/ereport: default output is a concise key=value summary; --verbose enables live detail and extra metrics. */
static int g_verbose = 0;
static int g_durable_zstd_level = -1; /* -1 = parse EREPORT_INDEX_ZSTD_LEVEL on first use */

/* --subtree <abs-path>: when non-NULL, only records whose reconstructed full path is at or under this
 * directory are indexed (full absolute paths kept), so the index mirrors an ereport --subtree report. */
static char g_subtree_buf[PATH_MAX];
static const char *g_subtree_prefix = NULL;

/* --journal-dir <path>: crawl-time trigram journals (ecrawl --trigram-journal). A shard whose
 * journal passes the five-fact live check is replayed straight into the write-batch pipeline,
 * skipping capture parse, catalog load, path reconstruction, and extraction for that shard. */
static const char *g_journal_dir = NULL;

/* Directory-boundary prefix test: matches `prefix` itself and `prefix/...` but not `prefixfoo`.
 * (Mirrors ereport.c's starts_with_dir_prefix.) */
static int subtree_path_under_prefix(const char *path) {
    size_t plen;

    if (!g_subtree_prefix || g_subtree_prefix[0] == '\0') return 1;
    if (!path) return 0;
    if (strcmp(g_subtree_prefix, "/") == 0) return path[0] == '/';
    plen = strlen(g_subtree_prefix);
    if (strncmp(path, g_subtree_prefix, plen) != 0) return 0;
    return path[plen] == '\0' || path[plen] == '/';
}

/* Validate + normalize a --subtree argument into g_subtree_buf and point g_subtree_prefix at it.
 * Returns 0 on success, non-zero (with a message on stderr) on error. */
static int set_subtree_prefix(const char *arg) {
    size_t sl;

    if (g_subtree_prefix != NULL) {
        fprintf(stderr, "ereport_index: duplicate --subtree\n");
        return -1;
    }
    if (!arg || arg[0] != '/') {
        fprintf(stderr, "ereport_index: --subtree path must be absolute (got '%s')\n", arg ? arg : "");
        return -1;
    }
    if (snprintf(g_subtree_buf, sizeof(g_subtree_buf), "%s", arg) >= (int)sizeof(g_subtree_buf)) {
        fprintf(stderr, "ereport_index: --subtree path too long\n");
        return -1;
    }
    sl = strlen(g_subtree_buf);
    while (sl > 1 && g_subtree_buf[sl - 1] == '/') g_subtree_buf[--sl] = '\0';
    g_subtree_prefix = g_subtree_buf;
    return 0;
}

/* --path-rewrite OLD=NEW: when set, every reconstructed path at or under OLD has its OLD prefix replaced
 * with NEW (directory boundary) at read time, so the index stores the rewritten paths (bins untouched).
 * Applied before the --subtree filter, so --subtree is given in NEW terms. Mirrors ereport's --path-rewrite. */
static char g_rewrite_from_buf[PATH_MAX];
static char g_rewrite_to_buf[PATH_MAX];
static const char *g_rewrite_from = NULL;
static size_t g_rewrite_from_len = 0;
static const char *g_rewrite_to = NULL;
static size_t g_rewrite_to_len = 0;

/* In-place prefix rewrite. Returns 0 (no-op when unset or no match), -1 if the result would not fit. */
static int rewrite_path_prefix(char *path, size_t bufsz) {
    size_t plen, suffix_len, newlen;

    if (!g_rewrite_from || !path) return 0;
    if (strncmp(path, g_rewrite_from, g_rewrite_from_len) != 0) return 0;
    if (path[g_rewrite_from_len] != '\0' && path[g_rewrite_from_len] != '/') return 0;
    plen = strlen(path);
    suffix_len = plen - g_rewrite_from_len; /* "" or "/..." (g_rewrite_from has no trailing slash) */
    newlen = g_rewrite_to_len + suffix_len;
    if (newlen + 1 > bufsz) return -1;
    memmove(path + g_rewrite_to_len, path + g_rewrite_from_len, suffix_len + 1); /* include NUL */
    memcpy(path, g_rewrite_to, g_rewrite_to_len);
    return 0;
}

/* Validate + normalize a "--path-rewrite OLD=NEW" argument. Returns 0 on success, non-zero on error. */
static int set_path_rewrite(const char *arg) {
    const char *eq;
    size_t fl, tl;

    if (g_rewrite_from != NULL) {
        fprintf(stderr, "ereport_index: duplicate --path-rewrite\n");
        return -1;
    }
    eq = arg ? strchr(arg, '=') : NULL;
    if (!arg || !eq || eq == arg || eq[1] == '\0') {
        fprintf(stderr, "ereport_index: --path-rewrite must be OLD=NEW (got '%s')\n", arg ? arg : "");
        return -1;
    }
    fl = (size_t)(eq - arg);
    if (arg[0] != '/' || eq[1] != '/') {
        fprintf(stderr, "ereport_index: --path-rewrite OLD and NEW must both be absolute\n");
        return -1;
    }
    if (fl >= sizeof(g_rewrite_from_buf) ||
        snprintf(g_rewrite_to_buf, sizeof(g_rewrite_to_buf), "%s", eq + 1) >= (int)sizeof(g_rewrite_to_buf)) {
        fprintf(stderr, "ereport_index: --path-rewrite path too long\n");
        return -1;
    }
    memcpy(g_rewrite_from_buf, arg, fl);
    g_rewrite_from_buf[fl] = '\0';
    while (fl > 1 && g_rewrite_from_buf[fl - 1] == '/') g_rewrite_from_buf[--fl] = '\0';
    tl = strlen(g_rewrite_to_buf);
    while (tl > 1 && g_rewrite_to_buf[tl - 1] == '/') g_rewrite_to_buf[--tl] = '\0';
    if (fl < 2 || tl < 2) {
        fprintf(stderr, "ereport_index: --path-rewrite OLD and NEW must name a directory below root (not '/')\n");
        return -1;
    }
    g_rewrite_from = g_rewrite_from_buf;
    g_rewrite_from_len = fl;
    g_rewrite_to = g_rewrite_to_buf;
    g_rewrite_to_len = tl;
    return 0;
}
#define MEMLOG_INTERVAL_SEC 8
#define MERGE_IO_BUFSIZE (1U << 20)
/* path_offsets.bin entries staged per write, so a 1.3M-path capture costs a few hundred writes on the serial
 * paths writer instead of one per path. */
#define PATH_OFFSETS_STAGE_ENTRIES 8192U
/*
 * tmp_trigrams shards: accumulate records per (worker × bucket) and emit one frame
 * per ~64 KiB of source so framing overhead and read()/decode syscalls amortize over
 * thousands of records instead of the ~1 record per frame the per-batch path produced.
 *
 * Also the unit of buffer memory: one frame is held per *open* (worker × bucket) shard, so the
 * footprint is frame bytes × open shards. Bigger frames mean fewer, larger writes and better
 * compression, at more resident buffer. Tunable at runtime with EREPORT_INDEX_TRIGRAM_FRAME_BYTES.
 */
#define TRIGRAM_TMP_FRAME_BYTES (1U << 16)
#define TRIGRAM_TMP_FRAME_RECORDS (TRIGRAM_TMP_FRAME_BYTES / sizeof(trigram_record_t))
#define TRIGRAM_TMP_FRAME_BYTES_MIN (1U << 12)
#define TRIGRAM_TMP_FRAME_BYTES_MAX (1U << 24)
/*
 * Ceiling on merge workers; the pool is min(online CPUs, nonempty buckets, this).
 *
 * Do not raise it without a measurement that survives a repeat. Swept on 8M paths in one directory
 * (383M trigram records, only 1035 distinct trigrams, so 34 very uneven nonempty buckets on 64 CPUs)
 * merge_phase_sec went 4.31 s at 8 workers to 2.87 s at 16, then 2.89 s at 32 and 2.44 s at 34 --
 * but a second run of the same sweep put 34 workers at 3.17 s against 3.15 s at 16, so everything
 * past 16 was noise. Neither this constant nor RAM admission is the limit there (peak was 768 MiB
 * against a 693 GiB budget): temp-read throughput sits at ~1.5 GB/s whatever the worker count,
 * because the merge is bound by per-bucket frame decode and radix sort.
 */
#define MERGE_MAX_WORKERS 16
#define MERGE_PARALLEL_MIN 4
/*
 * Within-bucket parallel sort: a single hot bucket (tens of GiB) is otherwise radix-sorted by one
 * thread while the other merge workers idle (RAM admission blocks them), so the merge wall is pinned
 * to that serial tail. Buckets with at least this many records use a parallel MSD-partition + parallel
 * per-partition radix sort that borrows idle merge threads from a shared budget. Small buckets keep the
 * single-threaded path (they already run concurrently across workers). The output is byte-identical to
 * the serial radix sort. NOTE: disabled by default (see merge_init_thread_budget) because the merge is
 * I/O/bandwidth-bound, not sort-CPU-bound; enable only via EREPORT_INDEX_MERGE_SORT_THREADS. */
#define MERGE_PARALLEL_SORT_MIN_RECORDS (8ULL << 20)
/* Target records per sort thread: keeps small buckets from spawning many threads for trivial work. */
#define MERGE_RECORDS_PER_SORT_THREAD (4ULL << 20)
/* MSD partition fan-out (one byte). */
#define MERGE_SORT_PARTITIONS 256
/* Each parallel merge worker holds ~2× bucket file bytes (mmap + radix aux) plus stdio buffers — cap workers to avoid OOM. */
#define MERGE_PER_WORKER_OVERHEAD_BYTES (16ULL << 20)
/*
 * A bucket whose decompressed records exceed this is sorted as fixed-size slices (each radix-sorted with a
 * reused aux buffer of this many bytes) and k-way merged, instead of allocating an n-sized radix aux. This
 * caps a giant bucket's peak at ~1× records + one slice (≈60 GiB for the 58 GiB hot bucket) instead of 2×
 * (≈116 GiB), so its admission reservation no longer monopolizes the merge RAM budget and the other workers
 * run alongside it — with no extra disk I/O (everything stays in RAM). Tune with
 * EREPORT_INDEX_MERGE_SORT_SLICE_MB (min 64). */
#define MERGE_SORT_SLICE_BYTES_DEFAULT (2ULL << 30)
/* Fraction of min(MemAvailable, cgroup memory.max) used as the merge anonymous-RAM budget (sum of
 * concurrent workers' record+aux buffers, via admission control). Kept well below 100% on purpose:
 * the merge ALSO drives heavy file I/O (reading hundreds of GB of compressed tmp_trigrams, writing
 * merge_seg/tri_postings), and that page cache — especially dirty writeback to a slow/network index
 * dir — competes with these anon buffers for physical RAM. At 55% a 953M-path build pinned ~367 GiB
 * anon and the page cache pushed total over the 768 GiB host → OOM kill. 35% leaves the cache headroom.
 * Tune with EREPORT_INDEX_MERGE_RAM_FRAC / EREPORT_INDEX_MERGE_MEMORY_MB. */
#define MERGE_RAM_FRAC_NUM 35U
#define MERGE_RAM_FRAC_DEN 100U
/*
 * Publish scanned_records to the stats thread this often while a chunk is in flight.
 * Without this, a single large chunk can run for a long time with no visible rec: progress.
 */
#define SCANNED_RECORDS_PUBLISH_STRIDE 65536U

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

typedef crawl_bin_file_chunk_t file_chunk_t;

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
    uint8_t is_dir; /* 1 if crawl record type is 'd' */
} parsed_path_t;

typedef struct write_batch {
    parsed_path_t *items;
    size_t count;
    size_t cap;
    size_t approx_body_bytes; /* path strings + trigram codes; excludes batch struct overhead */
    struct wb_arena *arena;   /* holds every path string and codes array in this batch */
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
    struct wb_arena *arena;   /* job struct and codes live here; released once per job */
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
    crawl_bin_catalog_t *catalog;
    int use_journal; /* --journal-dir validation passed: replay the shard's .tij, skip the capture */
} file_state_t;

/*
 * paths.bin (EPATH002): the path bytes are split into chunks of at most PATHS_CHUNK_RAW, each compressed
 * independently so a search can decompress exactly one chunk per hit. Chunks are cut on path boundaries,
 * so no path ever straddles two of them.
 *
 * path_offsets.bin keeps storing offsets into the *uncompressed* stream, which is what makes this a local
 * change: path-id arithmetic, --resume-merge and the offsets file are all untouched.
 *
 *   [header PATHS_HDR_BYTES][chunk frames…][chunk table]
 *
 * The table is at the end because chunk_count is only known once the last path has been written; the
 * header records where it starts, so opening the file is still one seek and one sequential read.
 *
 * A trained zstd dictionary was measured and dropped. A 256 KiB chunk already holds ~1400 neighbouring
 * paths, so its window captures nearly all of the shared-prefix redundancy on its own: on a 199k-path
 * /usr corpus a 64 KiB dictionary shrank the frames by 1.7% but had to be stored, making the file 3%
 * larger overall — and ZDICT_trainFromBuffer cost 0.34 s on the paths writer, the only serial stage of
 * the --make pipeline.
 */
static const unsigned char PATHS_MAGIC[8] = {'E', 'P', 'A', 'T', 'H', '0', '0', '2'};
#define PATHS_FORMAT_VERSION 2U
#define PATHS_HDR_BYTES 40
#define PATHS_CHUNK_RAW ((size_t)256 * 1024)

/* One row per chunk; stored_len == raw_len means the chunk did not compress and is stored verbatim. */
typedef struct __attribute__((packed)) {
    uint64_t logical_start;
    uint64_t file_off;
    uint32_t stored_len;
    uint32_t raw_len;
} paths_chunk_ent_t;

/* Header + chunk table + dictionary of an EPATH002 paths.bin. Read once per search and shared read-only
 * across the path-filter workers, which is what keeps the table off the per-thread memory budget. */
typedef struct {
    paths_chunk_ent_t *table;
    uint64_t chunk_count;
    uint64_t total_logical;
    uint32_t max_raw;
    uint32_t max_stored;
} paths_index_t;

static int paths_index_open(const char *paths_path, paths_index_t *pi);
static void paths_index_close(paths_index_t *pi);

typedef struct {
    uid_t target_uid;
    int aggregate_all_users;
    char display_name[256];
    char index_dir[PATH_MAX];
    FILE *paths_fp;
    FILE *path_offsets_fp;
    FILE *path_isdir_fp; /* bit-packed: bit i set iff path_id i is a directory */
    /* paths.bin write position and the staged path_offsets.bin entries. Both belong to the single paths
     * writer thread, which is the pipeline's only serial stage: asking stdio for the position (ftello) cost
     * it an lseek per path, and the offsets went out one 8-byte fwrite at a time. */
    uint64_t paths_pos;
    uint64_t *path_offsets_buf;
    size_t path_offsets_n;
    uint8_t isdir_pending_byte;
    unsigned isdir_pending_bits;
    /* EPATH002 writer state, also owned exclusively by the paths writer thread. */
    uint64_t paths_file_pos;
    unsigned char *paths_chunk;
    size_t paths_chunk_len;
    uint64_t paths_chunk_logical;
    unsigned char *paths_comp;
    size_t paths_comp_cap;
    ZSTD_CCtx *paths_cctx;
    paths_chunk_ent_t *paths_table;
    size_t paths_table_n;
    size_t paths_table_cap;
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
    uint8_t *tw_worker_fp_magic; /* 1 if the EITG header is already on disk for this open fp */
    trigram_record_t **tw_worker_buf; /* per open shard: pending records, flushed as one frame */
    uint32_t *tw_worker_buf_n; /* records currently buffered in tw_worker_buf[ix] */
    uint32_t tw_frame_records;  /* records per emitted frame; see parse_trigram_frame_records */
    uint64_t *tw_worker_lru_age;
    uint32_t *tw_worker_open_count; /* [trigram_tmp_shard_count] */
    uint32_t tw_worker_max_open;
    uint64_t *tw_worker_lru_next_tick; /* [trigram_tmp_shard_count] — per-worker LRU clock (no atomics). */
    /* Compact list of currently-open bucket ids per worker so LRU-victim search is O(max_open), not
     * O(TRIGRAM_BUCKET_COUNT). tw_worker_open_list is [trigram_tmp_shard_count * tw_worker_max_open];
     * tw_worker_list_pos[ix] holds the index of bucket within its worker's open_list (valid only while open). */
    uint32_t *tw_worker_open_list;
    uint32_t *tw_worker_list_pos; /* [trigram_tmp_shard_count * TRIGRAM_BUCKET_COUNT] */
    index_run_stats_t *run_stats;
    int merge_workers_used;
    int merge_workers_cpu; /* before memory cap */
    uint64_t merge_max_bucket_bytes;
    uint64_t merge_parallel_budget_bytes;
    /* Shared budget of idle CPU threads a worker may borrow to parallel-sort a hot bucket. While a huge
     * bucket sorts, the other merge workers are blocked in RAM admission, so their cores are free. */
    pthread_mutex_t merge_thr_mu;
    int merge_thr_free;
    int merge_sort_threads_max; /* per-bucket cap (default ncpu, env override) */
    double index_phase_sec;
    int index_workers_used;
    int trigram_writer_workers_used;
    uint64_t chunk_path_bytes_total; /* sum strlen(chunk.path); for backlog estimate */
    size_t chunk_total_count;
    atomic_uint_fast64_t merge_bucket_ram_peak; /* ~2× largest bucket file during merge */
    /* Dir-index sidecar phase (dirs.idx / rowgroups.idx); zero when it did not run. */
    int dir_index_built;
    int rowgroup_index_built;
    uint64_t dir_index_dirs;
    uint64_t dir_index_groups;
    uint64_t dir_index_bytes;
    uint64_t rowgroup_index_bytes;
    double dir_index_sec;
} build_ctx_t;

typedef struct {
    work_queue_t *queue;
    write_queue_t *write_queue;
    file_state_t *file_states;
    build_ctx_t *ctx;
    size_t write_batch_flush_at;
    /* Reused per record: the reconstructed path and its trigram codes are copied
     * into the batch arena, so neither needs a malloc per path. */
    char *path_buf;
    size_t path_cap;
    trigram_scratch_t tri;
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

static size_t mk_fread_verbose(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t r = fread(ptr, size, nmemb, stream);

    mk_io_tls.fread_calls++;
    mk_io_tls.fread_bytes += (unsigned long long)(r * size);
    return r;
}

static size_t mk_fwrite_verbose(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t w = fwrite(ptr, size, nmemb, stream);

    mk_io_tls.fwrite_calls++;
    mk_io_tls.fwrite_bytes += (unsigned long long)(w * size);
    return w;
}

static FILE *mk_fopen_verbose(const char *path, const char *mode) {
    mk_io_tls.fopen_calls++;
    return fopen(path, mode);
}

static int mk_fclose_verbose(FILE *stream) {
    mk_io_tls.fclose_calls++;
    return fclose(stream);
}

static int mk_open_verbose(const char *pathname, int flags, ...) {
    va_list ap;
    int ret;

    mk_io_tls.open_calls++;
    if (flags & O_CREAT) {
        mode_t mode;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
        ret = open(pathname, flags, mode);
    } else {
        ret = open(pathname, flags);
    }
    return ret;
}

static ssize_t mk_read_verbose(int fd, void *buf, size_t count) {
    ssize_t n = read(fd, buf, count);

    mk_io_tls.read_calls++;
    if (n > 0) mk_io_tls.read_bytes += (unsigned long long)n;
    return n;
}

static void *mk_mmap_verbose(void *addr, size_t length, int prot, int flags, int fd, off_t offset) {
    mk_io_tls.mmap_calls++;
    return mmap(addr, length, prot, flags, fd, offset);
}

static int mk_munmap_verbose(void *addr, size_t length) {
    mk_io_tls.munmap_calls++;
    return munmap(addr, length);
}

static crawl_bin_chunk_stdio_t index_chunk_stdio;

static FILE *(*ei_fopen)(const char *, const char *) = fopen;
static size_t (*ei_fread)(void *, size_t, size_t, FILE *) = fread;
static int (*ei_fclose)(FILE *) = fclose;
static size_t (*ei_fwrite)(const void *, size_t, size_t, FILE *) = fwrite;
static int (*ei_open)(const char *, int, ...) = open;
static ssize_t (*ei_read)(int, void *, size_t) = read;
static void *(*ei_mmap)(void *, size_t, int, int, int, off_t) = mmap;
static int (*ei_munmap)(void *, size_t) = munmap;

static void ei_note_trigram_append_nop(void) {}
static void (*ei_note_trigram_append_batch)(void) = ei_note_trigram_append_nop;

static void ei_finalize_chunk_nop(index_run_stats_t *rs, file_state_t *fs, size_t fi) {
    (void)rs;
    if (!fs) return;
    (void)atomic_fetch_sub(&fs[fi].remaining_chunks, 1U);
}

static void (*ei_finalize_chunk_file)(index_run_stats_t *, file_state_t *, size_t) = ei_finalize_chunk_nop;

static void ei_note_trigram_append_verbose(void) { mk_io_tls.trigram_append_batches++; }

static void finalize_chunk_progress_track(index_run_stats_t *rs, file_state_t *file_states, size_t file_index) {
    unsigned int old_remaining;

    if (!file_states || !rs) return;
    atomic_fetch_add_explicit(&rs->chunks_index_done, 1ULL, memory_order_relaxed);
    old_remaining = atomic_fetch_sub(&file_states[file_index].remaining_chunks, 1U);
    if (old_remaining == 1U) atomic_fetch_add(&rs->scanned_input_files, 1U);
}

/*
 * Shard reads only: the same shard is opened for its catalog and again for each of its
 * chunks, and glibc funnels every fopen/fclose in the process through one stdio list lock.
 * Deliberately not used for the merge spill files — those are unlinked right after they are
 * read, and a parked handle would hold their (multi-GB) blocks until the thread exits.
 */
static FILE *ei_shard_fopen(const char *path, const char *mode) {
    if (g_verbose) mk_io_tls.fopen_calls++;
    return crawl_fpcache_fopen(path, mode);
}

static int ei_shard_fclose(FILE *stream) {
    if (g_verbose) mk_io_tls.fclose_calls++;
    return crawl_fpcache_fclose(stream);
}

static void ereport_index_sync_chunk_stdio(void) {
    index_chunk_stdio.fopen = ei_shard_fopen;
    index_chunk_stdio.fread = ei_fread;
    index_chunk_stdio.fclose = ei_shard_fclose;
}

static void ereport_index_install_verbose_io(void) {
    if (g_verbose) {
        ei_fopen = mk_fopen_verbose;
        ei_fread = mk_fread_verbose;
        ei_fclose = mk_fclose_verbose;
        ei_fwrite = mk_fwrite_verbose;
        ei_open = mk_open_verbose;
        ei_read = mk_read_verbose;
        ei_mmap = mk_mmap_verbose;
        ei_munmap = mk_munmap_verbose;
        ei_note_trigram_append_batch = ei_note_trigram_append_verbose;
        ei_finalize_chunk_file = finalize_chunk_progress_track;
    } else {
        ei_fopen = fopen;
        ei_fread = fread;
        ei_fclose = fclose;
        ei_fwrite = fwrite;
        ei_open = open;
        ei_read = read;
        ei_mmap = mmap;
        ei_munmap = munmap;
        ei_note_trigram_append_batch = ei_note_trigram_append_nop;
        ei_finalize_chunk_file = ei_finalize_chunk_nop;
    }
    ereport_index_sync_chunk_stdio();
}

#define mk_fopen(path, mode) ((*ei_fopen)((path), (mode)))
#define mk_fclose(stream) ((*ei_fclose)((stream)))
#define mk_fread(ptr, size, nmemb, stream) ((*ei_fread)((ptr), (size), (nmemb), (stream)))
#define mk_fwrite(ptr, size, nmemb, stream) ((*ei_fwrite)((ptr), (size), (nmemb), (stream)))
#define mk_open(pathname, flags) ((*ei_open)((pathname), (flags)))
#define mk_read(fd, buf, count) ((*ei_read)((fd), (buf), (count)))
#define mk_mmap(addr, length, prot, flags, fd, offset) ((*ei_mmap)((addr), (length), (prot), (flags), (fd), (offset)))
#define mk_munmap(addr, length) ((*ei_munmap)((addr), (length)))

/*
 * tmp_trigrams_*.bin: 8-byte magic, then [u32 n_recs][u32 payload_bytes][payload]* frames.
 *
 * The payload is a delta-varint stream, not a zstd frame. Records reach a frame as runs of one path_id
 * with strictly ascending trigrams that all share the file's bucket (see append_trigram_codes_batch_parallel),
 * so a varint pass gets them to 2-3 bytes per record for a fraction of zstd's cost. These files are written
 * and read back inside one --make and never outlive it, so there is no on-disk format to stay compatible with.
 *
 * Each group is a maximal run of equal path_id with strictly ascending trigrams, encoded as
 *   varint(zigzag(path_id - prev_path_id)) varint(count - 1) varint(zigzag(trigram - prev_trigram))
 *   followed by count-1 x varint(trigram delta - 1).
 * prev_path_id and prev_trigram carry across groups within a frame and start at 0; frames are independent.
 */
static const unsigned char TRIGRAM_TMP_MAGIC[8] = {'E', 'I', 'T', 'G', '0', '0', '0', '2'};

/* Worst case for a one-record group: 10-byte path delta, 1-byte count, 5-byte trigram delta. */
#define TRIGRAM_TMP_MAX_BYTES_PER_REC 16

static int decode_varint_u64_buf(const unsigned char *buf, size_t len, size_t *pos, uint64_t *out);

static inline unsigned char *tmp_trigram_put_varint(unsigned char *p, uint64_t v) {
    while (v >= 0x80U) {
        *p++ = (unsigned char)((v & 0x7FU) | 0x80U);
        v >>= 7;
    }
    *p++ = (unsigned char)v;
    return p;
}

static inline uint64_t tmp_trigram_zigzag(int64_t v) {
    return ((uint64_t)v << 1) ^ (uint64_t)(v < 0 ? -1 : 0);
}

static inline int64_t tmp_trigram_unzigzag(uint64_t u) {
    return (int64_t)((u >> 1) ^ (uint64_t)(-(int64_t)(u & 1U)));
}

/* Compression level for the durable index (paths.bin, tri_postings.bin), from EREPORT_INDEX_ZSTD_LEVEL.
 * Separate from the temp level: temp files live for one run, these are read by every search. */
static int durable_zstd_level(void) {
    if (g_durable_zstd_level >= 0) return g_durable_zstd_level;
    {
        const char *env = getenv("EREPORT_INDEX_ZSTD_LEVEL");
        int level = 3;

        if (env && env[0]) {
            char *end = NULL;
            long v = strtol(env, &end, 10);

            if (end != env && *end == '\0' && v >= 1 && v <= ZSTD_maxCLevel())
                level = (int)v;
        }
        g_durable_zstd_level = level;
    }
    return g_durable_zstd_level;
}

static int tmp_trigram_read_exact_fd(int fd, void *buf, size_t n) {
    unsigned char *p = (unsigned char *)buf;
    size_t left = n;

    while (left > 0) {
        ssize_t r = mk_read(fd, p, left);

        if (r <= 0) return -1;
        p += (size_t)r;
        left -= (size_t)r;
    }
    return 0;
}

/* Encode scratch, kept per thread: every trigram worker flushes frames constantly,
 * and a malloc/free pair per frame is another cross-thread arena round-trip. */
static __thread unsigned char *tls_encbuf;
static __thread size_t tls_enccap;
static pthread_key_t g_encscratch_key;
static pthread_once_t g_encscratch_once = PTHREAD_ONCE_INIT;

static void tmp_trigram_scratch_free(void *unused) {
    (void)unused;
    free(tls_encbuf);
    tls_encbuf = NULL;
    tls_enccap = 0;
}

static void tmp_trigram_scratch_key_init(void) {
    (void)pthread_key_create(&g_encscratch_key, tmp_trigram_scratch_free);
}

static unsigned char *tmp_trigram_encbuf(size_t need) {
    if (need > tls_enccap) {
        unsigned char *p = (unsigned char *)realloc(tls_encbuf, need);

        if (!p) return NULL;
        tls_encbuf = p;
        tls_enccap = need;
        /* The key exists only so this thread's buffer is released when it exits. */
        (void)pthread_once(&g_encscratch_once, tmp_trigram_scratch_key_init);
        (void)pthread_setspecific(g_encscratch_key, (void *)tls_encbuf);
    }
    return tls_encbuf;
}

static int tmp_trigram_write_frame(FILE *fp, const trigram_record_t *recs, size_t n) {
    unsigned char *out;
    unsigned char *p;
    size_t i = 0;
    uint64_t prev_pid = 0;
    uint32_t prev_tri = 0;
    size_t len;
    uint32_t n32;
    uint32_t len32;

    if (n == 0) return 0;
    if (n > UINT32_MAX) return -1;
    out = tmp_trigram_encbuf(n * TRIGRAM_TMP_MAX_BYTES_PER_REC);
    if (!out) return -1;
    p = out;
    while (i < n) {
        uint64_t pid = recs[i].path_id;
        size_t j = i + 1;
        size_t k;

        while (j < n && recs[j].path_id == pid && recs[j].trigram > recs[j - 1].trigram) j++;
        p = tmp_trigram_put_varint(p, tmp_trigram_zigzag((int64_t)(pid - prev_pid)));
        p = tmp_trigram_put_varint(p, (uint64_t)(j - i - 1));
        p = tmp_trigram_put_varint(p, tmp_trigram_zigzag((int64_t)recs[i].trigram - (int64_t)prev_tri));
        for (k = i + 1; k < j; k++)
            p = tmp_trigram_put_varint(p, (uint64_t)(recs[k].trigram - recs[k - 1].trigram - 1U));
        prev_pid = pid;
        prev_tri = recs[j - 1].trigram;
        i = j;
    }
    len = (size_t)(p - out);
    if (len > UINT32_MAX) return -1;
    n32 = (uint32_t)n;
    len32 = (uint32_t)len;
    if (mk_fwrite(&n32, 1, 4, fp) != 4) return -1;
    if (mk_fwrite(&len32, 1, 4, fp) != 4) return -1;
    if (mk_fwrite(out, 1, len, fp) != len) return -1;
    return 0;
}

/* Inverse of tmp_trigram_write_frame; `out` must have room for exactly `n` records. */
static int tmp_trigram_decode_frame(const unsigned char *buf, size_t len, trigram_record_t *out, size_t n) {
    size_t pos = 0;
    size_t done = 0;
    uint64_t prev_pid = 0;
    uint32_t prev_tri = 0;

    while (done < n) {
        uint64_t zpid;
        uint64_t count_m1;
        uint64_t ztri;
        uint64_t pid;
        uint32_t tri;
        uint64_t k;

        if (decode_varint_u64_buf(buf, len, &pos, &zpid) != 0) return -1;
        if (decode_varint_u64_buf(buf, len, &pos, &count_m1) != 0) return -1;
        if (decode_varint_u64_buf(buf, len, &pos, &ztri) != 0) return -1;
        if (count_m1 >= (uint64_t)(n - done)) return -1;
        pid = prev_pid + (uint64_t)tmp_trigram_unzigzag(zpid);
        tri = (uint32_t)((int64_t)prev_tri + tmp_trigram_unzigzag(ztri));
        out[done].trigram = tri;
        out[done].path_id = pid;
        done++;
        for (k = 0; k < count_m1; k++) {
            uint64_t d;

            if (decode_varint_u64_buf(buf, len, &pos, &d) != 0) return -1;
            tri = (uint32_t)(tri + (uint32_t)d + 1U);
            out[done].trigram = tri;
            out[done].path_id = pid;
            done++;
        }
        prev_pid = pid;
        prev_tri = tri;
    }
    return pos == len ? 0 : -1;
}

static int tmp_trigram_fp_write_batch(FILE *fp, uint8_t *magic_on_disk, const trigram_record_t *recs, size_t n) {
    if (n == 0) return 0;
    if (!magic_on_disk || !*magic_on_disk) {
        if (mk_fwrite(TRIGRAM_TMP_MAGIC, 1, 8, fp) != 8) return -1;
        if (magic_on_disk) *magic_on_disk = 1;
    }
    return tmp_trigram_write_frame(fp, recs, n);
}

/* 1 if an existing tmp_trigrams shard already holds the EITG header (i.e. is non-empty); 0 for new/empty. */
static int tmp_trigram_file_has_magic(const char *path) {
    struct stat st;

    return stat(path, &st) == 0 && st.st_size > 0;
}

static int tmp_trigram_count_file_records(const char *path, size_t *n_out, uint64_t *bytes_out) {
    struct stat st;
    int fd;
    unsigned char hdr[8];
    ssize_t hr;

    if (n_out) *n_out = 0;
    if (bytes_out) *bytes_out = 0;
    if (stat(path, &st) != 0 || st.st_size <= 0) return -1;

    fd = mk_open(path, O_RDONLY);
    if (fd < 0) return -1;
    hr = mk_read(fd, hdr, 8);
    if (hr != 8 || memcmp(hdr, TRIGRAM_TMP_MAGIC, 8) != 0) {
        close(fd);
        return -1;
    }
    {
        size_t n = 0;

        for (;;) {
            uint32_t frame_n;
            uint32_t clen;
            ssize_t r = mk_read(fd, &frame_n, 4);

            if (r == 0) break;
            if (r != 4 || tmp_trigram_read_exact_fd(fd, &clen, 4) != 0) {
                close(fd);
                return -1;
            }
            if (clen > 0 && lseek(fd, (off_t)clen, SEEK_CUR) < 0) {
                close(fd);
                return -1;
            }
            n += (size_t)frame_n;
        }
        close(fd);
        if (n_out) *n_out = n;
        if (bytes_out) *bytes_out = n * sizeof(trigram_record_t);
        return 0;
    }
}

static int tmp_trigram_load_file(const char *path, trigram_record_t **recs_out, size_t *n_out, uint64_t *bytes_out) {
    struct stat st;
    int fd = -1;
    unsigned char hdr[8];
    ssize_t hr;

    if (recs_out) *recs_out = NULL;
    if (n_out) *n_out = 0;
    if (bytes_out) *bytes_out = 0;
    if (stat(path, &st) != 0 || st.st_size <= 0) return -1;

    fd = mk_open(path, O_RDONLY);
    if (fd < 0) return -1;
    hr = mk_read(fd, hdr, 8);
    if (hr != 8 || memcmp(hdr, TRIGRAM_TMP_MAGIC, 8) != 0) {
        close(fd);
        return -1;
    }
    {
        size_t cap = 4096;
        size_t n = 0;
        unsigned frame_idx = 0;
        trigram_record_t *recs = (trigram_record_t *)malloc(cap * sizeof(*recs));

        if (!recs) {
            close(fd);
            return -1;
        }
        for (;;) {
            uint32_t frame_n;
            uint32_t clen;
            ssize_t r = mk_read(fd, &frame_n, 4);

            if (r == 0) break;
            if (r != 4 || tmp_trigram_read_exact_fd(fd, &clen, 4) != 0) {
                fprintf(stderr, "  %s: truncated frame header at frame %u (file ends mid-write)\n", path, frame_idx);
                free(recs);
                close(fd);
                return -1;
            }
            if (frame_n == 0) continue;
            {
                unsigned char *comp = (unsigned char *)malloc((size_t)clen);

                if (!comp) {
                    free(recs);
                    close(fd);
                    return -1;
                }
                if (tmp_trigram_read_exact_fd(fd, comp, (size_t)clen) != 0) {
                    fprintf(stderr, "  %s: short read of frame %u (%u bytes, file truncated)\n", path, frame_idx,
                            clen);
                    free(comp);
                    free(recs);
                    close(fd);
                    return -1;
                }
                if (n + (size_t)frame_n > cap) {
                    size_t need = n + (size_t)frame_n;
                    size_t new_cap = cap;

                    while (new_cap < need) new_cap *= 2;
                    {
                        trigram_record_t *np = (trigram_record_t *)realloc(recs, new_cap * sizeof(*recs));

                        if (!np) {
                            free(comp);
                            free(recs);
                            close(fd);
                            return -1;
                        }
                        recs = np;
                        cap = new_cap;
                    }
                }
                if (tmp_trigram_decode_frame(comp, (size_t)clen, recs + n, (size_t)frame_n) != 0) {
                    free(comp);
                    fprintf(stderr, "  %s: cannot decode frame %u (%u records, %u bytes; corrupt frame)\n", path,
                            frame_idx, frame_n, clen);
                    free(recs);
                    close(fd);
                    return -1;
                }
                free(comp);
                n += (size_t)frame_n;
            }
            frame_idx++;
        }
        close(fd);
        if (recs_out) *recs_out = recs;
        else free(recs);
        if (n_out) *n_out = n;
        if (bytes_out) *bytes_out = n * sizeof(trigram_record_t);
        return 0;
    }
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

/*
 * Bump arena behind one write batch.
 *
 * A path and its trigram codes are produced by a parse worker, written out by
 * the paths writer, and consumed by whichever trigram workers get the slices of
 * that batch's job chain. Allocating each of those per path meant every free
 * crossed back into the producing thread's malloc arena: profiling showed 60% of
 * index-phase CPU inside free(), nearly all of it blocked on the glibc arena
 * lock. One arena per batch turns thousands of cross-thread frees into a handful.
 *
 * refs is one for the batch plus one per trigram job carved out of it, so the
 * last holder releases the chunks no matter who finishes first.
 */
#define WB_ARENA_CHUNK_BYTES (256U * 1024U)

typedef struct wb_chunk {
    struct wb_chunk *next;
    size_t used;
    size_t cap;
    unsigned char data[];
} wb_chunk_t;

typedef struct wb_arena {
    atomic_uint refs;
    wb_chunk_t *head; /* bump target; older chunks trail behind it */
} wb_arena_t;

/*
 * Chunks are recycled through a small per-thread freelist instead of going back to malloc.
 *
 * The arena already collapsed thousands of per-string frees into one free per chunk, but
 * wb_arena_release still showed 6.6% self time on a 12 M-path build: every batch mallocs its
 * chunks on a parse worker and frees them wherever the last job holding the arena happens to
 * finish, so the chunks keep crossing malloc arenas. They are all the same size and immediately
 * wanted again, so holding a few per thread turns that into a pointer swap. The freelist is
 * capped, and a chunk parked on one thread is simply reused by that thread later -- the cap is
 * what keeps a thread that releases more than it allocates from hoarding memory.
 */
#define WB_CHUNK_CACHE_MAX 8U

typedef struct {
    wb_chunk_t *head;
    unsigned count;
} wb_chunk_cache_t;

static pthread_key_t wb_chunk_cache_key;
static pthread_once_t wb_chunk_cache_once = PTHREAD_ONCE_INIT;

static void wb_chunk_cache_free(void *p) {
    wb_chunk_cache_t *cache = (wb_chunk_cache_t *)p;
    wb_chunk_t *c;

    if (!cache) return;
    c = cache->head;
    while (c) {
        wb_chunk_t *next = c->next;

        free(c);
        c = next;
    }
    free(cache);
}

static void wb_chunk_cache_key_init(void) {
    (void)pthread_key_create(&wb_chunk_cache_key, wb_chunk_cache_free);
}

/* NULL is a valid answer: without a cache the chunk just goes to malloc, as it used to. */
static wb_chunk_cache_t *wb_chunk_cache(void) {
    wb_chunk_cache_t *cache;

    if (pthread_once(&wb_chunk_cache_once, wb_chunk_cache_key_init) != 0) return NULL;
    cache = (wb_chunk_cache_t *)pthread_getspecific(wb_chunk_cache_key);
    if (!cache) {
        cache = (wb_chunk_cache_t *)calloc(1, sizeof(*cache));
        if (cache && pthread_setspecific(wb_chunk_cache_key, cache) != 0) {
            free(cache);
            cache = NULL;
        }
    }
    return cache;
}

/* Only standard-size chunks are pooled; an oversized one exists for a single large allocation. */
static wb_chunk_t *wb_chunk_get(size_t cap) {
    wb_chunk_t *c;

    if (cap == WB_ARENA_CHUNK_BYTES) {
        wb_chunk_cache_t *cache = wb_chunk_cache();

        if (cache && cache->head) {
            c = cache->head;
            cache->head = c->next;
            cache->count--;
            return c;
        }
    }
    c = (wb_chunk_t *)malloc(sizeof(*c) + cap);
    if (c) c->cap = cap;
    return c;
}

static void wb_chunk_put(wb_chunk_t *c) {
    wb_chunk_cache_t *cache;

    if (c->cap != WB_ARENA_CHUNK_BYTES) {
        free(c);
        return;
    }
    cache = wb_chunk_cache();
    if (!cache || cache->count >= WB_CHUNK_CACHE_MAX) {
        free(c);
        return;
    }
    c->next = cache->head;
    cache->head = c;
    cache->count++;
}

static wb_arena_t *wb_arena_create(void) {
    wb_arena_t *a = (wb_arena_t *)malloc(sizeof(*a));

    if (!a) return NULL;
    atomic_init(&a->refs, 1U);
    a->head = NULL;
    return a;
}

static void *wb_arena_alloc(wb_arena_t *a, size_t n, size_t align) {
    size_t pad;
    void *p;

    if (!a) return NULL;
    if (n == 0) n = 1;

    if (a->head) {
        pad = (align - (a->head->used & (align - 1U))) & (align - 1U);
        if (a->head->used + pad + n <= a->head->cap) {
            p = a->head->data + a->head->used + pad;
            a->head->used += pad + n;
            return p;
        }
    }

    {
        size_t cap = WB_ARENA_CHUNK_BYTES;
        wb_chunk_t *c;

        if (cap < n) cap = n;
        c = wb_chunk_get(cap);
        if (!c) return NULL;
        c->used = n;
        /* Keep the chunk with room at the front so the next bump finds it first. */
        if (a->head && a->head->cap - a->head->used > cap - n) {
            c->next = a->head->next;
            a->head->next = c;
        } else {
            c->next = a->head;
            a->head = c;
        }
        return c->data;
    }
}

static void wb_arena_release(wb_arena_t *a) {
    wb_chunk_t *c;

    if (!a) return;
    if (atomic_fetch_sub_explicit(&a->refs, 1U, memory_order_acq_rel) != 1U) return;

    c = a->head;
    while (c) {
        wb_chunk_t *next = c->next;

        wb_chunk_put(c);
        c = next;
    }
    free(a);
}

static write_batch_t *write_batch_create(void) {
    write_batch_t *batch = (write_batch_t *)calloc(1, sizeof(*batch));

    if (!batch) return NULL;
    batch->arena = wb_arena_create();
    if (!batch->arena) {
        free(batch);
        return NULL;
    }
    return batch;
}

/* Copies path and codes into the batch arena; the caller keeps its own buffers. */
static int write_batch_append(write_batch_t *batch, const char *path, size_t path_len, const uint32_t *codes,
                              size_t code_count, int is_dir) {
    parsed_path_t *tmp;
    char *path_copy;
    uint32_t *codes_copy = NULL;

    if (batch->count == batch->cap) {
        size_t new_cap = batch->cap ? batch->cap * 2 : 256;
        tmp = (parsed_path_t *)realloc(batch->items, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        batch->items = tmp;
        batch->cap = new_cap;
    }

    path_copy = (char *)wb_arena_alloc(batch->arena, path_len + 1U, 1U);
    if (!path_copy) return -1;
    memcpy(path_copy, path, path_len);
    path_copy[path_len] = '\0';

    if (code_count > 0) {
        codes_copy = (uint32_t *)wb_arena_alloc(batch->arena, code_count * sizeof(*codes_copy), sizeof(*codes_copy));
        if (!codes_copy) return -1;
        memcpy(codes_copy, codes, code_count * sizeof(*codes_copy));
    }

    batch->items[batch->count].path = path_copy;
    batch->items[batch->count].codes = codes_copy;
    batch->items[batch->count].code_count = code_count;
    batch->items[batch->count].is_dir = is_dir ? 1U : 0U;
    batch->approx_body_bytes += sizeof(parsed_path_t) + path_len + 1U + code_count * sizeof(uint32_t);
    batch->count++;
    return 0;
}

/* Drops the batch's arena reference; jobs already carved out of it keep theirs. */
static void write_batch_destroy(write_batch_t *batch) {
    if (!batch) return;
    wb_arena_release(batch->arena);
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

/* Release a chain of jobs that were handed off (queued): each drops one arena reference.
 * Never call this on a chain the paths writer has not pushed yet — the batch still holds
 * the only reference then, and write_batch_destroy releases it. */
static void trigram_job_chain_free(trigram_job_t *head) {
    while (head) {
        trigram_job_t *next = head->next;
        wb_arena_release(head->arena);
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
            /* Wake one worker, not all 32. A batch-popping worker drains many
             * jobs per lock and cascades a wake while work remains, so the
             * awake-worker count tracks available work without a thundering
             * herd (broadcast here was ~40% of all CPU under contention). */
            pthread_cond_signal(&q->has_job);
        }
        pthread_mutex_unlock(&q->mutex);
    }
    return 0;
}

/* Jobs drained per lock acquisition. Larger batches amortize the queue mutex
 * (one lock/unlock + one signal pair per ~256 jobs instead of per job), which
 * is the dominant cost on large indexes (millions of one-path jobs). */
#define TRIGRAM_POP_BATCH_MAX 256

/* Pop a chain of up to TRIGRAM_POP_BATCH_MAX jobs (caller iterates head->next,
 * detaching/freeing each). Returns NULL only when the queue is closed and
 * drained. Cascades a single has_job wake when work remains so concurrency
 * scales with the backlog without broadcasting to every worker. */
static trigram_job_t *trigram_job_queue_pop_batch_wait(trigram_job_queue_t *q) {
    trigram_job_t *head, *tail;
    size_t taken;
    uint64_t body;

    pthread_mutex_lock(&q->mutex);
    while (!q->head && !q->closed) {
        if (g_verbose)
            atomic_fetch_add_explicit(&q->run_stats->trigramq_worker_waits, 1ULL, memory_order_relaxed);
        pthread_cond_wait(&q->has_job, &q->mutex);
    }
    head = q->head;
    if (!head) {
        pthread_mutex_unlock(&q->mutex);
        return NULL; /* closed and empty */
    }
    tail = head;
    taken = 1;
    body = (uint64_t)tail->approx_body_bytes;
    while (taken < TRIGRAM_POP_BATCH_MAX && tail->next) {
        tail = tail->next;
        body += (uint64_t)tail->approx_body_bytes;
        taken++;
    }
    q->head = tail->next;
    if (!q->head) q->tail = NULL;
    tail->next = NULL;
    q->depth -= taken;
    q->queued_body_bytes -= body;
    /* More work left: wake one more worker (cascade, not herd). */
    if (q->head) pthread_cond_signal(&q->has_job);
    /* Freed `taken` slots; wake one blocked producer (queue is rarely full). */
    pthread_cond_signal(&q->has_space);
    pthread_mutex_unlock(&q->mutex);
    return head;
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

/*
 * The status tick and the memory log idle between wakeups. Polling their flags in fixed 100 ms
 * slices left each join waiting out the in-flight slice, so every --make paid that as pure idle
 * wall time — 0.20 s of which 0.14 s was idle on a 402-path capture. Wait on a condvar against an
 * absolute deadline (a broadcast meant for the other helper resumes the remaining time rather
 * than ticking early) and broadcast when a stop or an early wake is requested.
 */
static pthread_mutex_t g_helper_wait_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_helper_wait_cond = PTHREAD_COND_INITIALIZER;

static void helper_wait_broadcast(void) {
    pthread_mutex_lock(&g_helper_wait_lock);
    pthread_cond_broadcast(&g_helper_wait_cond);
    pthread_mutex_unlock(&g_helper_wait_lock);
}

static void helper_wait_until(double seconds, int (*ready)(void *), void *arg) {
    struct timespec deadline;
    long nsec;
    int rc = 0;

    if (ready(arg)) return;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += (time_t)seconds;
    nsec = deadline.tv_nsec + (long)((seconds - (double)(time_t)seconds) * 1e9);
    if (nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        nsec -= 1000000000L;
    }
    deadline.tv_nsec = nsec;

    pthread_mutex_lock(&g_helper_wait_lock);
    while (!ready(arg) && rc != ETIMEDOUT)
        rc = pthread_cond_timedwait(&g_helper_wait_cond, &g_helper_wait_lock, &deadline);
    pthread_mutex_unlock(&g_helper_wait_lock);
}

static int memlog_stop_ready(void *arg) {
    const memlog_shared_t *ml = (const memlog_shared_t *)arg;

    return atomic_load_explicit(&ml->stop, memory_order_acquire) != 0;
}

static void memlog_wait_interval(memlog_shared_t *ml) {
    helper_wait_until((double)MEMLOG_INTERVAL_SEC, memlog_stop_ready, ml);
}

static void memlog_shutdown(memlog_shared_t *ml, pthread_t *tid, int *started) {
    if (!started || !*started || !ml) return;
    atomic_store_explicit(&ml->stop, 1, memory_order_release);
    helper_wait_broadcast();
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
            memlog_wait_interval(ml);
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

        memlog_wait_interval(ml);
    }
    return NULL;
}

static int stats_cycle_ready(void *arg) {
    index_run_stats_t *rs = (index_run_stats_t *)arg;

    if (atomic_load(&rs->stop_stats)) return 1;
    return atomic_exchange_explicit(&rs->stats_wake, 0, memory_order_relaxed) != 0;
}

/* ~1s between status lines, but wake early when main signals (e.g. index phase just started). */
static void stats_wait_cycle(index_run_stats_t *rs) {
    helper_wait_until(1.0, stats_cycle_ready, rs);
}

static void index_stats_stop_request(index_run_stats_t *rs) {
    atomic_store(&rs->stop_stats, 1);
    helper_wait_broadcast();
}

static void index_stats_wake_request(index_run_stats_t *rs) {
    atomic_store_explicit(&rs->stats_wake, 1, memory_order_relaxed);
    helper_wait_broadcast();
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
            "  %s --make [--index-dir <path>] [--subtree <abs-path>] [--path-rewrite OLD=NEW] [--no-dir-index] [--journal-dir <path>] [username|uid] [bin_dir ...]\n"
            "  %s --resume-merge --index-dir <path>\n"
            "  %s --search [--index-dir <path>] <term> [--json] [--skip N] [--limit M]\n"
            "  Optional --verbose anywhere: detailed stderr progress, queue-wait stats, rusage, and I/O counters\n"
            "  for --make / --resume-merge; plain --search prints timing to stderr. Default output is a short summary.\n",
            argv0,
            argv0,
            argv0);
    fprintf(stderr,
            "  --make: Optional --index-dir <path> (must follow --make) writes index files directly under\n"
            "    <path> (paths.bin, tri_keys.bin, …); created if it does not exist. Default is ./<username>/index/ or ./all_users/index/.\n"
            "    Multiple bin_dir arguments are merged like ereport. If the first token after flags is a valid\n"
            "    login or numeric uid, it selects that user; remaining arguments are crawl directories (default .).\n"
            "    If that token is not a known user, every argument is a crawl directory (all-users index).\n"
            "    With no user/bin arguments after flags, the index is all-users for ./\n"
            "    Optional --subtree <abs-path> (may precede or follow --index-dir): index only records at or under\n"
            "    that directory (full absolute paths kept), mirroring `ereport --subtree` so search covers just that subtree.\n"
            "    Optional --path-rewrite OLD=NEW (both absolute dirs): relabel the OLD path prefix as NEW while indexing\n"
            "    (bins unchanged), e.g. /data1/group=/orcd/data; applied before --subtree.\n"
            "    Optional --journal-dir <path>: replay crawl-time trigram journals (ecrawl --trigram-journal DIR)\n"
            "    instead of parsing captures for shards whose journal matches the live .bin (basename, size, mtime,\n"
            "    catalog offset/entries). Missing or stale journals fall back to the capture parse per shard.\n"
            "    Journals that were replayed are deleted once the index build succeeds.\n"
            "    Also writes the directory-index sidecars dirs.idx and rowgroups.idx (~24 bytes per directory)\n"
            "    that `ecrawl_query --index-dir` uses to answer a subtree rollup without reading every catalog\n"
            "    row, and to scan only the row groups that can hold the subtree. --no-dir-index skips that phase;\n"
            "    they are rebuildable, and a reader that cannot find or verify them falls back to the catalogs.\n"
            "  --resume-merge: After paths.bin and path_offsets.bin exist, rebuild tri_keys.bin and tri_postings.bin\n"
            "    from remaining tmp_trigrams_*.bin and merge_seg_* files (e.g. after OOM during merge). Requires\n"
            "    --index-dir. Deletes partial tri_keys.bin / tri_postings.bin first.\n"
            "  --search: Optional --index-dir <path> (same flag as --make); default index dir is ./index.\n"
            "    Plain search prints paths.\n"
            "    With --json: UTF-8 JSON object {\"total\",\"skip\",\"limit\",\"search_ms\",\"index_keys\",\"indexed_paths\",\"paths\":[...]}\n"
            "      (indexed_paths from meta.txt = corpus size; index_keys = distinct trigrams in tri_keys.bin.)\n"
            "    Search: intersects rarest trigrams first and stops early, then filters the candidate\n"
            "    paths in parallel via EREPORT_INDEX_THREADS (default 32) when the candidate set is large.\n"
            "    --make tuning: EREPORT_INDEX_TRIGRAM_THREADS (default: same as EREPORT_INDEX_THREADS) parallel\n"
            "    writers to tmp_trigram bucket files; EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH (default scales with\n"
            "    EREPORT_INDEX_TRIGRAM_THREADS, max 16384 unless set; range 512…262144)\n"
            "    bounded queue from paths writer to trigram workers; EREPORT_INDEX_WRITE_BATCH_PATHS (default 4096,\n"
            "    512…65536) paths per batch to the writer. Also EREPORT_INDEX_WRITEQ_MAX_BATCHES, EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS.\n"
            "    Large --make: run `ulimit -n 65535` (or higher) first. \"File size limit exceeded\" / SIGXFSZ is\n"
            "    RLIMIT_FSIZE: use `ulimit -f unlimited` — bash `ulimit -f N` is kilobytes (N×1024 bytes), not like -n.\n"
            "    See README.\n");
    exit(2);
}

/* Below this soft RLIMIT_FSIZE, --make / resume-merge are likely to hit SIGXFSZ on real corpora; warn once. */
#define EREPORT_INDEX_WARN_FSIZE_BELOW_BYTES (64ULL * 1024ULL * 1024ULL * 1024ULL)

static void warn_rlimit_fsize_if_capped_for_index_io(void) {
    struct rlimit lim;
    uint64_t cap;
    double gib, mib;

    if (getrlimit(RLIMIT_FSIZE, &lim) != 0) return;
    if (lim.rlim_cur == RLIM_INFINITY) return;
    if (lim.rlim_cur == 0) return;
    cap = (uint64_t)lim.rlim_cur;
    if (cap >= EREPORT_INDEX_WARN_FSIZE_BELOW_BYTES) return;

    gib = (double)cap / (double)(1024ULL * 1024ULL * 1024ULL);
    mib = (double)cap / (double)(1024ULL * 1024ULL);
    fprintf(stderr,
            "ereport_index: warning: soft file-size cap (RLIMIT_FSIZE / `ulimit -f`) is %" PRIu64 " bytes",
            cap);
    if (gib >= 1.0)
        fprintf(stderr, " (~%.1f GiB)", gib);
    else
        fprintf(stderr, " (~%.0f MiB)", mib);
    fprintf(stderr,
            ", not unlimited; large --make can abort with SIGXFSZ. Use `ulimit -f unlimited`. "
            "In bash, `ulimit -f N` is kilobytes (N×1024 bytes), unlike `ulimit -n`; e.g. 200000 ≈ 195 MiB. "
            "See README.\n");
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

/* Bytes of source records per tmp_trigrams frame, rounded down to whole records (>= 1). */
static uint32_t parse_trigram_frame_records(void) {
    const char *e = getenv("EREPORT_INDEX_TRIGRAM_FRAME_BYTES");
    unsigned long v;
    uint32_t recs;
    char *end;

    if (!e || !*e) return (uint32_t)TRIGRAM_TMP_FRAME_RECORDS;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < TRIGRAM_TMP_FRAME_BYTES_MIN || v > TRIGRAM_TMP_FRAME_BYTES_MAX)
        return (uint32_t)TRIGRAM_TMP_FRAME_RECORDS;
    recs = (uint32_t)(v / sizeof(trigram_record_t));
    return recs ? recs : 1U;
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

typedef struct {
    char **paths;
    uint64_t *chunk_targets;
    size_t path_count;
    int *prep_rc;
    file_chunk_t **prep_chunks;
    size_t *prep_chunk_counts;
    file_state_t *file_states;
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

        /* A valid crawl-time trigram journal replaces the capture parse for this shard with
         * replay work items. The v2 block table lets one shard's journal split into parallel
         * block-range chunks (start_offset/end_offset carry block indices then); a table-less
         * or single-block journal stays one whole-shard [0, UINT64_MAX) replay chunk.
         * Missing/stale journals fall back to today's path. */
        if (g_journal_dir) {
            trij_reader_t jr;

            if (trij_reader_open_validate(&jr, g_journal_dir, pool->paths[i], NULL) == 1) {
                size_t cap = 0;
                int split = trij_reader_load_block_table(&jr) == 0 && jr.block_count > 1 &&
                            jr.hdr.record_count > 0;

                r = 0;
                if (split) {
                    /* ~4 work units per parse worker, whole blocks, balanced by entries. */
                    uint64_t want = (uint64_t)parse_index_thread_count() * 4ULL;
                    uint64_t per = (jr.hdr.record_count + want - 1ULL) / want;
                    uint64_t b = 0;

                    while (b < jr.block_count && r == 0) {
                        uint64_t e = b;
                        uint64_t acc = 0;

                        do {
                            acc += jr.blocks[e].n_entries;
                            e++;
                        } while (e < jr.block_count && acc < per);
                        r = crawl_bin_append_chunk(&local_chunks, &local_count, &cap,
                                                   pool->paths[i], b, e, i);
                        b = e;
                    }
                } else {
                    r = crawl_bin_append_chunk(&local_chunks, &local_count, &cap, pool->paths[i], 0,
                                               UINT64_MAX, i);
                }
                trij_reader_close(&jr);
                if (r == 0) pool->file_states[i].use_journal = 1;
            } else {
                r = crawl_bin_build_chunks_for_file(&index_chunk_stdio, mk_io_tls_flush, pool->paths[i], i,
                                                    pool->chunk_targets[i], parse_index_thread_count(),
                                                    &local_chunks, &local_count, &fc);
            }
        } else {
            r = crawl_bin_build_chunks_for_file(&index_chunk_stdio, mk_io_tls_flush, pool->paths[i], i,
                                                pool->chunk_targets[i], parse_index_thread_count(),
                                                &local_chunks, &local_count, &fc);
        }
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

/* LSD radix byte: little-endian, path_id bytes first (least significant key), then trigram bytes — so the
 * final order is trigram-major, path_id-minor (what write_sorted_bucket_records needs). */
static unsigned char trigram_record_radix_byte(const trigram_record_t *r, int pass, int pid_bytes) {
    if (pass < pid_bytes) return ((const unsigned char *)&r->path_id)[pass];
    return ((const unsigned char *)&r->trigram)[pass - pid_bytes];
}

static void radix_sort_trigram_records(trigram_record_t *records, size_t n, trigram_record_t *aux) {
    trigram_record_t *in;
    trigram_record_t *out;
    uint64_t max_pid = 0;
    uint32_t max_tri = 0;
    int pid_bytes = 1;
    int tri_bytes = 1;
    int total_passes;
    int pass;
    size_t i;

    if (n < 2) return;

    /* Only radix over the bytes actually present: path_id <= total paths and the trigram is 24-bit, so the
     * high bytes are uniformly zero. Skipping them drops the 12 fixed passes to ~7 on real corpora. The one
     * O(n) max scan is far cheaper than the ~5 passes it removes. Output is identical to the full-width sort. */
    for (i = 0; i < n; i++) {
        if (records[i].path_id > max_pid) max_pid = records[i].path_id;
        if (records[i].trigram > max_tri) max_tri = records[i].trigram;
    }
    while (pid_bytes < 8 && (max_pid >> (pid_bytes * 8)) != 0) pid_bytes++;
    while (tri_bytes < 4 && (max_tri >> (tri_bytes * 8)) != 0) tri_bytes++;
    total_passes = pid_bytes + tri_bytes;

    in = records;
    out = aux;
    for (pass = 0; pass < total_passes; pass++) {
        size_t c[256];
        size_t pos[256];
        memset(c, 0, sizeof(c));
        for (i = 0; i < n; i++) c[trigram_record_radix_byte(&in[i], pass, pid_bytes)]++;
        pos[0] = 0;
        for (i = 1; i < 256; i++) pos[i] = pos[i - 1] + c[i - 1];
        for (i = 0; i < n; i++) {
            unsigned k = trigram_record_radix_byte(&in[i], pass, pid_bytes);
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

/*
 * Parallel sort of one bucket's records. Strategy: one MSD counting-sort pass on the most-significant
 * byte that actually varies across the bucket (trigram bytes dominate path_id bytes in the sort order),
 * which splits the records into up to 256 contiguous, key-ordered partitions; then each partition is
 * radix-sorted independently by a pool of threads (work-stolen by partition). count, scatter and the
 * final copyback are also parallelized over disjoint record ranges, so essentially all of the sort runs
 * in parallel. Concatenating the sorted partitions in partition order reproduces the exact global
 * (trigram, path_id) order — output is byte-identical to radix_sort_trigram_records().
 */
typedef struct {
    const trigram_record_t *src; /* count/scatter: input; copyback: source (aux) */
    trigram_record_t *dst;       /* scatter: aux; copyback: records */
    size_t begin;
    size_t end;
    int mp;        /* MSD pass index */
    int pid_bytes;
    size_t hist[256];  /* count output */
    size_t woff[256];  /* scatter write-offset base (per partition) */
} psort_range_arg_t;

typedef struct {
    trigram_record_t *part_base;    /* aux (partitioned data) */
    trigram_record_t *scratch_base; /* records (per-partition radix scratch) */
    const size_t *part_start;
    const size_t *part_count;
    atomic_uint *cursor;
} psort_part_arg_t;

static void *psort_count_worker(void *v) {
    psort_range_arg_t *a = (psort_range_arg_t *)v;
    size_t i;

    memset(a->hist, 0, sizeof(a->hist));
    for (i = a->begin; i < a->end; i++)
        a->hist[trigram_record_radix_byte(&a->src[i], a->mp, a->pid_bytes)]++;
    return NULL;
}

static void *psort_scatter_worker(void *v) {
    psort_range_arg_t *a = (psort_range_arg_t *)v;
    size_t off[256];
    size_t i;

    memcpy(off, a->woff, sizeof(off));
    for (i = a->begin; i < a->end; i++) {
        unsigned b = trigram_record_radix_byte(&a->src[i], a->mp, a->pid_bytes);
        a->dst[off[b]++] = a->src[i];
    }
    return NULL;
}

static void *psort_copyback_worker(void *v) {
    psort_range_arg_t *a = (psort_range_arg_t *)v;

    if (a->end > a->begin)
        memcpy(a->dst + a->begin, a->src + a->begin, (a->end - a->begin) * sizeof(trigram_record_t));
    return NULL;
}

static void *psort_partition_worker(void *v) {
    psort_part_arg_t *a = (psort_part_arg_t *)v;

    for (;;) {
        unsigned p = atomic_fetch_add_explicit(a->cursor, 1U, memory_order_relaxed);

        if (p >= (unsigned)MERGE_SORT_PARTITIONS) break;
        if (a->part_count[p] >= 2)
            radix_sort_trigram_records(a->part_base + a->part_start[p], a->part_count[p],
                                       a->scratch_base + a->part_start[p]);
    }
    return NULL;
}

/* Spawn workers over T equal ranges of [0,n) running fn(&args[t]); the caller's thread runs range 0. */
static void psort_run_ranges(void *(*fn)(void *), psort_range_arg_t *args, int T, pthread_t *tids) {
    int t, spawned = 0;

    for (t = 1; t < T; t++) {
        if (pthread_create(&tids[t], NULL, fn, &args[t]) != 0) break;
        spawned = t;
    }
    fn(&args[0]);
    for (t = 1; t <= spawned; t++) pthread_join(tids[t], NULL);
    /* If a spawn failed mid-way, run the remaining ranges inline (correctness over parallelism). */
    for (t = spawned + 1; t < T; t++) fn(&args[t]);
}

static void parallel_radix_sort_trigram_records(trigram_record_t *records, size_t n, trigram_record_t *aux,
                                                int nthreads) {
    uint64_t pid_or = 0, pid_and = ~0ULL;
    uint32_t tri_or = 0, tri_and = ~0U;
    uint64_t pid_diff;
    uint32_t tri_diff;
    int pid_bytes = 1, tri_bytes = 1, total_passes, p, mp = -1;
    int T, t;
    size_t i;
    psort_range_arg_t *args = NULL;
    pthread_t *tids = NULL;
    size_t cnt[256];
    size_t start[256];
    size_t running[256];
    psort_part_arg_t parg;
    atomic_uint cursor;

    if (nthreads <= 1 || n < MERGE_PARALLEL_SORT_MIN_RECORDS) {
        radix_sort_trigram_records(records, n, aux);
        return;
    }

    for (i = 0; i < n; i++) {
        pid_or |= records[i].path_id;
        pid_and &= records[i].path_id;
        tri_or |= records[i].trigram;
        tri_and &= records[i].trigram;
    }
    while (pid_bytes < 8 && (pid_or >> (pid_bytes * 8)) != 0) pid_bytes++;
    while (tri_bytes < 4 && (tri_or >> (tri_bytes * 8)) != 0) tri_bytes++;
    total_passes = pid_bytes + tri_bytes;
    pid_diff = pid_or ^ pid_and;
    tri_diff = tri_or ^ tri_and;

    /* Most-significant pass whose byte actually varies (trigram bytes are the high passes). */
    for (p = total_passes - 1; p >= 0; p--) {
        unsigned char d;

        if (p >= pid_bytes)
            d = (unsigned char)((tri_diff >> ((p - pid_bytes) * 8)) & 0xFFU);
        else
            d = (unsigned char)((pid_diff >> (p * 8)) & 0xFFULL);
        if (d) { mp = p; break; }
    }
    if (mp < 0) return; /* all records identical: any order is correct */

    T = nthreads;
    if ((size_t)T > n) T = (int)n;
    if (T < 1) T = 1;

    args = (psort_range_arg_t *)malloc((size_t)T * sizeof(*args));
    tids = (pthread_t *)malloc((size_t)T * sizeof(*tids));
    if (!args || !tids) {
        free(args);
        free(tids);
        radix_sort_trigram_records(records, n, aux);
        return;
    }

    for (t = 0; t < T; t++) {
        args[t].src = records;
        args[t].dst = aux;
        args[t].begin = (size_t)((double)n * t / T);
        args[t].end = (size_t)((double)n * (t + 1) / T);
        args[t].mp = mp;
        args[t].pid_bytes = pid_bytes;
    }
    args[T - 1].end = n;

    /* 1. parallel count of the MSD byte */
    psort_run_ranges(psort_count_worker, args, T, tids);

    /* 2. global partition offsets + per-thread scatter write offsets (serial, 256*T) */
    memset(cnt, 0, sizeof(cnt));
    for (t = 0; t < T; t++) {
        int v;
        for (v = 0; v < 256; v++) cnt[v] += args[t].hist[v];
    }
    start[0] = 0;
    for (i = 1; i < 256; i++) start[i] = start[i - 1] + cnt[i - 1];
    {
        int v;
        for (v = 0; v < 256; v++) running[v] = start[v];
        for (t = 0; t < T; t++) {
            for (v = 0; v < 256; v++) {
                args[t].woff[v] = running[v];
                running[v] += args[t].hist[v];
            }
        }
    }

    /* 3. parallel scatter records -> aux (each thread writes disjoint slots) */
    psort_run_ranges(psort_scatter_worker, args, T, tids);

    /* 4. parallel per-partition radix sort (aux in place, records as scratch) */
    atomic_init(&cursor, 0);
    parg.part_base = aux;
    parg.scratch_base = records;
    parg.part_start = start;
    parg.part_count = cnt;
    parg.cursor = &cursor;
    {
        int spawned = 0;
        for (t = 1; t < T; t++) {
            if (pthread_create(&tids[t], NULL, psort_partition_worker, &parg) != 0) break;
            spawned = t;
        }
        psort_partition_worker(&parg);
        for (t = 1; t <= spawned; t++) pthread_join(tids[t], NULL);
    }

    /* 5. parallel copyback aux -> records */
    for (t = 0; t < T; t++) {
        args[t].src = aux;
        args[t].dst = records;
    }
    psort_run_ranges(psort_copyback_worker, args, T, tids);

    free(args);
    free(tids);
}

/* Postings flush buffer. The per-delta path used to call fwrite ~once per posting (~100B calls on a 1B-path
 * corpus); buffering the varints and flushing in MiB chunks removes that per-call overhead. Offsets are
 * tracked with a local counter (postings_pos) instead of ftello() since bytes may be unflushed. */
#define MERGE_POSTINGS_FLUSH_BYTES ((size_t)1 << 20)

/*
 * tri_postings.bin compression. A posting list is delta-varints, which zstd still shrinks by a good margin
 * because the byte histogram is heavily skewed towards small deltas. Two things stop that from being a
 * plain "one frame per list":
 *
 *  - The hottest trigrams on a billion-path corpus have posting lists in the hundreds of MiB, and a merge
 *    worker cannot hold a whole list in RAM just to compress it. So a large list is written as a sequence
 *    of independently-decodable chunks of at most POSTINGS_CHUNK_RAW raw varint bytes.
 *  - Most trigrams have a handful of postings, where a chunk header plus a zstd frame costs more than the
 *    payload. So a list that fits in POSTINGS_GROUP_RAW_MAX stays bare varints with no header at all.
 *
 * Which of the two a list uses is recorded in the (previously unused) trigram_key_t.reserved field.
 * postings_offset/postings_bytes still delimit one contiguous span, so merge_stitch_segments can keep
 * concatenating segment files and rebasing offsets without knowing any of this.
 */
#define POSTINGS_CHUNK_RAW ((size_t)128 * 1024)
#define POSTINGS_GROUP_RAW_MAX ((size_t)512)
#define POSTINGS_CHUNK_HDR 9 /* u32 raw_len, u32 stored_len, u8 stored_is_zstd */

#define POSTINGS_ENC_RAW 0U    /* span is bare delta-varints */
#define POSTINGS_ENC_CHUNKED 1U /* span is a sequence of POSTINGS_CHUNK_HDR-prefixed chunks */

/*
 * Streaming postings encoder. Fed a globally (trigram, path_id)-ascending stream one record at a time, it
 * groups by trigram, drops consecutive duplicate path_ids, delta-varint-encodes the postings, and writes a
 * trigram_key_t per group. Driving the encoding through one emit() lets both the flat-array writer and the
 * k-way slice merge produce byte-identical tri_keys/tri_postings output.
 */
typedef struct {
    FILE *keys_fp;
    FILE *postings_fp;
    unsigned char *pbuf; /* output coalescing buffer, flushed to postings_fp in MiB units */
    size_t plen;
    unsigned char *rawbuf; /* varints of the current chunk, not yet compressed */
    size_t rawlen;
    unsigned char *compbuf;
    size_t compcap;
    ZSTD_CCtx *cctx;
    uint64_t postings_pos; /* byte offset in postings_fp, including bytes still sitting in pbuf */
    uint64_t unique;
    uint32_t cur_trigram;
    uint64_t group_offset;
    uint64_t prev;      /* last path_id encoded in the current group (0 => none yet) */
    uint64_t last_path; /* dedup of consecutive equal path_ids within a group */
    int have_group;
    int group_chunked; /* set once the current group has spilled at least one chunk */
} postings_enc_t;

static void pe_destroy(postings_enc_t *pe) {
    free(pe->pbuf);
    free(pe->rawbuf);
    free(pe->compbuf);
    if (pe->cctx) ZSTD_freeCCtx(pe->cctx);
    memset(pe, 0, sizeof(*pe));
}

static int pe_init(postings_enc_t *pe, FILE *keys_fp, FILE *postings_fp) {
    memset(pe, 0, sizeof(*pe));
    pe->keys_fp = keys_fp;
    pe->postings_fp = postings_fp;
    pe->postings_pos = (uint64_t)ftello(postings_fp);
    pe->last_path = UINT64_MAX;
    pe->compcap = ZSTD_compressBound(POSTINGS_CHUNK_RAW);
    pe->pbuf = (unsigned char *)malloc(MERGE_POSTINGS_FLUSH_BYTES);
    /* Room for one more max-length varint past the chunk target, so pe_emit can append then check. */
    pe->rawbuf = (unsigned char *)malloc(POSTINGS_CHUNK_RAW + 16);
    pe->compbuf = (unsigned char *)malloc(pe->compcap);
    pe->cctx = ZSTD_createCCtx();
    if (!pe->pbuf || !pe->rawbuf || !pe->compbuf || !pe->cctx) {
        pe_destroy(pe);
        return -1;
    }
    return 0;
}

/* Append to the coalescing buffer and advance the logical file position. */
static int pe_out(postings_enc_t *pe, const void *src, size_t n) {
    if (n == 0) return 0;
    if (pe->plen + n > MERGE_POSTINGS_FLUSH_BYTES) {
        if (pe->plen > 0 && mk_fwrite(pe->pbuf, 1, pe->plen, pe->postings_fp) != pe->plen) return -1;
        pe->plen = 0;
    }
    if (n > MERGE_POSTINGS_FLUSH_BYTES) {
        if (mk_fwrite(src, 1, n, pe->postings_fp) != n) return -1;
    } else {
        memcpy(pe->pbuf + pe->plen, src, n);
        pe->plen += n;
    }
    pe->postings_pos += (uint64_t)n;
    return 0;
}

/* Emit rawbuf as one independently-decodable chunk and switch the group to the chunked layout. */
static int pe_flush_chunk(postings_enc_t *pe) {
    unsigned char hdr[POSTINGS_CHUNK_HDR];
    const unsigned char *payload;
    uint32_t raw_len, stored_len;
    size_t clen;

    if (pe->rawlen == 0) return 0;
    raw_len = (uint32_t)pe->rawlen;
    clen = ZSTD_compressCCtx(pe->cctx, pe->compbuf, pe->compcap, pe->rawbuf, pe->rawlen, durable_zstd_level());
    if (!ZSTD_isError(clen) && clen < pe->rawlen) {
        payload = pe->compbuf;
        stored_len = (uint32_t)clen;
        hdr[8] = 1;
    } else {
        payload = pe->rawbuf;
        stored_len = raw_len;
        hdr[8] = 0;
    }
    memcpy(hdr, &raw_len, sizeof(raw_len));
    memcpy(hdr + 4, &stored_len, sizeof(stored_len));
    if (pe_out(pe, hdr, sizeof(hdr)) != 0) return -1;
    if (pe_out(pe, payload, stored_len) != 0) return -1;
    pe->rawlen = 0;
    pe->group_chunked = 1;
    return 0;
}

/* Write the group's payload and its key (postings_bytes is only known once the payload is out). */
static int pe_close_group(postings_enc_t *pe) {
    trigram_key_t key;

    if (!pe->have_group) return 0;
    if (!pe->group_chunked && pe->rawlen <= POSTINGS_GROUP_RAW_MAX) {
        if (pe_out(pe, pe->rawbuf, pe->rawlen) != 0) return -1;
        pe->rawlen = 0;
    } else if (pe_flush_chunk(pe) != 0) {
        return -1;
    }
    key.trigram = pe->cur_trigram;
    key.reserved = pe->group_chunked ? POSTINGS_ENC_CHUNKED : POSTINGS_ENC_RAW;
    key.postings_offset = pe->group_offset;
    key.postings_bytes = pe->postings_pos - pe->group_offset;
    if (mk_fwrite(&key, sizeof(key), 1, pe->keys_fp) != 1) return -1;
    pe->unique++;
    pe->have_group = 0;
    return 0;
}

static int pe_emit(postings_enc_t *pe, uint32_t trigram, uint64_t path_id) {
    uint64_t v;

    if (!pe->have_group || trigram != pe->cur_trigram) {
        if (pe_close_group(pe) != 0) return -1;
        pe->cur_trigram = trigram;
        pe->group_offset = pe->postings_pos;
        pe->prev = 0;
        pe->last_path = UINT64_MAX;
        pe->have_group = 1;
        pe->group_chunked = 0;
        pe->rawlen = 0;
    }
    if (path_id == pe->last_path) return 0;
    v = (pe->prev == 0) ? (path_id + 1U) : (path_id - pe->prev);
    while (v >= 0x80U) {
        pe->rawbuf[pe->rawlen++] = (unsigned char)((v & 0x7FU) | 0x80U);
        v >>= 7;
    }
    pe->rawbuf[pe->rawlen++] = (unsigned char)v;
    /* rawbuf has 16 bytes of slack past the target, so the overflow check can follow the append. */
    if (pe->rawlen >= POSTINGS_CHUNK_RAW && pe_flush_chunk(pe) != 0) return -1;
    pe->prev = path_id;
    pe->last_path = path_id;
    return 0;
}

static int pe_finish(postings_enc_t *pe, uint64_t *unique_out) {
    if (pe_close_group(pe) != 0) return -1;
    if (pe->plen > 0 && mk_fwrite(pe->pbuf, 1, pe->plen, pe->postings_fp) != pe->plen) return -1;
    pe->plen = 0;
    *unique_out = pe->unique;
    return 0;
}

static int write_sorted_bucket_records(FILE *keys_fp, FILE *postings_fp, trigram_record_t *records, size_t record_count,
                                       uint64_t *unique_out) {
    postings_enc_t pe;
    size_t i;
    int rc = -1;

    *unique_out = 0;
    if (pe_init(&pe, keys_fp, postings_fp) != 0) return -1;
    for (i = 0; i < record_count; i++) {
        if (pe_emit(&pe, records[i].trigram, records[i].path_id) != 0) goto out;
    }
    if (pe_finish(&pe, unique_out) != 0) goto out;
    rc = 0;

out:
    pe_destroy(&pe);
    return rc;
}

/*
 * K-way merge writer for oversized buckets. `records` holds `nslices = ceil(n/slice_recs)` contiguous slices,
 * each already (trigram, path_id)-sorted in place. A binary min-heap streams the records back in global order
 * into the shared postings encoder, so the output is byte-identical to radix-sorting the whole bucket and
 * calling write_sorted_bucket_records — but without ever allocating an n-sized radix aux buffer.
 */
typedef struct {
    uint32_t trigram;
    uint64_t path_id;
    const trigram_record_t *next; /* next record after the loaded head */
    const trigram_record_t *end;
} kmerge_node_t;

static inline int kmerge_less(const kmerge_node_t *a, const kmerge_node_t *b) {
    if (a->trigram != b->trigram) return a->trigram < b->trigram;
    return a->path_id < b->path_id;
}

static void kmerge_sift_down(kmerge_node_t *h, size_t n, size_t i) {
    for (;;) {
        size_t l = 2 * i + 1;
        size_t r = 2 * i + 2;
        size_t m = i;
        kmerge_node_t tmp;

        if (l < n && kmerge_less(&h[l], &h[m])) m = l;
        if (r < n && kmerge_less(&h[r], &h[m])) m = r;
        if (m == i) break;
        tmp = h[i];
        h[i] = h[m];
        h[m] = tmp;
        i = m;
    }
}

static int merge_kway_write_bucket(FILE *keys_fp, FILE *postings_fp, const trigram_record_t *records, size_t n,
                                   size_t slice_recs, uint64_t *unique_out) {
    size_t nslices;
    kmerge_node_t *heap;
    size_t hn = 0;
    size_t s, i;
    postings_enc_t pe;
    int rc = -1;

    *unique_out = 0;
    if (n == 0) return 0;
    if (slice_recs == 0) slice_recs = 1;
    nslices = (n + slice_recs - 1) / slice_recs;

    heap = (kmerge_node_t *)malloc(nslices * sizeof(*heap));
    if (!heap) return -1;
    for (s = 0; s < nslices; s++) {
        size_t base = s * slice_recs;
        size_t cnt = (base + slice_recs <= n) ? slice_recs : (n - base);
        const trigram_record_t *b = records + base;

        heap[hn].trigram = b->trigram;
        heap[hn].path_id = b->path_id;
        heap[hn].next = b + 1;
        heap[hn].end = b + cnt;
        hn++;
    }
    if (hn > 1)
        for (i = hn / 2; i-- > 0;) kmerge_sift_down(heap, hn, i);

    if (pe_init(&pe, keys_fp, postings_fp) != 0) {
        free(heap);
        return -1;
    }
    while (hn > 0) {
        if (pe_emit(&pe, heap[0].trigram, heap[0].path_id) != 0) goto out;
        if (heap[0].next < heap[0].end) {
            heap[0].trigram = heap[0].next->trigram;
            heap[0].path_id = heap[0].next->path_id;
            heap[0].next++;
        } else {
            heap[0] = heap[hn - 1];
            hn--;
        }
        kmerge_sift_down(heap, hn, 0);
    }
    if (pe_finish(&pe, unique_out) != 0) goto out;
    rc = 0;

out:
    pe_destroy(&pe);
    free(heap);
    return rc;
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
 * Trigrams are basename-only at index time; directory hits expand to descendants at search time.
 * Verify still rejects scattered false positives (e.g. …/micro/…/iche/…/hel… without "michel"). */
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
    uint32_t k, best = UINT32_MAX;
    uint64_t best_age = 0;
    int found = 0;
    size_t base = (size_t)wid * (size_t)ctx->tw_worker_max_open;
    uint32_t cnt = ctx->tw_worker_open_count[wid];

    /* Scan only this worker's open buckets (O(open) ≤ max_open), not all TRIGRAM_BUCKET_COUNT slots. */
    for (k = 0; k < cnt; k++) {
        uint32_t b = ctx->tw_worker_open_list[base + k];
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
    ctx->tw_frame_records = parse_trigram_frame_records();

    gmax = compute_max_open_trigram_buckets();
    ctx->tw_worker_max_open = compute_tw_worker_max_open(shard_count, gmax);

    ctx->tw_worker_fp = (FILE **)calloc((size_t)shard_count * (size_t)TRIGRAM_BUCKET_COUNT, sizeof(FILE *));
    ctx->tw_worker_fp_magic = (uint8_t *)calloc((size_t)shard_count * (size_t)TRIGRAM_BUCKET_COUNT, sizeof(uint8_t));
    ctx->tw_worker_buf = (trigram_record_t **)calloc((size_t)shard_count * (size_t)TRIGRAM_BUCKET_COUNT, sizeof(trigram_record_t *));
    ctx->tw_worker_buf_n = (uint32_t *)calloc((size_t)shard_count * (size_t)TRIGRAM_BUCKET_COUNT, sizeof(uint32_t));
    ctx->tw_worker_lru_age = (uint64_t *)calloc((size_t)shard_count * (size_t)TRIGRAM_BUCKET_COUNT, sizeof(uint64_t));
    ctx->tw_worker_open_count = (uint32_t *)calloc((size_t)shard_count, sizeof(uint32_t));
    ctx->tw_worker_lru_next_tick = (uint64_t *)calloc((size_t)shard_count, sizeof(uint64_t));
    ctx->tw_worker_open_list = (uint32_t *)calloc((size_t)shard_count * (size_t)ctx->tw_worker_max_open, sizeof(uint32_t));
    ctx->tw_worker_list_pos = (uint32_t *)calloc((size_t)shard_count * (size_t)TRIGRAM_BUCKET_COUNT, sizeof(uint32_t));
    if (!ctx->tw_worker_fp || !ctx->tw_worker_fp_magic || !ctx->tw_worker_buf || !ctx->tw_worker_buf_n ||
        !ctx->tw_worker_lru_age || !ctx->tw_worker_open_count || !ctx->tw_worker_lru_next_tick ||
        !ctx->tw_worker_open_list || !ctx->tw_worker_list_pos) {
        free(ctx->tw_worker_fp);
        free(ctx->tw_worker_fp_magic);
        free(ctx->tw_worker_buf);
        free(ctx->tw_worker_buf_n);
        free(ctx->tw_worker_lru_age);
        free(ctx->tw_worker_open_count);
        free(ctx->tw_worker_lru_next_tick);
        free(ctx->tw_worker_open_list);
        free(ctx->tw_worker_list_pos);
        ctx->tw_worker_fp = NULL;
        ctx->tw_worker_fp_magic = NULL;
        ctx->tw_worker_buf = NULL;
        ctx->tw_worker_buf_n = NULL;
        ctx->tw_worker_lru_age = NULL;
        ctx->tw_worker_open_count = NULL;
        ctx->tw_worker_lru_next_tick = NULL;
        ctx->tw_worker_open_list = NULL;
        ctx->tw_worker_list_pos = NULL;
        ctx->trigram_tmp_shard_count = 0;
        return -1;
    }

    /* Optional: stage_path_offset writes straight through if this is NULL, so a failure here is not fatal. */
    ctx->path_offsets_buf =
        (uint64_t *)malloc((size_t)PATH_OFFSETS_STAGE_ENTRIES * sizeof(*ctx->path_offsets_buf));
    ctx->path_offsets_n = 0;

    if (g_verbose) {
        fprintf(stderr,
                "ereport_index: tmp_trigram shard writers=%u max_open_per_shard=%u (EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS=%u)\n",
                shard_count, ctx->tw_worker_max_open, gmax);
    }
    return 0;
}

/* Encode + write the records pending in tw_worker_buf[ix] as one frame; reset the buffer. */
static int tw_worker_flush_locked(build_ctx_t *ctx, size_t ix) {
    FILE *fp;

    if (ctx->tw_worker_buf_n[ix] == 0) return 0;
    fp = ctx->tw_worker_fp[ix];
    if (!fp) return -1;
    if (tmp_trigram_fp_write_batch(fp, &ctx->tw_worker_fp_magic[ix],
                                   ctx->tw_worker_buf[ix], ctx->tw_worker_buf_n[ix]) != 0)
        return -1;
    ctx->tw_worker_buf_n[ix] = 0;
    return 0;
}

/* Flush the pending frame, close the FILE*, and free the buffer for one open shard. */
static int tw_worker_close_ix(build_ctx_t *ctx, uint32_t worker_id, size_t ix) {
    int rc = 0;

    if (!ctx->tw_worker_fp[ix]) return 0;
    if (tw_worker_flush_locked(ctx, ix) != 0) rc = -1;
    if (mk_fclose(ctx->tw_worker_fp[ix]) != 0) rc = -1;
    ctx->tw_worker_fp[ix] = NULL;
    ctx->tw_worker_fp_magic[ix] = 0;
    free(ctx->tw_worker_buf[ix]);
    ctx->tw_worker_buf[ix] = NULL;
    ctx->tw_worker_buf_n[ix] = 0;
    if (ctx->tw_worker_open_count && ctx->tw_worker_open_count[worker_id] > 0) {
        /* Swap-remove this bucket from the worker's open list to keep it dense (and the victim scan O(open)). */
        if (ctx->tw_worker_open_list && ctx->tw_worker_list_pos) {
            size_t base = (size_t)worker_id * (size_t)ctx->tw_worker_max_open;
            uint32_t pos = ctx->tw_worker_list_pos[ix];
            uint32_t last = ctx->tw_worker_open_count[worker_id] - 1U;
            uint32_t moved = ctx->tw_worker_open_list[base + last];

            ctx->tw_worker_open_list[base + pos] = moved;
            ctx->tw_worker_list_pos[tw_worker_fp_ix(worker_id, moved)] = pos;
        }
        ctx->tw_worker_open_count[worker_id]--;
    }
    return rc;
}

static void parallel_bucket_io_shutdown(build_ctx_t *ctx) {
    uint32_t wid, b;

    /* Staged offsets are flushed by write_final_path_offset on the success path; on an error path the index
     * is incomplete anyway, so just release the buffer. */
    free(ctx->path_offsets_buf);
    ctx->path_offsets_buf = NULL;
    ctx->path_offsets_n = 0;

    if (!ctx->tw_worker_fp) return;
    for (wid = 0; wid < ctx->trigram_tmp_shard_count; wid++) {
        for (b = 0; b < TRIGRAM_BUCKET_COUNT; b++) {
            size_t ix = tw_worker_fp_ix(wid, b);

            if (ctx->tw_worker_fp[ix]) {
                if (tw_worker_close_ix(ctx, wid, ix) != 0)
                    fprintf(stderr, "ereport_index: flush/close tmp_trigrams bucket %u worker %u: %s\n",
                            b, wid, strerror(errno));
            }
        }
        if (ctx->tw_worker_open_count) ctx->tw_worker_open_count[wid] = 0;
    }
    free(ctx->tw_worker_fp);
    ctx->tw_worker_fp = NULL;
    free(ctx->tw_worker_fp_magic);
    ctx->tw_worker_fp_magic = NULL;
    free(ctx->tw_worker_buf);
    ctx->tw_worker_buf = NULL;
    free(ctx->tw_worker_buf_n);
    ctx->tw_worker_buf_n = NULL;
    free(ctx->tw_worker_lru_age);
    ctx->tw_worker_lru_age = NULL;
    free(ctx->tw_worker_open_count);
    ctx->tw_worker_open_count = NULL;
    free(ctx->tw_worker_lru_next_tick);
    ctx->tw_worker_lru_next_tick = NULL;
    free(ctx->tw_worker_open_list);
    ctx->tw_worker_open_list = NULL;
    free(ctx->tw_worker_list_pos);
    ctx->tw_worker_list_pos = NULL;
    /* trigram_tmp_shard_count kept for merge phase (merge_load uses worker shard paths). */
}

/*
 * Write n trigram codes for one bucket on one trigram worker's shard file (caller
 * groups by bucket). One path_id is stamped onto every record — the job already
 * shares a single path — so the worker never expands codes[] into a separate
 * trigram_record_t[] scratch first. Each worker_id maps to
 * tmp_trigrams_%04u_w%04u.bin — no mutex (exclusive FILE* per worker × bucket).
 */
static int append_trigram_codes_batch_parallel(build_ctx_t *ctx, uint32_t worker_id, uint32_t bucket,
                                               const uint32_t *codes, size_t n, uint64_t path_id) {
    FILE *fp;
    char path[PATH_MAX];
    size_t ix;

    if (n == 0) return 0;
    if (worker_id >= ctx->trigram_tmp_shard_count) return -1;

    ei_note_trigram_append_batch();
    ix = tw_worker_fp_ix(worker_id, bucket);
    fp = ctx->tw_worker_fp[ix];

    if (!fp) {
        /* Evict this worker's LRU shards (never the bucket we're about to use) to stay under the fd cap. */
        while (ctx->tw_worker_open_count[worker_id] >= ctx->tw_worker_max_open) {
            uint32_t victim = tw_worker_pick_lru_victim(ctx, worker_id, bucket);

            if (victim == UINT32_MAX) {
                fprintf(stderr, "ereport_index: internal: no LRU victim (worker %u open_count=%u max=%u)\n",
                        worker_id, ctx->tw_worker_open_count[worker_id], ctx->tw_worker_max_open);
                return -1;
            }
            if (tw_worker_close_ix(ctx, worker_id, tw_worker_fp_ix(worker_id, victim)) != 0) {
                fprintf(stderr, "ereport_index: flush/close tmp_trigrams bucket %u worker %u: %s\n",
                        victim, worker_id, strerror(errno));
                return -1;
            }
        }

        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, worker_id) >=
            (int)sizeof(path)) {
            fprintf(stderr, "ereport_index: tmp_trigrams path too long (bucket %u worker %u)\n", bucket, worker_id);
            return -1;
        }
        ctx->tw_worker_buf[ix] =
            (trigram_record_t *)malloc((size_t)ctx->tw_frame_records * sizeof(trigram_record_t));
        if (!ctx->tw_worker_buf[ix]) {
            fprintf(stderr, "ereport_index: alloc tmp_trigrams frame buffer (bucket %u worker %u)\n", bucket, worker_id);
            return -1;
        }
        ctx->tw_worker_buf_n[ix] = 0;
        /* Reopened-after-eviction shards already carry the EITG header; brand-new (empty) files do not. */
        ctx->tw_worker_fp_magic[ix] = (uint8_t)tmp_trigram_file_has_magic(path);
        fp = mk_fopen(path, "ab");
        if (!fp) {
            fprintf(stderr, "ereport_index: fopen %s: %s\n", path, strerror(errno));
            free(ctx->tw_worker_buf[ix]);
            ctx->tw_worker_buf[ix] = NULL;
            return -1;
        }
        if (setvbuf(fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
        }
        ctx->bucket_nonempty[bucket] = 1;
        ctx->tw_worker_fp[ix] = fp;
        {
            size_t base = (size_t)worker_id * (size_t)ctx->tw_worker_max_open;
            uint32_t pos = ctx->tw_worker_open_count[worker_id];
            ctx->tw_worker_open_list[base + pos] = bucket;
            ctx->tw_worker_list_pos[ix] = pos;
        }
        ctx->tw_worker_open_count[worker_id]++;
    }

    ctx->tw_worker_lru_age[ix] = ++ctx->tw_worker_lru_next_tick[worker_id];

    /* Stamp path_id once per record into the per-shard frame buffer; flush when full. */
    while (n > 0) {
        size_t space = (size_t)ctx->tw_frame_records - ctx->tw_worker_buf_n[ix];
        size_t take = n < space ? n : space;
        trigram_record_t *dst = ctx->tw_worker_buf[ix] + ctx->tw_worker_buf_n[ix];
        size_t k;

        for (k = 0; k < take; k++) {
            dst[k].trigram = codes[k];
            dst[k].path_id = path_id;
        }
        ctx->tw_worker_buf_n[ix] += (uint32_t)take;
        codes += take;
        n -= take;
        if (ctx->tw_worker_buf_n[ix] == ctx->tw_frame_records) {
            if (tw_worker_flush_locked(ctx, ix) != 0) {
                fprintf(stderr, "ereport_index: fwrite tmp_trigrams bucket %u worker %u: %s\n",
                        bucket, worker_id, strerror(errno));
                return -1;
            }
        }
    }
    return 0;
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

/* Write out whatever path offsets are staged. Called when the staging buffer fills and once at the end. */
static int flush_path_offsets(build_ctx_t *ctx) {
    size_t n = ctx->path_offsets_n;

    if (n == 0) return 0;
    ctx->path_offsets_n = 0;
    if (mk_fwrite(ctx->path_offsets_buf, sizeof(*ctx->path_offsets_buf), n, ctx->path_offsets_fp) != n) {
        fprintf(stderr, "ereport_index: fwrite path_offsets.bin: %s\n", strerror(errno));
        return -1;
    }
    return 0;
}

static int append_isdir_bit(build_ctx_t *ctx, int is_dir) {
    if (!ctx->path_isdir_fp) return -1;
    if (is_dir) ctx->isdir_pending_byte = (uint8_t)(ctx->isdir_pending_byte | (uint8_t)(1U << ctx->isdir_pending_bits));
    ctx->isdir_pending_bits++;
    if (ctx->isdir_pending_bits == 8U) {
        if (mk_fwrite(&ctx->isdir_pending_byte, 1, 1, ctx->path_isdir_fp) != 1) {
            fprintf(stderr, "ereport_index: fwrite path_isdir.bin: %s\n", strerror(errno));
            return -1;
        }
        ctx->isdir_pending_byte = 0;
        ctx->isdir_pending_bits = 0;
    }
    return 0;
}

static int flush_isdir_tail(build_ctx_t *ctx) {
    if (!ctx->path_isdir_fp) return -1;
    if (ctx->isdir_pending_bits == 0U) return 0;
    if (mk_fwrite(&ctx->isdir_pending_byte, 1, 1, ctx->path_isdir_fp) != 1) {
        fprintf(stderr, "ereport_index: fwrite path_isdir.bin: %s\n", strerror(errno));
        return -1;
    }
    ctx->isdir_pending_byte = 0;
    ctx->isdir_pending_bits = 0;
    return 0;
}

static int stage_path_offset(build_ctx_t *ctx, uint64_t offset) {
    if (!ctx->path_offsets_buf) /* allocation failed at startup: write straight through */
        return mk_fwrite(&offset, sizeof(offset), 1, ctx->path_offsets_fp) == 1 ? 0 : -1;

    ctx->path_offsets_buf[ctx->path_offsets_n++] = offset;
    if (ctx->path_offsets_n == PATH_OFFSETS_STAGE_ENTRIES) return flush_path_offsets(ctx);
    return 0;
}

static void paths_writer_free(build_ctx_t *ctx) {
    free(ctx->paths_chunk);
    ctx->paths_chunk = NULL;
    free(ctx->paths_comp);
    ctx->paths_comp = NULL;
    free(ctx->paths_table);
    ctx->paths_table = NULL;
    if (ctx->paths_cctx) {
        ZSTD_freeCCtx(ctx->paths_cctx);
        ctx->paths_cctx = NULL;
    }
}

/* Reserve the header; frames and the chunk table are appended after it as the run proceeds. */
static int paths_writer_open(build_ctx_t *ctx) {
    unsigned char hdr[PATHS_HDR_BYTES];

    ctx->paths_comp_cap = ZSTD_compressBound(PATHS_CHUNK_RAW);
    ctx->paths_chunk = (unsigned char *)malloc(PATHS_CHUNK_RAW);
    ctx->paths_comp = (unsigned char *)malloc(ctx->paths_comp_cap);
    ctx->paths_cctx = ZSTD_createCCtx();
    if (!ctx->paths_chunk || !ctx->paths_comp || !ctx->paths_cctx) {
        fprintf(stderr, "ereport_index: alloc paths.bin writer state\n");
        paths_writer_free(ctx);
        return -1;
    }

    memset(hdr, 0, sizeof(hdr));
    if (mk_fwrite(hdr, 1, sizeof(hdr), ctx->paths_fp) != sizeof(hdr)) {
        fprintf(stderr, "ereport_index: fwrite paths.bin header: %s\n", strerror(errno));
        paths_writer_free(ctx);
        return -1;
    }
    ctx->paths_file_pos = PATHS_HDR_BYTES;
    return 0;
}

static int paths_table_push(build_ctx_t *ctx, const paths_chunk_ent_t *ent) {
    if (ctx->paths_table_n == ctx->paths_table_cap) {
        size_t cap = ctx->paths_table_cap ? ctx->paths_table_cap * 2 : 1024;
        paths_chunk_ent_t *p = (paths_chunk_ent_t *)realloc(ctx->paths_table, cap * sizeof(*p));

        if (!p) return -1;
        ctx->paths_table = p;
        ctx->paths_table_cap = cap;
    }
    ctx->paths_table[ctx->paths_table_n++] = *ent;
    return 0;
}

static int paths_flush_chunk(build_ctx_t *ctx) {
    paths_chunk_ent_t ent;
    const unsigned char *payload;
    size_t clen;

    if (ctx->paths_chunk_len == 0) return 0;
    clen = ZSTD_compressCCtx(ctx->paths_cctx, ctx->paths_comp, ctx->paths_comp_cap, ctx->paths_chunk,
                             ctx->paths_chunk_len, durable_zstd_level());
    if (!ZSTD_isError(clen) && clen < ctx->paths_chunk_len) {
        payload = ctx->paths_comp;
    } else {
        payload = ctx->paths_chunk;
        clen = ctx->paths_chunk_len;
    }

    ent.logical_start = ctx->paths_chunk_logical;
    ent.file_off = ctx->paths_file_pos;
    ent.stored_len = (uint32_t)clen;
    ent.raw_len = (uint32_t)ctx->paths_chunk_len;
    if (paths_table_push(ctx, &ent) != 0) {
        fprintf(stderr, "ereport_index: alloc paths.bin chunk table\n");
        return -1;
    }
    if (mk_fwrite(payload, 1, clen, ctx->paths_fp) != clen) {
        fprintf(stderr, "ereport_index: fwrite paths.bin: %s\n", strerror(errno));
        return -1;
    }
    ctx->paths_file_pos += (uint64_t)clen;
    ctx->paths_chunk_len = 0;
    return 0;
}

/* Add bytes that live at `logical_off` in the uncompressed stream, cutting a chunk if they do not fit. */
static int paths_append_chunked(build_ctx_t *ctx, const void *bytes, size_t n, uint64_t logical_off) {
    if (n > PATHS_CHUNK_RAW) {
        fprintf(stderr, "ereport_index: path of %zu bytes exceeds the paths.bin chunk size\n", n);
        return -1;
    }
    if (ctx->paths_chunk_len > 0 && ctx->paths_chunk_len + n > PATHS_CHUNK_RAW && paths_flush_chunk(ctx) != 0) return -1;
    if (ctx->paths_chunk_len == 0) ctx->paths_chunk_logical = logical_off;
    memcpy(ctx->paths_chunk + ctx->paths_chunk_len, bytes, n);
    ctx->paths_chunk_len += n;
    return 0;
}

static int append_paths_only(build_ctx_t *ctx, const char *path) {
    size_t need = strlen(path) + 1;

    if (stage_path_offset(ctx, ctx->paths_pos) != 0) return -1;
    if (paths_append_chunked(ctx, path, need, ctx->paths_pos) != 0) return -1;
    ctx->paths_pos += (uint64_t)need;
    return 0;
}

/* Flush the tail chunk, append the chunk table, then patch the reserved header now that both are known. */
static int paths_writer_close(build_ctx_t *ctx) {
    unsigned char hdr[PATHS_HDR_BYTES];
    uint64_t table_offset;
    uint32_t v32;
    uint64_t v64;
    size_t table_bytes;

    if (paths_flush_chunk(ctx) != 0) return -1;

    table_offset = ctx->paths_file_pos;
    table_bytes = ctx->paths_table_n * sizeof(*ctx->paths_table);
    if (table_bytes > 0 && mk_fwrite(ctx->paths_table, 1, table_bytes, ctx->paths_fp) != table_bytes) {
        fprintf(stderr, "ereport_index: fwrite paths.bin chunk table: %s\n", strerror(errno));
        return -1;
    }
    ctx->paths_file_pos += (uint64_t)table_bytes;

    memset(hdr, 0, sizeof(hdr));
    memcpy(hdr, PATHS_MAGIC, sizeof(PATHS_MAGIC));
    v32 = PATHS_FORMAT_VERSION;
    memcpy(hdr + 8, &v32, sizeof(v32));
    v32 = (uint32_t)durable_zstd_level();
    memcpy(hdr + 12, &v32, sizeof(v32));
    v64 = (uint64_t)ctx->paths_table_n;
    memcpy(hdr + 16, &v64, sizeof(v64));
    memcpy(hdr + 24, &table_offset, sizeof(table_offset));
    memcpy(hdr + 32, &ctx->paths_pos, sizeof(ctx->paths_pos));

    if (fflush(ctx->paths_fp) != 0 || fseeko(ctx->paths_fp, 0, SEEK_SET) != 0 ||
        mk_fwrite(hdr, 1, sizeof(hdr), ctx->paths_fp) != sizeof(hdr) || fflush(ctx->paths_fp) != 0) {
        fprintf(stderr, "ereport_index: rewrite paths.bin header: %s\n", strerror(errno));
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

            /* The jobs are bump-allocated from the batch's own arena, next to the codes
             * they point at, so the whole batch costs one reference count instead of a
             * malloc/free pair per path. Until the push below, the batch holds the only
             * reference, so the error paths just destroy the batch. */
            for (size_t i = 0; i < batch->count; i++) {
                parsed_path_t *item = &batch->items[i];
                uint64_t path_id = base + (uint64_t)i;
                trigram_job_t *job;

                if (append_paths_only(pa->ctx, item->path) != 0 ||
                    append_isdir_bit(pa->ctx, item->is_dir) != 0) {
                    pa->ctx->indexed_paths = base + (uint64_t)i;
                    atomic_fetch_add_explicit(&rs->indexed_paths, (unsigned long long)i, memory_order_relaxed);
                    atomic_store(&rs->writer_failed, 1);
                    write_batch_destroy(batch);
                    mk_io_tls_flush();
                    return NULL;
                }

                job = (trigram_job_t *)wb_arena_alloc(batch->arena, sizeof(*job), sizeof(void *));
                if (!job) {
                    fprintf(stderr, "ereport_index: arena alloc(trigram_job): %s\n", strerror(errno));
                    pa->ctx->indexed_paths = base + (uint64_t)i + 1ULL;
                    atomic_fetch_add_explicit(&rs->indexed_paths, (unsigned long long)(i + 1U), memory_order_relaxed);
                    atomic_store(&rs->writer_failed, 1);
                    write_batch_destroy(batch);
                    mk_io_tls_flush();
                    return NULL;
                }
                job->path_id = path_id;
                job->codes = item->codes;
                job->code_count = item->code_count;
                job->approx_body_bytes = sizeof(trigram_job_t) + item->code_count * sizeof(uint32_t);
                job->arena = batch->arena;
                job->next = NULL;

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

            if (tj_n != 0) {
                /* One reference per job, taken before any worker can see them. */
                atomic_fetch_add_explicit(&batch->arena->refs, (unsigned int)tj_n, memory_order_relaxed);
                if (trigram_job_queue_push_chain_slices(pa->trigram_queue, tj_head) != 0) {
                    /* The push released every job it could not hand off. */
                    fprintf(stderr, "ereport_index: trigram job queue push failed (queue closed)\n");
                    atomic_store(&rs->writer_failed, 1);
                    write_batch_destroy(batch);
                    mk_io_tls_flush();
                    return NULL;
                }
            }
        }

        write_batch_destroy(batch);
    }

    mk_io_tls_flush();
    return NULL;
}

/*
 * Process and free one trigram job, appending its records to the per-(worker,
 * bucket) tmp files.
 *
 * The job's trigram codes arrive pre-sorted and de-duplicated (from
 * sort_codes_unique), and bucket = trigram >> (24 - TRIGRAM_BUCKET_BITS) is
 * monotonic in the trigram, so the codes are *already* grouped by bucket in
 * ascending order. Split them into contiguous per-bucket runs on codes[] and
 * stamp path_id while filling the shard frame buffer — no intermediate
 * trigram_record_t[] expansion. Profiling showed the previous per-job re-sort
 * was ~24% of index-phase CPU; the expand-then-memcpy pass was the next cost
 * inside trigram_worker_main on single_huge_dir.
 *
 * Note: correctness does not depend on the input being sorted — each emitted
 * run is a maximal same-bucket span, so every record lands in its own bucket
 * file regardless; pre-sorting just minimises the number of runs/appends.
 * Returns 0 on success, -1 on hard failure (writer_failed set before -1).
 */
static int trigram_worker_process_job(trigram_worker_arg_t *tw, index_run_stats_t *rs, trigram_job_t *job) {
    if (atomic_load(&rs->writer_failed)) {
        wb_arena_release(job->arena);
        return 0; /* drain quietly once the run is failing */
    }

    if (job->code_count > 0) {
        size_t run_i = 0;

        while (run_i < job->code_count) {
            uint32_t b = job->codes[run_i] >> (24 - TRIGRAM_BUCKET_BITS);
            size_t run_j = run_i + 1;

            while (run_j < job->code_count &&
                   (job->codes[run_j] >> (24 - TRIGRAM_BUCKET_BITS)) == b)
                run_j++;
            if (append_trigram_codes_batch_parallel(tw->ctx, tw->worker_index, b, job->codes + run_i,
                                                     run_j - run_i, job->path_id) != 0) {
                atomic_store(&rs->writer_failed, 1);
                wb_arena_release(job->arena);
                return -1;
            }
            run_i = run_j;
        }
    }

    atomic_fetch_add(&rs->trigram_records, (unsigned long long)job->code_count);
    wb_arena_release(job->arena);
    return 0;
}

static void *trigram_worker_main(void *arg_void) {
    trigram_worker_arg_t *tw = (trigram_worker_arg_t *)arg_void;
    index_run_stats_t *rs = tw->ctx->run_stats;

    for (;;) {
        trigram_job_t *chain = trigram_job_queue_pop_batch_wait(tw->trigram_queue);

        if (!chain) break;

        while (chain) {
            trigram_job_t *job = chain;

            chain = chain->next;
            job->next = NULL;
            if (trigram_worker_process_job(tw, rs, job) != 0) {
                trigram_job_chain_free(chain); /* drop the rest of this batch */
                mk_io_tls_flush();
                return NULL;
            }
        }
    }

    mk_io_tls_flush();
    return NULL;
}

static void finalize_chunk_file_progress(index_run_stats_t *rs, file_state_t *file_states, size_t file_index) {
    ei_finalize_chunk_file(rs, file_states, file_index);
}

static void index_free_file_states(file_state_t *fs, size_t n) {
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

/* Free only the shard catalogs (keep the file_state_t array). Catalogs back path reconstruction in
 * the index phase (process_chunk_make); nothing in the merge/stitch phase touches them, so freeing
 * them once the index workers join returns their footprint (per-dir arrays + name strdups, ~18 GiB on
 * a large crawl) before the memory-hungry merge runs. Idempotent: catalogs are NULLed, so the later
 * index_free_file_states is a no-op for them and still frees the array exactly once. */
static void index_free_shard_catalogs(file_state_t *fs, size_t n) {
    size_t i;

    if (!fs) return;
    for (i = 0; i < n; i++) {
        if (fs[i].catalog) {
            crawl_bin_catalog_free(fs[i].catalog);
            free(fs[i].catalog);
            fs[i].catalog = NULL;
        }
    }
}

/* Approximate resident bytes held by all loaded shard catalogs (diagnostic only; mirrors ereport). */
static size_t index_catalog_bytes(const file_state_t *fs, size_t n) {
    size_t i;
    size_t total = 0;
    /* parent_dir_id+depth+name_len+name_comp ptr; the optional rollup arrays are
     * not requested at load, so they cost nothing. */
    const size_t per_slot = 8 + 4 + 2 + 8;

    if (!fs) return 0;
    for (i = 0; i < n; i++) {
        const crawl_bin_catalog_t *c = fs[i].catalog;
        uint64_t slots, d;
        if (!c) continue;
        slots = c->cap + 1;
        total += (size_t)slots * per_slot;
        if (c->name_len) {
            for (d = 1; d <= c->max_dir_id; d++)
                if (c->name_len[d]) total += (size_t)c->name_len[d] + 1;
        }
    }
    return total;
}

static int index_attach_shard_catalog(file_state_t *fs, const char *path) {
    FILE *fp;
    bin_file_header_t fh;
    struct stat st;
    crawl_bin_catalog_t *cat;

    if (!fs || fs->catalog) return -1;
    cat = (crawl_bin_catalog_t *)malloc(sizeof(*cat));
    if (!cat) return -1;
    crawl_bin_catalog_init_empty(cat);

    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) goto fail;
    fp = ei_shard_fopen(path, "rb");
    if (!fp) goto fail;
    if (mk_fread(&fh, sizeof(fh), 1, fp) != 1) {
        ei_shard_fclose(fp);
        goto fail;
    }
    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
        ei_shard_fclose(fp);
        errno = EINVAL;
        goto fail;
    }
    if (fh.catalog_offset == 0ULL || fh.catalog_offset > (uint64_t)st.st_size) {
        ei_shard_fclose(fp);
        errno = EINVAL;
        goto fail;
    }
    /* The trigram index is built from paths alone; no rollup is consulted. */
    if (crawl_bin_catalog_load_sel(fp, fh.catalog_offset, (uint64_t)st.st_size, 0U, cat) != 0) {
        ei_shard_fclose(fp);
        goto fail;
    }
    ei_shard_fclose(fp);
    fs->catalog = cat;
    return 0;

fail:
    crawl_bin_catalog_free(cat);
    free(cat);
    return -1;
}

/*
 * Last-parent cache for path reconstruction. crawl_bin_catalog_entry_path walks the parent chain to
 * the root for every record, and records under one directory are contiguous in crawl-bin output, so
 * a single slot absorbs nearly all of it: on a 12M-path index the walk was the whole cost of turning
 * a record into a path. Lives on the stack of one process_chunk_make call, which keeps it private to
 * the worker and scoped to a chunk -- a chunk never spans shards, so dir_ids cannot collide.
 */
typedef struct {
    uint64_t id; /* cached parent_dir_id (>1); 0 = empty */
    size_t len;
    char dir[PATH_MAX];
} mk_dir_cache_t;

static int mk_entry_path_cached(const crawl_bin_catalog_t *cat, uint64_t parent_dir_id, const char *name,
                                size_t name_len, char *out, size_t out_sz, mk_dir_cache_t *cache) {
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
    if (plen > 0) out[plen++] = '/';
    if (name_len > 0 && name) memcpy(out + plen, name, name_len);
    out[plen + name_len] = '\0';
    return 0;
}

/* Replay one shard's crawl-time trigram journal: entries already carry the full path, uid, type,
 * and sorted-unique basename codes, so there is no catalog, no block reader, and no extraction.
 * Filters re-apply in the capture path's order: uid, --path-rewrite, --subtree. */
static int process_chunk_make_journal(worker_arg_t *worker, const file_chunk_t *chunk) {
    build_ctx_t *ctx = worker->ctx;
    index_run_stats_t *rs = ctx->run_stats;
    write_queue_t *write_queue = worker->write_queue;
    write_batch_t *batch = NULL;
    uint64_t scanned_local = 0;
    uint64_t scanned_published = 0;
    trij_reader_t jr;
    int rc = -1;

    memset(&jr, 0, sizeof(jr));
    jr.fd = -1;
    if (trij_reader_open_validate(&jr, g_journal_dir, chunk->path, NULL) != 1) {
        /* Validated during chunk prep; turning stale mid-run means the shard is being rewritten
         * under us — count it bad rather than silently indexing nothing. */
        fprintf(stderr, "warn: journal for %s turned stale mid-run\n", chunk->path);
        atomic_fetch_add(&rs->bad_input_files, 1U);
        return -1;
    }

    /* Chunk prep stashed a block range in the chunk offsets for journaled shards;
     * [0, UINT64_MAX) means the whole journal (no table or a single block). */
    if (chunk->start_offset != 0 || chunk->end_offset != UINT64_MAX) {
        if (trij_reader_load_block_table(&jr) != 0 ||
            trij_reader_set_block_range(&jr, chunk->start_offset,
                                        chunk->end_offset - chunk->start_offset) != 0) {
            fprintf(stderr, "warn: journal block table for %s unreadable mid-run\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            trij_reader_close(&jr);
            return -1;
        }
    }

    for (;;) {
        const char *path;
        size_t path_len;
        uint64_t uid;
        uint8_t type;
        const uint32_t *codes;
        size_t code_count;
        int got = trij_reader_next(&jr, &path, &path_len, &uid, &type, &codes, &code_count);

        if (got == 0) {
            rc = 0;
            break;
        }
        if (got < 0) {
            fprintf(stderr, "warn: corrupt journal for %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
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

        if (!ctx->aggregate_all_users && (uid_t)uid != ctx->target_uid) continue;

        if (path_len == 0 || path_len >= PATH_MAX) {
            fprintf(stderr, "warn: journal path out of range in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            break;
        }
        /* Rewrite/subtree need a NUL-terminated path; write_batch_append copies what survives. */
        if (trigram_ensure_buf(&worker->path_buf, &worker->path_cap, PATH_MAX) != 0) {
            fprintf(stderr, "warn: path alloc failed in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            break;
        }
        memcpy(worker->path_buf, path, path_len);
        worker->path_buf[path_len] = '\0';

        if (g_rewrite_from) {
            (void)rewrite_path_prefix(worker->path_buf, PATH_MAX);
        }
        if (g_subtree_prefix && !subtree_path_under_prefix(worker->path_buf)) continue;

        if (!batch) {
            batch = write_batch_create();
            if (!batch) {
                fprintf(stderr, "warn: failed to allocate write batch for %s\n", chunk->path);
                atomic_fetch_add(&rs->bad_input_files, 1U);
                break;
            }
        }
        if (write_batch_append(batch, worker->path_buf, strlen(worker->path_buf), codes, code_count,
                               type == (uint8_t)'d') != 0) {
            fprintf(stderr, "warn: failed to append write batch for %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            break;
        }

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

    trij_reader_close(&jr);
    if (scanned_local > scanned_published)
        atomic_fetch_add_explicit(&rs->scanned_records, scanned_local - scanned_published, memory_order_relaxed);
    if (batch) {
        if (write_queue_push(write_queue, batch) != 0) {
            atomic_fetch_add(&rs->bad_input_files, 1U);
            write_batch_destroy(batch);
        }
    }
    finalize_chunk_file_progress(rs, worker->file_states, chunk->file_index);
    return rc;
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
    mk_dir_cache_t dir_cache;
    crawl_bin_block_reader_t br;
    memset(&br, 0, sizeof(br));
    dir_cache.id = 0;
    dir_cache.len = 0;

    if (file_states[chunk->file_index].use_journal)
        return process_chunk_make_journal(worker, chunk);

    fp = ei_shard_fopen(chunk->path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", chunk->path, strerror(errno));
        atomic_fetch_add(&rs->bad_input_files, 1U);
        return -1;
    }
    /* Buffering is applied by the handle cache on the real open; a reused stream already has it. */

    {
        crawl_bin_chunk_stdio_t bio;
        bio.fopen = NULL;
        bio.fread = ei_fread;
        bio.fclose = NULL;
        if (crawl_bin_block_reader_init(&br, &bio, fp, chunk->start_offset, chunk->end_offset) != 0) {
            fprintf(stderr, "warn: seek failed in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            goto out;
        }
        /* Path, owner, and type (directory bit for search-time descendant expansion). */
        (void)crawl_bin_block_reader_set_projection(&br, CRAWL_COL_BIT(CRAWL_COL_PARENT_DIR_ID) |
                                                             CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES) |
                                                             CRAWL_COL_BIT(CRAWL_COL_UID) |
                                                             CRAWL_COL_BIT(CRAWL_COL_TYPE));
    }

    for (;;) {
        bin_record_hdr_t r;
        const unsigned char *rec_name = NULL;
        char *pathbuf = NULL;
        int got = crawl_bin_block_reader_next(&br, &r, &rec_name);

        if (got == 0) {
            rc = 0;
            break;
        }
        if (got < 0) {
            fprintf(stderr, "warn: read error in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
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

        if (!file_states[chunk->file_index].catalog) {
            fprintf(stderr, "warn: shard catalog not loaded for %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            break;
        }

        if (!ctx->aggregate_all_users && (uid_t)r.uid != ctx->target_uid) {
            /* Name bytes were decompressed with the record; nothing to skip. */
            continue;
        }

        if (r.parent_dir_id == 0ULL) {
            fprintf(stderr, "warn: incomplete/wire-format record in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            break;
        }

        /* Reconstruct into the worker's own buffer; write_batch_append copies the
         * bytes that survive the filters into the batch arena. */
        if (trigram_ensure_buf(&worker->path_buf, &worker->path_cap, PATH_MAX) != 0) {
            fprintf(stderr, "warn: path alloc failed in %s\n", chunk->path);
            atomic_fetch_add(&rs->bad_input_files, 1U);
            break;
        }
        pathbuf = worker->path_buf;
        {
            const unsigned char *name_bytes = (r.name_len > 0) ? rec_name : NULL;

            if (mk_entry_path_cached(file_states[chunk->file_index].catalog, r.parent_dir_id,
                                     (char *)name_bytes, r.name_len, pathbuf, PATH_MAX, &dir_cache) != 0) {
                fprintf(stderr, "warn: path reconstruct failed in %s\n", chunk->path);
                atomic_fetch_add(&rs->bad_input_files, 1U);
                break;
            }
        }

        /* --path-rewrite: relabel the stored prefix before indexing, so the index (and the --subtree filter)
         * use the rewritten namespace. */
        if (g_rewrite_from) {
            (void)rewrite_path_prefix(pathbuf, PATH_MAX);
        }

        /* --subtree: only index records at or under the requested directory (full absolute path kept). */
        if (g_subtree_prefix && !subtree_path_under_prefix(pathbuf)) continue;

        {
            uint32_t *codes = NULL;
            size_t code_count = 0;
            if (trigram_extract_path(pathbuf, &codes, &code_count, &worker->tri) != 0) {
                fprintf(stderr, "warn: failed to extract trigrams from %s\n", chunk->path);
                atomic_fetch_add(&rs->bad_input_files, 1U);
                break;
            }

            if (!batch) {
                batch = write_batch_create();
                if (!batch) {
                    fprintf(stderr, "warn: failed to allocate write batch for %s\n", chunk->path);
                    atomic_fetch_add(&rs->bad_input_files, 1U);
                    break;
                }
            }
            if (write_batch_append(batch, pathbuf, strlen(pathbuf), codes, code_count,
                                   r.type == (uint8_t)'d') != 0) {
                fprintf(stderr, "warn: failed to append write batch for %s\n", chunk->path);
                atomic_fetch_add(&rs->bad_input_files, 1U);
                break;
            }

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
    }

out:
    crawl_bin_block_reader_free(&br);
    if (scanned_local > scanned_published)
        atomic_fetch_add_explicit(&rs->scanned_records, scanned_local - scanned_published, memory_order_relaxed);
    ei_shard_fclose(fp);
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

    free(arg->tri.lower_seg_buf);
    arg->tri.lower_seg_buf = NULL;
    arg->tri.lower_seg_cap = 0;
    free(arg->path_buf);
    arg->path_buf = NULL;
    arg->path_cap = 0;
    free(arg->tri.codes_buf);
    arg->tri.codes_buf = NULL;
    arg->tri.codes_cap = 0;

    mk_io_tls_flush();
    return NULL;
}

static void close_index_path_files(build_ctx_t *ctx) {
    if (!ctx) return;
    if (ctx->paths_fp) {
        mk_fclose(ctx->paths_fp);
        ctx->paths_fp = NULL;
    }
    if (ctx->path_offsets_fp) {
        mk_fclose(ctx->path_offsets_fp);
        ctx->path_offsets_fp = NULL;
    }
    if (ctx->path_isdir_fp) {
        mk_fclose(ctx->path_isdir_fp);
        ctx->path_isdir_fp = NULL;
    }
}

/* path_offsets.bin carries one more entry than there are paths: the end of the last path. */
static int write_final_path_offset(build_ctx_t *ctx) {
    if (stage_path_offset(ctx, ctx->paths_pos) != 0) return -1;
    return flush_path_offsets(ctx);
}

/* paths.bin is authoritative for the level it was written at, so meta.txt just mirrors the header.
 * That matters for --resume-merge, which rewrites meta.txt beside a paths.bin it did not create. */
static uint32_t meta_read_paths_level(const char *index_dir) {
    char path[PATH_MAX];
    unsigned char hdr[PATHS_HDR_BYTES];
    FILE *fp;
    uint32_t level = 0;

    if (build_path(path, sizeof(path), index_dir, "paths.bin") != 0) return 0;
    fp = fopen(path, "rb");
    if (!fp) return 0;
    if (fread(hdr, 1, sizeof(hdr), fp) == sizeof(hdr) && memcmp(hdr, PATHS_MAGIC, sizeof(PATHS_MAGIC)) == 0)
        memcpy(&level, hdr + 12, sizeof(level));
    fclose(fp);
    return level;
}

/* ---------------------------------------------------------------------------
 * Dir-index sidecars (dirs.idx / rowgroups.idx)
 *
 * A phase of its own, run once the trigram merge has finished and every shard
 * catalog the index phase held has been freed. Two reasons it is not folded
 * into the chunk workers: process_chunk_make has no row-group identity to hang
 * a sketch on, and it attaches catalogs with fields=0U, so neither the DFS
 * permutation nor the row offsets these need are loaded there. Perturbing the
 * trigram pipeline to carry them would cost every build, including the ones
 * that pass --no-dir-index.
 *
 * Failure here is never fatal: the trigram index is complete and usable, the
 * sidecars are derived and rebuildable, and a reader that does not find them
 * behaves exactly as it did before they existed. So a bad shard warns, unlinks
 * the partial output and leaves the build succeeding.
 * ------------------------------------------------------------------------- */

/* Sidecars are built by default; --no-dir-index turns the phase off. */
static int g_dir_index = 1;

/* A stored path is bounded by the catalog's component limit (128 components of
 * up to 255 bytes), not by PATH_MAX, and none of those components had to have
 * come from one filesystem. Heap-allocated per worker, not on the stack. */
#define DIRX_PATH_BUF_BYTES 65536u

typedef struct {
    uint64_t prefix_dir; /* dir_id whose path fills buf; 0 when there is none */
    size_t prefix_len;
    char *buf; /* DIRX_PATH_BUF_BYTES */
} dirx_path_cache_t;

/*
 * Rebuild one directory's stored path, reusing the previous parent's.
 *
 * dir_ids are handed out as the crawl meets directories, so a walk in ascending
 * id order visits siblings together and one cached prefix absorbs nearly every
 * parent-chain walk. The result must be byte-identical to
 * crawl_bin_catalog_dir_path_len -- readers compare the query against that
 * spelling -- which is why a directory past the component limit is handed
 * straight to it: there the two routes genuinely differ, and the walk's answer
 * (leading components dropped) is the one that counts.
 */
static int dirx_dir_path(const crawl_bin_catalog_t *cat, dirx_path_cache_t *pc, uint64_t d, char *out,
                         size_t out_sz, size_t *len_out) {
    uint64_t par;
    size_t nl, end;

    if (!cat || d == 0ULL || d > cat->max_dir_id) return -1;
    if (d == 1ULL) {
        /* The synthetic root reconstructs to the empty string. */
        if (out_sz < 1U) return -1;
        out[0] = '\0';
        *len_out = 0;
        return 0;
    }
    if (cat->depth[d] > CRAWL_BIN_CATALOG_MAX_PATH_PARTS)
        return crawl_bin_catalog_dir_path_len(cat, d, out, out_sz, len_out);

    par = cat->parent_dir_id[d];
    if (par == 0ULL) return -1;
    if (par != pc->prefix_dir) {
        if (crawl_bin_catalog_dir_path_len(cat, par, pc->buf, DIRX_PATH_BUF_BYTES, &pc->prefix_len) != 0) {
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
    out[pc->prefix_len] = '/';
    if (nl > 0) memcpy(out + pc->prefix_len + 1U, cat->name_comp[d], nl);
    out[end] = '\0';
    *len_out = end;
    return 0;
}

typedef struct {
    const char *path; /* full shard path, borrowed from the caller */
    const char *base; /* basename inside path */
    crawl_sidecar_shard_id_t id; /* name_off/name_len filled by the writer */
    crawl_dirx_entry_t *entries;
    uint64_t entry_count;
    crawl_rgix_group_t *groups;
    uint64_t group_count;
    uint64_t dfs_domain;
    uint64_t unindexed_dirs; /* directories whose path would not rebuild */
    int rc;
} dirx_job_t;

static void dirx_job_release(dirx_job_t *j) {
    free(j->entries);
    j->entries = NULL;
    free(j->groups);
    j->groups = NULL;
}

static int dirx_cmp_entry(const void *a, const void *b) {
    const crawl_dirx_entry_t *x = (const crawl_dirx_entry_t *)a;
    const crawl_dirx_entry_t *y = (const crawl_dirx_entry_t *)b;

    if (x->path_hash != y->path_hash) return x->path_hash < y->path_hash ? -1 : 1;
    if (x->dir_id != y->dir_id) return x->dir_id < y->dir_id ? -1 : 1;
    return 0;
}

static int dirx_cmp_job_base(const void *a, const void *b) {
    return strcmp(((const dirx_job_t *)a)->base, ((const dirx_job_t *)b)->base);
}

/*
 * One pass over the record region decoding PARENT_DIR_ID alone, summarising each
 * row group by where its records' parents sit in DFS order.
 *
 * Both sketches are recorded. dir_id follows crawl arrival order, which tracks
 * DFS position only loosely, so the plain [min, max] interval can be far looser
 * than the set it stands for; the 1024-bit bitmap costs 128 bytes per group and
 * says which parts of the interval are actually occupied.
 */
static int dirx_scan_rowgroups(FILE *fp, uint64_t rec_lo, uint64_t rec_hi, const crawl_bin_catalog_t *cat,
                               uint64_t dfs_domain, crawl_rgix_group_t **out, uint64_t *out_n) {
    crawl_bin_block_reader_t br;
    crawl_bin_chunk_stdio_t bio;
    crawl_rgix_group_t *arr = NULL;
    size_t n = 0, cap = 0;

    *out = NULL;
    *out_n = 0;
    if (rec_hi <= rec_lo) return 0;

    memset(&br, 0, sizeof(br));
    bio.fopen = NULL;
    bio.fread = ei_fread;
    bio.fclose = NULL;
    if (crawl_bin_block_reader_init(&br, &bio, fp, rec_lo, rec_hi) != 0) return -1;
    (void)crawl_bin_block_reader_set_projection(&br, CRAWL_COL_BIT(CRAWL_COL_PARENT_DIR_ID));

    for (;;) {
        uint64_t start = br.pos;
        uint32_t nrec = 0;
        const uint64_t *pid;
        crawl_rgix_group_t *g;
        uint32_t k;
        int got = crawl_bin_block_reader_next_group(&br, &nrec);

        if (got == 0) break;
        if (got < 0) goto fail;

        if (n == cap) {
            size_t nc = cap ? cap * 2 : 256;
            crawl_rgix_group_t *p = (crawl_rgix_group_t *)realloc(arr, nc * sizeof(*p));

            if (!p) goto fail;
            arr = p;
            cap = nc;
        }
        g = &arr[n++];
        memset(g, 0, sizeof(*g));
        /* [start, br.pos) is the span the reader consumed. It can cover a run of
         * groups when empty ones were stepped over; both ends are still group
         * boundaries, which is all a chunked reader needs. */
        g->file_offset = start;
        g->group_bytes = br.pos - start;
        g->record_count = nrec;
        g->dfs_min = UINT64_MAX;
        g->dfs_max = 0;

        pid = crawl_bin_block_reader_column(&br, CRAWL_COL_PARENT_DIR_ID);
        if (!pid) {
            g->flags |= CRAWL_RGIX_GRP_UNKNOWN;
            continue;
        }
        for (k = 0; k < nrec; k++) {
            uint64_t d = pid[k];
            uint64_t dfs;
            unsigned bit;

            if (d == 0ULL || d > cat->max_dir_id) {
                g->flags |= CRAWL_RGIX_GRP_UNKNOWN;
                continue;
            }
            dfs = cat->dfs_index[d];
            if (dfs < g->dfs_min) g->dfs_min = dfs;
            if (dfs > g->dfs_max) g->dfs_max = dfs;
            bit = crawl_rgix_bucket_of(dfs, dfs_domain);
            g->buckets[bit >> 3] |= (unsigned char)(1U << (bit & 7U));
        }
    }

    crawl_bin_block_reader_free(&br);
    *out = arr;
    *out_n = (uint64_t)n;
    return 0;

fail:
    crawl_bin_block_reader_free(&br);
    free(arr);
    return -1;
}

static int dirx_build_shard(dirx_job_t *j) {
    FILE *fp = NULL;
    struct stat st;
    bin_file_header_t fh;
    crawl_bin_catalog_t cat;
    dirx_path_cache_t pc;
    char *pathbuf = NULL;
    uint64_t n_entries = 0;
    uint64_t did;
    uint64_t dfs_domain = 1;
    int rc = -1;

    crawl_bin_catalog_init_empty(&cat);
    memset(&pc, 0, sizeof(pc));

    if (stat(j->path, &st) != 0 || !S_ISREG(st.st_mode)) return -1;
    fp = ei_shard_fopen(j->path, "rb");
    if (!fp) return -1;
    if (mk_fread(&fh, sizeof(fh), 1, fp) != 1) goto out;
    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) goto out;
    if (fh.catalog_offset < sizeof(fh) || fh.catalog_offset > (uint64_t)st.st_size) goto out;
    if ((uint64_t)st.st_size - fh.catalog_offset < sizeof(uint64_t)) goto out;
    if (fseeko(fp, (off_t)fh.catalog_offset, SEEK_SET) != 0) goto out;
    if (mk_fread(&n_entries, sizeof(n_entries), 1, fp) != 1) goto out;

    /* The sketch needs the DFS permutation, and the paths the hash table is over
     * need the tree columns. Nothing else is loaded. */
    if (crawl_bin_catalog_load_sel(fp, fh.catalog_offset, (uint64_t)st.st_size, CRAWL_CAT_SUBTREE, &cat) != 0)
        goto out;
    if (!cat.dfs_index) goto out;

    j->id.shard_size = (uint64_t)st.st_size;
    j->id.shard_mtime_sec = (uint64_t)st.st_mtim.tv_sec;
    j->id.shard_mtime_nsec = (uint64_t)st.st_mtim.tv_nsec;
    j->id.catalog_offset = fh.catalog_offset;
    j->id.catalog_entries = n_entries;
    j->id.max_dir_id = cat.max_dir_id;

    pathbuf = (char *)malloc(DIRX_PATH_BUF_BYTES);
    pc.buf = (char *)malloc(DIRX_PATH_BUF_BYTES);
    j->entries = (crawl_dirx_entry_t *)malloc((size_t)(cat.max_dir_id + 1ULL) * sizeof(crawl_dirx_entry_t));
    if (!pathbuf || !pc.buf || !j->entries) goto out;

    for (did = 1; did <= cat.max_dir_id; did++) {
        size_t plen = 0;

        if (cat.dfs_index[did] >= dfs_domain) dfs_domain = cat.dfs_index[did] + 1ULL;
        if (dirx_dir_path(&cat, &pc, did, pathbuf, DIRX_PATH_BUF_BYTES, &plen) != 0) {
            /* subtree_find_dirs skips a directory whose path will not rebuild too,
             * so leaving it out of the table keeps the two routes in agreement. */
            j->unindexed_dirs++;
            continue;
        }
        j->entries[j->entry_count].path_hash = crawl_sidecar_path_hash(pathbuf, plen);
        j->entries[j->entry_count].dir_id = did;
        j->entry_count++;
    }
    qsort(j->entries, (size_t)j->entry_count, sizeof(*j->entries), dirx_cmp_entry);
    j->dfs_domain = dfs_domain;

    if (dirx_scan_rowgroups(fp, (uint64_t)sizeof(fh), fh.catalog_offset, &cat, dfs_domain, &j->groups,
                            &j->group_count) != 0)
        goto out;

    rc = 0;

out:
    free(pathbuf);
    free(pc.buf);
    crawl_bin_catalog_free(&cat);
    if (fp) ei_shard_fclose(fp);
    if (rc != 0) dirx_job_release(j);
    return rc;
}

typedef struct {
    dirx_job_t *jobs;
    size_t lo, hi; /* half-open batch bounds */
    _Atomic size_t next;
} dirx_pool_t;

static void *dirx_worker_main(void *arg) {
    dirx_pool_t *p = (dirx_pool_t *)arg;

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->next, 1, memory_order_relaxed);

        if (i >= p->hi) break;
        p->jobs[i].rc = dirx_build_shard(&p->jobs[i]);
    }
    mk_io_tls_flush();
    return NULL;
}

static int dirx_write_all(FILE *fp, const void *buf, size_t bytes, uint64_t *off) {
    if (bytes == 0U) return 0;
    if (mk_fwrite(buf, 1, bytes, fp) != bytes) return -1;
    *off += (uint64_t)bytes;
    return 0;
}

/*
 * Build both sidecars. Returns 0 when they were written, -1 when the phase was
 * abandoned (the caller treats that as a warning, not a build failure).
 *
 * Shards are processed in batches of `nthreads` and each batch's payload is
 * appended as soon as it is ready, so peak memory is one batch of catalogs plus
 * one batch of tables rather than the whole crawl's. That is also why the shard
 * descriptor arrays trail the payloads: their offsets are only known afterwards.
 */
static int build_dir_index_sidecars(build_ctx_t *ctx, char **paths, size_t path_count, int nthreads) {
    dirx_job_t *jobs = NULL;
    crawl_dirx_shard_t *dsh = NULL;
    crawl_rgix_shard_t *rsh = NULL;
    char *names = NULL;
    size_t names_len = 0;
    FILE *dfp = NULL, *rfp = NULL;
    char dtmp[PATH_MAX], rtmp[PATH_MAX], dfin[PATH_MAX], rfin[PATH_MAX];
    crawl_sidecar_hdr_t dh, rh;
    uint64_t doff = 0, roff = 0;
    uint64_t dirs_total = 0, groups_total = 0, unindexed_total = 0;
    size_t i, base;
    int rc = -1;

    if (path_count == 0) return -1;
    if (nthreads < 1) nthreads = 1;
    if ((size_t)nthreads > path_count) nthreads = (int)path_count;

    if (build_path(dtmp, sizeof(dtmp), ctx->index_dir, "dirs.idx.tmp") != 0 ||
        build_path(rtmp, sizeof(rtmp), ctx->index_dir, "rowgroups.idx.tmp") != 0 ||
        build_path(dfin, sizeof(dfin), ctx->index_dir, "dirs.idx") != 0 ||
        build_path(rfin, sizeof(rfin), ctx->index_dir, "rowgroups.idx") != 0)
        return -1;

    jobs = (dirx_job_t *)calloc(path_count, sizeof(*jobs));
    dsh = (crawl_dirx_shard_t *)calloc(path_count, sizeof(*dsh));
    rsh = (crawl_rgix_shard_t *)calloc(path_count, sizeof(*rsh));
    if (!jobs || !dsh || !rsh) goto out;

    for (i = 0; i < path_count; i++) {
        const char *slash = strrchr(paths[i], '/');

        jobs[i].path = paths[i];
        jobs[i].base = slash ? slash + 1 : paths[i];
    }
    /* Readers sort their own directory listing, so store shards the same way and
     * the identity check becomes a straight index-wise comparison. */
    qsort(jobs, path_count, sizeof(*jobs), dirx_cmp_job_base);
    for (i = 1; i < path_count; i++) {
        if (strcmp(jobs[i - 1].base, jobs[i].base) != 0) continue;
        /* Two input directories contributing the same shard name: a reader keyed
         * on basename could not tell the two apart, so do not claim to index them. */
        fprintf(stderr, "ereport_index: dir index skipped: duplicate shard name %s across input dirs\n",
                jobs[i].base);
        goto out;
    }

    dfp = mk_fopen(dtmp, "wb");
    rfp = mk_fopen(rtmp, "wb");
    if (!dfp || !rfp) goto out;
    if (setvbuf(dfp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) { /* speed only */
    }
    if (setvbuf(rfp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    memset(&dh, 0, sizeof(dh));
    memset(&rh, 0, sizeof(rh));
    if (dirx_write_all(dfp, &dh, sizeof(dh), &doff) != 0) goto out;
    if (dirx_write_all(rfp, &rh, sizeof(rh), &roff) != 0) goto out;

    for (base = 0; base < path_count; base += (size_t)nthreads) {
        size_t hi = base + (size_t)nthreads;
        dirx_pool_t pool;
        pthread_t *tids;
        int started = 0;
        int t;

        if (hi > path_count) hi = path_count;
        pool.jobs = jobs;
        pool.lo = base;
        pool.hi = hi;
        atomic_init(&pool.next, base);

        tids = (pthread_t *)calloc((size_t)nthreads, sizeof(*tids));
        if (!tids) goto out;
        for (t = 0; t < nthreads; t++) {
            if (pthread_create(&tids[t], NULL, dirx_worker_main, &pool) != 0) break;
            started++;
        }
        if (started == 0) {
            /* No worker started: run the batch here rather than silently skipping it. */
            dirx_worker_main(&pool);
        }
        for (t = 0; t < started; t++) pthread_join(tids[t], NULL);
        free(tids);

        for (i = base; i < hi; i++) {
            if (jobs[i].rc != 0) {
                fprintf(stderr, "ereport_index: dir index skipped: cannot summarise %s\n", jobs[i].path);
                goto out;
            }
            dsh[i].id = jobs[i].id;
            dsh[i].hash_count = jobs[i].entry_count;
            dsh[i].hash_off = doff;
            if (dirx_write_all(dfp, jobs[i].entries, (size_t)jobs[i].entry_count * sizeof(crawl_dirx_entry_t),
                               &doff) != 0)
                goto out;

            rsh[i].id = jobs[i].id;
            rsh[i].dfs_domain = jobs[i].dfs_domain;
            rsh[i].group_count = jobs[i].group_count;
            rsh[i].groups_off = roff;
            if (dirx_write_all(rfp, jobs[i].groups, (size_t)jobs[i].group_count * sizeof(crawl_rgix_group_t),
                               &roff) != 0)
                goto out;

            dirs_total += jobs[i].entry_count;
            groups_total += jobs[i].group_count;
            unindexed_total += jobs[i].unindexed_dirs;
            dirx_job_release(&jobs[i]);
        }
    }

    for (i = 0; i < path_count; i++) names_len += strlen(jobs[i].base);
    names = (char *)malloc(names_len ? names_len : 1U);
    if (!names) goto out;
    {
        size_t at = 0;

        for (i = 0; i < path_count; i++) {
            size_t nl = strlen(jobs[i].base);

            memcpy(names + at, jobs[i].base, nl);
            dsh[i].id.name_off = (uint64_t)at;
            dsh[i].id.name_len = (uint32_t)nl;
            rsh[i].id.name_off = (uint64_t)at;
            rsh[i].id.name_len = (uint32_t)nl;
            at += nl;
        }
    }

    memcpy(dh.magic, CRAWL_DIRX_MAGIC, CRAWL_SIDECAR_MAGIC_LEN);
    dh.version = CRAWL_SIDECAR_VERSION;
    dh.shard_count = (uint32_t)path_count;
    dh.shard_dir_off = doff;
    if (dirx_write_all(dfp, dsh, path_count * sizeof(*dsh), &doff) != 0) goto out;
    dh.names_off = doff;
    dh.names_bytes = (uint64_t)names_len;
    if (dirx_write_all(dfp, names, names_len, &doff) != 0) goto out;
    dh.entry_total = dirs_total;

    memcpy(rh.magic, CRAWL_RGIX_MAGIC, CRAWL_SIDECAR_MAGIC_LEN);
    rh.version = CRAWL_SIDECAR_VERSION;
    rh.shard_count = (uint32_t)path_count;
    rh.shard_dir_off = roff;
    if (dirx_write_all(rfp, rsh, path_count * sizeof(*rsh), &roff) != 0) goto out;
    rh.names_off = roff;
    rh.names_bytes = (uint64_t)names_len;
    if (dirx_write_all(rfp, names, names_len, &roff) != 0) goto out;
    rh.entry_total = groups_total;

    /* The header is last because it names offsets only the payload can settle. A
     * reader never sees the placeholder: the file is renamed into place after. */
    if (fseeko(dfp, 0, SEEK_SET) != 0 || mk_fwrite(&dh, sizeof(dh), 1, dfp) != 1) goto out;
    if (fseeko(rfp, 0, SEEK_SET) != 0 || mk_fwrite(&rh, sizeof(rh), 1, rfp) != 1) goto out;
    if (mk_fclose(dfp) != 0) {
        dfp = NULL;
        goto out;
    }
    dfp = NULL;
    if (mk_fclose(rfp) != 0) {
        rfp = NULL;
        goto out;
    }
    rfp = NULL;
    if (rename(dtmp, dfin) != 0) goto out;
    if (rename(rtmp, rfin) != 0) {
        (void)unlink(dfin);
        goto out;
    }

    ctx->dir_index_built = 1;
    ctx->rowgroup_index_built = 1;
    ctx->dir_index_dirs = dirs_total;
    ctx->dir_index_groups = groups_total;
    ctx->dir_index_bytes = doff;
    ctx->rowgroup_index_bytes = roff;
    if (unindexed_total && g_verbose)
        fprintf(stderr, "ereport_index: dir index: %" PRIu64 " directory(ies) had no reconstructable path\n",
                unindexed_total);
    rc = 0;

out:
    if (dfp) mk_fclose(dfp);
    if (rfp) mk_fclose(rfp);
    if (rc != 0) {
        (void)unlink(dtmp);
        (void)unlink(rtmp);
    }
    if (jobs)
        for (i = 0; i < path_count; i++) dirx_job_release(&jobs[i]);
    free(jobs);
    free(dsh);
    free(rsh);
    free(names);
    return rc;
}

static int write_meta_file(const build_ctx_t *ctx) {
    char path[PATH_MAX];
    FILE *fp;
    uint32_t paths_level;

    if (build_path(path, sizeof(path), ctx->index_dir, "meta.txt") != 0) return -1;
    fp = mk_fopen(path, "w");
    if (!fp) return -1;

    paths_level = meta_read_paths_level(ctx->index_dir);

    fprintf(fp, "ereport_index_version=%d\n", INDEX_VERSION);
    fprintf(fp, "trigrams=basename\n");
    fprintf(fp, "paths_comp=zstd_blocks\n");
    fprintf(fp, "postings_comp=zstd_framed\n");
    fprintf(fp, "zstd_level=%u\n", paths_level ? paths_level : (uint32_t)durable_zstd_level());
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
    /* Advisory only: the sidecars carry their own per-shard identity binding, and a
     * reader validates that rather than trusting these. No INDEX_VERSION bump, because
     * nothing keys on the lines: a reader falls back when the files are absent. */
    fprintf(fp, "dir_index=%d\n", ctx->dir_index_built ? 1 : 0);
    if (ctx->dir_index_built) fprintf(fp, "dir_index_dirs=%" PRIu64 "\n", ctx->dir_index_dirs);
    fprintf(fp, "rowgroup_index=%d\n", ctx->rowgroup_index_built ? 1 : 0);
    if (ctx->rowgroup_index_built) fprintf(fp, "rowgroup_index_groups=%" PRIu64 "\n", ctx->dir_index_groups);

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

/* Max worker id + 1 from tmp_trigrams_*_w%04u.bin names (0 when there are none). */
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
    char path[PATH_MAX];
    uint32_t w, max_w;

    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;
    for (w = 0; w < max_w; w++) {
        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, w) >= (int)sizeof(path))
            continue;
        (void)unlink(path);
    }
}

/*
 * Load all tmp_trigrams_%04u_w%04u.bin shards of a bucket into one buffer. Every tmp file is EITG0002
 * delta-varint framed; records are decoded and concatenated, then the caller's radix sort fixes global order.
 */
static int merge_load_bucket_tmp_files(build_ctx_t *ctx, uint32_t bucket, merge_loaded_bucket_t *out) {
    char path[PATH_MAX];
    struct stat st;
    uint64_t total_bytes = 0;
    size_t total_n = 0;
    uint32_t w, max_w;
    unsigned char *buf = NULL;
    size_t pos = 0;

    memset(out, 0, sizeof(*out));
    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;

    for (w = 0; w < max_w; w++) {
        size_t nrec = 0;
        uint64_t ubytes = 0;

        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, w) >= (int)sizeof(path))
            return -1;
        if (stat(path, &st) != 0 || st.st_size == 0) continue;
        if (tmp_trigram_count_file_records(path, &nrec, &ubytes) != 0) {
            fprintf(stderr, "ereport_index: merge bucket %04u: cannot count records in %s (corrupt/truncated tmp file?)\n",
                    bucket, path);
            return -1;
        }
        total_bytes += ubytes;
        total_n += nrec;
    }

    if (total_n == 0) {
        fprintf(stderr, "ereport_index: merge bucket %04u: tmp_trigrams present but 0 records counted\n", bucket);
        return -1;
    }

    buf = (unsigned char *)malloc(total_n * sizeof(trigram_record_t));
    if (!buf) {
        fprintf(stderr, "ereport_index: merge bucket %04u: malloc failed for %zu records (%.2f GiB)\n", bucket, total_n,
                (double)(total_n * sizeof(trigram_record_t)) / (1024.0 * 1024.0 * 1024.0));
        return -1;
    }

    for (w = 0; w < max_w; w++) {
        trigram_record_t *recs = NULL;
        size_t nrec = 0;

        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, w) >= (int)sizeof(path)) {
            free(buf);
            return -1;
        }
        if (stat(path, &st) != 0 || st.st_size == 0) continue;
        if (tmp_trigram_load_file(path, &recs, &nrec, NULL) != 0) {
            fprintf(stderr, "ereport_index: merge bucket %04u: failed to load %s\n", bucket, path);
            free(buf);
            return -1;
        }
        memcpy(buf + pos, recs, nrec * sizeof(trigram_record_t));
        pos += nrec * sizeof(trigram_record_t);
        free(recs);
    }

    out->records = (trigram_record_t *)buf;
    out->malloc_copy = 1;
    out->n = total_n;
    out->bytes = total_bytes;
    return 0;
}

static uint64_t bucket_tmp_files_total_bytes(build_ctx_t *ctx, uint32_t bucket) {
    char path[PATH_MAX];
    struct stat st;
    uint64_t sum = 0;
    uint32_t w, max_w;

    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;
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
    char path[PATH_MAX];
    struct stat st;
    size_t nb = 0;
    uint32_t i, w, max_w;

    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;

    for (i = 0; i < TRIGRAM_BUCKET_COUNT; i++) {
        uint64_t sz = 0;

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

/* Sum of *decoded* trigram-record bytes a bucket's tmp files expand to in RAM (n_records × 12),
 * read cheaply from the frame headers (no decoding). This — not the encoded on-disk size —
 * is what a merge worker actually allocates: merge_load_bucket_tmp_files mallocs this many bytes for the
 * records buffer and merge_bucket_to_segment_files mallocs an equal-size radix aux buffer (≈2× this). */
static uint64_t bucket_tmp_files_decompressed_bytes(build_ctx_t *ctx, uint32_t bucket) {
    char path[PATH_MAX];
    uint64_t sum = 0;
    uint32_t w, max_w;

    merge_ctx_ensure_trigram_shard_count(ctx);
    max_w = ctx->trigram_tmp_shard_count;
    for (w = 0; w < max_w; w++) {
        uint64_t ub = 0;
        if (snprintf(path, sizeof(path), "%s/tmp_trigrams_%04u_w%04u.bin", ctx->index_dir, bucket, w) >= (int)sizeof(path))
            continue;
        if (tmp_trigram_count_file_records(path, NULL, &ub) == 0) sum += ub;
    }
    return sum;
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

/* Current and peak resident set (KiB) from /proc/self/status; 0 if unreadable. Diagnostic only. */
static void read_proc_self_rss_kib(uint64_t *vmrss_kib, uint64_t *vmhwm_kib) {
    FILE *fp;
    char line[256];

    if (vmrss_kib) *vmrss_kib = 0;
    if (vmhwm_kib) *vmhwm_kib = 0;
    fp = fopen("/proc/self/status", "r");
    if (!fp) return;
    while (fgets(line, sizeof(line), fp)) {
        unsigned long kb = 0;
        if (vmrss_kib && strncmp(line, "VmRSS:", 6) == 0 && sscanf(line + 6, " %lu", &kb) == 1)
            *vmrss_kib = (uint64_t)kb;
        else if (vmhwm_kib && strncmp(line, "VmHWM:", 6) == 0 && sscanf(line + 6, " %lu", &kb) == 1)
            *vmhwm_kib = (uint64_t)kb;
    }
    fclose(fp);
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
 *           EREPORT_INDEX_MERGE_RAM_FRAC=0.35 (fraction of min(mem,cgroup) to use; default 35%).
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

/* Max aux bytes per radix-sort slice for the oversized-bucket path (see MERGE_SORT_SLICE_BYTES_DEFAULT). */
static uint64_t merge_sort_slice_bytes(void) {
    const char *e = getenv("EREPORT_INDEX_MERGE_SORT_SLICE_MB");

    if (e && *e) {
        char *end;
        unsigned long mb = strtoul(e, &end, 10);

        if (end != e && mb >= 64 && mb < (ULONG_MAX / (1024UL * 1024UL))) return (uint64_t)mb * 1024ULL * 1024ULL;
    }
    return MERGE_SORT_SLICE_BYTES_DEFAULT;
}

/*
 * Peak anonymous RAM a merge worker holds for a bucket with `dec_bytes` decompressed records, used by
 * admission control. Buckets up to one slice radix-sort with an equal-size aux (2× peak); larger buckets
 * use the slice + k-way path that caps aux at one slice (1× records + one slice peak).
 */
static uint64_t merge_bucket_ram_need(uint64_t dec_bytes) {
    uint64_t slice = merge_sort_slice_bytes();

    if (dec_bytes <= slice) return dec_bytes * 2ULL + MERGE_PER_WORKER_OVERHEAD_BYTES;
    return dec_bytes + slice + MERGE_PER_WORKER_OVERHEAD_BYTES;
}

typedef struct {
    build_ctx_t *ctx;
    const uint32_t *buckets;
    const uint64_t *needs; /* per-dispatch-index RAM need (see merge_bucket_ram_need); NULL = no gating */
    size_t bucket_count;
    atomic_size_t next;
    atomic_int failed;
    atomic_ullong bytes_in;
    atomic_ullong records_in;
    uint64_t budget;          /* merge RAM budget in bytes; 0 = no gating */
    pthread_mutex_t adm_mu;   /* guards ram_reserved */
    pthread_cond_t adm_cv;
    uint64_t ram_reserved;
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

/*
 * Set up the within-bucket parallel-sort thread budget. The merge worker pool is capped at
 * MERGE_MAX_WORKERS, but the host typically has many more cores; while a giant bucket sorts, the other
 * workers are blocked in RAM admission, so the sort may borrow up to the full core count. Default cap
 * and pool size = online CPUs; override the per-bucket cap with EREPORT_INDEX_MERGE_SORT_THREADS.
 */
static void merge_init_thread_budget(build_ctx_t *ctx, long ncpu_real) {
    /*
     * Default OFF (1 = serial sort). Measured on a 953M-path build: merge_phase_sec ≈
     * decompressed_bytes / temp_read_rate in every run — the merge is bound by the rate it pulls
     * records through (tmp-file read + memory bandwidth), not by sort CPU. Parallel-sorting many
     * buckets at once touches large fresh anon aux buffers in bursts, which under the merge's memory
     * budget triggers a kernel page-reclaim storm (it evicts the very tmp page cache the merge is
     * reading) and *lowers* throughput (489→421 MB/s, merge 2414→2806 s). Opt in with
     * EREPORT_INDEX_MERGE_SORT_THREADS>1 only when the merge is CPU-bound (e.g. fast local NVMe where
     * read bandwidth is not the limit). The parallel sort path is verified byte-identical to serial.
     *
     * Remeasured on node-local NVMe (single_huge_dir, 64 cores): merge_phase_sec 0.696 serial, 0.469 at
     * 4 threads, 0.329 at 16 — the opposite sign to the 953M-path build above, because that fixture is
     * small enough to be sort-bound. Left off by default: the workload it would speed up is the one that
     * does not need it. Raising the worker pool instead (EREPORT_INDEX_MERGE_WORKERS=32/64) did not help
     * either fixture (0.840 / 0.701 s), so MERGE_MAX_WORKERS stays at 16.
     */
    long maxt = 1;
    const char *e = getenv("EREPORT_INDEX_MERGE_SORT_THREADS");

    if (e && *e) {
        char *end;
        long v;

        errno = 0;
        v = strtol(e, &end, 10);
        if (!errno && end != e && !*end && v >= 1 && v <= 4096) maxt = v;
    }
    if (maxt < 1) maxt = 1;
    if (maxt > 4096) maxt = 4096;
    pthread_mutex_init(&ctx->merge_thr_mu, NULL);
    ctx->merge_sort_threads_max = (int)maxt;
    ctx->merge_thr_free = (int)(ncpu_real < 1 ? 1 : ncpu_real);
}

static void merge_destroy_thread_budget(build_ctx_t *ctx) {
    pthread_mutex_destroy(&ctx->merge_thr_mu);
}

/*
 * Merge worker pool size: online CPUs, capped at MERGE_MAX_WORKERS. Each worker holds a bucket's
 * records in anon RAM, but per-bucket admission control — not this number — is what keeps that
 * bounded. Overridable with EREPORT_INDEX_MERGE_WORKERS; see MERGE_MAX_WORKERS for why raising it
 * did not help.
 */
static int merge_worker_cap(void) {
    const char *e = getenv("EREPORT_INDEX_MERGE_WORKERS");
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    long cap = MERGE_MAX_WORKERS;

    if (e && *e) {
        char *end;
        long v;

        errno = 0;
        v = strtol(e, &end, 10);
        if (!errno && end != e && !*end && v >= 1 && v <= 4096) cap = v;
    }
    if (ncpu < 1) ncpu = 4;
    return (int)(ncpu > cap ? cap : ncpu);
}

/* Borrow up to `want` idle CPU threads from the shared merge budget; returns the number granted. */
static int merge_thr_acquire(build_ctx_t *ctx, int want) {
    int got;

    if (want <= 0) return 0;
    pthread_mutex_lock(&ctx->merge_thr_mu);
    got = ctx->merge_thr_free < want ? ctx->merge_thr_free : want;
    if (got < 0) got = 0;
    ctx->merge_thr_free -= got;
    pthread_mutex_unlock(&ctx->merge_thr_mu);
    return got;
}

static void merge_thr_release(build_ctx_t *ctx, int n) {
    if (n <= 0) return;
    pthread_mutex_lock(&ctx->merge_thr_mu);
    ctx->merge_thr_free += n;
    pthread_mutex_unlock(&ctx->merge_thr_mu);
}

/*
 * Allocate the radix aux buffer and sort one loaded bucket. Large buckets borrow idle merge threads
 * (the others are blocked in RAM admission while a giant bucket is resident) and sort in parallel;
 * small buckets sort single-threaded. Returns 0 on success, -1 on aux alloc failure.
 */
static int merge_sort_loaded_bucket(build_ctx_t *ctx, merge_loaded_bucket_t *L) {
    trigram_record_t *aux;
    int got = 0, threads = 1;

    if (L->n >= MERGE_PARALLEL_SORT_MIN_RECORDS && ctx->merge_sort_threads_max > 1) {
        uint64_t want64 = L->n / MERGE_RECORDS_PER_SORT_THREAD;
        int want = (int)(want64 > (uint64_t)ctx->merge_sort_threads_max ? (uint64_t)ctx->merge_sort_threads_max
                                                                        : want64);

        if (want > 1) {
            got = merge_thr_acquire(ctx, want - 1);
            threads = 1 + got;
        }
    }
    aux = (trigram_record_t *)malloc(L->n * sizeof(*aux));
    if (!aux) {
        merge_thr_release(ctx, got);
        return -1;
    }
    parallel_radix_sort_trigram_records(L->records, L->n, aux, threads);
    free(aux);
    merge_thr_release(ctx, got);
    return 0;
}

/*
 * Sort one loaded bucket and write its keys/postings to keys_fp/postings_fp (segment or final files).
 * Buckets that fit in a single sort slice take the existing full-width radix path; oversized buckets
 * radix-sort fixed-size slices with one reused aux buffer and k-way merge them straight into the postings
 * encoder, capping peak RAM at ~1× records + one slice. Output is byte-identical either way.
 */
static int merge_sort_and_write_bucket(build_ctx_t *ctx, merge_loaded_bucket_t *L, FILE *keys_fp, FILE *postings_fp,
                                       uint64_t *unique_out) {
    uint64_t slice_bytes = merge_sort_slice_bytes();

    if ((uint64_t)L->n * sizeof(trigram_record_t) <= slice_bytes) {
        if (merge_sort_loaded_bucket(ctx, L) != 0) return -1;
        return write_sorted_bucket_records(keys_fp, postings_fp, L->records, L->n, unique_out);
    }
    {
        size_t slice_recs = (size_t)(slice_bytes / sizeof(trigram_record_t));
        size_t nslices, s;
        trigram_record_t *aux;

        if (slice_recs < 1) slice_recs = 1;
        nslices = (L->n + slice_recs - 1) / slice_recs;
        aux = (trigram_record_t *)malloc(slice_recs * sizeof(*aux));
        if (!aux) return -1;
        for (s = 0; s < nslices; s++) {
            size_t base = s * slice_recs;
            size_t cnt = (base + slice_recs <= L->n) ? slice_recs : (L->n - base);

            radix_sort_trigram_records(L->records + base, cnt, aux);
        }
        free(aux); /* drop the slice aux before the merge so peak stays at ~1× records */
        return merge_kway_write_bucket(keys_fp, postings_fp, L->records, L->n, slice_recs, unique_out);
    }
}

static int merge_bucket_to_segment_files(build_ctx_t *ctx, uint32_t bucket, merge_parallel_arg_t *accum) {
    char kseg[PATH_MAX];
    char pseg[PATH_MAX];
    merge_loaded_bucket_t L;
    FILE *kf = NULL;
    FILE *pf = NULL;
    uint64_t u = 0;
    int rc = -1;

    if (snprintf(kseg, sizeof(kseg), "%s/merge_seg_k_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(kseg)) return -1;
    if (snprintf(pseg, sizeof(pseg), "%s/merge_seg_p_%04u.bin", ctx->index_dir, bucket) >= (int)sizeof(pseg)) return -1;

    if (merge_load_bucket_tmp_files(ctx, bucket, &L) != 0) return -1;
    {
        uint64_t peak = merge_bucket_ram_need(L.bytes) - MERGE_PER_WORKER_OVERHEAD_BYTES;
        uint64_t cur = atomic_load_explicit(&ctx->merge_bucket_ram_peak, memory_order_relaxed);

        while (peak > cur) {
            if (atomic_compare_exchange_weak_explicit(&ctx->merge_bucket_ram_peak, &cur, peak, memory_order_relaxed,
                                                      memory_order_relaxed))
                break;
        }
    }

    kf = mk_fopen(kseg, "wb");
    pf = mk_fopen(pseg, "wb");
    if (!kf || !pf) {
        fprintf(stderr, "ereport_index: merge bucket %04u: cannot open segment file %s: %s\n", bucket, kf ? pseg : kseg,
                strerror(errno));
        goto err;
    }
    if (setvbuf(kf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (setvbuf(pf, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    if (merge_sort_and_write_bucket(ctx, &L, kf, pf, &u) != 0) {
        fprintf(stderr, "ereport_index: merge bucket %04u: sort/write failed (%zu records; OOM, disk full, or "
                        "RLIMIT_FSIZE?)\n",
                bucket, L.n);
        goto err;
    }
    if (mk_fclose(kf) != 0) {
        fprintf(stderr, "ereport_index: merge bucket %04u: close %s failed: %s\n", bucket, kseg, strerror(errno));
        kf = NULL;
        goto err;
    }
    kf = NULL;
    if (mk_fclose(pf) != 0) {
        fprintf(stderr, "ereport_index: merge bucket %04u: close %s failed: %s\n", bucket, pseg, strerror(errno));
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
    int gated = (p->needs != NULL && p->budget > 0);

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->next, 1U, memory_order_relaxed);
        uint64_t need = 0;

        if (i >= p->bucket_count) break;

        /* Memory-aware admission: reserve this bucket's ~2x-decompressed footprint against a shared
         * budget before loading it. A bucket that fits waits only until enough RAM frees; a bucket
         * that alone exceeds the budget runs exclusively (admitted once nothing else is resident).
         * This keeps peak RSS bounded while letting the many small buckets run concurrently, instead
         * of pinning the whole pool to one worker because a single bucket is huge. Deadlock-free:
         * whichever worker sees ram_reserved==0 always proceeds. */
        if (gated) {
            need = p->needs[i];
            pthread_mutex_lock(&p->adm_mu);
            while (p->ram_reserved > 0 && p->ram_reserved + need > p->budget)
                pthread_cond_wait(&p->adm_cv, &p->adm_mu);
            p->ram_reserved += need;
            pthread_mutex_unlock(&p->adm_mu);
        }

        if (merge_bucket_to_segment_files(p->ctx, p->buckets[i], p) != 0)
            atomic_store_explicit(&p->failed, 1, memory_order_relaxed);

        if (gated) {
            pthread_mutex_lock(&p->adm_mu);
            p->ram_reserved -= need;
            pthread_cond_broadcast(&p->adm_cv);
            pthread_mutex_unlock(&p->adm_mu);
        }
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
    if (n > 1) qsort(out, n, sizeof(uint32_t), trigram_cmp_u32);
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
    if (n > 1) qsort(out, n, sizeof(uint32_t), trigram_cmp_u32);
    *out_n = n;
    return 0;
}

typedef struct {
    uint32_t bucket;
    uint64_t bytes;
} merge_bucket_size_t;

static int merge_bucket_size_cmp_desc(const void *a, const void *b) {
    const merge_bucket_size_t *x = (const merge_bucket_size_t *)a;
    const merge_bucket_size_t *y = (const merge_bucket_size_t *)b;

    if (x->bytes > y->bytes) return -1;
    if (x->bytes < y->bytes) return 1;
    if (x->bucket < y->bucket) return -1;
    if (x->bucket > y->bucket) return 1;
    return 0;
}

/*
 * Build a largest-first merge dispatch order keyed on each bucket's DECOMPRESSED footprint, plus the
 * per-entry RAM need (2× decompressed + per-worker overhead) used by merge admission control. Fills
 * dispatch_out[n] and needs_out[n] (aligned: needs_out[i] is for dispatch_out[i]) and returns the
 * largest single-bucket decompressed footprint (diagnostic). Falls back to canonical order on alloc
 * failure (still correct; admission still bounds RAM).
 */
static uint64_t merge_build_dispatch_and_needs(build_ctx_t *ctx, const uint32_t *bucket_list, size_t n,
                                               uint32_t *dispatch_out, uint64_t *needs_out) {
    merge_bucket_size_t *sz;
    size_t i;
    uint64_t maxdec = 0;

    sz = (merge_bucket_size_t *)malloc(n * sizeof(*sz));
    if (!sz) {
        for (i = 0; i < n; i++) {
            uint64_t d = bucket_tmp_files_decompressed_bytes(ctx, bucket_list[i]);
            dispatch_out[i] = bucket_list[i];
            needs_out[i] = merge_bucket_ram_need(d);
            if (d > maxdec) maxdec = d;
        }
        return maxdec;
    }
    for (i = 0; i < n; i++) {
        sz[i].bucket = bucket_list[i];
        sz[i].bytes = bucket_tmp_files_decompressed_bytes(ctx, bucket_list[i]);
    }
    qsort(sz, n, sizeof(*sz), merge_bucket_size_cmp_desc);
    for (i = 0; i < n; i++) {
        dispatch_out[i] = sz[i].bucket;
        needs_out[i] = merge_bucket_ram_need(sz[i].bytes);
        if (sz[i].bytes > maxdec) maxdec = sz[i].bytes;
    }
    free(sz);
    return maxdec;
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
    uint32_t *need_merge = NULL;
    uint32_t *stitch_buckets = NULL;
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
    {
        long ncpu_real = sysconf(_SC_NPROCESSORS_ONLN);
        merge_init_thread_budget(ctx, ncpu_real < 1 ? 4 : ncpu_real);
    }

    if (g_verbose) fprintf(stderr, "ereport_index: resuming merge in %s\n", ctx->index_dir);

    if (build_path(key_path, sizeof(key_path), ctx->index_dir, "tri_keys.bin") != 0 ||
        build_path(postings_path, sizeof(postings_path), ctx->index_dir, "tri_postings.bin") != 0) {
        merge_destroy_thread_budget(ctx);
        return -1;
    }

    need_merge = (uint32_t *)malloc((size_t)TRIGRAM_BUCKET_COUNT * sizeof(uint32_t));
    stitch_buckets = (uint32_t *)malloc((size_t)TRIGRAM_BUCKET_COUNT * sizeof(uint32_t));
    if (!need_merge || !stitch_buckets) {
        fprintf(stderr, "ereport_index: resume-merge: out of memory allocating bucket lists\n");
        goto out;
    }

    n_need = merge_resume_list_tmp_buckets(ctx, need_merge, TRIGRAM_BUCKET_COUNT);
    {
        struct stat st_tri;
        int has_tri = (stat(key_path, &st_tri) == 0 && st_tri.st_size > 0);

        if (n_need > 0 && has_tri && !merge_any_merge_seg_k_nonempty(ctx)) {
            fprintf(stderr,
                    "ereport_index: cannot resume-merge: tmp_trigrams_*.bin remain but tri_keys.bin exists and there are "
                    "no merge_seg_*.bin files (merge used the single-thread path). Re-run a full `ereport_index --make`.\n");
            goto out;
        }
    }

    if (g_verbose) {
        merge_ctx_ensure_trigram_shard_count(ctx);
        fprintf(stderr, "ereport_index: resume-merge: %zu tmp_trigram bucket(s) to merge, tmp shard writers=%u\n", n_need,
                ctx->trigram_tmp_shard_count);
    }

    unlink(key_path);
    unlink(postings_path);
    merge_unlink_orphan_segment_halves(ctx);

    /* Workers are sized by CPU; RAM is bounded at load time by per-bucket admission control (see
     * merge_parallel_worker), so the single largest bucket no longer pins the pool to one worker. */
    ctx->merge_parallel_budget_bytes = merge_parallel_ram_budget_bytes();

    ncpu = merge_worker_cap();
    if (n_need == 0) {
        merge_workers = 1;
    } else {
        merge_workers = (int)ncpu;
        if ((size_t)merge_workers > n_need) merge_workers = (int)n_need;
        if (n_need < (size_t)MERGE_PARALLEL_MIN) merge_workers = 1;
    }

    ctx->merge_workers_cpu = merge_workers;
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
            uint64_t *needs = NULL;
            int ti;

            parallel_list = (uint32_t *)malloc(n_need * sizeof(uint32_t));
            needs = (uint64_t *)malloc(n_need * sizeof(uint64_t));
            if (!parallel_list || !needs) {
                free(needs);
                goto out;
            }
            ctx->merge_max_bucket_bytes = merge_build_dispatch_and_needs(ctx, need_merge, n_need, parallel_list, needs);

            memset(&mp, 0, sizeof(mp));
            mp.ctx = ctx;
            mp.buckets = parallel_list;
            mp.needs = needs;
            mp.bucket_count = n_need;
            mp.budget = ctx->merge_parallel_budget_bytes;
            mp.ram_reserved = 0;
            pthread_mutex_init(&mp.adm_mu, NULL);
            pthread_cond_init(&mp.adm_cv, NULL);
            atomic_init(&mp.next, 0);
            atomic_init(&mp.failed, 0);
            atomic_init(&mp.bytes_in, 0);
            atomic_init(&mp.records_in, 0);

            threads = (pthread_t *)calloc((size_t)merge_workers, sizeof(*threads));
            if (!threads) {
                pthread_mutex_destroy(&mp.adm_mu);
                pthread_cond_destroy(&mp.adm_cv);
                free(needs);
                goto out;
            }
            for (ti = 0; ti < merge_workers; ti++) {
                if (pthread_create(&threads[ti], NULL, merge_parallel_worker, &mp) != 0) {
                    atomic_store(&mp.failed, 1);
                    break;
                }
            }
            ctx->merge_workers_used = ti;
            for (ti = 0; ti < ctx->merge_workers_used; ti++) pthread_join(threads[ti], NULL);
            free(threads);
            pthread_mutex_destroy(&mp.adm_mu);
            pthread_cond_destroy(&mp.adm_cv);
            free(needs);

            if (atomic_load(&mp.failed)) {
                fprintf(stderr, "ereport_index: resume-merge: a parallel tmp\u2192seg worker failed (see bucket error above)\n");
                for (bi = 0; bi < n_need; bi++) merge_unlink_segment_pair(ctx, need_merge[bi]);
                goto out;
            }
            merge_bytes_temp = (uint64_t)atomic_load(&mp.bytes_in);
            merge_records_in = (uint64_t)atomic_load(&mp.records_in);
        }
    }

    if (merge_list_segment_bucket_ids(ctx, stitch_buckets, &n_stitch, TRIGRAM_BUCKET_COUNT) != 0) {
        fprintf(stderr, "ereport_index: resume-merge: failed to list merge_seg_*.bin segments in %s\n", ctx->index_dir);
        goto out;
    }

    keys_fp = mk_fopen(key_path, "wb");
    postings_fp = mk_fopen(postings_path, "wb");
    if (!keys_fp || !postings_fp) {
        fprintf(stderr, "ereport_index: resume-merge: cannot create %s: %s\n", keys_fp ? postings_path : key_path,
                strerror(errno));
        goto out;
    }
    if (setvbuf(keys_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (setvbuf(postings_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    if (merge_stitch_segments(ctx, stitch_buckets, n_stitch, keys_fp, postings_fp, &unique_trigrams) != 0) {
        fprintf(stderr, "ereport_index: resume-merge: stitch of %zu segments failed (%s)\n", n_stitch, strerror(errno));
        goto out;
    }
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
    free(need_merge);
    free(stitch_buckets);
    merge_destroy_thread_budget(ctx);
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
    warn_rlimit_fsize_if_capped_for_index_io();
    if (path_resolve_existing(index_dir, ctx.index_dir, "ereport_index: ") != 0) return 1;
    if (build_path(paths_p, sizeof(paths_p), ctx.index_dir, "paths.bin") != 0 ||
        build_path(off_p, sizeof(off_p), ctx.index_dir, "path_offsets.bin") != 0) {
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
    /* The merge only rewrites tri_*, so paths.bin has to already be in the format this build reads. */
    {
        paths_index_t pi;

        if (paths_index_open(paths_p, &pi) != 0) return 1;
        paths_index_close(&pi);
    }

    ipc = path_offsets_indexed_path_count(ctx.index_dir);
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
    if (g_verbose && build_path(logpath, sizeof(logpath), ctx.index_dir, "ereport_index.log") == 0) {
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
        fprintf(stderr, "ereport_index: resume-merge failed in %s\n", ctx.index_dir);
        return 1;
    }
    memlog_shutdown(&ml_storage, &memlog_tid, &memlog_started);
    if (write_meta_file(&ctx) != 0) {
        fprintf(stderr, "ereport_index: could not write meta.txt in %s\n", ctx.index_dir);
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
    {
        long ncpu_real = sysconf(_SC_NPROCESSORS_ONLN);
        merge_init_thread_budget(ctx, ncpu_real < 1 ? 4 : ncpu_real);
    }

    nbuckets = merge_collect_nonempty_from_bitset(ctx, &bucket_list);
    if (nbuckets == 0) nbuckets = merge_collect_nonempty_buckets_stat_scan(ctx, &bucket_list);
    merge_skipped = (uint32_t)(TRIGRAM_BUCKET_COUNT - nbuckets);

    if (build_path(key_path, sizeof(key_path), ctx->index_dir, "tri_keys.bin") != 0 ||
        build_path(postings_path, sizeof(postings_path), ctx->index_dir, "tri_postings.bin") != 0) {
        free(bucket_list);
        merge_destroy_thread_budget(ctx);
        return -1;
    }

    keys_fp = mk_fopen(key_path, "wb");
    postings_fp = mk_fopen(postings_path, "wb");
    if (!keys_fp || !postings_fp) goto out;
    if (setvbuf(keys_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (setvbuf(postings_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }

    /* Workers are sized by CPU; RAM is bounded at load time by per-bucket admission control (the
     * worker pool is no longer pinned to the single largest bucket, which serialized the whole merge). */
    ctx->merge_parallel_budget_bytes = merge_parallel_ram_budget_bytes();

    ncpu = merge_worker_cap();
    merge_workers = (int)ncpu;
    if ((size_t)merge_workers > nbuckets) merge_workers = (int)nbuckets;
    if (nbuckets < (size_t)MERGE_PARALLEL_MIN) merge_workers = 1;

    ctx->merge_workers_cpu = merge_workers;
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
            uint64_t u = 0;

            if (merge_load_bucket_tmp_files(ctx, bucket, &L) != 0) goto out;
            {
                uint64_t peak = merge_bucket_ram_need(L.bytes) - MERGE_PER_WORKER_OVERHEAD_BYTES;
                uint64_t cur = atomic_load_explicit(&ctx->merge_bucket_ram_peak, memory_order_relaxed);

                while (peak > cur) {
                    if (atomic_compare_exchange_weak_explicit(&ctx->merge_bucket_ram_peak, &cur, peak, memory_order_relaxed,
                                                              memory_order_relaxed))
                        break;
                }
            }
            merge_bytes_temp += L.bytes;
            merge_records_in += (uint64_t)L.n;
            if (merge_sort_and_write_bucket(ctx, &L, keys_fp, postings_fp, &u) != 0) {
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
        uint32_t *dispatch_list;
        uint64_t *needs;
        int ti;

        /* Dispatch biggest-first by decompressed footprint; bucket_list stays canonical for the stitch. */
        dispatch_list = (uint32_t *)malloc(nbuckets * sizeof(uint32_t));
        needs = (uint64_t *)malloc(nbuckets * sizeof(uint64_t));
        if (!dispatch_list || !needs) {
            free(dispatch_list);
            free(needs);
            goto out;
        }
        ctx->merge_max_bucket_bytes = merge_build_dispatch_and_needs(ctx, bucket_list, nbuckets, dispatch_list, needs);

        memset(&mp, 0, sizeof(mp));
        mp.ctx = ctx;
        mp.buckets = dispatch_list;
        mp.needs = needs;
        mp.bucket_count = nbuckets;
        mp.budget = ctx->merge_parallel_budget_bytes;
        mp.ram_reserved = 0;
        pthread_mutex_init(&mp.adm_mu, NULL);
        pthread_cond_init(&mp.adm_cv, NULL);
        atomic_init(&mp.next, 0);
        atomic_init(&mp.failed, 0);
        atomic_init(&mp.bytes_in, 0);
        atomic_init(&mp.records_in, 0);

        threads = (pthread_t *)calloc((size_t)merge_workers, sizeof(*threads));
        if (!threads) {
            pthread_mutex_destroy(&mp.adm_mu);
            pthread_cond_destroy(&mp.adm_cv);
            free(dispatch_list);
            free(needs);
            goto out;
        }
        for (ti = 0; ti < merge_workers; ti++) {
            if (pthread_create(&threads[ti], NULL, merge_parallel_worker, &mp) != 0) {
                atomic_store(&mp.failed, 1);
                break;
            }
        }
        ctx->merge_workers_used = ti;
        for (ti = 0; ti < ctx->merge_workers_used; ti++) pthread_join(threads[ti], NULL);
        free(threads);
        pthread_mutex_destroy(&mp.adm_mu);
        pthread_cond_destroy(&mp.adm_cv);
        free(dispatch_list);
        free(needs);

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
    merge_destroy_thread_budget(ctx);
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
    uint64_t journal_shards_deleted = 0;
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

    warn_rlimit_fsize_if_capped_for_index_io();

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
            index_free_file_states(file_states, path_count);
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
            index_free_file_states(file_states, path_count);
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
        pool.file_states = file_states;
        pool.run_stats = &run_stats;
        atomic_store(&pool.next_path_index, 0);

        prep_tids = (pthread_t *)calloc((size_t)prep_threads, sizeof(*prep_tids));
        if (!prep_tids) {
            fprintf(stderr, "allocation failed\n");
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
            free(chunk_targets);
            free(prep_rc);
            free(prep_chunks);
            free(prep_chunk_counts);
            for (i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            index_free_file_states(file_states, path_count);
            return 1;
        }

        for (i = 0; i < (size_t)prep_threads; i++) {
            if (pthread_create(&prep_tids[i], NULL, chunk_prep_worker_main, &pool) != 0) {
                size_t j;
                fprintf(stderr, "failed to create chunk-prep thread\n");
                index_stats_stop_request(&run_stats);
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
                index_free_file_states(file_states, path_count);
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
                    crawl_bin_free_chunk_array_rows(prep_chunks[i], prep_chunk_counts[i]);
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
                index_stats_stop_request(&run_stats);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
                for (i = 0; i < path_count; i++) {
                    if (prep_rc[i] == 0 && prep_chunks[i]) crawl_bin_free_chunk_array_rows(prep_chunks[i], prep_chunk_counts[i]);
                }
                free(chunk_targets);
                free(prep_rc);
                free(prep_chunks);
                free(prep_chunk_counts);
                for (i = 0; i < path_count; i++) free(paths[i]);
                free(paths);
                index_free_file_states(file_states, path_count);
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
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        index_free_file_states(file_states, path_count);
        free(chunks);
        return 1;
    }

    for (i = 0; i < path_count; i++) {
        if (atomic_load(&file_states[i].remaining_chunks) == 0U) continue;
        if (file_states[i].use_journal) continue; /* journal replay needs no directory catalog */
        if (index_attach_shard_catalog(&file_states[i], paths[i]) != 0) {
            fprintf(stderr, "ereport_index: cannot load directory catalog from %s\n", paths[i]);
            if (stats_thread_started) {
                index_stats_stop_request(&run_stats);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
            }
            for (i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            for (i = 0; i < chunk_count; i++) free(chunks[i].path);
            free(chunks);
            index_free_file_states(file_states, path_count);
            return 1;
        }
    }

    if ((!index_dir_override || index_dir_override[0] == '\0') && ensure_dir_recursive(sanitized_name) != 0) {
        fprintf(stderr, "failed to create %s: %s\n", sanitized_name, strerror(errno));
        if (stats_thread_started) {
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        index_free_file_states(file_states, path_count);
        return 1;
    }
    if (ensure_dir_recursive(ctx.index_dir) != 0) {
        fprintf(stderr, "failed to create %s: %s\n", ctx.index_dir, strerror(errno));
        if (stats_thread_started) {
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        index_free_file_states(file_states, path_count);
        return 1;
    }

    (void)path_try_resolve_inplace(ctx.index_dir, sizeof(ctx.index_dir));

    {
        char isdir_path[PATH_MAX];

        if (build_path(paths_path, sizeof(paths_path), ctx.index_dir, "paths.bin") != 0 ||
            build_path(offsets_path, sizeof(offsets_path), ctx.index_dir, "path_offsets.bin") != 0 ||
            build_path(isdir_path, sizeof(isdir_path), ctx.index_dir, "path_isdir.bin") != 0) {
            if (stats_thread_started) {
                index_stats_stop_request(&run_stats);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
            }
            for (i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            for (i = 0; i < chunk_count; i++) free(chunks[i].path);
            free(chunks);
            index_free_file_states(file_states, path_count);
            return 1;
        }

        ctx.paths_fp = mk_fopen(paths_path, "wb");
        ctx.path_offsets_fp = mk_fopen(offsets_path, "wb");
        ctx.path_isdir_fp = mk_fopen(isdir_path, "wb");
        ctx.isdir_pending_byte = 0;
        ctx.isdir_pending_bits = 0;
    }
    if (ctx.paths_fp && setvbuf(ctx.paths_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (ctx.path_offsets_fp && setvbuf(ctx.path_offsets_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (ctx.path_isdir_fp && setvbuf(ctx.path_isdir_fp, NULL, _IOFBF, MERGE_IO_BUFSIZE) != 0) {
    }
    if (ctx.paths_fp && ctx.path_offsets_fp && ctx.path_isdir_fp && paths_writer_open(&ctx) != 0) {
        close_index_path_files(&ctx);
    }
    if (!ctx.paths_fp || !ctx.path_offsets_fp || !ctx.path_isdir_fp) {
        fprintf(stderr, "failed to initialize index outputs in %s\n", ctx.index_dir);
        if (stats_thread_started) {
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        close_index_path_files(&ctx);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        index_free_file_states(file_states, path_count);
        return 1;
    }

    if (parallel_bucket_io_init(&ctx, (uint32_t)trigram_threads) != 0) {
        fprintf(stderr, "ereport_index: failed to initialize parallel tmp_trigram I/O\n");
        if (stats_thread_started) {
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        close_index_path_files(&ctx);
        for (i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        for (i = 0; i < chunk_count; i++) free(chunks[i].path);
        free(chunks);
        index_free_file_states(file_states, path_count);
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
    index_stats_wake_request(&run_stats);

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
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        parallel_bucket_io_shutdown(&ctx);
        close_index_path_files(&ctx);
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
        index_free_file_states(file_states, path_count);
        return 1;
    }

    trigram_tids = (pthread_t *)calloc((size_t)trigram_threads, sizeof(*trigram_tids));
    if (!trigram_tids) {
        fprintf(stderr, "allocation failed\n");
        free(tids);
        free(args);
        if (stats_thread_started) {
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        parallel_bucket_io_shutdown(&ctx);
        close_index_path_files(&ctx);
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
        index_free_file_states(file_states, path_count);
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
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        parallel_bucket_io_shutdown(&ctx);
        close_index_path_files(&ctx);
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
        index_free_file_states(file_states, path_count);
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
                index_stats_stop_request(&run_stats);
                pthread_join(stats_thread, NULL);
                clear_status_line();
                stats_thread_started = 0;
            }
            parallel_bucket_io_shutdown(&ctx);
            close_index_path_files(&ctx);
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
            index_free_file_states(file_states, path_count);
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
            index_stats_stop_request(&run_stats);
            pthread_join(stats_thread, NULL);
            clear_status_line();
            stats_thread_started = 0;
        }
        parallel_bucket_io_shutdown(&ctx);
        close_index_path_files(&ctx);
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
        index_free_file_states(file_states, path_count);
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
        index_stats_stop_request(&run_stats);
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
        index_free_file_states(file_states, path_count);
        parallel_bucket_io_shutdown(&ctx);
        close_index_path_files(&ctx);
        return 1;
    }

    if (write_final_path_offset(&ctx) != 0 || paths_writer_close(&ctx) != 0 || flush_isdir_tail(&ctx) != 0) {
        close_index_path_files(&ctx);
        paths_writer_free(&ctx);
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
        index_free_file_states(file_states, path_count);
        parallel_bucket_io_shutdown(&ctx);
        return 1;
    }
    {
        int close_bad = 0;

        if (mk_fclose(ctx.paths_fp) != 0) close_bad = 1;
        ctx.paths_fp = NULL;
        if (mk_fclose(ctx.path_offsets_fp) != 0) close_bad = 1;
        ctx.path_offsets_fp = NULL;
        if (mk_fclose(ctx.path_isdir_fp) != 0) close_bad = 1;
        ctx.path_isdir_fp = NULL;
        if (close_bad) {
            paths_writer_free(&ctx);
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
            index_free_file_states(file_states, path_count);
            parallel_bucket_io_shutdown(&ctx);
            return 1;
        }
    }

    paths_writer_free(&ctx);

    ctx.index_phase_sec = now_sec() - t0;
    ctx.index_workers_used = threads_used;
    ctx.trigram_writer_workers_used = trigram_threads_used;

    parallel_bucket_io_shutdown(&ctx);

    if (getenv("EREPORT_INDEX_MEMSTATS")) {
        uint64_t vmrss = 0, vmhwm = 0;
        double gib = 1024.0 * 1024.0 * 1024.0;
        size_t cat_b = index_catalog_bytes(file_states, path_count);

        read_proc_self_rss_kib(&vmrss, &vmhwm);
        clear_status_line();
        fprintf(stderr,
            "\n=== ereport_index memstats (index phase done, before merge) ===\n"
            "  indexed paths   : %llu\n"
            "  trigram records : %llu\n"
            "  shard catalogs  : %10.2f GiB  (freed right after this point)\n"
            "  RSS now / peak  : %.2f / %.2f GiB\n"
            "  note: the merge phase decompresses each trigram bucket into RAM (~2x its records);\n"
            "        worker count is sized off the largest bucket's DECOMPRESSED bytes vs the RAM budget.\n"
            "================================================================\n\n",
            (unsigned long long)ctx.indexed_paths,
            (unsigned long long)ctx.trigram_records,
            (double)cat_b / gib,
            (double)vmrss * 1024.0 / gib, (double)vmhwm * 1024.0 / gib);
        fflush(stderr);
    }

    /* Catalogs back path reconstruction in the index phase only; the merge/stitch phase never touches
     * them. Free them now (index workers have joined) so their footprint is reclaimed before the
     * memory-hungry merge runs. The file_state_t array is released later by index_free_file_states. */
    index_free_shard_catalogs(file_states, path_count);

    {
        int merge_rc = process_trigram_buckets(&ctx);

        if (getrusage(RUSAGE_SELF, &ru_after_merge) == 0) ru_have_merge = 1;

        memlog_shutdown(&ml_storage, &memlog_tid, &memlog_started);

        /* Sidecars last: the trigram catalogs are gone by now, so the phase has the
         * machine to itself, and a failure here still leaves a complete index. */
        if (merge_rc == 0 && g_dir_index) {
            double t_dirx = now_sec();

            if (build_dir_index_sidecars(&ctx, paths, path_count, threads) != 0)
                fprintf(stderr, "ereport_index: dir index sidecars not written; queries fall back to the catalogs\n");
            ctx.dir_index_sec = now_sec() - t_dirx;
        }

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
            index_free_file_states(file_states, path_count);
            return 1;
        }
    }

    /* The index is complete; journals that were validated and replayed are dead weight
     * (a cache bound to a capture that is now indexed), so remove them. Stale or
     * missing journals were never consumed and stay untouched. Deletion failure is
     * advisory only. */
    if (g_journal_dir) {
        for (i = 0; i < path_count; i++) {
            char jpath[PATH_MAX];
            const char *base;

            if (!file_states[i].use_journal) continue;
            base = strrchr(paths[i], '/');
            base = base ? base + 1 : paths[i];
            if (trij_journal_path(jpath, sizeof(jpath), g_journal_dir, base, 0) != 0) continue;
            if (unlink(jpath) != 0 && errno != ENOENT) {
                fprintf(stderr, "ereport_index: warn: cannot delete consumed journal %s: %s\n", jpath,
                        strerror(errno));
                continue;
            }
            journal_shards_deleted++;
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
    if (g_journal_dir) {
        uint64_t jn = 0;
        for (i = 0; i < path_count; i++)
            if (file_states[i].use_journal) jn++;
        printf("journal_shards=%" PRIu64 "\n", jn);
        printf("journal_shards_deleted=%" PRIu64 "\n", journal_shards_deleted);
    }
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
    printf("dir_index=%d\n", ctx.dir_index_built ? 1 : 0);
    if (ctx.dir_index_built) {
        printf("dir_index_dirs=%" PRIu64 "\n", ctx.dir_index_dirs);
        printf("dir_index_bytes=%" PRIu64 "\n", ctx.dir_index_bytes);
        printf("rowgroup_index_groups=%" PRIu64 "\n", ctx.dir_index_groups);
        printf("rowgroup_index_bytes=%" PRIu64 "\n", ctx.rowgroup_index_bytes);
    }
    printf("dir_index_sec=%.3f\n", ctx.dir_index_sec);
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
    index_free_file_states(file_states, path_count);
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

/* Decode one run of delta-varints. `prev` carries across calls so a posting list split into chunks
 * decodes exactly like the same list stored as one run. */
static int decode_postings_run(const unsigned char *buf, size_t len, uint64_t *prev, u64_vec_t *vec) {
    size_t pos = 0;

    while (pos < len) {
        uint64_t delta;
        uint64_t path_id;

        if (decode_varint_u64_buf(buf, len, &pos, &delta) != 0) return -1;
        path_id = (*prev == 0) ? (delta - 1U) : (*prev + delta);
        if (u64_vec_push(vec, path_id) != 0) return -1;
        *prev = path_id;
    }
    return 0;
}

static int load_postings_list(FILE *postings_fp, const trigram_key_t *key, u64_vec_t *vec) {
    unsigned char *buf;
    unsigned char *raw = NULL;
    size_t raw_cap = 0;
    size_t span = (size_t)key->postings_bytes;
    uint64_t prev = 0;
    int rc = -1;

    memset(vec, 0, sizeof(*vec));
    if (key->postings_bytes == 0) return 0;

    buf = (unsigned char *)malloc(span);
    if (!buf) return -1;
    if (fseeko(postings_fp, (off_t)key->postings_offset, SEEK_SET) != 0 ||
        fread(buf, 1, span, postings_fp) != span) {
        free(buf);
        return -1;
    }

    if (key->reserved == POSTINGS_ENC_RAW) {
        rc = decode_postings_run(buf, span, &prev, vec);
    } else if (key->reserved == POSTINGS_ENC_CHUNKED) {
        size_t pos = 0;

        rc = 0;
        while (pos < span) {
            uint32_t raw_len, stored_len;
            unsigned char is_zstd;

            if (span - pos < (size_t)POSTINGS_CHUNK_HDR) {
                rc = -1;
                break;
            }
            memcpy(&raw_len, buf + pos, sizeof(raw_len));
            memcpy(&stored_len, buf + pos + 4, sizeof(stored_len));
            is_zstd = buf[pos + 8];
            pos += POSTINGS_CHUNK_HDR;
            if (stored_len > span - pos) {
                rc = -1;
                break;
            }
            if (!is_zstd) {
                if (stored_len != raw_len || decode_postings_run(buf + pos, raw_len, &prev, vec) != 0) {
                    rc = -1;
                    break;
                }
            } else {
                size_t dlen;

                if (raw_len > raw_cap) {
                    unsigned char *p = (unsigned char *)realloc(raw, raw_len);
                    if (!p) {
                        rc = -1;
                        break;
                    }
                    raw = p;
                    raw_cap = raw_len;
                }
                dlen = ZSTD_decompress(raw, raw_cap, buf + pos, stored_len);
                if (ZSTD_isError(dlen) || dlen != raw_len || decode_postings_run(raw, raw_len, &prev, vec) != 0) {
                    rc = -1;
                    break;
                }
            }
            pos += stored_len;
        }
    }

    free(raw);
    free(buf);
    if (rc != 0) {
        free(vec->ids);
        vec->ids = NULL;
        vec->count = vec->cap = 0;
    }
    return rc;
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

static void paths_index_close(paths_index_t *pi) {
    free(pi->table);
    memset(pi, 0, sizeof(*pi));
}

static int paths_index_open(const char *paths_path, paths_index_t *pi) {
    unsigned char hdr[PATHS_HDR_BYTES];
    FILE *fp;
    uint32_t version;
    uint64_t table_offset;
    size_t table_bytes;
    uint64_t i;

    memset(pi, 0, sizeof(*pi));
    fp = fopen(paths_path, "rb");
    if (!fp) {
        fprintf(stderr, "cannot open %s: %s\n", paths_path, strerror(errno));
        return -1;
    }
    if (fread(hdr, 1, sizeof(hdr), fp) != sizeof(hdr) || memcmp(hdr, PATHS_MAGIC, sizeof(PATHS_MAGIC)) != 0) {
        fprintf(stderr, "%s is not an EPATH002 paths file; rebuild with 'ereport_index --make'\n", paths_path);
        fclose(fp);
        return -1;
    }
    memcpy(&version, hdr + 8, sizeof(version));
    memcpy(&pi->chunk_count, hdr + 16, sizeof(pi->chunk_count));
    memcpy(&table_offset, hdr + 24, sizeof(table_offset));
    memcpy(&pi->total_logical, hdr + 32, sizeof(pi->total_logical));
    if (version != PATHS_FORMAT_VERSION) {
        fprintf(stderr, "%s has paths format version %u, expected %u; rebuild with 'ereport_index --make'\n",
                paths_path, version, PATHS_FORMAT_VERSION);
        fclose(fp);
        return -1;
    }

    if (pi->chunk_count > 0) {
        table_bytes = (size_t)pi->chunk_count * sizeof(*pi->table);
        pi->table = (paths_chunk_ent_t *)malloc(table_bytes);
        if (!pi->table || fseeko(fp, (off_t)table_offset, SEEK_SET) != 0 ||
            fread(pi->table, 1, table_bytes, fp) != table_bytes) {
            fprintf(stderr, "cannot read the paths chunk table in %s\n", paths_path);
            fclose(fp);
            paths_index_close(pi);
            return -1;
        }
        for (i = 0; i < pi->chunk_count; i++) {
            if (pi->table[i].raw_len > pi->max_raw) pi->max_raw = pi->table[i].raw_len;
            if (pi->table[i].stored_len > pi->max_stored) pi->max_stored = pi->table[i].stored_len;
        }
    }

    fclose(fp);
    return 0;
}

/* Per-thread cursor over one paths.bin. Holding the last decompressed chunk matters because postings come
 * back in path-id order, so a run of hits usually lands in the same chunk. */
typedef struct {
    FILE *fp;
    const paths_index_t *pi;
    ZSTD_DCtx *dctx;
    unsigned char *raw;
    unsigned char *stored;
    uint64_t cached_chunk; /* UINT64_MAX when nothing is cached */
} paths_reader_t;

static void paths_reader_free(paths_reader_t *pr) {
    free(pr->raw);
    free(pr->stored);
    if (pr->dctx) ZSTD_freeDCtx(pr->dctx);
    memset(pr, 0, sizeof(*pr));
}

static int paths_reader_init(paths_reader_t *pr, FILE *fp, const paths_index_t *pi) {
    memset(pr, 0, sizeof(*pr));
    pr->fp = fp;
    pr->pi = pi;
    pr->cached_chunk = UINT64_MAX;
    if (pi->chunk_count == 0) return 0;
    pr->dctx = ZSTD_createDCtx();
    pr->raw = (unsigned char *)malloc(pi->max_raw);
    pr->stored = (unsigned char *)malloc(pi->max_stored);
    if (!pr->dctx || !pr->raw || !pr->stored) {
        paths_reader_free(pr);
        return -1;
    }
    return 0;
}

/* Index of the chunk holding logical offset `off`, or chunk_count if there is none. */
static uint64_t paths_find_chunk(const paths_index_t *pi, uint64_t off) {
    uint64_t lo = 0, hi = pi->chunk_count;

    while (lo < hi) {
        uint64_t mid = lo + (hi - lo) / 2;

        if (pi->table[mid].logical_start <= off) lo = mid + 1;
        else hi = mid;
    }
    if (lo == 0) return pi->chunk_count;
    lo--;
    if (off - pi->table[lo].logical_start >= pi->table[lo].raw_len) return pi->chunk_count;
    return lo;
}

static int paths_reader_load_chunk(paths_reader_t *pr, uint64_t ci) {
    const paths_chunk_ent_t *ent = &pr->pi->table[ci];

    if (pr->cached_chunk == ci) return 0;
    pr->cached_chunk = UINT64_MAX;
    if (ent->stored_len == ent->raw_len) {
        if (fseeko(pr->fp, (off_t)ent->file_off, SEEK_SET) != 0 ||
            fread(pr->raw, 1, ent->raw_len, pr->fp) != ent->raw_len)
            return -1;
    } else {
        size_t dlen;

        if (fseeko(pr->fp, (off_t)ent->file_off, SEEK_SET) != 0 ||
            fread(pr->stored, 1, ent->stored_len, pr->fp) != ent->stored_len)
            return -1;
        dlen = ZSTD_decompressDCtx(pr->dctx, pr->raw, pr->pi->max_raw, pr->stored, ent->stored_len);
        if (ZSTD_isError(dlen) || dlen != ent->raw_len) return -1;
    }
    pr->cached_chunk = ci;
    return 0;
}

static char *read_path_by_id(paths_reader_t *pr, FILE *offsets_fp, uint64_t path_id) {
    uint64_t off0, off1, ci;
    size_t len, in_chunk;
    char *buf;

    if (read_path_offsets(offsets_fp, path_id, &off0, &off1) != 0) return NULL;
    if (off1 < off0) return NULL;
    len = (size_t)(off1 - off0);
    if (len == 0) return NULL;

    ci = paths_find_chunk(pr->pi, off0);
    if (ci >= pr->pi->chunk_count) return NULL;
    in_chunk = (size_t)(off0 - pr->pi->table[ci].logical_start);
    /* Chunks are cut on path boundaries, so a path never spans two of them. */
    if (in_chunk + len > pr->pi->table[ci].raw_len) return NULL;
    if (paths_reader_load_chunk(pr, ci) != 0) return NULL;

    buf = (char *)malloc(len);
    if (!buf) return NULL;
    memcpy(buf, pr->raw + in_chunk, len);
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

    qsort(codes, count, sizeof(*codes), trigram_cmp_u32);
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

/* Order query trigrams by posting-list size (rarest first); tie-break on trigram for a
 * deterministic intersection order. */
static int cmp_key_postings_bytes_asc(const void *a, const void *b) {
    const trigram_key_t *aa = (const trigram_key_t *)a;
    const trigram_key_t *bb = (const trigram_key_t *)b;
    if (aa->postings_bytes < bb->postings_bytes) return -1;
    if (aa->postings_bytes > bb->postings_bytes) return 1;
    if (aa->trigram < bb->trigram) return -1;
    if (aa->trigram > bb->trigram) return 1;
    return 0;
}

/*
 * Refuse an index written by an older build. v1 stored paths.bin and tri_postings.bin uncompressed and
 * there is no dual-read path, so the only fix is a rebuild.
 */
static int check_index_version(const char *index_dir) {
    char path[PATH_MAX];
    FILE *fp;
    char buf[512];
    int version = -1;

    if (build_path(path, sizeof(path), index_dir, "meta.txt") != 0) return -1;
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(buf, sizeof(buf), fp)) {
            int tmp;
            if (sscanf(buf, "ereport_index_version=%d", &tmp) == 1) {
                version = tmp;
                break;
            }
        }
        fclose(fp);
    }
    if (version == INDEX_VERSION) return 0;
    if (version < 0)
        fprintf(stderr, "no ereport_index_version in %s/meta.txt; rebuild with 'ereport_index --make'\n", index_dir);
    else
        fprintf(stderr, "index in %s is version %d, this build needs version %d; rebuild with 'ereport_index --make'\n",
                index_dir, version, INDEX_VERSION);
    return -1;
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
    SEARCH_PATH_PAR_MIN = 64,
    /* Stop intersecting once the next posting list is more than this many bytes per surviving
     * candidate: decoding it would cost more than just verifying the candidates we already have. */
    SEARCH_INTERSECT_STOP_RATIO = 64
};

static int path_is_under_any_dir(const char *path, char **dirs, size_t ndirs) {
    size_t i;

    for (i = 0; i < ndirs; i++) {
        size_t n;

        if (!dirs[i]) continue;
        n = strlen(dirs[i]);
        if (n == 0) continue;
        if (strncmp(path, dirs[i], n) == 0 && path[n] == '/') return 1;
    }
    return 0;
}

static int load_path_isdir_bits(const char *index_dir, uint64_t npaths, uint8_t **out_bits, size_t *out_bytes) {
    char path[PATH_MAX];
    FILE *fp;
    size_t need;
    uint8_t *bits;
    struct stat st;

    *out_bits = NULL;
    *out_bytes = 0;
    if (npaths == 0) return 0;
    if (build_path(path, sizeof(path), index_dir, "path_isdir.bin") != 0) return -1;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "missing path_isdir.bin in %s (rebuild with ereport_index --make)\n", index_dir);
        return -1;
    }
    need = (size_t)((npaths + 7ULL) / 8ULL);
    if ((uint64_t)st.st_size < (uint64_t)need) {
        fprintf(stderr, "path_isdir.bin in %s is truncated\n", index_dir);
        return -1;
    }
    bits = (uint8_t *)malloc(need);
    if (!bits) return -1;
    fp = fopen(path, "rb");
    if (!fp) {
        free(bits);
        return -1;
    }
    if (fread(bits, 1, need, fp) != need) {
        fclose(fp);
        free(bits);
        return -1;
    }
    fclose(fp);
    *out_bits = bits;
    *out_bytes = need;
    return 0;
}

static int path_isdir_bit(const uint8_t *bits, uint64_t path_id) {
    return (int)((bits[path_id >> 3] >> (path_id & 7ULL)) & 1U);
}

typedef struct {
    const char *paths_path;
    const char *offsets_path;
    const paths_index_t *paths_index;
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
    paths_reader_t pr;
    size_t i;

    if (a->lo >= a->hi) return NULL;
    if (atomic_load_explicit(a->err, memory_order_relaxed)) return NULL;
    paths_fp = fopen(a->paths_path, "rb");
    offsets_fp = fopen(a->offsets_path, "rb");
    if (!paths_fp || !offsets_fp || paths_reader_init(&pr, paths_fp, a->paths_index) != 0) {
        atomic_store_explicit(a->err, 1, memory_order_relaxed);
        if (paths_fp) fclose(paths_fp);
        if (offsets_fp) fclose(offsets_fp);
        return NULL;
    }

    for (i = a->lo; i < a->hi; i++) {
        char *p;

        if (atomic_load_explicit(a->err, memory_order_relaxed)) break;
        p = read_path_by_id(&pr, offsets_fp, a->ids[i]);
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
    paths_reader_free(&pr);
    fclose(paths_fp);
    fclose(offsets_fp);
    return NULL;
}

static int search_index_dir(const char *term, const char *index_dir, uint64_t skip_req, uint64_t limit_req,
                            int json_output) {
    char keys_path[PATH_MAX], postings_path[PATH_MAX], paths_path[PATH_MAX], offsets_path[PATH_MAX];
    FILE *keys_fp = NULL, *postings_fp = NULL, *paths_fp = NULL, *offsets_fp = NULL;
    paths_index_t paths_index;
    paths_reader_t serial_pr;
    struct stat st;
    struct timespec t_search_start;
    uint64_t key_count;
    uint64_t indexed_paths_corpus = 0;
    uint32_t *query_trigrams = NULL;
    size_t query_trigram_count = 0;
    trigram_key_t *qkeys = NULL;
    u64_vec_t current, next;
    char *lower_term = NULL;
    int rc = 1;
    int search_threads = parse_index_thread_count();

    memset(&paths_index, 0, sizeof(paths_index));
    memset(&serial_pr, 0, sizeof(serial_pr));

    /* Avoid full buffering when stdout is a pipe (eserve subprocess): empty reads → 502 */
    if (json_output) setvbuf(stdout, NULL, _IONBF, 0);

    if (build_path(keys_path, sizeof(keys_path), index_dir, "tri_keys.bin") != 0 ||
        build_path(postings_path, sizeof(postings_path), index_dir, "tri_postings.bin") != 0 ||
        build_path(paths_path, sizeof(paths_path), index_dir, "paths.bin") != 0 ||
        build_path(offsets_path, sizeof(offsets_path), index_dir, "path_offsets.bin") != 0) {
        return 1;
    }

    if (check_index_version(index_dir) != 0) return 1;
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

    memset(&current, 0, sizeof(current));
    memset(&next, 0, sizeof(next));

    /* collect_query_trigrams guarantees at least one trigram for any term of length >= 3. */
    if (query_trigram_count == 0) {
        if (json_output) json_print_empty_search(skip_req, limit_req, key_count, indexed_paths_corpus, &t_search_start);
        rc = 0;
        goto out;
    }

    keys_fp = fopen(keys_path, "rb");
    postings_fp = fopen(postings_path, "rb");
    if (!keys_fp || !postings_fp) {
        fprintf(stderr, "cannot open index under %s\n", index_dir);
        goto out;
    }

    /* Locate each query trigram's posting list (offset + size) without decoding it: a single
     * binary search per trigram.  Any trigram missing from the index means no path can match. */
    qkeys = (trigram_key_t *)calloc(query_trigram_count, sizeof(*qkeys));
    if (!qkeys) goto out;
    for (size_t i = 0; i < query_trigram_count; i++) {
        int found = find_trigram_key(keys_fp, key_count, query_trigrams[i], &qkeys[i]);
        if (found != 0) {
            if (found < 0) {
                rc = 1;
            } else {
                if (json_output)
                    json_print_empty_search(skip_req, limit_req, key_count, indexed_paths_corpus,
                                            &t_search_start);
                rc = 0;
            }
            goto out;
        }
    }

    /* Intersect rarest-first.  The final substring check (path_matches_term) is authoritative,
     * and every true match contains the whole term -- hence every query trigram -- so it appears
     * in the rarest list and survives all intersections.  Intersecting fewer trigrams therefore
     * only enlarges the candidate set; it never drops a match and never changes their order
     * (lists stay ascending by path_id).  So results are byte-identical no matter where we stop,
     * and we can skip decoding the pathologically common lists entirely. */
    qsort(qkeys, query_trigram_count, sizeof(*qkeys), cmp_key_postings_bytes_asc);

    if (load_postings_list(postings_fp, &qkeys[0], &current) != 0) goto out;

    for (size_t i = 1; i < query_trigram_count && current.count > 0; i++) {
        u64_vec_t list_i;

        /* Decoding the next list costs ~postings_bytes of work; verifying the surviving
         * candidates costs ~current.count path reads.  Once the next list dwarfs the candidate
         * set, verifying directly is cheaper, so stop and let path_matches_term finish the job. */
        if (qkeys[i].postings_bytes > (uint64_t)current.count * (uint64_t)SEARCH_INTERSECT_STOP_RATIO)
            break;

        if (load_postings_list(postings_fp, &qkeys[i], &list_i) != 0) {
            free(current.ids);
            current.ids = NULL;
            goto out;
        }
        if (intersect_postings(&current, &list_i, &next) != 0) {
            free(list_i.ids);
            free(current.ids);
            current.ids = NULL;
            goto out;
        }
        free(list_i.ids);
        free(current.ids);
        current = next;
        memset(&next, 0, sizeof(next));
    }

    if (keys_fp) {
        fclose(keys_fp);
        keys_fp = NULL;
    }
    if (postings_fp) {
        fclose(postings_fp);
        postings_fp = NULL;
    }
    free(qkeys);
    qkeys = NULL;

    {
        uint64_t max_emit = json_output ? limit_req : UINT64_MAX;
        uint64_t match_idx = 0;
        uint64_t page_emitted = 0;
        char **json_paths = NULL;
        size_t json_cap = 0;
        size_t ki;
        char **pref_paths = NULL;
        uint64_t *hit_ids = NULL;
        char **hit_paths = NULL;
        size_t hit_n = 0;
        size_t hit_cap = 0;
        char **dir_prefs = NULL;
        size_t dir_n = 0;
        size_t dir_cap = 0;
        uint8_t *isdir_bits = NULL;
        size_t isdir_bytes = 0; /* set by load_path_isdir_bits; unused beyond load */
        int need_expand = 0;
        uint64_t *final_ids = NULL;
        char **final_paths = NULL;
        size_t final_n = 0;
        size_t final_cap = 0;

        /* One shared header/chunk-table/dictionary load; the workers below only add a FILE* and a
         * decompression context each, so the table does not get duplicated per thread. */
        if (current.count > 0 && paths_index_open(paths_path, &paths_index) != 0) {
            free(current.ids);
            current.ids = NULL;
            goto out;
        }

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
                pargs[pw].paths_index = &paths_index;
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
            if (!paths_fp || !offsets_fp || paths_reader_init(&serial_pr, paths_fp, &paths_index) != 0) {
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
                pref_paths[i] = NULL;
            } else {
                path = read_path_by_id(&serial_pr, offsets_fp, current.ids[i]);
                if (!path) continue;
                if (!path_matches_term(path, lower_term)) {
                    free(path);
                    continue;
                }
            }
            if (!path) continue;
            if (hit_n == hit_cap) {
                size_t nc = hit_cap ? hit_cap * 2 : 64;
                uint64_t *ni;
                char **np;

                ni = (uint64_t *)realloc(hit_ids, nc * sizeof(*ni));
                if (!ni) {
                    free(path);
                    for (ki = 0; ki < hit_n; ki++) free(hit_paths[ki]);
                    free(hit_ids);
                    free(hit_paths);
                    free(pref_paths);
                    free(current.ids);
                    current.ids = NULL;
                    goto out;
                }
                hit_ids = ni;
                np = (char **)realloc(hit_paths, nc * sizeof(*np));
                if (!np) {
                    free(path);
                    for (ki = 0; ki < hit_n; ki++) free(hit_paths[ki]);
                    free(hit_ids);
                    free(hit_paths);
                    free(pref_paths);
                    free(current.ids);
                    current.ids = NULL;
                    goto out;
                }
                hit_paths = np;
                hit_cap = nc;
            }
            hit_ids[hit_n] = current.ids[i];
            hit_paths[hit_n] = path;
            hit_n++;
        }
        free(pref_paths);
        pref_paths = NULL;

        if (hit_n > 0 && indexed_paths_corpus > 0 &&
            load_path_isdir_bits(index_dir, indexed_paths_corpus, &isdir_bits, &isdir_bytes) != 0) {
            for (ki = 0; ki < hit_n; ki++) free(hit_paths[ki]);
            free(hit_ids);
            free(hit_paths);
            free(current.ids);
            current.ids = NULL;
            goto out;
        }
        (void)isdir_bytes;
        for (ki = 0; ki < hit_n; ki++) {
            if (!isdir_bits || !path_isdir_bit(isdir_bits, hit_ids[ki])) continue;
            if (dir_n == dir_cap) {
                size_t nc = dir_cap ? dir_cap * 2 : 16;
                char **np = (char **)realloc(dir_prefs, nc * sizeof(*np));

                if (!np) {
                    for (size_t j = 0; j < hit_n; j++) free(hit_paths[j]);
                    free(hit_ids);
                    free(hit_paths);
                    free(dir_prefs);
                    free(isdir_bits);
                    free(current.ids);
                    current.ids = NULL;
                    goto out;
                }
                dir_prefs = np;
                dir_cap = nc;
            }
            dir_prefs[dir_n++] = hit_paths[ki];
            need_expand = 1;
        }

        if (need_expand) {
            size_t hi = 0;
            uint64_t pid;

            paths_reader_free(&serial_pr);
            if (paths_fp) {
                fclose(paths_fp);
                paths_fp = NULL;
            }
            if (offsets_fp) {
                fclose(offsets_fp);
                offsets_fp = NULL;
            }
            paths_fp = fopen(paths_path, "rb");
            offsets_fp = fopen(offsets_path, "rb");
            if (!paths_fp || !offsets_fp || paths_reader_init(&serial_pr, paths_fp, &paths_index) != 0) {
                fprintf(stderr, "cannot open paths.bin under %s\n", index_dir);
                for (ki = 0; ki < hit_n; ki++) free(hit_paths[ki]);
                free(hit_ids);
                free(hit_paths);
                free(dir_prefs);
                free(isdir_bits);
                free(current.ids);
                current.ids = NULL;
                goto out;
            }

            for (pid = 0; pid < indexed_paths_corpus; pid++) {
                int at_hit = (hi < hit_n && hit_ids[hi] == pid);
                char *path;

                if (final_n == final_cap) {
                    size_t nc = final_cap ? final_cap * 2 : (hit_n ? hit_n * 2 : 64);
                    uint64_t *ni;
                    char **np;

                    ni = (uint64_t *)realloc(final_ids, nc * sizeof(*ni));
                    if (!ni) {
                        for (ki = 0; ki < hit_n; ki++) free(hit_paths[ki]);
                        for (ki = 0; ki < final_n; ki++) free(final_paths[ki]);
                        free(hit_ids);
                        free(hit_paths);
                        free(final_ids);
                        free(final_paths);
                        free(dir_prefs);
                        free(isdir_bits);
                        free(current.ids);
                        current.ids = NULL;
                        goto out;
                    }
                    final_ids = ni;
                    np = (char **)realloc(final_paths, nc * sizeof(*np));
                    if (!np) {
                        for (ki = 0; ki < hit_n; ki++) free(hit_paths[ki]);
                        for (ki = 0; ki < final_n; ki++) free(final_paths[ki]);
                        free(hit_ids);
                        free(hit_paths);
                        free(final_ids);
                        free(final_paths);
                        free(dir_prefs);
                        free(isdir_bits);
                        free(current.ids);
                        current.ids = NULL;
                        goto out;
                    }
                    final_paths = np;
                    final_cap = nc;
                }

                if (at_hit) {
                    final_ids[final_n] = pid;
                    final_paths[final_n] = hit_paths[hi];
                    hit_paths[hi] = NULL;
                    final_n++;
                    hi++;
                    continue;
                }

                path = read_path_by_id(&serial_pr, offsets_fp, pid);
                if (!path) continue;
                if (!path_is_under_any_dir(path, dir_prefs, dir_n)) {
                    free(path);
                    continue;
                }
                final_ids[final_n] = pid;
                final_paths[final_n] = path;
                final_n++;
            }
            for (ki = 0; ki < hit_n; ki++) free(hit_paths[ki]);
            free(hit_ids);
            free(hit_paths);
            hit_ids = final_ids;
            hit_paths = final_paths;
            hit_n = final_n;
            final_ids = NULL;
            final_paths = NULL;
        }

        free(dir_prefs);
        free(isdir_bits);
        paths_reader_free(&serial_pr);
        if (paths_fp) {
            fclose(paths_fp);
            paths_fp = NULL;
        }
        if (offsets_fp) {
            fclose(offsets_fp);
            offsets_fp = NULL;
        }

        for (ki = 0; ki < hit_n; ki++) {
            char *path = hit_paths[ki];

            if (!path) continue;
            if (match_idx >= skip_req && page_emitted < max_emit) {
                if (json_output) {
                    if (page_emitted >= json_cap) {
                        size_t nc = json_cap ? json_cap * 2 : 64;
                        char **np = (char **)realloc(json_paths, nc * sizeof(char *));
                        if (!np) {
                            free(path);
                            for (size_t j = ki + 1; j < hit_n; j++) free(hit_paths[j]);
                            for (size_t j = 0; j < page_emitted; j++) free(json_paths[j]);
                            free(json_paths);
                            free(hit_ids);
                            free(hit_paths);
                            free(current.ids);
                            current.ids = NULL;
                            goto out;
                        }
                        json_paths = np;
                        json_cap = nc;
                    }
                    json_paths[page_emitted++] = path;
                    hit_paths[ki] = NULL;
                } else {
                    printf("%s\n", path);
                    free(path);
                    hit_paths[ki] = NULL;
                    page_emitted++;
                }
            } else {
                free(path);
                hit_paths[ki] = NULL;
            }
            match_idx++;
        }
        free(hit_ids);
        free(hit_paths);

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
    free(qkeys);
    free(query_trigrams);
    free(lower_term);
    paths_reader_free(&serial_pr);
    paths_index_close(&paths_index);
    if (keys_fp) fclose(keys_fp);
    if (postings_fp) fclose(postings_fp);
    if (paths_fp) fclose(paths_fp);
    if (offsets_fp) fclose(offsets_fp);
    return rc;
}

static int run_build_index_dir_resolved(const char *user_spec,
                                        const char **dirpaths_in,
                                        size_t dirpath_count,
                                        int all_users_mode,
                                        const char *index_dir_override) {
    char *dir_blob = NULL;
    const char **dir_heap = NULL;
    char index_override_canon[PATH_MAX];
    const char *index_pass = index_dir_override;
    int rc;
    size_t i;

    if (index_dir_override && index_dir_override[0] != '\0') {
        struct stat st;

        if (stat(index_dir_override, &st) == 0) {
            if (!S_ISDIR(st.st_mode)) {
                fprintf(stderr, "ereport_index: --index-dir %s: not a directory\n", index_dir_override);
                return 2;
            }
        } else if (errno == ENOENT) {
            if (ensure_dir_recursive(index_dir_override) != 0) {
                fprintf(stderr, "ereport_index: could not create --index-dir %s: %s\n", index_dir_override,
                        strerror(errno));
                return 2;
            }
        } else {
            fprintf(stderr, "ereport_index: --index-dir %s: %s\n", index_dir_override, strerror(errno));
            return 2;
        }
        if (path_resolve_existing(index_dir_override, index_override_canon, "ereport_index: --index-dir ") != 0)
            return 2;
        index_pass = index_override_canon;
    }

    if (dirpath_count > 0) {
        dir_blob = (char *)malloc(dirpath_count * (size_t)PATH_MAX);
        dir_heap = (const char **)malloc(dirpath_count * sizeof(char *));
        if (!dir_blob || !dir_heap) {
            fprintf(stderr, "ereport_index: allocation failed\n");
            free(dir_blob);
            free(dir_heap);
            return 1;
        }
        for (i = 0; i < dirpath_count; i++) {
            char *slot = dir_blob + i * (size_t)PATH_MAX;

            if (path_resolve_existing(dirpaths_in[i], slot, "ereport_index: ") != 0) {
                free(dir_blob);
                free(dir_heap);
                return 2;
            }
            dir_heap[i] = slot;
        }
        dirpaths_in = dir_heap;
    }

    rc = build_index_dir(user_spec, dirpaths_in, dirpath_count, all_users_mode, index_pass);
    free(dir_blob);
    free(dir_heap);
    return rc;
}

int main(int argc, char **argv) {
    int vi;
    int cmd0;

    tune_allocator();
    crawl_fpcache_set_bufsz(MERGE_IO_BUFSIZE);

    if (argc < 2) die_usage(argv[0]);
    for (vi = 1; vi < argc; vi++) {
        if (arg_is_verbose(argv[vi])) g_verbose = 1;
    }

    ereport_index_install_verbose_io();

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

        /* Consume leading options (--index-dir, --subtree) in any order, skipping verbose tokens. */
        while (ai < argc) {
            if (arg_is_verbose(argv[ai])) {
                ai++;
                continue;
            }
            if (strcmp(argv[ai], "--index-dir") == 0) {
                if (ai + 2 > argc) {
                    fprintf(stderr, "ereport_index: --index-dir requires a path\n");
                    return 2;
                }
                index_dir_override = argv[ai + 1];
                ai += 2;
                continue;
            }
            if (strcmp(argv[ai], "--subtree") == 0) {
                if (ai + 2 > argc) {
                    fprintf(stderr, "ereport_index: --subtree requires an absolute directory path\n");
                    return 2;
                }
                if (set_subtree_prefix(argv[ai + 1]) != 0) return 2;
                ai += 2;
                continue;
            }
            if (strcmp(argv[ai], "--path-rewrite") == 0) {
                if (ai + 2 > argc) {
                    fprintf(stderr, "ereport_index: --path-rewrite requires OLD=NEW\n");
                    return 2;
                }
                if (set_path_rewrite(argv[ai + 1]) != 0) return 2;
                ai += 2;
                continue;
            }
            if (strcmp(argv[ai], "--no-dir-index") == 0) {
                g_dir_index = 0;
                ai++;
                continue;
            }
            if (strcmp(argv[ai], "--journal-dir") == 0) {
                if (ai + 2 > argc || argv[ai + 1][0] == '\0') {
                    fprintf(stderr, "ereport_index: --journal-dir requires a path\n");
                    return 2;
                }
                g_journal_dir = argv[ai + 1];
                ai += 2;
                continue;
            }
            break;
        }

        if (argc == ai) {
            static const char *dot = ".";
            all_users_mode = 1;
            dirpaths = &dot;
            dirpath_count = 1;
            return run_build_index_dir_resolved(NULL, dirpaths, dirpath_count, all_users_mode, index_dir_override);
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
        return run_build_index_dir_resolved(user_spec, dirpaths, dirpath_count, all_users_mode, index_dir_override);
    }

    if (strcmp(argv[cmd0], "--search") == 0) {
        const char *index_dir = "index";
        char index_dir_buf[PATH_MAX];
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

        if (path_resolve_existing(index_dir, index_dir_buf, "ereport_index: ") != 0) return 2;
        index_dir = index_dir_buf;

        return search_index_dir(term, index_dir, skip_req, limit_req, json_output);
    }

    die_usage(argv[0]);
    return 2;
}
