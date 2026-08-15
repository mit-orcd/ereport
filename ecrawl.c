/*
 * ecrawl.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Compact binary-output local filesystem metadata crawler.
 *
 * Features:
 *   - Main thread seeds the crawl with the root path only.
 *   - Worker threads consume queued batches of directory work.
 *   - Workers traverse directories iteratively with a local stack.
 *   - Workers may donate batches of accumulated subdirectories back to the global queue.
 *   - No cross-thread stat pool: every non-directory name is fstatat'd on the crawl thread that read it
 *     (inline). Trusted DT_DIR children are fstatat'd once under the parent dirfd (st cached on the work
 *     item so pop skips lstat), pushed on the crawl thread's local stack, and spilled to the global queue
 *     when the stack grows large or idle workers need work. If fstatat finds a directory where d_type
 *     said otherwise, ecrawl warns and does not crawl it.
 *   - Dedicated writer threads consume buffered batches and write uid-sharded output; each batch is sorted by
 *     uid shard before writing so interleaved path order does not thrash the per-writer shard LRU (fopen/fclose).
 *   - Writer threads pause shard writes when output filesystem free space falls below 10 GiB
 *     (checked every 30 seconds via statvfs); crawl workers keep running until writer queues fill.
 *   - Live progress is opt-in (--progress). Each worker counts dirents across directories (not
 *     per readdir) and publishes TLS file/entry/byte totals every DEFAULT_DONATE_ENTRY_CHECK_EVERY
 *     names; a coalesced line is printed on that path (stderr, or stdout with --count). Without
 *     --progress there is no stats thread and no GLOBAL_PERF_FLUSH_EVERY atomic multipack.
 *   - Each run clears prior crawl outputs in the chosen output-dir: uid_shard_*.bin, matching *.bin.ckpt,
 *     and crawl_manifest.txt (uid.txt/gid.txt are reopened truncated). An interrupted crawl has nothing to
 *     resume across runs; only in-process shard reopen (LRU) reloads checkpoints for shards written this run.
 *
 * Build:
 *   gcc -O2 -Wall -Wextra -pthread -o ecrawl ecrawl.c
 *
 * Usage:
 *   ./ecrawl [--no-write] [--progress] [--verbose] [--record-root <abs-path>] <start-path> [output-dir]
 *   --progress: cheap live files=/entries= (dirent cadence, across directories).
 *   --verbose: full metrics to stdout at exit.
 * Threading / shard layout (optional env): ECRAWL_CRAWL_THREADS,
 * ECRAWL_WRITER_THREADS, ECRAWL_WRITER_QUEUE_BATCHES, ECRAWL_UID_SHARDS
 *
 * Diagnostics (optional env, require --verbose):
 * ECRAWL_STALL_HINT_SECONDS=N (default 5; 0 disables): after the rolling window is warm, emit one stderr line if
 * window_entries stays at 0 for N consecutive seconds (throttled until the window goes non-zero again).
 */

#define _GNU_SOURCE /* statx() declaration in <sys/stat.h> (glibc >= 2.28) */
#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <stdatomic.h>
#include <dirent.h>
#include <limits.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <stdarg.h>

#include "alloc_tuning.h"
#include "crawl_bin_block.h"
#include "crawl_bin_catalog.h"
#include "crawl_ckpt.h"
#include "crawl_trijournal.h"
#include "ecrawl_trijournal.h"
#include "ecrawl_wire.h"
#include "path_canon.h"
#include "path_utils.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/*
 * glibc only exposes dirent.d_type and DT_* when _DIRENT_HAVE_D_TYPE is set; the #else branch in the
 * walker still compares to DT_DIR. Define Linux values when headers omit them (e.g. some RHEL 7 builds).
 */
#ifndef DT_UNKNOWN
#define DT_UNKNOWN 0
#endif
#ifndef DT_DIR
#define DT_DIR 4
#endif
#ifndef DT_REG
#define DT_REG 8
#endif
#ifndef DT_LNK
#define DT_LNK 10
#endif
#ifndef DT_FIFO
#define DT_FIFO 1
#endif
#ifndef DT_SOCK
#define DT_SOCK 12
#endif
#ifndef DT_CHR
#define DT_CHR 2
#endif
#ifndef DT_BLK
#define DT_BLK 6
#endif
#ifndef DT_WHT
#define DT_WHT 14
#endif

/* --iouring batches per-entry inode reads through a per-worker io_uring. Raw syscalls, no liburing
 * dependency; compiled out when the header or the arch's syscall numbers are missing. */
#if defined(__has_include)
#  if __has_include(<linux/io_uring.h>)
#    include <linux/io_uring.h>
#    define ECRAWL_HAVE_IOURING 1
#  endif
#endif
#if defined(ECRAWL_HAVE_IOURING) && !defined(__NR_io_uring_setup)
#  if defined(__x86_64__)
#    define __NR_io_uring_setup 425
#    define __NR_io_uring_enter 426
#  elif defined(__aarch64__)
#    define __NR_io_uring_setup 277
#    define __NR_io_uring_enter 278
#  else
#    undef ECRAWL_HAVE_IOURING
#  endif
#endif

#define DEFAULT_CRAWL_THREADS 16
#define DEFAULT_WRITER_THREADS 8
#define DEFAULT_UID_SHARDS 512U
/* Per-writer open-shard LRU target. 64 = ceil(DEFAULT_UID_SHARDS / DEFAULT_WRITER_THREADS), i.e. a writer can
 * hold every shard it owns open at once, eliminating LRU thrash (fopen/fclose churn) on many-UID workloads.
 * Always auto-capped against RLIMIT_NOFILE and the actual per-writer shard count in configure_max_open_shards().
 * 512 shards fit a 1024-fd ulimit with the default 16 crawl / 8 writer threads (64 open per writer). */
#define DEFAULT_MAX_OPEN_SHARDS 64U
#define DEFAULT_WRITER_QUEUE_BATCHES 64U
/* Crawl-time trigram journal pool (--trigram-journal): extraction threads and the bounded
 * handoff queue depth in record batches. Env: ECRAWL_TRIGRAM_JOURNAL_THREADS /
 * ECRAWL_TRIGRAM_JOURNAL_QUEUE_BATCHES. */
#define DEFAULT_TRIJOURNAL_THREADS 4
#define DEFAULT_TRIJOURNAL_QUEUE_BATCHES 64U
#define FD_RESERVE_BASE 128U
#define FD_RESERVE_PER_CRAWL_THREAD 4U
#define FD_RESERVE_PER_WRITER 4U
#define EMFILE_RETRY_LIMIT 8U
#define EMFILE_RETRY_USEC 50000U
#define RECORD_BATCH_BYTES (1U << 20)
#define WRITE_BUFFER_SIZE (1U << 20)
#define WINDOW_SECONDS 10
/* Per-crawl-thread buffer (bytes) for raw getdents64 directory reads. A larger buffer returns more
 * dirents per getdents64 syscall, cutting syscall count (and its mitigation/seccomp/trace overhead)
 * on large directories. 0 = fall back to libc opendir/readdir. Env: ECRAWL_GETDENTS_BUF. */
#define DEFAULT_GETDENTS_BUF (1024U * 1024U)
#define MIN_GETDENTS_BUF 4096U
#define MAX_GETDENTS_BUF (64U * 1024U * 1024U)

/* Fold thread-local perf into the global progress counters every N accounted entries so TTY obj/s and totals
 * stay live without an atomic RMW per entry. A batch is also flushed when the rolling window rolls, so the
 * displayed numbers are never more than one stats tick behind whatever N is. */
#define GLOBAL_PERF_FLUSH_EVERY 8192U
/* Workers resolving distinct uids/gids to names at finalize. The work is one blocking NSS call per
 * distinct id, so the useful width is set by round-trip latency, not by core count. */
#define DEFAULT_ID_RESOLVE_THREADS 16
#define DISK_SPACE_CHECK_INTERVAL_SEC 30
#define DISK_MIN_FREE_BYTES ((uint64_t)(10ULL * 1024 * 1024 * 1024))

#define LOCAL_STACK_DONATE_FLOOR 8
/* When a single thread holds many pending dirs (e.g. megafanout), spill to the global queue even if every
 * crawl worker is busy, so stack realloc stays bounded and peers can take tasks as they free up. */
#define LOCAL_STACK_FORCE_DONATE_COUNT 4096U
#define DONATE_CHUNK_MIN 4
#define DONATE_CHUNK_MAX 128
#define DONATE_CHUNK_FORCE_MAX 2048U
/* During readdir, check whether to donate local stack every N DT_DIR pushes (not every push). */
#define DEFAULT_DONATE_CHECK_EVERY 64U
/* And every N dirents, regardless of how many of them were directories: the check above is driven by
 * subdirectories, so it never fires inside a deep narrow chain (see donate_spill_on_entries). */
#define DEFAULT_DONATE_ENTRY_CHECK_EVERY 4096U
#define DONATE_QUEUE_TARGET_PER_IDLE 4
/* When every crawl thread holds a popped task (active == started), legacy policy refused all proactive
 * donation until force_donate_at — uneven local stacks could not rebalance. Spill anyway if the local
 * stack is this deep and the global queue is not already deep (per-thread qdepth cap below). */
#define DEFAULT_DONATE_ALL_BUSY_MIN_STACK 64U
#define DEFAULT_DONATE_ALL_BUSY_MAX_QDEPTH_MULT 4U
#define DEFAULT_DISCOVERED_DIR_ENQUEUE_BATCH 48U
#define TASK_NODE_FREE_MAX 65536U
#define HARDLINK_REGISTRY_SHARDS 256U

/*
 * POSIX and Linux document st_blocks in 512-byte units (not necessarily the volume's
 * physical sector size). Crawl totals convert with this constant: allocated_bytes = st_blocks * unit.
 */
#define ST_BLOCKS_BYTES_UNIT 512U

typedef struct task_node task_node_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    task_node_t *head;
    task_node_t *tail;
    task_node_t *node_free;
    size_t node_free_count;
    int closed;
    uint64_t queued_tasks;
} task_queue_t;

typedef struct {
    uint64_t total_entries;
    uint64_t total_dirs;
    uint64_t total_files;
    uint64_t total_hardlink_files;
    uint64_t total_symlinks;
    uint64_t total_other;
    uint64_t total_bytes;
    uint64_t total_allocated_bytes;
    uint64_t files_sparse_heuristic;
    uint64_t dir_apparent_bytes;
    uint64_t symlink_apparent_bytes;
    uint64_t other_apparent_bytes;
} crawl_stats_t;

typedef struct {
    uint64_t entries;
    uint64_t files;
    uint64_t dirs;
    uint64_t bytes;
    uint64_t allocated_bytes;
    uint64_t files_sparse_heuristic;
    /* Rolling-window bucket these pending counts belong to, sampled when the batch opened. Folding them
     * into whatever bucket happens to be current at flush time would credit them to a later second. */
    int bucket_idx;
} perf_local_t;

typedef struct {
    uint64_t donated_dirs;
    uint64_t donation_attempts;
    uint64_t donation_successes;
} worker_aux_stats_t;

/* Per-thread streamed output for --no-stat. Millions of paths from every crawl thread would
 * serialize on stdio's lock one line at a time, so each worker fills its own buffer and hands it
 * to write() in one piece; the mutex is taken once per full buffer rather than once per path. */
#define PATHOUT_BUF_BYTES (256u * 1024u)

typedef struct {
    char *buf;
    size_t len;
    size_t cap;
    /* Scratch for the windowed --contains test: lowercased parent tail + '/' + lowercased name. */
    char *win;
    size_t win_cap;
} nostat_ctx_t;

/* Per-directory --contains state, recomputed once on each stack pop. */
typedef struct {
    /* 1: this directory or an ancestor already contains the needle, so every entry at or below
     * it matches and no per-entry compare is needed. */
    int all_match;
    size_t prefix_len; /* bytes of ctx->win already holding the parent tail + '/' */
} dirmatch_t;

typedef struct {
    pthread_mutex_t stats_mutex;
    uint64_t total_entries;
    uint64_t total_dirs;
    uint64_t total_files;
    uint64_t total_hardlink_files;
    uint64_t total_symlinks;
    uint64_t total_other;
    uint64_t total_errors;
    uint64_t total_bytes;
    uint64_t total_allocated_bytes;
    uint64_t files_sparse_heuristic;
    uint64_t dir_apparent_bytes;
    uint64_t symlink_apparent_bytes;
    uint64_t other_apparent_bytes;
    uint64_t crawl_threads_started;
    uint64_t split_dirs_enqueued;
    uint64_t donated_dirs;
    uint64_t donation_attempts;
    uint64_t donation_successes;
} shared_state_t;

/* batch_frame_hdr_t and record_batch_t live in ecrawl_wire.h, shared with the
 * trigram journal pool (ecrawl_trijournal.c). */

typedef struct {
    char *path;
    size_t path_len;
    struct stat st;
    int have_stat;
    /* 1: account_entry_local + emit_record already ran for this directory (trusted DT_DIR discovery). */
    int pre_accounted_emit;
    /* --no-stat + --contains: CONTAINS_* for this directory's own path, decided by the parent's
     * windowed test so the child never rescans its full path. */
    int ancestor_matched;
} dir_work_t;

/* Whether a pushed directory's own path contains the --contains needle. The parent resolves this
 * for every child it pushes; only the crawl root arrives untested. */
#define CONTAINS_UNKNOWN (-1)
#define CONTAINS_NO 0
#define CONTAINS_YES 1

typedef struct {
    dir_work_t *items;
    size_t count;
    size_t cap;
} dir_stack_t;

#define ID_SLOT_EMPTY 0xFFFFFFFFu
/* Fills out with the id's account/group name, or with a placeholder when it cannot be resolved. */
typedef void (*id_name_resolver_fn)(uint32_t id, char *out, size_t out_sz);
typedef struct {
    uint32_t *slots;      /* open-addressing hash set; ID_SLOT_EMPTY marks empty */
    size_t cap;           /* power of two, 0 until first insert */
    size_t count;         /* number of distinct ids stored */
    int has_sentinel;     /* whether the literal ID_SLOT_EMPTY id value was inserted */
    pthread_mutex_t mutex;
    FILE *fp;
    id_name_resolver_fn resolve; /* run once per distinct id at finalize, never during the crawl */
    int finalized;               /* names already resolved and written; destroy must not repeat it */
    char path[PATH_MAX];
} id_registry_t;

typedef struct {
    uint64_t dev;
    uint64_t ino;
    unsigned char used;
} inode_entry_t;

typedef struct {
    pthread_mutex_t mutex;
    inode_entry_t *items;
    size_t count;
    size_t cap;
} inode_registry_shard_t;

typedef struct {
    inode_registry_shard_t shards[HARDLINK_REGISTRY_SHARDS];
} inode_registry_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond_nonempty;
    pthread_cond_t cond_nonfull;
    record_batch_t *head;
    record_batch_t *tail;
    size_t count;
    size_t max_batches;
    int closed;
} writer_queue_t;

typedef struct {
    unsigned char *data;
    size_t len;
    size_t cap;
} pending_batch_t;

/* One context per crawl thread. pending_locks[i] guards pending[i] only, so concurrent emitters
 * that hash to different writers never wait on each other, and the queue push happens after the
 * lock is dropped. stats_lock guards the owner's crawl_stats_t / perf_local_t (progress publish). */
typedef struct {
    writer_queue_t *writer_queues;
    int writer_threads;
    pending_batch_t *pending;
    pthread_mutex_t *pending_locks;
    int pending_locks_n;
    pthread_mutex_t *stats_lock;
    perf_local_t *perf;
} emit_context_t;

typedef struct {
    shared_state_t *shared;
    task_queue_t *queue;
    writer_queue_t *writer_queues;
    int writer_threads;
    uint64_t worker_index;
    crawl_stats_t stats;
    perf_local_t perf;
    worker_aux_stats_t aux;
    nostat_ctx_t nostat;
    pthread_mutex_t emit_stats_lock;
    size_t progress_since; /* accounted entries at last --progress publish; lives across directories */
} worker_arg_t;

/* Coalesce global-queue subdirectory tasks into fewer queue pushes (see discovered_dir_batch_*). */
typedef struct {
    task_queue_t *queue;
    shared_state_t *shared;
    dir_stack_t pending;
} discovered_dir_batch_t;

struct task_node {
    dir_work_t *items;
    size_t count;
    size_t cap;
    struct task_node *next;
};

typedef struct {
    writer_queue_t *queue;
    uint32_t writer_index;
} writer_arg_t;

typedef struct shard_cat_path_entry {
    char *path_key;
    uint64_t dir_id;
    uint32_t hash;                         /* full (unmasked) path hash; cached for cheap rehash on grow */
    struct shard_cat_path_entry *next;     /* per-bucket chain */
    struct shard_cat_path_entry *all_next; /* global insertion chain (for O(entries) destroy / rehash) */
} shard_cat_path_entry_t;

/* Initial bucket count (power of two). The table is heap-allocated per shard and doubles on load
 * factor, so shards holding a handful of dirs stay tiny instead of carrying a fixed 512 KiB array. */
#define SHARD_CAT_HT_INIT_BUCKETS 64U

typedef struct {
    shard_cat_path_entry_t **ht; /* heap bucket array; NULL until first insert */
    size_t ht_mask;              /* bucket count - 1 (bucket count is a power of two) */
    size_t ht_count;             /* number of entries, for load-factor growth */
    shard_cat_path_entry_t *all_head; /* head of the global entry chain threaded through all buckets */
    uint64_t next_dir_id;
    uint64_t *parent_dir_id;
    uint32_t *depth;
    uint16_t *name_len;
    char **name_comp;
    /* Per-dir_id rollups over immediate child records (records whose on-disk
     * parent_dir_id == dir_id). Updated by the writer thread when each record's
     * disk parent_dir_id is resolved. min_eff_time defaults to UINT64_MAX. */
    uint64_t *imm_child_bytes;
    uint64_t *imm_child_count;
    uint64_t *imm_child_ctime_led_count;
    uint64_t *imm_child_min_eff_time;
    uint64_t *imm_child_max_eff_time;
    /*
     * Hardlink-ambiguity counter. During the crawl this holds the number of
     * *immediate* child records with nlink > 1; the end-of-crawl post-pass turns
     * it into the subtree total in place. It is serialized in both states, which
     * is what lets an LRU-evicted shard resume accumulating after reload -- the
     * post-pass only ever runs once, on final close.
     */
    uint64_t *subtree_nlink_gt1_count;
    /* Same immediate-then-subtree treatment: per-type record counts, so a
     * subtree query can report its files/dirs/symlinks breakdown without a scan. */
    uint64_t *subtree_files;
    uint64_t *subtree_dirs;
    uint64_t *subtree_symlinks;
    /* Byte credit of this directory's *own* record, recorded when that record is
     * written. The subtree sums deliberately exclude it (it belongs to the
     * parent's immediate children), so a query on this directory adds it back. */
    uint64_t *self_bytes;
    unsigned char *self_present; /* this shard holds the directory's own record */
    /*
     * Allocated only by shard_cat_finalize, so the crawl does not carry 32 bytes
     * per directory it will not read until shutdown.
     */
    uint64_t *dfs_index;
    uint64_t *dfs_subtree_dirs;
    uint64_t *subtree_bytes;
    uint64_t *subtree_count;
    unsigned char finalized;
    size_t arr_cap;
} shard_cat_t;

typedef struct {
    FILE *fp;
    uint64_t bytes_written;
    uint64_t last_used;
    unsigned char initialized;
    /* When set, `cat`/`ckpt_*` hold this shard's live in-memory catalog and are kept across LRU
     * eviction (fp closed, state retained). Reopen then skips the on-disk catalog round-trip and the
     * catalog destroy/rebuild that otherwise dominate many-shard writer workloads. */
    unsigned char cat_live;
    uint64_t *ckpt_offs;
    size_t ckpt_n;
    size_t ckpt_cap;
    uint64_t seg_start_byte;
    shard_cat_t cat;
    /* Pending records are buffered here and flushed as zstd blocks. The
     * buffer is reset (raw_len=0) per open; partial blocks are flushed before
     * the catalog is written and on LRU eviction, so on-disk blocks are always
     * complete. */
    crawl_bin_block_writer_t blk;
    unsigned char blk_inited;
} shard_file_state_t;

/* Rolling 10-second stats */
static atomic_ullong g_total_entries = 0;
static atomic_ullong g_total_files   = 0;
static atomic_ullong g_total_dirs    = 0;
static atomic_ullong g_total_bytes   = 0;
static atomic_ullong g_total_allocated_bytes = 0;
static atomic_ullong g_files_sparse_heuristic = 0;

static atomic_ullong g_window_entries = 0;
static atomic_ullong g_window_files   = 0;
static atomic_ullong g_window_dirs    = 0;

static atomic_ullong g_bucket_entries[WINDOW_SECONDS];
static atomic_ullong g_bucket_files[WINDOW_SECONDS];
static atomic_ullong g_bucket_dirs[WINDOW_SECONDS];
/* Rolling window for stat(2)/lstat(2)/fstatat stat-like metadata reads (per-second deltas). */
static atomic_ullong g_bucket_stat_meta[WINDOW_SECONDS];
static atomic_ullong g_window_stat_meta = 0;

static atomic_int  g_bucket_index = 0;
static atomic_int  g_stop_stats   = 0;
static atomic_uint g_seconds_seen = 0;

static double g_ops_rate_sum = 0.0;
static double g_ops_rate_min = 0.0;
static double g_ops_rate_max = 0.0;
static uint64_t g_ops_rate_samples = 0;
static uint64_t g_active_workers_sum = 0;
static int g_active_workers_min = 0;
static int g_active_workers_max = 0;
static uint64_t g_active_workers_samples = 0;
static uint64_t g_seconds_single_worker = 0;
static uint64_t g_seconds_queue_empty_single_worker = 0;
static double g_run_start_sec = 0.0;
static time_t g_crawl_wall_clock_start;

static unsigned long long g_progress_prev_tot_entries = 0;
static unsigned long long g_progress_prev_readdir = 0;
static unsigned long long g_progress_prev_lstat = 0;
static unsigned long long g_progress_prev_stat = 0;
static int g_stall_hint_seconds_cfg = 5;

static double now_sec(void);
static void dir_stack_destroy(dir_stack_t *s);
static int dir_stack_init(dir_stack_t *s);
static int dir_stack_push_take(dir_stack_t *s, char *path_owned, size_t path_len, const struct stat *st,
                               int pre_accounted_emit, int ancestor_matched);
static void stats_add_error(shared_state_t *s);
static void perf_flush_local(perf_local_t *perf);
static int build_default_output_dir(char *out, size_t out_sz);
static void discovered_dir_batch_init(discovered_dir_batch_t *b, task_queue_t *queue, shared_state_t *shared);
static int discovered_dir_batch_flush(discovered_dir_batch_t *b);
static void discovered_dir_batch_fini(discovered_dir_batch_t *b);
static int discovered_dir_batch_push(discovered_dir_batch_t *b, char *path_owned, size_t path_len,
                                     const struct stat *st_opt, int pre_accounted_emit, int ancestor_matched);

/* Live visibility */
static atomic_ullong g_queue_depth         = 0;
static atomic_int    g_active_workers      = 0;
static atomic_int    g_main_done           = 0;
static atomic_ullong g_tasks_popped        = 0;
static atomic_ullong g_writer_queue_depth  = 0;
static atomic_ullong g_batches_enqueued    = 0;
static atomic_ullong g_batches_dequeued    = 0;

/* pthread_cond_wait wakeups (cheap relaxed atomics); high counts suggest queue starvation. */
static atomic_ullong g_wait_crawl_tasks = 0;
static atomic_ullong g_wait_writer_push = 0;
static atomic_ullong g_wait_writer_pop  = 0;
static atomic_ullong g_task_queue_pushes    = 0;
static atomic_ullong g_queue_lock_waits     = 0;
static atomic_ullong g_donate_calls         = 0;
static atomic_ullong g_writer_queue_wait_ns = 0;

/* Wall time slept, not wakeup counts: the counters above cannot distinguish a thread that woke a million
 * times from one that lost half the run to a single wait. Summed across a pool's threads, so the ceiling
 * is nthreads * elapsed and the ratio is that pool's idle fraction (see print_verbose_full_stats).
 *   crawl_idle_ns       - crawl workers with no task to pop (pool wider than the tree can feed) */
static atomic_ullong g_crawl_idle_ns          = 0;

static atomic_ullong g_io_lstat_calls      = 0;
static atomic_ullong g_io_stat_calls       = 0;
static atomic_ullong g_io_mkdir_calls      = 0;
static atomic_ullong g_io_opendir_calls    = 0;
static atomic_ullong g_io_readdir_calls    = 0;
static atomic_ullong g_io_getdents_calls   = 0;
static atomic_ullong g_io_closedir_calls   = 0;
static atomic_ullong g_io_fopen_calls      = 0;
static atomic_ullong g_io_fclose_calls     = 0;
static atomic_ullong g_io_fwrite_calls     = 0;
static atomic_ullong g_io_fflush_calls     = 0;
/* Break io_fopen_calls down by cause. Only the shard bins are stdio; the .ckpt sidecars are written
 * with raw fds (see shard_ckpt_write_sidecar), so a healthy write run opens ~1 stream per non-empty
 * shard and anything above that is LRU churn, which these counters attribute instead of leaving it to
 * be guessed at from totals. */
static atomic_ullong g_shard_bin_opens     = 0; /* first open of a shard bin (header written) */
static atomic_ullong g_shard_bin_reopens   = 0; /* reopen of a shard evicted earlier */
static atomic_ullong g_shard_evictions     = 0; /* closes forced by max_open_shards / fd pressure */
static atomic_ullong g_shard_ckpt_writes   = 0; /* .ckpt sidecar writes (one open()/write() each) */

#define ATOMIC_ADD_RELAXED(obj, value) atomic_fetch_add_explicit((obj), (value), memory_order_relaxed)
#define ATOMIC_SUB_RELAXED(obj, value) atomic_fetch_sub_explicit((obj), (value), memory_order_relaxed)
#define ATOMIC_LOAD_RELAXED(obj) atomic_load_explicit((obj), memory_order_relaxed)

/* Batch frequent relaxed counter updates on hot paths; flush remainders on thread exit. */
#define TLS_WAIT_COUNTER_BATCH 256U

static _Thread_local uint32_t tls_wait_crawl_pending;
static _Thread_local uint32_t tls_wait_writer_push_pending;
static _Thread_local uint32_t tls_wait_writer_pop_pending;
static _Thread_local uint32_t tls_donate_calls_pending;
static _Thread_local uint64_t tls_writer_queue_wait_ns_pending;
static _Thread_local uint64_t tls_crawl_idle_ns_pending;

static uint64_t monotonic_ns(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void tls_donate_calls_inc(void) {
    tls_donate_calls_pending++;
    if (tls_donate_calls_pending >= TLS_WAIT_COUNTER_BATCH) {
        atomic_fetch_add_explicit(&g_donate_calls, (unsigned long long)TLS_WAIT_COUNTER_BATCH, memory_order_relaxed);
        tls_donate_calls_pending -= TLS_WAIT_COUNTER_BATCH;
    }
}

/* Nanosecond accumulators flush per millisecond rather than per N events: a wait is already long enough
 * that the atomic is free, but a pool of idle threads would still contend on one cache line every wakeup. */
static void tls_wait_ns_add(uint64_t *pending, atomic_ullong *total, uint64_t ns) {
    *pending += ns;
    if (*pending >= 1000000ULL) {
        atomic_fetch_add_explicit(total, *pending, memory_order_relaxed);
        *pending = 0;
    }
}

static void tls_writer_queue_wait_add_ns(uint64_t ns) {
    tls_wait_ns_add(&tls_writer_queue_wait_ns_pending, &g_writer_queue_wait_ns, ns);
}

static void tls_crawl_idle_add_ns(uint64_t ns) {
    tls_wait_ns_add(&tls_crawl_idle_ns_pending, &g_crawl_idle_ns, ns);
}

static void tls_wait_crawl_inc(void) {
    tls_wait_crawl_pending++;
    if (tls_wait_crawl_pending >= TLS_WAIT_COUNTER_BATCH) {
        atomic_fetch_add_explicit(&g_wait_crawl_tasks, (unsigned long long)TLS_WAIT_COUNTER_BATCH,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&g_queue_lock_waits, (unsigned long long)TLS_WAIT_COUNTER_BATCH,
                                  memory_order_relaxed);
        tls_wait_crawl_pending -= TLS_WAIT_COUNTER_BATCH;
    }
}

static void tls_wait_writer_push_inc(void) {
    tls_wait_writer_push_pending++;
    if (tls_wait_writer_push_pending >= TLS_WAIT_COUNTER_BATCH) {
        atomic_fetch_add_explicit(&g_wait_writer_push, (unsigned long long)TLS_WAIT_COUNTER_BATCH,
                                  memory_order_relaxed);
        tls_wait_writer_push_pending -= TLS_WAIT_COUNTER_BATCH;
    }
}

static void tls_wait_writer_pop_inc(void) {
    tls_wait_writer_pop_pending++;
    if (tls_wait_writer_pop_pending >= TLS_WAIT_COUNTER_BATCH) {
        atomic_fetch_add_explicit(&g_wait_writer_pop, (unsigned long long)TLS_WAIT_COUNTER_BATCH,
                                  memory_order_relaxed);
        tls_wait_writer_pop_pending -= TLS_WAIT_COUNTER_BATCH;
    }
}

static void tls_flush_thread_batch_counters(void) {
    if (tls_wait_crawl_pending) {
        atomic_fetch_add_explicit(&g_wait_crawl_tasks, (unsigned long long)tls_wait_crawl_pending,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&g_queue_lock_waits, (unsigned long long)tls_wait_crawl_pending,
                                  memory_order_relaxed);
        tls_wait_crawl_pending = 0;
    }
    if (tls_wait_writer_push_pending) {
        atomic_fetch_add_explicit(&g_wait_writer_push, (unsigned long long)tls_wait_writer_push_pending,
                                  memory_order_relaxed);
        tls_wait_writer_push_pending = 0;
    }
    if (tls_wait_writer_pop_pending) {
        atomic_fetch_add_explicit(&g_wait_writer_pop, (unsigned long long)tls_wait_writer_pop_pending,
                                  memory_order_relaxed);
        tls_wait_writer_pop_pending = 0;
    }
    if (tls_donate_calls_pending) {
        atomic_fetch_add_explicit(&g_donate_calls, (unsigned long long)tls_donate_calls_pending, memory_order_relaxed);
        tls_donate_calls_pending = 0;
    }
    if (tls_writer_queue_wait_ns_pending) {
        atomic_fetch_add_explicit(&g_writer_queue_wait_ns, tls_writer_queue_wait_ns_pending, memory_order_relaxed);
        tls_writer_queue_wait_ns_pending = 0;
    }
    if (tls_crawl_idle_ns_pending) {
        atomic_fetch_add_explicit(&g_crawl_idle_ns, tls_crawl_idle_ns_pending, memory_order_relaxed);
        tls_crawl_idle_ns_pending = 0;
    }
}

static int g_split_depth = 2;
static int g_writer_threads = DEFAULT_WRITER_THREADS;
static uint32_t g_uid_shards = DEFAULT_UID_SHARDS;
static unsigned g_max_open_shards = DEFAULT_MAX_OPEN_SHARDS;
static unsigned g_requested_max_open_shards = DEFAULT_MAX_OPEN_SHARDS;
static int g_max_open_shards_explicit = 0; /* set when ECRAWL_MAX_OPEN_SHARDS asked for a value */
static unsigned g_writer_queue_batches = DEFAULT_WRITER_QUEUE_BATCHES;
static int g_shard_digits = 4;
static int g_no_write = 0;
/* --trigram-journal DIR: tee writer-bound record batches to a pool writing per-shard .tij
 * journals for `ereport_index --make --journal-dir`. Off by default; needs the writer stream. */
static char g_trijournal_dir[PATH_MAX];
static int g_trijournal_enabled = 0;
static int g_trijournal_threads = DEFAULT_TRIJOURNAL_THREADS;
static unsigned g_trijournal_queue_batches = DEFAULT_TRIJOURNAL_QUEUE_BATCHES;
static trijournal_pool_t *g_trijournal_pool = NULL;
/* Final pool tallies, captured at finish for the run summary. */
static uint64_t g_trijournal_final_bytes = 0;
static uint64_t g_trijournal_final_entries = 0;
static uint64_t g_trijournal_final_published = 0;
static int g_trijournal_final_voided = 0;
/* --no-stat: walk names only, never read an inode. Recursion needs just d_type, so records
 * (uid/size/inode/nlink/times all come from stat) cannot be written and paths stream instead. */
static int g_no_stat = 0;
/* --count (requires --no-stat): tally d_type counts only; do not stream paths to stdout. */
static int g_nostat_count = 0;
static int g_print0 = 0;
/* --contains: lowercased needle for the full-path substring filter; NULL when unset. */
static char *g_contains_lower = NULL;
static size_t g_contains_len = 0;
static int g_verbose = 0;
/* --progress: publish TLS totals every g_progress_every dirents (across directories). */
static int g_progress = 0;
static size_t g_progress_every = DEFAULT_DONATE_ENTRY_CHECK_EVERY;
static int g_crawl_threads = DEFAULT_CRAWL_THREADS;

/* Per-worker published totals for --progress (plain stores; owning thread only). */
typedef struct {
    uint64_t entries;
    uint64_t files;
    uint64_t bytes;
} progress_slot_t;

static progress_slot_t *g_progress_slots = NULL;
static size_t g_progress_nslots = 0;
static atomic_ullong g_progress_last_print_ns = 0;
#define PROGRESS_PRINT_MIN_NS 500000000ULL /* coalesce: at most ~2 lines/s */
static size_t g_getdents_buf_bytes = DEFAULT_GETDENTS_BUF;
static size_t g_donate_check_every_cfg     = DEFAULT_DONATE_CHECK_EVERY;
static size_t g_donate_entry_check_every_cfg = DEFAULT_DONATE_ENTRY_CHECK_EVERY;
static size_t g_donate_chunk_force_max_cfg = DONATE_CHUNK_FORCE_MAX;
static size_t g_force_donate_count_cfg     = LOCAL_STACK_FORCE_DONATE_COUNT;
static size_t g_donate_all_busy_min_stack_cfg              = DEFAULT_DONATE_ALL_BUSY_MIN_STACK;
static unsigned g_donate_all_busy_max_qdepth_mult_cfg = DEFAULT_DONATE_ALL_BUSY_MAX_QDEPTH_MULT;
static size_t g_discovered_dir_enqueue_batch_cfg = DEFAULT_DISCOVERED_DIR_ENQUEUE_BATCH;
static atomic_uint g_fd_pressure = 0;
static atomic_uint g_writer_failed = 0;
static atomic_uint g_disk_low = 0;
static atomic_uint g_disk_monitor_stop = 0;
static atomic_uint g_disk_wait_disabled = 0;
static char g_output_dir[PATH_MAX] = ".";
/* When set, bin records store paths under this root instead of the physical crawl start-path. */
static char g_record_root_buf[PATH_MAX];
static const char *g_record_root = NULL;
static char g_phys_prefix[PATH_MAX];
static size_t g_phys_prefix_len = 0;
/* Canonical crawl root: absolute, from argv or realpath(relative). */
static char g_start_path_canon[PATH_MAX];
static id_registry_t g_uid_registry;
static id_registry_t g_gid_registry;
static inode_registry_t g_hardlink_registry;
/* Distinct-owner counts and the wall time their NSS lookups took, captured before the registries are
 * torn down so the summary can attribute a slow finalize to owner count rather than to the crawl. */
static size_t g_uid_distinct = 0;
static size_t g_gid_distinct = 0;
static double g_id_resolve_sec = 0.0;

static FILE *ecrawl_pfopen(const char *path, const char *mode);
static int ecrawl_pfclose(FILE *fp);
static size_t ecrawl_pfwrite(const void *ptr, size_t size, size_t nmemb, FILE *fp);
static int ecrawl_pfflush(FILE *fp);
static int ecrawl_pstat(const char *path, struct stat *st);
static int ecrawl_plstat(const char *path, struct stat *st);
static int ecrawl_pfstatat_nf(int dirfd_value, const char *name, struct stat *st);
static int ecrawl_pmkdir(const char *path, mode_t mode);
static DIR *ecrawl_popendir(const char *path);
static struct dirent *ecrawl_preaddir(DIR *dir);
static int ecrawl_pclosedir(DIR *dir);

static FILE *(*ecrawl_io_fopen)(const char *, const char *) = fopen;
static int (*ecrawl_io_fclose)(FILE *) = fclose;
static size_t (*ecrawl_io_fwrite)(const void *, size_t, size_t, FILE *) = fwrite;
static int (*ecrawl_io_fflush)(FILE *) = fflush;
static int (*ecrawl_io_stat)(const char *, struct stat *) = stat;
static int (*ecrawl_io_lstat)(const char *, struct stat *) = lstat;
static int (*ecrawl_io_fstatat_nf)(int, const char *, struct stat *) = ecrawl_pfstatat_nf;
static int (*ecrawl_io_mkdir)(const char *, mode_t) = mkdir;
static DIR *(*ecrawl_io_opendir)(const char *) = opendir;
static struct dirent *(*ecrawl_io_readdir)(DIR *) = readdir;
static int (*ecrawl_io_closedir)(DIR *) = closedir;

static void ecrawl_hook_task_popped_nop(void) {}
static void ecrawl_hook_writer_push_nop(void) {}
static void ecrawl_hook_writer_pop_nop(void) {}

static void (*ecrawl_hook_task_popped)(void) = ecrawl_hook_task_popped_nop;
static void (*ecrawl_hook_writer_push)(void) = ecrawl_hook_writer_push_nop;
static void (*ecrawl_hook_writer_pop)(void) = ecrawl_hook_writer_pop_nop;

static int ecrawl_pfstatat_nf(int dirfd_value, const char *name, struct stat *st) {
    ATOMIC_ADD_RELAXED(&g_io_lstat_calls, 1);
    return fstatat(dirfd_value, name, st, AT_SYMLINK_NOFOLLOW);
}

static FILE *ecrawl_pfopen(const char *path, const char *mode) {
    ATOMIC_ADD_RELAXED(&g_io_fopen_calls, 1);
    return fopen(path, mode);
}

static int ecrawl_pfclose(FILE *fp) {
    ATOMIC_ADD_RELAXED(&g_io_fclose_calls, 1);
    return fclose(fp);
}

static size_t ecrawl_pfwrite(const void *ptr, size_t size, size_t nmemb, FILE *fp) {
    ATOMIC_ADD_RELAXED(&g_io_fwrite_calls, 1);
    return fwrite(ptr, size, nmemb, fp);
}

static int ecrawl_pfflush(FILE *fp) {
    ATOMIC_ADD_RELAXED(&g_io_fflush_calls, 1);
    return fflush(fp);
}

static int ecrawl_pstat(const char *path, struct stat *st) {
    ATOMIC_ADD_RELAXED(&g_io_stat_calls, 1);
    return stat(path, st);
}

static int ecrawl_plstat(const char *path, struct stat *st) {
    ATOMIC_ADD_RELAXED(&g_io_lstat_calls, 1);
    return lstat(path, st);
}

/* ----- --statx / --iouring: statx(2) with a mode-minimal attribute mask -----------------------------
 * fstatat(2) always collects STATX_BASIC_STATS; ecrawl never reads all of it. --no-write needs no
 * ownership or timestamps, --no-stat's d_type probe needs only the type bits, and even the capture
 * path reads exactly BASIC_STATS. Asking for less lets network/cluster filesystems skip attribute
 * work (the local-filesystem win is small). A filesystem that declines part of the mask (stx_mask
 * short of what was requested) is answered with a plain fstatat for that one entry, so consumers
 * never see zeroed fields. */
static int g_statx_mode = 0;    /* --statx */
static int g_iouring_statx = 0; /* --iouring (IORING_OP_STATX; same mask as --statx) */
static unsigned g_statx_mask = STATX_BASIC_STATS;

static void ecrawl_statx_select_mask(void) {
    if (g_no_stat)
        g_statx_mask = STATX_TYPE; /* d_type probe only */
    else if (g_no_write)
        g_statx_mask = STATX_TYPE | STATX_SIZE | STATX_BLOCKS | STATX_NLINK | STATX_INO;
    else
        g_statx_mask = STATX_BASIC_STATS;
}

static void ecrawl_statx_to_stat(const struct statx *sx, struct stat *st) {
    memset(st, 0, sizeof(*st));
    st->st_mode = sx->stx_mode;
    st->st_uid = sx->stx_uid;
    st->st_gid = sx->stx_gid;
    st->st_size = (off_t)sx->stx_size;
    st->st_blocks = (blkcnt_t)sx->stx_blocks;
    st->st_nlink = (nlink_t)sx->stx_nlink;
    st->st_ino = (ino_t)sx->stx_ino;
    st->st_dev = makedev(sx->stx_dev_major, sx->stx_dev_minor);
    st->st_rdev = makedev(sx->stx_rdev_major, sx->stx_rdev_minor);
    st->st_blksize = (blksize_t)sx->stx_blksize;
    st->st_atim.tv_sec = sx->stx_atime.tv_sec;
    st->st_atim.tv_nsec = sx->stx_atime.tv_nsec;
    st->st_mtim.tv_sec = sx->stx_mtime.tv_sec;
    st->st_mtim.tv_nsec = sx->stx_mtime.tv_nsec;
    st->st_ctim.tv_sec = sx->stx_ctime.tv_sec;
    st->st_ctim.tv_nsec = sx->stx_ctime.tv_nsec;
}

static int ecrawl_statx_fstatat_nf(int dirfd_value, const char *name, struct stat *st) {
    struct statx sx;

    ATOMIC_ADD_RELAXED(&g_io_lstat_calls, 1);
    if (statx(dirfd_value, name, AT_SYMLINK_NOFOLLOW, g_statx_mask, &sx) != 0) return -1;
    if ((sx.stx_mask & g_statx_mask) != g_statx_mask)
        return fstatat(dirfd_value, name, st, AT_SYMLINK_NOFOLLOW);
    ecrawl_statx_to_stat(&sx, st);
    return 0;
}

static int ecrawl_statx_lstat(const char *path, struct stat *st) {
    struct statx sx;

    ATOMIC_ADD_RELAXED(&g_io_lstat_calls, 1);
    if (statx(AT_FDCWD, path, AT_SYMLINK_NOFOLLOW, g_statx_mask, &sx) != 0) return -1;
    if ((sx.stx_mask & g_statx_mask) != g_statx_mask) return lstat(path, st);
    ecrawl_statx_to_stat(&sx, st);
    return 0;
}

/* Called after ecrawl_install_verbose_profile: the statx wrappers replace whichever stat pointers
 * are current, and they count g_io_lstat_calls themselves so --verbose accounting is unchanged. */
static void ecrawl_install_statx_profile(void) {
    if (!g_statx_mode && !g_iouring_statx) return;
    ecrawl_statx_select_mask();
    ecrawl_io_lstat = ecrawl_statx_lstat;
    ecrawl_io_fstatat_nf = ecrawl_statx_fstatat_nf;
}

/* ----- per-worker io_uring for batched statx -------------------------------------------------------
 * The walk loop is otherwise one synchronous fstatat per entry; with --iouring each worker
 * owns a small ring, queues up to ECRAWL_IOURING_DEPTH statx SQEs per directory batch, and reaps
 * them in readdir order. One io_uring_enter per batch amortizes the syscall boundary and lets the
 * kernel run the lookups concurrently. */
static unsigned g_iouring_depth = 256; /* ECRAWL_IOURING_DEPTH */
static unsigned g_iouring_min_batch = 8; /* ECRAWL_IOURING_MIN_BATCH: below this, stat inline */
static atomic_ullong g_io_uring_batches = 0;    /* batches submitted to a ring */
static atomic_ullong g_io_uring_sync_redos = 0; /* entries re-stat'd synchronously (short mask or dead ring) */
static atomic_ullong g_io_uring_inline_stats = 0; /* names that skipped the ring (batch < min) */
static atomic_int g_io_uring_warned = 0;        /* one fallback warning per run, not one per worker */

#ifdef ECRAWL_HAVE_IOURING
typedef struct {
    char name[256];      /* owned copy of the dirent name; the SQE points here */
    uint16_t name_len;
    unsigned char dtype; /* getdents d_type, kept for dispatch parity with the inline loop */
    struct statx stx;    /* SQE result buffer */
} uring_slot_t;

typedef struct {
    int fd;   /* -1: ring not usable */
    int dead; /* hard ring error mid-run: finish batches with synchronous statx */
    unsigned depth;
    unsigned sq_next; /* SQ is fully drained each batch, so this tracks *sq_tail */
    unsigned *sq_tail;
    unsigned *sq_ring_mask;
    unsigned *sq_array;
    struct io_uring_sqe *sqes;
    unsigned *cq_head;
    unsigned *cq_tail;
    unsigned *cq_ring_mask;
    struct io_uring_cqe *cqes;
    void *sq_map;
    size_t sq_map_len;
    void *cq_map;
    size_t cq_map_len;
    void *sqes_map;
    size_t sqes_map_len;
    uring_slot_t *slots;
    long *res; /* per-slot completion results, indexed by slot */
    unsigned batch;
} uring_stat_ctx_t;

static void uring_stat_destroy(uring_stat_ctx_t *u) {
    if (u->sq_map && u->sq_map != MAP_FAILED) munmap(u->sq_map, u->sq_map_len);
    if (u->cq_map && u->cq_map != MAP_FAILED && u->cq_map != u->sq_map) munmap(u->cq_map, u->cq_map_len);
    if (u->sqes_map && u->sqes_map != MAP_FAILED) munmap(u->sqes_map, u->sqes_map_len);
    free(u->slots);
    free(u->res);
    if (u->fd >= 0) close(u->fd);
    memset(u, 0, sizeof(*u));
    u->fd = -1;
}

static int uring_stat_init(uring_stat_ctx_t *u, unsigned depth) {
    struct io_uring_params p;
    size_t sq_ring, cq_ring;

    memset(u, 0, sizeof(*u));
    u->fd = -1;
    memset(&p, 0, sizeof(p));
    u->fd = (int)syscall(__NR_io_uring_setup, depth, &p);
    if (u->fd < 0) return -1;

    sq_ring = p.sq_off.array + (size_t)p.sq_entries * sizeof(unsigned);
    cq_ring = p.cq_off.cqes + (size_t)p.cq_entries * sizeof(struct io_uring_cqe);
    if (p.features & IORING_FEAT_SINGLE_MMAP) {
        if (cq_ring > sq_ring) sq_ring = cq_ring;
        cq_ring = sq_ring;
    }
    u->sq_map_len = sq_ring;
    u->cq_map_len = cq_ring;
    u->sq_map = mmap(NULL, sq_ring, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, u->fd, IORING_OFF_SQ_RING);
    if (u->sq_map == MAP_FAILED) goto fail;
    if (p.features & IORING_FEAT_SINGLE_MMAP) {
        u->cq_map = u->sq_map;
    } else {
        u->cq_map = mmap(NULL, cq_ring, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, u->fd, IORING_OFF_CQ_RING);
        if (u->cq_map == MAP_FAILED) goto fail;
    }
    u->sqes_map_len = (size_t)p.sq_entries * sizeof(struct io_uring_sqe);
    u->sqes_map = mmap(NULL, u->sqes_map_len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, u->fd, IORING_OFF_SQES);
    if (u->sqes_map == MAP_FAILED) goto fail;

    u->sq_tail = (unsigned *)((char *)u->sq_map + p.sq_off.tail);
    u->sq_ring_mask = (unsigned *)((char *)u->sq_map + p.sq_off.ring_mask);
    u->sq_array = (unsigned *)((char *)u->sq_map + p.sq_off.array);
    u->cq_head = (unsigned *)((char *)u->cq_map + p.cq_off.head);
    u->cq_tail = (unsigned *)((char *)u->cq_map + p.cq_off.tail);
    u->cq_ring_mask = (unsigned *)((char *)u->cq_map + p.cq_off.ring_mask);
    u->cqes = (struct io_uring_cqe *)((char *)u->cq_map + p.cq_off.cqes);
    u->sqes = (struct io_uring_sqe *)u->sqes_map;

    u->slots = calloc(depth, sizeof(uring_slot_t));
    u->res = calloc(depth, sizeof(long));
    if (!u->slots || !u->res) goto fail;
    u->depth = depth;
    return 0;
fail:
    {
        int saved_errno = errno;
        uring_stat_destroy(u);
        errno = saved_errno; /* caller reports why setup failed, not how cleanup went */
    }
    return -1;
}

/* Queue a statx SQE for slot i of the current batch. */
static void uring_prep_slot(uring_stat_ctx_t *u, unsigned i, int dirfd, uring_slot_t *slot) {
    unsigned idx = (u->sq_next + i) & *u->sq_ring_mask;
    struct io_uring_sqe *sqe = &u->sqes[idx];

    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_STATX;
    sqe->fd = dirfd;
    sqe->addr = (uint64_t)(uintptr_t)slot->name;
    sqe->len = g_statx_mask;
    sqe->off = (uint64_t)(uintptr_t)&slot->stx; /* statx has no file offset: off carries the out buffer */
    sqe->statx_flags = AT_SYMLINK_NOFOLLOW;
    sqe->user_data = i;
    u->sq_array[idx] = idx;
}

/* Submit the n prepped SQEs and wait for every completion; results land in u->res[slot].
 * Returns -1 on a hard ring error (caller degrades the batch to synchronous statx). */
static int uring_submit_wait_all(uring_stat_ctx_t *u, unsigned n) {
    unsigned done = 0;
    int ret;

    atomic_store_explicit((_Atomic unsigned *)u->sq_tail, u->sq_next + n, memory_order_release);
    while (done < n) {
        unsigned head, tail;

        ret = (int)syscall(__NR_io_uring_enter, u->fd, done == 0 ? n : 0, n - done, IORING_ENTER_GETEVENTS, NULL, 0);
        if (ret < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        head = atomic_load_explicit((_Atomic unsigned *)u->cq_head, memory_order_acquire);
        tail = atomic_load_explicit((_Atomic unsigned *)u->cq_tail, memory_order_acquire);
        while (head != tail) {
            struct io_uring_cqe *cqe = &u->cqes[head & *u->cq_ring_mask];
            u->res[cqe->user_data] = cqe->res;
            done++;
            head++;
        }
        atomic_store_explicit((_Atomic unsigned *)u->cq_head, head, memory_order_release);
    }
    u->sq_next += n;
    ATOMIC_ADD_RELAXED(&g_io_uring_batches, 1);
    return 0;
}

/* One-shot probe: some kernels/filesystems reject IORING_OP_STATX even when io_uring works. */
static int uring_stat_probe(uring_stat_ctx_t *u) {
    uring_slot_t *slot = &u->slots[0];

    memcpy(slot->name, ".", 2);
    slot->name_len = 1;
    uring_prep_slot(u, 0, AT_FDCWD, slot);
    if (uring_submit_wait_all(u, 1) != 0) return -1;
    if (u->res[0] == -EINVAL || u->res[0] == -EOPNOTSUPP || u->res[0] == -ENOSYS) return -1;
    return 0;
}
#else
typedef struct {
    int fd;
    int dead;
} uring_stat_ctx_t;

static int uring_stat_init(uring_stat_ctx_t *u, unsigned depth) {
    (void)depth;
    u->fd = -1;
    return -1;
}
static void uring_stat_destroy(uring_stat_ctx_t *u) { (void)u; }
static int uring_stat_probe(uring_stat_ctx_t *u) {
    (void)u;
    return -1;
}
#endif

static int ecrawl_pfstatat_nf_verbose(int dirfd_value, const char *name, struct stat *st) {
    return ecrawl_pfstatat_nf(dirfd_value, name, st);
}

static int ecrawl_pmkdir(const char *path, mode_t mode) {
    ATOMIC_ADD_RELAXED(&g_io_mkdir_calls, 1);
    return mkdir(path, mode);
}

static DIR *ecrawl_popendir(const char *path) {
    ATOMIC_ADD_RELAXED(&g_io_opendir_calls, 1);
    return opendir(path);
}

static struct dirent *ecrawl_preaddir(DIR *dir) {
    ATOMIC_ADD_RELAXED(&g_io_readdir_calls, 1);
    return readdir(dir);
}

static int ecrawl_pclosedir(DIR *dir) {
    ATOMIC_ADD_RELAXED(&g_io_closedir_calls, 1);
    return closedir(dir);
}

/* ASCII-only lowering, matching ereport_index's path_matches_term so `ecrawl --contains` and
 * `ereport_index --search` agree on what a path contains. */
static unsigned char ascii_lower_byte(unsigned char c) {
    return (c >= 'A' && c <= 'Z') ? (unsigned char)(c + 32) : c;
}

static char *ascii_lower_dup(const char *s) {
    size_t n = strlen(s);
    char *out = (char *)malloc(n + 1);
    size_t i;

    if (!out) return NULL;
    for (i = 0; i < n; i++) out[i] = (char)ascii_lower_byte((unsigned char)s[i]);
    out[n] = '\0';
    return out;
}

/* Substring search over an already-lowercased buffer. memmem would need _GNU_SOURCE, which this
 * file does not define, so anchor on the needle's first byte with memchr and memcmp the rest. */
static int lower_buf_contains(const char *hay, size_t hay_len, const char *needle, size_t needle_len) {
    const char *p = hay;
    size_t remaining = hay_len;

    if (needle_len == 0) return 1;
    if (hay_len < needle_len) return 0;

    for (;;) {
        size_t searchable = remaining - needle_len + 1;
        const char *hit = (const char *)memchr(p, (unsigned char)needle[0], searchable);

        if (!hit) return 0;
        remaining -= (size_t)(hit - p);
        if (remaining < needle_len) return 0;
        if (memcmp(hit, needle, needle_len) == 0) return 1;
        p = hit + 1;
        remaining--;
    }
}

static pthread_mutex_t g_pathout_lock = PTHREAD_MUTEX_INITIALIZER;
static atomic_ullong g_nostat_printed = 0;
static atomic_ullong g_nostat_dtype_unknown_fallbacks = 0;

/* Does `path` (any case, length path_len) contain the --contains needle? */
static int path_contains_needle(const char *path, size_t path_len) {
    char stackbuf[PATH_MAX];
    char *heap = NULL;
    char *buf;
    size_t i;
    int hit;

    if (g_contains_len == 0) return 1;
    if (path_len < g_contains_len) return 0;

    if (path_len <= sizeof(stackbuf)) {
        buf = stackbuf;
    } else {
        heap = (char *)malloc(path_len);
        if (!heap) return 0;
        buf = heap;
    }
    for (i = 0; i < path_len; i++) buf[i] = (char)ascii_lower_byte((unsigned char)path[i]);
    hit = lower_buf_contains(buf, path_len, g_contains_lower, g_contains_len);
    free(heap);
    return hit;
}

static int nostat_ctx_init(nostat_ctx_t *ctx) {
    memset(ctx, 0, sizeof(*ctx));
    if (!g_no_stat) return 0;

    /* --count never prints paths, so skip the pathout buffer unless --contains still needs
     * the windowed matcher (count matching names without streaming them). */
    if (!g_nostat_count) {
        ctx->buf = (char *)malloc(PATHOUT_BUF_BYTES);
        if (!ctx->buf) return -1;
        ctx->cap = PATHOUT_BUF_BYTES;
    }

    if (g_contains_len > 0) {
        /* Worst case: the whole parent tail (needle_len-1), a separator, and a NAME_MAX name. */
        ctx->win_cap = g_contains_len + (size_t)NAME_MAX + 2u;
        ctx->win = (char *)malloc(ctx->win_cap);
        if (!ctx->win) {
            free(ctx->buf);
            ctx->buf = NULL;
            return -1;
        }
    }
    return 0;
}

static void nostat_ctx_destroy(nostat_ctx_t *ctx) {
    free(ctx->buf);
    free(ctx->win);
    ctx->buf = NULL;
    ctx->win = NULL;
    ctx->cap = 0;
    ctx->len = 0;
    ctx->win_cap = 0;
}

static int pathout_write_all(const char *data, size_t len) {
    size_t off = 0;

    while (off < len) {
        ssize_t n = write(STDOUT_FILENO, data + off, len - off);

        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

static int pathout_flush(nostat_ctx_t *ctx) {
    int rc;

    if (ctx->len == 0) return 0;
    pthread_mutex_lock(&g_pathout_lock);
    rc = pathout_write_all(ctx->buf, ctx->len);
    pthread_mutex_unlock(&g_pathout_lock);
    ctx->len = 0;
    return rc;
}

static int pathout_emit(nostat_ctx_t *ctx, const char *path, size_t path_len) {
    size_t need = path_len + 1u;
    char sep = g_print0 ? '\0' : '\n';

    if (g_nostat_count) return 0;
    if (!ctx->buf) return -1;
    if (need > ctx->cap) {
        /* Longer than one whole buffer: flush what we have and write it straight through. */
        int rc;

        if (pathout_flush(ctx) != 0) return -1;
        pthread_mutex_lock(&g_pathout_lock);
        rc = pathout_write_all(path, path_len);
        if (rc == 0) rc = pathout_write_all(&sep, 1);
        pthread_mutex_unlock(&g_pathout_lock);
        if (rc == 0) ATOMIC_ADD_RELAXED(&g_nostat_printed, 1);
        return rc;
    }
    if (ctx->len + need > ctx->cap && pathout_flush(ctx) != 0) return -1;

    memcpy(ctx->buf + ctx->len, path, path_len);
    ctx->len += path_len;
    ctx->buf[ctx->len++] = sep;
    ATOMIC_ADD_RELAXED(&g_nostat_printed, 1);
    return 0;
}

/* Resolve the --contains state for one directory. When all_match comes back set, every entry in
 * this directory and every descendant matches, so no per-entry comparison is needed at all. */
static void dirmatch_begin(nostat_ctx_t *ctx, const char *dir_path, size_t dir_path_len,
                           int ancestor_matched, dirmatch_t *dm) {
    size_t tail;
    size_t i;

    dm->all_match = 0;
    dm->prefix_len = 0;

    if (g_contains_len == 0) {
        dm->all_match = 1;
        return;
    }
    if (ancestor_matched == CONTAINS_YES) {
        dm->all_match = 1;
        return;
    }
    /* CONTAINS_NO means the parent's windowed test already proved this path has no match, so the
     * only path that still needs a full scan is the crawl root. */
    if (ancestor_matched == CONTAINS_UNKNOWN && path_contains_needle(dir_path, dir_path_len)) {
        dm->all_match = 1;
        return;
    }

    /* The needle is absent from dir_path, so any match under it must reach into the final
     * component. An occurrence lying wholly inside dir_path would have been found above, so it
     * can only start within the last needle_len-1 bytes; that tail plus "/name" is the whole
     * search space, and it is bounded by the needle length rather than the path depth. */
    tail = g_contains_len - 1u;
    if (tail > dir_path_len) tail = dir_path_len;
    for (i = 0; i < tail; i++)
        ctx->win[i] = (char)ascii_lower_byte((unsigned char)dir_path[dir_path_len - tail + i]);
    dm->prefix_len = tail;
    /* Root is "/" and takes no extra separator, matching path_join_fast. */
    if (!(dir_path_len == 1 && dir_path[0] == '/')) ctx->win[dm->prefix_len++] = '/';
}

static int dirmatch_hit(nostat_ctx_t *ctx, const dirmatch_t *dm, const char *name, size_t name_len) {
    size_t total = dm->prefix_len + name_len;
    size_t i;

    if (dm->all_match) return 1;
    if (total < g_contains_len) return 0;
    if (total > ctx->win_cap) return 0; /* unreachable: name_len is bounded by NAME_MAX */

    for (i = 0; i < name_len; i++)
        ctx->win[dm->prefix_len + i] = (char)ascii_lower_byte((unsigned char)name[i]);
    return lower_buf_contains(ctx->win, total, g_contains_lower, g_contains_len);
}

static unsigned char dtype_from_mode(mode_t m) {
    if (S_ISDIR(m)) return DT_DIR;
    if (S_ISREG(m)) return DT_REG;
    if (S_ISLNK(m)) return DT_LNK;
    if (S_ISFIFO(m)) return DT_FIFO;
    if (S_ISSOCK(m)) return DT_SOCK;
    if (S_ISCHR(m)) return DT_CHR;
    if (S_ISBLK(m)) return DT_BLK;
    return DT_UNKNOWN;
}

/* Counting counterpart to account_entry_local for the no-stat walk: only what d_type can prove.
 * Byte totals, hardlink dedup and the uid/gid registries all need a struct stat and stay unset. */
static void account_entry_dtype(crawl_stats_t *stats, perf_local_t *perf, unsigned char d_type) {
    stats->total_entries++;
    perf->entries++;
    switch (d_type) {
    case DT_DIR:
        stats->total_dirs++;
        perf->dirs++;
        break;
    case DT_REG:
        stats->total_files++;
        perf->files++;
        break;
    case DT_LNK:
        stats->total_symlinks++;
        break;
    default:
        stats->total_other++;
        break;
    }

}

static void ecrawl_hook_task_popped_verbose(void) { ATOMIC_ADD_RELAXED(&g_tasks_popped, 1); }

static void ecrawl_hook_writer_push_verbose(void) {
    ATOMIC_ADD_RELAXED(&g_writer_queue_depth, 1);
    ATOMIC_ADD_RELAXED(&g_batches_enqueued, 1);
}

static void ecrawl_hook_writer_pop_verbose(void) {
    ATOMIC_SUB_RELAXED(&g_writer_queue_depth, 1);
    ATOMIC_ADD_RELAXED(&g_batches_dequeued, 1);
}

static void ecrawl_install_verbose_profile(void) {
    if (!g_verbose) return;
    ecrawl_io_fopen = ecrawl_pfopen;
    ecrawl_io_fclose = ecrawl_pfclose;
    ecrawl_io_fwrite = ecrawl_pfwrite;
    ecrawl_io_fflush = ecrawl_pfflush;
    ecrawl_io_stat = ecrawl_pstat;
    ecrawl_io_lstat = ecrawl_plstat;
    ecrawl_io_fstatat_nf = ecrawl_pfstatat_nf_verbose;
    ecrawl_io_mkdir = ecrawl_pmkdir;
    ecrawl_io_opendir = ecrawl_popendir;
    ecrawl_io_readdir = ecrawl_preaddir;
    ecrawl_io_closedir = ecrawl_pclosedir;
    ecrawl_hook_task_popped = ecrawl_hook_task_popped_verbose;
    ecrawl_hook_writer_push = ecrawl_hook_writer_push_verbose;
    ecrawl_hook_writer_pop = ecrawl_hook_writer_pop_verbose;
}

static void emfile_retry_pause(unsigned attempt) {
    unsigned long usec = (unsigned long)EMFILE_RETRY_USEC;
    struct timespec ts;

    if (attempt < 4U) usec *= (unsigned long)(attempt + 1U);
    ts.tv_sec = (time_t)(usec / 1000000UL);
    ts.tv_nsec = (long)((usec % 1000000UL) * 1000UL);
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
    }
}

static char file_type_char(mode_t mode) {
    if (S_ISREG(mode))  return 'f';
    if (S_ISDIR(mode))  return 'd';
    if (S_ISLNK(mode))  return 'l';
    if (S_ISCHR(mode))  return 'c';
    if (S_ISBLK(mode))  return 'b';
    if (S_ISFIFO(mode)) return 'p';
    if (S_ISSOCK(mode)) return 's';
    return 'o';
}

static int is_power_of_two_u32(uint32_t v) {
    return v && ((v & (v - 1U)) == 0U);
}

/* Crawl thread count (>=1). Env: ECRAWL_CRAWL_THREADS. */
static int parse_ecrawl_crawl_threads(void) {
    const char *e = getenv("ECRAWL_CRAWL_THREADS");
    long t;
    char *end;

    if (!e || !*e) return DEFAULT_CRAWL_THREADS;
    errno = 0;
    t = strtol(e, &end, 10);
    if (errno || end == e || *end || t < 1 || t > (long)INT_MAX) return DEFAULT_CRAWL_THREADS;
    return (int)t;
}

/* Workers used to resolve uid/gid names at finalize. Env: ECRAWL_ID_RESOLVE_THREADS (1 = serial). */
static int parse_ecrawl_id_resolve_threads(void) {
    const char *e = getenv("ECRAWL_ID_RESOLVE_THREADS");
    long t;
    char *end;

    if (!e || !*e) return DEFAULT_ID_RESOLVE_THREADS;
    errno = 0;
    t = strtol(e, &end, 10);
    if (errno || end == e || *end || t < 1 || t > 1024) return DEFAULT_ID_RESOLVE_THREADS;
    return (int)t;
}

/* Raw getdents64 read-buffer size in bytes (0 = use libc opendir/readdir). Env: ECRAWL_GETDENTS_BUF.
 * Out-of-range values clamp to [MIN_GETDENTS_BUF, MAX_GETDENTS_BUF]; an explicit 0 disables the raw path. */
static size_t parse_ecrawl_getdents_buf(void) {
    const char *e = getenv("ECRAWL_GETDENTS_BUF");
    unsigned long long v;
    char *end;

    if (!e || !*e) return DEFAULT_GETDENTS_BUF;
    errno = 0;
    v = strtoull(e, &end, 10);
    if (errno || end == e || *end) return DEFAULT_GETDENTS_BUF;
    if (v == 0ULL) return 0;
    if (v < MIN_GETDENTS_BUF) return MIN_GETDENTS_BUF;
    if (v > MAX_GETDENTS_BUF) return MAX_GETDENTS_BUF;
    return (size_t)v;
}

/* io_uring batch depth for --iouring (SQEs per directory batch). Env: ECRAWL_IOURING_DEPTH.
 * Clamped to [16, 4096]; the kernel may round the actual ring size up to a power of two. */
static unsigned parse_ecrawl_iouring_depth(void) {
    const char *e = getenv("ECRAWL_IOURING_DEPTH");
    unsigned long v;
    char *end;

    if (!e || !*e) return 256;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end) return 256;
    if (v < 16UL) return 16;
    if (v > 4096UL) return 4096;
    return (unsigned)v;
}

/* Minimum names in a collected directory batch before --iouring submits the ring.
 * Below this, those names are statx'd synchronously (tiny dirs: one enter is a tax).
 * Env: ECRAWL_IOURING_MIN_BATCH. Clamped to [1, depth]; 1 = always use the ring. */
static unsigned parse_ecrawl_iouring_min_batch(unsigned depth) {
    const char *e = getenv("ECRAWL_IOURING_MIN_BATCH");
    unsigned long v;
    char *end;

    if (!e || !*e) v = 8;
    else {
        errno = 0;
        v = strtoul(e, &end, 10);
        if (errno || end == e || *end) v = 8;
    }
    if (v < 1UL) v = 1;
    if (v > (unsigned long)depth) v = depth;
    return (unsigned)v;
}

static size_t parse_ecrawl_donate_check_every(void) {
    const char *e = getenv("ECRAWL_DONATE_CHECK_EVERY");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_DONATE_CHECK_EVERY;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 1UL || v > 65536UL) return DEFAULT_DONATE_CHECK_EVERY;
    return (size_t)v;
}

/* 0 disables the entry-driven check, leaving only the subdirectory-driven one. */
static size_t parse_ecrawl_donate_entry_check_every(void) {
    const char *e = getenv("ECRAWL_DONATE_ENTRY_CHECK_EVERY");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_DONATE_ENTRY_CHECK_EVERY;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v > 16777216UL) return DEFAULT_DONATE_ENTRY_CHECK_EVERY;
    return v == 0UL ? (size_t)SIZE_MAX : (size_t)v;
}

static size_t parse_ecrawl_donate_chunk_force_max(void) {
    const char *e = getenv("ECRAWL_DONATE_CHUNK_FORCE_MAX");
    unsigned long v;
    char *end;

    if (!e || !*e) return (size_t)DONATE_CHUNK_FORCE_MAX;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < (unsigned long)DONATE_CHUNK_MAX || v > 65536UL)
        return (size_t)DONATE_CHUNK_FORCE_MAX;
    return (size_t)v;
}

static size_t parse_ecrawl_force_donate_at(void) {
    const char *e = getenv("ECRAWL_FORCE_DONATE_AT");
    unsigned long v;
    char *end;

    if (!e || !*e) return LOCAL_STACK_FORCE_DONATE_COUNT;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 64UL || v > 1048576UL) return LOCAL_STACK_FORCE_DONATE_COUNT;
    return (size_t)v;
}

/* Min local dir_stack depth before proactive donation when every crawl thread already holds a task.
 * Env: ECRAWL_DONATE_ALL_BUSY_MIN_STACK (default DEFAULT_DONATE_ALL_BUSY_MIN_STACK). */
static size_t parse_ecrawl_donate_all_busy_min_stack(void) {
    const char *e = getenv("ECRAWL_DONATE_ALL_BUSY_MIN_STACK");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_DONATE_ALL_BUSY_MIN_STACK;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < (unsigned long)LOCAL_STACK_DONATE_FLOOR || v > 65536UL)
        return DEFAULT_DONATE_ALL_BUSY_MIN_STACK;
    return (size_t)v;
}

/* When all crawl threads are busy, skip donation if global queue depth >= started * mult.
 * Env: ECRAWL_DONATE_ALL_BUSY_MAX_QDEPTH_MULT (default DEFAULT_DONATE_ALL_BUSY_MAX_QDEPTH_MULT, range 1..256). */
static unsigned parse_ecrawl_donate_all_busy_max_qdepth_mult(void) {
    const char *e = getenv("ECRAWL_DONATE_ALL_BUSY_MAX_QDEPTH_MULT");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_DONATE_ALL_BUSY_MAX_QDEPTH_MULT;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 1UL || v > 256UL) return DEFAULT_DONATE_ALL_BUSY_MAX_QDEPTH_MULT;
    return (unsigned)v;
}

/* Batch this many discovered subdirs into one global task_queue push (fewer mutex ops). Env:
 * ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH (default DEFAULT_DISCOVERED_DIR_ENQUEUE_BATCH, range 1..4096). */
static size_t parse_ecrawl_discovered_dir_enqueue_batch(void) {
    const char *e = getenv("ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_DISCOVERED_DIR_ENQUEUE_BATCH;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 1UL || v > 4096UL) return DEFAULT_DISCOVERED_DIR_ENQUEUE_BATCH;
    return (size_t)v;
}

/* Writer thread count for uid-sharded output. Default: DEFAULT_WRITER_THREADS. */
static int parse_ecrawl_writer_threads_env(void) {
    const char *e = getenv("ECRAWL_WRITER_THREADS");
    long t;
    char *end;

    if (!e || !*e) return DEFAULT_WRITER_THREADS;
    errno = 0;
    t = strtol(e, &end, 10);
    if (errno || end == e || *end || t < 1 || t > 4096) return DEFAULT_WRITER_THREADS;
    return (int)t;
}

/* Pending record batches cap per writer queue. Env: ECRAWL_WRITER_QUEUE_BATCHES. */
static unsigned parse_ecrawl_writer_queue_batches_env(void) {
    const char *e = getenv("ECRAWL_WRITER_QUEUE_BATCHES");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_WRITER_QUEUE_BATCHES;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 4UL || v > 4096UL) return DEFAULT_WRITER_QUEUE_BATCHES;
    return (unsigned)v;
}

/* Trigram journal pool thread count (--trigram-journal). Default: DEFAULT_TRIJOURNAL_THREADS. */
static int parse_ecrawl_trijournal_threads_env(void) {
    const char *e = getenv("ECRAWL_TRIGRAM_JOURNAL_THREADS");
    long t;
    char *end;

    if (!e || !*e) return DEFAULT_TRIJOURNAL_THREADS;
    errno = 0;
    t = strtol(e, &end, 10);
    if (errno || end == e || *end || t < 1 || t > 256) return DEFAULT_TRIJOURNAL_THREADS;
    return (int)t;
}

/* Bounded journal handoff queue depth in record batches. Env: ECRAWL_TRIGRAM_JOURNAL_QUEUE_BATCHES. */
static unsigned parse_ecrawl_trijournal_queue_batches_env(void) {
    const char *e = getenv("ECRAWL_TRIGRAM_JOURNAL_QUEUE_BATCHES");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_TRIJOURNAL_QUEUE_BATCHES;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 1UL || v > 4096UL) return DEFAULT_TRIJOURNAL_QUEUE_BATCHES;
    return (unsigned)v;
}

/* Per-writer shard cache target. Auto-capped against RLIMIT_NOFILE later. */
static unsigned parse_ecrawl_max_open_shards_env(void) {
    const char *e = getenv("ECRAWL_MAX_OPEN_SHARDS");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_MAX_OPEN_SHARDS;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v < 1UL || v > (unsigned long)UINT_MAX)
        return DEFAULT_MAX_OPEN_SHARDS;
    g_max_open_shards_explicit = 1;
    return (unsigned)v;
}

static void configure_max_open_shards(void) {
    struct rlimit lim;
    rlim_t soft;
    rlim_t reserve;
    rlim_t available;
    rlim_t per_writer_fd_cap;
    rlim_t per_writer_shard_count;

    g_max_open_shards = g_requested_max_open_shards;
    if (g_no_write || g_writer_threads <= 0) return;
    if (getrlimit(RLIMIT_NOFILE, &lim) != 0 || lim.rlim_cur == RLIM_INFINITY) return;

    soft = lim.rlim_cur;
    reserve = FD_RESERVE_BASE +
              (rlim_t)g_crawl_threads * FD_RESERVE_PER_CRAWL_THREAD +
              (rlim_t)g_writer_threads * FD_RESERVE_PER_WRITER;
    if (soft <= reserve + (rlim_t)g_writer_threads) {
        g_max_open_shards = 1U;
        return;
    }

    available = soft - reserve;
    per_writer_fd_cap = available / (rlim_t)g_writer_threads;
    per_writer_shard_count = ((rlim_t)g_uid_shards + (rlim_t)g_writer_threads - 1U) /
                             (rlim_t)g_writer_threads;

    if (per_writer_fd_cap < 1U) per_writer_fd_cap = 1U;

    /*
     * The default is only right for the default writer count: with fewer writers each owns more than
     * 64 shards, and holding just 64 of them open puts the rest on an LRU that evicts and reopens a
     * shard for every burst of records belonging to it — each round trip costing an fopen, a catalog
     * reload and a rewritten .ckpt sidecar. Raise the default to cover the whole ownership set when
     * the fd budget allows; total buffer memory is bounded by uid_shards * WRITE_BUFFER_SIZE either
     * way, since writers * per_writer_shard_count is just the shard count. An explicit
     * ECRAWL_MAX_OPEN_SHARDS is a deliberate ceiling and is left alone.
     */
    if (!g_max_open_shards_explicit && per_writer_shard_count > (rlim_t)g_max_open_shards &&
        per_writer_shard_count <= per_writer_fd_cap)
        g_max_open_shards = (unsigned)per_writer_shard_count;
    if (per_writer_fd_cap < (rlim_t)g_max_open_shards) g_max_open_shards = (unsigned)per_writer_fd_cap;
    if (per_writer_shard_count < (rlim_t)g_max_open_shards) g_max_open_shards = (unsigned)per_writer_shard_count;
    if (g_max_open_shards < 1U) g_max_open_shards = 1U;
}

/* Must be a power of two. Default: DEFAULT_UID_SHARDS. */
static uint32_t parse_ecrawl_uid_shards_env(void) {
    const char *e = getenv("ECRAWL_UID_SHARDS");
    unsigned long v;
    char *end;

    if (!e || !*e) return DEFAULT_UID_SHARDS;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end || v == 0UL || v > (unsigned long)UINT32_MAX || !is_power_of_two_u32((uint32_t)v))
        return DEFAULT_UID_SHARDS;
    return (uint32_t)v;
}

/* Default 5; 0 disables stall hints. Env: ECRAWL_STALL_HINT_SECONDS. */
static int parse_ecrawl_stall_hint_seconds(void) {
    const char *e = getenv("ECRAWL_STALL_HINT_SECONDS");
    long v;
    char *end;

    if (!e || !*e) return 5;
    errno = 0;
    v = strtol(e, &end, 10);
    if (errno || end == e || *end || v < 0 || v > 86400L) return 5;
    return (int)v;
}

static uint32_t shard_for_uid(uid_t uid) {
    return ((uint32_t)uid) & (g_uid_shards - 1U);
}

static int shard_digits_for(uint32_t shards) {
    uint32_t max_index = shards ? (shards - 1U) : 0U;
    int digits = 1;
    while (max_index >= 10U) {
        max_index /= 10U;
        digits++;
    }
    return digits;
}

static void human_decimal(double v, char *buf, size_t sz) {
    const char *units[] = {"", "K", "M", "G", "T", "P", "E"};
    int i = 0;

    while (v >= 1000.0 && i < 6) {
        v /= 1000.0;
        i++;
    }

    if (v >= 100.0) snprintf(buf, sz, "%.0f%s", v, units[i]);
    else if (v >= 10.0) snprintf(buf, sz, "%.1f%s", v, units[i]);
    else snprintf(buf, sz, "%.2f%s", v, units[i]);
}

static void format_duration(double sec, char *out, size_t out_sz) {
    uint64_t total, hours, minutes, seconds;

    if (!out || out_sz == 0) return;
    if (sec < 0.0) sec = 0.0;

    total = (uint64_t)(sec + 0.5);
    hours = total / 3600;
    minutes = (total % 3600) / 60;
    seconds = total % 60;

    snprintf(out, out_sz, "%02" PRIu64 ":%02" PRIu64 ":%02" PRIu64, hours, minutes, seconds);
}

/* --count owns stdout for the census; live counts belong there too. Everyone else uses stderr
 * so a --no-stat path stream (or the write-mode summary) stays clean. */
static FILE *progress_stream(void) {
    return (g_no_stat && g_nostat_count) ? stdout : stderr;
}

static void clear_status_line(void) {
    if (isatty(STDOUT_FILENO)) {
        printf("\r\033[2K\r");
        fflush(stdout);
    }
    if (g_progress && progress_stream() == stderr && isatty(STDERR_FILENO)) {
        fprintf(stderr, "\r\033[2K\r");
        fflush(stderr);
    }
}

/* Publish this worker's crawl_stats_t into its progress slot (owning thread / under emit_stats_lock). */
static void progress_publish(worker_arg_t *wa) {
    size_t idx;

    if (!g_progress || !wa || !g_progress_slots) return;
    if (wa->worker_index == 0) return;
    idx = (size_t)wa->worker_index - 1U;
    if (idx >= g_progress_nslots) return;

    pthread_mutex_lock(&wa->emit_stats_lock);
    g_progress_slots[idx].entries = wa->stats.total_entries;
    g_progress_slots[idx].files = wa->stats.total_files;
    g_progress_slots[idx].bytes = wa->stats.total_bytes;
    pthread_mutex_unlock(&wa->emit_stats_lock);
}

static void progress_print_line(uint64_t files, uint64_t entries, uint64_t bytes) {
    FILE *fp = progress_stream();
    double elapsed_sec;
    char fe[32], ff[32], fb[32], el[32];
    int tty;

    elapsed_sec = g_run_start_sec > 0.0 ? now_sec() - g_run_start_sec : 0.0;
    human_decimal((double)files, ff, sizeof(ff));
    human_decimal((double)entries, fe, sizeof(fe));
    human_decimal((double)bytes, fb, sizeof(fb));
    format_duration(elapsed_sec, el, sizeof(el));
    tty = isatty(fileno(fp));
    if (bytes > 0ULL) {
        fprintf(fp, "%sfiles=%s entries=%s bytes=%s el=%s%s", tty ? "\r" : "", ff, fe, fb, el,
                tty ? "            " : "\n");
    } else {
        fprintf(fp, "%sfiles=%s entries=%s el=%s%s", tty ? "\r" : "", ff, fe, el, tty ? "            " : "\n");
    }
    fflush(fp);
}

/* Coalesced progress line; only one thread wins the last-print CAS per interval. */
static void progress_maybe_print(void) {
    struct timespec ts;
    unsigned long long now_ns;
    unsigned long long prev;
    uint64_t entries = 0;
    uint64_t files = 0;
    uint64_t bytes = 0;
    size_t i;

    if (!g_progress || !g_progress_slots) return;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return;
    now_ns = (unsigned long long)ts.tv_sec * 1000000000ULL + (unsigned long long)ts.tv_nsec;
    prev = atomic_load(&g_progress_last_print_ns);
    if (prev != 0ULL && (now_ns - prev) < PROGRESS_PRINT_MIN_NS) return;
    if (!atomic_compare_exchange_strong(&g_progress_last_print_ns, &prev, now_ns)) return;

    for (i = 0; i < g_progress_nslots; i++) {
        entries += g_progress_slots[i].entries;
        files += g_progress_slots[i].files;
        bytes += g_progress_slots[i].bytes;
    }
    progress_print_line(files, entries, bytes);
}

/* Accounted entries, across directories: a bushy tree of 10-name dirs never hit the old
 * per-readdir counter, so --progress stayed silent on the trees people actually walk.
 * Tick after account_entry_* so the first line is not files=0. */
static void progress_tick(worker_arg_t *wa) {
    uint64_t n;

    if (!g_progress || !wa) return;
    n = wa->stats.total_entries;
    if (n - (uint64_t)wa->progress_since < (uint64_t)g_progress_every) return;
    wa->progress_since = (size_t)n;
    progress_publish(wa);
    progress_maybe_print();
}

static inline size_t id_registry_hash(uint32_t id) {
    uint32_t x = id;
    x ^= x >> 16;
    x *= 0x7feb352dU;
    x ^= x >> 15;
    x *= 0x846ca68bU;
    x ^= x >> 16;
    return (size_t)x;
}

static int id_registry_grow_locked(id_registry_t *r) {
    size_t new_cap = (r->cap == 0) ? 256 : (r->cap * 2);
    uint32_t *new_slots = (uint32_t *)malloc(new_cap * sizeof(*new_slots));
    size_t i, new_mask;
    if (!new_slots) return -1;
    for (i = 0; i < new_cap; i++) new_slots[i] = ID_SLOT_EMPTY;
    new_mask = new_cap - 1;
    for (i = 0; i < r->cap; i++) {
        uint32_t v = r->slots[i];
        size_t j;
        if (v == ID_SLOT_EMPTY) continue;
        j = id_registry_hash(v) & new_mask;
        while (new_slots[j] != ID_SLOT_EMPTY) j = (j + 1) & new_mask;
        new_slots[j] = v;
    }
    free(r->slots);
    r->slots = new_slots;
    r->cap = new_cap;
    return 0;
}

/* Returns 1 if newly inserted, 0 if already present, -1 on allocation failure. */
static int id_registry_insert_locked(id_registry_t *r, uint32_t id) {
    size_t mask, i;

    if (id == ID_SLOT_EMPTY) {
        if (r->has_sentinel) return 0;
        r->has_sentinel = 1;
        r->count++;
        return 1;
    }

    /* Grow while load factor would exceed ~0.7 (keep probe chains short). */
    if (r->cap == 0 || (r->count + 1) * 10 >= r->cap * 7) {
        if (id_registry_grow_locked(r) != 0) return -1;
    }

    mask = r->cap - 1;
    i = id_registry_hash(id) & mask;
    while (r->slots[i] != ID_SLOT_EMPTY) {
        if (r->slots[i] == id) return 0;
        i = (i + 1) & mask;
    }
    r->slots[i] = id;
    r->count++;
    return 1;
}

static int id_registry_init(id_registry_t *r, const char *path, id_name_resolver_fn resolve) {
    int n;

    memset(r, 0, sizeof(*r));
    pthread_mutex_init(&r->mutex, NULL);
    r->resolve = resolve;

    n = snprintf(r->path, sizeof(r->path), "%s", path);
    if (n < 0 || (size_t)n >= sizeof(r->path)) {
        errno = ENAMETOOLONG;
        pthread_mutex_destroy(&r->mutex);
        return -1;
    }

    r->fp = ecrawl_io_fopen(path, "w");
    if (!r->fp) {
        pthread_mutex_destroy(&r->mutex);
        return -1;
    }

    return 0;
}

static int cmp_u32_asc(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;

    return (x < y) ? -1 : ((x > y) ? 1 : 0);
}

#define ID_NAME_MAX 256

/* Resolving one id is a blocking NSS call, so workers claim ids one at a time: the atomic is free
 * next to a lookup that may leave the process entirely. Every worker writes to its own names[i], so
 * the result array needs no locking and stays in the sorted order the caller built. */
typedef struct {
    const uint32_t *ids;
    char (*names)[ID_NAME_MAX];
    size_t n;
    id_name_resolver_fn resolve;
    atomic_size_t cursor;
} id_resolve_job_t;

static void id_resolve_run(id_resolve_job_t *j) {
    for (;;) {
        size_t i = atomic_fetch_add_explicit(&j->cursor, 1, memory_order_relaxed);

        if (i >= j->n) return;
        j->resolve(j->ids[i], j->names[i], ID_NAME_MAX);
    }
}

static void *id_resolve_worker_main(void *arg) {
    id_resolve_run((id_resolve_job_t *)arg);
    return NULL;
}

/* Resolve and write every id the crawl collected. Doing this once at finalize instead of on first
 * sight of each id keeps NSS off the crawl's hot path: getpwuid_r/getgrgid_r can mean a socket
 * round-trip to nss-systemd or sssd, and on a tree with thousands of distinct owners those lookups
 * showed up as ~10% of the write-mode profile, plus they hold up the crawl thread that drew the
 * unlucky entry.
 *
 * The lookups are latency-bound rather than CPU-bound, so they run across a small pool: a serial
 * pass over ~1000 owners cost 8.4s on a fixture whose crawl took 0.9s. Emitted in ascending id order
 * so the file is reproducible across runs rather than following whatever order the workers saw. */
static void id_registry_write_names(id_registry_t *r) {
    uint32_t *ids;
    char (*names)[ID_NAME_MAX];
    size_t n = 0, i;
    id_resolve_job_t job;
    pthread_t *threads = NULL;
    int nthreads, started = 0;

    if (!r->fp || !r->resolve || r->count == 0) return;

    ids = (uint32_t *)malloc(r->count * sizeof(*ids));
    if (!ids) {
        fprintf(stderr, "ERROR out of memory writing %s\n", r->path);
        return;
    }

    for (i = 0; i < r->cap && n < r->count; i++)
        if (r->slots[i] != ID_SLOT_EMPTY) ids[n++] = r->slots[i];
    if (r->has_sentinel && n < r->count) ids[n++] = ID_SLOT_EMPTY;

    qsort(ids, n, sizeof(*ids), cmp_u32_asc);

    names = (char (*)[ID_NAME_MAX])malloc(n * ID_NAME_MAX);
    if (!names) {
        /* Fall back to resolving and writing one id at a time rather than failing the run. */
        for (i = 0; i < n; i++) {
            char namebuf[ID_NAME_MAX];

            r->resolve(ids[i], namebuf, sizeof(namebuf));
            fprintf(r->fp, "%u %s\n", (unsigned int)ids[i], namebuf);
        }
        free(ids);
        return;
    }

    job.ids = ids;
    job.names = names;
    job.n = n;
    job.resolve = r->resolve;
    atomic_init(&job.cursor, 0);

    nthreads = parse_ecrawl_id_resolve_threads();
    if ((size_t)nthreads > n) nthreads = (int)n;
    if (nthreads > 1) {
        threads = (pthread_t *)malloc((size_t)(nthreads - 1) * sizeof(*threads));
        if (threads) {
            int t;

            for (t = 0; t < nthreads - 1; t++) {
                if (pthread_create(&threads[t], NULL, id_resolve_worker_main, &job) != 0) break;
                started++;
            }
        }
    }

    /* The caller participates, so a failed pthread_create just means less width, never lost work. */
    id_resolve_run(&job);
    for (i = 0; i < (size_t)started; i++) pthread_join(threads[i], NULL);
    free(threads);

    for (i = 0; i < n; i++) fprintf(r->fp, "%u %s\n", (unsigned int)ids[i], names[i]);

    free(names);
    free(ids);
}

/* Resolve names and write the file, leaving the registry otherwise intact. Split out from destroy so
 * the crawl can pay this cost inside its measured window instead of after the summary has printed. */
static void id_registry_finalize(id_registry_t *r) {
    if (r->finalized) return;
    r->finalized = 1;
    id_registry_write_names(r);
}

static void id_registry_destroy(id_registry_t *r) {
    id_registry_finalize(r);
    if (r->fp) ecrawl_io_fclose(r->fp);  /* fclose flushes the buffered id->name lines */
    free(r->slots);
    r->fp = NULL;
    r->slots = NULL;
    r->count = 0;
    r->cap = 0;
    r->has_sentinel = 0;
    pthread_mutex_destroy(&r->mutex);
}

static void resolve_uid_name(uint32_t id, char *out, size_t out_sz) {
    char namebuf[4096];
    struct passwd pwd;
    struct passwd *result = NULL;
    const char *name = "UNKNOWN";

    if (getpwuid_r((uid_t)id, &pwd, namebuf, sizeof(namebuf), &result) == 0 && result && result->pw_name)
        name = result->pw_name;
    snprintf(out, out_sz, "%s", name);
}

static void resolve_gid_name(uint32_t id, char *out, size_t out_sz) {
    char namebuf[4096];
    struct group grp;
    struct group *result = NULL;
    const char *name = "UNKNOWN";

    if (getgrgid_r((gid_t)id, &grp, namebuf, sizeof(namebuf), &result) == 0 && result && result->gr_name)
        name = result->gr_name;
    snprintf(out, out_sz, "%s", name);
}

/* Hot path: dedup only. Names are resolved and written by id_registry_destroy. */
static void write_uid_if_new(uid_t uid) {
    pthread_mutex_lock(&g_uid_registry.mutex);
    (void)id_registry_insert_locked(&g_uid_registry, (uint32_t)uid);
    pthread_mutex_unlock(&g_uid_registry.mutex);
}

static void write_gid_if_new(gid_t gid) {
    pthread_mutex_lock(&g_gid_registry.mutex);
    (void)id_registry_insert_locked(&g_gid_registry, (uint32_t)gid);
    pthread_mutex_unlock(&g_gid_registry.mutex);
}

static void record_ids_from_stat(const struct stat *st) {
    if (!st || g_no_write) return;
    write_uid_if_new(st->st_uid);
    write_gid_if_new(st->st_gid);
}

static uint64_t inode_hash_u64(uint64_t dev, uint64_t ino) {
    uint64_t x = dev + UINT64_C(0x9e3779b97f4a7c15);
    x ^= ino + UINT64_C(0x9e3779b97f4a7c15) + (x << 6) + (x >> 2);
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    x ^= x >> 31;
    return x;
}

static int inode_registry_resize_locked(inode_registry_shard_t *r, size_t new_cap) {
    inode_entry_t *new_items;
    size_t i;

    new_items = (inode_entry_t *)calloc(new_cap, sizeof(*new_items));
    if (!new_items) return -1;

    for (i = 0; i < r->cap; i++) {
        inode_entry_t entry;
        size_t idx;

        if (!r->items[i].used) continue;
        entry = r->items[i];
        idx = (size_t)(inode_hash_u64(entry.dev, entry.ino) & (uint64_t)(new_cap - 1));
        while (new_items[idx].used) idx = (idx + 1) & (new_cap - 1);
        new_items[idx] = entry;
    }

    free(r->items);
    r->items = new_items;
    r->cap = new_cap;
    return 0;
}

static int inode_registry_mark_seen(inode_registry_t *r, uint64_t dev, uint64_t ino) {
    int result = 0;
    uint64_t hash = inode_hash_u64(dev, ino);
    inode_registry_shard_t *shard = &r->shards[hash & (HARDLINK_REGISTRY_SHARDS - 1U)];

    pthread_mutex_lock(&shard->mutex);

    if (shard->cap == 0) {
        if (inode_registry_resize_locked(shard, 1U << 12) != 0) {
            pthread_mutex_unlock(&shard->mutex);
            return -1;
        }
    } else if ((shard->count + 1) * 10 >= shard->cap * 7) {
        if (inode_registry_resize_locked(shard, shard->cap << 1) != 0) {
            pthread_mutex_unlock(&shard->mutex);
            return -1;
        }
    }

    {
        size_t idx = (size_t)(hash & (uint64_t)(shard->cap - 1));
        while (shard->items[idx].used) {
            if (shard->items[idx].dev == dev && shard->items[idx].ino == ino) {
                result = 0;
                pthread_mutex_unlock(&shard->mutex);
                return result;
            }
            idx = (idx + 1) & (shard->cap - 1);
        }

        shard->items[idx].used = 1;
        shard->items[idx].dev = dev;
        shard->items[idx].ino = ino;
        shard->count++;
        result = 1;
    }

    pthread_mutex_unlock(&shard->mutex);
    return result;
}

static int inode_registry_init(inode_registry_t *r) {
    size_t i;

    memset(r, 0, sizeof(*r));
    for (i = 0; i < HARDLINK_REGISTRY_SHARDS; i++) {
        if (pthread_mutex_init(&r->shards[i].mutex, NULL) != 0) {
            while (i > 0) {
                i--;
                pthread_mutex_destroy(&r->shards[i].mutex);
            }
            return -1;
        }
    }
    return 0;
}

static void inode_registry_destroy(inode_registry_t *r) {
    size_t i;

    for (i = 0; i < HARDLINK_REGISTRY_SHARDS; i++) {
        free(r->shards[i].items);
        r->shards[i].items = NULL;
        r->shards[i].count = 0;
        r->shards[i].cap = 0;
        pthread_mutex_destroy(&r->shards[i].mutex);
    }
}

typedef struct {
    uint64_t byte_credit;
    uint64_t allocated_bytes;
    uint64_t sparse_heuristic_inc; /* 0 or 1 */
} regular_file_accounting_t;

/*
 * Per-regular-file stats sharing one inode_registry_mark_seen() result:
 *   - byte_credit: unique logical bytes (same rules as historical regular_file_byte_credit).
 *   - allocated_bytes: st_blocks * ST_BLOCKS_BYTES_UNIT on the same visits that credit logical bytes
 *     (st_nlink<=1 always; st_nlink>1 only when the inode is newly inserted in g_hardlink_registry).
 *   - sparse_heuristic_inc: 1 iff this visit credits logical bytes, st_size>0, and allocated_bytes < st_size.
 *     Hardlink aliases (byte_credit==0) do not increment — counts align with logical-byte credits, not raw stat paths.
 */
static void regular_file_accounting(shared_state_t *shared, crawl_stats_t *stats, const struct stat *st,
                                    regular_file_accounting_t *out) {
    uint64_t apparent;
    uint64_t from_blocks;
    blkcnt_t blocks;

    out->byte_credit = 0;
    out->allocated_bytes = 0;
    out->sparse_heuristic_inc = 0;

    if (!S_ISREG(st->st_mode)) return;

    apparent = (uint64_t)st->st_size;
    blocks = st->st_blocks;
    from_blocks = (blocks > 0) ? (uint64_t)blocks * (uint64_t)ST_BLOCKS_BYTES_UNIT : 0ULL;

    if (st->st_nlink <= 1) {
        out->byte_credit = apparent;
        out->allocated_bytes = from_blocks;
        if (apparent > 0ULL && from_blocks < apparent) out->sparse_heuristic_inc = 1ULL;
        return;
    }

    stats->total_hardlink_files++;
    {
        int seen_result =
            inode_registry_mark_seen(&g_hardlink_registry, (uint64_t)st->st_dev, (uint64_t)st->st_ino);

        if (seen_result < 0) {
            stats_add_error(shared);
            out->byte_credit = apparent;
            out->allocated_bytes = from_blocks;
            if (apparent > 0ULL && from_blocks < apparent) out->sparse_heuristic_inc = 1ULL;
            return;
        }
        if (!seen_result) return;

        out->byte_credit = apparent;
        out->allocated_bytes = from_blocks;
        if (apparent > 0ULL && from_blocks < apparent) out->sparse_heuristic_inc = 1ULL;
    }
}

/*
 * Account a single record locally and return its byte contribution for catalog
 * rollups. The contribution mirrors how each entry counts toward total bytes:
 *   - regular files: hardlink-aware credit (st_size on first inode visit, 0 on
 *     subsequent visits across the whole crawl via g_hardlink_registry)
 *   - dirs / symlinks / other: apparent st_size
 * Callers must thread this value into emit_record() so the writer can fold it
 * into the per-directory immediate-child rollup without recomputing the
 * hardlink dedup (which would double-count or undercount across threads).
 */
static uint64_t account_entry_local(shared_state_t *shared, crawl_stats_t *stats, perf_local_t *perf, const struct stat *st) {
    uint64_t byte_credit = 0;
    uint64_t apparent_size;
    uint64_t contrib = 0;
    regular_file_accounting_t rf;

    if (!shared || !stats || !perf || !st) return 0;

    memset(&rf, 0, sizeof(rf));
    apparent_size = (uint64_t)st->st_size;
    stats->total_entries++;
    perf->entries++;

    if (S_ISDIR(st->st_mode)) {
        stats->total_dirs++;
        stats->dir_apparent_bytes += apparent_size;
        perf->dirs++;
        contrib = apparent_size;
    } else if (S_ISREG(st->st_mode)) {
        stats->total_files++;
        perf->files++;
        regular_file_accounting(shared, stats, st, &rf);
        byte_credit = rf.byte_credit;
        stats->total_bytes += byte_credit;
        perf->bytes += byte_credit;
        stats->total_allocated_bytes += rf.allocated_bytes;
        perf->allocated_bytes += rf.allocated_bytes;
        stats->files_sparse_heuristic += rf.sparse_heuristic_inc;
        perf->files_sparse_heuristic += rf.sparse_heuristic_inc;
        contrib = byte_credit;
    } else if (S_ISLNK(st->st_mode)) {
        stats->total_symlinks++;
        stats->symlink_apparent_bytes += apparent_size;
        contrib = apparent_size;
    } else {
        stats->total_other++;
        stats->other_apparent_bytes += apparent_size;
        contrib = apparent_size;
    }

    return contrib;
}

static void stats_merge(shared_state_t *shared, const crawl_stats_t *local) {
    shared->total_entries += local->total_entries;
    shared->total_dirs += local->total_dirs;
    shared->total_files += local->total_files;
    shared->total_hardlink_files += local->total_hardlink_files;
    shared->total_symlinks += local->total_symlinks;
    shared->total_other += local->total_other;
    shared->total_bytes += local->total_bytes;
    shared->total_allocated_bytes += local->total_allocated_bytes;
    shared->files_sparse_heuristic += local->files_sparse_heuristic;
    shared->dir_apparent_bytes += local->dir_apparent_bytes;
    shared->symlink_apparent_bytes += local->symlink_apparent_bytes;
    shared->other_apparent_bytes += local->other_apparent_bytes;
}

static void stats_add_error(shared_state_t *s) {
    pthread_mutex_lock(&s->stats_mutex);
    s->total_errors++;
    pthread_mutex_unlock(&s->stats_mutex);
}

static void stats_add_crawl_thread_started(shared_state_t *s) {
    pthread_mutex_lock(&s->stats_mutex);
    s->crawl_threads_started++;
    pthread_mutex_unlock(&s->stats_mutex);
}

static void stats_merge_aux(shared_state_t *shared, const worker_aux_stats_t *local) {
    shared->donated_dirs += local->donated_dirs;
    shared->donation_attempts += local->donation_attempts;
    shared->donation_successes += local->donation_successes;
}

static void stats_add_donated_dirs_local(worker_aux_stats_t *s, uint64_t count) {
    s->donated_dirs += count;
    s->donation_successes += count;
}

static void stats_add_donation_attempt_local(worker_aux_stats_t *s, uint64_t count) {
    s->donation_attempts += count;
}

static void perf_flush_local(perf_local_t *perf) {
    /* Rolling-window globals / 1 Hz stats thread removed; crawl_stats_t is authoritative. */
    (void)perf;
}

static int write_bin_header(FILE *fp) {
    bin_file_header_t hdr;

    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, CRAWL_BIN_MAGIC, (size_t)CRAWL_BIN_MAGIC_LEN);
    hdr.version = FORMAT_VERSION;
    return ecrawl_io_fwrite(&hdr, sizeof(hdr), 1, fp) == 1 ? 0 : -1;
}

static void print_usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--no-write] [--no-stat [--count] [--contains <text>] [--print0]] "
            "[--statx] [--iouring] [--progress] "
            "[--verbose] [--record-root <abs-path>] <start-path> [output-dir]\n",
            prog);
    fprintf(stderr, "Example: %s /data1\n", prog);
    fprintf(stderr, "Example: %s /data1 /scratch/crawl_out\n", prog);
    fprintf(stderr, "Example: %s --record-root /storage/srv07 /mnt/server07 crawl_srv07\n", prog);
    fprintf(stderr, "Benchmark: %s --no-write /data1\n", prog);
    fprintf(stderr, "Count: %s --no-stat --count /data1\n", prog);
    fprintf(stderr, "Search: %s --no-stat --contains slurm- /data1\n", prog);
    fprintf(stderr,
            "--no-stat: walk names only (no inode reads) and stream paths to stdout; no capture is written.\n"
            "--count: with --no-stat, only count files/dirs/etc from d_type; do not print paths.\n"
            "--contains: keep paths whose full path contains <text>, case-insensitive "
            "(same rule as ereport_index --search). Requires --no-stat.\n"
            "--print0: NUL-separate the --no-stat stream for paths containing newlines.\n"
            "--progress: cheap live files=/entries= (donate-entry cadence, counted across directories); "
            "stderr, or stdout with --count. Not implied by --verbose.\n"
            "--statx: read inodes with statx(2) asking only for the fields this mode consumes "
            "(--no-write: type/size/blocks/nlink/ino; capture: BASIC_STATS).\n"
            "--iouring: batch each directory's inode reads through a per-worker io_uring "
            "(ECRAWL_IOURING_DEPTH, default 256, range 16..4096; ECRAWL_IOURING_MIN_BATCH, "
            "default 8: smaller collected batches are statx'd inline). The kernel opcode is "
            "STATX; the mask is the same as --statx.\n");
    fprintf(stderr,
            "Optional env: ECRAWL_CRAWL_THREADS (crawl threads, default %d, minimum 1), "
            "ECRAWL_WRITER_THREADS (default %d), ECRAWL_WRITER_QUEUE_BATCHES (per writer, default %u), "
            "ECRAWL_UID_SHARDS (power of 2, default %u), "
            "ECRAWL_MAX_OPEN_SHARDS (per writer, default %u, auto-capped by RLIMIT_NOFILE), "
            "ECRAWL_ID_RESOLVE_THREADS (uid/gid name lookups at finalize, default %d; 1=serial), "
            "ECRAWL_GETDENTS_BUF (raw getdents64 read-buffer bytes per crawl thread, default %u, "
            "range %u..%u; 0=use libc opendir/readdir).\n"
            "Donation (reduce task-queue mutex traffic): ECRAWL_DONATE_CHECK_EVERY (default %u: donate check every N "
            "DT_DIR pushes during readdir), ECRAWL_DONATE_ENTRY_CHECK_EVERY (default %u: also check every N dirents, "
            "which is what lets a deep narrow chain shed work; 0 disables), "
            "ECRAWL_DONATE_CHUNK_FORCE_MAX (default %u: max dirs per queue push when "
            "stack exceeds force threshold), ECRAWL_FORCE_DONATE_AT (default %u: spill stack to global queue above "
            "this depth), ECRAWL_DONATE_ALL_BUSY_MIN_STACK (default %u: when all crawl threads hold a task, still "
            "donate if local stack is at least this deep and queue is below started*MULT), "
            "ECRAWL_DONATE_ALL_BUSY_MAX_QDEPTH_MULT (default %u: MULT in that cap), "
            "ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH (default %u: coalesce discovered subdir enqueues into one queue push "
            "per batch).\n",
            DEFAULT_CRAWL_THREADS,
            DEFAULT_WRITER_THREADS,
            (unsigned)DEFAULT_WRITER_QUEUE_BATCHES,
            (unsigned)DEFAULT_UID_SHARDS,
            DEFAULT_MAX_OPEN_SHARDS,
            DEFAULT_ID_RESOLVE_THREADS,
            (unsigned)DEFAULT_GETDENTS_BUF,
            (unsigned)MIN_GETDENTS_BUF,
            (unsigned)MAX_GETDENTS_BUF,
            (unsigned)DEFAULT_DONATE_CHECK_EVERY,
            (unsigned)DEFAULT_DONATE_ENTRY_CHECK_EVERY,
            (unsigned)DONATE_CHUNK_FORCE_MAX,
            (unsigned)LOCAL_STACK_FORCE_DONATE_COUNT,
            (unsigned)DEFAULT_DONATE_ALL_BUSY_MIN_STACK,
            (unsigned)DEFAULT_DONATE_ALL_BUSY_MAX_QDEPTH_MULT,
            (unsigned)DEFAULT_DISCOVERED_DIR_ENQUEUE_BATCH);
    fprintf(stderr,
            "Trigram journal (--trigram-journal): ECRAWL_TRIGRAM_JOURNAL_THREADS (default %d), "
            "ECRAWL_TRIGRAM_JOURNAL_QUEUE_BATCHES (default %u).\n",
            DEFAULT_TRIJOURNAL_THREADS, (unsigned)DEFAULT_TRIJOURNAL_QUEUE_BATCHES);
    fprintf(stderr,
            "Diagnostics (with --verbose): ECRAWL_STALL_HINT_SECONDS=N warns on stderr after N consecutive "
            "seconds with zero rolling-window entries once the window is warm (default 5; 0=off).\n");
    fprintf(stderr,
            "--record-root: store paths in .bin as <root>/<relative-to-start-path> (resolved to absolute).\n"
            "--trigram-journal DIR: while crawling, also write per-shard trigram journals to DIR for\n"
            "    ereport_index --make --journal-dir (default off; incompatible with --no-write/--no-stat).\n");
    fprintf(stderr,
            "Default output is a concise summary. --verbose prints full metrics to stdout at exit.\n");
}

static int ensure_output_dir_exists(const char *path) {
    struct stat st;

    if (!path || path[0] == '\0') {
        errno = EINVAL;
        return -1;
    }

    if (ecrawl_io_stat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            errno = ENOTDIR;
            return -1;
        }
        return 0;
    }
    if (errno != ENOENT) return -1;
    return ecrawl_io_mkdir(path, 0775) == 0 ? 0 : -1;
}

static int crawl_output_artifact_should_delete(const char *name) {
    size_t len = strlen(name);

    if (strcmp(name, "crawl_manifest.txt") == 0) return 1;
    if (len < 15 || strncmp(name, "uid_shard_", 10) != 0) return 0;
    if (len >= 19 && strcmp(name + len - 9, ".bin.ckpt") == 0) return 1;
    return strcmp(name + len - 4, ".bin") == 0;
}

/* Remove leftover shard binaries/manifest from a previous run; interrupted crawls are not resumed. */
static int crawl_output_dir_scrub_prior_artifacts(void) {
    DIR *dir;
    struct dirent *de;
    char path[PATH_MAX];
    int n;

    dir = ecrawl_io_opendir(g_output_dir);
    if (!dir) {
        fprintf(stderr, "ERROR cannot open output directory %s: %s\n", g_output_dir, strerror(errno));
        return -1;
    }
    while ((de = ecrawl_io_readdir(dir)) != NULL) {
        const char *name = de->d_name;
        if (name[0] == '.') continue;
        if (!crawl_output_artifact_should_delete(name)) continue;
        n = snprintf(path, sizeof(path), "%s/%s", g_output_dir, name);
        if (n < 0 || (size_t)n >= sizeof(path)) {
            fprintf(stderr, "ERROR crawl scrub path too long under %s\n", g_output_dir);
            ecrawl_io_closedir(dir);
            errno = ENAMETOOLONG;
            return -1;
        }
        if (unlink(path) != 0 && errno != ENOENT) {
            fprintf(stderr, "ERROR unlink %s: %s\n", path, strerror(errno));
            ecrawl_io_closedir(dir);
            return -1;
        }
    }
    ecrawl_io_closedir(dir);
    return 0;
}

static int trijournal_artifact_should_delete(const char *name) {
    size_t len = strlen(name);

    if (len >= 8 && strcmp(name + len - 8, ".tij.tmp") == 0) return 1;
    return len >= 4 && strcmp(name + len - 4, ".tij") == 0;
}

/* Journals bind to one run's shards; a new run voids every leftover, published or not. */
static int trijournal_dir_scrub_prior_artifacts(void) {
    DIR *dir;
    struct dirent *de;
    char path[PATH_MAX];
    int n;

    dir = ecrawl_io_opendir(g_trijournal_dir);
    if (!dir) {
        fprintf(stderr, "ERROR cannot open --trigram-journal directory %s: %s\n", g_trijournal_dir,
                strerror(errno));
        return -1;
    }
    while ((de = ecrawl_io_readdir(dir)) != NULL) {
        const char *name = de->d_name;
        if (name[0] == '.') continue;
        if (!trijournal_artifact_should_delete(name)) continue;
        n = snprintf(path, sizeof(path), "%s/%s", g_trijournal_dir, name);
        if (n < 0 || (size_t)n >= sizeof(path)) {
            fprintf(stderr, "ERROR trigram journal scrub path too long under %s\n", g_trijournal_dir);
            ecrawl_io_closedir(dir);
            errno = ENAMETOOLONG;
            return -1;
        }
        if (unlink(path) != 0 && errno != ENOENT) {
            fprintf(stderr, "ERROR unlink %s: %s\n", path, strerror(errno));
            ecrawl_io_closedir(dir);
            return -1;
        }
    }
    ecrawl_io_closedir(dir);
    return 0;
}

static int build_default_output_dir(char *out, size_t out_sz) {
    static const char *months[] = {
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec"
    };
    char hostname_buf[256];
    time_t now;
    struct tm tm_now;
    const char *month = "unk";
    int n;

    if (!out || out_sz == 0) {
        errno = EINVAL;
        return -1;
    }

    memset(hostname_buf, 0, sizeof(hostname_buf));
    if (gethostname(hostname_buf, sizeof(hostname_buf) - 1) != 0) return -1;
    hostname_buf[sizeof(hostname_buf) - 1] = '\0';
    {
        char *dot = strchr(hostname_buf, '.');
        if (dot) *dot = '\0';
    }

    now = time(NULL);
    if (now == (time_t)-1) return -1;
    if (!localtime_r(&now, &tm_now)) return -1;
    if (tm_now.tm_mon >= 0 && tm_now.tm_mon < 12) month = months[tm_now.tm_mon];

    n = snprintf(out, out_sz, "%s_%s-%02d-%04d_%02d-%02d-%02d",
                 hostname_buf,
                 month,
                 tm_now.tm_mday,
                 tm_now.tm_year + 1900,
                 tm_now.tm_hour,
                 tm_now.tm_min,
                 tm_now.tm_sec);
    if (n < 0 || (size_t)n >= out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int build_shard_path(uint32_t shard, char *out, size_t out_sz) {
    int n = snprintf(out, out_sz, "%s/uid_shard_%0*u.bin", g_output_dir, g_shard_digits, shard);
    return (n < 0 || (size_t)n >= out_sz) ? -1 : 0;
}

/* Basename only; the trigram journal pool names <journal_dir>/<basename>.tij after the shard. */
static int ecrawl_shard_basename(uint32_t shard, char *out, size_t out_sz) {
    int n = snprintf(out, out_sz, "uid_shard_%0*u.bin", g_shard_digits, shard);
    return (n < 0 || (size_t)n >= out_sz) ? -1 : 0;
}

static int ckpt_sidecar_path(const char *bin_path, char *out, size_t out_sz) {
    int n = snprintf(out, out_sz, "%s.ckpt", bin_path);
    return (n < 0 || (size_t)n >= out_sz) ? -1 : 0;
}

static uint32_t shard_cat_hash_str(const char *s) {
    uint32_t h = 2166136261u;
    while (s && *s) {
        h ^= (uint32_t)(unsigned char)*s++;
        h *= 16777619u;
    }
    return h; /* full hash; callers mask with c->ht_mask */
}

static void shard_cat_destroy(shard_cat_t *c) {
    shard_cat_path_entry_t *e;
    if (!c) return;
    /* Free entries via the global insertion chain (O(entries)), then the bucket array and the parallel
     * dir arrays. The struct is small now (ht is a heap pointer, not a fixed 512 KiB array), so a final
     * memset is cheap and leaves the catalog in its freshly-zeroed state for reuse. */
    e = c->all_head;
    while (e) {
        shard_cat_path_entry_t *nx = e->all_next;
        free(e->path_key);
        free(e);
        e = nx;
    }
    free(c->ht);
    free(c->parent_dir_id);
    free(c->depth);
    free(c->name_len);
    if (c->name_comp) {
        size_t i;
        for (i = 0; i < c->arr_cap; i++) free(c->name_comp[i]);
        free(c->name_comp);
    }
    free(c->imm_child_bytes);
    free(c->imm_child_count);
    free(c->imm_child_ctime_led_count);
    free(c->imm_child_min_eff_time);
    free(c->imm_child_max_eff_time);
    free(c->subtree_nlink_gt1_count);
    free(c->subtree_files);
    free(c->subtree_dirs);
    free(c->subtree_symlinks);
    free(c->self_bytes);
    free(c->self_present);
    free(c->dfs_index);
    free(c->dfs_subtree_dirs);
    free(c->subtree_bytes);
    free(c->subtree_count);
    memset(c, 0, sizeof(*c));
}

static int shard_cat_grow_arrays(shard_cat_t *c, uint64_t need_id) {
    uint64_t ncap = c->arr_cap ? c->arr_cap : 8;
    uint64_t i;
    uint64_t *pp;
    uint32_t *dp;
    uint16_t *nl;
    char **nm;
    uint64_t *icb;
    uint64_t *icc;
    uint64_t *icl;
    uint64_t *icmin;
    uint64_t *icmax;
    uint64_t *ichl;
    uint64_t *icf;
    uint64_t *icd;
    uint64_t *ics;
    uint64_t *slfb;
    unsigned char *slfp;

    while (need_id >= ncap) ncap *= 2;
    if (need_id >= ncap) return -1;

    pp = (uint64_t *)realloc(c->parent_dir_id, (size_t)ncap * sizeof(*pp));
    dp = (uint32_t *)realloc(c->depth, (size_t)ncap * sizeof(*dp));
    nl = (uint16_t *)realloc(c->name_len, (size_t)ncap * sizeof(*nl));
    nm = (char **)realloc(c->name_comp, (size_t)ncap * sizeof(*nm));
    icb = (uint64_t *)realloc(c->imm_child_bytes, (size_t)ncap * sizeof(*icb));
    icc = (uint64_t *)realloc(c->imm_child_count, (size_t)ncap * sizeof(*icc));
    icl = (uint64_t *)realloc(c->imm_child_ctime_led_count, (size_t)ncap * sizeof(*icl));
    icmin = (uint64_t *)realloc(c->imm_child_min_eff_time, (size_t)ncap * sizeof(*icmin));
    icmax = (uint64_t *)realloc(c->imm_child_max_eff_time, (size_t)ncap * sizeof(*icmax));
    ichl = (uint64_t *)realloc(c->subtree_nlink_gt1_count, (size_t)ncap * sizeof(*ichl));
    icf = (uint64_t *)realloc(c->subtree_files, (size_t)ncap * sizeof(*icf));
    icd = (uint64_t *)realloc(c->subtree_dirs, (size_t)ncap * sizeof(*icd));
    ics = (uint64_t *)realloc(c->subtree_symlinks, (size_t)ncap * sizeof(*ics));
    slfb = (uint64_t *)realloc(c->self_bytes, (size_t)ncap * sizeof(*slfb));
    slfp = (unsigned char *)realloc(c->self_present, (size_t)ncap);
    if (!pp || !dp || !nl || !nm || !icb || !icc || !icl || !icmin || !icmax || !ichl || !icf || !icd ||
        !ics || !slfb || !slfp)
        return -1;
    c->parent_dir_id = pp;
    c->depth = dp;
    c->name_len = nl;
    c->name_comp = nm;
    c->imm_child_bytes = icb;
    c->imm_child_count = icc;
    c->imm_child_ctime_led_count = icl;
    c->imm_child_min_eff_time = icmin;
    c->imm_child_max_eff_time = icmax;
    c->subtree_nlink_gt1_count = ichl;
    c->subtree_files = icf;
    c->subtree_dirs = icd;
    c->subtree_symlinks = ics;
    c->self_bytes = slfb;
    c->self_present = slfp;
    for (i = c->arr_cap; i < ncap; i++) {
        c->parent_dir_id[i] = 0;
        c->depth[i] = 0;
        c->name_len[i] = 0;
        c->name_comp[i] = NULL;
        c->imm_child_bytes[i] = 0;
        c->imm_child_count[i] = 0;
        c->imm_child_ctime_led_count[i] = 0;
        c->imm_child_min_eff_time[i] = UINT64_MAX;
        c->imm_child_max_eff_time[i] = 0;
        c->subtree_nlink_gt1_count[i] = 0;
        c->subtree_files[i] = 0;
        c->subtree_dirs[i] = 0;
        c->subtree_symlinks[i] = 0;
        c->self_bytes[i] = 0;
        c->self_present[i] = 0;
    }
    c->arr_cap = (size_t)ncap;
    return 0;
}

static int shard_cat_ht_alloc(shard_cat_t *c, size_t nbuckets) {
    shard_cat_path_entry_t **t = (shard_cat_path_entry_t **)calloc(nbuckets, sizeof(*t));
    if (!t) return -1;
    c->ht = t;
    c->ht_mask = nbuckets - 1U;
    return 0;
}

/* Double the bucket array and re-link every entry into its new bucket (O(entries), no re-hashing of
 * strings since each entry caches its full hash). Single-threaded per shard, so no locking. */
static int shard_cat_ht_grow(shard_cat_t *c) {
    size_t new_buckets = (c->ht_mask + 1U) << 1;
    shard_cat_path_entry_t **t = (shard_cat_path_entry_t **)calloc(new_buckets, sizeof(*t));
    shard_cat_path_entry_t *e;

    if (!t) return -1;
    for (e = c->all_head; e; e = e->all_next) {
        size_t b = (size_t)(e->hash & (uint32_t)(new_buckets - 1U));
        e->next = t[b];
        t[b] = e;
    }
    free(c->ht);
    c->ht = t;
    c->ht_mask = new_buckets - 1U;
    return 0;
}

static int shard_cat_ht_insert(shard_cat_t *c, char *path_owned, uint64_t dir_id) {
    uint32_t h;
    size_t b;
    shard_cat_path_entry_t *e;

    if (!c->ht && shard_cat_ht_alloc(c, SHARD_CAT_HT_INIT_BUCKETS) != 0) {
        free(path_owned);
        return -1;
    }
    /* Keep load factor below 0.75; a failed grow is non-fatal (chains just get longer). */
    if (c->ht_count >= ((c->ht_mask + 1U) * 3U) / 4U) (void)shard_cat_ht_grow(c);

    h = shard_cat_hash_str(path_owned);
    e = (shard_cat_path_entry_t *)malloc(sizeof(*e));
    if (!e) {
        free(path_owned);
        return -1;
    }
    b = (size_t)(h & (uint32_t)c->ht_mask);
    e->path_key = path_owned;
    e->dir_id = dir_id;
    e->hash = h;
    e->next = c->ht[b];
    c->ht[b] = e;
    e->all_next = c->all_head;
    c->all_head = e;
    c->ht_count++;
    return 0;
}

static uint64_t shard_cat_lookup_dir_id(const shard_cat_t *c, const char *path_z) {
    uint32_t h;
    shard_cat_path_entry_t *e;

    if (!c->ht) return 0;
    h = shard_cat_hash_str(path_z);
    e = c->ht[h & (uint32_t)c->ht_mask];
    while (e) {
        if (e->hash == h && strcmp(e->path_key, path_z) == 0) return e->dir_id;
        e = e->next;
    }
    return 0;
}

static int shard_cat_init_fresh(shard_cat_t *c) {
    memset(c, 0, sizeof(*c));
    if (shard_cat_grow_arrays(c, 2) != 0) return -1;
    c->parent_dir_id[1] = 0;
    c->depth[1] = 0;
    c->name_len[1] = 0;
    c->name_comp[1] = NULL;
    c->next_dir_id = 2;
    {
        char *root_key = strdup("");
        if (!root_key) return -1;
        if (shard_cat_ht_insert(c, root_key, 1ULL) != 0) return -1;
    }
    return 0;
}

static int shard_cat_load_from_disk_catalog(shard_cat_t *c, const crawl_bin_catalog_t *L) {
    uint64_t id;

    shard_cat_destroy(c);
    if (!L || L->max_dir_id == 0ULL) return shard_cat_init_fresh(c);

    if (shard_cat_grow_arrays(c, L->max_dir_id + 1ULL) != 0) return -1;
    c->next_dir_id = L->max_dir_id + 1ULL;

    for (id = 1; id <= L->max_dir_id; id++) {
        char pb[PATH_MAX];
        char *key;

        if (crawl_bin_catalog_dir_path(L, id, pb, sizeof(pb)) != 0) {
            shard_cat_destroy(c);
            return -1;
        }
        key = strdup(pb);
        if (!key) {
            shard_cat_destroy(c);
            return -1;
        }
        c->parent_dir_id[id] = L->parent_dir_id[id];
        c->depth[id] = L->depth[id];
        c->name_len[id] = L->name_len[id];
        if (L->name_len[id] > 0 && L->name_comp[id]) {
            /* Bounded by name_len: a loaded catalog may borrow its names from a mapping, where they
             * sit back to back with no NUL. ecrawl's own copy stays owned and terminated. */
            c->name_comp[id] = strndup(L->name_comp[id], L->name_len[id]);
            if (!c->name_comp[id]) {
                free(key);
                shard_cat_destroy(c);
                return -1;
            }
        } else {
            c->name_comp[id] = NULL;
        }
        c->imm_child_bytes[id] = L->imm_child_bytes[id];
        c->imm_child_count[id] = L->imm_child_count[id];
        c->imm_child_ctime_led_count[id] = L->imm_child_ctime_led_count[id];
        c->imm_child_min_eff_time[id] = L->imm_child_min_eff_time[id];
        c->imm_child_max_eff_time[id] = L->imm_child_max_eff_time[id];
        /* Pre-finalize this holds the immediate-child count, which is what an
         * evicted shard must resume accumulating from. */
        c->subtree_nlink_gt1_count[id] = L->subtree_nlink_gt1_count[id];
        c->subtree_files[id] = L->subtree_files[id];
        c->subtree_dirs[id] = L->subtree_dirs[id];
        c->subtree_symlinks[id] = L->subtree_symlinks[id];
        c->self_bytes[id] = L->self_bytes[id];
        c->self_present[id] = L->self_present[id];
        if (shard_cat_ht_insert(c, key, id) != 0) {
            shard_cat_destroy(c);
            return -1;
        }
    }
    return 0;
}

static int split_parent_basename(const char *path_z, char *parent, size_t parent_sz, const char **base_out,
                                 size_t *base_len_out) {
    const char *slash = strrchr(path_z, '/');

    if (!slash) {
        if (parent_sz < 1) return -1;
        parent[0] = '\0';
        *base_out = path_z;
        *base_len_out = strlen(path_z);
        return 0;
    }
    if ((size_t)(slash - path_z) >= parent_sz) return -1;
    memcpy(parent, path_z, (size_t)(slash - path_z));
    parent[slash - path_z] = '\0';
    *base_out = slash + 1;
    *base_len_out = strlen(*base_out);
    return 0;
}

static uint64_t shard_cat_ensure_dir(shard_cat_t *c, const char *path_z) {
    char parent[PATH_MAX];
    const char *base;
    size_t base_len;
    uint64_t pid;
    uint64_t nid;
    char *path_owned;
    char *comp_owned;

    if (!path_z || path_z[0] == '\0') return 1ULL;

    {
        uint64_t ex = shard_cat_lookup_dir_id(c, path_z);
        if (ex != 0ULL) return ex;
    }

    if (split_parent_basename(path_z, parent, sizeof(parent), &base, &base_len) != 0) return 0;
    if (base_len > (size_t)UINT16_MAX) return 0;

    pid = shard_cat_ensure_dir(c, parent);
    if (pid == 0ULL) return 0ULL;

    nid = c->next_dir_id++;
    if (shard_cat_grow_arrays(c, nid) != 0) return 0;
    comp_owned = (char *)malloc(base_len + 1);
    path_owned = strdup(path_z);
    if (!comp_owned || !path_owned) {
        free(comp_owned);
        free(path_owned);
        return 0;
    }
    memcpy(comp_owned, base, base_len);
    comp_owned[base_len] = '\0';

    c->parent_dir_id[nid] = pid;
    c->depth[nid] = c->depth[pid] + 1U;
    c->name_len[nid] = (uint16_t)base_len;
    c->name_comp[nid] = comp_owned;

    if (shard_cat_ht_insert(c, path_owned, nid) != 0) {
        free(comp_owned);
        c->name_comp[nid] = NULL;
        return 0;
    }
    return nid;
}

/* Fold a single emitted record into the rollup of its on-disk parent. */
static void shard_cat_update_imm_child_rollup(shard_cat_t *c, uint64_t pid, uint64_t byte_credit,
                                              const bin_record_hdr_t *r) {
    uint64_t eff;

    if (!c || pid == 0ULL || (size_t)pid >= c->arr_cap) return;

    c->imm_child_bytes[pid] += byte_credit;
    c->imm_child_count[pid]++;
    eff = crawl_bin_record_eff_time(r);
    if (eff < c->imm_child_min_eff_time[pid]) c->imm_child_min_eff_time[pid] = eff;
    if (eff > c->imm_child_max_eff_time[pid]) c->imm_child_max_eff_time[pid] = eff;
    if (crawl_bin_record_ctime_led(r)) c->imm_child_ctime_led_count[pid]++;
    /* Matches the set of records the query side treats as hardlink-ambiguous
     * (any non-directory with nlink > 1), so a zero total genuinely means the
     * byte rollup equals what an exact scan would compute. */
    if (r->type != (uint8_t)'d' && r->nlink > 1ULL) c->subtree_nlink_gt1_count[pid]++;
    if (r->type == (uint8_t)'f')
        c->subtree_files[pid]++;
    else if (r->type == (uint8_t)'d')
        c->subtree_dirs[pid]++;
    else if (r->type == (uint8_t)'l')
        c->subtree_symlinks[pid]++;
}

/* Record a directory record's own byte credit on that directory's catalog row. */
static void shard_cat_set_self_bytes(shard_cat_t *c, uint64_t dir_id, uint64_t byte_credit) {
    if (!c || dir_id == 0ULL || (size_t)dir_id >= c->arr_cap) return;
    c->self_bytes[dir_id] = byte_credit;
    c->self_present[dir_id] = 1U;
}

/*
 * End-of-crawl post-pass, O(directories): assign each directory its DFS
 * pre-order position and roll the immediate-child aggregates up the tree.
 *
 * dir_id is handed out in crawl arrival order and is referenced by every
 * record's parent_dir_id, so it cannot be renumbered into DFS order without
 * rewriting the whole record region. dfs_index is therefore a permutation
 * alongside it, which is enough to make subtree membership a range test.
 *
 * Must run exactly once, on final close. Running it on an LRU eviction would
 * fold the same immediate counts into their ancestors a second time when the
 * shard reopened and finalized again.
 */
static int shard_cat_finalize(shard_cat_t *c) {
    uint64_t n;
    uint64_t *child_head = NULL;   /* first child of each dir, 0 = none */
    uint64_t *child_next = NULL;   /* next sibling */
    uint64_t *order = NULL;        /* dfs position -> dir_id */
    uint64_t *stack = NULL;
    uint64_t id, top = 0, pos = 0;
    int rc = -1;

    if (!c) return -1;
    if (c->finalized) return 0;
    if (c->next_dir_id <= 1ULL) return 0;
    n = c->next_dir_id; /* slots 1..n-1 are live */

    c->dfs_index = (uint64_t *)calloc((size_t)n, sizeof(uint64_t));
    c->dfs_subtree_dirs = (uint64_t *)calloc((size_t)n, sizeof(uint64_t));
    c->subtree_bytes = (uint64_t *)calloc((size_t)n, sizeof(uint64_t));
    c->subtree_count = (uint64_t *)calloc((size_t)n, sizeof(uint64_t));
    child_head = (uint64_t *)calloc((size_t)n, sizeof(uint64_t));
    child_next = (uint64_t *)calloc((size_t)n, sizeof(uint64_t));
    order = (uint64_t *)calloc((size_t)n, sizeof(uint64_t));
    stack = (uint64_t *)calloc((size_t)n, sizeof(uint64_t));
    if (!c->dfs_index || !c->dfs_subtree_dirs || !c->subtree_bytes || !c->subtree_count || !child_head ||
        !child_next || !order || !stack)
        goto done;

    /* Sibling lists, built by prepending so children of a parent end up in
     * descending dir_id; the traversal below pushes them onto a stack, which
     * flips them back to ascending. */
    for (id = n - 1ULL; id >= 1ULL; id--) {
        uint64_t p = c->parent_dir_id[id];

        if (p == 0ULL || p >= n || p == id) continue; /* the root has no parent */
        child_next[id] = child_head[p];
        child_head[p] = id;
    }

    /* Iterative pre-order from the synthetic root; recursion would blow the
     * stack on a deep tree, and directory depth is not bounded here. */
    stack[top++] = 1ULL;
    while (top > 0) {
        uint64_t d = stack[--top];
        uint64_t ch;

        if (pos >= n) goto done;
        c->dfs_index[d] = pos;
        order[pos++] = d;
        for (ch = child_head[d]; ch != 0ULL; ch = child_next[ch]) {
            if (top >= n) goto done;
            stack[top++] = ch;
        }
    }

    /*
     * A directory unreachable from the root would keep dfs_index 0 and alias the
     * root's subtree range, silently pulling unrelated directories into every
     * subtree query. Refuse instead of answering wrongly.
     */
    if (pos != n - 1ULL) {
        errno = EINVAL;
        goto done;
    }

    /* Seed with this directory's own immediate aggregates, then roll up. */
    for (id = 1ULL; id < n; id++) {
        c->subtree_bytes[id] = c->imm_child_bytes[id];
        c->subtree_count[id] = c->imm_child_count[id];
        c->dfs_subtree_dirs[id] = 1ULL;
    }

    /* Reverse pre-order visits every descendant before its ancestor, so each
     * directory's totals are complete by the time they fold into its parent. */
    while (pos > 0) {
        uint64_t d = order[--pos];
        uint64_t p = c->parent_dir_id[d];

        if (d == 1ULL || p == 0ULL || p >= n || p == d) continue;
        c->subtree_bytes[p] += c->subtree_bytes[d];
        c->subtree_count[p] += c->subtree_count[d];
        c->subtree_nlink_gt1_count[p] += c->subtree_nlink_gt1_count[d];
        c->subtree_files[p] += c->subtree_files[d];
        c->subtree_dirs[p] += c->subtree_dirs[d];
        c->subtree_symlinks[p] += c->subtree_symlinks[d];
        c->dfs_subtree_dirs[p] += c->dfs_subtree_dirs[d];
    }

    c->finalized = 1;
    rc = 0;

done:
    free(child_head);
    free(child_next);
    free(order);
    free(stack);
    return rc;
}

static int shard_cat_write_tail(shard_cat_t *c, FILE *fp, uint64_t *catalog_start_out) {
    crawl_bin_catalog_src_t src;
    off_t st;

    if (!c || !fp || !catalog_start_out) return -1;
    st = ftello(fp);
    if (st < 0) return -1;
    *catalog_start_out = (uint64_t)st;

    if (c->next_dir_id <= 1ULL) {
        errno = EINVAL;
        return -1;
    }

    memset(&src, 0, sizeof(src));
    src.n_entries = c->next_dir_id - 1ULL;
    src.parent_dir_id = c->parent_dir_id;
    src.depth = c->depth;
    src.name_len = c->name_len;
    src.name_comp = c->name_comp;
    src.imm_child_bytes = c->imm_child_bytes;
    src.imm_child_count = c->imm_child_count;
    src.imm_child_ctime_led_count = c->imm_child_ctime_led_count;
    src.imm_child_min_eff_time = c->imm_child_min_eff_time;
    src.imm_child_max_eff_time = c->imm_child_max_eff_time;
    src.self_present = c->self_present;
    src.self_bytes = c->self_bytes;
    src.subtree_nlink_gt1_count = c->subtree_nlink_gt1_count;
    src.subtree_files = c->subtree_files;
    src.subtree_dirs = c->subtree_dirs;
    src.subtree_symlinks = c->subtree_symlinks;
    /* NULL until the final close runs the post-pass, which the writer turns into
     * a column of zeroes. An interim tail written on LRU eviction is only ever
     * read back by the reopen path, which uses the imm_child_* fields and the
     * raw nlink counter above. */
    if (c->finalized) {
        src.dfs_index = c->dfs_index;
        src.dfs_subtree_dirs = c->dfs_subtree_dirs;
        src.subtree_bytes = c->subtree_bytes;
        src.subtree_count = c->subtree_count;
    }
    return crawl_bin_catalog_write(&src, fp, ecrawl_io_fwrite);
}

static int patch_bin_header_catalog_offset(FILE *fp, uint64_t catalog_off) {
    bin_file_header_t hdr;

    if (!fp) return -1;
    if (fseeko(fp, 0, SEEK_SET) != 0) return -1;
    if (fread(&hdr, sizeof(hdr), 1, fp) != 1) return -1;
    hdr.catalog_offset = catalog_off;
    if (fseeko(fp, 0, SEEK_SET) != 0) return -1;
    return ecrawl_io_fwrite(&hdr, sizeof(hdr), 1, fp) == 1 ? 0 : -1;
}

static void shard_ckpt_free(shard_file_state_t *s) {
    free(s->ckpt_offs);
    s->ckpt_offs = NULL;
    s->ckpt_n = 0;
    s->ckpt_cap = 0;
}

static int shard_ckpt_push(shard_file_state_t *s, uint64_t off) {
    if (s->ckpt_n == s->ckpt_cap) {
        size_t ncap = s->ckpt_cap ? s->ckpt_cap * 2 : 16;
        uint64_t *p = (uint64_t *)realloc(s->ckpt_offs, ncap * sizeof(*p));
        if (!p) return -1;
        s->ckpt_offs = p;
        s->ckpt_cap = ncap;
    }
    s->ckpt_offs[s->ckpt_n++] = off;
    return 0;
}

/*
 * Sidecar I/O deliberately bypasses stdio. Every stream a process opens goes on one glibc-global
 * list under one lock, so the burst of sidecar writes when a crawl closes its shards serializes
 * against every other fopen/fclose in flight — and a sidecar is one header plus one array, which
 * needs no buffering. write()/pread() on a raw fd produce the identical bytes.
 */
static int write_all_fd(int fd, const void *buf, size_t n) {
    const unsigned char *p = (const unsigned char *)buf;

    while (n > 0) {
        ssize_t w = write(fd, p, n);

        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) {
            errno = EIO;
            return -1;
        }
        p += (size_t)w;
        n -= (size_t)w;
    }
    return 0;
}

static int pread_all_fd(int fd, void *buf, size_t n, off_t off) {
    unsigned char *p = (unsigned char *)buf;

    while (n > 0) {
        ssize_t r = pread(fd, p, n, off);

        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) {
            errno = EINVAL; /* short file */
            return -1;
        }
        p += (size_t)r;
        n -= (size_t)r;
        off += (off_t)r;
    }
    return 0;
}

static int shard_ckpt_write_sidecar(const char *bin_path, const uint64_t *offs, size_t n) {
    char ckpath[PATH_MAX];
    crawl_ckpt_file_hdr_t ch;
    int fd;

    if (!offs || n == 0) return -1;
    if (ckpt_sidecar_path(bin_path, ckpath, sizeof(ckpath)) != 0) return -1;
    ATOMIC_ADD_RELAXED(&g_shard_ckpt_writes, 1);
    fd = open(ckpath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    memset(&ch, 0, sizeof(ch));
    memcpy(ch.magic, CRAWL_CKPT_MAGIC, CRAWL_CKPT_MAGIC_LEN);
    ch.version = CRAWL_CKPT_ONDISK_VERSION;
    ch.stride_bytes = CRAWL_CKPT_STRIDE_BYTES;
    ch.num_offsets = (uint64_t)n;
    if (write_all_fd(fd, &ch, sizeof(ch)) != 0 || write_all_fd(fd, offs, n * sizeof(uint64_t)) != 0) {
        int e = errno ? errno : EIO;

        (void)close(fd);
        errno = e;
        return -1;
    }
    return close(fd) == 0 ? 0 : -1;
}

static int shard_ckpt_read_sidecar(const char *bin_path, uint64_t record_region_end, uint64_t **offs_out,
                                   size_t *n_out) {
    char ckpath[PATH_MAX];
    crawl_ckpt_file_hdr_t ch;
    uint64_t *buf = NULL;
    size_t i;
    int fd;

    *offs_out = NULL;
    *n_out = 0;
    if (ckpt_sidecar_path(bin_path, ckpath, sizeof(ckpath)) != 0) return -1;
    fd = open(ckpath, O_RDONLY);
    if (fd < 0) return -1;
    if (pread_all_fd(fd, &ch, sizeof(ch), 0) != 0) {
        (void)close(fd);
        errno = EINVAL;
        return -1;
    }
    if (memcmp(ch.magic, CRAWL_CKPT_MAGIC, CRAWL_CKPT_MAGIC_LEN) != 0 || ch.version != CRAWL_CKPT_ONDISK_VERSION ||
        ch.stride_bytes != CRAWL_CKPT_STRIDE_BYTES || ch.num_offsets == 0 || ch.num_offsets > (uint64_t)(SIZE_MAX / sizeof(uint64_t))) {
        (void)close(fd);
        errno = EINVAL;
        return -1;
    }
    buf = (uint64_t *)malloc((size_t)ch.num_offsets * sizeof(*buf));
    if (!buf) {
        int e = errno ? errno : ENOMEM;

        (void)close(fd);
        errno = e;
        return -1;
    }
    if (pread_all_fd(fd, buf, (size_t)ch.num_offsets * sizeof(uint64_t), (off_t)sizeof(ch)) != 0 || close(fd) != 0) {
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
        if (buf[i] <= buf[i - 1] || buf[i] >= record_region_end) {
            free(buf);
            errno = EINVAL;
            return -1;
        }
    }
    *offs_out = buf;
    *n_out = (size_t)ch.num_offsets;
    return 0;
}

static int shard_ckpt_rebuild_scan(const char *bin_path, uint64_t file_sz, uint64_t **offs_out, size_t *n_out,
                                   uint64_t *seg_start_out) {
    FILE *fp;
    uint64_t *buf = NULL;
    size_t n, cap;
    uint64_t seg0;
    off_t pos;
    bin_file_header_t fh;
    uint64_t scan_end;

    *offs_out = NULL;
    *n_out = 0;
    fp = ecrawl_io_fopen(bin_path, "rb");
    if (!fp) return -1;
    if (file_sz < sizeof(bin_file_header_t)) {
        ecrawl_io_fclose(fp);
        errno = EINVAL;
        return -1;
    }
    if (fread(&fh, sizeof(fh), 1, fp) != 1) {
        ecrawl_io_fclose(fp);
        errno = EINVAL;
        return -1;
    }
    scan_end = file_sz;
    if (fh.catalog_offset != 0ULL) {
        if (fh.catalog_offset < sizeof(fh) || fh.catalog_offset > file_sz) {
            ecrawl_io_fclose(fp);
            errno = EINVAL;
            return -1;
        }
        scan_end = fh.catalog_offset;
    }

    buf = (uint64_t *)malloc(16 * sizeof(*buf));
    if (!buf) {
        ecrawl_io_fclose(fp);
        return -1;
    }
    n = 1;
    cap = 16;
    buf[0] = sizeof(bin_file_header_t);
    seg0 = sizeof(bin_file_header_t);

    if (fseeko(fp, (off_t)sizeof(bin_file_header_t), SEEK_SET) != 0) goto fail;
    pos = (off_t)sizeof(bin_file_header_t);

    /* Walk the self-describing row groups, recording group-start offsets at
     * stride boundaries. A group's total size comes from its header alone, so
     * this never touches the column directory or any payload. */
    while ((uint64_t)pos < scan_end) {
        uint64_t blk_start = (uint64_t)pos;
        bin_rowgroup_hdr_t rg;
        uint64_t block_end;
        uint64_t group_total;

        if (blk_start - seg0 >= CRAWL_CKPT_STRIDE_BYTES) {
            if (n == cap) {
                size_t ncap = cap * 2;
                uint64_t *p = (uint64_t *)realloc(buf, ncap * sizeof(*p));
                if (!p) goto fail;
                buf = p;
                cap = ncap;
            }
            buf[n++] = blk_start;
            seg0 = blk_start;
        }
        if (scan_end - blk_start < sizeof(rg)) goto fail;
        if (fread(&rg, sizeof(rg), 1, fp) != 1) goto fail;
        group_total = crawl_bin_rowgroup_total_bytes(&rg);
        block_end = blk_start + group_total;
        if (block_end > scan_end) goto fail;
        if (fseeko(fp, (off_t)(group_total - sizeof(rg)), SEEK_CUR) != 0) goto fail;
        pos = (off_t)block_end;
    }
    if ((uint64_t)pos != scan_end) {
        errno = EINVAL;
        goto fail;
    }
    ecrawl_io_fclose(fp);
    *offs_out = buf;
    *n_out = n;
    *seg_start_out = seg0;
    return 0;
fail:
    ecrawl_io_fclose(fp);
    free(buf);
    if (errno == 0) errno = EINVAL;
    return -1;
}

static int shard_ckpt_load_for_append(shard_file_state_t *s, const char *bin_path, uint64_t file_sz) {
    uint64_t *rd = NULL;
    size_t rn = 0;

    shard_ckpt_free(s);
    if (shard_ckpt_read_sidecar(bin_path, file_sz, &rd, &rn) == 0) {
        s->ckpt_offs = rd;
        s->ckpt_n = rn;
        s->ckpt_cap = rn;
        s->seg_start_byte = rd[rn - 1];
        errno = 0;
        return 0;
    }
    /* Missing *.bin.ckpt is normal; do not leak ENOENT into rebuild failure reporting. */
    if (errno == ENOENT) errno = 0;
    if (shard_ckpt_rebuild_scan(bin_path, file_sz, &rd, &rn, &s->seg_start_byte) == 0) {
        s->ckpt_offs = rd;
        s->ckpt_n = rn;
        s->ckpt_cap = rn;
        errno = 0;
        return 0;
    }
    if (errno == 0 || errno == ENOENT) errno = EINVAL;
    return -1;
}

static int shard_ckpt_init_new(shard_file_state_t *s) {
    shard_ckpt_free(s);
    s->ckpt_offs = (uint64_t *)malloc(16 * sizeof(*s->ckpt_offs));
    if (!s->ckpt_offs) return -1;
    s->ckpt_cap = 16;
    s->ckpt_offs[0] = sizeof(bin_file_header_t);
    s->ckpt_n = 1;
    s->seg_start_byte = sizeof(bin_file_header_t);
    return 0;
}

static int shard_block_flush(shard_file_state_t *s);

/*
 * Write the shard's catalog tail and checkpoint sidecar. `final` marks the last
 * close of this shard for the whole crawl, which is the only point at which the
 * DFS/subtree post-pass may run: an LRU eviction is followed by a reopen that
 * keeps appending, and folding the rollups twice would double-count.
 */
static int shard_flush_ckpt_before_close(shard_file_state_t *s, const char *bin_path, int final) {
    uint64_t cat_off;
    int r;

    if (!s->fp) return 0;
    if (!s->ckpt_offs || s->ckpt_n == 0) return -1;

    /* Flush the pending row group so bytes_written == EOF and the catalog is
     * written immediately after the last group. */
    if (shard_block_flush(s) != 0) return -1;

    if (final && shard_cat_finalize(&s->cat) != 0) return -1;

    if (ecrawl_io_fflush(s->fp) != 0) return -1;
    if (fseeko(s->fp, 0, SEEK_END) != 0) return -1;
    if ((uint64_t)ftello(s->fp) != s->bytes_written) {
        errno = EINVAL;
        return -1;
    }
    if (shard_cat_write_tail(&s->cat, s->fp, &cat_off) != 0) return -1;
    if (cat_off != s->bytes_written) {
        errno = EINVAL;
        return -1;
    }
    if (ecrawl_io_fflush(s->fp) != 0) return -1;
    if (patch_bin_header_catalog_offset(s->fp, cat_off) != 0) return -1;
    if (ecrawl_io_fflush(s->fp) != 0) return -1;

    /* Persist the catalog and sidecar so the on-disk shard stays complete/recoverable, but keep
     * the in-memory catalog/checkpoint resident: a later reopen reuses them instead of
     * reloading from disk. Final teardown frees them via shard_state_release(). */
    r = shard_ckpt_write_sidecar(bin_path, s->ckpt_offs, s->ckpt_n);
    return r;
}

/* Release a shard's retained in-memory catalog/ckpt (call once the shard will not be reopened). */
static void shard_state_release(shard_file_state_t *s) {
    shard_ckpt_free(s);
    shard_cat_destroy(&s->cat);
    s->seg_start_byte = 0;
    s->cat_live = 0;
}

static task_node_t *task_node_take_alloc(task_queue_t *q) {
    task_node_t *node;

    if (q->node_free) {
        node = q->node_free;
        q->node_free = node->next;
        if (q->node_free_count > 0) q->node_free_count--;
        return node;
    }
    return (task_node_t *)malloc(sizeof(*node));
}

static void task_node_recycle(task_queue_t *q, task_node_t *node) {
    if (!node) return;
    pthread_mutex_lock(&q->mutex);
    if (q->node_free_count < (size_t)TASK_NODE_FREE_MAX) {
        node->next = q->node_free;
        q->node_free = node;
        q->node_free_count++;
    } else {
        free(node);
    }
    pthread_mutex_unlock(&q->mutex);
}

static void queue_init(task_queue_t *q) {
    memset(q, 0, sizeof(*q));
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
}

static void queue_destroy(task_queue_t *q) {
    task_node_t *cur, *next;

    pthread_mutex_lock(&q->mutex);
    cur = q->head;
    while (cur) {
        dir_stack_t task = {cur->items, cur->count, cur->cap};
        next = cur->next;
        dir_stack_destroy(&task);
        free(cur);
        cur = next;
    }
    cur = q->node_free;
    while (cur) {
        next = cur->next;
        free(cur);
        cur = next;
    }
    q->node_free = NULL;
    q->node_free_count = 0;
    pthread_mutex_unlock(&q->mutex);

    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
}

static int queue_push_stack_take(task_queue_t *q, dir_stack_t *task) {
    task_node_t *node;

    if (!task || task->count == 0) return 0;

    pthread_mutex_lock(&q->mutex);
    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    {
        node = task_node_take_alloc(q);
        if (!node) {
            pthread_mutex_unlock(&q->mutex);
            return -1;
        }
        node->items = task->items;
        node->count = task->count;
        node->cap = task->cap;
        node->next = NULL;

        if (q->tail) q->tail->next = node;
        else q->head = node;
        q->tail = node;
        q->queued_tasks++;
        ATOMIC_ADD_RELAXED(&g_queue_depth, 1);
        /* One task_node per push: a single waiter can consume the head; broadcast would thundering-herd. */
        pthread_cond_signal(&q->cond);
    }
    pthread_mutex_unlock(&q->mutex);

    ATOMIC_ADD_RELAXED(&g_task_queue_pushes, 1);

    task->items = NULL;
    task->count = 0;
    task->cap = 0;
    return 0;
}

/* Enqueue a discovered subdirectory as its own task so other workers can run while this thread blocks
 * in readdir. Same pattern as enqueue_root_task (single-item dir_stack_t + queue_push_stack_take). */
static int enqueue_discovered_dir_task(task_queue_t *queue, char *path_owned, size_t path_len,
                                       const struct stat *st_opt, shared_state_t *shared, int pre_accounted_emit,
                                       int ancestor_matched) {
    dir_stack_t task;

    dir_stack_init(&task);
    if (dir_stack_push_take(&task, path_owned, path_len, st_opt, pre_accounted_emit, ancestor_matched) != 0) {
        fprintf(stderr, "ERROR worker stack push %s: %s\n", path_owned, strerror(errno));
        free(path_owned);
        stats_add_error(shared);
        return -1;
    }
    if (queue_push_stack_take(queue, &task) != 0) {
        fprintf(stderr, "ERROR worker failed to enqueue subdirectory task: %s\n", strerror(errno));
        dir_stack_destroy(&task);
        stats_add_error(shared);
        return -1;
    }
    return 0;
}

static void discovered_dir_batch_init(discovered_dir_batch_t *b, task_queue_t *queue, shared_state_t *shared) {
    b->queue = queue;
    b->shared = shared;
    dir_stack_init(&b->pending);
}

static int discovered_dir_batch_flush(discovered_dir_batch_t *b) {
    if (b->pending.count == 0) return 0;
    if (queue_push_stack_take(b->queue, &b->pending) != 0) {
        fprintf(stderr, "ERROR worker failed to flush discovered-directory batch to global queue: %s\n",
                strerror(errno));
        dir_stack_destroy(&b->pending);
        stats_add_error(b->shared);
        return -1;
    }
    return 0;
}

static void discovered_dir_batch_fini(discovered_dir_batch_t *b) {
    if (b->pending.count > 0) (void)discovered_dir_batch_flush(b);
}

static int discovered_dir_batch_push(discovered_dir_batch_t *b, char *path_owned, size_t path_len,
                                     const struct stat *st_opt, int pre_accounted_emit, int ancestor_matched) {
    if (dir_stack_push_take(&b->pending, path_owned, path_len, st_opt, pre_accounted_emit, ancestor_matched) != 0) {
        fprintf(stderr, "ERROR worker discovered-dir batch push %s: %s\n", path_owned, strerror(errno));
        free(path_owned);
        stats_add_error(b->shared);
        return -1;
    }
    if (b->pending.count >= g_discovered_dir_enqueue_batch_cfg) return discovered_dir_batch_flush(b);
    return 0;
}

/* Detach the head task and release q->mutex. Call with the mutex held and q->head non-NULL. Counts the
 * caller into g_active_workers, which is what keeps the queue from latching closed underneath it, so
 * every caller owes a matching decrement once the task is done (see crawl_run_task). */
static void queue_take_head_and_unlock(task_queue_t *q, dir_stack_t *task) {
    task_node_t *node = q->head;

    q->head = node->next;
    if (!q->head) q->tail = NULL;
    if (q->queued_tasks > 0) q->queued_tasks--;
    ATOMIC_SUB_RELAXED(&g_queue_depth, 1);
    atomic_fetch_add(&g_active_workers, 1);
    pthread_mutex_unlock(&q->mutex);

    ecrawl_hook_task_popped();

    task->items = node->items;
    task->count = node->count;
    task->cap = node->cap;
    task_node_recycle(q, node);
}

static int queue_pop_wait(task_queue_t *q, dir_stack_t *task) {
    pthread_mutex_lock(&q->mutex);
    for (;;) {
        if (q->head) break;
        if (q->closed) {
            pthread_mutex_unlock(&q->mutex);
            return -1;
        }
        if (atomic_load(&g_main_done) && atomic_load(&g_active_workers) == 0) {
            q->closed = 1;
            pthread_cond_broadcast(&q->cond);
            pthread_mutex_unlock(&q->mutex);
            return -1;
        }
        {
            uint64_t t0 = monotonic_ns();

            pthread_cond_wait(&q->cond, &q->mutex);
            if (t0) tls_crawl_idle_add_ns(monotonic_ns() - t0);
        }
        tls_wait_crawl_inc();
    }

    queue_take_head_and_unlock(q, task);
    return 0;
}

static int writer_queue_init(writer_queue_t *q, size_t max_batches) {
    memset(q, 0, sizeof(*q));
    q->max_batches = max_batches;
    if (pthread_mutex_init(&q->mutex, NULL) != 0) return -1;
    if (pthread_cond_init(&q->cond_nonempty, NULL) != 0) {
        pthread_mutex_destroy(&q->mutex);
        return -1;
    }
    if (pthread_cond_init(&q->cond_nonfull, NULL) != 0) {
        pthread_cond_destroy(&q->cond_nonempty);
        pthread_mutex_destroy(&q->mutex);
        return -1;
    }
    return 0;
}

static void writer_queue_close(writer_queue_t *q) {
    pthread_mutex_lock(&q->mutex);
    q->closed = 1;
    pthread_cond_broadcast(&q->cond_nonempty);
    pthread_cond_broadcast(&q->cond_nonfull);
    pthread_mutex_unlock(&q->mutex);
}

static void writer_queue_destroy(writer_queue_t *q) {
    record_batch_t *cur, *next;

    pthread_mutex_lock(&q->mutex);
    cur = q->head;
    while (cur) {
        next = cur->next;
        record_batch_release(cur);
        cur = next;
    }
    pthread_mutex_unlock(&q->mutex);

    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond_nonempty);
    pthread_cond_destroy(&q->cond_nonfull);
}

static int writer_queue_push(writer_queue_t *q, record_batch_t *batch) {
    pthread_mutex_lock(&q->mutex);
    while (!q->closed && q->count >= q->max_batches) {
        uint64_t t0 = monotonic_ns();

        pthread_cond_wait(&q->cond_nonfull, &q->mutex);
        if (t0) tls_writer_queue_wait_add_ns(monotonic_ns() - t0);
        tls_wait_writer_push_inc();
    }
    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    batch->next = NULL;
    if (q->tail) q->tail->next = batch;
    else q->head = batch;
    q->tail = batch;
    q->count++;

    ecrawl_hook_writer_push();

    pthread_cond_signal(&q->cond_nonempty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static record_batch_t *writer_queue_pop(writer_queue_t *q) {
    record_batch_t *batch;

    pthread_mutex_lock(&q->mutex);
    for (;;) {
        if (q->head) break;
        if (q->closed) {
            pthread_mutex_unlock(&q->mutex);
            return NULL;
        }
        {
            uint64_t t0 = monotonic_ns();

            pthread_cond_wait(&q->cond_nonempty, &q->mutex);
            if (t0) tls_writer_queue_wait_add_ns(monotonic_ns() - t0);
        }
        tls_wait_writer_pop_inc();
    }

    batch = q->head;
    q->head = batch->next;
    if (!q->head) q->tail = NULL;
    q->count--;
    pthread_cond_signal(&q->cond_nonfull);
    pthread_mutex_unlock(&q->mutex);

    ecrawl_hook_writer_pop();
    return batch;
}

static int emit_context_init(emit_context_t *ctx, writer_queue_t *writer_queues, int writer_threads,
                             perf_local_t *perf, pthread_mutex_t *stats_lock) {
    int i;

    memset(ctx, 0, sizeof(*ctx));
    ctx->writer_queues = writer_queues;
    ctx->writer_threads = writer_threads;
    ctx->perf = perf;
    ctx->stats_lock = stats_lock;
    if (g_no_write || writer_threads <= 0) return 0;
    ctx->pending = (pending_batch_t *)calloc((size_t)writer_threads, sizeof(*ctx->pending));
    if (!ctx->pending) return -1;
    ctx->pending_locks = (pthread_mutex_t *)calloc((size_t)writer_threads, sizeof(*ctx->pending_locks));
    if (!ctx->pending_locks) {
        free(ctx->pending);
        ctx->pending = NULL;
        return -1;
    }
    for (i = 0; i < writer_threads; i++) {
        if (pthread_mutex_init(&ctx->pending_locks[i], NULL) != 0) {
            while (i > 0) pthread_mutex_destroy(&ctx->pending_locks[--i]);
            free(ctx->pending_locks);
            ctx->pending_locks = NULL;
            free(ctx->pending);
            ctx->pending = NULL;
            return -1;
        }
        ctx->pending_locks_n = i + 1;
    }
    return 0;
}

static void emit_context_destroy(emit_context_t *ctx) {
    int i;
    if (!ctx) return;
    for (i = 0; i < ctx->pending_locks_n; i++) pthread_mutex_destroy(&ctx->pending_locks[i]);
    ctx->pending_locks_n = 0;
    free(ctx->pending_locks);
    ctx->pending_locks = NULL;
    if (ctx->pending)
        for (i = 0; i < ctx->writer_threads; i++) free(ctx->pending[i].data);
    free(ctx->pending);
    ctx->pending = NULL;
}

/* Detach a filled buffer so the owning lock can be dropped before the queue push. */
static void pending_batch_take(pending_batch_t *p, unsigned char **data_out, size_t *len_out) {
    if (p->len == 0) return;
    *data_out = p->data;
    *len_out = p->len;
    p->data = NULL;
    p->len = 0;
    p->cap = 0;
}

/* Hand a detached buffer to its writer. Runs without any emit lock held: the malloc and the
 * push (which blocks when the queue is full) must not stall other emitters. */
static int submit_detached_batch(emit_context_t *ctx, int writer_index, unsigned char *data, size_t len) {
    record_batch_t *batch = (record_batch_t *)malloc(sizeof(*batch));

    if (!batch) {
        free(data);
        return -1;
    }
    batch->data = data;
    batch->len = len;
    batch->next = NULL;
    atomic_init(&batch->refs, 1u);

    if (writer_queue_push(&ctx->writer_queues[writer_index], batch) != 0) {
        record_batch_release(batch);
        return -1;
    }

    /* Folding the owner's pending perf on every MiB batch keeps the TTY totals moving on captures whose
     * directories are too small to reach GLOBAL_PERF_FLUSH_EVERY. The fold needs the owner's lock. */
    if (ctx->perf) {
        if (ctx->stats_lock) pthread_mutex_lock(ctx->stats_lock);
        perf_flush_local(ctx->perf);
        if (ctx->stats_lock) pthread_mutex_unlock(ctx->stats_lock);
    }

    return 0;
}

static int flush_pending_batch(emit_context_t *ctx, int writer_index) {
    unsigned char *data = NULL;
    size_t len = 0;

    pthread_mutex_lock(&ctx->pending_locks[writer_index]);
    pending_batch_take(&ctx->pending[writer_index], &data, &len);
    pthread_mutex_unlock(&ctx->pending_locks[writer_index]);

    if (!data) return 0;
    return submit_detached_batch(ctx, writer_index, data, len);
}

static int ensure_pending_capacity(pending_batch_t *p, size_t need) {
    if (p->cap >= need) return 0;

    {
        size_t new_cap = p->cap ? p->cap : RECORD_BATCH_BYTES;
        while (new_cap < need) new_cap <<= 1;
        p->data = (unsigned char *)realloc(p->data, new_cap);
        if (!p->data) {
            p->cap = 0;
            p->len = 0;
            return -1;
        }
        p->cap = new_cap;
    }

    return 0;
}

static int init_record_path_prefix(const char *start_path) {
    if (snprintf(g_phys_prefix, sizeof(g_phys_prefix), "%s", start_path) >= (int)sizeof(g_phys_prefix)) return -1;
    path_rstrip_slashes(g_phys_prefix);
    g_phys_prefix_len = strlen(g_phys_prefix);
    return 0;
}

/* Map physical path to stored path when --record-root is set. */
static int map_path_for_record(const char *path, size_t path_len, char *out, size_t out_sz, size_t *out_len) {
    char tmp[PATH_MAX];
    const char *rel;
    int n;

    if (!path || path_len >= sizeof(tmp)) return -1;
    memcpy(tmp, path, path_len);
    tmp[path_len] = '\0';

    if (!g_record_root) {
        if (path_len >= out_sz) return -1;
        memcpy(out, path, path_len);
        out[path_len] = '\0';
        *out_len = path_len;
        return 0;
    }

    if (strncmp(tmp, g_phys_prefix, g_phys_prefix_len) != 0 ||
        !(tmp[g_phys_prefix_len] == '/' || tmp[g_phys_prefix_len] == '\0')) {
        static int warned_record_root_prefix;
        if (!warned_record_root_prefix) {
            warned_record_root_prefix = 1;
            fprintf(stderr,
                    "warn: path does not start with crawl root %s — storing raw path "
                    "(further occurrences suppressed)\n",
                    g_phys_prefix);
        }
        if (path_len >= out_sz) return -1;
        memcpy(out, path, path_len);
        *out_len = path_len;
        return 0;
    }

    rel = tmp + g_phys_prefix_len;
    if (*rel == '/') rel++;

    if (*rel == '\0') {
        n = snprintf(out, out_sz, "%s", g_record_root);
    } else {
        n = snprintf(out, out_sz, "%s/%s", g_record_root, rel);
    }
    if (n < 0 || (size_t)n >= out_sz) return -1;
    *out_len = (size_t)n;
    return 0;
}

static int emit_record(emit_context_t *ctx, const char *path, size_t path_len, const struct stat *st,
                       uint64_t byte_credit) {
    bin_record_hdr_t hdr;
    batch_frame_hdr_t frame;
    pending_batch_t *pending;
    uint32_t shard;
    int writer_index;
    size_t record_len;
    size_t frame_len;
    char path_buf[PATH_MAX];
    const char *path_write = path;
    size_t path_len_write = path_len;
    unsigned char *full_data = NULL;
    size_t full_len = 0;
    unsigned char *ready_data = NULL;
    size_t ready_len = 0;

    if (!ctx || !path || !st) return -1;
    if (g_no_write) return 0;

    if (map_path_for_record(path, path_len, path_buf, sizeof(path_buf), &path_len_write) != 0) return -1;
    if (path_len_write > UINT16_MAX) return -1;
    path_write = path_buf;

    memset(&hdr, 0, sizeof(hdr));
    /* Wire-format: parent_dir_id==0 + full stored path in name bytes; writer splits to the on-disk form. */
    hdr.parent_dir_id = 0;
    hdr.name_len = (uint16_t)path_len_write;
    hdr.type = (uint8_t)file_type_char(st->st_mode);
    hdr.uid = (uint64_t)st->st_uid;
    hdr.gid = (uint64_t)st->st_gid;
    hdr.mode = (uint32_t)st->st_mode;
    hdr.size = (uint64_t)st->st_size;
    hdr.inode = (uint64_t)st->st_ino;
    hdr.dev_major = (uint32_t)major(st->st_dev);
    hdr.dev_minor = (uint32_t)minor(st->st_dev);
    hdr.nlink = (uint64_t)st->st_nlink;
    hdr.atime = (uint64_t)st->st_atime;
    hdr.mtime = (uint64_t)st->st_mtime;
    hdr.ctime = (uint64_t)st->st_ctime;

    shard = shard_for_uid(st->st_uid);
    writer_index = (int)(shard % (uint32_t)ctx->writer_threads);
    pending = &ctx->pending[writer_index];

    record_len = sizeof(hdr) + path_len_write;
    memset(&frame, 0, sizeof(frame));
    frame.shard = shard;
    frame.data_len = (uint32_t)record_len;
    frame.byte_credit = byte_credit;
    frame_len = sizeof(frame) + record_len;

    /* Everything above is thread-local. Only the append is shared, and only with emitters that
     * hash to the same writer. Full buffers leave the lock as detached handoffs, submitted below
     * in the order they were filled. */
    pthread_mutex_lock(&ctx->pending_locks[writer_index]);

    if (pending->len > 0 && pending->len + frame_len > pending->cap)
        pending_batch_take(pending, &full_data, &full_len);

    if (ensure_pending_capacity(pending, pending->len + frame_len) != 0) {
        pthread_mutex_unlock(&ctx->pending_locks[writer_index]);
        if (full_data) (void)submit_detached_batch(ctx, writer_index, full_data, full_len);
        return -1;
    }

    memcpy(pending->data + pending->len, &frame, sizeof(frame));
    pending->len += sizeof(frame);
    memcpy(pending->data + pending->len, &hdr, sizeof(hdr));
    pending->len += sizeof(hdr);
    if (path_len_write > 0) {
        memcpy(pending->data + pending->len, path_write, path_len_write);
        pending->len += path_len_write;
    }

    if (pending->len >= RECORD_BATCH_BYTES) pending_batch_take(pending, &ready_data, &ready_len);

    pthread_mutex_unlock(&ctx->pending_locks[writer_index]);

    if (full_data && submit_detached_batch(ctx, writer_index, full_data, full_len) != 0) return -1;
    if (ready_data && submit_detached_batch(ctx, writer_index, ready_data, ready_len) != 0) return -1;

    return 0;
}

/* Keep the critical section to the accounting itself: the stat call, the path join and
 * emit_record all stay outside it. */
static uint64_t account_entry_shared(emit_context_t *ctx, shared_state_t *shared, crawl_stats_t *stats,
                                     perf_local_t *perf, const struct stat *st) {
    uint64_t contrib;

    if (!ctx || !ctx->stats_lock) return account_entry_local(shared, stats, perf, st);

    pthread_mutex_lock(ctx->stats_lock);
    contrib = account_entry_local(shared, stats, perf, st);
    pthread_mutex_unlock(ctx->stats_lock);
    return contrib;
}

static int emit_context_flush_all(emit_context_t *ctx) {
    int i;
    if (!ctx || g_no_write || ctx->writer_threads <= 0) return 0;
    for (i = 0; i < ctx->writer_threads; i++) {
        if (flush_pending_batch(ctx, i) != 0) return -1;
    }
    return 0;
}

/* ----- crawl identity -----------------------------------------------------------------------------
 * The per-thread state process_directory_iterative needs: writer-batch buffers, the --no-stat path
 * buffer, and one getdents64 buffer (a thread reads one directory at a time, so one buffer suffices;
 * NULL/0 falls back to libc readdir). */
typedef struct {
    emit_context_t emit;
    char *dirbuf;
    size_t dirbuf_cap;
    /* --iouring: per-worker ring (fd < 0 when unavailable or disabled). */
    uring_stat_ctx_t uring;
} crawl_identity_t;

static int crawl_identity_init(crawl_identity_t *ci, worker_arg_t *arg) {
    ci->dirbuf = NULL;
    ci->dirbuf_cap = g_getdents_buf_bytes;

    if (emit_context_init(&ci->emit, arg->writer_queues, arg->writer_threads, &arg->perf,
                          &arg->emit_stats_lock) != 0) {
        fprintf(stderr, "ERROR worker %" PRIu64 " failed to initialize emit context\n", arg->worker_index);
        stats_add_error(arg->shared);
        return -1;
    }
    if (nostat_ctx_init(&arg->nostat) != 0) {
        fprintf(stderr, "ERROR worker %" PRIu64 " failed to allocate path output buffer\n", arg->worker_index);
        stats_add_error(arg->shared);
        emit_context_destroy(&ci->emit);
        return -1;
    }
    if (ci->dirbuf_cap > 0) {
        ci->dirbuf = malloc(ci->dirbuf_cap);
        if (!ci->dirbuf) ci->dirbuf_cap = 0; /* fall back to libc readdir on OOM */
    }
    ci->uring.fd = -1;
    ci->uring.dead = 0;
    if (g_iouring_statx) {
        const char *why = NULL;

        if (uring_stat_init(&ci->uring, g_iouring_depth) != 0)
            why = strerror(errno);
        else if (uring_stat_probe(&ci->uring) != 0)
            why = "kernel rejects IORING_OP_STATX";
        if (why) {
            if (atomic_exchange(&g_io_uring_warned, 1) == 0)
                fprintf(stderr, "WARN: io_uring unavailable (%s); --iouring falls back to statx\n", why);
            uring_stat_destroy(&ci->uring);
        }
    }
    return 0;
}

static int process_directory_iterative(dir_stack_t *stack, worker_arg_t *wa, emit_context_t *emit,
                                       task_queue_t *queue, char *dirbuf, size_t dirbuf_cap,
                                       uring_stat_ctx_t *uring);

/* Run one task popped from the global queue, then account for no longer holding it. The pop already
 * counted this thread into g_active_workers, and quiescence is "seeding done, queue empty, nobody
 * active", so the thread that drops the last active count has to wake the sleepers that can now exit. */
static void crawl_run_task(dir_stack_t *task, worker_arg_t *arg, crawl_identity_t *ci) {
    process_directory_iterative(task, arg, &ci->emit, arg->queue, ci->dirbuf, ci->dirbuf_cap,
                                g_iouring_statx ? &ci->uring : NULL);
    atomic_fetch_sub(&g_active_workers, 1);

    if (atomic_load(&g_main_done) && atomic_load(&g_active_workers) == 0) {
        pthread_mutex_lock(&arg->queue->mutex);
        pthread_cond_broadcast(&arg->queue->cond);
        pthread_mutex_unlock(&arg->queue->mutex);
    }

    dir_stack_destroy(task);
}

static void crawl_identity_destroy(crawl_identity_t *ci, worker_arg_t *arg) {
    /* Last slot write so a short walk (never hit g_progress_every) still has totals for the
     * post-join line. Do not print here: idle workers would flash files=0. */
    progress_publish(arg);
    /* Last fold for this owner, under its lock like every other fold. */
    pthread_mutex_lock(&arg->emit_stats_lock);
    perf_flush_local(&arg->perf);
    pthread_mutex_unlock(&arg->emit_stats_lock);
    if (emit_context_flush_all(&ci->emit) != 0) stats_add_error(arg->shared);
    emit_context_destroy(&ci->emit);
    if (pathout_flush(&arg->nostat) != 0) stats_add_error(arg->shared);
    nostat_ctx_destroy(&arg->nostat);
    free(ci->dirbuf);
    ci->dirbuf = NULL;
    ci->dirbuf_cap = 0;
    uring_stat_destroy(&ci->uring);
}

static int dir_stack_init(dir_stack_t *s) {
    s->items = NULL;
    s->count = 0;
    s->cap = 0;
    return 0;
}

static void dir_stack_destroy(dir_stack_t *s) {
    size_t i;
    for (i = 0; i < s->count; i++) free(s->items[i].path);
    free(s->items);
    s->items = NULL;
    s->count = 0;
    s->cap = 0;
}

static int dir_stack_push_take(dir_stack_t *s, char *path_owned, size_t path_len, const struct stat *st,
                               int pre_accounted_emit, int ancestor_matched) {
    if (s->count == s->cap) {
        size_t new_cap = (s->cap == 0) ? 64 : (s->cap * 2);
        dir_work_t *new_items = (dir_work_t *)realloc(s->items, new_cap * sizeof(*new_items));
        if (!new_items) return -1;
        s->items = new_items;
        s->cap = new_cap;
    }

    s->items[s->count].path = path_owned;
    s->items[s->count].path_len = path_len;
    s->items[s->count].ancestor_matched = ancestor_matched;
    if (st) {
        s->items[s->count].st = *st;
        s->items[s->count].have_stat = 1;
        s->items[s->count].pre_accounted_emit = pre_accounted_emit ? 1 : 0;
    } else {
        memset(&s->items[s->count].st, 0, sizeof(s->items[s->count].st));
        s->items[s->count].have_stat = 0;
        s->items[s->count].pre_accounted_emit = 0;
    }
    s->count++;
    return 0;
}

static int dir_stack_pop(dir_stack_t *s, dir_work_t *work) {
    if (s->count == 0) return -1;
    *work = s->items[--s->count];
    return 0;
}

static int should_donate_work(const shared_state_t *shared, const dir_stack_t *local_stack) {
    uint64_t qdepth = ATOMIC_LOAD_RELAXED(&g_queue_depth);
    int active = atomic_load(&g_active_workers);
    int started = (int)shared->crawl_threads_started;
    int idle = started - active;
    uint64_t max_q_idle;
    uint64_t max_q_all_busy;

    if (started <= 1) return 0;
    if (local_stack->count < LOCAL_STACK_DONATE_FLOOR) return 0;
    if (local_stack->count >= g_force_donate_count_cfg) return 1;

    if (active >= started) {
        if (local_stack->count < g_donate_all_busy_min_stack_cfg) return 0;
        max_q_all_busy = (uint64_t)started * (uint64_t)g_donate_all_busy_max_qdepth_mult_cfg;
        if (qdepth >= max_q_all_busy) return 0;
        return 1;
    }

    if (idle > 0) {
        max_q_idle = (uint64_t)idle * (uint64_t)DONATE_QUEUE_TARGET_PER_IDLE;
        if (qdepth >= max_q_idle) return 0;
    }
    return 1;
}

/* Move the top `count` directories off the local stack and onto the global queue. */
static int donate_stack_take(dir_stack_t *local_stack, task_queue_t *queue, worker_aux_stats_t *aux, size_t count) {
    dir_stack_t donated;
    size_t start;

    if (count == 0) return 0;

    dir_stack_init(&donated);
    donated.items = (dir_work_t *)malloc(count * sizeof(*donated.items));
    if (!donated.items) return -1;

    donated.count = count;
    donated.cap = count;
    start = local_stack->count - count;
    memcpy(donated.items, local_stack->items + start, count * sizeof(*donated.items));
    local_stack->count = start;

    stats_add_donation_attempt_local(aux, count);
    if (queue_push_stack_take(queue, &donated) != 0) {
        local_stack->count += count;
        memcpy(local_stack->items + start, donated.items, count * sizeof(*donated.items));
        free(donated.items);
        return -1;
    }

    stats_add_donated_dirs_local(aux, count);
    return 0;
}

static int donate_stack_chunk(dir_stack_t *local_stack, task_queue_t *queue, worker_aux_stats_t *aux) {
    size_t count;
    int force_spill;

    if (!local_stack || local_stack->count < LOCAL_STACK_DONATE_FLOOR) return 0;

    tls_donate_calls_inc();
    force_spill = (local_stack->count >= g_force_donate_count_cfg);
    if (force_spill) {
        size_t target = g_force_donate_count_cfg - (g_force_donate_count_cfg / 4U);

        if (target < (size_t)LOCAL_STACK_DONATE_FLOOR) target = (size_t)LOCAL_STACK_DONATE_FLOOR;
        count = (local_stack->count > target) ? (local_stack->count - target) : 0U;
        if (count > g_donate_chunk_force_max_cfg) count = g_donate_chunk_force_max_cfg;
    } else {
        count = local_stack->count / 2;
        if (count < (size_t)DONATE_CHUNK_MIN) count = (size_t)DONATE_CHUNK_MIN;
        if (count > (size_t)DONATE_CHUNK_MAX) count = (size_t)DONATE_CHUNK_MAX;
    }
    if (count >= local_stack->count) count = local_stack->count - 1;
    return donate_stack_take(local_stack, queue, aux, count);
}

/* Drain local stack to global queue while donation policy says spill (bounded iterations per call). */
static void donate_spill_if_needed(shared_state_t *shared, dir_stack_t *stack, task_queue_t *queue,
                                   worker_aux_stats_t *aux) {
    int guard = 0;

    while (should_donate_work(shared, stack) && guard++ < 32) {
        if (donate_stack_chunk(stack, queue, aux) != 0) {
            fprintf(stderr, "ERROR worker donate chunk: %s\n", strerror(errno));
            stats_add_error(shared);
            break;
        }
    }
}

/* During readdir: avoid checking the global queue after every DT_DIR; still spill immediately on force depth. */
static void donate_spill_periodic(shared_state_t *shared, dir_stack_t *stack, task_queue_t *queue,
                                  worker_aux_stats_t *aux, size_t *dirs_since_check) {
    if (stack->count >= g_force_donate_count_cfg) {
        donate_spill_if_needed(shared, stack, queue, aux);
        if (dirs_since_check) *dirs_since_check = 0;
        return;
    }
    if (dirs_since_check) {
        if (*dirs_since_check < g_donate_check_every_cfg) return;
        *dirs_since_check = 0;
    }
    if (should_donate_work(shared, stack)) donate_spill_if_needed(shared, stack, queue, aux);
}

/*
 * The other donation trigger: entries read, not subdirectories found.
 *
 * Everything above keys off the local stack growing, which is exactly what a deep narrow chain
 * never does. depth_slash_profile is 411,892 entries and ran at avg_active_workers=1.00 with
 * tasks_popped=2 -- one thread walked the chain while the rest of the pool waited, because each
 * directory yields one subdirectory and the stack never reached the donation floor of 8.
 *
 * A worker part-way through a large directory has work in hand regardless of its stack, so when
 * peers are actually idle it can hand over every directory it is holding and lose nothing. That
 * is only worth doing when someone is waiting: with no idle peer this would push work through the
 * queue for nothing, so the queue-depth-against-idle-count test still gates it.
 */
static int should_donate_to_idle(const shared_state_t *shared, const dir_stack_t *stack) {
    int started = (int)shared->crawl_threads_started;
    int idle;

    if (started <= 1 || stack->count == 0) return 0;
    idle = started - atomic_load(&g_active_workers);
    if (idle <= 0) return 0;
    return ATOMIC_LOAD_RELAXED(&g_queue_depth) < (uint64_t)idle;
}

static void donate_spill_on_entries(shared_state_t *shared, dir_stack_t *stack, task_queue_t *queue,
                                    worker_aux_stats_t *aux, size_t *entries_since_check) {
    size_t want;

    if (*entries_since_check < g_donate_entry_check_every_cfg) return;
    *entries_since_check = 0;
    if (!should_donate_to_idle(shared, stack)) return;

    /* One per idle peer, capped by what is on the stack: enough to wake them, not so much that a
     * later burst of subdirectories has nothing left to balance with. */
    want = (size_t)(shared->crawl_threads_started - atomic_load(&g_active_workers));
    if (want > stack->count) want = stack->count;
    if (want > (size_t)DONATE_CHUNK_MAX) want = (size_t)DONATE_CHUNK_MAX;

    tls_donate_calls_inc();
    if (donate_stack_take(stack, queue, aux, want) != 0) {
        fprintf(stderr, "ERROR worker donate to idle: %s\n", strerror(errno));
        stats_add_error(shared);
    }
}

/* d_type==DT_DIR: fstatat once under dir_fd (skip later lstat on pop), push onto local stack; account+emit on pop.
 * Wrong d_type: account+emit here as a leaf. Stack push failure falls back to enqueue. */
static void crawl_handle_dirent_dt_dir(int dir_fd, const char *dir_path, size_t dir_path_len, const char *name,
                                       size_t name_len, shared_state_t *shared, crawl_stats_t *stats, perf_local_t *perf,
                                       emit_context_t *emit, dir_stack_t *stack, task_queue_t *queue,
                                       worker_aux_stats_t *aux, discovered_dir_batch_t *disc_batch) {
    struct stat st;
    char *owned;
    size_t owned_len;

    (void)aux;
    if (ecrawl_io_fstatat_nf(dir_fd, name, &st) != 0) {
        fprintf(stderr, "ERROR worker fstatat %s/%s: %s\n", dir_path, name, strerror(errno));
        stats_add_error(shared);
        return;
    }
    if (!S_ISDIR(st.st_mode)) {
        uint64_t contrib;

        record_ids_from_stat(&st);
        contrib = account_entry_shared(emit, shared, stats, perf, &st);
        if (!g_no_write) {
            char child[PATH_MAX];
            size_t child_emitted_len = dir_path_len + name_len + ((dir_path_len == 1 && dir_path[0] == '/') ? 0U : 1U);

            if (path_join_fast(dir_path, dir_path_len, name, name_len, child, sizeof(child)) != 0) {
                fprintf(stderr, "ERROR worker path too long: %s/%s\n", dir_path, name);
                stats_add_error(shared);
                return;
            }
            if (emit_record(emit, child, child_emitted_len, &st, contrib) != 0) {
                fprintf(stderr, "ERROR worker emit_record %s: %s\n", child, strerror(errno));
                stats_add_error(shared);
            }
        }
        return;
    }
    if (path_join_alloc(dir_path, dir_path_len, name, name_len, &owned, &owned_len) != 0) {
        fprintf(stderr, "ERROR worker path alloc %s/%s: %s\n", dir_path, name, strerror(errno));
        stats_add_error(shared);
        return;
    }
    if (dir_stack_push_take(stack, owned, owned_len, &st, 0, 0) != 0) {
        fprintf(stderr, "ERROR worker stack push %s: %s\n", owned, strerror(errno));
        stats_add_error(shared);
        if (disc_batch) {
            (void)discovered_dir_batch_push(disc_batch, owned, owned_len, &st, 0, 0);
        } else if (enqueue_discovered_dir_task(queue, owned, owned_len, &st, shared, 0, 0) != 0) {
            fprintf(stderr, "ERROR worker failed to enqueue subdirectory task: %s\n", strerror(errno));
            free(owned);
        }
        return;
    }
}


/* ----- raw getdents64 directory reader (with libc readdir fallback) ------------------------------
 * When g_getdents_buf_bytes > 0 a directory is read with raw getdents64(2) into a per-crawl-thread
 * reusable buffer, returning many dirents per syscall (far fewer syscalls than libc readdir on large
 * directories, and less syscall-entry / seccomp / ptrace overhead). When 0 (or buffer allocation
 * failed) it falls back to opendir/readdir, leaving behaviour and portability unchanged. Either way
 * the same dir fd is reused for the per-child fstatat calls. */
struct ecrawl_linux_dirent64 {
    uint64_t       d_ino;
    int64_t        d_off;
    unsigned short d_reclen;
    unsigned char  d_type;
    char           d_name[];
};

typedef struct {
    int         fd;       /* dir fd (raw path); also used for child fstatat. -1 => libc fallback */
    char       *buf;      /* caller-owned reusable buffer (raw path only) */
    size_t      buf_cap;
    size_t      buf_len;  /* valid bytes returned by the last getdents64 */
    size_t      buf_off;  /* parse cursor into buf */
    DIR        *dirp;     /* non-NULL => libc opendir/readdir fallback */
    const char *path;     /* for diagnostics on getdents errors */
} ecrawl_dir_reader_t;

/* Open `path` for iteration. Uses raw getdents64 when buf/buf_cap are usable, else libc opendir.
 * Returns 0 on success; on failure returns -1 with errno set (caller keeps its EMFILE retry). */
static int ecrawl_dir_reader_open(ecrawl_dir_reader_t *r, const char *path, char *buf, size_t buf_cap) {
    r->fd = -1;
    r->buf = buf;
    r->buf_cap = buf_cap;
    r->buf_len = 0;
    r->buf_off = 0;
    r->dirp = NULL;
    r->path = path;
    if (buf && buf_cap >= MIN_GETDENTS_BUF) {
        int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) return -1;
        r->fd = fd;
        if (g_verbose) ATOMIC_ADD_RELAXED(&g_io_opendir_calls, 1);
        return 0;
    }
    r->dirp = ecrawl_io_opendir(path); /* counts opendir in verbose via the fn pointer */
    if (!r->dirp) return -1;
    return 0;
}

/* Directory fd usable for fstatat(dirfd, name, ...). */
static int ecrawl_dir_reader_fd(ecrawl_dir_reader_t *r) {
    return (r->fd >= 0) ? r->fd : dirfd(r->dirp);
}

/* Fetch the next entry's name, d_type and inode. Returns 1 = got entry, 0 = end-of-directory,
 * -1 = read error. "." / ".." are not filtered here (callers already skip them), matching readdir. */
static int ecrawl_dir_reader_next(ecrawl_dir_reader_t *r, const char **name_out, unsigned char *type_out,
                                  uint64_t *ino_out) {
    if (r->fd < 0) {
        struct dirent *de = ecrawl_io_readdir(r->dirp);
        if (!de) return 0;
        *name_out = de->d_name;
#if defined(_DIRENT_HAVE_D_TYPE) && defined(DT_UNKNOWN)
        *type_out = de->d_type;
#else
        *type_out = DT_UNKNOWN;
#endif
        if (ino_out) *ino_out = (uint64_t)de->d_ino;
        return 1;
    }
    for (;;) {
        struct ecrawl_linux_dirent64 *d;
        if (r->buf_off >= r->buf_len) {
            long n = syscall(SYS_getdents64, r->fd, r->buf, r->buf_cap);
            if (n < 0) {
                fprintf(stderr, "ERROR worker getdents64 %s: %s\n",
                        r->path ? r->path : "(dir)", strerror(errno));
                return -1;
            }
            if (n == 0) return 0;
            r->buf_len = (size_t)n;
            r->buf_off = 0;
            if (g_verbose) ATOMIC_ADD_RELAXED(&g_io_getdents_calls, 1);
        }
        d = (struct ecrawl_linux_dirent64 *)(void *)(r->buf + r->buf_off);
        r->buf_off += d->d_reclen;
        *name_out = d->d_name;
        *type_out = d->d_type;
        if (ino_out) *ino_out = d->d_ino;
        if (g_verbose) ATOMIC_ADD_RELAXED(&g_io_readdir_calls, 1);
        return 1;
    }
}

static void ecrawl_dir_reader_close(ecrawl_dir_reader_t *r) {
    if (r->fd >= 0) {
        close(r->fd);
        r->fd = -1;
        if (g_verbose) ATOMIC_ADD_RELAXED(&g_io_closedir_calls, 1);
    } else if (r->dirp) {
        ecrawl_io_closedir(r->dirp);
        r->dirp = NULL;
    }
}

/* ----- --iouring: batched per-entry inode reads (IORING_OP_STATX) ----------------------------------
 * Same per-entry semantics as the inline loop in process_directory_iterative, but the per-entry
 * statx calls of one directory are queued as a batch on this worker's ring and reaped in readdir
 * order. A ring that dies mid-run degrades to synchronous statx per slot, so no entry is lost. */
#ifdef ECRAWL_HAVE_IOURING

/* Leaf accounting shared by the dispatch branches (mirrors the inline loop's leaf body). */
static void crawl_iouring_leaf(const char *dir_path, size_t dir_path_len, const char *name, size_t name_len,
                               const struct stat *st, shared_state_t *shared, crawl_stats_t *stats,
                               perf_local_t *perf, emit_context_t *emit) {
    uint64_t contrib;

    record_ids_from_stat(st);
    contrib = account_entry_shared(emit, shared, stats, perf, st);
    if (!g_no_write) {
        char child[PATH_MAX];
        size_t child_len = dir_path_len + name_len + ((dir_path_len == 1 && dir_path[0] == '/') ? 0U : 1U);

        if (path_join_fast(dir_path, dir_path_len, name, name_len, child, sizeof(child)) != 0) {
            fprintf(stderr, "ERROR worker path too long: %s/%s\n", dir_path, name);
            stats_add_error(shared);
            return;
        }
        if (emit_record(emit, child, child_len, st, contrib) != 0) {
            fprintf(stderr, "ERROR worker emit_record %s: %s\n", child, strerror(errno));
            stats_add_error(shared);
        }
    }
}

/* Dispatch one stat'd entry exactly like the inline loop: a trusted DT_DIR goes to the local stack,
 * a d_type-surprise directory goes to the discovered batch, anything else is a leaf. */
static void crawl_iouring_dispatch(const char *dir_path, size_t dir_path_len, const char *name, size_t name_len,
                                   unsigned char dtype, const struct stat *st, shared_state_t *shared,
                                   crawl_stats_t *stats, perf_local_t *perf, emit_context_t *emit,
                                   dir_stack_t *stack, task_queue_t *queue, worker_aux_stats_t *aux,
                                   discovered_dir_batch_t *disc_batch, size_t *dirs_since_donate_check) {
    char *owned;
    size_t owned_len;

    if (dtype == DT_DIR && S_ISDIR(st->st_mode)) {
        if (path_join_alloc(dir_path, dir_path_len, name, name_len, &owned, &owned_len) != 0) {
            fprintf(stderr, "ERROR worker path alloc %s/%s: %s\n", dir_path, name, strerror(errno));
            stats_add_error(shared);
            return;
        }
        if (dir_stack_push_take(stack, owned, owned_len, st, 0, 0) != 0) {
            fprintf(stderr, "ERROR worker stack push %s: %s\n", owned, strerror(errno));
            stats_add_error(shared);
            if (disc_batch) {
                (void)discovered_dir_batch_push(disc_batch, owned, owned_len, st, 0, 0);
            } else if (enqueue_discovered_dir_task(queue, owned, owned_len, st, shared, 0, 0) != 0) {
                fprintf(stderr, "ERROR worker failed to enqueue subdirectory task: %s\n", strerror(errno));
                free(owned);
            }
            return;
        }
        (*dirs_since_donate_check)++;
        donate_spill_periodic(shared, stack, queue, aux, dirs_since_donate_check);
        return;
    }
    if (dtype != DT_DIR && S_ISDIR(st->st_mode)) {
        if (path_join_alloc(dir_path, dir_path_len, name, name_len, &owned, &owned_len) != 0) {
            fprintf(stderr, "ERROR worker path alloc %s/%s: %s\n", dir_path, name, strerror(errno));
            stats_add_error(shared);
            return;
        }
        if (discovered_dir_batch_push(disc_batch, owned, owned_len, st, 0, 0) != 0) return;
        (*dirs_since_donate_check)++;
        donate_spill_periodic(shared, stack, queue, aux, dirs_since_donate_check);
        return;
    }
    crawl_iouring_leaf(dir_path, dir_path_len, name, name_len, st, shared, stats, perf, emit);
}

static void crawl_dir_entries_iouring(ecrawl_dir_reader_t *rd, int dir_fd, const char *dir_path,
                                      size_t dir_path_len, shared_state_t *shared, crawl_stats_t *stats,
                                      perf_local_t *perf, emit_context_t *emit, dir_stack_t *stack,
                                      task_queue_t *queue, worker_aux_stats_t *aux,
                                      discovered_dir_batch_t *disc_batch, worker_arg_t *wa,
                                      uring_stat_ctx_t *u) {
    const char *ent_name;
    unsigned char ent_dtype;
    uint64_t ent_ino = 0;
    size_t dirs_since_donate_check = 0;
    size_t entries_since_donate_check = 0;
    int eof = 0;

    while (!eof) {
        unsigned i, n;
        int use_ring;

        /* Phase 1: collect names only. Prep/submit waits until we know n vs min_batch. */
        u->batch = 0;
        while (u->batch < u->depth) {
            uring_slot_t *slot;
            size_t name_len;
            int rr = ecrawl_dir_reader_next(rd, &ent_name, &ent_dtype, &ent_ino);

            if (rr != 1) {
                eof = 1;
                break;
            }
            if (strcmp(ent_name, ".") == 0 || strcmp(ent_name, "..") == 0) continue;
            entries_since_donate_check++;
            donate_spill_on_entries(shared, stack, queue, aux, &entries_since_donate_check);
            name_len = strlen(ent_name);
            slot = &u->slots[u->batch];
            if (name_len >= sizeof(slot->name)) {
                /* Beyond NAME_MAX: stat inline, keep the batch for the rest. */
                struct stat st;

                if (ecrawl_io_fstatat_nf(dir_fd, ent_name, &st) != 0) {
                    fprintf(stderr, "ERROR worker fstatat %s/%s: %s\n", dir_path, ent_name, strerror(errno));
                    stats_add_error(shared);
                    continue;
                }
                crawl_iouring_dispatch(dir_path, dir_path_len, ent_name, name_len, ent_dtype, &st, shared,
                                       stats, perf, emit, stack, queue, aux, disc_batch,
                                       &dirs_since_donate_check);
                progress_tick(wa);
                continue;
            }
            memcpy(slot->name, ent_name, name_len + 1);
            slot->name_len = (uint16_t)name_len;
            slot->dtype = ent_dtype;
            u->batch++;
        }
        n = u->batch;
        if (n == 0) break;

        /* Tiny directories: one io_uring_enter for 1–2 names is a tax (neutral_flat). */
        use_ring = !u->dead && n >= g_iouring_min_batch;
        if (use_ring) {
            for (i = 0; i < n; i++) uring_prep_slot(u, i, dir_fd, &u->slots[i]);
            if (uring_submit_wait_all(u, n) != 0) {
                u->dead = 1;
                use_ring = 0;
                fprintf(stderr, "WARN worker io_uring failed mid-run; continuing with statx\n");
            }
        }

        for (i = 0; i < n; i++) {
            uring_slot_t *slot = &u->slots[i];
            struct stat st;

            if (!use_ring) {
                if (u->dead)
                    ATOMIC_ADD_RELAXED(&g_io_uring_sync_redos, 1);
                else
                    ATOMIC_ADD_RELAXED(&g_io_uring_inline_stats, 1);
                if (ecrawl_statx_fstatat_nf(dir_fd, slot->name, &st) != 0) {
                    fprintf(stderr, "ERROR worker fstatat %s/%s: %s\n", dir_path, slot->name, strerror(errno));
                    stats_add_error(shared);
                    continue;
                }
            } else {
                long res = u->res[i];

                ATOMIC_ADD_RELAXED(&g_io_lstat_calls, 1);
                if (res != 0) {
                    fprintf(stderr, "ERROR worker fstatat %s/%s: %s\n", dir_path, slot->name,
                            strerror((int)-res));
                    stats_add_error(shared);
                    continue;
                }
                if ((slot->stx.stx_mask & g_statx_mask) != g_statx_mask) {
                    ATOMIC_ADD_RELAXED(&g_io_uring_sync_redos, 1);
                    if (fstatat(dir_fd, slot->name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
                        fprintf(stderr, "ERROR worker fstatat %s/%s: %s\n", dir_path, slot->name,
                                strerror(errno));
                        stats_add_error(shared);
                        continue;
                    }
                } else {
                    ecrawl_statx_to_stat(&slot->stx, &st);
                }
            }
            crawl_iouring_dispatch(dir_path, dir_path_len, slot->name, slot->name_len, slot->dtype, &st,
                                   shared, stats, perf, emit, stack, queue, aux, disc_batch,
                                   &dirs_since_donate_check);
            progress_tick(wa);
        }
    }
    donate_spill_if_needed(shared, stack, queue, aux);
}
#else
/* Never called: the stub ring's fd is always -1, so the caller takes the inline loop. */
static void crawl_dir_entries_iouring(ecrawl_dir_reader_t *rd, int dir_fd, const char *dir_path,
                                      size_t dir_path_len, shared_state_t *shared, crawl_stats_t *stats,
                                      perf_local_t *perf, emit_context_t *emit, dir_stack_t *stack,
                                      task_queue_t *queue, worker_aux_stats_t *aux,
                                      discovered_dir_batch_t *disc_batch, worker_arg_t *wa,
                                      uring_stat_ctx_t *u) {
    (void)rd; (void)dir_fd; (void)dir_path; (void)dir_path_len; (void)shared; (void)stats;
    (void)perf; (void)emit; (void)stack; (void)queue; (void)aux; (void)disc_batch; (void)wa; (void)u;
}
#endif

static int process_directory_iterative(dir_stack_t *stack,
                                       worker_arg_t *wa,
                                       emit_context_t *emit,
                                       task_queue_t *queue,
                                       char *dirbuf,
                                       size_t dirbuf_cap,
                                       uring_stat_ctx_t *uring) {
    shared_state_t *shared = wa->shared;
    crawl_stats_t *stats = &wa->stats;
    perf_local_t *perf = &wa->perf;
    worker_aux_stats_t *aux = &wa->aux;
    discovered_dir_batch_t disc_b;

    discovered_dir_batch_init(&disc_b, queue, shared);

    while (stack->count > 0) {
        dir_work_t work;
        char *dir_path;
        size_t dir_path_len;
        struct stat st;
        ecrawl_dir_reader_t rd;
        const char *ent_name;
        unsigned char ent_dtype;
        uint64_t ent_ino = 0;
        dirmatch_t dm;

        if (dir_stack_pop(stack, &work) != 0) break;

        dir_path = work.path;
        dm.all_match = 1;
        dm.prefix_len = 0;

        if (g_no_stat) {
            /* d_type already proved this is a directory when the parent pushed it, so the walk
             * never reads its inode. Directories are printed here, on pop, so each appears once. */
            dir_path_len = work.path_len;
            account_entry_dtype(stats, perf, DT_DIR);
            progress_tick(wa);
            dirmatch_begin(&wa->nostat, dir_path, dir_path_len, work.ancestor_matched, &dm);
            if (!g_nostat_count && dm.all_match && pathout_emit(&wa->nostat, dir_path, dir_path_len) != 0) {
                stats_add_error(shared);
                free(dir_path);
                continue;
            }
        } else {
            if (work.have_stat) st = work.st;
            else {
                memset(&st, 0, sizeof(st));
                if (ecrawl_io_lstat(dir_path, &st) != 0) {
                    fprintf(stderr, "ERROR worker lstat %s: %s\n", dir_path, strerror(errno));
                    stats_add_error(shared);
                    free(dir_path);
                    continue;
                }
            }

            if (!work.pre_accounted_emit) {
                record_ids_from_stat(&st);
                {
                    uint64_t contrib = account_entry_shared(emit, shared, stats, perf, &st);

                    progress_tick(wa);
                    dir_path_len = work.path_len;
                    if (emit_record(emit, dir_path, dir_path_len, &st, contrib) != 0) {
                        fprintf(stderr, "ERROR worker emit_record %s: %s\n", dir_path, strerror(errno));
                        stats_add_error(shared);
                        free(dir_path);
                        continue;
                    }
                }
            } else
                dir_path_len = work.path_len;

            if (!S_ISDIR(st.st_mode)) {
                free(dir_path);
                continue;
            }
        }

        {
            unsigned retry;
            int oprc = -1;
            for (retry = 0; retry <= EMFILE_RETRY_LIMIT; retry++) {
                oprc = ecrawl_dir_reader_open(&rd, dir_path, dirbuf, dirbuf_cap);
                if (oprc == 0 || errno != EMFILE || retry == EMFILE_RETRY_LIMIT) break;
                atomic_store(&g_fd_pressure, 1U);
                emfile_retry_pause(retry);
            }
            if (oprc != 0) {
                fprintf(stderr, "ERROR worker opendir %s: %s\n", dir_path, strerror(errno));
                stats_add_error(shared);
                (void)discovered_dir_batch_flush(&disc_b);
                free(dir_path);
                continue;
            }
        }

        {
            int dir_fd = ecrawl_dir_reader_fd(&rd);
            if (dir_fd < 0) {
                fprintf(stderr, "ERROR worker dirfd %s: %s\n", dir_path, strerror(errno));
                stats_add_error(shared);
                (void)discovered_dir_batch_flush(&disc_b);
                ecrawl_dir_reader_close(&rd);
                free(dir_path);
                continue;
            }

            if (g_no_stat) {
                size_t dirs_since_donate_check = 0;
                size_t entries_since_donate_check = 0;

                while (ecrawl_dir_reader_next(&rd, &ent_name, &ent_dtype, &ent_ino) == 1) {
                    size_t child_name_len;
                    unsigned char child_d_type = ent_dtype;

                    if (strcmp(ent_name, ".") == 0 || strcmp(ent_name, "..") == 0) continue;
                    entries_since_donate_check++;
                    donate_spill_on_entries(shared, stack, queue, aux, &entries_since_donate_check);
                    child_name_len = strlen(ent_name);

                    /* Filesystems without d_type support report DT_UNKNOWN, and we cannot recurse
                     * without knowing whether this is a directory. This is the one case where a
                     * names-only walk still has to read an inode; the counter makes it visible. */
                    if (child_d_type == DT_UNKNOWN) {
                        struct stat probe;

                        if (ecrawl_io_fstatat_nf(dir_fd, ent_name, &probe) != 0) {
                            fprintf(stderr, "ERROR worker fstatat %s/%s: %s\n", dir_path, ent_name,
                                    strerror(errno));
                            stats_add_error(shared);
                            continue;
                        }
                        ATOMIC_ADD_RELAXED(&g_nostat_dtype_unknown_fallbacks, 1);
                        child_d_type = dtype_from_mode(probe.st_mode);
                    }

                    if (child_d_type == DT_DIR) {
                        char *child_path_owned;
                        size_t child_path_len;
                        int child_contains;

                        if (path_join_alloc(dir_path, dir_path_len, ent_name, child_name_len, &child_path_owned,
                                            &child_path_len) != 0) {
                            fprintf(stderr, "ERROR worker path alloc %s/%s: %s\n", dir_path, ent_name,
                                    strerror(errno));
                            stats_add_error(shared);
                            continue;
                        }
                        /* Resolve the child's match here, while the parent's window is loaded, so the
                         * child never rescans its own full path when it is popped. */
                        child_contains = dirmatch_hit(&wa->nostat, &dm, ent_name, child_name_len) ? CONTAINS_YES
                                                                                                  : CONTAINS_NO;
                        /* Accounted and printed on pop, like every other directory. */
                        if (dir_stack_push_take(stack, child_path_owned, child_path_len, NULL, 0, child_contains) !=
                            0) {
                            fprintf(stderr, "ERROR worker stack push %s: %s\n", child_path_owned, strerror(errno));
                            stats_add_error(shared);
                            (void)discovered_dir_batch_push(&disc_b, child_path_owned, child_path_len, NULL, 0,
                                                            child_contains);
                        }
                        dirs_since_donate_check++;
                        donate_spill_periodic(shared, stack, queue, aux, &dirs_since_donate_check);
                    } else {
                        account_entry_dtype(stats, perf, child_d_type);
                        progress_tick(wa);
                        /* --count tallies only; skip path assembly and the stdout stream. */
                        if (!g_nostat_count && dirmatch_hit(&wa->nostat, &dm, ent_name, child_name_len)) {
                            char child[PATH_MAX];
                            size_t child_path_len = dir_path_len + child_name_len +
                                                    ((dir_path_len == 1 && dir_path[0] == '/') ? 0U : 1U);

                            /* Only matches pay for a full path assembly. */
                            if (path_join_fast(dir_path, dir_path_len, ent_name, child_name_len, child,
                                               sizeof(child)) != 0) {
                                fprintf(stderr, "ERROR worker path too long: %s/%s\n", dir_path, ent_name);
                                stats_add_error(shared);
                                continue;
                            }
                            if (pathout_emit(&wa->nostat, child, child_path_len) != 0) stats_add_error(shared);
                        }
                    }
                }
                donate_spill_if_needed(shared, stack, queue, aux);
            } else if (uring && uring->fd >= 0) {
                crawl_dir_entries_iouring(&rd, dir_fd, dir_path, dir_path_len, shared, stats, perf, emit,
                                          stack, queue, aux, &disc_b, wa, uring);
            } else {
                size_t dirs_since_donate_check = 0;
                size_t entries_since_donate_check = 0;

                while (ecrawl_dir_reader_next(&rd, &ent_name, &ent_dtype, &ent_ino) == 1) {
                    size_t child_name_len;
                    struct stat child_st;
                    unsigned char child_d_type = ent_dtype;

                    if (strcmp(ent_name, ".") == 0 || strcmp(ent_name, "..") == 0) continue;
                    entries_since_donate_check++;
                    donate_spill_on_entries(shared, stack, queue, aux, &entries_since_donate_check);

                    child_name_len = strlen(ent_name);
                    if (child_d_type == DT_DIR) {
                        crawl_handle_dirent_dt_dir(dir_fd, dir_path, dir_path_len, ent_name, child_name_len,
                                                   shared, stats, perf, emit, stack, queue, aux, &disc_b);
                        progress_tick(wa);
                        dirs_since_donate_check++;
                        donate_spill_periodic(shared, stack, queue, aux, &dirs_since_donate_check);
                    } else {
                        if (ecrawl_io_fstatat_nf(dir_fd, ent_name, &child_st) != 0) {
                            fprintf(stderr, "ERROR worker fstatat %s/%s: %s\n", dir_path, ent_name, strerror(errno));
                            stats_add_error(shared);
                            continue;
                        }
                        if (S_ISDIR(child_st.st_mode)) {
                            char *child_path_owned;
                            size_t child_path_len;

                            if (path_join_alloc(dir_path, dir_path_len, ent_name, child_name_len,
                                                &child_path_owned, &child_path_len) != 0) {
                                fprintf(stderr, "ERROR worker path alloc %s/%s: %s\n", dir_path, ent_name,
                                        strerror(errno));
                                stats_add_error(shared);
                                continue;
                            }
                            if (discovered_dir_batch_push(&disc_b, child_path_owned, child_path_len, &child_st,
                                                          0, 0) != 0)
                                continue;
                            dirs_since_donate_check++;
                            donate_spill_periodic(shared, stack, queue, aux, &dirs_since_donate_check);
                        } else {
                            uint64_t contrib;

                            record_ids_from_stat(&child_st);
                            contrib = account_entry_shared(emit, shared, stats, perf, &child_st);
                            progress_tick(wa);
                            if (!g_no_write) {
                                char child[PATH_MAX];
                                size_t child_path_len = dir_path_len + child_name_len +
                                                        ((dir_path_len == 1 && dir_path[0] == '/') ? 0U : 1U);

                                if (path_join_fast(dir_path, dir_path_len, ent_name, child_name_len, child,
                                                   sizeof(child)) != 0) {
                                    fprintf(stderr, "ERROR worker path too long: %s/%s\n", dir_path, ent_name);
                                    stats_add_error(shared);
                                    continue;
                                }
                                if (emit_record(emit, child, child_path_len, &child_st, contrib) != 0) {
                                    fprintf(stderr, "ERROR worker emit_record %s: %s\n", child, strerror(errno));
                                    stats_add_error(shared);
                                }
                            }
                        }
                    }
                }
                donate_spill_if_needed(shared, stack, queue, aux);
            }
        }

        (void)discovered_dir_batch_flush(&disc_b);
        ecrawl_dir_reader_close(&rd);
        free(dir_path);
    }

    discovered_dir_batch_fini(&disc_b);
    return 0;
}

/* Helper threads (stats tick, disk-space monitor) idle between wakeups. A plain sleep(1) is
 * not interruptible, so pthread_join at shutdown had to wait out the in-flight sleep: every
 * run paid up to a full second of dead time after the crawl finished, which quantized total
 * wall time to whole seconds. Wait on a condvar instead, broadcast when a stop is requested. */
static pthread_mutex_t g_helper_wait_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_helper_wait_cond = PTHREAD_COND_INITIALIZER;

static int stats_stop_requested(void) {
    return atomic_load(&g_stop_stats) != 0;
}

static int disk_monitor_stop_requested(void) {
    return atomic_load_explicit(&g_disk_monitor_stop, memory_order_acquire) != 0U;
}

/* Idle for `seconds` unless stopped(). Returns 1 when the full interval elapsed, 0 when a
 * stop was requested. Waiting on an absolute deadline makes spurious wakeups (including a
 * broadcast aimed at the other helper) resume the remaining time rather than tick early. */
static int helper_wait_or_stop(double seconds, int (*stopped)(void)) {
    struct timespec deadline;
    long nsec;
    int rc = 0;

    if (stopped()) return 0;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += (time_t)seconds;
    nsec = deadline.tv_nsec + (long)((seconds - (double)(time_t)seconds) * 1e9);
    if (nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        nsec -= 1000000000L;
    }
    deadline.tv_nsec = nsec;

    pthread_mutex_lock(&g_helper_wait_lock);
    while (!stopped() && rc != ETIMEDOUT) {
        rc = pthread_cond_timedwait(&g_helper_wait_cond, &g_helper_wait_lock, &deadline);
    }
    pthread_mutex_unlock(&g_helper_wait_lock);

    return stopped() ? 0 : 1;
}

/* Wake both helpers so their joins return without waiting out the current interval. */
static void helper_stop_broadcast(void) {
    pthread_mutex_lock(&g_helper_wait_lock);
    pthread_cond_broadcast(&g_helper_wait_cond);
    pthread_mutex_unlock(&g_helper_wait_lock);
}

static void stats_stop_request(void) {
    atomic_store(&g_stop_stats, 1);
    helper_stop_broadcast();
}

static void disk_monitor_stop_request(void) {
    atomic_store(&g_disk_monitor_stop, 1);
    helper_stop_broadcast();
}

static void *stats_thread_main(void *arg) {
    static int stall_zero_secs = 0;
    static int stall_announced = 0;
    (void)arg;

    while (!atomic_load(&g_stop_stats)) {
        /* A partial final interval would skew the per-second rolling window, so a stop
         * request ends the loop instead of recording a short tick. */
        if (!helper_wait_or_stop(1.0, stats_stop_requested)) break;

        {
            int idx_record = atomic_load(&g_bucket_index);
            unsigned long long io_ls_tick = atomic_load(&g_io_lstat_calls);
            unsigned long long io_st_tick = atomic_load(&g_io_stat_calls);
            unsigned long long d_meta =
                (io_ls_tick - g_progress_prev_lstat) + (io_st_tick - g_progress_prev_stat);

            ATOMIC_ADD_RELAXED(&g_bucket_stat_meta[idx_record], d_meta);
            ATOMIC_ADD_RELAXED(&g_window_stat_meta, d_meta);
        }

        {
            int next = (atomic_load(&g_bucket_index) + 1) % WINDOW_SECONDS;
            unsigned long long expired_entries = atomic_exchange(&g_bucket_entries[next], 0);
            unsigned long long expired_files = atomic_exchange(&g_bucket_files[next], 0);
            unsigned long long expired_dirs = atomic_exchange(&g_bucket_dirs[next], 0);
            unsigned long long expired_stat_meta = atomic_exchange(&g_bucket_stat_meta[next], 0);
            atomic_fetch_sub(&g_window_entries, expired_entries);
            atomic_fetch_sub(&g_window_files, expired_files);
            atomic_fetch_sub(&g_window_dirs, expired_dirs);
            atomic_fetch_sub(&g_window_stat_meta, expired_stat_meta);
            atomic_store(&g_bucket_index, next);
        }

        {
            unsigned int seen = atomic_load(&g_seconds_seen);
            if (seen < WINDOW_SECONDS) atomic_store(&g_seconds_seen, seen + 1U);
        }

        {
            unsigned long long total_entries = atomic_load(&g_total_entries);
            unsigned long long total_files = atomic_load(&g_total_files);
            unsigned long long total_dirs = atomic_load(&g_total_dirs);
            unsigned long long total_bytes = atomic_load(&g_total_bytes);
            unsigned long long window_entries = atomic_load(&g_window_entries);
            unsigned long long window_stat_meta = atomic_load(&g_window_stat_meta);
            unsigned int divisor = atomic_load(&g_seconds_seen);
            double ops_rate;
            double stat_meta_rate;
            double elapsed_sec = g_run_start_sec > 0.0 ? now_sec() - g_run_start_sec : 0.0;
            char te[32], tf[32], td[32], ts[32], robj[32], rst[32], elapsed_buf[32];
            unsigned long long io_rd = atomic_load(&g_io_readdir_calls);
            unsigned long long io_ls = atomic_load(&g_io_lstat_calls);
            unsigned long long io_st = atomic_load(&g_io_stat_calls);
            unsigned long long d_tot = total_entries - g_progress_prev_tot_entries;
            unsigned long long d_rd = io_rd - g_progress_prev_readdir;
            unsigned long long d_ls = io_ls - g_progress_prev_lstat;

            if (divisor == 0) divisor = 1;
            ops_rate = (double)window_entries / (double)divisor;
            stat_meta_rate = (double)window_stat_meta / (double)divisor;

            if (g_stall_hint_seconds_cfg > 0 && elapsed_sec >= (double)WINDOW_SECONDS &&
                divisor >= (unsigned int)WINDOW_SECONDS && window_entries == 0ULL) {
                stall_zero_secs++;
                if (stall_zero_secs >= g_stall_hint_seconds_cfg && !stall_announced) {
                    unsigned long long qdepth = atomic_load(&g_queue_depth);
                    unsigned long long wqdepth = atomic_load(&g_writer_queue_depth);
                    int active = atomic_load(&g_active_workers);
                    unsigned long long wcrawl = atomic_load(&g_wait_crawl_tasks);
                    unsigned long long wwp = atomic_load(&g_wait_writer_push);
                    unsigned long long wwc = atomic_load(&g_wait_writer_pop);

                    fprintf(stderr,
                            "ecrawl: stall hint: rolling-window entries stayed at 0 for %d s "
                            "(elapsed %.1f s; total_entries=%llu; q=%llu wq=%llu active=%d; "
                            "last_sec delta: entries=%llu readdir=%llu stat_meta=%llu; "
                            "wait: crawl_q=%llu wr_push=%llu wr_pop=%llu)\n",
                            stall_zero_secs, elapsed_sec, (unsigned long long)total_entries, qdepth, wqdepth, active,
                            d_tot, d_rd, d_ls + (io_st - g_progress_prev_stat), wcrawl, wwp, wwc);
                    fflush(stderr);
                    stall_announced = 1;
                }
            } else {
                stall_zero_secs = 0;
                stall_announced = 0;
            }

            g_progress_prev_tot_entries = total_entries;
            g_progress_prev_readdir = io_rd;
            g_progress_prev_lstat = io_ls;
            g_progress_prev_stat = io_st;

            if (g_verbose) {
                unsigned long long qdepth = atomic_load(&g_queue_depth);
                int active = atomic_load(&g_active_workers);

                g_ops_rate_sum += ops_rate;
                if (g_ops_rate_samples == 0 || ops_rate < g_ops_rate_min) g_ops_rate_min = ops_rate;
                if (g_ops_rate_samples == 0 || ops_rate > g_ops_rate_max) g_ops_rate_max = ops_rate;
                g_ops_rate_samples++;
                g_active_workers_sum += (uint64_t)active;
                if (g_active_workers_samples == 0 || active < g_active_workers_min) g_active_workers_min = active;
                if (g_active_workers_samples == 0 || active > g_active_workers_max) g_active_workers_max = active;
                g_active_workers_samples++;
                if (active <= 1) g_seconds_single_worker++;
                if (qdepth == 0 && active == 1) g_seconds_queue_empty_single_worker++;
            }
            human_decimal((double)total_entries, te, sizeof(te));
            human_decimal((double)total_files, tf, sizeof(tf));
            human_decimal((double)total_dirs, td, sizeof(td));
            human_decimal((double)total_bytes, ts, sizeof(ts));
            human_decimal(ops_rate, robj, sizeof(robj));
            human_decimal(stat_meta_rate, rst, sizeof(rst));
            format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

            if (isatty(STDOUT_FILENO)) {
                if (!g_verbose) {
                    printf("\rstat/s(10s):%s obj/s(10s):%s | tot:%s f:%s d:%s s:%s | el:%s            ",
                           rst, robj, te, tf, td, ts, elapsed_buf);
                } else {
                    unsigned long long qdepth = atomic_load(&g_queue_depth);
                    unsigned long long popped = atomic_load(&g_tasks_popped);
                    unsigned long long writer_qdepth = atomic_load(&g_writer_queue_depth);
                    int active = atomic_load(&g_active_workers);
                    char pp[32];

                    human_decimal((double)popped, pp, sizeof(pp));
                    printf("\rstat/s(10s):%s obj/s(10s):%s | tot:%s f:%s d:%s s:%s | q:%llu wq:%llu t:%d p:%s | "
                           "el:%s            ",
                           rst, robj, te, tf, td, ts, qdepth, writer_qdepth, active, pp, elapsed_buf);
                }
                fflush(stdout);
            }
        }
    }

    return NULL;
}

static void *worker_thread_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;
    crawl_identity_t ci;

    if (crawl_identity_init(&ci, arg) != 0) return NULL;

    for (;;) {
        dir_stack_t task;

        if (queue_pop_wait(arg->queue, &task) != 0) break;

        crawl_run_task(&task, arg, &ci);
    }

    crawl_identity_destroy(&ci, arg);
    tls_flush_thread_batch_counters();
    return NULL;
}

static int writer_close_lru_shard(shard_file_state_t *shards, uint32_t writer_index,
                                  uint32_t uid_shards, unsigned *open_count) {
    uint32_t i;
    shard_file_state_t *victim = NULL;

    for (i = writer_index; i < uid_shards; i += (uint32_t)g_writer_threads) {
        if (shards[i].fp) {
            if (!victim || shards[i].last_used < victim->last_used) victim = &shards[i];
        }
    }
    if (!victim) return 0;

    {
        uint32_t shard = (uint32_t)(victim - shards);
        char path[PATH_MAX];
        if (build_shard_path(shard, path, sizeof(path)) == 0)
            (void)shard_flush_ckpt_before_close(victim, path, 0);
    }
    ATOMIC_ADD_RELAXED(&g_shard_evictions, 1);
    ecrawl_io_fclose(victim->fp);
    victim->fp = NULL;
    /* Release the block writer's buffers while evicted to bound writer memory to
     * ~max_open_shards blocks; reopen re-inits a fresh (empty) block writer. */
    if (victim->blk_inited) {
        crawl_bin_block_writer_free(&victim->blk);
        victim->blk_inited = 0;
    }
    if (*open_count > 0) (*open_count)--;
    return 1;
}

static void writer_trim_shards(shard_file_state_t *shards, uint32_t writer_index,
                               uint32_t uid_shards, unsigned *open_count, unsigned target) {
    while (*open_count > target) {
        if (!writer_close_lru_shard(shards, writer_index, uid_shards, open_count)) break;
    }
}

static int writer_open_shard_file(shard_file_state_t *state, const char *path) {
    if (!state->blk_inited) {
        if (crawl_bin_block_writer_init(&state->blk) != 0) return -1;
        state->blk_inited = 1;
    }
    if (state->bytes_written == 0) {
        /*
         * Avoid "a+" here: append streams may ignore seeks on write, so rewriting the 32-byte
         * header at offset 0 when finalizing the catalog would corrupt the shard tail instead.
         */
        ATOMIC_ADD_RELAXED(&g_shard_bin_opens, 1);
        state->fp = ecrawl_io_fopen(path, "wb+");
        if (!state->fp) return -1;
        setvbuf(state->fp, NULL, _IOFBF, WRITE_BUFFER_SIZE);
        if (write_bin_header(state->fp) != 0 || ecrawl_io_fflush(state->fp) != 0) {
            int saved_errno = errno ? errno : EIO;
            ecrawl_io_fclose(state->fp);
            state->fp = NULL;
            errno = saved_errno;
            return -1;
        }
        state->bytes_written = sizeof(bin_file_header_t);
        if (shard_ckpt_init_new(state) != 0) {
            int saved_errno = errno ? errno : ENOMEM;
            ecrawl_io_fclose(state->fp);
            state->fp = NULL;
            errno = saved_errno;
            return -1;
        }
        if (shard_cat_init_fresh(&state->cat) != 0) {
            int saved_errno = errno ? errno : ENOMEM;
            ecrawl_io_fclose(state->fp);
            state->fp = NULL;
            errno = saved_errno;
            return -1;
        }
        state->cat_live = 1;
    } else {
        struct stat st;
        bin_file_header_t fh;
        crawl_bin_catalog_t cat;
        uint64_t fsz;
        uint64_t co;

        if (ecrawl_io_stat(path, &st) != 0) return -1;
        fsz = (uint64_t)st.st_size;

        ATOMIC_ADD_RELAXED(&g_shard_bin_reopens, 1);
        state->fp = ecrawl_io_fopen(path, "rb+");
        if (!state->fp) return -1;
        setvbuf(state->fp, NULL, _IOFBF, WRITE_BUFFER_SIZE);

        if (fread(&fh, sizeof(fh), 1, state->fp) != 1) goto reopen_fail;
        if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
            errno = EINVAL;
            goto reopen_fail;
        }
        co = fh.catalog_offset;
        if (co == 0ULL || co < sizeof(fh) || co > fsz) {
            errno = EINVAL;
            goto reopen_fail;
        }

        /* Hot path for many-shard workloads: the in-memory catalog survived eviction, so reuse it
         * and skip the disk read + full hash-table rebuild. Otherwise (defensive / not retained)
         * reconstruct it from the on-disk catalog. */
        if (!state->cat_live) {
            crawl_bin_catalog_init_empty(&cat);
            if (crawl_bin_catalog_load(state->fp, co, fsz, &cat) != 0) {
                crawl_bin_catalog_free(&cat);
                goto reopen_fail;
            }
            if (shard_cat_load_from_disk_catalog(&state->cat, &cat) != 0) {
                crawl_bin_catalog_free(&cat);
                goto reopen_fail;
            }
            crawl_bin_catalog_free(&cat);
        }

        if (ftruncate(fileno(state->fp), (off_t)co) != 0) goto reopen_fail;

        fh.catalog_offset = 0ULL;
        if (fseeko(state->fp, 0, SEEK_SET) != 0) goto reopen_fail;
        if (ecrawl_io_fwrite(&fh, sizeof(fh), 1, state->fp) != 1) goto reopen_fail;
        if (ecrawl_io_fflush(state->fp) != 0) goto reopen_fail;
        if (fseeko(state->fp, 0, SEEK_END) != 0) goto reopen_fail;
        if ((uint64_t)ftello(state->fp) != co) {
            errno = EINVAL;
            goto reopen_fail;
        }

        state->bytes_written = co;
        /* ckpt offsets are retained alongside a live catalog; only reload them from the sidecar when
         * the in-memory state was not kept (cat_live == 0). */
        if (!state->cat_live) {
            if (shard_ckpt_load_for_append(state, path, co) != 0) {
                int saved_errno = errno ? errno : EINVAL;
                ecrawl_io_fclose(state->fp);
                state->fp = NULL;
                shard_state_release(state);
                errno = saved_errno;
                return -1;
            }
            state->cat_live = 1;
        }
    }
    return 0;

reopen_fail:
    {
        int saved_errno = errno ? errno : EINVAL;
        ecrawl_io_fclose(state->fp);
        state->fp = NULL;
        shard_state_release(state);
        errno = saved_errno;
        return -1;
    }
}

static int writer_acquire_shard(shard_file_state_t *shards, uint32_t writer_index, uint32_t shard,
                                uint32_t uid_shards, unsigned max_open_shards,
                                unsigned *open_count, uint64_t *tick, FILE **fp_out) {
    shard_file_state_t *state = &shards[shard];
    char path[PATH_MAX];
    struct stat st;
    unsigned retry;

    if (state->fp) {
        state->last_used = ++(*tick);
        *fp_out = state->fp;
        return 0;
    }

    if (!state->initialized) {
        state->bytes_written = 0;
        state->initialized = 1;
    }

    if (build_shard_path(shard, path, sizeof(path)) != 0) return -1;

    if (state->bytes_written != 0 && ecrawl_io_stat(path, &st) != 0) return -1;

    if (*open_count >= max_open_shards)
        writer_close_lru_shard(shards, writer_index, uid_shards, open_count);

    for (retry = 0; retry <= EMFILE_RETRY_LIMIT; retry++) {
        if (writer_open_shard_file(state, path) == 0) break;
        if (errno != EMFILE || retry == EMFILE_RETRY_LIMIT) return -1;
        atomic_store(&g_fd_pressure, 1U);
        writer_close_lru_shard(shards, writer_index, uid_shards, open_count);
        emfile_retry_pause(retry);
    }

    (*open_count)++;
    state->last_used = ++(*tick);
    *fp_out = state->fp;
    return 0;
}

static void writer_wait_disk_ok(void) {
    if (g_no_write) return;
    for (;;) {
        if (atomic_load_explicit(&g_disk_wait_disabled, memory_order_acquire)) return;
        if (!atomic_load_explicit(&g_disk_low, memory_order_acquire)) return;
        {
            struct timespec ts;

            ts.tv_sec = 0;
            ts.tv_nsec = 100000000L;
            nanosleep(&ts, NULL);
        }
    }
}

static void *disk_monitor_thread_main(void *arg) {
    int show_paused = 0;

    (void)arg;

    for (;;) {
        struct statvfs sv;
        uint64_t avail = 0;
        int err = 0;
        int have = 0;

        if (atomic_load_explicit(&g_disk_monitor_stop, memory_order_acquire)) break;

        if (statvfs(g_output_dir, &sv) != 0) {
            err = errno;
            have = 0;
        } else if (sv.f_frsize == 0) {
            err = EINVAL;
            have = 0;
        } else {
            avail = (uint64_t)sv.f_bavail * (uint64_t)sv.f_frsize;
            have = 1;
        }

        if (!have || avail < DISK_MIN_FREE_BYTES) {
            if (!show_paused) {
                if (!have)
                    fprintf(stderr, "PAUSE ecrawl: statvfs(%s) failed: %s — shard writes paused until check succeeds\n",
                            g_output_dir, strerror(err));
                else
                    fprintf(stderr,
                            "PAUSE ecrawl: output filesystem free space %" PRIu64 " bytes (minimum %" PRIu64 " bytes) — shard writes paused\n",
                            avail, (uint64_t)DISK_MIN_FREE_BYTES);
                fflush(stderr);
                show_paused = 1;
            }
            atomic_store_explicit(&g_disk_low, 1U, memory_order_release);
        } else {
            if (show_paused) {
                fprintf(stderr,
                        "CONTINUE ecrawl: output filesystem free space %" PRIu64 " bytes — shard writes unpaused\n",
                        avail);
                fflush(stderr);
                show_paused = 0;
            }
            atomic_store_explicit(&g_disk_low, 0U, memory_order_release);
        }

        if (!helper_wait_or_stop((double)DISK_SPACE_CHECK_INTERVAL_SEC, disk_monitor_stop_requested)) break;
    }

    return NULL;
}

/* When many records share the same parent path within one uid shard, skip repeated shard_cat_ensure_dir
 * walks (hash hit is cheap; strcmp+hash still adds up after sort-by-shard groups siblings). */
static __thread struct {
    uint32_t shard;
    unsigned char valid;
    char parent[PATH_MAX];
    size_t parent_len;
    uint64_t pid;
} g_writer_parent_cache;

/* When disk record fits this many bytes, write header + name with one fwrite (fewer stdio calls than two). */
#define WRITER_ONESHOT_RECORD_BYTES 8192U

typedef struct {
    size_t off;
    uint32_t shard;
} writer_batch_frame_t;

static int cmp_writer_batch_frame(const void *a, const void *b) {
    const writer_batch_frame_t *x = (const writer_batch_frame_t *)a;
    const writer_batch_frame_t *y = (const writer_batch_frame_t *)b;

    if (x->shard < y->shard) return -1;
    if (x->shard > y->shard) return 1;
    if (x->off < y->off) return -1;
    if (x->off > y->off) return 1;
    return 0;
}

/*
 * Process one framed record in a writer batch. frame_off points at batch_frame_hdr_t.
 * Crawl threads append frames in path order, which interleaves many uid shards per ~1MiB batch and
 * thrashes the per-writer LRU (fopen/fclose). writer_process_batch sorts frame offsets by shard
 * before calling here; shard_cat_ensure_dir() still builds parents from full paths, and per-parent
 * rollups are order-independent sums, so output remains consistent while improving shard locality.
 * Records whose on-disk wire size is <= WRITER_ONESHOT_RECORD_BYTES are written with one fwrite
 * (header + name) instead of two.
 */
/* Compress and append the shard's pending record block to its file, pushing
 * a checkpoint offset at the block boundary once a stride's worth of file data
 * has accumulated (ckpt offsets are block starts, so segments align to blocks).
 * No-op when nothing is buffered. */
static int shard_block_flush(shard_file_state_t *st) {
    uint64_t block_start = st->bytes_written;
    uint64_t written = 0;

    if (crawl_bin_block_writer_pending(&st->blk) == 0) return 0;
    if (block_start - st->seg_start_byte >= CRAWL_CKPT_STRIDE_BYTES) {
        if (shard_ckpt_push(st, block_start) != 0) {
            if (errno == 0) errno = ENOMEM;
            return -1;
        }
        st->seg_start_byte = block_start;
    }
    if (crawl_bin_block_writer_flush(&st->blk, st->fp, ecrawl_io_fwrite, &written) != 0) return -1;
    st->bytes_written += written;
    return 0;
}

static int writer_process_batch_frame(uint32_t writer_index, shard_file_state_t *shards, unsigned *open_count,
                                      uint64_t *tick, const record_batch_t *batch, size_t frame_off,
                                      size_t *next_off_out) {
    batch_frame_hdr_t frame;
    FILE *fp;
    size_t payload_off = frame_off + sizeof(frame);

    memcpy(&frame, batch->data + frame_off, sizeof(frame));

    if (frame.shard >= g_uid_shards || payload_off + frame.data_len > batch->len) {
        errno = EINVAL;
        return -1;
    }
    if ((frame.shard % (uint32_t)g_writer_threads) != writer_index) {
        errno = EINVAL;
        return -1;
    }

    if (writer_acquire_shard(shards, writer_index, frame.shard, g_uid_shards, g_max_open_shards, open_count, tick,
                             &fp) != 0) {
        return -1;
    }
    (void)fp; /* records now flow through the per-shard block writer (st->blk -> st->fp) */

    {
        shard_file_state_t *st = &shards[frame.shard];
        unsigned char *payload = batch->data + payload_off;
        bin_record_hdr_t wire;
        bin_record_hdr_t disk;
        size_t wire_len = (size_t)frame.data_len;
        size_t disk_len;
        const char *base = NULL;
        size_t base_len = 0;
        /* path_z holds the record's full path while parent_dir_id == 0; `base` points into it and is
         * read again below when the record name is written, so path_z must outlive that inner block. */
        char path_z[PATH_MAX];

        if (wire_len < sizeof(wire)) {
            errno = EINVAL;
            return -1;
        }
        memcpy(&wire, payload, sizeof(wire));

        if (wire.parent_dir_id == 0ULL) {
            char parent[PATH_MAX];
            uint64_t pid;

            if (wire.name_len == 0 || wire_len != sizeof(wire) + (size_t)wire.name_len) {
                errno = EINVAL;
                return -1;
            }
            if ((size_t)wire.name_len + 1ULL >= sizeof(path_z)) {
                errno = EINVAL;
                return -1;
            }
            memcpy(path_z, payload + sizeof(wire), wire.name_len);
            path_z[wire.name_len] = '\0';

            if (split_parent_basename(path_z, parent, sizeof(parent), &base, &base_len) != 0) {
                errno = EINVAL;
                return -1;
            }
            if (base_len > (size_t)UINT16_MAX) {
                errno = EINVAL;
                return -1;
            }

            if (!g_writer_parent_cache.valid || g_writer_parent_cache.shard != frame.shard) {
                g_writer_parent_cache.valid = 0;
            } else {
                size_t plen = strlen(parent);

                if (g_writer_parent_cache.parent_len == plen && memcmp(g_writer_parent_cache.parent, parent, plen + 1) == 0) {
                    pid = g_writer_parent_cache.pid;
                    goto parent_resolved;
                }
            }

            pid = shard_cat_ensure_dir(&st->cat, parent);
            if (pid == 0ULL) {
                if (errno == 0) errno = EINVAL;
                return -1;
            }
            {
                size_t plen = strlen(parent);

                if (plen < sizeof(g_writer_parent_cache.parent)) {
                    memcpy(g_writer_parent_cache.parent, parent, plen + 1);
                    g_writer_parent_cache.parent_len = plen;
                    g_writer_parent_cache.pid = pid;
                    g_writer_parent_cache.shard = frame.shard;
                    g_writer_parent_cache.valid = 1;
                } else
                    g_writer_parent_cache.valid = 0;
            }
        parent_resolved:

            disk = wire;
            disk.parent_dir_id = pid;
            disk.name_len = (uint16_t)base_len;
            disk_len = sizeof(disk) + disk.name_len;
        } else {
            if (wire_len != crawl_bin_record_total_bytes(&wire)) {
                errno = EINVAL;
                return -1;
            }
            disk = wire;
            disk_len = wire_len;
        }

        /* Update per-dir catalog aggregates (immediate-child rollup) using
         * the byte_credit computed once during the crawl. */
        shard_cat_update_imm_child_rollup(&st->cat, disk.parent_dir_id, frame.byte_credit, &disk);

        /*
         * A directory's own record is an immediate child of its *parent*, so the
         * subtree sums rooted at it never include it. Park its byte credit on its
         * own catalog row so a --subtree query can add it back and match du -sb,
         * which counts the directory it was handed. Only reachable when the
         * writer resolved the path itself (parent_dir_id == 0 on the wire);
         * path_z holds the full path in exactly that case.
         */
        if (disk.type == (uint8_t)'d' && wire.parent_dir_id == 0ULL) {
            uint64_t own = shard_cat_ensure_dir(&st->cat, path_z);

            if (own == 0ULL) {
                if (errno == 0) errno = EINVAL;
                return -1;
            }
            shard_cat_set_self_bytes(&st->cat, own, frame.byte_credit);
        }

        /* Serialize the record into the pending block buffer; flush a
         * compressed block once it reaches the raw target. (void)disk_len. */
        (void)disk_len;
        {
            const unsigned char *nm = payload + sizeof(wire);

            if (wire.parent_dir_id == 0ULL) nm = (const unsigned char *)base;
            if (crawl_bin_block_writer_append_record(&st->blk, &disk, nm) != 0) {
                if (errno == 0) errno = ENOMEM;
                return -1;
            }
        }
        /* Flush on decoded bytes or on the record cap, whichever comes first:
         * a shard of very short names would otherwise buffer far more rows than
         * the column arrays are allowed to hold. */
        if (crawl_bin_block_writer_pending(&st->blk) >= CRAWL_BIN_BLOCK_RAW_TARGET ||
            crawl_bin_block_writer_records(&st->blk) >= CRAWL_BIN_ROWGROUP_MAX_RECORDS) {
            if (shard_block_flush(st) != 0) return -1;
        }
    }

    shards[frame.shard].last_used = ++(*tick);
    *next_off_out = payload_off + frame.data_len;
    return 0;
}

static int writer_process_batch(uint32_t writer_index,
                                shard_file_state_t *shards,
                                unsigned *open_count,
                                uint64_t *tick,
                                      record_batch_t *batch) {
    size_t scan;
    size_t nframes;
    size_t i;
    writer_batch_frame_t *order = NULL;

    /* Trigram journal tee: one atomic incref + bounded try-push per pool lane; on overflow the
     * pool voids the run's journals and the crawl continues unaffected. The batch buffer is
     * read-only for both consumers (the per-batch qsort below orders an offset index). */
    if (g_trijournal_pool) trijournal_pool_offer(g_trijournal_pool, batch);

    writer_wait_disk_ok();

    if (atomic_load(&g_fd_pressure) && *open_count > 1U) {
        writer_trim_shards(shards, writer_index, g_uid_shards, open_count, *open_count / 2U);
        atomic_store(&g_fd_pressure, 0U);
    }

    nframes = 0;
    for (scan = 0; scan + sizeof(batch_frame_hdr_t) <= batch->len;) {
        batch_frame_hdr_t fh;

        memcpy(&fh, batch->data + scan, sizeof(fh));
        if (fh.shard >= g_uid_shards || scan + sizeof(fh) + fh.data_len > batch->len) {
            errno = EINVAL;
            return -1;
        }
        if ((fh.shard % (uint32_t)g_writer_threads) != writer_index) {
            errno = EINVAL;
            return -1;
        }
        nframes++;
        scan += sizeof(fh) + fh.data_len;
    }
    if (scan != batch->len) {
        errno = EINVAL;
        return -1;
    }

    if (nframes > 1U) {
        order = (writer_batch_frame_t *)malloc(nframes * sizeof(*order));
        if (!order) {
            errno = ENOMEM;
            return -1;
        }
        scan = 0;
        for (i = 0; i < nframes; i++) {
            batch_frame_hdr_t fh;

            memcpy(&fh, batch->data + scan, sizeof(fh));
            order[i].off = scan;
            order[i].shard = fh.shard;
            scan += sizeof(fh) + fh.data_len;
        }
        qsort(order, nframes, sizeof(*order), cmp_writer_batch_frame);
        for (i = 0; i < nframes; i++) {
            size_t next_off;

            if (writer_process_batch_frame(writer_index, shards, open_count, tick, batch, order[i].off, &next_off) !=
                0) {
                free(order);
                return -1;
            }
            (void)next_off;
        }
        free(order);
        return 0;
    }

    if (nframes == 1U) {
        size_t next_off;

        if (writer_process_batch_frame(writer_index, shards, open_count, tick, batch, 0, &next_off) != 0) return -1;
        if (next_off != batch->len) {
            errno = EINVAL;
            return -1;
        }
        return 0;
    }

    return 0;
}

/* Binding facts for the trigram journal finalize: re-stat the shard and re-read its header,
 * the same five facts the journal reader will re-check (sidecar five-fact contract). */
static void trijournal_collect_shard_facts(const char *bin_path, uint32_t shard, uint32_t *shards_out,
                                           trij_binding_t *facts_out, size_t *n) {
    int fd;
    struct stat st;
    bin_file_header_t fh;
    uint64_t n_entries = 0;
    trij_binding_t *b;

    fd = open(bin_path, O_RDONLY);
    if (fd < 0) return;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) goto out;
    if (pread(fd, &fh, sizeof(fh), 0) != (ssize_t)sizeof(fh)) goto out;
    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) goto out;
    if (fh.catalog_offset < sizeof(fh) || fh.catalog_offset >= (uint64_t)st.st_size) goto out;
    if ((uint64_t)st.st_size - fh.catalog_offset < sizeof(uint64_t)) goto out;
    if (pread(fd, &n_entries, sizeof(n_entries), (off_t)fh.catalog_offset) != (ssize_t)sizeof(n_entries))
        goto out;

    b = &facts_out[*n];
    b->shard_size = (uint64_t)st.st_size;
    b->shard_mtime_sec = (uint64_t)st.st_mtim.tv_sec;
    b->shard_mtime_nsec = (uint64_t)st.st_mtim.tv_nsec;
    b->catalog_offset = fh.catalog_offset;
    b->catalog_entries = n_entries;
    b->max_dir_id = n_entries; /* v9 dir_ids are dense 1..N within the shard */
    shards_out[*n] = shard;
    (*n)++;
out:
    (void)close(fd);
}

static void *writer_thread_main(void *arg_void) {
    writer_arg_t *arg = (writer_arg_t *)arg_void;
    shard_file_state_t *shards = (shard_file_state_t *)calloc(g_uid_shards, sizeof(*shards));
    unsigned open_count = 0;
    uint64_t tick = 0;
    uint32_t *trij_fact_shards = NULL;
    trij_binding_t *trij_facts = NULL;
    size_t trij_facts_n = 0;

    if (!shards) {
        fprintf(stderr, "ERROR writer %u failed to allocate shard state\n", arg->writer_index);
        return NULL;
    }
    if (g_trijournal_pool) {
        trij_fact_shards = (uint32_t *)malloc(g_uid_shards * sizeof(*trij_fact_shards));
        trij_facts = (trij_binding_t *)malloc(g_uid_shards * sizeof(*trij_facts));
        if (!trij_fact_shards || !trij_facts) {
            fprintf(stderr, "warn: writer %u could not allocate trigram journal finalize state\n",
                    arg->writer_index);
            free(trij_fact_shards);
            free(trij_facts);
            trij_fact_shards = NULL;
            trij_facts = NULL;
        }
    }

    for (;;) {
        record_batch_t *batch = writer_queue_pop(arg->queue);
        if (!batch) break;
        if (writer_process_batch(arg->writer_index, shards, &open_count, &tick, batch) != 0) {
            fprintf(stderr, "ERROR writer %u failed processing batch: %s\n", arg->writer_index, strerror(errno));
            atomic_store(&g_writer_failed, 1U);
        }
        record_batch_release(batch);
    }

    {
        uint32_t i;
        for (i = arg->writer_index; i < g_uid_shards; i += (uint32_t)g_writer_threads) {
            /*
             * A shard evicted earlier and never touched again still has an
             * un-finalized catalog on disk, so reopen it rather than leaving its
             * subtree rollups zeroed. writer_open_shard_file's non-zero
             * bytes_written path reopens, truncates the old tail and clears
             * catalog_offset, which is exactly the state the final write needs.
             */
            if (!shards[i].fp && shards[i].initialized && shards[i].bytes_written != 0) {
                char path[PATH_MAX];
                if (build_shard_path(i, path, sizeof(path)) == 0 &&
                    writer_open_shard_file(&shards[i], path) != 0)
                    fprintf(stderr, "ERROR writer %u could not reopen shard %u to finalize: %s\n",
                            arg->writer_index, i, strerror(errno));
            }
            if (shards[i].fp) {
                char path[PATH_MAX];
                int flush_ok = 0;
                if (build_shard_path(i, path, sizeof(path)) == 0) {
                    if (shard_flush_ckpt_before_close(&shards[i], path, 1) != 0) {
                        fprintf(stderr, "ERROR writer %u failed finalizing shard %u: %s\n", arg->writer_index, i,
                                strerror(errno));
                        atomic_store(&g_writer_failed, 1U);
                    } else {
                        flush_ok = 1;
                    }
                }
                ecrawl_io_fclose(shards[i].fp);
                shards[i].fp = NULL;
                if (flush_ok && trij_facts)
                    trijournal_collect_shard_facts(path, i, trij_fact_shards, trij_facts, &trij_facts_n);
            }
            /* Free in-memory catalog/ckpt retained across eviction (now persisted on disk). */
            if (shards[i].cat_live) shard_state_release(&shards[i]);
            if (shards[i].blk_inited) {
                crawl_bin_block_writer_free(&shards[i].blk);
                shards[i].blk_inited = 0;
            }
        }
    }
    /* All of this writer's batches were teed before processing, so the journal queues hold
     * every shard frame ahead of this finalize (FIFO). */
    if (trij_facts) {
        if (trijournal_pool_finalize_shards(g_trijournal_pool, trij_fact_shards, trij_facts,
                                            trij_facts_n) != 0)
            fprintf(stderr, "warn: writer %u could not post trigram journal finalize\n", arg->writer_index);
        free(trij_fact_shards);
        free(trij_facts);
    }
    free(shards);
    crawl_bin_block_writer_tls_release();
    tls_flush_thread_batch_counters();
    return NULL;
}

static int enqueue_root_task(const char *path, shared_state_t *shared, task_queue_t *queue) {
    dir_stack_t task;
    struct stat st;
    char *dup;
    size_t path_len;

    dir_stack_init(&task);
    memset(&st, 0, sizeof(st));
    if (ecrawl_io_lstat(path, &st) != 0) {
        fprintf(stderr, "ERROR main lstat %s: %s\n", path, strerror(errno));
        stats_add_error(shared);
        return -1;
    }

    dup = strdup(path);
    if (!dup) {
        fprintf(stderr, "ERROR main stack push %s: %s\n", path, strerror(errno));
        stats_add_error(shared);
        return -1;
    }

    path_len = strlen(path);
    if (dir_stack_push_take(&task, dup, path_len, &st, 0, CONTAINS_UNKNOWN) != 0) {
        fprintf(stderr, "ERROR main stack push %s: %s\n", path, strerror(errno));
        free(dup);
        stats_add_error(shared);
        return -1;
    }

    if (queue_push_stack_take(queue, &task) != 0) {
        fprintf(stderr, "ERROR main failed to enqueue root task: %s\n", path);
        dir_stack_destroy(&task);
        stats_add_error(shared);
        return -1;
    }

    return 0;
}

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static int write_crawl_manifest(const char *start_path, int worker_count_started, double elapsed_sec,
                                const shared_state_t *totals) {
    FILE *fp;
    char manifest_path[PATH_MAX];
    time_t end_wall = time(NULL);

    if (snprintf(manifest_path, sizeof(manifest_path), "%s/crawl_manifest.txt", g_output_dir) >= (int)sizeof(manifest_path)) return -1;
    fp = ecrawl_io_fopen(manifest_path, "w");
    if (!fp) return -1;

    fprintf(fp, "format_version=%u\n", FORMAT_VERSION);
    fprintf(fp, "layout=uid_shards\n");
    fprintf(fp, "seed_mode=root_only\n");
    fprintf(fp, "start_path=%s\n", start_path);
    if (g_record_root && g_record_root[0] != '\0') fprintf(fp, "record_root=%s\n", g_record_root);
    fprintf(fp, "split_depth=%d\n", g_split_depth);
    fprintf(fp, "byte_accounting=unique_regular_files\n");
    fprintf(fp, "st_blocks_bytes_unit=%u\n", (unsigned)ST_BLOCKS_BYTES_UNIT);
    fprintf(fp, "total_allocated_bytes=%" PRIu64 "\n", totals->total_allocated_bytes);
    fprintf(fp, "files_sparse_heuristic=%" PRIu64 "\n", totals->files_sparse_heuristic);
    fprintf(fp, "crawl_threads=%d\n", worker_count_started);
    fprintf(fp, "writer_threads=%d\n", g_writer_threads);
    fprintf(fp, "uid_shards=%u\n", g_uid_shards);
    fprintf(fp, "uid_shard_digits=%d\n", g_shard_digits);
    fprintf(fp, "max_open_shards=%u\n", g_max_open_shards);
    fprintf(fp, "uid_output=uid.txt\n");
    fprintf(fp, "gid_output=gid.txt\n");
    fprintf(fp, "crawl_started_epoch=%lld\n", (long long)g_crawl_wall_clock_start);
    fprintf(fp, "crawl_finished_epoch=%lld\n", (long long)end_wall);
    fprintf(fp, "crawl_elapsed_sec=%.3f\n", elapsed_sec);
    ecrawl_io_fclose(fp);
    return 0;
}

static void print_queue_wait_metrics_to(FILE *fp) {
    fprintf(fp, "task_queue_pushes=%" PRIu64 "\n", (uint64_t)atomic_load(&g_task_queue_pushes));
    fprintf(fp, "queue_lock_waits=%" PRIu64 "\n", (uint64_t)atomic_load(&g_queue_lock_waits));
    fprintf(fp, "donate_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_donate_calls));
    fprintf(fp, "writer_queue_wait_ns=%" PRIu64 "\n", (uint64_t)atomic_load(&g_writer_queue_wait_ns));
    fprintf(fp, "wait_crawl_tasks=%" PRIu64 "\n", (uint64_t)atomic_load(&g_wait_crawl_tasks));
    fprintf(fp, "wait_writer_push=%" PRIu64 "\n", (uint64_t)atomic_load(&g_wait_writer_push));
    fprintf(fp, "wait_writer_pop=%" PRIu64 "\n", (uint64_t)atomic_load(&g_wait_writer_pop));
    fprintf(fp, "crawl_idle_ns=%" PRIu64 "\n", (uint64_t)atomic_load(&g_crawl_idle_ns));
}

/* What a names-only walk can and cannot report. Byte totals, hardlink dedup and the uid/gid
 * registries all derive from struct stat, so they are named as unavailable rather than printed
 * as zeros that would read as measured facts. */
static void print_no_stat_stats(FILE *fp) {
    fprintf(fp, "byte_accounting=(unavailable: --no-stat reads no inodes)\n");
    fprintf(fp, "hardlink_files=(unavailable: --no-stat reads no inodes)\n");
    fprintf(fp, "total_bytes=(unavailable: --no-stat reads no inodes)\n");
    fprintf(fp, "nostat_count=%d\n", g_nostat_count);
    if (g_contains_lower)
        fprintf(fp, "contains=%s\n", g_contains_lower);
    if (!g_nostat_count)
        fprintf(fp, "paths_printed=%" PRIu64 "\n", (uint64_t)atomic_load(&g_nostat_printed));
    /* Nonzero only on filesystems that do not report d_type, where recursion still needs a stat. */
    fprintf(fp, "dtype_unknown_fallbacks=%" PRIu64 "\n", (uint64_t)atomic_load(&g_nostat_dtype_unknown_fallbacks));
}

static void print_verbose_full_stats(FILE *fp, const shared_state_t *shared, double elapsed_sec,
                                      int writer_threads_used, const char *start_path) {
    double avg_ops = elapsed_sec > 0.0 ? (double)shared->total_entries / elapsed_sec : 0.0;
    double mean_ops = g_ops_rate_samples ? g_ops_rate_sum / (double)g_ops_rate_samples : avg_ops;
    double max_ops = g_ops_rate_samples ? g_ops_rate_max : avg_ops;
    double min_ops = g_ops_rate_samples ? g_ops_rate_min : avg_ops;
    uint64_t tasks_popped = (uint64_t)atomic_load(&g_tasks_popped);
    uint64_t apparent_bytes_total = shared->total_bytes + shared->dir_apparent_bytes + shared->symlink_apparent_bytes +
                                    shared->other_apparent_bytes;
    char avg_ops_buf[32], mean_ops_buf[32], max_ops_buf[32], min_ops_buf[32];

    human_decimal(avg_ops, avg_ops_buf, sizeof(avg_ops_buf));
    human_decimal(mean_ops, mean_ops_buf, sizeof(mean_ops_buf));
    human_decimal(max_ops, max_ops_buf, sizeof(max_ops_buf));
    human_decimal(min_ops, min_ops_buf, sizeof(min_ops_buf));

    fprintf(fp, "start_path=%s\n", start_path);
    if (g_record_root) fprintf(fp, "record_root=%s\n", g_record_root);
    fprintf(fp, "no_write=%d\n", g_no_write);
    fprintf(fp, "stat_impl=%s\n", g_iouring_statx ? "iouring" : (g_statx_mode ? "statx" : "fstatat"));
    fprintf(fp, "statx_mask=0x%x\n", (g_statx_mode || g_iouring_statx) ? g_statx_mask : 0u);
    fprintf(fp, "iouring_depth=%u\n", g_iouring_statx ? g_iouring_depth : 0u);
    fprintf(fp, "iouring_min_batch=%u\n", g_iouring_statx ? g_iouring_min_batch : 0u);
    fprintf(fp, "output_dir=%s\n", g_no_write ? "(disabled)" : g_output_dir);
    fprintf(fp, "output_layout=%s\n", g_no_write ? "none" : "uid_shards");
    /* Unique-byte hardlink dedup uses the inode registry whenever inodes are read. */
    fprintf(fp, "hardlink_dedup=%s\n", g_no_stat ? "off" : "on");
    fprintf(fp, "format_version=%u\n", FORMAT_VERSION);
    fprintf(fp, "seed_mode=%s\n", "root_only");
    fprintf(fp, "uid_shards=%u\n", g_uid_shards);
    fprintf(fp, "uid_shard_digits=%d\n", g_shard_digits);
    fprintf(fp, "writer_threads=%d\n", writer_threads_used);
    fprintf(fp, "crawl_threads=%d\n", g_crawl_threads);
    /* Idle time as a fraction of the crawl pool's thread-seconds: crawl_idle high means the pool is
     * wider than the tree can feed. */
    {
        double crawl_thread_sec = (double)shared->crawl_threads_started * elapsed_sec;
        double crawl_idle_sec = (double)atomic_load(&g_crawl_idle_ns) / 1e9;

        fprintf(fp, "crawl_idle_sec=%.3f\n", crawl_idle_sec);
        if (crawl_thread_sec > 0.0) fprintf(fp, "crawl_idle_pct=%.1f\n", 100.0 * crawl_idle_sec / crawl_thread_sec);
    }
    fprintf(fp, "max_open_shards=%u\n", g_no_write ? 0U : g_max_open_shards);
    fprintf(fp, "writer_queue_batches=%u\n", g_no_write ? 0U : g_writer_queue_batches);
    fprintf(fp, "trigram_journal=%s\n", g_trijournal_enabled ? g_trijournal_dir : "off");
    if (g_trijournal_enabled) {
        fprintf(fp, "trigram_journal_threads=%d\n", g_trijournal_threads);
        fprintf(fp, "trigram_journal_queue_batches=%u\n", g_trijournal_queue_batches);
        fprintf(fp, "trigram_journal_published=%" PRIu64 "\n", g_trijournal_final_published);
        fprintf(fp, "trigram_journal_entries=%" PRIu64 "\n", g_trijournal_final_entries);
        fprintf(fp, "trigram_journal_bytes=%" PRIu64 "\n", g_trijournal_final_bytes);
        fprintf(fp, "trigram_journal_voided=%d\n", g_trijournal_final_voided);
    }
    fprintf(fp, "record_batch_bytes=%u\n", (unsigned)RECORD_BATCH_BYTES);
    fprintf(fp, "write_buffer_size=%u\n", g_no_write ? 0U : (unsigned)WRITE_BUFFER_SIZE);
    fprintf(fp, "byte_accounting=%s\n", "unique_regular_files");
    fprintf(fp, "crawl_threads_started=%" PRIu64 "\n", shared->crawl_threads_started);
    fprintf(fp, "split_dirs_enqueued=%" PRIu64 "\n", shared->split_dirs_enqueued);
    fprintf(fp, "donated_dirs=%" PRIu64 "\n", shared->donated_dirs);
    fprintf(fp, "donation_attempts=%" PRIu64 "\n", shared->donation_attempts);
    fprintf(fp, "donation_successes=%" PRIu64 "\n", shared->donation_successes);
    fprintf(fp, "donation_success_pct=%.1f\n",
            shared->donation_attempts ? (100.0 * (double)shared->donation_successes) / (double)shared->donation_attempts
                                      : 0.0);
    fprintf(fp, "tasks_popped=%" PRIu64 "\n", tasks_popped);
    fprintf(fp, "avg_entries_per_task=%.2f\n", tasks_popped ? (double)shared->total_entries / (double)tasks_popped : 0.0);
    fprintf(fp, "avg_dirs_per_task=%.2f\n", tasks_popped ? (double)shared->total_dirs / (double)tasks_popped : 0.0);
    fprintf(fp, "avg_files_per_task=%.2f\n", tasks_popped ? (double)shared->total_files / (double)tasks_popped : 0.0);
    fprintf(fp, "batches_enqueued=%" PRIu64 "\n", (uint64_t)atomic_load(&g_batches_enqueued));
    fprintf(fp, "batches_dequeued=%" PRIu64 "\n", (uint64_t)atomic_load(&g_batches_dequeued));
    fprintf(fp, "entries=%" PRIu64 "\n", shared->total_entries);
    fprintf(fp, "dirs=%" PRIu64 "\n", shared->total_dirs);
    fprintf(fp, "files=%" PRIu64 "\n", shared->total_files);
    fprintf(fp, "symlinks=%" PRIu64 "\n", shared->total_symlinks);
    fprintf(fp, "other=%" PRIu64 "\n", shared->total_other);
    if (g_no_stat) {
        print_no_stat_stats(fp);
    } else {
        fprintf(fp, "hardlink_files=%" PRIu64 "\n", shared->total_hardlink_files);
        fprintf(fp, "total_bytes=%" PRIu64 "\n", shared->total_bytes);
        fprintf(fp, "st_blocks_bytes_unit=%u\n", (unsigned)ST_BLOCKS_BYTES_UNIT);
        fprintf(fp, "total_allocated_bytes=%" PRIu64 "\n", shared->total_allocated_bytes);
        fprintf(fp, "files_sparse_heuristic=%" PRIu64 "\n", shared->files_sparse_heuristic);
        fprintf(fp, "dir_apparent_bytes=%" PRIu64 "\n", shared->dir_apparent_bytes);
        fprintf(fp, "symlink_apparent_bytes=%" PRIu64 "\n", shared->symlink_apparent_bytes);
        fprintf(fp, "other_apparent_bytes=%" PRIu64 "\n", shared->other_apparent_bytes);
        fprintf(fp, "apparent_bytes_total=%" PRIu64 "\n", apparent_bytes_total);
    }
    fprintf(fp, "errors=%" PRIu64 "\n", shared->total_errors);
    fprintf(fp, "writer_failed=%u\n", atomic_load(&g_writer_failed));
    fprintf(fp, "io_lstat_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_lstat_calls));
    fprintf(fp, "io_uring_batches=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_uring_batches));
    fprintf(fp, "io_uring_sync_redos=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_uring_sync_redos));
    fprintf(fp, "io_uring_inline_stats=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_uring_inline_stats));
    fprintf(fp, "io_stat_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_stat_calls));
    fprintf(fp, "io_mkdir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_mkdir_calls));
    fprintf(fp, "io_opendir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_opendir_calls));
    fprintf(fp, "io_readdir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_readdir_calls));
    fprintf(fp, "io_getdents_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_getdents_calls));
    fprintf(fp, "getdents_buf_bytes=%zu\n", g_getdents_buf_bytes);
    fprintf(fp, "io_closedir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_closedir_calls));
    fprintf(fp, "io_fopen_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fopen_calls));
    fprintf(fp, "io_fclose_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fclose_calls));
    fprintf(fp, "io_fwrite_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fwrite_calls));
    fprintf(fp, "io_fflush_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fflush_calls));
    fprintf(fp, "shard_bin_opens=%" PRIu64 "\n", (uint64_t)atomic_load(&g_shard_bin_opens));
    fprintf(fp, "shard_bin_reopens=%" PRIu64 "\n", (uint64_t)atomic_load(&g_shard_bin_reopens));
    fprintf(fp, "shard_evictions=%" PRIu64 "\n", (uint64_t)atomic_load(&g_shard_evictions));
    fprintf(fp, "shard_ckpt_writes=%" PRIu64 "\n", (uint64_t)atomic_load(&g_shard_ckpt_writes));
    fprintf(fp, "manifest=%s\n", g_no_write ? "(disabled)" : "crawl_manifest.txt");
    fprintf(fp, "uid_output=%s\n", g_no_write ? "(disabled)" : g_uid_registry.path);
    fprintf(fp, "gid_output=%s\n", g_no_write ? "(disabled)" : g_gid_registry.path);
    fprintf(fp, "uid_distinct=%zu\n", g_uid_distinct);
    fprintf(fp, "gid_distinct=%zu\n", g_gid_distinct);
    fprintf(fp, "id_resolve_threads=%d\n", g_no_write ? 0 : parse_ecrawl_id_resolve_threads());
    fprintf(fp, "id_resolve_sec=%.3f\n", g_id_resolve_sec);
    fprintf(fp, "ops_window_sec=%d\n", WINDOW_SECONDS);
    fprintf(fp, "avg_ops_per_sec=%s\n", avg_ops_buf);
    fprintf(fp, "mean_ops_per_sec=%s\n", mean_ops_buf);
    fprintf(fp, "max_ops_per_sec=%s\n", max_ops_buf);
    fprintf(fp, "min_ops_per_sec=%s\n", min_ops_buf);
    fprintf(fp, "donate_check_every=%zu\n", g_donate_check_every_cfg);
    fprintf(fp, "donate_entry_check_every=%zu\n", g_donate_entry_check_every_cfg);
    fprintf(fp, "force_donate_at=%zu\n", g_force_donate_count_cfg);
    fprintf(fp, "donate_chunk_force_max=%zu\n", g_donate_chunk_force_max_cfg);
    fprintf(fp, "donate_all_busy_min_stack=%zu\n", g_donate_all_busy_min_stack_cfg);
    fprintf(fp, "donate_all_busy_max_qdepth_mult=%u\n", g_donate_all_busy_max_qdepth_mult_cfg);
    fprintf(fp, "discovered_dir_enqueue_batch=%zu\n", g_discovered_dir_enqueue_batch_cfg);
    fprintf(fp, "donate_floor=%d\n", LOCAL_STACK_DONATE_FLOOR);
    fprintf(fp, "avg_active_workers=%.2f\n",
            g_active_workers_samples ? (double)g_active_workers_sum / (double)g_active_workers_samples : 0.0);
    fprintf(fp, "min_active_workers=%d\n", g_active_workers_min);
    fprintf(fp, "max_active_workers=%d\n", g_active_workers_max);
    /* Worker occupancy is sampled by the once-per-second stats tick, so a sub-second run
     * collects no samples and the averages above are 0 for lack of data, not for lack of
     * workers. Report the sample count so readers can tell those two cases apart. */
    fprintf(fp, "active_workers_samples=%" PRIu64 "\n", g_active_workers_samples);
    fprintf(fp, "seconds_single_worker=%" PRIu64 "\n", g_seconds_single_worker);
    fprintf(fp, "seconds_queue_empty_single_worker=%" PRIu64 "\n", g_seconds_queue_empty_single_worker);
    fprintf(fp, "elapsed_sec=%.3f\n", elapsed_sec);
    fprintf(fp, "queue_depth=%" PRIu64 "\n", (uint64_t)atomic_load(&g_queue_depth));
    fprintf(fp, "disk_low=%u\n", (unsigned)atomic_load(&g_disk_low));
    print_queue_wait_metrics_to(fp);
}

int main(int argc, char **argv) {
    const char *start_path;
    const char *positionals[2];
    shared_state_t shared;
    task_queue_t queue;
    writer_queue_t *writer_queues = NULL;
    pthread_t *workers = NULL;
    worker_arg_t *worker_args = NULL;
    pthread_t *writer_threads = NULL;
    writer_arg_t *writer_args = NULL;
    pthread_t stats_thread;
    pthread_t disk_monitor_thread;
    int disk_monitor_started = 0;
    double t0, t1;
    int worker_count_started = 0;
    int positional_count = 0;
    int output_dir_explicit = 0;
    int writer_slots;
    int writer_threads_used = 0;
    int uid_registry_ready = 0;
    int gid_registry_ready = 0;
    int hardlink_registry_ready = 0;
    int stats_thread_started = 0;
    int i;
    int j;

    tune_allocator();

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--no-write") == 0) {
            g_no_write = 1;
            continue;
        }
        if (strcmp(argv[i], "--no-stat") == 0) {
            g_no_stat = 1;
            g_no_write = 1;
            continue;
        }
        if (strcmp(argv[i], "--statx") == 0) {
            g_statx_mode = 1;
            continue;
        }
        if (strcmp(argv[i], "--iouring") == 0) {
            g_iouring_statx = 1;
            g_statx_mode = 1; /* IORING_OP_STATX is the only inode-read opcode */
            continue;
        }
        if (strcmp(argv[i], "--count") == 0) {
            g_nostat_count = 1;
            continue;
        }
        if (strcmp(argv[i], "--print0") == 0) {
            g_print0 = 1;
            continue;
        }
        if (strcmp(argv[i], "--contains") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--contains requires a substring\n");
                print_usage(argv[0]);
                return 2;
            }
            i++;
            if (argv[i][0] == '\0') {
                fprintf(stderr, "--contains substring must not be empty\n");
                return 2;
            }
            free(g_contains_lower);
            g_contains_lower = ascii_lower_dup(argv[i]);
            if (!g_contains_lower) {
                fprintf(stderr, "--contains: out of memory\n");
                return 2;
            }
            g_contains_len = strlen(g_contains_lower);
            continue;
        }
        if (strcmp(argv[i], "--verbose") == 0) {
            g_verbose = 1;
            continue;
        }
        if (strcmp(argv[i], "--progress") == 0) {
            g_progress = 1;
            continue;
        }
        if (strcmp(argv[i], "--record-root") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--record-root requires a path\n");
                print_usage(argv[0]);
                return 2;
            }
            i++;
            if (snprintf(g_record_root_buf, sizeof(g_record_root_buf), "%s", argv[i]) >= (int)sizeof(g_record_root_buf)) {
                fprintf(stderr, "--record-root path too long\n");
                return 2;
            }
            path_rstrip_slashes(g_record_root_buf);
            if (g_record_root_buf[0] == '\0') {
                fprintf(stderr, "--record-root invalid\n");
                return 2;
            }
            if (g_record_root_buf[0] != '/') {
                char joined[PATH_MAX];
                char cwd[PATH_MAX];

                if (!getcwd(cwd, sizeof(cwd))) {
                    fprintf(stderr, "--record-root: getcwd: %s\n", strerror(errno));
                    return 2;
                }
                if (snprintf(joined, sizeof(joined), "%s/%s", cwd, g_record_root_buf) >= (int)sizeof(joined)) {
                    fprintf(stderr, "--record-root path too long\n");
                    return 2;
                }
                if (snprintf(g_record_root_buf, sizeof(g_record_root_buf), "%s", joined) >= (int)sizeof(g_record_root_buf)) {
                    fprintf(stderr, "--record-root path too long\n");
                    return 2;
                }
            }
            {
                char can[PATH_MAX];
                int nr;

                if (realpath(g_record_root_buf, can)) {
                    nr = snprintf(g_record_root_buf, sizeof(g_record_root_buf), "%s", can);
                    if (nr < 0 || (size_t)nr >= sizeof(g_record_root_buf)) {
                        fprintf(stderr, "--record-root path too long after resolution\n");
                        return 2;
                    }
                }
            }
            path_rstrip_slashes(g_record_root_buf);
            g_record_root = g_record_root_buf;
            continue;
        }
        if (strcmp(argv[i], "--trigram-journal") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--trigram-journal requires a directory\n");
                print_usage(argv[0]);
                return 2;
            }
            i++;
            if (argv[i][0] == '\0') {
                fprintf(stderr, "--trigram-journal directory must not be empty\n");
                return 2;
            }
            if (snprintf(g_trijournal_dir, sizeof(g_trijournal_dir), "%s", argv[i]) >=
                (int)sizeof(g_trijournal_dir)) {
                fprintf(stderr, "--trigram-journal path too long\n");
                return 2;
            }
            path_rstrip_slashes(g_trijournal_dir);
            if (g_trijournal_dir[0] == '\0') {
                fprintf(stderr, "--trigram-journal directory invalid\n");
                return 2;
            }
            g_trijournal_enabled = 1;
            continue;
        }
        if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
        if (argv[i][0] == '-') {
            fprintf(stderr, "unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 2;
        }
        if (positional_count >= (int)(sizeof(positionals) / sizeof(positionals[0]))) {
            print_usage(argv[0]);
            return 2;
        }
        positionals[positional_count++] = argv[i];
    }

    if (positional_count < 1) {
        print_usage(argv[0]);
        return 2;
    }

    /* The journal tees the writer-bound record stream; --no-write/--no-stat have no writer stream. */
    if (g_trijournal_enabled && g_no_write) {
        fprintf(stderr, "--trigram-journal cannot be combined with --no-write/--no-stat\n");
        return 2;
    }

    /* A record's uid/size/inode/nlink/times all come from stat, so a names-only walk cannot
     * produce a capture. Refuse rather than silently writing an empty output directory. */
    if (g_no_stat && positional_count >= 2) {
        fprintf(stderr, "--no-stat streams paths and cannot write a capture; drop the output-dir argument\n");
        return 2;
    }
    if (g_nostat_count && !g_no_stat) {
        fprintf(stderr, "--count tallies a names-only walk and requires --no-stat\n");
        return 2;
    }
    if (g_contains_lower && !g_no_stat) {
        fprintf(stderr, "--contains filters the streamed path list and requires --no-stat\n");
        return 2;
    }
    if (g_print0 && !g_no_stat) {
        fprintf(stderr, "--print0 applies to the --no-stat path stream\n");
        return 2;
    }
    if (g_print0 && g_nostat_count) {
        fprintf(stderr, "--print0 applies to the path stream; drop it when using --count\n");
        return 2;
    }
#ifndef ECRAWL_HAVE_IOURING
    if (g_iouring_statx) {
        fprintf(stderr, "--iouring: this build has no io_uring support (missing <linux/io_uring.h> or arch syscall numbers)\n");
        return 2;
    }
#endif

    ecrawl_install_verbose_profile();
    ecrawl_install_statx_profile();

    if (path_resolve_existing(positionals[0], g_start_path_canon, "ecrawl: start-path ") != 0) return 2;
    start_path = g_start_path_canon;
    if (init_record_path_prefix(start_path) != 0) {
        fprintf(stderr, "ERROR crawl start-path too long for record mapping\n");
        return 1;
    }
    if (positional_count >= 2) {
        int n = snprintf(g_output_dir, sizeof(g_output_dir), "%s", positionals[1]);
        if (n < 0 || (size_t)n >= sizeof(g_output_dir)) {
            fprintf(stderr, "output-dir is too long\n");
            return 2;
        }
        output_dir_explicit = 1;
    }

    g_crawl_threads = parse_ecrawl_crawl_threads();
    g_stall_hint_seconds_cfg = parse_ecrawl_stall_hint_seconds();
    g_getdents_buf_bytes = parse_ecrawl_getdents_buf();
    g_iouring_depth = parse_ecrawl_iouring_depth();
    g_iouring_min_batch = parse_ecrawl_iouring_min_batch(g_iouring_depth);
    g_donate_check_every_cfg     = parse_ecrawl_donate_check_every();
    g_donate_entry_check_every_cfg = parse_ecrawl_donate_entry_check_every();
    /* 0 disables entry-driven donation (SIZE_MAX); progress still uses the default cadence. */
    g_progress_every = (g_donate_entry_check_every_cfg == (size_t)SIZE_MAX)
                           ? (size_t)DEFAULT_DONATE_ENTRY_CHECK_EVERY
                           : g_donate_entry_check_every_cfg;
    g_donate_chunk_force_max_cfg = parse_ecrawl_donate_chunk_force_max();
    g_force_donate_count_cfg     = parse_ecrawl_force_donate_at();
    g_donate_all_busy_min_stack_cfg              = parse_ecrawl_donate_all_busy_min_stack();
    g_donate_all_busy_max_qdepth_mult_cfg = parse_ecrawl_donate_all_busy_max_qdepth_mult();
    g_discovered_dir_enqueue_batch_cfg    = parse_ecrawl_discovered_dir_enqueue_batch();
    g_uid_shards = parse_ecrawl_uid_shards_env();
    g_writer_threads = parse_ecrawl_writer_threads_env();
    g_writer_queue_batches = parse_ecrawl_writer_queue_batches_env();
    g_trijournal_threads = parse_ecrawl_trijournal_threads_env();
    g_trijournal_queue_batches = parse_ecrawl_trijournal_queue_batches_env();
    if ((uint32_t)g_writer_threads > g_uid_shards) g_writer_threads = (int)g_uid_shards;
    g_requested_max_open_shards = parse_ecrawl_max_open_shards_env();
    configure_max_open_shards();
    g_shard_digits = shard_digits_for(g_uid_shards);
    writer_slots = g_writer_threads;
    writer_threads_used = g_no_write ? 0 : g_writer_threads;

    if (!g_no_write && !output_dir_explicit) {
        if (build_default_output_dir(g_output_dir, sizeof(g_output_dir)) != 0) {
            fprintf(stderr, "ERROR failed to build default output directory name: %s\n", strerror(errno));
            return 1;
        }
    }

    if (!g_no_write) {
        if (ensure_output_dir_exists(g_output_dir) != 0) {
            fprintf(stderr, "ERROR invalid output directory %s: %s\n", g_output_dir, strerror(errno));
            return 1;
        }
        if (path_resolve_inplace(g_output_dir, sizeof(g_output_dir), "ecrawl: output-dir ") != 0) return 1;

        if (crawl_output_dir_scrub_prior_artifacts() != 0) return 1;

        if (g_trijournal_enabled) {
            if (ensure_output_dir_exists(g_trijournal_dir) != 0) {
                fprintf(stderr, "ERROR invalid --trigram-journal directory %s: %s\n", g_trijournal_dir,
                        strerror(errno));
                return 1;
            }
            if (trijournal_dir_scrub_prior_artifacts() != 0) return 1;
        }

        {
            char uid_path[PATH_MAX];
            char gid_path[PATH_MAX];

            if (snprintf(uid_path, sizeof(uid_path), "%s/uid.txt", g_output_dir) < 0 ||
                snprintf(gid_path, sizeof(gid_path), "%s/gid.txt", g_output_dir) < 0) {
                fprintf(stderr, "ERROR failed to build uid/gid output paths\n");
                return 1;
            }

            if (id_registry_init(&g_uid_registry, uid_path, resolve_uid_name) != 0) {
                fprintf(stderr, "ERROR failed to open %s: %s\n", uid_path, strerror(errno));
                return 1;
            }
            uid_registry_ready = 1;
            if (id_registry_init(&g_gid_registry, gid_path, resolve_gid_name) != 0) {
                fprintf(stderr, "ERROR failed to open %s: %s\n", gid_path, strerror(errno));
                id_registry_destroy(&g_uid_registry);
                uid_registry_ready = 0;
                return 1;
            }
            gid_registry_ready = 1;
        }
    }

    memset(&shared, 0, sizeof(shared));
    pthread_mutex_init(&shared.stats_mutex, NULL);
    /* Hardlink unique-byte credit needs the inode registry whenever we stat (write or --no-write).
     * --no-stat never reads inodes, so it cannot dedupe. */
    if (!g_no_stat) {
        if (inode_registry_init(&g_hardlink_registry) != 0) {
            fprintf(stderr, "ERROR failed to initialize hardlink registry\n");
            pthread_mutex_destroy(&shared.stats_mutex);
            if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
            if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
            return 1;
        }
        hardlink_registry_ready = 1;
    }
    queue_init(&queue);

    if (!g_no_write) {
        writer_queues = (writer_queue_t *)calloc((size_t)writer_slots, sizeof(*writer_queues));
        writer_threads = (pthread_t *)calloc((size_t)writer_slots, sizeof(*writer_threads));
        writer_args = (writer_arg_t *)calloc((size_t)writer_slots, sizeof(*writer_args));
        if (!writer_queues || !writer_threads || !writer_args) {
            fprintf(stderr, "ERROR allocation failed for writer threads\n");
            free(writer_queues);
            free(writer_threads);
            free(writer_args);
            queue_destroy(&queue);
            pthread_mutex_destroy(&shared.stats_mutex);
            if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);
            if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
            if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
            return 1;
        }

        for (i = 0; i < writer_slots; i++) {
            if (writer_queue_init(&writer_queues[i], g_writer_queue_batches) != 0) {
                fprintf(stderr, "ERROR failed to initialize writer queue %d\n", i);
                while (--i >= 0) writer_queue_destroy(&writer_queues[i]);
                free(writer_queues);
                free(writer_threads);
                free(writer_args);
                queue_destroy(&queue);
                pthread_mutex_destroy(&shared.stats_mutex);
                if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);
                if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
                if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
                return 1;
            }
        }
    }

    for (i = 0; i < WINDOW_SECONDS; i++) {
        atomic_store(&g_bucket_entries[i], 0);
        atomic_store(&g_bucket_files[i], 0);
        atomic_store(&g_bucket_dirs[i], 0);
        atomic_store(&g_bucket_stat_meta[i], 0);
    }
    atomic_store(&g_total_entries, 0);
    atomic_store(&g_total_files, 0);
    atomic_store(&g_total_dirs, 0);
    atomic_store(&g_total_bytes, 0);
    atomic_store(&g_total_allocated_bytes, 0);
    atomic_store(&g_files_sparse_heuristic, 0);
    atomic_store(&g_window_entries, 0);
    atomic_store(&g_window_files, 0);
    atomic_store(&g_window_dirs, 0);
    atomic_store(&g_window_stat_meta, 0);
    atomic_store(&g_bucket_index, 0);
    atomic_store(&g_stop_stats, 0);
    atomic_store(&g_seconds_seen, 0);
    g_ops_rate_sum = 0.0;
    g_ops_rate_min = 0.0;
    g_ops_rate_max = 0.0;
    g_ops_rate_samples = 0;
    g_active_workers_sum = 0;
    g_active_workers_min = 0;
    g_active_workers_max = 0;
    g_active_workers_samples = 0;
    g_seconds_single_worker = 0;
    g_seconds_queue_empty_single_worker = 0;
    g_progress_prev_tot_entries = 0;
    g_progress_prev_readdir = 0;
    g_progress_prev_lstat = 0;
    g_progress_prev_stat = 0;
    atomic_store(&g_queue_depth, 0);
    atomic_store(&g_active_workers, 0);
    atomic_store(&g_main_done, 0);
    atomic_store(&g_fd_pressure, 0);
    atomic_store(&g_writer_failed, 0);
    atomic_store(&g_disk_low, 0);
    atomic_store(&g_disk_monitor_stop, 0);
    atomic_store(&g_disk_wait_disabled, 0);
    atomic_store(&g_tasks_popped, 0);
    atomic_store(&g_writer_queue_depth, 0);
    atomic_store(&g_batches_enqueued, 0);
    atomic_store(&g_batches_dequeued, 0);
    atomic_store(&g_wait_crawl_tasks, 0);
    atomic_store(&g_wait_writer_push, 0);
    atomic_store(&g_wait_writer_pop, 0);
    atomic_store(&g_task_queue_pushes, 0);
    atomic_store(&g_queue_lock_waits, 0);
    atomic_store(&g_donate_calls, 0);
    atomic_store(&g_writer_queue_wait_ns, 0);
    atomic_store(&g_crawl_idle_ns, 0);
    atomic_store(&g_io_lstat_calls, 0);
    atomic_store(&g_io_stat_calls, 0);
    atomic_store(&g_io_mkdir_calls, 0);
    atomic_store(&g_io_opendir_calls, 0);
    atomic_store(&g_io_readdir_calls, 0);
    atomic_store(&g_io_getdents_calls, 0);
    atomic_store(&g_io_closedir_calls, 0);
    atomic_store(&g_io_fopen_calls, 0);
    atomic_store(&g_io_fclose_calls, 0);
    atomic_store(&g_io_fwrite_calls, 0);
    atomic_store(&g_io_fflush_calls, 0);
    atomic_store(&g_shard_bin_opens, 0);
    atomic_store(&g_shard_bin_reopens, 0);
    atomic_store(&g_shard_evictions, 0);
    atomic_store(&g_shard_ckpt_writes, 0);

    t0 = now_sec();
    g_run_start_sec = t0;
    g_crawl_wall_clock_start = time(NULL);

    if (!g_no_write) {
        if (g_trijournal_enabled &&
            trijournal_pool_start(&g_trijournal_pool, g_trijournal_dir, g_uid_shards, g_trijournal_threads,
                                  g_trijournal_queue_batches, ecrawl_shard_basename) != 0) {
            fprintf(stderr, "ERROR failed to start trigram journal pool in %s\n", g_trijournal_dir);
            for (i = 0; i < writer_slots; i++) writer_queue_destroy(&writer_queues[i]);
            free(writer_queues);
            free(writer_threads);
            free(writer_args);
            queue_destroy(&queue);
            pthread_mutex_destroy(&shared.stats_mutex);
            if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);
            if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
            if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
            return 1;
        }
        writer_threads_used = 0;
        for (i = 0; i < writer_slots; i++) {
            writer_args[i].queue = &writer_queues[i];
            writer_args[i].writer_index = (uint32_t)i;
            if (pthread_create(&writer_threads[i], NULL, writer_thread_main, &writer_args[i]) != 0) {
                fprintf(stderr, "ERROR failed to create writer thread %d\n", i);
                break;
            }
            writer_threads_used++;
        }
        if (writer_threads_used == 0) {
            fprintf(stderr, "ERROR no writer threads started\n");
            for (i = 0; i < writer_slots; i++) writer_queue_destroy(&writer_queues[i]);
            free(writer_queues);
            free(writer_threads);
            free(writer_args);
            queue_destroy(&queue);
            pthread_mutex_destroy(&shared.stats_mutex);
            if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);
            if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
            if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
            return 1;
        }
        g_writer_threads = writer_threads_used;
        if (pthread_create(&disk_monitor_thread, NULL, disk_monitor_thread_main, NULL) != 0) {
            fprintf(stderr, "ERROR failed to create disk space monitor thread\n");
            atomic_store(&g_disk_wait_disabled, 1);
            for (i = 0; i < writer_threads_used; i++) writer_queue_close(&writer_queues[i]);
            for (i = 0; i < writer_threads_used; i++) pthread_join(writer_threads[i], NULL);
            for (i = 0; i < writer_slots; i++) writer_queue_destroy(&writer_queues[i]);
            free(writer_queues);
            free(writer_threads);
            free(writer_args);
            queue_destroy(&queue);
            pthread_mutex_destroy(&shared.stats_mutex);
            if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);
            if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
            if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
            return 1;
        }
        disk_monitor_started = 1;
    }

    /* No 1 Hz stats thread: default is silent; --progress prints on the dirent cadence. */
    stats_thread_started = 0;
    (void)stats_thread;
    (void)sizeof(&stats_thread_main);

    workers = (pthread_t *)calloc((size_t)g_crawl_threads, sizeof(*workers));
    worker_args = (worker_arg_t *)calloc((size_t)g_crawl_threads, sizeof(*worker_args));
    if (!workers || !worker_args) {
        free(workers);
        free(worker_args);
        fprintf(stderr, "ERROR allocation failed for crawl thread table (%d threads)\n", g_crawl_threads);
        if (!g_no_write) {
            atomic_store(&g_disk_wait_disabled, 1);
            disk_monitor_stop_request();
            if (disk_monitor_started) pthread_join(disk_monitor_thread, NULL);
            disk_monitor_started = 0;
            for (i = 0; i < writer_threads_used; i++) writer_queue_close(&writer_queues[i]);
            for (i = 0; i < writer_threads_used; i++) pthread_join(writer_threads[i], NULL);
        }
        if (!g_no_write) {
            for (i = 0; i < writer_slots; i++) writer_queue_destroy(&writer_queues[i]);
            free(writer_queues);
            free(writer_threads);
            free(writer_args);
        }
        queue_destroy(&queue);
        pthread_mutex_destroy(&shared.stats_mutex);
        if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);
        if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
        if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
        return 1;
    }

    if (g_progress) {
        g_progress_nslots = (size_t)g_crawl_threads;
        g_progress_slots = (progress_slot_t *)calloc(g_progress_nslots, sizeof(*g_progress_slots));
        if (!g_progress_slots) {
            free(workers);
            free(worker_args);
            fprintf(stderr, "ERROR allocation failed for progress slots\n");
            if (!g_no_write) {
                atomic_store(&g_disk_wait_disabled, 1);
                disk_monitor_stop_request();
                if (disk_monitor_started) pthread_join(disk_monitor_thread, NULL);
                disk_monitor_started = 0;
                for (i = 0; i < writer_threads_used; i++) writer_queue_close(&writer_queues[i]);
                for (i = 0; i < writer_threads_used; i++) pthread_join(writer_threads[i], NULL);
            }
            if (!g_no_write) {
                for (i = 0; i < writer_slots; i++) writer_queue_destroy(&writer_queues[i]);
                free(writer_queues);
                free(writer_threads);
                free(writer_args);
            }
            queue_destroy(&queue);
            pthread_mutex_destroy(&shared.stats_mutex);
            if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);
            if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
            if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
            return 1;
        }
        atomic_store(&g_progress_last_print_ns, 0);
    }

    for (i = 0; i < g_crawl_threads; i++) {
        if (pthread_mutex_init(&worker_args[i].emit_stats_lock, NULL) != 0) {
            fprintf(stderr, "ERROR crawl worker mutex init failed\n");
            while (i > 0) pthread_mutex_destroy(&worker_args[--i].emit_stats_lock);
            free(g_progress_slots);
            g_progress_slots = NULL;
            free(workers);
            free(worker_args);
            if (!g_no_write) {
                atomic_store(&g_disk_wait_disabled, 1);
                disk_monitor_stop_request();
                if (disk_monitor_started) pthread_join(disk_monitor_thread, NULL);
                disk_monitor_started = 0;
                for (j = 0; j < writer_threads_used; j++) writer_queue_close(&writer_queues[j]);
                for (j = 0; j < writer_threads_used; j++) pthread_join(writer_threads[j], NULL);
            }
            if (!g_no_write) {
                for (j = 0; j < writer_slots; j++) writer_queue_destroy(&writer_queues[j]);
                free(writer_queues);
                free(writer_threads);
                free(writer_args);
            }
            queue_destroy(&queue);
            pthread_mutex_destroy(&shared.stats_mutex);
            if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);
            if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
            if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
            return 1;
        }
    }

    for (i = 0; i < g_crawl_threads; i++) {
        worker_args[i].shared = &shared;
        worker_args[i].queue = &queue;
        worker_args[i].writer_queues = writer_queues;
        worker_args[i].writer_threads = writer_threads_used;
        worker_args[i].worker_index = (uint64_t)(i + 1);
        memset(&worker_args[i].stats, 0, sizeof(worker_args[i].stats));
        memset(&worker_args[i].perf, 0, sizeof(worker_args[i].perf));
        memset(&worker_args[i].aux, 0, sizeof(worker_args[i].aux));
    }

    for (i = 0; i < g_crawl_threads; i++) {
        if (pthread_create(&workers[i], NULL, worker_thread_main, &worker_args[i]) != 0) {
            fprintf(stderr, "ERROR failed to create worker %d\n", i + 1);
            stats_add_error(&shared);
            break;
        }
        worker_count_started++;
        stats_add_crawl_thread_started(&shared);
    }

    enqueue_root_task(start_path, &shared, &queue);

    atomic_store(&g_main_done, 1);
    pthread_mutex_lock(&queue.mutex);
    pthread_cond_broadcast(&queue.cond);
    pthread_mutex_unlock(&queue.mutex);

    for (i = 0; i < worker_count_started; i++) pthread_join(workers[i], NULL);

    pthread_mutex_lock(&queue.mutex);
    queue.closed = 1;
    pthread_cond_broadcast(&queue.cond);
    pthread_mutex_unlock(&queue.mutex);

    for (i = 0; i < worker_count_started; i++) {
        stats_merge(&shared, &worker_args[i].stats);
        stats_merge_aux(&shared, &worker_args[i].aux);
    }

    if (!g_no_write) {
        atomic_store(&g_disk_wait_disabled, 1);
        for (i = 0; i < writer_threads_used; i++) writer_queue_close(&writer_queues[i]);
        for (i = 0; i < writer_threads_used; i++) pthread_join(writer_threads[i], NULL);
        disk_monitor_stop_request();
        if (disk_monitor_started) pthread_join(disk_monitor_thread, NULL);
    }

    if (g_trijournal_pool) {
        double jt0 = now_sec();
        int jrc = trijournal_pool_finish(g_trijournal_pool);
        if (g_verbose) fprintf(stderr, "trijournal_pool_finish_sec=%.3f\n", now_sec() - jt0);
        g_trijournal_final_bytes = trijournal_pool_bytes(g_trijournal_pool);
        g_trijournal_final_entries = trijournal_pool_entries(g_trijournal_pool);
        g_trijournal_final_published = trijournal_pool_published(g_trijournal_pool);
        g_trijournal_final_voided = trijournal_pool_voided(g_trijournal_pool);
        trijournal_pool_free(g_trijournal_pool);
        g_trijournal_pool = NULL;
        if (jrc != 0)
            fprintf(stderr,
                    "warn: trigram journal voided/incomplete; ereport_index falls back to capture parse\n");
    }

    if (stats_thread_started) {
        stats_stop_request();
        pthread_join(stats_thread, NULL);
    }
    if (g_progress)
        progress_print_line(shared.total_files, shared.total_entries, shared.total_bytes);
    clear_status_line();

    /* Resolve uid/gid names here rather than leaving it to id_registry_destroy at the end of main.
     * It is real work whose cost scales with distinct owners, so it belongs inside t0..t1 where
     * elapsed_sec and the ops rate can see it. */
    if (uid_registry_ready || gid_registry_ready) {
        double id_t0 = now_sec();

        g_uid_distinct = uid_registry_ready ? g_uid_registry.count : 0;
        g_gid_distinct = gid_registry_ready ? g_gid_registry.count : 0;
        if (uid_registry_ready) id_registry_finalize(&g_uid_registry);
        if (gid_registry_ready) id_registry_finalize(&g_gid_registry);
        g_id_resolve_sec = now_sec() - id_t0;
    }

    t1 = now_sec();

    {
        double elapsed = t1 - t0;

        if (!g_no_write && write_crawl_manifest(start_path, worker_count_started, elapsed, &shared) != 0) {
            fprintf(stderr, "ERROR failed to write crawl manifest: %s\n", strerror(errno));
        }

        double avg_ops = elapsed > 0.0 ? (double)shared.total_entries / elapsed : 0.0;
        double mean_ops = g_ops_rate_samples ? g_ops_rate_sum / (double)g_ops_rate_samples : avg_ops;
        double max_ops = g_ops_rate_samples ? g_ops_rate_max : avg_ops;
        double min_ops = g_ops_rate_samples ? g_ops_rate_min : avg_ops;
        uint64_t apparent_bytes_total = shared.total_bytes + shared.dir_apparent_bytes +
                                        shared.symlink_apparent_bytes + shared.other_apparent_bytes;
        char avg_ops_buf[32], mean_ops_buf[32], max_ops_buf[32], min_ops_buf[32];

        human_decimal(avg_ops, avg_ops_buf, sizeof(avg_ops_buf));
        human_decimal(mean_ops, mean_ops_buf, sizeof(mean_ops_buf));
        human_decimal(max_ops, max_ops_buf, sizeof(max_ops_buf));
        human_decimal(min_ops, min_ops_buf, sizeof(min_ops_buf));

        /* --no-stat owns stdout for the path stream, so the summary goes to stderr and
         * `ecrawl --no-stat ... | sort` stays a clean path list. --count does not stream,
         * so the summary stays on stdout. */
        FILE *sumfp = (g_no_stat && !g_nostat_count) ? stderr : stdout;

        if (!g_verbose) {
            fprintf(sumfp, "start_path=%s\n", start_path);
            if (g_record_root) fprintf(sumfp, "record_root=%s\n", g_record_root);
            fprintf(sumfp, "no_write=%d\n", g_no_write);
            fprintf(sumfp, "no_stat=%d\n", g_no_stat);
            fprintf(sumfp, "stat_impl=%s\n",
                    g_iouring_statx ? "iouring" : (g_statx_mode ? "statx" : "fstatat"));
            fprintf(sumfp, "hardlink_dedup=%s\n", g_no_stat ? "off" : "on");
            fprintf(sumfp, "output_dir=%s\n", g_no_write ? "(disabled)" : g_output_dir);
            fprintf(sumfp, "crawl_threads_started=%" PRIu64 "\n", shared.crawl_threads_started);
            fprintf(sumfp, "writer_threads=%d\n", writer_threads_used);
            fprintf(sumfp, "uid_shards=%u\n", g_uid_shards);
            fprintf(sumfp, "max_open_shards=%u\n", g_no_write ? 0U : g_max_open_shards);
            fprintf(sumfp, "entries=%" PRIu64 "\n", shared.total_entries);
            fprintf(sumfp, "dirs=%" PRIu64 "\n", shared.total_dirs);
            fprintf(sumfp, "files=%" PRIu64 "\n", shared.total_files);
            fprintf(sumfp, "symlinks=%" PRIu64 "\n", shared.total_symlinks);
            fprintf(sumfp, "other=%" PRIu64 "\n", shared.total_other);
            if (g_no_stat) {
                print_no_stat_stats(sumfp);
            } else {
                fprintf(sumfp, "byte_accounting=%s\n", "unique_regular_files");
                fprintf(sumfp, "hardlink_files=%" PRIu64 "\n", shared.total_hardlink_files);
                fprintf(sumfp, "total_bytes=%" PRIu64 "\n", shared.total_bytes);
                fprintf(sumfp, "st_blocks_bytes_unit=%u\n", (unsigned)ST_BLOCKS_BYTES_UNIT);
                fprintf(sumfp, "total_allocated_bytes=%" PRIu64 "\n", shared.total_allocated_bytes);
                fprintf(sumfp, "files_sparse_heuristic=%" PRIu64 "\n", shared.files_sparse_heuristic);
                fprintf(sumfp, "dir_apparent_bytes=%" PRIu64 "\n", shared.dir_apparent_bytes);
                fprintf(sumfp, "symlink_apparent_bytes=%" PRIu64 "\n", shared.symlink_apparent_bytes);
                fprintf(sumfp, "other_apparent_bytes=%" PRIu64 "\n", shared.other_apparent_bytes);
                fprintf(sumfp, "apparent_bytes_total=%" PRIu64 "\n", apparent_bytes_total);
            }
            if (g_trijournal_enabled) {
                fprintf(sumfp, "trigram_journal=%s\n", g_trijournal_dir);
                fprintf(sumfp, "trigram_journal_published=%" PRIu64 "\n", g_trijournal_final_published);
                fprintf(sumfp, "trigram_journal_entries=%" PRIu64 "\n", g_trijournal_final_entries);
                fprintf(sumfp, "trigram_journal_bytes=%" PRIu64 "\n", g_trijournal_final_bytes);
                fprintf(sumfp, "trigram_journal_voided=%d\n", g_trijournal_final_voided);
            }
            fprintf(sumfp, "avg_ops_per_sec=%s\n", avg_ops_buf);
            fprintf(sumfp, "elapsed_sec=%.3f\n", elapsed);
            fprintf(sumfp, "errors=%" PRIu64 "\n", shared.total_errors);
            fprintf(sumfp, "writer_failed=%u\n", atomic_load(&g_writer_failed));
            print_queue_wait_metrics_to(sumfp);
        } else {
            print_verbose_full_stats(sumfp, &shared, elapsed, writer_threads_used, start_path);
        }
    }

    for (i = 0; i < g_crawl_threads; i++) pthread_mutex_destroy(&worker_args[i].emit_stats_lock);

    free(g_progress_slots);
    g_progress_slots = NULL;
    g_progress_nslots = 0;
    free(workers);
    free(worker_args);

    if (!g_no_write) {
        for (i = 0; i < writer_slots; i++) writer_queue_destroy(&writer_queues[i]);
        free(writer_queues);
        free(writer_threads);
        free(writer_args);
    }
    queue_destroy(&queue);
    pthread_mutex_destroy(&shared.stats_mutex);
    if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
    if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
    if (hardlink_registry_ready) inode_registry_destroy(&g_hardlink_registry);

    /* Everything after t1 is invisible to elapsed_sec, which is exactly how a serial uid/gid resolve
     * once hid 8.4s of a 9.3s run. Report the gap so the next such regression shows up in the
     * summary instead of only in wall-clock time. */
    if (g_verbose) fprintf((g_no_stat && !g_nostat_count) ? stderr : stdout, "teardown_sec=%.3f\n", now_sec() - t1);
    return atomic_load(&g_writer_failed) ? 1 : 0;
}
