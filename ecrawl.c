/*
 * ecrawl.c
 *
 * Compact binary-output local filesystem metadata crawler.
 *
 * Features:
 *   - Main thread seeds the crawl with the root path only.
 *   - Worker threads consume queued batches of directory work.
 *   - Workers traverse directories iteratively with a local stack.
 *   - Workers may donate batches of accumulated subdirectories back to the global queue.
 *   - Crawl workers only crawl and enqueue record batches.
 *   - Dedicated writer threads consume buffered batches and write uid-sharded output.
 *   - Existing shard files are appended to, never truncated.
 *   - Rolling 10-second stats are printed once per second.
 *   - Live stats always show q, t, p, and wq even when zero.
 *
 * Build:
 *   gcc -O2 -Wall -Wextra -pthread -o ecrawl ecrawl.c
 *
 * Usage:
 *   ./ecrawl [--no-write] <start-path> [split-depth] [output-dir] [uid-shards] [writer-threads]
 */

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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdatomic.h>
#include <dirent.h>
#include <limits.h>
#include <pwd.h>
#include <grp.h>
#include <stdarg.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define MAX_WORKERS 16
#define DEFAULT_WRITER_THREADS 8
#define DEFAULT_UID_SHARDS 8192U
#define DEFAULT_MAX_OPEN_SHARDS 256U
#define DEFAULT_WRITER_QUEUE_BATCHES 64U
#define RECORD_BATCH_BYTES (1U << 20)
#define WRITE_BUFFER_SIZE (1U << 20)
#define WINDOW_SECONDS 10
#define PERF_FLUSH_INTERVAL 1024U

#define FILE_MAGIC "NFSCBIN"
#define FILE_MAGIC_LEN 8
#define FORMAT_VERSION 2

#define LOCAL_STACK_DONATE_FLOOR 8
#define DONATE_CHUNK_MIN 4
#define DONATE_CHUNK_MAX 128
#define DONATE_QUEUE_TARGET_PER_IDLE 4
#define HARDLINK_REGISTRY_SHARDS 256U

typedef struct task_node task_node_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    task_node_t *head;
    task_node_t *tail;
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
} crawl_stats_t;

typedef struct {
    uint64_t entries;
    uint64_t files;
    uint64_t dirs;
    uint64_t bytes;
} perf_local_t;

typedef struct {
    uint64_t donated_dirs;
    uint64_t donation_attempts;
    uint64_t donation_successes;
} worker_aux_stats_t;

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
    uint64_t worker_threads_started;
    uint64_t split_dirs_enqueued;
    uint64_t donated_dirs;
    uint64_t donation_attempts;
    uint64_t donation_successes;
} shared_state_t;

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
    uint32_t shard;
    uint32_t data_len;
} batch_frame_hdr_t;

typedef struct {
    char *path;
    size_t path_len;
    struct stat st;
    int have_stat;
} dir_work_t;

typedef struct {
    dir_work_t *items;
    size_t count;
    size_t cap;
} dir_stack_t;

typedef struct {
    uint32_t *items;
    size_t count;
    size_t cap;
    pthread_mutex_t mutex;
    FILE *fp;
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

typedef struct record_batch {
    unsigned char *data;
    size_t len;
    struct record_batch *next;
} record_batch_t;

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

typedef struct {
    writer_queue_t *writer_queues;
    int writer_threads;
    pending_batch_t *pending;
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
} worker_arg_t;

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

typedef struct {
    FILE *fp;
    uint64_t bytes_written;
    uint64_t last_used;
    unsigned char initialized;
} shard_file_state_t;

/* Rolling 10-second stats */
static atomic_ullong g_total_entries = 0;
static atomic_ullong g_total_files   = 0;
static atomic_ullong g_total_dirs    = 0;
static atomic_ullong g_total_bytes   = 0;

static atomic_ullong g_window_entries = 0;
static atomic_ullong g_window_files   = 0;
static atomic_ullong g_window_dirs    = 0;

static atomic_ullong g_bucket_entries[WINDOW_SECONDS];
static atomic_ullong g_bucket_files[WINDOW_SECONDS];
static atomic_ullong g_bucket_dirs[WINDOW_SECONDS];

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

static double now_sec(void);
static void dir_stack_destroy(dir_stack_t *s);
static void stats_add_error(shared_state_t *s);
static void perf_flush_local(perf_local_t *perf);

/* Live visibility */
static atomic_ullong g_queue_depth         = 0;
static atomic_int    g_active_workers      = 0;
static atomic_int    g_main_done           = 0;
static atomic_ullong g_tasks_popped        = 0;
static atomic_ullong g_writer_queue_depth  = 0;
static atomic_ullong g_batches_enqueued    = 0;
static atomic_ullong g_batches_dequeued    = 0;

static atomic_ullong g_io_lstat_calls      = 0;
static atomic_ullong g_io_stat_calls       = 0;
static atomic_ullong g_io_mkdir_calls      = 0;
static atomic_ullong g_io_opendir_calls    = 0;
static atomic_ullong g_io_readdir_calls    = 0;
static atomic_ullong g_io_closedir_calls   = 0;
static atomic_ullong g_io_fopen_calls      = 0;
static atomic_ullong g_io_fclose_calls     = 0;
static atomic_ullong g_io_fwrite_calls     = 0;
static atomic_ullong g_io_fflush_calls     = 0;

#define ATOMIC_ADD_RELAXED(obj, value) atomic_fetch_add_explicit((obj), (value), memory_order_relaxed)
#define ATOMIC_SUB_RELAXED(obj, value) atomic_fetch_sub_explicit((obj), (value), memory_order_relaxed)
#define ATOMIC_LOAD_RELAXED(obj) atomic_load_explicit((obj), memory_order_relaxed)

static int g_split_depth = 2;
static int g_writer_threads = DEFAULT_WRITER_THREADS;
static uint32_t g_uid_shards = DEFAULT_UID_SHARDS;
static unsigned g_max_open_shards = DEFAULT_MAX_OPEN_SHARDS;
static unsigned g_writer_queue_batches = DEFAULT_WRITER_QUEUE_BATCHES;
static int g_shard_digits = 4;
static int g_no_write = 0;
static char g_output_dir[PATH_MAX] = ".";
static id_registry_t g_uid_registry;
static id_registry_t g_gid_registry;
static inode_registry_t g_hardlink_registry;

static FILE *counted_fopen(const char *path, const char *mode) {
    ATOMIC_ADD_RELAXED(&g_io_fopen_calls, 1);
    return fopen(path, mode);
}

static int counted_fclose(FILE *fp) {
    ATOMIC_ADD_RELAXED(&g_io_fclose_calls, 1);
    return fclose(fp);
}

static size_t counted_fwrite(const void *ptr, size_t size, size_t nmemb, FILE *fp) {
    ATOMIC_ADD_RELAXED(&g_io_fwrite_calls, 1);
    return fwrite(ptr, size, nmemb, fp);
}

static int counted_fflush(FILE *fp) {
    ATOMIC_ADD_RELAXED(&g_io_fflush_calls, 1);
    return fflush(fp);
}

static int counted_stat(const char *path, struct stat *st) {
    ATOMIC_ADD_RELAXED(&g_io_stat_calls, 1);
    return stat(path, st);
}

static int counted_lstat(const char *path, struct stat *st) {
    ATOMIC_ADD_RELAXED(&g_io_lstat_calls, 1);
    return lstat(path, st);
}

static int counted_fstatat_nofollow(int dirfd_value, const char *name, struct stat *st) {
    ATOMIC_ADD_RELAXED(&g_io_lstat_calls, 1);
    return fstatat(dirfd_value, name, st, AT_SYMLINK_NOFOLLOW);
}

static int counted_mkdir(const char *path, mode_t mode) {
    ATOMIC_ADD_RELAXED(&g_io_mkdir_calls, 1);
    return mkdir(path, mode);
}

static DIR *counted_opendir(const char *path) {
    ATOMIC_ADD_RELAXED(&g_io_opendir_calls, 1);
    return opendir(path);
}

static struct dirent *counted_readdir(DIR *dir) {
    ATOMIC_ADD_RELAXED(&g_io_readdir_calls, 1);
    return readdir(dir);
}

static int counted_closedir(DIR *dir) {
    ATOMIC_ADD_RELAXED(&g_io_closedir_calls, 1);
    return closedir(dir);
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

static int join_path_fast(const char *base, size_t base_len,
                          const char *name, size_t name_len,
                          char *out, size_t out_sz) {
    size_t need;

    if (!base || !name || !out || out_sz == 0) return -1;

    if (base_len == 1 && base[0] == '/') {
        need = 1 + name_len + 1;
        if (need > out_sz) return -1;
        out[0] = '/';
        if (name_len > 0) memcpy(out + 1, name, name_len);
        out[1 + name_len] = '\0';
        return 0;
    }

    need = base_len + 1 + name_len + 1;
    if (need > out_sz) return -1;
    memcpy(out, base, base_len);
    out[base_len] = '/';
    if (name_len > 0) memcpy(out + base_len + 1, name, name_len);
    out[base_len + 1 + name_len] = '\0';
    return 0;
}

static int join_path_alloc(const char *base, size_t base_len,
                           const char *name, size_t name_len,
                           char **out_path, size_t *out_len) {
    char *path;
    size_t need;

    if (!base || !name || !out_path || !out_len) {
        errno = EINVAL;
        return -1;
    }

    if (base_len == 1 && base[0] == '/') need = 1 + name_len + 1;
    else need = base_len + 1 + name_len + 1;

    path = (char *)malloc(need);
    if (!path) return -1;
    if (join_path_fast(base, base_len, name, name_len, path, need) != 0) {
        free(path);
        errno = ENAMETOOLONG;
        return -1;
    }

    *out_path = path;
    *out_len = need - 1;
    return 0;
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

static void clear_status_line(void) {
    if (isatty(STDOUT_FILENO)) printf("\r\033[2K\r");
    else printf("\r%160s\r", "");
    fflush(stdout);
}

static int id_registry_contains_locked(const id_registry_t *r, uint32_t id) {
    size_t i;
    for (i = 0; i < r->count; i++) {
        if (r->items[i] == id) return 1;
    }
    return 0;
}

static int id_registry_append_locked(id_registry_t *r, uint32_t id) {
    if (r->count == r->cap) {
        size_t new_cap = (r->cap == 0) ? 64 : (r->cap * 2);
        uint32_t *new_items = (uint32_t *)realloc(r->items, new_cap * sizeof(*new_items));
        if (!new_items) return -1;
        r->items = new_items;
        r->cap = new_cap;
    }

    r->items[r->count++] = id;
    return 0;
}

static int id_registry_init(id_registry_t *r, const char *path) {
    int n;

    memset(r, 0, sizeof(*r));
    pthread_mutex_init(&r->mutex, NULL);

    n = snprintf(r->path, sizeof(r->path), "%s", path);
    if (n < 0 || (size_t)n >= sizeof(r->path)) {
        errno = ENAMETOOLONG;
        pthread_mutex_destroy(&r->mutex);
        return -1;
    }

    r->fp = counted_fopen(path, "w");
    if (!r->fp) {
        pthread_mutex_destroy(&r->mutex);
        return -1;
    }

    return 0;
}

static void id_registry_destroy(id_registry_t *r) {
    if (r->fp) counted_fclose(r->fp);
    free(r->items);
    r->fp = NULL;
    r->items = NULL;
    r->count = 0;
    r->cap = 0;
    pthread_mutex_destroy(&r->mutex);
}

static void write_uid_if_new(uid_t uid) {
    char namebuf[4096];
    struct passwd pwd;
    struct passwd *result = NULL;
    const char *name;

    pthread_mutex_lock(&g_uid_registry.mutex);
    if (id_registry_contains_locked(&g_uid_registry, (uint32_t)uid)) {
        pthread_mutex_unlock(&g_uid_registry.mutex);
        return;
    }
    if (id_registry_append_locked(&g_uid_registry, (uint32_t)uid) != 0) {
        pthread_mutex_unlock(&g_uid_registry.mutex);
        return;
    }

    if (getpwuid_r(uid, &pwd, namebuf, sizeof(namebuf), &result) == 0 && result && result->pw_name) name = result->pw_name;
    else name = "UNKNOWN";

    fprintf(g_uid_registry.fp, "%u %s\n", (unsigned int)uid, name);
    counted_fflush(g_uid_registry.fp);
    pthread_mutex_unlock(&g_uid_registry.mutex);
}

static void write_gid_if_new(gid_t gid) {
    char namebuf[4096];
    struct group grp;
    struct group *result = NULL;
    const char *name;

    pthread_mutex_lock(&g_gid_registry.mutex);
    if (id_registry_contains_locked(&g_gid_registry, (uint32_t)gid)) {
        pthread_mutex_unlock(&g_gid_registry.mutex);
        return;
    }
    if (id_registry_append_locked(&g_gid_registry, (uint32_t)gid) != 0) {
        pthread_mutex_unlock(&g_gid_registry.mutex);
        return;
    }

    if (getgrgid_r(gid, &grp, namebuf, sizeof(namebuf), &result) == 0 && result && result->gr_name) name = result->gr_name;
    else name = "UNKNOWN";

    fprintf(g_gid_registry.fp, "%u %s\n", (unsigned int)gid, name);
    counted_fflush(g_gid_registry.fp);
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

static uint64_t regular_file_byte_credit(shared_state_t *shared, crawl_stats_t *stats, const struct stat *st) {
    int seen_result;

    if (!S_ISREG(st->st_mode)) return 0;
    if (st->st_nlink <= 1) return (uint64_t)st->st_size;

    stats->total_hardlink_files++;
    seen_result = inode_registry_mark_seen(&g_hardlink_registry, (uint64_t)st->st_dev, (uint64_t)st->st_ino);
    if (seen_result < 0) {
        stats_add_error(shared);
        return (uint64_t)st->st_size;
    }
    return seen_result ? (uint64_t)st->st_size : 0;
}

static void account_entry_local(shared_state_t *shared, crawl_stats_t *stats, perf_local_t *perf, const struct stat *st) {
    uint64_t byte_credit = 0;

    if (!shared || !stats || !perf || !st) return;

    stats->total_entries++;
    perf->entries++;

    if (S_ISDIR(st->st_mode)) {
        stats->total_dirs++;
        perf->dirs++;
    } else if (S_ISREG(st->st_mode)) {
        stats->total_files++;
        perf->files++;
        byte_credit = regular_file_byte_credit(shared, stats, st);
        stats->total_bytes += byte_credit;
        perf->bytes += byte_credit;
    } else if (S_ISLNK(st->st_mode)) {
        stats->total_symlinks++;
    } else {
        stats->total_other++;
    }

    if (perf->entries >= PERF_FLUSH_INTERVAL) perf_flush_local(perf);
}

static void stats_merge(shared_state_t *shared, const crawl_stats_t *local) {
    shared->total_entries += local->total_entries;
    shared->total_dirs += local->total_dirs;
    shared->total_files += local->total_files;
    shared->total_hardlink_files += local->total_hardlink_files;
    shared->total_symlinks += local->total_symlinks;
    shared->total_other += local->total_other;
    shared->total_bytes += local->total_bytes;
}

static void stats_add_error(shared_state_t *s) {
    pthread_mutex_lock(&s->stats_mutex);
    s->total_errors++;
    pthread_mutex_unlock(&s->stats_mutex);
}

static void stats_add_worker_started(shared_state_t *s) {
    pthread_mutex_lock(&s->stats_mutex);
    s->worker_threads_started++;
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
    int idx;

    if (!perf || perf->entries == 0) return;

    idx = (int)ATOMIC_LOAD_RELAXED(&g_bucket_index);
    ATOMIC_ADD_RELAXED(&g_total_entries, perf->entries);
    ATOMIC_ADD_RELAXED(&g_window_entries, perf->entries);
    ATOMIC_ADD_RELAXED(&g_bucket_entries[idx], perf->entries);

    if (perf->dirs > 0) {
        ATOMIC_ADD_RELAXED(&g_total_dirs, perf->dirs);
        ATOMIC_ADD_RELAXED(&g_window_dirs, perf->dirs);
        ATOMIC_ADD_RELAXED(&g_bucket_dirs[idx], perf->dirs);
    }
    if (perf->files > 0) {
        ATOMIC_ADD_RELAXED(&g_total_files, perf->files);
        ATOMIC_ADD_RELAXED(&g_window_files, perf->files);
        ATOMIC_ADD_RELAXED(&g_bucket_files[idx], perf->files);
        ATOMIC_ADD_RELAXED(&g_total_bytes, perf->bytes);
    }

    memset(perf, 0, sizeof(*perf));
}

static int write_bin_header(FILE *fp) {
    bin_file_header_t hdr;

    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, FILE_MAGIC, strlen(FILE_MAGIC));
    hdr.magic[7] = '\0';
    hdr.version = FORMAT_VERSION;
    return counted_fwrite(&hdr, sizeof(hdr), 1, fp) == 1 ? 0 : -1;
}

static void print_usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--no-write] <start-path> [split-depth] [output-dir] [uid-shards] [writer-threads]\n",
            prog);
    fprintf(stderr, "Example: %s /data1 3 /scratch/crawl_out 8192 8\n", prog);
    fprintf(stderr, "Benchmark: %s --no-write /data1 3\n", prog);
    fprintf(stderr, "Note: split-depth is accepted for compatibility; scheduling now seeds from the root path only.\n");
}

static int ensure_output_dir_exists(const char *path) {
    struct stat st;

    if (!path || path[0] == '\0') {
        errno = EINVAL;
        return -1;
    }

    if (counted_stat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            errno = ENOTDIR;
            return -1;
        }
        return 0;
    }
    if (errno != ENOENT) return -1;
    return counted_mkdir(path, 0775) == 0 ? 0 : -1;
}

static int build_shard_path(uint32_t shard, char *out, size_t out_sz) {
    int n = snprintf(out, out_sz, "%s/uid_shard_%0*u.bin", g_output_dir, g_shard_digits, shard);
    return (n < 0 || (size_t)n >= out_sz) ? -1 : 0;
}

static int inspect_existing_shard(const char *path, uint64_t *size_out, int *valid_out) {
    struct stat st;
    FILE *fp = NULL;
    bin_file_header_t hdr;

    *size_out = 0;
    *valid_out = 0;

    if (counted_stat(path, &st) != 0) {
        if (errno == ENOENT) return 0;
        return -1;
    }

    *size_out = (uint64_t)st.st_size;
    if (st.st_size == 0) return 0;

    fp = counted_fopen(path, "rb");
    if (!fp) return -1;

    if (fread(&hdr, sizeof(hdr), 1, fp) == 1 &&
        memcmp(hdr.magic, "NFSCBIN", 7) == 0 &&
        hdr.version == FORMAT_VERSION) {
        *valid_out = 1;
    }

    counted_fclose(fp);
    return 0;
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
    pthread_mutex_unlock(&q->mutex);

    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
}

static int queue_push_stack_take(task_queue_t *q, dir_stack_t *task) {
    task_node_t *node;

    if (!task || task->count == 0) return 0;

    node = (task_node_t *)malloc(sizeof(*node));
    if (!node) return -1;

    node->items = task->items;
    node->count = task->count;
    node->cap = task->cap;
    node->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        free(node);
        return -1;
    }
    if (q->tail) q->tail->next = node;
    else q->head = node;
    q->tail = node;
    q->queued_tasks++;
    ATOMIC_ADD_RELAXED(&g_queue_depth, 1);
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);

    task->items = NULL;
    task->count = 0;
    task->cap = 0;
    return 0;
}

static int queue_pop_wait(task_queue_t *q, dir_stack_t *task) {
    task_node_t *node;

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
        pthread_cond_wait(&q->cond, &q->mutex);
    }

    node = q->head;
    q->head = node->next;
    if (!q->head) q->tail = NULL;
    atomic_fetch_add(&g_active_workers, 1);
    pthread_mutex_unlock(&q->mutex);

    ATOMIC_SUB_RELAXED(&g_queue_depth, 1);
    ATOMIC_ADD_RELAXED(&g_tasks_popped, 1);

    task->items = node->items;
    task->count = node->count;
    task->cap = node->cap;
    free(node);
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
        free(cur->data);
        free(cur);
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
        pthread_cond_wait(&q->cond_nonfull, &q->mutex);
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

    ATOMIC_ADD_RELAXED(&g_writer_queue_depth, 1);
    ATOMIC_ADD_RELAXED(&g_batches_enqueued, 1);

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
        pthread_cond_wait(&q->cond_nonempty, &q->mutex);
    }

    batch = q->head;
    q->head = batch->next;
    if (!q->head) q->tail = NULL;
    q->count--;
    pthread_cond_signal(&q->cond_nonfull);
    pthread_mutex_unlock(&q->mutex);

    ATOMIC_SUB_RELAXED(&g_writer_queue_depth, 1);
    ATOMIC_ADD_RELAXED(&g_batches_dequeued, 1);
    return batch;
}

static int emit_context_init(emit_context_t *ctx, writer_queue_t *writer_queues, int writer_threads) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->writer_queues = writer_queues;
    ctx->writer_threads = writer_threads;
    if (g_no_write || writer_threads <= 0) return 0;
    ctx->pending = (pending_batch_t *)calloc((size_t)writer_threads, sizeof(*ctx->pending));
    return ctx->pending ? 0 : -1;
}

static void emit_context_destroy(emit_context_t *ctx) {
    int i;
    if (!ctx) return;
    for (i = 0; i < ctx->writer_threads; i++) free(ctx->pending[i].data);
    free(ctx->pending);
    ctx->pending = NULL;
}

static int flush_pending_batch(emit_context_t *ctx, int writer_index) {
    pending_batch_t *p = &ctx->pending[writer_index];
    record_batch_t *batch;

    if (p->len == 0) return 0;

    batch = (record_batch_t *)malloc(sizeof(*batch));
    if (!batch) return -1;
    batch->data = p->data;
    batch->len = p->len;
    batch->next = NULL;

    p->data = NULL;
    p->len = 0;
    p->cap = 0;

    if (writer_queue_push(&ctx->writer_queues[writer_index], batch) != 0) {
        free(batch->data);
        free(batch);
        return -1;
    }

    return 0;
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

static int emit_record(emit_context_t *ctx, const char *path, size_t path_len, const struct stat *st) {
    bin_record_hdr_t hdr;
    batch_frame_hdr_t frame;
    pending_batch_t *pending;
    uint32_t shard;
    int writer_index;
    size_t record_len;
    size_t frame_len;

    if (!ctx || !path || !st) return -1;
    if (g_no_write) return 0;

    if (path_len > UINT16_MAX) return -1;

    memset(&hdr, 0, sizeof(hdr));
    hdr.path_len = (uint16_t)path_len;
    hdr.type = (uint8_t)file_type_char(st->st_mode);
    hdr.mode = (uint32_t)st->st_mode;
    hdr.uid = (uint64_t)st->st_uid;
    hdr.gid = (uint64_t)st->st_gid;
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

    record_len = sizeof(hdr) + path_len;
    frame.shard = shard;
    frame.data_len = (uint32_t)record_len;
    frame_len = sizeof(frame) + record_len;

    if (pending->len > 0 && pending->len + frame_len > pending->cap) {
        if (flush_pending_batch(ctx, writer_index) != 0) return -1;
        pending = &ctx->pending[writer_index];
    }

    if (ensure_pending_capacity(pending, pending->len + frame_len) != 0) return -1;

    memcpy(pending->data + pending->len, &frame, sizeof(frame));
    pending->len += sizeof(frame);
    memcpy(pending->data + pending->len, &hdr, sizeof(hdr));
    pending->len += sizeof(hdr);
    if (path_len > 0) {
        memcpy(pending->data + pending->len, path, path_len);
        pending->len += path_len;
    }

    if (pending->len >= RECORD_BATCH_BYTES) {
        if (flush_pending_batch(ctx, writer_index) != 0) return -1;
    }

    return 0;
}

static int emit_context_flush_all(emit_context_t *ctx) {
    int i;
    if (!ctx || g_no_write || ctx->writer_threads <= 0) return 0;
    for (i = 0; i < ctx->writer_threads; i++) {
        if (flush_pending_batch(ctx, i) != 0) return -1;
    }
    return 0;
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

static int dir_stack_push_take(dir_stack_t *s, char *path_owned, size_t path_len, const struct stat *st) {
    if (s->count == s->cap) {
        size_t new_cap = (s->cap == 0) ? 64 : (s->cap * 2);
        dir_work_t *new_items = (dir_work_t *)realloc(s->items, new_cap * sizeof(*new_items));
        if (!new_items) return -1;
        s->items = new_items;
        s->cap = new_cap;
    }

    s->items[s->count].path = path_owned;
    s->items[s->count].path_len = path_len;
    if (st) s->items[s->count].st = *st;
    else memset(&s->items[s->count].st, 0, sizeof(s->items[s->count].st));
    s->items[s->count].have_stat = st ? 1 : 0;
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
    int started = (int)shared->worker_threads_started;
    int idle = started - active;

    if (started <= 1) return 0;
    if (active >= started) return 0;
    if (local_stack->count < LOCAL_STACK_DONATE_FLOOR) return 0;
    if (qdepth >= (uint64_t)(idle * DONATE_QUEUE_TARGET_PER_IDLE)) return 0;
    return 1;
}

static int donate_stack_chunk(dir_stack_t *local_stack, task_queue_t *queue, worker_aux_stats_t *aux) {
    dir_stack_t donated;
    size_t count, start;

    if (!local_stack || local_stack->count < LOCAL_STACK_DONATE_FLOOR) return 0;

    count = local_stack->count / 2;
    if (count < DONATE_CHUNK_MIN) count = DONATE_CHUNK_MIN;
    if (count > DONATE_CHUNK_MAX) count = DONATE_CHUNK_MAX;
    if (count >= local_stack->count) count = local_stack->count - 1;
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
        free(donated.items);
        return -1;
    }

    stats_add_donated_dirs_local(aux, count);
    return 0;
}

static int process_directory_iterative(dir_stack_t *stack,
                                       shared_state_t *shared,
                                       crawl_stats_t *stats,
                                       perf_local_t *perf,
                                       worker_aux_stats_t *aux,
                                       emit_context_t *emit,
                                       task_queue_t *queue) {
    while (stack->count > 0) {
        dir_work_t work;
        char *dir_path;
        size_t dir_path_len;
        struct stat st;
        DIR *dir = NULL;
        struct dirent *ent;

        if (dir_stack_pop(stack, &work) != 0) break;
        dir_path = work.path;

        if (work.have_stat) st = work.st;
        else {
            memset(&st, 0, sizeof(st));
            if (counted_lstat(dir_path, &st) != 0) {
                fprintf(stderr, "ERROR worker lstat %s: %s\n", dir_path, strerror(errno));
                stats_add_error(shared);
                free(dir_path);
                continue;
            }
        }

        record_ids_from_stat(&st);
        account_entry_local(shared, stats, perf, &st);
        dir_path_len = work.path_len;
        if (emit_record(emit, dir_path, dir_path_len, &st) != 0) {
            fprintf(stderr, "ERROR worker emit_record %s: %s\n", dir_path, strerror(errno));
            stats_add_error(shared);
            free(dir_path);
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            free(dir_path);
            continue;
        }

        dir = counted_opendir(dir_path);
        if (!dir) {
            fprintf(stderr, "ERROR worker opendir %s: %s\n", dir_path, strerror(errno));
            stats_add_error(shared);
            free(dir_path);
            continue;
        }

        {
            int dir_fd = dirfd(dir);
            if (dir_fd < 0) {
                fprintf(stderr, "ERROR worker dirfd %s: %s\n", dir_path, strerror(errno));
                stats_add_error(shared);
                counted_closedir(dir);
                free(dir_path);
                continue;
            }

            while ((ent = counted_readdir(dir)) != NULL) {
                char child[PATH_MAX];
                size_t child_name_len;
                struct stat child_st;

                if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;

                if (counted_fstatat_nofollow(dir_fd, ent->d_name, &child_st) != 0) {
                    fprintf(stderr, "ERROR worker fstatat %s/%s: %s\n", dir_path, ent->d_name, strerror(errno));
                    stats_add_error(shared);
                    continue;
                }

                child_name_len = strlen(ent->d_name);
                if (join_path_fast(dir_path, dir_path_len, ent->d_name, child_name_len, child, sizeof(child)) != 0) {
                    fprintf(stderr, "ERROR worker path too long: %s/%s\n", dir_path, ent->d_name);
                    stats_add_error(shared);
                    continue;
                }

                if (S_ISDIR(child_st.st_mode)) {
                    char *child_path_owned;
                    size_t child_path_len;

                    if (join_path_alloc(dir_path, dir_path_len, ent->d_name, child_name_len,
                                        &child_path_owned, &child_path_len) != 0) {
                        fprintf(stderr, "ERROR worker path alloc %s/%s: %s\n", dir_path, ent->d_name, strerror(errno));
                        stats_add_error(shared);
                        continue;
                    }
                    if (dir_stack_push_take(stack, child_path_owned, child_path_len, &child_st) != 0) {
                        fprintf(stderr, "ERROR worker stack push %s: %s\n", child_path_owned, strerror(errno));
                        free(child_path_owned);
                        stats_add_error(shared);
                        continue;
                    }
                    while (should_donate_work(shared, stack)) {
                        if (donate_stack_chunk(stack, queue, aux) != 0) {
                            fprintf(stderr, "ERROR worker donate chunk under %s: %s\n", dir_path, strerror(errno));
                            stats_add_error(shared);
                            break;
                        }
                    }
                } else {
                    record_ids_from_stat(&child_st);
                    account_entry_local(shared, stats, perf, &child_st);
                    if (emit_record(emit, child, dir_path_len + child_name_len + (dir_path_len == 1 && dir_path[0] == '/' ? 0 : 1), &child_st) != 0) {
                        fprintf(stderr, "ERROR worker emit_record %s: %s\n", child, strerror(errno));
                        stats_add_error(shared);
                    }
                }
            }
        }

        counted_closedir(dir);
        free(dir_path);
    }

    return 0;
}


static void *stats_thread_main(void *arg) {
    (void)arg;

    while (!atomic_load(&g_stop_stats)) {
        sleep(1);

        {
            int next = (atomic_load(&g_bucket_index) + 1) % WINDOW_SECONDS;
            unsigned long long expired_entries = atomic_exchange(&g_bucket_entries[next], 0);
            unsigned long long expired_files = atomic_exchange(&g_bucket_files[next], 0);
            unsigned long long expired_dirs = atomic_exchange(&g_bucket_dirs[next], 0);
            atomic_fetch_sub(&g_window_entries, expired_entries);
            atomic_fetch_sub(&g_window_files, expired_files);
            atomic_fetch_sub(&g_window_dirs, expired_dirs);
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
            unsigned long long qdepth = atomic_load(&g_queue_depth);
            unsigned long long popped = atomic_load(&g_tasks_popped);
            unsigned long long writer_qdepth = atomic_load(&g_writer_queue_depth);
            int active = atomic_load(&g_active_workers);
            unsigned int divisor = atomic_load(&g_seconds_seen);
            double ops_rate;
            double elapsed_sec = g_run_start_sec > 0.0 ? now_sec() - g_run_start_sec : 0.0;
            char te[32], tf[32], td[32], ts[32], re[32], pp[32], elapsed_buf[32];

            if (divisor == 0) divisor = 1;
            ops_rate = (double)window_entries / (double)divisor;
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
            human_decimal((double)total_entries, te, sizeof(te));
            human_decimal((double)total_files, tf, sizeof(tf));
            human_decimal((double)total_dirs, td, sizeof(td));
            human_decimal((double)total_bytes, ts, sizeof(ts));
            human_decimal(ops_rate, re, sizeof(re));
            human_decimal((double)popped, pp, sizeof(pp));
            format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

            printf("\r%s ops/s(10s) | tot:%s f:%s d:%s s:%s | q:%llu wq:%llu t:%d p:%s | el:%s            ",
                   re, te, tf, td, ts, qdepth, writer_qdepth, active, pp, elapsed_buf);
            fflush(stdout);
        }
    }

    return NULL;
}

static void *worker_thread_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;
    emit_context_t emit;

    if (emit_context_init(&emit, arg->writer_queues, arg->writer_threads) != 0) {
        fprintf(stderr, "ERROR worker %" PRIu64 " failed to initialize emit context\n", arg->worker_index);
        stats_add_error(arg->shared);
        return NULL;
    }

    for (;;) {
        dir_stack_t task;

        if (queue_pop_wait(arg->queue, &task) != 0) break;

        process_directory_iterative(&task, arg->shared, &arg->stats, &arg->perf, &arg->aux, &emit, arg->queue);
        atomic_fetch_sub(&g_active_workers, 1);

        if (atomic_load(&g_main_done) && atomic_load(&g_active_workers) == 0) {
            pthread_mutex_lock(&arg->queue->mutex);
            pthread_cond_broadcast(&arg->queue->cond);
            pthread_mutex_unlock(&arg->queue->mutex);
        }

        dir_stack_destroy(&task);
    }

    perf_flush_local(&arg->perf);
    if (emit_context_flush_all(&emit) != 0) stats_add_error(arg->shared);
    emit_context_destroy(&emit);
    return NULL;
}

static int writer_acquire_shard(shard_file_state_t *shards, uint32_t writer_index, uint32_t shard,
                                uint32_t uid_shards, unsigned max_open_shards,
                                unsigned *open_count, uint64_t *tick, FILE **fp_out) {
    shard_file_state_t *state = &shards[shard];
    char path[PATH_MAX];
    struct stat st;

    if (state->fp) {
        state->last_used = ++(*tick);
        *fp_out = state->fp;
        return 0;
    }

    if (*open_count >= max_open_shards) {
        uint32_t i;
        shard_file_state_t *victim = NULL;
        for (i = writer_index; i < uid_shards; i += (uint32_t)g_writer_threads) {
            if (shards[i].fp) {
                if (!victim || shards[i].last_used < victim->last_used) victim = &shards[i];
            }
        }
        if (victim) {
            counted_fclose(victim->fp);
            victim->fp = NULL;
            (*open_count)--;
        }
    }

    if (!state->initialized) {
        uint64_t sz = 0;
        int valid = 0;
        if (build_shard_path(shard, path, sizeof(path)) != 0) return -1;
        if (inspect_existing_shard(path, &sz, &valid) != 0) return -1;
        if (sz != 0 && !valid) {
            errno = EINVAL;
            return -1;
        }
        state->bytes_written = sz;
        state->initialized = 1;
    }

    if (build_shard_path(shard, path, sizeof(path)) != 0) return -1;

    if (state->bytes_written == 0) {
        state->fp = counted_fopen(path, "ab+");
        if (!state->fp) return -1;
        setvbuf(state->fp, NULL, _IOFBF, WRITE_BUFFER_SIZE);
        if (write_bin_header(state->fp) != 0) return -1;
        counted_fflush(state->fp);
        state->bytes_written = sizeof(bin_file_header_t);
    } else {
        if (counted_stat(path, &st) != 0) return -1;
        state->fp = counted_fopen(path, "ab");
        if (!state->fp) return -1;
        setvbuf(state->fp, NULL, _IOFBF, WRITE_BUFFER_SIZE);
    }

    (*open_count)++;
    state->last_used = ++(*tick);
    *fp_out = state->fp;
    return 0;
}

static int writer_process_batch(uint32_t writer_index,
                                shard_file_state_t *shards,
                                unsigned *open_count,
                                uint64_t *tick,
                                record_batch_t *batch) {
    size_t off = 0;

    while (off + sizeof(batch_frame_hdr_t) <= batch->len) {
        batch_frame_hdr_t frame;
        FILE *fp;

        memcpy(&frame, batch->data + off, sizeof(frame));
        off += sizeof(frame);

        if (frame.shard >= g_uid_shards || off + frame.data_len > batch->len) return -1;
        if ((frame.shard % (uint32_t)g_writer_threads) != writer_index) return -1;

        if (writer_acquire_shard(shards, writer_index, frame.shard, g_uid_shards,
                                 g_max_open_shards, open_count, tick, &fp) != 0) {
            return -1;
        }

        if (counted_fwrite(batch->data + off, 1, frame.data_len, fp) != frame.data_len) return -1;
        shards[frame.shard].bytes_written += frame.data_len;
        shards[frame.shard].last_used = ++(*tick);
        off += frame.data_len;
    }

    return (off == batch->len) ? 0 : -1;
}

static void *writer_thread_main(void *arg_void) {
    writer_arg_t *arg = (writer_arg_t *)arg_void;
    shard_file_state_t *shards = (shard_file_state_t *)calloc(g_uid_shards, sizeof(*shards));
    unsigned open_count = 0;
    uint64_t tick = 0;

    if (!shards) {
        fprintf(stderr, "ERROR writer %u failed to allocate shard state\n", arg->writer_index);
        return NULL;
    }

    for (;;) {
        record_batch_t *batch = writer_queue_pop(arg->queue);
        if (!batch) break;
        if (writer_process_batch(arg->writer_index, shards, &open_count, &tick, batch) != 0) {
            fprintf(stderr, "ERROR writer %u failed processing batch: %s\n", arg->writer_index, strerror(errno));
        }
        free(batch->data);
        free(batch);
    }

    {
        uint32_t i;
        for (i = arg->writer_index; i < g_uid_shards; i += (uint32_t)g_writer_threads) {
            if (shards[i].fp) counted_fclose(shards[i].fp);
        }
    }
    free(shards);
    return NULL;
}

static int enqueue_root_task(const char *path, shared_state_t *shared, task_queue_t *queue) {
    dir_stack_t task;
    struct stat st;
    char *dup;
    size_t path_len;

    dir_stack_init(&task);
    memset(&st, 0, sizeof(st));
    if (counted_lstat(path, &st) != 0) {
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
    if (dir_stack_push_take(&task, dup, path_len, &st) != 0) {
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

static int write_crawl_manifest(const char *start_path, int worker_count_started) {
    FILE *fp;
    char manifest_path[PATH_MAX];

    if (snprintf(manifest_path, sizeof(manifest_path), "%s/crawl_manifest.txt", g_output_dir) >= (int)sizeof(manifest_path)) return -1;
    fp = counted_fopen(manifest_path, "w");
    if (!fp) return -1;

    fprintf(fp, "format_version=%u\n", FORMAT_VERSION);
    fprintf(fp, "layout=uid_shards\n");
    fprintf(fp, "seed_mode=root_only\n");
    fprintf(fp, "start_path=%s\n", start_path);
    fprintf(fp, "split_depth=%d\n", g_split_depth);
    fprintf(fp, "byte_accounting=unique_regular_files\n");
    fprintf(fp, "crawl_workers=%d\n", worker_count_started);
    fprintf(fp, "writer_threads=%d\n", g_writer_threads);
    fprintf(fp, "uid_shards=%u\n", g_uid_shards);
    fprintf(fp, "uid_shard_digits=%d\n", g_shard_digits);
    fprintf(fp, "uid_output=uid.txt\n");
    fprintf(fp, "gid_output=gid.txt\n");
    counted_fclose(fp);
    return 0;
}

int main(int argc, char **argv) {
    const char *start_path;
    const char *positionals[5];
    shared_state_t shared;
    task_queue_t queue;
    writer_queue_t *writer_queues = NULL;
    pthread_t workers[MAX_WORKERS];
    worker_arg_t worker_args[MAX_WORKERS];
    pthread_t *writer_threads = NULL;
    writer_arg_t *writer_args = NULL;
    pthread_t stats_thread;
    double t0, t1;
    int worker_count_started = 0;
    int positional_count = 0;
    int writer_slots;
    int writer_threads_used = 0;
    int uid_registry_ready = 0;
    int gid_registry_ready = 0;
    int hardlink_registry_ready = 0;
    int i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--no-write") == 0) {
            g_no_write = 1;
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

    start_path = positionals[0];
    if (start_path[0] != '/') {
        fprintf(stderr, "start-path must begin with '/'\n");
        return 2;
    }
    if (positional_count >= 2) {
        g_split_depth = atoi(positionals[1]);
        if (g_split_depth < 1) {
            fprintf(stderr, "split-depth must be >= 1\n");
            return 2;
        }
    }
    if (positional_count >= 3) {
        int n = snprintf(g_output_dir, sizeof(g_output_dir), "%s", positionals[2]);
        if (n < 0 || (size_t)n >= sizeof(g_output_dir)) {
            fprintf(stderr, "output-dir is too long\n");
            return 2;
        }
    }
    if (positional_count >= 4) {
        unsigned long shards = strtoul(positionals[3], NULL, 10);
        if (shards == 0 || shards > UINT32_MAX || !is_power_of_two_u32((uint32_t)shards)) {
            fprintf(stderr, "uid-shards must be a power of two > 0\n");
            return 2;
        }
        g_uid_shards = (uint32_t)shards;
    }
    if (positional_count >= 5) {
        g_writer_threads = atoi(positionals[4]);
        if (g_writer_threads <= 0) {
            fprintf(stderr, "writer-threads must be > 0\n");
            return 2;
        }
    }
    if ((uint32_t)g_writer_threads > g_uid_shards) g_writer_threads = (int)g_uid_shards;
    g_shard_digits = shard_digits_for(g_uid_shards);
    writer_slots = g_writer_threads;
    writer_threads_used = g_no_write ? 0 : g_writer_threads;

    if (!g_no_write) {
        if (ensure_output_dir_exists(g_output_dir) != 0) {
            fprintf(stderr, "ERROR invalid output directory %s: %s\n", g_output_dir, strerror(errno));
            return 1;
        }

        {
            char uid_path[PATH_MAX];
            char gid_path[PATH_MAX];

            if (snprintf(uid_path, sizeof(uid_path), "%s/uid.txt", g_output_dir) < 0 ||
                snprintf(gid_path, sizeof(gid_path), "%s/gid.txt", g_output_dir) < 0) {
                fprintf(stderr, "ERROR failed to build uid/gid output paths\n");
                return 1;
            }

            if (id_registry_init(&g_uid_registry, uid_path) != 0) {
                fprintf(stderr, "ERROR failed to open %s: %s\n", uid_path, strerror(errno));
                return 1;
            }
            uid_registry_ready = 1;
            if (id_registry_init(&g_gid_registry, gid_path) != 0) {
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
    if (inode_registry_init(&g_hardlink_registry) != 0) {
        fprintf(stderr, "ERROR failed to initialize hardlink registry\n");
        pthread_mutex_destroy(&shared.stats_mutex);
        if (uid_registry_ready) id_registry_destroy(&g_uid_registry);
        if (gid_registry_ready) id_registry_destroy(&g_gid_registry);
        return 1;
    }
    hardlink_registry_ready = 1;
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
    }
    atomic_store(&g_total_entries, 0);
    atomic_store(&g_total_files, 0);
    atomic_store(&g_total_dirs, 0);
    atomic_store(&g_total_bytes, 0);
    atomic_store(&g_window_entries, 0);
    atomic_store(&g_window_files, 0);
    atomic_store(&g_window_dirs, 0);
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
    atomic_store(&g_queue_depth, 0);
    atomic_store(&g_active_workers, 0);
    atomic_store(&g_main_done, 0);
    atomic_store(&g_tasks_popped, 0);
    atomic_store(&g_writer_queue_depth, 0);
    atomic_store(&g_batches_enqueued, 0);
    atomic_store(&g_batches_dequeued, 0);
    atomic_store(&g_io_lstat_calls, 0);
    atomic_store(&g_io_stat_calls, 0);
    atomic_store(&g_io_mkdir_calls, 0);
    atomic_store(&g_io_opendir_calls, 0);
    atomic_store(&g_io_readdir_calls, 0);
    atomic_store(&g_io_closedir_calls, 0);
    atomic_store(&g_io_fopen_calls, 0);
    atomic_store(&g_io_fclose_calls, 0);
    atomic_store(&g_io_fwrite_calls, 0);
    atomic_store(&g_io_fflush_calls, 0);

    t0 = now_sec();
    g_run_start_sec = t0;

    if (!g_no_write) {
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
    }

    if (pthread_create(&stats_thread, NULL, stats_thread_main, NULL) != 0) {
        fprintf(stderr, "ERROR failed to create stats thread\n");
        if (!g_no_write) {
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

    for (i = 0; i < MAX_WORKERS; i++) {
        worker_args[i].shared = &shared;
        worker_args[i].queue = &queue;
        worker_args[i].writer_queues = writer_queues;
        worker_args[i].writer_threads = writer_threads_used;
        worker_args[i].worker_index = (uint64_t)(i + 1);
        memset(&worker_args[i].stats, 0, sizeof(worker_args[i].stats));
        memset(&worker_args[i].perf, 0, sizeof(worker_args[i].perf));
        memset(&worker_args[i].aux, 0, sizeof(worker_args[i].aux));

        if (pthread_create(&workers[i], NULL, worker_thread_main, &worker_args[i]) != 0) {
            fprintf(stderr, "ERROR failed to create worker %d\n", i + 1);
            stats_add_error(&shared);
            break;
        }
        worker_count_started++;
        stats_add_worker_started(&shared);
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
        for (i = 0; i < writer_threads_used; i++) writer_queue_close(&writer_queues[i]);
        for (i = 0; i < writer_threads_used; i++) pthread_join(writer_threads[i], NULL);
    }

    atomic_store(&g_stop_stats, 1);
    pthread_join(stats_thread, NULL);

    clear_status_line();
    t1 = now_sec();

    if (!g_no_write && write_crawl_manifest(start_path, worker_count_started) != 0) {
        fprintf(stderr, "ERROR failed to write crawl manifest: %s\n", strerror(errno));
    }

    printf("start_path=%s\n", start_path);
    printf("no_write=%d\n", g_no_write);
    printf("output_dir=%s\n", g_no_write ? "(disabled)" : g_output_dir);
    printf("output_layout=%s\n", g_no_write ? "none" : "uid_shards");
    printf("format_version=%u\n", FORMAT_VERSION);
    printf("seed_mode=%s\n", "root_only");
    printf("split_depth=%d\n", g_split_depth);
    printf("uid_shards=%u\n", g_uid_shards);
    printf("uid_shard_digits=%d\n", g_shard_digits);
    printf("writer_threads=%d\n", writer_threads_used);
    printf("max_worker_threads=%d\n", MAX_WORKERS);
    printf("max_open_shards=%u\n", g_no_write ? 0U : g_max_open_shards);
    printf("writer_queue_batches=%u\n", g_no_write ? 0U : g_writer_queue_batches);
    printf("record_batch_bytes=%u\n", (unsigned)RECORD_BATCH_BYTES);
    printf("write_buffer_size=%u\n", g_no_write ? 0U : (unsigned)WRITE_BUFFER_SIZE);
    printf("byte_accounting=%s\n", "unique_regular_files");
    printf("worker_threads_started=%" PRIu64 "\n", shared.worker_threads_started);
    printf("split_dirs_enqueued=%" PRIu64 "\n", shared.split_dirs_enqueued);
    printf("donated_dirs=%" PRIu64 "\n", shared.donated_dirs);
    printf("donation_attempts=%" PRIu64 "\n", shared.donation_attempts);
    printf("donation_successes=%" PRIu64 "\n", shared.donation_successes);
    printf("donation_success_pct=%.1f\n", shared.donation_attempts ? (100.0 * (double)shared.donation_successes) / (double)shared.donation_attempts : 0.0);
    printf("tasks_popped=%" PRIu64 "\n", (uint64_t)atomic_load(&g_tasks_popped));
    printf("avg_entries_per_task=%.2f\n", atomic_load(&g_tasks_popped) ? (double)shared.total_entries / (double)atomic_load(&g_tasks_popped) : 0.0);
    printf("avg_dirs_per_task=%.2f\n", atomic_load(&g_tasks_popped) ? (double)shared.total_dirs / (double)atomic_load(&g_tasks_popped) : 0.0);
    printf("avg_files_per_task=%.2f\n", atomic_load(&g_tasks_popped) ? (double)shared.total_files / (double)atomic_load(&g_tasks_popped) : 0.0);
    printf("batches_enqueued=%" PRIu64 "\n", (uint64_t)atomic_load(&g_batches_enqueued));
    printf("batches_dequeued=%" PRIu64 "\n", (uint64_t)atomic_load(&g_batches_dequeued));
    printf("entries=%" PRIu64 "\n", shared.total_entries);
    printf("dirs=%" PRIu64 "\n", shared.total_dirs);
    printf("files=%" PRIu64 "\n", shared.total_files);
    printf("hardlink_files=%" PRIu64 "\n", shared.total_hardlink_files);
    printf("symlinks=%" PRIu64 "\n", shared.total_symlinks);
    printf("other=%" PRIu64 "\n", shared.total_other);
    printf("total_bytes=%" PRIu64 "\n", shared.total_bytes);
    printf("errors=%" PRIu64 "\n", shared.total_errors);
    printf("io_lstat_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_lstat_calls));
    printf("io_stat_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_stat_calls));
    printf("io_mkdir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_mkdir_calls));
    printf("io_opendir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_opendir_calls));
    printf("io_readdir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_readdir_calls));
    printf("io_closedir_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_closedir_calls));
    printf("io_fopen_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fopen_calls));
    printf("io_fclose_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fclose_calls));
    printf("io_fwrite_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fwrite_calls));
    printf("io_fflush_calls=%" PRIu64 "\n", (uint64_t)atomic_load(&g_io_fflush_calls));
    printf("manifest=%s\n", g_no_write ? "(disabled)" : "crawl_manifest.txt");
    printf("uid_output=%s\n", g_no_write ? "(disabled)" : g_uid_registry.path);
    printf("gid_output=%s\n", g_no_write ? "(disabled)" : g_gid_registry.path);
    printf("ops_window_sec=%d\n", WINDOW_SECONDS);
    {
        double elapsed = t1 - t0;
        double avg_ops = elapsed > 0.0 ? (double)shared.total_entries / elapsed : 0.0;
        double mean_ops = g_ops_rate_samples ? g_ops_rate_sum / (double)g_ops_rate_samples : avg_ops;
        double max_ops = g_ops_rate_samples ? g_ops_rate_max : avg_ops;
        double min_ops = g_ops_rate_samples ? g_ops_rate_min : avg_ops;
        char avg_ops_buf[32], mean_ops_buf[32], max_ops_buf[32], min_ops_buf[32];

        human_decimal(avg_ops, avg_ops_buf, sizeof(avg_ops_buf));
        human_decimal(mean_ops, mean_ops_buf, sizeof(mean_ops_buf));
        human_decimal(max_ops, max_ops_buf, sizeof(max_ops_buf));
        human_decimal(min_ops, min_ops_buf, sizeof(min_ops_buf));

        printf("avg_ops_per_sec=%s\n", avg_ops_buf);
        printf("mean_ops_per_sec=%s\n", mean_ops_buf);
        printf("max_ops_per_sec=%s\n", max_ops_buf);
        printf("min_ops_per_sec=%s\n", min_ops_buf);
    }
    printf("donate_floor=%d\n", LOCAL_STACK_DONATE_FLOOR);
    printf("avg_active_workers=%.2f\n", g_active_workers_samples ? (double)g_active_workers_sum / (double)g_active_workers_samples : 0.0);
    printf("min_active_workers=%d\n", g_active_workers_min);
    printf("max_active_workers=%d\n", g_active_workers_max);
    printf("seconds_single_worker=%" PRIu64 "\n", g_seconds_single_worker);
    printf("seconds_queue_empty_single_worker=%" PRIu64 "\n", g_seconds_queue_empty_single_worker);
    printf("elapsed_sec=%.3f\n", t1 - t0);

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
    return 0;
}
