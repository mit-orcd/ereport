/*
 * ecrawl_nfs.c
 *
 * Compact binary-output NFS metadata crawler using libnfs.
 *
 * Features:
 *   - Main thread walks to a configurable split depth.
 *   - Directories found at split depth are queued as initial work.
 *   - Worker threads consume queued directories.
 *   - Workers traverse directories iteratively with a local stack.
 *   - Workers may donate deeper subdirectories back to the global queue.
 *   - Proper termination detection.
 *   - Existing output shards are appended to, never truncated.
 *   - No output shard grows beyond 128 MiB; files rotate automatically.
 *   - Rolling 10-second stats are printed once per second.
 *   - Live stats always show q, t, and p even when zero.
 *
 * Build:
 *   gcc -O2 -Wall -Wextra -pthread -o ecrawl_nfs ecrawl_nfs.c -lnfs
 *
 * Usage:
 *   ./ecrawl_nfs <nfs-url> [start-path] [split-depth]
 */

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
#include <unistd.h>
#include <stdatomic.h>

#include <nfsc/libnfs.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define MAX_WORKERS 16
#define WINDOW_SECONDS 10

#define FILE_MAGIC "NFSCBIN"
#define FILE_MAGIC_LEN 8
#define FORMAT_VERSION 2

#define LOCAL_STACK_DONATE_FLOOR 4
#define DONATE_LIMIT_PER_DIR 4

#define MAX_OUTPUT_FILE_SIZE (128ULL * 1024ULL * 1024ULL) /* 128 MiB */

typedef struct task_node {
    char *path;
    struct task_node *next;
} task_node_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    task_node_t *head;
    task_node_t *tail;
    int closed;
    uint64_t queued_tasks;
} task_queue_t;

typedef struct {
    pthread_mutex_t stats_mutex;
    uint64_t total_entries;
    uint64_t total_dirs;
    uint64_t total_files;
    uint64_t total_symlinks;
    uint64_t total_other;
    uint64_t total_errors;
    uint64_t total_bytes;
    uint64_t worker_threads_started;
    uint64_t split_dirs_enqueued;
    uint64_t nfs4_delay_retries;
    uint64_t donated_dirs;
} shared_state_t;

typedef struct {
    const char *url;
    shared_state_t *shared;
    task_queue_t *queue;
    uint64_t worker_index;
} worker_arg_t;

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

typedef struct {
    char **items;
    size_t count;
    size_t cap;
} dir_stack_t;

typedef struct {
    FILE *fp;
    uint64_t thread_index;
    uint64_t shard_index;
    uint64_t bytes_written;
    char path[PATH_MAX];
} output_writer_t;

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

/* Live visibility */
static atomic_ullong g_queue_depth    = 0;
static atomic_int    g_active_workers = 0;
static atomic_int    g_main_done      = 0;
static atomic_ullong g_tasks_popped   = 0;

static int g_split_depth = 2;

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

static int join_path(const char *base, const char *name, char *out, size_t out_sz) {
    int n;

    if (!base || !name || !out || out_sz == 0) {
        return -1;
    }

    if (strcmp(base, "/") == 0) {
        n = snprintf(out, out_sz, "/%s", name);
    } else {
        n = snprintf(out, out_sz, "%s/%s", base, name);
    }

    if (n < 0 || (size_t)n >= out_sz) {
        return -1;
    }

    return 0;
}

static void human_decimal(double v, char *buf, size_t sz) {
    const char *units[] = {"", "K", "M", "G", "T", "P", "E"};
    int i = 0;

    while (v >= 1000.0 && i < 6) {
        v /= 1000.0;
        i++;
    }

    if (v >= 100.0) {
        snprintf(buf, sz, "%.0f%s", v, units[i]);
    } else if (v >= 10.0) {
        snprintf(buf, sz, "%.1f%s", v, units[i]);
    } else {
        snprintf(buf, sz, "%.2f%s", v, units[i]);
    }
}

static void stats_add_entry(shared_state_t *s, mode_t mode, uint64_t size) {
    pthread_mutex_lock(&s->stats_mutex);

    s->total_entries++;
    if (S_ISDIR(mode)) {
        s->total_dirs++;
    } else if (S_ISREG(mode)) {
        s->total_files++;
        s->total_bytes += size;
    } else if (S_ISLNK(mode)) {
        s->total_symlinks++;
    } else {
        s->total_other++;
    }

    pthread_mutex_unlock(&s->stats_mutex);
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

static void stats_add_split_dir(shared_state_t *s) {
    pthread_mutex_lock(&s->stats_mutex);
    s->split_dirs_enqueued++;
    pthread_mutex_unlock(&s->stats_mutex);
}

static void stats_add_delay_retry(shared_state_t *s) {
    pthread_mutex_lock(&s->stats_mutex);
    s->nfs4_delay_retries++;
    pthread_mutex_unlock(&s->stats_mutex);
}

static void stats_add_donated_dir(shared_state_t *s) {
    pthread_mutex_lock(&s->stats_mutex);
    s->donated_dirs++;
    pthread_mutex_unlock(&s->stats_mutex);
}

static void perf_account(mode_t mode, uint64_t size) {
    int idx = atomic_load(&g_bucket_index);

    atomic_fetch_add(&g_total_entries, 1);
    atomic_fetch_add(&g_window_entries, 1);
    atomic_fetch_add(&g_bucket_entries[idx], 1);

    if (S_ISDIR(mode)) {
        atomic_fetch_add(&g_total_dirs, 1);
        atomic_fetch_add(&g_window_dirs, 1);
        atomic_fetch_add(&g_bucket_dirs[idx], 1);
    } else if (S_ISREG(mode)) {
        atomic_fetch_add(&g_total_files, 1);
        atomic_fetch_add(&g_window_files, 1);
        atomic_fetch_add(&g_bucket_files[idx], 1);
        atomic_fetch_add(&g_total_bytes, size);
    }
}

static int write_bin_header(FILE *fp) {
    bin_file_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, FILE_MAGIC, strlen(FILE_MAGIC));
    hdr.magic[7] = '\0';
    hdr.version = FORMAT_VERSION;
    hdr.reserved = 0;

    return fwrite(&hdr, sizeof(hdr), 1, fp) == 1 ? 0 : -1;
}

static int build_shard_path(uint64_t thread_index, uint64_t shard_index, char *out, size_t out_sz) {
    int n = snprintf(out, out_sz, "thread_%04" PRIu64 "_%04" PRIu64 ".bin",
                     thread_index, shard_index);
    return (n < 0 || (size_t)n >= out_sz) ? -1 : 0;
}

static int inspect_existing_shard(const char *path, uint64_t *size_out, int *valid_out) {
    struct stat st;
    FILE *fp = NULL;
    bin_file_header_t hdr;

    *size_out = 0;
    *valid_out = 0;

    if (stat(path, &st) != 0) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }

    *size_out = (uint64_t)st.st_size;

    if (st.st_size == 0) {
        *valid_out = 0;
        return 0;
    }

    fp = fopen(path, "rb");
    if (!fp) {
        return -1;
    }

    if (fread(&hdr, sizeof(hdr), 1, fp) == 1 &&
        memcmp(hdr.magic, "NFSCBIN", 7) == 0 &&
        hdr.version == FORMAT_VERSION) {
        *valid_out = 1;
    }

    fclose(fp);
    return 0;
}

static int open_or_create_shard(output_writer_t *w, uint64_t thread_index, uint64_t shard_index) {
    char path[PATH_MAX];
    uint64_t sz = 0;
    int valid = 0;
    FILE *fp = NULL;

    if (build_shard_path(thread_index, shard_index, path, sizeof(path)) != 0) {
        return -1;
    }

    if (inspect_existing_shard(path, &sz, &valid) != 0) {
        return -1;
    }

    if (sz == 0) {
        fp = fopen(path, "ab+");
        if (!fp) {
            return -1;
        }
        if (write_bin_header(fp) != 0) {
            fclose(fp);
            return -1;
        }
        fflush(fp);
        sz = sizeof(bin_file_header_t);
    } else {
        if (!valid) {
            return -1;
        }
        fp = fopen(path, "ab");
        if (!fp) {
            return -1;
        }
    }

    w->fp = fp;
    w->thread_index = thread_index;
    w->shard_index = shard_index;
    w->bytes_written = sz;
    snprintf(w->path, sizeof(w->path), "%s", path);
    return 0;
}

static int output_writer_init(output_writer_t *w, uint64_t thread_index) {
    uint64_t shard = 1;

    memset(w, 0, sizeof(*w));

    for (;;) {
        char path[PATH_MAX];
        uint64_t sz = 0;
        int valid = 0;

        if (build_shard_path(thread_index, shard, path, sizeof(path)) != 0) {
            return -1;
        }

        if (inspect_existing_shard(path, &sz, &valid) != 0) {
            return -1;
        }

        if (sz == 0) {
            return open_or_create_shard(w, thread_index, shard);
        }

        if (!valid) {
            return -1;
        }

        if (sz < MAX_OUTPUT_FILE_SIZE) {
            return open_or_create_shard(w, thread_index, shard);
        }

        shard++;
    }
}

static int output_writer_rotate(output_writer_t *w) {
    if (w->fp) {
        fclose(w->fp);
        w->fp = NULL;
    }
    return output_writer_init(w, w->thread_index);
}

static void output_writer_close(output_writer_t *w) {
    if (w->fp) {
        fclose(w->fp);
        w->fp = NULL;
    }
}

static int output_writer_write_record(output_writer_t *w, const char *path, const struct nfs_stat_64 *st) {
    bin_record_hdr_t hdr;
    size_t path_len;
    uint64_t record_size;

    if (!w || !w->fp || !path || !st) {
        return -1;
    }

    path_len = strlen(path);
    if (path_len > UINT16_MAX) {
        return -1;
    }

    record_size = (uint64_t)sizeof(hdr) + (uint64_t)path_len;
    if (w->bytes_written + record_size > MAX_OUTPUT_FILE_SIZE) {
        if (output_writer_rotate(w) != 0) {
            return -1;
        }
    }

    memset(&hdr, 0, sizeof(hdr));
    hdr.path_len = (uint16_t)path_len;
    hdr.type     = (uint8_t)file_type_char(st->nfs_mode);
    hdr.mode     = (uint32_t)(st->nfs_mode & 07777);
    hdr.uid      = (uint64_t)st->nfs_uid;
    hdr.gid      = (uint64_t)st->nfs_gid;
    hdr.size     = (uint64_t)st->nfs_size;
    hdr.inode    = (uint64_t)st->nfs_ino;
    hdr.dev_major = 0;
    hdr.dev_minor = 0;
    hdr.nlink    = (uint64_t)st->nfs_nlink;
    hdr.atime    = (uint64_t)st->nfs_atime;
    hdr.mtime    = (uint64_t)st->nfs_mtime;
    hdr.ctime    = (uint64_t)st->nfs_ctime;

    if (fwrite(&hdr, sizeof(hdr), 1, w->fp) != 1) {
        return -1;
    }
    if (path_len > 0 && fwrite(path, 1, path_len, w->fp) != path_len) {
        return -1;
    }

    w->bytes_written += record_size;
    return 0;
}

/* QUEUE FUNCTIONS THAT WERE MISSING */

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
        next = cur->next;
        free(cur->path);
        free(cur);
        cur = next;
    }
    q->head = NULL;
    q->tail = NULL;
    pthread_mutex_unlock(&q->mutex);

    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
}

static int queue_push(task_queue_t *q, const char *path) {
    task_node_t *node = malloc(sizeof(*node));
    if (!node) {
        return -1;
    }

    node->path = strdup(path);
    if (!node->path) {
        free(node);
        return -1;
    }
    node->next = NULL;

    pthread_mutex_lock(&q->mutex);

    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        free(node->path);
        free(node);
        return -1;
    }

    if (q->tail) {
        q->tail->next = node;
    } else {
        q->head = node;
    }
    q->tail = node;
    q->queued_tasks++;

    atomic_fetch_add(&g_queue_depth, 1);

    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static char *queue_pop_wait(task_queue_t *q) {
    task_node_t *node;
    char *path;

    pthread_mutex_lock(&q->mutex);

    for (;;) {
        if (q->head) {
            break;
        }

        if (q->closed) {
            pthread_mutex_unlock(&q->mutex);
            return NULL;
        }

        if (atomic_load(&g_main_done) && atomic_load(&g_active_workers) == 0) {
            q->closed = 1;
            pthread_cond_broadcast(&q->cond);
            pthread_mutex_unlock(&q->mutex);
            return NULL;
        }

        pthread_cond_wait(&q->cond, &q->mutex);
    }

    node = q->head;
    q->head = node->next;
    if (!q->head) {
        q->tail = NULL;
    }

    pthread_mutex_unlock(&q->mutex);

    atomic_fetch_sub(&g_queue_depth, 1);
    atomic_fetch_add(&g_tasks_popped, 1);

    path = node->path;
    free(node);
    return path;
}

static int is_nfs4_delay_error(struct nfs_context *nfs) {
    const char *err = nfs_get_error(nfs);
    return err && strstr(err, "NFS4ERR_DELAY") != NULL;
}

static int nfs_opendir_retry(struct nfs_context *nfs,
                             const char *path,
                             struct nfsdir **dir_out,
                             shared_state_t *shared) {
    static const useconds_t waits[] = {
        100000,
        250000,
        500000,
        1000000,
        2000000
    };
    size_t i;

    for (i = 0; i < sizeof(waits) / sizeof(waits[0]); i++) {
        if (nfs_opendir(nfs, path, dir_out) == 0) {
            return 0;
        }

        if (!is_nfs4_delay_error(nfs)) {
            return -1;
        }

        stats_add_delay_retry(shared);
        usleep(waits[i]);
    }

    return nfs_opendir(nfs, path, dir_out);
}

static int dir_stack_init(dir_stack_t *s) {
    s->items = NULL;
    s->count = 0;
    s->cap = 0;
    return 0;
}

static void dir_stack_destroy(dir_stack_t *s) {
    size_t i;
    for (i = 0; i < s->count; i++) {
        free(s->items[i]);
    }
    free(s->items);
    s->items = NULL;
    s->count = 0;
    s->cap = 0;
}

static int dir_stack_push_take(dir_stack_t *s, char *path_owned) {
    if (s->count == s->cap) {
        size_t new_cap = (s->cap == 0) ? 64 : (s->cap * 2);
        char **new_items = (char **)realloc(s->items, new_cap * sizeof(*new_items));
        if (!new_items) {
            return -1;
        }
        s->items = new_items;
        s->cap = new_cap;
    }

    s->items[s->count++] = path_owned;
    return 0;
}

static int dir_stack_push_dup(dir_stack_t *s, const char *path) {
    char *dup = strdup(path);
    if (!dup) {
        return -1;
    }
    if (dir_stack_push_take(s, dup) != 0) {
        free(dup);
        return -1;
    }
    return 0;
}

static char *dir_stack_pop(dir_stack_t *s) {
    if (s->count == 0) {
        return NULL;
    }
    return s->items[--s->count];
}

static int should_donate_work(const shared_state_t *shared, const dir_stack_t *local_stack) {
    uint64_t qdepth = atomic_load(&g_queue_depth);
    int active = atomic_load(&g_active_workers);
    int started = (int)shared->worker_threads_started;

    if (started <= 1) return 0;
    if (active >= started) return 0;
    if (local_stack->count < LOCAL_STACK_DONATE_FLOOR) return 0;
    if (qdepth >= (uint64_t)((started - active) * 2)) return 0;

    return 1;
}

static int process_directory_iterative(struct nfs_context *nfs,
                                       const char *root_path,
                                       shared_state_t *shared,
                                       output_writer_t *writer,
                                       task_queue_t *queue) {
    dir_stack_t stack;

    dir_stack_init(&stack);
    if (dir_stack_push_dup(&stack, root_path) != 0) {
        fprintf(stderr, "ERROR worker stack push %s: %s\n", root_path, strerror(errno));
        stats_add_error(shared);
        return -1;
    }

    while (stack.count > 0) {
        char *dir_path = dir_stack_pop(&stack);
        struct nfs_stat_64 st;
        struct nfsdir *dir = NULL;
        struct nfsdirent *ent;

        memset(&st, 0, sizeof(st));

        if (nfs_lstat64(nfs, dir_path, &st) != 0) {
            fprintf(stderr, "ERROR worker lstat %s: %s\n", dir_path, nfs_get_error(nfs));
            stats_add_error(shared);
            free(dir_path);
            continue;
        }

        stats_add_entry(shared, st.nfs_mode, (uint64_t)st.nfs_size);
        perf_account(st.nfs_mode, (uint64_t)st.nfs_size);

        if (output_writer_write_record(writer, dir_path, &st) != 0) {
            fprintf(stderr, "ERROR worker write_record %s: %s\n", dir_path, strerror(errno));
            stats_add_error(shared);
            free(dir_path);
            continue;
        }

        if (!S_ISDIR(st.nfs_mode)) {
            free(dir_path);
            continue;
        }

        if (nfs_opendir_retry(nfs, dir_path, &dir, shared) != 0) {
            fprintf(stderr, "ERROR worker opendir %s after retries: %s\n",
                    dir_path, nfs_get_error(nfs));
            stats_add_error(shared);
            free(dir_path);
            continue;
        }

        {
            int donated_this_dir = 0;

            while ((ent = nfs_readdir(nfs, dir)) != NULL) {
                char child[PATH_MAX];
                struct nfs_stat_64 child_st;

                if (strcmp(ent->name, ".") == 0 || strcmp(ent->name, "..") == 0) {
                    continue;
                }

                if (join_path(dir_path, ent->name, child, sizeof(child)) != 0) {
                    fprintf(stderr, "ERROR worker path too long: %s/%s\n", dir_path, ent->name);
                    stats_add_error(shared);
                    continue;
                }

                memset(&child_st, 0, sizeof(child_st));
                if (nfs_lstat64(nfs, child, &child_st) != 0) {
                    fprintf(stderr, "ERROR worker lstat %s: %s\n", child, nfs_get_error(nfs));
                    stats_add_error(shared);
                    continue;
                }

                if (S_ISDIR(child_st.nfs_mode)) {
                    if (donated_this_dir < DONATE_LIMIT_PER_DIR &&
                        should_donate_work(shared, &stack)) {
                        if (queue_push(queue, child) == 0) {
                            stats_add_donated_dir(shared);
                            donated_this_dir++;
                            pthread_mutex_lock(&queue->mutex);
                            pthread_cond_signal(&queue->cond);
                            pthread_mutex_unlock(&queue->mutex);
                        } else {
                            if (dir_stack_push_dup(&stack, child) != 0) {
                                fprintf(stderr, "ERROR worker stack push %s: %s\n",
                                        child, strerror(errno));
                                stats_add_error(shared);
                            }
                        }
                    } else {
                        if (dir_stack_push_dup(&stack, child) != 0) {
                            fprintf(stderr, "ERROR worker stack push %s: %s\n",
                                    child, strerror(errno));
                            stats_add_error(shared);
                        }
                    }
                } else {
                    stats_add_entry(shared, child_st.nfs_mode, (uint64_t)child_st.nfs_size);
                    perf_account(child_st.nfs_mode, (uint64_t)child_st.nfs_size);

                    if (output_writer_write_record(writer, child, &child_st) != 0) {
                        fprintf(stderr, "ERROR worker write_record %s: %s\n",
                                child, strerror(errno));
                        stats_add_error(shared);
                    }
                }
            }
        }

        nfs_closedir(nfs, dir);
        free(dir_path);
    }

    dir_stack_destroy(&stack);
    return 0;
}

static void *stats_thread_main(void *arg) {
    (void)arg;

    while (!atomic_load(&g_stop_stats)) {
        sleep(1);

        {
            int next = (atomic_load(&g_bucket_index) + 1) % WINDOW_SECONDS;

            unsigned long long expired_entries = atomic_exchange(&g_bucket_entries[next], 0);
            unsigned long long expired_files   = atomic_exchange(&g_bucket_files[next], 0);
            unsigned long long expired_dirs    = atomic_exchange(&g_bucket_dirs[next], 0);

            atomic_fetch_sub(&g_window_entries, expired_entries);
            atomic_fetch_sub(&g_window_files, expired_files);
            atomic_fetch_sub(&g_window_dirs, expired_dirs);

            atomic_store(&g_bucket_index, next);
        }

        {
            unsigned int seen = atomic_load(&g_seconds_seen);
            if (seen < WINDOW_SECONDS) {
                atomic_store(&g_seconds_seen, seen + 1);
            }
        }

        {
            unsigned long long total_entries  = atomic_load(&g_total_entries);
            unsigned long long total_files    = atomic_load(&g_total_files);
            unsigned long long total_dirs     = atomic_load(&g_total_dirs);
            unsigned long long total_bytes    = atomic_load(&g_total_bytes);
            unsigned long long window_entries = atomic_load(&g_window_entries);
            unsigned long long qdepth         = atomic_load(&g_queue_depth);
            unsigned long long popped         = atomic_load(&g_tasks_popped);
            int active                        = atomic_load(&g_active_workers);

            unsigned int divisor = atomic_load(&g_seconds_seen);
            if (divisor == 0) {
                divisor = 1;
            }

            char te[32], tf[32], td[32], ts[32], re[32], pp[32];

            human_decimal((double)total_entries, te, sizeof(te));
            human_decimal((double)total_files,   tf, sizeof(tf));
            human_decimal((double)total_dirs,    td, sizeof(td));
            human_decimal((double)total_bytes,   ts, sizeof(ts));
            human_decimal((double)window_entries / divisor, re, sizeof(re));
            human_decimal((double)popped, pp, sizeof(pp));

            printf("\r%s ops/s | tot:%s f:%s d:%s s:%s | q:%llu t:%d p:%s            ",
                   re, te, tf, td, ts, qdepth, active, pp);
            fflush(stdout);
        }
    }

    return NULL;
}

static void *worker_thread_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;
    struct nfs_context *nfs = NULL;
    struct nfs_url *url = NULL;
    output_writer_t writer;

    if (output_writer_init(&writer, arg->worker_index) != 0) {
        fprintf(stderr, "ERROR worker %" PRIu64 " failed to open output shard\n", arg->worker_index);
        stats_add_error(arg->shared);
        return NULL;
    }

    nfs = nfs_init_context();
    if (!nfs) {
        fprintf(stderr, "ERROR worker %" PRIu64 " nfs_init_context failed\n", arg->worker_index);
        stats_add_error(arg->shared);
        output_writer_close(&writer);
        return NULL;
    }

    url = nfs_parse_url_dir(nfs, arg->url);
    if (!url) {
        fprintf(stderr, "ERROR worker %" PRIu64 " parse url failed: %s\n",
                arg->worker_index, nfs_get_error(nfs));
        stats_add_error(arg->shared);
        nfs_destroy_context(nfs);
        output_writer_close(&writer);
        return NULL;
    }

    if (nfs_mount(nfs, url->server, url->path) != 0) {
        fprintf(stderr, "ERROR worker %" PRIu64 " mount failed: %s\n",
                arg->worker_index, nfs_get_error(nfs));
        stats_add_error(arg->shared);
        nfs_destroy_url(url);
        nfs_destroy_context(nfs);
        output_writer_close(&writer);
        return NULL;
    }

    for (;;) {
        char *task_path = queue_pop_wait(arg->queue);
        if (!task_path) {
            break;
        }

        atomic_fetch_add(&g_active_workers, 1);

        process_directory_iterative(nfs, task_path, arg->shared, &writer, arg->queue);

        atomic_fetch_sub(&g_active_workers, 1);

        pthread_mutex_lock(&arg->queue->mutex);
        pthread_cond_broadcast(&arg->queue->cond);
        pthread_mutex_unlock(&arg->queue->mutex);

        free(task_path);
    }

    nfs_destroy_url(url);
    nfs_destroy_context(nfs);
    output_writer_close(&writer);
    return NULL;
}

static int walk_and_seed(struct nfs_context *nfs,
                         const char *path,
                         int depth,
                         shared_state_t *shared,
                         output_writer_t *main_writer,
                         task_queue_t *queue) {
    struct nfs_stat_64 st;
    struct nfsdir *dir = NULL;
    struct nfsdirent *ent;
    int rc;

    memset(&st, 0, sizeof(st));

    rc = nfs_lstat64(nfs, path, &st);
    if (rc != 0) {
        fprintf(stderr, "ERROR main lstat %s: %s\n", path, nfs_get_error(nfs));
        stats_add_error(shared);
        return -1;
    }

    /*
     * Ownership rule:
     *   - main thread accounts only objects strictly above split depth
     *   - workers account directories at split depth and below
     *
     * This avoids double-counting split-depth directories that are
     * discovered by the main thread and later processed by workers.
     */

    if (!S_ISDIR(st.nfs_mode)) {
        stats_add_entry(shared, st.nfs_mode, (uint64_t)st.nfs_size);
        perf_account(st.nfs_mode, (uint64_t)st.nfs_size);

        if (depth <= g_split_depth) {
            if (output_writer_write_record(main_writer, path, &st) != 0) {
                fprintf(stderr, "ERROR main write_record %s: %s\n", path, strerror(errno));
                stats_add_error(shared);
                return -1;
            }
        }

        return 0;
    }

    if (depth == g_split_depth) {
        if (queue_push(queue, path) != 0) {
            fprintf(stderr, "ERROR main failed to enqueue split-depth dir: %s\n", path);
            stats_add_error(shared);
            return -1;
        }
        stats_add_split_dir(shared);

        pthread_mutex_lock(&queue->mutex);
        pthread_cond_signal(&queue->cond);
        pthread_mutex_unlock(&queue->mutex);

        return 0;
    }

    stats_add_entry(shared, st.nfs_mode, (uint64_t)st.nfs_size);
    perf_account(st.nfs_mode, (uint64_t)st.nfs_size);

    if (output_writer_write_record(main_writer, path, &st) != 0) {
        fprintf(stderr, "ERROR main write_record %s: %s\n", path, strerror(errno));
        stats_add_error(shared);
        return -1;
    }

    rc = nfs_opendir_retry(nfs, path, &dir, shared);
    if (rc != 0) {
        fprintf(stderr, "ERROR main opendir %s after retries: %s\n",
                path, nfs_get_error(nfs));
        stats_add_error(shared);
        return -1;
    }

    while ((ent = nfs_readdir(nfs, dir)) != NULL) {
        char child[PATH_MAX];

        if (strcmp(ent->name, ".") == 0 || strcmp(ent->name, "..") == 0) {
            continue;
        }

        if (join_path(path, ent->name, child, sizeof(child)) != 0) {
            fprintf(stderr, "ERROR main path too long: %s/%s\n", path, ent->name);
            stats_add_error(shared);
            continue;
        }

        walk_and_seed(nfs, child, depth + 1, shared, main_writer, queue);
    }

    nfs_closedir(nfs, dir);
    return 0;
}

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

int main(int argc, char **argv) {
    const char *url_str;
    const char *start_path = "/";
    struct nfs_context *nfs = NULL;
    struct nfs_url *url = NULL;
    shared_state_t shared;
    task_queue_t queue;
    pthread_t workers[MAX_WORKERS];
    worker_arg_t worker_args[MAX_WORKERS];
    pthread_t stats_thread;
    output_writer_t main_writer;
    double t0, t1;
    int worker_count_started = 0;
    int i;

    if (argc < 2 || argc > 4) {
        fprintf(stderr, "Usage: %s <nfs-url> [start-path] [split-depth]\n", argv[0]);
        fprintf(stderr, "Example: %s \"nfs://fstor004:/?version=4&uid=0&gid=0\" / 3\n", argv[0]);
        return 2;
    }

    url_str = argv[1];
    if (argc >= 3) {
        start_path = argv[2];
        if (start_path[0] != '/') {
            fprintf(stderr, "start-path must begin with '/'\n");
            return 2;
        }
    }
    if (argc == 4) {
        g_split_depth = atoi(argv[3]);
        if (g_split_depth < 1) {
            fprintf(stderr, "split-depth must be >= 1\n");
            return 2;
        }
    }

    memset(&shared, 0, sizeof(shared));
    pthread_mutex_init(&shared.stats_mutex, NULL);
    queue_init(&queue);

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
    atomic_store(&g_queue_depth, 0);
    atomic_store(&g_active_workers, 0);
    atomic_store(&g_main_done, 0);
    atomic_store(&g_tasks_popped, 0);

    if (output_writer_init(&main_writer, 0) != 0) {
        fprintf(stderr, "ERROR failed to open main output shard\n");
        queue_destroy(&queue);
        pthread_mutex_destroy(&shared.stats_mutex);
        return 1;
    }

    t0 = now_sec();

    if (pthread_create(&stats_thread, NULL, stats_thread_main, NULL) != 0) {
        fprintf(stderr, "ERROR failed to create stats thread\n");
        output_writer_close(&main_writer);
        queue_destroy(&queue);
        pthread_mutex_destroy(&shared.stats_mutex);
        return 1;
    }

    for (i = 0; i < MAX_WORKERS; i++) {
        worker_args[i].url = url_str;
        worker_args[i].shared = &shared;
        worker_args[i].queue = &queue;
        worker_args[i].worker_index = (uint64_t)(i + 1);

        if (pthread_create(&workers[i], NULL, worker_thread_main, &worker_args[i]) != 0) {
            fprintf(stderr, "ERROR failed to create worker %d\n", i + 1);
            stats_add_error(&shared);
            break;
        } else {
            worker_count_started++;
            stats_add_worker_started(&shared);
        }
    }

    nfs = nfs_init_context();
    if (!nfs) {
        fprintf(stderr, "ERROR nfs_init_context failed\n");
        goto out;
    }

    url = nfs_parse_url_dir(nfs, url_str);
    if (!url) {
        fprintf(stderr, "ERROR parse url failed: %s\n", nfs_get_error(nfs));
        goto out;
    }

    if (nfs_mount(nfs, url->server, url->path) != 0) {
        fprintf(stderr, "ERROR mount failed: %s\n", nfs_get_error(nfs));
        goto out;
    }

    walk_and_seed(nfs, start_path, 0, &shared, &main_writer, &queue);

out:
    atomic_store(&g_main_done, 1);

    pthread_mutex_lock(&queue.mutex);
    pthread_cond_broadcast(&queue.cond);
    pthread_mutex_unlock(&queue.mutex);

    for (i = 0; i < worker_count_started; i++) {
        pthread_join(workers[i], NULL);
    }

    pthread_mutex_lock(&queue.mutex);
    queue.closed = 1;
    pthread_cond_broadcast(&queue.cond);
    pthread_mutex_unlock(&queue.mutex);

    atomic_store(&g_stop_stats, 1);
    pthread_join(stats_thread, NULL);

    printf("\n");

    t1 = now_sec();

    printf("main_output=%s\n", main_writer.path);
    printf("max_worker_threads=%d\n", MAX_WORKERS);
    printf("split_depth=%d\n", g_split_depth);
    printf("worker_threads_started=%" PRIu64 "\n", shared.worker_threads_started);
    printf("split_dirs_enqueued=%" PRIu64 "\n", shared.split_dirs_enqueued);
    printf("donated_dirs=%" PRIu64 "\n", shared.donated_dirs);
    printf("tasks_popped=%" PRIu64 "\n", (uint64_t)atomic_load(&g_tasks_popped));
    printf("delay_retries=%" PRIu64 "\n", shared.nfs4_delay_retries);
    printf("entries=%" PRIu64 "\n", shared.total_entries);
    printf("dirs=%" PRIu64 "\n", shared.total_dirs);
    printf("files=%" PRIu64 "\n", shared.total_files);
    printf("symlinks=%" PRIu64 "\n", shared.total_symlinks);
    printf("other=%" PRIu64 "\n", shared.total_other);
    printf("total_bytes=%" PRIu64 "\n", shared.total_bytes);
    printf("errors=%" PRIu64 "\n", shared.total_errors);
    printf("elapsed_sec=%.3f\n", t1 - t0);

    output_writer_close(&main_writer);
    if (url) {
        nfs_destroy_url(url);
    }
    if (nfs) {
        nfs_destroy_context(nfs);
    }
    queue_destroy(&queue);
    pthread_mutex_destroy(&shared.stats_mutex);

    return 0;
}

