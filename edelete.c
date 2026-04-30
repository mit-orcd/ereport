/*
 * edelete.c
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Parallel directory walker (same task-queue / donation model as ecrawl) that
 * unlinks non-directory paths (regular files, symlinks, pipes, sockets, etc.)
 * whose atime, mtime, or ctime is at least N full days behind wall-clock now.
 * Traversal uses lstat/fstatat without following symlinks. In delete mode, after
 * the crawl finishes it removes directories that became empty (rmdir only),
 * deepest first, without ascending above the start path or removing "/".
 *
 * Usage:
 *   ./edelete [--delete] [--verbose] <atime|mtime|ctime> <days> <absolute-path>
 *
 * Default is dry-run (counts would_delete, no unlink). Pass --delete to unlink.
 *
 * Thread count: EDELETE_THREADS (default 16, minimum 1).
 * Delete mode: EDELETE_MAX_UNLINK_INFLIGHT caps concurrent unlink(2) calls across all threads
 * (default 256; set to 0 for unlimited).
 *
 * Build:
 *   gcc -O2 -Wall -Wextra -pthread -o edelete edelete.c
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
#include <fcntl.h>
#include <unistd.h>
#include <stdatomic.h>
#include <dirent.h>
#include <limits.h>
#include <time.h>

#ifndef DT_UNKNOWN
#define DT_UNKNOWN 0
#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define DEFAULT_THREADS 16
#define DEFAULT_MAX_UNLINK_INFLIGHT 256
#define WINDOW_SECONDS 10
#define PERF_FLUSH_INTERVAL 1024U
#define LOCAL_STACK_DONATE_FLOOR 8
#define DONATE_CHUNK_MIN 4
#define DONATE_CHUNK_MAX 128
#define DONATE_QUEUE_TARGET_PER_IDLE 4
#define EMFILE_RETRY_LIMIT 8U
#define EMFILE_RETRY_USEC 50000U

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

struct task_node {
    dir_work_t *items;
    size_t count;
    size_t cap;
    struct task_node *next;
};

typedef struct {
    pthread_mutex_t stats_mutex;
    pthread_mutex_t rmdir_list_mutex;
    char **rmdir_parents;
    size_t rmdir_parents_n;
    size_t rmdir_parents_cap;
    uint64_t deleted_files;
    uint64_t would_delete;
    uint64_t removed_empty_dirs;
    uint64_t total_errors;
    uint64_t crawl_threads_started;
    uint64_t donated_dirs;
    uint64_t donation_attempts;
    uint64_t donation_successes;
} shared_state_t;

typedef struct {
    uint64_t entries;
    uint64_t files;
    uint64_t dirs;
} perf_local_t;

typedef struct {
    uint64_t donated_dirs;
    uint64_t donation_attempts;
    uint64_t donation_successes;
} worker_aux_stats_t;

typedef struct {
    shared_state_t *shared;
    task_queue_t *queue;
    uint64_t worker_index;
    perf_local_t perf;
    worker_aux_stats_t aux;
} worker_arg_t;

typedef enum {
    TB_ATIME = 0,
    TB_MTIME = 1,
    TB_CTIME = 2
} time_basis_t;

static atomic_ullong g_queue_depth = 0;
static atomic_int g_active_workers = 0;
static atomic_int g_main_done = 0;
static atomic_ullong g_tasks_popped = 0;
static atomic_ullong g_wait_crawl_tasks = 0;

static atomic_ullong g_total_entries = 0;
static atomic_ullong g_total_dirs = 0;
static atomic_ullong g_total_files = 0;
static atomic_ullong g_window_entries = 0;
static atomic_ullong g_bucket_entries[WINDOW_SECONDS];
static atomic_int g_bucket_index = 0;
static atomic_int g_stop_stats = 0;
static atomic_uint g_seconds_seen = 0;

static int g_verbose = 0;
static int g_dry_run = 1;
static int g_threads = DEFAULT_THREADS;
static int g_max_unlink_inflight = 0;
static pthread_mutex_t g_unlink_gate_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_unlink_gate_cond = PTHREAD_COND_INITIALIZER;
static int g_unlink_inflight = 0;
static time_basis_t g_basis = TB_MTIME;
static int g_age_days = 0;
static time_t g_now = 0;

#define ATOMIC_ADD_RELAXED(obj, value) atomic_fetch_add_explicit((obj), (value), memory_order_relaxed)
#define ATOMIC_SUB_RELAXED(obj, value) atomic_fetch_sub_explicit((obj), (value), memory_order_relaxed)
#define ATOMIC_LOAD_RELAXED(obj) atomic_load_explicit((obj), memory_order_relaxed)

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
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

static int parse_basis(const char *s, time_basis_t *out) {
    if (!s || !out) return -1;
    if (strcmp(s, "atime") == 0) {
        *out = TB_ATIME;
        return 0;
    }
    if (strcmp(s, "mtime") == 0) {
        *out = TB_MTIME;
        return 0;
    }
    if (strcmp(s, "ctime") == 0) {
        *out = TB_CTIME;
        return 0;
    }
    return -1;
}

static int parse_thread_count(void) {
    const char *e = getenv("EDELETE_THREADS");
    long t;
    char *end;

    if (!e || !*e) return DEFAULT_THREADS;
    errno = 0;
    t = strtol(e, &end, 10);
    if (errno || end == e || *end || t < 1 || t > (long)INT_MAX) return DEFAULT_THREADS;
    return (int)t;
}

/* 0 = unlimited; unset uses DEFAULT_MAX_UNLINK_INFLIGHT */
static int parse_max_unlink_inflight(void) {
    const char *e = getenv("EDELETE_MAX_UNLINK_INFLIGHT");
    long v;
    char *end;

    if (!e || !*e) return DEFAULT_MAX_UNLINK_INFLIGHT;
    errno = 0;
    v = strtol(e, &end, 10);
    if (errno || end == e || *end || v < 0 || v > (long)INT_MAX) return DEFAULT_MAX_UNLINK_INFLIGHT;
    if (v == 0) return 0;
    return (int)v;
}

static void unlink_gate_enter(void) {
    if (g_max_unlink_inflight <= 0) return;
    pthread_mutex_lock(&g_unlink_gate_mutex);
    while (g_unlink_inflight >= g_max_unlink_inflight) pthread_cond_wait(&g_unlink_gate_cond, &g_unlink_gate_mutex);
    g_unlink_inflight++;
    pthread_mutex_unlock(&g_unlink_gate_mutex);
}

static void unlink_gate_leave(void) {
    if (g_max_unlink_inflight <= 0) return;
    pthread_mutex_lock(&g_unlink_gate_mutex);
    g_unlink_inflight--;
    pthread_cond_signal(&g_unlink_gate_cond);
    pthread_mutex_unlock(&g_unlink_gate_mutex);
}

static time_t pick_ts(const struct stat *st, time_basis_t b) {
    switch (b) {
        case TB_ATIME:
            return st->st_atime;
        case TB_CTIME:
            return st->st_ctime;
        case TB_MTIME:
        default:
            return st->st_mtime;
    }
}

static int age_eligible_seconds(time_t ts) {
    time_t cutoff;
    if (g_age_days <= 0) return 0;
    if ((time_t)-1 == g_now) return 0;
    cutoff = g_now - (time_t)g_age_days * (time_t)86400;
    return ts <= cutoff;
}

static int path_is_under_root(const char *path, const char *root) {
    size_t lr;

    if (!path || !root) return 0;
    if (strcmp(root, "/") == 0) return strcmp(path, "/") != 0;
    lr = strlen(root);
    if (strcmp(path, root) == 0) return 1;
    if (strncmp(path, root, lr) != 0) return 0;
    return path[lr] == '/';
}

static char *dup_parent_dir(const char *path) {
    const char *slash;
    size_t len;
    char *out;

    slash = strrchr(path, '/');
    if (!slash || slash == path) return strdup("/");
    len = (size_t)(slash - path);
    if (len == 0) return strdup("/");
    out = (char *)malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, path, len);
    out[len] = '\0';
    return out;
}

static int record_deleted_file_parent(shared_state_t *s, const char *parent_dir) {
    char *dup = strdup(parent_dir);
    char **np;
    size_t nc;

    if (!dup) return -1;
    pthread_mutex_lock(&s->rmdir_list_mutex);
    if (s->rmdir_parents_n == s->rmdir_parents_cap) {
        nc = s->rmdir_parents_cap ? s->rmdir_parents_cap * 2 : 64;
        np = (char **)realloc(s->rmdir_parents, nc * sizeof(*np));
        if (!np) {
            pthread_mutex_unlock(&s->rmdir_list_mutex);
            free(dup);
            return -1;
        }
        s->rmdir_parents = np;
        s->rmdir_parents_cap = nc;
    }
    s->rmdir_parents[s->rmdir_parents_n++] = dup;
    pthread_mutex_unlock(&s->rmdir_list_mutex);
    return 0;
}

static int path_slash_count(const char *p) {
    int n = 0;
    for (; *p; p++)
        if (*p == '/') n++;
    return n;
}

static int cmp_parent_path_desc(const void *a, const void *b) {
    const char *pa = *(const char *const *)a;
    const char *pb = *(const char *const *)b;
    int da = path_slash_count(pa);
    int db = path_slash_count(pb);

    if (da != db) return db - da;
    return strcmp(pa, pb);
}

static void stats_add_error(shared_state_t *s) {
    pthread_mutex_lock(&s->stats_mutex);
    s->total_errors++;
    pthread_mutex_unlock(&s->stats_mutex);
}

static void try_rmdir_chain(shared_state_t *shared, const char *root_path, const char *start_dir) {
    char *cur = strdup(start_dir);
    char *next;

    if (!cur) {
        stats_add_error(shared);
        return;
    }

    while (cur && strcmp(cur, "/") != 0) {
        if (!path_is_under_root(cur, root_path)) {
            free(cur);
            return;
        }
        if (rmdir(cur) != 0) {
            if (errno == ENOENT || errno == ENOTEMPTY || errno == EBUSY) {
                free(cur);
                return;
            }
            fprintf(stderr, "edelete: rmdir %s: %s\n", cur, strerror(errno));
            stats_add_error(shared);
            free(cur);
            return;
        }

        pthread_mutex_lock(&shared->stats_mutex);
        shared->removed_empty_dirs++;
        pthread_mutex_unlock(&shared->stats_mutex);

        next = dup_parent_dir(cur);
        free(cur);
        cur = next;
        if (!cur) {
            stats_add_error(shared);
            return;
        }
    }

    free(cur);
}

static void remove_empty_directories_after_delete(shared_state_t *shared, const char *root_path) {
    size_t n, i, w;
    char **list;

    pthread_mutex_lock(&shared->rmdir_list_mutex);
    n = shared->rmdir_parents_n;
    list = shared->rmdir_parents;
    shared->rmdir_parents = NULL;
    shared->rmdir_parents_n = 0;
    shared->rmdir_parents_cap = 0;
    pthread_mutex_unlock(&shared->rmdir_list_mutex);

    if (!list || n == 0) {
        free(list);
        return;
    }

    qsort(list, n, sizeof(*list), cmp_parent_path_desc);

    w = 0;
    for (i = 0; i < n; i++) {
        if (w > 0 && strcmp(list[i], list[w - 1]) == 0) {
            free(list[i]);
            continue;
        }
        list[w++] = list[i];
    }
    n = w;

    for (i = 0; i < n; i++) try_rmdir_chain(shared, root_path, list[i]);

    for (i = 0; i < n; i++) free(list[i]);
    free(list);
}

static void stats_add_started(shared_state_t *s) {
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
    int idx;

    if (!perf || perf->entries == 0) return;

    idx = (int)ATOMIC_LOAD_RELAXED(&g_bucket_index);
    ATOMIC_ADD_RELAXED(&g_total_entries, perf->entries);
    ATOMIC_ADD_RELAXED(&g_window_entries, perf->entries);
    ATOMIC_ADD_RELAXED(&g_bucket_entries[idx], perf->entries);

    if (perf->dirs > 0) ATOMIC_ADD_RELAXED(&g_total_dirs, perf->dirs);
    if (perf->files > 0) ATOMIC_ADD_RELAXED(&g_total_files, perf->files);

    memset(perf, 0, sizeof(*perf));
}

static int join_path_fast(const char *base, size_t base_len, const char *name, size_t name_len, char *out,
                          size_t out_sz) {
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

static int join_path_alloc(const char *base, size_t base_len, const char *name, size_t name_len, char **out_path,
                           size_t *out_len) {
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

static void emfile_retry_pause(unsigned attempt) {
    useconds_t delay = EMFILE_RETRY_USEC;

    if (attempt < 4U) delay *= (useconds_t)(attempt + 1U);
    usleep(delay);
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
        size_t i;
        for (i = 0; i < task.count; i++) free(task.items[i].path);
        free(task.items);
        next = cur->next;
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
        atomic_fetch_add_explicit(&g_wait_crawl_tasks, 1ULL, memory_order_relaxed);
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
    int started = (int)shared->crawl_threads_started;
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

static void account_leaf(perf_local_t *perf, const struct stat *st) {
    perf->entries++;
    if (S_ISDIR(st->st_mode))
        perf->dirs++;
    else
        perf->files++;

    if (perf->entries >= PERF_FLUSH_INTERVAL) perf_flush_local(perf);
}

/* Unlink any non-directory; symlinks and special files are included. Directories use opendir, not unlink here.
 * Returns 1 if unlinked, 0 if skipped or dry-run eligible, -1 on unlink error. */
static int try_delete_nondir(shared_state_t *shared, const char *path, const struct stat *st) {
    time_t ts;

    if (S_ISDIR(st->st_mode)) return 0;

    ts = pick_ts(st, g_basis);
    if (!age_eligible_seconds(ts)) return 0;

    if (g_dry_run) {
        pthread_mutex_lock(&shared->stats_mutex);
        shared->would_delete++;
        pthread_mutex_unlock(&shared->stats_mutex);
        return 0;
    }

    unlink_gate_enter();
    if (unlink(path) != 0) {
        fprintf(stderr, "edelete: unlink %s: %s\n", path, strerror(errno));
        stats_add_error(shared);
        unlink_gate_leave();
        return -1;
    }
    unlink_gate_leave();

    pthread_mutex_lock(&shared->stats_mutex);
    shared->deleted_files++;
    pthread_mutex_unlock(&shared->stats_mutex);
    return 1;
}

static int process_directory_iterative(dir_stack_t *stack, shared_state_t *shared, perf_local_t *perf,
                                       worker_aux_stats_t *aux, task_queue_t *queue) {
    while (stack->count > 0) {
        dir_work_t work;
        char *dir_path;
        size_t dir_path_len;
        struct stat st;
        DIR *dir = NULL;
        struct dirent *ent;

        if (dir_stack_pop(stack, &work) != 0) break;
        dir_path = work.path;

        if (work.have_stat)
            st = work.st;
        else {
            memset(&st, 0, sizeof(st));
            if (lstat(dir_path, &st) != 0) {
                fprintf(stderr, "edelete: lstat %s: %s\n", dir_path, strerror(errno));
                stats_add_error(shared);
                free(dir_path);
                continue;
            }
        }

        account_leaf(perf, &st);
        dir_path_len = work.path_len;

        if (!S_ISDIR(st.st_mode)) {
            if (try_delete_nondir(shared, dir_path, &st) == 1) {
                char *par = dup_parent_dir(dir_path);
                if (!par)
                    stats_add_error(shared);
                else {
                    if (record_deleted_file_parent(shared, par) != 0) stats_add_error(shared);
                    free(par);
                }
            }
            free(dir_path);
            continue;
        }

        {
            unsigned retry;
            for (retry = 0; retry <= EMFILE_RETRY_LIMIT; retry++) {
                dir = opendir(dir_path);
                if (dir || errno != EMFILE || retry == EMFILE_RETRY_LIMIT) break;
                emfile_retry_pause(retry);
            }
        }
        if (!dir) {
            fprintf(stderr, "edelete: opendir %s: %s\n", dir_path, strerror(errno));
            stats_add_error(shared);
            free(dir_path);
            continue;
        }

        {
            int dir_fd = dirfd(dir);
            if (dir_fd < 0) {
                fprintf(stderr, "edelete: dirfd %s: %s\n", dir_path, strerror(errno));
                stats_add_error(shared);
                closedir(dir);
                free(dir_path);
                continue;
            }

            while ((ent = readdir(dir)) != NULL) {
                size_t child_name_len;
                struct stat child_st;
#if defined(_DIRENT_HAVE_D_TYPE) && defined(DT_DIR) && defined(DT_UNKNOWN)
                unsigned char child_d_type = ent->d_type;
#else
                unsigned char child_d_type = DT_UNKNOWN;
#endif

                if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;

                child_name_len = strlen(ent->d_name);
                if (child_d_type == DT_DIR) {
                    char *child_path_owned;
                    size_t child_path_len;

                    if (join_path_alloc(dir_path, dir_path_len, ent->d_name, child_name_len, &child_path_owned,
                                        &child_path_len) != 0) {
                        fprintf(stderr, "edelete: path alloc %s/%s\n", dir_path, ent->d_name);
                        stats_add_error(shared);
                        continue;
                    }
                    if (dir_stack_push_take(stack, child_path_owned, child_path_len, NULL) != 0) {
                        fprintf(stderr, "edelete: stack push %s\n", child_path_owned);
                        free(child_path_owned);
                        stats_add_error(shared);
                        continue;
                    }
                    while (should_donate_work(shared, stack)) {
                        if (donate_stack_chunk(stack, queue, aux) != 0) {
                            stats_add_error(shared);
                            break;
                        }
                    }
                } else {
                    if (fstatat(dir_fd, ent->d_name, &child_st, AT_SYMLINK_NOFOLLOW) != 0) {
                        fprintf(stderr, "edelete: fstatat %s/%s: %s\n", dir_path, ent->d_name, strerror(errno));
                        stats_add_error(shared);
                        continue;
                    }
                    if (S_ISDIR(child_st.st_mode)) {
                        char *child_path_owned;
                        size_t child_path_len;

                        if (join_path_alloc(dir_path, dir_path_len, ent->d_name, child_name_len, &child_path_owned,
                                            &child_path_len) != 0) {
                            fprintf(stderr, "edelete: path alloc %s/%s\n", dir_path, ent->d_name);
                            stats_add_error(shared);
                            continue;
                        }
                        if (dir_stack_push_take(stack, child_path_owned, child_path_len, &child_st) != 0) {
                            fprintf(stderr, "edelete: stack push %s\n", child_path_owned);
                            free(child_path_owned);
                            stats_add_error(shared);
                            continue;
                        }
                        while (should_donate_work(shared, stack)) {
                            if (donate_stack_chunk(stack, queue, aux) != 0) {
                                stats_add_error(shared);
                                break;
                            }
                        }
                    } else {
                        char child[PATH_MAX];

                        account_leaf(perf, &child_st);

                        if (join_path_fast(dir_path, dir_path_len, ent->d_name, child_name_len, child, sizeof(child)) !=
                            0) {
                            fprintf(stderr, "edelete: path too long %s/%s\n", dir_path, ent->d_name);
                            stats_add_error(shared);
                            continue;
                        }
                        if (try_delete_nondir(shared, child, &child_st) == 1) {
                            if (record_deleted_file_parent(shared, dir_path) != 0) stats_add_error(shared);
                        }
                    }
                }
            }
        }

        closedir(dir);
        free(dir_path);
    }

    return 0;
}

static void *worker_thread_main(void *arg_void) {
    worker_arg_t *arg = (worker_arg_t *)arg_void;

    (void)arg->worker_index;

    for (;;) {
        dir_stack_t task;

        if (queue_pop_wait(arg->queue, &task) != 0) break;

        process_directory_iterative(&task, arg->shared, &arg->perf, &arg->aux, arg->queue);
        atomic_fetch_sub(&g_active_workers, 1);

        if (atomic_load(&g_main_done) && atomic_load(&g_active_workers) == 0) {
            pthread_mutex_lock(&arg->queue->mutex);
            pthread_cond_broadcast(&arg->queue->cond);
            pthread_mutex_unlock(&arg->queue->mutex);
        }

        dir_stack_destroy(&task);
    }

    perf_flush_local(&arg->perf);
    return NULL;
}

static int enqueue_root_task(const char *path, shared_state_t *shared, task_queue_t *queue) {
    dir_stack_t task;
    struct stat st;
    char *dup;
    size_t path_len;

    dir_stack_init(&task);
    memset(&st, 0, sizeof(st));
    if (lstat(path, &st) != 0) {
        fprintf(stderr, "edelete: lstat %s: %s\n", path, strerror(errno));
        stats_add_error(shared);
        return -1;
    }

    dup = strdup(path);
    if (!dup) {
        fprintf(stderr, "edelete: strdup failed\n");
        stats_add_error(shared);
        return -1;
    }

    path_len = strlen(path);
    if (dir_stack_push_take(&task, dup, path_len, &st) != 0) {
        fprintf(stderr, "edelete: stack push root\n");
        free(dup);
        stats_add_error(shared);
        return -1;
    }

    if (queue_push_stack_take(queue, &task) != 0) {
        fprintf(stderr, "edelete: enqueue root failed\n");
        dir_stack_destroy(&task);
        stats_add_error(shared);
        return -1;
    }

    return 0;
}

static void *stats_thread_main(void *arg) {
    double *run_start_ptr = (double *)arg;

    while (!atomic_load(&g_stop_stats)) {
        sleep(1);

        {
            int next = (atomic_load(&g_bucket_index) + 1) % WINDOW_SECONDS;
            unsigned long long expired = atomic_exchange(&g_bucket_entries[next], 0);
            atomic_fetch_sub(&g_window_entries, expired);
            atomic_store(&g_bucket_index, next);
        }

        {
            unsigned int seen = atomic_load(&g_seconds_seen);
            if (seen < WINDOW_SECONDS) atomic_store(&g_seconds_seen, seen + 1U);
        }

        {
            unsigned long long total_entries = atomic_load(&g_total_entries);
            unsigned long long window_entries = atomic_load(&g_window_entries);
            unsigned int divisor = atomic_load(&g_seconds_seen);
            double ops_rate;
            double elapsed_sec = now_sec() - *run_start_ptr;
            char te[32], re[32], elapsed_buf[32];

            if (divisor == 0) divisor = 1;
            ops_rate = (double)window_entries / (double)divisor;
            human_decimal((double)total_entries, te, sizeof(te));
            human_decimal(ops_rate, re, sizeof(re));
            format_duration(elapsed_sec, elapsed_buf, sizeof(elapsed_buf));

            printf("\r%s entries/s(10s) | scanned:%s | el:%s            ", re, te, elapsed_buf);
            fflush(stdout);
        }
    }

    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--delete] [--verbose] <atime|mtime|ctime> <days> <absolute-path>\n"
            "  Walks in parallel without following symlinks; by default dry-run (counts would_delete, no unlink).\n"
            "  --delete: unlink matching non-directory entries (any type), then rmdir empty dirs (under start path).\n"
            "  Thread count: EDELETE_THREADS (default %d).\n"
            "  Max concurrent unlinks (all threads): EDELETE_MAX_UNLINK_INFLIGHT (default %d; 0 = unlimited).\n",
            prog, DEFAULT_THREADS, DEFAULT_MAX_UNLINK_INFLIGHT);
}

int main(int argc, char **argv) {
    shared_state_t shared;
    task_queue_t queue;
    pthread_t *workers = NULL;
    worker_arg_t *worker_args = NULL;
    pthread_t stats_thread;
    double t0, t1;
    double run_start = 0.0;
    int worker_count_started = 0;
    int i;
    int ai = 1;
    const char *basis_str = NULL;
    const char *days_str = NULL;
    const char *root_path = NULL;

    while (ai < argc && argv[ai][0] == '-') {
        if (strcmp(argv[ai], "--delete") == 0) {
            g_dry_run = 0;
            ai++;
            continue;
        }
        if (strcmp(argv[ai], "--verbose") == 0) {
            g_verbose = 1;
            ai++;
            continue;
        }
        if (strcmp(argv[ai], "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
        fprintf(stderr, "edelete: unknown option %s\n", argv[ai]);
        usage(argv[0]);
        return 2;
    }

    if (argc - ai < 3) {
        usage(argv[0]);
        return 2;
    }

    basis_str = argv[ai++];
    days_str = argv[ai++];
    root_path = argv[ai++];

    if (ai != argc) {
        fprintf(stderr, "edelete: extra arguments after start path\n");
        usage(argv[0]);
        return 2;
    }

    if (parse_basis(basis_str, &g_basis) != 0) {
        fprintf(stderr, "edelete: time basis must be atime, mtime, or ctime\n");
        return 2;
    }

    {
        long d;
        char *end = NULL;
        errno = 0;
        d = strtol(days_str, &end, 10);
        if (errno || !end || *end || d < 1 || d > 365000L) {
            fprintf(stderr, "edelete: days must be an integer in [1, 365000]\n");
            return 2;
        }
        g_age_days = (int)d;
    }

    if (root_path[0] != '/') {
        fprintf(stderr, "edelete: path must be absolute\n");
        return 2;
    }

    g_now = time(NULL);
    if (g_now == (time_t)-1) {
        fprintf(stderr, "edelete: time() failed\n");
        return 1;
    }

    g_threads = parse_thread_count();
    g_max_unlink_inflight = parse_max_unlink_inflight();

    memset(&shared, 0, sizeof(shared));
    pthread_mutex_init(&shared.stats_mutex, NULL);
    pthread_mutex_init(&shared.rmdir_list_mutex, NULL);
    queue_init(&queue);

    for (i = 0; i < WINDOW_SECONDS; i++) atomic_store(&g_bucket_entries[i], 0);
    atomic_store(&g_total_entries, 0);
    atomic_store(&g_total_dirs, 0);
    atomic_store(&g_total_files, 0);
    atomic_store(&g_window_entries, 0);
    atomic_store(&g_bucket_index, 0);
    atomic_store(&g_stop_stats, 0);
    atomic_store(&g_seconds_seen, 0);
    atomic_store(&g_queue_depth, 0);
    atomic_store(&g_active_workers, 0);
    atomic_store(&g_main_done, 0);
    atomic_store(&g_tasks_popped, 0);
    atomic_store(&g_wait_crawl_tasks, 0);

    t0 = now_sec();
    run_start = t0;

    workers = (pthread_t *)calloc((size_t)g_threads, sizeof(*workers));
    worker_args = (worker_arg_t *)calloc((size_t)g_threads, sizeof(*worker_args));
    if (!workers || !worker_args) {
        fprintf(stderr, "edelete: allocation failed\n");
        free(workers);
        free(worker_args);
        queue_destroy(&queue);
        pthread_mutex_destroy(&shared.rmdir_list_mutex);
        pthread_mutex_destroy(&shared.stats_mutex);
        return 1;
    }

    for (i = 0; i < g_threads; i++) {
        worker_args[i].shared = &shared;
        worker_args[i].queue = &queue;
        worker_args[i].worker_index = (uint64_t)(i + 1);
        memset(&worker_args[i].perf, 0, sizeof(worker_args[i].perf));
        memset(&worker_args[i].aux, 0, sizeof(worker_args[i].aux));

        if (pthread_create(&workers[i], NULL, worker_thread_main, &worker_args[i]) != 0) {
            fprintf(stderr, "edelete: pthread_create failed\n");
            stats_add_error(&shared);
            break;
        }
        worker_count_started++;
        stats_add_started(&shared);
    }

    if (worker_count_started == 0) {
        fprintf(stderr, "edelete: no worker threads started\n");
        free(workers);
        free(worker_args);
        queue_destroy(&queue);
        pthread_mutex_destroy(&shared.rmdir_list_mutex);
        pthread_mutex_destroy(&shared.stats_mutex);
        return 1;
    }

    if (pthread_create(&stats_thread, NULL, stats_thread_main, &run_start) != 0) {
        fprintf(stderr, "edelete: stats thread failed\n");
        pthread_mutex_lock(&queue.mutex);
        queue.closed = 1;
        pthread_cond_broadcast(&queue.cond);
        pthread_mutex_unlock(&queue.mutex);
        for (i = 0; i < worker_count_started; i++) pthread_join(workers[i], NULL);
        free(workers);
        free(worker_args);
        queue_destroy(&queue);
        pthread_mutex_destroy(&shared.rmdir_list_mutex);
        pthread_mutex_destroy(&shared.stats_mutex);
        return 1;
    }

    if (enqueue_root_task(root_path, &shared, &queue) != 0) {
        atomic_store(&g_stop_stats, 1);
        pthread_mutex_lock(&queue.mutex);
        queue.closed = 1;
        pthread_cond_broadcast(&queue.cond);
        pthread_mutex_unlock(&queue.mutex);
        pthread_join(stats_thread, NULL);
        clear_status_line();
        for (i = 0; i < worker_count_started; i++) pthread_join(workers[i], NULL);
        free(workers);
        free(worker_args);
        queue_destroy(&queue);
        pthread_mutex_destroy(&shared.rmdir_list_mutex);
        pthread_mutex_destroy(&shared.stats_mutex);
        return 1;
    }

    atomic_store(&g_main_done, 1);
    pthread_mutex_lock(&queue.mutex);
    pthread_cond_broadcast(&queue.cond);
    pthread_mutex_unlock(&queue.mutex);

    for (i = 0; i < worker_count_started; i++) pthread_join(workers[i], NULL);

    pthread_mutex_lock(&queue.mutex);
    queue.closed = 1;
    pthread_cond_broadcast(&queue.cond);
    pthread_mutex_unlock(&queue.mutex);

    for (i = 0; i < worker_count_started; i++) stats_merge_aux(&shared, &worker_args[i].aux);

    if (!g_dry_run) remove_empty_directories_after_delete(&shared, root_path);

    atomic_store(&g_stop_stats, 1);
    pthread_join(stats_thread, NULL);
    clear_status_line();

    t1 = now_sec();

    free(workers);
    free(worker_args);
    queue_destroy(&queue);

    {
        double elapsed = t1 - t0;
        double avg_ops =
            elapsed > 0.0 ? (double)atomic_load(&g_total_entries) / elapsed : 0.0;
        char avg_ops_buf[32];

        human_decimal(avg_ops, avg_ops_buf, sizeof(avg_ops_buf));
        printf("basis=%s\n", basis_str);
        printf("age_days=%d\n", g_age_days);
        printf("mode=%s\n", g_dry_run ? "dry-run" : "delete");
        printf("start_path=%s\n", root_path);
        printf("threads=%d\n", worker_count_started);
        printf("max_unlink_inflight=%d\n", g_max_unlink_inflight);
        printf("entries_scanned=%" PRIu64 "\n", (uint64_t)atomic_load(&g_total_entries));
        printf("dirs_seen=%" PRIu64 "\n", (uint64_t)atomic_load(&g_total_dirs));
        printf("files_seen=%" PRIu64 "\n", (uint64_t)atomic_load(&g_total_files));
        printf("deleted_files=%" PRIu64 "\n", shared.deleted_files);
        printf("removed_empty_dirs=%" PRIu64 "\n", shared.removed_empty_dirs);
        printf("would_delete=%" PRIu64 "\n", shared.would_delete);
        printf("errors=%" PRIu64 "\n", shared.total_errors);
        printf("elapsed_sec=%.3f\n", elapsed);
        printf("avg_entries_per_sec=%s\n", avg_ops_buf);
        printf("tasks_popped=%" PRIu64 "\n", (uint64_t)atomic_load(&g_tasks_popped));
        printf("wait_crawl_tasks=%" PRIu64 "\n", (uint64_t)atomic_load(&g_wait_crawl_tasks));
        printf("donated_dirs=%" PRIu64 "\n", shared.donated_dirs);
        printf("donation_attempts=%" PRIu64 "\n", shared.donation_attempts);
        printf("donation_successes=%" PRIu64 "\n", shared.donation_successes);
    }

    pthread_mutex_destroy(&shared.rmdir_list_mutex);
    pthread_mutex_destroy(&shared.stats_mutex);
    return shared.total_errors ? 1 : 0;
}
