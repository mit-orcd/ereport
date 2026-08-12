/*
 * ewalkbench — standalone directory-walk benchmark for inode access strategy.
 *
 * Walks a tree and stats every non-directory entry, and does nothing else: no
 * capture files, no records, no env knobs. The only variables are traversal
 * order and within-directory stat order, so a cold-cache timing difference is
 * attributable to strategy rather than to the rest of a crawler.
 *
 * The baseline (--order dfs --stat hash) replicates ecrawl's syscall pattern:
 * raw getdents64 with a 1 MiB buffer, d_type shortcutting so only
 * non-directories are stat'd, fstatat(AT_SYMLINK_NOFOLLOW) on the parent
 * dirfd, batches of 1024 names, a LIFO local stack plus a global queue.
 *
 * Matching the syscalls is not enough on its own: the concurrency has to match
 * too. Statting a batch on the thread that read the directory makes a whole
 * directory the unit of parallelism, so one large directory is walked by one
 * thread whatever --threads says — measured as cpu_util 0.70 against 16
 * threads on a 1M-file directory, a queue depth of about 1 that describes the
 * bench rather than the strategy under test. ecrawl did not work that way: it
 * handed every mid-directory batch to a stat pool (8 threads by default) and
 * stat'd only end-of-directory tails inline, until measurement showed the
 * pool rarely won and it was removed. --stat-threads still defaults to 8 here
 * so that walker remains reproducible. --stat-threads 0 selects the old
 * fully-inline path and is how the 18-row dataset in
 * logs/ewalk-strategy-manual-567654/ was produced, so those results stay
 * reproducible rather than merely remembered.
 *
 * Discovered directories reach the global queue in batches of
 * --dir-enqueue-batch (ecrawl's ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH default of
 * 48), for the same reason the stat pool exists: a benchmark that enqueues
 * differently from the tool it models measures itself. Publishing one
 * directory per lock made the 200,000-directory shape bimodal — 0.395 s or
 * 1.100 s, decided by whether the producer stayed ahead of the consumers —
 * which is a distribution no strategy difference could have been read
 * through. --dir-enqueue-batch 1 restores that form for comparison.
 *
 * --no-stat and --no-locality exist so the walk can be compared against tools
 * that do less: --no-stat is ecrawl's own names-only mode, which is the only
 * fair comparison against a traversal-only tool like fd, and --no-locality
 * drops the per-stat bookkeeping below, which ecrawl has no equivalent of and
 * which therefore sits as a tax on any ewalkbench-vs-ecrawl comparison.
 *
 * --no-dtype goes the other way and makes the walk do more: it ignores d_type
 * and classifies every entry by its inode, which is what a filesystem that
 * does not supply dirent ftype (some NFS configurations) forces on any walker.
 * Every tree measured here is on XFS, which always populates d_type, so
 * without this flag the classify-by-stat path is unreachable and its cost is
 * unmeasured. The stat count rises by exactly one per directory below the
 * root, and every subdirectory is then discovered from inside a stat batch
 * rather than from the read loop.
 *
 * --sort-window widens that 1024-name batch, up to the whole directory. It is
 * opt-in and defaults to 1024 precisely because 1024 is the window ecrawl's
 * removed stat pool used; the wider window exists to tell "inode
 * ordering does not help here" apart from "the batch is too small to express
 * inode ordering in this directory". See the comment above walk_dir_batched.
 *
 * --stat-call changes the syscall rather than the order. fstatat is what
 * ecrawl issues and the default; statx is what dut issues, and on this system
 * the two are genuinely different syscalls, since glibc 2.28 routes fstatat to
 * newfstatat. The hypothesis it exists to falsify is that STATX_BASIC_STATS
 * buys nothing over newfstatat, because it names the same fields and both end
 * in vfs_getattr, and that a win needs a narrowed mask on a filesystem that
 * acts on one — for NFS, a mask the client can answer out of its attribute
 * cache without a GETATTR round trip, which is what statx-min asks for. Worth
 * saying where the flag lives rather than discovering it later: ecrawl reports
 * bytes, so the production crawl cannot drop STATX_SIZE and cannot collect
 * that win however large it measures here.
 *
 * Alongside the timing it reports inode-locality counters, so a run can show
 * that the requested strategy actually changed inode access rather than being
 * assumed to have done so. They are on by default and --no-locality turns
 * them off; the keys they feed are then reported as unavailable rather than
 * as zeros.
 *
 * Summary goes to stdout as key=value lines (the convention the other tools
 * here use); diagnostics go to stderr.
 *
 * Exit: 0 walk completed, 1 fatal error, 2 usage.
 */

#define _GNU_SOURCE

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* Same read-buffer size and stat batch size ecrawl defaults to, so the
 * baseline cell issues the same number of syscalls of the same shape. */
#define GETDENTS_BUF_BYTES (1024 * 1024)
#define STAT_BATCH_NAMES   1024

/* --sort-window ceiling. A window holds one nent_t (24 bytes) per name plus
 * the name bytes themselves, so 2^21 names is roughly 50 MB of entries plus
 * ~40 MB of names for 20-byte names — about 90 MB for one batch buffer that
 * meets a directory that large, and nothing at all for one that does not.
 * With the stat pool on that buffer is a pool slot rather than a worker, and
 * a slot returned to the pool keeps its arena, so the ceiling multiplies by
 * the batchpool bound (walk threads + queue depth + stat threads) and not by
 * --threads.
 * `--sort-window all` resolves to this number and the resolved value is echoed
 * as stat_sort_window, so a capped window is visible in the results rather
 * than silently applied. */
#define STAT_SORT_WINDOW_CAP (1u << 21)

/* --stat spread: how many directories a worker keeps open at once. */
#define SPREAD_SLOTS 8

/* --stat-threads: depth of the batch queue between readers and stat threads,
 * and the tail-batch size below which a batch is stat'd inline instead. Both
 * were ecrawl's defaults before its stat pool was removed, so a cell with the
 * pool on differed from ecrawl in strategy and not in how work is split.
 * Queue-full means stat inline, which is also what ecrawl's fallback did.
 *
 * The offload threshold was laddered on XFS (job 19684544 cold / 19678135 warm)
 * against a 200,000×5 many_dirs tree, a 200×5000 few_dirs tree and a 1M-file
 * one_dir tree. At 32, many_dirs offloads 0 of 200,000 batches — every directory
 * is smaller than the gate, so the pool never runs. At 0 the pool engages
 * (~87k–200k offloaded) but many_dirs regresses ~40–50% (warm median 0.308 s →
 * 0.471 s at 8 stat threads), because the queue handoff costs more than an
 * inline fstatat on a five-name batch. So 32 stays: it matches what ecrawl's
 * pool did, and zeroing it to "engage the pool" is a measured loss, not a
 * free fix. */
#define STAT_QUEUE_BATCHES       256
#define DEFAULT_STAT_MIN_OFFLOAD 32
/* ecrawl's removed stat-pool default, and therefore this one: a benchmark
 * that stands in for that walker has to have its stat concurrency, or its wall
 * time describes a walker nobody runs. The same ladder found no default that
 * beats 8 on both one_dir and many_dirs: cold one_dir stays ~5 s from 8 to 128
 * (the single getdents64 reader is the bottleneck, not the pool depth), and
 * many_dirs under the 32-name gate never reaches the pool at all. --stat-threads
 * 0 selects the older fully-inline path. */
#define DEFAULT_STAT_THREADS 8

/* A DFS worker hands the shallow half of its local stack to the global queue
 * once the stack is at least this deep, which is how the other workers get
 * work without giving up depth-first order locally. */
#define DONATE_AT 16

/* --dir-enqueue-batch: how many discovered directories a worker publishes per
 * acquisition of the global queue lock. ecrawl's
 * ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH default, and therefore this one: a
 * benchmark that enqueues differently from the tool it models is measuring
 * itself. 1 reproduces the one-directory-per-lock form this file had before
 * the batch existed, which is what makes the cost of that form measurable
 * rather than remembered. */
#define DEFAULT_DIR_ENQUEUE_BATCH 48
#define DIR_ENQUEUE_BATCH_MAX     4096

/* chunk_reuse_rate: an XFS inode chunk is 64 inodes, so ino>>6 names the chunk
 * one inode read pulls in. "Recently" is the last CHUNK_WINDOW stats on the
 * same thread — a short window on purpose, since with a long one every order
 * eventually revisits every chunk and the counter stops separating them. */
#define CHUNK_SHIFT  6
#define CHUNK_WINDOW 16

/* distinct_ino_buckets: coarse ino>>20 histogram. Filesystem-agnostic proxy
 * for spread; on XFS the allocation group is derivable from the inode number,
 * so this shows whether the frontier really fanned across AGs. */
#define BUCKET_SHIFT 20

/* median_ino_delta is taken from a bounded reservoir sample rather than every
 * delta, so memory stays flat on a 1M-file tree. */
#define DELTA_SAMPLE_CAP (1u << 16)

enum walk_order { ORDER_DFS = 0, ORDER_BFS = 1 };
enum stat_order { STAT_HASH = 0, STAT_INO = 1, STAT_SPREAD = 2 };

/* --stat-call: which syscall reads the inode. fstatat is what ecrawl does and
 * what every result before this dimension existed was measured with; statx is
 * byte-for-byte what dut does. They are genuinely different syscalls here —
 * glibc 2.28 routes fstatat to newfstatat — so the two are comparable rather
 * than two spellings of one thing. */
enum stat_call { CALL_FSTATAT = 0, CALL_STATX = 1, CALL_STATX_MIN = 2, CALL_STATX_NOSYNC = 3 };

#ifdef STATX_BASIC_STATS
#define HAVE_STATX 1
/* Everything ecrawl reads from an inode except size, blocks and the
 * timestamps. Those are precisely the fields an NFS client cannot produce
 * without a GETATTR round trip, so this mask is the only one of the three that
 * a filesystem can act on; the others name what newfstatat already returns. */
#define STATX_MIN_MASK (STATX_TYPE | STATX_MODE | STATX_INO | STATX_NLINK | STATX_UID | STATX_GID)
#endif

struct linux_dirent64 {
    uint64_t       d_ino;
    int64_t        d_off;
    unsigned short d_reclen;
    unsigned char  d_type;
    char           d_name[];
};

/* ----- owned path items ------------------------------------------------- */

typedef struct {
    char  *path;
    size_t len;
} dirtask_t;

/* ----- deque of directory tasks (ring buffer, both ends) ---------------- */

typedef struct {
    dirtask_t *items;
    size_t     cap;
    size_t     head;
    size_t     count;
} taskdeq_t;

static int taskdeq_grow(taskdeq_t *d) {
    size_t new_cap = (d->cap == 0) ? 64 : d->cap * 2;
    dirtask_t *items = (dirtask_t *)malloc(new_cap * sizeof(*items));
    size_t i;

    if (!items) return -1;
    for (i = 0; i < d->count; i++)
        items[i] = d->items[(d->head + i) % d->cap];
    free(d->items);
    d->items = items;
    d->cap = new_cap;
    d->head = 0;
    return 0;
}

static int taskdeq_push_back(taskdeq_t *d, dirtask_t t) {
    if (d->count == d->cap && taskdeq_grow(d) != 0) return -1;
    d->items[(d->head + d->count) % d->cap] = t;
    d->count++;
    return 0;
}

static int taskdeq_push_front(taskdeq_t *d, dirtask_t t) {
    if (d->count == d->cap && taskdeq_grow(d) != 0) return -1;
    d->head = (d->head + d->cap - 1) % d->cap;
    d->items[d->head] = t;
    d->count++;
    return 0;
}

static int taskdeq_pop_front(taskdeq_t *d, dirtask_t *out) {
    if (d->count == 0) return -1;
    *out = d->items[d->head];
    d->head = (d->head + 1) % d->cap;
    d->count--;
    return 0;
}

static int taskdeq_pop_back(taskdeq_t *d, dirtask_t *out) {
    if (d->count == 0) return -1;
    d->count--;
    *out = d->items[(d->head + d->count) % d->cap];
    return 0;
}

static void taskdeq_destroy(taskdeq_t *d) {
    dirtask_t t;

    while (taskdeq_pop_front(d, &t) == 0)
        free(t.path);
    free(d->items);
    d->items = NULL;
    d->cap = d->head = d->count = 0;
}

/* ----- global work queue ------------------------------------------------ */

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    taskdeq_t       q;
    int             nthreads;
    int             nwaiting;
    int             done;
    int             oom;
    /* Batches handed to the stat pool and not yet finished. A batch still
     * being stat'd can classify a DT_UNKNOWN entry as a directory and push it,
     * so it is outstanding work even though no walk thread is holding it. */
    uint64_t        inflight;
    uint64_t        max_inflight;
} workq_t;

static int workq_init(workq_t *wq, int nthreads) {
    memset(wq, 0, sizeof(*wq));
    if (pthread_mutex_init(&wq->mu, NULL) != 0) return -1;
    if (pthread_cond_init(&wq->cv, NULL) != 0) {
        pthread_mutex_destroy(&wq->mu);
        return -1;
    }
    wq->nthreads = nthreads;
    return 0;
}

static void workq_destroy(workq_t *wq) {
    taskdeq_destroy(&wq->q);
    pthread_cond_destroy(&wq->cv);
    pthread_mutex_destroy(&wq->mu);
}

/* front = LIFO end (depth-first), back = FIFO end (breadth-first). */
static int workq_push(workq_t *wq, dirtask_t t, int to_front) {
    int rc;

    pthread_mutex_lock(&wq->mu);
    rc = to_front ? taskdeq_push_front(&wq->q, t) : taskdeq_push_back(&wq->q, t);
    if (rc != 0)
        wq->oom = 1;
    else
        pthread_cond_signal(&wq->cv);
    pthread_mutex_unlock(&wq->mu);
    return rc;
}

/*
 * Move up to n tasks from a worker-private deque into the global queue, taking
 * wq->mu once for all of them.
 *
 * Measured on many_dirs (200,000 directories, 5 files each, 16 threads, warm
 * node-local XFS): the one-push-per-directory form this replaces spent the
 * walk parking and waking on wq->cv — 426,000 voluntary context switches per
 * run, about two per directory — and the run came out bimodal, 0.395 s or
 * 1.100 s depending on whether the producer happened to stay ahead of the
 * consumers. Publishing a group leaves the queue non-empty for long enough
 * that a consumer usually finds work without sleeping for it.
 *
 * from_back and to_front are the caller's, so the queue ends up in the order a
 * run of single pushes would have left it in: batching changes how often the
 * lock is taken and nothing else about the traversal.
 *
 * One broadcast rather than one signal per task. A signal per task would put
 * the wakeups back, and the herd is bounded by the number of walk threads.
 */
static int workq_push_take(workq_t *wq, taskdeq_t *src, size_t n, int from_back, int to_front) {
    size_t moved = 0;
    int rc = 0;

    if (n == 0) return 0;
    pthread_mutex_lock(&wq->mu);
    while (moved < n) {
        dirtask_t t;

        if ((from_back ? taskdeq_pop_back(src, &t) : taskdeq_pop_front(src, &t)) != 0)
            break;
        if ((to_front ? taskdeq_push_front(&wq->q, t) : taskdeq_push_back(&wq->q, t)) != 0) {
            free(t.path);
            wq->oom = 1;
            rc = -1;
            break;
        }
        moved++;
    }
    if (moved == 1)
        pthread_cond_signal(&wq->cv);
    else if (moved > 1)
        pthread_cond_broadcast(&wq->cv);
    pthread_mutex_unlock(&wq->mu);
    return rc;
}

/* 0 = got work, -1 = queue empty right now (caller still has its own work). */
static int workq_trypop(workq_t *wq, dirtask_t *out) {
    int rc;

    pthread_mutex_lock(&wq->mu);
    rc = taskdeq_pop_front(&wq->q, out);
    pthread_mutex_unlock(&wq->mu);
    return rc;
}

/* Called when a batch is accepted by the stat pool and when it is finished.
 * Only walk threads count towards nwaiting, so inflight is the one other thing
 * that can still produce work. */
static void workq_batch_begin(workq_t *wq) {
    pthread_mutex_lock(&wq->mu);
    wq->inflight++;
    if (wq->inflight > wq->max_inflight)
        wq->max_inflight = wq->inflight;
    pthread_mutex_unlock(&wq->mu);
}

static void workq_batch_done(workq_t *wq) {
    pthread_mutex_lock(&wq->mu);
    wq->inflight--;
    if (wq->inflight == 0)
        pthread_cond_broadcast(&wq->cv);
    pthread_mutex_unlock(&wq->mu);
}

/* Allocation failure anywhere: release everyone parked in workq_pop rather
 * than letting them wait for work that will never be produced. */
static void workq_fail(workq_t *wq) {
    pthread_mutex_lock(&wq->mu);
    wq->done = 1;
    pthread_cond_broadcast(&wq->cv);
    pthread_mutex_unlock(&wq->mu);
}

/*
 * Blocks until work arrives or the walk is over. A worker only calls this with
 * nothing of its own left to do, so "every walk thread is in here, the queue is
 * empty, and no stat batch is outstanding" is exactly the termination
 * condition. Dropping the last clause would end the walk while a stat thread
 * still held entries that can turn out to be directories.
 *
 * The wakeup invariant is that every transition which can make the loop
 * predicate true also signals wq->cv while holding wq->mu: workq_push and
 * workq_push_take signal after enqueuing (including pushes from a stat
 * thread), workq_batch_done broadcasts when inflight reaches zero, and
 * workq_fail broadcasts when it sets done. A batch pushes its discovered
 * directories before its inflight count is dropped, so inflight == 0 implies
 * the queue already holds whatever that batch found. With the pool off
 * inflight is always zero and the condition is the original one.
 *
 * The condition counts two places work can be: this queue, and a stat batch.
 * A worker's unflushed enqueue buffer is a third, and it is invisible to both,
 * so the rule that keeps this sound is that the buffer is empty whenever its
 * owner could be counted as parked or finished — see worker_flush_dirs.
 */
static int workq_pop(workq_t *wq, dirtask_t *out) {
    int rc = -1;

    pthread_mutex_lock(&wq->mu);
    for (;;) {
        if (wq->q.count > 0) {
            rc = taskdeq_pop_front(&wq->q, out);
            break;
        }
        if (wq->done) break;
        wq->nwaiting++;
        if (wq->nwaiting >= wq->nthreads && wq->inflight == 0) {
            wq->done = 1;
            pthread_cond_broadcast(&wq->cv);
            wq->nwaiting--;
            break;
        }
        pthread_cond_wait(&wq->cv, &wq->mu);
        wq->nwaiting--;
    }
    pthread_mutex_unlock(&wq->mu);
    return rc;
}

/* ----- set of uint64 keys (inode buckets) ------------------------------- */

typedef struct {
    uint64_t *keys;
    size_t    cap;   /* power of two, 0 = empty */
    size_t    count;
} u64set_t;

static int u64set_rehash(u64set_t *s, size_t new_cap) {
    uint64_t *keys = (uint64_t *)calloc(new_cap, sizeof(*keys));
    size_t i;

    if (!keys) return -1;
    for (i = 0; i < s->cap; i++) {
        uint64_t k = s->keys[i];
        size_t j;

        if (k == 0) continue;
        j = (size_t)((k * 0x9E3779B97F4A7C15ULL) >> 32) & (new_cap - 1);
        while (keys[j] != 0)
            j = (j + 1) & (new_cap - 1);
        keys[j] = k;
    }
    free(s->keys);
    s->keys = keys;
    s->cap = new_cap;
    return 0;
}

/* Keys are stored +1 so that 0 can mean "empty slot". */
static int u64set_add(u64set_t *s, uint64_t key) {
    uint64_t k = key + 1;
    size_t j;

    if (s->cap == 0 && u64set_rehash(s, 256) != 0) return -1;
    if (s->count * 10 >= s->cap * 7 && u64set_rehash(s, s->cap * 2) != 0) return -1;
    j = (size_t)((k * 0x9E3779B97F4A7C15ULL) >> 32) & (s->cap - 1);
    while (s->keys[j] != 0) {
        if (s->keys[j] == k) return 0;
        j = (j + 1) & (s->cap - 1);
    }
    s->keys[j] = k;
    s->count++;
    return 0;
}

static void u64set_destroy(u64set_t *s) {
    free(s->keys);
    s->keys = NULL;
    s->cap = s->count = 0;
}

/* ----- batch of names awaiting fstatat --------------------------------- */

typedef struct {
    uint64_t ino;      /* d_ino from getdents64: sortable before any stat */
    uint32_t off;      /* offset into arena */
    uint32_t len;
    uint8_t  unknown;  /* d_type was DT_UNKNOWN, so the stat decides the type */
} nent_t;

typedef struct {
    nent_t *ents;
    size_t  count;
    size_t  cap;
    size_t  next;      /* cursor, used by --stat spread */
    char   *arena;
    size_t  arena_len;
    size_t  arena_cap;
} nbatch_t;

static void nbatch_reset(nbatch_t *b) {
    b->count = 0;
    b->next = 0;
    b->arena_len = 0;
}

static void nbatch_destroy(nbatch_t *b) {
    free(b->ents);
    free(b->arena);
    memset(b, 0, sizeof(*b));
}

static int nbatch_add(nbatch_t *b, const char *name, size_t name_len, uint64_t ino, int unknown) {
    /* nent_t.off is 32-bit. A 1024-name batch could never reach that, but a
     * whole-directory window can hold millions of names, so the assumption is
     * checked rather than left implicit. */
    if (b->arena_len + name_len + 1 > (size_t)UINT32_MAX)
        return -1;
    if (b->count == b->cap) {
        size_t new_cap = (b->cap == 0) ? STAT_BATCH_NAMES : b->cap * 2;
        nent_t *e = (nent_t *)realloc(b->ents, new_cap * sizeof(*e));

        if (!e) return -1;
        b->ents = e;
        b->cap = new_cap;
    }
    if (b->arena_len + name_len + 1 > b->arena_cap) {
        size_t new_cap = b->arena_cap ? b->arena_cap * 2 : 64 * 1024;
        char *a;

        while (new_cap < b->arena_len + name_len + 1)
            new_cap *= 2;
        a = (char *)realloc(b->arena, new_cap);
        if (!a) return -1;
        b->arena = a;
        b->arena_cap = new_cap;
    }
    memcpy(b->arena + b->arena_len, name, name_len);
    b->arena[b->arena_len + name_len] = '\0';
    b->ents[b->count].ino = ino;
    b->ents[b->count].off = (uint32_t)b->arena_len;
    b->ents[b->count].len = (uint32_t)name_len;
    b->ents[b->count].unknown = (uint8_t)(unknown ? 1 : 0);
    b->count++;
    b->arena_len += name_len + 1;
    return 0;
}

static int cmp_nent_ino(const void *a, const void *b) {
    uint64_t x = ((const nent_t *)a)->ino;
    uint64_t y = ((const nent_t *)b)->ino;

    return (x > y) - (x < y);
}

/* ----- pool of reusable batches ---------------------------------------- */

/*
 * A reader that hands a batch to the stat pool needs another one to keep
 * reading into, and gets it from here rather than by allocating per batch.
 * The pool is capped: a batch is only ever held by a reader, sitting in the
 * stat queue, or being stat'd, so walk threads + queue depth + stat threads
 * bounds how many can exist at once. Returned batches keep their name arena,
 * which is the reuse that matters on a --sort-window all cell.
 */
typedef struct {
    pthread_mutex_t mu;
    nbatch_t      **free_list;
    size_t          free_count;
    size_t          allocated;
    size_t          cap;
} batchpool_t;

static int batchpool_init(batchpool_t *p, size_t cap) {
    memset(p, 0, sizeof(*p));
    p->free_list = (nbatch_t **)calloc(cap, sizeof(*p->free_list));
    if (!p->free_list) return -1;
    if (pthread_mutex_init(&p->mu, NULL) != 0) {
        free(p->free_list);
        p->free_list = NULL;
        return -1;
    }
    p->cap = cap;
    return 0;
}

/* NULL = pool exhausted or out of memory; every caller treats that as "stat
 * this batch inline", so it degrades rather than failing the walk. */
static nbatch_t *batchpool_get(batchpool_t *p) {
    nbatch_t *b = NULL;

    pthread_mutex_lock(&p->mu);
    if (p->free_count > 0) {
        b = p->free_list[--p->free_count];
    } else if (p->allocated < p->cap) {
        b = (nbatch_t *)calloc(1, sizeof(*b));
        if (b) p->allocated++;
    }
    pthread_mutex_unlock(&p->mu);
    return b;
}

static void batchpool_put(batchpool_t *p, nbatch_t *b) {
    if (!b) return;
    nbatch_reset(b);
    pthread_mutex_lock(&p->mu);
    if (p->free_count < p->cap) {
        p->free_list[p->free_count++] = b;
        b = NULL;
    }
    pthread_mutex_unlock(&p->mu);
    if (b) {
        nbatch_destroy(b);
        free(b);
    }
}

static void batchpool_destroy(batchpool_t *p) {
    size_t i;

    for (i = 0; i < p->free_count; i++) {
        nbatch_destroy(p->free_list[i]);
        free(p->free_list[i]);
    }
    free(p->free_list);
    if (p->cap > 0)
        pthread_mutex_destroy(&p->mu);
    memset(p, 0, sizeof(*p));
}

/* ----- refcounted open directory --------------------------------------- */

/*
 * The dirfd and the directory path outlive the reader's position in the
 * directory once a batch is stat'd on another thread: fstatat needs the dirfd,
 * and a DT_UNKNOWN entry that turns out to be a directory needs the path to
 * build the child task. Refcounted so the fd is closed exactly once, by
 * whichever of the reader and the last stat thread finishes last.
 */
typedef struct {
    int    fd;
    char  *path;
    size_t path_len;
    int    refs;
} dirref_t;

static dirref_t *dirref_new(int fd, char *path_owned, size_t path_len) {
    dirref_t *d = (dirref_t *)malloc(sizeof(*d));

    if (!d) return NULL;
    d->fd = fd;
    d->path = path_owned;
    d->path_len = path_len;
    d->refs = 1;
    return d;
}

static dirref_t *dirref_acquire(dirref_t *d) {
    __atomic_fetch_add(&d->refs, 1, __ATOMIC_RELAXED);
    return d;
}

static void dirref_release(dirref_t *d) {
    if (!d) return;
    if (__atomic_sub_fetch(&d->refs, 1, __ATOMIC_ACQ_REL) != 0)
        return;
    if (d->fd >= 0)
        close(d->fd);
    free(d->path);
    free(d);
}

/* ----- one open directory being read ----------------------------------- */

typedef struct {
    dirref_t *dir;
    char     *buf;      /* borrowed 1 MiB getdents64 buffer */
    size_t    buf_len;
    size_t    buf_off;
    int       eof;
    uint64_t  names;    /* stat-eligible entries seen in this directory */
    uint64_t  last_ino; /* previous stat-eligible d_ino, for readdir_asc_frac */
    int       have_last_ino;
} dstream_t;

/* ----- queue of batches awaiting a stat thread ------------------------- */

typedef struct {
    dirref_t *dir;
    nbatch_t *batch;
} statitem_t;

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    statitem_t     *items;
    size_t          cap;
    size_t          head;
    size_t          count;
    int             shutdown;
} statq_t;

static int statq_init(statq_t *sq, size_t cap) {
    memset(sq, 0, sizeof(*sq));
    sq->items = (statitem_t *)calloc(cap, sizeof(*sq->items));
    if (!sq->items) return -1;
    if (pthread_mutex_init(&sq->mu, NULL) != 0) goto out_items;
    if (pthread_cond_init(&sq->cv, NULL) != 0) goto out_mutex;
    sq->cap = cap;
    return 0;

out_mutex:
    pthread_mutex_destroy(&sq->mu);
out_items:
    free(sq->items);
    sq->items = NULL;
    return -1;
}

static void statq_destroy(statq_t *sq) {
    if (sq->cap > 0) {
        pthread_cond_destroy(&sq->cv);
        pthread_mutex_destroy(&sq->mu);
    }
    free(sq->items);
    memset(sq, 0, sizeof(*sq));
}

/*
 * -1 = the queue is full and the caller should stat the batch itself. The
 * reader never blocks here: waiting for a slot would put the directory read
 * behind the stat stream, which is the coupling the pool exists to remove.
 * wq->mu is taken while sq->mu is held and never the other way round.
 */
static int statq_try_push(statq_t *sq, workq_t *wq, statitem_t it) {
    int rc = -1;

    pthread_mutex_lock(&sq->mu);
    if (sq->count < sq->cap) {
        sq->items[(sq->head + sq->count) % sq->cap] = it;
        sq->count++;
        workq_batch_begin(wq);
        pthread_cond_signal(&sq->cv);
        rc = 0;
    }
    pthread_mutex_unlock(&sq->mu);
    return rc;
}

/* 0 = got a batch. Queued batches are drained even after shutdown, so their
 * directory references and batch buffers are always released by their owner. */
static int statq_pop(statq_t *sq, statitem_t *out) {
    int rc = -1;

    pthread_mutex_lock(&sq->mu);
    for (;;) {
        if (sq->count > 0) {
            *out = sq->items[sq->head];
            sq->head = (sq->head + 1) % sq->cap;
            sq->count--;
            rc = 0;
            break;
        }
        if (sq->shutdown) break;
        pthread_cond_wait(&sq->cv, &sq->mu);
    }
    pthread_mutex_unlock(&sq->mu);
    return rc;
}

static void statq_shutdown(statq_t *sq) {
    pthread_mutex_lock(&sq->mu);
    sq->shutdown = 1;
    pthread_cond_broadcast(&sq->cv);
    pthread_mutex_unlock(&sq->mu);
}

/* ----- per-worker state ------------------------------------------------ */

typedef struct {
    int       tid;
    workq_t  *wq;
    int       order;
    int       stat_mode;
    size_t    fill_names;  /* names accumulated before the batch is statted */

    /* Stat pool. sq == NULL is the default and means every batch is stat'd on
     * the thread that read it, which is the behaviour that predates the pool. */
    statq_t     *sq;
    batchpool_t *bpool;
    size_t       min_offload;
    int          stat_thread; /* this context belongs to the pool, not a walker */

    int       no_stat;  /* names only; d_type decides the type, no inode is read */
    int       no_dtype; /* ignore d_type; every entry is classified by its inode */
    int       locality; /* keep the inode-locality counters (default) */

    /* --stat-call. The mask and flags are resolved once in main rather than
     * switched on per entry, so the hot path carries the call it was given. */
    int          stat_call;
    unsigned int statx_mask;
    int          statx_flags;

    taskdeq_t local;   /* DFS local stack (front = top) */

    /* Discovered directories bound for the global queue, held back until
     * enqueue_batch of them have accumulated. Empty at every point where this
     * worker could be counted as parked or finished. */
    taskdeq_t pending;
    size_t    enqueue_batch;

    uint64_t  getdents_calls;
    uint64_t  fstatat_calls;
    uint64_t  dirs;
    uint64_t  files;
    uint64_t  errors;
    uint64_t  batches_offloaded;
    uint64_t  batches_inlined;
    uint64_t  queue_push_ops;  /* acquisitions of wq->mu to publish directories */
    uint64_t  max_dir_names;
    uint64_t  unknown_stats;   /* entries classified by inode rather than by d_type */
    uint64_t  statx_short;     /* statx replies missing a field the walk needs */

    /* readdir_asc_frac: consecutive stat-eligible entries within one directory,
     * in getdents64 order, whose inode number went up. A property of the tree
     * rather than of the strategy — it is counted while reading, before any
     * reordering — so every cell walking the same tree must report the same
     * value, and an aged tree that still reads ~1.0 was not actually aged. */
    uint64_t  rd_pairs;
    uint64_t  rd_asc;

    uint64_t  stats_done;
    uint64_t  chunk_hits;
    uint64_t  chunk_ring[CHUNK_WINDOW];
    size_t    chunk_ring_len;
    size_t    chunk_ring_pos;

    uint64_t  last_ino;
    int       have_last_ino;

    uint64_t *delta_samples;
    size_t    delta_n;
    uint64_t  delta_seen;
    uint64_t  rng;

    u64set_t  buckets;

    int       fatal;   /* allocation failure: stop walking, report it */
} worker_t;

static uint64_t rng_next(worker_t *w) {
    uint64_t x = w->rng;

    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    w->rng = x;
    return x;
}

/*
 * Everything the locality counters need, recorded once per inode actually
 * looked up. Order of the calls to this function *is* the measurement.
 *
 * With --stat-threads the caller is a stat thread, and its ring then records
 * that thread's own sequence of lookups rather than one reader's. That is the
 * sequence the filesystem sees from that thread, and it is what ecrawl's stat
 * workers produce, so it is the thing worth measuring; it is not the same
 * quantity as the single-reader sequence, which is why --stat-threads 0 stays
 * available rather than being replaced by the pool.
 *
 * This is also the one thing ewalkbench does that ecrawl does not do at all: a
 * ring scan, a reservoir sample and a hash insert per inode, on the measured
 * path. --no-locality drops it for runs whose point is walk cost rather than
 * inode order. The stat itself is still counted, so `stats` means the same
 * thing either way.
 */
static void worker_note_stat(worker_t *w, uint64_t ino) {
    uint64_t chunk = ino >> CHUNK_SHIFT;
    size_t i;

    w->stats_done++;
    if (!w->locality)
        return;

    for (i = 0; i < w->chunk_ring_len; i++) {
        if (w->chunk_ring[i] == chunk) {
            w->chunk_hits++;
            break;
        }
    }
    w->chunk_ring[w->chunk_ring_pos] = chunk;
    w->chunk_ring_pos = (w->chunk_ring_pos + 1) % CHUNK_WINDOW;
    if (w->chunk_ring_len < CHUNK_WINDOW)
        w->chunk_ring_len++;

    if (w->have_last_ino) {
        uint64_t d = (ino > w->last_ino) ? (ino - w->last_ino) : (w->last_ino - ino);

        if (w->delta_samples) {
            if (w->delta_n < DELTA_SAMPLE_CAP) {
                w->delta_samples[w->delta_n++] = d;
            } else {
                uint64_t r = rng_next(w) % (w->delta_seen + 1);

                if (r < DELTA_SAMPLE_CAP)
                    w->delta_samples[r] = d;
            }
        }
        w->delta_seen++;
    }
    w->last_ino = ino;
    w->have_last_ino = 1;

    if (u64set_add(&w->buckets, ino >> BUCKET_SHIFT) != 0)
        w->fatal = 1;
}

static char *join_path(const char *dir, size_t dir_len, const char *name, size_t name_len, size_t *out_len) {
    int need_slash = (dir_len > 0 && dir[dir_len - 1] != '/');
    size_t len = dir_len + (need_slash ? 1 : 0) + name_len;
    char *p = (char *)malloc(len + 1);

    if (!p) return NULL;
    memcpy(p, dir, dir_len);
    if (need_slash)
        p[dir_len] = '/';
    memcpy(p + dir_len + (need_slash ? 1 : 0), name, name_len);
    p[len] = '\0';
    *out_len = len;
    return p;
}

/*
 * Publish everything this worker has discovered and not yet enqueued.
 *
 * Termination rests on this. A directory sitting in w->pending is in neither
 * the global queue nor the in-flight batch count, so workq_pop would be
 * entitled to declare the walk over while a subtree was still buffered, and
 * the run would report a plausible short count with errors=0. The buffer is
 * therefore drained at every point where its owner stops being visible as
 * work in progress: before a walk thread can block in workq_pop, and before a
 * stat thread drops its in-flight count. Both of those go through here, and
 * nothing else can park a thread.
 *
 * Not called on the fatal path: the walk is being abandoned there, and
 * taskdeq_destroy frees what is left.
 */
static void worker_flush_dirs(worker_t *w) {
    size_t n = w->pending.count;

    if (n == 0) return;
    w->queue_push_ops++;
    if (workq_push_take(w->wq, &w->pending, n, 0, w->order == ORDER_DFS) != 0)
        w->fatal = 1;
}

/* DFS keeps discovered directories local (LIFO); BFS publishes them to the
 * tail of the global queue, which is what makes the frontier wide.
 *
 * A stat thread has no local stack to walk, so whatever it discovers has to
 * reach the global queue or it would never be visited. It pushes to the LIFO
 * end under DFS so the frontier keeps its depth-first shape.
 *
 * Publication is by batch, not by directory. The frontier is the same set of
 * directories in the same order either way — a BFS batch still goes to the
 * FIFO tail in discovery order — and only the moment of publication moves,
 * by at most enqueue_batch discoveries. That is what ecrawl does with
 * ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH. */
static void worker_push_dir(worker_t *w, char *path_owned, size_t path_len) {
    dirtask_t t;

    t.path = path_owned;
    t.len = path_len;
    w->dirs++;
    if (w->order == ORDER_DFS && !w->stat_thread) {
        if (taskdeq_push_front(&w->local, t) != 0) {
            free(path_owned);
            w->fatal = 1;
        }
        return;
    }
    if (taskdeq_push_back(&w->pending, t) != 0) {
        free(path_owned);
        w->fatal = 1;
        return;
    }
    if (w->pending.count >= w->enqueue_batch)
        worker_flush_dirs(w);
}

/* Hand the shallow half of the local stack to the other workers, in batches
 * for the same reason worker_push_dir publishes in batches. In chunks rather
 * than one splice: the first chunk is what wakes the other threads, and on
 * this shape the donated half is 100,000 directories, so a single critical
 * section would hold every other thread off the queue until all of them were
 * copied. */
static void worker_donate(worker_t *w) {
    size_t n;

    if (w->order != ORDER_DFS || w->local.count < DONATE_AT) return;
    n = w->local.count / 2;
    while (n > 0 && !w->fatal) {
        size_t chunk = (n < w->enqueue_batch) ? n : w->enqueue_batch;

        w->queue_push_ops++;
        if (workq_push_take(w->wq, &w->local, chunk, 1, 1) != 0) {
            w->fatal = 1;
            break;
        }
        n -= chunk;
    }
}

static int worker_try_next_dir(worker_t *w, dirtask_t *out) {
    if (w->order == ORDER_DFS && taskdeq_pop_front(&w->local, out) == 0)
        return 0;
    worker_flush_dirs(w);
    return workq_trypop(w->wq, out);
}

static int worker_next_dir(worker_t *w, dirtask_t *out) {
    if (w->order == ORDER_DFS && taskdeq_pop_front(&w->local, out) == 0)
        return 0;
    worker_flush_dirs(w);
    return workq_pop(w->wq, out);
}

static int dstream_open(worker_t *w, dstream_t *s, dirtask_t t, char *buf) {
    int fd = open(t.path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);

    if (fd < 0) {
        fprintf(stderr, "ewalkbench: open %s: %s\n", t.path, strerror(errno));
        w->errors++;
        free(t.path);
        return -1;
    }
    s->dir = dirref_new(fd, t.path, t.len);
    if (!s->dir) {
        close(fd);
        free(t.path);
        w->fatal = 1;
        return -1;
    }
    s->buf = buf;
    s->buf_len = 0;
    s->buf_off = 0;
    s->eof = 0;
    s->names = 0;
    s->last_ino = 0;
    s->have_last_ino = 0;
    return 0;
}

static void dstream_close(dstream_t *s) {
    dirref_release(s->dir);
    s->dir = NULL;
}

/*
 * Read from `s` until the batch holds w->fill_names names to stat or the
 * directory ends. Subdirectories are pushed as work here and never stat'd:
 * that d_type shortcut is what ecrawl does, and it keeps the stat stream pure.
 *
 * Under --no-dtype the shortcut is unavailable and every entry is an unknown,
 * so nothing is pushed from here at all and the batch decides. Two counters
 * then cover more entries than they otherwise would: s->names (and through it
 * max_dir_names) and the readdir_asc_frac pairs both count stat-eligible
 * entries, and under --no-dtype that set includes the subdirectories. Their
 * values are therefore not comparable with a run that had d_type — which is
 * a property of the question, not a defect: a walker without ftype genuinely
 * cannot tell a directory from a file before it reads the inode.
 */
static void dstream_fill_batch(worker_t *w, dstream_t *s, nbatch_t *b) {
    while (b->count < w->fill_names && !s->eof && !w->fatal) {
        struct linux_dirent64 *d;
        const char *name;
        size_t name_len;
        int unknown;

        if (s->buf_off >= s->buf_len) {
            long n = syscall(SYS_getdents64, s->dir->fd, s->buf, (size_t)GETDENTS_BUF_BYTES);

            if (n < 0) {
                fprintf(stderr, "ewalkbench: getdents64 %s: %s\n", s->dir->path, strerror(errno));
                w->errors++;
                s->eof = 1;
                return;
            }
            w->getdents_calls++;
            if (n == 0) {
                s->eof = 1;
                return;
            }
            s->buf_len = (size_t)n;
            s->buf_off = 0;
        }

        d = (struct linux_dirent64 *)(void *)(s->buf + s->buf_off);
        if (d->d_reclen == 0) { /* malformed; do not spin */
            s->eof = 1;
            return;
        }
        s->buf_off += d->d_reclen;
        name = d->d_name;
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0')))
            continue;
        name_len = strlen(name);
        unknown = (d->d_type == DT_UNKNOWN) || w->no_dtype;

        if (!unknown && d->d_type == DT_DIR) {
            size_t child_len;
            char *child = join_path(s->dir->path, s->dir->path_len, name, name_len, &child_len);

            if (!child) {
                w->fatal = 1;
                return;
            }
            worker_push_dir(w, child, child_len);
            continue;
        }
        /* max_dir_names feeds the skew warning: one directory holding most of
         * the tree is the shape the pool exists for. Both these and the
         * readdir_asc_frac pairs come from getdents64 and so are the same
         * whether or not the entry is later stat'd. */
        s->names++;
        if (s->names > w->max_dir_names)
            w->max_dir_names = s->names;
        if (s->have_last_ino) {
            w->rd_pairs++;
            if (d->d_ino > s->last_ino)
                w->rd_asc++;
        }
        s->last_ino = d->d_ino;
        s->have_last_ino = 1;
        if (!unknown)
            w->files++;

        /* --no-stat: d_type has already classified this entry, so no inode is
         * read and it never enters a batch. DT_UNKNOWN is the exception, and
         * it falls through to the normal path below for the reason ecrawl
         * gives: without an inode there is no way to know whether the entry is
         * a directory to recurse into. Those stats are counted separately, so
         * "names only" is a claim the summary can support rather than assert. */
        if (w->no_stat && !unknown)
            continue;

        if (nbatch_add(b, name, name_len, d->d_ino, unknown) != 0) {
            w->fatal = 1;
            return;
        }
    }
}

static const char *stat_call_name(int call) {
    switch (call) {
    case CALL_STATX:        return "statx";
    case CALL_STATX_MIN:    return "statx-min";
    case CALL_STATX_NOSYNC: return "statx-nosync";
    default:                return "fstatat";
    }
}

/*
 * Read the inode with whichever call --stat-call named, and hand back the only
 * two fields the walk consumes.
 *
 * statx is allowed to fill less than it was asked for, and stx_mask is what it
 * actually filled. A reply without the type or the inode falls back to fstatat
 * for that entry rather than being used: a mode that misclassified a directory
 * or fed a zero inode to the locality counters would stop being comparable
 * with the others, and the comparison is the whole reason the dimension
 * exists. The shortfalls are counted, so "statx answered in full" is something
 * the summary can support rather than assume.
 */
static int stat_fields(worker_t *w, int dirfd, const char *name, uint64_t *ino, mode_t *mode) {
    struct stat st;

#ifdef HAVE_STATX
    if (w->stat_call != CALL_FSTATAT) {
        struct statx stx;

        if (statx(dirfd, name, w->statx_flags, w->statx_mask, &stx) != 0)
            return -1;
        if ((stx.stx_mask & (STATX_TYPE | STATX_INO)) == (STATX_TYPE | STATX_INO)) {
            *ino = (uint64_t)stx.stx_ino;
            *mode = (mode_t)stx.stx_mode;
            return 0;
        }
        w->statx_short++;
    }
#endif
    if (fstatat(dirfd, name, &st, AT_SYMLINK_NOFOLLOW) != 0)
        return -1;
    *ino = (uint64_t)st.st_ino;
    *mode = st.st_mode;
    return 0;
}

/*
 * Stat one name relative to the parent dirfd, exactly as ecrawl does.
 * DT_UNKNOWN entries (filesystems without dirent ftype) are classified here,
 * so entry and directory totals stay identical whatever the strategy is.
 *
 * This is the only place a directory is discovered by anything other than the
 * read loop, and with the stat pool on the caller is a stat thread, so the
 * push below runs while walk threads may be parked in workq_pop. Counting the
 * unknowns off e->unknown rather than off --no-stat makes dtype_unknown_stats
 * the count of entries that took this route whatever put them on it: under
 * --no-stat only the dirents the filesystem left unknown reach a batch at all,
 * so the value there is unchanged.
 */
static void stat_one(worker_t *w, dirref_t *dir, nbatch_t *b, const nent_t *e) {
    const char *name = b->arena + e->off;
    uint64_t ino = 0;
    mode_t mode = 0;

    w->fstatat_calls++;
    if (e->unknown)
        w->unknown_stats++;
    if (stat_fields(w, dir->fd, name, &ino, &mode) != 0) {
        fprintf(stderr, "ewalkbench: %s %s/%s: %s\n",
                stat_call_name(w->stat_call), dir->path, name, strerror(errno));
        w->errors++;
        if (e->unknown)
            w->files++;
        return;
    }
    worker_note_stat(w, ino);
    if (!e->unknown)
        return;
    if (S_ISDIR(mode)) {
        size_t child_len;
        char *child = join_path(dir->path, dir->path_len, name, e->len, &child_len);

        if (!child) {
            w->fatal = 1;
            return;
        }
        worker_push_dir(w, child, child_len);
    } else {
        w->files++;
    }
}

/* ----- strategies ------------------------------------------------------- */

/*
 * --stat hash and --stat ino: one directory at a time, in batches of
 * w->fill_names (1024 by default). hash leaves the batch in getdents64 order;
 * ino sorts the batch by d_ino, so consecutive lookups walk inode chunks in
 * order. The default window is 1024 rather than the directory because that is
 * the window ecrawl's removed stat pool used.
 *
 * How far apart those two orders actually are is a property of the
 * filesystem, not an assumption to make: measured on ORCD /scratch (XFS),
 * getdents64 returns a freshly built directory in inode order already, so
 * chunk_reuse_rate reads the same for both. It separates sharply (0.05 vs
 * 0.70) once readdir order and inode order genuinely differ.
 *
 * The window is what makes a null attributable in a big directory. A sorted
 * window of W names drawn from a directory of D files leaves consecutive
 * inode gaps of about D/W, and chunk locality needs that under 64, so the
 * default window can only help while D <= 1024*64 ~ 65,000. Measured on a
 * scrambled 1M-entry directory: sorting each 1024-name batch moved
 * chunk_reuse_rate from 0.0010 only to 0.0316, while sorting the whole
 * directory reached 0.9844. The window applies to --stat hash as well as
 * --stat ino, so that a hash cell at the same window isolates the sort from
 * the change in how reads and stats interleave.
 *
 * The batch is enqueued for the stat pool after the sort, so the sort still
 * defines the stat order and the reader goes straight back to getdents64 with
 * a fresh buffer. Which batches go to the pool follows ecrawl: at least
 * min_offload names, so full mid-directory flushes always offload and only a
 * short tail is stat'd inline, and a full queue also falls back to inline
 * rather than stalling the read.
 *
 * That "short tail" framing only holds when directories are larger than the
 * threshold. On a tree of 200,000 five-file directories every batch is a "tail",
 * so the gate at the default of 32 offloads nothing (stat_batches_offloaded=0,
 * stat_batches_inlined=200000). Setting the threshold to 0 engages the pool,
 * but the same tree then gets ~40–50% slower: the handoff costs more than
 * stating five names inline. The counter is printed as stat_min_offload next to
 * the two batch counters so a run that hits this can be recognised rather than
 * read as a pool that was never asked for.
 *
 * At --stat-threads 0 the batch is instead stat'd here, on the thread that
 * read it, which makes one directory the unit of parallelism however many
 * threads exist. That is the pre-pool behaviour, kept because the existing
 * result set was measured with it; it is not what ecrawl does, and on a
 * single-directory tree it holds the walk to one outstanding inode read.
 */
static void walk_dir_batched(worker_t *w, dirtask_t t, char *buf, nbatch_t **bp) {
    dstream_t s;

    if (dstream_open(w, &s, t, buf) != 0)
        return;
    for (;;) {
        nbatch_t *b = *bp;
        size_t i;

        nbatch_reset(b);
        dstream_fill_batch(w, &s, b);
        if (w->fatal) break;
        if (b->count == 0) break;
        if (w->stat_mode == STAT_INO && b->count > 1)
            qsort(b->ents, b->count, sizeof(b->ents[0]), cmp_nent_ino);
        if (w->sq && b->count >= w->min_offload) {
            nbatch_t *fresh = batchpool_get(w->bpool);

            if (fresh) {
                statitem_t it;

                it.dir = dirref_acquire(s.dir);
                it.batch = b;
                if (statq_try_push(w->sq, w->wq, it) == 0) {
                    w->batches_offloaded++;
                    *bp = fresh;
                    continue;
                }
                dirref_release(it.dir);
                batchpool_put(w->bpool, fresh);
            }
        }
        w->batches_inlined++;
        for (i = 0; i < b->count && !w->fatal; i++)
            stat_one(w, s.dir, b, &b->ents[i]);
        if (w->fatal) break;
    }
    dstream_close(&s);
}

/*
 * --stat spread: the literal "as wide as possible" hypothesis. The worker
 * keeps SPREAD_SLOTS directories open at once and takes one entry from each in
 * rotation, so consecutive lookups land in different directories and therefore
 * far apart in inode space.
 *
 * Rotation rather than a greedy "pick the furthest pending inode": greedy
 * degenerates into ping-ponging between the two extreme slots and starves the
 * middle ones, which would test something narrower than the hypothesis.
 *
 * --sort-window does not apply here: this mode sorts nothing, and a window per
 * slot would multiply the per-worker memory by SPREAD_SLOTS for no measurement.
 * Its slots always refill 1024 names at a time.
 *
 * The stat pool does not apply either, for the same kind of reason: the point
 * of the mode is the interleaving of consecutive lookups on one thread, and
 * handing slot batches to a pool would stat them contiguously per directory
 * and measure something else. So this mode reports stat_threads=0 whatever
 * --stat-threads said, and counts its batches as inlined.
 */
typedef struct {
    dstream_t stream;
    nbatch_t  batch;
    int       active;
} spread_slot_t;

/* Pull the next batch for a slot; closes and deactivates it at end of dir. */
static void spread_slot_refill(worker_t *w, spread_slot_t *sl) {
    while (sl->active) {
        nbatch_reset(&sl->batch);
        dstream_fill_batch(w, &sl->stream, &sl->batch);
        if (w->fatal) return;
        if (sl->batch.count > 0) {
            w->batches_inlined++;
            return;
        }
        if (sl->stream.eof) {
            dstream_close(&sl->stream);
            sl->active = 0;
            return;
        }
    }
}

static void walk_spread(worker_t *w, char **bufs) {
    spread_slot_t slots[SPREAD_SLOTS];
    size_t rr = 0;
    int i;
    int nactive = 0;

    memset(slots, 0, sizeof(slots));

    while (!w->fatal) {
        /* Top up idle slots. Only block for work when nothing is open, so a
         * worker holding directories never stalls the termination check. */
        for (i = 0; i < SPREAD_SLOTS && !w->fatal; i++) {
            dirtask_t t;

            if (slots[i].active) continue;
            if (nactive == 0 && i == 0) {
                if (worker_next_dir(w, &t) != 0)
                    goto drain;
            } else if (worker_try_next_dir(w, &t) != 0) {
                continue;
            }
            if (dstream_open(w, &slots[i].stream, t, bufs[i]) != 0)
                continue;
            slots[i].active = 1;
            nactive++;
            spread_slot_refill(w, &slots[i]);
            if (!slots[i].active)
                nactive--;
        }
        if (w->fatal) break;

        /* One stat per active slot, in rotation: consecutive lookups come
         * from different directories. A slot is only ever active with a
         * pending entry, so this always makes progress while nactive > 0. */
        for (i = 0; i < SPREAD_SLOTS && !w->fatal; i++) {
            size_t k = (rr + (size_t)i) % SPREAD_SLOTS;
            spread_slot_t *sl = &slots[k];

            if (!sl->active) continue;
            stat_one(w, sl->stream.dir, &sl->batch, &sl->batch.ents[sl->batch.next]);
            sl->batch.next++;
            if (sl->batch.next >= sl->batch.count) {
                spread_slot_refill(w, sl);
                if (!sl->active)
                    nactive--;
            }
        }
        rr++;
    }

drain:
    for (i = 0; i < SPREAD_SLOTS; i++) {
        if (slots[i].active)
            dstream_close(&slots[i].stream);
        nbatch_destroy(&slots[i].batch);
    }
}

static void *worker_main(void *arg) {
    worker_t *w = (worker_t *)arg;
    char *bufs[SPREAD_SLOTS];
    nbatch_t *batch = NULL;
    int nbufs = (w->stat_mode == STAT_SPREAD) ? SPREAD_SLOTS : 1;
    int i;

    memset(bufs, 0, sizeof(bufs));
    for (i = 0; i < nbufs; i++) {
        bufs[i] = (char *)malloc(GETDENTS_BUF_BYTES);
        if (!bufs[i]) {
            w->fatal = 1;
            goto out;
        }
    }
    if (w->locality) {
        w->delta_samples = (uint64_t *)malloc(DELTA_SAMPLE_CAP * sizeof(uint64_t));
        if (!w->delta_samples) {
            w->fatal = 1;
            goto out;
        }
    }

    if (w->stat_mode == STAT_SPREAD) {
        walk_spread(w, bufs);
    } else {
        batch = batchpool_get(w->bpool);
        if (!batch) {
            w->fatal = 1;
            goto out;
        }
        for (;;) {
            dirtask_t t;

            if (worker_next_dir(w, &t) != 0) break;
            walk_dir_batched(w, t, bufs[0], &batch);
            if (w->fatal) break;
            worker_donate(w);
        }
    }

out:
    if (w->fatal)
        workq_fail(w->wq);
    batchpool_put(w->bpool, batch);
    for (i = 0; i < nbufs; i++)
        free(bufs[i]);
    taskdeq_destroy(&w->local);
    taskdeq_destroy(&w->pending);
    return NULL;
}

/*
 * A stat thread owns no directory and never reads one: it only stats batches
 * other threads have already read and ordered. It keeps its own worker_t so
 * the locality counters stay per-thread, and its totals are folded into the
 * same sums as the walk threads, so a run with the pool on reports exactly the
 * same entry, dir, file and stat counts as one without it.
 */
static void *stat_main(void *arg) {
    worker_t *w = (worker_t *)arg;
    statitem_t it;

    if (w->locality) {
        w->delta_samples = (uint64_t *)malloc(DELTA_SAMPLE_CAP * sizeof(uint64_t));
        if (!w->delta_samples) {
            w->fatal = 1;
            workq_fail(w->wq);
        }
    }

    /* Queued batches are still drained after a failure: their directory
     * references and batch buffers are only released here. */
    while (statq_pop(w->sq, &it) == 0) {
        size_t i;

        for (i = 0; i < it.batch->count && !w->fatal; i++)
            stat_one(w, it.dir, it.batch, &it.batch->ents[i]);
        dirref_release(it.dir);
        batchpool_put(w->bpool, it.batch);
        /* After the pushes and after the buffer holding them is drained,
         * never before: a walk thread may end the walk the moment this
         * reaches zero, and anything still buffered here would be a subtree
         * nobody ever visits. */
        worker_flush_dirs(w);
        workq_batch_done(w->wq);
        if (w->fatal)
            workq_fail(w->wq);
    }

    if (w->fatal)
        workq_fail(w->wq);
    taskdeq_destroy(&w->pending);
    return NULL;
}

/* ----- CLI and summary -------------------------------------------------- */

static void usage(FILE *fp) {
    fprintf(fp,
        "usage: ewalkbench [--order dfs|bfs] [--stat hash|ino|spread] [--threads N]\n"
        "                  [--sort-window N|all] [--stat-threads N]\n"
        "                  [--stat-min-offload N] [--dir-enqueue-batch N]\n"
        "                  [--stat-call fstatat|statx|statx-min|statx-nosync]\n"
        "                  [--no-stat] [--no-dtype] [--no-locality] <root>\n"
        "\n"
        "  --order dfs      depth-first: LIFO local stack per thread plus a global queue (default)\n"
        "  --order bfs      breadth-first: every directory goes to a shared FIFO frontier\n"
        "  --stat hash      stat each batch in getdents64 order (default; what ecrawl does today)\n"
        "  --stat ino       sort each batch by inode before statting\n"
        "  --stat spread    keep %d directories open and rotate one stat across them\n"
        "  --threads N      worker threads, 1..1024 (default 8)\n"
        "  --sort-window N  names accumulated before a batch is statted, 1..%u\n"
        "                   (default %d, the window ecrawl's removed stat pool\n"
        "                   used; --stat ino sorts this window, and --stat hash reads it\n"
        "                   the same way so the two differ only by the sort)\n"
        "  --sort-window all  one window per directory, capped at %u names (~24 bytes\n"
        "                   per name plus the name bytes, per worker). Ignored by\n"
        "                   --stat spread, whose slots always refill %d names.\n"
        "  --stat-threads N stat batches on a pool of N threads instead of on the\n"
        "                   thread that read the directory, 0..1024 (default %d,\n"
        "                   ecrawl's removed stat-pool default). 0 stats every batch on\n"
        "                   the thread that read it, which makes one directory the\n"
        "                   unit of parallelism; it is the pre-pool behaviour and what\n"
        "                   logs/ewalk-strategy-manual-567654/ was measured with.\n"
        "                   Ignored by --stat spread, whose whole point is the\n"
        "                   interleaving of consecutive lookups on one thread.\n"
        "  --stat-min-offload N  batches of fewer than N names are stat'd inline\n"
        "                   rather than queued, 0..%u (default %d, what ecrawl's\n"
        "                   removed stat pool used; 0 queues\n"
        "                   every batch). On a tree of five-file directories the\n"
        "                   default offloads nothing, and 0 engages the pool but\n"
        "                   is the slower of the two — measured, not assumed.\n"
        "                   Only meaningful with --stat-threads.\n"
        "  --dir-enqueue-batch N  discovered directories published per acquisition\n"
        "                   of the global queue lock, 1..%u (default %d, which is\n"
        "                   ecrawl's ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH). 1 is one\n"
        "                   lock and one condvar signal per directory, the form\n"
        "                   this file had before the batch existed; on a\n"
        "                   200,000-directory tree that is bimodal, 0.395 s or\n"
        "                   1.100 s per run. Reported as queue_push_ops.\n"
        "  --stat-call C    which syscall reads the inode (default fstatat, what\n"
        "                   ecrawl does and what every earlier result here was\n"
        "                   measured with). statx is AT_SYMLINK_NOFOLLOW |\n"
        "                   AT_NO_AUTOMOUNT with STATX_BASIC_STATS, byte-for-byte\n"
        "                   what dut does; statx-min narrows the mask to type, mode,\n"
        "                   ino, nlink, uid and gid, dropping the size, blocks and\n"
        "                   timestamps that force an NFS GETATTR round trip;\n"
        "                   statx-nosync adds AT_STATX_DONT_SYNC to the full mask.\n"
        "                   The walk reads only the type and the inode whichever is\n"
        "                   chosen, and a statx reply missing either falls back to\n"
        "                   fstatat for that entry and is counted as\n"
        "                   statx_mask_short, so the modes stay comparable.\n"
        "  --no-stat        walk names only and read no inode, as ecrawl --no-stat\n"
        "                   does: d_type classifies each entry and only DT_UNKNOWN\n"
        "                   still costs an fstatat (reported as dtype_unknown_stats).\n"
        "                   The comparison against a traversal-only tool. Entry, dir\n"
        "                   and file totals are unchanged wherever d_type is supplied.\n"
        "  --no-dtype       ignore d_type and classify every entry by fstatat, as a\n"
        "                   filesystem without dirent ftype (some NFS configurations)\n"
        "                   forces. Costs one extra stat per directory below the root\n"
        "                   and moves every subdirectory discovery into a stat batch;\n"
        "                   entry, dir and file totals are unchanged, and the entries\n"
        "                   taking this route are reported as dtype_unknown_stats.\n"
        "                   Refused with --no-stat: a names-only walk has no inode to\n"
        "                   classify by and would silently stop recursing. Accepted\n"
        "                   with --stat spread, which stats inline, so the discoveries\n"
        "                   happen on the walk thread rather than in the pool.\n"
        "  --no-locality    drop the per-stat inode-locality bookkeeping, which ecrawl\n"
        "                   has no equivalent of. chunk_reuse_rate, median_ino_delta\n"
        "                   and distinct_ino_buckets are then reported as unavailable\n"
        "                   rather than as zeros. readdir_asc_frac is counted while\n"
        "                   reading and survives.\n"
        "\n"
        "Walks the tree and stats every non-directory entry. Writes nothing.\n"
        "Summary is printed to stdout as key=value lines.\n",
        SPREAD_SLOTS, STAT_SORT_WINDOW_CAP, STAT_BATCH_NAMES,
        STAT_SORT_WINDOW_CAP, STAT_BATCH_NAMES,
        DEFAULT_STAT_THREADS, STAT_SORT_WINDOW_CAP, DEFAULT_STAT_MIN_OFFLOAD,
        DIR_ENQUEUE_BATCH_MAX, DEFAULT_DIR_ENQUEUE_BATCH);
}

static int parse_threads(const char *s, int *out) {
    char *end = NULL;
    long v;

    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || !end || *end != '\0' || v < 1 || v > 1024)
        return -1;
    *out = (int)v;
    return 0;
}

/* Plain bounded integer, for the flags that have no "all" spelling and no
 * clamp: out of range is a usage error rather than a silent adjustment. */
static int parse_bounded(const char *s, long lo, long hi, int *out) {
    char *end = NULL;
    long v;

    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || !end || *end != '\0' || v < lo || v > hi)
        return -1;
    *out = (int)v;
    return 0;
}

/* "all" resolves to the cap, and a number above the cap is clamped to it with
 * a warning: the resolved value is echoed as stat_sort_window, so the clamp is
 * never invisible in the results. */
static int parse_sort_window(const char *s, size_t *out) {
    char *end = NULL;
    long long v;

    if (strcmp(s, "all") == 0 || strcmp(s, "dir") == 0) {
        *out = STAT_SORT_WINDOW_CAP;
        return 0;
    }
    errno = 0;
    v = strtoll(s, &end, 10);
    if (errno != 0 || !end || *end != '\0' || v < 1)
        return -1;
    if (v > (long long)STAT_SORT_WINDOW_CAP) {
        fprintf(stderr, "ewalkbench: --sort-window %lld exceeds the %u-name cap; using the cap\n",
                v, STAT_SORT_WINDOW_CAP);
        v = (long long)STAT_SORT_WINDOW_CAP;
    }
    *out = (size_t)v;
    return 0;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;

    return (x > y) - (x < y);
}

static double now_sec(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

int main(int argc, char **argv) {
    const char *order_name = "dfs";
    const char *stat_name = "hash";
    const char *root = NULL;
    int order = ORDER_DFS;
    int stat_mode = STAT_HASH;
    int stat_call = CALL_FSTATAT;
    unsigned int statx_mask = 0;
    int statx_flags = 0;
    int nthreads = 8;
    size_t sort_window = STAT_BATCH_NAMES;
    int stat_threads = DEFAULT_STAT_THREADS;
    int stat_threads_req = 0;
    int min_offload = DEFAULT_STAT_MIN_OFFLOAD;
    int enqueue_batch = DEFAULT_DIR_ENQUEUE_BATCH;
    int no_stat = 0;
    int no_dtype = 0;
    int locality = 1;
    int i;
    workq_t wq;
    statq_t sq;
    batchpool_t bpool;
    worker_t *workers = NULL;
    worker_t *stat_workers = NULL;
    pthread_t *tids = NULL;
    pthread_t *stat_tids = NULL;
    dirtask_t root_task;
    double t0, t1, elapsed;
    uint64_t getdents_calls = 0, fstatat_calls = 0, dirs = 0, files = 0, errors = 0;
    uint64_t stats_done = 0, chunk_hits = 0;
    uint64_t rd_pairs = 0, rd_asc = 0;
    uint64_t batches_offloaded = 0, batches_inlined = 0, max_dir_names = 0;
    uint64_t queue_push_ops = 0;
    uint64_t unknown_stats = 0;
    uint64_t statx_short = 0;
    uint64_t *deltas = NULL;
    size_t ndeltas = 0, deltas_cap = 0;
    uint64_t median_delta = 0;
    u64set_t all_buckets;
    int fatal = 0;
    int started = 0;
    int stat_started = 0;

    memset(&all_buckets, 0, sizeof(all_buckets));
    memset(&sq, 0, sizeof(sq));
    memset(&bpool, 0, sizeof(bpool));

    for (i = 1; i < argc; i++) {
        const char *a = argv[i];

        if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
            usage(stdout);
            return 0;
        }
        if (strncmp(a, "--order", 7) == 0 && (a[7] == '\0' || a[7] == '=')) {
            const char *v = (a[7] == '=') ? a + 8 : (++i < argc ? argv[i] : NULL);

            if (!v) { usage(stderr); return 2; }
            if (strcmp(v, "dfs") == 0) order = ORDER_DFS;
            else if (strcmp(v, "bfs") == 0) order = ORDER_BFS;
            else { fprintf(stderr, "ewalkbench: invalid --order %s\n", v); return 2; }
            order_name = (order == ORDER_DFS) ? "dfs" : "bfs";
            continue;
        }
        if (strncmp(a, "--stat", 6) == 0 && (a[6] == '\0' || a[6] == '=')) {
            const char *v = (a[6] == '=') ? a + 7 : (++i < argc ? argv[i] : NULL);

            if (!v) { usage(stderr); return 2; }
            if (strcmp(v, "hash") == 0) stat_mode = STAT_HASH;
            else if (strcmp(v, "ino") == 0) stat_mode = STAT_INO;
            else if (strcmp(v, "spread") == 0) stat_mode = STAT_SPREAD;
            else { fprintf(stderr, "ewalkbench: invalid --stat %s\n", v); return 2; }
            stat_name = (stat_mode == STAT_HASH) ? "hash" : (stat_mode == STAT_INO ? "ino" : "spread");
            continue;
        }
        if (strncmp(a, "--stat-call", 11) == 0 && (a[11] == '\0' || a[11] == '=')) {
            const char *v = (a[11] == '=') ? a + 12 : (++i < argc ? argv[i] : NULL);

            if (!v) { usage(stderr); return 2; }
            if (strcmp(v, "fstatat") == 0) stat_call = CALL_FSTATAT;
#ifdef HAVE_STATX
            else if (strcmp(v, "statx") == 0) stat_call = CALL_STATX;
            else if (strcmp(v, "statx-min") == 0) stat_call = CALL_STATX_MIN;
            else if (strcmp(v, "statx-nosync") == 0) stat_call = CALL_STATX_NOSYNC;
#else
            /* Refused rather than quietly served by fstatat: a run that asked
             * for statx and got the baseline would report a difference of
             * zero as a measurement. */
            else if (strcmp(v, "statx") == 0 || strcmp(v, "statx-min") == 0 ||
                     strcmp(v, "statx-nosync") == 0) {
                fprintf(stderr, "ewalkbench: --stat-call %s needs STATX_BASIC_STATS, "
                                "which this build's <sys/stat.h> does not define\n", v);
                return 2;
            }
#endif
            else { fprintf(stderr, "ewalkbench: invalid --stat-call %s\n", v); return 2; }
            continue;
        }
        if (strncmp(a, "--sort-window", 13) == 0 && (a[13] == '\0' || a[13] == '=')) {
            const char *v = (a[13] == '=') ? a + 14 : (++i < argc ? argv[i] : NULL);

            if (!v || parse_sort_window(v, &sort_window) != 0) {
                fprintf(stderr, "ewalkbench: invalid --sort-window %s\n", v ? v : "(missing)");
                return 2;
            }
            continue;
        }
        if (strcmp(a, "--no-stat") == 0) {
            no_stat = 1;
            continue;
        }
        if (strcmp(a, "--no-dtype") == 0) {
            no_dtype = 1;
            continue;
        }
        if (strcmp(a, "--no-locality") == 0) {
            locality = 0;
            continue;
        }
        if (strncmp(a, "--stat-threads", 14) == 0 && (a[14] == '\0' || a[14] == '=')) {
            const char *v = (a[14] == '=') ? a + 15 : (++i < argc ? argv[i] : NULL);

            if (!v || parse_bounded(v, 0, 1024, &stat_threads) != 0) {
                fprintf(stderr, "ewalkbench: invalid --stat-threads %s\n", v ? v : "(missing)");
                return 2;
            }
            continue;
        }
        if (strncmp(a, "--stat-min-offload", 18) == 0 && (a[18] == '\0' || a[18] == '=')) {
            const char *v = (a[18] == '=') ? a + 19 : (++i < argc ? argv[i] : NULL);

            if (!v || parse_bounded(v, 0, (long)STAT_SORT_WINDOW_CAP, &min_offload) != 0) {
                fprintf(stderr, "ewalkbench: invalid --stat-min-offload %s\n", v ? v : "(missing)");
                return 2;
            }
            continue;
        }
        if (strncmp(a, "--dir-enqueue-batch", 19) == 0 && (a[19] == '\0' || a[19] == '=')) {
            const char *v = (a[19] == '=') ? a + 20 : (++i < argc ? argv[i] : NULL);

            if (!v || parse_bounded(v, 1, DIR_ENQUEUE_BATCH_MAX, &enqueue_batch) != 0) {
                fprintf(stderr, "ewalkbench: invalid --dir-enqueue-batch %s\n", v ? v : "(missing)");
                return 2;
            }
            continue;
        }
        if (strncmp(a, "--threads", 9) == 0 && (a[9] == '\0' || a[9] == '=')) {
            const char *v = (a[9] == '=') ? a + 10 : (++i < argc ? argv[i] : NULL);

            if (!v || parse_threads(v, &nthreads) != 0) {
                fprintf(stderr, "ewalkbench: invalid --threads %s\n", v ? v : "(missing)");
                return 2;
            }
            continue;
        }
        if (a[0] == '-' && a[1] != '\0') {
            fprintf(stderr, "ewalkbench: unknown option %s\n", a);
            usage(stderr);
            return 2;
        }
        if (root) {
            fprintf(stderr, "ewalkbench: unexpected extra argument %s\n", a);
            return 2;
        }
        root = a;
    }

    /* Refused rather than resolved either way round: with no d_type and no
     * inode nothing can be classified, so the walk would read the top of the
     * tree, recurse into nothing, and report a plausible-looking entry count
     * for a fraction of the tree. Silently promoting it to a full stat walk
     * would be worse still, since --no-stat is the one thing the run asked
     * for. */
    if (no_stat && no_dtype) {
        fprintf(stderr, "ewalkbench: --no-stat and --no-dtype are contradictory: "
                        "--no-dtype classifies every entry by its inode and --no-stat "
                        "reads none, so nothing could be recursed into\n");
        return 2;
    }

    if (!root) {
        usage(stderr);
        return 2;
    }

    {
        struct stat st;

        if (stat(root, &st) != 0) {
            fprintf(stderr, "ewalkbench: stat %s: %s\n", root, strerror(errno));
            return 1;
        }
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "ewalkbench: %s is not a directory\n", root);
            return 1;
        }
    }

#ifdef HAVE_STATX
    if (stat_call != CALL_FSTATAT) {
        statx_flags = AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT;
        statx_mask = (stat_call == CALL_STATX_MIN) ? STATX_MIN_MASK : STATX_BASIC_STATS;
        if (stat_call == CALL_STATX_NOSYNC)
            statx_flags |= AT_STATX_DONT_SYNC;
    }
#endif

    if (stat_threads > 0 && stat_mode == STAT_SPREAD)
        stat_threads = 0; /* the mode stats one entry at a time by design */
    /* Nothing to offload when only DT_UNKNOWN entries are stat'd, and ecrawl
     * drops its pool under --no-stat for the same reason. */
    if (stat_threads > 0 && no_stat)
        stat_threads = 0;

    if (workq_init(&wq, nthreads) != 0) {
        fprintf(stderr, "ewalkbench: queue init failed\n");
        return 1;
    }

    /* A batch is held by a reader, queued, or being stat'd, and never in two
     * of those at once, so this bounds the pool exactly. */
    if (batchpool_init(&bpool, (size_t)nthreads + STAT_QUEUE_BATCHES + (size_t)stat_threads) != 0) {
        fprintf(stderr, "ewalkbench: out of memory\n");
        workq_destroy(&wq);
        return 1;
    }
    if (stat_threads > 0 && statq_init(&sq, STAT_QUEUE_BATCHES) != 0) {
        fprintf(stderr, "ewalkbench: out of memory\n");
        batchpool_destroy(&bpool);
        workq_destroy(&wq);
        return 1;
    }

    root_task.len = strlen(root);
    root_task.path = strdup(root);
    workers = (worker_t *)calloc((size_t)nthreads, sizeof(*workers));
    tids = (pthread_t *)calloc((size_t)nthreads, sizeof(*tids));
    if (stat_threads > 0) {
        stat_workers = (worker_t *)calloc((size_t)stat_threads, sizeof(*stat_workers));
        stat_tids = (pthread_t *)calloc((size_t)stat_threads, sizeof(*stat_tids));
    }
    if (!root_task.path || !workers || !tids ||
        (stat_threads > 0 && (!stat_workers || !stat_tids)) ||
        workq_push(&wq, root_task, 1) != 0) {
        fprintf(stderr, "ewalkbench: out of memory\n");
        free(root_task.path);
        free(workers);
        free(tids);
        free(stat_workers);
        free(stat_tids);
        statq_destroy(&sq);
        batchpool_destroy(&bpool);
        workq_destroy(&wq);
        return 1;
    }

    for (i = 0; i < nthreads; i++) {
        workers[i].tid = i;
        workers[i].wq = &wq;
        workers[i].order = order;
        workers[i].stat_mode = stat_mode;
        workers[i].fill_names = (stat_mode == STAT_SPREAD) ? STAT_BATCH_NAMES : sort_window;
        workers[i].bpool = &bpool;
        workers[i].min_offload = (size_t)min_offload;
        workers[i].enqueue_batch = (size_t)enqueue_batch;
        workers[i].no_stat = no_stat;
        workers[i].no_dtype = no_dtype;
        workers[i].locality = locality;
        workers[i].stat_call = stat_call;
        workers[i].statx_mask = statx_mask;
        workers[i].statx_flags = statx_flags;
        workers[i].rng = 0x9E3779B97F4A7C15ULL ^ ((uint64_t)i + 1);
    }
    for (i = 0; i < stat_threads; i++) {
        stat_workers[i].tid = nthreads + i;
        stat_workers[i].wq = &wq;
        stat_workers[i].order = order;
        stat_workers[i].stat_mode = stat_mode;
        stat_workers[i].sq = &sq;
        stat_workers[i].bpool = &bpool;
        stat_workers[i].enqueue_batch = (size_t)enqueue_batch;
        stat_workers[i].stat_thread = 1;
        stat_workers[i].no_stat = no_stat;
        stat_workers[i].no_dtype = no_dtype;
        stat_workers[i].locality = locality;
        stat_workers[i].stat_call = stat_call;
        stat_workers[i].statx_mask = statx_mask;
        stat_workers[i].statx_flags = statx_flags;
        stat_workers[i].rng = 0x9E3779B97F4A7C15ULL ^ ((uint64_t)(nthreads + i) + 1);
    }

    stat_threads_req = stat_threads;
    t0 = now_sec();
    /* Stat threads first: a walk thread only offloads once its own pool
     * pointer is set, and that is set from stat_started.
     *
     * pthread_create returns its error code and leaves errno alone, so the
     * reason is taken from the return value: strerror(errno) here would name
     * whatever syscall failed last, which is the wrong thing to read while
     * diagnosing an unattended run that came back short of threads. */
    for (i = 0; i < stat_threads; i++) {
        int rc = pthread_create(&stat_tids[i], NULL, stat_main, &stat_workers[i]);

        if (rc != 0) {
            fprintf(stderr, "ewalkbench: pthread_create failed at stat thread %d: %s\n",
                    i, strerror(rc));
            break;
        }
        stat_started++;
    }
    if (stat_started > 0) {
        for (i = 0; i < nthreads; i++)
            workers[i].sq = &sq;
    } else if (stat_threads > 0) {
        fprintf(stderr, "ewalkbench: no stat threads started; batches are stat'd inline\n");
    }
    stat_threads = stat_started;
    for (i = 0; i < nthreads; i++) {
        int rc = pthread_create(&tids[i], NULL, worker_main, &workers[i]);

        if (rc != 0) {
            fprintf(stderr, "ewalkbench: pthread_create failed at thread %d: %s\n", i, strerror(rc));
            /* Let the threads that did start finish the walk. */
            pthread_mutex_lock(&wq.mu);
            wq.nthreads = i > 0 ? i : 1;
            pthread_cond_broadcast(&wq.cv);
            pthread_mutex_unlock(&wq.mu);
            fatal = 1;
            break;
        }
        started++;
    }
    for (i = 0; i < started; i++)
        pthread_join(tids[i], NULL);
    /* Walk threads only end once no batch is outstanding, so the pool is
     * already idle here and shutdown just releases it from its wait. */
    if (stat_started > 0) {
        statq_shutdown(&sq);
        for (i = 0; i < stat_started; i++)
            pthread_join(stat_tids[i], NULL);
    }
    t1 = now_sec();
    elapsed = t1 - t0;

    for (i = 0; i < started + stat_started; i++) {
        worker_t *w = (i < started) ? &workers[i] : &stat_workers[i - started];
        size_t k;

        getdents_calls += w->getdents_calls;
        fstatat_calls += w->fstatat_calls;
        dirs += w->dirs;
        files += w->files;
        errors += w->errors;
        stats_done += w->stats_done;
        chunk_hits += w->chunk_hits;
        rd_pairs += w->rd_pairs;
        rd_asc += w->rd_asc;
        batches_offloaded += w->batches_offloaded;
        batches_inlined += w->batches_inlined;
        queue_push_ops += w->queue_push_ops;
        unknown_stats += w->unknown_stats;
        statx_short += w->statx_short;
        if (w->max_dir_names > max_dir_names)
            max_dir_names = w->max_dir_names;
        if (w->fatal) fatal = 1;

        if (w->delta_n > 0) {
            if (ndeltas + w->delta_n > deltas_cap) {
                size_t new_cap = deltas_cap ? deltas_cap * 2 : (w->delta_n + 1024);
                uint64_t *p;

                while (new_cap < ndeltas + w->delta_n)
                    new_cap *= 2;
                p = (uint64_t *)realloc(deltas, new_cap * sizeof(*p));
                if (p) {
                    deltas = p;
                    deltas_cap = new_cap;
                }
            }
            if (ndeltas + w->delta_n <= deltas_cap) {
                memcpy(deltas + ndeltas, w->delta_samples, w->delta_n * sizeof(*deltas));
                ndeltas += w->delta_n;
            }
        }
        for (k = 0; k < w->buckets.cap; k++) {
            if (w->buckets.keys[k] != 0 &&
                u64set_add(&all_buckets, w->buckets.keys[k] - 1) != 0)
                fatal = 1;
        }
        free(w->delta_samples);
        u64set_destroy(&w->buckets);
    }

    if (ndeltas > 0) {
        qsort(deltas, ndeltas, sizeof(*deltas), cmp_u64);
        median_delta = (ndeltas % 2 == 1)
            ? deltas[ndeltas / 2]
            : (deltas[ndeltas / 2 - 1] + deltas[ndeltas / 2]) / 2;
    }

    /* The root itself is an entry and a directory, like ecrawl counts it. */
    dirs += 1;

    /* A shape where one directory holds most of the entries is exactly the one
     * the inline path cannot parallelise: the directory is the unit of work, so
     * the run is single-threaded whatever --threads says, and the timing then
     * describes that limit rather than the strategy. Said here rather than left
     * to be inferred from cpu_util in a perf stat afterwards. */
    if (stat_started == 0 && !no_stat && stat_mode != STAT_SPREAD &&
        files >= 1000 && max_dir_names * 2 > files) {
        fprintf(stderr, "ewalkbench: one directory holds %" PRIu64 " of %" PRIu64
                        " walked entries and --stat-threads is 0, so those stats ran on a\n"
                        "ewalkbench: single thread regardless of --threads %d; "
                        "compare against --stat-threads %d before reading the timing as a strategy result\n",
                max_dir_names, files, nthreads, DEFAULT_STAT_THREADS);
    }

    printf("order=%s\n", order_name);
    printf("stat=%s\n", stat_name);
    /* Both thread keys report what actually started, so a run that lost a
     * thread to pthread_create reports the walker it was rather than the one
     * it asked for; the requested counts are reported alongside so the two are
     * never confused. The requested stat count is the number this run tried to
     * create, after --stat spread and --no-stat have had their say, which is
     * what makes stat_threads < stat_threads_requested mean exactly "creation
     * failed". */
    printf("threads=%d\n", started);
    printf("threads_requested=%d\n", nthreads);
    printf("spread_slots=%d\n", SPREAD_SLOTS);
    printf("stat_sort_window=%zu\n", (stat_mode == STAT_SPREAD) ? (size_t)STAT_BATCH_NAMES : sort_window);
    printf("stat_threads=%d\n", stat_threads);
    printf("stat_threads_requested=%d\n", stat_threads_req);
    printf("stat_call=%s\n", stat_call_name(stat_call));
    /* The mask is echoed rather than left to be derived from the mode name,
     * since the mode name is a label and the mask is the thing the kernel was
     * asked for. Under fstatat there is no mask and no statx to come up short,
     * so both are named as unavailable rather than printed as zeros a reader
     * could take for measurements. */
    if (stat_call == CALL_FSTATAT) {
        printf("statx_mask_requested=(unavailable: --stat-call fstatat)\n");
        printf("statx_mask_short=(unavailable: --stat-call fstatat)\n");
    } else {
        printf("statx_mask_requested=0x%08x\n", statx_mask);
        printf("statx_mask_short=%" PRIu64 "\n", statx_short);
    }
    printf("no_stat=%d\n", no_stat);
    printf("no_dtype=%d\n", no_dtype);
    printf("locality_counters=%d\n", locality);
    printf("root=%s\n", root);
    printf("entries=%" PRIu64 "\n", dirs + files);
    printf("dirs=%" PRIu64 "\n", dirs);
    printf("files=%" PRIu64 "\n", files);
    printf("getdents_calls=%" PRIu64 "\n", getdents_calls);
    printf("fstatat_calls=%" PRIu64 "\n", fstatat_calls);
    printf("elapsed_sec=%.3f\n", elapsed);
    printf("entries_per_sec=%.0f\n", elapsed > 0.0 ? (double)(dirs + files) / elapsed : 0.0);
    /* Named as unavailable rather than printed as zeros, which would read as
     * measured facts; the same rule ecrawl --no-stat follows for byte totals. */
    if (locality) {
        printf("chunk_reuse_rate=%.4f\n", stats_done ? (double)chunk_hits / (double)stats_done : 0.0);
        printf("chunk_reuse_window=%d\n", CHUNK_WINDOW);
        printf("median_ino_delta=%" PRIu64 "\n", median_delta);
        printf("distinct_ino_buckets=%zu\n", all_buckets.count);
    } else {
        printf("chunk_reuse_rate=(unavailable: --no-locality)\n");
        printf("chunk_reuse_window=%d\n", CHUNK_WINDOW);
        printf("median_ino_delta=(unavailable: --no-locality)\n");
        printf("distinct_ino_buckets=(unavailable: --no-locality)\n");
    }
    printf("readdir_asc_frac=%.4f\n", rd_pairs ? (double)rd_asc / (double)rd_pairs : 0.0);
    printf("readdir_pairs=%" PRIu64 "\n", rd_pairs);
    printf("stats=%" PRIu64 "\n", stats_done);
    printf("dtype_unknown_stats=%" PRIu64 "\n", unknown_stats);
    /* queue_push_ops counts what the batch is meant to reduce, so a run can
     * show that it did rather than be assumed to have done. The root push
     * from here is not counted: these are the workers' own acquisitions. */
    printf("dir_enqueue_batch=%d\n", enqueue_batch);
    printf("queue_push_ops=%" PRIu64 "\n", queue_push_ops);
    /* The threshold decides the two counters under it, so it is echoed with
     * them rather than left to be inferred from the flags: a run whose
     * directories are all smaller than it offloads nothing, and without this
     * key that reads as a pool that was never asked for. Named as unavailable
     * when no pool started, since the gate is then inert rather than zero. */
    if (stat_started > 0)
        printf("stat_min_offload=%d\n", min_offload);
    else
        printf("stat_min_offload=(unavailable: no stat pool)\n");
    printf("stat_batches_offloaded=%" PRIu64 "\n", batches_offloaded);
    printf("stat_batches_inlined=%" PRIu64 "\n", batches_inlined);
    printf("stat_pool_max_inflight=%" PRIu64 "\n", wq.max_inflight);
    printf("errors=%" PRIu64 "\n", errors);
    fflush(stdout);

    free(deltas);
    u64set_destroy(&all_buckets);
    free(workers);
    free(tids);
    free(stat_workers);
    free(stat_tids);
    statq_destroy(&sq);
    batchpool_destroy(&bpool);
    workq_destroy(&wq);
    return fatal ? 1 : 0;
}
