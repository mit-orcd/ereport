/*
 * ecrawl_mount — mount an ecrawl crawl result as a read-only FUSE filesystem,
 * so ordinary POSIX tools (find, ls, stat, du, fd, rsync -n) can walk a crawl
 * without the source filesystem being present.
 *
 * Linux only. Not supported on macOS: macFUSE mounts of this filesystem can
 * wedge the VFS badly enough to hang umount/mount, and the Makefile does not
 * build this target on Darwin.
 *
 * The view is metadata-only, because that is all a crawl records:
 *   - read() returns zeros up to the recorded st_size. Sizes, and therefore
 *     `wc -c` and `du`, are exact; contents are not stored and never were.
 *   - permissions are synthesized: 0555 for directories, 0444 otherwise, with
 *     the recorded uid and a configurable gid. The v8 record does carry mode
 *     and gid, but the in-memory index deliberately omits them: the whole
 *     namespace is resident, and two more fields per entry is a memory cost
 *     paid on every mount for a read-only view that cannot honour the bits
 *     anyway.
 *   - symlink targets are not stored, so readlink() fails with EIO. Type is
 *     still correct, so `find -type l` works.
 *
 * Shards are sharded by owner uid, so one directory's children are spread over
 * many shard files and there is no on-disk path index. The whole namespace is
 * therefore indexed in memory at mount time; see build_index() below.
 *
 * SPDX-License-Identifier: MIT
 */
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <ctype.h>
#include <errno.h>
#include <fuse.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "crawl_bin_block.h"
#include "crawl_bin_catalog.h"
#include "crawl_bin_chunks.h"
#include "crawl_bin_format.h"
#include "crawl_result.h"

#define PROG "ecrawl_mount"

#define DEFAULT_MOUNT_THREADS 32

/* Attributes never change once mounted, so let the kernel cache them hard.
 * This is what makes a second `find` over the mount nearly free. */
#define CACHE_TIMEOUT_SEC 86400

#define MDIR_NONE UINT32_MAX
#define ENT_NONE UINT64_MAX

/* ---------------------------------------------------------------------------
 * Index structures
 *
 * g_dirs is the directory namespace merged from every shard's catalog: one row
 * per distinct directory path, parent-linked, with a sibling list so the
 * synthetic ancestors above the crawl root are still traversable.
 *
 * g_ents is one row per crawl record, sorted by (parent, name). Each directory
 * therefore owns a contiguous run of g_ents, which makes readdir a slice and
 * lookup a binary search with no extra index.
 * ------------------------------------------------------------------------- */

typedef struct {
    uint64_t name_off; /* into g_names */
    uint64_t size;
    uint64_t ino;
    uint64_t nlink;
    int64_t atime;
    int64_t mtime;
    int64_t ctime;
    uint32_t parent;  /* g_dirs index of the containing directory */
    uint32_t dir_idx; /* type=='d': its own g_dirs index; MDIR_NONE if childless */
    uint32_t uid;
    uint16_t name_len;
    uint8_t type; /* find(1)-style code: f d l c b p s o */
    uint8_t pad;
} ment_t;

typedef struct {
    uint64_t name_off; /* into g_dir_names */
    uint64_t first_ent;
    uint32_t ent_count;
    uint32_t parent;
    uint32_t first_child;
    uint32_t next_sibling;
    uint32_t depth;
    uint16_t name_len;
    uint16_t pad;
} mdir_t;

static mdir_t *g_dirs;
static uint32_t g_dir_count;
static uint32_t g_dir_cap;

static char *g_dir_names;
static uint64_t g_dir_names_len;
static uint64_t g_dir_names_cap;

/* Open-addressed (parent, name) -> g_dirs index. Built in phase 1, read-only
 * afterwards, which is what lets phase 2 and the FUSE ops probe it lock-free. */
static uint32_t *g_dir_ht;
static size_t g_dir_ht_size;
static size_t g_dir_ht_used;

static ment_t *g_ents;
static uint64_t g_ent_count;

static char *g_names;
static uint64_t g_names_len;

/* Root of the mounted view: 0 (the synthetic "/" of the crawl) unless --subtree. */
static uint32_t g_root_dir;

/* Directories reachable in the mounted view; differs from g_dir_count under --subtree. */
static uint32_t g_dirs_mounted;

/* Totals for statfs(), summed over the entries actually mounted. */
static uint64_t g_total_bytes;

static crawl_result_t g_cr;

static uid_t g_mount_uid;
static gid_t g_synth_gid;
static time_t g_synth_time; /* stamp for directories that have no record */
static int g_verbose;
static int g_threads = DEFAULT_MOUNT_THREADS;

static uint64_t g_stat_records_skipped; /* records with an unresolvable parent */

static void vlog(const char *fmt, ...) {
    va_list ap;

    if (!g_verbose) return;
    va_start(ap, fmt);
    fprintf(stderr, PROG ": ");
    vfprintf(stderr, fmt, ap);
    va_end(ap);
}

static double now_sec(void) {
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static void human_bytes(uint64_t v, char *out, size_t out_sz) {
    static const char *unit[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    double d = (double)v;
    size_t u = 0;

    while (d >= 1024.0 && u + 1 < sizeof(unit) / sizeof(unit[0])) {
        d /= 1024.0;
        u++;
    }
    snprintf(out, out_sz, "%.1f %s", d, unit[u]);
}

/* ---------------------------------------------------------------------------
 * Small parallel-for: workers pull [begin,end) ranges off one atomic cursor.
 * ------------------------------------------------------------------------- */

typedef void (*par_body_fn)(void *ctx, size_t begin, size_t end, int tid);

typedef struct {
    par_body_fn body;
    void *ctx;
    size_t n;
    size_t grain;
    _Atomic size_t cursor;
} par_state_t;

typedef struct {
    par_state_t *st;
    int tid;
} par_worker_arg_t;

static void *par_worker(void *p) {
    par_worker_arg_t *a = (par_worker_arg_t *)p;
    par_state_t *st = a->st;

    for (;;) {
        size_t begin = atomic_fetch_add(&st->cursor, st->grain);
        size_t end;

        if (begin >= st->n) break;
        end = begin + st->grain;
        if (end > st->n) end = st->n;
        st->body(st->ctx, begin, end, a->tid);
    }
    return NULL;
}

static int par_for(size_t n, size_t grain, int nthreads, par_body_fn body, void *ctx) {
    par_state_t st;
    pthread_t *tids;
    par_worker_arg_t *args;
    int started = 0;

    if (n == 0) return 0;
    if (grain == 0) grain = 1;
    if (nthreads < 1) nthreads = 1;
    if ((size_t)nthreads > (n + grain - 1) / grain) nthreads = (int)((n + grain - 1) / grain);

    st.body = body;
    st.ctx = ctx;
    st.n = n;
    st.grain = grain;
    atomic_init(&st.cursor, (size_t)0);

    if (nthreads == 1) {
        par_worker_arg_t a;
        a.st = &st;
        a.tid = 0;
        par_worker(&a);
        return 0;
    }

    tids = (pthread_t *)calloc((size_t)nthreads, sizeof(*tids));
    args = (par_worker_arg_t *)calloc((size_t)nthreads, sizeof(*args));
    if (!tids || !args) {
        free(tids);
        free(args);
        return -1;
    }
    for (int i = 0; i < nthreads; i++) {
        args[i].st = &st;
        args[i].tid = i;
        if (pthread_create(&tids[i], NULL, par_worker, &args[i]) != 0) break;
        started++;
    }
    /* Threads that never started leave work on the cursor; run it here so the
     * region still completes rather than silently dropping records. */
    if (started < nthreads) {
        par_worker_arg_t a;
        a.st = &st;
        a.tid = started;
        par_worker(&a);
    }
    for (int i = 0; i < started; i++) pthread_join(tids[i], NULL);

    free(tids);
    free(args);
    return 0;
}

/* ---------------------------------------------------------------------------
 * Directory namespace: arena, intern table, merge
 * ------------------------------------------------------------------------- */

static int dir_names_reserve(uint64_t extra) {
    if (g_dir_names_len + extra <= g_dir_names_cap) return 0;
    {
        uint64_t ncap = g_dir_names_cap ? g_dir_names_cap * 2 : (1ULL << 16);
        char *nx;

        while (ncap < g_dir_names_len + extra) ncap *= 2;
        nx = (char *)realloc(g_dir_names, (size_t)ncap);
        if (!nx) return -1;
        g_dir_names = nx;
        g_dir_names_cap = ncap;
    }
    return 0;
}

static uint32_t hash_dir_key(uint32_t parent, const char *name, size_t name_len) {
    uint64_t h = 1469598103934665603ULL; /* FNV-1a */

    for (size_t i = 0; i < name_len; i++) {
        h ^= (unsigned char)name[i];
        h *= 1099511628211ULL;
    }
    h ^= (uint64_t)parent * 0x9E3779B97F4A7C15ULL;
    h *= 1099511628211ULL;
    return (uint32_t)(h ^ (h >> 32));
}

static int dir_key_eq(uint32_t idx, uint32_t parent, const char *name, size_t name_len) {
    const mdir_t *d = &g_dirs[idx];

    if (d->parent != parent || d->name_len != name_len) return 0;
    if (name_len == 0) return 1; /* the root's empty name; arena may be unallocated */
    return memcmp(g_dir_names + d->name_off, name, name_len) == 0;
}

static int dir_ht_grow(void);

static int dir_ht_insert(uint32_t idx) {
    const mdir_t *d = &g_dirs[idx];
    uint32_t h = hash_dir_key(d->parent, g_dir_names + d->name_off, d->name_len);
    size_t mask = g_dir_ht_size - 1;
    size_t slot = h & mask;

    while (g_dir_ht[slot] != MDIR_NONE) slot = (slot + 1) & mask;
    g_dir_ht[slot] = idx;
    g_dir_ht_used++;
    if (g_dir_ht_used * 10 >= g_dir_ht_size * 7) return dir_ht_grow();
    return 0;
}

static int dir_ht_grow(void) {
    size_t nsize = g_dir_ht_size * 2;
    uint32_t *nht = (uint32_t *)malloc(nsize * sizeof(*nht));
    size_t mask = nsize - 1;

    if (!nht) return -1;
    memset(nht, 0xFF, nsize * sizeof(*nht));
    for (size_t i = 0; i < g_dir_ht_size; i++) {
        uint32_t idx = g_dir_ht[i];
        size_t slot;
        const mdir_t *d;

        if (idx == MDIR_NONE) continue;
        d = &g_dirs[idx];
        slot = hash_dir_key(d->parent, g_dir_names + d->name_off, d->name_len) & mask;
        while (nht[slot] != MDIR_NONE) slot = (slot + 1) & mask;
        nht[slot] = idx;
    }
    free(g_dir_ht);
    g_dir_ht = nht;
    g_dir_ht_size = nsize;
    return 0;
}

/* Read-only probe; safe from many threads once phase 1 has finished. */
static uint32_t dir_lookup(uint32_t parent, const char *name, size_t name_len) {
    uint32_t h;
    size_t mask, slot;

    if (!g_dir_ht) return MDIR_NONE;
    h = hash_dir_key(parent, name, name_len);
    mask = g_dir_ht_size - 1;
    slot = h & mask;
    for (;;) {
        uint32_t idx = g_dir_ht[slot];

        if (idx == MDIR_NONE) return MDIR_NONE;
        if (dir_key_eq(idx, parent, name, name_len)) return idx;
        slot = (slot + 1) & mask;
    }
}

static uint32_t dir_intern(uint32_t parent, const char *name, size_t name_len, uint32_t depth) {
    uint32_t idx = dir_lookup(parent, name, name_len);
    mdir_t *d;

    if (idx != MDIR_NONE) return idx;
    if (g_dir_count == MDIR_NONE) return MDIR_NONE; /* index space exhausted */

    if (g_dir_count == g_dir_cap) {
        uint32_t ncap = g_dir_cap ? g_dir_cap * 2 : 1024;
        mdir_t *nx = (mdir_t *)realloc(g_dirs, (size_t)ncap * sizeof(*nx));

        if (!nx) return MDIR_NONE;
        g_dirs = nx;
        g_dir_cap = ncap;
    }
    if (dir_names_reserve(name_len) != 0) return MDIR_NONE;

    idx = g_dir_count++;
    d = &g_dirs[idx];
    d->name_off = g_dir_names_len;
    d->name_len = (uint16_t)name_len;
    d->pad = 0;
    d->parent = parent;
    d->depth = depth;
    d->first_ent = 0;
    d->ent_count = 0;
    d->first_child = MDIR_NONE;
    d->next_sibling = MDIR_NONE;
    /* The root has an empty name and may be interned before the arena exists. */
    if (name_len) memcpy(g_dir_names + g_dir_names_len, name, name_len);
    g_dir_names_len += name_len;

    if (parent != MDIR_NONE) {
        d->next_sibling = g_dirs[parent].first_child;
        g_dirs[parent].first_child = idx;
    }
    if (dir_ht_insert(idx) != 0) return MDIR_NONE;
    return idx;
}

static int dir_ht_init(void) {
    g_dir_ht_size = 1U << 16;
    g_dir_ht = (uint32_t *)malloc(g_dir_ht_size * sizeof(*g_dir_ht));
    if (!g_dir_ht) return -1;
    memset(g_dir_ht, 0xFF, g_dir_ht_size * sizeof(*g_dir_ht));
    g_dir_ht_used = 0;
    return 0;
}

/*
 * Ancestor chain being resolved, grown on demand. Trees hundreds of levels deep
 * are a normal adversarial case (see DEPTH_CHAIN in the tree generator), so this
 * must not be a fixed-size stack.
 */
typedef struct {
    uint64_t *ids;
    size_t cap;
} dirstack_t;

static int dirstack_set(dirstack_t *s, size_t depth, uint64_t id) {
    if (depth >= s->cap) {
        size_t ncap = s->cap ? s->cap * 2 : 256;
        uint64_t *nx;

        while (ncap <= depth) ncap *= 2;
        nx = (uint64_t *)realloc(s->ids, ncap * sizeof(*nx));
        if (!nx) return -1;
        s->ids = nx;
        s->cap = ncap;
    }
    s->ids[depth] = id;
    return 0;
}

/*
 * Map one shard's local dir_id onto the global namespace. Ancestors are
 * resolved first, so this works whatever order the catalog rows arrive in.
 * l2g[] doubles as the memo; MDIR_NONE means "not yet resolved".
 */
static uint32_t resolve_local_dir(const crawl_bin_catalog_t *cat, uint32_t *l2g, uint64_t dir_id,
                                  dirstack_t *stack) {
    size_t depth = 0;

    while (dir_id != 0 && dir_id <= cat->max_dir_id && l2g[dir_id] == MDIR_NONE) {
        if (dir_id == 1ULL) break; /* synthetic root, pre-seeded by the caller */
        /* An acyclic chain visits each dir_id at most once, so exceeding the
         * row count means the catalog's parent links form a cycle. */
        if (depth > cat->max_dir_id) return MDIR_NONE;
        if (dirstack_set(stack, depth, dir_id) != 0) return MDIR_NONE;
        depth++;
        dir_id = cat->parent_dir_id[dir_id];
    }
    if (dir_id == 0 || dir_id > cat->max_dir_id) return MDIR_NONE;

    /* Unwind deepest-last so each dir_intern sees a resolved parent. */
    while (depth > 0) {
        uint64_t did = stack->ids[--depth];
        uint64_t pid = cat->parent_dir_id[did];
        uint32_t parent;

        if (pid == 0 || pid > cat->max_dir_id) return MDIR_NONE;
        parent = l2g[pid];
        if (parent == MDIR_NONE) return MDIR_NONE;
        if (cat->name_len[did] == 0 || !cat->name_comp[did]) return MDIR_NONE;
        l2g[did] = dir_intern(parent, cat->name_comp[did], cat->name_len[did], g_dirs[parent].depth + 1U);
        if (l2g[did] == MDIR_NONE) return MDIR_NONE;
    }
    return l2g[dir_id];
}

/*
 * Phase 1: merge every shard catalog into g_dirs and record a local->global
 * dir_id map per shard for phase 2 to use.
 */
static int build_dir_namespace(uint32_t **l2g_out, uint64_t *l2g_len_out) {
    double t0 = now_sec();
    dirstack_t stack = {NULL, 0};

    if (dir_ht_init() != 0) {
        fprintf(stderr, PROG ": out of memory allocating the directory table\n");
        return -1;
    }
    /* g_dirs[0] is the crawl's synthetic root: empty name, no parent. */
    if (dir_intern(MDIR_NONE, "", 0, 0) != 0) {
        fprintf(stderr, PROG ": failed to create the root directory node\n");
        return -1;
    }

    for (size_t i = 0; i < g_cr.shard_count; i++) {
        const crawl_result_shard_t *sh = &g_cr.shards[i];
        crawl_bin_catalog_t cat;
        FILE *fp;
        uint32_t *l2g;

        fp = fopen(sh->path, "rb");
        if (!fp) {
            fprintf(stderr, PROG ": %s: %s\n", sh->path, strerror(errno));
            free(stack.ids);
            return -1;
        }
        /* Tree fields only: the mount rebuilds its own index and reads no rollup. */
        if (crawl_bin_catalog_load_sel(fp, sh->catalog_offset, sh->file_size, 0U, &cat) != 0) {
            fprintf(stderr, PROG ": %s: cannot load the directory catalog\n", sh->path);
            fclose(fp);
            free(stack.ids);
            return -1;
        }
        fclose(fp);

        l2g_len_out[i] = cat.max_dir_id + 1;
        l2g = (uint32_t *)malloc((size_t)(cat.max_dir_id + 1) * sizeof(*l2g));
        if (!l2g) {
            fprintf(stderr, PROG ": out of memory mapping %s\n", sh->path);
            crawl_bin_catalog_free(&cat);
            free(stack.ids);
            return -1;
        }
        for (uint64_t d = 0; d <= cat.max_dir_id; d++) l2g[d] = MDIR_NONE;
        if (cat.max_dir_id >= 1) l2g[1] = 0; /* local root -> global root */

        for (uint64_t d = 1; d <= cat.max_dir_id; d++) {
            if (l2g[d] != MDIR_NONE) continue;
            /* Gaps are normal: catalog_ensure_slots zero-fills unused ids. */
            if (cat.name_len[d] == 0 && cat.parent_dir_id[d] == 0) continue;
            if (resolve_local_dir(&cat, l2g, d, &stack) == MDIR_NONE) {
                fprintf(stderr, PROG ": %s: unresolvable catalog dir_id %" PRIu64 "\n", sh->path, d);
                free(l2g);
                crawl_bin_catalog_free(&cat);
                free(stack.ids);
                return -1;
            }
        }
        crawl_bin_catalog_free(&cat);
        l2g_out[i] = l2g;
    }

    free(stack.ids);
    vlog("merged %" PRIu32 " directories from %zu shard catalog(s) in %.2fs\n", g_dir_count, g_cr.shard_count,
         now_sec() - t0);
    return 0;
}

/* ---------------------------------------------------------------------------
 * Phase 2: parallel record scan
 * ------------------------------------------------------------------------- */

typedef struct {
    ment_t *ents;
    uint64_t count;
    uint64_t cap;
    char *names;
    uint64_t names_len;
    uint64_t names_cap;
    uint64_t names_base; /* offset of this buffer inside the merged g_names */
    uint64_t skipped;
    int failed;
} scan_buf_t;

typedef struct {
    crawl_bin_file_chunk_t *chunks;
    uint32_t **l2g;
    uint64_t *l2g_len;
    const unsigned char *in_subtree; /* NULL when the whole tree is mounted */
    scan_buf_t *bufs;
    _Atomic int failed;
} scan_ctx_t;

static const crawl_bin_chunk_stdio_t g_io = {fopen, fread, fclose};

static int scan_buf_push(scan_buf_t *b, const ment_t *e, const unsigned char *name, size_t name_len) {
    if (b->count == b->cap) {
        uint64_t ncap = b->cap ? b->cap * 2 : 4096;
        ment_t *nx = (ment_t *)realloc(b->ents, (size_t)ncap * sizeof(*nx));

        if (!nx) return -1;
        b->ents = nx;
        b->cap = ncap;
    }
    if (b->names_len + name_len > b->names_cap) {
        uint64_t ncap = b->names_cap ? b->names_cap * 2 : (1ULL << 16);
        char *nx;

        while (ncap < b->names_len + name_len) ncap *= 2;
        nx = (char *)realloc(b->names, (size_t)ncap);
        if (!nx) return -1;
        b->names = nx;
        b->names_cap = ncap;
    }
    b->ents[b->count] = *e;
    b->ents[b->count].name_off = b->names_len; /* rebased to g_names during the scatter */
    if (name_len) memcpy(b->names + b->names_len, name, name_len);
    b->names_len += name_len;
    b->count++;
    return 0;
}

static void scan_body(void *ctxv, size_t begin, size_t end, int tid) {
    scan_ctx_t *ctx = (scan_ctx_t *)ctxv;
    scan_buf_t *buf = &ctx->bufs[tid];

    for (size_t ci = begin; ci < end && !atomic_load(&ctx->failed); ci++) {
        const crawl_bin_file_chunk_t *chunk = &ctx->chunks[ci];
        const uint32_t *l2g = ctx->l2g[chunk->file_index];
        uint64_t l2g_len = ctx->l2g_len[chunk->file_index];
        crawl_bin_block_reader_t br;
        FILE *fp;

        fp = fopen(chunk->path, "rb");
        if (!fp) {
            fprintf(stderr, PROG ": %s: %s\n", chunk->path, strerror(errno));
            atomic_store(&ctx->failed, 1);
            return;
        }
        if (crawl_bin_block_reader_init(&br, &g_io, fp, chunk->start_offset, chunk->end_offset) != 0) {
            fprintf(stderr, PROG ": %s: cannot read records at offset %" PRIu64 "\n", chunk->path,
                    chunk->start_offset);
            fclose(fp);
            atomic_store(&ctx->failed, 1);
            return;
        }
        /* The in-memory index holds neither gid nor mode, so those two columns
         * are never decoded. */
        (void)crawl_bin_block_reader_set_projection(
            &br, CRAWL_PROJECTION_ALL & ~(CRAWL_COL_BIT(CRAWL_COL_GID) | CRAWL_COL_BIT(CRAWL_COL_MODE)));

        for (;;) {
            bin_record_hdr_t r;
            const unsigned char *name = NULL;
            uint32_t parent;
            ment_t e;
            int got = crawl_bin_block_reader_next(&br, &r, &name);

            if (got == 0) break;
            if (got < 0) {
                fprintf(stderr, PROG ": %s: corrupt record stream\n", chunk->path);
                atomic_store(&ctx->failed, 1);
                break;
            }
            if (r.parent_dir_id == 0ULL || r.parent_dir_id >= l2g_len) {
                buf->skipped++;
                continue;
            }
            parent = l2g[r.parent_dir_id];
            if (parent == MDIR_NONE) {
                buf->skipped++;
                continue;
            }
            if (ctx->in_subtree && !ctx->in_subtree[parent]) continue;

            e.name_off = 0;
            e.size = r.size;
            e.ino = r.inode;
            e.nlink = r.nlink;
            e.atime = (int64_t)r.atime;
            e.mtime = (int64_t)r.mtime;
            e.ctime = (int64_t)r.ctime;
            e.parent = parent;
            e.uid = (uint32_t)r.uid;
            e.name_len = r.name_len;
            e.type = r.type;
            e.pad = 0;
            /* A directory's own g_dirs row exists only if it has children; the
             * catalog has no row for a leaf directory. MDIR_NONE therefore
             * means "empty directory", not an error. */
            e.dir_idx = (r.type == (uint8_t)'d') ? dir_lookup(parent, (const char *)name, r.name_len) : MDIR_NONE;

            if (scan_buf_push(buf, &e, name, r.name_len) != 0) {
                fprintf(stderr, PROG ": out of memory indexing records\n");
                buf->failed = 1;
                atomic_store(&ctx->failed, 1);
                break;
            }
        }
        crawl_bin_block_reader_free(&br);
        fclose(fp);
    }
}

/* ---------------------------------------------------------------------------
 * Phase 3: bucket entries by parent, then sort each directory by name
 * ------------------------------------------------------------------------- */

typedef struct {
    scan_buf_t *bufs;
    _Atomic uint32_t *cursor; /* per-directory write cursor into g_ents */
} scatter_ctx_t;

static void count_body(void *ctxv, size_t begin, size_t end, int tid) {
    scatter_ctx_t *ctx = (scatter_ctx_t *)ctxv;
    (void)tid;

    for (size_t bi = begin; bi < end; bi++) {
        const scan_buf_t *b = &ctx->bufs[bi];

        for (uint64_t i = 0; i < b->count; i++)
            atomic_fetch_add_explicit(&ctx->cursor[b->ents[i].parent], 1, memory_order_relaxed);
    }
}

static void scatter_body(void *ctxv, size_t begin, size_t end, int tid) {
    scatter_ctx_t *ctx = (scatter_ctx_t *)ctxv;
    (void)tid;

    for (size_t bi = begin; bi < end; bi++) {
        const scan_buf_t *b = &ctx->bufs[bi];

        for (uint64_t i = 0; i < b->count; i++) {
            const ment_t *src = &b->ents[i];
            uint32_t slot = atomic_fetch_add_explicit(&ctx->cursor[src->parent], 1, memory_order_relaxed);
            ment_t *dst = &g_ents[g_dirs[src->parent].first_ent + slot];

            *dst = *src;
            dst->name_off = b->names_base + src->name_off;
        }
    }
}

static int cmp_ent_name(const void *a, const void *b) {
    const ment_t *x = (const ment_t *)a;
    const ment_t *y = (const ment_t *)b;
    size_t n = x->name_len < y->name_len ? x->name_len : y->name_len;
    int c = memcmp(g_names + x->name_off, g_names + y->name_off, n);

    if (c != 0) return c;
    if (x->name_len < y->name_len) return -1;
    if (x->name_len > y->name_len) return 1;
    return 0;
}

static void sort_dirs_body(void *ctxv, size_t begin, size_t end, int tid) {
    (void)ctxv;
    (void)tid;

    for (size_t d = begin; d < end; d++) {
        if (g_dirs[d].ent_count > 1)
            qsort(&g_ents[g_dirs[d].first_ent], g_dirs[d].ent_count, sizeof(ment_t), cmp_ent_name);
    }
}

/*
 * Entries arrive grouped by shard, not by directory. Bucketing by parent is a
 * counting sort (O(n), and the histogram is exactly the per-directory child
 * count readdir needs); only the small within-directory name sorts remain, and
 * those are independent so they parallelize perfectly.
 */
static int finalize_index(scan_buf_t *bufs, int nbufs) {
    _Atomic uint32_t *cursor;
    scatter_ctx_t sctx;
    uint64_t total = 0;
    uint64_t names_total = 0;
    uint64_t off;
    double t0 = now_sec();

    for (int i = 0; i < nbufs; i++) {
        if (bufs[i].failed) return -1;
        total += bufs[i].count;
        names_total += bufs[i].names_len;
    }

    g_ents = (ment_t *)malloc((size_t)total * sizeof(*g_ents) + 1);
    g_names = (char *)malloc((size_t)names_total + 1);
    cursor = (_Atomic uint32_t *)calloc((size_t)g_dir_count, sizeof(*cursor));
    if (!g_ents || !g_names || !cursor) {
        fprintf(stderr, PROG ": out of memory building the index (%" PRIu64 " entries)\n", total);
        free(cursor);
        return -1;
    }
    g_ent_count = total;
    g_names_len = names_total;

    /* Concatenate the per-thread name arenas, freeing each as it is copied so
     * peak footprint stays at one full arena plus one thread's worth. */
    off = 0;
    for (int i = 0; i < nbufs; i++) {
        bufs[i].names_base = off;
        if (bufs[i].names_len) memcpy(g_names + off, bufs[i].names, (size_t)bufs[i].names_len);
        off += bufs[i].names_len;
        free(bufs[i].names);
        bufs[i].names = NULL;
    }

    sctx.bufs = bufs;
    sctx.cursor = cursor;

    if (par_for((size_t)nbufs, 1, g_threads, count_body, &sctx) != 0) goto oom;

    off = 0;
    for (uint32_t d = 0; d < g_dir_count; d++) {
        uint32_t c = atomic_load_explicit(&cursor[d], memory_order_relaxed);

        g_dirs[d].first_ent = off;
        g_dirs[d].ent_count = c;
        off += c;
        atomic_store_explicit(&cursor[d], 0, memory_order_relaxed);
    }

    if (par_for((size_t)nbufs, 1, g_threads, scatter_body, &sctx) != 0) goto oom;

    for (int i = 0; i < nbufs; i++) {
        free(bufs[i].ents);
        bufs[i].ents = NULL;
    }
    free(cursor);

    if (par_for((size_t)g_dir_count, 256, g_threads, sort_dirs_body, NULL) != 0) return -1;

    for (uint64_t i = 0; i < g_ent_count; i++) g_total_bytes += g_ents[i].size;

    vlog("indexed %" PRIu64 " records, %" PRIu32 " directories in %.2fs\n", g_ent_count, g_dir_count,
         now_sec() - t0);
    return 0;

oom:
    fprintf(stderr, PROG ": failed to start index worker threads\n");
    free(cursor);
    return -1;
}

/* ---------------------------------------------------------------------------
 * Path resolution
 * ------------------------------------------------------------------------- */

/* Binary search a directory's name-sorted child records. */
static uint64_t dir_find_ent(uint32_t dir, const char *name, size_t name_len) {
    const mdir_t *d = &g_dirs[dir];
    uint64_t lo = 0, hi = d->ent_count;

    while (lo < hi) {
        uint64_t mid = lo + (hi - lo) / 2;
        const ment_t *e = &g_ents[d->first_ent + mid];
        size_t n = e->name_len < name_len ? e->name_len : name_len;
        int c = memcmp(g_names + e->name_off, name, n);

        if (c == 0) {
            if (e->name_len == name_len) return d->first_ent + mid;
            c = e->name_len < name_len ? -1 : 1;
        }
        if (c < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return ENT_NONE;
}

typedef struct {
    uint64_t ent; /* ENT_NONE when no record exists for this path */
    uint32_t dir; /* MDIR_NONE when the path is not a directory */
} resolved_t;

/* Walk a slash-separated path of directory components starting at `from`. */
static uint32_t walk_dirs(uint32_t from, const char *p, const char *end) {
    uint32_t cur = from;

    while (p < end) {
        const char *slash;
        size_t len;

        while (p < end && *p == '/') p++;
        if (p >= end) break;
        slash = memchr(p, '/', (size_t)(end - p));
        len = slash ? (size_t)(slash - p) : (size_t)(end - p);
        cur = dir_lookup(cur, p, len);
        if (cur == MDIR_NONE) return MDIR_NONE;
        p = slash ? slash : end;
    }
    return cur;
}

static int resolve_path(const char *path, resolved_t *out) {
    const char *base;
    const char *slash;
    size_t base_len;
    uint32_t parent;
    uint64_t ent;

    while (*path == '/') path++;
    if (*path == '\0') {
        out->ent = ENT_NONE;
        out->dir = g_root_dir;
        return 0;
    }

    slash = strrchr(path, '/');
    if (slash) {
        parent = walk_dirs(g_root_dir, path, slash);
        base = slash + 1;
    } else {
        parent = g_root_dir;
        base = path;
    }
    if (parent == MDIR_NONE) return -1;
    base_len = strlen(base);
    if (base_len == 0) { /* trailing slash: the parent itself */
        out->ent = ENT_NONE;
        out->dir = parent;
        return 0;
    }

    ent = dir_find_ent(parent, base, base_len);
    if (ent != ENT_NONE) {
        out->ent = ent;
        out->dir = (g_ents[ent].type == (uint8_t)'d') ? g_ents[ent].dir_idx : MDIR_NONE;
        /* A directory record whose catalog row was never created has no
         * children; give it an empty dir so readdir succeeds. */
        return 0;
    }

    /* No record, but the catalog knows the path: a synthetic ancestor of the
     * crawl root (e.g. "/home" when only /home/users was crawled). */
    out->dir = dir_lookup(parent, base, base_len);
    if (out->dir == MDIR_NONE) return -1;
    out->ent = ENT_NONE;
    return 0;
}

/* ---------------------------------------------------------------------------
 * FUSE operations
 * ------------------------------------------------------------------------- */

static mode_t synth_mode(uint8_t type) {
    switch (type) {
        case 'd':
            return S_IFDIR | 0555;
        case 'l':
            return S_IFLNK | 0777;
        case 'c':
            return S_IFCHR | 0444;
        case 'b':
            return S_IFBLK | 0444;
        case 'p':
            return S_IFIFO | 0444;
        case 's':
            return S_IFSOCK | 0444;
        default:
            return S_IFREG | 0444;
    }
}

static void fill_stat_ent(const ment_t *e, struct stat *st) {
    memset(st, 0, sizeof(*st));
    st->st_mode = synth_mode(e->type);
    st->st_ino = (ino_t)e->ino;
    st->st_nlink = (nlink_t)(e->nlink ? e->nlink : 1);
    st->st_uid = (uid_t)e->uid;
    st->st_gid = g_synth_gid;
    st->st_size = (off_t)e->size;
    /* st_blocks is not recorded; approximate it from the apparent size so
     * plain `du` is in the right ballpark instead of reporting zero. */
    st->st_blocks = (blkcnt_t)((e->size + 511) / 512);
    st->st_atime = (time_t)e->atime;
    st->st_mtime = (time_t)e->mtime;
    st->st_ctime = (time_t)e->ctime;
}

/*
 * Directories above the crawl root have no record and therefore no recorded
 * inode. They still need a distinct nonzero one: readdir consumers treat
 * d_ino == 0 as a deleted entry and skip it, which would hide the whole path
 * spine leading down to the crawled tree. Counting down from UINT64_MAX keeps
 * these clear of any real inode number.
 */
static ino_t synth_dir_ino(uint32_t dir) { return (ino_t)(UINT64_MAX - (uint64_t)dir); }

static void fill_stat_synth_dir(uint32_t dir, struct stat *st) {
    memset(st, 0, sizeof(*st));
    st->st_mode = S_IFDIR | 0555;
    st->st_ino = synth_dir_ino(dir);
    st->st_nlink = 2;
    st->st_uid = g_mount_uid;
    st->st_gid = g_synth_gid;
    st->st_atime = st->st_mtime = st->st_ctime = g_synth_time;
}

static int em_getattr(const char *path, struct stat *st) {
    resolved_t r;

    if (resolve_path(path, &r) != 0) return -ENOENT;
    if (r.ent != ENT_NONE)
        fill_stat_ent(&g_ents[r.ent], st);
    else
        fill_stat_synth_dir(r.dir, st);
    return 0;
}

static int em_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t off,
                      struct fuse_file_info *fi) {
    resolved_t r;
    const mdir_t *d;
    struct stat st;

    (void)off;
    (void)fi;

    if (resolve_path(path, &r) != 0) return -ENOENT;
    if (r.dir == MDIR_NONE) {
        /* A directory record with no catalog row is an empty directory; any
         * other record type is simply not a directory. */
        if (r.ent != ENT_NONE && g_ents[r.ent].type == (uint8_t)'d') {
            filler(buf, ".", NULL, 0);
            filler(buf, "..", NULL, 0);
            return 0;
        }
        return -ENOTDIR;
    }

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    d = &g_dirs[r.dir];
    for (uint32_t i = 0; i < d->ent_count; i++) {
        const ment_t *e = &g_ents[d->first_ent + i];
        char name[NAME_MAX + 1];

        if (e->name_len > NAME_MAX) continue;
        memcpy(name, g_names + e->name_off, e->name_len);
        name[e->name_len] = '\0';
        fill_stat_ent(e, &st);
        if (filler(buf, name, &st, 0) != 0) return 0;
    }

    /* Child directories that have no record of their own: the synthetic spine
     * above the crawl root. Inside the crawl every directory has a record, so
     * this normally adds nothing. */
    for (uint32_t c = d->first_child; c != MDIR_NONE; c = g_dirs[c].next_sibling) {
        const mdir_t *cd = &g_dirs[c];
        char name[NAME_MAX + 1];

        if (cd->name_len == 0 || cd->name_len > NAME_MAX) continue;
        if (dir_find_ent(r.dir, g_dir_names + cd->name_off, cd->name_len) != ENT_NONE) continue;
        memcpy(name, g_dir_names + cd->name_off, cd->name_len);
        name[cd->name_len] = '\0';
        fill_stat_synth_dir(c, &st);
        if (filler(buf, name, &st, 0) != 0) return 0;
    }
    return 0;
}

static int em_open(const char *path, struct fuse_file_info *fi) {
    resolved_t r;

    if ((fi->flags & O_ACCMODE) != O_RDONLY) return -EROFS;
    if (resolve_path(path, &r) != 0) return -ENOENT;
    if (r.ent == ENT_NONE) return -EISDIR;
    if (g_ents[r.ent].type == (uint8_t)'d') return -EISDIR;
    return 0;
}

/*
 * Contents are not part of a crawl. Returning zeros keeps every size-derived
 * tool honest (wc -c, dd, du) at the cost of `cat` and checksums seeing a
 * file of NULs rather than an error.
 */
static int em_read(const char *path, char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
    resolved_t r;
    uint64_t fsize;

    (void)fi;
    if (resolve_path(path, &r) != 0) return -ENOENT;
    if (r.ent == ENT_NONE) return -EISDIR;

    fsize = g_ents[r.ent].size;
    if (off < 0) return -EINVAL;
    if ((uint64_t)off >= fsize) return 0;
    if (size > fsize - (uint64_t)off) size = (size_t)(fsize - (uint64_t)off);
    if (size > (size_t)INT_MAX) size = (size_t)INT_MAX; /* the return value is an int */
    memset(buf, 0, size);
    return (int)size;
}

/* Symlink targets are not recorded, so the type is known but the target is not. */
static int em_readlink(const char *path, char *buf, size_t size) {
    resolved_t r;

    (void)buf;
    (void)size;
    if (resolve_path(path, &r) != 0) return -ENOENT;
    if (r.ent == ENT_NONE || g_ents[r.ent].type != (uint8_t)'l') return -EINVAL;
    return -EIO;
}

static int em_statfs(const char *path, struct statvfs *sv) {
    (void)path;
    memset(sv, 0, sizeof(*sv));
    sv->f_bsize = 512;
    sv->f_frsize = 512;
    sv->f_blocks = (fsblkcnt_t)((g_total_bytes + 511) / 512);
    sv->f_bfree = 0;
    sv->f_bavail = 0;
    sv->f_files = (fsfilcnt_t)g_ent_count;
    sv->f_ffree = 0;
    sv->f_favail = 0;
    sv->f_namemax = NAME_MAX;
    return 0;
}

static int em_access(const char *path, int mask) {
    resolved_t r;
    mode_t mode;

    if (resolve_path(path, &r) != 0) return -ENOENT;
    if (mask & W_OK) return -EACCES;
    mode = (r.ent != ENT_NONE) ? synth_mode(g_ents[r.ent].type) : (S_IFDIR | 0555);
    if ((mask & X_OK) && !S_ISDIR(mode)) return -EACCES;
    return 0;
}

static struct fuse_operations em_ops = {
    .getattr = em_getattr,
    .readdir = em_readdir,
    .open = em_open,
    .read = em_read,
    .readlink = em_readlink,
    .statfs = em_statfs,
    .access = em_access,
};

/* ---------------------------------------------------------------------------
 * Index build driver
 * ------------------------------------------------------------------------- */

static unsigned char *build_subtree_filter(const char *subtree) {
    unsigned char *in;
    uint32_t target;
    size_t len = strlen(subtree);

    while (len > 1 && subtree[len - 1] == '/') len--;
    target = walk_dirs(0, subtree, subtree + len);
    if (target == MDIR_NONE) {
        fprintf(stderr, PROG ": --subtree %s is not a directory in this crawl\n", subtree);
        return NULL;
    }

    in = (unsigned char *)calloc((size_t)g_dir_count, 1);
    if (!in) {
        fprintf(stderr, PROG ": out of memory\n");
        return NULL;
    }
    /* dir_intern always creates a parent before its children, so a single
     * ascending pass propagates membership down the tree. */
    in[target] = 1;
    g_dirs_mounted = 1;
    for (uint32_t d = 0; d < g_dir_count; d++) {
        if (in[d]) continue;
        if (d > target && g_dirs[d].parent != MDIR_NONE && in[g_dirs[d].parent]) {
            in[d] = 1;
            g_dirs_mounted++;
        }
    }
    g_root_dir = target;
    return in;
}

static int build_index(const char *subtree) {
    uint32_t **l2g;
    uint64_t *l2g_len;
    crawl_bin_file_chunk_t *chunks = NULL;
    size_t chunk_count = 0, chunk_cap = 0;
    unsigned char *in_subtree = NULL;
    scan_ctx_t sctx;
    scan_buf_t *bufs;
    double t0;
    int rc = -1;

    l2g = (uint32_t **)calloc(g_cr.shard_count ? g_cr.shard_count : 1, sizeof(*l2g));
    l2g_len = (uint64_t *)calloc(g_cr.shard_count ? g_cr.shard_count : 1, sizeof(*l2g_len));
    bufs = (scan_buf_t *)calloc((size_t)g_threads, sizeof(*bufs));
    if (!l2g || !l2g_len || !bufs) {
        fprintf(stderr, PROG ": out of memory\n");
        goto done;
    }

    if (build_dir_namespace(l2g, l2g_len) != 0) goto done;

    g_dirs_mounted = g_dir_count;
    if (subtree) {
        in_subtree = build_subtree_filter(subtree);
        if (!in_subtree) goto done;
    }

    /* Split each shard's record region at its checkpoint boundaries so workers
     * get roughly equal byte ranges instead of one job per shard. */
    t0 = now_sec();
    for (size_t i = 0; i < g_cr.shard_count; i++) {
        const crawl_result_shard_t *sh = &g_cr.shards[i];
        uint64_t *offs = NULL;
        size_t n_offs = 0;

        if (crawl_bin_load_ckpt(&g_io, sh->path, sh->catalog_offset, &offs, &n_offs) == 0 && n_offs > 0) {
            for (size_t k = 0; k < n_offs; k++) {
                uint64_t seg_start = offs[k];
                uint64_t seg_end = (k + 1 < n_offs) ? offs[k + 1] : sh->catalog_offset;

                if (seg_start >= seg_end) continue;
                if (crawl_bin_append_chunk(&chunks, &chunk_count, &chunk_cap, sh->path, seg_start, seg_end, i) !=
                    0) {
                    fprintf(stderr, PROG ": out of memory building scan jobs\n");
                    free(offs);
                    goto done;
                }
            }
            free(offs);
        } else {
            free(offs);
            if (sh->catalog_offset > sizeof(bin_file_header_t) &&
                crawl_bin_append_chunk(&chunks, &chunk_count, &chunk_cap, sh->path, sizeof(bin_file_header_t),
                                       sh->catalog_offset, i) != 0) {
                fprintf(stderr, PROG ": out of memory building scan jobs\n");
                goto done;
            }
        }
    }
    vlog("built %zu scan job(s) in %.2fs\n", chunk_count, now_sec() - t0);

    sctx.chunks = chunks;
    sctx.l2g = l2g;
    sctx.l2g_len = l2g_len;
    sctx.in_subtree = in_subtree;
    sctx.bufs = bufs;
    atomic_init(&sctx.failed, 0);

    t0 = now_sec();
    if (par_for(chunk_count, 1, g_threads, scan_body, &sctx) != 0) {
        fprintf(stderr, PROG ": failed to start scan threads\n");
        goto done;
    }
    if (atomic_load(&sctx.failed)) goto done;
    vlog("scanned %zu job(s) in %.2fs\n", chunk_count, now_sec() - t0);

    for (int i = 0; i < g_threads; i++) g_stat_records_skipped += bufs[i].skipped;

    if (finalize_index(bufs, g_threads) != 0) goto done;
    rc = 0;

done:
    if (rc != 0) {
        for (int i = 0; i < g_threads && bufs; i++) {
            free(bufs[i].ents);
            free(bufs[i].names);
        }
    }
    for (size_t i = 0; i < g_cr.shard_count && l2g; i++) free(l2g[i]);
    free(l2g);
    free(l2g_len);
    free(bufs);
    free(in_subtree);
    crawl_bin_free_chunk_array_rows(chunks, chunk_count); /* frees the array too */
    return rc;
}

/* ---------------------------------------------------------------------------
 * CLI
 * ------------------------------------------------------------------------- */

static void usage(void) {
    fprintf(stderr,
            "Usage: " PROG " [options] <crawl-dir> <mountpoint>\n"
            "       " PROG " -o path=<crawl-dir> [options] <mountpoint>\n"
            "       " PROG " --dry-run [options] <crawl-dir>\n"
            "\n"
            "Mount an ecrawl crawl result as a read-only, metadata-only filesystem.\n"
            "Unmount with: fusermount -u <mountpoint>\n"
            "\n"
            "Options:\n"
            "  -o path=DIR        crawl output directory (alternative to the positional form)\n"
            "  -o subtree=PATH    mount only this subtree, which also shrinks the index\n"
            "  -o gid=N           st_gid to report (default 0; the mount index omits gid)\n"
            "  -o threads=N       index build threads (default %d, or " "ECRAWL_MOUNT_THREADS" ")\n"
            "  -o allow_other     needs user_allow_other in /etc/fuse.conf\n"
            "  --subtree PATH     same as -o subtree=PATH\n"
            "  --dry-run          build the index, print key=value stats, do not mount\n"
            "  -f                 stay in the foreground\n"
            "  -d                 FUSE debug output (implies -f)\n"
            "  -s                 single-threaded FUSE event loop\n"
            "  -v, --verbose      report index build progress on stderr\n"
            "  -h, --help         this message\n"
            "\n"
            "The view is metadata-only: read() returns zeros up to the recorded size,\n"
            "permissions are synthesized (0555 dirs / 0444 files) because the mount index\n"
            "omits mode and gid, and readlink() fails with EIO because symlink targets are\n"
            "not recorded. Sizes, timestamps, uid, nlink and inode numbers are exact.\n",
            DEFAULT_MOUNT_THREADS);
}

/* Grown string, for assembling the option list handed to libfuse. */
typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} strbuf_t;

static int sb_add(strbuf_t *sb, const char *s) {
    size_t n = strlen(s);

    if (sb->len + n + 1 > sb->cap) {
        size_t ncap = sb->cap ? sb->cap * 2 : 256;
        char *nx;

        while (ncap < sb->len + n + 1) ncap *= 2;
        nx = (char *)realloc(sb->buf, ncap);
        if (!nx) return -1;
        sb->buf = nx;
        sb->cap = ncap;
    }
    memcpy(sb->buf + sb->len, s, n + 1);
    sb->len += n;
    return 0;
}

static int sb_addf(strbuf_t *sb, const char *fmt, ...) {
    char tmp[512];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    return sb_add(sb, tmp);
}

/*
 * mount(8) passes filesystem-independent flags down to /sbin/mount.<type>, and
 * with -s expects the helper to ignore what it does not understand. Swallow
 * those quietly so the same binary works as a mount helper; anything else is
 * forwarded to libfuse, which reports genuinely bad options.
 */
static int is_ignorable_mount_opt(const char *o) {
    static const char *ignore[] = {"rw",   "ro",     "auto",  "noauto", "user",  "users",  "nouser",
                                   "owner", "group", "_netdev", "defaults", "exec", "noexec", "suid",
                                   "nosuid", "dev",  "nodev", "atime",  "noatime", "diratime",
                                   "nodiratime", "relatime", "strictatime", "sync", "async", "dirsync",
                                   "mand", "nomand", "silent", "loud", "iversion", "noiversion",
                                   "lazytime", "nolazytime", "nofail", "x-systemd.automount"};

    for (size_t i = 0; i < sizeof(ignore) / sizeof(ignore[0]); i++)
        if (strcmp(o, ignore[i]) == 0) return 1;
    /* x-* and comment= are conventionally helper-ignorable. */
    if (strncmp(o, "x-", 2) == 0) return 1;
    if (strncmp(o, "comment=", 8) == 0) return 1;
    return 0;
}

int main(int argc, char **argv) {
    const char *crawl_dir = NULL;
    const char *mountpoint = NULL;
    const char *subtree = NULL;
    char *opt_path = NULL;    /* backing store for -o path= */
    char *opt_subtree = NULL; /* backing store for -o subtree= */
    char *positional[4];
    size_t npositional = 0;
    strbuf_t fuse_opts = {NULL, 0, 0};
    int foreground = 0, debug = 0, single = 0, dry_run = 0;
    char *fargv[8];
    int fargc = 0;
    struct stat dst;
    double t0 = now_sec();
    const char *env;
    int rc;

    g_mount_uid = getuid();
    g_synth_gid = 0;

    env = getenv("ECRAWL_MOUNT_THREADS");
    if (env && *env) {
        long v = strtol(env, NULL, 10);
        if (v > 0 && v <= 4096) g_threads = (int)v;
    }

    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];

        if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
            usage();
            return 0;
        } else if (strcmp(a, "-v") == 0 || strcmp(a, "--verbose") == 0) {
            g_verbose = 1;
        } else if (strcmp(a, "-f") == 0) {
            foreground = 1;
        } else if (strcmp(a, "-d") == 0) {
            debug = 1;
            foreground = 1;
        } else if (strcmp(a, "-s") == 0) {
            single = 1;
        } else if (strcmp(a, "-n") == 0) {
            /* mount(8): do not update /etc/mtab. Nothing for us to do. */
        } else if (strcmp(a, "--subtree") == 0) {
            if (++i >= argc) {
                fprintf(stderr, PROG ": --subtree needs a path\n");
                return 2;
            }
            subtree = argv[i];
        } else if (strcmp(a, "--dry-run") == 0) {
            dry_run = 1;
        } else if (strcmp(a, "-o") == 0) {
            char *list, *saveptr = NULL, *tok;

            if (++i >= argc) {
                fprintf(stderr, PROG ": -o needs an option list\n");
                return 2;
            }
            list = strdup(argv[i]);
            if (!list) return 1;
            /* Values are copied out of the tokenized list, which is freed
             * below. A path containing a comma cannot be expressed here at all
             * (comma is the option separator); use the positional form. */
            for (tok = strtok_r(list, ",", &saveptr); tok; tok = strtok_r(NULL, ",", &saveptr)) {
                if (strncmp(tok, "path=", 5) == 0) {
                    free(opt_path);
                    opt_path = strdup(tok + 5);
                    if (!opt_path) return 1;
                    crawl_dir = opt_path;
                } else if (strncmp(tok, "subtree=", 8) == 0) {
                    free(opt_subtree);
                    opt_subtree = strdup(tok + 8);
                    if (!opt_subtree) return 1;
                    subtree = opt_subtree;
                } else if (strncmp(tok, "gid=", 4) == 0) {
                    g_synth_gid = (gid_t)strtoul(tok + 4, NULL, 10);
                } else if (strncmp(tok, "threads=", 8) == 0) {
                    long v = strtol(tok + 8, NULL, 10);
                    if (v > 0 && v <= 4096) g_threads = (int)v;
                } else if (is_ignorable_mount_opt(tok)) {
                    /* filesystem-independent mount(8) flag; not ours */
                } else {
                    if (fuse_opts.len && sb_add(&fuse_opts, ",") != 0) return 1;
                    if (sb_add(&fuse_opts, tok) != 0) return 1;
                }
            }
            free(list);
        } else if (a[0] == '-' && a[1] != '\0') {
            fprintf(stderr, PROG ": unknown option %s (try --help)\n", a);
            return 2;
        } else {
            if (npositional < sizeof(positional) / sizeof(positional[0])) positional[npositional++] = argv[i];
            else {
                fprintf(stderr, PROG ": too many arguments\n");
                return 2;
            }
        }
    }

    /*
     * Positional forms:
     *   <crawl-dir> <mountpoint>              normal use
     *   <mountpoint>                          with -o path=
     *   <spec> <mountpoint>                   mount(8) helper; spec ignored when
     *                                         -o path= supplied it instead
     */
    if (dry_run) {
        /* No mountpoint: the crawl dir is the only positional argument. */
        if (!crawl_dir && npositional == 1)
            crawl_dir = positional[0];
        else if (!(crawl_dir && npositional == 0)) {
            usage();
            return 2;
        }
    } else if (crawl_dir && npositional == 1) {
        mountpoint = positional[0];
    } else if (crawl_dir && npositional == 2) {
        mountpoint = positional[1];
    } else if (!crawl_dir && npositional == 2) {
        crawl_dir = positional[0];
        mountpoint = positional[1];
    } else {
        usage();
        return 2;
    }

    if (stat(crawl_dir, &dst) != 0) {
        fprintf(stderr, PROG ": %s: %s\n", crawl_dir, strerror(errno));
        return 1;
    }
    if (!S_ISDIR(dst.st_mode)) {
        fprintf(stderr, PROG ": %s is not a directory\n", crawl_dir);
        return 1;
    }
    /* Check the mountpoint before building the index: on a large crawl that is
     * minutes of work, and libfuse would only reject the path afterwards. */
    if (mountpoint) {
        struct stat mst;

        if (stat(mountpoint, &mst) != 0) {
            fprintf(stderr, PROG ": %s: %s\n", mountpoint, strerror(errno));
            return 1;
        }
        if (!S_ISDIR(mst.st_mode)) {
            fprintf(stderr, PROG ": %s is not a directory\n", mountpoint);
            return 1;
        }
    }
    /* Directories above the crawl root have no record of their own; date them
     * from the crawl output rather than showing the epoch. */
    g_synth_time = dst.st_mtime;

    if (crawl_result_open(crawl_dir, &g_cr) != 0) return 1;
    if (g_cr.shard_count == 0) {
        fprintf(stderr, PROG ": %s: no finalized uid_shard_*.bin found", crawl_dir);
        if (g_cr.skipped_incomplete)
            fprintf(stderr, " (%zu shard(s) still being written)", g_cr.skipped_incomplete);
        fprintf(stderr, "\n");
        crawl_result_free(&g_cr);
        return 1;
    }
    vlog("%zu shard(s) in %s%s\n", g_cr.shard_count, crawl_dir,
         crawl_result_stored_root(&g_cr) ? "" : " (no crawl_manifest.txt)");
    if (g_cr.skipped_incomplete)
        fprintf(stderr, PROG ": warning: skipping %zu shard(s) still being written\n", g_cr.skipped_incomplete);

    if (build_index(subtree) != 0) {
        crawl_result_free(&g_cr);
        return 1;
    }
    if (g_stat_records_skipped)
        fprintf(stderr, PROG ": warning: skipped %" PRIu64 " record(s) with an unresolvable parent\n",
                g_stat_records_skipped);

    {
        char szbuf[32], membuf[32];
        uint64_t mem = g_ent_count * sizeof(ment_t) + g_names_len + (uint64_t)g_dir_count * sizeof(mdir_t) +
                       g_dir_names_len + g_dir_ht_size * sizeof(uint32_t);

        if (dry_run) {
            /* key=value on stdout, like the other tools, so scripts and the
             * test harness can check an index without mounting anything. */
            printf(PROG "\n");
            printf("crawl_dir=%s\n", crawl_dir);
            printf("shards=%zu\n", g_cr.shard_count);
            printf("shards_incomplete=%zu\n", g_cr.skipped_incomplete);
            printf("shards_unreadable=%zu\n", g_cr.skipped_unreadable);
            printf("records_total=%" PRIu64 "\n", g_ent_count);
            printf("directories=%" PRIu32 "\n", g_dirs_mounted);
            printf("bytes_total=%" PRIu64 "\n", g_total_bytes);
            printf("records_skipped=%" PRIu64 "\n", g_stat_records_skipped);
            printf("index_memory_bytes=%" PRIu64 "\n", mem);
            printf("elapsed_sec=%.3f\n", now_sec() - t0);
            free(fuse_opts.buf);
            free(opt_path);
            free(opt_subtree);
            free(g_ents);
            free(g_names);
            free(g_dirs);
            free(g_dir_names);
            free(g_dir_ht);
            crawl_result_free(&g_cr);
            return 0;
        }

        human_bytes(g_total_bytes, szbuf, sizeof(szbuf));
        human_bytes(mem, membuf, sizeof(membuf));
        fprintf(stderr,
                PROG ": ready in %.2fs: %" PRIu64 " entries, %" PRIu32 " directories, %s of file data, %s "
                     "index memory\n",
                now_sec() - t0, g_ent_count, g_dirs_mounted, szbuf, membuf);
    }

    /*
     * Immutable data, so cache aggressively; ro so the kernel rejects writes
     * before they reach us; subtype makes /proc/mounts read "fuse.ecrawl".
     */
    {
        strbuf_t o = {NULL, 0, 0};

        if (sb_addf(&o, "ro,use_ino,kernel_cache,entry_timeout=%d,attr_timeout=%d,negative_timeout=%d,subtype=ecrawl",
                    CACHE_TIMEOUT_SEC, CACHE_TIMEOUT_SEC, CACHE_TIMEOUT_SEC) != 0)
            return 1;
        /* fsname is comma-separated inside -o, so only pass paths without one. */
        if (!strchr(crawl_dir, ',') && sb_addf(&o, ",fsname=%s", crawl_dir) != 0) return 1;
        if (fuse_opts.len) {
            if (sb_add(&o, ",") != 0) return 1;
            if (sb_add(&o, fuse_opts.buf) != 0) return 1;
        }

        fargv[fargc++] = argv[0];
        fargv[fargc++] = (char *)mountpoint;
        fargv[fargc++] = (char *)"-o";
        fargv[fargc++] = o.buf;
        if (foreground) fargv[fargc++] = (char *)"-f";
        if (debug) fargv[fargc++] = (char *)"-d";
        if (single) fargv[fargc++] = (char *)"-s";
        fargv[fargc] = NULL;

        rc = fuse_main(fargc, fargv, &em_ops, NULL);
        free(o.buf);
    }

    free(fuse_opts.buf);
    free(opt_path);
    free(opt_subtree);
    free(g_ents);
    free(g_names);
    free(g_dirs);
    free(g_dir_names);
    free(g_dir_ht);
    crawl_result_free(&g_cr);
    return rc == 0 ? 0 : 1;
}
