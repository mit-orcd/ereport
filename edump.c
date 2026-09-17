/*
 * edump.c — Recreate an ecrawl tree with scrambled names and repeating contents.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * Usage: run with --help (or no arguments) for the flag list.
 * Writers: --writers N or EDUMP_WRITERS (default 8).
 * Regular files of 4 KiB+ are written with O_DIRECT when --block-size allows.
 */

#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#if defined(__linux__)
#include <sys/sysmacros.h>
#endif

#include "alloc_tuning.h"
#include "crawl_bin_block.h"
#include "crawl_bin_catalog.h"
#include "crawl_bin_chunks.h"
#include "crawl_bin_format.h"
#include "crawl_result.h"
#include "path_utils.h"

#define PROG "edump"
#define DEFAULT_WRITERS 8
#define DEFAULT_SEED 1ULL
#define DEFAULT_BLOCK_SIZE (1U << 20)
#define EDUMP_IO_ALIGN 4096U
#define EDUMP_JOB_TARGET_RECORDS 8192ULL
#define EDUMP_JOB_TARGET_BYTES (8ULL << 20)
#define MAX_WRITERS 4096
#define EDUMP_PATH_MAX 65536U
#define EDUMP_NAME_LEN 8
#define EDUMP_NAME_BYTES 9 /* 8 payload + hyphen */
#define EDUMP_NAME_SPACE 2821109907456ULL /* 36^8 */
#define HL_SHARDS 256U
#define NAME_SELF_TEST_N 4096U

static const char EDUMP_ALPHA[36] = "0123456789abcdefghijklmnopqrstuvwxyz";
static const crawl_bin_chunk_stdio_t g_io = {fopen, fread, fclose};

static uint64_t g_seed = DEFAULT_SEED;
static int g_writers = DEFAULT_WRITERS;
static size_t g_block_size = DEFAULT_BLOCK_SIZE;
static const char *g_out_dir;
static size_t g_out_dir_len;
static const char *g_stored_root;
static const char *g_only;              /* --only abs path in crawl namespace, or NULL */
static char g_only_rel[EDUMP_PATH_MAX]; /* --only prefix relative to crawl root; "" = no filter */
static size_t g_only_rel_len;
static unsigned g_only_depth;           /* component count of g_only_rel */
static int g_only_seen;                 /* prefix directory found in the catalog */
static uint64_t g_dir_id_max; /* D: interned directories live in 1..D */
static unsigned char *g_block;
static crawl_bin_catalog_t *g_cats;
static size_t g_shard_count;

static atomic_ullong g_next_chunk;
static atomic_int g_failed;
static atomic_ullong g_n_dirs;
static atomic_ullong g_n_files;
static atomic_ullong g_n_hardlinks;
static atomic_ullong g_n_symlinks;
static atomic_ullong g_n_fifos;
static atomic_ullong g_n_devices;
static atomic_ullong g_n_skipped;
static atomic_ullong g_n_bytes;
static atomic_ullong g_n_collisions;

typedef struct {
    char *key;
    uint32_t len;
    uint64_t id;
} intern_slot_t;

typedef struct {
    intern_slot_t *slots;
    size_t cap;
    size_t used;
} intern_t;

static intern_t g_intern;

typedef struct hl_ent {
    uint32_t maj;
    uint32_t min;
    uint64_t ino;
    char *path;
    int state; /* 0 pending, 1 ready, -1 fail */
    struct hl_ent *next;
} hl_ent_t;

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    hl_ent_t *head;
} hl_shard_t;

static hl_shard_t g_hl[HL_SHARDS];

typedef struct {
    crawl_bin_file_chunk_t *chunks;
    size_t chunk_cap;
    uint64_t *chunk_rec_base;
    size_t rec_base_cap;
    size_t chunk_count;
} dump_jobs_t;

typedef struct {
    uint64_t parent_dir_id;
    size_t prefix_len;
    int have;
    char prefix[EDUMP_PATH_MAX];
} dest_parent_cache_t;

static dump_jobs_t g_jobs;

static double now_sec(void) {
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec * 1e-6;
}

static uint64_t fnv1a(const char *s, size_t n) {
    uint64_t h = 14695981039346656037ULL;
    size_t i;

    for (i = 0; i < n; i++) {
        h ^= (unsigned char)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static uint64_t edump_multiplier(uint64_t seed) {
    uint64_t k = (uint64_t)(((__uint128_t)seed * 0x9E3779B97F4A7C15ULL + 1u) % EDUMP_NAME_SPACE);

    if (k == 0) k = 1;
    while ((k % 2ULL) == 0ULL || (k % 3ULL) == 0ULL) {
        k++;
        if (k >= EDUMP_NAME_SPACE) k = 1;
    }
    return k;
}

/* id in [1, 36^8). out gets xxxx-xxxx plus NUL. Returns 0, or -1 if id is unusable. */
static int edump_id_to_name(uint64_t id, uint64_t seed, char out[10]) {
    uint64_t mixed;
    char digits[EDUMP_NAME_LEN];
    int i;

    if (!out || id == 0ULL || id >= EDUMP_NAME_SPACE) return -1;
    mixed = (uint64_t)(((__uint128_t)id * edump_multiplier(seed)) % EDUMP_NAME_SPACE);
    for (i = EDUMP_NAME_LEN - 1; i >= 0; i--) {
        digits[i] = EDUMP_ALPHA[mixed % 36ULL];
        mixed /= 36ULL;
    }
    memcpy(out, digits, 4);
    out[4] = '-';
    memcpy(out + 5, digits + 4, 4);
    out[9] = '\0';
    return 0;
}

static int intern_grow(intern_t *t) {
    intern_slot_t *old = t->slots;
    size_t old_cap = t->cap;
    size_t ncap = old_cap ? old_cap * 2 : 1024;
    intern_slot_t *ns;
    size_t i;

    ns = (intern_slot_t *)calloc(ncap, sizeof(*ns));
    if (!ns) return -1;
    for (i = 0; i < old_cap; i++) {
        uint64_t h;
        size_t j;

        if (!old[i].key) continue;
        h = fnv1a(old[i].key, old[i].len);
        j = (size_t)(h & (ncap - 1));
        while (ns[j].key) j = (j + 1) & (ncap - 1);
        ns[j] = old[i];
    }
    free(old);
    t->slots = ns;
    t->cap = ncap;
    return 0;
}

/* First see: assign next id. Returns the id, or 0 on OOM. */
static uint64_t intern_put(intern_t *t, const char *key) {
    uint64_t h;
    size_t j;
    size_t n;

    if (!key || !*key) return 0;
    if (t->cap == 0 || t->used * 10 >= t->cap * 7) {
        if (intern_grow(t) != 0) return 0;
    }
    n = strlen(key);
    h = fnv1a(key, n);
    j = (size_t)(h & (t->cap - 1));
    for (;;) {
        if (!t->slots[j].key) {
            char *copy = (char *)malloc(n + 1);

            if (!copy) return 0;
            memcpy(copy, key, n + 1);
            t->slots[j].key = copy;
            t->slots[j].len = (uint32_t)n;
            t->used++;
            t->slots[j].id = (uint64_t)t->used; /* 1-based */
            return t->slots[j].id;
        }
        if (t->slots[j].len == (uint32_t)n && memcmp(t->slots[j].key, key, n) == 0) return t->slots[j].id;
        j = (j + 1) & (t->cap - 1);
    }
}

static uint64_t intern_get(const intern_t *t, const char *key, size_t n) {
    uint64_t h;
    size_t j;

    if (!t->cap || !key || n == 0) return 0;
    h = fnv1a(key, n);
    j = (size_t)(h & (t->cap - 1));
    for (;;) {
        if (!t->slots[j].key) return 0;
        if (t->slots[j].len == (uint32_t)n && memcmp(t->slots[j].key, key, n) == 0) return t->slots[j].id;
        j = (j + 1) & (t->cap - 1);
    }
}

static void intern_free(intern_t *t) {
    size_t i;

    if (!t->slots) return;
    for (i = 0; i < t->cap; i++) free(t->slots[i].key);
    free(t->slots);
    t->slots = NULL;
    t->cap = 0;
    t->used = 0;
}

static uint32_t hl_shard_of(uint32_t maj, uint32_t min, uint64_t ino) {
    uint64_t h = ino ^ ((uint64_t)maj << 32) ^ (uint64_t)min;

    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    return (uint32_t)(h & (HL_SHARDS - 1U));
}

static int hl_init(void) {
    uint32_t i;

    for (i = 0; i < HL_SHARDS; i++) {
        if (pthread_mutex_init(&g_hl[i].mu, NULL) != 0) return -1;
        if (pthread_cond_init(&g_hl[i].cv, NULL) != 0) return -1;
        g_hl[i].head = NULL;
    }
    return 0;
}

static void hl_free(void) {
    uint32_t i;

    for (i = 0; i < HL_SHARDS; i++) {
        hl_ent_t *e = g_hl[i].head;

        while (e) {
            hl_ent_t *n = e->next;

            free(e->path);
            free(e);
            e = n;
        }
        pthread_mutex_destroy(&g_hl[i].mu);
        pthread_cond_destroy(&g_hl[i].cv);
    }
}

/*
 * Claim or wait for (dev,ino). Returns:
 *   1  this thread writes the file (caller must hl_publish)
 *   0  another thread already wrote; *existing is the dump path (malloc'd, caller frees)
 *  -1  failure
 */
static int hl_claim(uint32_t maj, uint32_t min, uint64_t ino, char **existing) {
    hl_shard_t *s = &g_hl[hl_shard_of(maj, min, ino)];
    hl_ent_t *e;

    *existing = NULL;
    pthread_mutex_lock(&s->mu);
    for (e = s->head; e; e = e->next) {
        if (e->maj == maj && e->min == min && e->ino == ino) break;
    }
    if (!e) {
        e = (hl_ent_t *)calloc(1, sizeof(*e));
        if (!e) {
            pthread_mutex_unlock(&s->mu);
            return -1;
        }
        e->maj = maj;
        e->min = min;
        e->ino = ino;
        e->state = 0;
        e->next = s->head;
        s->head = e;
        pthread_mutex_unlock(&s->mu);
        return 1;
    }
    while (e->state == 0) pthread_cond_wait(&s->cv, &s->mu);
    if (e->state < 0 || !e->path) {
        pthread_mutex_unlock(&s->mu);
        return -1;
    }
    *existing = strdup(e->path);
    pthread_mutex_unlock(&s->mu);
    return *existing ? 0 : -1;
}

static void hl_publish(uint32_t maj, uint32_t min, uint64_t ino, const char *path, int ok) {
    hl_shard_t *s = &g_hl[hl_shard_of(maj, min, ino)];
    hl_ent_t *e;

    pthread_mutex_lock(&s->mu);
    for (e = s->head; e; e = e->next) {
        if (e->maj == maj && e->min == min && e->ino == ino) break;
    }
    if (e) {
        if (ok && path) e->path = strdup(path);
        e->state = (ok && e->path) ? 1 : -1;
        pthread_cond_broadcast(&s->cv);
    }
    pthread_mutex_unlock(&s->mu);
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [options] <crawl-dir> <output-dir>\n"
            "\n"
            "  Recreate a crawl tree with scrambled names and repeating seed-derived contents.\n"
            "  Same seed + same crawl always yields the same dump. Sparse files are written\n"
            "  at logical size (holes are not preserved).\n"
            "\n"
            "Options:\n"
            "  --seed N           name and content seed (default 1)\n"
            "  --writers N         parallel dump workers (default %d, or EDUMP_WRITERS)\n"
            "  --block-size N     repeating content block bytes (default %u)\n"
            "  --only PATH        dump only the subtree under PATH (absolute, crawl-side);\n"
            "                     PATH itself becomes the dump root\n"
            "  --progress         live files/dirs/objects/volume line on stderr\n"
            "                     (default: on when stderr is a TTY; --no-progress disables)\n"
            "  --name-self-test   check id→name injectivity on a sample and exit\n"
            "  -h, --help         this message\n"
            "\n"
            "  Regular files of at least %u bytes are written with O_DIRECT (sub-%u-byte\n"
            "  tails stay buffered); needs --block-size to be a multiple of %u, else all\n"
            "  writes are buffered.\n"
            "\n"
            "Environment:\n"
            "  EDUMP_WRITERS      worker count when --writers is omitted (default %d)\n",
            prog, DEFAULT_WRITERS, DEFAULT_BLOCK_SIZE, EDUMP_IO_ALIGN, EDUMP_IO_ALIGN, EDUMP_IO_ALIGN,
            DEFAULT_WRITERS);
}

static int parse_u64_arg(const char *flag, const char *s, uint64_t *out) {
    char *end = NULL;
    unsigned long long v;

    errno = 0;
    v = strtoull(s, &end, 10);
    if (errno || !s[0] || (end && *end)) {
        fprintf(stderr, PROG ": %s: invalid number '%s'\n", flag, s);
        return -1;
    }
    *out = (uint64_t)v;
    return 0;
}

static int parse_writers_env(void) {
    const char *e = getenv("EDUMP_WRITERS");
    uint64_t v;

    if (!e || !*e) return DEFAULT_WRITERS;
    if (parse_u64_arg("EDUMP_WRITERS", e, &v) != 0) return -1;
    if (v < 1ULL || v > (uint64_t)MAX_WRITERS) {
        fprintf(stderr, PROG ": EDUMP_WRITERS must be 1..%d\n", MAX_WRITERS);
        return -1;
    }
    return (int)v;
}

static int name_cmp(const void *x, const void *y) {
    return strcmp((const char *)x, (const char *)y);
}

static int name_self_test(void) {
    char a[10], b[10], again[10];
    char (*names)[10];
    size_t i;
    unsigned differ = 0;

    if (edump_id_to_name(0, 1, a) == 0) {
        fprintf(stderr, PROG ": id 0 must be rejected\n");
        return 1;
    }
    if (edump_id_to_name(EDUMP_NAME_SPACE, 1, a) == 0) {
        fprintf(stderr, PROG ": id 36^8 must be rejected\n");
        return 1;
    }
    if (edump_id_to_name(1000000000ULL, 1, a) != 0 || edump_id_to_name(1000000001ULL, 1, b) != 0) {
        fprintf(stderr, PROG ": sample encode failed\n");
        return 1;
    }
    if (strcmp(a, b) == 0) {
        fprintf(stderr, PROG ": sequential ids produced the same name\n");
        return 1;
    }
    for (i = 0; i < EDUMP_NAME_BYTES; i++) {
        if (a[i] != b[i]) differ++;
    }
    if (differ < 3U) {
        fprintf(stderr, PROG ": nearby ids too similar: %s vs %s\n", a, b);
        return 1;
    }
    if (edump_id_to_name(1000000000ULL, 1, again) != 0 || strcmp(a, again) != 0) {
        fprintf(stderr, PROG ": encode is not stable\n");
        return 1;
    }
    printf("id=1000000000 name=%s\n", a);
    printf("id=1000000001 name=%s\n", b);

    names = (char (*)[10])malloc((size_t)NAME_SELF_TEST_N * 10);
    if (!names) {
        fprintf(stderr, PROG ": out of memory\n");
        return 1;
    }
    for (i = 0; i < NAME_SELF_TEST_N; i++) {
        if (edump_id_to_name((uint64_t)i + 1ULL, 1, names[i]) != 0) {
            free(names);
            fprintf(stderr, PROG ": encode(%zu) failed\n", i + 1);
            return 1;
        }
    }
    qsort(names, NAME_SELF_TEST_N, 10, name_cmp);
    for (i = 1; i < NAME_SELF_TEST_N; i++) {
        if (strcmp(names[i - 1], names[i]) == 0) {
            fprintf(stderr, PROG ": collision %s\n", names[i]);
            free(names);
            return 1;
        }
    }
    free(names);
    printf("injective=%u\n", NAME_SELF_TEST_N);
    return 0;
}

/* 1 = this path is the stored root (dump root). 0 = under it, rel filled. -1 = skip. */
static int strip_to_rel(const char *stored, const char *root, char *rel, size_t rel_sz) {
    size_t lr, ls;

    if (!stored || !root || !rel || rel_sz == 0) return -1;
    rel[0] = '\0';
    ls = strlen(stored);
    lr = strlen(root);
    if (strcmp(root, "/") == 0) {
        if (strcmp(stored, "/") == 0) return 1;
        if (stored[0] != '/') return -1;
        if (ls < 2 || ls - 1 >= rel_sz) return -1;
        memcpy(rel, stored + 1, ls - 1);
        rel[ls - 1] = '\0';
        return 0;
    }
    if (strcmp(stored, root) == 0) return 1;
    if (ls < lr + 1 || strncmp(stored, root, lr) != 0 || stored[lr] != '/') return -1;
    if (ls - lr - 1 >= rel_sz) return -1;
    memcpy(rel, stored + lr + 1, ls - lr - 1);
    rel[ls - lr - 1] = '\0';
    return 0;
}

/*
 * rel is a crawl-root-relative path. With --only, only paths strictly below the
 * prefix are kept; the prefix itself maps to the dump root (strip semantics).
 */
static int rel_kept(const char *rel) {
    if (!g_only_rel_len) return 1;
    return strncmp(rel, g_only_rel, g_only_rel_len) == 0 && rel[g_only_rel_len] == '/';
}

static int dir_is_empty(const char *path) {
    DIR *d = opendir(path);
    struct dirent *de;
    int empty = 1;

    if (!d) return -1;
    while ((de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        empty = 0;
        break;
    }
    closedir(d);
    return empty;
}

static int prepare_out_dir(const char *path) {
    struct stat st;

    if (stat(path, &st) != 0) {
        if (errno != ENOENT) {
            fprintf(stderr, PROG ": %s: %s\n", path, strerror(errno));
            return -1;
        }
        if (mkdir(path, 0755) != 0) {
            fprintf(stderr, PROG ": mkdir %s: %s\n", path, strerror(errno));
            return -1;
        }
        return 0;
    }
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, PROG ": %s exists and is not a directory\n", path);
        return -1;
    }
    {
        int empty = dir_is_empty(path);

        if (empty < 0) {
            fprintf(stderr, PROG ": %s: %s\n", path, strerror(errno));
            return -1;
        }
        if (!empty) {
            fprintf(stderr, PROG ": %s is not empty\n", path);
            return -1;
        }
    }
    return 0;
}

static int mkdir_one(const char *path) {
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) {
        struct stat st;

        if (lstat(path, &st) == 0 && S_ISDIR(st.st_mode)) return 0;
        atomic_fetch_add(&g_n_collisions, 1);
        fprintf(stderr, PROG ": collision at %s\n", path);
        return -1;
    }
    fprintf(stderr, PROG ": mkdir %s: %s\n", path, strerror(errno));
    return -1;
}

static int map_rel_to_dest(const char *rel, int is_dir, uint64_t file_id, char *dest, size_t dest_sz);

static int dir_mk_cmp(const void *a, const void *b) {
    const intern_slot_t *x = *(const intern_slot_t *const *)a;
    const intern_slot_t *y = *(const intern_slot_t *const *)b;
    uint32_t dx = 0, dy = 0;
    size_t i;

    for (i = 0; i < x->len; i++) {
        if (x->key[i] == '/') dx++;
    }
    for (i = 0; i < y->len; i++) {
        if (y->key[i] == '/') dy++;
    }
    if (dx != dy) return (dx < dy) ? -1 : 1;
    if (x->len != y->len) {
        size_t n = x->len < y->len ? x->len : y->len;
        int c = memcmp(x->key, y->key, n);

        if (c != 0) return c;
        return (x->len < y->len) ? -1 : 1;
    }
    return memcmp(x->key, y->key, x->len);
}

static int mkdir_interned_dirs(void) {
    intern_slot_t **order;
    size_t i, n = 0;
    char dest[EDUMP_PATH_MAX];

    if (g_intern.used == 0) return 0;
    order = (intern_slot_t **)calloc(g_intern.used, sizeof(*order));
    if (!order) {
        fprintf(stderr, PROG ": out of memory\n");
        return -1;
    }
    for (i = 0; i < g_intern.cap; i++) {
        if (!g_intern.slots[i].key) continue;
        order[n++] = &g_intern.slots[i];
    }
    qsort(order, n, sizeof(*order), dir_mk_cmp);
    for (i = 0; i < n; i++) {
        if (!rel_kept(order[i]->key)) continue;
        if (map_rel_to_dest(order[i]->key, 1, 0, dest, sizeof(dest)) != 0) {
            free(order);
            return -1;
        }
        if (mkdir_one(dest) != 0) {
            free(order);
            return -1;
        }
        atomic_fetch_add(&g_n_dirs, 1);
    }
    free(order);
    return 0;
}

static int append_name(char *dest, size_t *len, size_t cap, uint64_t id) {
    char name[10];
    size_t need;

    if (edump_id_to_name(id, g_seed, name) != 0) {
        fprintf(stderr, PROG ": id %" PRIu64 " is out of range\n", id);
        return -1;
    }
    need = *len + 1 + EDUMP_NAME_BYTES + 1;
    if (need > cap) {
        fprintf(stderr, PROG ": dump path too long\n");
        return -1;
    }
    dest[(*len)++] = '/';
    memcpy(dest + *len, name, EDUMP_NAME_BYTES);
    *len += EDUMP_NAME_BYTES;
    dest[*len] = '\0';
    return 0;
}

/*
 * Map original relative path to dump dest.
 * is_dir: every component from intern.
 * else: all but last from intern; last from file_id.
 */
static int map_rel_to_dest(const char *rel, int is_dir, uint64_t file_id, char *dest, size_t dest_sz) {
    size_t len;
    const char *p;
    char prefix[EDUMP_PATH_MAX];
    size_t plen = 0;
    unsigned comp = 0;

    if (dest_sz < g_out_dir_len + 1) return -1;
    memcpy(dest, g_out_dir, g_out_dir_len);
    dest[g_out_dir_len] = '\0';
    len = g_out_dir_len;
    if (!rel || !rel[0]) return 0;

    p = rel;
    prefix[0] = '\0';
    while (*p) {
        const char *slash = strchr(p, '/');
        size_t clen = slash ? (size_t)(slash - p) : strlen(p);
        int last = (slash == NULL);
        uint64_t id;

        if (clen == 0) {
            fprintf(stderr, PROG ": empty path component in %s\n", rel);
            return -1;
        }
        if (plen + (plen ? 1 : 0) + clen >= sizeof(prefix)) {
            fprintf(stderr, PROG ": path too long\n");
            return -1;
        }
        if (plen) prefix[plen++] = '/';
        memcpy(prefix + plen, p, clen);
        plen += clen;
        prefix[plen] = '\0';

        if (!last || is_dir) {
            id = intern_get(&g_intern, prefix, plen);
            if (id == 0) {
                fprintf(stderr, PROG ": missing directory id for %s\n", prefix);
                return -1;
            }
        } else {
            id = file_id;
        }
        /* --only: the prefix's own components map to the dump root; append only below it */
        comp++;
        if (comp > g_only_depth && append_name(dest, &len, dest_sz, id) != 0) return -1;
        if (!slash) break;
        p = slash + 1;
    }
    return 0;
}

static int write_full(int fd, const unsigned char *buf, size_t n) {
    size_t off = 0;

    while (off < n) {
        ssize_t w = write(fd, buf + off, n - off);

        if (w < 0) return -1;
        if (w == 0) {
            errno = EIO;
            return -1;
        }
        off += (size_t)w;
    }
    return 0;
}

static int write_repeating(int fd, uint64_t size) {
    uint64_t left = size;

    while (left) {
        size_t n = left > (uint64_t)g_block_size ? g_block_size : (size_t)left;

        if (write_full(fd, g_block, n) != 0) return -1;
        left -= (uint64_t)n;
    }
    return 0;
}

static int write_repeating_direct(int fd, uint64_t size) {
    uint64_t aligned = size & ~(uint64_t)(EDUMP_IO_ALIGN - 1);
    uint64_t rem = size - aligned;
    uint64_t off = 0;

    if (size >= (uint64_t)g_block_size) {
        int frc = posix_fallocate(fd, 0, (off_t)size);

        if (frc != 0 && frc != EOPNOTSUPP && frc != ENOSYS && frc != EINVAL) {
            errno = frc;
            return -1;
        }
    }
    while (off < aligned) {
        size_t n = g_block_size;

        if ((uint64_t)n > aligned - off) n = (size_t)(aligned - off);
        if (write_full(fd, g_block, n) != 0) return -1;
        off += (uint64_t)n;
    }
    if (rem) {
        /*
         * Unaligned tail: drop O_DIRECT and pwrite it buffered. Cheaper than a
         * padded 4 KiB direct write plus ftruncate (one less data IO and one
         * less truncate transaction per file). We opened this fd and the only
         * settable status flag it carries is O_DIRECT, so a single F_SETFL
         * clears it — no F_GETFL round-trip.
         */
        unsigned char tail[EDUMP_IO_ALIGN];
        size_t src = (size_t)(aligned % (uint64_t)g_block_size);
        uint64_t done = 0;

        if (fcntl(fd, F_SETFL, 0) != 0) return -1;
        if (src + rem <= g_block_size) {
            memcpy(tail, g_block + src, (size_t)rem);
        } else {
            size_t first = g_block_size - src;

            memcpy(tail, g_block + src, first);
            memcpy(tail + first, g_block, (size_t)rem - first);
        }
        while (done < rem) {
            ssize_t w = pwrite(fd, tail + done, (size_t)(rem - done), (off_t)(aligned + done));

            if (w < 0) return -1;
            if (w == 0) {
                errno = EIO;
                return -1;
            }
            done += (uint64_t)w;
        }
    }
    return 0;
}

static int want_direct(uint64_t size) {
#ifndef O_DIRECT
    (void)size;
    return 0;
#else
    return size >= (uint64_t)EDUMP_IO_ALIGN && (g_block_size % EDUMP_IO_ALIGN) == 0;
#endif
}

static int dump_regular(const char *dest, const bin_record_hdr_t *r, const char *orig) {
    int claim = 1;
    char *existing = NULL;
    int fd = -1;
    int used_direct = 0;
    int wr;
    int flags = O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC;
    int hl = (r->nlink > 1ULL);

    if (hl) {
        claim = hl_claim(r->dev_major, r->dev_minor, r->inode, &existing);
        if (claim < 0) return -1;
        if (claim == 0) {
            if (link(existing, dest) != 0) {
                fprintf(stderr, PROG ": link %s -> %s: %s (orig %s)\n", existing, dest, strerror(errno), orig);
                free(existing);
                return -1;
            }
            free(existing);
            atomic_fetch_add(&g_n_hardlinks, 1);
            return 0;
        }
    }

#ifdef O_DIRECT
    if (want_direct(r->size)) {
        fd = open(dest, flags | O_DIRECT, 0644);
        if (fd >= 0) {
            used_direct = 1;
        } else if (errno != EINVAL && errno != EOPNOTSUPP) {
            if (errno == EEXIST) {
                atomic_fetch_add(&g_n_collisions, 1);
                fprintf(stderr, PROG ": name collision at %s (orig %s)\n", dest, orig);
            } else {
                fprintf(stderr, PROG ": open %s: %s (orig %s)\n", dest, strerror(errno), orig);
            }
            if (hl) hl_publish(r->dev_major, r->dev_minor, r->inode, NULL, 0);
            return -1;
        }
    }
#endif
    if (fd < 0) {
        fd = open(dest, flags, 0644);
        used_direct = 0;
    }
    if (fd < 0) {
        if (errno == EEXIST) {
            atomic_fetch_add(&g_n_collisions, 1);
            fprintf(stderr, PROG ": name collision at %s (orig %s)\n", dest, orig);
        } else {
            fprintf(stderr, PROG ": open %s: %s (orig %s)\n", dest, strerror(errno), orig);
        }
        if (hl) hl_publish(r->dev_major, r->dev_minor, r->inode, NULL, 0);
        return -1;
    }
    wr = used_direct ? write_repeating_direct(fd, r->size) : write_repeating(fd, r->size);
#ifdef O_DIRECT
    if (wr != 0 && used_direct && errno == EINVAL) {
        close(fd);
        unlink(dest);
        fd = open(dest, flags, 0644);
        if (fd < 0) {
            fprintf(stderr, PROG ": open %s: %s (orig %s)\n", dest, strerror(errno), orig);
            if (hl) hl_publish(r->dev_major, r->dev_minor, r->inode, NULL, 0);
            return -1;
        }
        wr = write_repeating(fd, r->size);
    }
#endif
    if (wr != 0) {
        fprintf(stderr, PROG ": write %s: %s (orig %s)\n", dest, strerror(errno), orig);
        close(fd);
        unlink(dest);
        if (hl) hl_publish(r->dev_major, r->dev_minor, r->inode, NULL, 0);
        return -1;
    }
    close(fd);
    atomic_fetch_add(&g_n_files, 1);
    atomic_fetch_add(&g_n_bytes, (unsigned long long)r->size);
    if (hl) hl_publish(r->dev_major, r->dev_minor, r->inode, dest, 1);
    return 0;
}

static int dump_symlink(const char *dest, uint64_t size, const char *orig) {
    char target[PATH_MAX];
    size_t n;
    size_t i;

    if (size == 0ULL) {
        if (symlink("x", dest) != 0) {
            fprintf(stderr, PROG ": symlink %s: %s (orig %s)\n", dest, strerror(errno), orig);
            return -1;
        }
        atomic_fetch_add(&g_n_symlinks, 1);
        return 0;
    }
    n = size > (uint64_t)(PATH_MAX - 1) ? (size_t)(PATH_MAX - 1) : (size_t)size;
    for (i = 0; i < n; i++) target[i] = g_block[i % g_block_size];
    target[n] = '\0';
    if (symlink(target, dest) != 0) {
        fprintf(stderr, PROG ": symlink %s: %s (orig %s)\n", dest, strerror(errno), orig);
        return -1;
    }
    atomic_fetch_add(&g_n_symlinks, 1);
    return 0;
}

static int dump_special(const char *dest, const bin_record_hdr_t *r, const char *orig) {
    mode_t mode;
    dev_t dev;

    if (r->type == (uint8_t)'p') {
        if (mkfifo(dest, 0644) != 0) {
            fprintf(stderr, PROG ": mkfifo %s: %s (orig %s)\n", dest, strerror(errno), orig);
            return -1;
        }
        atomic_fetch_add(&g_n_fifos, 1);
        return 0;
    }
    if (r->type == (uint8_t)'c' || r->type == (uint8_t)'b') {
        mode = (r->type == (uint8_t)'c') ? (mode_t)(S_IFCHR | 0666) : (mode_t)(S_IFBLK | 0666);
        dev = makedev((unsigned int)r->dev_major, (unsigned int)r->dev_minor);
        if (mknod(dest, mode, dev) != 0) {
            if (errno == EPERM || errno == EACCES) {
                atomic_fetch_add(&g_n_skipped, 1);
                return 0;
            }
            fprintf(stderr, PROG ": mknod %s: %s (orig %s)\n", dest, strerror(errno), orig);
            return -1;
        }
        atomic_fetch_add(&g_n_devices, 1);
        return 0;
    }
    atomic_fetch_add(&g_n_skipped, 1);
    return 0;
}

static int dump_record(const crawl_bin_catalog_t *cat, const bin_record_hdr_t *r, const unsigned char *name,
                       uint64_t file_id, dest_parent_cache_t *pc) {
    char dest[EDUMP_PATH_MAX];
    char stored[EDUMP_PATH_MAX];
    const char *orig = dest;

    if (r->type == (uint8_t)'d') return 0;

    if (pc->have && pc->parent_dir_id == r->parent_dir_id) {
        size_t len = pc->prefix_len;

        memcpy(dest, pc->prefix, len + 1);
        if (append_name(dest, &len, sizeof(dest), file_id) != 0) return -1;
    } else {
        char rel[EDUMP_PATH_MAX];
        char *slash;
        int st;

        if (crawl_bin_catalog_entry_path(cat, r->parent_dir_id, (const char *)name, (size_t)r->name_len, stored,
                                          sizeof(stored)) != 0) {
            fprintf(stderr, PROG ": cannot rebuild stored path\n");
            return -1;
        }
        st = strip_to_rel(stored, g_stored_root, rel, sizeof(rel));
        if (st < 0) {
            atomic_fetch_add(&g_n_skipped, 1);
            return 0;
        }
        if (st == 1) return 0;
        if (!rel_kept(rel)) {
            atomic_fetch_add(&g_n_skipped, 1);
            return 0;
        }
        if (map_rel_to_dest(rel, 0, file_id, dest, sizeof(dest)) != 0) return -1;
        orig = stored;
        slash = strrchr(dest, '/');
        if (slash) {
            pc->prefix_len = (size_t)(slash - dest);
            memcpy(pc->prefix, dest, pc->prefix_len);
            pc->prefix[pc->prefix_len] = '\0';
            pc->parent_dir_id = r->parent_dir_id;
            pc->have = 1;
        }
    }

    if (r->type == (uint8_t)'f') return dump_regular(dest, r, orig);
    if (r->type == (uint8_t)'l') return dump_symlink(dest, r->size, orig);
    return dump_special(dest, r, orig);
}

static uint32_t dump_projection(void) {
    return CRAWL_COL_BIT(CRAWL_COL_PARENT_DIR_ID) | CRAWL_COL_BIT(CRAWL_COL_NAME_LEN) |
           CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES) | CRAWL_COL_BIT(CRAWL_COL_TYPE) | CRAWL_COL_BIT(CRAWL_COL_SIZE) |
           CRAWL_COL_BIT(CRAWL_COL_INODE) | CRAWL_COL_BIT(CRAWL_COL_DEV_MAJOR) | CRAWL_COL_BIT(CRAWL_COL_DEV_MINOR) |
           CRAWL_COL_BIT(CRAWL_COL_NLINK);
}

static int process_chunk(size_t ci) {
    const crawl_bin_file_chunk_t *chunk = &g_jobs.chunks[ci];
    const crawl_bin_catalog_t *cat = &g_cats[chunk->file_index];
    crawl_bin_block_reader_t br;
    FILE *fp;
    uint64_t rec_in = 0;
    int rc = 0;
    dest_parent_cache_t *pc;

    pc = (dest_parent_cache_t *)calloc(1, sizeof(*pc));
    if (!pc) {
        fprintf(stderr, PROG ": out of memory\n");
        return -1;
    }
    fp = fopen(chunk->path, "rb");
    if (!fp) {
        fprintf(stderr, PROG ": %s: %s\n", chunk->path, strerror(errno));
        free(pc);
        return -1;
    }
    if (crawl_bin_block_reader_init(&br, &g_io, fp, chunk->start_offset, chunk->end_offset) != 0) {
        fprintf(stderr, PROG ": %s: cannot read records at offset %" PRIu64 "\n", chunk->path,
                chunk->start_offset);
        fclose(fp);
        free(pc);
        return -1;
    }
    (void)crawl_bin_block_reader_set_projection(&br, dump_projection());

    for (;;) {
        bin_record_hdr_t r;
        const unsigned char *name = NULL;
        int got = crawl_bin_block_reader_next(&br, &r, &name);
        uint64_t file_id;

        if (got == 0) break;
        if (got < 0) {
            fprintf(stderr, PROG ": %s: record decode failed\n", chunk->path);
            rc = -1;
            break;
        }
        file_id = g_dir_id_max + 1ULL + g_jobs.chunk_rec_base[ci] + rec_in;
        rec_in++;
        if (file_id >= EDUMP_NAME_SPACE) {
            fprintf(stderr, PROG ": id space exhausted\n");
            rc = -1;
            break;
        }
        if (dump_record(cat, &r, name, file_id, pc) != 0) {
            rc = -1;
            break;
        }
    }
    crawl_bin_block_reader_free(&br);
    fclose(fp);
    free(pc);
    return rc;
}

static void *worker_main(void *arg) {
    (void)arg;
    for (;;) {
        size_t ci = (size_t)atomic_fetch_add(&g_next_chunk, 1ULL);

        if (ci >= g_jobs.chunk_count) break;
        if (atomic_load(&g_failed)) break;
        if (process_chunk(ci) != 0) {
            atomic_store(&g_failed, 1);
            break;
        }
    }
    return NULL;
}

static int jobs_append(const char *path, uint64_t start, uint64_t end, size_t file_index, uint64_t rec_base) {
    if (g_jobs.chunk_count == g_jobs.rec_base_cap) {
        size_t ncap = g_jobs.rec_base_cap ? g_jobs.rec_base_cap * 2 : 64;
        uint64_t *n = (uint64_t *)realloc(g_jobs.chunk_rec_base, ncap * sizeof(*n));

        if (!n) return -1;
        g_jobs.chunk_rec_base = n;
        g_jobs.rec_base_cap = ncap;
    }
    if (crawl_bin_append_chunk(&g_jobs.chunks, &g_jobs.chunk_count, &g_jobs.chunk_cap, path, start, end, file_index) !=
        0) {
        return -1;
    }
    g_jobs.chunk_rec_base[g_jobs.chunk_count - 1] = rec_base;
    return 0;
}

static int split_shard_into_jobs(const crawl_result_shard_t *sh, size_t si, uint64_t *total_recs) {
    FILE *fp;
    uint64_t pos;
    uint64_t end;
    uint64_t job_start;
    uint64_t job_nrec;
    uint64_t job_bytes;

    if (sh->catalog_offset <= sizeof(bin_file_header_t)) return 0;
    fp = fopen(sh->path, "rb");
    if (!fp) {
        fprintf(stderr, PROG ": %s: %s\n", sh->path, strerror(errno));
        return -1;
    }
    pos = sizeof(bin_file_header_t);
    end = sh->catalog_offset;
    job_start = pos;
    job_nrec = 0;
    job_bytes = 0;
    if (fseeko(fp, (off_t)pos, SEEK_SET) != 0) {
        fclose(fp);
        return -1;
    }
    while (pos < end) {
        bin_rowgroup_hdr_t rg;
        uint64_t total;
        int flush;

        if (end - pos < sizeof(rg)) {
            fclose(fp);
            return -1;
        }
        if (fread(&rg, sizeof(rg), 1, fp) != 1) {
            fclose(fp);
            return -1;
        }
        total = crawl_bin_rowgroup_total_bytes(&rg);
        if (total == 0 || pos + total > end) {
            fclose(fp);
            return -1;
        }
        job_nrec += rg.record_count;
        job_bytes += total;
        pos += total;
        if (fseeko(fp, (off_t)pos, SEEK_SET) != 0) {
            fclose(fp);
            return -1;
        }
        flush = (job_nrec >= EDUMP_JOB_TARGET_RECORDS) || (job_bytes >= EDUMP_JOB_TARGET_BYTES) || (pos >= end);
        if (!flush) continue;
        if (jobs_append(sh->path, job_start, pos, si, *total_recs) != 0) {
            fprintf(stderr, PROG ": out of memory building dump jobs\n");
            fclose(fp);
            return -1;
        }
        *total_recs += job_nrec;
        job_start = pos;
        job_nrec = 0;
        job_bytes = 0;
    }
    fclose(fp);
    return 0;
}

static int intern_shard_dirs(const crawl_result_t *cr, size_t si) {
    const crawl_result_shard_t *sh = &cr->shards[si];
    FILE *fp;
    uint64_t d;

    fp = fopen(sh->path, "rb");
    if (!fp) {
        fprintf(stderr, PROG ": %s: %s\n", sh->path, strerror(errno));
        return -1;
    }
    if (crawl_bin_catalog_load_sel(fp, sh->catalog_offset, sh->file_size, 0U, &g_cats[si]) != 0) {
        fprintf(stderr, PROG ": %s: cannot load directory catalog\n", sh->path);
        fclose(fp);
        return -1;
    }
    fclose(fp);

    for (d = 1; d <= g_cats[si].max_dir_id; d++) {
        char stored[EDUMP_PATH_MAX];
        char rel[EDUMP_PATH_MAX];
        int st;

        if (g_cats[si].name_len[d] == 0 && g_cats[si].parent_dir_id[d] == 0) continue;
        if (crawl_bin_catalog_dir_path(&g_cats[si], d, stored, sizeof(stored)) != 0) continue;
        if (g_only_rel_len && !g_only_seen && strcmp(stored, g_only) == 0) g_only_seen = 1;
        st = strip_to_rel(stored, g_stored_root, rel, sizeof(rel));
        if (st != 0) continue;
        if (intern_put(&g_intern, rel) == 0) {
            fprintf(stderr, PROG ": out of memory interning directories\n");
            return -1;
        }
    }
    return 0;
}

static void warn_sparse_if_needed(const char *crawl_dir) {
    char path[PATH_MAX];
    FILE *fp;
    char line[4096];
    uint64_t heuristic = 0;
    uint64_t allocated = 0;
    int saw_h = 0, saw_a = 0;

    if (snprintf(path, sizeof(path), "%s/crawl_manifest.txt", crawl_dir) >= (int)sizeof(path)) return;
    fp = fopen(path, "r");

    if (!fp) return;
    while (fgets(line, sizeof(line), fp)) {
        char *nl = strchr(line, '\n');

        if (nl) *nl = '\0';
        if (strncmp(line, "files_sparse_heuristic=", 23) == 0) {
            heuristic = strtoull(line + 23, NULL, 10);
            saw_h = 1;
        } else if (strncmp(line, "total_allocated_bytes=", 22) == 0) {
            allocated = strtoull(line + 22, NULL, 10);
            saw_a = 1;
        }
    }
    fclose(fp);
    if (saw_h && heuristic > 0ULL) {
        fprintf(stderr,
                PROG ": warning: crawl reports files_sparse_heuristic=%" PRIu64 " (total_allocated_bytes=%" PRIu64
                     "); dump writes logical st_size for every regular file\n",
                heuristic, saw_a ? allocated : (uint64_t)0);
    }
}

static int fill_block(uint64_t seed, size_t block_size) {
    size_t i;
    size_t alloc = block_size;

    if (alloc < EDUMP_IO_ALIGN) alloc = EDUMP_IO_ALIGN;
    if (alloc % EDUMP_IO_ALIGN) alloc += EDUMP_IO_ALIGN - (alloc % EDUMP_IO_ALIGN);
    if (posix_memalign((void **)&g_block, EDUMP_IO_ALIGN, alloc) != 0) return -1;
    srand((unsigned)seed);
    for (i = 0; i < block_size; i++) g_block[i] = (unsigned char)('A' + rand() % 26);
    return 0;
}

static void usage_exit(const char *prog, int code) {
    usage(prog);
    exit(code);
}

/*
 * Live progress: a reporter thread reads the counters the workers already
 * maintain and rewrites one stderr line about once a second. Workers never
 * touch progress state, so the dump hot path is unchanged. Default is on
 * when stderr is a TTY; --progress forces it on, --no-progress forces off.
 */
static int g_progress; /* 0 = auto (TTY), 1 = on, -1 = off */
static atomic_int g_progress_done;

static int progress_wanted(void) {
    if (g_progress > 0) return 1;
    if (g_progress < 0) return 0;
    return isatty(STDERR_FILENO) != 0;
}

static void progress_fmt_volume(char *buf, size_t n, unsigned long long b) {
    if (b >= (1ULL << 30))
        snprintf(buf, n, "%.2f GiB", (double)b / 1073741824.0);
    else if (b >= (1ULL << 20))
        snprintf(buf, n, "%.1f MiB", (double)b / 1048576.0);
    else
        snprintf(buf, n, "%llu B", b);
}

static void progress_print(int tty, double t0) {
    char vol[32];
    unsigned long long f, d, o, b;

    f = (unsigned long long)atomic_load(&g_n_files);
    d = (unsigned long long)atomic_load(&g_n_dirs);
    o = (unsigned long long)atomic_load(&g_n_symlinks) + (unsigned long long)atomic_load(&g_n_fifos) +
        (unsigned long long)atomic_load(&g_n_devices) + (unsigned long long)atomic_load(&g_n_hardlinks);
    b = (unsigned long long)atomic_load(&g_n_bytes);
    progress_fmt_volume(vol, sizeof(vol), b);
    if (tty) fputs("\r\033[2K\r", stderr);
    fprintf(stderr, PROG ": files=%llu dirs=%llu objects=%llu volume=%s elapsed=%.0fs%s", f, d, o, vol,
            now_sec() - t0, tty ? "" : "\n");
    fflush(stderr);
}

static void *progress_main(void *arg) {
    double t0 = now_sec();
    int tty = isatty(STDERR_FILENO);

    (void)arg;
    while (!atomic_load_explicit(&g_progress_done, memory_order_acquire)) {
        sleep(1);
        if (atomic_load_explicit(&g_progress_done, memory_order_acquire)) break;
        progress_print(tty, t0);
    }
    progress_print(tty, t0); /* final line with accurate counters */
    return NULL;
}

int main(int argc, char **argv) {
    const char *crawl_dir = NULL;
    const char *out_dir = NULL;
    int i;
    crawl_result_t cr;
    pthread_t *th = NULL;
    pthread_t pth;
    int prog_on = 0;
    int nstarted = 0;
    double t0;
    uint64_t total_recs = 0;
    size_t si;
    int w;

    tune_allocator();
    g_writers = parse_writers_env();
    if (g_writers < 0) return 2;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            usage(argv[0]);
            return 0;
        }
        if (strcmp(argv[i], "--name-self-test") == 0) return name_self_test();
        if (strcmp(argv[i], "--seed") == 0) {
            if (i + 1 >= argc) usage_exit(argv[0], 2);
            if (parse_u64_arg("--seed", argv[++i], &g_seed) != 0) return 2;
            continue;
        }
        if (strcmp(argv[i], "--writers") == 0) {
            uint64_t v;

            if (i + 1 >= argc) usage_exit(argv[0], 2);
            if (parse_u64_arg("--writers", argv[++i], &v) != 0) return 2;
            if (v < 1ULL || v > (uint64_t)MAX_WRITERS) {
                fprintf(stderr, PROG ": --writers must be 1..%d\n", MAX_WRITERS);
                return 2;
            }
            g_writers = (int)v;
            continue;
        }
        if (strcmp(argv[i], "--block-size") == 0) {
            uint64_t v;

            if (i + 1 >= argc) usage_exit(argv[0], 2);
            if (parse_u64_arg("--block-size", argv[++i], &v) != 0) return 2;
            if (v < 1ULL || v > (1ULL << 30)) {
                fprintf(stderr, PROG ": --block-size out of range\n");
                return 2;
            }
            g_block_size = (size_t)v;
            continue;
        }
        if (strcmp(argv[i], "--only") == 0) {
            if (i + 1 >= argc) usage_exit(argv[0], 2);
            g_only = argv[++i];
            if (g_only[0] != '/') {
                fprintf(stderr, PROG ": --only must be an absolute path in the crawl namespace\n");
                return 2;
            }
            continue;
        }
        if (strcmp(argv[i], "--progress") == 0) {
            g_progress = 1;
            continue;
        }
        if (strcmp(argv[i], "--no-progress") == 0) {
            g_progress = -1;
            continue;
        }
        if (argv[i][0] == '-') {
            fprintf(stderr, PROG ": unknown option %s (try --help)\n", argv[i]);
            usage(argv[0]);
            return 2;
        }
        if (!crawl_dir) {
            crawl_dir = argv[i];
        } else if (!out_dir) {
            out_dir = argv[i];
        } else {
            fprintf(stderr, PROG ": extra argument %s\n", argv[i]);
            usage(argv[0]);
            return 2;
        }
    }
    if (!crawl_dir || !out_dir) {
        usage(argv[0]);
        return 2;
    }

    crawl_result_init(&cr);
    if (crawl_result_open(crawl_dir, &cr) != 0) return 2;
    if (cr.shard_count == 0) {
        fprintf(stderr, PROG ": %s: no usable shards\n", crawl_dir);
        crawl_result_free(&cr);
        return 2;
    }
    g_stored_root = crawl_result_stored_root(&cr);
    if (!g_stored_root || !g_stored_root[0]) {
        fprintf(stderr, PROG ": %s: crawl_manifest.txt has no start_path\n", crawl_dir);
        crawl_result_free(&cr);
        return 2;
    }

    {
        char *root_copy = strdup(g_stored_root);

        if (!root_copy) {
            fprintf(stderr, PROG ": out of memory\n");
            crawl_result_free(&cr);
            return 2;
        }
        path_rstrip_slashes(root_copy);
        g_stored_root = root_copy;
    }

    if (g_only) {
        char *only_copy = strdup(g_only);
        const char *p;
        int st;

        if (!only_copy) {
            fprintf(stderr, PROG ": out of memory\n");
            crawl_result_free(&cr);
            return 2;
        }
        path_rstrip_slashes(only_copy);
        st = strip_to_rel(only_copy, g_stored_root, g_only_rel, sizeof(g_only_rel));
        if (st < 0) {
            fprintf(stderr, PROG ": --only %s is outside the crawl root %s\n", only_copy, g_stored_root);
            crawl_result_free(&cr);
            return 2;
        }
        if (st == 1) g_only_rel[0] = '\0'; /* prefix == crawl root: no filter */
        g_only_rel_len = strlen(g_only_rel);
        g_only_depth = 0;
        if (g_only_rel_len) {
            g_only_depth = 1;
            for (p = g_only_rel; *p; p++)
                if (*p == '/') g_only_depth++;
        }
        g_only = only_copy;
    }

    if (prepare_out_dir(out_dir) != 0) {
        crawl_result_free(&cr);
        return 2;
    }
    {
        char *out_copy = strdup(out_dir);

        if (!out_copy) {
            fprintf(stderr, PROG ": out of memory\n");
            crawl_result_free(&cr);
            return 2;
        }
        path_rstrip_slashes(out_copy);
        g_out_dir = out_copy;
        g_out_dir_len = strlen(g_out_dir);
    }

    warn_sparse_if_needed(crawl_dir);
    if (fill_block(g_seed, g_block_size) != 0) {
        fprintf(stderr, PROG ": out of memory\n");
        return 2;
    }
    if (hl_init() != 0) {
        fprintf(stderr, PROG ": hardlink table init failed\n");
        return 2;
    }

    g_shard_count = cr.shard_count;
    g_cats = (crawl_bin_catalog_t *)calloc(g_shard_count, sizeof(*g_cats));
    if (!g_cats) {
        fprintf(stderr, PROG ": out of memory\n");
        return 2;
    }

    t0 = now_sec();
    for (si = 0; si < cr.shard_count; si++) {
        if (intern_shard_dirs(&cr, si) != 0) {
            crawl_result_free(&cr);
            return 1;
        }
    }
    g_dir_id_max = g_intern.used;
    if (g_only_rel_len && !g_only_seen) {
        fprintf(stderr, PROG ": --only %s: directory not found in the crawl\n", g_only);
        crawl_result_free(&cr);
        return 2;
    }
    if (mkdir_interned_dirs() != 0) {
        crawl_result_free(&cr);
        return 1;
    }

    for (si = 0; si < cr.shard_count; si++) {
        if (split_shard_into_jobs(&cr.shards[si], si, &total_recs) != 0) {
            crawl_result_free(&cr);
            return 1;
        }
    }
    if (g_dir_id_max + 1ULL + total_recs >= EDUMP_NAME_SPACE) {
        fprintf(stderr, PROG ": too many entries for 8-character names\n");
        return 1;
    }

    if (g_only_rel_len)
        fprintf(stderr, PROG ": dumping only subtree %s (rel %s)\n", g_only, g_only_rel);
    fprintf(stderr, PROG ": %zu shard(s), %zu job(s), %" PRIu64 " dirs, %" PRIu64 " records, %d writer(s)\n",
            cr.shard_count, g_jobs.chunk_count, g_dir_id_max, total_recs, g_writers);

    atomic_store(&g_next_chunk, 0);
    atomic_store(&g_failed, 0);
    th = (pthread_t *)calloc((size_t)g_writers, sizeof(*th));
    if (!th) {
        fprintf(stderr, PROG ": out of memory\n");
        return 1;
    }
    if (progress_wanted()) {
        atomic_store(&g_progress_done, 0);
        if (pthread_create(&pth, NULL, progress_main, NULL) == 0) prog_on = 1;
    }
    for (w = 0; w < g_writers; w++) {
        if (pthread_create(&th[w], NULL, worker_main, NULL) != 0) {
            atomic_store(&g_failed, 1);
            break;
        }
        nstarted++;
    }
    for (w = 0; w < nstarted; w++) pthread_join(th[w], NULL);
    free(th);
    if (prog_on) {
        atomic_store(&g_progress_done, 1);
        pthread_join(pth, NULL);
        if (isatty(STDERR_FILENO)) fputc('\n', stderr);
    }

    {
        double elapsed = now_sec() - t0;
        int fail = atomic_load(&g_failed);

        printf("dirs=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_dirs));
        printf("files=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_files));
        printf("hardlinks=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_hardlinks));
        printf("symlinks=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_symlinks));
        printf("fifos=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_fifos));
        printf("devices=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_devices));
        printf("skipped=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_skipped));
        printf("bytes_written=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_bytes));
        printf("collisions=%" PRIu64 "\n", (uint64_t)atomic_load(&g_n_collisions));
        printf("elapsed_sec=%.3f\n", elapsed);
        if (fail || atomic_load(&g_n_collisions)) return 1;
    }

    intern_free(&g_intern);
    hl_free();
    if (g_cats) {
        for (si = 0; si < g_shard_count; si++) crawl_bin_catalog_free(&g_cats[si]);
        free(g_cats);
    }
    crawl_bin_free_chunk_array_rows(g_jobs.chunks, g_jobs.chunk_count);
    free(g_jobs.chunk_rec_base);
    free(g_block);
    crawl_result_free(&cr);
    return 0;
}
