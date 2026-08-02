/*
 * Per-thread parking cache for read-only shard FILE handles — see crawl_fpcache.h.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#include "crawl_fpcache.h"

#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

/*
 * Two slots cover the patterns that matter — a shard's .bin plus its .ckpt during
 * chunk planning, and "load this shard's catalog, then scan a chunk of it" during
 * the record pass. Each slot costs an fd and, once buffered, one stdio buffer per
 * thread, so this is deliberately small rather than a general LRU.
 */
#define FPCACHE_SLOTS 2

typedef struct {
    FILE *fp;
    int parked; /* 1: idle and reusable; 0: handed out to a caller */
    char path[PATH_MAX];
} fpcache_slot_t;

static _Thread_local fpcache_slot_t tls_slots[FPCACHE_SLOTS];
static _Thread_local int tls_registered;

static atomic_ullong g_fpcache_real_opens;
static atomic_ullong g_fpcache_hits;
static size_t g_fpcache_bufsz; /* set once before threads start */

static pthread_key_t g_fpcache_key;
static pthread_once_t g_fpcache_once = PTHREAD_ONCE_INIT;

static void fpcache_thread_dtor(void *unused) {
    (void)unused;
    crawl_fpcache_release_thread();
}

static void fpcache_key_init(void) {
    (void)pthread_key_create(&g_fpcache_key, fpcache_thread_dtor);
}

/*
 * Arrange for this thread's parked handles to be closed when it exits. The slots
 * live in thread-local storage; the pthread key exists only so glibc calls the
 * destructor, which is why the value stored is a dummy non-NULL pointer.
 */
static void fpcache_register_thread(void) {
    if (tls_registered) return;
    tls_registered = 1;
    (void)pthread_once(&g_fpcache_once, fpcache_key_init);
    (void)pthread_setspecific(g_fpcache_key, (void *)&tls_slots[0]);
}

/* Cacheable = read-only: "r", "rb", never "r+" (a stream that can write must not be shared blindly). */
static int fpcache_mode_is_read_only(const char *mode) {
    const char *m;

    if (!mode || mode[0] != 'r') return 0;
    for (m = mode + 1; *m; m++)
        if (*m == '+') return 0;
    return 1;
}

FILE *crawl_fpcache_fopen(const char *path, const char *mode) {
    int i;
    int free_slot = -1;
    int park_victim = -1;
    FILE *fp;

    if (!path || !fpcache_mode_is_read_only(mode)) return fopen(path, mode);

    for (i = 0; i < FPCACHE_SLOTS; i++) {
        if (!tls_slots[i].fp) {
            if (free_slot < 0) free_slot = i;
            continue;
        }
        if (!tls_slots[i].parked) continue;
        if (strcmp(tls_slots[i].path, path) == 0) {
            fp = tls_slots[i].fp;
            tls_slots[i].parked = 0;
            clearerr(fp);
            if (fseeko(fp, 0, SEEK_SET) != 0) {
                /* Unusable handle: drop it and fall through to a real open. */
                fclose(fp);
                tls_slots[i].fp = NULL;
                tls_slots[i].path[0] = '\0';
                free_slot = i;
                break;
            }
            atomic_fetch_add_explicit(&g_fpcache_hits, 1, memory_order_relaxed);
            return fp;
        }
        if (park_victim < 0) park_victim = i;
    }

    fp = fopen(path, mode);
    atomic_fetch_add_explicit(&g_fpcache_real_opens, 1, memory_order_relaxed);
    if (!fp) return NULL;
    if (g_fpcache_bufsz) (void)setvbuf(fp, NULL, _IOFBF, g_fpcache_bufsz);

    if (free_slot < 0 && park_victim >= 0) {
        fclose(tls_slots[park_victim].fp);
        tls_slots[park_victim].fp = NULL;
        free_slot = park_victim;
    }
    if (free_slot >= 0 && strlen(path) < sizeof(tls_slots[free_slot].path)) {
        fpcache_register_thread();
        tls_slots[free_slot].fp = fp;
        tls_slots[free_slot].parked = 0;
        memcpy(tls_slots[free_slot].path, path, strlen(path) + 1);
    }
    /* No slot (every one handed out) or the path does not fit: untracked, closed normally. */
    return fp;
}

int crawl_fpcache_fclose(FILE *fp) {
    int i;

    if (!fp) return EOF;
    for (i = 0; i < FPCACHE_SLOTS; i++) {
        if (tls_slots[i].fp != fp || tls_slots[i].parked) continue;
        tls_slots[i].parked = 1;
        return 0;
    }
    return fclose(fp);
}

void crawl_fpcache_release_thread(void) {
    int i;

    for (i = 0; i < FPCACHE_SLOTS; i++) {
        if (!tls_slots[i].fp || !tls_slots[i].parked) continue;
        fclose(tls_slots[i].fp);
        tls_slots[i].fp = NULL;
        tls_slots[i].path[0] = '\0';
    }
}

void crawl_fpcache_set_bufsz(size_t bytes) {
    g_fpcache_bufsz = bytes;
}

uint64_t crawl_fpcache_real_opens(void) {
    return (uint64_t)atomic_load_explicit(&g_fpcache_real_opens, memory_order_relaxed);
}

uint64_t crawl_fpcache_hits(void) {
    return (uint64_t)atomic_load_explicit(&g_fpcache_hits, memory_order_relaxed);
}
