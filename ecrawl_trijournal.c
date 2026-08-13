/*
 * ecrawl_trijournal — crawl-time trigram journal pool (see ecrawl_trijournal.h).
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _XOPEN_SOURCE 700

#include "ecrawl_trijournal.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "crawl_bin_format.h"
#include "trigram_extract.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* Open journal fds per pool thread; the fd is only touched at block flush/finalize. */
#define TRIJ_POOL_FD_CAP 64u

typedef enum { TRIJ_MSG_BATCH, TRIJ_MSG_FINALIZE } trij_msg_kind_t;

typedef struct {
    trij_msg_kind_t kind;
    record_batch_t *batch; /* BATCH: one pool reference */
    uint32_t *shards;      /* FINALIZE: owned by the message */
    trij_binding_t *facts; /* FINALIZE: owned by the message */
    size_t facts_n;
} trij_msg_t;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond_nonempty;
    pthread_cond_t cond_nonfull;
    trij_msg_t *ring;
    size_t cap;
    size_t head;
    size_t count;
    int closed;
} trij_msg_queue_t;

typedef struct trij_shard_state {
    trij_writer_t w;
    uint64_t bytes_accounted; /* w.bytes_written already added to pool->bytes */
    int open;   /* writer created (.tmp exists) */
    int on_lru; /* fd is open and linked into the lane LRU */
    struct trij_shard_state *lru_prev;
    struct trij_shard_state *lru_next;
} trij_shard_state_t;

typedef struct {
    struct trijournal_pool *pool;
    uint32_t lane_index;
    trij_msg_queue_t q;
    int q_inited;
    trij_shard_state_t *shards; /* nshards entries; only shard % nthreads == lane_index used */
    trigram_scratch_t scratch;
    trij_shard_state_t *lru_head; /* fd-open list, most recent first */
    trij_shard_state_t *lru_tail;
    unsigned fd_open;
    uint64_t entries_local; /* folded into pool->entries per batch, not per record */
    char pathbuf[PATH_MAX];
} trij_lane_t;

typedef struct trijournal_pool {
    char *dir;
    uint32_t nshards;
    int nthreads;
    trij_lane_t *lanes;
    pthread_t *threads;
    int (*shard_name_fn)(uint32_t shard, char *buf, size_t cap);
    _Atomic int voided;
    _Atomic int error;
    _Atomic uint64_t bytes;
    _Atomic uint64_t entries;
    _Atomic uint64_t published;
} trijournal_pool_t;

/* ------------------------------------------------------------ message queue */

static int trij_queue_init(trij_msg_queue_t *q, size_t cap) {
    memset(q, 0, sizeof(*q));
    q->ring = (trij_msg_t *)calloc(cap, sizeof(*q->ring));
    if (!q->ring) return -1;
    q->cap = cap;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond_nonempty, NULL);
    pthread_cond_init(&q->cond_nonfull, NULL);
    return 0;
}

static void trij_queue_destroy(trij_msg_queue_t *q) {
    free(q->ring);
    q->ring = NULL;
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond_nonempty);
    pthread_cond_destroy(&q->cond_nonfull);
}

/* 0 = queued, -1 = full or closed. Never blocks. */
static int trij_queue_try_push(trij_msg_queue_t *q, const trij_msg_t *msg) {
    int rc = -1;

    pthread_mutex_lock(&q->mutex);
    if (!q->closed && q->count < q->cap) {
        q->ring[(q->head + q->count) % q->cap] = *msg;
        q->count++;
        pthread_cond_signal(&q->cond_nonempty);
        rc = 0;
    }
    pthread_mutex_unlock(&q->mutex);
    return rc;
}

/* Finalize messages must not be dropped; blocks while full. */
static int trij_queue_push_blocking(trij_msg_queue_t *q, const trij_msg_t *msg) {
    pthread_mutex_lock(&q->mutex);
    while (!q->closed && q->count >= q->cap)
        pthread_cond_wait(&q->cond_nonfull, &q->mutex);
    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    q->ring[(q->head + q->count) % q->cap] = *msg;
    q->count++;
    pthread_cond_signal(&q->cond_nonempty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* NULL = closed and drained. */
static int trij_queue_pop(trij_msg_queue_t *q, trij_msg_t *out) {
    pthread_mutex_lock(&q->mutex);
    for (;;) {
        if (q->count > 0) break;
        if (q->closed) {
            pthread_mutex_unlock(&q->mutex);
            return -1;
        }
        pthread_cond_wait(&q->cond_nonempty, &q->mutex);
    }
    *out = q->ring[q->head];
    q->head = (q->head + 1) % q->cap;
    q->count--;
    pthread_cond_signal(&q->cond_nonfull);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static void trij_queue_close(trij_msg_queue_t *q) {
    pthread_mutex_lock(&q->mutex);
    q->closed = 1;
    pthread_cond_broadcast(&q->cond_nonempty);
    pthread_cond_broadcast(&q->cond_nonfull);
    pthread_mutex_unlock(&q->mutex);
}

/* ----------------------------------------------------------------- fd LRU */

static void trij_lane_lru_unlink(trij_lane_t *lane, trij_shard_state_t *st) {
    if (!st->on_lru) return;
    if (st->lru_prev) st->lru_prev->lru_next = st->lru_next;
    else lane->lru_head = st->lru_next;
    if (st->lru_next) st->lru_next->lru_prev = st->lru_prev;
    else lane->lru_tail = st->lru_prev;
    st->lru_prev = st->lru_next = NULL;
    st->on_lru = 0;
    lane->fd_open--;
}

static void trij_lane_lru_push_head(trij_lane_t *lane, trij_shard_state_t *st) {
    st->lru_prev = NULL;
    st->lru_next = lane->lru_head;
    if (lane->lru_head) lane->lru_head->lru_prev = st;
    else lane->lru_tail = st;
    lane->lru_head = st;
    st->on_lru = 1;
    lane->fd_open++;
}

/* The fd is needed for a flush/finalize: reopen if evicted, evict tail if over cap. */
static int trij_lane_shard_fd(trij_lane_t *lane, trij_shard_state_t *st) {
    if (st->on_lru) {
        trij_lane_lru_unlink(lane, st);
        trij_lane_lru_push_head(lane, st);
        return 0;
    }
    while (lane->fd_open >= TRIJ_POOL_FD_CAP && lane->lru_tail) {
        trij_shard_state_t *victim = lane->lru_tail;
        trij_lane_lru_unlink(lane, victim);
        trij_writer_close_fd(&victim->w);
    }
    if (trij_writer_reopen(&st->w) != 0) return -1;
    trij_lane_lru_push_head(lane, st);
    return 0;
}

/* ------------------------------------------------------------ batch decode */

static void trij_lane_void(trij_lane_t *lane) {
    atomic_store_explicit(&lane->pool->voided, 1, memory_order_relaxed);
}

static int trij_lane_append(trij_lane_t *lane, uint32_t shard, const char *path, size_t path_len,
                            uint64_t uid, uint8_t type, const uint32_t *codes, size_t code_count) {
    trijournal_pool_t *pool = lane->pool;
    trij_shard_state_t *st = &lane->shards[shard];
    uint64_t bytes_before;

    if (!st->open) {
        char name[64];

        if (pool->shard_name_fn(shard, name, sizeof(name)) != 0) return -1;
        if (trij_writer_create(&st->w, pool->dir, name) != 0) return -1;
        st->open = 1;
    }

    /* The fd is only touched when a block flushes; open it under the lane's fd
     * budget when this entry can cross the block target (upper-bound entry size). */
    if (st->w.block_len + path_len + 5 * code_count + 32 >= TRIJ_BLOCK_TARGET_BYTES &&
        trij_lane_shard_fd(lane, st) != 0)
        return -1;

    bytes_before = st->w.bytes_written;
    if (trij_writer_append(&st->w, path, path_len, uid, type, codes, code_count) != 0) return -1;
    if (st->w.bytes_written != bytes_before) {
        atomic_fetch_add_explicit(&pool->bytes, st->w.bytes_written - bytes_before, memory_order_relaxed);
        st->bytes_accounted = st->w.bytes_written;
    }
    return 0;
}

/* One frame is one record: batch_frame_hdr_t, then bin_record_hdr_t + full-path name bytes. */
static void trij_lane_process_batch(trij_lane_t *lane, const record_batch_t *batch) {
    trijournal_pool_t *pool = lane->pool;
    size_t scan = 0;

    while (scan + sizeof(batch_frame_hdr_t) <= batch->len) {
        batch_frame_hdr_t fh;
        bin_record_hdr_t wire;
        size_t payload_off;
        uint32_t *codes = NULL;
        size_t code_count = 0;

        memcpy(&fh, batch->data + scan, sizeof(fh));
        payload_off = scan + sizeof(fh);
        if ((uint64_t)payload_off + (uint64_t)fh.data_len > (uint64_t)batch->len) {
            trij_lane_void(lane);
            return;
        }
        scan = payload_off + fh.data_len;
        if (fh.shard >= pool->nshards) {
            trij_lane_void(lane);
            return;
        }
        if (fh.shard % (uint32_t)pool->nthreads != lane->lane_index) continue;
        if (fh.data_len < sizeof(wire)) {
            trij_lane_void(lane);
            return;
        }
        memcpy(&wire, batch->data + payload_off, sizeof(wire));
        if (wire.parent_dir_id != 0ULL || wire.name_len == 0 ||
            (uint64_t)fh.data_len != (uint64_t)sizeof(wire) + (uint64_t)wire.name_len ||
            (size_t)wire.name_len + 1U > sizeof(lane->pathbuf)) {
            trij_lane_void(lane);
            return;
        }
        memcpy(lane->pathbuf, batch->data + payload_off + sizeof(wire), wire.name_len);
        lane->pathbuf[wire.name_len] = '\0';

        if (trigram_extract_path(lane->pathbuf, &codes, &code_count, &lane->scratch) != 0) {
            trij_lane_void(lane);
            return;
        }
        if (trij_lane_append(lane, fh.shard, lane->pathbuf, (size_t)wire.name_len, wire.uid, wire.type,
                             codes, code_count) != 0) {
            trij_lane_void(lane);
            return;
        }
        lane->entries_local++;
    }
    if (lane->entries_local) {
        atomic_fetch_add_explicit(&pool->entries, lane->entries_local, memory_order_relaxed);
        lane->entries_local = 0;
    }
    if (scan != batch->len) trij_lane_void(lane);
}

/* -------------------------------------------------------------- finalization */

static void trij_lane_finalize(trij_lane_t *lane, const uint32_t *shards, const trij_binding_t *facts,
                               size_t n) {
    trijournal_pool_t *pool = lane->pool;
    size_t i;

    for (i = 0; i < n; i++) {
        uint32_t shard = shards[i];
        trij_shard_state_t *st;

        if (shard >= pool->nshards || shard % (uint32_t)pool->nthreads != lane->lane_index) continue;
        st = &lane->shards[shard];
        if (!st->open) continue; /* no records were journaled for this shard */
        if (atomic_load_explicit(&pool->voided, memory_order_relaxed)) {
            trij_lane_lru_unlink(lane, st);
            trij_writer_abort(&st->w);
            st->open = 0;
            continue;
        }
        if (trij_lane_shard_fd(lane, st) != 0 || trij_writer_finalize(&st->w, &facts[i]) != 0) {
            atomic_store_explicit(&pool->error, 1, memory_order_relaxed);
            trij_lane_void(lane);
            trij_lane_lru_unlink(lane, st);
            trij_writer_abort(&st->w);
            st->open = 0;
            continue;
        }
        /* finalize closed the fd and renamed the file; credit the final block
         * flush and header rewrite that no append witnessed. */
        trij_lane_lru_unlink(lane, st);
        if (st->w.bytes_written > st->bytes_accounted)
            atomic_fetch_add_explicit(&pool->bytes, st->w.bytes_written - st->bytes_accounted,
                                      memory_order_relaxed);
        atomic_fetch_add_explicit(&pool->published, 1, memory_order_relaxed);
    }
}

/* Unfinalized journals are voided at lane exit: close fds and delete the .tmp files. */
static void trij_lane_abort_all(trij_lane_t *lane) {
    trijournal_pool_t *pool = lane->pool;
    uint32_t s;

    for (s = lane->lane_index; s < pool->nshards; s += (uint32_t)pool->nthreads) {
        trij_shard_state_t *st = &lane->shards[s];
        if (st->open) {
            trij_writer_abort(&st->w);
            st->open = 0;
        }
    }
    lane->lru_head = lane->lru_tail = NULL;
    lane->fd_open = 0;
}

static void *trij_lane_main(void *arg_void) {
    trij_lane_t *lane = (trij_lane_t *)arg_void;
    trijournal_pool_t *pool = lane->pool;
    trij_msg_t msg;

    for (;;) {
        if (trij_queue_pop(&lane->q, &msg) != 0) break;
        if (msg.kind == TRIJ_MSG_BATCH) {
            if (!atomic_load_explicit(&pool->voided, memory_order_relaxed))
                trij_lane_process_batch(lane, msg.batch);
            record_batch_release(msg.batch);
        } else {
            trij_lane_finalize(lane, msg.shards, msg.facts, msg.facts_n);
            free(msg.shards);
            free(msg.facts);
        }
    }

    trij_lane_abort_all(lane);
    free(lane->scratch.codes_buf);
    lane->scratch.codes_buf = NULL;
    lane->scratch.codes_cap = 0;
    free(lane->scratch.lower_seg_buf);
    lane->scratch.lower_seg_buf = NULL;
    lane->scratch.lower_seg_cap = 0;
    return NULL;
}

/* ------------------------------------------------------------------ pool API */

int trijournal_pool_start(trijournal_pool_t **out, const char *dir, uint32_t nshards, int nthreads,
                          unsigned queue_batches, int (*shard_name_fn)(uint32_t, char *, size_t)) {
    trijournal_pool_t *p;
    int t;

    *out = NULL;
    if (!dir || nshards == 0 || nthreads < 1 || queue_batches < 1 || !shard_name_fn) return -1;
    p = (trijournal_pool_t *)calloc(1, sizeof(*p));
    if (!p) return -1;
    p->dir = strdup(dir);
    p->nshards = nshards;
    p->nthreads = nthreads;
    p->shard_name_fn = shard_name_fn;
    p->lanes = (trij_lane_t *)calloc((size_t)nthreads, sizeof(*p->lanes));
    p->threads = (pthread_t *)calloc((size_t)nthreads, sizeof(*p->threads));
    if (!p->dir || !p->lanes || !p->threads) goto fail;

    for (t = 0; t < nthreads; t++) {
        trij_lane_t *lane = &p->lanes[t];

        lane->pool = p;
        lane->lane_index = (uint32_t)t;
        lane->shards = (trij_shard_state_t *)calloc(nshards, sizeof(*lane->shards));
        if (!lane->shards) goto fail;
        for (uint32_t s = 0; s < nshards; s++) lane->shards[s].w.fd = -1;
        if (trij_queue_init(&lane->q, queue_batches) != 0) goto fail;
        lane->q_inited = 1;
    }
    for (t = 0; t < nthreads; t++) {
        if (pthread_create(&p->threads[t], NULL, trij_lane_main, &p->lanes[t]) != 0) {
            /* Stop the lanes that started; join them before unwinding. */
            atomic_store_explicit(&p->voided, 1, memory_order_relaxed);
            for (int k = 0; k < t; k++) trij_queue_close(&p->lanes[k].q);
            for (int k = 0; k < t; k++) pthread_join(p->threads[k], NULL);
            goto fail;
        }
    }
    *out = p;
    return 0;

fail:
    if (p->lanes) {
        for (t = 0; t < nthreads; t++) {
            free(p->lanes[t].shards);
            if (p->lanes[t].q_inited) trij_queue_destroy(&p->lanes[t].q);
        }
    }
    free(p->lanes);
    free(p->threads);
    free(p->dir);
    free(p);
    return -1;
}

void trijournal_pool_offer(trijournal_pool_t *p, record_batch_t *batch) {
    int t;

    if (atomic_load_explicit(&p->voided, memory_order_relaxed)) return;
    for (t = 0; t < p->nthreads; t++) {
        trij_msg_t msg;

        memset(&msg, 0, sizeof(msg));
        msg.kind = TRIJ_MSG_BATCH;
        msg.batch = batch;
        atomic_fetch_add_explicit(&batch->refs, 1u, memory_order_relaxed);
        if (trij_queue_try_push(&p->lanes[t].q, &msg) != 0) {
            record_batch_release(batch);
            /* Bounded queue full: the crawl must not wait on the journal. Void the run. */
            atomic_store_explicit(&p->voided, 1, memory_order_relaxed);
            return;
        }
    }
}

int trijournal_pool_finalize_shards(trijournal_pool_t *p, const uint32_t *shards,
                                    const trij_binding_t *facts, size_t n) {
    int t;
    int rc = 0;

    for (t = 0; t < p->nthreads; t++) {
        trij_msg_t msg;
        size_t i;

        memset(&msg, 0, sizeof(msg));
        msg.kind = TRIJ_MSG_FINALIZE;
        msg.shards = (uint32_t *)malloc((n ? n : 1) * sizeof(*msg.shards));
        msg.facts = (trij_binding_t *)malloc((n ? n : 1) * sizeof(*msg.facts));
        if (!msg.shards || !msg.facts) {
            free(msg.shards);
            free(msg.facts);
            return -1;
        }
        msg.facts_n = 0;
        for (i = 0; i < n; i++) {
            if (shards[i] % (uint32_t)p->nthreads != (uint32_t)t) continue;
            msg.shards[msg.facts_n] = shards[i];
            msg.facts[msg.facts_n] = facts[i];
            msg.facts_n++;
        }
        if (trij_queue_push_blocking(&p->lanes[t].q, &msg) != 0) {
            free(msg.shards);
            free(msg.facts);
            rc = -1;
        }
    }
    return rc;
}

int trijournal_pool_finish(trijournal_pool_t *p) {
    int t;

    for (t = 0; t < p->nthreads; t++) trij_queue_close(&p->lanes[t].q);
    for (t = 0; t < p->nthreads; t++) pthread_join(p->threads[t], NULL);

    return (atomic_load(&p->voided) || atomic_load(&p->error)) ? -1 : 0;
}

void trijournal_pool_free(trijournal_pool_t *p) {
    int t;

    if (!p) return;
    for (t = 0; t < p->nthreads; t++) {
        free(p->lanes[t].shards);
        if (p->lanes[t].q_inited) trij_queue_destroy(&p->lanes[t].q);
    }
    free(p->lanes);
    free(p->threads);
    free(p->dir);
    free(p);
}

int trijournal_pool_voided(const trijournal_pool_t *p) {
    return atomic_load(&p->voided);
}

uint64_t trijournal_pool_bytes(const trijournal_pool_t *p) {
    return atomic_load(&p->bytes);
}

uint64_t trijournal_pool_entries(const trijournal_pool_t *p) {
    return atomic_load(&p->entries);
}

uint64_t trijournal_pool_published(const trijournal_pool_t *p) {
    return atomic_load(&p->published);
}
