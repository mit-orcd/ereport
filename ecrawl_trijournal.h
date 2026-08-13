#ifndef ECRAWL_TRIJOURNAL_H
#define ECRAWL_TRIJOURNAL_H

/*
 * ecrawl crawl-time trigram journal pool (--trigram-journal DIR).
 *
 * Writer threads tee each record batch (full-path wire form) into the pool by
 * refcount; pool threads extract basename trigram codes and append them to
 * per-shard journals (crawl_trijournal format). The tee never blocks the
 * crawl: the handoff queues are bounded try-push, and the first overflow sets
 * a run-global void flag — the pool then drains without publishing and
 * ereport_index falls back to parsing the capture.
 *
 * Batches are fanned out to every pool queue (shared buffer, one refcount per
 * queue); pool thread t only decodes frames with shard % nthreads == t, so
 * each shard's journal state is single-threaded.
 */

#include <stddef.h>
#include <stdint.h>

#include "crawl_trijournal.h"
#include "ecrawl_wire.h"

typedef struct trijournal_pool trijournal_pool_t;

/*
 * Start the pool: nthreads queues/threads, each bounded at queue_batches
 * messages. dir must already exist. shard_name_fn maps a shard id to the
 * capture shard's basename (e.g. uid_shard_0007.bin). Returns 0 on success.
 */
int trijournal_pool_start(trijournal_pool_t **out, const char *dir, uint32_t nshards, int nthreads,
                          unsigned queue_batches, int (*shard_name_fn)(uint32_t, char *, size_t));

/*
 * Tee one batch. On success the pool holds its references and releases them
 * after processing. On overflow the run-global void flag is set (the batch is
 * still released correctly everywhere) and the caller keeps its own reference
 * either way. Never blocks.
 */
void trijournal_pool_offer(trijournal_pool_t *p, record_batch_t *batch);

/*
 * Blocking push of one writer's finalize facts (shards[i] finalized with
 * facts[i]); the pool copies both arrays. Called from writer teardown.
 */
int trijournal_pool_finalize_shards(trijournal_pool_t *p, const uint32_t *shards,
                                    const trij_binding_t *facts, size_t n);

/*
 * Close the queues and join the pool threads. Journals that received a
 * finalize are published (header + rename) unless the void flag is set;
 * everything else is deleted. Returns 0, or -1 when the pool voided or hit an
 * error (the capture itself is unaffected; treat as a warning). The struct
 * stays valid for the accessors below until trijournal_pool_free.
 */
int trijournal_pool_finish(trijournal_pool_t *p);
void trijournal_pool_free(trijournal_pool_t *p);

int trijournal_pool_voided(const trijournal_pool_t *p);
uint64_t trijournal_pool_bytes(const trijournal_pool_t *p); /* compressed journal bytes written */
uint64_t trijournal_pool_entries(const trijournal_pool_t *p);
uint64_t trijournal_pool_published(const trijournal_pool_t *p); /* finalized shard journals */

#endif /* ECRAWL_TRIJOURNAL_H */
