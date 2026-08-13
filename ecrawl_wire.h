#ifndef ECRAWL_WIRE_H
#define ECRAWL_WIRE_H

/*
 * Wire types shared by ecrawl's emit/writer path and the crawl-time trigram
 * journal pool (ecrawl_trijournal.c). A record batch is a byte buffer of
 * frames, each [batch_frame_hdr_t][bin_record_hdr_t + name bytes]; while
 * parent_dir_id == 0 the name bytes carry the record's full stored path.
 */

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct __attribute__((packed)) {
    uint32_t shard;
    uint32_t data_len;
    /* Per-record byte contribution (account_entry_local result). Wire-only —
     * carried so the writer can roll it into per-dir catalog aggregates without
     * re-running hardlink dedup. Not written to disk. */
    uint64_t byte_credit;
} batch_frame_hdr_t;

typedef struct record_batch {
    unsigned char *data;
    size_t len;
    struct record_batch *next;
    /* 1 for the owning writer queue; +1 per trigram-journal pool queue the
     * batch was teed into. Freed when the last reference drops. */
    _Atomic unsigned refs;
} record_batch_t;

static inline void record_batch_release(record_batch_t *batch) {
    if (atomic_fetch_sub_explicit(&batch->refs, 1u, memory_order_acq_rel) == 1u) {
        free(batch->data);
        free(batch);
    }
}

#endif /* ECRAWL_WIRE_H */
