/*
 * crawl_bin_block — zstd block (de)compression for v7 uid_shard_*.bin record
 * regions. Shared by the writer (ecrawl), the chunker, and every reader
 * (ereport, ereport_index, ecrawl_analyze, ecrawl_repair).
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_BIN_BLOCK_H
#define CRAWL_BIN_BLOCK_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "crawl_bin_format.h"
#include "crawl_bin_chunks.h" /* crawl_bin_chunk_stdio_t */

/* zstd compression level for record blocks (override with ECRAWL_ZSTD_LEVEL). */
#define CRAWL_BIN_ZSTD_DEFAULT_LEVEL 3

/* ----------------------------------------------------------------------------
 * Writer side: accumulate serialized records, flush them as compressed blocks.
 * -------------------------------------------------------------------------- */
typedef struct {
    unsigned char *raw; /* accumulated uncompressed record bytes */
    size_t raw_len;
    size_t raw_cap;
    unsigned char *comp; /* scratch for the compressed frame */
    size_t comp_cap;
    int level;
    /* Summary of the pending block, written into its bin_block_hdr_t on flush. */
    uint64_t max_record_size;
    uint16_t type_mask;
    uint32_t record_count;
} crawl_bin_block_writer_t;

typedef size_t (*crawl_bin_block_fwrite_fn)(const void *ptr, size_t size, size_t nmemb, FILE *stream);

int crawl_bin_block_writer_init(crawl_bin_block_writer_t *w);
void crawl_bin_block_writer_free(crawl_bin_block_writer_t *w);

/* Append one complete record and update the pending block's query metadata. */
int crawl_bin_block_writer_append_record(crawl_bin_block_writer_t *w, const bin_record_hdr_t *hdr,
                                         const void *name);

/* Bytes currently buffered (un-flushed) in the pending block. */
static inline size_t crawl_bin_block_writer_pending(const crawl_bin_block_writer_t *w) {
    return w->raw_len;
}

/*
 * Compress and write the pending block via wfwrite, then reset. On success sets
 * *bytes_written_out to the on-disk size of the block (sizeof(bin_block_hdr_t) +
 * comp_size), or 0 when nothing was pending. Returns 0 on success, -1 on error.
 */
int crawl_bin_block_writer_flush(crawl_bin_block_writer_t *w, FILE *fp, crawl_bin_block_fwrite_fn wfwrite,
                                 uint64_t *bytes_written_out);

/* ----------------------------------------------------------------------------
 * Reader side: iterate records across the compressed blocks in a byte range.
 * -------------------------------------------------------------------------- */
typedef struct {
    crawl_bin_chunk_stdio_t io;
    FILE *fp;
    uint64_t pos; /* file offset of the next block to read */
    uint64_t end; /* exclusive end file offset of this range */
    unsigned char *comp;
    size_t comp_cap;
    unsigned char *raw;
    size_t raw_cap;
    size_t raw_len; /* decompressed size of the current block */
    size_t raw_off; /* cursor within the current block */
    /* Optional block-skipping predicate; see crawl_bin_block_reader_set_filter. */
    uint64_t size_gt;
    int have_size_gt;
    uint16_t type_bit;
    uint64_t blocks_decompressed;
    uint64_t blocks_skipped;
    uint64_t records_skipped; /* records in skipped blocks, from their headers */
    void *dctx; /* reusable ZSTD_DCtx, kept opaque in the public header */
} crawl_bin_block_reader_t;

/*
 * Initialise a reader over [start_off, end_off) of an already-open shard fp.
 * Both offsets must be block boundaries (catalog_offset or chunk bounds). io
 * provides the fread used to pull compressed bytes (so callers can keep I/O
 * accounting). Returns 0 on success.
 */
int crawl_bin_block_reader_init(crawl_bin_block_reader_t *r, const crawl_bin_chunk_stdio_t *io, FILE *fp,
                                uint64_t start_off, uint64_t end_off);

/* Reuse an initialized reader's ZSTD context and buffers for another range. */
int crawl_bin_block_reader_reinit(crawl_bin_block_reader_t *r, const crawl_bin_chunk_stdio_t *io, FILE *fp,
                                  uint64_t start_off, uint64_t end_off);

/*
 * Skip whole blocks whose bin_block_hdr_t summary proves no record inside can
 * match: size_gt is the strict lower bound from --size-gt, type_filter is a
 * record type code (0 for none). The predicate only ever skips blocks, so
 * results are identical to an unfiltered scan; it must be re-armed after each
 * reinit. Returns -1 if neither term is usable, leaving the reader unfiltered.
 */
int crawl_bin_block_reader_set_filter(crawl_bin_block_reader_t *r, int have_size_gt, uint64_t size_gt,
                                      int type_filter);

/*
 * Yield the next record. Copies the fixed header into *hdr and points *name at
 * the name bytes inside the reader's decompression buffer (valid only until the
 * next call). Returns 1 for a record, 0 at end of range, -1 on error.
 */
int crawl_bin_block_reader_next(crawl_bin_block_reader_t *r, bin_record_hdr_t *hdr, const unsigned char **name);

void crawl_bin_block_reader_free(crawl_bin_block_reader_t *r);

#endif /* CRAWL_BIN_BLOCK_H */
