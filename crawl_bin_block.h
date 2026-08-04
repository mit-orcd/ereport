/*
 * crawl_bin_block — columnar row-group (de)serialization for v8 uid_shard_*.bin
 * record regions. Shared by the writer (ecrawl), the chunker, and every reader
 * (ereport, ereport_index, ecrawl_query, ecrawl_repair, ecrawl_mount).
 *
 * The file keeps its historical name because it still owns "the record region",
 * but a v8 group is a set of per-column chunks rather than one interleaved
 * frame; see crawl_bin_format.h for the on-disk layout and why.
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_BIN_BLOCK_H
#define CRAWL_BIN_BLOCK_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "crawl_bin_chunks.h" /* crawl_bin_chunk_stdio_t */
#include "crawl_bin_format.h"

/* zstd compression level for column chunks (override with ECRAWL_ZSTD_LEVEL). */
#define CRAWL_BIN_ZSTD_DEFAULT_LEVEL 3

/* ----------------------------------------------------------------------------
 * Writer side: accumulate records column-wise, flush them as a row group.
 * -------------------------------------------------------------------------- */
typedef struct {
    /* One uint64 array per numeric column, indexed by CRAWL_COL_*. The
     * CRAWL_COL_NAME_BYTES slot is unused here; names go to the byte buffer. */
    uint64_t *col[CRAWL_COL__COUNT];
    size_t count;    /* records buffered */
    size_t cap;      /* allocated entries per column array */
    unsigned char *names; /* concatenated name bytes for the pending group */
    size_t names_len;
    size_t names_cap;
    uint64_t raw_bytes; /* running total of decoded bytes, for the flush threshold */
    uint16_t type_mask;
    int level;
    unsigned char *scratch; /* codec output before compression */
    size_t scratch_cap;
    unsigned char *comp; /* compressed column payload */
    size_t comp_cap;
    void *cctx;          /* reusable ZSTD_CCtx, kept opaque in the public header */
} crawl_bin_block_writer_t;

typedef size_t (*crawl_bin_block_fwrite_fn)(const void *ptr, size_t size, size_t nmemb, FILE *stream);

int crawl_bin_block_writer_init(crawl_bin_block_writer_t *w);
void crawl_bin_block_writer_free(crawl_bin_block_writer_t *w);

/* Append one complete record to the pending row group. */
int crawl_bin_block_writer_append_record(crawl_bin_block_writer_t *w, const bin_record_hdr_t *hdr,
                                         const void *name);

/* Decoded bytes currently buffered (un-flushed) in the pending row group. */
static inline size_t crawl_bin_block_writer_pending(const crawl_bin_block_writer_t *w) {
    return (size_t)w->raw_bytes;
}

/* Records currently buffered; the writer must flush before exceeding
 * CRAWL_BIN_ROWGROUP_MAX_RECORDS. */
static inline size_t crawl_bin_block_writer_records(const crawl_bin_block_writer_t *w) { return w->count; }

/*
 * Encode, compress and write the pending row group via wfwrite, then reset. On
 * success sets *bytes_written_out to the on-disk size of the group, or 0 when
 * nothing was pending. Returns 0 on success, -1 on error.
 */
int crawl_bin_block_writer_flush(crawl_bin_block_writer_t *w, FILE *fp, crawl_bin_block_fwrite_fn wfwrite,
                                 uint64_t *bytes_written_out);

/* ----------------------------------------------------------------------------
 * Reader side: iterate records across the row groups in a byte range.
 * -------------------------------------------------------------------------- */

/*
 * Which columns a scan actually needs. A reader that only answers --size-gt
 * --type reads two column payloads and seeks past the other twelve, which is
 * the main reason the record region is columnar at all.
 *
 * Build one with crawl_bin_projection_all() (everything, what the
 * row-reconstruction shim uses) or by OR-ing CRAWL_COL_BIT() terms.
 */
#define CRAWL_COL_BIT(col) (1U << (col))
#define CRAWL_PROJECTION_ALL ((1U << CRAWL_COL__COUNT) - 1U)

/* Simultaneous zone-map range constraints a reader will track. */
#define CRAWL_BIN_MAX_RANGE_FILTERS 4

typedef struct {
    crawl_bin_chunk_stdio_t io;
    FILE *fp;
    uint64_t pos; /* file offset of the next row group to read */
    uint64_t end; /* exclusive end file offset of this range */

    uint32_t projection; /* bitmask of CRAWL_COL_BIT(); columns to decode */
    /* Subset of projection needed only when a row group contains a hardlink; see
     * crawl_bin_block_reader_set_hardlink_columns. */
    uint32_t hardlink_only;
    /* Columns actually decoded for the current row group: projection minus any hardlink_only
     * columns this group did not need. Read it, not projection, when consuming decoded data. */
    uint32_t rg_projection;

    /* Decoded columns for the current row group. Only those in the projection
     * are allocated and populated, so a two-column scan never pays for the
     * other twelve arrays. */
    uint64_t *col[CRAWL_COL__COUNT];
    size_t col_cap[CRAWL_COL__COUNT];
    unsigned char *names;
    size_t names_len;
    size_t names_cap;
    uint64_t *name_off; /* prefix sums of NAME_LEN, name_off[i] starts record i */
    size_t name_off_cap;

    uint32_t rg_records; /* records in the current row group */
    uint32_t rg_cursor;  /* next record index within the current row group */

    unsigned char *comp;
    size_t comp_cap;
    unsigned char *raw;
    size_t raw_cap;

    /* Optional row-group-skipping predicate; see crawl_bin_block_reader_set_filter. */
    struct {
        uint8_t column_id;
        uint64_t lo;
        uint64_t hi;
    } ranges[CRAWL_BIN_MAX_RANGE_FILTERS];
    unsigned range_count;
    uint16_t type_bit;

    uint64_t blocks_decompressed; /* row groups decoded */
    uint64_t blocks_skipped;      /* row groups proven unable to match */
    uint64_t records_skipped;     /* records inside skipped row groups */
    uint64_t column_bytes_read;   /* compressed payload bytes actually read */
    uint64_t column_bytes_skipped;/* compressed payload bytes seeked past */

    void *dctx; /* reusable ZSTD_DCtx, kept opaque in the public header */
} crawl_bin_block_reader_t;

/*
 * Initialise a reader over [start_off, end_off) of an already-open shard fp.
 * Both offsets must be row-group boundaries (catalog_offset or chunk bounds).
 * io provides the fread used to pull bytes (so callers can keep I/O
 * accounting). Starts with the full projection. Returns 0 on success.
 */
int crawl_bin_block_reader_init(crawl_bin_block_reader_t *r, const crawl_bin_chunk_stdio_t *io, FILE *fp,
                                uint64_t start_off, uint64_t end_off);

/* Reuse an initialized reader's ZSTD context and buffers for another range.
 * Resets the projection to all columns and clears any filter. */
int crawl_bin_block_reader_reinit(crawl_bin_block_reader_t *r, const crawl_bin_chunk_stdio_t *io, FILE *fp,
                                  uint64_t start_off, uint64_t end_off);

/*
 * Restrict decoding to the given columns. CRAWL_COL_NAME_BYTES implies
 * CRAWL_COL_NAME_LEN, since names cannot be split without their lengths.
 * Must be re-applied after each reinit. Returns 0 on success.
 */
int crawl_bin_block_reader_set_projection(crawl_bin_block_reader_t *r, uint32_t projection);

/*
 * Require column_id to fall in [lo, hi] for a record to be interesting. A row
 * group whose zone map for that column lies entirely outside the range cannot
 * contain a match and is seeked past without decoding anything.
 *
 * This only ever skips groups, so results are identical to an unfiltered scan;
 * a predicate the caller cannot express as a range simply prunes nothing. Must
 * be re-armed after each reinit. Returns -1 if the column is invalid or the
 * range table is full.
 */
int crawl_bin_block_reader_add_range(crawl_bin_block_reader_t *r, int column_id, uint64_t lo, uint64_t hi);

/*
 * Convenience wrapper for the two predicates ecrawl_query exposes: size_gt is
 * the strict lower bound from --size-gt (a range on the size column) and
 * type_filter is a record type code, which uses the row group's type_mask -- an
 * exact set membership test, so strictly sharper than a range on the type
 * column. Returns -1 if neither term is usable, leaving the reader unfiltered.
 */
int crawl_bin_block_reader_set_filter(crawl_bin_block_reader_t *r, int have_size_gt, uint64_t size_gt,
                                      int type_filter);

/*
 * Mark projected columns as needed only by hardlink handling. A row group whose NLINK zone map has
 * max_value <= 1 holds no hardlink, so INODE and DEV_MAJOR/DEV_MINOR exist there only to be ignored;
 * on a tree with no hardlinks that is three of the widest columns decompressed for nothing.
 *
 * Such a column reads back as absent for those groups: NULL from _column(), 0 in the _next() row,
 * and cleared from rg_projection. Only pass columns whose value the caller ignores unless nlink > 1.
 * NLINK itself must stay projected, since it is what proves a group can be pruned; if the group has
 * no NLINK chunk the columns are decoded as usual. Must be re-applied after each reinit.
 */
int crawl_bin_block_reader_set_hardlink_columns(crawl_bin_block_reader_t *r, uint32_t mask);

/*
 * Yield the next record, reconstructing a row from the decoded columns. Fields
 * outside the projection are zero. *name points at the name bytes inside the
 * reader's buffer (valid only until the next call) and is NULL when
 * CRAWL_COL_NAME_BYTES is not projected. Returns 1 for a record, 0 at end of
 * range, -1 on error.
 */
int crawl_bin_block_reader_next(crawl_bin_block_reader_t *r, bin_record_hdr_t *hdr, const unsigned char **name);

/*
 * Column-at-a-time access, for scans that want to run a predicate over a whole
 * row group without materializing rows. Loads the next row group and returns 1
 * with *records_out set, 0 at end of range, -1 on error. After it returns 1,
 * crawl_bin_block_reader_column() hands back the decoded array for any
 * projected column, and crawl_bin_block_reader_name() the bytes for one record.
 */
int crawl_bin_block_reader_next_group(crawl_bin_block_reader_t *r, uint32_t *records_out);

static inline const uint64_t *crawl_bin_block_reader_column(const crawl_bin_block_reader_t *r, int column_id) {
    if (column_id < 0 || column_id >= CRAWL_COL__COUNT) return NULL;
    /* rg_projection, not projection: a hardlink-only column is absent from groups that had no
     * hardlink, and its array still holds whatever the last group that did need it decoded. */
    if (!(r->rg_projection & CRAWL_COL_BIT(column_id))) return NULL;
    return r->col[column_id];
}

/* Name bytes for record index i of the current row group. Returns NULL when
 * names are not projected or i is out of range; *len_out gets the length. */
const unsigned char *crawl_bin_block_reader_name(const crawl_bin_block_reader_t *r, uint32_t i, size_t *len_out);

void crawl_bin_block_reader_free(crawl_bin_block_reader_t *r);

#endif /* CRAWL_BIN_BLOCK_H */
