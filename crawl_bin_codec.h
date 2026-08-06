/*
 * crawl_bin_codec — per-column encodings for the v8 columnar record region.
 *
 * Every numeric column is handled as a uint64 array regardless of its on-disk
 * width; the encoder narrows it. That keeps one code path for fourteen columns
 * at the cost of a transient uint64 buffer in the writer, which is bounded by
 * CRAWL_BIN_ROWGROUP_MAX_RECORDS.
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_BIN_CODEC_H
#define CRAWL_BIN_CODEC_H

#include <stddef.h>
#include <stdint.h>

#include "crawl_bin_format.h"

/*
 * Choose an encoding for one column and serialize it into *out (grown as
 * needed). Returns 0 on success. On success sets:
 *   enc_out        one of CRAWL_ENC_*
 *   bit_width_out  bits per value for CRAWL_ENC_FOR_BITPACK, else 0
 *   min_out, max_out   the zone map for this chunk
 *   len_out        encoded bytes written to *out
 *
 * The chosen encoding is whichever of CONST / RLE / FOR_BITPACK / RAW produces
 * the fewest pre-compression bytes, so a near-constant column (uid inside a uid
 * shard, dev, mode) collapses to a few pairs or vanishes entirely, and a narrow
 * range (timestamps inside one row group) costs a couple of bits per value
 * instead of eight bytes.
 */
int crawl_bin_codec_encode_u64(const uint64_t *values, size_t count, unsigned char **out, size_t *out_cap,
                               size_t *len_out, uint8_t *enc_out, uint8_t *bit_width_out, uint64_t *min_out,
                               uint64_t *max_out);

/*
 * Serialize the same column as CRAWL_ENC_DELTA -- zigzagged first differences,
 * re-encoded by the chooser above -- or as CRAWL_ENC_REF_MTIME, the same thing
 * against a per-record reference column. Returns 1 when a candidate was written
 * to *out, 0 when the column is too short for one, -1 on error. Neither sets a
 * zone map: min/max stay whatever crawl_bin_codec_encode_u64 reported for the
 * absolute values, because that is what a skipping reader tests.
 *
 * These exist as separate calls rather than as further arms of the chooser
 * because they win on post-zstd bytes while being *larger* pre-zstd, so the
 * caller has to compress both candidates and keep the smaller; see
 * CRAWL_BIN_ENC_TRIAL_MIN_BYTES.
 */
int crawl_bin_codec_encode_delta_u64(const uint64_t *values, size_t count, unsigned char **out, size_t *out_cap,
                                     size_t *len_out, uint8_t *bit_width_out);
int crawl_bin_codec_encode_ref_u64(const uint64_t *values, const uint64_t *ref, size_t count, unsigned char **out,
                                   size_t *out_cap, size_t *len_out, uint8_t *bit_width_out);

/*
 * Decode a column chunk payload back into a uint64 array of exactly count
 * values. dst must have room for count entries. Returns 0 on success, -1 if the
 * payload is inconsistent with the header (truncated, wrong length, bad width),
 * or if enc is not an encoding this call can decode -- CRAWL_ENC_REF_MTIME needs
 * its reference column and so goes through crawl_bin_codec_decode_ref_u64.
 */
int crawl_bin_codec_decode_u64(const unsigned char *src, size_t src_len, size_t count, uint8_t enc,
                               uint8_t bit_width, uint64_t min_value, uint64_t *dst);

/* Decode a CRAWL_ENC_REF_MTIME chunk against the reference column it was encoded
 * against; ref must hold count already-decoded values. */
int crawl_bin_codec_decode_ref_u64(const unsigned char *src, size_t src_len, size_t count, uint8_t enc,
                                   uint8_t bit_width, const uint64_t *ref, uint64_t *dst);

/*
 * Release the calling thread's residual staging buffer. Shared by every column
 * the thread encodes, so it outlives any single writer; call it once as a writer
 * thread winds down. crawl_bin_block_writer_tls_release() already does.
 */
void crawl_bin_codec_tls_release(void);

/* Bits needed to represent span (the largest delta from the frame-of-reference
 * base). Returns 1 for span == 0 so a bit width is never zero. */
unsigned crawl_bin_codec_bit_width(uint64_t span);

#endif /* CRAWL_BIN_CODEC_H */
