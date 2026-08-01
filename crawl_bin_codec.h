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
 * Decode a column chunk payload back into a uint64 array of exactly count
 * values. dst must have room for count entries. Returns 0 on success, -1 if the
 * payload is inconsistent with the header (truncated, wrong length, bad width).
 */
int crawl_bin_codec_decode_u64(const unsigned char *src, size_t src_len, size_t count, uint8_t enc,
                               uint8_t bit_width, uint64_t min_value, uint64_t *dst);

/* Bits needed to represent span (the largest delta from the frame-of-reference
 * base). Returns 1 for span == 0 so a bit width is never zero. */
unsigned crawl_bin_codec_bit_width(uint64_t span);

#endif /* CRAWL_BIN_CODEC_H */
