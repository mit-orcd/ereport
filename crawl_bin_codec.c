/*
 * crawl_bin_codec — per-column encodings for the v8 columnar record region.
 *
 * SPDX-License-Identifier: MIT
 */
#include "crawl_bin_codec.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

static int codec_grow(unsigned char **buf, size_t *cap, size_t need) {
    size_t nc;
    unsigned char *p;

    if (*cap >= need) return 0;
    nc = *cap ? *cap : 4096;
    while (nc < need) {
        if (nc > (SIZE_MAX / 2)) {
            nc = need;
            break;
        }
        nc *= 2;
    }
    p = (unsigned char *)realloc(*buf, nc);
    if (!p) return -1;
    *buf = p;
    *cap = nc;
    return 0;
}

unsigned crawl_bin_codec_bit_width(uint64_t span) {
    unsigned w = 0;

    if (span == 0ULL) return 1U;
    while (span) {
        w++;
        span >>= 1;
    }
    return w;
}

static void put_u64le(unsigned char *p, uint64_t v) {
    p[0] = (unsigned char)(v);
    p[1] = (unsigned char)(v >> 8);
    p[2] = (unsigned char)(v >> 16);
    p[3] = (unsigned char)(v >> 24);
    p[4] = (unsigned char)(v >> 32);
    p[5] = (unsigned char)(v >> 40);
    p[6] = (unsigned char)(v >> 48);
    p[7] = (unsigned char)(v >> 56);
}

static uint64_t get_u64le(const unsigned char *p) {
    return (uint64_t)p[0] | ((uint64_t)p[1] << 8) | ((uint64_t)p[2] << 16) | ((uint64_t)p[3] << 24) |
           ((uint64_t)p[4] << 32) | ((uint64_t)p[5] << 40) | ((uint64_t)p[6] << 48) | ((uint64_t)p[7] << 56);
}

/* Little-endian bit packing: value i occupies bits [i*w, i*w + w) of the stream. */
static void bitpack(const uint64_t *values, size_t count, uint64_t base, unsigned w, unsigned char *out,
                    size_t out_len) {
    size_t i;

    memset(out, 0, out_len);
    for (i = 0; i < count; i++) {
        uint64_t v = values[i] - base;
        size_t bit = i * (size_t)w;
        size_t byte = bit >> 3;
        unsigned off = (unsigned)(bit & 7U);
        unsigned written = 0;

        while (written < w) {
            unsigned room = 8U - off;
            unsigned take = w - written;

            if (take > room) take = room;
            out[byte] |= (unsigned char)(((v >> written) & ((take == 64U) ? ~0ULL : ((1ULL << take) - 1ULL)))
                                         << off);
            written += take;
            byte++;
            off = 0;
        }
    }
}

/* Hot path for CRAWL_ENC_FOR_BITPACK: width-specialized loops beat the generic
 * bit walker when a whole column uses a common width (uid/gid/mode/timestamps). */
static void bitunpack(const unsigned char *src, size_t count, uint64_t base, unsigned w, uint64_t *dst) {
    size_t i;

    if (w == 8U) {
        for (i = 0; i < count; i++) dst[i] = base + (uint64_t)src[i];
        return;
    }
    if (w == 16U) {
        for (i = 0; i < count; i++) {
            size_t o = i * 2U;
            dst[i] = base + ((uint64_t)src[o] | ((uint64_t)src[o + 1U] << 8));
        }
        return;
    }
    if (w == 32U) {
        for (i = 0; i < count; i++) {
            size_t o = i * 4U;
            dst[i] = base + ((uint64_t)src[o] | ((uint64_t)src[o + 1U] << 8) | ((uint64_t)src[o + 2U] << 16) |
                             ((uint64_t)src[o + 3U] << 24));
        }
        return;
    }
    if (w == 64U) {
        for (i = 0; i < count; i++) dst[i] = base + get_u64le(src + i * 8U);
        return;
    }
    if (w == 1U) {
        for (i = 0; i < count; i++) {
            unsigned char byte = src[i >> 3];
            dst[i] = base + (uint64_t)((byte >> (i & 7U)) & 1U);
        }
        return;
    }
    if (w > 0U && w < 8U && (8U % w) == 0U) {
        /* Pack several values into each source byte (2, 4). */
        unsigned per_byte = 8U / w;
        unsigned mask = (1U << w) - 1U;

        for (i = 0; i < count; i++) {
            unsigned char byte = src[i / per_byte];
            unsigned slot = (unsigned)(i % per_byte);
            dst[i] = base + (uint64_t)((byte >> (slot * w)) & mask);
        }
        return;
    }

    for (i = 0; i < count; i++) {
        size_t bit = i * (size_t)w;
        size_t byte = bit >> 3;
        unsigned off = (unsigned)(bit & 7U);
        unsigned got = 0;
        uint64_t v = 0;

        while (got < w) {
            unsigned room = 8U - off;
            unsigned take = w - got;

            if (take > room) take = room;
            v |= ((uint64_t)((src[byte] >> off) & (unsigned char)((1U << take) - 1U))) << got;
            got += take;
            byte++;
            off = 0;
        }
        dst[i] = base + v;
    }
}

static size_t bitpack_bytes(size_t count, unsigned w) { return (count * (size_t)w + 7U) / 8U; }

/* Count runs of equal adjacent values, capped so a high-cardinality column stops
 * counting as soon as RLE is provably worse than the alternatives. */
static size_t count_runs(const uint64_t *values, size_t count, size_t cap) {
    size_t runs = 1;
    size_t i;

    if (count == 0) return 0;
    for (i = 1; i < count; i++) {
        if (values[i] != values[i - 1]) {
            runs++;
            if (runs >= cap) return cap;
        }
    }
    return runs;
}

static uint64_t zigzag_encode(uint64_t residual) {
    int64_t s = (int64_t)residual;

    return ((uint64_t)s << 1) ^ (s < 0 ? ~0ULL : 0ULL);
}

static uint64_t zigzag_decode(uint64_t z) { return (z >> 1) ^ ((z & 1ULL) ? ~0ULL : 0ULL); }

/*
 * Residual staging, thread-local rather than per-writer: a writer thread encodes
 * one column at a time, so this is one buffer per thread instead of one per live
 * shard, which at the default ECRAWL_UID_SHARDS of 1024 is the difference
 * between half a megabyte and half a gigabyte of buffers that are live for the
 * microseconds an encode takes.
 */
static __thread uint64_t *tls_residual;
static __thread size_t tls_residual_cap;

void crawl_bin_codec_tls_release(void) {
    free(tls_residual);
    tls_residual = NULL;
    tls_residual_cap = 0;
}

static int residual_reserve(size_t n) {
    uint64_t *p;

    if (tls_residual_cap >= n) return 0;
    p = (uint64_t *)realloc(tls_residual, n * sizeof(*p));
    if (!p) return -1;
    tls_residual = p;
    tls_residual_cap = n;
    return 0;
}

int crawl_bin_codec_encode_u64(const uint64_t *values, size_t count, unsigned char **out, size_t *out_cap,
                               size_t *len_out, uint8_t *enc_out, uint8_t *bit_width_out, uint64_t *min_out,
                               uint64_t *max_out) {
    uint64_t mn, mx;
    size_t i;
    unsigned w;
    size_t raw_bytes, for_bytes, rle_bytes, runs, rle_cap;

    if (!values || !out || !out_cap || !len_out || !enc_out || !bit_width_out || !min_out || !max_out) {
        errno = EINVAL;
        return -1;
    }
    *bit_width_out = 0;
    if (count == 0) {
        *len_out = 0;
        *enc_out = (uint8_t)CRAWL_ENC_CONST;
        *min_out = 0;
        *max_out = 0;
        return 0;
    }

    mn = mx = values[0];
    for (i = 1; i < count; i++) {
        if (values[i] < mn) mn = values[i];
        if (values[i] > mx) mx = values[i];
    }
    *min_out = mn;
    *max_out = mx;

    /* A constant column needs no payload at all: min_value in the header is the
     * whole story. This is the common case for uid inside a uid shard, and for
     * dev_major/dev_minor on a single-filesystem crawl. */
    if (mn == mx) {
        *len_out = 0;
        *enc_out = (uint8_t)CRAWL_ENC_CONST;
        return 0;
    }

    raw_bytes = count * sizeof(uint64_t);
    w = crawl_bin_codec_bit_width(mx - mn);
    for_bytes = bitpack_bytes(count, w);

    /* RLE stores 16 bytes per run, so it only wins below raw/16 runs; stop
     * counting there rather than walking a high-cardinality column to the end. */
    rle_cap = (raw_bytes / 16U) + 1U;
    runs = count_runs(values, count, rle_cap);
    rle_bytes = runs * 2U * sizeof(uint64_t);

    if (rle_bytes < for_bytes && rle_bytes < raw_bytes) {
        unsigned char *p;
        size_t pos = 0;

        if (codec_grow(out, out_cap, rle_bytes) != 0) return -1;
        p = *out;
        i = 0;
        while (i < count) {
            size_t j = i + 1;

            while (j < count && values[j] == values[i]) j++;
            put_u64le(p + pos, values[i]);
            put_u64le(p + pos + 8, (uint64_t)(j - i));
            pos += 16;
            i = j;
        }
        *len_out = pos;
        *enc_out = (uint8_t)CRAWL_ENC_RLE;
        return 0;
    }

    if (for_bytes < raw_bytes) {
        if (codec_grow(out, out_cap, for_bytes ? for_bytes : 1U) != 0) return -1;
        bitpack(values, count, mn, w, *out, for_bytes);
        *len_out = for_bytes;
        *enc_out = (uint8_t)CRAWL_ENC_FOR_BITPACK;
        *bit_width_out = (uint8_t)w;
        return 0;
    }

    if (codec_grow(out, out_cap, raw_bytes) != 0) return -1;
    for (i = 0; i < count; i++) put_u64le(*out + i * 8U, values[i]);
    *len_out = raw_bytes;
    *enc_out = (uint8_t)CRAWL_ENC_RAW;
    return 0;
}

/*
 * Serialize a residual stream: the chooser above picks how to store it, then the
 * payload prefix is inserted in front of what it wrote. crawl_bin_codec_encode_u64
 * never returns a residual encoding, so the nesting is one level deep by
 * construction rather than by a depth check.
 */
static int encode_residual(const uint64_t *residual, size_t n, uint64_t seed, unsigned char **out, size_t *out_cap,
                           size_t *len_out, uint8_t *bit_width_out) {
    size_t sub_len = 0;
    uint8_t sub_enc = 0, sub_bw = 0;
    uint64_t sub_mn = 0, sub_mx = 0;

    if (crawl_bin_codec_encode_u64(residual, n, out, out_cap, &sub_len, &sub_enc, &sub_bw, &sub_mn, &sub_mx) != 0)
        return -1;
    if (codec_grow(out, out_cap, CRAWL_ENC_RESIDUAL_PREFIX_BYTES + sub_len) != 0) return -1;
    memmove(*out + CRAWL_ENC_RESIDUAL_PREFIX_BYTES, *out, sub_len);
    memset(*out, 0, CRAWL_ENC_RESIDUAL_PREFIX_BYTES);
    (*out)[0] = sub_enc;
    put_u64le(*out + 8, seed);
    put_u64le(*out + 16, sub_mn);
    *len_out = CRAWL_ENC_RESIDUAL_PREFIX_BYTES + sub_len;
    *bit_width_out = sub_bw;
    return 1;
}

int crawl_bin_codec_encode_delta_u64(const uint64_t *values, size_t count, unsigned char **out, size_t *out_cap,
                                     size_t *len_out, uint8_t *bit_width_out) {
    size_t i;

    if (!values || !out || !out_cap || !len_out || !bit_width_out) {
        errno = EINVAL;
        return -1;
    }
    if (count < 2) return 0;
    if (residual_reserve(count - 1) != 0) return -1;
    for (i = 1; i < count; i++) tls_residual[i - 1] = zigzag_encode(values[i] - values[i - 1]);
    return encode_residual(tls_residual, count - 1, values[0], out, out_cap, len_out, bit_width_out);
}

int crawl_bin_codec_encode_ref_u64(const uint64_t *values, const uint64_t *ref, size_t count, unsigned char **out,
                                   size_t *out_cap, size_t *len_out, uint8_t *bit_width_out) {
    size_t i;

    if (!values || !ref || !out || !out_cap || !len_out || !bit_width_out) {
        errno = EINVAL;
        return -1;
    }
    if (count == 0) return 0;
    if (residual_reserve(count) != 0) return -1;
    for (i = 0; i < count; i++) tls_residual[i] = zigzag_encode(values[i] - ref[i]);
    return encode_residual(tls_residual, count, 0, out, out_cap, len_out, bit_width_out);
}

/* Split a residual payload into its prefix fields and its sub payload. */
static int residual_split(const unsigned char *src, size_t src_len, uint8_t *sub_enc_out, uint64_t *seed_out,
                          uint64_t *sub_min_out, const unsigned char **sub_out, size_t *sub_len_out) {
    if (src_len < CRAWL_ENC_RESIDUAL_PREFIX_BYTES) return -1;
    *sub_enc_out = src[0];
    /* Only the four numeric encodings may nest, which is what bounds the decode
     * recursion at one level; anything else is a damaged or hostile payload. */
    if (*sub_enc_out > (uint8_t)CRAWL_ENC_CONST) {
        errno = EINVAL;
        return -1;
    }
    *seed_out = get_u64le(src + 8);
    *sub_min_out = get_u64le(src + 16);
    *sub_out = src + CRAWL_ENC_RESIDUAL_PREFIX_BYTES;
    *sub_len_out = src_len - CRAWL_ENC_RESIDUAL_PREFIX_BYTES;
    return 0;
}

int crawl_bin_codec_decode_u64(const unsigned char *src, size_t src_len, size_t count, uint8_t enc,
                               uint8_t bit_width, uint64_t min_value, uint64_t *dst) {
    size_t i;

    if (!dst || (src_len > 0 && !src)) {
        errno = EINVAL;
        return -1;
    }
    if (count == 0) return src_len == 0 ? 0 : -1;

    switch (enc) {
        case CRAWL_ENC_CONST:
            if (src_len != 0) return -1;
            if (min_value == 0ULL) {
                memset(dst, 0, count * sizeof(*dst));
            } else {
                /* Tight fill; compilers vectorize this more readily than a
                 * growing RLE-style store of the same constant. */
                uint64_t *p = dst;
                uint64_t *end = dst + count;
                while (p < end) *p++ = min_value;
            }
            return 0;

        case CRAWL_ENC_RAW:
            if (src_len != count * sizeof(uint64_t)) return -1;
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            /* On-disk layout is little-endian; host LE can memcpy the column. */
            memcpy(dst, src, count * sizeof(uint64_t));
#else
            for (i = 0; i < count; i++) dst[i] = get_u64le(src + i * 8U);
#endif
            return 0;

        case CRAWL_ENC_FOR_BITPACK:
            if (bit_width == 0 || bit_width > 64) return -1;
            if (src_len != bitpack_bytes(count, bit_width)) return -1;
            bitunpack(src, count, min_value, bit_width, dst);
            return 0;

        case CRAWL_ENC_RLE: {
            size_t pos = 0;
            size_t written = 0;

            if (src_len % 16U != 0U) return -1;
            while (pos < src_len) {
                uint64_t v = get_u64le(src + pos);
                uint64_t run = get_u64le(src + pos + 8);

                if (run == 0ULL || run > (uint64_t)(count - written)) return -1;
                for (i = 0; i < (size_t)run; i++) dst[written + i] = v;
                written += (size_t)run;
                pos += 16;
            }
            /* A run table that does not cover exactly count values means the
             * chunk header and payload disagree; refuse rather than return a
             * partly-filled column. */
            return written == count ? 0 : -1;
        }

        case CRAWL_ENC_DELTA: {
            uint8_t sub_enc;
            uint64_t seed, sub_min;
            const unsigned char *sub;
            size_t sub_len;

            if (residual_split(src, src_len, &sub_enc, &seed, &sub_min, &sub, &sub_len) != 0) return -1;
            dst[0] = seed;
            if (count == 1) return sub_len == 0 ? 0 : -1;
            if (crawl_bin_codec_decode_u64(sub, sub_len, count - 1, sub_enc, bit_width, sub_min, dst + 1) != 0)
                return -1;
            for (i = 1; i < count; i++) dst[i] = dst[i - 1] + zigzag_decode(dst[i]);
            return 0;
        }

        default:
            errno = EINVAL;
            return -1;
    }
}

int crawl_bin_codec_decode_ref_u64(const unsigned char *src, size_t src_len, size_t count, uint8_t enc,
                                   uint8_t bit_width, const uint64_t *ref, uint64_t *dst) {
    uint8_t sub_enc;
    uint64_t seed, sub_min;
    const unsigned char *sub;
    size_t sub_len, i;

    if (!dst || !ref || (src_len > 0 && !src) || enc != (uint8_t)CRAWL_ENC_REF_MTIME) {
        errno = EINVAL;
        return -1;
    }
    if (count == 0) return src_len == 0 ? 0 : -1;
    if (residual_split(src, src_len, &sub_enc, &seed, &sub_min, &sub, &sub_len) != 0) return -1;
    if (crawl_bin_codec_decode_u64(sub, sub_len, count, sub_enc, bit_width, sub_min, dst) != 0) return -1;
    for (i = 0; i < count; i++) dst[i] = ref[i] + zigzag_decode(dst[i]);
    return 0;
}
