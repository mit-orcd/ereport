/*
 * Round-trip tests for the v8 column codecs.
 *
 * Encoding is chosen by the writer from the data, so these check two separate
 * things: that every encoding decodes back to exactly what went in, and that
 * the chooser actually picks the encoding each data shape is meant to hit.
 * A codec that silently falls back to RAW would still be correct and would
 * still pass a pure round-trip test, while giving up the whole point of the
 * columnar layout.
 *
 * SPDX-License-Identifier: MIT
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "crawl_bin_codec.h"

static int g_fail = 0;
static int g_checks = 0;

static const char *enc_name(uint8_t e) {
    switch (e) {
        case CRAWL_ENC_RAW: return "RAW";
        case CRAWL_ENC_FOR_BITPACK: return "FOR_BITPACK";
        case CRAWL_ENC_RLE: return "RLE";
        case CRAWL_ENC_CONST: return "CONST";
        case CRAWL_ENC_BYTES: return "BYTES";
        case CRAWL_ENC_DELTA: return "DELTA";
        case CRAWL_ENC_REF_MTIME: return "REF_MTIME";
        default: return "?";
    }
}

/* Encode, decode, compare. want_enc < 0 accepts any encoding. */
static void roundtrip(const char *label, const uint64_t *vals, size_t n, int want_enc) {
    unsigned char *buf = NULL;
    size_t cap = 0, len = 0;
    uint8_t enc = 0, bw = 0;
    uint64_t mn = 0, mx = 0;
    uint64_t *back;
    size_t i;
    uint64_t emn, emx;

    g_checks++;
    if (crawl_bin_codec_encode_u64(vals, n, &buf, &cap, &len, &enc, &bw, &mn, &mx) != 0) {
        printf("FAIL %s: encode failed\n", label);
        g_fail++;
        free(buf);
        return;
    }

    if (want_enc >= 0 && enc != (uint8_t)want_enc) {
        printf("FAIL %s: chose %s, expected %s\n", label, enc_name(enc), enc_name((uint8_t)want_enc));
        g_fail++;
        free(buf);
        return;
    }

    back = (uint64_t *)malloc((n ? n : 1) * sizeof(uint64_t));
    if (!back) {
        printf("FAIL %s: oom\n", label);
        g_fail++;
        free(buf);
        return;
    }
    memset(back, 0xAA, (n ? n : 1) * sizeof(uint64_t));

    if (crawl_bin_codec_decode_u64(buf, len, n, enc, bw, mn, back) != 0) {
        printf("FAIL %s: decode failed (enc=%s len=%zu bw=%u)\n", label, enc_name(enc), len, (unsigned)bw);
        g_fail++;
        free(buf);
        free(back);
        return;
    }

    for (i = 0; i < n; i++) {
        if (back[i] != vals[i]) {
            printf("FAIL %s: value %zu: got %llu want %llu (enc=%s bw=%u)\n", label, i,
                   (unsigned long long)back[i], (unsigned long long)vals[i], enc_name(enc), (unsigned)bw);
            g_fail++;
            free(buf);
            free(back);
            return;
        }
    }

    /* The zone map is what lets a reader skip a whole row group, so an
     * understated range is a silent wrong-answer bug, not a size regression. */
    if (n > 0) {
        emn = emx = vals[0];
        for (i = 1; i < n; i++) {
            if (vals[i] < emn) emn = vals[i];
            if (vals[i] > emx) emx = vals[i];
        }
        if (mn != emn || mx != emx) {
            printf("FAIL %s: zone map [%llu,%llu] but data is [%llu,%llu]\n", label,
                   (unsigned long long)mn, (unsigned long long)mx, (unsigned long long)emn,
                   (unsigned long long)emx);
            g_fail++;
            free(buf);
            free(back);
            return;
        }
    }

    printf("ok   %-42s n=%-6zu enc=%-11s bw=%-2u bytes=%zu\n", label, n, enc_name(enc), (unsigned)bw, len);
    free(buf);
    free(back);
}

/*
 * Round-trip a residual candidate. ref == NULL asks for CRAWL_ENC_DELTA, a
 * non-NULL ref for CRAWL_ENC_REF_MTIME. want_bytes >= 0 asserts the payload
 * size, which is how "a constant stride costs 24 bytes" gets checked rather than
 * assumed. The DELTA decode is deliberately handed min_value 0: the frame base
 * lives in the payload, because the header's is the absolute zone map.
 */
static void roundtrip_residual(const char *label, const uint64_t *vals, const uint64_t *ref, size_t n,
                               int want_staged, long want_bytes) {
    unsigned char *buf = NULL;
    size_t cap = 0, len = 0;
    uint8_t bw = 0;
    uint8_t enc = ref ? (uint8_t)CRAWL_ENC_REF_MTIME : (uint8_t)CRAWL_ENC_DELTA;
    uint64_t *back = NULL;
    size_t i;
    int staged;

    g_checks++;
    staged = ref ? crawl_bin_codec_encode_ref_u64(vals, ref, n, &buf, &cap, &len, &bw)
                 : crawl_bin_codec_encode_delta_u64(vals, n, &buf, &cap, &len, &bw);
    if (staged != want_staged) {
        printf("FAIL %s: staged %d, expected %d\n", label, staged, want_staged);
        g_fail++;
        goto out;
    }
    if (staged != 1) {
        printf("ok   %-42s n=%-6zu no candidate\n", label, n);
        goto out;
    }
    if (want_bytes >= 0 && len != (size_t)want_bytes) {
        printf("FAIL %s: %zu payload bytes, expected %ld\n", label, len, want_bytes);
        g_fail++;
        goto out;
    }

    back = (uint64_t *)malloc((n ? n : 1) * sizeof(uint64_t));
    if (!back) {
        printf("FAIL %s: oom\n", label);
        g_fail++;
        goto out;
    }
    memset(back, 0xAA, (n ? n : 1) * sizeof(uint64_t));

    if (ref ? crawl_bin_codec_decode_ref_u64(buf, len, n, enc, bw, ref, back) != 0
            : crawl_bin_codec_decode_u64(buf, len, n, enc, bw, 0, back) != 0) {
        printf("FAIL %s: decode failed (enc=%s len=%zu bw=%u)\n", label, enc_name(enc), len, (unsigned)bw);
        g_fail++;
        goto out;
    }
    for (i = 0; i < n; i++) {
        if (back[i] != vals[i]) {
            printf("FAIL %s: value %zu: got %llu want %llu (enc=%s bw=%u)\n", label, i,
                   (unsigned long long)back[i], (unsigned long long)vals[i], enc_name(enc), (unsigned)bw);
            g_fail++;
            goto out;
        }
    }
    printf("ok   %-42s n=%-6zu enc=%-11s bw=%-2u bytes=%zu\n", label, n, enc_name(enc), (unsigned)bw, len);

out:
    free(buf);
    free(back);
}

int main(void) {
    uint64_t *v;
    size_t i;
    size_t n = 4096;

    v = (uint64_t *)malloc(n * sizeof(uint64_t));
    if (!v) return 1;

    /* uid inside a uid shard, dev on a single-filesystem crawl: one value. */
    for (i = 0; i < n; i++) v[i] = 1000;
    roundtrip("constant column (uid in a uid shard)", v, n, CRAWL_ENC_CONST);

    /* nlink: almost all 1, a few hardlinks. Few runs -> RLE. */
    for (i = 0; i < n; i++) v[i] = 1;
    v[100] = 2;
    v[101] = 2;
    v[2000] = 3;
    roundtrip("mostly-1 with a few runs (nlink)", v, n, CRAWL_ENC_RLE);

    /* type letters arrive clustered because a directory's children are written
     * together, so the type column is a short run table. */
    for (i = 0; i < n; i++) v[i] = (i < n / 2) ? (uint64_t)'f' : (uint64_t)'d';
    roundtrip("two clustered runs (type)", v, n, CRAWL_ENC_RLE);

    /* Timestamps inside one row group span a narrow range: frame-of-reference
     * plus bit packing is the whole reason this column is cheap. */
    for (i = 0; i < n; i++) v[i] = 1750000000ULL + (uint64_t)(i % 86400);
    roundtrip("narrow-range timestamps (mtime)", v, n, CRAWL_ENC_FOR_BITPACK);

    /* parent_dir_id: dense-ish ids in a small window. */
    for (i = 0; i < n; i++) v[i] = 500000ULL + (uint64_t)(i / 8);
    roundtrip("clustered parent_dir_id", v, n, CRAWL_ENC_FOR_BITPACK);

    /* Sizes: wide range, high cardinality. Must still be exact. */
    for (i = 0; i < n; i++) v[i] = ((uint64_t)i * 2654435761ULL) ^ ((uint64_t)i << 33);
    roundtrip("high-entropy 64-bit values (size)", v, n, -1);

    /* Full 64-bit span: bit width 64, the worst case for the packer. */
    for (i = 0; i < n; i++) v[i] = ((uint64_t)i * 0x9E3779B97F4A7C15ULL);
    v[0] = 0;
    v[1] = UINT64_MAX;
    roundtrip("full 64-bit span including UINT64_MAX", v, n, -1);

    /* inode numbers: large base, moderate spread. */
    for (i = 0; i < n; i++) v[i] = 0x7000000000ULL + (uint64_t)(i * 37);
    roundtrip("large base, moderate spread (inode)", v, n, CRAWL_ENC_FOR_BITPACK);

    /* Bit-width boundaries: a packed value that straddles byte edges is where
     * an off-by-one in the shift loop shows up. */
    {
        unsigned w;
        for (w = 1; w <= 64; w++) {
            char label[64];
            uint64_t span = (w >= 64) ? UINT64_MAX : ((1ULL << w) - 1ULL);
            size_t m = 37; /* deliberately not a multiple of 8 */

            for (i = 0; i < m; i++) v[i] = (i % 2) ? span : 0ULL;
            /* Force the exact width by pinning min=0 and max=span. */
            snprintf(label, sizeof(label), "bit width %u straddling byte edges", w);
            roundtrip(label, v, m, -1);
        }
    }

    /* Degenerate lengths. */
    v[0] = 42;
    roundtrip("single value", v, 1, CRAWL_ENC_CONST);
    v[0] = 0;
    v[1] = 1;
    roundtrip("two values", v, 2, -1);
    roundtrip("empty column", v, 0, CRAWL_ENC_CONST);

    /* ---- residual encodings ------------------------------------------------
     * These are chosen by the writer on post-zstd bytes, so the codec is only
     * asked whether the candidate round-trips; the sizes below are the ones the
     * chooser is comparing against, not a promise about the winner. */

    /* An allocator run: one stride, so the whole chunk is the 24-byte prefix. */
    for (i = 0; i < n; i++) v[i] = 0x7000000000ULL + (uint64_t)i;
    roundtrip_residual("delta: constant stride (inode run)", v, NULL, n, 1, 24);

    /* All values equal: the deltas are a constant zero, same 24 bytes. A column
     * like this never reaches the trial in practice -- CONST already won. */
    for (i = 0; i < n; i++) v[i] = 1234567ULL;
    roundtrip_residual("delta: all values equal", v, NULL, n, 1, 24);

    /* What sorting by (parent_dir_id, name) actually hands the inode column:
     * runs of +1 broken by jumps to another allocation group, both directions. */
    for (i = 0; i < n; i++) v[i] = 0x180000000ULL + (uint64_t)i;
    for (i = 0; i < n; i += 37) v[i] = 0x40000000ULL + (uint64_t)(i * 7919);
    roundtrip_residual("delta: +1 runs with allocator jumps", v, NULL, n, 1, -1);

    /* Strictly decreasing, so every residual is negative. */
    for (i = 0; i < n; i++) v[i] = UINT64_MAX - (uint64_t)(i * 3);
    roundtrip_residual("delta: strictly decreasing from UINT64_MAX", v, NULL, n, 1, -1);

    /* Maximum-magnitude residuals in both directions, and values at both ends of
     * the range: the zigzag must wrap exactly, not saturate. */
    v[0] = 0;
    v[1] = UINT64_MAX;
    v[2] = 0;
    v[3] = UINT64_MAX;
    roundtrip_residual("delta: alternating 0 and UINT64_MAX", v, NULL, 4, 1, -1);
    v[0] = 0;
    v[1] = 1ULL << 63;
    v[2] = 0;
    roundtrip_residual("delta: 2^63 residual (int64 min)", v, NULL, 3, 1, -1);
    v[0] = UINT64_MAX;
    v[1] = 1;
    roundtrip_residual("delta: two values, wrapping", v, NULL, 2, 1, -1);

    /* Below two values there is no difference to take. */
    v[0] = 42;
    roundtrip_residual("delta: single value has no candidate", v, NULL, 1, 0, -1);
    roundtrip_residual("delta: empty column has no candidate", v, NULL, 0, 0, -1);

    /* High-entropy values, where the candidate is expected to lose on size but
     * must still be exact -- the writer will compress both and drop this one. */
    for (i = 0; i < n; i++) v[i] = ((uint64_t)i * 0x9E3779B97F4A7C15ULL) ^ ((uint64_t)i << 29);
    roundtrip_residual("delta: high-entropy values", v, NULL, n, 1, -1);

    /* Timestamps against mtime. ctime == mtime is the common case on a tree
     * nobody has chmod'd, and collapses to the prefix alone. */
    {
        uint64_t *base = (uint64_t *)malloc(n * sizeof(uint64_t));

        if (!base) return 1;
        for (i = 0; i < n; i++) base[i] = 1750000000ULL + (uint64_t)(i % 977);

        for (i = 0; i < n; i++) v[i] = base[i];
        roundtrip_residual("ref: ctime equal to mtime", v, base, n, 1, 24);

        for (i = 0; i < n; i++) v[i] = base[i] + (uint64_t)(i % 7);
        roundtrip_residual("ref: atime a few seconds after mtime", v, base, n, 1, -1);

        /* atime before mtime, which is what a restored-from-backup tree looks
         * like: the residual is negative for every record. */
        for (i = 0; i < n; i++) v[i] = base[i] - 86400ULL * (uint64_t)(1 + i % 31);
        roundtrip_residual("ref: atime before mtime", v, base, n, 1, -1);

        /* Uncorrelated: the residual is as wide as the values. Still exact. */
        for (i = 0; i < n; i++) v[i] = ((uint64_t)i * 2654435761ULL) & 0xFFFFFFFFULL;
        roundtrip_residual("ref: atime unrelated to mtime", v, base, n, 1, -1);

        for (i = 0; i < n; i++) v[i] = UINT64_MAX;
        roundtrip_residual("ref: UINT64_MAX against a real mtime", v, base, n, 1, -1);

        v[0] = base[0] + 5ULL;
        roundtrip_residual("ref: single record", v, base, 1, 1, 24);
        free(base);
    }

    /* A truncated or inconsistent payload must be refused, not half-decoded:
     * these headers are attacker-adjacent in the sense that a damaged shard
     * should error rather than yield silently wrong query results. */
    {
        unsigned char *buf = NULL;
        size_t cap = 0, len = 0;
        uint8_t enc = 0, bw = 0;
        uint64_t mn = 0, mx = 0;
        uint64_t back[8];

        for (i = 0; i < 8; i++) v[i] = 1000000ULL + i;
        g_checks++;
        if (crawl_bin_codec_encode_u64(v, 8, &buf, &cap, &len, &enc, &bw, &mn, &mx) != 0) {
            printf("FAIL truncation setup\n");
            g_fail++;
        } else if (crawl_bin_codec_decode_u64(buf, len - 1, 8, enc, bw, mn, back) == 0) {
            printf("FAIL truncated payload was accepted\n");
            g_fail++;
        } else {
            printf("ok   %-42s\n", "truncated payload rejected");
        }

        g_checks++;
        if (crawl_bin_codec_decode_u64(buf, len, 8, 99, bw, mn, back) == 0) {
            printf("FAIL unknown encoding was accepted\n");
            g_fail++;
        } else {
            printf("ok   %-42s\n", "unknown encoding rejected");
        }

        /* An RLE run table that does not cover exactly count values. */
        g_checks++;
        {
            unsigned char rle[32];
            memset(rle, 0, sizeof(rle));
            rle[0] = 7;  /* value 7 */
            rle[8] = 3;  /* run of 3 */
            rle[16] = 9; /* value 9 */
            rle[24] = 2; /* run of 2, total 5 != 8 */
            if (crawl_bin_codec_decode_u64(rle, sizeof(rle), 8, CRAWL_ENC_RLE, 0, 0, back) == 0) {
                printf("FAIL short RLE run table was accepted\n");
                g_fail++;
            } else {
                printf("ok   %-42s\n", "short RLE run table rejected");
            }
        }
        free(buf);
    }

    /* Residual payloads carry a nested sub-encoding byte, which is the one place
     * a damaged shard could ask the decoder to recurse or to reconstruct a column
     * without the reference it needs. */
    {
        unsigned char *buf = NULL;
        size_t cap = 0, len = 0;
        uint8_t bw = 0;
        uint64_t back[8];
        uint64_t ref[8];

        for (i = 0; i < 8; i++) {
            v[i] = 5000000ULL + i * 3ULL;
            ref[i] = 5000000ULL;
        }
        if (crawl_bin_codec_encode_delta_u64(v, 8, &buf, &cap, &len, &bw) != 1) {
            printf("FAIL residual rejection setup\n");
            g_fail++;
        } else {
            g_checks++;
            if (crawl_bin_codec_decode_u64(buf, len - 1, 8, CRAWL_ENC_DELTA, bw, 0, back) == 0) {
                printf("FAIL truncated DELTA payload was accepted\n");
                g_fail++;
            } else {
                printf("ok   %-42s\n", "truncated DELTA payload rejected");
            }

            g_checks++;
            if (crawl_bin_codec_decode_u64(buf, 16, 8, CRAWL_ENC_DELTA, bw, 0, back) == 0) {
                printf("FAIL DELTA payload shorter than its prefix was accepted\n");
                g_fail++;
            } else {
                printf("ok   %-42s\n", "DELTA payload without a prefix rejected");
            }

            g_checks++;
            buf[0] = (unsigned char)CRAWL_ENC_DELTA;
            if (crawl_bin_codec_decode_u64(buf, len, 8, CRAWL_ENC_DELTA, bw, 0, back) == 0) {
                printf("FAIL nested DELTA sub-encoding was accepted\n");
                g_fail++;
            } else {
                printf("ok   %-42s\n", "nested residual sub-encoding rejected");
            }

            g_checks++;
            buf[0] = (unsigned char)CRAWL_ENC_BYTES;
            if (crawl_bin_codec_decode_u64(buf, len, 8, CRAWL_ENC_DELTA, bw, 0, back) == 0) {
                printf("FAIL byte-blob DELTA sub-encoding was accepted\n");
                g_fail++;
            } else {
                printf("ok   %-42s\n", "byte-blob residual sub-encoding rejected");
            }
        }
        free(buf);

        /* REF_MTIME cannot be decoded without its reference column, and the
         * reference decoder must not be handed some other encoding. */
        buf = NULL;
        cap = 0;
        len = 0;
        if (crawl_bin_codec_encode_ref_u64(v, ref, 8, &buf, &cap, &len, &bw) != 1) {
            printf("FAIL ref rejection setup\n");
            g_fail++;
        } else {
            g_checks++;
            if (crawl_bin_codec_decode_u64(buf, len, 8, CRAWL_ENC_REF_MTIME, bw, 0, back) == 0) {
                printf("FAIL REF_MTIME decoded without its reference column\n");
                g_fail++;
            } else {
                printf("ok   %-42s\n", "REF_MTIME rejected by the plain decoder");
            }

            g_checks++;
            if (crawl_bin_codec_decode_ref_u64(buf, len, 8, CRAWL_ENC_DELTA, bw, ref, back) == 0) {
                printf("FAIL reference decoder accepted a DELTA chunk\n");
                g_fail++;
            } else {
                printf("ok   %-42s\n", "reference decoder rejects other encodings");
            }
        }
        free(buf);
    }

    free(v);

    printf("\n");
    if (g_fail == 0) {
        printf("crawl_bin_codec: all %d checks passed\n", g_checks);
        return 0;
    }
    printf("crawl_bin_codec: %d of %d checks FAILED\n", g_fail, g_checks);
    return 1;
}
