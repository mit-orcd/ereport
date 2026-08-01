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

    free(v);

    printf("\n");
    if (g_fail == 0) {
        printf("crawl_bin_codec: all %d checks passed\n", g_checks);
        return 0;
    }
    printf("crawl_bin_codec: %d of %d checks FAILED\n", g_fail, g_checks);
    return 1;
}
