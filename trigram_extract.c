#include "trigram_extract.h"

#include <stdlib.h>
#include <string.h>

int trigram_ensure_buf(char **buf, size_t *cap, size_t need) {
    char *p;
    size_t nc;

    if (*cap >= need) return 0;
    nc = *cap ? *cap : 4096U;
    while (nc < need) nc <<= 1;
    p = (char *)realloc(*buf, nc);
    if (!p) return -1;
    *buf = p;
    *cap = nc;
    return 0;
}

int trigram_cmp_u32(const void *a, const void *b) {
    uint32_t aa = *(const uint32_t *)a;
    uint32_t bb = *(const uint32_t *)b;
    return (aa > bb) - (aa < bb);
}

static void insertion_sort_u32(uint32_t *a, size_t n) {
    size_t i;
    for (i = 1; i < n; i++) {
        uint32_t key = a[i];
        size_t j = i;
        while (j > 0 && a[j - 1] > key) {
            a[j] = a[j - 1];
            j--;
        }
        a[j] = key;
    }
}

/*
 * Longest input this can take on the stack. A path yields fewer trigram codes than it has bytes,
 * so PATH_MAX rounded up covers every real input; anything larger falls back to qsort.
 */
#define RADIX_U32_MAX_N 4096

/*
 * LSD radix over the code bytes. glibc's qsort costs an indirect call per comparison plus a temp
 * allocation, and this runs once per path — 8.3% of the badge run sat in msort_with_tmp. All four
 * digit histograms come from a single counting pass, and a digit whose byte is the same in every
 * key is skipped, so 24-bit trigram codes cost three scatter passes rather than four.
 */
static void radix_sort_u32(uint32_t *a, size_t n) {
    uint32_t scratch[RADIX_U32_MAX_N];
    size_t count[4][256];
    uint32_t *src = a;
    uint32_t *dst = scratch;
    size_t i;
    int pass;

    memset(count, 0, sizeof(count));
    for (i = 0; i < n; i++) {
        uint32_t v = a[i];

        count[0][v & 0xFFU]++;
        count[1][(v >> 8) & 0xFFU]++;
        count[2][(v >> 16) & 0xFFU]++;
        count[3][(v >> 24) & 0xFFU]++;
    }

    for (pass = 0; pass < 4; pass++) {
        unsigned shift = (unsigned)pass * 8U;
        size_t off[256];
        size_t acc = 0;
        int d;

        /* A permutation does not change the multiset, so the counts stay valid across passes. */
        if (count[pass][(a[0] >> shift) & 0xFFU] == n) continue;

        for (d = 0; d < 256; d++) {
            off[d] = acc;
            acc += count[pass][d];
        }
        for (i = 0; i < n; i++) dst[off[(src[i] >> shift) & 0xFFU]++] = src[i];
        {
            uint32_t *sw = src;

            src = dst;
            dst = sw;
        }
    }
    if (src != a) memcpy(a, src, n * sizeof(*a));
}

static void sort_codes_unique(uint32_t *codes, size_t *count) {
    size_t n = *count;
    size_t w;
    size_t i;

    if (n <= 1) return;
    if (n <= 64U)
        insertion_sort_u32(codes, n);
    else if (n <= (size_t)RADIX_U32_MAX_N)
        radix_sort_u32(codes, n);
    else
        qsort(codes, n, sizeof(*codes), trigram_cmp_u32);

    w = 1;
    for (i = 1; i < n; i++) {
        if (codes[i] != codes[w - 1]) codes[w++] = codes[i];
    }
    *count = w;
}

/* Count sliding 3-byte windows (one trigram each) across path segments (length >= 3). */
/* Final path component only — parents are indexed when those entries appear as their own basenames. */
static const char *path_basename_seg(const char *path, size_t *len_out) {
    const char *slash;
    const char *base;

    if (!path) {
        *len_out = 0;
        return "";
    }
    slash = strrchr(path, '/');
    base = slash ? slash + 1 : path;
    *len_out = strlen(base);
    return base;
}

static size_t count_basename_trigram_windows(const char *path) {
    size_t seg_len = 0;

    (void)path_basename_seg(path, &seg_len);
    return seg_len >= 3 ? seg_len - 2 : 0;
}

int trigram_extract_path(const char *path, uint32_t **out_codes, size_t *out_count,
                         trigram_scratch_t *scratch) {
    const char *base;
    size_t seg_len = 0;
    size_t nraw;
    uint32_t *codes;
    size_t pos = 0;
    size_t i;
    char *lower_seg;
    int codes_owned = 0;
    int lower_owned = 0;

    base = path_basename_seg(path, &seg_len);
    nraw = count_basename_trigram_windows(path);
    if (nraw == 0) {
        *out_codes = NULL;
        *out_count = 0;
        return 0;
    }

    if (scratch) {
        if (trigram_ensure_buf(&scratch->codes_buf, &scratch->codes_cap, nraw * sizeof(uint32_t)) != 0) return -1;
        codes = (uint32_t *)scratch->codes_buf;
        if (trigram_ensure_buf(&scratch->lower_seg_buf, &scratch->lower_seg_cap, seg_len + 1U) != 0) return -1;
        lower_seg = scratch->lower_seg_buf;
    } else {
        codes = (uint32_t *)malloc(nraw * sizeof(uint32_t));
        if (!codes) return -1;
        codes_owned = 1;
        lower_seg = (char *)malloc(seg_len + 1);
        if (!lower_seg) {
            free(codes);
            return -1;
        }
        lower_owned = 1;
    }

    for (i = 0; i < seg_len; i++) {
        unsigned char b = (unsigned char)base[i];
        lower_seg[i] = (char)((b >= (unsigned char)'A' && b <= (unsigned char)'Z') ? (b + 32U) : b);
    }
    lower_seg[seg_len] = '\0';

    /* Rolling 24-bit window: one byte in, one code out after the first triplet. */
    {
        uint32_t tri = ((uint32_t)(unsigned char)lower_seg[0] << 16) |
                       ((uint32_t)(unsigned char)lower_seg[1] << 8) |
                       (uint32_t)(unsigned char)lower_seg[2];
        codes[pos++] = tri;
        for (i = 3; i < seg_len; i++) {
            tri = ((tri << 8) | (uint32_t)(unsigned char)lower_seg[i]) & 0x00FFFFFFU;
            codes[pos++] = tri;
        }
    }
    if (lower_owned) free(lower_seg);

    if (pos != nraw) {
        if (codes_owned) free(codes);
        return -1;
    }

    sort_codes_unique(codes, &nraw);
    *out_codes = codes;
    *out_count = nraw;
    return 0;
}
