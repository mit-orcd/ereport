/*
 * crawl_bin_block — columnar row groups for v8 shard record regions.
 *
 * SPDX-License-Identifier: MIT
 */
#include "crawl_bin_block.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <zstd.h>

#include "crawl_bin_codec.h"

/* Columns written for every record, in the order they appear in a row group.
 * NAME_LEN precedes NAME_BYTES so a reader can build offsets while streaming,
 * and MTIME precedes ATIME and CTIME because those two may be stored as
 * CRAWL_ENC_REF_MTIME residuals against it. */
static const uint8_t k_write_columns[] = {
    CRAWL_COL_PARENT_DIR_ID, CRAWL_COL_NAME_LEN, CRAWL_COL_TYPE,      CRAWL_COL_UID,
    CRAWL_COL_GID,           CRAWL_COL_MODE,     CRAWL_COL_SIZE,      CRAWL_COL_INODE,
    CRAWL_COL_DEV_MAJOR,     CRAWL_COL_DEV_MINOR, CRAWL_COL_NLINK,    CRAWL_COL_MTIME,
    CRAWL_COL_ATIME,         CRAWL_COL_CTIME,    CRAWL_COL_NAME_BYTES};

#define WRITE_COLUMN_COUNT ((uint32_t)(sizeof(k_write_columns) / sizeof(k_write_columns[0])))

/* Decoded bytes a record contributes, used only for the flush threshold. */
#define ROWGROUP_FIXED_BYTES_PER_RECORD ((uint64_t)(CRAWL_COL__COUNT - 1) * 8ULL)

static int block_grow(unsigned char **buf, size_t *cap, size_t need) {
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

static int u64_grow(uint64_t **buf, size_t *cap, size_t need) {
    size_t nc;
    uint64_t *p;

    if (*cap >= need) return 0;
    nc = *cap ? *cap : 1024;
    while (nc < need) {
        if (nc > (SIZE_MAX / (2 * sizeof(uint64_t)))) {
            nc = need;
            break;
        }
        nc *= 2;
    }
    p = (uint64_t *)realloc(*buf, nc * sizeof(uint64_t));
    if (!p) return -1;
    *buf = p;
    *cap = nc;
    return 0;
}

/* ---- writer ------------------------------------------------------------- */

int crawl_bin_block_writer_init(crawl_bin_block_writer_t *w) {
    if (!w) return -1;
    memset(w, 0, sizeof(*w));
    w->level = CRAWL_BIN_ZSTD_DEFAULT_LEVEL;
    {
        const char *e = getenv("ECRAWL_ZSTD_LEVEL");
        if (e && *e) {
            char *end = NULL;
            long v = strtol(e, &end, 10);
            if (end && *end == '\0' && v >= 1 && v <= 22) w->level = (int)v;
        }
    }
    return 0;
}

void crawl_bin_block_writer_free(crawl_bin_block_writer_t *w) {
    int i;

    if (!w) return;
    for (i = 0; i < CRAWL_COL__COUNT; i++) free(w->col[i]);
    free(w->names);
    free(w->scratch);
    free(w->comp);
    if (w->cctx) ZSTD_freeCCtx((ZSTD_CCtx *)w->cctx);
    memset(w, 0, sizeof(*w));
}

static int writer_reserve(crawl_bin_block_writer_t *w, size_t need) {
    size_t nc;
    int i;

    if (w->cap >= need) return 0;
    nc = w->cap ? w->cap : 1024;
    while (nc < need) nc *= 2;
    if (nc > CRAWL_BIN_ROWGROUP_MAX_RECORDS) nc = CRAWL_BIN_ROWGROUP_MAX_RECORDS;
    if (nc < need) return -1;

    for (i = 0; i < CRAWL_COL__COUNT; i++) {
        uint64_t *p;

        if (i == CRAWL_COL_NAME_BYTES) continue; /* names live in the byte buffer */
        p = (uint64_t *)realloc(w->col[i], nc * sizeof(uint64_t));
        if (!p) return -1;
        w->col[i] = p;
    }
    w->cap = nc;
    return 0;
}

int crawl_bin_block_writer_append_record(crawl_bin_block_writer_t *w, const bin_record_hdr_t *hdr,
                                         const void *name) {
    uint16_t type_bit;
    size_t n;

    if (!w || !hdr || (hdr->name_len > 0U && !name) || w->count >= CRAWL_BIN_ROWGROUP_MAX_RECORDS) {
        errno = EINVAL;
        return -1;
    }
    type_bit = crawl_bin_type_bit(hdr->type);
    if (type_bit == 0U) {
        errno = EINVAL;
        return -1;
    }
    if (writer_reserve(w, w->count + 1U) != 0) return -1;
    if (hdr->name_len > 0U && block_grow(&w->names, &w->names_cap, w->names_len + hdr->name_len) != 0) return -1;

    n = w->count;
    w->col[CRAWL_COL_PARENT_DIR_ID][n] = hdr->parent_dir_id;
    w->col[CRAWL_COL_NAME_LEN][n] = (uint64_t)hdr->name_len;
    w->col[CRAWL_COL_TYPE][n] = (uint64_t)hdr->type;
    w->col[CRAWL_COL_UID][n] = hdr->uid;
    w->col[CRAWL_COL_GID][n] = hdr->gid;
    w->col[CRAWL_COL_MODE][n] = (uint64_t)hdr->mode;
    w->col[CRAWL_COL_SIZE][n] = hdr->size;
    w->col[CRAWL_COL_INODE][n] = hdr->inode;
    w->col[CRAWL_COL_DEV_MAJOR][n] = (uint64_t)hdr->dev_major;
    w->col[CRAWL_COL_DEV_MINOR][n] = (uint64_t)hdr->dev_minor;
    w->col[CRAWL_COL_NLINK][n] = hdr->nlink;
    w->col[CRAWL_COL_ATIME][n] = hdr->atime;
    w->col[CRAWL_COL_MTIME][n] = hdr->mtime;
    w->col[CRAWL_COL_CTIME][n] = hdr->ctime;

    if (hdr->name_len > 0U) {
        memcpy(w->names + w->names_len, name, hdr->name_len);
        w->names_len += hdr->name_len;
    }
    w->type_mask |= type_bit;
    w->count++;
    w->raw_bytes += ROWGROUP_FIXED_BYTES_PER_RECORD + (uint64_t)hdr->name_len;
    return 0;
}

/* ---- row-group ordering --------------------------------------------------
 *
 * Records reach the writer in crawl arrival order, which interleaves the
 * directories several crawl threads happen to be reading, so parent_dir_id
 * arrives as short broken runs and sibling names arrive scattered. Ordering a
 * group by (parent_dir_id, name) just before it is encoded gives the column
 * codecs one run per directory and puts a directory's names next to each other
 * in the name blob.
 *
 * Ordering happens at flush, so it permutes rows *within* a group and never
 * moves a record between groups: the flush threshold counts decoded bytes,
 * which do not depend on order. Group membership, record counts, per-column
 * min/max zone maps and type_mask are therefore all bit-identical to what an
 * unsorted writer produces.
 *
 * Ties are broken by name_len then by arrival position, so two records that
 * agree on parent and name still land in a defined order.
 */

/* Runs shorter than this sort faster by insertion than by merge. */
#define SORT_RUN_INSERTION_MAX 32u

/*
 * Sort scratch is thread-local, not per-writer: a writer thread flushes one
 * shard at a time, so this is 24 bytes per buffered record per *thread*
 * instead of per live shard. Per shard it would add a quarter of a gigabyte at
 * the default ECRAWL_UID_SHARDS of 1024, for buffers that are live for the
 * microseconds a flush takes.
 */
static __thread uint32_t *tls_sort_idx;
static __thread uint32_t *tls_sort_aux;
static __thread uint64_t *tls_sort_noff;
static __thread uint64_t *tls_sort_vals;
static __thread size_t tls_sort_cap;
static __thread unsigned char *tls_sort_names;
static __thread size_t tls_sort_names_cap;

/* Staging for the residual candidate a large column chunk is trial-compressed
 * as. Thread-local for the same reason the sort scratch is: a writer thread
 * encodes one column of one shard at a time. */
static __thread unsigned char *tls_trial_enc;
static __thread size_t tls_trial_enc_cap;
static __thread unsigned char *tls_trial_comp;
static __thread size_t tls_trial_comp_cap;

void crawl_bin_block_writer_tls_release(void) {
    free(tls_sort_idx);
    free(tls_sort_aux);
    free(tls_sort_noff);
    free(tls_sort_vals);
    free(tls_sort_names);
    free(tls_trial_enc);
    free(tls_trial_comp);
    tls_sort_idx = NULL;
    tls_sort_aux = NULL;
    tls_sort_noff = NULL;
    tls_sort_vals = NULL;
    tls_sort_names = NULL;
    tls_trial_enc = NULL;
    tls_trial_comp = NULL;
    tls_sort_cap = 0;
    tls_sort_names_cap = 0;
    tls_trial_enc_cap = 0;
    tls_trial_comp_cap = 0;
    crawl_bin_codec_tls_release();
}

static int sort_reserve(size_t n) {
    uint32_t *i32;
    uint64_t *u64;

    if (tls_sort_cap >= n) return 0;
    i32 = (uint32_t *)realloc(tls_sort_idx, n * sizeof(*i32));
    if (!i32) return -1;
    tls_sort_idx = i32;
    i32 = (uint32_t *)realloc(tls_sort_aux, n * sizeof(*i32));
    if (!i32) return -1;
    tls_sort_aux = i32;
    u64 = (uint64_t *)realloc(tls_sort_noff, (n + 1U) * sizeof(*u64));
    if (!u64) return -1;
    tls_sort_noff = u64;
    u64 = (uint64_t *)realloc(tls_sort_vals, n * sizeof(*u64));
    if (!u64) return -1;
    tls_sort_vals = u64;
    tls_sort_cap = n;
    return 0;
}

/* Byte-wise name order, shorter name first on a shared prefix. */
static int sort_name_cmp(const crawl_bin_block_writer_t *w, uint32_t a, uint32_t b) {
    const uint64_t *len = w->col[CRAWL_COL_NAME_LEN];
    size_t la = (size_t)len[a];
    size_t lb = (size_t)len[b];
    size_t m = la < lb ? la : lb;
    int r = m ? memcmp(w->names + tls_sort_noff[a], w->names + tls_sort_noff[b], m) : 0;

    if (r != 0) return r;
    if (la != lb) return la < lb ? -1 : 1;
    return a < b ? -1 : (a > b ? 1 : 0);
}

static void sort_run_insertion(const crawl_bin_block_writer_t *w, uint32_t *v, size_t n) {
    size_t i, j;

    for (i = 1; i < n; i++) {
        uint32_t key = v[i];

        for (j = i; j > 0 && sort_name_cmp(w, v[j - 1], key) > 0; j--) v[j] = v[j - 1];
        v[j] = key;
    }
}

/* Bottom-up merge, so a single directory holding a whole row group cannot hit a
 * quadratic path on adversarial names. */
static void sort_run_merge(const crawl_bin_block_writer_t *w, uint32_t *v, size_t n, uint32_t *tmp) {
    uint32_t *src = v;
    uint32_t *dst = tmp;
    size_t width;

    for (width = 1; width < n; width *= 2) {
        size_t i;

        for (i = 0; i < n; i += 2U * width) {
            size_t l = i;
            size_t lend = (i + width < n) ? i + width : n;
            size_t r = lend;
            size_t rend = (i + 2U * width < n) ? i + 2U * width : n;
            size_t k = i;

            while (l < lend && r < rend) dst[k++] = (sort_name_cmp(w, src[r], src[l]) < 0) ? src[r++] : src[l++];
            while (l < lend) dst[k++] = src[l++];
            while (r < rend) dst[k++] = src[r++];
        }
        {
            uint32_t *t = src;

            src = dst;
            dst = t;
        }
    }
    if (src != v) memcpy(v, src, n * sizeof(*v));
}

/*
 * Stable LSD radix over the frame-of-reference parent_dir_id, one byte per pass
 * and only as many passes as the group's own span needs -- a group covers a
 * narrow slice of the shard's dir_ids, so this is usually two. Stability is what
 * leaves equal-parent records in arrival order for the name pass to refine.
 */
static void sort_by_parent(const crawl_bin_block_writer_t *w, size_t n) {
    const uint64_t *pid = w->col[CRAWL_COL_PARENT_DIR_ID];
    uint32_t *src = tls_sort_idx;
    uint32_t *dst = tls_sort_aux;
    uint64_t mn = pid[0];
    uint64_t mx = pid[0];
    uint64_t span;
    unsigned pass, passes;
    size_t i;

    for (i = 1; i < n; i++) {
        if (pid[i] < mn) mn = pid[i];
        if (pid[i] > mx) mx = pid[i];
    }
    span = mx - mn;
    for (passes = 0; span != 0ULL; span >>= 8) passes++;

    for (pass = 0; pass < passes; pass++) {
        size_t cnt[256];
        unsigned shift = pass * 8U;
        size_t sum = 0;
        size_t b;

        memset(cnt, 0, sizeof(cnt));
        for (i = 0; i < n; i++) cnt[((pid[src[i]] - mn) >> shift) & 0xFFU]++;
        for (b = 0; b < 256; b++) {
            size_t c = cnt[b];

            cnt[b] = sum;
            sum += c;
        }
        for (i = 0; i < n; i++) dst[cnt[((pid[src[i]] - mn) >> shift) & 0xFFU]++] = src[i];
        {
            uint32_t *t = src;

            src = dst;
            dst = t;
        }
    }
    if (src != tls_sort_idx) memcpy(tls_sort_idx, src, n * sizeof(*src));
}

static int writer_apply_permutation(crawl_bin_block_writer_t *w) {
    unsigned char *nb;
    size_t i;
    int c;

    for (c = 0; c < CRAWL_COL__COUNT; c++) {
        if (c == CRAWL_COL_NAME_BYTES || !w->col[c]) continue;
        for (i = 0; i < w->count; i++) tls_sort_vals[i] = w->col[c][tls_sort_idx[i]];
        memcpy(w->col[c], tls_sort_vals, w->count * sizeof(*tls_sort_vals));
    }

    if (w->names_len == 0) return 0;
    if (block_grow(&tls_sort_names, &tls_sort_names_cap, w->names_len) != 0) return -1;
    nb = tls_sort_names;
    for (i = 0; i < w->count; i++) {
        uint32_t s = tls_sort_idx[i];
        size_t len = (size_t)(tls_sort_noff[s + 1U] - tls_sort_noff[s]);

        if (len) memcpy(nb, w->names + tls_sort_noff[s], len);
        nb += len;
    }
    memcpy(w->names, tls_sort_names, w->names_len);
    return 0;
}

static int writer_sort_group(crawl_bin_block_writer_t *w) {
    const uint64_t *pid;
    uint64_t off = 0;
    size_t i, run;

    if (w->count < 2U) return 0;
    if (sort_reserve(w->count) != 0) return -1;

    for (i = 0; i < w->count; i++) {
        tls_sort_idx[i] = (uint32_t)i;
        tls_sort_noff[i] = off;
        off += w->col[CRAWL_COL_NAME_LEN][i];
    }
    tls_sort_noff[w->count] = off;

    sort_by_parent(w, w->count);

    pid = w->col[CRAWL_COL_PARENT_DIR_ID];
    for (i = 0; i < w->count; i = run) {
        for (run = i + 1U; run < w->count && pid[tls_sort_idx[run]] == pid[tls_sort_idx[i]]; run++)
            ;
        if (run - i < 2U) continue;
        if (run - i <= SORT_RUN_INSERTION_MAX)
            sort_run_insertion(w, tls_sort_idx + i, run - i);
        else
            sort_run_merge(w, tls_sort_idx + i, run - i, tls_sort_aux);
    }

    return writer_apply_permutation(w);
}

/* Compress src into *dst (grown as needed). Returns compressed length, or
 * (size_t)-1. Two candidates for the same column are compressed into separate
 * buffers so both survive until the smaller is known. */
static size_t writer_compress(crawl_bin_block_writer_t *w, unsigned char **dst, size_t *dst_cap,
                              const unsigned char *src, size_t len) {
    size_t bound;
    size_t cs;

    if (len == 0) return 0;
    bound = ZSTD_compressBound(len);
    if (block_grow(dst, dst_cap, bound) != 0) return (size_t)-1;
    if (!w->cctx) {
        w->cctx = ZSTD_createCCtx();
        if (!w->cctx) return (size_t)-1;
    }
    cs = ZSTD_compressCCtx((ZSTD_CCtx *)w->cctx, *dst, *dst_cap, src, len, w->level);
    if (ZSTD_isError(cs)) return (size_t)-1;
    return cs;
}

/*
 * Stage the residual candidate for one column in tls_trial_enc: differences
 * against mtime for atime and ctime, which on a real tree are frequently equal
 * to it, and against the previous value for everything else. Timestamps are only
 * offered the mtime form -- measured, their first differences never beat their
 * plain encoding, so trying both would buy a second compression pass nothing.
 * Returns 1 when a candidate was staged, 0 when there is none, -1 on error.
 */
static int writer_residual_candidate(const crawl_bin_block_writer_t *w, uint8_t col, size_t *len_out,
                                     uint8_t *enc_out, uint8_t *bw_out) {
    if (col == (uint8_t)CRAWL_COL_ATIME || col == (uint8_t)CRAWL_COL_CTIME) {
        *enc_out = (uint8_t)CRAWL_ENC_REF_MTIME;
        return crawl_bin_codec_encode_ref_u64(w->col[col], w->col[CRAWL_COL_MTIME], w->count, &tls_trial_enc,
                                              &tls_trial_enc_cap, len_out, bw_out);
    }
    *enc_out = (uint8_t)CRAWL_ENC_DELTA;
    return crawl_bin_codec_encode_delta_u64(w->col[col], w->count, &tls_trial_enc, &tls_trial_enc_cap, len_out,
                                            bw_out);
}

int crawl_bin_block_writer_flush(crawl_bin_block_writer_t *w, FILE *fp, crawl_bin_block_fwrite_fn wfwrite,
                                 uint64_t *bytes_written_out) {
    bin_rowgroup_hdr_t rg;
    bin_colchunk_hdr_t dir[WRITE_COLUMN_COUNT];
    unsigned char **payload = NULL;
    size_t *payload_len = NULL;
    uint32_t ci;
    uint64_t total_payload = 0;
    uint64_t total_raw = 0;
    int rc = -1;

    if (bytes_written_out) *bytes_written_out = 0;
    if (!w || !fp || !wfwrite) return -1;
    if (w->count == 0) return 0;
    if (w->type_mask == 0U) return -1;
    if (writer_sort_group(w) != 0) return -1;

    /* Each column is encoded and compressed independently, so all payloads must
     * be held until the directory that describes them can be written first. */
    payload = (unsigned char **)calloc(WRITE_COLUMN_COUNT, sizeof(*payload));
    payload_len = (size_t *)calloc(WRITE_COLUMN_COUNT, sizeof(*payload_len));
    if (!payload || !payload_len) goto done;

    memset(dir, 0, sizeof(dir));

    for (ci = 0; ci < WRITE_COLUMN_COUNT; ci++) {
        uint8_t col = k_write_columns[ci];
        uint8_t enc = 0, bw = 0;
        uint64_t mn = 0, mx = 0;
        size_t enc_len = 0;
        size_t cs;
        const unsigned char *src;
        const unsigned char *comp;
        uint64_t raw_len;

        if (col == CRAWL_COL_NAME_BYTES) {
            /* Names are an opaque blob; lengths in CRAWL_COL_NAME_LEN slice it.
             * A zone map over bytes is meaningless, so it stays zeroed. */
            src = w->names;
            enc_len = w->names_len;
            raw_len = (uint64_t)w->names_len;
            enc = (uint8_t)CRAWL_ENC_BYTES;
        } else {
            if (crawl_bin_codec_encode_u64(w->col[col], w->count, &w->scratch, &w->scratch_cap, &enc_len, &enc,
                                           &bw, &mn, &mx) != 0)
                goto done;
            src = w->scratch;
            raw_len = (uint64_t)enc_len;
        }

        cs = writer_compress(w, &w->comp, &w->comp_cap, src, enc_len);
        if (cs == (size_t)-1) goto done;
        comp = w->comp;

        /* Store-if-smaller, measured after zstd rather than before it: a residual
         * candidate is often the larger payload and the smaller frame. */
        if (col != CRAWL_COL_NAME_BYTES && enc_len >= CRAWL_BIN_ENC_TRIAL_MIN_BYTES) {
            size_t trial_len = 0;
            uint8_t trial_enc = 0, trial_bw = 0;
            int staged = writer_residual_candidate(w, col, &trial_len, &trial_enc, &trial_bw);

            if (staged < 0) goto done;
            if (staged == 1) {
                size_t trial_cs =
                    writer_compress(w, &tls_trial_comp, &tls_trial_comp_cap, tls_trial_enc, trial_len);

                if (trial_cs == (size_t)-1) goto done;
                if (trial_cs < cs) {
                    cs = trial_cs;
                    comp = tls_trial_comp;
                    enc = trial_enc;
                    bw = trial_bw;
                    raw_len = (uint64_t)trial_len;
                }
            }
        }
        if (cs > 0xFFFFFFFFULL) goto done;

        if (cs > 0) {
            payload[ci] = (unsigned char *)malloc(cs);
            if (!payload[ci]) goto done;
            memcpy(payload[ci], comp, cs);
        }
        payload_len[ci] = cs;

        dir[ci].column_id = col;
        dir[ci].encoding = enc;
        dir[ci].bit_width = bw;
        dir[ci].comp_bytes = (uint32_t)cs;
        dir[ci].raw_bytes = raw_len;
        dir[ci].min_value = mn;
        dir[ci].max_value = mx;

        total_payload += (uint64_t)cs;
        total_raw += raw_len;
    }

    memset(&rg, 0, sizeof(rg));
    rg.record_count = (uint32_t)w->count;
    rg.column_count = WRITE_COLUMN_COUNT;
    rg.comp_bytes = total_payload;
    rg.raw_bytes = total_raw;
    rg.type_mask = w->type_mask;

    if (wfwrite(&rg, sizeof(rg), 1, fp) != 1) goto done;
    if (wfwrite(dir, sizeof(dir[0]), WRITE_COLUMN_COUNT, fp) != WRITE_COLUMN_COUNT) goto done;
    for (ci = 0; ci < WRITE_COLUMN_COUNT; ci++) {
        if (payload_len[ci] == 0) continue;
        if (wfwrite(payload[ci], 1, payload_len[ci], fp) != payload_len[ci]) goto done;
    }

    if (bytes_written_out) *bytes_written_out = crawl_bin_rowgroup_total_bytes(&rg);

    w->count = 0;
    w->names_len = 0;
    w->raw_bytes = 0;
    w->type_mask = 0;
    rc = 0;

done:
    if (payload) {
        for (ci = 0; ci < WRITE_COLUMN_COUNT; ci++) free(payload[ci]);
        free(payload);
    }
    free(payload_len);
    return rc;
}

/* ---- reader ------------------------------------------------------------- */

int crawl_bin_block_reader_init(crawl_bin_block_reader_t *r, const crawl_bin_chunk_stdio_t *io, FILE *fp,
                                uint64_t start_off, uint64_t end_off) {
    if (!r || !io || !fp) return -1;
    memset(r, 0, sizeof(*r));
    return crawl_bin_block_reader_reinit(r, io, fp, start_off, end_off);
}

int crawl_bin_block_reader_reinit(crawl_bin_block_reader_t *r, const crawl_bin_chunk_stdio_t *io, FILE *fp,
                                  uint64_t start_off, uint64_t end_off) {
    if (!r || !io || !fp) return -1;
    r->io = *io;
    r->fp = fp;
    r->pos = start_off;
    r->end = end_off;
    r->projection = CRAWL_PROJECTION_ALL;
    r->rg_projection = CRAWL_PROJECTION_ALL;
    r->hardlink_only = 0;
    r->rg_records = 0;
    r->rg_cursor = 0;
    r->names_len = 0;
    r->range_count = 0;
    r->type_bit = 0;
    r->blocks_decompressed = 0;
    r->blocks_skipped = 0;
    r->records_skipped = 0;
    r->column_bytes_read = 0;
    r->column_bytes_skipped = 0;
    if (fseeko(fp, (off_t)start_off, SEEK_SET) != 0) return -1;
    return 0;
}

int crawl_bin_block_reader_set_projection(crawl_bin_block_reader_t *r, uint32_t projection) {
    if (!r) return -1;
    projection &= CRAWL_PROJECTION_ALL;
    /* Name bytes are a flat blob; without the lengths there is no way to tell
     * where one record's name ends and the next begins. */
    if (projection & CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES)) projection |= CRAWL_COL_BIT(CRAWL_COL_NAME_LEN);
    /* atime and ctime may be stored as residuals against mtime, and which chunks
     * are is not known until their headers are read, so mtime is pulled in
     * whenever either is wanted. */
    if (projection & (CRAWL_COL_BIT(CRAWL_COL_ATIME) | CRAWL_COL_BIT(CRAWL_COL_CTIME)))
        projection |= CRAWL_COL_BIT(CRAWL_COL_MTIME);
    r->projection = projection;
    r->hardlink_only &= projection;
    /* Until a group is loaded, everything projected is nominally available. */
    r->rg_projection = projection;
    return 0;
}

int crawl_bin_block_reader_set_hardlink_columns(crawl_bin_block_reader_t *r, uint32_t mask) {
    if (!r) return -1;
    /* NLINK is the evidence the pruning rests on, and the name columns are unrelated to hardlinks.
     * MTIME can be what atime and ctime are stored against, so dropping it per group would leave
     * them undecodable. */
    mask &= ~(CRAWL_COL_BIT(CRAWL_COL_NLINK) | CRAWL_COL_BIT(CRAWL_COL_NAME_LEN) |
              CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES) | CRAWL_COL_BIT(CRAWL_COL_MTIME));
    r->hardlink_only = mask & r->projection;
    return 0;
}

int crawl_bin_block_reader_add_range(crawl_bin_block_reader_t *r, int column_id, uint64_t lo, uint64_t hi) {
    if (!r || column_id < 0 || column_id >= CRAWL_COL__COUNT || column_id == CRAWL_COL_NAME_BYTES) return -1;
    if (lo > hi || r->range_count >= CRAWL_BIN_MAX_RANGE_FILTERS) return -1;
    r->ranges[r->range_count].column_id = (uint8_t)column_id;
    r->ranges[r->range_count].lo = lo;
    r->ranges[r->range_count].hi = hi;
    r->range_count++;
    return 0;
}

int crawl_bin_block_reader_set_filter(crawl_bin_block_reader_t *r, int have_size_gt, uint64_t size_gt,
                                      int type_filter) {
    uint16_t type_bit = type_filter ? crawl_bin_type_bit((uint8_t)type_filter) : 0U;

    if (!r || (!have_size_gt && !type_filter) || (type_filter && type_bit == 0U)) return -1;
    /* --size-gt is strict, so the smallest interesting size is size_gt + 1;
     * saturate rather than wrap, which would turn "everything" into "nothing". */
    if (have_size_gt && size_gt != UINT64_MAX &&
        crawl_bin_block_reader_add_range(r, CRAWL_COL_SIZE, size_gt + 1ULL, UINT64_MAX) != 0)
        return -1;
    r->type_bit = type_bit;
    return 0;
}

void crawl_bin_block_reader_free(crawl_bin_block_reader_t *r) {
    int i;

    if (!r) return;
    for (i = 0; i < CRAWL_COL__COUNT; i++) free(r->col[i]);
    free(r->names);
    free(r->name_off);
    free(r->comp);
    free(r->raw);
    if (r->dctx) ZSTD_freeDCtx((ZSTD_DCtx *)r->dctx);
    memset(r, 0, sizeof(*r));
}

/* Read and zstd-decode one column payload into r->raw. Returns 0 on success. */
static int reader_pull_column(crawl_bin_block_reader_t *r, const bin_colchunk_hdr_t *ch, size_t *raw_len_out) {
    size_t got;

    *raw_len_out = 0;
    if (ch->comp_bytes == 0U) {
        if (ch->raw_bytes != 0ULL) return -1;
        return 0;
    }
    if (block_grow(&r->comp, &r->comp_cap, ch->comp_bytes) != 0) return -1;
    if (r->io.fread(r->comp, 1, ch->comp_bytes, r->fp) != ch->comp_bytes) return -1;
    r->column_bytes_read += ch->comp_bytes;

    if (ch->raw_bytes > (uint64_t)SIZE_MAX) return -1;
    if (block_grow(&r->raw, &r->raw_cap, (size_t)(ch->raw_bytes ? ch->raw_bytes : 1)) != 0) return -1;
    if (!r->dctx) {
        r->dctx = ZSTD_createDCtx();
        if (!r->dctx) return -1;
    }
    got = ZSTD_decompressDCtx((ZSTD_DCtx *)r->dctx, r->raw, r->raw_cap, r->comp, ch->comp_bytes);
    if (ZSTD_isError(got) || got != (size_t)ch->raw_bytes) return -1;
    *raw_len_out = got;
    return 0;
}

/*
 * Load the next row group. Returns 1 when a group was decoded, 2 when the zone
 * map proved it cannot match and it was skipped, 0 at end of range, -1 on error.
 */
static int reader_load_group(crawl_bin_block_reader_t *r) {
    bin_rowgroup_hdr_t rg;
    bin_colchunk_hdr_t *dir = NULL;
    uint64_t dir_bytes;
    uint64_t group_total;
    uint32_t ci;
    uint64_t skip_run = 0; /* payload bytes to seek past before the next wanted column */
    uint32_t want;         /* columns to decode for this group; see hardlink_only */
    int rc = -1;
    int have_name_len = 0;
    int have_mtime = 0;

    if (r->pos >= r->end) return 0;
    if (r->end - r->pos < sizeof(bin_rowgroup_hdr_t)) return -1;

    if (r->io.fread(&rg, sizeof(rg), 1, r->fp) != 1) return -1;
    if (rg.column_count == 0U || rg.column_count > 64U) return -1;
    group_total = crawl_bin_rowgroup_total_bytes(&rg);
    if (group_total > (r->end - r->pos)) return -1;

    dir_bytes = (uint64_t)rg.column_count * (uint64_t)sizeof(bin_colchunk_hdr_t);
    dir = (bin_colchunk_hdr_t *)malloc((size_t)dir_bytes);
    if (!dir) return -1;
    if (r->io.fread(dir, sizeof(dir[0]), rg.column_count, r->fp) != rg.column_count) goto done;

    if (r->range_count || r->type_bit) {
        /* A zeroed summary means a damaged header, so fall through and decode:
         * skipping on it could drop records that do match. */
        int summary_ok = rg.record_count != 0U && rg.type_mask != 0U;
        int range_skip = 0;
        int type_skip = r->type_bit && !(rg.type_mask & r->type_bit);
        unsigned k;

        for (k = 0; k < r->range_count && !range_skip; k++) {
            for (ci = 0; ci < rg.column_count; ci++) {
                if (dir[ci].column_id != r->ranges[k].column_id) continue;
                /* Zone map and wanted range are disjoint, so nothing here can
                 * match this term, and the terms combine with AND. */
                if (dir[ci].max_value < r->ranges[k].lo || dir[ci].min_value > r->ranges[k].hi) range_skip = 1;
                break;
            }
        }

        if (summary_ok && (range_skip || type_skip)) {
            if (rg.comp_bytes && fseeko(r->fp, (off_t)rg.comp_bytes, SEEK_CUR) != 0) goto done;
            r->pos += group_total;
            r->rg_records = 0;
            r->rg_cursor = 0;
            r->blocks_skipped++;
            r->records_skipped += rg.record_count;
            r->column_bytes_skipped += rg.comp_bytes;
            rc = 2;
            goto done;
        }
    }

    if (rg.record_count > 0U && u64_grow(&r->name_off, &r->name_off_cap, (size_t)rg.record_count + 1U) != 0)
        goto done;
    r->names_len = 0;

    want = r->projection;
    if (r->hardlink_only) {
        /* Drop the hardlink-only columns when the NLINK zone map proves this group has no hardlink.
         * No NLINK chunk means no evidence, so keep them: guessing wrong here loses data. */
        for (ci = 0; ci < rg.column_count; ci++) {
            if (dir[ci].column_id != (uint8_t)CRAWL_COL_NLINK) continue;
            if (dir[ci].max_value <= 1ULL) want &= ~r->hardlink_only;
            break;
        }
    }

    for (ci = 0; ci < rg.column_count; ci++) {
        const bin_colchunk_hdr_t *ch = &dir[ci];
        int col = (int)ch->column_id;
        int wanted = (col >= 0 && col < CRAWL_COL__COUNT) && (want & CRAWL_COL_BIT(col));
        size_t raw_len = 0;

        if (!wanted) {
            /* Coalesce consecutive unwanted columns into one seek: skipping ten
             * of twelve columns should cost one lseek, not ten. */
            skip_run += ch->comp_bytes;
            r->column_bytes_skipped += ch->comp_bytes;
            continue;
        }
        if (skip_run) {
            if (fseeko(r->fp, (off_t)skip_run, SEEK_CUR) != 0) goto done;
            skip_run = 0;
        }

        if (reader_pull_column(r, ch, &raw_len) != 0) goto done;

        if (col == CRAWL_COL_NAME_BYTES) {
            if (block_grow(&r->names, &r->names_cap, raw_len ? raw_len : 1) != 0) goto done;
            if (raw_len) memcpy(r->names, r->raw, raw_len);
            r->names_len = raw_len;
            continue;
        }

        if (u64_grow(&r->col[col], &r->col_cap[col], rg.record_count ? rg.record_count : 1U) != 0) goto done;
        if (ch->encoding == (uint8_t)CRAWL_ENC_REF_MTIME) {
            /* The writer emits mtime ahead of atime and ctime, so a well-formed
             * group has it decoded by now; a group that does not is one whose
             * residual column cannot be reconstructed at all. */
            if (!have_mtime) goto done;
            if (crawl_bin_codec_decode_ref_u64(r->raw, raw_len, rg.record_count, ch->encoding, ch->bit_width,
                                               r->col[CRAWL_COL_MTIME], r->col[col]) != 0)
                goto done;
        } else if (crawl_bin_codec_decode_u64(r->raw, raw_len, rg.record_count, ch->encoding, ch->bit_width,
                                              ch->min_value, r->col[col]) != 0) {
            goto done;
        }
        if (col == CRAWL_COL_NAME_LEN) have_name_len = 1;
        if (col == CRAWL_COL_MTIME) have_mtime = 1;
    }
    if (skip_run && fseeko(r->fp, (off_t)skip_run, SEEK_CUR) != 0) goto done;

    /* Name offsets are the prefix sum of the lengths, so no offsets column is
     * stored on disk. Only crawl_bin_block_reader_name reads them, and it refuses
     * without the name bytes, so a consumer that projects the lengths alone (to
     * tell an empty name from a real one) does not pay for the sum. */
    if (have_name_len && (want & CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES))) {
        uint64_t off = 0;
        uint32_t i;

        for (i = 0; i < rg.record_count; i++) {
            r->name_off[i] = off;
            off += r->col[CRAWL_COL_NAME_LEN][i];
        }
        r->name_off[rg.record_count] = off;
        if (off != (uint64_t)r->names_len) goto done; /* lengths must tile the blob exactly */
    }

    r->rg_projection = want;
    r->pos += group_total;
    r->rg_records = rg.record_count;
    r->rg_cursor = 0;
    r->blocks_decompressed++;
    rc = 1;

done:
    free(dir);
    return rc;
}

int crawl_bin_block_reader_next_group(crawl_bin_block_reader_t *r, uint32_t *records_out) {
    if (!r) return -1;
    for (;;) {
        int rc = reader_load_group(r);

        if (rc == 2) continue; /* zone map proved this group cannot match */
        if (rc != 1) return rc;
        if (r->rg_records == 0U) continue;
        if (records_out) *records_out = r->rg_records;
        /* Column-at-a-time consumers own the whole group, so the row cursor is
         * parked at the end to keep _next() from re-yielding these rows. */
        r->rg_cursor = r->rg_records;
        return 1;
    }
}

const unsigned char *crawl_bin_block_reader_name(const crawl_bin_block_reader_t *r, uint32_t i, size_t *len_out) {
    uint64_t start, len;

    if (len_out) *len_out = 0;
    if (!r || i >= r->rg_records) return NULL;
    if (!(r->rg_projection & CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES))) return NULL;
    if (!r->name_off) return NULL;
    start = r->name_off[i];
    len = r->name_off[i + 1] - start;
    if (start + len > (uint64_t)r->names_len) return NULL;
    if (len_out) *len_out = (size_t)len;
    return r->names + start;
}

int crawl_bin_block_reader_next(crawl_bin_block_reader_t *r, bin_record_hdr_t *hdr, const unsigned char **name) {
    uint32_t i;

    if (!r || !hdr) return -1;
    while (r->rg_cursor >= r->rg_records) {
        int rc = reader_load_group(r);

        if (rc == 2) continue; /* zone map proved this group cannot match */
        if (rc <= 0) return rc; /* 0 end, -1 error */
    }
    i = r->rg_cursor++;

    memset(hdr, 0, sizeof(*hdr));
#define COL_AT(c) ((r->rg_projection & CRAWL_COL_BIT(c)) && r->col[c] ? r->col[c][i] : 0ULL)
    hdr->parent_dir_id = COL_AT(CRAWL_COL_PARENT_DIR_ID);
    hdr->name_len = (uint16_t)COL_AT(CRAWL_COL_NAME_LEN);
    hdr->type = (uint8_t)COL_AT(CRAWL_COL_TYPE);
    hdr->uid = COL_AT(CRAWL_COL_UID);
    hdr->gid = COL_AT(CRAWL_COL_GID);
    hdr->mode = (uint32_t)COL_AT(CRAWL_COL_MODE);
    hdr->size = COL_AT(CRAWL_COL_SIZE);
    hdr->inode = COL_AT(CRAWL_COL_INODE);
    hdr->dev_major = (uint32_t)COL_AT(CRAWL_COL_DEV_MAJOR);
    hdr->dev_minor = (uint32_t)COL_AT(CRAWL_COL_DEV_MINOR);
    hdr->nlink = COL_AT(CRAWL_COL_NLINK);
    hdr->atime = COL_AT(CRAWL_COL_ATIME);
    hdr->mtime = COL_AT(CRAWL_COL_MTIME);
    hdr->ctime = COL_AT(CRAWL_COL_CTIME);
#undef COL_AT

    if (name) {
        size_t nl = 0;
        *name = crawl_bin_block_reader_name(r, i, &nl);
    }
    return 1;
}
