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
 * NAME_LEN precedes NAME_BYTES so a reader can build offsets while streaming. */
static const uint8_t k_write_columns[] = {
    CRAWL_COL_PARENT_DIR_ID, CRAWL_COL_NAME_LEN, CRAWL_COL_TYPE,      CRAWL_COL_UID,
    CRAWL_COL_GID,           CRAWL_COL_MODE,     CRAWL_COL_SIZE,      CRAWL_COL_INODE,
    CRAWL_COL_DEV_MAJOR,     CRAWL_COL_DEV_MINOR, CRAWL_COL_NLINK,    CRAWL_COL_ATIME,
    CRAWL_COL_MTIME,         CRAWL_COL_CTIME,    CRAWL_COL_NAME_BYTES};

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

/* Compress src into w->comp. Returns compressed length, or (size_t)-1. */
static size_t writer_compress(crawl_bin_block_writer_t *w, const unsigned char *src, size_t len) {
    size_t bound;
    size_t cs;

    if (len == 0) return 0;
    bound = ZSTD_compressBound(len);
    if (block_grow(&w->comp, &w->comp_cap, bound) != 0) return (size_t)-1;
    if (!w->cctx) {
        w->cctx = ZSTD_createCCtx();
        if (!w->cctx) return (size_t)-1;
    }
    cs = ZSTD_compressCCtx((ZSTD_CCtx *)w->cctx, w->comp, w->comp_cap, src, len, w->level);
    if (ZSTD_isError(cs)) return (size_t)-1;
    return cs;
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

        cs = writer_compress(w, src, enc_len);
        if (cs == (size_t)-1) goto done;
        if (cs > 0xFFFFFFFFULL) goto done;

        if (cs > 0) {
            payload[ci] = (unsigned char *)malloc(cs);
            if (!payload[ci]) goto done;
            memcpy(payload[ci], w->comp, cs);
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
    r->projection = projection;
    r->hardlink_only &= projection;
    /* Until a group is loaded, everything projected is nominally available. */
    r->rg_projection = projection;
    return 0;
}

int crawl_bin_block_reader_set_hardlink_columns(crawl_bin_block_reader_t *r, uint32_t mask) {
    if (!r) return -1;
    /* NLINK is the evidence the pruning rests on, and the name columns are unrelated to hardlinks. */
    mask &= ~(CRAWL_COL_BIT(CRAWL_COL_NLINK) | CRAWL_COL_BIT(CRAWL_COL_NAME_LEN) |
              CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES));
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
        if (crawl_bin_codec_decode_u64(r->raw, raw_len, rg.record_count, ch->encoding, ch->bit_width,
                                       ch->min_value, r->col[col]) != 0)
            goto done;
        if (col == CRAWL_COL_NAME_LEN) have_name_len = 1;
    }
    if (skip_run && fseeko(r->fp, (off_t)skip_run, SEEK_CUR) != 0) goto done;

    /* Name offsets are the prefix sum of the lengths, so no offsets column is
     * stored on disk. */
    if (have_name_len) {
        uint64_t off = 0;
        uint32_t i;

        for (i = 0; i < rg.record_count; i++) {
            r->name_off[i] = off;
            off += r->col[CRAWL_COL_NAME_LEN][i];
        }
        r->name_off[rg.record_count] = off;
        if ((want & CRAWL_COL_BIT(CRAWL_COL_NAME_BYTES)) && off != (uint64_t)r->names_len) goto done;
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
