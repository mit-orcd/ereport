/*
 * crawl_bin_block — zstd block (de)compression for v6 shard record regions.
 *
 * SPDX-License-Identifier: MIT
 */
#include "crawl_bin_block.h"

#include <stdlib.h>
#include <string.h>

#include <zstd.h>

static int block_grow(unsigned char **buf, size_t *cap, size_t need) {
    if (*cap >= need) return 0;
    size_t nc = *cap ? *cap : 4096;
    while (nc < need) {
        if (nc > (SIZE_MAX / 2)) {
            nc = need;
            break;
        }
        nc *= 2;
    }
    unsigned char *p = (unsigned char *)realloc(*buf, nc);
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
    if (!w) return;
    free(w->raw);
    free(w->comp);
    memset(w, 0, sizeof(*w));
}

int crawl_bin_block_writer_append(crawl_bin_block_writer_t *w, const void *data, size_t len) {
    if (!w) return -1;
    if (len == 0) return 0;
    if (block_grow(&w->raw, &w->raw_cap, w->raw_len + len) != 0) return -1;
    memcpy(w->raw + w->raw_len, data, len);
    w->raw_len += len;
    return 0;
}

int crawl_bin_block_writer_flush(crawl_bin_block_writer_t *w, FILE *fp, crawl_bin_block_fwrite_fn wfwrite,
                                 uint64_t *bytes_written_out) {
    if (bytes_written_out) *bytes_written_out = 0;
    if (!w || !fp || !wfwrite) return -1;
    if (w->raw_len == 0) return 0;

    size_t bound = ZSTD_compressBound(w->raw_len);
    if (block_grow(&w->comp, &w->comp_cap, bound) != 0) return -1;

    size_t cs = ZSTD_compress(w->comp, w->comp_cap, w->raw, w->raw_len, w->level);
    if (ZSTD_isError(cs)) return -1;
    if (cs > 0xFFFFFFFFULL || w->raw_len > 0xFFFFFFFFULL) return -1;

    bin_block_hdr_t bh;
    bh.raw_size = (uint32_t)w->raw_len;
    bh.comp_size = (uint32_t)cs;
    if (wfwrite(&bh, sizeof(bh), 1, fp) != 1) return -1;
    if (wfwrite(w->comp, 1, cs, fp) != cs) return -1;

    if (bytes_written_out) *bytes_written_out = (uint64_t)sizeof(bh) + (uint64_t)cs;
    w->raw_len = 0;
    return 0;
}

/* ---- reader ------------------------------------------------------------- */

int crawl_bin_block_reader_init(crawl_bin_block_reader_t *r, const crawl_bin_chunk_stdio_t *io, FILE *fp,
                                uint64_t start_off, uint64_t end_off) {
    if (!r || !io || !fp) return -1;
    memset(r, 0, sizeof(*r));
    r->io = *io;
    r->fp = fp;
    r->pos = start_off;
    r->end = end_off;
    if (fseeko(fp, (off_t)start_off, SEEK_SET) != 0) return -1;
    return 0;
}

void crawl_bin_block_reader_free(crawl_bin_block_reader_t *r) {
    if (!r) return;
    free(r->comp);
    free(r->raw);
    r->comp = NULL;
    r->raw = NULL;
    r->comp_cap = r->raw_cap = r->raw_len = r->raw_off = 0;
}

/* Pull and decompress the next block into r->raw. Returns 1 ok, 0 end, -1 err. */
static int reader_load_block(crawl_bin_block_reader_t *r) {
    if (r->pos >= r->end) return 0;
    if (r->end - r->pos < sizeof(bin_block_hdr_t)) return -1;

    bin_block_hdr_t bh;
    if (r->io.fread(&bh, sizeof(bh), 1, r->fp) != 1) return -1;
    uint64_t block_total = (uint64_t)sizeof(bh) + (uint64_t)bh.comp_size;
    if (block_total > (r->end - r->pos)) return -1;

    if (block_grow(&r->comp, &r->comp_cap, bh.comp_size) != 0) return -1;
    if (bh.comp_size && r->io.fread(r->comp, 1, bh.comp_size, r->fp) != bh.comp_size) return -1;

    if (block_grow(&r->raw, &r->raw_cap, bh.raw_size ? bh.raw_size : 1) != 0) return -1;
    size_t got = ZSTD_decompress(r->raw, r->raw_cap, r->comp, bh.comp_size);
    if (ZSTD_isError(got) || got != bh.raw_size) return -1;

    r->raw_len = bh.raw_size;
    r->raw_off = 0;
    r->pos += block_total;
    return 1;
}

int crawl_bin_block_reader_next(crawl_bin_block_reader_t *r, bin_record_hdr_t *hdr, const unsigned char **name) {
    if (!r || !hdr) return -1;
    while (r->raw_off >= r->raw_len) {
        int rc = reader_load_block(r);
        if (rc <= 0) return rc; /* 0 end, -1 error */
    }
    if (r->raw_len - r->raw_off < sizeof(bin_record_hdr_t)) return -1;
    memcpy(hdr, r->raw + r->raw_off, sizeof(*hdr));
    size_t name_off = r->raw_off + sizeof(bin_record_hdr_t);
    if (r->raw_len - name_off < (size_t)hdr->name_len) return -1;
    if (name) *name = r->raw + name_off;
    r->raw_off = name_off + (size_t)hdr->name_len;
    return 1;
}
