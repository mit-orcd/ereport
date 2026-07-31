/*
 * Focused regression tests for the v7 block header summary and the
 * block-skipping reader built on it.
 *
 * SPDX-License-Identifier: MIT
 */
#include "crawl_bin_block.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static size_t stdio_fread(void *p, size_t s, size_t n, FILE *fp) {
    return fread(p, s, n, fp);
}

static int fail(const char *what) {
    fprintf(stderr, "test_crawl_block_filter: %s\n", what);
    return 1;
}

static int append_record(crawl_bin_block_writer_t *w, uint8_t type, uint64_t size, const char *name) {
    bin_record_hdr_t h;

    memset(&h, 0, sizeof(h));
    h.parent_dir_id = 1;
    h.name_len = (uint16_t)strlen(name);
    h.type = type;
    h.size = size;
    return crawl_bin_block_writer_append_record(w, &h, name);
}

static int flush_block(crawl_bin_block_writer_t *w, FILE *fp, uint64_t *off) {
    uint64_t written = 0;

    if (crawl_bin_block_writer_flush(w, fp, fwrite, &written) != 0) return -1;
    *off += written;
    return 0;
}

static int read_block_hdr(const char *path, uint64_t off, bin_block_hdr_t *bh) {
    FILE *fp = fopen(path, "rb");
    int ok;

    if (!fp) return -1;
    ok = fseeko(fp, (off_t)off, SEEK_SET) == 0 && fread(bh, sizeof(*bh), 1, fp) == 1;
    fclose(fp);
    return ok ? 0 : -1;
}

/*
 * Scan [header, end) with the given predicate and check both the records it
 * yields and how much work it avoided. want_yielded counts records handed to the
 * caller (a skipped block yields none); want_matched counts those that actually
 * satisfy the predicate, which is what a query would report.
 */
static int check_filter(const char *path, uint64_t end, int have_size, uint64_t size_gt, int type,
                        uint64_t want_yielded, uint64_t want_matched, uint64_t want_decompressed,
                        uint64_t want_skipped_blocks, uint64_t want_skipped_records) {
    crawl_bin_chunk_stdio_t io = {NULL, stdio_fread, NULL};
    crawl_bin_block_reader_t r;
    FILE *fp = fopen(path, "rb");
    uint64_t yielded = 0;
    uint64_t matched = 0;
    int rc;

    if (!fp) return -1;
    if (crawl_bin_block_reader_init(&r, &io, fp, sizeof(bin_file_header_t), end) != 0) {
        fclose(fp);
        return -1;
    }
    if ((have_size || type) && crawl_bin_block_reader_set_filter(&r, have_size, size_gt, type) != 0) {
        crawl_bin_block_reader_free(&r);
        fclose(fp);
        return -1;
    }
    for (;;) {
        bin_record_hdr_t h;
        const unsigned char *name;

        rc = crawl_bin_block_reader_next(&r, &h, &name);
        (void)name;
        if (rc <= 0) break;
        yielded++;
        if ((!have_size || h.size > size_gt) && (!type || h.type == (uint8_t)type)) matched++;
    }
    if (rc != 0 || yielded != want_yielded || matched != want_matched ||
        r.blocks_decompressed != want_decompressed || r.blocks_skipped != want_skipped_blocks ||
        r.records_skipped != want_skipped_records) {
        crawl_bin_block_reader_free(&r);
        fclose(fp);
        return -1;
    }
    crawl_bin_block_reader_free(&r);
    fclose(fp);
    return 0;
}

int main(void) {
    char path[] = "/tmp/test_crawl_block_filter.XXXXXX";
    int fd = mkstemp(path);
    FILE *fp;
    bin_file_header_t fh;
    bin_block_hdr_t bh;
    crawl_bin_block_writer_t w;
    crawl_bin_block_reader_t r;
    crawl_bin_chunk_stdio_t io = {NULL, stdio_fread, NULL};
    uint64_t blk1 = sizeof(bin_file_header_t);
    uint64_t off = sizeof(bin_file_header_t);
    uint64_t blk2;
    uint16_t zero16 = 0;
    uint32_t zero32 = 0;

    if (fd < 0) return fail("mkstemp failed");
    fp = fdopen(fd, "wb+");
    if (!fp) {
        close(fd);
        unlink(path);
        return fail("fdopen failed");
    }
    memset(&fh, 0, sizeof(fh));
    if (fwrite(&fh, sizeof(fh), 1, fp) != 1) return fail("header write failed");
    if (crawl_bin_block_writer_init(&w) != 0) return fail("writer init failed");

    /* Block 1 deliberately has a directory larger than its exactly-500-byte
     * file, so max_record_size stays conservative for an AND size/type query. */
    if (append_record(&w, 'f', 500, "equal") != 0 || append_record(&w, 'd', 999, "dir") != 0 ||
        flush_block(&w, fp, &off) != 0)
        return fail("first block failed");
    blk2 = off;
    if (append_record(&w, 'l', 10, "link") != 0 || append_record(&w, 'c', 20, "char") != 0 ||
        append_record(&w, 'b', 30, "block") != 0 || append_record(&w, 'p', 40, "fifo") != 0 ||
        append_record(&w, 's', 50, "sock") != 0 || append_record(&w, 'o', 60, "other") != 0 ||
        flush_block(&w, fp, &off) != 0)
        return fail("second block failed");
    crawl_bin_block_writer_free(&w);
    if (fclose(fp) != 0) return fail("bin close failed");

    /* The summary must cover every record in its own block. */
    if (read_block_hdr(path, blk1, &bh) != 0) return fail("could not read first block header");
    if (bh.max_record_size != 999ULL || bh.record_count != 2U || bh.reserved16 != 0U ||
        bh.type_mask != (uint16_t)(crawl_bin_type_bit('f') | crawl_bin_type_bit('d')))
        return fail("wrong first block summary");
    if (read_block_hdr(path, blk2, &bh) != 0) return fail("could not read second block header");
    if (bh.max_record_size != 60ULL || bh.record_count != 6U ||
        bh.type_mask != (uint16_t)(crawl_bin_type_bit('l') | crawl_bin_type_bit('c') | crawl_bin_type_bit('b') |
                                   crawl_bin_type_bit('p') | crawl_bin_type_bit('s') | crawl_bin_type_bit('o')))
        return fail("wrong second block summary");

    if (check_filter(path, off, 0, 0, 0, 8, 8, 2, 0, 0) != 0) return fail("unfiltered scan failed");
    if (check_filter(path, off, 1, 1000, 'f', 0, 0, 0, 2, 8) != 0) return fail("all-block size skip failed");
    if (check_filter(path, off, 0, 0, 'l', 6, 1, 1, 1, 2) != 0) return fail("type-mask skip failed");
    if (check_filter(path, off, 1, 500, 'f', 2, 0, 1, 1, 6) != 0)
        return fail("strict greater-than boundary failed");
    if (check_filter(path, off, 1, 0, 0, 8, 8, 2, 0, 0) != 0) return fail("size_gt 0 skipped a block");

    /* A predicate with no usable term must leave the reader unfiltered. */
    fp = fopen(path, "rb");
    if (!fp) return fail("reopen failed");
    if (crawl_bin_block_reader_init(&r, &io, fp, blk1, off) != 0) return fail("reader init failed");
    if (crawl_bin_block_reader_set_filter(&r, 0, 0, 0) == 0) return fail("empty predicate was accepted");
    if (crawl_bin_block_reader_set_filter(&r, 0, 0, 'z') == 0) return fail("unknown type was accepted");
    crawl_bin_block_reader_free(&r);
    fclose(fp);

    /* A zeroed summary means a damaged header, so the reader must decompress the
     * block rather than trust it and drop records. */
    fp = fopen(path, "r+b");
    if (!fp) return fail("reopen for corruption failed");
    if (fseeko(fp, (off_t)(blk1 + offsetof(bin_block_hdr_t, record_count)), SEEK_SET) != 0 ||
        fwrite(&zero32, sizeof(zero32), 1, fp) != 1 || fwrite(&zero16, sizeof(zero16), 1, fp) != 1 ||
        fclose(fp) != 0)
        return fail("could not zero first block summary");
    if (check_filter(path, off, 0, 0, 'l', 8, 1, 2, 0, 0) != 0) return fail("damaged summary was trusted");

    unlink(path);
    puts("test_crawl_block_filter: ok");
    return 0;
}
