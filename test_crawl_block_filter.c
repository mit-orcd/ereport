/*
 * Focused regression tests for the v8 row-group zone map, the group-skipping
 * reader built on it, and column projection.
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
    h.uid = 1000;
    h.gid = 100;
    h.mode = 0100644;
    h.nlink = 1;
    h.mtime = 1750000000ULL + size;
    return crawl_bin_block_writer_append_record(w, &h, name);
}

static int flush_group(crawl_bin_block_writer_t *w, FILE *fp, uint64_t *off) {
    uint64_t written = 0;

    if (crawl_bin_block_writer_flush(w, fp, fwrite, &written) != 0) return -1;
    *off += written;
    return 0;
}

/* Read a row group's header plus the directory entry for one column. */
static int read_group(const char *path, uint64_t off, bin_rowgroup_hdr_t *rg, int column_id,
                      bin_colchunk_hdr_t *col_out) {
    FILE *fp = fopen(path, "rb");
    uint32_t i;
    int found = 0;

    if (!fp) return -1;
    if (fseeko(fp, (off_t)off, SEEK_SET) != 0 || fread(rg, sizeof(*rg), 1, fp) != 1) {
        fclose(fp);
        return -1;
    }
    for (i = 0; i < rg->column_count; i++) {
        bin_colchunk_hdr_t ch;

        if (fread(&ch, sizeof(ch), 1, fp) != 1) {
            fclose(fp);
            return -1;
        }
        if ((int)ch.column_id == column_id) {
            *col_out = ch;
            found = 1;
        }
    }
    fclose(fp);
    return found ? 0 : -1;
}

/*
 * Scan [header, end) with the given predicate and check both the records it
 * yields and how much work it avoided. want_yielded counts records handed to the
 * caller (a skipped group yields none); want_matched counts those that actually
 * satisfy the predicate, which is what a query would report.
 */
static int check_filter(const char *path, uint64_t end, int have_size, uint64_t size_gt, int type,
                        uint64_t want_yielded, uint64_t want_matched, uint64_t want_decompressed,
                        uint64_t want_skipped_groups, uint64_t want_skipped_records) {
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
        r.blocks_decompressed != want_decompressed || r.blocks_skipped != want_skipped_groups ||
        r.records_skipped != want_skipped_records) {
        crawl_bin_block_reader_free(&r);
        fclose(fp);
        return -1;
    }
    crawl_bin_block_reader_free(&r);
    fclose(fp);
    return 0;
}

/* Every field must survive the column round trip, names included. */
static int check_roundtrip(const char *path, uint64_t end) {
    static const uint8_t types[] = {'f', 'd', 'l', 'c', 'b', 'p', 's', 'o'};
    static const uint64_t sizes[] = {500, 999, 10, 20, 30, 40, 50, 60};
    static const char *names[] = {"equal", "dir", "link", "char", "block", "fifo", "sock", "other"};
    crawl_bin_chunk_stdio_t io = {NULL, stdio_fread, NULL};
    crawl_bin_block_reader_t r;
    FILE *fp = fopen(path, "rb");
    size_t i = 0;
    int bad = 0;

    if (!fp) return -1;
    if (crawl_bin_block_reader_init(&r, &io, fp, sizeof(bin_file_header_t), end) != 0) {
        fclose(fp);
        return -1;
    }
    for (;;) {
        bin_record_hdr_t h;
        const unsigned char *name = NULL;

        if (crawl_bin_block_reader_next(&r, &h, &name) != 1) break;
        if (i >= sizeof(types) / sizeof(types[0])) {
            bad = 1;
            break;
        }
        if (h.type != types[i] || h.size != sizes[i] || h.parent_dir_id != 1ULL || h.uid != 1000ULL ||
            h.gid != 100ULL || h.mode != 0100644U || h.nlink != 1ULL || h.mtime != 1750000000ULL + sizes[i] ||
            h.name_len != (uint16_t)strlen(names[i]) || !name ||
            memcmp(name, names[i], h.name_len) != 0) {
            bad = 1;
            break;
        }
        i++;
    }
    crawl_bin_block_reader_free(&r);
    fclose(fp);
    return (!bad && i == sizeof(types) / sizeof(types[0])) ? 0 : -1;
}

/*
 * Projecting two columns must return the same values as a full scan while
 * actually reading less: if the reader silently decoded everything, the layout
 * would be correct but pointless.
 */
static int check_projection(const char *path, uint64_t end, uint64_t full_bytes) {
    crawl_bin_chunk_stdio_t io = {NULL, stdio_fread, NULL};
    crawl_bin_block_reader_t r;
    FILE *fp = fopen(path, "rb");
    uint64_t sum = 0;
    uint64_t n = 0;
    int rc = -1;

    if (!fp) return -1;
    if (crawl_bin_block_reader_init(&r, &io, fp, sizeof(bin_file_header_t), end) != 0) goto out;
    if (crawl_bin_block_reader_set_projection(&r, CRAWL_COL_BIT(CRAWL_COL_SIZE) | CRAWL_COL_BIT(CRAWL_COL_TYPE)) != 0)
        goto out;

    for (;;) {
        uint32_t recs = 0;
        const uint64_t *size_col;
        const uint64_t *type_col;
        uint32_t i;
        int grc = crawl_bin_block_reader_next_group(&r, &recs);

        if (grc == 0) break;
        if (grc < 0) goto out;
        size_col = crawl_bin_block_reader_column(&r, CRAWL_COL_SIZE);
        type_col = crawl_bin_block_reader_column(&r, CRAWL_COL_TYPE);
        if (!size_col || !type_col) goto out;
        /* An unprojected column must be reported absent rather than stale. */
        if (crawl_bin_block_reader_column(&r, CRAWL_COL_MTIME) != NULL) goto out;
        if (crawl_bin_block_reader_name(&r, 0, NULL) != NULL) goto out;
        for (i = 0; i < recs; i++) {
            sum += size_col[i];
            n++;
        }
    }

    if (n != 8ULL || sum != 500 + 999 + 10 + 20 + 30 + 40 + 50 + 60) goto out;
    if (r.column_bytes_skipped == 0ULL) goto out;
    if (r.column_bytes_read >= full_bytes) goto out;
    rc = 0;

out:
    crawl_bin_block_reader_free(&r);
    fclose(fp);
    return rc;
}

/* Compressed payload bytes a full-projection scan reads, as the baseline the
 * projected scan must beat. */
static int measure_full_read(const char *path, uint64_t end, uint64_t *out) {
    crawl_bin_chunk_stdio_t io = {NULL, stdio_fread, NULL};
    crawl_bin_block_reader_t r;
    FILE *fp = fopen(path, "rb");
    int rc = -1;

    if (!fp) return -1;
    if (crawl_bin_block_reader_init(&r, &io, fp, sizeof(bin_file_header_t), end) != 0) goto out;
    for (;;) {
        bin_record_hdr_t h;
        const unsigned char *name;
        int grc = crawl_bin_block_reader_next(&r, &h, &name);

        if (grc == 0) break;
        if (grc < 0) goto out;
    }
    *out = r.column_bytes_read;
    rc = 0;

out:
    crawl_bin_block_reader_free(&r);
    fclose(fp);
    return rc;
}

int main(void) {
    char path[] = "/tmp/test_crawl_block_filter.XXXXXX";
    int fd = mkstemp(path);
    FILE *fp;
    bin_file_header_t fh;
    bin_rowgroup_hdr_t rg;
    bin_colchunk_hdr_t col;
    crawl_bin_block_writer_t w;
    crawl_bin_block_reader_t r;
    crawl_bin_chunk_stdio_t io = {NULL, stdio_fread, NULL};
    uint64_t grp1 = sizeof(bin_file_header_t);
    uint64_t off = sizeof(bin_file_header_t);
    uint64_t grp2;
    uint64_t full_bytes = 0;
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

    /* Group 1 deliberately has a directory larger than its exactly-500-byte
     * file, so the size zone map stays conservative for an AND size/type query. */
    if (append_record(&w, 'f', 500, "equal") != 0 || append_record(&w, 'd', 999, "dir") != 0 ||
        flush_group(&w, fp, &off) != 0)
        return fail("first group failed");
    grp2 = off;
    if (append_record(&w, 'l', 10, "link") != 0 || append_record(&w, 'c', 20, "char") != 0 ||
        append_record(&w, 'b', 30, "block") != 0 || append_record(&w, 'p', 40, "fifo") != 0 ||
        append_record(&w, 's', 50, "sock") != 0 || append_record(&w, 'o', 60, "other") != 0 ||
        flush_group(&w, fp, &off) != 0)
        return fail("second group failed");
    crawl_bin_block_writer_free(&w);
    if (fclose(fp) != 0) return fail("bin close failed");

    /* The zone map must cover every record in its own group. */
    if (read_group(path, grp1, &rg, CRAWL_COL_SIZE, &col) != 0) return fail("could not read first group");
    if (col.min_value != 500ULL || col.max_value != 999ULL || rg.record_count != 2U || rg.reserved16 != 0U ||
        rg.column_count == 0U ||
        rg.type_mask != (uint16_t)(crawl_bin_type_bit('f') | crawl_bin_type_bit('d')))
        return fail("wrong first group summary");
    if (read_group(path, grp2, &rg, CRAWL_COL_SIZE, &col) != 0) return fail("could not read second group");
    if (col.min_value != 10ULL || col.max_value != 60ULL || rg.record_count != 6U ||
        rg.type_mask != (uint16_t)(crawl_bin_type_bit('l') | crawl_bin_type_bit('c') | crawl_bin_type_bit('b') |
                                   crawl_bin_type_bit('p') | crawl_bin_type_bit('s') | crawl_bin_type_bit('o')))
        return fail("wrong second group summary");

    /* uid is constant across the shard, which is the case the CONST encoding
     * exists for; it must still report a correct zone map. */
    if (read_group(path, grp2, &rg, CRAWL_COL_UID, &col) != 0) return fail("could not read uid column");
    if (col.encoding != CRAWL_ENC_CONST || col.min_value != 1000ULL || col.max_value != 1000ULL ||
        col.comp_bytes != 0U)
        return fail("constant uid column was not stored as CONST");

    if (check_roundtrip(path, off) != 0) return fail("record round trip failed");

    if (check_filter(path, off, 0, 0, 0, 8, 8, 2, 0, 0) != 0) return fail("unfiltered scan failed");
    if (check_filter(path, off, 1, 1000, 'f', 0, 0, 0, 2, 8) != 0) return fail("all-group size skip failed");
    if (check_filter(path, off, 0, 0, 'l', 6, 1, 1, 1, 2) != 0) return fail("type-mask skip failed");
    if (check_filter(path, off, 1, 500, 'f', 2, 0, 1, 1, 6) != 0)
        return fail("strict greater-than boundary failed");
    if (check_filter(path, off, 1, 0, 0, 8, 8, 2, 0, 0) != 0) return fail("size_gt 0 skipped a group");

    if (measure_full_read(path, off, &full_bytes) != 0 || full_bytes == 0ULL)
        return fail("could not measure full-projection read");
    if (check_projection(path, off, full_bytes) != 0) return fail("column projection failed");

    /* A predicate with no usable term must leave the reader unfiltered. */
    fp = fopen(path, "rb");
    if (!fp) return fail("reopen failed");
    if (crawl_bin_block_reader_init(&r, &io, fp, grp1, off) != 0) return fail("reader init failed");
    if (crawl_bin_block_reader_set_filter(&r, 0, 0, 0) == 0) return fail("empty predicate was accepted");
    if (crawl_bin_block_reader_set_filter(&r, 0, 0, 'z') == 0) return fail("unknown type was accepted");
    crawl_bin_block_reader_free(&r);
    fclose(fp);

    /* A zeroed type_mask means a damaged header, so the reader must decode the
     * group rather than trust it and drop records. */
    fp = fopen(path, "r+b");
    if (!fp) return fail("reopen for corruption failed");
    if (fseeko(fp, (off_t)(grp1 + offsetof(bin_rowgroup_hdr_t, type_mask)), SEEK_SET) != 0 ||
        fwrite(&zero16, sizeof(zero16), 1, fp) != 1 || fclose(fp) != 0)
        return fail("could not zero first group type mask");
    if (check_filter(path, off, 0, 0, 'l', 8, 1, 2, 0, 0) != 0) return fail("damaged type mask was trusted");

    /*
     * record_count is load-bearing: every column decodes to exactly that many
     * values, so the payload is not self-delimiting and a zeroed count cannot be
     * tolerated. It must be rejected outright rather than silently yielding an
     * empty group, which would look like a shard with no records.
     */
    fp = fopen(path, "r+b");
    if (!fp) return fail("reopen for count corruption failed");
    if (fseeko(fp, (off_t)(grp1 + offsetof(bin_rowgroup_hdr_t, record_count)), SEEK_SET) != 0 ||
        fwrite(&zero32, sizeof(zero32), 1, fp) != 1 || fclose(fp) != 0)
        return fail("could not zero first group record count");
    {
        crawl_bin_block_reader_t rr;
        bin_record_hdr_t h;
        const unsigned char *nm;
        int saw_error = 0;

        fp = fopen(path, "rb");
        if (!fp) return fail("reopen after count corruption failed");
        if (crawl_bin_block_reader_init(&rr, &io, fp, sizeof(bin_file_header_t), off) != 0)
            return fail("reader init after count corruption failed");
        for (;;) {
            int rc2 = crawl_bin_block_reader_next(&rr, &h, &nm);

            if (rc2 == 0) break;
            if (rc2 < 0) {
                saw_error = 1;
                break;
            }
        }
        crawl_bin_block_reader_free(&rr);
        fclose(fp);
        if (!saw_error) return fail("zeroed record count was accepted");
    }

    unlink(path);
    puts("test_crawl_block_filter: ok");
    return 0;
}
