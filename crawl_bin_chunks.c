/*
 * Shared uid-shard .bin chunking implementation.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#include "crawl_bin_chunks.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "crawl_bin_format.h"
#include "crawl_ckpt.h"

static int bin_ckpt_sidecar_path(const char *bin_path, char *out, size_t out_sz) {
    int n = snprintf(out, out_sz, "%s.ckpt", bin_path);
    return (n < 0 || (size_t)n >= out_sz) ? -1 : 0;
}

void crawl_bin_free_chunk_array_rows(crawl_bin_file_chunk_t *chunks, size_t count) {
    size_t j;

    for (j = 0; j < count; j++) free(chunks[j].path);
    free(chunks);
}

int crawl_bin_append_chunk(crawl_bin_file_chunk_t **chunks, size_t *count, size_t *cap, const char *path,
                           uint64_t start_offset, uint64_t end_offset, size_t file_index) {
    crawl_bin_file_chunk_t *tmp;

    if (*count == *cap) {
        size_t new_cap = (*cap == 0) ? 64 : (*cap * 2);
        tmp = (crawl_bin_file_chunk_t *)realloc(*chunks, new_cap * sizeof(*tmp));
        if (!tmp) return -1;
        *chunks = tmp;
        *cap = new_cap;
    }

    (*chunks)[*count].path = strdup(path);
    if (!(*chunks)[*count].path) return -1;
    (*chunks)[*count].start_offset = start_offset;
    (*chunks)[*count].end_offset = end_offset;
    (*chunks)[*count].file_index = file_index;
    (*count)++;
    return 0;
}

int crawl_bin_load_ckpt(const crawl_bin_chunk_stdio_t *io, const char *bin_path, uint64_t file_sz, uint64_t **offs_out,
                        size_t *n_out) {
    char ckpath[PATH_MAX];
    crawl_ckpt_file_hdr_t ch;
    uint64_t *buf = NULL;
    size_t i;
    FILE *fp;

    *offs_out = NULL;
    *n_out = 0;
    if (bin_ckpt_sidecar_path(bin_path, ckpath, sizeof(ckpath)) != 0) return -1;
    fp = io->fopen(ckpath, "rb");
    if (!fp) return -1;
    if (io->fread(&ch, sizeof(ch), 1, fp) != 1) {
        io->fclose(fp);
        errno = EINVAL;
        return -1;
    }
    if (memcmp(ch.magic, CRAWL_CKPT_MAGIC, CRAWL_CKPT_MAGIC_LEN) != 0 || ch.version != CRAWL_CKPT_ONDISK_VERSION ||
        ch.stride_bytes != CRAWL_CKPT_STRIDE_BYTES || ch.num_offsets == 0 || ch.num_offsets > (uint64_t)(SIZE_MAX / sizeof(uint64_t))) {
        io->fclose(fp);
        errno = EINVAL;
        return -1;
    }
    buf = (uint64_t *)malloc((size_t)ch.num_offsets * sizeof(*buf));
    if (!buf) {
        io->fclose(fp);
        return -1;
    }
    if (io->fread(buf, sizeof(uint64_t), (size_t)ch.num_offsets, fp) != (size_t)ch.num_offsets || io->fclose(fp) != 0) {
        free(buf);
        errno = EINVAL;
        return -1;
    }
    if (buf[0] != sizeof(bin_file_header_t)) {
        free(buf);
        errno = EINVAL;
        return -1;
    }
    for (i = 1; i < (size_t)ch.num_offsets; i++) {
        if (buf[i] <= buf[i - 1] || buf[i] > file_sz) {
            free(buf);
            errno = EINVAL;
            return -1;
        }
    }
    *offs_out = buf;
    *n_out = (size_t)ch.num_offsets;
    return 0;
}

int crawl_bin_build_chunks_for_segment(const crawl_bin_chunk_stdio_t *io, const char *path, size_t file_index,
                                       uint64_t chunk_target_bytes, uint64_t seg_start, uint64_t seg_end,
                                       crawl_bin_file_chunk_t **chunks_out, size_t *chunk_count_out,
                                       unsigned int *file_chunk_counter_out) {
    FILE *fp = NULL;
    uint64_t chunk_start;
    uint64_t next_target;
    crawl_bin_file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    size_t chunk_cap = 0;
    unsigned int fc = 0;
    int rc = -1;

    *chunks_out = NULL;
    *chunk_count_out = 0;
    *file_chunk_counter_out = 0;

    if (seg_start > seg_end) return -1;

    fp = io->fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }

    if (fseeko(fp, (off_t)seg_start, SEEK_SET) != 0) goto out;

    if (chunk_target_bytes == 0) chunk_target_bytes = CRAWL_BIN_PARSE_CHUNK_BYTES;
    chunk_start = seg_start;
    next_target = chunk_start + chunk_target_bytes;

    for (;;) {
        bin_record_hdr_t r;
        off_t record_start = ftello(fp);
        off_t record_end;

        if (record_start < 0) goto out;

        if ((uint64_t)record_start >= seg_end) {
            if ((uint64_t)record_start > seg_end) goto out;
            if ((uint64_t)record_start > chunk_start) {
                if (crawl_bin_append_chunk(&chunks, &chunk_count, &chunk_cap, path, chunk_start, (uint64_t)record_start,
                                           file_index) != 0)
                    goto out;
                fc++;
            }
            rc = 0;
            goto out;
        }

        if (io->fread(&r, sizeof(r), 1, fp) != 1) {
            if (feof(fp)) fprintf(stderr, "warn: unexpected EOF in segment of %s\n", path);
            goto out;
        }

        if (fseeko(fp, (off_t)r.path_len, SEEK_CUR) != 0) goto out;
        record_end = ftello(fp);
        if (record_end < 0) goto out;
        if ((uint64_t)record_end > seg_end) goto out;

        while ((uint64_t)record_end >= next_target) {
            if ((uint64_t)record_end > chunk_start) {
                if (crawl_bin_append_chunk(&chunks, &chunk_count, &chunk_cap, path, chunk_start, (uint64_t)record_end,
                                           file_index) != 0)
                    goto out;
                fc++;
            }
            chunk_start = (uint64_t)record_end;
            next_target = chunk_start + chunk_target_bytes;
        }
    }

out:
    if (fp) io->fclose(fp);
    if (rc != 0) {
        crawl_bin_free_chunk_array_rows(chunks, chunk_count);
        return -1;
    }
    *chunks_out = chunks;
    *chunk_count_out = chunk_count;
    *file_chunk_counter_out = fc;
    return 0;
}

static int chunk_list_take_all(crawl_bin_file_chunk_t **dst, size_t *dn, size_t *dcap, crawl_bin_file_chunk_t *src,
                               size_t sn) {
    size_t j;

    for (j = 0; j < sn; j++) {
        if (crawl_bin_append_chunk(dst, dn, dcap, src[j].path, src[j].start_offset, src[j].end_offset,
                                   src[j].file_index) != 0) {
            for (; j < sn; j++) free(src[j].path);
            free(src);
            return -1;
        }
        free(src[j].path);
    }
    free(src);
    return 0;
}

void *crawl_bin_chunk_bundle_worker_main(void *arg) {
    crawl_bin_chunk_bundle_t *b = (crawl_bin_chunk_bundle_t *)arg;
    int si;

    b->rc = 0;
    b->chunks = NULL;
    b->chunk_count = 0;
    b->chunk_cap = 0;
    b->fc = 0;

    for (si = b->seg_a; si < b->seg_b; si++) {
        crawl_bin_file_chunk_t *seg_chunks = NULL;
        size_t seg_count = 0;
        unsigned int seg_fc = 0;
        uint64_t lo = b->offs[si];
        uint64_t hi = ((size_t)si + 1U < b->n_offs) ? b->offs[(size_t)si + 1U] : b->file_size;

        if (crawl_bin_build_chunks_for_segment(&b->io, b->path, b->file_index, b->chunk_target_bytes, lo, hi, &seg_chunks,
                                               &seg_count, &seg_fc) != 0) {
            crawl_bin_free_chunk_array_rows(b->chunks, b->chunk_count);
            b->chunks = NULL;
            b->chunk_count = 0;
            b->rc = -1;
            if (b->tls_flush) b->tls_flush();
            return NULL;
        }
        if (chunk_list_take_all(&b->chunks, &b->chunk_count, &b->chunk_cap, seg_chunks, seg_count) != 0) {
            crawl_bin_free_chunk_array_rows(b->chunks, b->chunk_count);
            b->chunks = NULL;
            b->chunk_count = 0;
            b->rc = -1;
            if (b->tls_flush) b->tls_flush();
            return NULL;
        }
        b->fc += seg_fc;
    }
    if (b->tls_flush) b->tls_flush();
    return NULL;
}

int crawl_bin_build_chunks_for_file(const crawl_bin_chunk_stdio_t *io, void (*tls_flush)(void), const char *path,
                                    size_t file_index, uint64_t chunk_target_bytes, int chunk_workers,
                                    crawl_bin_file_chunk_t **chunks_out, size_t *chunk_count_out,
                                    unsigned int *file_chunk_counter_out) {
    struct stat st;
    bin_file_header_t fh;
    uint64_t *offs = NULL;
    size_t n_off = 0;
    uint64_t fsz;
    FILE *fp = NULL;
    int rc = -1;
    crawl_bin_file_chunk_t *chunks = NULL;
    size_t chunk_count = 0;
    unsigned int fc = 0;
    int nw;

    *chunks_out = NULL;
    *chunk_count_out = 0;
    *file_chunk_counter_out = 0;

    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) {
        fprintf(stderr, "warn: cannot stat %s: %s\n", path, strerror(errno));
        return -1;
    }
    fsz = (uint64_t)st.st_size;

    fp = io->fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "warn: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }
    if (io->fread(&fh, sizeof(fh), 1, fp) != 1) {
        fprintf(stderr, "warn: short read on header: %s\n", path);
        io->fclose(fp);
        return -1;
    }
    io->fclose(fp);
    fp = NULL;

    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
        fprintf(stderr, "warn: bad format/version in %s\n", path);
        return -1;
    }

    if (crawl_bin_load_ckpt(io, path, fsz, &offs, &n_off) != 0) {
        fprintf(stderr, "warn: missing or invalid checkpoint sidecar (.ckpt) for %s\n", path);
        return -1;
    }
    if (n_off == 0 || offs[0] != (uint64_t)sizeof(fh)) {
        fprintf(stderr, "warn: bad checkpoint offsets in %s\n", path);
        free(offs);
        return -1;
    }

    if (chunk_target_bytes == 0) chunk_target_bytes = CRAWL_BIN_PARSE_CHUNK_BYTES;

    nw = chunk_workers;
    if (nw < 1) nw = 1;

    {
        size_t n_seg = n_off;

        if (nw > (int)n_seg) nw = (int)n_seg;

        if (n_seg <= 1 || nw <= 1) {
            rc = crawl_bin_build_chunks_for_segment(io, path, file_index, chunk_target_bytes, offs[0], fsz, &chunks,
                                                    &chunk_count, &fc);
            free(offs);
            if (rc != 0) return -1;
            *chunks_out = chunks;
            *chunk_count_out = chunk_count;
            *file_chunk_counter_out = fc;
            return 0;
        }

        {
            crawl_bin_chunk_bundle_t *bundles = (crawl_bin_chunk_bundle_t *)calloc((size_t)nw, sizeof(*bundles));
            pthread_t *tids = (pthread_t *)calloc((size_t)nw, sizeof(*tids));
            int w, lo, base, rem;

            if (!bundles || !tids) {
                free(bundles);
                free(tids);
                free(offs);
                return -1;
            }
            lo = 0;
            base = (int)n_seg / nw;
            rem = (int)n_seg % nw;
            for (w = 0; w < nw; w++) {
                int cnt = base + (w < rem ? 1 : 0);
                bundles[w].path = path;
                bundles[w].file_index = file_index;
                bundles[w].chunk_target_bytes = chunk_target_bytes;
                bundles[w].offs = offs;
                bundles[w].n_offs = n_off;
                bundles[w].file_size = fsz;
                bundles[w].seg_a = lo;
                bundles[w].seg_b = lo + cnt;
                bundles[w].io = *io;
                bundles[w].tls_flush = tls_flush;
                lo += cnt;
            }
            for (w = 0; w < nw; w++) {
                if (pthread_create(&tids[w], NULL, crawl_bin_chunk_bundle_worker_main, &bundles[w]) != 0) {
                    int j;
                    for (j = 0; j < w; j++) pthread_join(tids[j], NULL);
                    for (j = w; j < nw; j++) {
                        crawl_bin_free_chunk_array_rows(bundles[j].chunks, bundles[j].chunk_count);
                        bundles[j].chunks = NULL;
                    }
                    free(bundles);
                    free(tids);
                    free(offs);
                    return -1;
                }
            }
            for (w = 0; w < nw; w++) pthread_join(tids[w], NULL);

            chunks = NULL;
            chunk_count = 0;
            {
                size_t cap = 0;
                fc = 0;
                rc = 0;
                for (w = 0; w < nw; w++) {
                    if (bundles[w].rc != 0) {
                        rc = -1;
                        break;
                    }
                    if (chunk_list_take_all(&chunks, &chunk_count, &cap, bundles[w].chunks, bundles[w].chunk_count) != 0) {
                        rc = -1;
                        break;
                    }
                    bundles[w].chunks = NULL;
                    bundles[w].chunk_count = 0;
                    fc += bundles[w].fc;
                }
                if (rc != 0) {
                    crawl_bin_free_chunk_array_rows(chunks, chunk_count);
                    for (w = 0; w < nw; w++) crawl_bin_free_chunk_array_rows(bundles[w].chunks, bundles[w].chunk_count);
                    free(bundles);
                    free(tids);
                    free(offs);
                    return -1;
                }
            }
            free(bundles);
            free(tids);
        }
    }

    free(offs);
    *chunks_out = chunks;
    *chunk_count_out = chunk_count;
    *file_chunk_counter_out = fc;
    return 0;
}
