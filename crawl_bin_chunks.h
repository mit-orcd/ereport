/*
 * Shared uid-shard .bin chunking: read checkpoint sidecar, split shard byte ranges for parallel workers.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */
#ifndef CRAWL_BIN_CHUNKS_H
#define CRAWL_BIN_CHUNKS_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/* Default target bytes per parse chunk when caller passes 0 (matches legacy PARSE_CHUNK_BYTES). */
#define CRAWL_BIN_PARSE_CHUNK_BYTES (32ULL << 20)

typedef struct crawl_bin_file_chunk {
    char *path;
    uint64_t start_offset;
    uint64_t end_offset;
    size_t file_index;
} crawl_bin_file_chunk_t;

typedef struct crawl_bin_chunk_stdio {
    FILE *(*fopen)(const char *path, const char *mode);
    size_t (*fread)(void *ptr, size_t size, size_t nmemb, FILE *stream);
    int (*fclose)(FILE *stream);
} crawl_bin_chunk_stdio_t;

void crawl_bin_free_chunk_array_rows(crawl_bin_file_chunk_t *chunks, size_t count);

int crawl_bin_append_chunk(crawl_bin_file_chunk_t **chunks, size_t *count, size_t *cap, const char *path,
                           uint64_t start_offset, uint64_t end_offset, size_t file_index);

int crawl_bin_load_ckpt(const crawl_bin_chunk_stdio_t *io, const char *bin_path, uint64_t file_sz, uint64_t **offs_out,
                        size_t *n_out);

int crawl_bin_build_chunks_for_segment(const crawl_bin_chunk_stdio_t *io, const char *path, size_t file_index,
                                       uint64_t chunk_target_bytes, uint64_t seg_start, uint64_t seg_end,
                                       crawl_bin_file_chunk_t **chunks_out, size_t *chunk_count_out,
                                       unsigned int *file_chunk_counter_out);

typedef struct crawl_bin_chunk_bundle {
    const char *path;
    size_t file_index;
    uint64_t chunk_target_bytes;
    const uint64_t *offs;
    size_t n_offs;
    uint64_t file_size;
    int seg_a;
    int seg_b;
    crawl_bin_file_chunk_t *chunks;
    size_t chunk_count;
    size_t chunk_cap;
    unsigned int fc;
    int rc;
    crawl_bin_chunk_stdio_t io;
    void (*tls_flush)(void);
} crawl_bin_chunk_bundle_t;

void *crawl_bin_chunk_bundle_worker_main(void *arg);

int crawl_bin_build_chunks_for_file(const crawl_bin_chunk_stdio_t *io, void (*tls_flush)(void), const char *path,
                                    size_t file_index, uint64_t chunk_target_bytes, int chunk_workers,
                                    crawl_bin_file_chunk_t **chunks_out, size_t *chunk_count_out,
                                    unsigned int *file_chunk_counter_out);

#endif /* CRAWL_BIN_CHUNKS_H */
