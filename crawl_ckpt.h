/*
 * Crawl shard checkpoint sidecar (ecrawl v3+).
 * Written next to each uid_shard_*.bin as uid_shard_*.bin.ckpt
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_CKPT_H
#define CRAWL_CKPT_H

#include <stdint.h>

#include "crawl_bin_format.h"

#define CRAWL_CKPT_MAGIC "ERCCKPT\0"
#define CRAWL_CKPT_MAGIC_LEN 8
#define CRAWL_CKPT_ONDISK_VERSION 1u
#define CRAWL_CKPT_STRIDE_BYTES (32ULL << 20)

typedef struct __attribute__((packed)) {
    char magic[8];
    uint32_t version;
    uint32_t reserved;
    uint64_t stride_bytes;
    uint64_t num_offsets;
} crawl_ckpt_file_hdr_t;

#endif /* CRAWL_CKPT_H */
