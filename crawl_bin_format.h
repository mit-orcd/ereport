/*
 * On-disk layout for ecrawl uid_shard_*.bin shards (format version in header).
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_BIN_FORMAT_H
#define CRAWL_BIN_FORMAT_H

#include <stdint.h>
#include <string.h>

#define CRAWL_BIN_MAGIC "ERCBIN05"
#define CRAWL_BIN_MAGIC_LEN 8
#define CRAWL_BIN_FORMAT_VERSION 5u

/* Aliases used by ecrawl / readers */
#define FILE_MAGIC_LEN CRAWL_BIN_MAGIC_LEN
#define FORMAT_VERSION CRAWL_BIN_FORMAT_VERSION

/*
 * Shared "ctime-led" definition: a record is ctime-led when ctime exceeds
 * max(atime, mtime) by at least CTIME_LED_MIN_DELTA_SEC (180 days). Used by
 * ereport.c and the per-directory catalog rollups maintained by ecrawl.
 */
#define CTIME_LED_MIN_DELTA_SEC (180ULL * 86400ULL)

/*
 * Fixed file header (32 bytes). Records occupy [sizeof(bin_file_header_t), catalog_offset).
 * Catalog blob is [catalog_offset, EOF): uint64_t n_entries then packed bin_dir_catalog_entry_t rows.
 * catalog_offset == 0 means an incomplete shard (still being written); readers must reject.
 */
typedef struct __attribute__((packed)) {
    char magic[8];
    uint32_t version;
    uint32_t reserved;
    uint64_t catalog_offset;
    uint64_t reserved64;
} bin_file_header_t;

/*
 * Per-directory row in the catalog (directory identity is unique per shard).
 * dir_id is 1-based; parent_dir_id == 0 only for the synthetic root row (dir_id == 1).
 * name is a single path component (no slashes); root row uses name_len == 0.
 *
 * Aggregates cover **immediate child records** of this directory only — i.e. records
 * whose on-disk parent_dir_id equals this row's dir_id. Child directories' rollups
 * are not propagated up (deliberately; readers do that if needed). Aggregates are
 * scoped per shard (a shard only records entries for its uid bucket).
 *
 * imm_child_bytes follows ecrawl's accounting rules:
 *   - regular files: hardlink-aware credit (st_size on first inode visit, 0 on
 *     subsequent visits within this shard's writer thread)
 *   - dirs/symlinks/other: apparent st_size
 *
 * imm_child_min_eff_time / imm_child_max_eff_time use per-record effective time
 * = max(atime, mtime, ctime) (matches ereport's TIME_EFFECTIVE notion).
 * imm_child_min_eff_time == UINT64_MAX when imm_child_count == 0.
 *
 * imm_child_ctime_led_count counts records satisfying record_ctime_led() above.
 */
typedef struct __attribute__((packed)) {
    uint64_t dir_id;
    uint64_t parent_dir_id;
    uint32_t depth;
    uint16_t name_len;
    uint16_t reserved16;
    uint64_t imm_child_bytes;
    uint64_t imm_child_count;
    uint64_t imm_child_ctime_led_count;
    uint64_t imm_child_min_eff_time;
    uint64_t imm_child_max_eff_time;
    /* Followed by name_len UTF-8 bytes (component only). */
} bin_dir_catalog_entry_t;

typedef struct __attribute__((packed)) {
    uint64_t parent_dir_id;
    uint16_t name_len;
    uint8_t type;
    uint8_t reserved8;
    uint32_t mode;
    uint64_t uid;
    uint64_t gid;
    uint64_t size;
    uint64_t inode;
    uint32_t dev_major;
    uint32_t dev_minor;
    uint64_t nlink;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
} bin_record_hdr_t;

#define BIN_RECORD_HDR_FIXED_BYTES ((size_t)sizeof(bin_record_hdr_t))

static inline size_t crawl_bin_record_total_bytes(const bin_record_hdr_t *r) {
    return BIN_RECORD_HDR_FIXED_BYTES + (size_t)r->name_len;
}

static inline int crawl_bin_hdr_magic_ok(const char *magic, uint32_t version, uint32_t expect_ver) {
    return version == expect_ver &&
           memcmp(magic, CRAWL_BIN_MAGIC, (size_t)CRAWL_BIN_MAGIC_LEN) == 0;
}

/* Per-record effective time = max(atime, mtime, ctime). */
static inline uint64_t crawl_bin_record_eff_time(const bin_record_hdr_t *r) {
    uint64_t t = r->atime;
    if (r->mtime > t) t = r->mtime;
    if (r->ctime > t) t = r->ctime;
    return t;
}

/* Match ereport.c record_ctime_led(): ctime > max(atime,mtime) and the gap is >= CTIME_LED_MIN_DELTA_SEC. */
static inline int crawl_bin_record_ctime_led(const bin_record_hdr_t *r) {
    uint64_t tam = r->atime;
    if (r->mtime > tam) tam = r->mtime;
    if (r->ctime <= tam) return 0;
    return (r->ctime - tam) >= CTIME_LED_MIN_DELTA_SEC;
}

#endif /* CRAWL_BIN_FORMAT_H */
