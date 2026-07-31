/*
 * On-disk layout for ecrawl uid_shard_*.bin shards (format version in header).
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_BIN_FORMAT_H
#define CRAWL_BIN_FORMAT_H

#include <stdint.h>
#include <string.h>

#define CRAWL_BIN_MAGIC "ERCBIN07"
#define CRAWL_BIN_MAGIC_LEN 8
#define CRAWL_BIN_FORMAT_VERSION 7u

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
 * Fixed file header (32 bytes). The record region occupies
 * [sizeof(bin_file_header_t), catalog_offset) and, since format v6, holds a
 * sequence of independently zstd-compressed blocks (see bin_block_hdr_t below)
 * rather than raw records. Catalog blob is [catalog_offset, EOF): uint64_t
 * n_entries then packed bin_dir_catalog_entry_t rows.
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
 * v7 record-region block frame. The record region is a back-to-back sequence of
 * blocks; each block is this 24-byte header followed by comp_size bytes of a zstd
 * frame that decompresses to raw_size bytes. The decompressed payload is a
 * concatenation of whole records (bin_record_hdr_t + name_len name bytes); a
 * record never spans a block boundary. Blocks are self-describing and
 * contiguous, so a reader walks them with header reads alone (no side index),
 * and chunk boundaries for parallel workers are always block boundaries.
 *
 * v7 adds a summary of the records inside the frame, so a reader can prove a
 * block cannot satisfy a size/type predicate and seek past the frame instead of
 * decompressing it (see crawl_bin_block_reader_set_filter). The summary is
 * inline rather than in a side file precisely so it cannot go stale relative to
 * the frame it describes: max_record_size and type_mask must cover *every*
 * record in the block, since understating either silently drops query results.
 * record_count lets a skipping reader still report an accurate scanned-record
 * total. 16 bytes per ~256 KiB block is ~0.006% overhead.
 */
typedef struct __attribute__((packed)) {
    uint32_t raw_size;
    uint32_t comp_size;
    uint64_t max_record_size; /* max bin_record_hdr_t.size in this block */
    uint32_t record_count;    /* records in this block */
    uint16_t type_mask;       /* OR of crawl_bin_type_bit() over this block */
    uint16_t reserved16;
} bin_block_hdr_t;

/*
 * Stable bit per record type code, for bin_block_hdr_t.type_mask. The codes are
 * the find(1)-style letters ecrawl stores in bin_record_hdr_t.type and
 * ecrawl_analyze accepts for --type. Returns 0 for an unknown code, which the
 * writer treats as an error rather than recording an incomplete mask.
 */
static inline uint16_t crawl_bin_type_bit(uint8_t type) {
    static const char types[] = "fdlcbpso";
    const char *p = (type != 0U) ? strchr(types, (int)type) : NULL;

    return p ? (uint16_t)(1U << (unsigned)(p - types)) : 0U;
}

/* Target uncompressed bytes accumulated per block before it is flushed. 256 KiB
 * keeps per-open-shard writer memory modest (raw + compressed scratch) while
 * still giving metadata excellent compression. */
#define CRAWL_BIN_BLOCK_RAW_TARGET (1u << 18)

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

/*
 * v6 lean record header (75 bytes packed). Compared with v5 this drops the
 * never-read gid (8) and mode (4) fields and the unused reserved8 pad (1):
 * readers derive file kind from `type`, and no reader consumed gid/mode. The
 * remaining fields are all still consumed (path, uid filter, size/time rollups,
 * and inode/dev/nlink for hardlink-aware byte credit).
 */
typedef struct __attribute__((packed)) {
    uint64_t parent_dir_id;
    uint16_t name_len;
    uint8_t type;
    uint64_t uid;
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
