/*
 * On-disk layout for ecrawl uid_shard_*.bin shards (format version in header).
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_BIN_FORMAT_H
#define CRAWL_BIN_FORMAT_H

#include <stdint.h>
#include <string.h>

#define CRAWL_BIN_MAGIC "ERCBIN08"
#define CRAWL_BIN_MAGIC_LEN 8
#define CRAWL_BIN_FORMAT_VERSION 8u

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
 * [sizeof(bin_file_header_t), catalog_offset) and, since format v8, holds a
 * sequence of columnar row groups (see bin_rowgroup_hdr_t below) rather than
 * row-major compressed blocks. Catalog blob is [catalog_offset, EOF): uint64_t
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

/* ---------------------------------------------------------------------------
 * v8 columnar record region
 *
 * The record region is a back-to-back sequence of row groups. Each row group is
 *
 *     bin_rowgroup_hdr_t
 *     bin_colchunk_hdr_t[column_count]      (the column directory)
 *     <column 0 payload> ... <column n-1 payload>
 *
 * laid out in the same order as the directory, so a reader that wants only two
 * columns reads the header plus directory and then seeks straight to those two
 * payloads. Row groups are self-describing and contiguous, so a reader walks
 * them with header reads alone and every .ckpt chunk boundary is a group
 * boundary.
 *
 * Why columnar: bin_record_hdr_t is ~75 bytes of pure metadata, so a row-major
 * layout forces `--size-gt` to decompress every timestamp and name to read two
 * fields. Column chunks also compress far better than interleaved records,
 * because each chunk is homogeneous (near-constant uid, narrow timestamp range,
 * nlink almost always 1).
 *
 * Every column chunk carries min/max, a zone map: a reader can prove no record
 * in the group can satisfy a predicate and seek past it without decompressing.
 * The zone map is inline rather than in a side file precisely so it cannot go
 * stale relative to the data it describes -- understating a range silently
 * drops query results.
 * ------------------------------------------------------------------------- */

typedef struct __attribute__((packed)) {
    uint32_t record_count;
    uint32_t column_count;
    /* Payload bytes following the column directory: a skipping reader seeks
     * sizeof(hdr) + column_count*sizeof(colhdr) + comp_bytes to reach the next
     * row group without decoding anything. */
    uint64_t comp_bytes;
    uint64_t raw_bytes; /* total decoded payload bytes (informational) */
    uint16_t type_mask; /* OR of crawl_bin_type_bit() over this row group */
    uint16_t reserved16;
    uint32_t reserved32;
} bin_rowgroup_hdr_t;

/* Logical columns. Values are on-disk identifiers: never renumber, only append. */
enum {
    CRAWL_COL_PARENT_DIR_ID = 0,
    CRAWL_COL_NAME_LEN = 1,
    CRAWL_COL_TYPE = 2,
    CRAWL_COL_UID = 3,
    CRAWL_COL_GID = 4,
    CRAWL_COL_MODE = 5,
    CRAWL_COL_SIZE = 6,
    CRAWL_COL_INODE = 7,
    CRAWL_COL_DEV_MAJOR = 8,
    CRAWL_COL_DEV_MINOR = 9,
    CRAWL_COL_NLINK = 10,
    CRAWL_COL_ATIME = 11,
    CRAWL_COL_MTIME = 12,
    CRAWL_COL_CTIME = 13,
    CRAWL_COL_NAME_BYTES = 14, /* concatenated name bytes; lengths are CRAWL_COL_NAME_LEN */
    CRAWL_COL__COUNT = 15
};

/*
 * Column encodings, chosen per chunk by the writer from the data it just saw.
 * All of them are followed by a zstd frame; the encoding describes what the
 * frame decompresses to.
 */
enum {
    CRAWL_ENC_RAW = 0,         /* record_count little-endian uint64 values */
    CRAWL_ENC_FOR_BITPACK = 1, /* (value - min_value) packed at bit_width bits each */
    CRAWL_ENC_RLE = 2,         /* (uint64 value, uint64 run_length) pairs */
    CRAWL_ENC_CONST = 3,       /* every value == min_value; payload is empty */
    CRAWL_ENC_BYTES = 4        /* opaque byte blob (CRAWL_COL_NAME_BYTES) */
};

typedef struct __attribute__((packed)) {
    uint8_t column_id;
    uint8_t encoding;
    uint8_t bit_width;  /* CRAWL_ENC_FOR_BITPACK only: bits per packed value, 1..64 */
    uint8_t reserved8;
    uint32_t comp_bytes; /* payload bytes on disk for this column */
    uint64_t raw_bytes;  /* decoded payload bytes */
    /* Zone map. For CRAWL_COL_NAME_BYTES these are 0 (a byte blob has no order). */
    uint64_t min_value;
    uint64_t max_value;
} bin_colchunk_hdr_t;

/* On-disk bytes of a whole row group, from its header alone. Lets a chunker or a
 * skipping reader step to the next group without decoding anything. */
static inline uint64_t crawl_bin_rowgroup_total_bytes(const bin_rowgroup_hdr_t *rg) {
    return (uint64_t)sizeof(bin_rowgroup_hdr_t) +
           (uint64_t)rg->column_count * (uint64_t)sizeof(bin_colchunk_hdr_t) + rg->comp_bytes;
}

/*
 * Stable bit per record type code, for bin_rowgroup_hdr_t.type_mask. The codes
 * are the find(1)-style letters ecrawl stores in bin_record_hdr_t.type and
 * ecrawl_analyze accepts for --type. Returns 0 for an unknown code, which the
 * writer treats as an error rather than recording an incomplete mask.
 */
static inline uint16_t crawl_bin_type_bit(uint8_t type) {
    static const char types[] = "fdlcbpso";
    const char *p = (type != 0U) ? strchr(types, (int)type) : NULL;

    return p ? (uint16_t)(1U << (unsigned)(p - types)) : 0U;
}

/*
 * Target uncompressed bytes accumulated per row group before it is flushed, and
 * a hard record cap so a shard of tiny names cannot build an unbounded buffer.
 * 1 MiB gives the column codecs enough runway to pay off (a few thousand records
 * is too short for run-length and frame-of-reference to matter) while keeping
 * per-writer memory modest: the writer holds one uint64 array per numeric column.
 */
#define CRAWL_BIN_ROWGROUP_RAW_TARGET (1u << 20)
#define CRAWL_BIN_ROWGROUP_MAX_RECORDS 65536u

/* Alias: ecrawl sizes its flush threshold from this. */
#define CRAWL_BIN_BLOCK_RAW_TARGET CRAWL_BIN_ROWGROUP_RAW_TARGET

/*
 * Per-directory row in the catalog (directory identity is unique per shard).
 * dir_id is 1-based; parent_dir_id == 0 only for the synthetic root row (dir_id == 1).
 * name is a single path component (no slashes); root row uses name_len == 0.
 *
 * imm_child_* aggregates cover **immediate child records** of this directory only
 * -- i.e. records whose on-disk parent_dir_id equals this row's dir_id. Aggregates
 * are scoped per shard (a shard only records entries for its uid bucket).
 *
 * imm_child_bytes follows ecrawl's accounting rules:
 *   - regular files: hardlink-aware credit (st_size on first inode visit, 0 on
 *     subsequent visits; the registry is process-wide, so dedup is global)
 *   - dirs/symlinks/other: apparent st_size
 *
 * imm_child_min_eff_time / imm_child_max_eff_time use per-record effective time
 * = max(atime, mtime, ctime) (matches ereport's TIME_EFFECTIVE notion).
 * imm_child_min_eff_time == UINT64_MAX when imm_child_count == 0.
 *
 * imm_child_ctime_led_count counts records satisfying record_ctime_led() above.
 *
 * The dfs_* / subtree_* fields are computed by a single O(directories) post-pass
 * at end of crawl (see crawl_bin_catalog_finalize). dir_id is assigned in crawl
 * arrival order and is referenced by every record's parent_dir_id, so it cannot
 * be renumbered without rewriting the whole record region. dfs_index is instead
 * a *permutation*: it gives each directory its position in DFS pre-order, which
 * makes "is X at or under D" the O(1) range test
 *
 *     dfs_index[D] <= dfs_index[X] < dfs_index[D] + dfs_subtree_dirs[D]
 *
 * with no per-shard bitmap and without touching a single record.
 *
 * subtree_bytes / subtree_count are imm_child_bytes / imm_child_count summed
 * over that same DFS range, so a subtree total is a lookup rather than a scan.
 * subtree_files / subtree_dirs / subtree_symlinks break that count down by
 * record type; everything else is count minus those three.
 *
 * All of the subtree_* sums cover records *under* the directory and exclude the
 * directory's own record, which by construction hangs off its parent and is
 * therefore counted in the parent's imm_child_bytes. self_bytes carries that one
 * record's byte credit on the directory's own row so a subtree query can add it
 * back and match `du -sb`, which counts the directory it was given.
 *
 * Note dfs_subtree_dirs counts *catalog* directories -- path components -- while
 * subtree_dirs counts directory *records* in this shard. They differ because a
 * shard is per-uid: a directory owned by another user still needs a catalog
 * entry here to give its children a path, but its record lives in that user's
 * shard.
 *
 * subtree_nlink_gt1_count is the number of records in the subtree with nlink > 1.
 * When it is zero, subtree_bytes provably equals what a full scan would compute.
 * When it is nonzero the two can differ, because crawl-time hardlink credit is
 * attributed to the first link visited anywhere in the tree while a scan dedups
 * within the queried subtree; readers must fall back to the scan for an exact
 * answer rather than presenting the rollup as exact.
 */
/*
 * flags bit: this shard holds this directory's *own* record, so self_bytes is
 * meaningful and the directory counts once toward a query rooted at it. A shard
 * is per-uid, so a directory owned by another user still gets a catalog row here
 * (its children need the path) with no record and no self bytes -- without this
 * bit a cross-uid subtree would count the root directory once per shard.
 */
#define CRAWL_DIR_FLAG_SELF_RECORD 0x0001u

typedef struct __attribute__((packed)) {
    uint64_t dir_id;
    uint64_t parent_dir_id;
    uint32_t depth;
    uint16_t name_len;
    uint16_t flags;
    uint64_t imm_child_bytes;
    uint64_t imm_child_count;
    uint64_t imm_child_ctime_led_count;
    uint64_t imm_child_min_eff_time;
    uint64_t imm_child_max_eff_time;
    /* v8: DFS ordering permutation and subtree rollups. */
    uint64_t dfs_index;
    uint64_t dfs_subtree_dirs;
    uint64_t subtree_bytes;
    uint64_t subtree_count;
    uint64_t subtree_nlink_gt1_count;
    uint64_t subtree_files;
    uint64_t subtree_dirs;
    uint64_t subtree_symlinks;
    uint64_t self_bytes;
    /* Followed by name_len UTF-8 bytes (component only). */
} bin_dir_catalog_entry_t;

/*
 * The record header. gid and mode are carried because columnar storage makes
 * them nearly free: both are extremely low cardinality, so their column chunks
 * encode to a handful of run-length pairs (often a single constant), which is
 * what makes ownership and permission predicates answerable from the capture
 * instead of a live walk. Row-major they would have cost 12 bytes on every
 * record.
 *
 * This struct is the in-memory record exchanged between the writer, the readers
 * and the row-reconstruction shim. It is no longer written to disk verbatim --
 * the row group stores each field in its own column chunk.
 */
typedef struct __attribute__((packed)) {
    uint64_t parent_dir_id;
    uint16_t name_len;
    uint8_t type;
    uint64_t uid;
    uint64_t gid;
    uint32_t mode;
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
