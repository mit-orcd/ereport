#ifndef CRAWL_TRIJOURNAL_H
#define CRAWL_TRIJOURNAL_H

/*
 * Crawl-time trigram journal: one file per capture shard, written by ecrawl
 * (--trigram-journal DIR) alongside the run and consumed by
 * `ereport_index --make --journal-dir DIR` to skip capture parsing for that
 * shard.
 *
 * File layout (<journal_dir>/<shard basename>.tij):
 *
 *     trij_hdr_t                       (rewritten at finalize)
 *     shard basename                   (hdr.name_len bytes)
 *     trij_block_hdr_t + zstd frame    (repeated: one per block of entries)
 *
 * Each decompressed block is a sequence of entries:
 *
 *     varint path_len, path bytes, varint uid, u8 type,
 *     varint code_count, varint first_code, varint (gap-1) x (code_count-1)
 *
 * Codes are the sorted-unique 24-bit basename trigrams from trigram_extract.c,
 * so gaps are >= 1 and (gap-1) stays unsigned. Reads are strictly sequential.
 *
 * Publication contract: ecrawl writes <name>.tij.tmp with flags=0 and only
 * renames to <name>.tij after rewriting the header with TRIJ_FLAG_COMPLETE
 * plus the shard binding facts. A reader validates the five-fact contract
 * (basename, size, mtime, catalog_offset, catalog_entries — the
 * sidecar_shard_matches pattern) against the live shard; any mismatch, a
 * missing file, or a leftover .tmp means "fall back to parsing the capture".
 * Journals are a cache: reject, never repair.
 */

#include <stddef.h>
#include <stdint.h>

#include "crawl_bin_format.h"

#define TRIJ_FLAG_COMPLETE 1u

/* Uncompressed block assembly target; one zstd frame per block. */
#define TRIJ_BLOCK_TARGET_BYTES ((size_t)256 * 1024)

/* Sanity ceilings for decoding untrusted-ish input. */
#define TRIJ_MAX_PATH_BYTES (1u << 20)
#define TRIJ_MAX_CODES (1u << 20)

typedef struct __attribute__((packed)) {
    char magic[8]; /* CRAWL_TRIJ_MAGIC */
    uint32_t version; /* TRIJOURNAL_VERSION */
    uint32_t flags; /* TRIJ_FLAG_COMPLETE when published */
    uint64_t record_count;
    uint64_t shard_size;
    uint64_t shard_mtime_sec;
    uint64_t shard_mtime_nsec;
    uint64_t catalog_offset;
    uint64_t catalog_entries; /* the uint64 that opens the catalog blob */
    uint64_t max_dir_id;
    uint32_t name_len; /* shard basename bytes follow the header */
    uint32_t reserved32;
} trij_hdr_t;

typedef struct __attribute__((packed)) {
    uint32_t n_entries;
    uint32_t comp_len;
    uint32_t uncomp_len;
    uint32_t reserved;
} trij_block_hdr_t;

/* The facts finalize records and validation re-checks against the live shard. */
typedef struct {
    uint64_t shard_size;
    uint64_t shard_mtime_sec;
    uint64_t shard_mtime_nsec;
    uint64_t catalog_offset;
    uint64_t catalog_entries;
    uint64_t max_dir_id;
} trij_binding_t;

/* "<dir>/<base>.tij" (tmp=0) or "<dir>/<base>.tij.tmp" (tmp=1). -1 on overflow. */
int trij_journal_path(char *buf, size_t cap, const char *dir, const char *shard_basename, int tmp);

/* ---------------------------------------------------------------- writer */

typedef struct {
    int fd;
    char *path_tmp;
    char *path_final;
    uint32_t name_len; /* shard basename bytes, written after the header at create */
    uint64_t file_off; /* append position; all writes are pwrite at tracked offsets */
    uint64_t bytes_written; /* header + name + flushed blocks; informational */
    unsigned char *block; /* uncompressed assembly buffer */
    size_t block_cap;
    size_t block_len;
    uint32_t block_entries;
    uint64_t record_count;
    int failed; /* sticky: any I/O or alloc error voids this shard's journal */
} trij_writer_t;

/*
 * Create <dir>/<base>.tij.tmp and write the placeholder (flags=0) header.
 * Returns 0 on success, -1 on error.
 */
int trij_writer_create(trij_writer_t *w, const char *journal_dir, const char *shard_basename);

/* Append one entry. codes must be sorted-unique (trigram_extract_path output). */
int trij_writer_append(trij_writer_t *w, const char *path, size_t path_len, uint64_t uid,
                       uint8_t type, const uint32_t *codes, size_t code_count);

/*
 * fd lifecycle for callers holding many writers under an fd budget: the fd is
 * only needed while a block flushes (or at finalize), so it may be closed
 * between flushes and is reopened lazily. The in-memory block buffer survives.
 */
int trij_writer_reopen(trij_writer_t *w);
void trij_writer_close_fd(trij_writer_t *w);

/*
 * Flush the pending block, rewrite the header with TRIJ_FLAG_COMPLETE and the
 * binding facts, fdatasync, and rename to the published name. 0 on success.
 */
int trij_writer_finalize(trij_writer_t *w, const trij_binding_t *binding);

/* Close and unlink the .tmp without publishing. Idempotent. */
void trij_writer_abort(trij_writer_t *w);

/* ---------------------------------------------------------------- reader */

typedef struct {
    int fd;
    trij_hdr_t hdr;
    unsigned char *cbuf; /* compressed block */
    size_t cbuf_cap;
    unsigned char *ubuf; /* decompressed block */
    size_t ubuf_cap;
    size_t ubuf_len;
    size_t ubuf_pos;
    uint32_t block_entries_left;
    uint64_t entries_read;
    uint32_t *codes; /* decode buffer, valid until the next trij_reader_next */
    size_t codes_cap;
    int eof;
} trij_reader_t;

/*
 * Open <journal_dir>/<basename(shard_path)>.tij and validate it against the
 * live shard (magic, version, complete flag, basename, size, mtime,
 * catalog_offset, catalog_entries). Returns 1 when the journal is valid and
 * ready to replay, 0 when it is missing/stale/incomplete — the caller falls
 * back to parsing the capture. On 1, *hdr_out (if non-NULL) gets the header.
 */
int trij_reader_open_validate(trij_reader_t *r, const char *journal_dir, const char *shard_path,
                              trij_hdr_t *hdr_out);

/*
 * Next entry: 1 = the out-params are filled (pointers into reader storage,
 * valid until the next call), 0 = clean EOF, -1 = corrupt journal.
 */
int trij_reader_next(trij_reader_t *r, const char **path, size_t *path_len, uint64_t *uid,
                     uint8_t *type, const uint32_t **codes, size_t *code_count);

void trij_reader_close(trij_reader_t *r);

#endif /* CRAWL_TRIJOURNAL_H */
