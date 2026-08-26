/*
 * crawl_result — open a crawl output directory: parse crawl_manifest.txt and
 * enumerate the finalized uid_shard_*.bin shards inside it.
 *
 * ereport and ereport_index each carry their own copy of this discovery logic
 * because they also filter shards by the requested uid. This module is the
 * uid-agnostic version (every shard, in shard order) used by ecrawl_mount;
 * those two can migrate onto it later.
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_RESULT_H
#define CRAWL_RESULT_H

#include <stddef.h>
#include <stdint.h>

/* One finalized shard file. Incomplete shards (catalog_offset == 0) are excluded. */
typedef struct crawl_result_shard {
    char *path;              /* full path, "<dir>/uid_shard_NNNN.bin" */
    uint32_t shard;          /* shard number parsed from the filename */
    uint64_t file_size;      /* physical size */
    uint64_t catalog_offset; /* end of the record region, start of the catalog */
} crawl_result_shard_t;

typedef struct crawl_result {
    char *dir;
    /* From crawl_manifest.txt; zero/NULL when the manifest is absent. */
    uint32_t format_version;
    uint32_t uid_shards; /* 0 unless "layout=uid_shards" was present */
    int uid_shard_digits;
    char *start_path;  /* filesystem root the crawl walked */
    char *record_root; /* crawl-time path rewrite, if the manifest has one (legacy: ecrawl --record-root was removed; relabel at read time with --path-rewrite) */

    crawl_result_shard_t *shards; /* ascending by shard number */
    size_t shard_count;

    /* Shards found on disk but not usable, for the caller to report. */
    size_t skipped_incomplete; /* catalog_offset == 0, still being written */
    size_t skipped_unreadable; /* bad magic/version, truncated, or open failed */
} crawl_result_t;

void crawl_result_init(crawl_result_t *cr);
void crawl_result_free(crawl_result_t *cr);

/*
 * Parse <dir>/crawl_manifest.txt (optional) and collect every finalized
 * uid_shard_*.bin. Returns 0 on success, -1 on error (message on stderr).
 * Succeeds with shard_count == 0 when the directory holds no usable shard;
 * callers decide whether that is fatal.
 */
int crawl_result_open(const char *dir, crawl_result_t *cr);

/*
 * The path the stored records are rooted at: record_root when the manifest
 * carries one (only crawls from before the ecrawl --record-root removal do),
 * else start_path, else NULL when no manifest was found.
 */
const char *crawl_result_stored_root(const crawl_result_t *cr);

#endif /* CRAWL_RESULT_H */
