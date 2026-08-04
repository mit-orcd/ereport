/*
 * Reader for the dir-index sidecars `ereport_index --make` writes: dirs.idx
 * (EDIRX001) and rowgroups.idx (ERGIX001). See crawl_bin_format.h for the
 * layouts.
 *
 * Shared by ecrawl_query and ereport. Both need the same three things from a
 * sidecar -- prove it still describes the shards in hand, turn a directory path
 * into a dir_id and its catalog row, and decide which row groups a subtree
 * query can skip -- and a second copy of a format reader is how the two would
 * come to disagree about what a byte means.
 *
 * Two rules govern everything here:
 *
 *   - Verify, do not trust. A hash hit names a candidate dir_id; the answer is
 *     accepted only after the row's parent chain has been walked back into a
 *     path that compares byte-equal to the query. A 64-bit collision costs a
 *     wasted read, never a wrong answer. Every offset is bounds-checked against
 *     the mapping before it is followed.
 *   - Degrade, do not fail. A missing file, a bad header, or a shard whose
 *     size, mtime, catalog offset or catalog entry count has moved since the
 *     index was built retires the sidecar for the whole run, and the caller
 *     takes the route it took before sidecars existed.
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_SIDECAR_H
#define CRAWL_SIDECAR_H

#include <stddef.h>
#include <stdint.h>

#include "crawl_bin_catalog.h"
#include "crawl_bin_chunks.h"
#include "crawl_bin_format.h"

/*
 * Longest path this reader will rebuild. Capped by the catalog's component
 * limit, not by PATH_MAX: 128 components of up to 255 bytes do not have to have
 * existed on any one filesystem to be in a bin.
 */
#define CRAWL_SIDECAR_PATH_MAX 65536U

/* One shard's slice of dirs.idx, resolved once and passed around by value. */
typedef struct {
    const crawl_dirx_entry_t *ents; /* sorted by path_hash */
    uint64_t ent_count;
    const uint64_t *rows; /* row_offset[dir_id], slot 0 unused */
    uint64_t max_dir_id;
    uint64_t shard_size;
    uint64_t catalog_entries;
    int fd; /* the shard, open read-only, for the row preads */
} crawl_dirx_view_t;

/* One shard's slice of rowgroups.idx. */
typedef struct {
    const crawl_rgix_group_t *groups;
    uint64_t group_count;
    uint64_t dfs_domain;
} crawl_rgix_view_t;

/*
 * Both sidecars, mapped and bound to the caller's shard list.
 *
 * Indexing is the caller's: dirs[i] and groups[i] describe shard_paths[i],
 * whatever order the sidecar itself stores its shards in. dirs.idx is required;
 * rowgroups.idx is an extra that only scan pruning uses, so a run keeps the
 * first when the second is missing (have_groups == 0).
 */
typedef struct {
    unsigned char *dmap;
    size_t dlen;
    unsigned char *rmap;
    size_t rlen;
    crawl_dirx_view_t *dirs;
    crawl_rgix_view_t *groups;
    int *fd;
    size_t shard_count;
    int have_dirs;
    int have_groups;
} crawl_sidecar_t;

/*
 * Scratch for one path rebuild. ~100 KB, so heap-allocate it once per thread
 * rather than putting it on a stack.
 */
typedef struct {
    unsigned char comp[CRAWL_BIN_CATALOG_MAX_PATH_PARTS][256];
    size_t clen[CRAWL_BIN_CATALOG_MAX_PATH_PARTS];
    char path[CRAWL_SIDECAR_PATH_MAX];
} crawl_dirx_walk_t;

/* Where one shard's subtree sits, resolved once per shard. */
typedef struct {
    uint64_t root;        /* dir_id of the subtree root in this shard; 0 when absent */
    uint64_t root_parent; /* the root row's parent_dir_id; 0 when root is absent */
    uint64_t dfs_lo;      /* the subtree's half-open DFS range; empty when lo >= hi */
    uint64_t dfs_hi;
    uint64_t dfs_par; /* the parent's DFS position, for the subtree's own record */
    int have_par;
    int self_record; /* CRAWL_DIR_FLAG_SELF_RECORD: this shard holds the root's own record */
    int in_shard;    /* the shard can hold something: root or parent resolved */
} crawl_sidecar_scope_t;

/*
 * How many row groups each sketch would have kept. Both are recorded because
 * which one earns its bytes is an empirical question: dir_id follows crawl
 * arrival order, so the interval can be very loose, but 128 bytes a group is
 * eight times what the interval costs.
 */
typedef struct {
    uint64_t total;
    uint64_t kept;          /* both tests agreed the group may match */
    uint64_t kept_interval; /* the interval alone would have kept this many */
    uint64_t kept_bitmap;   /* the bitmap alone would have kept this many */
    uint64_t bytes_total;
    uint64_t bytes_kept;
    uint64_t records_total; /* records in every group, pruned or not */
    uint64_t records_kept;
    int used;
} crawl_rgix_prune_stats_t;

/*
 * Map both sidecars and bind them to shard_paths[0..shard_count).
 *
 * Every named shard must appear in dirs.idx and pass its identity check;
 * anything else retires the sidecar. Returns 0 with sc->have_dirs set, or -1
 * with nothing mapped.
 */
int crawl_sidecar_open(const char *index_dir, const char *const *shard_paths, size_t shard_count,
                       crawl_sidecar_t *sc);
void crawl_sidecar_close(crawl_sidecar_t *sc);

crawl_dirx_walk_t *crawl_dirx_walk_new(void);

/*
 * Read one catalog row plus its name with a single pread. rows_read, when
 * non-NULL, is incremented per row actually read, which is what the diagnostics
 * report as directories_examined.
 */
int crawl_dirx_read_row(const crawl_dirx_view_t *v, uint64_t did, bin_dir_catalog_entry_t *ent,
                        unsigned char *name, size_t name_cap, size_t *name_len_out, uint64_t *rows_read);

/* Rebuild dir_id's stored path from its parent chain, one row read per level. */
int crawl_dirx_path_of(const crawl_dirx_view_t *v, uint64_t did, crawl_dirx_walk_t *w, char *out, size_t out_sz,
                       size_t *len_out, uint64_t *rows_read);

/* The dir_id in this shard whose stored path is exactly `path`, or 0. */
uint64_t crawl_dirx_lookup(const crawl_dirx_view_t *v, const char *path, size_t plen, crawl_dirx_walk_t *w,
                           uint64_t *rows_read);

/*
 * Locate `subtree` in every shard, filling scope[0..sc->shard_count).
 * sub_parent is the subtree's parent directory ("" when that is the capture
 * root). Returns 0, or -1 when a row the sidecar promised could not be read --
 * which means the sidecar is not usable and the caller must fall back.
 */
int crawl_sidecar_scope_subtree(const crawl_sidecar_t *sc, const char *subtree, size_t subtree_len,
                                const char *sub_parent, crawl_sidecar_scope_t *scope, uint64_t *rows_read);

/*
 * Can this row group hold a record whose parent is in the subtree -- or the
 * subtree's own directory record, which hangs off the parent outside it?
 * interval_out / bitmap_out report what each sketch alone decided.
 */
int crawl_rgix_group_survives(const crawl_rgix_group_t *g, uint64_t dfs_domain, const crawl_sidecar_scope_t *sp,
                              int *interval_out, int *bitmap_out);

/*
 * Build a scan job list from the row groups that survive pruning, in place of
 * the .ckpt segment boundaries. Chunks are byte ranges either way and every
 * group boundary is a legal chunk boundary, so this drops into any chunked
 * reader. Output is shard-major (all of shard 0, then shard 1, ...); a caller
 * that wants the shards interleaved reorders afterwards.
 *
 * split_min/split_max/jobs_per_thread size the jobs from the bytes that survive
 * rather than from the shard sizes, so pruning does not hand the pool a handful
 * of jobs after throwing most of the bytes away.
 *
 * Returns -1 -- with st zeroed and nothing allocated -- when pruning cannot
 * help: no rowgroups.idx, the subtree resolved nowhere, nothing survived, or
 * every group survived (then the checkpoint segments are the better-tested way
 * to cut the same bytes up).
 */
int crawl_rgix_build_chunks(const crawl_sidecar_t *sc, const char *const *shard_paths, size_t shard_count,
                            const crawl_sidecar_scope_t *scope, unsigned nthreads, unsigned jobs_per_thread,
                            uint64_t split_min, uint64_t split_max, crawl_bin_file_chunk_t **chunks_out,
                            size_t *chunk_count_out, uint64_t *chunk_bytes_out, crawl_rgix_prune_stats_t *st);

#endif /* CRAWL_SIDECAR_H */
