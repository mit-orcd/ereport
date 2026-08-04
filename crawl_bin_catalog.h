/*
 * Load directory catalogs from ERCBIN shard tails (see crawl_bin_format.h).
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_BIN_CATALOG_H
#define CRAWL_BIN_CATALOG_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "crawl_bin_format.h"

/*
 * Which optional per-directory arrays to materialize. The tree fields
 * (parent_dir_id/depth/name_len/name_comp) are always loaded because path
 * reconstruction needs them; the rollups are opt-in because they cost 8 bytes
 * per directory each, and on a shard with tens of millions of directories a
 * consumer that only builds paths should not pay for aggregates it never reads.
 */
#define CRAWL_CAT_IMM_CHILD (1u << 0) /* imm_child_* */
#define CRAWL_CAT_SUBTREE (1u << 1)   /* dfs_*, subtree_*, self_* */
#define CRAWL_CAT_ALL (CRAWL_CAT_IMM_CHILD | CRAWL_CAT_SUBTREE)

typedef struct crawl_bin_catalog {
    uint64_t max_dir_id; /* highest dir_id with valid data (arrays valid for 1..max_dir_id) */
    uint64_t cap;        /* allocated slots: arrays sized cap+1 (cap >= max_dir_id) */
    unsigned fields;     /* CRAWL_CAT_* actually loaded; unrequested arrays stay NULL */
    uint64_t *parent_dir_id; /* index by dir_id; 0 unused */
    uint32_t *depth;
    uint16_t *name_len;
    /*
     * Component name, exactly name_len bytes. NULL if name_len == 0.
     *
     * NOT NUL-terminated: these point either into names_arena or straight into the mapped catalog,
     * so a shard with a million directories costs one allocation instead of a million small ones.
     * Always read these with name_len; never strlen/strcmp them.
     */
    char **name_comp;
    char *names_arena;  /* decompressed name bytes, when the blob was not borrowed from map_base */
    void *map_base;     /* mapping backing the catalog; munmap'd by crawl_bin_catalog_free */
    size_t map_len;
    unsigned char *cat_buf; /* heap copy of the catalog, for catalogs too small to be worth mapping */

    /*
     * Per-directory rollups over immediate child records (records whose on-disk
     * parent_dir_id equals dir_id). See crawl_bin_format.h for semantics.
     * imm_child_min_eff_time == UINT64_MAX when imm_child_count == 0.
     */
    uint64_t *imm_child_bytes;
    uint64_t *imm_child_count;
    uint64_t *imm_child_ctime_led_count;
    uint64_t *imm_child_min_eff_time;
    uint64_t *imm_child_max_eff_time;

    /*
     * DFS permutation and subtree prefix sums, filled by the end-of-crawl
     * post-pass. A subtree is the contiguous dfs_index range
     * [dfs_index[d], dfs_index[d] + dfs_subtree_dirs[d]).
     */
    uint64_t *dfs_index;
    uint64_t *dfs_subtree_dirs;
    uint64_t *subtree_bytes;
    uint64_t *subtree_count;
    uint64_t *subtree_nlink_gt1_count;
    uint64_t *subtree_files;
    uint64_t *subtree_dirs;
    uint64_t *subtree_symlinks;
    uint64_t *self_bytes;
    unsigned char *self_present; /* CRAWL_DIR_FLAG_SELF_RECORD, unpacked */
} crawl_bin_catalog_t;

void crawl_bin_catalog_init_empty(crawl_bin_catalog_t *c);
void crawl_bin_catalog_free(crawl_bin_catalog_t *c);

/*
 * Parse catalog blob starting at catalog_offset (must be <= file_sz), loading
 * only the optional arrays named in fields. Returns 0 on success.
 */
int crawl_bin_catalog_load_sel(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, unsigned fields,
                               crawl_bin_catalog_t *out);

/* Load every array. */
int crawl_bin_catalog_load(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, crawl_bin_catalog_t *out);

/*
 * Columns to serialize, indexed by dir_id with slot 0 unused. dfs_index,
 * dfs_subtree_dirs, subtree_bytes and subtree_count may be NULL, which writes
 * them as zero: ecrawl only allocates them in the end-of-crawl post-pass, and
 * an interim tail written on LRU eviction is read back only by the reopen path.
 */
typedef struct {
    uint64_t n_entries; /* dir_ids 1..n_entries, dense */
    const uint64_t *parent_dir_id;
    const uint32_t *depth;
    const uint16_t *name_len;
    char *const *name_comp;
    const uint64_t *imm_child_bytes;
    const uint64_t *imm_child_count;
    const uint64_t *imm_child_ctime_led_count;
    const uint64_t *imm_child_min_eff_time;
    const uint64_t *imm_child_max_eff_time;
    const unsigned char *self_present;
    const uint64_t *self_bytes;
    const uint64_t *subtree_nlink_gt1_count;
    const uint64_t *subtree_files;
    const uint64_t *subtree_dirs;
    const uint64_t *subtree_symlinks;
    const uint64_t *dfs_index;
    const uint64_t *dfs_subtree_dirs;
    const uint64_t *subtree_bytes;
    const uint64_t *subtree_count;
} crawl_bin_catalog_src_t;

/*
 * Write the catalog tail at the current file position, which must be EOF. The
 * caller supplies its own fwrite so the writer's I/O accounting and error
 * injection still apply.
 */
typedef size_t (*crawl_bin_catalog_fwrite_fn)(const void *ptr, size_t size, size_t nmemb, FILE *fp);

int crawl_bin_catalog_write(const crawl_bin_catalog_src_t *src, FILE *fp, crawl_bin_catalog_fwrite_fn wfwrite);

/*
 * Random access to single catalog rows, for a consumer that has an index naming
 * the dir_ids it wants and would rather not materialize every row to reach a
 * handful of them.
 *
 * crawl_bin_catalog_map_t is the shard's chunk table, read once; the chunk that
 * holds a dir_id follows from the id, so nothing else has to be recorded
 * anywhere. crawl_bin_catalog_chunk_t is one decoded chunk, reused across calls
 * so a walk up a parent chain usually decodes nothing after the first hit.
 */
typedef struct {
    uint64_t *chunk_off; /* absolute file offsets, chunk_count entries */
    uint32_t chunk_count;
    uint32_t chunk_dirs;
    uint64_t n_entries;
    uint64_t file_sz;
} crawl_bin_catalog_map_t;

typedef struct {
    uint64_t first_dir_id; /* 0 when nothing is loaded */
    uint32_t dir_count;
    unsigned fields;
    uint64_t *col[CRAWL_CATCOL__COUNT];
    size_t col_cap[CRAWL_CATCOL__COUNT];
    unsigned char *names;
    size_t names_len;
    size_t names_cap;
    uint64_t *name_off; /* prefix sum of the name lengths, dir_count + 1 entries */
    size_t name_off_cap;
    unsigned char *comp;
    size_t comp_cap;
    unsigned char *raw;
    size_t raw_cap;
    void *dctx;
} crawl_bin_catalog_chunk_t;

int crawl_bin_catalog_map_read(int fd, uint64_t catalog_offset, uint64_t file_sz, crawl_bin_catalog_map_t *m);
void crawl_bin_catalog_map_free(crawl_bin_catalog_map_t *m);

void crawl_bin_catalog_chunk_init(crawl_bin_catalog_chunk_t *c);
void crawl_bin_catalog_chunk_free(crawl_bin_catalog_chunk_t *c);

/*
 * Fill *ent and *name with dir_id's row, decoding its chunk if the one already
 * in *c is not the right one. fields names the optional column groups the
 * caller will read; the rest are left zero rather than decoded. Returns 0, or
 * -1 when dir_id is out of range or the chunk does not parse.
 */
int crawl_bin_catalog_read_row(int fd, const crawl_bin_catalog_map_t *m, crawl_bin_catalog_chunk_t *c,
                               uint64_t dir_id, unsigned fields, bin_dir_catalog_entry_t *ent,
                               const unsigned char **name, size_t *name_len_out, int *decoded_out);

/*
 * True when dir_id `d` is at or under `root` -- a range test on the DFS
 * permutation, so it is O(1) per call with no per-shard bitmap. Requires
 * CRAWL_CAT_SUBTREE; returns 0 if the catalog was loaded without it.
 */
static inline int crawl_bin_catalog_in_subtree(const crawl_bin_catalog_t *c, uint64_t root, uint64_t d) {
    uint64_t lo;

    if (!c->dfs_index || !c->dfs_subtree_dirs) return 0;
    if (root == 0ULL || root > c->max_dir_id || d == 0ULL || d > c->max_dir_id) return 0;
    lo = c->dfs_index[root];
    return c->dfs_index[d] >= lo && c->dfs_index[d] < lo + c->dfs_subtree_dirs[root];
}

/*
 * Components crawl_bin_catalog_dir_path_len will walk before it stops.
 *
 * A directory deeper than this comes back with its leading components missing rather than as an
 * error. Callers that reconstruct a path any other way have to match that, or the same directory
 * gets two different names depending on which route reached it.
 */
#define CRAWL_BIN_CATALOG_MAX_PATH_PARTS 128

/*
 * Build absolute stored path for directory dir_id into out (NUL-terminated).
 */
int crawl_bin_catalog_dir_path(const crawl_bin_catalog_t *c, uint64_t dir_id, char *out, size_t out_sz);
int crawl_bin_catalog_dir_path_len(const crawl_bin_catalog_t *c, uint64_t dir_id, char *out, size_t out_sz,
                                   size_t *len_out);

/*
 * parent_dir_id + file/dir name (not NUL-terminated; length name_len) -> full stored path.
 */
int crawl_bin_catalog_entry_path(const crawl_bin_catalog_t *c, uint64_t parent_dir_id, const char *name,
                                 size_t name_len, char *out, size_t out_sz);
int crawl_bin_catalog_entry_path_len(const crawl_bin_catalog_t *c, uint64_t parent_dir_id, const char *name,
                                     size_t name_len, char *out, size_t out_sz, size_t *len_out);

#endif /* CRAWL_BIN_CATALOG_H */
