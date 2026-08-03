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

/*
 * Which optional per-directory arrays to materialize. The tree fields
 * (parent_dir_id/depth/name_len/name_comp) are always loaded because path
 * reconstruction needs them; the rollups are opt-in because they cost 8 bytes
 * per directory each, and on a shard with tens of millions of directories a
 * consumer that only builds paths should not pay for aggregates it never reads.
 */
#define CRAWL_CAT_IMM_CHILD (1u << 0) /* imm_child_* */
#define CRAWL_CAT_SUBTREE (1u << 1)   /* dfs_* and subtree_* */
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
     * NOT NUL-terminated when names_borrowed is set: the load path maps the shard and points these
     * straight at the mapped bytes, so a shard with a million directories costs one mapping instead
     * of a million small allocations. Always read these with name_len; never strlen/strcmp them.
     */
    char **name_comp;
    int names_borrowed; /* name_comp points into map_base and must not be freed per entry */
    void *map_base;     /* mapping backing the borrowed names; munmap'd by crawl_bin_catalog_free */
    size_t map_len;

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
