/*
 * Load directory catalogs from ERCBIN05 shard tails (see crawl_bin_format.h).
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef CRAWL_BIN_CATALOG_H
#define CRAWL_BIN_CATALOG_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

typedef struct crawl_bin_catalog {
    uint64_t max_dir_id; /* highest dir_id with valid data (arrays valid for 1..max_dir_id) */
    uint64_t cap;        /* allocated slots: arrays sized cap+1 (cap >= max_dir_id) */
    uint64_t *parent_dir_id; /* index by dir_id; 0 unused */
    uint32_t *depth;
    uint16_t *name_len;
    char **name_comp; /* owned strdup per component; NULL if name_len==0 */

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
} crawl_bin_catalog_t;

void crawl_bin_catalog_init_empty(crawl_bin_catalog_t *c);
void crawl_bin_catalog_free(crawl_bin_catalog_t *c);

/*
 * Parse catalog blob starting at catalog_offset (must be <= file_sz).
 * Returns 0 on success.
 */
int crawl_bin_catalog_load(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, crawl_bin_catalog_t *out);

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
