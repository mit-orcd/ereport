/*
 * Write a tiny uid_shard_*.bin whose catalog has two dir_ids that both
 * reconstruct to /dup, each with a disjoint DFS subtree and one file child.
 *
 * Clean trees cannot produce that shape; this is the regression fixture for
 * --subtree unioning every path-equal catalog root instead of taking the first.
 *
 * Usage: test_query_subtree_dups <output-dir>
 *        writes <output-dir>/uid_shard_000.bin
 *
 * SPDX-License-Identifier: MIT
 */
#include "crawl_bin_block.h"
#include "crawl_bin_catalog.h"
#include "crawl_bin_format.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int add_rec(crawl_bin_block_writer_t *w, uint64_t pid, uint8_t type, uint64_t size, const char *name) {
    bin_record_hdr_t h;

    memset(&h, 0, sizeof(h));
    h.parent_dir_id = pid;
    h.name_len = (uint16_t)strlen(name);
    h.type = type;
    h.size = size;
    h.uid = 1000;
    h.gid = 100;
    h.mode = (type == (uint8_t)'d') ? (uint32_t)040755 : (uint32_t)0100644;
    h.nlink = 1;
    h.mtime = 1750000000ULL;
    return crawl_bin_block_writer_append_record(w, &h, name);
}

int main(int argc, char **argv) {
    char path[4096];
    FILE *fp;
    bin_file_header_t fh;
    crawl_bin_block_writer_t w;
    crawl_bin_catalog_src_t src;
    uint64_t cat_off;
    uint64_t parent_dir_id[4] = {0, 0, 1, 1};
    uint32_t depth[4] = {0, 0, 1, 1};
    uint16_t name_len[4] = {0, 0, 3, 3};
    char *name_comp[4] = {NULL, NULL, "dup", "dup"};
    uint64_t imm_child_bytes[4] = {0, 0, 10, 20};
    uint64_t imm_child_count[4] = {0, 0, 1, 1};
    uint64_t imm_child_ctime_led_count[4] = {0, 0, 0, 0};
    uint64_t imm_child_min_eff_time[4] = {UINT64_MAX, UINT64_MAX, 1700000000ULL, 1700000000ULL};
    uint64_t imm_child_max_eff_time[4] = {0, 0, 1700000000ULL, 1700000000ULL};
    unsigned char self_present[4] = {0, 0, 1, 1};
    uint64_t self_bytes[4] = {0, 0, 4096, 4096};
    uint64_t subtree_nlink_gt1_count[4] = {0, 0, 0, 0};
    uint64_t subtree_files[4] = {0, 0, 1, 1};
    uint64_t subtree_dirs[4] = {0, 0, 0, 0};
    uint64_t subtree_symlinks[4] = {0, 0, 0, 0};
    uint64_t dfs_index[4] = {0, 0, 1, 2};
    uint64_t dfs_subtree_dirs[4] = {0, 3, 1, 1};
    uint64_t subtree_bytes[4] = {0, 0, 10, 20};
    uint64_t subtree_count[4] = {0, 0, 1, 1};

    if (argc != 2 || !argv[1][0]) {
        fprintf(stderr, "Usage: %s <output-dir>\n", argv[0]);
        return 1;
    }
    if (snprintf(path, sizeof(path), "%s/uid_shard_000.bin", argv[1]) >= (int)sizeof(path)) {
        fprintf(stderr, "test_query_subtree_dups: path too long\n");
        return 1;
    }
    fp = fopen(path, "wb+");
    if (!fp) {
        perror(path);
        return 1;
    }

    memset(&fh, 0, sizeof(fh));
    memcpy(fh.magic, CRAWL_BIN_MAGIC, CRAWL_BIN_MAGIC_LEN);
    fh.version = CRAWL_BIN_FORMAT_VERSION;
    if (fwrite(&fh, sizeof(fh), 1, fp) != 1) goto fail;

    if (crawl_bin_block_writer_init(&w) != 0) goto fail;
    if (add_rec(&w, 1, 'd', 4096, "dup") != 0 || add_rec(&w, 2, 'f', 10, "a") != 0 ||
        add_rec(&w, 1, 'd', 4096, "dup") != 0 || add_rec(&w, 3, 'f', 20, "b") != 0) {
        crawl_bin_block_writer_free(&w);
        goto fail;
    }
    if (crawl_bin_block_writer_flush(&w, fp, fwrite, NULL) != 0) {
        crawl_bin_block_writer_free(&w);
        goto fail;
    }
    crawl_bin_block_writer_free(&w);

    cat_off = (uint64_t)ftello(fp);
    memset(&src, 0, sizeof(src));
    src.n_entries = 3;
    src.parent_dir_id = parent_dir_id;
    src.depth = depth;
    src.name_len = name_len;
    src.name_comp = name_comp;
    src.imm_child_bytes = imm_child_bytes;
    src.imm_child_count = imm_child_count;
    src.imm_child_ctime_led_count = imm_child_ctime_led_count;
    src.imm_child_min_eff_time = imm_child_min_eff_time;
    src.imm_child_max_eff_time = imm_child_max_eff_time;
    src.self_present = self_present;
    src.self_bytes = self_bytes;
    src.subtree_nlink_gt1_count = subtree_nlink_gt1_count;
    src.subtree_files = subtree_files;
    src.subtree_dirs = subtree_dirs;
    src.subtree_symlinks = subtree_symlinks;
    src.dfs_index = dfs_index;
    src.dfs_subtree_dirs = dfs_subtree_dirs;
    src.subtree_bytes = subtree_bytes;
    src.subtree_count = subtree_count;
    if (crawl_bin_catalog_write(&src, fp, fwrite) != 0) goto fail;

    fh.catalog_offset = cat_off;
    if (fseeko(fp, 0, SEEK_SET) != 0 || fwrite(&fh, sizeof(fh), 1, fp) != 1) goto fail;
    if (fclose(fp) != 0) return 1;
    return 0;

fail:
    fclose(fp);
    return 1;
}
