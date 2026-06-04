/*
 * SPDX-License-Identifier: MIT
 */

#include "crawl_bin_catalog.h"

#include "crawl_bin_format.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

void crawl_bin_catalog_init_empty(crawl_bin_catalog_t *c) {
    if (!c) return;
    memset(c, 0, sizeof(*c));
}

void crawl_bin_catalog_free(crawl_bin_catalog_t *c) {
    uint64_t i;

    if (!c) return;
    if (c->parent_dir_id) free(c->parent_dir_id);
    if (c->depth) free(c->depth);
    if (c->name_len) free(c->name_len);
    if (c->name_comp) {
        /* Slot 0 is unused (dir_ids are 1-based); never initialized by catalog_ensure_slots. */
        for (i = 1; i <= c->max_dir_id; i++) free(c->name_comp[i]);
        free(c->name_comp);
    }
    free(c->imm_child_bytes);
    free(c->imm_child_count);
    free(c->imm_child_ctime_led_count);
    free(c->imm_child_min_eff_time);
    free(c->imm_child_max_eff_time);
    crawl_bin_catalog_init_empty(c);
}

static int catalog_ensure_slots(crawl_bin_catalog_t *c, uint64_t dir_id) {
    uint64_t new_cap;
    uint64_t i;

    if (dir_id <= c->max_dir_id) return 0;

    if (dir_id > c->cap) {
        /*
         * Grow geometrically. Catalog entries arrive with roughly sequential
         * dir_ids, so growing to exactly dir_id (the old behaviour) reallocated
         * all nine arrays on nearly every entry -> O(n^2) copying (~27% of CPU
         * in realloc/memmove/page-faults on a 1M-parent shard). Double, bounded
         * to the request, with a small floor, to amortize realloc to O(n).
         */
        new_cap = c->cap * 2ULL;
        if (new_cap < dir_id) new_cap = dir_id;
        if (new_cap < 256ULL) new_cap = 256ULL;

        c->parent_dir_id = (uint64_t *)realloc(c->parent_dir_id, (size_t)(new_cap + 1ULL) * sizeof(uint64_t));
        c->depth = (uint32_t *)realloc(c->depth, (size_t)(new_cap + 1ULL) * sizeof(uint32_t));
        c->name_len = (uint16_t *)realloc(c->name_len, (size_t)(new_cap + 1ULL) * sizeof(uint16_t));
        c->name_comp = (char **)realloc(c->name_comp, (size_t)(new_cap + 1ULL) * sizeof(char *));
        c->imm_child_bytes = (uint64_t *)realloc(c->imm_child_bytes, (size_t)(new_cap + 1ULL) * sizeof(uint64_t));
        c->imm_child_count = (uint64_t *)realloc(c->imm_child_count, (size_t)(new_cap + 1ULL) * sizeof(uint64_t));
        c->imm_child_ctime_led_count =
            (uint64_t *)realloc(c->imm_child_ctime_led_count, (size_t)(new_cap + 1ULL) * sizeof(uint64_t));
        c->imm_child_min_eff_time =
            (uint64_t *)realloc(c->imm_child_min_eff_time, (size_t)(new_cap + 1ULL) * sizeof(uint64_t));
        c->imm_child_max_eff_time =
            (uint64_t *)realloc(c->imm_child_max_eff_time, (size_t)(new_cap + 1ULL) * sizeof(uint64_t));
        if (!c->parent_dir_id || !c->depth || !c->name_len || !c->name_comp ||
            !c->imm_child_bytes || !c->imm_child_count || !c->imm_child_ctime_led_count ||
            !c->imm_child_min_eff_time || !c->imm_child_max_eff_time) return -1;
        c->cap = new_cap;
    }

    for (i = c->max_dir_id + 1; i <= dir_id; i++) {
        c->parent_dir_id[i] = 0;
        c->depth[i] = 0;
        c->name_len[i] = 0;
        c->name_comp[i] = NULL;
        c->imm_child_bytes[i] = 0;
        c->imm_child_count[i] = 0;
        c->imm_child_ctime_led_count[i] = 0;
        c->imm_child_min_eff_time[i] = UINT64_MAX;
        c->imm_child_max_eff_time[i] = 0;
    }
    c->max_dir_id = dir_id;
    return 0;
}

int crawl_bin_catalog_load(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, crawl_bin_catalog_t *out) {
    uint64_t n, i;
    unsigned char *tmp_name = NULL;
    size_t tmp_cap = 0;

    crawl_bin_catalog_init_empty(out);
    if (!fp || catalog_offset > file_sz || catalog_offset + sizeof(uint64_t) > file_sz) {
        errno = EINVAL;
        return -1;
    }

    if (fseeko(fp, (off_t)catalog_offset, SEEK_SET) != 0) return -1;
    if (fread(&n, sizeof(n), 1, fp) != 1) return -1;

    if (n > (uint64_t)(1ULL << 28)) {
        errno = EINVAL;
        return -1;
    }

    for (i = 0; i < n; i++) {
        bin_dir_catalog_entry_t ent;
        uint64_t did;
        size_t nl;

        if (fread(&ent, sizeof(ent), 1, fp) != 1) goto fail;
        did = ent.dir_id;
        if (did == 0) goto fail;
        if (catalog_ensure_slots(out, did) != 0) goto fail;

        nl = (size_t)ent.name_len;
        if ((uint64_t)ftello(fp) + nl > file_sz) goto fail;
        if (nl > tmp_cap) {
            unsigned char *nx = (unsigned char *)realloc(tmp_name, nl + 1);
            if (!nx) goto fail;
            tmp_name = nx;
            tmp_cap = nl + 1;
        }
        free(out->name_comp[did]);
        out->name_comp[did] = NULL;
        if (nl > 0) {
            if (fread(tmp_name, 1, nl, fp) != nl) goto fail;
            tmp_name[nl] = '\0';
            out->name_comp[did] = (char *)malloc(nl + 1);
            if (!out->name_comp[did]) goto fail;
            memcpy(out->name_comp[did], tmp_name, nl + 1);
        }
        out->parent_dir_id[did] = ent.parent_dir_id;
        out->depth[did] = ent.depth;
        out->name_len[did] = ent.name_len;
        out->imm_child_bytes[did] = ent.imm_child_bytes;
        out->imm_child_count[did] = ent.imm_child_count;
        out->imm_child_ctime_led_count[did] = ent.imm_child_ctime_led_count;
        out->imm_child_min_eff_time[did] = ent.imm_child_min_eff_time;
        out->imm_child_max_eff_time[did] = ent.imm_child_max_eff_time;
    }

    free(tmp_name);
    return 0;

fail:
    free(tmp_name);
    crawl_bin_catalog_free(out);
    errno = EINVAL;
    return -1;
}

int crawl_bin_catalog_dir_path(const crawl_bin_catalog_t *c, uint64_t dir_id, char *out, size_t out_sz) {
    size_t nparts = 0;
    size_t parts_len[128];
    const char *parts_ptr[128];
    uint64_t cur = dir_id;
    size_t tot = 0;
    size_t pi;

    if (!c || !out || out_sz == 0) return -1;
    out[0] = '\0';
    if (dir_id == 0 || dir_id > c->max_dir_id) return -1;

    while (cur != 0 && nparts < 128) {
        if (cur > c->max_dir_id) return -1;
        if (cur == 1ULL) break;
        parts_len[nparts] = (size_t)c->name_len[cur];
        parts_ptr[nparts] = c->name_comp[cur];
        if (parts_len[nparts] > 0 && !parts_ptr[nparts]) return -1;
        tot += parts_len[nparts] + (nparts > 0 ? 1 : 0);
        nparts++;
        cur = c->parent_dir_id[cur];
    }

    if (tot + 1 > out_sz) return -1;
    {
        size_t pos = 0;
        for (pi = nparts; pi > 0; pi--) {
            size_t idx = pi - 1;
            if (pos > 0) {
                if (pos + 1 >= out_sz) return -1;
                out[pos++] = '/';
            }
            if (parts_len[idx] > 0) {
                if (pos + parts_len[idx] >= out_sz) return -1;
                memcpy(out + pos, parts_ptr[idx], parts_len[idx]);
                pos += parts_len[idx];
            }
        }
        out[pos] = '\0';
    }
    return 0;
}

int crawl_bin_catalog_entry_path(const crawl_bin_catalog_t *c, uint64_t parent_dir_id, const char *name,
                                 size_t name_len, char *out, size_t out_sz) {
    size_t plen;
    int rc;

    if (!c || !out || out_sz == 0) return -1;
    if (parent_dir_id == 0ULL) {
        errno = EINVAL;
        return -1;
    }

    if (parent_dir_id == 1ULL) {
        if (name_len + 2 > out_sz) return -1;
        if (name_len > 0 && name) {
            memcpy(out, name, name_len);
            out[name_len] = '\0';
        } else {
            out[0] = '\0';
        }
        return 0;
    }

    rc = crawl_bin_catalog_dir_path(c, parent_dir_id, out, out_sz);
    if (rc != 0) return rc;
    plen = strlen(out);
    if (plen + 1 + name_len + 1 > out_sz) return -1;
    if (plen > 0) {
        out[plen] = '/';
        plen++;
    }
    if (name_len > 0 && name) memcpy(out + plen, name, name_len);
    out[plen + name_len] = '\0';
    return 0;
}
