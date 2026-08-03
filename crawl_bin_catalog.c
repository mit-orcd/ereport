/*
 * SPDX-License-Identifier: MIT
 */

#include "crawl_bin_catalog.h"

#include "crawl_bin_format.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

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
        /* Borrowed names live in the mapping released below, so only owned ones are freed here.
         * Slot 0 is unused (dir_ids are 1-based); never initialized by catalog_ensure_slots. */
        if (!c->names_borrowed)
            for (i = 1; i <= c->max_dir_id; i++) free(c->name_comp[i]);
        free(c->name_comp);
    }
    if (c->map_base) munmap(c->map_base, c->map_len);
    free(c->imm_child_bytes);
    free(c->imm_child_count);
    free(c->imm_child_ctime_led_count);
    free(c->imm_child_min_eff_time);
    free(c->imm_child_max_eff_time);
    free(c->dfs_index);
    free(c->dfs_subtree_dirs);
    free(c->subtree_bytes);
    free(c->subtree_count);
    free(c->subtree_nlink_gt1_count);
    free(c->subtree_files);
    free(c->subtree_dirs);
    free(c->subtree_symlinks);
    free(c->self_bytes);
    free(c->self_present);
    crawl_bin_catalog_init_empty(c);
}

/* realloc one optional uint64 array only when its group was requested. */
static int cat_opt_grow(uint64_t **arr, int wanted, uint64_t slots) {
    uint64_t *p;

    if (!wanted) return 0;
    p = (uint64_t *)realloc(*arr, (size_t)slots * sizeof(uint64_t));
    if (!p) return -1;
    *arr = p;
    return 0;
}

/*
 * Grow the arrays to hold dir_ids up to `want_cap`, without publishing any of the new slots
 * (max_dir_id is the caller's business). Geometric: catalog entries arrive with roughly sequential
 * dir_ids, so growing to exactly the requested id reallocated all nine arrays on nearly every entry
 * -> O(n^2) copying (~27% of CPU in realloc/memmove/page-faults on a 1M-parent shard).
 */
static int catalog_reserve_cap(crawl_bin_catalog_t *c, uint64_t want_cap) {
    uint64_t new_cap;
    int want_imm = (c->fields & CRAWL_CAT_IMM_CHILD) != 0;
    int want_sub = (c->fields & CRAWL_CAT_SUBTREE) != 0;

    if (want_cap <= c->cap) return 0;
    {
        new_cap = c->cap * 2ULL;
        if (new_cap < want_cap) new_cap = want_cap;
        if (new_cap < 256ULL) new_cap = 256ULL;

        c->parent_dir_id = (uint64_t *)realloc(c->parent_dir_id, (size_t)(new_cap + 1ULL) * sizeof(uint64_t));
        c->depth = (uint32_t *)realloc(c->depth, (size_t)(new_cap + 1ULL) * sizeof(uint32_t));
        c->name_len = (uint16_t *)realloc(c->name_len, (size_t)(new_cap + 1ULL) * sizeof(uint16_t));
        c->name_comp = (char **)realloc(c->name_comp, (size_t)(new_cap + 1ULL) * sizeof(char *));
        if (!c->parent_dir_id || !c->depth || !c->name_len || !c->name_comp) return -1;

        if (cat_opt_grow(&c->imm_child_bytes, want_imm, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->imm_child_count, want_imm, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->imm_child_ctime_led_count, want_imm, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->imm_child_min_eff_time, want_imm, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->imm_child_max_eff_time, want_imm, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->dfs_index, want_sub, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->dfs_subtree_dirs, want_sub, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->subtree_bytes, want_sub, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->subtree_count, want_sub, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->subtree_nlink_gt1_count, want_sub, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->subtree_files, want_sub, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->subtree_dirs, want_sub, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->subtree_symlinks, want_sub, new_cap + 1ULL) != 0 ||
            cat_opt_grow(&c->self_bytes, want_sub, new_cap + 1ULL) != 0)
            return -1;
        if (want_sub) {
            unsigned char *sp = (unsigned char *)realloc(c->self_present, (size_t)(new_cap + 1ULL));

            if (!sp) return -1;
            c->self_present = sp;
        }
        c->cap = new_cap;
    }
    return 0;
}

static int catalog_ensure_slots(crawl_bin_catalog_t *c, uint64_t dir_id) {
    uint64_t i;
    int want_imm = (c->fields & CRAWL_CAT_IMM_CHILD) != 0;
    int want_sub = (c->fields & CRAWL_CAT_SUBTREE) != 0;

    if (dir_id <= c->max_dir_id) return 0;
    if (catalog_reserve_cap(c, dir_id) != 0) return -1;

    for (i = c->max_dir_id + 1; i <= dir_id; i++) {
        c->parent_dir_id[i] = 0;
        c->depth[i] = 0;
        c->name_len[i] = 0;
        c->name_comp[i] = NULL;
        if (want_imm) {
            c->imm_child_bytes[i] = 0;
            c->imm_child_count[i] = 0;
            c->imm_child_ctime_led_count[i] = 0;
            c->imm_child_min_eff_time[i] = UINT64_MAX;
            c->imm_child_max_eff_time[i] = 0;
        }
        if (want_sub) {
            c->dfs_index[i] = 0;
            c->dfs_subtree_dirs[i] = 0;
            c->subtree_bytes[i] = 0;
            c->subtree_count[i] = 0;
            c->subtree_nlink_gt1_count[i] = 0;
            c->subtree_files[i] = 0;
            c->subtree_dirs[i] = 0;
            c->subtree_symlinks[i] = 0;
            c->self_bytes[i] = 0;
            c->self_present[i] = 0;
        }
    }
    c->max_dir_id = dir_id;
    return 0;
}

int crawl_bin_catalog_load(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, crawl_bin_catalog_t *out) {
    return crawl_bin_catalog_load_sel(fp, catalog_offset, file_sz, CRAWL_CAT_ALL, out);
}

/* Copy one parsed entry's fields into the catalog arrays. Shared by the mapped and stdio loaders so
 * the two paths cannot drift; the name is stored by the caller, which knows if it owns the bytes. */
static void catalog_store_entry(crawl_bin_catalog_t *out, uint64_t did, const bin_dir_catalog_entry_t *ent) {
    out->parent_dir_id[did] = ent->parent_dir_id;
    out->depth[did] = ent->depth;
    out->name_len[did] = ent->name_len;
    if (out->fields & CRAWL_CAT_IMM_CHILD) {
        out->imm_child_bytes[did] = ent->imm_child_bytes;
        out->imm_child_count[did] = ent->imm_child_count;
        out->imm_child_ctime_led_count[did] = ent->imm_child_ctime_led_count;
        out->imm_child_min_eff_time[did] = ent->imm_child_min_eff_time;
        out->imm_child_max_eff_time[did] = ent->imm_child_max_eff_time;
    }
    if (out->fields & CRAWL_CAT_SUBTREE) {
        out->dfs_index[did] = ent->dfs_index;
        out->dfs_subtree_dirs[did] = ent->dfs_subtree_dirs;
        out->subtree_bytes[did] = ent->subtree_bytes;
        out->subtree_count[did] = ent->subtree_count;
        out->subtree_nlink_gt1_count[did] = ent->subtree_nlink_gt1_count;
        out->subtree_files[did] = ent->subtree_files;
        out->subtree_dirs[did] = ent->subtree_dirs;
        out->subtree_symlinks[did] = ent->subtree_symlinks;
        out->self_bytes[did] = ent->self_bytes;
        out->self_present[did] = (ent->flags & CRAWL_DIR_FLAG_SELF_RECORD) ? 1U : 0U;
    }
}

/*
 * Below this many catalog bytes, read with stdio instead of mapping.
 *
 * A mapping is not free to take down: munmap in a multi-threaded process sends a TLB shootdown IPI
 * to every CPU the process has run on, and that cost does not shrink with the mapping. On a crawl
 * split into 1019 small shards, ecrawl_analyze spent 22.5% of the run inside
 * crawl_bin_catalog_load_sel and crawl_bin_catalog_free -- 8.4% in munmap alone, with
 * smp_call_function_many_cond, tlb_is_not_lazy and flush_tlb_func together near a third of all
 * samples on a 32-thread run across 96 CPUs. One large catalog still wins from mapping, so gate on
 * size rather than dropping the path.
 */
#define CATALOG_MMAP_MIN_BYTES (1ULL << 20)

/*
 * Parse the catalog straight out of a read-only mapping of the shard.
 *
 * Two costs disappear versus the stdio walk: the per-entry fread/ftello pair, and the malloc+copy
 * that gave every directory name its own heap block. Names instead point into the mapping, so a
 * shard with a million directories holds one VMA of page cache rather than a million allocations --
 * which is also why they are not NUL-terminated (see name_comp in the header).
 *
 * Returns 0 on success, -1 with the catalog left empty so the caller can fall back to stdio.
 */
static int catalog_load_mapped(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, uint64_t n,
                               crawl_bin_catalog_t *out) {
    long page = sysconf(_SC_PAGESIZE);
    uint64_t base_off, map_len;
    unsigned char *map, *p, *end;
    uint64_t i;
    int fd;

    if (page <= 0) return -1;
    fd = fileno(fp);
    if (fd < 0) return -1;

    base_off = catalog_offset & ~((uint64_t)page - 1ULL);
    map_len = file_sz - base_off;
    if (map_len > (uint64_t)SIZE_MAX) return -1;

    map = (unsigned char *)mmap(NULL, (size_t)map_len, PROT_READ, MAP_PRIVATE, fd, (off_t)base_off);
    if (map == MAP_FAILED) return -1;

    /* The walk is a single forward pass, so tell the kernel to read ahead instead of faulting it in
     * one page at a time. Advisory: a failure here costs speed, never correctness. */
    (void)madvise(map, (size_t)map_len, MADV_WILLNEED);
    (void)madvise(map, (size_t)map_len, MADV_SEQUENTIAL);

    end = map + map_len;
    p = map + (catalog_offset - base_off) + sizeof(uint64_t); /* skip the entry count */

    /* dir_ids are handed out densely from 1, so the entry count sizes the arrays: reserving up front
     * collapses the doubling walk into one allocation per array. A sparse shard still grows. */
    if (catalog_reserve_cap(out, n) != 0) goto fail;

    for (i = 0; i < n; i++) {
        bin_dir_catalog_entry_t ent;
        uint64_t did;
        size_t nl;

        if ((size_t)(end - p) < sizeof(ent)) goto fail;
        memcpy(&ent, p, sizeof(ent)); /* the on-disk struct is packed; copy out to stay aligned */
        p += sizeof(ent);

        did = ent.dir_id;
        if (did == 0) goto fail;
        if (catalog_ensure_slots(out, did) != 0) goto fail;

        nl = (size_t)ent.name_len;
        if ((size_t)(end - p) < nl) goto fail;
        out->name_comp[did] = (nl > 0) ? (char *)p : NULL;
        p += nl;

        catalog_store_entry(out, did, &ent);
    }

    out->names_borrowed = 1;
    out->map_base = map;
    out->map_len = (size_t)map_len;
    /* Leave the stream where the stdio loader would have left it, since callers reuse the handle. */
    (void)fseeko(fp, (off_t)(base_off + (uint64_t)(p - map)), SEEK_SET);
    return 0;

fail:
    /* Drop the arrays before unmapping: they may hold pointers into the mapping. */
    out->names_borrowed = 1;
    crawl_bin_catalog_free(out);
    munmap(map, (size_t)map_len);
    return -1;
}

int crawl_bin_catalog_load_sel(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, unsigned fields,
                               crawl_bin_catalog_t *out) {
    uint64_t n, i;
    unsigned char *tmp_name = NULL;
    size_t tmp_cap = 0;

    crawl_bin_catalog_init_empty(out);
    out->fields = fields & CRAWL_CAT_ALL;
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

    if (n > 0 && (file_sz - catalog_offset) >= CATALOG_MMAP_MIN_BYTES) {
        unsigned saved_fields = out->fields;

        if (catalog_load_mapped(fp, catalog_offset, file_sz, n, out) == 0) return 0;
        /* Mapping failed or the blob did not parse; retry with plain reads. */
        crawl_bin_catalog_init_empty(out);
        out->fields = saved_fields;
        if (fseeko(fp, (off_t)(catalog_offset + sizeof(uint64_t)), SEEK_SET) != 0) return -1;
    }

    /* ecrawl hands out dir_ids densely from 1, so the entry count is the array size: reserving it up
     * front turns the doubling walk (nine reallocs and their page faults) into one allocation each.
     * A sparse shard still grows through catalog_ensure_slots as its ids arrive. */
    if (n > 0 && catalog_reserve_cap(out, n) != 0) goto fail;

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
        /* Need nl+1 bytes (name + NUL); grow whenever capacity can't hold the
         * terminator, not only when nl strictly exceeds it (tmp_cap already
         * includes the +1, so `nl > tmp_cap` left tmp_name[nl] one byte short
         * when a later name length equalled an earlier capacity). */
        if (nl + 1 > tmp_cap) {
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
        catalog_store_entry(out, did, &ent);
    }

    free(tmp_name);
    return 0;

fail:
    free(tmp_name);
    crawl_bin_catalog_free(out);
    errno = EINVAL;
    return -1;
}

int crawl_bin_catalog_dir_path_len(const crawl_bin_catalog_t *c, uint64_t dir_id, char *out, size_t out_sz,
                                   size_t *len_out) {
    size_t nparts = 0;
    size_t parts_len[128];
    const char *parts_ptr[128];
    uint64_t cur = dir_id;
    size_t tot = 0;
    size_t pi;

    if (!c || !out || out_sz == 0) return -1;
    if (len_out) *len_out = 0;
    out[0] = '\0';
    if (dir_id == 0 || dir_id > c->max_dir_id) return -1;

    while (cur != 0 && nparts < 128) {
        if (cur > c->max_dir_id) return -1;
        if (cur == 1ULL) break;
        parts_len[nparts] = (size_t)c->name_len[cur];
        parts_ptr[nparts] = c->name_comp[cur];
        if (parts_len[nparts] > 0 && !parts_ptr[nparts]) return -1;
        tot += parts_len[nparts] + 1; /* one leading '/' per component (paths are absolute) */
        nparts++;
        cur = c->parent_dir_id[cur];
    }

    if (tot + 1 > out_sz) return -1;
    {
        /* Emit a '/' before every component so absolute paths keep their leading
         * slash (e.g. /orcd/data/...). nparts == 0 means dir_id == 1 (the synthetic
         * root); it stays "" so it matches the empty root key used during build. */
        size_t pos = 0;
        for (pi = nparts; pi > 0; pi--) {
            size_t idx = pi - 1;
            if (pos + 1 >= out_sz) return -1;
            out[pos++] = '/';
            if (parts_len[idx] > 0) {
                if (pos + parts_len[idx] >= out_sz) return -1;
                memcpy(out + pos, parts_ptr[idx], parts_len[idx]);
                pos += parts_len[idx];
            }
        }
        out[pos] = '\0';
        if (len_out) *len_out = pos;
    }
    return 0;
}

int crawl_bin_catalog_dir_path(const crawl_bin_catalog_t *c, uint64_t dir_id, char *out, size_t out_sz) {
    return crawl_bin_catalog_dir_path_len(c, dir_id, out, out_sz, NULL);
}

int crawl_bin_catalog_entry_path_len(const crawl_bin_catalog_t *c, uint64_t parent_dir_id, const char *name,
                                     size_t name_len, char *out, size_t out_sz, size_t *len_out) {
    size_t plen;
    int rc;

    if (!c || !out || out_sz == 0) return -1;
    if (len_out) *len_out = 0;
    if (parent_dir_id == 0ULL) {
        errno = EINVAL;
        return -1;
    }

    if (parent_dir_id == 1ULL) {
        /* Direct child of the synthetic root: path is /<name> (paths are absolute). */
        if (name_len + 2 > out_sz) return -1;
        out[0] = '/';
        if (name_len > 0 && name) memcpy(out + 1, name, name_len);
        out[1 + name_len] = '\0';
        if (len_out) *len_out = 1U + name_len;
        return 0;
    }

    rc = crawl_bin_catalog_dir_path_len(c, parent_dir_id, out, out_sz, &plen);
    if (rc != 0) return rc;
    if (plen + 1 + name_len + 1 > out_sz) return -1;
    if (plen > 0) {
        out[plen] = '/';
        plen++;
    }
    if (name_len > 0 && name) memcpy(out + plen, name, name_len);
    out[plen + name_len] = '\0';
    if (len_out) *len_out = plen + name_len;
    return 0;
}

int crawl_bin_catalog_entry_path(const crawl_bin_catalog_t *c, uint64_t parent_dir_id, const char *name,
                                 size_t name_len, char *out, size_t out_sz) {
    return crawl_bin_catalog_entry_path_len(c, parent_dir_id, name, name_len, out, out_sz, NULL);
}
