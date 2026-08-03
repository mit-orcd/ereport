/*
 * Catalog loader tests, aimed at the slot-defaulting path.
 *
 * The loaders bulk-default slots 1..n up front because n is known before parsing
 * and catalog_store_entry overwrites every field of a slot as its entry arrives.
 * That is only safe if a slot no entry ever claims still reads back as an
 * unwritten slot -- and a hole is silent: nothing crashes, a query just sees a
 * directory with depth 0, no name, and imm_child_min_eff_time of 0 instead of
 * UINT64_MAX, which reads as "epoch" rather than "no children".
 *
 * So these tests care about the ids an entry list skips, not the ids it covers:
 * sparse ids, ids arriving out of order, ids past the entry count, and each
 * combination of optional field groups (an ungated bulk write would go through a
 * NULL array). Both loaders are exercised, since one maps the blob and the other
 * reads it with stdio, and the size threshold decides which.
 *
 * SPDX-License-Identifier: MIT
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "crawl_bin_catalog.h"
#include "crawl_bin_format.h"

static int g_fail = 0;
static int g_checks = 0;

static void ok(const char *what) {
    g_checks++;
    printf("ok   %s\n", what);
}

static void fail(const char *what, const char *detail) {
    g_checks++;
    g_fail++;
    printf("FAIL %s: %s\n", what, detail);
}

static void check_u64(const char *what, uint64_t got, uint64_t want) {
    char buf[160];

    if (got == want) {
        ok(what);
        return;
    }
    snprintf(buf, sizeof(buf), "got %llu want %llu", (unsigned long long)got, (unsigned long long)want);
    fail(what, buf);
}

/* One entry to write into the synthetic blob. Fields we do not vary stay 0. */
typedef struct {
    uint64_t dir_id;
    uint64_t parent_dir_id;
    uint32_t depth;
    const char *name;
} tentry_t;

/*
 * Write `count` entries as a catalog blob and load it back.
 *
 * pad_bytes is prepended before the blob so the caller can push the catalog past
 * CATALOG_MMAP_MIN_BYTES and reach the mapped loader; the loader is told the
 * offset, so the padding is never parsed.
 */
static int load_synth(const tentry_t *ents, size_t count, unsigned fields, size_t pad_bytes,
                      crawl_bin_catalog_t *out) {
    char path[] = "/tmp/ereport_cat_test_XXXXXX";
    int fd = mkstemp(path);
    FILE *fp;
    uint64_t n = (uint64_t)count;
    size_t i;
    long file_sz;
    int rc;

    if (fd < 0) return -1;
    fp = fdopen(fd, "w+b");
    if (!fp) {
        close(fd);
        unlink(path);
        return -1;
    }

    for (i = 0; i < pad_bytes; i++) fputc(0, fp);
    if (fwrite(&n, sizeof(n), 1, fp) != 1) goto io_fail;
    for (i = 0; i < count; i++) {
        bin_dir_catalog_entry_t e;
        size_t nl = ents[i].name ? strlen(ents[i].name) : 0;

        memset(&e, 0, sizeof(e));
        e.dir_id = ents[i].dir_id;
        e.parent_dir_id = ents[i].parent_dir_id;
        e.depth = ents[i].depth;
        e.name_len = (uint16_t)nl;
        e.flags = CRAWL_DIR_FLAG_SELF_RECORD;
        /* Distinct non-default values, so a slot that was wrongly re-defaulted
         * after its entry landed is visible rather than coincidentally right. */
        e.imm_child_count = ents[i].dir_id * 10ULL;
        e.imm_child_min_eff_time = 1000ULL + ents[i].dir_id;
        e.subtree_files = ents[i].dir_id * 100ULL;
        e.dfs_index = ents[i].dir_id;
        if (fwrite(&e, sizeof(e), 1, fp) != 1) goto io_fail;
        if (nl && fwrite(ents[i].name, 1, nl, fp) != nl) goto io_fail;
    }
    if (fflush(fp) != 0) goto io_fail;
    file_sz = ftell(fp);
    if (file_sz < 0) goto io_fail;
    rewind(fp);

    crawl_bin_catalog_init_empty(out);
    rc = crawl_bin_catalog_load_sel(fp, (uint64_t)pad_bytes, (uint64_t)file_sz, fields, out);
    fclose(fp);
    unlink(path);
    return rc;

io_fail:
    fclose(fp);
    unlink(path);
    return -1;
}

/* Every field of dir_id `d` must read as a slot no entry ever wrote. */
static void expect_hole(const crawl_bin_catalog_t *c, uint64_t d, const char *label) {
    char what[96];

    snprintf(what, sizeof(what), "%s: slot %llu parent_dir_id defaulted", label, (unsigned long long)d);
    check_u64(what, c->parent_dir_id[d], 0);
    snprintf(what, sizeof(what), "%s: slot %llu depth defaulted", label, (unsigned long long)d);
    check_u64(what, c->depth[d], 0);
    snprintf(what, sizeof(what), "%s: slot %llu name_len defaulted", label, (unsigned long long)d);
    check_u64(what, c->name_len[d], 0);

    g_checks++;
    if (c->name_comp[d] == NULL) {
        printf("ok   %s: slot %llu name_comp NULL\n", label, (unsigned long long)d);
    } else {
        g_fail++;
        printf("FAIL %s: slot %llu name_comp not NULL\n", label, (unsigned long long)d);
    }

    if (c->fields & CRAWL_CAT_IMM_CHILD) {
        snprintf(what, sizeof(what), "%s: slot %llu imm_child_count defaulted", label,
                 (unsigned long long)d);
        check_u64(what, c->imm_child_count[d], 0);
        /* The one default that is not zero: UINT64_MAX means "no children", and
         * a bulk memset(0) would quietly turn it into the epoch. */
        snprintf(what, sizeof(what), "%s: slot %llu imm_child_min_eff_time is UINT64_MAX", label,
                 (unsigned long long)d);
        check_u64(what, c->imm_child_min_eff_time[d], UINT64_MAX);
    }
    if (c->fields & CRAWL_CAT_SUBTREE) {
        snprintf(what, sizeof(what), "%s: slot %llu subtree_files defaulted", label,
                 (unsigned long long)d);
        check_u64(what, c->subtree_files[d], 0);
        snprintf(what, sizeof(what), "%s: slot %llu dfs_index defaulted", label, (unsigned long long)d);
        check_u64(what, c->dfs_index[d], 0);
        snprintf(what, sizeof(what), "%s: slot %llu self_present defaulted", label,
                 (unsigned long long)d);
        check_u64(what, c->self_present[d], 0);
    }
}

/* dir_id `d` must carry exactly what load_synth wrote for it. */
static void expect_written(const crawl_bin_catalog_t *c, uint64_t d, uint64_t parent, uint32_t depth,
                           const char *label) {
    char what[96];

    snprintf(what, sizeof(what), "%s: slot %llu parent_dir_id", label, (unsigned long long)d);
    check_u64(what, c->parent_dir_id[d], parent);
    snprintf(what, sizeof(what), "%s: slot %llu depth", label, (unsigned long long)d);
    check_u64(what, c->depth[d], depth);
    if (c->fields & CRAWL_CAT_IMM_CHILD) {
        snprintf(what, sizeof(what), "%s: slot %llu imm_child_count", label, (unsigned long long)d);
        check_u64(what, c->imm_child_count[d], d * 10ULL);
        snprintf(what, sizeof(what), "%s: slot %llu imm_child_min_eff_time", label,
                 (unsigned long long)d);
        check_u64(what, c->imm_child_min_eff_time[d], 1000ULL + d);
    }
    if (c->fields & CRAWL_CAT_SUBTREE) {
        snprintf(what, sizeof(what), "%s: slot %llu subtree_files", label, (unsigned long long)d);
        check_u64(what, c->subtree_files[d], d * 100ULL);
    }
}

/* Dense ids in order: the ordinary case, and the control for the sparse tests. */
static void test_dense(unsigned fields, const char *label) {
    static const tentry_t ents[] = {
        {1, 0, 0, "root"},
        {2, 1, 1, "a"},
        {3, 1, 1, "b"},
        {4, 2, 2, "c"},
    };
    crawl_bin_catalog_t c;
    char what[96];

    if (load_synth(ents, 4, fields, 0, &c) != 0) {
        fail(label, "dense load failed");
        return;
    }
    snprintf(what, sizeof(what), "%s: dense max_dir_id", label);
    check_u64(what, c.max_dir_id, 4);
    expect_written(&c, 1, 0, 0, label);
    expect_written(&c, 4, 2, 2, label);
    crawl_bin_catalog_free(&c);
}

/*
 * Ids arriving out of order with holes on both sides of the entry count.
 *
 * With three entries the loaders default slots 1..3 in bulk, so id 5 extends the
 * range past it, id 3 lands on an already-defaulted slot, and id 10 extends it
 * again. That covers the three ways a slot can be reached: bulk-defaulted then
 * written, bulk-defaulted and never written, and defaulted by the growth path.
 */
static void test_sparse_out_of_order(unsigned fields, const char *label) {
    static const tentry_t ents[] = {
        {5, 1, 2, "five"},
        {3, 1, 1, "three"},
        {10, 5, 3, "ten"},
    };
    crawl_bin_catalog_t c;
    char what[96];
    uint64_t holes[] = {1, 2, 4, 6, 7, 8, 9};
    size_t i;

    if (load_synth(ents, 3, fields, 0, &c) != 0) {
        fail(label, "sparse load failed");
        return;
    }
    snprintf(what, sizeof(what), "%s: sparse max_dir_id is highest id seen", label);
    check_u64(what, c.max_dir_id, 10);

    expect_written(&c, 3, 1, 1, label);
    expect_written(&c, 5, 1, 2, label);
    expect_written(&c, 10, 5, 3, label);
    for (i = 0; i < sizeof(holes) / sizeof(holes[0]); i++) expect_hole(&c, holes[i], label);

    crawl_bin_catalog_free(&c);
}

/*
 * Same shape, but padded past the mmap threshold so the mapped loader runs.
 *
 * The two loaders default slots independently, so a fix applied to one and not
 * the other would pass every test above.
 */
static void test_sparse_mapped(unsigned fields, const char *label) {
    size_t n = 12000; /* x sizeof(bin_dir_catalog_entry_t) comfortably over 1 MiB */
    tentry_t *ents = (tentry_t *)calloc(n, sizeof(*ents));
    crawl_bin_catalog_t c;
    char what[96];
    size_t i;

    if (!ents) {
        fail(label, "out of memory");
        return;
    }
    /* Ids 2,4,6,... so every odd id above 1 is a hole inside the primed range. */
    for (i = 0; i < n; i++) {
        ents[i].dir_id = (uint64_t)(2 * (i + 1));
        ents[i].parent_dir_id = 1;
        ents[i].depth = 1;
        ents[i].name = "d";
    }
    if (load_synth(ents, n, fields, 0, &c) != 0) {
        fail(label, "mapped sparse load failed");
        free(ents);
        return;
    }
    free(ents);

    snprintf(what, sizeof(what), "%s: mapped max_dir_id", label);
    check_u64(what, c.max_dir_id, (uint64_t)(2 * n));
    snprintf(what, sizeof(what), "%s: mapped borrowed names", label);
    check_u64(what, (uint64_t)(c.names_borrowed != 0), 1);

    expect_written(&c, 2, 1, 1, label);
    expect_written(&c, (uint64_t)(2 * n), 1, 1, label);
    /* Odd ids are holes: one low, one mid-range, one near the top. */
    expect_hole(&c, 3, label);
    expect_hole(&c, 4001, label);
    expect_hole(&c, (uint64_t)(2 * n - 1), label);

    crawl_bin_catalog_free(&c);
}

/* A reused catalog struct must not treat the previous load's defaults as its own. */
static void test_reuse_after_free(void) {
    static const tentry_t big[] = {{9, 1, 1, "nine"}};
    static const tentry_t small[] = {{2, 1, 1, "two"}};
    crawl_bin_catalog_t c;

    if (load_synth(big, 1, CRAWL_CAT_ALL, 0, &c) != 0) {
        fail("reuse", "first load failed");
        return;
    }
    check_u64("reuse: first load max_dir_id", c.max_dir_id, 9);
    crawl_bin_catalog_free(&c);

    if (load_synth(small, 1, CRAWL_CAT_ALL, 0, &c) != 0) {
        fail("reuse", "second load failed");
        return;
    }
    check_u64("reuse: second load max_dir_id", c.max_dir_id, 2);
    expect_written(&c, 2, 1, 1, "reuse");
    expect_hole(&c, 1, "reuse");
    crawl_bin_catalog_free(&c);
}

int main(void) {
    struct {
        unsigned fields;
        const char *label;
    } sets[] = {
        {CRAWL_CAT_ALL, "all"},
        {CRAWL_CAT_IMM_CHILD, "imm_child"},
        {CRAWL_CAT_SUBTREE, "subtree"},
        {0u, "tree-only"},
    };
    size_t s;

    for (s = 0; s < sizeof(sets) / sizeof(sets[0]); s++) {
        printf("-- field set: %s --\n", sets[s].label);
        test_dense(sets[s].fields, sets[s].label);
        test_sparse_out_of_order(sets[s].fields, sets[s].label);
        test_sparse_mapped(sets[s].fields, sets[s].label);
    }
    printf("-- reuse --\n");
    test_reuse_after_free();

    printf("\n");
    if (g_fail == 0) {
        printf("crawl_bin_catalog: all %d checks passed\n", g_checks);
        return 0;
    }
    printf("crawl_bin_catalog: %d of %d checks FAILED\n", g_fail, g_checks);
    return 1;
}
