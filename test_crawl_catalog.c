/*
 * Catalog round-trip tests for the v9 columnar catalog.
 *
 * The catalog is written column-wise and read back either through a mapping or
 * through a heap copy, so what these tests are really after is that nothing is
 * lost or shifted between those two representations: the codec narrows every
 * field to uint64 and back, the name blob is sliced by a separate length column,
 * dir_id is not stored at all but inferred from a row's position in its chunk,
 * and the optional field groups may be skipped entirely.
 *
 * The cases that matter are therefore the boundaries: a catalog that spans more
 * than one chunk and ends on a short one, names of zero and of maximum length,
 * each combination of field groups (a skipped group must leave its arrays
 * unallocated, not half-filled), and both loader paths, which the catalog's size
 * alone decides between. The single-row reader is checked against the full load
 * rather than against expectations of its own, since the two disagreeing is the
 * only way it can be wrong.
 *
 * SPDX-License-Identifier: MIT
 */
#include <limits.h>
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

/*
 * A synthetic catalog, generated rather than tabulated so a case can be as many
 * directories as it needs to cross a chunk boundary.
 *
 * Every column gets a distinct pattern derived from the dir_id: a column read
 * back into the wrong array, or off by a row, then shows up as a value that
 * belongs to some other column or some other directory rather than as a zero
 * that half the columns hold anyway.
 */
typedef struct {
    uint64_t n;
    uint64_t *parent_dir_id;
    uint32_t *depth;
    uint16_t *name_len;
    char **name_comp;
    uint64_t *imm_child_bytes;
    uint64_t *imm_child_count;
    uint64_t *imm_child_ctime_led_count;
    uint64_t *imm_child_min_eff_time;
    uint64_t *imm_child_max_eff_time;
    unsigned char *self_present;
    uint64_t *self_bytes;
    uint64_t *subtree_nlink_gt1_count;
    uint64_t *subtree_files;
    uint64_t *subtree_dirs;
    uint64_t *subtree_symlinks;
    uint64_t *dfs_index;
    uint64_t *dfs_subtree_dirs;
    uint64_t *subtree_bytes;
    uint64_t *subtree_count;
} synth_t;

static void synth_free(synth_t *s) {
    uint64_t i;

    if (s->name_comp)
        for (i = 0; i <= s->n; i++) free(s->name_comp[i]);
    free(s->name_comp);
    free(s->parent_dir_id);
    free(s->depth);
    free(s->name_len);
    free(s->imm_child_bytes);
    free(s->imm_child_count);
    free(s->imm_child_ctime_led_count);
    free(s->imm_child_min_eff_time);
    free(s->imm_child_max_eff_time);
    free(s->self_present);
    free(s->self_bytes);
    free(s->subtree_nlink_gt1_count);
    free(s->subtree_files);
    free(s->subtree_dirs);
    free(s->subtree_symlinks);
    free(s->dfs_index);
    free(s->dfs_subtree_dirs);
    free(s->subtree_bytes);
    free(s->subtree_count);
    memset(s, 0, sizeof(*s));
}

/* Component name for `id`: id 1 is the nameless synthetic root, id 2 is empty
 * too (a name_len of 0 mid-blob is what a mis-sliced name column trips over),
 * and the rest grow so no two adjacent rows share a length. */
static char *synth_name(uint64_t id, uint16_t *len_out) {
    char buf[300];
    size_t nl, at;
    char *p;

    if (id <= 2ULL) {
        *len_out = 0;
        return NULL;
    }
    nl = (size_t)(id % 251ULL) + 1U;
    snprintf(buf, sizeof(buf), "d%llu", (unsigned long long)id);
    for (at = strlen(buf); at < nl; at++) buf[at] = (char)('a' + (int)(at % 26U));
    buf[nl] = '\0';
    p = (char *)malloc(nl + 1U);
    if (!p) return NULL;
    memcpy(p, buf, nl + 1U);
    *len_out = (uint16_t)nl;
    return p;
}

static int synth_build(synth_t *s, uint64_t n) {
    uint64_t i;

    memset(s, 0, sizeof(*s));
    s->n = n;
    s->parent_dir_id = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->depth = (uint32_t *)calloc((size_t)n + 1U, sizeof(uint32_t));
    s->name_len = (uint16_t *)calloc((size_t)n + 1U, sizeof(uint16_t));
    s->name_comp = (char **)calloc((size_t)n + 1U, sizeof(char *));
    s->imm_child_bytes = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->imm_child_count = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->imm_child_ctime_led_count = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->imm_child_min_eff_time = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->imm_child_max_eff_time = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->self_present = (unsigned char *)calloc((size_t)n + 1U, 1U);
    s->self_bytes = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->subtree_nlink_gt1_count = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->subtree_files = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->subtree_dirs = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->subtree_symlinks = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->dfs_index = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->dfs_subtree_dirs = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->subtree_bytes = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    s->subtree_count = (uint64_t *)calloc((size_t)n + 1U, sizeof(uint64_t));
    if (!s->parent_dir_id || !s->depth || !s->name_len || !s->name_comp || !s->imm_child_bytes ||
        !s->imm_child_count || !s->imm_child_ctime_led_count || !s->imm_child_min_eff_time ||
        !s->imm_child_max_eff_time || !s->self_present || !s->self_bytes || !s->subtree_nlink_gt1_count ||
        !s->subtree_files || !s->subtree_dirs || !s->subtree_symlinks || !s->dfs_index ||
        !s->dfs_subtree_dirs || !s->subtree_bytes || !s->subtree_count)
        return -1;

    for (i = 1; i <= n; i++) {
        s->parent_dir_id[i] = (i == 1ULL) ? 0ULL : (i / 2ULL);
        s->depth[i] = (uint32_t)(i % 37ULL);
        s->name_comp[i] = synth_name(i, &s->name_len[i]);
        if (s->name_len[i] > 0U && !s->name_comp[i]) return -1;
        s->imm_child_bytes[i] = i * 1048576ULL + 7ULL;
        s->imm_child_count[i] = i * 3ULL;
        s->imm_child_ctime_led_count[i] = i % 5ULL;
        /* The one default that is not zero: UINT64_MAX means "no children", and
         * a column that lost it reads back as the epoch instead. */
        s->imm_child_min_eff_time[i] = (i % 4ULL == 0ULL) ? UINT64_MAX : 1700000000ULL + i;
        s->imm_child_max_eff_time[i] = 1700000000ULL + i * 2ULL;
        s->self_present[i] = (unsigned char)(i % 3ULL == 0ULL);
        s->self_bytes[i] = i * 4096ULL;
        s->subtree_nlink_gt1_count[i] = i % 7ULL;
        s->subtree_files[i] = i * 11ULL;
        s->subtree_dirs[i] = i * 13ULL;
        s->subtree_symlinks[i] = i % 9ULL;
        s->dfs_index[i] = n - i;
        s->dfs_subtree_dirs[i] = i % 17ULL + 1ULL;
        s->subtree_bytes[i] = i * 65536ULL;
        s->subtree_count[i] = i * 19ULL;
    }
    return 0;
}

static void synth_to_src(const synth_t *s, crawl_bin_catalog_src_t *src, int finalized) {
    memset(src, 0, sizeof(*src));
    src->n_entries = s->n;
    src->parent_dir_id = s->parent_dir_id;
    src->depth = s->depth;
    src->name_len = s->name_len;
    src->name_comp = s->name_comp;
    src->imm_child_bytes = s->imm_child_bytes;
    src->imm_child_count = s->imm_child_count;
    src->imm_child_ctime_led_count = s->imm_child_ctime_led_count;
    src->imm_child_min_eff_time = s->imm_child_min_eff_time;
    src->imm_child_max_eff_time = s->imm_child_max_eff_time;
    src->self_present = s->self_present;
    src->self_bytes = s->self_bytes;
    src->subtree_nlink_gt1_count = s->subtree_nlink_gt1_count;
    src->subtree_files = s->subtree_files;
    src->subtree_dirs = s->subtree_dirs;
    src->subtree_symlinks = s->subtree_symlinks;
    if (finalized) {
        src->dfs_index = s->dfs_index;
        src->dfs_subtree_dirs = s->dfs_subtree_dirs;
        src->subtree_bytes = s->subtree_bytes;
        src->subtree_count = s->subtree_count;
    }
}

/*
 * Write the catalog to a temp file after `pad_bytes` of filler, so a caller can
 * put the catalog at a non-zero offset the way a real shard does, and hand back
 * the open file plus its path.
 */
static FILE *synth_write(const synth_t *s, int finalized, size_t pad_bytes, char *path_out, size_t path_sz,
                         uint64_t *cat_off_out, uint64_t *file_sz_out) {
    char path[] = "/tmp/ereport_cat_test_XXXXXX";
    crawl_bin_catalog_src_t src;
    int fd = mkstemp(path);
    FILE *fp;
    size_t i;
    off_t sz;

    if (fd < 0) return NULL;
    fp = fdopen(fd, "w+b");
    if (!fp) {
        close(fd);
        unlink(path);
        return NULL;
    }
    for (i = 0; i < pad_bytes; i++) fputc(0, fp);
    *cat_off_out = (uint64_t)ftello(fp);

    synth_to_src(s, &src, finalized);
    if (crawl_bin_catalog_write(&src, fp, fwrite) != 0) goto io_fail;
    if (fflush(fp) != 0) goto io_fail;
    sz = ftello(fp);
    if (sz < 0) goto io_fail;
    *file_sz_out = (uint64_t)sz;
    rewind(fp);
    snprintf(path_out, path_sz, "%s", path);
    return fp;

io_fail:
    fclose(fp);
    unlink(path);
    return NULL;
}

/* Every row must read back exactly what was written, for the groups requested. */
static void expect_round_trip(const synth_t *s, const crawl_bin_catalog_t *c, unsigned fields,
                              const char *label) {
    uint64_t i;
    uint64_t bad_tree = 0, bad_name = 0, bad_imm = 0, bad_sub = 0;
    char what[128];

    for (i = 1; i <= s->n; i++) {
        if (c->parent_dir_id[i] != s->parent_dir_id[i] || c->depth[i] != s->depth[i] ||
            c->name_len[i] != s->name_len[i])
            bad_tree++;
        if (s->name_len[i] == 0U) {
            if (c->name_comp[i] != NULL) bad_name++;
        } else if (!c->name_comp[i] || memcmp(c->name_comp[i], s->name_comp[i], s->name_len[i]) != 0) {
            bad_name++;
        }
        if (fields & CRAWL_CAT_IMM_CHILD) {
            if (c->imm_child_bytes[i] != s->imm_child_bytes[i] ||
                c->imm_child_count[i] != s->imm_child_count[i] ||
                c->imm_child_ctime_led_count[i] != s->imm_child_ctime_led_count[i] ||
                c->imm_child_min_eff_time[i] != s->imm_child_min_eff_time[i] ||
                c->imm_child_max_eff_time[i] != s->imm_child_max_eff_time[i])
                bad_imm++;
        }
        if (fields & CRAWL_CAT_SUBTREE) {
            if (c->dfs_index[i] != s->dfs_index[i] || c->dfs_subtree_dirs[i] != s->dfs_subtree_dirs[i] ||
                c->subtree_bytes[i] != s->subtree_bytes[i] || c->subtree_count[i] != s->subtree_count[i] ||
                c->subtree_nlink_gt1_count[i] != s->subtree_nlink_gt1_count[i] ||
                c->subtree_files[i] != s->subtree_files[i] || c->subtree_dirs[i] != s->subtree_dirs[i] ||
                c->subtree_symlinks[i] != s->subtree_symlinks[i] || c->self_bytes[i] != s->self_bytes[i] ||
                c->self_present[i] != s->self_present[i])
                bad_sub++;
        }
    }

    snprintf(what, sizeof(what), "%s: tree columns round-trip", label);
    check_u64(what, bad_tree, 0);
    snprintf(what, sizeof(what), "%s: names round-trip", label);
    check_u64(what, bad_name, 0);
    if (fields & CRAWL_CAT_IMM_CHILD) {
        snprintf(what, sizeof(what), "%s: imm_child columns round-trip", label);
        check_u64(what, bad_imm, 0);
    } else {
        snprintf(what, sizeof(what), "%s: imm_child arrays not allocated", label);
        check_u64(what, (uint64_t)(c->imm_child_count == NULL), 1);
    }
    if (fields & CRAWL_CAT_SUBTREE) {
        snprintf(what, sizeof(what), "%s: subtree columns round-trip", label);
        check_u64(what, bad_sub, 0);
    } else {
        snprintf(what, sizeof(what), "%s: subtree arrays not allocated", label);
        check_u64(what, (uint64_t)(c->dfs_index == NULL), 1);
    }
}

static void test_round_trip(uint64_t n, unsigned fields, size_t pad_bytes, const char *label) {
    synth_t s;
    crawl_bin_catalog_t c;
    char path[PATH_MAX];
    FILE *fp;
    uint64_t cat_off = 0, file_sz = 0;
    char what[128];

    if (synth_build(&s, n) != 0) {
        fail(label, "out of memory");
        synth_free(&s);
        return;
    }
    fp = synth_write(&s, 1, pad_bytes, path, sizeof(path), &cat_off, &file_sz);
    if (!fp) {
        fail(label, "catalog write failed");
        synth_free(&s);
        return;
    }

    crawl_bin_catalog_init_empty(&c);
    if (crawl_bin_catalog_load_sel(fp, cat_off, file_sz, fields, &c) != 0) {
        fail(label, "catalog load failed");
        fclose(fp);
        unlink(path);
        synth_free(&s);
        return;
    }
    snprintf(what, sizeof(what), "%s: max_dir_id", label);
    check_u64(what, c.max_dir_id, n);
    expect_round_trip(&s, &c, fields, label);

    crawl_bin_catalog_free(&c);
    fclose(fp);
    unlink(path);
    synth_free(&s);
}

/*
 * The single-row reader must agree with the full load on every row, including
 * across a chunk boundary and after a switch of field groups, which is what
 * makes it re-decode.
 */
static void test_single_row(uint64_t n) {
    synth_t s;
    crawl_bin_catalog_t c;
    crawl_bin_catalog_map_t m;
    crawl_bin_catalog_chunk_t chunk;
    char path[PATH_MAX];
    FILE *fp;
    uint64_t cat_off = 0, file_sz = 0, i;
    uint64_t bad = 0, bad_sub = 0, bad_name = 0;

    if (synth_build(&s, n) != 0) {
        fail("single-row", "out of memory");
        synth_free(&s);
        return;
    }
    fp = synth_write(&s, 1, 64, path, sizeof(path), &cat_off, &file_sz);
    if (!fp) {
        fail("single-row", "catalog write failed");
        synth_free(&s);
        return;
    }
    crawl_bin_catalog_init_empty(&c);
    if (crawl_bin_catalog_load_sel(fp, cat_off, file_sz, CRAWL_CAT_ALL, &c) != 0) {
        fail("single-row", "reference load failed");
        goto out;
    }
    if (crawl_bin_catalog_map_read(fileno(fp), cat_off, file_sz, &m) != 0) {
        fail("single-row", "chunk table read failed");
        goto out;
    }
    check_u64("single-row: chunk table entry count", m.n_entries, n);
    check_u64("single-row: chunk count", m.chunk_count,
              (n + CRAWL_BIN_CATALOG_CHUNK_DIRS - 1ULL) / CRAWL_BIN_CATALOG_CHUNK_DIRS);

    crawl_bin_catalog_chunk_init(&chunk);
    for (i = 1; i <= n; i++) {
        bin_dir_catalog_entry_t ent;
        const unsigned char *name = NULL;
        size_t nl = 0;
        /* Alternate the field set so the reader has to drop and re-decode a
         * chunk it already holds rather than serve stale columns from it. */
        unsigned fields = (i % 2ULL) ? 0U : CRAWL_CAT_SUBTREE;

        if (crawl_bin_catalog_read_row(fileno(fp), &m, &chunk, i, fields, &ent, &name, &nl, NULL) != 0) {
            bad++;
            continue;
        }
        if (ent.dir_id != i || ent.parent_dir_id != c.parent_dir_id[i] || ent.depth != c.depth[i] ||
            ent.name_len != c.name_len[i])
            bad++;
        if (nl != (size_t)c.name_len[i] || (nl > 0 && memcmp(name, c.name_comp[i], nl) != 0)) bad_name++;
        if (fields & CRAWL_CAT_SUBTREE) {
            if (ent.dfs_index != c.dfs_index[i] || ent.dfs_subtree_dirs != c.dfs_subtree_dirs[i] ||
                ent.subtree_bytes != c.subtree_bytes[i] || ent.subtree_count != c.subtree_count[i] ||
                ent.self_bytes != c.self_bytes[i] ||
                ((ent.flags & CRAWL_DIR_FLAG_SELF_RECORD) ? 1U : 0U) != c.self_present[i])
                bad_sub++;
        }
    }
    check_u64("single-row: tree fields match the full load", bad, 0);
    check_u64("single-row: names match the full load", bad_name, 0);
    check_u64("single-row: subtree fields match the full load", bad_sub, 0);

    {
        bin_dir_catalog_entry_t ent;

        check_u64("single-row: dir_id 0 rejected",
                  (uint64_t)(crawl_bin_catalog_read_row(fileno(fp), &m, &chunk, 0ULL, 0U, &ent, NULL, NULL,
                                                        NULL) != 0),
                  1);
        check_u64("single-row: dir_id past the end rejected",
                  (uint64_t)(crawl_bin_catalog_read_row(fileno(fp), &m, &chunk, n + 1ULL, 0U, &ent, NULL,
                                                        NULL, NULL) != 0),
                  1);
    }
    crawl_bin_catalog_chunk_free(&chunk);
    crawl_bin_catalog_map_free(&m);

out:
    crawl_bin_catalog_free(&c);
    fclose(fp);
    unlink(path);
    synth_free(&s);
}

/*
 * A shard with bytes appended after its catalog.
 *
 * The catalog is the file's tail, so it is tempting to find things by counting
 * back from the end -- and then a shard that grew by one byte stops loading,
 * which is exactly the case where the caller has already given up on its
 * indexes and is relying on the catalog to still be readable.
 */
static void test_trailing_bytes(void) {
    synth_t s;
    crawl_bin_catalog_t c;
    char path[PATH_MAX];
    FILE *fp;
    uint64_t cat_off = 0, file_sz = 0;

    if (synth_build(&s, (uint64_t)CRAWL_BIN_CATALOG_CHUNK_DIRS + 3ULL) != 0) {
        fail("trailing", "out of memory");
        synth_free(&s);
        return;
    }
    fp = synth_write(&s, 1, 0, path, sizeof(path), &cat_off, &file_sz);
    if (!fp) {
        fail("trailing", "catalog write failed");
        synth_free(&s);
        return;
    }
    crawl_bin_catalog_init_empty(&c);
    if (fseeko(fp, (off_t)file_sz, SEEK_SET) != 0 || fputc('x', fp) == EOF || fflush(fp) != 0) {
        fail("trailing", "append failed");
        goto out;
    }
    if (crawl_bin_catalog_load_sel(fp, cat_off, file_sz + 1ULL, CRAWL_CAT_ALL, &c) != 0) {
        fail("trailing", "catalog load failed after an append");
        goto out;
    }
    check_u64("trailing: max_dir_id", c.max_dir_id, s.n);
    expect_round_trip(&s, &c, CRAWL_CAT_ALL, "trailing");

out:
    crawl_bin_catalog_free(&c);
    fclose(fp);
    unlink(path);
    synth_free(&s);
}

/*
 * An unfinalized tail: ecrawl writes one on LRU eviction before the DFS post-pass
 * has run, so those four columns are absent and must read back as zero rather
 * than as whatever the previous column left behind.
 */
static void test_unfinalized(void) {
    synth_t s;
    crawl_bin_catalog_t c;
    char path[PATH_MAX];
    FILE *fp;
    uint64_t cat_off = 0, file_sz = 0, i, nonzero = 0, bad_imm = 0;

    if (synth_build(&s, 500ULL) != 0) {
        fail("unfinalized", "out of memory");
        synth_free(&s);
        return;
    }
    fp = synth_write(&s, 0, 0, path, sizeof(path), &cat_off, &file_sz);
    if (!fp) {
        fail("unfinalized", "catalog write failed");
        synth_free(&s);
        return;
    }
    crawl_bin_catalog_init_empty(&c);
    if (crawl_bin_catalog_load_sel(fp, cat_off, file_sz, CRAWL_CAT_ALL, &c) != 0) {
        fail("unfinalized", "catalog load failed");
        goto out;
    }
    for (i = 1; i <= s.n; i++) {
        if (c.dfs_index[i] || c.dfs_subtree_dirs[i] || c.subtree_bytes[i] || c.subtree_count[i]) nonzero++;
        if (c.imm_child_count[i] != s.imm_child_count[i] ||
            c.imm_child_min_eff_time[i] != s.imm_child_min_eff_time[i])
            bad_imm++;
    }
    check_u64("unfinalized: post-pass columns are zero", nonzero, 0);
    check_u64("unfinalized: imm_child columns survive", bad_imm, 0);

out:
    crawl_bin_catalog_free(&c);
    fclose(fp);
    unlink(path);
    synth_free(&s);
}

/* A catalog whose bytes do not describe a catalog must be refused, not guessed at. */
static void test_reject_garbage(void) {
    crawl_bin_catalog_t c;
    char path[] = "/tmp/ereport_cat_bad_XXXXXX";
    int fd = mkstemp(path);
    FILE *fp;
    unsigned char junk[512];
    size_t i;

    if (fd < 0) {
        fail("reject", "mkstemp failed");
        return;
    }
    fp = fdopen(fd, "w+b");
    if (!fp) {
        close(fd);
        unlink(path);
        fail("reject", "fdopen failed");
        return;
    }
    for (i = 0; i < sizeof(junk); i++) junk[i] = (unsigned char)(i * 7U + 3U);
    fwrite(junk, 1, sizeof(junk), fp);
    fflush(fp);
    rewind(fp);

    crawl_bin_catalog_init_empty(&c);
    check_u64("reject: garbage catalog refused",
              (uint64_t)(crawl_bin_catalog_load_sel(fp, 0ULL, (uint64_t)sizeof(junk), CRAWL_CAT_ALL, &c) != 0),
              1);
    check_u64("reject: nothing left allocated", (uint64_t)(c.parent_dir_id == NULL), 1);
    check_u64("reject: a catalog shorter than its own header is refused",
              (uint64_t)(crawl_bin_catalog_load_sel(fp, 0ULL, 4ULL, CRAWL_CAT_ALL, &c) != 0), 1);

    crawl_bin_catalog_free(&c);
    fclose(fp);
    unlink(path);
}

/* A reused catalog struct must not carry anything over from the previous load. */
static void test_reuse_after_free(void) {
    synth_t big, small;
    crawl_bin_catalog_t c;
    char path[PATH_MAX];
    FILE *fp;
    uint64_t cat_off = 0, file_sz = 0;

    if (synth_build(&big, 9000ULL) != 0 || synth_build(&small, 5ULL) != 0) {
        fail("reuse", "out of memory");
        synth_free(&big);
        synth_free(&small);
        return;
    }
    crawl_bin_catalog_init_empty(&c);

    fp = synth_write(&big, 1, 0, path, sizeof(path), &cat_off, &file_sz);
    if (!fp || crawl_bin_catalog_load_sel(fp, cat_off, file_sz, CRAWL_CAT_ALL, &c) != 0) {
        fail("reuse", "first load failed");
        goto out;
    }
    check_u64("reuse: first load max_dir_id", c.max_dir_id, 9000);
    crawl_bin_catalog_free(&c);
    fclose(fp);
    unlink(path);

    fp = synth_write(&small, 1, 0, path, sizeof(path), &cat_off, &file_sz);
    if (!fp || crawl_bin_catalog_load_sel(fp, cat_off, file_sz, CRAWL_CAT_ALL, &c) != 0) {
        fail("reuse", "second load failed");
        goto out;
    }
    check_u64("reuse: second load max_dir_id", c.max_dir_id, 5);
    expect_round_trip(&small, &c, CRAWL_CAT_ALL, "reuse");

out:
    crawl_bin_catalog_free(&c);
    if (fp) {
        fclose(fp);
        unlink(path);
    }
    synth_free(&big);
    synth_free(&small);
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
        char label[96];

        printf("-- field set: %s --\n", sets[s].label);
        /* One chunk, then enough to end on a short final chunk, then a catalog
         * big enough for the loader to map instead of copying. */
        snprintf(label, sizeof(label), "%s/single-chunk", sets[s].label);
        test_round_trip(37ULL, sets[s].fields, 0, label);
        snprintf(label, sizeof(label), "%s/exact-chunk", sets[s].label);
        test_round_trip((uint64_t)CRAWL_BIN_CATALOG_CHUNK_DIRS, sets[s].fields, 0, label);
        snprintf(label, sizeof(label), "%s/short-final-chunk", sets[s].label);
        test_round_trip((uint64_t)CRAWL_BIN_CATALOG_CHUNK_DIRS * 2ULL + 11ULL, sets[s].fields, 128, label);
        snprintf(label, sizeof(label), "%s/mapped", sets[s].label);
        test_round_trip(120000ULL, sets[s].fields, 0, label);
    }

    printf("-- single-row reader --\n");
    test_single_row((uint64_t)CRAWL_BIN_CATALOG_CHUNK_DIRS + 257ULL);
    printf("-- trailing bytes --\n");
    test_trailing_bytes();
    printf("-- unfinalized tail --\n");
    test_unfinalized();
    printf("-- rejection --\n");
    test_reject_garbage();
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
