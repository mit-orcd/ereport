/*
 * SPDX-License-Identifier: MIT
 */

#include "crawl_bin_catalog.h"

#include "crawl_bin_codec.h"
#include "crawl_bin_format.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <zstd.h>

/*
 * Column order on disk. The four tree columns come first so a consumer that only
 * reconstructs paths -- which is most of them -- reads a contiguous prefix of
 * every chunk and never touches a rollup payload.
 */
static const uint8_t k_cat_columns[] = {CRAWL_CATCOL_PARENT_DIR_ID,
                                        CRAWL_CATCOL_DEPTH,
                                        CRAWL_CATCOL_NAME_LEN,
                                        CRAWL_CATCOL_NAME_BYTES,
                                        CRAWL_CATCOL_IMM_CHILD_BYTES,
                                        CRAWL_CATCOL_IMM_CHILD_COUNT,
                                        CRAWL_CATCOL_IMM_CHILD_CTIME_LED_COUNT,
                                        CRAWL_CATCOL_IMM_CHILD_MIN_EFF_TIME,
                                        CRAWL_CATCOL_IMM_CHILD_MAX_EFF_TIME,
                                        CRAWL_CATCOL_FLAGS,
                                        CRAWL_CATCOL_DFS_INDEX,
                                        CRAWL_CATCOL_DFS_SUBTREE_DIRS,
                                        CRAWL_CATCOL_SUBTREE_BYTES,
                                        CRAWL_CATCOL_SUBTREE_COUNT,
                                        CRAWL_CATCOL_SUBTREE_NLINK_GT1_COUNT,
                                        CRAWL_CATCOL_SUBTREE_FILES,
                                        CRAWL_CATCOL_SUBTREE_DIRS,
                                        CRAWL_CATCOL_SUBTREE_SYMLINKS,
                                        CRAWL_CATCOL_SELF_BYTES};

#define CAT_COLUMN_COUNT ((uint32_t)(sizeof(k_cat_columns) / sizeof(k_cat_columns[0])))

/* Which optional field group a column belongs to; 0 means always loaded. */
static unsigned cat_column_group(uint8_t col) {
    switch (col) {
        case CRAWL_CATCOL_PARENT_DIR_ID:
        case CRAWL_CATCOL_DEPTH:
        case CRAWL_CATCOL_NAME_LEN:
        case CRAWL_CATCOL_NAME_BYTES:
            return 0U;
        case CRAWL_CATCOL_IMM_CHILD_BYTES:
        case CRAWL_CATCOL_IMM_CHILD_COUNT:
        case CRAWL_CATCOL_IMM_CHILD_CTIME_LED_COUNT:
        case CRAWL_CATCOL_IMM_CHILD_MIN_EFF_TIME:
        case CRAWL_CATCOL_IMM_CHILD_MAX_EFF_TIME:
            return CRAWL_CAT_IMM_CHILD;
        default:
            return CRAWL_CAT_SUBTREE;
    }
}

static int cat_grow(unsigned char **buf, size_t *cap, size_t need) {
    unsigned char *p;
    size_t nc;

    if (*cap >= need) return 0;
    nc = *cap ? *cap : 4096;
    while (nc < need) {
        if (nc > (SIZE_MAX / 2)) {
            nc = need;
            break;
        }
        nc *= 2;
    }
    p = (unsigned char *)realloc(*buf, nc);
    if (!p) return -1;
    *buf = p;
    *cap = nc;
    return 0;
}

static uint64_t cat_chunk_total_bytes(const bin_catchunk_hdr_t *h) {
    return (uint64_t)sizeof(bin_catchunk_hdr_t) +
           (uint64_t)h->column_count * (uint64_t)sizeof(bin_colchunk_hdr_t) + h->comp_bytes;
}

void crawl_bin_catalog_init_empty(crawl_bin_catalog_t *c) {
    if (!c) return;
    memset(c, 0, sizeof(*c));
}

void crawl_bin_catalog_free(crawl_bin_catalog_t *c) {
    if (!c) return;
    free(c->parent_dir_id);
    free(c->depth);
    free(c->name_len);
    free(c->name_comp);
    free(c->names_arena);
    free(c->cat_buf);
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

/* ---- writer -------------------------------------------------------------- */

typedef struct {
    uint64_t *vals; /* one chunk's values, staged before encoding */
    unsigned char *names;
    size_t names_cap;
    unsigned char *scratch;
    size_t scratch_cap;
    unsigned char *comp;
    size_t comp_cap;
    /* One chunk's payloads, back to back in directory order: the directory has
     * to be written before them, so they are held until it is. */
    unsigned char *pay;
    size_t pay_len;
    size_t pay_cap;
    int level;
    ZSTD_CCtx *cctx;
} cat_writer_t;

static void cat_writer_free(cat_writer_t *w) {
    free(w->vals);
    free(w->names);
    free(w->scratch);
    free(w->comp);
    free(w->pay);
    if (w->cctx) ZSTD_freeCCtx(w->cctx);
    memset(w, 0, sizeof(*w));
}

/* Compress src into w->comp. Returns compressed length, or (size_t)-1. */
static size_t cat_compress(cat_writer_t *w, const unsigned char *src, size_t len) {
    size_t bound, cs;

    if (len == 0) return 0;
    bound = ZSTD_compressBound(len);
    if (cat_grow(&w->comp, &w->comp_cap, bound) != 0) return (size_t)-1;
    if (!w->cctx) {
        w->cctx = ZSTD_createCCtx();
        if (!w->cctx) return (size_t)-1;
    }
    cs = ZSTD_compressCCtx(w->cctx, w->comp, w->comp_cap, src, len, w->level);
    if (ZSTD_isError(cs)) return (size_t)-1;
    return cs;
}

/* Widen one chunk's worth of `col` into w->vals. */
static void cat_stage_column(cat_writer_t *w, const crawl_bin_catalog_src_t *s, uint8_t col, uint64_t first,
                             uint32_t n) {
    const uint64_t *u64 = NULL;
    uint32_t i;

    switch (col) {
        case CRAWL_CATCOL_PARENT_DIR_ID: u64 = s->parent_dir_id; break;
        case CRAWL_CATCOL_IMM_CHILD_BYTES: u64 = s->imm_child_bytes; break;
        case CRAWL_CATCOL_IMM_CHILD_COUNT: u64 = s->imm_child_count; break;
        case CRAWL_CATCOL_IMM_CHILD_CTIME_LED_COUNT: u64 = s->imm_child_ctime_led_count; break;
        case CRAWL_CATCOL_IMM_CHILD_MIN_EFF_TIME: u64 = s->imm_child_min_eff_time; break;
        case CRAWL_CATCOL_IMM_CHILD_MAX_EFF_TIME: u64 = s->imm_child_max_eff_time; break;
        case CRAWL_CATCOL_SELF_BYTES: u64 = s->self_bytes; break;
        case CRAWL_CATCOL_SUBTREE_NLINK_GT1_COUNT: u64 = s->subtree_nlink_gt1_count; break;
        case CRAWL_CATCOL_SUBTREE_FILES: u64 = s->subtree_files; break;
        case CRAWL_CATCOL_SUBTREE_DIRS: u64 = s->subtree_dirs; break;
        case CRAWL_CATCOL_SUBTREE_SYMLINKS: u64 = s->subtree_symlinks; break;
        case CRAWL_CATCOL_DFS_INDEX: u64 = s->dfs_index; break;
        case CRAWL_CATCOL_DFS_SUBTREE_DIRS: u64 = s->dfs_subtree_dirs; break;
        case CRAWL_CATCOL_SUBTREE_BYTES: u64 = s->subtree_bytes; break;
        case CRAWL_CATCOL_SUBTREE_COUNT: u64 = s->subtree_count; break;
        case CRAWL_CATCOL_DEPTH:
            for (i = 0; i < n; i++) w->vals[i] = (uint64_t)s->depth[first + i];
            return;
        case CRAWL_CATCOL_NAME_LEN:
            for (i = 0; i < n; i++) w->vals[i] = (uint64_t)s->name_len[first + i];
            return;
        case CRAWL_CATCOL_FLAGS:
            for (i = 0; i < n; i++)
                w->vals[i] = s->self_present[first + i] ? (uint64_t)CRAWL_DIR_FLAG_SELF_RECORD : 0ULL;
            return;
        default: break;
    }

    if (u64) {
        for (i = 0; i < n; i++) w->vals[i] = u64[first + i];
        return;
    }
    /* An array the caller left NULL: zero is what v8 put on disk in the same
     * situation, and the only reader of an unfinalized tail expects it. */
    for (i = 0; i < n; i++) w->vals[i] = 0ULL;
}

static int cat_write_chunk(cat_writer_t *w, const crawl_bin_catalog_src_t *s, uint64_t first, uint32_t n,
                           FILE *fp, crawl_bin_catalog_fwrite_fn wfwrite) {
    bin_catchunk_hdr_t ch;
    bin_colchunk_hdr_t dir[CAT_COLUMN_COUNT];
    uint64_t total_raw = 0;
    uint32_t ci;

    memset(dir, 0, sizeof(dir));
    w->pay_len = 0;

    for (ci = 0; ci < CAT_COLUMN_COUNT; ci++) {
        uint8_t col = k_cat_columns[ci];
        uint8_t enc = 0, bw = 0;
        uint64_t mn = 0, mx = 0;
        size_t enc_len = 0, cs;
        const unsigned char *src;

        if (col == CRAWL_CATCOL_NAME_BYTES) {
            uint32_t i;

            for (i = 0; i < n; i++) {
                uint16_t nl = s->name_len[first + i];

                if (nl == 0U) continue;
                if (cat_grow(&w->names, &w->names_cap, enc_len + (size_t)nl) != 0) return -1;
                memcpy(w->names + enc_len, s->name_comp[first + i], (size_t)nl);
                enc_len += nl;
            }
            src = w->names;
            enc = CRAWL_BIN_CATALOG_COMPRESS_NAMES ? (uint8_t)CRAWL_ENC_BYTES : (uint8_t)CRAWL_ENC_BYTES_STORED;
        } else {
            cat_stage_column(w, s, col, first, n);
            if (crawl_bin_codec_encode_u64(w->vals, n, &w->scratch, &w->scratch_cap, &enc_len, &enc, &bw, &mn,
                                           &mx) != 0)
                return -1;
            src = w->scratch;
        }

        if (enc == (uint8_t)CRAWL_ENC_BYTES_STORED) {
            cs = enc_len;
        } else {
            cs = cat_compress(w, src, enc_len);
            if (cs == (size_t)-1) return -1;
            src = w->comp;
        }
        if (cs > 0xFFFFFFFFULL) return -1;
        if (cs > 0) {
            if (cat_grow(&w->pay, &w->pay_cap, w->pay_len + cs) != 0) return -1;
            memcpy(w->pay + w->pay_len, src, cs);
            w->pay_len += cs;
        }

        dir[ci].column_id = col;
        dir[ci].encoding = enc;
        dir[ci].bit_width = bw;
        dir[ci].comp_bytes = (uint32_t)cs;
        dir[ci].raw_bytes = (uint64_t)enc_len;
        dir[ci].min_value = mn;
        dir[ci].max_value = mx;

        total_raw += (uint64_t)enc_len;
    }

    memset(&ch, 0, sizeof(ch));
    ch.dir_count = n;
    ch.column_count = CAT_COLUMN_COUNT;
    ch.comp_bytes = (uint64_t)w->pay_len;
    ch.raw_bytes = total_raw;

    if (wfwrite(&ch, sizeof(ch), 1, fp) != 1) return -1;
    if (wfwrite(dir, sizeof(dir[0]), CAT_COLUMN_COUNT, fp) != CAT_COLUMN_COUNT) return -1;
    if (w->pay_len && wfwrite(w->pay, 1, w->pay_len, fp) != w->pay_len) return -1;
    return 0;
}

int crawl_bin_catalog_write(const crawl_bin_catalog_src_t *src, FILE *fp, crawl_bin_catalog_fwrite_fn wfwrite) {
    cat_writer_t w;
    bin_catalog_hdr_t hdr;
    uint64_t *chunk_off = NULL;
    uint64_t n, k, chunk_count;
    int rc = -1;

    if (!src || !fp || !wfwrite || src->n_entries == 0ULL) {
        errno = EINVAL;
        return -1;
    }
    memset(&w, 0, sizeof(w));
    w.level = CRAWL_BIN_ZSTD_DEFAULT_LEVEL;
    {
        const char *e = getenv("ECRAWL_ZSTD_LEVEL");

        if (e && *e) {
            char *end = NULL;
            long v = strtol(e, &end, 10);

            if (end && *end == '\0' && v >= 1 && v <= 22) w.level = (int)v;
        }
    }

    n = src->n_entries;
    chunk_count = (n + CRAWL_BIN_CATALOG_CHUNK_DIRS - 1ULL) / CRAWL_BIN_CATALOG_CHUNK_DIRS;
    if (chunk_count > 0xFFFFFFFFULL) {
        errno = EINVAL;
        return -1;
    }

    w.vals = (uint64_t *)malloc((size_t)CRAWL_BIN_CATALOG_CHUNK_DIRS * sizeof(uint64_t));
    chunk_off = (uint64_t *)malloc((size_t)chunk_count * sizeof(uint64_t));
    if (!w.vals || !chunk_off) goto done;

    if (wfwrite(&n, sizeof(n), 1, fp) != 1) goto done;
    memset(&hdr, 0, sizeof(hdr));
    hdr.chunk_count = (uint32_t)chunk_count;
    hdr.chunk_dirs = CRAWL_BIN_CATALOG_CHUNK_DIRS;
    if (wfwrite(&hdr, sizeof(hdr), 1, fp) != 1) goto done;

    for (k = 0; k < chunk_count; k++) {
        uint64_t first = k * CRAWL_BIN_CATALOG_CHUNK_DIRS + 1ULL;
        uint64_t left = n - (first - 1ULL);
        uint32_t cnt = (left < CRAWL_BIN_CATALOG_CHUNK_DIRS) ? (uint32_t)left : CRAWL_BIN_CATALOG_CHUNK_DIRS;
        off_t here = ftello(fp);

        if (here < 0) goto done;
        chunk_off[k] = (uint64_t)here;
        if (cat_write_chunk(&w, src, first, cnt, fp, wfwrite) != 0) goto done;
    }

    if (wfwrite(chunk_off, sizeof(chunk_off[0]), (size_t)chunk_count, fp) != (size_t)chunk_count) goto done;
    rc = 0;

done:
    free(chunk_off);
    cat_writer_free(&w);
    return rc;
}

/* ---- shared decode ------------------------------------------------------- */

/*
 * Decompress one column payload. The result is either the payload itself (a
 * stored blob is already what it decodes to) or *scratch, grown as needed.
 */
static int cat_pull_column(const bin_colchunk_hdr_t *ch, const unsigned char *payload, ZSTD_DCtx **dctx,
                           unsigned char **scratch, size_t *scratch_cap, const unsigned char **out,
                           size_t *out_len) {
    size_t got;

    *out = NULL;
    *out_len = 0;
    if (ch->comp_bytes == 0U) return ch->raw_bytes == 0ULL ? 0 : -1;
    if (ch->encoding == (uint8_t)CRAWL_ENC_BYTES_STORED) {
        if (ch->raw_bytes != (uint64_t)ch->comp_bytes) return -1;
        *out = payload;
        *out_len = ch->comp_bytes;
        return 0;
    }
    if (ch->raw_bytes > (uint64_t)SIZE_MAX) return -1;
    if (cat_grow(scratch, scratch_cap, (size_t)(ch->raw_bytes ? ch->raw_bytes : 1)) != 0) return -1;
    if (!*dctx) {
        *dctx = ZSTD_createDCtx();
        if (!*dctx) return -1;
    }
    got = ZSTD_decompressDCtx(*dctx, *scratch, *scratch_cap, payload, ch->comp_bytes);
    if (ZSTD_isError(got) || got != (size_t)ch->raw_bytes) return -1;
    *out = *scratch;
    *out_len = got;
    return 0;
}

/* ---- full load ----------------------------------------------------------- */

/*
 * Below this many catalog bytes, read with stdio instead of mapping.
 *
 * A mapping is not free to take down: munmap in a multi-threaded process sends a TLB shootdown IPI
 * to every CPU the process has run on, and that cost does not shrink with the mapping. On a crawl
 * split into 1019 small shards, ecrawl_query spent 22.5% of the run inside
 * crawl_bin_catalog_load_sel and crawl_bin_catalog_free -- 8.4% in munmap alone, with
 * smp_call_function_many_cond, tlb_is_not_lazy and flush_tlb_func together near a third of all
 * samples on a 32-thread run across 96 CPUs. A large catalog still wins from mapping, because the
 * column payloads a partial field selection skips are then never read from disk at all.
 */
#define CATALOG_MMAP_MIN_BYTES (1ULL << 20)

/* realloc one optional uint64 array only when its group was requested. */
static int cat_opt_grow(uint64_t **arr, int wanted, uint64_t slots) {
    uint64_t *p;

    if (!wanted) return 0;
    p = (uint64_t *)realloc(*arr, (size_t)slots * sizeof(uint64_t));
    if (!p) return -1;
    *arr = p;
    return 0;
}

/* Size the arrays for dir_ids 1..n. v9 catalogs are dense, so this is exact and
 * happens once, rather than doubling as ids arrive. */
static int catalog_reserve(crawl_bin_catalog_t *c, uint64_t n) {
    int want_imm = (c->fields & CRAWL_CAT_IMM_CHILD) != 0;
    int want_sub = (c->fields & CRAWL_CAT_SUBTREE) != 0;
    uint64_t slots = n + 1ULL;

    c->parent_dir_id = (uint64_t *)realloc(c->parent_dir_id, (size_t)slots * sizeof(uint64_t));
    c->depth = (uint32_t *)realloc(c->depth, (size_t)slots * sizeof(uint32_t));
    c->name_len = (uint16_t *)realloc(c->name_len, (size_t)slots * sizeof(uint16_t));
    c->name_comp = (char **)realloc(c->name_comp, (size_t)slots * sizeof(char *));
    if (!c->parent_dir_id || !c->depth || !c->name_len || !c->name_comp) return -1;

    if (cat_opt_grow(&c->imm_child_bytes, want_imm, slots) != 0 ||
        cat_opt_grow(&c->imm_child_count, want_imm, slots) != 0 ||
        cat_opt_grow(&c->imm_child_ctime_led_count, want_imm, slots) != 0 ||
        cat_opt_grow(&c->imm_child_min_eff_time, want_imm, slots) != 0 ||
        cat_opt_grow(&c->imm_child_max_eff_time, want_imm, slots) != 0 ||
        cat_opt_grow(&c->dfs_index, want_sub, slots) != 0 ||
        cat_opt_grow(&c->dfs_subtree_dirs, want_sub, slots) != 0 ||
        cat_opt_grow(&c->subtree_bytes, want_sub, slots) != 0 ||
        cat_opt_grow(&c->subtree_count, want_sub, slots) != 0 ||
        cat_opt_grow(&c->subtree_nlink_gt1_count, want_sub, slots) != 0 ||
        cat_opt_grow(&c->subtree_files, want_sub, slots) != 0 ||
        cat_opt_grow(&c->subtree_dirs, want_sub, slots) != 0 ||
        cat_opt_grow(&c->subtree_symlinks, want_sub, slots) != 0 ||
        cat_opt_grow(&c->self_bytes, want_sub, slots) != 0)
        return -1;
    if (want_sub) {
        unsigned char *sp = (unsigned char *)realloc(c->self_present, (size_t)slots);

        if (!sp) return -1;
        c->self_present = sp;
    }

    /* Slot 0 is not a directory and nothing reads it, but leaving it
     * uninitialized makes every array look uninitialized to a memory checker. */
    c->parent_dir_id[0] = 0ULL;
    c->depth[0] = 0U;
    c->name_len[0] = 0U;
    c->name_comp[0] = NULL;
    if (want_imm) {
        c->imm_child_bytes[0] = 0ULL;
        c->imm_child_count[0] = 0ULL;
        c->imm_child_ctime_led_count[0] = 0ULL;
        c->imm_child_min_eff_time[0] = UINT64_MAX;
        c->imm_child_max_eff_time[0] = 0ULL;
    }
    if (want_sub) {
        c->dfs_index[0] = 0ULL;
        c->dfs_subtree_dirs[0] = 0ULL;
        c->subtree_bytes[0] = 0ULL;
        c->subtree_count[0] = 0ULL;
        c->subtree_nlink_gt1_count[0] = 0ULL;
        c->subtree_files[0] = 0ULL;
        c->subtree_dirs[0] = 0ULL;
        c->subtree_symlinks[0] = 0ULL;
        c->self_bytes[0] = 0ULL;
        c->self_present[0] = 0U;
    }
    c->cap = n;
    return 0;
}

/*
 * Where a decoded column lands. The uint64 columns decode straight into the
 * catalog array, so a chunk costs no copy at all; the narrow ones go through a
 * scratch buffer because the codec only speaks uint64.
 */
typedef struct {
    uint64_t *u64;
    uint32_t *u32;
    uint16_t *u16;
    unsigned char *flag; /* CRAWL_DIR_FLAG_SELF_RECORD, unpacked to 0/1 */
} cat_col_target_t;

static void cat_column_target(const crawl_bin_catalog_t *c, uint8_t col, uint64_t first, cat_col_target_t *t) {
    memset(t, 0, sizeof(*t));
    switch (col) {
        case CRAWL_CATCOL_PARENT_DIR_ID: t->u64 = c->parent_dir_id + first; break;
        case CRAWL_CATCOL_DEPTH: t->u32 = c->depth + first; break;
        case CRAWL_CATCOL_NAME_LEN: t->u16 = c->name_len + first; break;
        case CRAWL_CATCOL_IMM_CHILD_BYTES: t->u64 = c->imm_child_bytes + first; break;
        case CRAWL_CATCOL_IMM_CHILD_COUNT: t->u64 = c->imm_child_count + first; break;
        case CRAWL_CATCOL_IMM_CHILD_CTIME_LED_COUNT: t->u64 = c->imm_child_ctime_led_count + first; break;
        case CRAWL_CATCOL_IMM_CHILD_MIN_EFF_TIME: t->u64 = c->imm_child_min_eff_time + first; break;
        case CRAWL_CATCOL_IMM_CHILD_MAX_EFF_TIME: t->u64 = c->imm_child_max_eff_time + first; break;
        case CRAWL_CATCOL_FLAGS: t->flag = c->self_present + first; break;
        case CRAWL_CATCOL_DFS_INDEX: t->u64 = c->dfs_index + first; break;
        case CRAWL_CATCOL_DFS_SUBTREE_DIRS: t->u64 = c->dfs_subtree_dirs + first; break;
        case CRAWL_CATCOL_SUBTREE_BYTES: t->u64 = c->subtree_bytes + first; break;
        case CRAWL_CATCOL_SUBTREE_COUNT: t->u64 = c->subtree_count + first; break;
        case CRAWL_CATCOL_SUBTREE_NLINK_GT1_COUNT: t->u64 = c->subtree_nlink_gt1_count + first; break;
        case CRAWL_CATCOL_SUBTREE_FILES: t->u64 = c->subtree_files + first; break;
        case CRAWL_CATCOL_SUBTREE_DIRS: t->u64 = c->subtree_dirs + first; break;
        case CRAWL_CATCOL_SUBTREE_SYMLINKS: t->u64 = c->subtree_symlinks + first; break;
        case CRAWL_CATCOL_SELF_BYTES: t->u64 = c->self_bytes + first; break;
        default: break;
    }
}

/* One chunk's header, column directory and payload base within a byte view. */
typedef struct {
    const bin_catchunk_hdr_t *hdr;
    const bin_colchunk_hdr_t *dir;
    const unsigned char *payload;
} cat_chunk_view_t;

static int cat_chunk_parse(const unsigned char *base, uint64_t avail, cat_chunk_view_t *cv) {
    uint64_t dir_bytes;

    if (avail < sizeof(bin_catchunk_hdr_t)) return -1;
    cv->hdr = (const bin_catchunk_hdr_t *)(const void *)base;
    if (cv->hdr->column_count == 0U || cv->hdr->column_count > 64U) return -1;
    if (cat_chunk_total_bytes(cv->hdr) > avail) return -1;
    dir_bytes = (uint64_t)cv->hdr->column_count * (uint64_t)sizeof(bin_colchunk_hdr_t);
    cv->dir = (const bin_colchunk_hdr_t *)(const void *)(base + sizeof(bin_catchunk_hdr_t));
    cv->payload = base + sizeof(bin_catchunk_hdr_t) + dir_bytes;
    return 0;
}

/*
 * Point name_comp at this chunk's slice of the name blob. `names` must outlive
 * the catalog: it is either the arena or the mapped bytes themselves.
 */
static int cat_bind_names(crawl_bin_catalog_t *c, uint64_t first, uint32_t n, char *names, size_t names_len) {
    size_t pos = 0;
    uint32_t i;

    for (i = 0; i < n; i++) {
        uint16_t nl = c->name_len[first + i];

        if (nl == 0U) {
            c->name_comp[first + i] = NULL;
            continue;
        }
        if (pos + (size_t)nl > names_len) return -1;
        c->name_comp[first + i] = names + pos;
        pos += nl;
    }
    /* The lengths must tile the blob exactly, or every name past the
     * disagreement is silently off by however many bytes are missing. */
    return pos == names_len ? 0 : -1;
}

int crawl_bin_catalog_load(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, crawl_bin_catalog_t *out) {
    return crawl_bin_catalog_load_sel(fp, catalog_offset, file_sz, CRAWL_CAT_ALL, out);
}

int crawl_bin_catalog_load_sel(FILE *fp, uint64_t catalog_offset, uint64_t file_sz, unsigned fields,
                               crawl_bin_catalog_t *out) {
    const unsigned char *cat = NULL; /* the catalog region, based at catalog_offset */
    uint64_t cat_len, n, k, pos;
    uint64_t names_total = 0, names_pos = 0;
    bin_catalog_hdr_t hdr;
    unsigned char *scratch = NULL;
    size_t scratch_cap = 0;
    uint64_t *narrow = NULL;
    ZSTD_DCtx *dctx = NULL;
    int borrow_names = 1;
    int rc = -1;

    crawl_bin_catalog_init_empty(out);
    out->fields = fields & CRAWL_CAT_ALL;
    if (!fp || catalog_offset > file_sz ||
        file_sz - catalog_offset < sizeof(uint64_t) + sizeof(bin_catalog_hdr_t)) {
        errno = EINVAL;
        return -1;
    }
    cat_len = file_sz - catalog_offset;
    if (cat_len > (uint64_t)SIZE_MAX) {
        errno = EINVAL;
        return -1;
    }

    if (cat_len >= CATALOG_MMAP_MIN_BYTES) {
        long page = sysconf(_SC_PAGESIZE);
        int fd = fileno(fp);

        if (page > 0 && fd >= 0) {
            uint64_t base_off = catalog_offset & ~((uint64_t)page - 1ULL);
            uint64_t map_len = file_sz - base_off;

            if (map_len <= (uint64_t)SIZE_MAX) {
                unsigned char *map =
                    (unsigned char *)mmap(NULL, (size_t)map_len, PROT_READ, MAP_PRIVATE, fd, (off_t)base_off);

                if (map != MAP_FAILED) {
                    out->map_base = map;
                    out->map_len = (size_t)map_len;
                    cat = map + (catalog_offset - base_off);
                }
            }
        }
    }
    if (!cat) {
        /* Compressed, so even a big catalog is a modest read, and a heap copy
         * costs no TLB shootdown when it is released. */
        out->cat_buf = (unsigned char *)malloc((size_t)cat_len);
        if (!out->cat_buf) goto fail;
        if (fseeko(fp, (off_t)catalog_offset, SEEK_SET) != 0) goto fail;
        if (fread(out->cat_buf, 1, (size_t)cat_len, fp) != (size_t)cat_len) goto fail;
        cat = out->cat_buf;
    }

    memcpy(&n, cat, sizeof(n));
    memcpy(&hdr, cat + sizeof(uint64_t), sizeof(hdr));
    if (n == 0ULL || n > (uint64_t)(1ULL << 28) || hdr.chunk_dirs == 0U || hdr.chunk_count == 0U) goto fail;
    if ((uint64_t)hdr.chunk_count != (n + (uint64_t)hdr.chunk_dirs - 1ULL) / (uint64_t)hdr.chunk_dirs) goto fail;

    if (catalog_reserve(out, n) != 0) goto fail;
    out->max_dir_id = n;

    /*
     * Names need one span that outlives the chunk loop. A stored blob already
     * has one -- the catalog bytes themselves -- so only a compressed one gets
     * an arena, sized here from the column directories rather than grown.
     *
     * Both passes walk the chunks back to back rather than through the trailing
     * offset table, which is only there for single-row reads. That keeps a
     * whole-catalog load independent of where the file ends, so a shard with
     * bytes appended after its catalog still loads: the sidecar retires itself
     * over a size change, and the fallback this leaves is expected to be right.
     */
    pos = sizeof(uint64_t) + sizeof(hdr);
    for (k = 0; k < hdr.chunk_count; k++) {
        cat_chunk_view_t cv;
        uint32_t ci;

        if (cat_chunk_parse(cat + pos, cat_len - pos, &cv) != 0) goto fail;
        pos += cat_chunk_total_bytes(cv.hdr);
        for (ci = 0; ci < cv.hdr->column_count; ci++) {
            if (cv.dir[ci].column_id != (uint8_t)CRAWL_CATCOL_NAME_BYTES) continue;
            if (cv.dir[ci].encoding != (uint8_t)CRAWL_ENC_BYTES_STORED) borrow_names = 0;
            names_total += cv.dir[ci].raw_bytes;
            break;
        }
    }
    if (!borrow_names && names_total > 0ULL) {
        if (names_total > (uint64_t)SIZE_MAX) goto fail;
        out->names_arena = (char *)malloc((size_t)names_total);
        if (!out->names_arena) goto fail;
    }
    narrow = (uint64_t *)malloc((size_t)hdr.chunk_dirs * sizeof(uint64_t));
    if (!narrow) goto fail;

    pos = sizeof(uint64_t) + sizeof(hdr);
    for (k = 0; k < hdr.chunk_count; k++) {
        cat_chunk_view_t cv;
        uint64_t first = k * (uint64_t)hdr.chunk_dirs + 1ULL;
        uint64_t left = n - (first - 1ULL);
        uint32_t want = (left < (uint64_t)hdr.chunk_dirs) ? (uint32_t)left : hdr.chunk_dirs;
        const unsigned char *p;
        uint32_t ci;

        if (cat_chunk_parse(cat + pos, cat_len - pos, &cv) != 0) goto fail;
        pos += cat_chunk_total_bytes(cv.hdr);
        if (cv.hdr->dir_count != want) goto fail;

        p = cv.payload;
        for (ci = 0; ci < cv.hdr->column_count; ci++) {
            const bin_colchunk_hdr_t *ch = &cv.dir[ci];
            uint8_t col = ch->column_id;
            unsigned group;
            const unsigned char *raw = NULL;
            size_t raw_len = 0;
            cat_col_target_t t;
            uint32_t i;

            if (col >= (uint8_t)CRAWL_CATCOL__COUNT) {
                p += ch->comp_bytes;
                continue;
            }
            group = cat_column_group(col);
            if (group != 0U && !(out->fields & group)) {
                p += ch->comp_bytes;
                continue;
            }
            if (cat_pull_column(ch, p, &dctx, &scratch, &scratch_cap, &raw, &raw_len) != 0) goto fail;
            p += ch->comp_bytes;

            if (col == (uint8_t)CRAWL_CATCOL_NAME_BYTES) {
                char *dst;

                if (borrow_names) {
                    dst = (char *)(uintptr_t)raw;
                } else {
                    if (names_pos + raw_len > names_total) goto fail;
                    dst = out->names_arena + names_pos;
                    if (raw_len) memcpy(dst, raw, raw_len);
                    names_pos += raw_len;
                }
                if (cat_bind_names(out, first, want, dst, raw_len) != 0) goto fail;
                continue;
            }

            cat_column_target(out, col, first, &t);
            if (t.u64) {
                if (crawl_bin_codec_decode_u64(raw, raw_len, want, ch->encoding, ch->bit_width, ch->min_value,
                                               t.u64) != 0)
                    goto fail;
                continue;
            }
            if (!t.u32 && !t.u16 && !t.flag) continue;
            if (crawl_bin_codec_decode_u64(raw, raw_len, want, ch->encoding, ch->bit_width, ch->min_value,
                                           narrow) != 0)
                goto fail;
            if (t.u32)
                for (i = 0; i < want; i++) t.u32[i] = (uint32_t)narrow[i];
            else if (t.u16)
                for (i = 0; i < want; i++) t.u16[i] = (uint16_t)narrow[i];
            else
                for (i = 0; i < want; i++)
                    t.flag[i] = (narrow[i] & (uint64_t)CRAWL_DIR_FLAG_SELF_RECORD) ? 1U : 0U;
        }
    }
    /* Leave the stream past the catalog, where the v8 loader left it: callers
     * reuse the handle. */
    (void)fseeko(fp, (off_t)file_sz, SEEK_SET);
    rc = 0;

fail:
    free(scratch);
    free(narrow);
    if (dctx) ZSTD_freeDCtx(dctx);
    if (rc != 0) {
        crawl_bin_catalog_free(out);
        errno = EINVAL;
    }
    return rc;
}

/* ---- single-row access --------------------------------------------------- */

int crawl_bin_catalog_map_read(int fd, uint64_t catalog_offset, uint64_t file_sz, crawl_bin_catalog_map_t *m) {
    unsigned char head[sizeof(uint64_t) + sizeof(bin_catalog_hdr_t)];
    bin_catalog_hdr_t hdr;
    uint64_t n, table_off, table_bytes;

    memset(m, 0, sizeof(*m));
    if (fd < 0 || catalog_offset > file_sz || file_sz - catalog_offset < sizeof(head)) return -1;
    if (pread(fd, head, sizeof(head), (off_t)catalog_offset) != (ssize_t)sizeof(head)) return -1;
    memcpy(&n, head, sizeof(n));
    memcpy(&hdr, head + sizeof(uint64_t), sizeof(hdr));
    if (n == 0ULL || n > (uint64_t)(1ULL << 28) || hdr.chunk_dirs == 0U || hdr.chunk_count == 0U) return -1;
    if ((uint64_t)hdr.chunk_count != (n + (uint64_t)hdr.chunk_dirs - 1ULL) / (uint64_t)hdr.chunk_dirs) return -1;

    table_bytes = (uint64_t)hdr.chunk_count * sizeof(uint64_t);
    if (table_bytes > file_sz - catalog_offset) return -1;
    table_off = file_sz - table_bytes;

    m->chunk_off = (uint64_t *)malloc((size_t)table_bytes);
    if (!m->chunk_off) return -1;
    if (pread(fd, m->chunk_off, (size_t)table_bytes, (off_t)table_off) != (ssize_t)table_bytes) {
        free(m->chunk_off);
        memset(m, 0, sizeof(*m));
        return -1;
    }
    m->chunk_count = hdr.chunk_count;
    m->chunk_dirs = hdr.chunk_dirs;
    m->n_entries = n;
    m->file_sz = file_sz;
    return 0;
}

void crawl_bin_catalog_map_free(crawl_bin_catalog_map_t *m) {
    if (!m) return;
    free(m->chunk_off);
    memset(m, 0, sizeof(*m));
}

void crawl_bin_catalog_chunk_init(crawl_bin_catalog_chunk_t *c) {
    if (c) memset(c, 0, sizeof(*c));
}

void crawl_bin_catalog_chunk_free(crawl_bin_catalog_chunk_t *c) {
    int i;

    if (!c) return;
    for (i = 0; i < CRAWL_CATCOL__COUNT; i++) free(c->col[i]);
    free(c->names);
    free(c->name_off);
    free(c->comp);
    free(c->raw);
    if (c->dctx) ZSTD_freeDCtx((ZSTD_DCtx *)c->dctx);
    memset(c, 0, sizeof(*c));
}

static int cat_chunk_col_reserve(crawl_bin_catalog_chunk_t *c, uint8_t col, uint32_t n) {
    uint64_t *p;

    if (c->col_cap[col] >= (size_t)n) return 0;
    p = (uint64_t *)realloc(c->col[col], (size_t)n * sizeof(uint64_t));
    if (!p) return -1;
    c->col[col] = p;
    c->col_cap[col] = n;
    return 0;
}

/* Read and decode one chunk into *c, keeping only the requested field groups. */
static int cat_chunk_load(int fd, const crawl_bin_catalog_map_t *m, crawl_bin_catalog_chunk_t *c,
                          uint32_t index, unsigned fields) {
    bin_catchunk_hdr_t hdr;
    bin_colchunk_hdr_t *dir = NULL;
    uint64_t off = m->chunk_off[index];
    uint64_t first = (uint64_t)index * (uint64_t)m->chunk_dirs + 1ULL;
    uint64_t left = m->n_entries - (first - 1ULL);
    uint32_t want = (left < (uint64_t)m->chunk_dirs) ? (uint32_t)left : m->chunk_dirs;
    size_t dir_bytes;
    uint64_t pos;
    uint32_t ci;
    ZSTD_DCtx *dctx = (ZSTD_DCtx *)c->dctx;
    int rc = -1;

    c->first_dir_id = 0ULL;
    if (off < sizeof(bin_file_header_t) || off >= m->file_sz) return -1;
    if (m->file_sz - off < sizeof(hdr)) return -1;
    if (pread(fd, &hdr, sizeof(hdr), (off_t)off) != (ssize_t)sizeof(hdr)) return -1;
    if (hdr.dir_count != want || hdr.column_count == 0U || hdr.column_count > 64U) return -1;
    if (cat_chunk_total_bytes(&hdr) > m->file_sz - off) return -1;

    dir_bytes = (size_t)hdr.column_count * sizeof(*dir);
    dir = (bin_colchunk_hdr_t *)malloc(dir_bytes);
    if (!dir) return -1;
    if (pread(fd, dir, dir_bytes, (off_t)(off + sizeof(hdr))) != (ssize_t)dir_bytes) goto done;

    pos = off + sizeof(hdr) + dir_bytes;
    c->names_len = 0;

    for (ci = 0; ci < hdr.column_count; ci++) {
        const bin_colchunk_hdr_t *ch = &dir[ci];
        uint8_t col = ch->column_id;
        unsigned group;
        const unsigned char *raw = NULL;
        size_t raw_len = 0;

        if (col >= (uint8_t)CRAWL_CATCOL__COUNT) {
            pos += ch->comp_bytes;
            continue;
        }
        group = cat_column_group(col);
        if (group != 0U && !(fields & group)) {
            pos += ch->comp_bytes;
            continue;
        }
        if (ch->comp_bytes > 0U) {
            if (cat_grow(&c->comp, &c->comp_cap, ch->comp_bytes) != 0) goto done;
            if (pread(fd, c->comp, ch->comp_bytes, (off_t)pos) != (ssize_t)ch->comp_bytes) goto done;
        }
        if (cat_pull_column(ch, c->comp, &dctx, &c->raw, &c->raw_cap, &raw, &raw_len) != 0) goto done;
        pos += ch->comp_bytes;

        if (col == (uint8_t)CRAWL_CATCOL_NAME_BYTES) {
            /* Copied rather than kept as a pointer into c->raw, which the next
             * column's decode overwrites. */
            if (cat_grow(&c->names, &c->names_cap, raw_len ? raw_len : 1) != 0) goto done;
            if (raw_len) memcpy(c->names, raw, raw_len);
            c->names_len = raw_len;
            continue;
        }
        if (cat_chunk_col_reserve(c, col, want) != 0) goto done;
        if (crawl_bin_codec_decode_u64(raw, raw_len, want, ch->encoding, ch->bit_width, ch->min_value,
                                       c->col[col]) != 0)
            goto done;
    }

    /* Names are sliced by a prefix sum over the lengths, built once per chunk so
     * a row read is a lookup rather than a walk of every earlier name. */
    if (!c->col[CRAWL_CATCOL_NAME_LEN]) goto done;
    if (c->name_off_cap < (size_t)want + 1U) {
        uint64_t *p = (uint64_t *)realloc(c->name_off, ((size_t)want + 1U) * sizeof(uint64_t));

        if (!p) goto done;
        c->name_off = p;
        c->name_off_cap = (size_t)want + 1U;
    }
    {
        uint64_t noff = 0;

        for (ci = 0; ci < want; ci++) {
            c->name_off[ci] = noff;
            noff += c->col[CRAWL_CATCOL_NAME_LEN][ci];
        }
        c->name_off[want] = noff;
        /* The lengths must tile the blob exactly, or every name past the
         * disagreement is silently off by however many bytes are missing. */
        if (noff != (uint64_t)c->names_len) goto done;
    }

    c->first_dir_id = first;
    c->dir_count = want;
    c->fields = fields;
    rc = 0;

done:
    c->dctx = dctx;
    free(dir);
    return rc;
}

int crawl_bin_catalog_read_row(int fd, const crawl_bin_catalog_map_t *m, crawl_bin_catalog_chunk_t *c,
                               uint64_t dir_id, unsigned fields, bin_dir_catalog_entry_t *ent,
                               const unsigned char **name, size_t *name_len_out, int *decoded_out) {
    uint32_t index, slot;
    size_t nlen = 0;

    if (name) *name = NULL;
    if (name_len_out) *name_len_out = 0;
    if (decoded_out) *decoded_out = 0;
    if (!m || !c || !ent || dir_id == 0ULL || dir_id > m->n_entries) return -1;

    index = (uint32_t)((dir_id - 1ULL) / (uint64_t)m->chunk_dirs);
    if (index >= m->chunk_count) return -1;
    fields &= CRAWL_CAT_ALL;
    if (c->first_dir_id == 0ULL || dir_id < c->first_dir_id ||
        dir_id - c->first_dir_id >= (uint64_t)c->dir_count || c->fields != fields) {
        if (cat_chunk_load(fd, m, c, index, fields) != 0) return -1;
        if (decoded_out) *decoded_out = 1;
    }
    slot = (uint32_t)(dir_id - c->first_dir_id);

    memset(ent, 0, sizeof(*ent));
    ent->dir_id = dir_id;
#define CAT_AT(which) (c->col[which] ? c->col[which][slot] : 0ULL)
    ent->parent_dir_id = CAT_AT(CRAWL_CATCOL_PARENT_DIR_ID);
    ent->depth = (uint32_t)CAT_AT(CRAWL_CATCOL_DEPTH);
    ent->name_len = (uint16_t)CAT_AT(CRAWL_CATCOL_NAME_LEN);
    if (fields & CRAWL_CAT_IMM_CHILD) {
        ent->imm_child_bytes = CAT_AT(CRAWL_CATCOL_IMM_CHILD_BYTES);
        ent->imm_child_count = CAT_AT(CRAWL_CATCOL_IMM_CHILD_COUNT);
        ent->imm_child_ctime_led_count = CAT_AT(CRAWL_CATCOL_IMM_CHILD_CTIME_LED_COUNT);
        ent->imm_child_min_eff_time = CAT_AT(CRAWL_CATCOL_IMM_CHILD_MIN_EFF_TIME);
        ent->imm_child_max_eff_time = CAT_AT(CRAWL_CATCOL_IMM_CHILD_MAX_EFF_TIME);
    }
    if (fields & CRAWL_CAT_SUBTREE) {
        ent->flags = (uint16_t)CAT_AT(CRAWL_CATCOL_FLAGS);
        ent->dfs_index = CAT_AT(CRAWL_CATCOL_DFS_INDEX);
        ent->dfs_subtree_dirs = CAT_AT(CRAWL_CATCOL_DFS_SUBTREE_DIRS);
        ent->subtree_bytes = CAT_AT(CRAWL_CATCOL_SUBTREE_BYTES);
        ent->subtree_count = CAT_AT(CRAWL_CATCOL_SUBTREE_COUNT);
        ent->subtree_nlink_gt1_count = CAT_AT(CRAWL_CATCOL_SUBTREE_NLINK_GT1_COUNT);
        ent->subtree_files = CAT_AT(CRAWL_CATCOL_SUBTREE_FILES);
        ent->subtree_dirs = CAT_AT(CRAWL_CATCOL_SUBTREE_DIRS);
        ent->subtree_symlinks = CAT_AT(CRAWL_CATCOL_SUBTREE_SYMLINKS);
        ent->self_bytes = CAT_AT(CRAWL_CATCOL_SELF_BYTES);
    }
#undef CAT_AT

    if (ent->name_len > 0U) {
        uint64_t noff = c->name_off[slot];

        nlen = (size_t)(c->name_off[slot + 1U] - noff);
        if (nlen != (size_t)ent->name_len || noff + nlen > c->names_len) return -1;
        if (name) *name = c->names + noff;
    }
    if (name_len_out) *name_len_out = nlen;
    return 0;
}

/* ---- path reconstruction ------------------------------------------------- */

int crawl_bin_catalog_dir_path_len(const crawl_bin_catalog_t *c, uint64_t dir_id, char *out, size_t out_sz,
                                   size_t *len_out) {
    size_t nparts = 0;
    size_t parts_len[CRAWL_BIN_CATALOG_MAX_PATH_PARTS];
    const char *parts_ptr[CRAWL_BIN_CATALOG_MAX_PATH_PARTS];
    uint64_t cur = dir_id;
    size_t tot = 0;
    size_t pi;

    if (!c || !out || out_sz == 0) return -1;
    if (len_out) *len_out = 0;
    out[0] = '\0';
    if (dir_id == 0 || dir_id > c->max_dir_id) return -1;

    while (cur != 0 && nparts < CRAWL_BIN_CATALOG_MAX_PATH_PARTS) {
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
