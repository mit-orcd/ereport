/*
 * ereport_sunburst — see ereport_sunburst.h for the data-flow overview.
 *
 * Three stages: per-shard bottom-up rollup of the scan-time accumulators (self
 * totals -> subtree totals), a top-down merge that walks the shard catalogs'
 * child indexes from the collapsed content root and materializes only the
 * nodes the chart will show (top-N + min-fraction trim applied during the
 * walk, trimmed children folded into "(other)" nodes), and emission of
 * sunburst.json / sunburst.html. Because selection happens before interning,
 * memory tracks the emitted chart (bounded by SUNBURST_NODE_BUDGET), not the
 * number of directories within depth_max of the root.
 */
#define _GNU_SOURCE /* qsort_r: comparators take their table as context, so builds and
                       writes are safe to run concurrently (per-user trees) */
#include "ereport_sunburst.h"
#include "compat_qsort_r.h"

#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* Children kept per node: in the top N by bytes or by files (union), and above
 * SUNBURST_MIN_FRAC of their parent in at least one metric; the rest fold into
 * an "(other)" leaf. SUNBURST_NODE_BUDGET is the hard backstop against
 * multiplicative blowup on trees that are wide at every level. */
#define SUNBURST_TOP_N 12
#define SUNBURST_MIN_FRAC 0.001
#define SUNBURST_NODE_BUDGET 65536

int ereport_sunburst_accum_init(ereport_sunburst_accum_t *a, uint64_t max_dir_id) {
    if (!a) return -1;
    a->bytes = NULL;
    a->files = NULL;
    a->max_dir_id = 0;
    if (max_dir_id == 0) return 0;
    a->bytes = malloc(((size_t)max_dir_id + 1) * sizeof(*a->bytes));
    a->files = malloc(((size_t)max_dir_id + 1) * sizeof(*a->files));
    if (!a->bytes || !a->files) {
        ereport_sunburst_accum_free(a);
        return -1;
    }
    /* Write-first zeroing (see sb_child_index_build): the rollup reads every
     * slot before adding into parents, which on calloc's lazily mapped pages
     * would turn each first write into a copy-on-write TLB-flush broadcast. */
    memset(a->bytes, 0, ((size_t)max_dir_id + 1) * sizeof(*a->bytes));
    memset(a->files, 0, ((size_t)max_dir_id + 1) * sizeof(*a->files));
    a->max_dir_id = max_dir_id;
    return 0;
}

void ereport_sunburst_accum_free(ereport_sunburst_accum_t *a) {
    if (!a) return;
    free(a->bytes);
    free(a->files);
    a->bytes = NULL;
    a->files = NULL;
    a->max_dir_id = 0;
}

/* ------------------------------------------------------------------ */
/* Merged tree                                                         */
/* ------------------------------------------------------------------ */

typedef struct {
    char *path;              /* full display path (after --path-rewrite); owned */
    const char *name;        /* last path component: points into path, or the static "(other)" */
    uint64_t total_bytes;    /* subtree totals, summed across shards */
    uint64_t total_files;
    int32_t parent;          /* node index; -1 for the displayed root */
    int32_t first_child;     /* linked list through next_sibling */
    int32_t next_sibling;
    uint32_t depth;          /* levels below the displayed root; root = 0 */
} sb_node_t;

struct ereport_sunburst_tree {
    sb_node_t *nodes;
    size_t n;
    size_t cap;
    size_t budget;           /* interned-node cap (excluding "(other)" folds) */
    int32_t root;            /* displayed root after single-child collapse */
    unsigned depth_max;
    /* Self totals of the ancestors the collapse skipped; credited to the
     * displayed root at emit time so the grand total stays exact. */
    uint64_t root_boost_bytes;
    uint64_t root_boost_files;
    /* --sunburst-buckets: per-node 6x6 (age x size, row-major) matrices, filled
     * by the second pass in ereport.c; plus, per shard, a dense dir_id -> node
     * map resolving every directory to its deepest materialized ancestor
     * (trimmed/depth-folded subtrees resolve to their fold node, so every
     * record lands on exactly one node and each node's matrix sums to the
     * "bytes"/"files" the JSON emits for it). NULL/0 when buckets are off. */
    _Atomic uint64_t *bucket_bytes; /* n * 36 */
    _Atomic uint64_t *bucket_files; /* n * 36 */
    uint32_t **dir_node;            /* [dir_node_n == cats length] per-shard maps, NULL per skipped shard */
    size_t dir_node_n;
};

/* One shard's catalog plus the post-rollup accumulator and a child index
 * (first_child/next_sibling over dir_ids) so a node's children enumerate in
 * O(fanout) instead of a full catalog scan. 8 bytes per dir, per shard;
 * uint32 is enough because a build is refused past UINT32_MAX dirs in one
 * shard (see below). */
typedef struct {
    const crawl_bin_catalog_t *cat;
    ereport_sunburst_accum_t *acc;
    uint32_t *first_child;
    uint32_t *next_sibling;
    uint32_t fi; /* index in the caller's cats[] array (ereport's file_index) */
    int borrowed_index; /* first_child/next_sibling belong to a caller workspace: do not free */
} sb_shard_t;

typedef struct {
    uint32_t shard;
    uint64_t lid;            /* shard-local dir_id */
} sb_lid_t;

/* One merged child while a node's children are being enumerated: the combined
 * subtree totals plus every shard-local dir_id behind it (the same name can
 * appear in several shards, and twice in one shard via duplicate catalog
 * inserts). The first lid is inline; multi-shard names allocate. */
typedef struct {
    const char *name;        /* catalog arena; NOT NUL-terminated, name_len bytes */
    uint32_t name_len;
    uint64_t bytes, files;   /* combined subtree totals */
    uint32_t first_shard;
    uint64_t first_lid;
    sb_lid_t *more;
    size_t n_more, cap_more;
    int32_t next;            /* hash chain */
} sb_cagg_t;

typedef struct {
    sb_cagg_t *ents;
    size_t n, cap;
    int32_t *buckets;        /* FNV(name) chains; 0 = empty, else entry index + 1 */
    size_t bcap;
} sb_cmap_t;

/* Materialization recorder: while the top-down merge walks, every shard-local
 * dir_id behind each interned node (and each "(other)" fold) is appended here.
 * After the walk, the dense per-shard dir_id -> node maps are derived from
 * these anchors with one forward pass per shard (dir_ids are handed out
 * parent-first, so an unmapped directory inherits its parent's node). */
typedef struct {
    uint32_t shard; /* compacted shard index */
    uint32_t lid;   /* shard-local dir_id */
    int32_t node;
} sb_mapent_t;

typedef struct {
    sb_mapent_t *v;
    size_t n, cap;
} sb_maprec_t;

static int sb_maprec_add(sb_maprec_t *mr, uint32_t shard, uint64_t lid, int32_t node) {
    if (mr->n == mr->cap) {
        size_t nc = mr->cap ? mr->cap * 2 : 1024;
        sb_mapent_t *nv = realloc(mr->v, nc * sizeof(*nv));
        if (!nv) return -1;
        mr->v = nv;
        mr->cap = nc;
    }
    mr->v[mr->n++] = (sb_mapent_t){ shard, (uint32_t)lid, node };
    return 0;
}

static int sb_maprec_add_entry(sb_maprec_t *mr, const sb_cagg_t *e, int32_t node) {
    if (sb_maprec_add(mr, e->first_shard, e->first_lid, node) != 0) return -1;
    for (size_t i = 0; i < e->n_more; i++)
        if (sb_maprec_add(mr, e->more[i].shard, e->more[i].lid, node) != 0) return -1;
    return 0;
}

static void sb_cmap_init(sb_cmap_t *cm) { memset(cm, 0, sizeof(*cm)); }

static void sb_cmap_free(sb_cmap_t *cm) {
    for (size_t i = 0; i < cm->n; i++) free(cm->ents[i].more);
    free(cm->ents);
    free(cm->buckets);
    sb_cmap_init(cm);
}

static uint64_t sb_hash_name(const char *s, uint32_t len) {
    uint64_t h = 1469598103934665603ULL; /* FNV-1a */
    for (uint32_t i = 0; i < len; i++) {
        h ^= (unsigned char)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static int sb_cmap_grow(sb_cmap_t *cm) {
    size_t ncap = cm->bcap ? cm->bcap * 2 : 64;
    int32_t *nb = calloc(ncap, sizeof(*nb));
    if (!nb) return -1;
    for (size_t i = 0; i < cm->bcap; i++) {
        int32_t e = cm->buckets[i];
        while (e) {
            int32_t nxt = cm->ents[e - 1].next;
            size_t j = (size_t)sb_hash_name(cm->ents[e - 1].name, cm->ents[e - 1].name_len) & (ncap - 1);
            cm->ents[e - 1].next = nb[j];
            nb[j] = e;
            e = nxt;
        }
    }
    free(cm->buckets);
    cm->buckets = nb;
    cm->bcap = ncap;
    return 0;
}

/* Add (shard, lid)'s subtree totals to the entry for name, creating it if needed. */
static int sb_cmap_add(sb_cmap_t *cm, const char *name, uint32_t name_len,
                       uint32_t shard, uint64_t lid, uint64_t b, uint64_t f) {
    sb_cagg_t *e;
    size_t mask, i;

    if (!cm->bcap && sb_cmap_grow(cm) != 0) return -1;
    mask = cm->bcap - 1;
    i = (size_t)sb_hash_name(name, name_len) & mask;
    for (int32_t c = cm->buckets[i]; c; c = cm->ents[c - 1].next) {
        e = &cm->ents[c - 1];
        if (e->name_len == name_len && memcmp(e->name, name, name_len) == 0) {
            e->bytes += b;
            e->files += f;
            if (e->n_more == e->cap_more) {
                size_t nc = e->cap_more ? e->cap_more * 2 : 4;
                sb_lid_t *nm = realloc(e->more, nc * sizeof(*nm));
                if (!nm) return -1;
                e->more = nm;
                e->cap_more = nc;
            }
            e->more[e->n_more++] = (sb_lid_t){ shard, lid };
            return 0;
        }
    }
    if ((cm->n + 1) * 10 >= cm->bcap * 7) {
        if (sb_cmap_grow(cm) != 0) return -1;
    }
    if (cm->n == cm->cap) {
        size_t nc = cm->cap ? cm->cap * 2 : 64;
        sb_cagg_t *ne = realloc(cm->ents, nc * sizeof(*ne));
        if (!ne) return -1;
        cm->ents = ne;
        cm->cap = nc;
    }
    e = &cm->ents[cm->n];
    e->name = name;
    e->name_len = name_len;
    e->bytes = b;
    e->files = f;
    e->first_shard = shard;
    e->first_lid = lid;
    e->more = NULL;
    e->n_more = 0;
    e->cap_more = 0;
    mask = cm->bcap - 1;
    i = (size_t)sb_hash_name(name, name_len) & mask;
    e->next = cm->buckets[i];
    cm->buckets[i] = (int32_t)cm->n + 1;
    cm->n++;
    return 0;
}

/* Aggregate the content-bearing children of the directory represented by
 * lids[] (one merged node, possibly several shard-local dir_ids) into cm.
 * Zero-content children are skipped: they would be dropped at emit anyway. */
static int sb_enum_children(const sb_shard_t *sh,
                            const sb_lid_t *lids, size_t n_lids, sb_cmap_t *cm) {
    for (size_t i = 0; i < n_lids; i++) {
        const sb_shard_t *s = &sh[lids[i].shard];
        const crawl_bin_catalog_t *cat = s->cat;
        uint64_t lid = lids[i].lid;
        uint64_t c;

        if (lid < 1 || lid > cat->max_dir_id) continue;
        for (c = s->first_child[lid]; c; c = s->next_sibling[c]) {
            /* c fits uint32: the build refuses shards past UINT32_MAX dirs. */
            uint64_t b = atomic_load_explicit(&s->acc->bytes[c], memory_order_relaxed);
            uint64_t f = atomic_load_explicit(&s->acc->files[c], memory_order_relaxed);
            const char *nm;
            if (!b && !f) continue;
            nm = (cat->name_len && cat->name_len[c]) ? cat->name_comp[c] : "";
            if (sb_cmap_add(cm, nm, cat->name_len ? cat->name_len[c] : 0,
                            lids[i].shard, c, b, f) != 0)
                return -1;
        }
    }
    return 0;
}

/* Selection sort order: primary metric desc, the other metric as tie-break,
 * name bytes last for full determinism (siblings routinely tie on files).
 * The entry table arrives as qsort_r context, never via a global. */
static int sb_cagg_cmp_bytes_desc(const void *pa, const void *pb, void *ctx) {
    const sb_cagg_t *ents = ctx;
    const sb_cagg_t *a = &ents[*(const int32_t *)pa];
    const sb_cagg_t *b = &ents[*(const int32_t *)pb];
    int c;
    if (a->bytes != b->bytes) return (a->bytes < b->bytes) ? 1 : -1;
    if (a->files != b->files) return (a->files < b->files) ? 1 : -1;
    c = memcmp(a->name, b->name, a->name_len < b->name_len ? a->name_len : b->name_len);
    if (c) return c;
    return (a->name_len > b->name_len) - (a->name_len < b->name_len);
}
static int sb_cagg_cmp_files_desc(const void *pa, const void *pb, void *ctx) {
    const sb_cagg_t *ents = ctx;
    const sb_cagg_t *a = &ents[*(const int32_t *)pa];
    const sb_cagg_t *b = &ents[*(const int32_t *)pb];
    int c;
    if (a->files != b->files) return (a->files < b->files) ? 1 : -1;
    if (a->bytes != b->bytes) return (a->bytes < b->bytes) ? 1 : -1;
    c = memcmp(a->name, b->name, a->name_len < b->name_len ? a->name_len : b->name_len);
    if (c) return c;
    return (a->name_len > b->name_len) - (a->name_len < b->name_len);
}

static const char *sb_name_of(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

/* Append one node (taking ownership of path_owned) and link it to its parent. */
static int32_t sb_node_add(ereport_sunburst_tree_t *t, char *path_owned, int32_t parent,
                           unsigned depth, uint64_t bytes, uint64_t files) {
    int32_t idx;
    sb_node_t *n;

    if (t->n == t->cap) {
        size_t ncap = t->cap ? t->cap * 2 : 256;
        sb_node_t *nn = realloc(t->nodes, ncap * sizeof(*nn));
        if (!nn) { free(path_owned); return -1; }
        t->nodes = nn;
        t->cap = ncap;
    }
    idx = (int32_t)t->n++;
    n = &t->nodes[idx];
    n->path = path_owned;
    n->name = sb_name_of(path_owned);
    n->total_bytes = bytes;
    n->total_files = files;
    n->parent = parent;
    n->first_child = -1;
    n->depth = depth;
    n->next_sibling = (parent >= 0) ? t->nodes[parent].first_child : -1;
    if (parent >= 0) t->nodes[parent].first_child = idx;
    return idx;
}

/* --path-rewrite prefix swap, malloc'd result. Directory boundary only. */
static char *sb_rewrite_apply(const char *from, const char *to, const char *orig) {
    size_t fl, tl, rest;
    char *out;

    if (!from || !to || !*from) return strdup(orig);
    fl = strlen(from);
    if (strncmp(orig, from, fl) != 0) return strdup(orig);
    if (orig[fl] != '\0' && orig[fl] != '/') return strdup(orig);
    rest = strlen(orig + fl);
    tl = strlen(to);
    out = malloc(tl + rest + 1);
    if (!out) return NULL;
    memcpy(out, to, tl);
    memcpy(out + tl, orig + fl, rest + 1);
    return out;
}

/* Join parent path + name into out (NUL-terminated). The synthetic root has
 * the empty path; its children are absolute top-level components, so they
 * gain the leading slash here. */
static int sb_path_join(char *out, size_t cap, const char *parent, const char *name, uint32_t name_len) {
    size_t pl = strlen(parent);
    int need_sep = (pl == 0 || parent[pl - 1] != '/');
    size_t total = pl + (need_sep ? 1 : 0) + name_len;

    if (total + 1 > cap) return -1;
    memcpy(out, parent, pl);
    if (need_sep) out[pl++] = '/';
    memcpy(out + pl, name, name_len);
    out[pl + name_len] = '\0';
    return 0;
}

/* Enumerate one cmap entry's children (all of its shard-local dir_ids). */
static int sb_enum_entry(const sb_shard_t *sh, const sb_cagg_t *e, sb_cmap_t *cm) {
    sb_lid_t one = { e->first_shard, e->first_lid };

    if (sb_enum_children(sh, &one, 1, cm) != 0) return -1;
    return sb_enum_children(sh, e->more, e->n_more, cm);
}

/* Expand node ni from its enumerated children cm: keep the union of the top-N
 * by bytes and by files that also clear the parent-relative min-fraction bar,
 * fold the rest into an "(other)" child, and recurse into the kept children.
 * orig is ni's pre-rewrite path. DFS: recursion depth is bounded by depth_max.
 * Folding never loses totals: "(other)" carries the trimmed children's subtree
 * totals, so a node's self stays total - sum(children) exactly. */
static int sb_expand_map(ereport_sunburst_tree_t *t, const sb_shard_t *sh,
                         int32_t ni, const char *orig, sb_cmap_t *cm,
                         const char *rewrite_from, const char *rewrite_to, sb_maprec_t *mr) {
    unsigned depth = t->nodes[ni].depth;
    const char *parent_path = t->nodes[ni].path; /* stable: the nodes array may move, strings don't */
    uint64_t min_b, min_f, other_b = 0, other_f = 0;
    int32_t *ord = NULL;
    int32_t *trimmed = NULL; /* entry indexes folded into "(other)", in trim order */
    size_t n_trimmed = 0;
    char *keep = NULL;
    int rc = -1;

    if (depth >= t->depth_max || t->n >= t->budget || cm->n == 0) return 0;

    ord = malloc(cm->n * sizeof(*ord));
    keep = calloc(cm->n, 1);
    trimmed = malloc(cm->n * sizeof(*trimmed));
    if (!ord || !keep || !trimmed) goto out;
    for (size_t i = 0; i < cm->n; i++) ord[i] = (int32_t)i;

    ereport_qsort_r(ord, cm->n, sizeof(*ord), sb_cagg_cmp_bytes_desc, cm->ents);
    for (size_t i = 0; i < cm->n && i < SUNBURST_TOP_N; i++) keep[ord[i]] = 1;
    ereport_qsort_r(ord, cm->n, sizeof(*ord), sb_cagg_cmp_files_desc, cm->ents);
    for (size_t i = 0; i < cm->n && i < SUNBURST_TOP_N; i++) keep[ord[i]] = 1;

    min_b = (uint64_t)((long double)t->nodes[ni].total_bytes * SUNBURST_MIN_FRAC);
    min_f = (uint64_t)((long double)t->nodes[ni].total_files * SUNBURST_MIN_FRAC);

    ereport_qsort_r(ord, cm->n, sizeof(*ord), sb_cagg_cmp_bytes_desc, cm->ents); /* intern in bytes order */
    for (size_t i = 0; i < cm->n; i++) {
        sb_cagg_t *e = &cm->ents[ord[i]];
        int above = (min_b > 0 && e->bytes >= min_b) || (min_f > 0 && e->files >= min_f);
        char corig[PATH_MAX];
        char dbuf[PATH_MAX];
        char *display;
        int32_t ci;
        sb_cmap_t c2;

        if (!keep[ord[i]] || !above || t->n >= t->budget) {
            other_b += e->bytes;
            other_f += e->files;
            trimmed[n_trimmed++] = ord[i];
            continue;
        }
        if (sb_path_join(corig, sizeof(corig), orig, e->name, e->name_len) != 0) goto out;
        if (rewrite_from && strcmp(corig, rewrite_from) == 0) {
            display = strdup(rewrite_to);
        } else {
            if (sb_path_join(dbuf, sizeof(dbuf), parent_path, e->name, e->name_len) != 0) goto out;
            display = strdup(dbuf);
        }
        if (!display) goto out;
        ci = sb_node_add(t, display, ni, depth + 1, e->bytes, e->files); /* takes display */
        if (ci < 0) goto out;
        if (mr && sb_maprec_add_entry(mr, e, ci) != 0) goto out;
        sb_cmap_init(&c2);
        if (sb_enum_entry(sh, e, &c2) == 0)
            rc = sb_expand_map(t, sh, ci, corig, &c2, rewrite_from, rewrite_to, mr);
        sb_cmap_free(&c2);
        if (rc != 0) goto out;
    }

    if (other_b || other_f) {
        /* Distinct path (parent + "/(other)") so flat id/parent conversions of
         * the JSON (see tools.md) never see duplicate ids. */
        char opath[PATH_MAX];
        int32_t oi;
        if (sb_path_join(opath, sizeof(opath), parent_path, "(other)", 7) != 0) goto out;
        oi = sb_node_add(t, strdup(opath), ni, depth + 1, other_b, other_f);
        if (oi < 0) goto out;
        t->nodes[oi].name = "(other)";
        /* The fold, not the parent, owns the trimmed children's records. */
        if (mr) {
            for (size_t i = 0; i < n_trimmed; i++)
                if (sb_maprec_add_entry(mr, &cm->ents[trimmed[i]], oi) != 0) goto out;
        }
    }
    rc = 0;
out:
    free(ord);
    free(keep);
    free(trimmed);
    return rc;
}

/* Per-shard prep: roll the scan-time accumulator up the catalog tree (self ->
 * subtree totals, in place; dir_ids are handed out parent-first, so a single
 * reverse pass settles every descendant), then build the child index for
 * O(fanout) enumeration. Shards are independent, so this runs in parallel. */
static void sb_rollup_shard(const crawl_bin_catalog_t *cat, ereport_sunburst_accum_t *a) {
    uint64_t nd = cat->max_dir_id, d;

    for (d = nd; d >= 2; d--) {
        uint64_t p = cat->parent_dir_id[d];
        uint64_t b, f;
        if (p < 1 || p > nd) continue;
        b = atomic_load_explicit(&a->bytes[d], memory_order_relaxed);
        f = atomic_load_explicit(&a->files[d], memory_order_relaxed);
        if (b) atomic_fetch_add_explicit(&a->bytes[p], b, memory_order_relaxed);
        if (f) atomic_fetch_add_explicit(&a->files[p], f, memory_order_relaxed);
    }
}

/* The child index depends on the catalog alone (not on the accumulator), which
 * is what lets a workspace share one across every per-user build on a shard. */
static int sb_child_index_build(const crawl_bin_catalog_t *cat, uint32_t **first_child_out,
                                uint32_t **next_sibling_out) {
    uint64_t nd = cat->max_dir_id, d;
    uint32_t *fc = malloc(((size_t)nd + 1) * sizeof(*fc));
    uint32_t *ns = malloc(((size_t)nd + 1) * sizeof(*ns));

    if (!fc || !ns) {
        free(fc);
        free(ns);
        return -1;
    }
    /* malloc + memset rather than calloc: the fill below reads fc[p] before
     * writing it, and a read fault on untouched mmap'd memory maps the shared
     * zero page, making the write a copy-on-write with a TLB-flush IPI to every
     * CPU of the process. A write-first touch takes a private page directly. */
    memset(fc, 0, ((size_t)nd + 1) * sizeof(*fc));
    memset(ns, 0, ((size_t)nd + 1) * sizeof(*ns));
    for (d = 2; d <= nd; d++) {
        uint64_t p = cat->parent_dir_id[d];
        if (p < 1 || p > nd) continue;
        ns[d] = fc[p];
        fc[p] = (uint32_t)d;
    }
    *first_child_out = fc;
    *next_sibling_out = ns;
    return 0;
}

static int sb_prep_shard(sb_shard_t *s) {
    sb_rollup_shard(s->cat, s->acc);
    if (s->borrowed_index) return 0;
    return sb_child_index_build(s->cat, &s->first_child, &s->next_sibling);
}

int ereport_sunburst_ws_init(ereport_sunburst_ws_t *ws, const crawl_bin_catalog_t *cat) {
    if (!ws) return -1;
    memset(ws, 0, sizeof(*ws));
    if (!cat || !cat->parent_dir_id || cat->max_dir_id == 0 || cat->max_dir_id > UINT32_MAX) return -1;
    if (sb_child_index_build(cat, &ws->first_child, &ws->next_sibling) != 0) return -1;
    ws->cat = cat;
    return 0;
}

void ereport_sunburst_ws_free(ereport_sunburst_ws_t *ws) {
    if (!ws) return;
    free(ws->first_child);
    free(ws->next_sibling);
    memset(ws, 0, sizeof(*ws));
}

typedef struct {
    sb_shard_t *sh;
    size_t n;
    _Atomic size_t next;
    _Atomic int err;
} sb_prep_pool_t;

static void *sb_prep_worker(void *arg) {
    sb_prep_pool_t *p = arg;
    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->next, 1, memory_order_relaxed);
        if (i >= p->n) return NULL;
        if (sb_prep_shard(&p->sh[i]) != 0) {
            atomic_store_explicit(&p->err, 1, memory_order_relaxed);
            return NULL;
        }
    }
}

/* Derive one shard's dense dir_id -> node map from the materialization anchors.
 * dir_ids are handed out parent-first, so after seeding the anchors a single
 * forward pass resolves every remaining directory to its parent's node; dir_id 1
 * (the shard top) and anything above the displayed root fold into the root. */
static int sb_build_dir_node_map(const sb_shard_t *s, uint32_t shard_idx, const sb_mapent_t *ents,
                                 size_t n_ents, int32_t root, uint32_t **out) {
    const crawl_bin_catalog_t *cat = s->cat;
    uint64_t nd = cat->max_dir_id, d;
    uint32_t *m = malloc(((size_t)nd + 1) * sizeof(*m));
    size_t i;

    if (!m) return -1;
    memset(m, 0xFF, ((size_t)nd + 1) * sizeof(*m));
    m[1] = (uint32_t)root;
    for (i = 0; i < n_ents; i++)
        if (ents[i].shard == shard_idx) m[ents[i].lid] = (uint32_t)ents[i].node;
    for (d = 2; d <= nd; d++) {
        uint64_t p;
        if (m[d] != UINT32_MAX) continue;
        p = cat->parent_dir_id[d];
        m[d] = (p >= 1 && p <= nd) ? m[p] : (uint32_t)root;
    }
    *out = m;
    return 0;
}

typedef struct {
    sb_shard_t *sh;
    size_t n;
    const sb_mapent_t *ents;
    size_t n_ents;
    int32_t root;
    uint32_t **out; /* [n] compacted shard order */
    _Atomic size_t next;
    _Atomic int err;
} sb_map_pool_t;

static void *sb_map_worker(void *arg) {
    sb_map_pool_t *p = arg;
    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->next, 1, memory_order_relaxed);
        if (i >= p->n) return NULL;
        if (sb_build_dir_node_map(&p->sh[i], (uint32_t)i, p->ents, p->n_ents, p->root, &p->out[i]) != 0) {
            atomic_store_explicit(&p->err, 1, memory_order_relaxed);
            return NULL;
        }
    }
}

static ereport_sunburst_tree_t *sb_build_impl(crawl_bin_catalog_t *const *cats,
                                              ereport_sunburst_accum_t *accs, size_t n,
                                              unsigned depth_max, unsigned threads,
                                              const char *rewrite_from, const char *rewrite_to,
                                              int want_buckets, const ereport_sunburst_ws_t *ws) {
    ereport_sunburst_tree_t *t = calloc(1, sizeof(*t));
    sb_shard_t *sh = NULL;
    size_t ns = 0;
    sb_lid_t *cur_lids = NULL;
    size_t ncur = 0;
    sb_lid_t *chain = NULL; /* every lid behind the collapse chain, for root mapping */
    size_t nchain = 0, capchain = 0;
    sb_maprec_t mr;
    sb_maprec_t *mrp = NULL;
    uint64_t cur_b = 0, cur_f = 0, grand_b = 0, grand_f = 0;
    char cur_orig[PATH_MAX];
    char *display = NULL;
    int32_t ri;

    memset(&mr, 0, sizeof(mr));
    if (want_buckets) mrp = &mr;

    if (!t) return NULL;
    t->depth_max = depth_max ? depth_max : 6;
    t->budget = SUNBURST_NODE_BUDGET;
    t->root = 0;
    cur_orig[0] = '\0';

    sh = calloc(n ? n : 1, sizeof(*sh));
    if (!sh) goto fail;

    /* Eligibility pass: metadata checks only, no big-array touches. */
    for (size_t s = 0; s < n; s++) {
        const crawl_bin_catalog_t *cat = cats[s];
        ereport_sunburst_accum_t *a = accs ? &accs[s] : NULL;
        uint64_t nd;

        if (!cat || !a || !a->bytes || a->max_dir_id == 0) continue;
        nd = cat->max_dir_id;
        if (nd < 1) continue;
        if (nd != a->max_dir_id) continue; /* accumulator/catalog mismatch: skip rather than guess */
        if (!cat->parent_dir_id) continue;
        if (nd > UINT32_MAX) goto fail; /* child index is uint32; no realistic shard is near this */
        sh[ns].cat = cat;
        sh[ns].acc = a;
        sh[ns].fi = (uint32_t)s;
        if (ws && ws->cat == cat && ws->first_child && ws->next_sibling) {
            sh[ns].first_child = ws->first_child;
            sh[ns].next_sibling = ws->next_sibling;
            sh[ns].borrowed_index = 1;
        }
        ns++;
    }

    /* Rollup + child index per shard. This is the bulk of the build's CPU (one
     * pass over every catalog entry of every shard), so spread it across the
     * threads the scan just vacated. */
    if (ns > 1 && threads > 1) {
        sb_prep_pool_t pool;
        pthread_t th[64];
        size_t nth = threads;
        if (nth > ns) nth = ns;
        if (nth > sizeof(th) / sizeof(*th)) nth = sizeof(th) / sizeof(*th);
        pool.sh = sh;
        pool.n = ns;
        atomic_init(&pool.next, 0);
        atomic_init(&pool.err, 0);
        size_t started = 0;
        for (; started < nth; started++) {
            if (pthread_create(&th[started], NULL, sb_prep_worker, &pool) != 0) break;
        }
        if (started == 0) {
            for (size_t i = 0; i < ns; i++)
                if (sb_prep_shard(&sh[i]) != 0) goto fail;
        } else {
            for (size_t i = 0; i < started; i++) pthread_join(th[i], NULL);
            if (atomic_load_explicit(&pool.err, memory_order_relaxed)) goto fail;
        }
    } else {
        for (size_t i = 0; i < ns; i++)
            if (sb_prep_shard(&sh[i]) != 0) goto fail;
    }

    /* Collapse cursor: the merged directory under consideration for displayed
     * root, as the set of shard-local dir_ids behind it. Starts at the
     * synthetic root, which every shard's dir_id 1 hangs under. */
    cur_lids = malloc((ns ? ns : 1) * sizeof(*cur_lids));
    if (!cur_lids) goto fail;
    for (size_t i = 0; i < ns; i++) {
        uint64_t b = atomic_load_explicit(&sh[i].acc->bytes[1], memory_order_relaxed);
        uint64_t f = atomic_load_explicit(&sh[i].acc->files[1], memory_order_relaxed);
        cur_lids[ncur++] = (sb_lid_t){ (uint32_t)i, 1 };
        grand_b += b;
        grand_f += f;
    }
    cur_b = grand_b;
    cur_f = grand_f;

/* Every lid set the collapse chain passes through belongs to the displayed
 * root; collect them (cheap: one entry per shard per collapsed level). */
#define SB_CHAIN_APPEND(list, cnt)                                                                     \
    do {                                                                                               \
        if (mrp) {                                                                                     \
            if (nchain + (cnt) > capchain) {                                                           \
                size_t nc = capchain ? capchain * 2 : 64;                                              \
                sb_lid_t *nl_;                                                                         \
                while (nc < nchain + (cnt)) nc *= 2;                                                   \
                nl_ = realloc(chain, nc * sizeof(*nl_));                                               \
                if (!nl_) goto fail;                                                                   \
                chain = nl_;                                                                           \
                capchain = nc;                                                                         \
            }                                                                                          \
            memcpy(chain + nchain, (list), (cnt) * sizeof(*chain));                                    \
            nchain += (cnt);                                                                           \
        }                                                                                              \
    } while (0)

    /* Collapse the single-child chain from the top: the displayed root is the
     * deepest directory that still holds all the content on its own. Stop at
     * the first node with a sibling fork or with files of its own (a --subtree
     * root with direct files must not be collapsed through); directory records
     * do not count, or the chain above the crawl root -- whose selves are
     * exactly those records -- would stop it. Ancestor self values are folded
     * back into the displayed root at emit time (root_boost_*). */
    for (;;) {
        sb_cmap_t cm;
        uint64_t kids_f = 0;
        size_t i;

        sb_cmap_init(&cm);
        if (sb_enum_children(sh, cur_lids, ncur, &cm) != 0) {
            sb_cmap_free(&cm);
            goto fail;
        }
        for (i = 0; i < cm.n; i++) kids_f += cm.ents[i].files;
        if (cm.n == 1 && cur_f == kids_f) {
            sb_cagg_t *e = &cm.ents[0];
            sb_lid_t *nl;
            if (sb_path_join(cur_orig, sizeof(cur_orig), cur_orig, e->name, e->name_len) != 0) {
                sb_cmap_free(&cm);
                goto fail;
            }
            cur_b = e->bytes;
            cur_f = e->files;
            nl = malloc((e->n_more + 1) * sizeof(*nl));
            if (!nl) { sb_cmap_free(&cm); goto fail; }
            nl[0] = (sb_lid_t){ e->first_shard, e->first_lid };
            memcpy(nl + 1, e->more, e->n_more * sizeof(*nl));
            SB_CHAIN_APPEND(cur_lids, ncur); /* the level being collapsed through */
            free(cur_lids);
            cur_lids = nl;
            ncur = e->n_more + 1;
            sb_cmap_free(&cm);
        } else {
            sb_cmap_free(&cm);
            break;
        }
    }

    /* The synthetic root's canonical display path is "/": its children are the
     * absolute top-level components, so this is where content that spans more
     * than one crawl root (or strays outside one) converges. */
    if (cur_orig[0] == '\0') {
        cur_orig[0] = '/';
        cur_orig[1] = '\0';
    }

    /* --path-rewrite swaps the displayed root's prefix; children inherit it
     * (a rewrite landing below the root is applied when that node is interned). */
    display = sb_rewrite_apply(rewrite_from, rewrite_to, cur_orig);
    if (!display) goto fail;
    ri = sb_node_add(t, display, -1, 0, cur_b, cur_f); /* takes display */
    if (ri < 0) goto fail;
    if (!*t->nodes[ri].name) t->nodes[ri].name = t->nodes[ri].path; /* "/" has no last component */
    t->root = ri;
    t->root_boost_bytes = grand_b - cur_b;
    t->root_boost_files = grand_f - cur_f;

    if (mrp) {
        size_t i;
        SB_CHAIN_APPEND(cur_lids, ncur); /* the displayed root's own lids */
        for (i = 0; i < nchain; i++)
            if (sb_maprec_add(mrp, chain[i].shard, chain[i].lid, ri) != 0) goto fail;
    }

    {
        sb_cmap_t cm;
        int rc;
        sb_cmap_init(&cm);
        if (sb_enum_children(sh, cur_lids, ncur, &cm) != 0) {
            sb_cmap_free(&cm);
            goto fail;
        }
        rc = sb_expand_map(t, sh, ri, cur_orig, &cm, rewrite_from, rewrite_to, mrp);
        sb_cmap_free(&cm);
        if (rc != 0) goto fail;
    }

    if (mrp) {
        /* Bucket matrices, one 36-cell row per node, zeroed for the second
         * pass; plus the per-shard dir_id -> node maps derived from the
         * materialization anchors. maps[] holds the compacted results until
         * they land at dir_node[fi]. */
        size_t i;
        uint32_t **maps = calloc(ns ? ns : 1, sizeof(*maps));
        int ok = 0;

        t->bucket_bytes = calloc(t->n * 36, sizeof(*t->bucket_bytes));
        t->bucket_files = calloc(t->n * 36, sizeof(*t->bucket_files));
        t->dir_node = calloc(n ? n : 1, sizeof(*t->dir_node));
        if (!t->bucket_bytes || !t->bucket_files || !t->dir_node || !maps) {
            free(maps);
            goto fail;
        }
        t->dir_node_n = n;

        if (ns > 1 && threads > 1) {
            sb_map_pool_t pool;
            pthread_t th[64];
            size_t nth = threads, started = 0;
            if (nth > ns) nth = ns;
            if (nth > sizeof(th) / sizeof(*th)) nth = sizeof(th) / sizeof(*th);
            pool.sh = sh;
            pool.n = ns;
            pool.ents = mrp->v;
            pool.n_ents = mrp->n;
            pool.root = ri;
            pool.out = maps;
            atomic_init(&pool.next, 0);
            atomic_init(&pool.err, 0);
            for (; started < nth; started++)
                if (pthread_create(&th[started], NULL, sb_map_worker, &pool) != 0) break;
            if (started == 0) {
                for (i = 0; i < ns; i++)
                    if (sb_build_dir_node_map(&sh[i], (uint32_t)i, mrp->v, mrp->n, ri, &maps[i]) != 0)
                        break;
                ok = (i == ns);
            } else {
                for (i = 0; i < started; i++) pthread_join(th[i], NULL);
                ok = !atomic_load_explicit(&pool.err, memory_order_relaxed);
            }
        } else {
            for (i = 0; i < ns; i++)
                if (sb_build_dir_node_map(&sh[i], (uint32_t)i, mrp->v, mrp->n, ri, &maps[i]) != 0)
                    break;
            ok = (i == ns);
        }
        if (!ok) {
            for (i = 0; i < ns; i++) free(maps[i]);
            free(maps);
            goto fail;
        }
        for (i = 0; i < ns; i++) t->dir_node[sh[i].fi] = maps[i];
        free(maps);
    }

    free(chain);
    free(mr.v);
    free(cur_lids);
    for (size_t i = 0; i < ns; i++) {
        if (sh[i].borrowed_index) continue;
        free(sh[i].first_child);
        free(sh[i].next_sibling);
    }
    free(sh);
#undef SB_CHAIN_APPEND
    return t;

fail:
    free(chain);
    free(mr.v);
    free(cur_lids);
    if (sh) {
        for (size_t i = 0; i < ns; i++) {
            if (sh[i].borrowed_index) continue;
            free(sh[i].first_child);
            free(sh[i].next_sibling);
        }
        free(sh);
    }
    ereport_sunburst_tree_free(t);
    return NULL;
}

ereport_sunburst_tree_t *ereport_sunburst_build(crawl_bin_catalog_t *const *cats,
                                                ereport_sunburst_accum_t *accs, size_t n,
                                                unsigned depth_max, unsigned threads,
                                                const char *rewrite_from, const char *rewrite_to,
                                                int want_buckets) {
    return sb_build_impl(cats, accs, n, depth_max, threads, rewrite_from, rewrite_to, want_buckets, NULL);
}

ereport_sunburst_tree_t *ereport_sunburst_build_ws(crawl_bin_catalog_t *cat, ereport_sunburst_accum_t *acc,
                                                   const ereport_sunburst_ws_t *ws, unsigned depth_max,
                                                   const char *rewrite_from, const char *rewrite_to,
                                                   int want_buckets) {
    crawl_bin_catalog_t *cats1[1];

    cats1[0] = cat;
    return sb_build_impl(cats1, acc, 1, depth_max, 1, rewrite_from, rewrite_to, want_buckets, ws);
}

size_t ereport_sunburst_tree_nodes(const ereport_sunburst_tree_t *t) {
    return t ? t->n : 0;
}

_Atomic uint64_t *ereport_sunburst_bucket_bytes(ereport_sunburst_tree_t *t) {
    return t ? t->bucket_bytes : NULL;
}

_Atomic uint64_t *ereport_sunburst_bucket_files(ereport_sunburst_tree_t *t) {
    return t ? t->bucket_files : NULL;
}

const uint32_t *ereport_sunburst_dir_node_map(const ereport_sunburst_tree_t *t, uint64_t file_index) {
    if (!t || !t->dir_node || file_index >= t->dir_node_n) return NULL;
    return t->dir_node[file_index];
}

void ereport_sunburst_dir_node_maps_clear(ereport_sunburst_tree_t *t) {
    if (!t || !t->dir_node) return;
    for (size_t i = 0; i < t->dir_node_n; i++) free(t->dir_node[i]);
    free(t->dir_node);
    t->dir_node = NULL;
    t->dir_node_n = 0;
}

void ereport_sunburst_buckets_clear(ereport_sunburst_tree_t *t) {
    if (!t) return;
    free(t->bucket_bytes);
    free(t->bucket_files);
    t->bucket_bytes = NULL;
    t->bucket_files = NULL;
    ereport_sunburst_dir_node_maps_clear(t);
}

void ereport_sunburst_tree_free(ereport_sunburst_tree_t *t) {
    if (!t) return;
    for (size_t i = 0; i < t->n; i++) free(t->nodes[i].path);
    free(t->nodes);
    free(t->bucket_bytes);
    free(t->bucket_files);
    ereport_sunburst_dir_node_maps_clear(t);
    free(t);
}

/* ------------------------------------------------------------------ */
/* JSON emission                                                       */
/* ------------------------------------------------------------------ */

static void sb_json_escape(FILE *out, const char *s) {
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        switch (c) {
        case '"': fputs("\\\"", out); break;
        case '\\': fputs("\\\\", out); break;
        case '<': fputs("\\u003C", out); break; /* safe to embed in <script> and HTML */
        case '>': fputs("\\u003E", out); break;
        case '&': fputs("\\u0026", out); break;
        default:
            if (c < 0x20) fprintf(out, "\\u%04x", c);
            else fputc(c, out);
        }
    }
}

typedef struct {
    const ereport_sunburst_tree_t *t;
    uint64_t self_boost_bytes;  /* collapsed ancestors' selves, credited to the root */
    uint64_t self_boost_files;
    size_t emitted;             /* node budget bookkeeping */
} sb_emit_ctx_t;

/* Deterministic totals order: primary metric desc, the other metric as tie-break
 * (siblings routinely tie on files, e.g. one file per directory), node id last.
 * The node table arrives as qsort_r context, never via a global. */
static int sb_cmp_bytes_desc(const void *pa, const void *pb, void *ctx) {
    const sb_node_t *nodes = ctx;
    int32_t ia = *(const int32_t *)pa, ib = *(const int32_t *)pb;
    const sb_node_t *a = &nodes[ia], *b = &nodes[ib];
    if (a->total_bytes != b->total_bytes) return (a->total_bytes < b->total_bytes) ? 1 : -1;
    if (a->total_files != b->total_files) return (a->total_files < b->total_files) ? 1 : -1;
    return (ia > ib) - (ia < ib);
}

/* One node's 36-cell bucket matrix row as a JSON array (no brackets). */
static void sb_json_emit_matrix(FILE *out, const _Atomic uint64_t *row) {
    int i;
    for (i = 0; i < 36; i++)
        fprintf(out, "%s%" PRIu64, i ? "," : "",
                atomic_load_explicit(&row[i], memory_order_relaxed));
}

/* Emit one node. The build already applied the top-N + min-fraction trim and
 * materialized the "(other)" folds, so this is a plain serialization walk:
 * internal nodes carry self values (total minus children, plus the collapsed
 * ancestors' boost at the root); leaves carry subtree totals. Children print
 * in descending bytes for a stable, useful order. */
static int sb_json_emit_node(sb_emit_ctx_t *ctx, int32_t idx, unsigned depth_from_root, FILE *out) {
    const ereport_sunburst_tree_t *t = ctx->t;
    const sb_node_t *n = &t->nodes[idx];
    int is_root = (idx == t->root);
    unsigned child_depth = depth_from_root + 1;
    int emit_children = (child_depth <= t->depth_max) && (ctx->emitted < SUNBURST_NODE_BUDGET);

    /* Collect content-bearing children (a zero-total child should not exist
     * after the build's trim; guard anyway). */
    int32_t *kids = NULL;
    size_t nkids = 0, kcap = 0;
    uint64_t kids_bytes = 0, kids_files = 0;
    if (emit_children) {
        for (int32_t c = n->first_child; c >= 0; c = t->nodes[c].next_sibling) {
            const sb_node_t *cn = &t->nodes[c];
            if (cn->total_bytes == 0 && cn->total_files == 0) continue;
            if (nkids == kcap) {
                size_t nc = kcap ? kcap * 2 : 16;
                int32_t *nk = realloc(kids, nc * sizeof(*nk));
                if (!nk) { free(kids); return -1; }
                kids = nk;
                kcap = nc;
            }
            kids[nkids++] = c;
            kids_bytes += cn->total_bytes;
            kids_files += cn->total_files;
        }
    }

    uint64_t self_b = n->total_bytes - kids_bytes; /* subtree totals: children sum <= total */
    uint64_t self_f = n->total_files - kids_files;
    if (is_root) {
        self_b += ctx->self_boost_bytes;
        self_f += ctx->self_boost_files;
    }

    if (nkids == 0) {
        /* Leaf (natural, depth-folded, or budget-cut): carry the full subtree total. */
        fprintf(out, "{\"name\":\"");
        sb_json_escape(out, n->name);
        fprintf(out, "\",\"path\":\"");
        sb_json_escape(out, n->path);
        fprintf(out, "\",\"bytes\":%" PRIu64 ",\"files\":%" PRIu64,
                is_root ? n->total_bytes + ctx->self_boost_bytes : n->total_bytes,
                is_root ? n->total_files + ctx->self_boost_files : n->total_files);
        if (t->bucket_bytes) {
            fputs(",\"bucket_bytes\":[", out);
            sb_json_emit_matrix(out, &t->bucket_bytes[(size_t)idx * 36]);
            fputs("],\"bucket_files\":[", out);
            sb_json_emit_matrix(out, &t->bucket_files[(size_t)idx * 36]);
            fputc(']', out);
        }
        fputc('}', out);
        ctx->emitted++;
        free(kids);
        return 0;
    }

    fprintf(out, "{\"name\":\"");
    sb_json_escape(out, n->name);
    fprintf(out, "\",\"path\":\"");
    sb_json_escape(out, n->path);
    fprintf(out, "\",\"bytes\":%" PRIu64 ",\"files\":%" PRIu64, self_b, self_f);
    if (t->bucket_bytes) {
        fputs(",\"bucket_bytes\":[", out);
        sb_json_emit_matrix(out, &t->bucket_bytes[(size_t)idx * 36]);
        fputs("],\"bucket_files\":[", out);
        sb_json_emit_matrix(out, &t->bucket_files[(size_t)idx * 36]);
        fputc(']', out);
    }
    fputs(",\"children\":[", out);
    ctx->emitted++;

    ereport_qsort_r(kids, nkids, sizeof(*kids), sb_cmp_bytes_desc, (void *)t->nodes);
    for (size_t i = 0; i < nkids; i++) {
        if (i) fputc(',', out);
        if (sb_json_emit_node(ctx, kids[i], child_depth, out) != 0) {
            free(kids);
            return -1;
        }
    }
    fputc(']', out);
    fputc('}', out);
    free(kids);
    return 0;
}

static int sb_json_write(const ereport_sunburst_tree_t *t, FILE *out) {
    sb_emit_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.t = t;
    /* The collapsed ancestors each held one directory record of self; keep the
     * grand total exact by crediting them to the displayed root. */
    ctx.self_boost_bytes = t->root_boost_bytes;
    ctx.self_boost_files = t->root_boost_files;
    return sb_json_emit_node(&ctx, t->root, 0, out);
}

/* ------------------------------------------------------------------ */
/* HTML page                                                           */
/* ------------------------------------------------------------------ */

static void sb_html_escape(FILE *out, const char *s) {
    for (; *s; s++) {
        switch (*s) {
        case '&': fputs("&amp;", out); break;
        case '<': fputs("&lt;", out); break;
        case '>': fputs("&gt;", out); break;
        case '"': fputs("&quot;", out); break;
        default: fputc(*s, out);
        }
    }
}

static void sb_html_prefix(FILE *out, const char *subject, const char *base_name,
                           const char *report_href, int has_buckets) {
    fputs("<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n"
          "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>Sunburst", out);
    if (subject) {
        fputs(" — ", out);
        sb_html_escape(out, subject);
    }
    fputs("</title>\n<style>\n"
          "body{font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,Helvetica,Arial,sans-serif;"
          "margin:0;color:#1d2939;background:#fff}\n"
          ".topbar{display:flex;flex-wrap:wrap;align-items:baseline;gap:10px 18px;padding:14px 22px;"
          "border-bottom:1px solid #e4e7ec}\n"
          ".topbar h1{font-size:18px;margin:0}\n"
          ".topbar a{color:#175cd3;text-decoration:none}\n"
          ".topbar a:hover{text-decoration:underline}\n"
          ".crumbs{font-size:13px;color:#667085;word-break:break-all}\n"
          ".crumbs a{color:#175cd3;cursor:pointer}\n"
          ".toggle{margin-left:auto;display:flex;border:1px solid #d0d5dd;border-radius:7px;overflow:hidden}\n"
          ".toggle button{border:0;background:#fff;padding:6px 14px;font-size:13px;cursor:pointer;color:#344054}\n"
          ".toggle button.on{background:#eff4ff;color:#175cd3;font-weight:600}\n"
          "#chartwrap{display:flex;justify-content:center;padding:8px}\n"
          "#chart{width:min(92vmin,860px);height:auto;display:block}\n"
          "#chart path{cursor:pointer;stroke:#fff;stroke-width:1}\n"
          "#chart path.leafless{cursor:default}\n"
          "#chart text{pointer-events:none;font-size:11px;fill:#1d2939}\n"
          "#tip{position:fixed;pointer-events:none;background:#101828;color:#fff;padding:8px 10px;border-radius:6px;"
          "font-size:12px;line-height:1.5;max-width:min(480px,80vw);word-break:break-all;box-shadow:0 4px 12px rgba(0,0,0,.25);"
          "z-index:10}\n"
          "#tip .dim{color:#98a2b3}\n"
          ".hint{padding:0 22px 14px;font-size:12px;color:#667085}\n"
          /* Author display rules beat the UA [hidden] rule, so without this
           * guard the filters bar and user picker would show even when their
           * JS never unhides them (and the colorby select would sit there
           * with no listener attached). */
          "[hidden]{display:none!important}\n"
          ".filters{padding:10px 22px;border-bottom:1px solid #e4e7ec;display:flex;flex-direction:column;gap:8px}\n"
          ".frow{display:flex;flex-wrap:wrap;align-items:center;gap:6px;font-size:12px}\n"
          ".flabel{color:#667085;min-width:38px;font-weight:600}\n"
          ".chip{border:1px solid #d0d5dd;border-radius:999px;background:#fff;padding:3px 10px;"
          "font-size:12px;cursor:pointer;color:#344054;user-select:none}\n"
          ".chip.on{background:#eff4ff;border-color:#84adff;color:#175cd3;font-weight:600}\n"
          ".fnote{color:#98a2b3}\n"
          ".frow select{font-size:12px;padding:2px 6px;color:#344054}\n"
          ".userpick{font-size:13px;color:#344054;display:flex;align-items:center;gap:6px}\n"
          ".userpick select{font-size:13px;padding:3px 6px;color:#344054;max-width:min(420px,60vw)}\n"
          ".legend{font-size:12px;color:#344054}\n"
          ".legend-below{margin:0 auto 14px;max-width:min(92vmin,860px);"
          "border:1px solid #e4e7ec;border-radius:8px;padding:8px 12px}\n"
          ".legend summary{cursor:pointer;font-weight:600;color:#475467;user-select:none}\n"
          ".legend-body{padding-top:8px;display:flex;flex-direction:column;gap:6px}\n"
          ".lrow{display:flex;flex-wrap:wrap;align-items:center;gap:4px 14px;line-height:1.5}\n"
          ".ltitle{font-weight:600;color:#475467;min-width:70px}\n"
          ".switem{display:inline-flex;align-items:center;gap:5px;white-space:nowrap}\n"
          ".sw{display:inline-block;width:12px;height:12px;border-radius:3px}\n"
          "</style>\n</head>\n<body>\n", out);
    fputs("<div class=\"topbar\">\n<h1>Sunburst", out);
    if (subject) {
        fputs(" — ", out);
        sb_html_escape(out, subject);
    }
    fputs("</h1>\n<div class=\"crumbs\" id=\"crumbs\"></div>\n"
          "<label class=\"userpick\" id=\"userpick-wrap\" hidden>User "
          "<select id=\"userpick\"></select></label>\n"
          "<div class=\"toggle\"><button id=\"btn-bytes\" class=\"on\" type=\"button\">Bytes</button>"
          "<button id=\"btn-files\" type=\"button\">Files</button></div>\n", out);
    fputs("<a href=\"", out);
    sb_html_escape(out, report_href);
    fputs("\">&larr; report</a>\n</div>\n"
          "<div class=\"filters\" id=\"filters\" hidden>\n"
          "<div class=\"frow\"><span class=\"flabel\">Age</span><span id=\"chips-age\"></span></div>\n"
          "<div class=\"frow\"><span class=\"flabel\">Size</span><span id=\"chips-size\"></span>\n"
          "<span class=\"fnote\">bucket filters re-slice every wedge; per-node sums stay exact</span></div>\n"
          "<div class=\"frow\"><span class=\"flabel\">Color</span>"
          "<select id=\"colorby\"><option value=\"dir\">directory</option>"
          "<option value=\"age\">dominant age bucket</option>"
          "<option value=\"size\">dominant size bucket</option></select></div>\n", out);
    /* Bucket reports: the legend is the filter panel's last row, directly under
       the Color dropdown. Without buckets the panel stays hidden, so the legend
       keeps its old boxed spot below the chart. */
    if (has_buckets)
        fputs("<details class=\"legend\" id=\"legend\"><summary>Legend</summary>"
              "<div class=\"legend-body\" id=\"legend-body\"></div></details>\n", out);
    fputs("</div>\n"
          "<div id=\"chartwrap\"><svg id=\"chart\" viewBox=\"0 0 1000 1000\" role=\"img\" "
          "aria-label=\"Sunburst chart\"></svg></div>\n", out);
    if (!has_buckets)
        fputs("<details class=\"legend legend-below\" id=\"legend\"><summary>Legend</summary>"
              "<div class=\"legend-body\" id=\"legend-body\"></div></details>\n", out);
    fputs("<div class=\"hint\">Click a wedge to zoom in; click the center to go back up. "
          "Data: <a href=\"", out);
    sb_html_escape(out, base_name);
    fputs(".json\">", out);
    sb_html_escape(out, base_name);
    fputs(".json</a>.</div>\n"
          "<div id=\"tip\" hidden></div>\n"
          "<script>\nconst SUNBURST = ", out);
}

static void sb_html_suffix(FILE *out) {
    fputs(
        "let mode = 'bytes';\n"
        "let cur = SUNBURST;\n"
        "\n"
        "/* Bucket axes -- keep in sync with age_bucket_names/size_bucket_names in ereport.c\n"
        "   and the [age][size] row-major layout of each node's 36-cell matrix. */\n"
        "const AGE_NAMES = ['< 30 days', '30\\u201390 days', '90\\u2013180 days', '180 days \\u2013 1 yr', '1\\u20133 years', '3+ years'];\n"
        "const SIZE_NAMES = ['< 4K', '4K \\u2013 1M', '1M \\u2013 100M', '100M \\u2013 1G', '1G \\u2013 10G', '10G+'];\n"
        "const HAS_BUCKETS = !!SUNBURST.bucket_bytes;\n"
        "const ageSel = [1, 1, 1, 1, 1, 1], sizeSel = [1, 1, 1, 1, 1, 1];\n"
        "let colorBy = 'dir';\n"
        "\n"
        "/* Sum of one node's own matrix over the selected cells. Node matrices mirror\n"
        "   the bytes/files semantics: self values for internal nodes, subtree totals\n"
        "   for leaves, so filtered totals annotate exactly like unfiltered ones. */\n"
        "function selfSum(n, key) {\n"
        "  const m = n[key];\n"
        "  let s = 0;\n"
        "  for (let a = 0; a < 6; a++) {\n"
        "    if (!ageSel[a]) continue;\n"
        "    for (let z = 0; z < 6; z++) if (sizeSel[z]) s += m[a * 6 + z];\n"
        "  }\n"
        "  return s;\n"
        "}\n"
        "\n"
        "/* Totals: internal nodes carry self values, leaves carry subtree totals.\n"
        "   Re-run after every bucket filter change. */\n"
        "function annotate(n, parent) {\n"
        "  n._p = parent || null;\n"
        "  if (HAS_BUCKETS) {\n"
        "    n._b = selfSum(n, 'bucket_bytes');\n"
        "    n._f = selfSum(n, 'bucket_files');\n"
        "  } else {\n"
        "    n._b = n.bytes; n._f = n.files;\n"
        "  }\n"
        "  if (n.children) for (const c of n.children) { annotate(c, n); n._b += c._b; n._f += c._f; }\n"
        "}\n"
        "annotate(SUNBURST, null);\n"
        "\n"
        "/* Color anchor: index of each node's ancestor among the root's children. */\n"
        "(function paint() {\n"
        "  const top = SUNBURST.children || [];\n"
        "  top.forEach(function (c, i) { (function mark(n) {\n"
        "    n._ci = i;\n"
        "    if (n.children) n.children.forEach(mark);\n"
        "  })(c); });\n"
        "})();\n"
        "\n"
        "function val(n) { return mode === 'bytes' ? n._b : n._f; }\n"
        "\n"
        "/* Children plus a synthetic wedge for bytes/files sitting directly in n.\n"
        "   With buckets on, the wedge shows the filtered self value. Leaves and the\n"
        "   synthetic wedge itself carry no bucket matrix, so bail out before the\n"
        "   selfSum below: feeding the wedge back through selfSum would throw. */\n"
        "function kidsOf(n) {\n"
        "  const kids = (n.children || []).slice();\n"
        "  if (!n.children) return kids;\n"
        "  const sb = HAS_BUCKETS ? selfSum(n, 'bucket_bytes') : n.bytes;\n"
        "  const sf = HAS_BUCKETS ? selfSum(n, 'bucket_files') : n.files;\n"
        "  const self = mode === 'bytes' ? sb : sf;\n"
        "  if (self > 0)\n"
        "    kids.unshift({ name: '(self)', path: n.path, bytes: sb, files: sf,\n"
        "                   _b: sb, _f: sf, self: true });\n"
        "  return kids;\n"
        "}\n"
        "\n"
        "function layout(root) {\n"
        "  const nodes = [];\n"
        "  root.x0 = 0; root.x1 = Math.PI * 2; root.y = 0;\n"
        "  const stack = [root];\n"
        "  while (stack.length) {\n"
        "    const n = stack.pop();\n"
        "    nodes.push(n);\n"
        "    const total = val(n);\n"
        "    if (!(total > 0)) continue;\n"
        "    let x = n.x0;\n"
        "    for (const c of kidsOf(n)) {\n"
        "      const cv = val(c);\n"
        "      const w = (n.x1 - n.x0) * (cv / total);\n"
        "      c.x0 = x; c.x1 = x + w; c.y = n.y + 1;\n"
        "      x = c.x1;\n"
        "      if (cv > 0) stack.push(c);\n"
        "    }\n"
        "  }\n"
        "  return nodes;\n"
        "}\n"
        "\n"
        "const NS = 'http://www.w3.org/2000/svg';\n"
        "const svg = document.getElementById('chart');\n"
        "const tip = document.getElementById('tip');\n"
        "const CX = 500, CY = 500, R0 = 95, RMAX = 480;\n"
        "\n"
        "function polar(a, r) { return [CX + r * Math.sin(a), CY - r * Math.cos(a)]; }\n"
        "\n"
        "/* Shared canvas for exact label width measurement (same font as #chart text). */\n"
        "const lblCtx = document.createElement('canvas').getContext('2d');\n"
        "lblCtx.font = '11px -apple-system, \"Segoe UI\", Roboto, Helvetica, Arial, sans-serif';\n"
        "function labelWidth(s) { return lblCtx.measureText(s).width; }\n"
        "\n"
        "/* Rotated glyph box of a wedge label: w wide, ~11px tall, baseline through the\n"
        "   anchor, text-anchor middle; 1px padding keeps neighbors visually separated. */\n"
        "function labelRect(pt, w, deg) {\n"
        "  const hw = w / 2 + 1, up = 10, dn = 4;\n"
        "  const cs = Math.cos(deg * Math.PI / 180), sn = Math.sin(deg * Math.PI / 180);\n"
        "  return [[-hw, -up], [hw, -up], [hw, dn], [-hw, dn]].map(function (o) {\n"
        "    return [pt[0] + o[0] * cs - o[1] * sn, pt[1] + o[0] * sn + o[1] * cs];\n"
        "  });\n"
        "}\n"
        "/* Separating-axis test for two rotated rectangles. */\n"
        "function rectsHit(a, b) {\n"
        "  for (const r of [a, b]) {\n"
        "    for (let i = 0; i < 4; i++) {\n"
        "      const p1 = r[i], p2 = r[(i + 1) % 4];\n"
        "      const ax = [-(p2[1] - p1[1]), p2[0] - p1[0]];\n"
        "      let a0 = 1e18, a1 = -1e18, b0 = 1e18, b1 = -1e18;\n"
        "      for (const p of a) { const d = p[0] * ax[0] + p[1] * ax[1]; if (d < a0) a0 = d; if (d > a1) a1 = d; }\n"
        "      for (const p of b) { const d = p[0] * ax[0] + p[1] * ax[1]; if (d < b0) b0 = d; if (d > b1) b1 = d; }\n"
        "      if (a1 < b0 || b1 < a0) return false;\n"
        "    }\n"
        "  }\n"
        "  return true;\n"
        "}\n"
        "\n"
        "function arcPath(x0, x1, r0, r1) {\n"
        "  const large = (x1 - x0) > Math.PI ? 1 : 0;\n"
        "  const p00 = polar(x0, r1), p01 = polar(x1, r1), p10 = polar(x1, r0), p11 = polar(x0, r0);\n"
        "  return 'M' + p00[0].toFixed(2) + ' ' + p00[1].toFixed(2) +\n"
        "    'A' + r1 + ' ' + r1 + ' 0 ' + large + ' 1 ' + p01[0].toFixed(2) + ' ' + p01[1].toFixed(2) +\n"
        "    'L' + p10[0].toFixed(2) + ' ' + p10[1].toFixed(2) +\n"
        "    'A' + r0 + ' ' + r0 + ' 0 ' + large + ' 0 ' + p11[0].toFixed(2) + ' ' + p11[1].toFixed(2) + 'Z';\n"
        "}\n"
        "\n"
        "function color(n) {\n"
        "  if (n.self || n.name === '(other)') return '#d0d5dd';\n"
        "  if (HAS_BUCKETS && colorBy !== 'dir') {\n"
        "    /* Dominant selected cell, following the bytes/files mode. Age runs\n"
        "       green (fresh) to red (old); size runs blue (small) to magenta (large). */\n"
        "    const m = mode === 'bytes' ? n.bucket_bytes : n.bucket_files;\n"
        "    if (m) {\n"
        "      let best = 0, bi = -1;\n"
        "      for (let a = 0; a < 6; a++) {\n"
        "        if (!ageSel[a]) continue;\n"
        "        for (let z = 0; z < 6; z++) {\n"
        "          if (!sizeSel[z]) continue;\n"
        "          const v = m[a * 6 + z];\n"
        "          if (v > best) { best = v; bi = colorBy === 'age' ? a : z; }\n"
        "        }\n"
        "      }\n"
        "      if (bi < 0) return '#e4e7ec';\n"
        "      const hue = colorBy === 'age' ? 140 - bi * 28 : 210 + bi * 18;\n"
        "      return 'hsl(' + hue + ' 62% 52%)';\n"
        "    }\n"
        "  }\n"
        "  const hue = ((n._ci || 0) * 137.508) % 360;\n"
        "  let d = 0, a = n;\n"
        "  while (a._p && a._p !== SUNBURST) { d++; a = a._p; }\n"
        "  if (a._p === SUNBURST) d++;\n"
        "  const light = Math.max(38, 68 - Math.min(d, 10) * 2.6);\n"
        "  return 'hsl(' + hue.toFixed(1) + ' 58% ' + light + '%)';\n"
        "}\n"
        "\n"
        "function fmtB(x) {\n"
        "  const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];\n"
        "  let i = 0;\n"
        "  while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }\n"
        "  return (x >= 100 || i === 0 ? Math.round(x) : x.toFixed(1)) + ' ' + u[i];\n"
        "}\n"
        "function fmtN(x) { return x.toLocaleString('en-US'); }\n"
        "function fmtV(n) { return mode === 'bytes' ? fmtB(val(n)) : fmtN(val(n)) + ' files'; }\n"
        "\n"
        "function showTip(ev, n) {\n"
        "  const pct = val(cur) > 0 ? (100 * val(n) / val(cur)) : 0;\n"
        "  tip.innerHTML = '<strong></strong><br><span class=\"dim\"></span><br>' +\n"
        "    fmtB(n._b) + ' · ' + fmtN(n._f) + ' files · ' + (pct >= 0.05 ? pct.toFixed(1) : '<0.1') + '% of view';\n"
        "  tip.querySelector('strong').textContent = n.name || '(root)';\n"
        "  tip.querySelector('.dim').textContent = n.path || '';\n"
        "  tip.hidden = false;\n"
        "  const pad = 14;\n"
        "  let x = ev.clientX + pad, y = ev.clientY + pad;\n"
        "  const r = tip.getBoundingClientRect();\n"
        "  if (x + r.width > innerWidth - 8) x = ev.clientX - r.width - pad;\n"
        "  if (y + r.height > innerHeight - 8) y = ev.clientY - r.height - pad;\n"
        "  tip.style.left = x + 'px'; tip.style.top = y + 'px';\n"
        "}\n"
        "\n"
        "function renderCrumbs() {\n"
        "  const el = document.getElementById('crumbs');\n"
        "  el.innerHTML = '';\n"
        "  const chain = [];\n"
        "  for (let n = cur; n; n = n._p) chain.unshift(n);\n"
        "  chain.forEach(function (n, i) {\n"
        "    if (i) el.appendChild(document.createTextNode(' > '));\n"
        "    const a = document.createElement('a');\n"
        "    a.textContent = n === SUNBURST ? (n.path || n.name || 'root') : (n.name || n.path);\n"
        "    a.title = n.path || '';\n"
        "    a.addEventListener('click', function () { cur = n; render(); });\n"
        "    el.appendChild(a);\n"
        "  });\n"
        "}\n"
        "\n"
        "function render() {\n"
        "  svg.innerHTML = '';\n"
        "  renderCrumbs();\n"
        "  const nodes = layout(cur);\n"
        "  let maxY = 1;\n"
        "  for (const n of nodes) if (n.y > maxY && val(n) > 0) maxY = n.y;\n"
        "  maxY = Math.min(maxY, 7);\n"
        "  const ring = (RMAX - R0) / maxY;\n"
        "  const placedLabels = [];\n"
        "\n"
        "  for (const n of nodes) {\n"
        "    if (n === cur || n.y > maxY) continue;\n"
        "    if (val(n) <= 0) continue;\n"
        "    const r0 = R0 + (n.y - 1) * ring, r1 = R0 + n.y * ring;\n"
        "    const p = document.createElementNS(NS, 'path');\n"
        "    p.setAttribute('d', arcPath(n.x0, n.x1, r0, r1));\n"
        "    p.setAttribute('fill', color(n));\n"
        "    const zoomable = !n.self && n.name !== '(other)';\n"
        "    if (!zoomable) p.classList.add('leafless');\n"
        "    p.addEventListener('mousemove', function (ev) { showTip(ev, n); });\n"
        "    p.addEventListener('mouseleave', function () { tip.hidden = true; });\n"
        "    if (zoomable) p.addEventListener('click', function () { cur = n; tip.hidden = true; render(); });\n"
        "    svg.appendChild(p);\n"
        "\n"
        "    const aw = n.x1 - n.x0;\n"
        "    const rMid = (r0 + r1) / 2;\n"
        "    const arcLen = aw * rMid;\n"
        "    if (arcLen > 24 && n.name) {\n"
        "      /* Fit the label to the wedge's arc with a small margin, using measured\n"
        "         glyph widths -- a fixed per-char estimate spills onto neighbors on\n"
        "         caps/digit-heavy names. Full name if it fits, else an ellipsis\n"
        "         truncation with >= 5 visible chars, else no label at all. */\n"
        "      let label = n.name;\n"
        "      const w0 = labelWidth(label);\n"
        "      if (w0 > arcLen - 6) {\n"
        "        let k = Math.max(5, Math.floor(label.length * (arcLen - 6) / w0) - 1);\n"
        "        while (k > 5 && labelWidth(label.slice(0, k - 1) + '…') > arcLen - 6) k--;\n"
        "        label = labelWidth(label.slice(0, k - 1) + '…') <= arcLen - 6 ? label.slice(0, k - 1) + '…' : '';\n"
        "      }\n"
        "      if (label) {\n"
        "        const aMid = (n.x0 + n.x1) / 2;\n"
        "        const pt = polar(aMid, rMid);\n"
        "        let deg = aMid * 180 / Math.PI - 90;\n"
        "        if (deg > 90) deg -= 180;\n"
        "        if (deg < -90) deg += 180;\n"
        "        /* Skip a label whose rotated box would touch an already-placed one:\n"
        "           long tangent labels on adjacent rings cross even when each fits its\n"
        "           own wedge. Parents come first in layout order, so the dropped label\n"
        "           is usually the redundant deeper one of a stacked chain. */\n"
        "        const rect = labelRect(pt, labelWidth(label), deg);\n"
        "        let hits = false;\n"
        "        for (const q of placedLabels) { if (rectsHit(rect, q)) { hits = true; break; } }\n"
        "        if (!hits) {\n"
        "          const t = document.createElementNS(NS, 'text');\n"
        "          t.setAttribute('x', pt[0]); t.setAttribute('y', pt[1]);\n"
        "          t.setAttribute('text-anchor', 'middle');\n"
        "          t.setAttribute('transform', 'rotate(' + deg.toFixed(1) + ' ' + pt[0] + ' ' + pt[1] + ')');\n"
        "          t.textContent = label;\n"
        "          svg.appendChild(t);\n"
        "          placedLabels.push(rect);\n"
        "        }\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "\n"
        "  const c = document.createElementNS(NS, 'circle');\n"
        "  c.setAttribute('cx', CX); c.setAttribute('cy', CY); c.setAttribute('r', R0 - 8);\n"
        "  c.setAttribute('fill', '#fff'); c.setAttribute('stroke', '#d0d5dd');\n"
        "  c.style.cursor = cur === SUNBURST ? 'default' : 'pointer';\n"
        "  c.addEventListener('click', function () { if (cur._p) { cur = cur._p; render(); } });\n"
        "  svg.appendChild(c);\n"
        "\n"
        "  const label = cur === SUNBURST ? (cur.path || cur.name || 'root') : cur.name;\n"
        "  const t1 = document.createElementNS(NS, 'text');\n"
        "  t1.setAttribute('x', CX); t1.setAttribute('y', CY - 8);\n"
        "  t1.setAttribute('text-anchor', 'middle');\n"
        "  t1.style.fontSize = '13px'; t1.style.fontWeight = '600';\n"
        "  t1.textContent = label.length > 22 ? '…' + label.slice(-21) : label;\n"
        "  const t2 = document.createElementNS(NS, 'text');\n"
        "  t2.setAttribute('x', CX); t2.setAttribute('y', CY + 12);\n"
        "  t2.setAttribute('text-anchor', 'middle');\n"
        "  t2.style.fontSize = '12px'; t2.style.fill = '#667085';\n"
        "  t2.textContent = fmtV(cur);\n"
        "  svg.appendChild(t1); svg.appendChild(t2);\n"
        "}\n"
        "\n"
        "document.getElementById('btn-bytes').addEventListener('click', function () {\n"
        "  if (mode === 'bytes') return;\n"
        "  mode = 'bytes';\n"
        "  this.classList.add('on');\n"
        "  document.getElementById('btn-files').classList.remove('on');\n"
        "  render();\n"
        "});\n"
        "document.getElementById('btn-files').addEventListener('click', function () {\n"
        "  if (mode === 'files') return;\n"
        "  mode = 'files';\n"
        "  this.classList.add('on');\n"
        "  document.getElementById('btn-bytes').classList.remove('on');\n"
        "  render();\n"
        "});\n"
        "\n"
        "/* Bucket filter chips: toggle a bucket in/out of every wedge's total.\n"
        "   A chip carries its bucket's hue (same scale as the legend swatches and\n"
        "   the wedges in the matching bucket color mode): full bucket color when\n"
        "   on, a light tint of it when off, so the toggle state stays obvious.\n"
        "   Chips stay neutral unless their bucket type is the active color mode:\n"
        "   directory-mode wedges use per-directory hues, and age chips would not\n"
        "   match a size-colored chart (or vice versa). */\n"
        "if (HAS_BUCKETS) {\n"
        "  document.getElementById('filters').hidden = false;\n"
        "  const chipPaints = [];\n"
        "  const buildChips = function (elId, names, sel, mode) {\n"
        "    const el = document.getElementById(elId);\n"
        "    names.forEach(function (nm, i) {\n"
        "      const b = document.createElement('button');\n"
        "      const hue = mode === 'age' ? 140 - i * 28 : 210 + i * 18;\n"
        "      const paint = function () {\n"
        "        if (colorBy !== mode) {\n"
        "          /* Neutral: fall back to the .chip / .chip.on CSS classes. */\n"
        "          b.style.background = '';\n"
        "          b.style.borderColor = '';\n"
        "          b.style.color = '';\n"
        "          b.style.fontWeight = '';\n"
        "          return;\n"
        "        }\n"
        "        if (sel[i]) {\n"
        "          b.style.background = 'hsl(' + hue + ' 62% 52%)';\n"
        "          b.style.borderColor = 'hsl(' + hue + ' 62% 40%)';\n"
        "          b.style.color = '#fff';\n"
        "          b.style.fontWeight = '600';\n"
        "        } else {\n"
        "          b.style.background = 'hsl(' + hue + ' 62% 94%)';\n"
        "          b.style.borderColor = 'hsl(' + hue + ' 62% 70%)';\n"
        "          b.style.color = 'hsl(' + hue + ' 55% 32%)';\n"
        "          b.style.fontWeight = '400';\n"
        "        }\n"
        "      };\n"
        "      b.type = 'button';\n"
        "      b.className = 'chip on';\n"
        "      b.textContent = nm;\n"
        "      paint();\n"
        "      chipPaints.push(paint);\n"
        "      b.addEventListener('click', function () {\n"
        "        sel[i] = sel[i] ? 0 : 1;\n"
        "        b.classList.toggle('on', !!sel[i]);\n"
        "        paint();\n"
        "        annotate(SUNBURST, null);\n"
        "        render();\n"
        "      });\n"
        "      el.appendChild(b);\n"
        "    });\n"
        "  };\n"
        "  buildChips('chips-age', AGE_NAMES, ageSel, 'age');\n"
        "  buildChips('chips-size', SIZE_NAMES, sizeSel, 'size');\n"
        "  document.getElementById('colorby').addEventListener('change', function () {\n"
        "    colorBy = this.value;\n"
        "    chipPaints.forEach(function (p) { p(); });\n"
        "    render();\n"
        "  });\n"
        "}\n"
        "\n"
        "/* User picker (aggregate reports that materialized per-user pages):\n"
        "   navigate to the selected user's sunburst. */\n"
        "if (USERS.length > 0) {\n"
        "  const pick = document.getElementById('userpick');\n"
        "  USERS.forEach(function (u, i) {\n"
        "    const o = document.createElement('option');\n"
        "    o.value = u[1];\n"
        "    o.textContent = u[0];\n"
        "    if (i === USER_CUR) o.selected = true;\n"
        "    pick.appendChild(o);\n"
        "  });\n"
        "  pick.addEventListener('change', function () {\n"
        "    if (this.value) window.location.href = this.value;\n"
        "  });\n"
        "  document.getElementById('userpick-wrap').hidden = false;\n"
        "}\n"
        "\n"
        "/* Collapsible legend: last filter-panel row on bucket reports, below the\n"
        "   chart otherwise. The directory scheme always applies; the bucket\n"
        "   scales only when this page carries bucket data. */\n"
        "(function buildLegend() {\n"
        "  const body = document.getElementById('legend-body');\n"
        "  function row(title) {\n"
        "    const d = document.createElement('div');\n"
        "    d.className = 'lrow';\n"
        "    if (title) {\n"
        "      const t = document.createElement('span');\n"
        "      t.className = 'ltitle';\n"
        "      t.textContent = title;\n"
        "      d.appendChild(t);\n"
        "    }\n"
        "    body.appendChild(d);\n"
        "    return d;\n"
        "  }\n"
        "  function sw(d, color, label) {\n"
        "    const item = document.createElement('span');\n"
        "    item.className = 'switem';\n"
        "    const box = document.createElement('span');\n"
        "    box.className = 'sw';\n"
        "    box.style.background = color;\n"
        "    item.appendChild(box);\n"
        "    item.appendChild(document.createTextNode(label));\n"
        "    d.appendChild(item);\n"
        "  }\n"
        "  const d0 = row('Directory');\n"
        "  d0.appendChild(document.createTextNode('each top-level directory gets its own hue, '\n"
        "    + 'deeper levels are lighter shades. Grey wedges: (self) = files directly in that '\n"
        "    + 'directory, (other) = smaller siblings folded together.'));\n"
        "  if (HAS_BUCKETS) {\n"
        "    const d1 = row('Age');\n"
        "    for (let i = 0; i < 6; i++) sw(d1, 'hsl(' + (140 - i * 28) + ' 62% 52%)', AGE_NAMES[i]);\n"
        "    const d2 = row('Size');\n"
        "    for (let i = 0; i < 6; i++) sw(d2, 'hsl(' + (210 + i * 18) + ' 62% 52%)', SIZE_NAMES[i]);\n"
        "    const d3 = row('');\n"
        "    d3.appendChild(document.createTextNode('In a bucket color mode each wedge takes the hue '\n"
        "      + 'of the bucket holding most of its selected bytes (or files).'));\n"
        "  }\n"
        "})();\n"
        "\n"
        "render();\n"
        "</script>\n</body>\n</html>\n", out);
}

/* The user picker data: a JS array of [label, href] pairs plus the selected
 * index. Emitted between the tree JSON and the static suffix so every page
 * defines USERS/USER_CUR, picker or not. */
static void sb_users_js(FILE *out, const ereport_sunburst_link_t *users, size_t n_users,
                        long current_user) {
    size_t i;

    fputs("const USERS = [", out);
    for (i = 0; i < n_users; i++) {
        fprintf(out, "%s[\"", i ? "," : "");
        sb_json_escape(out, users[i].label);
        fputs("\",\"", out);
        sb_json_escape(out, users[i].href);
        fputs("\"]", out);
    }
    fprintf(out, "];\nconst USER_CUR = %ld;\n", current_user);
}

int ereport_sunburst_write_ex(const ereport_sunburst_tree_t *t, const char *out_dir,
                              const char *base_name, const char *subject,
                              const char *report_href,
                              const ereport_sunburst_link_t *users, size_t n_users,
                              long current_user) {
    char path[PATH_MAX];
    FILE *out;
    int n;

    if (!t || !out_dir || !out_dir[0] || !base_name || !base_name[0] ||
        !report_href || !report_href[0]) return -1;

    n = snprintf(path, sizeof(path), "%s/%s.json", out_dir, base_name);
    if (n < 0 || (size_t)n >= sizeof(path)) return -1;
    out = fopen(path, "w");
    if (!out) return -1;
    if (sb_json_write(t, out) != 0 || ferror(out)) {
        fclose(out);
        return -1;
    }
    if (fclose(out) != 0) return -1;

    n = snprintf(path, sizeof(path), "%s/%s.html", out_dir, base_name);
    if (n < 0 || (size_t)n >= sizeof(path)) return -1;
    out = fopen(path, "w");
    if (!out) return -1;
    sb_html_prefix(out, subject, base_name, report_href, t->bucket_bytes != NULL);
    if (sb_json_write(t, out) != 0) {
        fclose(out);
        return -1;
    }
    fputs(";\n", out);
    sb_users_js(out, users, n_users, current_user);
    sb_html_suffix(out);
    if (fclose(out) != 0) return -1;
    return 0;
}

uint64_t ereport_sunburst_tree_total_bytes(const ereport_sunburst_tree_t *t) {
    if (!t || t->root < 0) return 0;
    return t->nodes[t->root].total_bytes + t->root_boost_bytes;
}

uint64_t ereport_sunburst_tree_total_files(const ereport_sunburst_tree_t *t) {
    if (!t || t->root < 0) return 0;
    return t->nodes[t->root].total_files + t->root_boost_files;
}
