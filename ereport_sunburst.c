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
#include "ereport_sunburst.h"

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
    a->bytes = calloc((size_t)max_dir_id + 1, sizeof(*a->bytes));
    a->files = calloc((size_t)max_dir_id + 1, sizeof(*a->files));
    if (!a->bytes || !a->files) {
        ereport_sunburst_accum_free(a);
        return -1;
    }
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
 * name bytes last for full determinism (siblings routinely tie on files). */
static const sb_cagg_t *g_sb_cagg;

static int sb_cagg_cmp_bytes_desc(const void *pa, const void *pb) {
    const sb_cagg_t *a = &g_sb_cagg[*(const int32_t *)pa];
    const sb_cagg_t *b = &g_sb_cagg[*(const int32_t *)pb];
    int c;
    if (a->bytes != b->bytes) return (a->bytes < b->bytes) ? 1 : -1;
    if (a->files != b->files) return (a->files < b->files) ? 1 : -1;
    c = memcmp(a->name, b->name, a->name_len < b->name_len ? a->name_len : b->name_len);
    if (c) return c;
    return (a->name_len > b->name_len) - (a->name_len < b->name_len);
}
static int sb_cagg_cmp_files_desc(const void *pa, const void *pb) {
    const sb_cagg_t *a = &g_sb_cagg[*(const int32_t *)pa];
    const sb_cagg_t *b = &g_sb_cagg[*(const int32_t *)pb];
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
                         const char *rewrite_from, const char *rewrite_to) {
    unsigned depth = t->nodes[ni].depth;
    const char *parent_path = t->nodes[ni].path; /* stable: the nodes array may move, strings don't */
    uint64_t min_b, min_f, other_b = 0, other_f = 0;
    int32_t *ord = NULL;
    char *keep = NULL;
    int rc = -1;

    if (depth >= t->depth_max || t->n >= t->budget || cm->n == 0) return 0;

    ord = malloc(cm->n * sizeof(*ord));
    keep = calloc(cm->n, 1);
    if (!ord || !keep) goto out;
    for (size_t i = 0; i < cm->n; i++) ord[i] = (int32_t)i;

    g_sb_cagg = cm->ents;
    qsort(ord, cm->n, sizeof(*ord), sb_cagg_cmp_bytes_desc);
    for (size_t i = 0; i < cm->n && i < SUNBURST_TOP_N; i++) keep[ord[i]] = 1;
    qsort(ord, cm->n, sizeof(*ord), sb_cagg_cmp_files_desc);
    for (size_t i = 0; i < cm->n && i < SUNBURST_TOP_N; i++) keep[ord[i]] = 1;

    min_b = (uint64_t)((long double)t->nodes[ni].total_bytes * SUNBURST_MIN_FRAC);
    min_f = (uint64_t)((long double)t->nodes[ni].total_files * SUNBURST_MIN_FRAC);

    qsort(ord, cm->n, sizeof(*ord), sb_cagg_cmp_bytes_desc); /* intern in bytes order */
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
        sb_cmap_init(&c2);
        if (sb_enum_entry(sh, e, &c2) == 0)
            rc = sb_expand_map(t, sh, ci, corig, &c2, rewrite_from, rewrite_to);
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
    }
    rc = 0;
out:
    free(ord);
    free(keep);
    return rc;
}

/* Per-shard prep: roll the scan-time accumulator up the catalog tree (self ->
 * subtree totals, in place; dir_ids are handed out parent-first, so a single
 * reverse pass settles every descendant), then build the child index for
 * O(fanout) enumeration. Shards are independent, so this runs in parallel. */
static int sb_prep_shard(sb_shard_t *s) {
    const crawl_bin_catalog_t *cat = s->cat;
    ereport_sunburst_accum_t *a = s->acc;
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

    s->first_child = calloc((size_t)nd + 1, sizeof(*s->first_child));
    s->next_sibling = calloc((size_t)nd + 1, sizeof(*s->next_sibling));
    if (!s->first_child || !s->next_sibling) return -1;
    for (d = 2; d <= nd; d++) {
        uint64_t p = cat->parent_dir_id[d];
        if (p < 1 || p > nd) continue;
        s->next_sibling[d] = s->first_child[p];
        s->first_child[p] = (uint32_t)d;
    }
    return 0;
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

ereport_sunburst_tree_t *ereport_sunburst_build(crawl_bin_catalog_t *const *cats,
                                                ereport_sunburst_accum_t *accs, size_t n,
                                                unsigned depth_max, unsigned threads,
                                                const char *rewrite_from, const char *rewrite_to) {
    ereport_sunburst_tree_t *t = calloc(1, sizeof(*t));
    sb_shard_t *sh = NULL;
    size_t ns = 0;
    sb_lid_t *cur_lids = NULL;
    size_t ncur = 0;
    uint64_t cur_b = 0, cur_f = 0, grand_b = 0, grand_f = 0;
    char cur_orig[PATH_MAX];
    char *display = NULL;
    int32_t ri;

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

    {
        sb_cmap_t cm;
        int rc;
        sb_cmap_init(&cm);
        if (sb_enum_children(sh, cur_lids, ncur, &cm) != 0) {
            sb_cmap_free(&cm);
            goto fail;
        }
        rc = sb_expand_map(t, sh, ri, cur_orig, &cm, rewrite_from, rewrite_to);
        sb_cmap_free(&cm);
        if (rc != 0) goto fail;
    }

    free(cur_lids);
    for (size_t i = 0; i < ns; i++) {
        free(sh[i].first_child);
        free(sh[i].next_sibling);
    }
    free(sh);
    return t;

fail:
    free(cur_lids);
    if (sh) {
        for (size_t i = 0; i < ns; i++) {
            free(sh[i].first_child);
            free(sh[i].next_sibling);
        }
        free(sh);
    }
    ereport_sunburst_tree_free(t);
    return NULL;
}

size_t ereport_sunburst_tree_nodes(const ereport_sunburst_tree_t *t) {
    return t ? t->n : 0;
}

void ereport_sunburst_tree_free(ereport_sunburst_tree_t *t) {
    if (!t) return;
    for (size_t i = 0; i < t->n; i++) free(t->nodes[i].path);
    free(t->nodes);
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

static const sb_node_t *g_sb_sort_nodes;

/* Deterministic totals order: primary metric desc, the other metric as tie-break
 * (siblings routinely tie on files, e.g. one file per directory), node id last. */
static int sb_cmp_bytes_desc(const void *pa, const void *pb) {
    int32_t ia = *(const int32_t *)pa, ib = *(const int32_t *)pb;
    const sb_node_t *a = &g_sb_sort_nodes[ia], *b = &g_sb_sort_nodes[ib];
    if (a->total_bytes != b->total_bytes) return (a->total_bytes < b->total_bytes) ? 1 : -1;
    if (a->total_files != b->total_files) return (a->total_files < b->total_files) ? 1 : -1;
    return (ia > ib) - (ia < ib);
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
        fprintf(out, "\",\"bytes\":%" PRIu64 ",\"files\":%" PRIu64 "}",
                is_root ? n->total_bytes + ctx->self_boost_bytes : n->total_bytes,
                is_root ? n->total_files + ctx->self_boost_files : n->total_files);
        ctx->emitted++;
        free(kids);
        return 0;
    }

    fprintf(out, "{\"name\":\"");
    sb_json_escape(out, n->name);
    fprintf(out, "\",\"path\":\"");
    sb_json_escape(out, n->path);
    fprintf(out, "\",\"bytes\":%" PRIu64 ",\"files\":%" PRIu64 ",\"children\":[", self_b, self_f);
    ctx->emitted++;

    g_sb_sort_nodes = t->nodes;
    qsort(kids, nkids, sizeof(*kids), sb_cmp_bytes_desc);
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

static void sb_html_prefix(FILE *out, const char *subject) {
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
          "</style>\n</head>\n<body>\n", out);
    fputs("<div class=\"topbar\">\n<h1>Sunburst", out);
    if (subject) {
        fputs(" — ", out);
        sb_html_escape(out, subject);
    }
    fputs("</h1>\n<div class=\"crumbs\" id=\"crumbs\"></div>\n"
          "<div class=\"toggle\"><button id=\"btn-bytes\" class=\"on\" type=\"button\">Bytes</button>"
          "<button id=\"btn-files\" type=\"button\">Files</button></div>\n"
          "<a href=\"index.html\">&larr; report</a>\n</div>\n"
          "<div id=\"chartwrap\"><svg id=\"chart\" viewBox=\"0 0 1000 1000\" role=\"img\" "
          "aria-label=\"Sunburst chart\"></svg></div>\n"
          "<div class=\"hint\">Click a wedge to zoom in; click the center to go back up. "
          "Data: <a href=\"sunburst.json\">sunburst.json</a>.</div>\n"
          "<div id=\"tip\" hidden></div>\n"
          "<script>\nconst SUNBURST = ", out);
}

static void sb_html_suffix(FILE *out) {
    fputs(";\n", out);
    fputs(
        "let mode = 'bytes';\n"
        "let cur = SUNBURST;\n"
        "\n"
        "/* Totals: internal nodes carry self values, leaves carry subtree totals. */\n"
        "(function annotate(n, parent) {\n"
        "  n._p = parent || null;\n"
        "  n._b = n.bytes; n._f = n.files;\n"
        "  if (n.children) for (const c of n.children) { annotate(c, n); n._b += c._b; n._f += c._f; }\n"
        "})(SUNBURST, null);\n"
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
        "/* Children plus a synthetic wedge for bytes/files sitting directly in n. */\n"
        "function kidsOf(n) {\n"
        "  const kids = (n.children || []).slice();\n"
        "  const self = mode === 'bytes' ? n.bytes : n.files;\n"
        "  if (n.children && self > 0)\n"
        "    kids.unshift({ name: '(self)', path: n.path, bytes: n.bytes, files: n.files,\n"
        "                   _b: n.bytes, _f: n.files, self: true });\n"
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
        "    if (aw * rMid > 42 && n.name) {\n"
        "      const aMid = (n.x0 + n.x1) / 2;\n"
        "      const pt = polar(aMid, rMid);\n"
        "      const t = document.createElementNS(NS, 'text');\n"
        "      let deg = aMid * 180 / Math.PI - 90;\n"
        "      if (deg > 90) deg -= 180;\n"
        "      if (deg < -90) deg += 180;\n"
        "      t.setAttribute('x', pt[0]); t.setAttribute('y', pt[1]);\n"
        "      t.setAttribute('text-anchor', 'middle');\n"
        "      t.setAttribute('transform', 'rotate(' + deg.toFixed(1) + ' ' + pt[0] + ' ' + pt[1] + ')');\n"
        "      const maxChars = Math.max(3, Math.floor(aw * rMid / 7));\n"
        "      t.textContent = n.name.length > maxChars ? n.name.slice(0, maxChars - 1) + '…' : n.name;\n"
        "      svg.appendChild(t);\n"
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
        "render();\n"
        "</script>\n</body>\n</html>\n", out);
}

int ereport_sunburst_write(const ereport_sunburst_tree_t *t, const char *out_dir, const char *subject) {
    char path[PATH_MAX];
    FILE *out;
    int n;

    if (!t || !out_dir || !out_dir[0]) return -1;

    n = snprintf(path, sizeof(path), "%s/sunburst.json", out_dir);
    if (n < 0 || (size_t)n >= sizeof(path)) return -1;
    out = fopen(path, "w");
    if (!out) return -1;
    if (sb_json_write(t, out) != 0 || ferror(out)) {
        fclose(out);
        return -1;
    }
    if (fclose(out) != 0) return -1;

    n = snprintf(path, sizeof(path), "%s/sunburst.html", out_dir);
    if (n < 0 || (size_t)n >= sizeof(path)) return -1;
    out = fopen(path, "w");
    if (!out) return -1;
    sb_html_prefix(out, subject);
    if (sb_json_write(t, out) != 0) {
        fclose(out);
        return -1;
    }
    sb_html_suffix(out);
    if (fclose(out) != 0) return -1;
    return 0;
}
