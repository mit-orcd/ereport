/*
 * ereport_sunburst — per-directory aggregation behind the report's sunburst view.
 *
 * The data comes from the scan ereport already runs: parse workers add each matched
 * record's accounted bytes / file count into per-shard arrays indexed by the record's
 * parent_dir_id (a column, so no path strings are read for this). After the workers
 * join — while the shard catalogs are still attached — the arrays are rolled up the
 * catalog tree into per-directory subtree totals, merged across shards by path, and
 * written as sunburst.json plus a self-contained sunburst.html next to index.html.
 *
 * Nothing here re-reads the crawl bins or parses report HTML.
 */
#ifndef EREPORT_SUNBURST_H
#define EREPORT_SUNBURST_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#include "crawl_bin_catalog.h"

/* Per-shard accumulation arrays, indexed by dir_id (slots 1..max_dir_id; 0 unused). */
typedef struct {
    _Atomic uint64_t *bytes; /* accounted bytes of matched records whose parent is dir_id */
    _Atomic uint64_t *files; /* regular-file matched records whose parent is dir_id */
    uint64_t max_dir_id;     /* arrays sized max_dir_id + 1; 0 = empty */
} ereport_sunburst_accum_t;

int ereport_sunburst_accum_init(ereport_sunburst_accum_t *a, uint64_t max_dir_id);
void ereport_sunburst_accum_free(ereport_sunburst_accum_t *a);

typedef struct ereport_sunburst_tree ereport_sunburst_tree_t;

/*
 * Roll the per-shard self totals up the catalog trees (single reverse pass over
 * dir_ids; parents are interned before children), then merge the shards
 * top-down from the displayed root — found by collapsing the single-child
 * chain from the top — materializing only the nodes the chart will show: per
 * node, the top-N children by bytes or by files that also clear the
 * min-fraction bar against their parent, with the rest folded into an
 * "(other)" node, down to depth_max levels below the root. Selection happens
 * before interning, so memory tracks the emitted chart, not the directory
 * count. rewrite_from/rewrite_to apply the --path-rewrite prefix swap to the
 * displayed paths (NULL = off).
 *
 * cats/accs are parallel arrays of length n; NULL catalogs are skipped. The
 * accumulators are mutated in place (the rollup turns self totals into subtree
 * totals) but not freed. threads parallelizes the per-shard rollup/index prep
 * (0 or 1 = serial).
 */
ereport_sunburst_tree_t *ereport_sunburst_build(crawl_bin_catalog_t *const *cats,
                                                ereport_sunburst_accum_t *accs, size_t n,
                                                unsigned depth_max, unsigned threads,
                                                const char *rewrite_from, const char *rewrite_to);

/*
 * Write <out_dir>/sunburst.json (the tool-agnostic data source) and
 * <out_dir>/sunburst.html (the self-contained chart page, JSON embedded).
 * Returns 0 on success. JSON schema: one root node
 * {"name","path","bytes","files","children":[...]}; internal nodes carry self
 * values, leaves carry subtree totals (a depth-folded leaf includes everything
 * below it), trimmed siblings fold into an "(other)" leaf.
 */
int ereport_sunburst_write(const ereport_sunburst_tree_t *t, const char *out_dir, const char *subject);

/* Node count and grand-total accessors for run stats. */
size_t ereport_sunburst_tree_nodes(const ereport_sunburst_tree_t *t);

void ereport_sunburst_tree_free(ereport_sunburst_tree_t *t);

#endif
