/*
 * dirs.idx / rowgroups.idx reader. See crawl_sidecar.h for the contract and
 * crawl_bin_format.h for the layouts.
 *
 * SPDX-License-Identifier: MIT
 */
#define _GNU_SOURCE

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "crawl_sidecar.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* True when [off, off+len) lies inside a mapping of map_len bytes, without overflowing. */
static int sidecar_span_ok(uint64_t off, uint64_t len, size_t map_len) {
    if (len > (uint64_t)map_len) return 0;
    if (off > (uint64_t)map_len - len) return 0;
    return 1;
}

static const char *sidecar_basename(const char *path) {
    const char *slash = strrchr(path, '/');

    return slash ? slash + 1 : path;
}

void crawl_sidecar_close(crawl_sidecar_t *sc) {
    size_t i;

    if (!sc) return;
    if (sc->fd) {
        for (i = 0; i < sc->shard_count; i++)
            if (sc->fd[i] >= 0) close(sc->fd[i]);
        free(sc->fd);
    }
    if (sc->dirs) {
        for (i = 0; i < sc->shard_count; i++) crawl_bin_catalog_map_free(&sc->dirs[i].cmap);
        free(sc->dirs);
    }
    free(sc->groups);
    if (sc->dmap) munmap(sc->dmap, sc->dlen);
    if (sc->rmap) munmap(sc->rmap, sc->rlen);
    memset(sc, 0, sizeof(*sc));
}

/* Map one sidecar and validate its header. Returns the mapping through *map_out, or -1. */
static int sidecar_map(const char *index_dir, const char *fname, const char *magic, unsigned char **map_out,
                       size_t *len_out, const crawl_sidecar_hdr_t **hdr_out) {
    char full[PATH_MAX];
    struct stat st;
    unsigned char *map;
    const crawl_sidecar_hdr_t *h;
    int fd;

    *map_out = NULL;
    *len_out = 0;
    *hdr_out = NULL;
    if (snprintf(full, sizeof(full), "%s/%s", index_dir, fname) >= (int)sizeof(full)) return -1;
    fd = open(full, O_RDONLY);
    if (fd < 0) return -1;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || (uint64_t)st.st_size < sizeof(*h)) {
        close(fd);
        return -1;
    }
    map = (unsigned char *)mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return -1;

    h = (const crawl_sidecar_hdr_t *)map;
    if (memcmp(h->magic, magic, CRAWL_SIDECAR_MAGIC_LEN) != 0 || h->version != CRAWL_SIDECAR_VERSION) {
        munmap(map, (size_t)st.st_size);
        return -1;
    }
    *map_out = map;
    *len_out = (size_t)st.st_size;
    *hdr_out = h;
    return 0;
}

/*
 * Does this descriptor still describe the shard on disk? Five facts, all cheap:
 * the basename, st_size, mtime, the catalog offset in the shard's own header,
 * and the entry count that opens the catalog blob. Anything moved and the
 * dir_ids and DFS positions the sidecar recorded may name something else
 * entirely, so the answer has to be no.
 */
static int sidecar_shard_matches(const crawl_sidecar_shard_id_t *id, const unsigned char *names,
                                 uint64_t names_bytes, const char *shard_name, int fd) {
    struct stat st;
    bin_file_header_t fh;
    uint64_t n_entries = 0;
    size_t nl = strlen(shard_name);

    if ((uint64_t)id->name_len != (uint64_t)nl) return 0;
    if (!sidecar_span_ok(id->name_off, id->name_len, (size_t)names_bytes)) return 0;
    if (memcmp(names + id->name_off, shard_name, nl) != 0) return 0;

    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) return 0;
    if ((uint64_t)st.st_size != id->shard_size) return 0;
    if ((uint64_t)st.st_mtim.tv_sec != id->shard_mtime_sec) return 0;
    if ((uint64_t)st.st_mtim.tv_nsec != id->shard_mtime_nsec) return 0;

    if (pread(fd, &fh, sizeof(fh), 0) != (ssize_t)sizeof(fh)) return 0;
    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) return 0;
    if (fh.catalog_offset != id->catalog_offset) return 0;
    if (id->catalog_offset < sizeof(fh) || id->catalog_offset > id->shard_size) return 0;
    if (id->shard_size - id->catalog_offset < sizeof(uint64_t)) return 0;
    if (pread(fd, &n_entries, sizeof(n_entries), (off_t)id->catalog_offset) != (ssize_t)sizeof(n_entries))
        return 0;
    if (n_entries != id->catalog_entries) return 0;
    return 1;
}

/*
 * Which sidecar shard is named `want`, or n?
 *
 * Matched by name rather than by position, because the caller's shard list is
 * its own: a single-user ereport run names one shard out of sixteen, while
 * ecrawl_query names all of them. A capture has tens of shards, not
 * thousands, so this stays a scan.
 */
static size_t sidecar_find_shard(const unsigned char *names, uint64_t names_bytes,
                                 const crawl_sidecar_shard_id_t *ids, size_t id_stride, size_t n,
                                 const char *want) {
    size_t wl = strlen(want);
    size_t i;

    for (i = 0; i < n; i++) {
        const crawl_sidecar_shard_id_t *id =
            (const crawl_sidecar_shard_id_t *)((const unsigned char *)ids + i * id_stride);

        if ((size_t)id->name_len != wl) continue;
        if (!sidecar_span_ok(id->name_off, id->name_len, (size_t)names_bytes)) return n;
        if (memcmp(names + id->name_off, want, wl) == 0) return i;
    }
    return n;
}

int crawl_sidecar_open(const char *index_dir, const char *const *shard_paths, size_t shard_count,
                       crawl_sidecar_t *sc) {
    const crawl_sidecar_hdr_t *dh = NULL, *rh = NULL;
    const crawl_dirx_shard_t *dsh = NULL;
    const crawl_rgix_shard_t *rsh = NULL;
    size_t fi;

    memset(sc, 0, sizeof(*sc));
    if (!index_dir || shard_count == 0) return -1;
    sc->shard_count = shard_count;

    sc->fd = (int *)malloc(shard_count * sizeof(int));
    sc->dirs = (crawl_dirx_view_t *)calloc(shard_count, sizeof(*sc->dirs));
    if (!sc->fd || !sc->dirs) {
        free(sc->fd);
        free(sc->dirs);
        memset(sc, 0, sizeof(*sc));
        return -1;
    }
    for (fi = 0; fi < shard_count; fi++) sc->fd[fi] = -1;
    for (fi = 0; fi < shard_count; fi++) {
        sc->fd[fi] = open(shard_paths[fi], O_RDONLY);
        if (sc->fd[fi] < 0) goto fail;
    }

    if (sidecar_map(index_dir, "dirs.idx", CRAWL_DIRX_MAGIC, &sc->dmap, &sc->dlen, &dh) != 0) goto fail;
    if (!sidecar_span_ok(dh->shard_dir_off, (uint64_t)dh->shard_count * sizeof(crawl_dirx_shard_t), sc->dlen) ||
        !sidecar_span_ok(dh->names_off, dh->names_bytes, sc->dlen))
        goto fail;
    dsh = (const crawl_dirx_shard_t *)(sc->dmap + dh->shard_dir_off);
    for (fi = 0; fi < shard_count; fi++) {
        const char *base = sidecar_basename(shard_paths[fi]);
        size_t si = sidecar_find_shard(sc->dmap + dh->names_off, dh->names_bytes, &dsh[0].id,
                                       sizeof(crawl_dirx_shard_t), (size_t)dh->shard_count, base);
        const crawl_dirx_shard_t *s;

        if (si >= (size_t)dh->shard_count) goto fail;
        s = &dsh[si];
        if (!sidecar_shard_matches(&s->id, sc->dmap + dh->names_off, dh->names_bytes, base, sc->fd[fi]))
            goto fail;
        if (s->hash_count > s->id.max_dir_id + 1ULL) goto fail;
        if (!sidecar_span_ok(s->hash_off, s->hash_count * sizeof(crawl_dirx_entry_t), sc->dlen)) goto fail;
        /* Rows come out of the shard, so the shard's own chunk table is what a
         * hit is resolved through; a catalog that will not describe itself
         * retires the sidecar exactly as a moved shard does. */
        if (crawl_bin_catalog_map_read(sc->fd[fi], s->id.catalog_offset, s->id.shard_size,
                                       &sc->dirs[fi].cmap) != 0)
            goto fail;
        if (sc->dirs[fi].cmap.n_entries != s->id.catalog_entries) goto fail;

        sc->dirs[fi].ents = (const crawl_dirx_entry_t *)(sc->dmap + s->hash_off);
        sc->dirs[fi].ent_count = s->hash_count;
        sc->dirs[fi].max_dir_id = s->id.max_dir_id;
        sc->dirs[fi].shard_size = s->id.shard_size;
        sc->dirs[fi].catalog_entries = s->id.catalog_entries;
        sc->dirs[fi].fd = sc->fd[fi];
    }
    sc->have_dirs = 1;

    sc->groups = (crawl_rgix_view_t *)calloc(shard_count, sizeof(*sc->groups));
    if (!sc->groups) return 0;
    if (sidecar_map(index_dir, "rowgroups.idx", CRAWL_RGIX_MAGIC, &sc->rmap, &sc->rlen, &rh) != 0) return 0;
    if (!sidecar_span_ok(rh->shard_dir_off, (uint64_t)rh->shard_count * sizeof(crawl_rgix_shard_t), sc->rlen) ||
        !sidecar_span_ok(rh->names_off, rh->names_bytes, sc->rlen))
        goto drop_groups;
    rsh = (const crawl_rgix_shard_t *)(sc->rmap + rh->shard_dir_off);
    for (fi = 0; fi < shard_count; fi++) {
        const char *base = sidecar_basename(shard_paths[fi]);
        size_t si = sidecar_find_shard(sc->rmap + rh->names_off, rh->names_bytes, &rsh[0].id,
                                       sizeof(crawl_rgix_shard_t), (size_t)rh->shard_count, base);
        const crawl_rgix_shard_t *s;

        if (si >= (size_t)rh->shard_count) goto drop_groups;
        s = &rsh[si];
        if (!sidecar_shard_matches(&s->id, sc->rmap + rh->names_off, rh->names_bytes, base, sc->fd[fi]))
            goto drop_groups;
        if (s->group_count > (uint64_t)sc->rlen / sizeof(crawl_rgix_group_t)) goto drop_groups;
        if (!sidecar_span_ok(s->groups_off, s->group_count * sizeof(crawl_rgix_group_t), sc->rlen))
            goto drop_groups;

        sc->groups[fi].groups = (const crawl_rgix_group_t *)(sc->rmap + s->groups_off);
        sc->groups[fi].group_count = s->group_count;
        sc->groups[fi].dfs_domain = s->dfs_domain;
    }
    sc->have_groups = 1;
    return 0;

drop_groups:
    munmap(sc->rmap, sc->rlen);
    sc->rmap = NULL;
    sc->rlen = 0;
    memset(sc->groups, 0, shard_count * sizeof(*sc->groups));
    return 0;

fail:
    crawl_sidecar_close(sc);
    return -1;
}

crawl_dirx_walk_t *crawl_dirx_walk_new(void) {
    crawl_dirx_walk_t *w = (crawl_dirx_walk_t *)malloc(sizeof(crawl_dirx_walk_t));

    if (w) {
        crawl_bin_catalog_chunk_init(&w->chunk);
        w->chunk_view = NULL;
    }
    return w;
}

void crawl_dirx_walk_free(crawl_dirx_walk_t *w) {
    if (!w) return;
    crawl_bin_catalog_chunk_free(&w->chunk);
    free(w);
}

/*
 * The row is read out of the shard rather than out of the sidecar, and the
 * sidecar is only advisory, so a dir_id it names that the catalog does not have
 * is a miss rather than something to believe.
 */
int crawl_dirx_read_row(const crawl_dirx_view_t *v, crawl_dirx_walk_t *w, uint64_t did, unsigned fields,
                        bin_dir_catalog_entry_t *ent, unsigned char *name, size_t name_cap,
                        size_t *name_len_out, uint64_t *rows_read) {
    const unsigned char *nb = NULL;
    size_t nl = 0;

    if (did == 0ULL || did > v->max_dir_id) return -1;
    if (w->chunk_view != (const void *)v) {
        /* The cache belongs to whichever shard filled it, and chunk offsets do
         * not survive the move. */
        crawl_bin_catalog_chunk_free(&w->chunk);
        w->chunk_view = (const void *)v;
    }
    if (crawl_bin_catalog_read_row(v->fd, &v->cmap, &w->chunk, did, fields, ent, &nb, &nl, NULL) != 0)
        return -1;
    if (nl > name_cap) return -1;
    if (nl > 0) memcpy(name, nb, nl);
    if (name_len_out) *name_len_out = nl;
    if (rows_read) (*rows_read)++;
    return 0;
}

/*
 * Deliberately a transcription of crawl_bin_catalog_dir_path_len, component
 * limit and all: the query is compared against the spelling that function
 * produces, so a route that improved on it -- say by not dropping the leading
 * components of a 200-deep path -- would disagree with the scan about which
 * directory the query names.
 */
int crawl_dirx_path_of(const crawl_dirx_view_t *v, uint64_t did, crawl_dirx_walk_t *w, char *out, size_t out_sz,
                       size_t *len_out, uint64_t *rows_read) {
    size_t nparts = 0;
    size_t tot = 0, pos = 0, pi;
    uint64_t cur = did;

    if (!out || out_sz == 0) return -1;
    out[0] = '\0';
    if (len_out) *len_out = 0;
    if (did == 0ULL || did > v->max_dir_id) return -1;

    while (cur != 0ULL && nparts < CRAWL_BIN_CATALOG_MAX_PATH_PARTS) {
        bin_dir_catalog_entry_t ent;
        size_t nl = 0;

        if (cur > v->max_dir_id) return -1;
        if (cur == 1ULL) break;
        if (crawl_dirx_read_row(v, w, cur, 0U, &ent, w->comp[nparts], sizeof(w->comp[0]), &nl, rows_read) != 0)
            return -1;
        w->clen[nparts] = nl;
        tot += nl + 1U; /* one leading '/' per component (paths are absolute) */
        nparts++;
        cur = ent.parent_dir_id;
    }

    if (tot + 1U > out_sz) return -1;
    for (pi = nparts; pi > 0; pi--) {
        size_t idx = pi - 1U;

        out[pos++] = '/';
        if (w->clen[idx] > 0) {
            memcpy(out + pos, w->comp[idx], w->clen[idx]);
            pos += w->clen[idx];
        }
    }
    out[pos] = '\0';
    if (len_out) *len_out = pos;
    return 0;
}

/*
 * Binary search to the first entry with the path's hash, then every entry
 * sharing it is rebuilt and compared. Hash equality is a hint; the string
 * decides.
 */
uint64_t crawl_dirx_lookup(const crawl_dirx_view_t *v, const char *path, size_t plen, crawl_dirx_walk_t *w,
                           uint64_t *rows_read) {
    uint64_t h = crawl_sidecar_path_hash(path, plen);
    uint64_t lo = 0, hi = v->ent_count;

    while (lo < hi) {
        uint64_t mid = lo + (hi - lo) / 2ULL;

        if (v->ents[mid].path_hash < h) lo = mid + 1ULL;
        else hi = mid;
    }
    for (; lo < v->ent_count && v->ents[lo].path_hash == h; lo++) {
        size_t got = 0;

        if (crawl_dirx_path_of(v, v->ents[lo].dir_id, w, w->path, sizeof(w->path), &got, rows_read) != 0)
            continue;
        if (got == plen && memcmp(w->path, path, plen) == 0) return v->ents[lo].dir_id;
    }
    return 0;
}

int crawl_sidecar_scope_subtree(const crawl_sidecar_t *sc, const char *subtree, size_t subtree_len,
                                const char *sub_parent, crawl_sidecar_scope_t *scope, uint64_t *rows_read) {
    crawl_dirx_walk_t *walk;
    size_t fi;
    int rc = -1;

    memset(scope, 0, sc->shard_count * sizeof(*scope));
    if (!sc->have_dirs) return -1;
    walk = crawl_dirx_walk_new();
    if (!walk) return -1;

    for (fi = 0; fi < sc->shard_count; fi++) {
        const crawl_dirx_view_t *v = &sc->dirs[fi];
        crawl_sidecar_scope_t *sp = &scope[fi];
        bin_dir_catalog_entry_t ent;
        unsigned char namebuf[256];
        uint64_t parent;

        sp->root = crawl_dirx_lookup(v, subtree, subtree_len, walk, rows_read);
        /* The subtree's own record hangs off its parent, outside the DFS range, so
         * the groups holding that parent's children have to be reachable too. */
        parent = crawl_dirx_lookup(v, sub_parent, strlen(sub_parent), walk, rows_read);
        if (parent == 0ULL && sub_parent[0] == '\0' && v->max_dir_id >= 1ULL) parent = 1ULL;

        if (sp->root != 0ULL) {
            if (crawl_dirx_read_row(v, walk, sp->root, CRAWL_CAT_SUBTREE, &ent, namebuf, sizeof(namebuf), NULL,
                                    rows_read) != 0)
                goto done;
            sp->root_parent = ent.parent_dir_id;
            sp->dfs_lo = ent.dfs_index;
            sp->dfs_hi = sp->dfs_lo + ent.dfs_subtree_dirs;
            sp->self_record = (ent.flags & CRAWL_DIR_FLAG_SELF_RECORD) ? 1 : 0;
        }
        if (parent != 0ULL) {
            if (crawl_dirx_read_row(v, walk, parent, CRAWL_CAT_SUBTREE, &ent, namebuf, sizeof(namebuf), NULL,
                                    rows_read) != 0)
                goto done;
            sp->dfs_par = ent.dfs_index;
            sp->have_par = 1;
        }
        sp->in_shard = (sp->root != 0ULL || sp->have_par);
    }
    rc = 0;

done:
    crawl_dirx_walk_free(walk);
    return rc;
}

/* Any bucket in [lo_bit, hi_bit] set? */
static int rgix_buckets_hit(const unsigned char *bits, unsigned lo_bit, unsigned hi_bit) {
    unsigned b;

    for (b = lo_bit; b <= hi_bit; b++)
        if (bits[b >> 3] & (unsigned char)(1U << (b & 7U))) return 1;
    return 0;
}

/*
 * Both sketches are conservative supersets on their own, so keeping only groups
 * both accept is still a superset: a real match sits inside the interval and
 * lights its own bucket, so neither test can reject it.
 */
int crawl_rgix_group_survives(const crawl_rgix_group_t *g, uint64_t dfs_domain, const crawl_sidecar_scope_t *sp,
                              int *interval_out, int *bitmap_out) {
    uint64_t lo = sp->dfs_lo, hi = sp->dfs_hi;
    int iv = 0, bm = 0;

    if (g->flags & CRAWL_RGIX_GRP_UNKNOWN) {
        /* The sketch is incomplete, so it proves nothing. */
        *interval_out = 1;
        *bitmap_out = 1;
        return 1;
    }
    if (g->dfs_min <= g->dfs_max) {
        if (lo < hi && g->dfs_max >= lo && g->dfs_min < hi) iv = 1;
        if (sp->have_par && sp->dfs_par >= g->dfs_min && sp->dfs_par <= g->dfs_max) iv = 1;

        if (lo < hi && rgix_buckets_hit(g->buckets, crawl_rgix_bucket_of(lo, dfs_domain),
                                        crawl_rgix_bucket_of(hi - 1ULL, dfs_domain)))
            bm = 1;
        if (sp->have_par) {
            unsigned pb = crawl_rgix_bucket_of(sp->dfs_par, dfs_domain);

            if (rgix_buckets_hit(g->buckets, pb, pb)) bm = 1;
        }
    }
    *interval_out = iv;
    *bitmap_out = bm;
    return iv && bm;
}

static int rgix_append_chunk(crawl_bin_file_chunk_t **all, size_t *all_n, size_t *all_cap, const char *path,
                             uint64_t lo, uint64_t hi, size_t file_index, uint64_t *byte_sum) {
    if (lo >= hi) return -1;
    if (crawl_bin_append_chunk(all, all_n, all_cap, path, lo, hi, file_index) != 0) return -1;
    *byte_sum += hi - lo;
    return 0;
}

int crawl_rgix_build_chunks(const crawl_sidecar_t *sc, const char *const *shard_paths, size_t shard_count,
                            const crawl_sidecar_scope_t *scope, unsigned nthreads, unsigned jobs_per_thread,
                            uint64_t split_min, uint64_t split_max, crawl_bin_file_chunk_t **chunks_out,
                            size_t *chunk_count_out, uint64_t *chunk_bytes_out, crawl_rgix_prune_stats_t *st) {
    crawl_bin_file_chunk_t *all = NULL;
    size_t all_n = 0, all_cap = 0;
    uint64_t byte_sum = 0;
    uint64_t split_target;
    size_t fi;
    int resolved_any = 0;

    memset(st, 0, sizeof(*st));
    *chunks_out = NULL;
    *chunk_count_out = 0;
    *chunk_bytes_out = 0;

    if (!sc->have_dirs || !sc->have_groups) return -1;

    /* Pass 1: total up what survives. Shards the subtree is not in still have
     * their groups and records counted, so a caller can report how much of the
     * capture the pruned scan stood in for. */
    for (fi = 0; fi < shard_count; fi++) {
        const crawl_rgix_view_t *rv = &sc->groups[fi];
        const crawl_sidecar_scope_t *sp = &scope[fi];
        uint64_t gi;

        if (sp->in_shard) resolved_any = 1;
        for (gi = 0; gi < rv->group_count; gi++) {
            const crawl_rgix_group_t *g = &rv->groups[gi];
            int iv = 0, bm = 0;

            st->total++;
            st->bytes_total += g->group_bytes;
            st->records_total += g->record_count;
            if (!sp->in_shard) continue;
            if (!crawl_rgix_group_survives(g, rv->dfs_domain, sp, &iv, &bm)) {
                if (iv) st->kept_interval++;
                if (bm) st->kept_bitmap++;
                continue;
            }
            st->kept_interval++;
            st->kept_bitmap++;
            st->kept++;
            st->bytes_kept += g->group_bytes;
            st->records_kept += g->record_count;
        }
    }

    if (!resolved_any) goto fail; /* the subtree is nowhere: let the usual path say so */
    if (st->kept == 0ULL) goto fail; /* nothing survived; scan normally rather than report a guess */
    /* Every group has to be read anyway, so there are no bytes to save and the
     * checkpoint segments are the better-tested way to cut the same file up. */
    if (st->kept == st->total) goto fail;

    /* Size the jobs from what will actually be read. Deriving the target from the
     * shard sizes, as the unpruned path does, would hand the pool a handful of
     * jobs when pruning has already thrown most of those bytes away. */
    split_target = st->bytes_kept / ((uint64_t)(nthreads ? nthreads : 1U) * (jobs_per_thread ? jobs_per_thread : 1U));
    if (split_target < split_min) split_target = split_min;
    if (split_target > split_max) split_target = split_max;

    /* Pass 2: coalesce the survivors into byte ranges. */
    for (fi = 0; fi < shard_count; fi++) {
        const crawl_rgix_view_t *rv = &sc->groups[fi];
        const crawl_sidecar_scope_t *sp = &scope[fi];
        uint64_t gi, run_lo = 0, run_hi = 0;

        if (!sp->in_shard) continue;

        for (gi = 0; gi <= rv->group_count; gi++) {
            int iv = 0, bm = 0;

            if (gi < rv->group_count) {
                const crawl_rgix_group_t *g = &rv->groups[gi];

                if (crawl_rgix_group_survives(g, rv->dfs_domain, sp, &iv, &bm)) {
                    if (run_hi == g->file_offset && run_hi > run_lo) {
                        run_hi += g->group_bytes;
                    } else {
                        if (run_hi > run_lo && rgix_append_chunk(&all, &all_n, &all_cap, shard_paths[fi], run_lo,
                                                                 run_hi, fi, &byte_sum) != 0)
                            goto fail;
                        run_lo = g->file_offset;
                        run_hi = g->file_offset + g->group_bytes;
                    }
                    /* Cut a long run at the group boundary that crosses the target, so one
                     * dense shard still spreads across the thread budget. */
                    if (run_hi - run_lo >= split_target) {
                        if (rgix_append_chunk(&all, &all_n, &all_cap, shard_paths[fi], run_lo, run_hi, fi,
                                              &byte_sum) != 0)
                            goto fail;
                        run_lo = run_hi = 0;
                    }
                    continue;
                }
            }
            /* A pruned group, or the end of the shard, closes the run. */
            if (run_hi > run_lo &&
                rgix_append_chunk(&all, &all_n, &all_cap, shard_paths[fi], run_lo, run_hi, fi, &byte_sum) != 0)
                goto fail;
            run_lo = run_hi = 0;
        }
    }

    if (all_n == 0U) goto fail;

    st->used = 1;
    *chunks_out = all;
    *chunk_count_out = all_n;
    *chunk_bytes_out = byte_sum;
    return 0;

fail:
    crawl_bin_free_chunk_array_rows(all, all_n);
    memset(st, 0, sizeof(*st));
    return -1;
}
