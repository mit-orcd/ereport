/*
 * --path-rewrite OLD=NEW rule sets, one per crawl bin directory, shared by ereport and ereport_index.
 *
 * A rule relabels every path at or under OLD (directory boundary) as living under NEW; the on-disk bins
 * are never modified. Rules are grouped per crawl directory: one given before the positionals applies
 * to every directory, one given right after a directory only to that one (repeatable), and a
 * `path_rewrites.txt` (OLD=NEW per line, '#' comments) inside a crawl directory joins that directory's
 * set. This is what lets a report merging several storage servers show each one's exports under its own
 * cluster-visible path (/data2/group/dandi/002 on hstor004 as /orcd/data/dandi/002) instead of merging
 * identical local paths by name.
 *
 * Rules are applied once per shard, at catalog attach, by grafting the catalog
 * (crawl_bin_catalog_graft): the directory OLD is re-parented under synthetic ancestors spelling NEW, so
 * every consumer of reconstructed paths -- record paths, --subtree, the sunburst merge -- sees the
 * rewritten namespace at no per-path cost. --subtree is therefore given in NEW terms.
 * path_rewrite_set_apply is the one string-level helper, for manifest-derived labels.
 *
 * Header-only (static inline) so both tools share the parsing and the messages without a new object.
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef PATH_REWRITE_H
#define PATH_REWRITE_H

#include <limits.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "crawl_bin_catalog.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct {
    char *from;
    size_t from_len;
    char *to;
    size_t to_len;
    _Atomic uint64_t hits; /* shards whose catalog held OLD */
} path_rewrite_rule_t;

typedef struct {
    path_rewrite_rule_t *rules;
    size_t n, cap;
    char *bin_dir; /* the crawl directory this set belongs to (for messages); NULL for the global set */
} path_rewrite_set_t;

/* path is at or under prefix on a directory boundary (prefix has no trailing '/'). */
static inline int path_rewrite_dir_prefix(const char *path, const char *prefix) {
    size_t plen;

    if (!prefix || prefix[0] == '\0') return 1;
    if (strcmp(prefix, "/") == 0) return path[0] == '/';
    plen = strlen(prefix);
    if (strncmp(path, prefix, plen) != 0) return 0;
    return path[plen] == '\0' || path[plen] == '/';
}

static inline void path_rewrite_set_free(path_rewrite_set_t *s) {
    size_t i;
    if (!s) return;
    for (i = 0; i < s->n; i++) {
        free(s->rules[i].from);
        free(s->rules[i].to);
    }
    free(s->rules);
    free(s->bin_dir);
    memset(s, 0, sizeof(*s));
}

/* Append a normalized rule. Returns 0, -1 on allocation failure. */
static inline int path_rewrite_set_push(path_rewrite_set_t *s, const char *from, size_t fl, const char *to,
                                        size_t tl) {
    path_rewrite_rule_t *r;

    if (s->n == s->cap) {
        size_t nc = s->cap ? s->cap * 2 : 4;
        path_rewrite_rule_t *t = (path_rewrite_rule_t *)realloc(s->rules, nc * sizeof(*t));
        if (!t) return -1;
        s->rules = t;
        s->cap = nc;
    }
    r = &s->rules[s->n];
    memset(r, 0, sizeof(*r));
    r->from = (char *)malloc(fl + 1);
    r->to = (char *)malloc(tl + 1);
    if (!r->from || !r->to) {
        free(r->from);
        free(r->to);
        return -1;
    }
    memcpy(r->from, from, fl);
    r->from[fl] = '\0';
    memcpy(r->to, to, tl);
    r->to[tl] = '\0';
    r->from_len = fl;
    r->to_len = tl;
    atomic_init(&r->hits, 0);
    s->n++;
    return 0;
}

/*
 * Validate + normalize one "OLD=NEW" argument into set. prog names the tool and `where` (may be "")
 * prefixes the reason in the message. Returns 0, or -1 after printing why (or on allocation failure).
 */
static inline int path_rewrite_set_add_arg(path_rewrite_set_t *s, const char *arg, const char *prog,
                                           const char *where) {
    const char *eq;
    size_t fl, tl;

    eq = arg ? strchr(arg, '=') : NULL;
    if (!eq || eq == arg || eq[1] == '\0') {
        fprintf(stderr, "%s: %s--path-rewrite must be OLD=NEW (got '%s')\n", prog, where, arg ? arg : "");
        return -1;
    }
    fl = (size_t)(eq - arg);
    tl = strlen(eq + 1);
    if (arg[0] != '/' || eq[1] != '/') {
        fprintf(stderr, "%s: %s--path-rewrite OLD and NEW must both be absolute\n", prog, where);
        return -1;
    }
    if (fl >= PATH_MAX || tl >= PATH_MAX) {
        fprintf(stderr, "%s: %s--path-rewrite path too long\n", prog, where);
        return -1;
    }
    /* Normalize: strip trailing '/'. Both sides must name a directory below root (not a bare "/"). */
    while (fl > 1 && arg[fl - 1] == '/') fl--;
    while (tl > 1 && eq[tl] == '/') tl--;
    if (fl < 2 || tl < 2) {
        fprintf(stderr, "%s: %s--path-rewrite OLD and NEW must name a directory below root (not '/')\n", prog,
                where);
        return -1;
    }
    if (path_rewrite_set_push(s, arg, fl, eq + 1, tl) != 0) {
        fprintf(stderr, "%s: allocation failed\n", prog);
        return -1;
    }
    return 0;
}

/*
 * DIR/path_rewrites.txt: one OLD=NEW per line, '#' comments and blank lines skipped. A missing file adds
 * nothing (returns 0); a malformed line is an error (-1, message printed).
 */
static inline int path_rewrite_set_load_file(path_rewrite_set_t *s, const char *dir, const char *prog) {
    char path[PATH_MAX];
    char line[PATH_MAX * 2];
    char where[PATH_MAX + 64];
    FILE *fp;
    unsigned lineno = 0;

    if (snprintf(path, sizeof(path), "%s/path_rewrites.txt", dir) >= (int)sizeof(path)) return 0;
    fp = fopen(path, "r");
    if (!fp) return 0;
    while (fgets(line, sizeof(line), fp)) {
        char *p = line, *e;

        lineno++;
        e = p + strlen(p);
        while (e > p && (e[-1] == '\n' || e[-1] == '\r' || e[-1] == ' ' || e[-1] == '\t')) *--e = '\0';
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '\0' || *p == '#') continue;
        snprintf(where, sizeof(where), "%s:%u: ", path, lineno);
        if (path_rewrite_set_add_arg(s, p, prog, where) != 0) {
            fclose(fp);
            return -1;
        }
    }
    fclose(fp);
    return 0;
}

/* Two OLDs of one set must not nest (nor repeat): the graft moves a directory once, and a rule inside a
 * moved subtree would then name a path that no longer exists. Returns 0 or -1 (message printed). */
static inline int path_rewrite_set_validate(const path_rewrite_set_t *s, const char *prog, const char *bin_dir) {
    size_t i, j;

    for (i = 0; i < s->n; i++)
        for (j = 0; j < s->n; j++) {
            if (i == j) continue;
            if (path_rewrite_dir_prefix(s->rules[j].from, s->rules[i].from)) {
                fprintf(stderr, "%s: --path-rewrite rules for %s overlap: %s is at or under %s\n", prog, bin_dir,
                        s->rules[j].from, s->rules[i].from);
                return -1;
            }
        }
    return 0;
}

/* Apply the set to a path string in place (the rule whose OLD covers it, if any). Returns 0; -1 when the
 * result would not fit (path unchanged). For labels only: records go through the grafted catalogs. */
static inline int path_rewrite_set_apply(const path_rewrite_set_t *s, char *path, size_t bufsz) {
    size_t i;

    if (!s || !path) return 0;
    for (i = 0; i < s->n; i++) {
        const path_rewrite_rule_t *r = &s->rules[i];
        size_t plen, suffix_len, newlen;

        if (!path_rewrite_dir_prefix(path, r->from)) continue;
        plen = strlen(path);
        suffix_len = plen - r->from_len; /* "" (exact match) or "/..." */
        newlen = r->to_len + suffix_len;
        if (newlen + 1 > bufsz) return -1;
        memmove(path + r->to_len, path + r->from_len, suffix_len + 1); /* include NUL */
        memcpy(path, r->to, r->to_len);
        return 0;
    }
    return 0;
}

/*
 * Graft every rule of the set into a freshly loaded shard catalog, counting the rules the shard held.
 * A rule whose OLD this shard never saw (a uid shard with no files under that export) is simply not
 * there. Returns 0, -1 on allocation failure.
 */
static inline int path_rewrite_graft_catalog(const path_rewrite_set_t *s, crawl_bin_catalog_t *cat) {
    size_t k;

    for (k = 0; s && k < s->n; k++) {
        int grc = crawl_bin_catalog_graft(cat, s->rules[k].from, s->rules[k].to);

        if (grc < 0) return -1;
        if (grc == 0) atomic_fetch_add_explicit(&s->rules[k].hits, 1ULL, memory_order_relaxed);
    }
    return 0;
}

/*
 * Crawl directories from argv[ai..]. A `--path-rewrite OLD=NEW` that follows a directory binds to that
 * directory (several may follow); `skip_arg` (may be NULL) says which other tokens to ignore (a trailing
 * --verbose, say); any other option here is an error since flags go first. No directory means "." with
 * no rules. *sets_out gets one set per directory (own CLI rules only; the global rules and
 * path_rewrites.txt join in path_rewrite_finish). Returns 0, or -1 after a message.
 */
static inline int path_rewrite_collect(int argc, char **argv, int ai, const char *prog, int (*skip_arg)(const char *),
                                       const char ***dirs_out, size_t *count_out, path_rewrite_set_t **sets_out) {
    const char **dirs;
    path_rewrite_set_t *sets;
    size_t n = 0, cap = (size_t)(argc > ai ? argc - ai : 1);

    dirs = (const char **)calloc(cap, sizeof(char *));
    sets = (path_rewrite_set_t *)calloc(cap, sizeof(*sets));
    if (!dirs || !sets) {
        fprintf(stderr, "%s: allocation failed\n", prog);
        free((void *)dirs);
        free(sets);
        return -1;
    }
    while (ai < argc) {
        if (strcmp(argv[ai], "--path-rewrite") == 0) {
            char where[PATH_MAX + 32];

            if (n == 0) {
                fprintf(stderr,
                        "%s: --path-rewrite here must follow a crawl directory (or go before the positionals to "
                        "apply to every directory)\n",
                        prog);
                goto fail;
            }
            if (ai + 1 >= argc) {
                fprintf(stderr, "%s: --path-rewrite requires OLD=NEW (two absolute directory paths)\n", prog);
                goto fail;
            }
            snprintf(where, sizeof(where), "%s: ", dirs[n - 1]);
            if (path_rewrite_set_add_arg(&sets[n - 1], argv[ai + 1], prog, where) != 0) goto fail;
            ai += 2;
            continue;
        }
        if (skip_arg && skip_arg(argv[ai])) {
            ai++;
            continue;
        }
        if (argv[ai][0] == '-') {
            fprintf(stderr, "%s: unknown option %s (flags go first; see --help)\n", prog, argv[ai]);
            goto fail;
        }
        dirs[n++] = argv[ai];
        ai++;
    }
    if (n == 0) {
        dirs[0] = ".";
        n = 1;
    }
    *dirs_out = dirs;
    *count_out = n;
    *sets_out = sets;
    return 0;

fail:
    {
        size_t i;
        for (i = 0; i < cap; i++) path_rewrite_set_free(&sets[i]);
    }
    free((void *)dirs);
    free(sets);
    return -1;
}

/*
 * Final per-directory rule sets for the resolved directories: global rules, then the directory's own
 * CLI rules, then its path_rewrites.txt (unless no_file). Validates each set. Consumes cli_sets
 * (may be NULL). *out gets n sets; *any_out says whether any rule exists at all. Returns 0 or -1.
 */
static inline int path_rewrite_finish(const path_rewrite_set_t *global, const char **bin_dirs, size_t n,
                                      path_rewrite_set_t *cli_sets, int no_file, const char *prog,
                                      path_rewrite_set_t **out, int *any_out) {
    path_rewrite_set_t *sets;
    size_t i, k;
    int any = 0;

    sets = (path_rewrite_set_t *)calloc(n ? n : 1, sizeof(*sets));
    if (!sets) {
        fprintf(stderr, "%s: allocation failed\n", prog);
        return -1;
    }
    for (i = 0; i < n; i++) {
        path_rewrite_set_t *s = &sets[i];

        s->bin_dir = strdup(bin_dirs[i]);
        if (!s->bin_dir) goto oom;
        for (k = 0; global && k < global->n; k++) {
            const path_rewrite_rule_t *r = &global->rules[k];
            if (path_rewrite_set_push(s, r->from, r->from_len, r->to, r->to_len) != 0) goto oom;
        }
        for (k = 0; cli_sets && k < cli_sets[i].n; k++) {
            const path_rewrite_rule_t *r = &cli_sets[i].rules[k];
            if (path_rewrite_set_push(s, r->from, r->from_len, r->to, r->to_len) != 0) goto oom;
        }
        if (cli_sets) path_rewrite_set_free(&cli_sets[i]);
        if (!no_file && path_rewrite_set_load_file(s, bin_dirs[i], prog) != 0) goto fail;
        if (path_rewrite_set_validate(s, prog, bin_dirs[i]) != 0) goto fail;
        if (s->n) any = 1;
    }
    free(cli_sets);
    *out = sets;
    if (any_out) *any_out = any;
    return 0;

oom:
    fprintf(stderr, "%s: allocation failed\n", prog);
fail:
    for (i = 0; i < n; i++) path_rewrite_set_free(&sets[i]);
    free(sets);
    return -1;
}

/* After every shard is attached: a rule that matched nowhere in its crawl directory is most likely a
 * typo or a rule meant for another server. The report is still right; it just does nothing. */
static inline void path_rewrite_warn_unmatched(const path_rewrite_set_t *sets, size_t n) {
    size_t k, r;

    for (k = 0; sets && k < n; k++)
        for (r = 0; r < sets[k].n; r++) {
            const path_rewrite_rule_t *rule = &sets[k].rules[r];
            if (atomic_load_explicit(&rule->hits, memory_order_relaxed) == 0ULL)
                fprintf(stderr, "warn: --path-rewrite %s=%s matched no directory in %s\n", rule->from, rule->to,
                        sets[k].bin_dir ? sets[k].bin_dir : "its crawl directory");
        }
}

#endif /* PATH_REWRITE_H */
