# Capability matrix: Robinhood / GUFI / XDU vs ecrawl suite

Paper reference: [docs/A Comparison of Three Open-source File Indexers Robinhood, GUFI, XDU.pdf](../../docs/A%20Comparison%20of%20Three%20Open-source%20File%20Indexers%20Robinhood,%20GUFI,%20XDU.pdf) (Bjornson, PEARC ’26). Paper versions: Robinhood 3.2.0, GUFI 0.6.10, XDU 0.4.1.

**Fairness note:** `ecrawl` is a capture tool (ERCBIN08 uid shards). Query parity uses the **ecrawl suite**: `ecrawl` writes the capture, `ereport_index` adds a path trigram index and answers Q1/Q2, and `ecrawl_analyze` reads the capture for Q3/Q4/Q5. Figure 5 names the binary that ran, not the suite, so each panel says which part did the work.

## Roles

| Tool | Role | Index storage |
|------|------|---------------|
| Robinhood | Policy engine + MariaDB index | RDB (NAMES / ENTRIES / ACCT_STAT) |
| GUFI | Permission-preserving replica index | Per-directory SQLite under a replica tree (+ optional rollup) |
| XDU | Compact analytics index | Hive-partitioned Parquet (path, size, atime) |
| Starfish | Commercial (paper baseline; omit unless licensed) | Proprietary |
| find / du | Live walk baselines (paper): serial search / serial aggregate | None |
| fd / dua | Parallel counterparts: `fd` search (`test.sh` convention), `dua` aggregate | None |
| **ecrawl** | Parallel local capture | `uid_shard_*.bin` + `.ckpt` + manifest (ERCBIN08 columnar / zstd) |
| **ereport** | Age×size reports, `--subtree` aggregates | Consumes crawl bins (HTML + stdout stats) |
| **ereport_index** | Path trigram search | `paths.bin` / `tri_keys.bin` / `tri_postings.bin` (extra post-crawl index) |
| **ecrawl_analyze** | Record selection over the capture: `--subtree`, `--size-gt`, `--type`, `--list` | Consumes crawl bins; stores nothing |

## Privileges

| | Scan / index | Query |
|--|--------------|-------|
| Robinhood | Admin / full-tree read | DB credentials |
| GUFI | Admin / full-tree read | Users see only accessible paths; admin queries often as root / ACL |
| XDU | Admin / full-tree read | Read access to Parquet index |
| ecrawl suite | Needs read of scanned tree; often root for production | Reports/search for whoever can read bins + index |

## Metadata richness

| Field | Robinhood | GUFI | XDU | ecrawl record |
|-------|-----------|------|-----|---------------|
| Path | yes | yes | yes | yes (reconstructed) |
| Size | yes | yes | yes | yes |
| atime | yes | yes | yes | yes |
| mtime / ctime | yes | yes | no | yes |
| uid / ownership | yes | yes | no (partition≈top dir) | yes — uid, gid and mode, queryable via `ecrawl_analyze --gid` / `--perm` |
| inode / nlink / type | yes | yes | no | yes |
| Allocated / sparse | policy-dependent | possible via SQL | optional disk vs apparent at index | yes (manifest + accounting) |

## Predicate / query coverage (paper Q1–Q5 + suite)

| Need | Robinhood | GUFI | XDU | ecrawl suite |
|------|-----------|------|-----|--------------|
| Q1 unique name | rich find | `gufi_find -name` | `xdu-find -p` regex | `ereport_index --search` \| `grep -E '/name$'` |
| Q2 glob `slurm-*.out` | find predicates | find-like | regex (not shell glob) | `ereport_index --search <longest literal run>` \| `grep -E '/slurm-[^/]*\.out$'` |
| Q3 size > 500MB | `rbh-find -size +N` (bare bytes) | `gufi_find -size +Nc` | `xdu-find --min-size N` | `ecrawl_analyze --size-gt N --type f --list` |
| Q4 subtree disk usage | `rbh-du -s` (unit probed) | `gufi_du -s --apparent-size --block-size 1` | sum `xdu-find -f size` under prefix | `ecrawl_analyze --subtree DIR` → `bytes=` (du -sb semantics) |
| Q5 subtree file count | SQL / find | find / SQL | `xdu-find --count` | `ecrawl_analyze --subtree DIR --type f --list` |
| User-scoped search | DB filters | designed for unprivileged users | partition (`-u`) | per-uid crawl/report or all-users |

## Ops / architecture

| | Robinhood | GUFI | XDU | ecrawl suite |
|--|-----------|------|-----|--------------|
| Separate DB server | yes (MariaDB on NVMe) | no | no | no |
| Incremental / resume | policy / changelogs (FS-dependent) | re-index / rollup | partition re-index | **no** cross-run resume; scrub output dir |
| Rollup | ACCT_STAT (user/group); weak dir rollup | optional `gufi_rollup` (cutoff) | partition pruning | heat-map + `ecrawl_analyze` shapes |
| Typical footprint / 1M files | modest GiB-class DB | large with rollup | smallest (Parquet) | bins + optional trigram index (richer → larger than XDU) |
| Workflow fit here | general RDB | secure multi-tenant find/du | tiny index + analytics | ORCD report HTML, `edelete`, path rewrite/subtree |

## Known parity gaps (do not over-claim)

1. **Q1 / Q2 are two stages, and the timing includes both.** `ereport_index --search` matches a
   literal substring anywhere in the path, which is a *superset* of what `find -name` means, so the
   harness asks the index for the longest literal run in the pattern — the smallest candidate set the
   index can produce for it — and pipes that through a basename-anchored `grep -E` to land on find's
   exact answer. Case folding in the index only widens the candidate set, and the `grep` is
   case-sensitive, so the result is identical to `find`. Two consequences worth naming: a pattern whose
   longest literal run is under three characters has no trigram to look up and the row is skipped, and
   character classes (`[abc]`) are not translated, so such a glob is skipped rather than answered
   approximately. `query_params.txt` records the term and the regex that ran.
2. **Q3 / Q4 / Q5 read the capture, not an index.** `ecrawl_analyze` selects records with
   `--size-gt` / `--type` / `--subtree`, so it always pays a full pass over the shards — there is no
   path index to prune with, which is why a small-subtree query costs about the same as a whole-tree
   one. Subtree membership is resolved per shard catalog as a directory-id lookup rather than by
   rebuilding a path per record, and `--subtree` totals count each multiply-linked inode once, so
   `bytes=` matches `du -sb` exactly (verified against `du` and `find` on trees with hard links,
   symlinks and sparse files).
3. **Index rows:** Report **crawl-only** size/time and **crawl + `ereport_index --make`** as separate rows.
4. **Bytes:** Suite unique-inode logical bytes (`test.sh` style). Align external tools / `find` the same way.
5. **GUFI rollup:** Run with and without rollup as two variants (paper).
6. **Docs:** Fixed — `docs/binary-format.md` and `docs/performance.md` now describe **ERCBIN08**.
7. **fd baseline:** `fd` has no `du` equivalent, so Q4 is skipped for it. It also needs `--hidden --no-ignore` or it silently omits dotfiles and ignore-file matches; `test.sh` uses `find` for directory counts because `fd -t d` omits the walk root.
8. **du / dua baselines:** both answer Q4 only — no name, type or size predicates, so Q1–Q3 and Q5 are skipped rather than emulated with `-a | wc -l` style hacks. `dua --stats` reports *entries traversed* (files plus directories), which is not Q5's regular-file count. `dua` exits 0 even when it could not read part of the tree, so unlike `find`/`du` a partial walk shows up only in its captured stderr, never as `partial_exit=1`.
9. **Thread budget:** `THREADS` (default 16) is the *total* worker count each tool is given, since default fan-outs vary wildly — ecrawl ships 16 crawl + 8 stat + 8 writer threads, `fd` and `dua` take every logical processor, Robinhood scans with 2. Tools that run several pools at once split the budget: ecrawl keeps its stock 2:1:1 crawl/writer/stat shape, `ereport_index --make` halves it between parse workers and trigram writers, and Robinhood splits it between `nb_threads_scan` and `EntryProcessor`. GUFI (`-n`), XDU (`-j`), `fd` and `dua` take it whole. `find` and `du` are single-threaded by design — an inherent property, not a handicap the harness imposes. `SUMMARY_TABLE.txt` prints the resolved split, and any tool that could not be pinned says so in its notes.

   Two caveats. GUFI's query wrappers are pinned only if the installed build advertises a thread flag in `--help`, since the spelling has moved between releases and passing an unknown flag would fail the row outright. And Robinhood's counts are baked into the config at `mariadb.sh setup` time, so changing `THREADS` afterwards needs a rerun of setup; `run_index.sh` compares the two and flags a mismatch.
10. **Units are not comparable by default, so the harness pins each one.** Every Q3 threshold and Q4
    total in this comparison is *apparent* bytes, matching `find -size +Nc` and `du -sb`, and reaching
    that means overriding three different defaults. `gufi_du` reports rounded blocks unless given
    `--apparent-size --block-size 1`. `xdu` indexes `st_blocks`, so without `--apparent-size` at index
    time its Q3 matched nothing and its Q4 came out at zero on a tree of sparse files. Because that is
    a different unit and not a near miss, a build lacking the flag does not get to answer at all: Q3
    and Q4 are skipped with `index_holds_st_blocks_size_answers_would_be_in_the_wrong_unit`, since a
    number in blocks reported next to `du -sb` reads as a defect in `xdu` rather than as a gap in the
    build, and `env.txt` records the flag under `xdu_index_args`. `rbh-du`'s unit flag is
    not spelled the same across builds, so the harness reads `--help` and records which form it used.
    Size *arguments* are given as bare byte counts wherever a tool accepts them, because a `K`/`M`/`G`
    suffix does not say whether it is 1024- or 1000-based and would silently ask a different question.
11. **A refused predicate is reported once, not once per repetition.** Before the timed loop, each
    external tool is asked the shape of every query against the small seeded subtree — a name match, a
    size filter, a type filter, an aggregate. Anything it rejects becomes a `skipped` row carrying the
    tool's own error message, and the real query is never run. This is why `rbh-find -size +Nc` and a
    `--min-size` a build cannot parse cost one cheap probe instead of three full-tree scans that each
    end in the same parse error. `FAILURES.txt` separates these *refused* rows from tools that
    genuinely broke, from questions a tool cannot express at all, and from tools that were not
    installed or configured — and, first of all, from tools that answered with a straight face and
    got it wrong, which no exit code reports.
12. **Open files:** every tool here wants far more than the stock 1024 — `ereport_index` keeps trigram writers × LRU shards open, GUFI opens a database per directory. `benchmark.sh` raises `RLIMIT_NOFILE` to 128k (`NOFILE_TARGET`) for the whole run, and the step-by-step scripts do the same for themselves. Non-root runs are capped by the hard limit, which the summary records.
