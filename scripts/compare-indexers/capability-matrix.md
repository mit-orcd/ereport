# Capability matrix: Robinhood / GUFI / XDU vs ecrawl suite

Paper reference: [docs/A Comparison of Three Open-source File Indexers Robinhood, GUFI, XDU.pdf](../../docs/A%20Comparison%20of%20Three%20Open-source%20File%20Indexers%20Robinhood,%20GUFI,%20XDU.pdf) (Bjornson, PEARC ’26). Paper versions: Robinhood 3.2.0, GUFI 0.6.10, XDU 0.4.1.

**Fairness note:** `ecrawl` is a capture tool (ERCBIN08 uid shards). Query parity uses the **ecrawl suite**: `ecrawl` writes the capture, `ereport_index` adds a path trigram index and answers Q1/Q2/Q6, and `ecrawl_query` reads the capture for Q3/Q4/Q5. The query figure names the binary that ran, not the suite, so each panel says which part did the work.

Three more fairness decisions shape every number here. Each phase is measured **cold and hot**, in that order, so no tool is credited with a cache another tool warmed and no single number stands in for two situations. Robinhood's tables sit on the **same filesystem** as every other tool's index (`RBH_DB_DATADIR`, default `<work>/mariadb`) instead of the OS disk a packaged MariaDB would use, and it is always **queried with indexes**, whose creation is timed as a phase of its own. GUFI is **two series**, plain and rolled-up, because the two cost very different amounts to build and do not answer the same questions.

## Roles

| Tool | Role | Index storage |
|------|------|---------------|
| Robinhood | Policy engine + MariaDB index | RDB (NAMES / ENTRIES / ACCT_STAT) |
| GUFI | Permission-preserving replica index | Per-directory SQLite under a replica tree (+ optional rollup) |
| XDU | Compact analytics index | Hive-partitioned Parquet (path, size, atime) |
| Starfish | Commercial (paper baseline; omit unless licensed) | Proprietary |
| find / du | Live walk baselines (paper): serial search / serial aggregate | None |
| fd / dua / dut | Parallel counterparts: `fd` search (`test.sh` convention), `dua` and `dut` aggregate | None |
| **ecrawl** | Parallel local capture | `uid_shard_*.bin` + `.ckpt` + manifest (ERCBIN08 columnar / zstd) |
| **ereport** | Age×size reports, `--subtree` aggregates | Consumes crawl bins (HTML + stdout stats) |
| **ereport_index** | Path trigram search | `paths.bin` / `tri_keys.bin` / `tri_postings.bin` (extra post-crawl index) |
| **ecrawl_query** | Record selection over the capture: `--subtree`, `--size-gt`, `--type`, `--list` | Consumes crawl bins; stores nothing |
| **ecrawl (live walk)** | Index-free name search (`ecrawl --no-stat --contains` + exact `grep -E`) and subtree file count (`ecrawl --no-stat --count`) | None (no capture, no index) |

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
| uid / ownership | yes | yes | no (partition≈top dir) | yes — uid, gid and mode, queryable via `ecrawl_query --gid` / `--perm` |
| inode / nlink / type | yes | yes | no | yes |
| Allocated / sparse | policy-dependent | possible via SQL | optional disk vs apparent at index | yes (manifest + accounting) |

## Predicate / query coverage (paper Q1–Q5, plus Q6 and the suite)

| Need | Robinhood | GUFI | XDU | ecrawl suite |
|------|-----------|------|-----|--------------|
| Q1 unique name | rich find (`name_index`) | `gufi_find -name` | `xdu-find -p` regex | `ereport_index --search` \| `grep -E '/name$'` |
| Q2 glob `slurm-*.out` | find predicates (prefix seek) | find-like | regex (not shell glob) | `ereport_index --search <longest literal run>` \| `grep -E '/slurm-[^/]*\.out$'` |
| Q3 size > 500MB | `rbh-find -size +N` (bare bytes, `size_index`) | `gufi_find -size +Nc` | `xdu-find --min-size N` | `ecrawl_query --size-gt N --type f --list` |
| Q4 subtree disk usage | `rbh-du -s` (unit probed) | `gufi_du -s --apparent-size --block-size 1`, **rolled-up index only** | sum `xdu-find -f size` under prefix | `ecrawl_query --subtree DIR` → `bytes=` (du -sb semantics) |
| Q5 subtree file count | SQL / find | find / SQL | `xdu-find --count` | `ecrawl_query --subtree DIR --type f --list` |
| Q6 substring `*token*.dat` **[extra]** | find predicates, but no index can serve it | find-like | regex | `ereport_index --search token` \| `grep -E '/[^/]*token[^/]*\.dat$'` |
| User-scoped search | DB filters | designed for unprivileged users | partition (`-u`) | per-uid crawl/report or all-users |

Each query has **three argument sets**. The measured series is set 1: all cold reps, then all hot reps, so the cache delta is identical work. Sets 2 and 3 run once hot afterwards, so those bars are different questions. Correctness is checked per (query, argument set), each against its own reference.

## Ops / architecture

| | Robinhood | GUFI | XDU | ecrawl suite |
|--|-----------|------|-----|--------------|
| Separate DB server | yes (MariaDB, datadir moved onto the benchmark filesystem) | no | no | no |
| Index build phases | scan (index-free tables) + `CREATE INDEX` × 3 | `gufi_dir2index` + optional `gufi_rollup` | one pass | `ecrawl` capture + optional `ereport_index --make` |
| Incremental / resume | policy / changelogs (FS-dependent) | re-index / rollup | partition re-index | **no** cross-run resume; scrub output dir |
| Rollup | ACCT_STAT (user/group); weak dir rollup | optional `gufi_rollup` (cutoff) | partition pruning | heat-map + `ecrawl_query` shapes |
| Typical footprint / 1M files | modest GiB-class DB | large with rollup | smallest (Parquet) | bins + optional trigram index (richer → larger than XDU) |
| Workflow fit here | general RDB | secure multi-tenant find/du | tiny index + analytics | ORCD report HTML, `edelete`, path rewrite/subtree |

## Known parity gaps (do not over-claim)

1. **Q1 / Q2 / Q6 are two stages, and the timing includes both.** `ereport_index --search` matches a
   literal substring anywhere in the path, which is a *superset* of what `find -name` means, so the
   harness asks the index for the longest literal run in the pattern — the smallest candidate set the
   index can produce for it — and pipes that through a basename-anchored `grep -E` to land on find's
   exact answer. Case folding in the index only widens the candidate set, and the `grep` is
   case-sensitive, so the result is identical to `find`. Two consequences worth naming: a pattern whose
   longest literal run is under three characters has no trigram to look up and the row is skipped, and
   character classes (`[abc]`) are not translated, so such a glob is skipped rather than answered
   approximately. `query_params.txt` records the term and the regex that ran. The **ecrawl (live walk)**
   row answers Q1/Q2/Q6 with the *same* term and `grep -E`, but no index at all: `ecrawl --no-stat
   --contains <term>` streams the paths holding the needle straight off the walk, so its time is the
   multithreaded traversal alone. It is fd's live-search peer — the index answers faster, but has to be
   built first; the live walk answers on a cold tree with nothing stored. For Q5 the same walk tallies
   `d_type` under the seeded subtree (`ecrawl --no-stat --count`), the live counterpart to
   `ecrawl_query`'s dir-index answer and to find/fd's traversal.
2. **Q3 reads the capture, not an index; Q4 and Q5 read the capture plus the dir-index sidecars.**
   `ecrawl_query` selects records with `--size-gt` / `--type` / `--subtree`. Q3 has no path to
   prune by and always pays a full pass over the shards. Q4 and Q5 are given `--index-dir`, so a
   subtree root is looked up and Q5's scan is cut to the row groups that can hold a descendant —
   which means both depend on the `ereport_index --make` row, and fall back to the full pass when
   that phase did not run or its sidecars belong to another capture. Subtree membership is
   otherwise resolved per shard catalog as a directory-id lookup rather than by
   rebuilding a path per record, and `--subtree` totals count each multiply-linked inode once, so
   `bytes=` matches `du -sb` exactly (verified against `du` and `find` on trees with hard links,
   symlinks and sparse files).
3. **Index rows:** Report **crawl-only** size/time and **crawl + `ereport_index --make`** as separate rows.
4. **Bytes:** Suite unique-inode logical bytes (`test.sh` style). Align external tools / `find` the same way.
5. **GUFI rollup is two indexes, so it is two of everything.** The build runs `gufi_dir2index` twice
   into two directories and reports `plain`, `rollup_index` and the `rollup_step` that turns the second
   into the first's rolled-up twin — a step that on a 4.4M-entry tree took an order of magnitude longer
   than the build it post-processes and grew the index from tens to hundreds of gigabytes. The queries
   then run against **both**, as `GUFI` and `GUFI + rollup`, because charting one let the cheap index
   take credit for the expensive one's query times. It also, worse, took credit for a capability it does
   not have: `gufi_du -s` answers Q4 from treesummary rows that only `gufi_rollup` writes, and on a
   plain index it warns, prints 0 and still exits 0. So the plain series has no Q4 row — skipped with
   `rollup_required`, which the charts label *needs the rolled-up index* to keep it distinct from a
   query GUFI simply cannot express.
6. **Docs:** Fixed — `docs/binary-format.md` and `docs/performance.md` now describe **ERCBIN08**.
7. **fd baseline:** `fd` has no `du` equivalent, so Q4 is skipped for it. It also needs `--hidden --no-ignore` or it silently omits dotfiles and ignore-file matches; `test.sh` uses `find` for directory counts because `fd -t d` omits the walk root.
8. **du / dua / dut baselines:** all three answer Q4 only — no name, type or size predicates, so Q1–Q3 and Q5 are skipped rather than emulated with `-a | wc -l` style hacks. `dua --stats` reports *entries traversed* (files plus directories), which is not Q5's regular-file count, and `dut -f` counts files and directories together with no type predicate, so it is skipped for the same reason. `dut -b -s` gets the unit right by itself — apparent bytes with hard links deduplicated, matching `du -sb` to the byte on the seeded fixture — so its Q4 needs no annotation, but every invocation carries `-d 0 -n 1`: it takes its row count from the terminal height and with stdout on a pipe would otherwise print every entry in the tree. Only its walk row adds `-x`, where it stands in for `find -xdev`; its Q4 leaves it off so it covers the same tree as the `du -sb` it is compared against, and `env.txt` records the two vectors separately. `dua` exits 0 even when it could not read part of the tree, so unlike `find`/`du` a partial walk shows up only in its captured stderr, never as `partial_exit=1`.
9. **Thread budget:** `THREADS` (default 16) is the *total* worker count each tool is given, since default fan-outs vary wildly — ecrawl ships 16 crawl + 8 stat + 8 writer threads, `fd` and `dua` take every logical processor, Robinhood scans with 2. Tools that run several pools at once split the budget: ecrawl keeps its stock 2:1:1 crawl/writer/stat shape, `ereport_index --make` halves it between parse workers and trigram writers, and Robinhood splits it between `nb_threads_scan` and `EntryProcessor`. GUFI (`-n`), XDU (`-j`), `fd`, `dua` and `dut` (`-t`, whose default is 4 threads or one per logical processor, whichever is larger) take it whole. `find` and `du` are single-threaded by design — an inherent property, not a handicap the harness imposes. `SUMMARY_TABLE.txt` prints the resolved split, and any tool that could not be pinned says so in its notes.

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

    Three differences survive that pinning, and they are annotated rather than skipped, because each
    one is the tool answering the question its index can answer. **Unit:** Robinhood 3.2.0's `rbh-du`
    documents no byte flag at all and reports allocated blocks, so on the seeded subtree — sparse by
    construction, `total_allocated_bytes=0` — it answers 80 KiB where `du -sb` answers 3.7 GiB. That
    is the paper's Q4, *"disk usage of large subdirectory"*, answered exactly; this comparison asks a
    different question so that every tool answers one. **Scope:** `gufi_du` and `xdu-find` total file
    records, while `du -sb` also counts directory inodes, which is a 49,371-byte gap on the main
    subtree and the entire answer on one whose files are empty. **Identity:** Robinhood's `ENTRIES` is
    keyed by inode, so its counts are per inode where `find`'s are per name — eight hard-linked names
    over the Q3 threshold, two Robinhood rows. Its size column is `st_size` regardless: it matched
    every seeded sparse fixture at all three thresholds, which an index of `st_blocks` could not.
    `SUMMARY_TABLE.txt`, `FAILURES.txt` and Figure 6 each name the reason beside the row.
11. **A refused predicate is reported once, not once per repetition.** Before the timed loop, each
    external tool is asked the shape of every query against the small seeded subtree — a name match, a
    size filter, a type filter, an aggregate. Anything it rejects becomes a `skipped` row carrying the
    tool's own error message, and the real query is never run. This is why `rbh-find -size +Nc` and a
    `--min-size` a build cannot parse cost one cheap probe instead of three full-tree scans that each
    end in the same parse error. `FAILURES.txt` separates these *refused* rows from tools that
    genuinely broke, from questions a tool cannot express at all, and from tools that were not
    installed or configured — and, first of all, from tools that answered with a straight face and
    got it wrong, which no exit code reports.
12. **Robinhood is never measured unindexed, and the indexes are not free.** A relational index is a
    thing you build, and nobody queries a database without one, so the harness splits the difference
    rather than choosing: the scan is timed filling index-free tables, then `name_index` on
    `NAMES(name)` and `size_index` / `type_index` on `ENTRIES` are timed as a phase of their own, and
    only then do the queries run. The query phase drops all three when it finishes, so the next run's
    scan is index-free again and its index phase measures the same work — a run that left them in place
    would report a cheap scan and no index cost at all. Both rows are Robinhood's cost and the summary
    adds them, the same way it adds `ecrawl` + `ereport_index` and GUFI + rollup. Note what the indexes
    do *not* buy: Q6's unanchored `%token%` cannot seek into a B-tree on names, so `name_index` is there
    and the optimiser still reads every row.
13. **Q6 is not from the paper and says so.** It is labelled *extra* in the charts and the summary. It
    exists because the paper's five queries never ask for a token with nothing anchored at either end,
    which is the one shape that separates a trigram index from a B-tree: `*token*.dat` forces a full
    scan out of Robinhood despite `name_index`, while `ereport_index` takes the token as its most
    selective term and does not care where in the basename it sits. Q2 and Q6 run the same predicate
    shape with one anchor's difference, so the pair is the measurement, not either row alone. The seeds
    plant a directory carrying the token and a file with the wrong extension, so a tool that indexes
    directories or matches the whole path over-reports and the correctness check catches it.
14. **Cold and hot are separate measurements, not a range.** Each tool finishes all of its cold reps
    (one drop, then the whole pipeline) before any hot rep, so the last cold run is what warms the
    hot series. The `cache` column keeps them apart in the CSVs, the tables and the figures. A cold
    pass on a host where the harness could not drop anything records itself as `warm`, because a claim
    the run cannot support should not be in the data. Crawl and index are one unit, so a hot
    `ereport_index` reads the shards the hot `ecrawl` in that same unit just wrote.
15. **Open files:** every tool here wants far more than the stock 1024 — `ereport_index` keeps trigram writers × LRU shards open, GUFI opens a database per directory. `benchmark.sh` raises `RLIMIT_NOFILE` to 128k (`NOFILE_TARGET`) for the whole run, and the step-by-step scripts do the same for themselves. Non-root runs are capped by the hard limit, which the summary records.
