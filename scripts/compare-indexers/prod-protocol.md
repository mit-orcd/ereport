# Production-scale read-only protocol

Use after synthetic smoke (`prepare-synth.sh` + `check_correctness.sh` + `run_index.sh` / `run_queries.sh`) succeeds.

## Goals

- Same methodology as the PEARC paper (index time and size; Q1–Q5 × 3 reps), plus a cold and a hot pass over each of them and one extra query, Q6
- Local numbers on ORCD storage — **do not** require matching the paper’s 116M GPFS figures
- Read-only against the live tree (or a ZFS snapshot if available)

## Preconditions

1. Admin / full-tree read for indexers that require it (GUFI, XDU, Robinhood, usually `ecrawl` on shared trees).
2. Pin tool versions; record in `env.txt` (paper: Robinhood 3.2.0, GUFI 0.6.10, XDU 0.4.1).
3. NVMe (or fast local) space for GUFI replica / XDU parquet / ecrawl bins. Robinhood's tables belong on the same storage: set `RBH_DB_DATADIR` before `mariadb.sh setup`, or let `benchmark.sh --do` point it at `<work>/mariadb`. A packaged MariaDB keeps them on the OS disk, and a row measured there is not comparable with the rest of the table.
4. Agree on a **large subdirectory** for Q4/Q5 (stable path that will not be deleted mid-run), and on three of them if you want the hot passes to rotate through argument sets.
5. Prefer a **quiescent** tree or a **ZFS snapshot** so counts do not drift vs `find`.

## Procedure

### 1. Record environment

```bash
RESULTS=/data1/$USER/indexer-compare-prod-$(date +%Y%m%d)
mkdir -p "$RESULTS"
# FS type, mount options, ARC/cache state, ulimit -n, nproc, tool versions
uname -a | tee "$RESULTS/env_host.txt"
df -T /path/to/tree | tee -a "$RESULTS/env_host.txt"
# If ZFS:
#   cat /proc/spl/kstat/zfs/arcstats | tee "$RESULTS/arcstats_before.txt"
```

Set `THREADS` once and leave it alone: it is the total worker budget handed to every tool that accepts one (ecrawl's three pools, `ereport_index`'s two, GUFI `-n`, XDU `-j`, `fd`, `dua`, `dut` `-t`, and Robinhood's scan and pipeline threads via its config). Setting a specific `ECRAWL_*` or `EREPORT_INDEX_*` variable overrides its share, which the summary then reports as resolved. Robinhood's share is written by `mariadb.sh setup`, so change `THREADS` before that step, not after.

Open-file limits matter at this scale; `benchmark.sh` raises `RLIMIT_NOFILE` to 128k, and a non-root run is capped by the hard limit.

### 2. Cold and hot

Both are measured, in that order, as a per-tool series:
`CACHE_MODES="cold hot"` is the default. Each tool (and each `ecrawl` variant)
does all of its cold reps — drop, then crawl and index together, or Q1–Q6 —
and only then its hot reps, so the last cold run is what warms the hot series.
Run as root with `DROP_CACHES=1` and
`DROP_DB_CACHE=1` so the cold pass is genuinely cold (`sync; echo 3 >
/proc/sys/vm/drop_caches`, plus a MariaDB restart for the rows that read it);
without the privileges to drop anything the cold pass records itself as `warm`
rather than claiming a state the run could not produce. The hot pass drops
nothing, which is what the work costs with the metadata already in memory.

`CACHE_MODES=cold` restores a single-pass run. The `cache` column of both CSVs
carries the pass, and nothing averages across it.

### 3. Index / capture

```bash
TREE=/path/to/production/root   # or snapshot mount
export DROP_CACHES=0   # or 1
export REPS=1          # production index once; queries use REPS=3
                       # or keep REPS=3 and spare only the slow rows:
                       #   export REPS=3 REPS_GUFI=1 REPS_ROBINHOOD=1
export TOOLS="ecrawl gufi xdu find fd du dua dut"   # add robinhood when RBH_SCAN_ARGS is set
export INCLUDE_EREPORT_INDEX=1   # Q1/Q2/Q6 need it, and so do Q4/Q5
export DO_NOWRITE=0    # optional: 1 = ecrawl --no-write (apparent-size peer of du/dua/dut)
export DO_NOSTAT=0     # optional: 1 = ecrawl --no-stat --count (file-count peer of find/fd)

scripts/compare-indexers/run_index.sh "$TREE" "$RESULTS/index"
```

For GUFI rollup variant: ensure `gufi_rollup` is on `PATH` (`GUFI_DO_ROLLUP=1`, default).

Budget the time for it. On a 4.4M-entry synthetic tree the rollup took **29 minutes per repetition** (485 s per million files, growing the index from 39 GB to 580 GB) against 33 seconds for `gufi_dir2index` — 91% of the entire index phase, which is why a 3-repetition run spent 1.6 hours there. It is measured as its own row precisely so that cost stays visible instead of being folded into GUFI's build. Two consequences: hold the rollup to one pass (`REPS_GUFI=1`, or `REPS=1` for the whole index phase — the index is built once in production anyway; per-tool counts mean the cheap rows can still take their three), and expect the untimed bookkeeping around it — sizing a half-terabyte index tree with `du`, then deleting the previous repetition's copy — to add minutes of its own per repetition. `GUFI_DO_ROLLUP=0` drops the variant entirely, which is what `benchmark.sh --quick` does.

Robinhood: set `RBH_SCAN`, `RBH_SCAN_ARGS`, and optionally `RBH_INDEX_DIR` / `RBH_INDEX_BYTES` for DB footprint. Otherwise the footprint is `du` over MariaDB's data directory, which is what GUFI's and XDU's rows measure too; `information_schema`'s figure for Robinhood's own tables goes in the row's note, since the two answer different questions.

Robinhood is two timed phases, and both are what it costs. The scan fills index-free tables — not a database anyone queries — and the `indexes` row times `CREATE INDEX name_index ON NAMES (name)`, `size_index ON ENTRIES (size)` and `type_index ON ENTRIES (type)`. The query phase drops all three when it finishes, so the next run's scan is index-free again and its `indexes` row measures the same work. A standalone `run_queries.sh` creates them itself, untimed, rather than measuring an unindexed database: nobody runs one.

Robinhood ships no schema. It creates `ENTRIES`, `NAMES` and the rest on its first run with `--alter-db`, which in this harness is the index step, so a database provisioned but never scanned answers every query with "table ENTRIES does not exist" rather than with an empty result. `mariadb.sh setup` therefore ends with one `robinhood --scan --once --alter-db`, which it runs before the tree is generated and so returns immediately, and `mariadb.sh schema` repeats that on an older prefix. Queries check for the tables before running and skip with `database_has_no_schema_the_scan_step_did_not_run`; the index step deliberately does not, because its scan is what builds them (and rebuilds them after `RBH_AUTO_RESET` drops the database). `env.txt` records `rbh_schema=present` or the reason it is absent.

Both GUFI and Robinhood take their index root, thread count and credentials from a config file rather than from argv. The harness writes GUFI's per run (`IndexRoot` = the index this run built) and `mariadb.sh setup` writes Robinhood's; both paths, and Robinhood's configured `fs_path`, are recorded in `env.txt` and printed in the summary's provenance block. A Robinhood config whose `fs_path` is not the tree under test scans the wrong tree, so that row is skipped rather than reported.

### 4. Seed production queries

Production trees may not contain the synthetic `query_seeds/`. Export seeds explicitly:

```bash
export Q1_NAME='some_unique_filename.dat'   # known to exist once
export Q2_GLOB='slurm-*.out'
export Q2_TERM='slurm-'
export Q3_MIN_BYTES=$((500*1024*1024))
export Q4_SUBTREE=/abs/path/large/subdir
export Q5_SUBTREE=$Q4_SUBTREE
export Q6_GLOB='*somemidtoken*.dat'         # unanchored; see below

export ECRAWL_BIN_DIR=$(cat "$RESULTS/index/ecrawl_bin_dir.txt")
export EREPORT_INDEX_DIR=$(cat "$RESULTS/index/ereport_index_dir.txt")
export GUFI_PLAIN_INDEX_DIR=$(cat "$RESULTS/index/gufi_plain_index_dir.txt" 2>/dev/null || true)
export GUFI_ROLLUP_INDEX_DIR=$(cat "$RESULTS/index/gufi_rollup_index_dir.txt" 2>/dev/null || true)
export XDU_INDEX_DIR=$(cat "$RESULTS/index/xdu_index_dir.txt" 2>/dev/null || true)
export INDEX_RESULTS_DIR=$RESULTS/index
export REPS=3
export TOOLS="find fd du dua dut ecrawl_suite gufi xdu"

scripts/compare-indexers/run_queries.sh "$TREE" "$RESULTS/queries"
```

`EREPORT_INDEX_DIR` is no longer only Q1/Q2/Q6's trigram index. `ecrawl_query`
is handed it for Q4 and Q5, which read the dir-index sidecars that
`ereport_index --make` writes there: the subtree root is looked up rather than
found by materialising every directory row, and Q5's scan is narrowed to the row
groups that can hold a descendant. That build's elapsed time and its size,
sidecars included, are already measured as their own row, so the speed-up is not
free — report the two together. Without the index phase both queries still
answer, from the catalog, at the cost they cost before.

The sidecars are bound to the exact capture they were built from (per shard:
name, size, mtime, catalog offset, highest directory id), so the two pointers
above must come from the same run. An index paired with a different crawl is
rejected rather than believed, and both queries fall back to a full catalog
pass. `run_index.sh` builds from `ecrawl`'s last repetition for that reason, and
the `ereport_index`/`make` row names its input capture; the Q4 row's
`answered_from=` note says which path ran (`dir_index`, `catalog_rollup` or
`record_scan`).

An exported query variable pins that value for every argument set, so a
production run seeded this way asks one question per query and its hot passes
repeat it. To get the rotation, export the indexed forms instead — `Q1_NAME_1`,
`Q1_NAME_2`, `Q1_NAME_3` and their `Q2_GLOB_n`, `Q3_MIN_BYTES_n`,
`Q4_SUBTREE_n`, `Q5_SUBTREE_n`, `Q6_GLOB_n` counterparts — or lower `ARG_SETS` to
1 and accept that hot rows re-answer the cold one's question.

Q6 is this comparison's own addition, marked *extra* everywhere it appears. Pick a
token with nothing anchored at either end and no reason to appear elsewhere in the
tree: `*token*.dat` is what a B-tree on names cannot seek into, so Robinhood scans
every row despite `name_index`, while a trigram index takes the token as its most
selective term and does not care where in the basename it sits.

GUFI queries run twice, once against each index the build produced. Export both
pointers; with only `GUFI_ROLLUP_INDEX_DIR` set you get the rolled-up series and
nothing to compare it against, and `GUFI_INDEX_DIR` alone is read as the plain
one. Q4 has no plain-index row at all — `gufi_du -s` reads treesummary rows only
`gufi_rollup` writes, and on a plain index it warns, prints 0 and exits 0, so
recording that would credit the cheap index with the expensive one's capability.

### 5. Summarize

```bash
python3 scripts/compare-indexers/summarize.py "$RESULTS/index" "$RESULTS/queries" \
  --out "$RESULTS/SUMMARY_TABLE.txt"
```

### 6. Correctness spot-check (optional, expensive)

On a **subtree** only (not the whole production tree):

```bash
scripts/compare-indexers/check_correctness.sh "$Q4_SUBTREE" "$RESULTS/correctness-subtree"
```

Expect strict equality only if the subtree is quiescent.

## What to record in the final report

| Item | Notes |
|------|--------|
| FS type / vendor | ZFS, GPFS, VAST, XFS, … |
| Cold and hot | both, per phase, never averaged together |
| Thread / job counts | per tool |
| Index rows | `ecrawl` write + `ereport_index` make (report the total and the split); GUFI plain; GUFI `rollup_index` + `rollup_step` (likewise); XDU; Robinhood `scan` + `indexes` (likewise, and say where its datadir was); walk references in two peer groups — file count (`find`/`fd`/`ecrawl` nostat → `answer_files=`) and apparent size (`du`/`dua`/`dut`/`ecrawl` nowrite → `answer_bytes=`; hard links credited once) |
| Query rows | Q1–Q6 mean ± stddev per cache state, with Q6 marked extra; skip notes for unsupported predicates. GUFI appears twice, plain and rolled-up, and its Q4 only under the latter. The suite's rows are named for the binary that ran: `ereport_index` for Q1/Q2/Q6 (index search piped through an exact filter, both stages timed), `ecrawl_query` for Q3/Q4/Q5 (a pass over the capture, which for Q4 and Q5 the dir-index sidecars from the `ereport_index` make row cut down or replace) |
| Parity gaps | Patterns whose longest literal run is under three characters, or that use character classes, have no index path and are skipped; Q3 has no subtree to resolve and still costs a full capture scan, and so do Q4 and Q5 when the sidecars are absent or bound to another capture; byte definition |
| Units | Every size is apparent bytes: `gufi_du --apparent-size --block-size 1`, `xdu` indexed with `--apparent-size` (Q3 and Q4 are skipped when the build has no such flag, since the index then holds `st_blocks`), `rbh-du`'s unit form probed from `--help` and recorded per row. Thresholds are passed as bare byte counts, never `K`/`M`/`G` |
| Row status | `SUMMARY_TABLE.txt` ends with a tally, and `FAILURES.txt` splits rows five ways: wrong answer (finished ok but disagrees with `find`/`du`), failed, predicate refused by this build (from the probe pass), cannot express the query, and not available in this run. Only the first two are defects in the run, and the first is the one no exit code reports |
| Ops cost | MariaDB vs local files; privilege model |

## Safety

- Do **not** point destructive tools (`xdu-rm`, `edelete --delete`) at production during this benchmark.
- Prefer snapshot mounts for multi-hour indexes.
- Watch free space: GUFI replicas and ecrawl bins can be large.
