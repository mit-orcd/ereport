# Compare Robinhood / GUFI / XDU against the ecrawl suite

Benchmark harness following Bjornson's PEARC '26 methodology ([paper](../../docs/A%20Comparison%20of%20Three%20Open-source%20File%20Indexers%20Robinhood,%20GUFI,%20XDU.pdf)): build time and index size for each indexer, then queries Q1–Q6 cold and hot, all on one synthetic tree with one thread budget. The suite is `ecrawl` (capture) + `ereport_index` (trigram index + dir-index sidecars) + `ecrawl_query` (record queries). `find`, `fd`, `du`, `dua` and `dut` run as the walk baselines.

| Path | Purpose |
|------|---------|
| [benchmark.sh](benchmark.sh) | one-command install → tree → benchmark → summary → charts, and full teardown |
| [init.sh](init.sh) | clone and build the external tools at pinned tags into `PREFIX` |
| [mariadb.sh](mariadb.sh) | provision / reset / clean up the disposable Robinhood database |
| [prepare-synth.sh](prepare-synth.sh) | synthetic tree plus Q1–Q6 seeds (three argument sets each) |
| [check_correctness.sh](check_correctness.sh) | `find` / `du` vs every tool on the seeded subtree |
| [run_index.sh](run_index.sh), [run_queries.sh](run_queries.sh) | the timed phases |
| [run_smoke.sh](run_smoke.sh) | the pipeline above without the driver: synth → correctness → index → queries → summary |
| [summarize.py](summarize.py), [plot_results.py](plot_results.py) | `SUMMARY_TABLE.txt`, `FAILURES.txt`, Figures 1–6 |
| [capability-matrix.md](capability-matrix.md) | what each tool can answer, and the fairness rules (units, thread budget, GUFI's two indexes) |
| [prod-protocol.md](prod-protocol.md) | read-only protocol for a production filesystem |
| [lib.sh](lib.sh) | shared helpers and every env override |

Missing external tools are skipped (`status=skipped`), never fatal.

## Quick start

```bash
make -C ../.. ecrawl ereport ereport_index ecrawl_query edelete

# 1. Under a minute: does the suite still answer Q1–Q6 correctly? (find/du only; no external tools)
scripts/compare-indexers/benchmark.sh --do /tmp/small --small

# 2. A few minutes: every installed indexer against the tiny tree — checks the wiring
sudo scripts/compare-indexers/benchmark.sh --do /tmp/ic-smoke/tree --smoke --yes

# 3. Minutes to an hour: the real tree, one repetition, warm caches, no GUFI rollup
sudo scripts/compare-indexers/benchmark.sh --do /data1/indexer-compare-synth --quick

# 4. Hours: the measured run (REPS=3, cold and hot, drop_caches as root)
sudo scripts/compare-indexers/benchmark.sh --do /data1/indexer-compare-synth \
     --work /data1/indexer-compare-work --results ~/bench-results

# Redraw figures and tables from an existing results dir (seconds, needs nothing else)
scripts/compare-indexers/benchmark.sh --charts ~/bench-results/run-<ts>

# Tear down: tree, indexes, built tools, database (keeps results unless --purge-results)
scripts/compare-indexers/benchmark.sh --undo /data1/indexer-compare-synth
```

Read the `result` column first in every mode: a tool that answers wrong is fast for the wrong reasons. `FAILURES.txt` lists *wrong answer* rows before *failed*, *predicate refused*, *cannot express* and *not available*.

### Modes

| Flag | Tree | Tools | Reps / cache | Results dir |
|------|------|-------|--------------|-------------|
| `--small` | `SYNTH_PROFILE=tiny`, ~2.5k entries, every shape (hardlinks, sparse, specials) | suite + `find` + `du`, rebuilt from the working tree | 1, warm | `results/small` |
| `--smoke` | same tiny tree | everything installed; runs `init.sh` and provisions MariaDB | 1, warm | `results/smoke` |
| `--quick` | the real tree | everything; `GUFI_DO_ROLLUP=0` | 1, warm | `results/quick` |
| *(default)* | the real tree, `SYNTH_PROFILE=medium` | everything | `REPS=3`, cold then hot, `DROP_CACHES=1` as root | `results/run-<ts>` |

Any env var set explicitly (`REPS=`, `TOOLS=`, `SYNTH_PROFILE=`, `DROP_CACHES=`, …) wins over a mode's default. At tiny sizes every timing is process start-up; the summary and figures say so in a banner.

### Other flags

| Flag | Effect |
|------|--------|
| `--work DIR` | scratch for every tool's index and `TMPDIR` (default `<tree>-work`); must not be inside the tree |
| `--results DIR` | where CSVs, summary and charts go (default `results/` next to the script) |
| `--reps N` / `--reps gufi=1,robinhood=1` / `--reps 3,gufi=1` | repetitions, globally or per tool (env: `REPS_GUFI=1`); a GUFI rollup can take 30 minutes a rep where `find` takes seconds |
| `--no-robinhood` | skip MariaDB entirely |
| `--yes` | no confirmation prompts (required off a terminal) |
| `--adopt TREE` | record a tree built by the step-by-step flow so `--undo` will accept it |
| `--keep-tools`, `--purge-results` | with `--undo` |

`--do` is re-runnable and resumes: a tree with a matching `FIXTURE_MANIFEST.txt` is not regenerated (a different `SYNTH_PROFILE` against the same root is an error; `FORCE=1` rebuilds). `--undo` refuses paths without the `.indexer-compare-run` marker and any top-level path, and deletes the two big trees with this repo's `edelete` rather than `rm -rf`. Step 0 prints the filesystem each of the three paths resolved to — check that `--work` is real scratch, since GUFI writes one SQLite database per directory.

## On a cluster

Everything here is CPU- and I/O-heavy and nothing it produces belongs on a quota'd home filesystem, so run it in a job with `PREFIX`, `SRC_ROOT`, the tree, `--work` and `--results` on scratch. On ORCD `benchmark.sh` and `init.sh` default `PREFIX` to `$HOME/orcd/scratch/ereport-automated-testing/prefix` when that directory exists.

```bash
SCRATCH=$HOME/orcd/scratch/ereport-automated-testing
srun -p mit_normal -N 1 -c 16 -t 3:00:00 bash -lc "
  export PREFIX=$SCRATCH/prefix SRC_ROOT=$SCRATCH/src \
         CARGO_HOME=$SCRATCH/cargo RUSTUP_HOME=$SCRATCH/rustup \
         TMPDIR=/scratch/\$USER/ic-\$SLURM_JOB_ID
  scripts/compare-indexers/benchmark.sh --do $SCRATCH/data/tree --smoke --yes \
    --work $SCRATCH/work --results $SCRATCH/results"
```

A compute node usually cannot install packages (`INSTALL_PACKAGES=0`) or run MariaDB (`--no-robinhood`, or a node you have root on); GUFI needs `cmake` loaded. `init.sh` records the libc it built against and later runs warn when they find an older one. A baseline that resolves but will not start (a `dua` built against a newer glibc) is reported as `dua is unusable: …` and filed under *not available*, not as a wrong answer.

## Installing the external tools

```bash
scripts/compare-indexers/init.sh                              # GUFI, XDU, Robinhood, dua, dut → $PREFIX; asks before dnf/apt
source "$PREFIX/env.sh"
TOOLS="gufi xdu" INSTALL_PACKAGES=1 scripts/compare-indexers/init.sh          # without Robinhood/MariaDB
INSTALL_PACKAGES=1 SETUP_MARIADB=1 RBH_FS_PATH=/data1/tree scripts/compare-indexers/init.sh   # root: + database and Robinhood config
```

Pinned versions: GUFI 0.6.10, XDU v0.4.1, Robinhood 3.2.0, dua-cli 2.39.1 (distro package when present, else `cargo install`), `dut` from a local checkout (`DUT_SRC`, default `~/git/dut`, because upstream's only tag predates the file-count flag). Overrides: `PREFIX`, `SRC_ROOT`, `JOBS`, `TOOLS`, `*_VERSION`, and the `*_BIN` variables in `lib.sh`. `INSTALL_PACKAGES` is `ask` by default (`1` installs, `0` never; non-interactive behaves as `0`). On dnf hosts `PKG_ARGS` defaults to `--disableplugin=etckeeper` because that hook can stall the transaction on an ssh prompt; set `PKG_ARGS=` for stock behaviour.

`mariadb.sh setup` creates the `rbh_indexer_compare` database, moves the server's data directory under `--work` so Robinhood's tables sit on the filesystem under test like every other index (`RBH_DB_RELOCATE=0` to keep them on the OS disk, flagged in the summary), and runs Robinhood's first scan so the schema exists. It only touches a database carrying its own ownership marker; `mariadb.sh adopt` rewrites the marker when `PREFIX` changed, `mariadb.sh cleanup` drops the database and moves the datadir back. The MariaDB package and service are always left alone.

## Step by step

```bash
scripts/compare-indexers/run_smoke.sh /data1/tree [/path/to/results]        # the whole pipeline, REPS=1

scripts/compare-indexers/prepare-synth.sh /data1/tree                        # tree + query_seeds/ + QUERY_SEEDS.txt
scripts/compare-indexers/check_correctness.sh /data1/tree/query_seeds
scripts/compare-indexers/run_index.sh /data1/tree
INDEX_RESULTS_DIR=… ECRAWL_BIN_DIR=… EREPORT_INDEX_DIR=… scripts/compare-indexers/run_queries.sh /data1/tree
python3 scripts/compare-indexers/summarize.py results/index-* results/queries-*
scripts/compare-indexers/plot_results.py results/run-A results/run-B --labels 'before,after' --out-dir /tmp/charts
```

`SYNTH_PROFILE` (`tiny`, `medium` default, `heavy`, `extreme`) picks the tree via [`generate-ecrawl-adversarial-tree.sh`](../fixtures/generate-ecrawl-adversarial-tree.sh). `plot_results.py` takes any number of result directories and draws each as its own series.

## What a run produces

- `SUMMARY_TABLE.txt` — a provenance block (host, kernel, libc, compiler, `ereport` commit, thread split, cache policy, tool versions), then one table per phase grouped by tool and cache state. Rows whose answer differs from the reference are marked `DISAGREES`.
- `FAILURES.txt` — every non-`ok` row and every `ok` row the reference contradicts, in five sections.
- `index/COMMANDS.txt`, `queries/COMMANDS.txt` — the exact argv of every timed command, shell-quoted.
- `index_results.csv`, `query_results.csv`, `env.txt` — everything the summary and charts are derived from; `--charts` regenerates both from these alone.
- `charts/` — six figures as PNG and PDF:

| Figure | Question |
|--------|----------|
| 1 `walk_time`, 2 `walk_rate` | how long does each *walk* take, split into file-count peers (`find`, `fd`, `ecrawl --no-stat --count`) and apparent-size peers (`du`, `dua`, `dut`, `ecrawl --no-write`) |
| 3 `build_time`, 4 `build_rate` | cold tree → queryable index, end to end; Figure 3 segments bars into `crawl` / `index` / `rollup` and draws the fastest walk as the floor |
| 5 `index_size` | bytes kept on disk, with bytes per file in the label; the suite's bar counts capture + trigram index, Robinhood's includes its three indexes |
| 6 `queries` | one panel per query on a shared log axis, cold and hot as separate bars, wrong answers hatched, missing bars explained (`no equivalent query` vs `needs the rolled-up index` vs `failed`) |

Elapsed seconds are the raw `elapsed_s` column; files per second divides by the tree's file count and is the number to compare across trees of different size. Axes go log once the spread exceeds 10×.

## Queries and rules

| ID | Question | Reference |
|----|----------|-----------|
| Q1 | one unique basename | `find -name` |
| Q2 | `slurm-*.out` glob | `find -name` |
| Q3 | regular files > 500 MB | `find -size +Nc -type f` |
| Q4 | apparent bytes of a subtree | `du -sb` (0.5% tolerance) |
| Q5 | regular-file count of a subtree | `find -type f` |
| Q6 *[extra]* | `*token*.dat` — unanchored, the shape a B-tree on names cannot seek | `find -name` |

The suite answers Q1/Q2/Q6 with `ereport_index --search <longest literal>` piped through a basename-anchored `grep -E` that reproduces the glob; Q3 with `ecrawl_query --size-gt --type f --list`; Q4/Q5 with `ecrawl_query --subtree [--type f]` using the dir-index sidecars from the `--make` phase. The **ecrawl (live walk)** row answers Q1/Q2/Q6 with `ecrawl --no-stat --contains` and Q5 with `--no-stat --count`, no index at all. Each query has three argument sets: set 1 is the measured cold+hot series, sets 2 and 3 run once hot so a hot number is never a re-answer of the same question.

The rules that make the rows comparable — apparent bytes everywhere (`gufi_du --apparent-size`, `xdu --apparent-size` at index time; `rbh-du` cannot and is marked *by definition*), a match is per name so hardlinks count per link, `THREADS` is the total budget per tool and split for tools with several pools, `RLIMIT_NOFILE` raised to 128k, GUFI charted as two series (`dir2index` alone and `+ rollup`, with no Q4 on the plain index), Robinhood always measured *with* its three indexes and its tables on the filesystem under test — are spelled out with their evidence in [capability-matrix.md](capability-matrix.md).

## Cache control

| Setting | Effect |
|---------|--------|
| `CACHE_MODES="cold hot"` | per tool: all cold reps, then all hot reps (`cold` alone for a single series) |
| `DROP_CACHES=1` | `sync` + `drop_caches` before each cold rep (root); the `cache` column says `warm` when a drop was wanted but not possible |
| `DROP_CACHES_SCOPE=first-rep` | only cold rep 1 drops |
| `DROP_DB_CACHE=1` | also restart MariaDB, since `drop_caches` leaves InnoDB's buffer pool warm |
| `ARG_SETS=3` | extra hot-only argument sets after the measured series |

A drop starts a unit (crawl + index, or Q1–Q6), not every command inside it. Remote-filesystem and controller caches are outside the harness's control.
