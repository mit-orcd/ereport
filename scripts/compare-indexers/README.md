# Compare Robinhood / GUFI / XDU against the ecrawl suite

Harness implementing the plan in `.cursor/plans/indexer_comparison_plan_*.plan.md`, based on Bjornson’s PEARC ’26 paper ([docs/…](../../docs/A%20Comparison%20of%20Three%20Open-source%20File%20Indexers%20Robinhood,%20GUFI,%20XDU.pdf)).

**ecrawl alone captures bins.** Query parity uses the suite: `ecrawl` + `ereport` + `ereport_index`.

## Contents

| Path | Purpose |
|------|---------|
| [benchmark.sh](benchmark.sh) | One-command install + run, and full teardown |
| [capability-matrix.md](capability-matrix.md) | Feature / privilege / predicate gaps |
| [prod-protocol.md](prod-protocol.md) | Read-only production-scale runbook |
| [prepare-synth.sh](prepare-synth.sh) | Medium synthetic tree + Q1–Q6 seeds, three argument sets each |
| [check_correctness.sh](check_correctness.sh) | `find` vs `ecrawl` counts |
| [run_index.sh](run_index.sh) | Index/capture timing + size, cold and hot |
| [run_queries.sh](run_queries.sh) | Q1–Q6 wrappers (3 reps × cold/hot) |
| [summarize.py](summarize.py) | `SUMMARY_TABLE.txt` + `FAILURES.txt` |
| [plot_results.py](plot_results.py) | Paper-style Figures 1–5 (PNG/PDF) |
| [run_smoke.sh](run_smoke.sh) | End-to-end synthetic smoke |
| [lib.sh](lib.sh) | Shared helpers / env overrides |

Missing external binaries are **skipped** (status=`skipped`), not hard errors — so the suite always runs.

Timing uses GNU `/usr/bin/time -v` when present (`dnf install time` / `apt install time`); otherwise a `date`-based fallback records `elapsed_sec=`.

## Quick start (one command)

`benchmark.sh` runs the whole thing — build the external tools, provision the
Robinhood database, generate the tree, benchmark, summarize, chart — and later
removes everything it created:

```bash
make -C ../.. ecrawl ereport ereport_index          # suite binaries

sudo scripts/compare-indexers/benchmark.sh --do   /data1/indexer-compare-synth
     scripts/compare-indexers/benchmark.sh --undo /data1/indexer-compare-synth
```

### On a batch cluster

Every step here is CPU- and I/O-heavy, so on a scheduled cluster it belongs in a
job, not on the login node, and nothing it produces belongs on a home
filesystem: the tool prefix, the cloned sources, the synthetic tree and the
indexes are all large, and a prefix built against one machine's glibc will not
run on an older one. No script needs changing for that — every path is already
an environment variable or a flag.

On ORCD, when `$HOME/orcd/scratch/ereport-automated-testing/prefix` already
exists, `benchmark.sh` and `init.sh` default `PREFIX` (and the matching `src/`)
there instead of `$HOME/.local/indexer-compare`. That stops a root or
login-node run from quietly using a home prefix that then fails probes on the
compute node. Explicit `PREFIX=` / `SRC_ROOT=` still win. Elsewhere the home
defaults remain.

```bash
SCRATCH=$HOME/orcd/scratch/ereport-automated-testing   # ORCD layout
srun -p <partition> -N 1 -c 16 -t 3:00:00 bash -lc "
  export PREFIX=$SCRATCH/prefix SRC_ROOT=$SCRATCH/src \
         CARGO_HOME=$SCRATCH/cargo RUSTUP_HOME=$SCRATCH/rustup \
         TMPDIR=/scratch/\$USER/ic-\$SLURM_JOB_ID
  scripts/compare-indexers/benchmark.sh --do $SCRATCH/data/tree --smoke --yes \
    --work $SCRATCH/work --results $SCRATCH/results"
```

After `source "$PREFIX/env.sh"`, a quick check that baselines are actually
runnable (not merely present) is `"$DUA_BIN" aggregate --help`. If the probe
fails, smoke still lists dua/fd and writes `skipped` CSV rows with
`dua_not_runnable: …` / `fd_not_runnable: …` instead of omitting them.

`benchmark.sh` now makes that check itself, once it has sourced `env.sh` and so
knows the `FD_BIN` and `DUA_BIN` the run will really use. A baseline that
resolves but will not start prints `dua is unusable: …` before the measured
work, naming which of the four reasons it was (gone, a directory, unreadable,
or no `+x`) and how to repair it. Those rows are also filed under NOT AVAILABLE
IN THIS RUN in `FAILURES.txt` rather than CANNOT EXPRESS THE QUERY: a broken
install is something to fix, not an expected empty cell.

Two things a compute node usually cannot do: install packages and run a MariaDB
server. So `INSTALL_PACKAGES=0`, and Robinhood needs either a node you have root
on or `--no-robinhood`; GUFI additionally needs `cmake`, which on a module-based
site means loading it in the job. `init.sh` records the libc it built against
and `lib.sh` warns when a later run finds an older one, which is the failure
mode a shared prefix produces.

### Fast test cycle (`--small`)

A full run takes hours, which is the wrong loop to be in while editing the C
code. `--small` asks the other question — does the suite still answer Q1–Q6
correctly — and answers it in well under a minute:

```bash
scripts/compare-indexers/benchmark.sh --do /tmp/small --small
```

It rebuilds `ecrawl`, `ereport`, `ereport_index`, `ecrawl_query` and `edelete`
from the working tree (so a cycle always tests what you just edited), generates
`SYNTH_PROFILE=tiny` (~2.5k entries in a second, one of every shape including
hard links, symlinks, specials and sparse files), checks correctness over the
whole tree rather than the seeded subtree, and compares against `find` and `du`
only. No external indexers are built, no packages are installed, MariaDB is
skipped, `REPS=1`, one cache pass instead of two, caches left warm. Results land
in `results/small`, reused
each cycle so the path stays stable. First cycle ~40 s including the compile,
later cycles ~17 s.

Read the `result` column, not the timings: at this size every elapsed figure is
process start-up, and the summary and both figures lead with a banner saying so.
Every default it picks — `SYNTH_PROFILE`, `REPS`, `TOOLS`, `DROP_CACHES`,
`CACHE_MODES`, `INSTALL_PACKAGES` — yields to the same variable set explicitly,
so `REPS=3 ... --small` does three repetitions.

### The external tools on the tiny tree (`--smoke`)

`--small` never touches Robinhood, GUFI or XDU, so it cannot tell you whether
*those* rows are right. `--smoke` is the same tiny tree with every installed
indexer running against it, which is enough because the wiring is what breaks:

```bash
sudo scripts/compare-indexers/benchmark.sh --do /tmp/ic-smoke/tree --smoke --yes
```

Every failure the first real run hit reproduces here in a minute. The tiny tree
carries the same fixtures as the big one, including three sparse 500 MB files
and a 1.68 GB subtree, which is what exposed XDU indexing allocated blocks: a
sparse 500 MB file allocates none, so it read as zero bytes and Q3 matched
nothing. GUFI's Python wrappers either import their modules or do not, Robinhood
either has its tables or does not, and `dua` either starts or names the library
it is missing.

It rebuilds the suite *and* runs `init.sh`, so the GUFI reinstall and the `dua`
rebuild are part of what is being tested, provisions MariaDB when Robinhood is
in `TOOLS`, keeps GUFI's rollup (instant at this size), runs one repetition with
warm caches (one pass, not two), and writes to `results/smoke`. The pass
condition is not a timing: it is every tool answering Q1–Q6 with the same numbers
as the `find` and `du` rows, and an empty `WRONG ANSWER` section in
`FAILURES.txt`. Robinhood's indexes are created and dropped here too, so that
wiring is under test at a size where it costs nothing.

### Correctness pass with the real tools (`--quick`)

`--smoke` proves the tools are wired up; it says nothing about a tree big enough
to have a shape. `--quick` runs everything against the real tree, but once and
without GUFI's rollup variant:

```bash
sudo scripts/compare-indexers/benchmark.sh --do /data1/indexer-compare-synth --quick
```

That turns hours into minutes. In a 3-repetition run on a 4.4M-entry tree the
index phase took 1.6 hours and GUFI's rollup was 91% of it — 29 minutes per
repetition against 33 seconds for `gufi_dir2index` — before counting the untimed
`du` over the resulting 580 GB index tree and the delete of the previous
repetition's copy. None of that tells you anything new about correctness, so
`--quick` sets `REPS=1` and `GUFI_DO_ROLLUP=0`, leaves caches warm and makes one
pass rather than a cold and a hot one, and writes to `results/quick`. Its timings
are real, just single-sample: check the `result` column against the `find` and
`du` rows, then start the measured run. Note what dropping the rollup costs you
here: with no rolled-up index there is no `gufi_rollup` query series and no GUFI
Q4 at all, so that row's correctness goes unchecked until the measured run.

Before the timed loop, every external tool is asked whether it accepts the shape
of each query — a name match, a size filter, a type filter, an aggregate —
against the small seeded subtree. Anything it rejects becomes one `skipped` row
carrying the tool's own error message instead of one full-tree scan per
repetition that ends in the same parse error. `FAILURES.txt` then separates rows
that came back with the *wrong answer* from rows that *failed*, predicates
*refused by this build*, questions a tool *cannot express*, and tools that were
simply *not available*.

Read the wrong-answer section first. It is the only one that catches a tool
which exits 0 and is believed: XDU reported 0 files over 500 MB and 0 bytes in a
subtree where `find` and `du` found 174,711 files and 1.68 GB, because its index
held `st_blocks` and the tree's large files are sparse. Any query row whose
result differs from the reference (`find` for Q1–Q3, Q5 and Q6, `du` for Q4,
within 0.5% for byte totals) lands there with both numbers side by side. The
comparison is per argument set, since each set has its own right answer.

### The three paths a run uses

| Path | Holds | Default | Size |
|------|-------|---------|------|
| benchmark tree | what every tool crawls | the `--do` argument | as generated |
| scratch (`--work`) | each tool's index, plus `TMPDIR` | `<tree>-work` | the bulk of the run |
| results (`--results`) | CSVs, `SUMMARY_TABLE.txt`, charts | `results/run-<ts>` next to the script | a few MiB |

Scratch defaults to a sibling of the tree so the indexes land on the filesystem
you pointed the benchmark at. That matters: the results directory defaults into
this checkout, and on a cluster `$HOME` is usually a quota'd NFS mount, while
GUFI writes one SQLite database per directory — millions of inodes on a large
tree. Point `--work` at real scratch space:

```bash
sudo scripts/compare-indexers/benchmark.sh --do /data1/indexer-compare-synth \
  --work /data1/indexer-compare-work --results ~/bench-results
```

All three paths are created if they are missing, parents included, so the three
arguments above need no `mkdir` first. Step 0 of the run prints the filesystem
and mount point each one resolved to — worth a glance, because a mount point
that is not mounted looks like an ordinary empty directory and would quietly
send the whole run to the root filesystem.

Scratch may not live inside the tree (every tool would index its own output) and
neither may the results. `--undo` deletes the `indexes/` and `tmp/` directories
it created but never the `--work` parent, which may be shared scratch.
`SUMMARY_TABLE.txt` records the filesystem of each, and flags when the indexes
were written to the same filesystem being crawled.

`--do` is re-runnable and skips work already done, so it is also how to resume
after an interrupted setup. It defaults to `REPS=3` (rather than `run_smoke.sh`'s
`REPS=1`) so the charts get error bars, and to `DROP_CACHES=1` plus
`DROP_DB_CACHE=1` when run as root. Add `--no-robinhood` to skip MariaDB
entirely, and pass any environment variable the underlying scripts accept.

### Repetitions per tool (`--reps`)

Three repetitions is the right default for the cheap rows and an expensive
mistake for the slow ones: a GUFI rollup takes 29 minutes a repetition on a
4.4M-entry tree where `find` takes seconds, so a run that wants error bars on
`find` should not have to buy three rollups to get them. `--reps` sets the count
per tool.

```bash
--reps 1                    # every tool once
--reps gufi=1,robinhood=1   # those two once, everything else at REPS
--reps 3,gufi=1             # spell the default alongside the exception
```

A bare number sets the default for every tool and counts as an explicit `REPS`,
so `--small`, `--quick` and `--smoke` will not override it. A `tool=n` entry
overrides just that tool and leaves the mode defaults alone, so
`--smoke --reps gufi=1` still runs everything else once. Entries are comma or
space separated and may be repeated; the env spelling is `REPS_GUFI=1`. Tool
names are checked against the list the harness actually runs, because a typo
that was accepted would cost a whole run at a count nobody chose.

Tools are still visited repetition-major, so each one meets the same cache state
it would have met at a flat `REPS`; a tool that has had its repetitions simply
drops out of the later passes. `REPS_EREPORT_INDEX` defaults to 1 — it indexes
the capture `ecrawl` just wrote, so a second pass re-measures identical input —
and is capped by `REPS_ECRAWL`. Its repetitions are `ecrawl`'s *last* ones, not
its first: each `ecrawl` repetition overwrites the capture pointer and prunes the
one before it, so only the final capture is still there to be queried, and an
index built from any earlier one is paired with a crawl that no longer exists.

When any tool differs, `env.txt` records `reps_per_tool=`, and both
`SUMMARY_TABLE.txt` and the chart captions name the exceptions rather than
claiming one sample size for rows that do not share it.

`--undo` prints exactly what it will remove and asks first (`--yes` to skip the
prompt, which is mandatory off a terminal). It drops the benchmark database and
moves MariaDB back to its packaged data directory before deleting the copy the
run made under `--work`, deletes the tree, and removes the built tools and their
clones, while **keeping
the results** unless `--purge-results` is given; `--keep-tools` keeps the built
binaries. It refuses to delete a tree that lacks the `.indexer-compare-run`
state file written by `--do`, and refuses any top-level path, so a mistyped or
hand-edited path cannot turn into an `rm -rf /data1`. Installed system packages
and the MariaDB service are always left alone.

The two big paths — the tree and the scratch — are deleted with this repo's
[`edelete`](../../edelete.c) (`--delete --force`, `EDELETE_THREADS` threads,
defaulting to the run's thread budget), because `rm -rf` unlinks a tree of tens
of millions of files single-threaded and takes hours over it. `--undo` says
which of the two it will use before asking, prints edelete's live progress
while it runs, and falls back to `rm -rf` if edelete is not built, cannot start
(a jemalloc-linked binary on a host without the library) or exits nonzero. The
small paths always use `rm -rf`. Override the binary with `EDELETE_BIN`.

A tree built by the step-by-step flow below has no state file, so `--undo` will
not touch it. Record it first with `--adopt`, which requires evidence that the
harness generated the tree: `prepare-synth.sh`'s `QUERY_SEEDS.txt` and
`query_seeds/`, or a generator directory such as `single_huge_dir/`, or an empty
directory. Anything else is refused, with its top-level contents printed:

```bash
scripts/compare-indexers/benchmark.sh --adopt /data1/indexer-compare-synth
scripts/compare-indexers/benchmark.sh --undo  /data1/indexer-compare-synth
```

### Redrawing the figures (`--charts`)

The summary table and the charts are pure functions of the CSVs a run leaves
behind, so revising them does not require measuring anything again:

```bash
scripts/compare-indexers/benchmark.sh --charts scripts/compare-indexers/results
```

This rereads `index_results.csv` and `query_results.csv` and rewrites
`SUMMARY_TABLE.txt`, `FAILURES.txt` and every figure in `charts/` as both PNG and
PDF. It takes seconds, needs neither the benchmark tree nor any external tool,
and is the intended way to iterate on presentation — a measured run costs hours,
and the figures get revised far more often than the numbers behind them. The
summary is rewritten along with the charts on purpose: the two are read side by
side, and a fresh figure next to a stale table is worse than either alone.

To put several runs on one set of axes, call the plotter directly; it takes any
number of result directories and turns each into its own series:

```bash
scripts/compare-indexers/plot_results.py results/run-A results/run-B \
  --labels 'before,after' --out-dir /tmp/compare-charts
```

## Step by step

The `/tmp` paths below are only the no-argument default of the tiny local smoke
test; pass a real path (and `WORK_ROOT=`) for anything larger.

```bash
# Full smoke: synth → correctness → index → queries → summary
scripts/compare-indexers/run_smoke.sh /tmp/indexer-compare-synth

# Or step by step:
scripts/compare-indexers/prepare-synth.sh /tmp/indexer-compare-synth
scripts/compare-indexers/check_correctness.sh /tmp/indexer-compare-synth/query_seeds
scripts/compare-indexers/run_index.sh /tmp/indexer-compare-synth
# then run_queries with INDEX_RESULTS_DIR / ECRAWL_BIN_DIR / EREPORT_INDEX_DIR set
python3 scripts/compare-indexers/summarize.py scripts/compare-indexers/results/index-* ...
```

`SYNTH_PROFILE=medium` is the default for `prepare-synth.sh` (reuse [generate-ecrawl-adversarial-tree.sh](../fixtures/generate-ecrawl-adversarial-tree.sh)). Raise to `heavy` / `extreme` for stress shapes (`mega_dir1`, etc.).

A second run against the same synth root does not rebuild the tree. The generator leaves a `FIXTURE_MANIFEST.txt` recording the parameters it built from, and skips when a later run asks for the same ones — so only the first benchmark against a given root pays the generation cost. Asking for a *different* profile against that root is an error rather than an overlay, because the two trees would be laid over each other; `rm -rf` the root, or point at a different one. `FORCE=1` rebuilds in place. `prepare-synth.sh` still recreates `query_seeds/` on every run, which is cheap, keeps Q1's unique basenames fresh and plants all three argument sets; `benchmark.sh --do` reseeds on its own when it finds a manifest from before the sets existed.

### Checking a code change (`SYNTH_PROFILE=tiny`)

For proving that a change to `ecrawl`, `ereport_index` or `ecrawl_query` still
answers Q1–Q6 correctly, the benchmark trees are the wrong tool: they take
hours, and the external indexers add nothing to a correctness question. The
`tiny` profile builds ~2.5k entries in about a second, still with one of every
shape that matters (a flat directory, a deep chain, a wide fan-out, hard links,
symlinks, specials, sparse and real payloads), runs the correctness check over
the whole tree rather than the seeded subtree, and compares only against `find`
and `du`, which are always installed. The whole round trip is under 30 seconds:

```bash
SYNTH_PROFILE=tiny scripts/compare-indexers/run_smoke.sh /tmp/tinytree /tmp/tinyres
```

Read the `result` column, not the elapsed columns: at this size every timing is
process start-up, so the summary and both figures say so in a banner. Set
`TOOLS_INDEX` / `TOOLS_QUERY` explicitly to bring other tools back in.

A site dnf plugin that runs on every transaction can stall the dependency
install on a prompt of its own — etckeeper committing `/etc` and pushing it to a
management node over ssh, for example, which surfaces as a password prompt and
then `Error: "etckeeper post-install" returned: -2`. The packages install fine;
only the hook fails, but its exit status still aborts the run. So on dnf hosts
`PKG_ARGS` **defaults to `--disableplugin=etckeeper`** (`--disable-plugin=` on
dnf5, and nothing on apt, which has no equivalent). The trade-off is that
etckeeper stops recording the `/etc` changes these installs make; fix the hook's
ssh access instead if that matters. Override with any dnf/apt arguments, or an
empty value to restore stock behaviour:

```bash
PKG_ARGS= scripts/compare-indexers/init.sh                       # nothing added
PKG_ARGS='--disableplugin=etckeeper --setopt=install_weak_deps=False' ...
```

`mariadb.sh` refuses to touch a database it cannot prove it created, using an
ownership marker under `$PREFIX/etc/robinhood.d/`. If that marker is lost — most
often because `PREFIX` differs from the run that created the database — recover
with `mariadb.sh adopt`, which rewrites the marker for an existing database so
`setup`, `reset` and `cleanup` work again. Setup writes the marker before
creating anything, so an interrupted run stays recoverable.

Setup also ends by creating Robinhood's tables, because Robinhood does not ship
a schema: it builds one on its first `--scan --once --alter-db`, and a database
that has never been scanned answers every query with `table ENTRIES does not
exist`. That scan runs before the tree is generated, so it walks nothing and
returns at once. On a prefix provisioned before setup did this, `mariadb.sh
schema` adds the tables on their own; `benchmark.sh --do` asks for it whenever it
finds an existing config.

Each run writes two audit files alongside the summary:

- `<results>/index/COMMANDS.txt` and `<results>/queries/COMMANDS.txt` — the exact
  argv of every timed command in execution order, shell-quoted so a line can be
  pasted back to reproduce one measurement by hand. Written by `time_cmd`, so it
  covers precisely what was measured, nothing else.
- `<results>/FAILURES.txt` — every row that did not finish `ok`, plus every row
  that finished `ok` with an answer the reference tool contradicts, grouped by
  tool and phase with the reps affected, the note the harness recorded, and the
  tail of that command's own stderr. Five sections: *wrong answer* (no exit code
  reports these), *failed*, *predicate refused by this build*, *cannot express
  the query* (a deliberate capability gap), and *not available in this run*.

`SUMMARY_TABLE.txt` opens with a provenance block so a result stays
interpretable later: host and OS, kernel, libc, compiler, the `ereport` commit
(with a `-dirty` marker), CPU count, thread and repetition settings, cache
policy and which passes ran, how many argument sets the queries rotated through,
where Robinhood's tables lived, the filesystem holding the results, and a version
line per tool. Every table below it is grouped by cache state as well as by tool,
so a cold and a hot measurement never share a row. Tools
that print no version banner — GUFI, and the suite binaries, which have no
`--version` — fall back to the tag `init.sh` pinned and to the build commit plus
binary timestamp respectively. The same keys are in each run's `env.txt`.

Each smoke run writes six figures, each as both PNG and PDF:

| File | Title | Question it answers |
|---|---|---|
| `charts/figure1_walk_time` | walking, elapsed time | Names-only vs metadata+size: how long does each peer walk take? |
| `charts/figure2_walk_rate` | walking, throughput | The same split, in files per second |
| `charts/figure3_build_time` | building, elapsed time | How long does it take to get from a cold tree to a queryable index? |
| `charts/figure4_build_rate` | building, throughput | How many files per second is *that*? |
| `charts/figure5_index_size` | index storage | What does the index cost to keep? |
| `charts/figure6_queries` | query performance | How fast is each query, and did it answer correctly? |

Figure 6 is paged three query panels at a time — Q1–Q3, then Q4–Q6 — because six
panels of up to seven bars on one sheet is a page nobody can read.
`figure6_queries.pdf` is a single file with one page per group; the PNGs, which
cannot be paged, are written one per page as `figure6_queries_p1.png` and
`figure6_queries_p2.png`. Every page shares one time axis, so a bar on page two
compares directly with a bar on page one.

The two rate figures use the same words as everything else here: a tool either
**walks** the tree (Figures 1 and 2) or **builds** an index from it (Figures 3
and 4), the denominator of both rates is the same file count, and both say
*files per second*. Phase names inside a bar — `crawl`, `scan`, `dir2index`,
`rollup`, `indexes`, `trigram index` — are the tools' own names for their own
commands, and are the one place the vocabulary varies on purpose.

Two questions, each asked twice. **Elapsed seconds** are what actually happened
and are exactly the `elapsed_s` column of `SUMMARY_TABLE.txt` — no conversion,
no per-1M scaling. **Files per second** is that same measurement divided into
the tree's file count, and it is the number that still means something on a tree
of a different size: with two result directories overlaid, the rate figures are
the pair to read, because elapsed time does not compare across two trees.

Every bar carries an unqualified measurement. Nothing is subtracted, estimated
or bounded, and no bar mixes tools whose costs were arrived at differently — the
one figure that did (build time with a walk taken off every bar) is gone, since
with elapsed seconds on the axis there is nothing left to subtract.

Every bar panel switches to a log x-axis once the spread exceeds 10×, which
these runs almost always do: with GUFI's rollup at 244 s against a 50 s
`ecrawl + ereport_index` and Robinhood at 1405 s, a linear axis ranks nothing
but the slowest tool and flattens everything else into a sliver at the origin.
On a log axis a phase split cannot be read additively, so the phases move from
the bar into its label.

**Figures 1 and 2** are the traversal figures, each drawn as two side-by-side
panels so peers stay with peers. Every bar’s timed command also returns an
answer (recorded in the CSV `notes` and the summary footnote):

- **Left — regular-file count:** `find -type f | wc -l`, `fd -t f | wc -l`, and
  `ecrawl --no-stat --count` (`files=`). Dirents only; no inode reads.
- **Right — apparent size:** `du -sb`, `dua`, `dut`, and `ecrawl --no-write`
  (`total_bytes=`). All credit each hard-linked inode once (`ecrawl` reports
  `hardlink_dedup=on`).

`ecrawl`'s full capture is not on these figures; its cost stays readable in
`SUMMARY_TABLE.txt` as `ecrawl/write` minus `ecrawl/nowrite`. `benchmark.sh`
defaults `DO_NOSTAT=1` and `DO_NOWRITE=1` so both walk panels get an ecrawl row;
set either to `0` to skip that extra traversal.

**Figures 3 and 4** are the build figures: everything it takes to go from an
unindexed tree to something queryable, as run, measured end to end for every
tool. `ecrawl + ereport_index`, `GUFI + rollup` and `Robinhood (scan + indexes)`
take two commands each, and
Figure 3 gives their phases in the bar (or in the label, on a log axis) so the
total that compares against a one-shot indexer does not hide which half the time
went to. Figure 4 does not split its bars: rates do not add, and a segmented
rate bar would say they do. Figure 3 also draws the fastest bare walk in the run
as a dashed line, the floor no indexer can go below.

GUFI appears on both as two rows, and they are **alternatives, never a sum**.
`gufi_dir2index` walks the tree and writes one SQLite database per directory —
that is a complete, queryable index, and the `GUFI (dir2index)` row is its cost.
`gufi_rollup` is an optional second pass that touches no part of the tree: it
copies each directory's rows up into its ancestors so that a query over a
subtree opens one database instead of thousands, trading a much larger index and
a much longer build for faster queries. `GUFI + rollup` is therefore the
`dir2index` build *plus* that pass, which is why it is roughly six times the
cost. The harness runs `gufi_dir2index` twice per repetition — the same command
into two directories, so the rollup has its own copy to work on — and both
figures pool those runs into the single `dir2index` measurement rather than
charting the same command twice.

Robinhood is a sum, not a choice: its scan fills index-free tables, which is not a
database anyone queries, and the second phase builds the three indexes its queries
need. `Robinhood (scan only)` is drawn as well, so it is visible how much of the
total is crawl and how much is `CREATE INDEX`, but the row that compares against a
one-shot indexer is the sum.

The queries follow suit: **GUFI is two query series**, `GUFI` reading what
`gufi_dir2index` wrote and `GUFI + rollup` reading what `gufi_rollup` then made of
it. Charting one GUFI let the cheap index take credit for the expensive one's
query times, and worse, for a capability it does not have: `gufi_du -s` answers
Q4 from treesummary rows that only `gufi_rollup` writes, and on a plain index it
warns, prints 0 and still exits 0. So the plain series has **no Q4 row at all** —
skipped with `rollup_required`, which Figure 6 labels *needs the rolled-up index*
rather than *no equivalent query*, and which `SUMMARY_TABLE.txt` spells out. A
GUFI Q4 time is a time against the 244 s build, never the 38 s one.

**Figure 5** is index storage: the total kept on disk, with bytes per file in
the bar label so the number carries to another tree. `GUFI + rollup` counts the
`dir2index` databases *and* the rolled-up copies, since both are on disk. The
`ecrawl + ereport_index` bar counts the capture alongside the trigram index as
one footprint, because **the capture is itself a queried index, not scratch on
the way to one**: `ecrawl_query` answers Q3 straight from the ERCBIN shards,
and Q4 and Q5 from those shards plus the dir-index sidecars in the same build.
The like-for-like pairing is GUFI's
SQLite replica against `ecrawl`'s capture, with the trigram index as a layer
GUFI has no equivalent to. Walk-only tools are absent because they store
nothing, which is the trade behind their times in Figure 1.

Robinhood's bar there **includes its three indexes**, since its index is a
database rather than a directory and the question is easy to get wrong. The
number is `du` of the relocated, benchmark-only MariaDB datadir after the scan,
plus the growth the three `CREATE INDEX` statements caused, which the
`Robinhood (scan only)` bar shows as its own row: 3.06 GiB of tables and 508 MiB
of indexes in the production run. Because it is the datadir, it also carries
InnoDB's own system tablespace and redo logs, a few hundred MiB that no other
tool has an equivalent of; `data_length + index_length` for the benchmark schema
alone is recorded per repetition in the `tables_and_indexes=` note of the
`robinhood/indexes` row.

**Figure 6** gives each query a panel, ranked fastest-last, on a shared log time
axis, with **Q6 titled *[extra]*** so nobody reads it as one of the paper's five.
Three things the earlier layout hid are now explicit:

- **Wrong answers are marked.** Each tool's result count is checked against a
  reference (`find` for Q1–Q3, Q5 and Q6, `du` for Q4, with a 0.5% tolerance on
  Q4's byte totals for hard-link and sentinel differences), per argument set. A
  tool that disagrees is hatched and labelled, because a query that returns
  nothing is fast for reasons that have nothing to do with its index.
  `SUMMARY_TABLE.txt` marks the same rows `DISAGREES`. This is not hypothetical:
  XDU answers Q3 with 0 of 84,840 matches in 83 ms, which would otherwise read as
  a win.
- **Missing bars are explained**, distinguishing "no equivalent query" (`du`
  cannot search by name) from "needs the rolled-up index" (GUFI's Q4) and from
  "failed" (the tool errored), which an empty slot cannot convey.
- **Cold and hot are separate bars**, so a query's warm latency is never reported
  as its cost against untouched storage, or the other way round.

Every figure carries the run's conditions — host, cpus, threads per tool, cache
state, repetitions — so a chart lifted into a document stays interpretable.
Captions stop there: they say what the bars are means of, which direction is
better, and anything the picture cannot show, and they leave the rest to this
file. A bar that names its own phases, a legend that names its own series and a
line annotated where it is drawn do not also get a sentence underneath.
Traditional walkers are drawn in neutral greys and indexers in colour. Each cache
state is its own series, and with several result directories each becomes a column
in Figure 6 and a lighter shade of the same tool colour in Figures 1–5.

Charts need matplotlib, which often belongs to a different interpreter than the
`python3` on `PATH`. `init.sh` installs the distribution package when available
and otherwise builds `$PREFIX/chartvenv`; `CHART_PYTHON` overrides the choice.
Charting is the last step and is non-fatal, so the CSVs and summary table
survive a missing plotting stack.

To reproduce the paper's two-dataset panels from two completed runs:

```bash
python3 scripts/compare-indexers/plot_results.py \
  scripts/compare-indexers/results/smoke-set1 \
  scripts/compare-indexers/results/smoke-set2 \
  --labels "Set 1 (10.3M files),Set 2 (116M files)" \
  --out-dir scripts/compare-indexers/results/paper-charts
```

## Paper queries

| ID | Meaning | find | fd | du | dua | dut | suite | GUFI | XDU |
|----|---------|------|----|----|-----|-----|-------|------|-----|
| Q1 | unique name | `-name` | `-g` glob | – | – | – | `ereport_index --search` \| `grep` | `gufi_find -name` | `xdu-find -p` |
| Q2 | `slurm-*.out` | `-name` glob | `-g` glob | – | – | – | `ereport_index --search slurm-` \| `grep` | `gufi_find` | regex |
| Q3 | size > 500MB | `-size` | `--size` | – | – | – | `ecrawl_query --size-gt --type f --list` | `-size` | `--min-size` |
| Q4 | subtree bytes, apparent | – | – | `du -sb` | `dua aggregate -A` | `dut -b -s` | `ecrawl_query --subtree` → `bytes=` | `gufi_du -s` (rollup only) | sum sizes |
| Q5 | subtree file count | `-type f` | `-t f` | – | – | – | `ecrawl_query --subtree --type f --list` | `gufi_find` | `--count` |
| Q6 | `*token*.dat` **[extra]** | `-name` glob | `-g` glob | – | – | – | `ereport_index --search token` \| `grep` | `gufi_find` | regex |

### Q6 is extra

Q6 is not one of the paper's five and is marked *extra* wherever it appears. It
asks for a token with nothing anchored at either end — `*token*.dat`, or
`%token%` in SQL — which is the one shape a B-tree on names cannot seek into: the
index is there, Robinhood's `name_index` included, and the optimiser still has to
read every row. A trigram index has no such trouble; the longest literal run of
`*token*.dat` is the token itself, so `ereport_index --search` is handed the most
selective term the query contains and does not care that it sits in the middle of
the name. Q6 and Q2 run the same predicate shape with one anchor's difference
between them, and the difference between the two rows is the whole point.

The seeds plant a control alongside the matches: a *directory* whose name carries
the token, and a file with the token but the wrong extension. A tool that indexes
directories as well as files, or that matches on the path rather than the
basename, will over-report, and the correctness check catches it.

The suite answers Q1, Q2 and Q6 in two stages, and the row times both. The trigram
index matches a literal substring anywhere in the path, which is a superset of
what `-name` means, so the harness searches for the longest literal run in the
pattern — the smallest candidate set the index can produce — and pipes it
through a basename-anchored `grep -E` that reproduces the glob. The answer is
`find`'s, not an approximation of it; `query_params.txt` records the term and
the regex, and `COMMANDS.txt` shows the pipeline as it ran.

Q3, Q4 and Q5 go to `ecrawl_query`, which selects records straight out of the
capture: `--size-gt N` for the size predicate, `--subtree DIR` for the two
subtree questions, `--type f` for regular files, and `--list` to print paths the
way `find` does. `--subtree` totals count each multiply-linked inode once, so
`bytes=` matches `du -sb` exactly.

### Q4 and Q5 depend on the `ereport_index --make` phase

Both are handed `--index-dir`, the same directory the Q1/Q2/Q6 rows search, and
read the dir-index sidecars that build writes beside the trigram index: Q4 looks
its subtree root up instead of materialising every directory row, and Q5 skips
the row groups whose DFS sketch cannot hold a descendant. Nothing there is free —
the build's elapsed time and its size, sidecars included, are already a row of
their own and a bar in Figures 3–5. Q3 has no subtree to resolve, is not given
the index, and still costs a full pass over the shards.

The sidecars are **bound to the exact capture they were built from** — per shard
the name, size, mtime, catalog offset and highest directory id — so an index
paired with a different crawl is rejected rather than believed, and the query
falls back to the catalog. The index phase therefore builds from `ecrawl`'s
*last* repetition, the one whose capture survives the run and gets queried; the
`ereport_index`/`make` row records that capture in its `input=` note.
`answered_from=` in the Q4 row's note says which path actually ran: `dir_index`,
`catalog_rollup`, or `record_scan` when a hard link under the subtree forces the
full scan.

### Why the tools disagree on Q3 and Q4

Some tools return a different number for the size questions, and none of them
are broken. A count or a byte total only compares when everyone is asked for the
same thing, so this comparison pins one definition — **Q4 is apparent bytes
including directory inodes, which is `du -sb`, and a match count is per name,
so a file with four hard links counts four times** — and then reports, per row,
where a tool's index answers something else. Those rows are still run, still
timed and still charted. They are marked, not dropped: a tool that cannot
express the question is a finding, and a blank cell hides it.

Three axes, each verified against the seeded tree:

**Unit: apparent bytes against allocated blocks.** The seeds plant their large
files with `truncate`, so they are all hole and allocate nothing —
`results/correctness/ecrawl.stdout.txt` records `total_bytes=3984591872` beside
`total_allocated_bytes=0` and `files_sparse_heuristic=8`. `rbh-du -s` reports
that subtree as `80`, its directory blocks in KiB, where `du -sb`, `dua`,
`gufi_du --apparent-size` and `ecrawl_query` all report 3,984,641,243. That is
not a Robinhood defect: the paper's Q4 is literally *"disk usage of large
subdirectory"*, which is what `rbh-du` answers, and Robinhood 3.2.0's `rbh-du`
documents no byte flag to switch it (`env.txt` records the probe as
`rbh_du_args=`). XDU has the same split and can be told: without
`--apparent-size` at index time its records hold `st_blocks`, its Q3 matches
nothing and its Q4 comes out at zero, so `init.sh` pins the flag when the build
has it and the harness skips both size queries when it does not — a number in
blocks printed beside `du -sb` reads as a bug in XDU rather than as a gap in the
build.

**Scope: files against files and directories.** `gufi_du` and `xdu-find` total
their file records; `du -sb` also counts the apparent size of every directory
inode. On the main subtree that is the 49,371-byte gap between GUFI's
3,984,591,872 and `du`'s 3,984,641,243. On the `bulk/` subtree, whose 2,000
files are all empty, directories are the entire answer: `du -sb` says 49,152 and
both GUFI and XDU say 0.

**Identity: names against inodes.** Robinhood's `ENTRIES` is keyed by inode and
`rbh-find` prints one path per entry, so a hard-linked file is one row for it and
one row per link for `find`. Over the 500 MB threshold `find` lists eight names
in `links_and_specials/`, Robinhood lists two — a constant six-row shortfall at
every threshold, which is how it was told apart from a size problem. Robinhood's
size column itself is `st_size`: it matched exactly the seeded sparse fixtures at
each of the three thresholds (5, 7 and 2), which an index holding `st_blocks`
could not do.

Where a difference is definitional, `SUMMARY_TABLE.txt` names it under the
DISAGREES tally, `FAILURES.txt` gives it a `by definition:` line, and Figure 6
puts it on the bar next to the two numbers.

### Traditional baselines

Five traditional tools run as separate rows, one serial search and one serial
aggregate against their parallel counterparts, so a slow `find` or `du` is not
mistaken for the best the traditional approach can do:

| Tool | Pair | Answers |
|------|------|---------|
| `find` | serial search | Q1–Q3, Q5, Q6 |
| `fd` | parallel search | Q1–Q3, Q5, Q6 |
| `du` | serial aggregate | Q4 |
| `dua` ([dua-cli](https://github.com/Byron/dua-cli)) | parallel aggregate | Q4 |
| `dut` ([dut](https://codeberg.org/201984/dut)) | parallel aggregate | Q4 |

A dash above means the tool has no primitive for that query, not that it lost:
`find`/`fd` cannot total a subtree, and `du`/`dua`/`dut` have no name, type or
size predicates. Q4 therefore moved off the `find` rows onto `du`, which measures
the identical `du -sb` call. `find` is also the reference every other tool's count
is checked against, Q6 included — an unanchored glob costs it exactly what an
anchored one does, since it was reading every name either way.

`fd` follows the [test.sh](../test/test.sh) convention (`--hidden --no-ignore`,
plus `--one-file-system` when supported, matching `find -xdev`); Debian's
`fd-find` package installs it as `fdfind`, which `FD_BIN` detects. `dua` runs
`aggregate --apparent-size` to stay on the harness's byte convention, and is
handed an empty sentinel directory as a second input because `dua aggregate`
with a *single* input prints one line per child instead of the subtree total.
With the sentinel its total matches `du -sb` exactly. `DUA_BIN` is probed by
running it, since a `dua` built against a newer glibc resolves on `PATH` but
dies at startup.

`dut` is the other parallel `du`, and runs as `-c -b -s -d 0 -n 1`, plus `-x` on
the walk row only. `-s -b` is what makes its total apparent bytes rather than
size on disk, and it deduplicates hard links the way `du` does, so it needs no
annotation: on the seeded fixture it returns `du -sb`'s number to the byte. The
rest are not cosmetic — `dut` sizes its output from the terminal height and,
with stdout on a pipe and no `-n`, prints *every entry in the tree*, which on a
production tree would make the row a measurement of printing rather than of
walking, and `-c` drops the colour escapes so the total parses. The `-x` split
is deliberate: on the walk row `dut` is the counterpart of `find -xdev` and is
bounded like every other walk baseline, while Q4 is compared against `du -sb`,
which crosses mount points, so a `dut` held to one filesystem would answer a
smaller number on any tree with a mount inside it and be marked DISAGREES for a
difference this harness introduced. `env.txt` records both vectors. It answers
Q4 only: `-f` counts files and directories together with no type predicate, so
Q5 is skipped rather than answered with a number that means something else.

All three parallel walkers are pinned to `THREADS`, the same budget ecrawl,
GUFI, XDU and Robinhood get — `dut` defaults to 4 threads or one per logical
processor, whichever is larger; `find` and `du` are single-threaded by design,
which is the point of reporting all five.

### Thread budget

`THREADS` (default 16) is the **total** worker count per tool, not a per-pool
setting, because the interesting comparison is between indexers rather than
between their shipped defaults — which range from Robinhood's 2 scan threads to
`fd`'s one-per-core (448 on a fat node). Tools that run several pools at once
split the budget:

| Tool | How the budget is spent |
|------|------------------------|
| ecrawl | crawl + stat + writer, keeping its stock 2:1:1 shape (16 → 8+4+4) |
| `ereport_index --make` | parse workers + trigram writers, halved |
| ereport, GUFI `-n`, XDU `-j`, `fd`, `dua`, `dut` `-t` | one pool, the whole budget |
| Robinhood | `nb_threads_scan` + `EntryProcessor`, halved, written into its config |
| `find`, `du` | single-threaded, nothing to set |

Setting a specific variable (`ECRAWL_CRAWL_THREADS`, `EREPORT_INDEX_THREADS`, …)
overrides its share and is reported as resolved. Robinhood is the one tool whose
share is fixed at `mariadb.sh setup` time rather than per run, so `run_index.sh`
re-reads the config and flags a mismatch in the notes column. GUFI's query
wrappers are pinned only when the installed build advertises a thread flag in
`--help`. `SUMMARY_TABLE.txt` prints the resolved split.

`benchmark.sh` also raises `RLIMIT_NOFILE` to 128k (`NOFILE_TARGET`) before
anything runs; `ereport_index` and GUFI both exceed the stock 1024 easily. A
non-root run is capped by the hard limit, and the summary records what was
actually in force.

Seeds live under `<synth>/query_seeds/` and `<synth>/QUERY_SEEDS.txt` after `prepare-synth.sh`.

## Index metrics

Every row records `elapsed_sec`, `index_bytes` and `file_count`. The charts and
the summary table derive two numbers from those: files per second
(`file_count / elapsed_sec`) and bytes per file (`index_bytes / file_count`).
`run_index.sh` also writes the paper's `sec_per_1M_files` and
`mib_per_1M_files`, which nothing reads any more but which stay in the CSV so
older result sets and the paper's own tables remain comparable.

**Separate rows:**

1. `ecrawl` / `write` — ERCBIN08 shards (+ ckpts)
2. `ereport_index` / `make` — trigram index **on top of** crawl bins, plus the
   dir-index sidecars `ecrawl_query` answers Q4 and Q5 from. Built from
   `ecrawl`'s last repetition, because that is the capture the query phase reads
   and the sidecars only work with the capture they were made from; the `input=`
   note names it.
3. `ecrawl` / `nowrite` — stat walk, storing nothing. Like-for-like against
   `du` / `dua` / `dut` on the walk figures' right panel. The gap to
   `ecrawl`/`write` is the cost of writing the capture (`SUMMARY_TABLE.txt`).
   Default on under `benchmark.sh --do` (`DO_NOWRITE=1`).
4. `ecrawl` / `nostat` — `--no-stat --count`; answers regular-file count
   (`answer_files=`). Like-for-like against `find` / `fd` on the walk figures'
   left panel. Default on under `benchmark.sh --do` (`DO_NOSTAT=1`).
5. GUFI `plain` and `rollup_index` — the same `gufi_dir2index` command into two
   directories, so the rollup has its own copy to work on; the charts pool them
   into one measurement. `rollup_step` is the separate `gufi_rollup` pass, and
   its `index_bytes` is what the rollup adds on top of the replica, so the two
   rows sum to what is on disk.
6. XDU parquet
7. Robinhood `scan` and `indexes` — two stages, not alternatives. The scan
   (site-configured via `RBH_SCAN` + `RBH_SCAN_ARGS`) crawls into index-free
   tables, which is not a database anyone queries; `indexes` times the three
   `CREATE INDEX` statements that make them queryable. Both are what Robinhood
   costs, and the charts and the summary add them.
8. `find` / `fd` / `du` / `dua` / `dut` `walk` — live walks with `index_bytes=0`
   that return `answer_files=` (find/fd) or `answer_bytes=` (du/dua/dut)

The walk rows are reference points rather than indexers: they stay out of the
build and storage charts and appear only on Figures 1 and 2, split into
file-count peers (`find`/`fd`/`ecrawl`/`nostat`) and apparent-size peers
(`du`/`dua`/`dut`/`ecrawl`/`nowrite`). Answers live in CSV `notes` and the
summary footnote; charts still plot elapsed time and files/s.

Also reuse the heavier fixture profiler when needed:

```bash
DO_WRITE=1 DROP_CACHES=1 ./scripts/profile/ecrawl-fixtures.sh \
  /tmp/indexer-compare-synth /data1/ecrawl-bins
```

## External tool installation (hints)

The benchmark needs the tools' source repositories to reproduce the paper's
versions. [`init.sh`](init.sh) clones and builds the pinned tags into a private
prefix:

```bash
# Installs GUFI, XDU, Robinhood, dua and dut into the default PREFIX
# (~/.local/indexer-compare, or the ORCD scratch prefix when that dir exists).
# Lists the dnf/apt build dependencies and asks before installing them.
scripts/compare-indexers/init.sh
source "$PREFIX/env.sh"   # or ~/.local/indexer-compare/env.sh

# Root/sudo: also install/start MariaDB, create a dedicated benchmark database,
# and generate the Robinhood config for this tree.
INSTALL_PACKAGES=1 SETUP_MARIADB=1 \
  RBH_FS_PATH=/tmp/indexer-compare-synth \
  scripts/compare-indexers/init.sh
source "$PREFIX/env.sh"

# Faster initial setup without Robinhood/MariaDB:
TOOLS="gufi xdu" INSTALL_PACKAGES=1 scripts/compare-indexers/init.sh
```

`INSTALL_PACKAGES` defaults to `ask`: the required and optional package lists
are printed and confirmed before anything touches the host. `1` installs without
asking and `0` never installs; a non-interactive run (no terminal on stdin)
behaves like `0` rather than blocking on a prompt. Missing dependencies are
reported by name — including pkg-config modules such as `libpcre2-8`, checked
before CMake runs — instead of as a configure-time stack trace.

Defaults can be overridden with `PREFIX`, `SRC_ROOT`, `JOBS`, `TOOLS`, and
`*_VERSION`; see the script header. Sources are cloned at pinned tags into
`SRC_ROOT` and built outside the checkouts, so re-running `init.sh` rebuilds in
place instead of tripping over build output from the previous run.

The generated database is named `rbh_indexer_compare`. `run_index.sh` resets
only this marked database before each Robinhood repetition and sizes its index by
measuring MariaDB's data directory. After benchmarking, remove the database, its
dedicated user, password file, config, and logs:

```bash
PREFIX=$HOME/.local/indexer-compare \
  scripts/compare-indexers/mariadb.sh cleanup
```

Cleanup deliberately leaves the MariaDB package and service installed. The
helper refuses to drop a database unless its private provision marker matches.

- **GUFI 0.6.10:** <https://github.com/mar-file-system/GUFI> — `gufi_dir2index`, `gufi_find`, `gufi_du`, optional `gufi_rollup`
- **XDU v0.4.1:** <https://github.com/glentner/xdu> — `xdu`, `xdu-find`
- **Robinhood 3.2.0:** <https://github.com/cea-hpc/robinhood> — POSIX build; optional `SETUP_MARIADB=1` provisions a disposable local database and config
- **dua-cli 2.39.1:** <https://github.com/Byron/dua-cli> — `dua`; taken from the distribution package when present, otherwise `cargo install`
- **dut:** <https://codeberg.org/201984/dut> — `dut`; built from a local checkout (`DUT_SRC`, default `~/git/dut`) rather than a pinned tag, because upstream's only tag predates both the flag that counts files and a fix for losing entries when the getdents buffer fills. The sources are copied into `SRC_ROOT` and built there, so the checkout stays clean, and the stamp is `git describe --tags --always --dirty` on it — edit that tree and the next `init.sh` rebuilds

Overrides: see `lib.sh` (`GUFI_DIR2INDEX`, `XDU_BIN`, `ECRAWL_BIN`, …).

### Robinhood's tables live on the storage under test

Every other tool writes its index under `--work`, on the filesystem being
benchmarked. A packaged MariaDB keeps its tables in `/var/lib/mysql`, on the
operating system's disk — a different filesystem, usually a different class of
device — so a Robinhood row measured there is not comparable with the rest of the
table. `mariadb.sh setup` therefore **moves the data directory** to
`RBH_DB_DATADIR`, which `benchmark.sh --do` points at `<work>/mariadb`, and asks
for the move on every `--do` rather than only at provisioning time: a database set
up before this existed, or one whose `--work` has changed, would otherwise keep
writing to the OS disk for the rest of its life.

The move is a copy of the packaged datadir plus a `datadir=` drop-in, with the
SELinux label copied from the original and a systemd drop-in relaxing
`ProtectHome`/`ProtectSystem` — which is what actually blocks a datadir under
`/home`, whatever its permissions say. `mariadb.sh cleanup` puts the server back
on its packaged directory and deletes the copy; `mariadb.sh datadir` prints where
the tables are now, and `--do` reports it in step 0 beside the other paths.
`SUMMARY_TABLE.txt` records
the path with its filesystem, so a run that could not move them says so.
`RBH_DB_RELOCATE=0` keeps the packaged location and warns that the rows are not
comparable.

### Robinhood is never measured without its indexes

Nobody runs a relational database unindexed, so the harness never measures one.
The scan fills index-free tables and is timed; then the three indexes its queries
need are created as a **timed phase of their own**:

```sql
CREATE INDEX name_index ON NAMES (name);
CREATE INDEX size_index ON ENTRIES (size);
CREATE INDEX type_index ON ENTRIES (type);
```

The queries then run against them, and the query phase **drops them again** when
it is done, so the next `--do` crawls index-free tables and pays for creating them
once more. That is what makes the two runs comparable. `run_queries.sh` creates
them itself, untimed, if the index phase did not run — a standalone query run, or
a results directory whose crawl predates this — and notes that it did in
`harness.log`. `name_index` is what Q1 and Q2 need, `size_index` is Q3,
`type_index` is the `-type f` in Q3 and Q5; nothing there helps Q6, which is the
point of Q6.

## Production

See [prod-protocol.md](prod-protocol.md).

## Cache control

| Setting | Effect |
|---------|--------|
| `CACHE_MODES="cold hot"` | which passes each repetition makes; `CACHE_MODES=cold` restores the old single-pass behaviour |
| `DROP_CACHES=1` | `sync` + `drop_caches` before each timed command on a cold pass (needs root) |
| `DROP_CACHES_SCOPE=first-rep` | only rep 1 is cold, so later reps show warm latency as in the paper |
| `DROP_DB_CACHE=1` | also restart MariaDB, since `drop_caches` leaves InnoDB's buffer pool warm |
| `ARG_SETS=3` | ceiling on how many query argument sets the hot passes rotate through |

### Cold and hot are both measured

Cold and hot answer different questions, and a run that reports only one of them
is answering the other by accident. So every repetition runs twice:

- **cold** — caches dropped before each timed command (and MariaDB restarted for
  the rows that read it), which is what the work costs against storage nobody
  has touched.
- **hot** — nothing dropped, which is what it costs when the metadata is already
  in memory: the state a second query on a live system meets.

The `cache` column of both CSVs says which pass a row belongs to, and it is
`warm` rather than `cold` when the run wanted a cold pass but had no privileges
to drop anything — a claim the run cannot make should not be in the data. The
summary table and the charts key on that column, so cold and hot appear as two
series rather than being averaged into a number neither of them measured.

In the index phase each tool's **whole pipeline** runs cold and then hot, so a
hot `ereport_index` reads the shards a hot `ecrawl` just wrote and a hot
`gufi_rollup` the tree its own `dir2index` wrote. The hot pass rebuilds into the
same directory, so the disk footprint is unchanged.

### Query argument sets

A hot query that asks exactly what the cold one just asked is measuring the
answer it already has. `prepare-synth.sh` therefore plants **three argument sets**
per query — three unique basenames, three glob families with their own literals,
three size thresholds, three subtrees, three infix tokens — and each repetition
asks:

1. cold, set 1
2. hot, set 1 — the same work, so the cold/hot delta is a clean comparison
3. hot, set 2 and hot, set 3 — different questions, so no later bar is a
   re-answer

Set 1 is what the manifest's unindexed keys name, so a seed file written by hand,
or by a `prepare-synth.sh` from before the sets existed, still resolves as set 1.
An exported `Q2_GLOB` (or any other query variable) pins that value for every
set. Correctness is checked per (query, argument set): each set has its own right
answer, so checking one against another's reference would report every tool as
disagreeing. `benchmark.sh --do` reseeds a tree whose manifest predates the sets,
which recreates `query_seeds/` and leaves the tree itself alone.

Remote filesystem and storage-controller caches are still outside this harness's
control.

## Fairness checklist

- Same tree, host, and thread budget
- `THREADS` is the total per tool and covers ecrawl, `ereport_index`, ereport, GUFI, XDU, `fd`, `dua`, `dut` and Robinhood; `env.txt` records the resolved split plus `fd_args` / `dua_args` / `dut_walk_args` / `dut_query_args`
- `RLIMIT_NOFILE` raised to 128k, so no tool is throttled by a limit another one escaped
- Every index and every query measured cold **and** hot, recorded per row in the `cache` column and never averaged together
- Hot queries rotate through three argument sets, so no hot number is a re-answer of the question the pass before it asked
- Robinhood's tables on the same filesystem as every other tool's index, and always queried with the three indexes its queries need
- GUFI's two indexes charted as two series, so the cheap one cannot borrow the expensive one's query times or its capabilities
- Byte totals: unique-inode logical (as `test.sh`)
- Do not force foreign indexes through `scripts/profile/ereport*` (ecrawl-bin only)
- See [capability-matrix.md](capability-matrix.md) for predicate gaps
