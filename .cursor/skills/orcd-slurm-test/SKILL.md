---
name: orcd-slurm-test
description: >-
  Runs ereport builds and tests on ORCD via Slurm compute nodes with local
  /scratch TMPDIR and shared toolchains under ~/orcd/scratch/ereport-automated-testing.
  Use when compiling, running test.sh, profiling.sh, make check, compare-indexers,
  or any heavy build/benchmark on orcd-login or ORCD HPC.
---

# ORCD Slurm build/test

## When to use

Any compile, `./scripts/test/test.sh`, `make check`, `scripts/profile/profiling.sh`, or heavy indexer work while the agent shell is on an ORCD login node.

Chart and report regeneration counts. `plot_results.py` is easy to mistake for a
light script because it only reads a CSV, but matplotlib renders five figures to
PNG and PDF and takes ~15 s a call; iterating on a figure means calling it
repeatedly.

## Layout

```text
~/orcd/scratch/ereport-automated-testing/
  bin/       # suite binaries if deploying outside the git tree
  prefix/    # PREFIX for scripts/compare-indexers/init.sh
  cargo/     # CARGO_HOME
  rustup/    # RUSTUP_HOME
  work/      # --work indexes / TMPDIR for long runs
  data/      # synthetic trees that more than one job needs
  src/       # third-party source checkouts init.sh reuses
  report/    # REPORT_DIR for profiling.sh
  scripts/   # job drivers written for a task, not for the repo
  logs/      # sbatch stdout, one file per job id
  results/   # per-job output dirs, named <run>-<jobid>
```

Repo stays on home NFS (`~/git/ereport`). Scripts stay unchanged; override paths with env / flags they already accept.

Keep new files inside those directories and name job artifacts after the job id,
so a later session can tell a finished run from one still going. This tree is
shared with concurrent sessions: add and clean up your own runs, and leave
directories you did not create alone.

## Env (export on the compute node before tool installs)

```bash
SCRATCH=~/orcd/scratch/ereport-automated-testing
export PREFIX=$SCRATCH/prefix
export CARGO_HOME=$SCRATCH/cargo
export RUSTUP_HOME=$SCRATCH/rustup
export PATH=$PREFIX/bin:$CARGO_HOME/bin:$PATH
```

`benchmark.sh` / `init.sh` already default `PREFIX` (and matching `src/`) to
`$SCRATCH/prefix` when that directory exists, so a forgotten export does not
fall back to `$HOME/.local/indexer-compare`. Still export explicitly in job
scripts; keep Cargo/Rustup on scratch either way.

After `init.sh` once: `source "$PREFIX/env.sh"` when present. Confirm baselines
with `"$DUA_BIN" aggregate --help` before a measured run — a present-but-broken
binary now emits `skipped` CSV rows rather than vanishing from the tool list.

## Run test.sh (default recipe)

CPU smoke / unit harness — submit to `mit_normal`:

```bash
srun -p mit_normal -N 1 -c 8 -t 30:00 bash -lc '
set -euo pipefail
echo "host=$(hostname) job=${SLURM_JOB_ID:-?}"
if [[ -d /scratch && -w /scratch ]]; then
  export TMPDIR="/scratch/${USER}/ereport-test-${SLURM_JOB_ID}"
else
  export TMPDIR="/tmp/${USER}/ereport-test-${SLURM_JOB_ID}"
  echo "WARN: /scratch not usable; TMPDIR=$TMPDIR"
fi
mkdir -p "$TMPDIR"
cd /home/erbmi1/git/ereport
make -j"$(nproc)"
./scripts/test/test.sh
rm -rf "$TMPDIR"
'
```

Raise `-t` as needed, up to the `mit_normal` 12-hour maximum, for longer
profiling or compare-indexers runs. Prefer `sbatch` for multi-hour unattended
jobs.

## Picking a partition

Partition selection is fixed by explicit user instruction. Two partitions are
allowed and no others:

- `mit_quicktest` — 15-minute maximum, at most 4 nodes. Toolchain probes, smoke
  checks, chart re-renders. Its queue turns over quickly, so prefer it whenever
  the work honestly fits inside the cap.
- `mit_normal` — 12-hour maximum, 50 nodes. Everything that cannot finish in 15
  minutes: full builds, `test.sh`, profiling, compare-indexers, benchmarks.

Do not select any other partition, regardless of queue depth, apparent
capacity, or wall-time. `mit_normal` is often deep — several hundred pending
jobs is ordinary — and queueing there is still the instruction rather than a
reason to look elsewhere.

Size the request honestly: a job that overruns `mit_quicktest`'s 15 minutes is
killed mid-run, and a serial `make clean && make` followed by `test.sh` does not
reliably fit. When in doubt use `mit_normal`, because a long queue costs
latency while a truncated job costs the whole run.

Do not pass `-A` / `--account`. There is a single association, both partitions
are `AllowAccounts=ALL`, and naming the account can only introduce a way to
disagree with the default.

Agents are explicitly authorized to submit Slurm jobs without asking the user
for permission first.

Never keep anything that has to outlive a job on node-local `/tmp` or `/scratch`:
the next job may land on a different node, and what the last one wrote is then
simply not there. Anything to be reused — fixture trees, tool prefixes, results,
logs — goes under `$SCRATCH` (see the layout above).

Node-local disk is still the right place for data a *single* job creates and
consumes, so a million-file fixture tree belongs there when generation and
measurement happen in one job. If two jobs need the same tree, either merge
them or put the tree in `$SCRATCH/data` and accept NFS speed.

Probe toolchain and zstd before trusting an unfamiliar node. The default `gcc`
may be a spack build under `/orcd/software/...`; that alone is no defect, so do
not force `CC=/usr/bin/gcc`.

```bash
srun -p mit_quicktest -N 1 -c1 -t 3:00 --mem=2G bash -lc \
  'echo "int main(void){return 0;}" >/tmp/p$$.c; printf "%s " "$(hostname)"; \
   gcc -o /tmp/p$$ /tmp/p$$.c 2>/dev/null && echo cc=ok || echo cc=FAIL; \
   pkg-config --modversion libzstd || echo zstd=MISSING; rm -f /tmp/p$$*'
```

Ask for memory explicitly. The default is 1 GB per CPU, and a `-c 16` job therefore gets 16 GB, which is not enough to build XDU (it pins `lto = true` and `codegen-units = 1`, so `arrow` and `parquet` are optimised whole-program in one process). That allocation was OOM-killed 12 times; `--mem=64G` is a safe floor for a Rust build.

## Submission privileges

Agents do not need to ask permission before submitting Slurm jobs.

- The user is not a Slurm administrator.
- Submit only to `mit_quicktest` (15-minute maximum, 4 nodes) or `mit_normal`
  (12-hour maximum, 50 nodes). No other partition, and no GPU partition.
- Do not pass `-A` / `--account`; the single association is already the default.
- User QOS includes `normal` and `unlimited`. On `mit_normal`,
  `--qos=unlimited` can override aggregate resource caps but cannot override
  the 12-hour partition wall-time.
- Default memory is 1 GB per requested CPU; request memory explicitly.

Slurm configuration can change. For an unusually large or long job, recheck
the association and the two permitted partitions without launching a job:

```bash
sacctmgr -P show assoc where user="$USER" \
  format=User,Account,Partition,QOS,DefaultQOS,GrpTRES,MaxTRESPJ,MaxJobs,MaxSubmit,MaxWall
scontrol show partition mit_normal -o
scontrol show partition mit_quicktest -o
scontrol show config | rg '^(AccountingStorageEnforce|DefMemPerCPU|MaxArraySize)'
sbatch --test-only -p mit_normal -N 1 -c 1 --mem=1G -t 00:01:00 --wrap=true
```

Read `MaxTime` out of that output rather than trusting the figures above; the
caps are the reason a partition gets chosen, so a stale cap in this file is
worse than no figure at all.

## profiling.sh / compare-indexers

Same pattern: `srun`/`sbatch` wrapper, compute-node cwd = git tree, heavy paths under `$SCRATCH`:

```bash
export EREPORT_BIN_DIR=$SCRATCH/bin
export DATA_DIR=$SCRATCH/data/ecrawl-synth
export REPORT_DIR=$SCRATCH/report
# compare-indexers: --work $SCRATCH/work  and  PREFIX=$SCRATCH/prefix
# cargo builds go on node-local disk, not $SCRATCH: a cargo build of XDU with
# its target dir on the shared filesystem stalled for over an hour.
export CARGO_TARGET_DIR=/scratch/$USER/cargo-$SLURM_JOB_ID
```

`init.sh` builds each tool once and then leaves it alone (version stamps in `$PREFIX/var/lib/indexer-compare/`), so calling it before every run costs nothing. `FORCE_REINSTALL=1` rebuilds anyway.

## Re-rendering charts from an existing results dir

Use `benchmark.sh --charts <results-dir>`, not `plot_results.py` directly. It
rereads the CSVs already in the results dir and rewrites `SUMMARY_TABLE.txt`,
`FAILURES.txt` and `charts/`; nothing is crawled, indexed or queried, so it
finishes in seconds and needs neither the benchmark tree nor the external
tools. It also rewrites the summary alongside the figures on purpose — the two
are read side by side, and a fresh figure beside a stale table is worse than
either alone.

```bash
srun -p mit_quicktest -N 1 -c 4 -t 10:00 --mem=8G bash -lc '
set -euo pipefail
export PREFIX=$HOME/orcd/scratch/ereport-automated-testing/prefix
cd /home/erbmi1/git/ereport/scripts/compare-indexers
./benchmark.sh --charts results
'
```

`PREFIX` is the one thing that must be set: it defaults to
`~/.local/indexer-compare`, while the chart venv `resolve_chart_python` needs is
under scratch. The action also points `MPLCONFIGDIR` at the results dir, which
keeps matplotlib's font cache off home NFS.

Call `plot_results.py` directly only for what the action cannot do — overlaying
several result sets (`plot_results.py A B --labels before,after`) or writing
figures somewhere other than `<results>/charts`. Then set
`INDEXER_COMPARE_PREFIX` and use `$PREFIX/chartvenv/bin/python`, write output
under `$SCRATCH/results/`, and keep repeated renders inside one allocation
rather than one job per render.

## Do not

- Run `make -j`, `./scripts/test/test.sh`, profilers, or `plot_results.py` on `orcd-login*`
- Submit to any partition other than `mit_quicktest` and `mit_normal`
- Pass `-A` / `--account` to `srun` or `sbatch`
- Send work that cannot finish in 15 minutes to `mit_quicktest`
- Ask the user for permission before submitting an otherwise appropriate Slurm job
- Rewrite harness scripts just to relocate scratch
- Put GUFI/XDU indexes or Rust/venv trees under home NFS
