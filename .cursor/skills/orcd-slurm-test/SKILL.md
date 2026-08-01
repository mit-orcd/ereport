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

Any compile, `./test.sh`, `make check`, `scripts/profile/profiling.sh`, or heavy indexer work while the agent shell is on an ORCD login node.

## Layout

```text
~/orcd/scratch/ereport-automated-testing/
  bin/       # suite binaries if deploying outside the git tree
  prefix/    # PREFIX for scripts/compare-indexers/init.sh
  cargo/     # CARGO_HOME
  rustup/    # RUSTUP_HOME
  work/      # --work indexes / TMPDIR for long runs
  data/      # synthetic trees
```

Repo stays on home NFS (`~/git/ereport`). Scripts stay unchanged; override paths with env / flags they already accept.

## Env (export on the compute node before tool installs)

```bash
SCRATCH=~/orcd/scratch/ereport-automated-testing
export PREFIX=$SCRATCH/prefix
export CARGO_HOME=$SCRATCH/cargo
export RUSTUP_HOME=$SCRATCH/rustup
export PATH=$PREFIX/bin:$CARGO_HOME/bin:$PATH
```

After `init.sh` once: `source "$PREFIX/env.sh"` when present.

## Run test.sh (default recipe)

Short smoke / unit harness — use `mit_quicktest` (15 min):

```bash
srun -p mit_quicktest -N 1 -c 8 -t 15:00 bash -lc '
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
./test.sh
rm -rf "$TMPDIR"
'
```

Raise `-t` / switch to `-p mit_normal` (or `mit_preemptable`) for longer profiling or compare-indexers runs. Prefer `sbatch` for multi-hour unattended jobs.

## Picking a partition

Check what is actually free before submitting, rather than defaulting to `mit_normal`:

```bash
sinfo -o '%15P %6t %5D %10l' | grep -E 'mit_(quicktest|testing|normal|preemptable)'
scontrol show partition mit_testing | grep -o 'MaxTime=\S*'
```

- `mit_testing` — check this one. Few nodes, but they are large (224 CPUs, 2 TB) with a 7-day limit, and they are often idle while `mit_normal` queues on priority. Best choice for a tool build or a long indexer run.
- `mit_quicktest` — 15 min hard cap. Environment probes and `./test.sh`, nothing that compiles a dependency tree.
- `mit_normal` — 12 h, 50 nodes, but expect to wait behind `(Priority)`.
- `mit_preemptable` — 2 days and hundreds of nodes; fine for restartable work, not for a long build you do not want killed.

Ask for memory explicitly. The default is 1 GB per CPU, and a `-c 16` job therefore gets 16 GB, which is not enough to build XDU (it pins `lto = true` and `codegen-units = 1`, so `arrow` and `parquet` are optimised whole-program in one process). That allocation was OOM-killed 12 times; `--mem=64G` is a safe floor for a Rust build.

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

## Do not

- Run `make -j`, `./test.sh`, or profilers on `orcd-login*`
- Rewrite harness scripts just to relocate scratch
- Put GUFI/XDU indexes or Rust/venv trees under home NFS
