#!/usr/bin/env bash
#
# profile/ewalk-strategy-bench.sh
#
# SPDX-License-Identifier: MIT
#
# Cold-cache A/B/A matrix over ewalkbench's traversal and stat-order strategies,
# to settle whether wide inode spread or per-directory inode locality wins.
#
# ewalkbench only walks and stats (no capture writer, no env knobs), so strategy
# is the sole variable. This script supplies the protocol around it, and the
# protocol is the part that has burned us before:
#
#   * Cold every rep. `sync; echo 3 > /proc/sys/vm/drop_caches` runs before
#     EVERY run. Warm cache hides the entire effect under test — inode-chunk
#     amplification only exists when the inode has to be read — so this script
#     REFUSES to run the matrix when it cannot drop caches, instead of quietly
#     producing warm numbers that look like a result. EWALK_ALLOW_WARM=1 exists
#     only for testing the plumbing and labels every artifact WARM-INVALID.
#   * Reps interleaved across cells, never grouped. Running all reps of one
#     config before the next produced a convincing but entirely false 18.6%
#     result in this repo; interleaving erased it. The loop here is reps outer,
#     cells inner, and the cell order rotates by rep so no cell is permanently
#     first.
#   * A/A control. `dfs-hash-B` is byte-identical to `dfs-hash`. The gap between
#     them is the noise floor; a difference smaller than that is not a result.
#   * Full sorted rep list per cell next to the median, because overlapping
#     distributions are how the false result above was caught.
#   * Node-local block storage only. A tree on tmpfs (or NFS) has no inode reads
#     to schedule, so it cannot measure this at all; both are refused. Note
#     /tmp is a real device on ORCD compute nodes but tmpfs on the login node.
#   * ewalkbench's `errors` counter is recorded and checked, because a cell that
#     failed to stat part of the tree did less work than the others and would
#     read as a speed-up.
#   * Aging is measured, not assumed. Every run reports `readdir_asc_frac`, the
#     fraction of consecutive same-directory dirents whose inode number goes
#     up, and an aged (shape, age) group whose walk still reads ~1.0 is failed
#     rather than reported as a null. See "Tree age" below.
#   * Loud failure over blank cells. Every required summary key is checked per
#     run; a missing key marks the run failed rather than emitting an empty
#     column that an accounting check would compare against another empty
#     column and call a match.
#   * The timed matrix is never instrumented. perf lives in its own mode and
#     writes its own files; see "perf" below.
#
# Usage:
#   scripts/profile/ewalk-strategy-bench.sh <prepare|calibrate|matrix|perf> [results-dir]
#
#   prepare    build the three tree shapes on node-local storage and exit. No
#              root needed. Slow (millions of files), so it is worth doing in
#              its own job; the generator reuses an existing matching tree.
#   calibrate  one cold `dfs-hash` rep per shape, then print measured cold
#              throughput, the file counts that would land each shape in the
#              30-90 s target, and the projected wall time of the full matrix
#              against BUDGET_SEC. Run this before committing to the matrix:
#              cold runs are far slower than warm ones.
#   matrix     the full interleaved cold matrix (shapes x cells x REPS).
#              Completely uninstrumented, by design.
#   perf       the instrumented pass, run separately and AFTER the matrix.
#              Its wall times are NOT comparable to matrix wall times and can
#              never enter the timing tables: it writes perfstat.tsv and
#              PERF_REPORT.txt, never cells.tsv or SUMMARY_TABLE.txt.
#
# Tree age (AGES, opt-in, default `fresh` = exactly the behaviour that shipped):
#
# On XFS, `getdents64` returns a directory in the order its entries were
# inserted, which on a freshly written tree is also inode-allocation order.
# (Name-hash readdir order is an ext4 property; XFS's hash-sorted leaf blocks
# are a lookup index readdir never consults.) Measured on node-local XFS at 4,
# 16, 64, 256, 1k, 5k, 50k, 500k and 1M entries per directory, under
# sequential, 64-way parallel and randomised-name creation, a fresh tree
# arrives in inode order every time: median inode delta 1. So on a fresh tree
# the `-ino` cells are tautological — `--stat ino` sorts an already-sorted
# list — and that is a real result about fresh trees, not a broken measurement.
#
# A long-lived filesystem is the other real configuration, and there readdir
# order and inode order genuinely differ. AGES adds those trees as SEPARATE
# rows; fresh and aged are never merged, because the severity of the aging is
# a parameter we choose and it sets the size of any effect:
#
#   fresh       the tree as generated. Always available; the default.
#   renameshuf  rename every file in place to a random name. The recommended
#               mechanism: in place, adds no inodes, leaves link counts at 1.
#               It works not by changing the name hash but because re-inserted
#               entries land in directory slots freed by the unlink half of the
#               rename. ~13-26 s per million files at AGE_JOBS=64.
#   churn       delete AGE_CHURN_FRAC of the files at random and recreate the
#               same number, AGE_CHURN_ROUNDS times. The honest model of an
#               aged scratch filesystem (67-75% ascending). Recreated files are
#               empty, so file sizes change; ewalkbench never reads content.
#               ~24-47 s per million files.
#   linkshuf    hard-link every file into a sibling directory in random order,
#               unlink the originals, rename the sibling into place. This is a
#               uniform random permutation, so it is an INSTRUMENT SENSITIVITY
#               CHECK and an UPPER BOUND, not a filesystem state anyone has;
#               the summary labels it as such. ~18-39 s per million files.
#
# Aging is an additive post-pass: the generator is used unmodified and is not
# aware of it. Each (shape, age) is its own tree under
# TREE_BASE/<shape>[.<age>], aged exactly once at prepare time — never between
# reps, never inside a timed region — and stamped with EWALK_AGE.txt so a later
# `prepare` reuses it instead of aging twice. Budget accordingly: `AGES="fresh
# renameshuf"` doubles both the tree build time and the inode footprint.
#
# Why perf is a separate mode: every profiler in this repo notes that timing
# under heavy instrumentation is not representative, and a dwarf call-graph
# `perf record` inflates and distorts wall clock. This benchmark exists to
# compare wall clock across six cells against an A/A noise floor a few percent
# wide, so instrumenting the timed reps would destroy the measurement. Counting
# with `perf stat` perturbs far less, which is why DO_PERF_STAT defaults on
# within this mode while DO_PERF (sampling) defaults off — but neither runs
# during `matrix`, and `DO_PERF=1 … matrix` is a hard error rather than a
# silently instrumented matrix.
#
# What the perf-stat pass is for: on a cold walk most of the time is stall, not
# computation. Wall clock alone cannot tell "this strategy issued fewer I/Os"
# from "this strategy merely burned less CPU". task-clock, cycles, instructions
# and IPC separate them: low IPC with flat wall time is the signature of an
# I/O-bound cell, which is what makes a null wall-clock result interpretable
# rather than merely disappointing.
#
# One caveat the perf summary checks and reports for you: with
# kernel.perf_event_paranoid >= 2 (the default on the ORCD compute nodes) an
# unprivileged user only gets `:u` counters, and a cold walk spends nearly all
# of its cycles inside the kernel. Root — the same privilege the cold protocol
# already needs for drop_caches — or perf_event_paranoid=-1 is what makes these
# counters cover the syscall path actually under test.
#
# Environment:
#   EWALKBENCH=./ewalkbench    binary override (same idea as ECRAWL_BIN).
#   BUILD=0                    1 runs `make ewalkbench` in the repo first.
#   COPY_BIN=1                 copy the binary to node-local storage and run
#                              that, so dropping caches does not re-read it
#                              over NFS before every timed run.
#   THREADS=16                 --threads passed to every cell. 16 is deliberate:
#                              docs/performance.md records the cold protocol on
#                              the root-capable host node9901 as a "16-thread
#                              budget", so matching it keeps these numbers
#                              comparable with the cold compare-indexers results
#                              already in that document. ewalkbench's own
#                              default is 8; this script always passes the flag,
#                              so the binary default never applies here.
#   REPS=5                     reps per cell.
#   SHAPES="many_dirs few_dirs one_dir"
#   AGES="fresh"               tree ages to build and measure, as separate rows:
#                              fresh, renameshuf, churn, linkshuf (see above).
#                              Default `fresh` keeps the shipped behaviour.
#   CELLS="dfs-hash dfs-hash-B dfs-ino bfs-hash bfs-ino bfs-spread"
#                              also available, not default: dfs-hash-full,
#                              dfs-ino-full, bfs-hash-full, bfs-ino-full, which
#                              pass --sort-window all (one window per directory
#                              instead of 1024 names). Those exist so a null at
#                              a megadirectory is attributable: per-batch
#                              sorting can only reach chunk locality while a
#                              directory holds at most ~1024x64 entries, so
#                              without a whole-directory cell `one_dir` cannot
#                              tell "inode order does not help" from "the batch
#                              is 15x too small to express inode order".
#   SORT_WINDOW=<unset>        pass --sort-window to every cell that does not
#                              already name one. Unset means the flag is never
#                              passed and ewalkbench's 1024-name default (what
#                              ecrawl's ECRAWL_STAT_INODE_ORDER does) applies.
#   ROTATE_CELLS=1             rotate cell order by rep index.
#   BASELINE_CELL=dfs-hash     the cell others are compared against.
#   CONTROL_CELL=dfs-hash-B    the A/A cell that sets the noise floor.
#   RUN_TIMEOUT_SEC=900        per-run timeout (0 disables); a hung run
#                              otherwise eats the whole job.
#   TREE_BASE=<auto>           node-local dir holding the trees. Default
#                              /scratch/$USER/ewalk-trees when /scratch is
#                              writable, else /tmp/$USER/ewalk-trees. One tree
#                              per (shape, age): <shape> and <shape>.<age>.
#   KEEP_TREES=1               keep the trees on exit (0 deletes them). They
#                              must persist across the whole run: drop_caches
#                              between reps is the point, regenerating is not.
#   ALLOW_ANY_FSTYPE=0         1 to run on a filesystem this script does not
#                              recognise as node-local block storage.
#   EWALK_ALLOW_WARM=0         1 to run warm for a plumbing dry-run. Output is
#                              labelled WARM-INVALID and is not a result.
#   RESULTS_BASE / LOG_BASE    default ~/orcd/scratch/ereport-automated-testing/
#                              {results,logs}.
#   TARGET_SEC=60              per-run target used by calibrate's sizing advice.
#   BUDGET_SEC=21600           job wall-clock budget calibrate projects against.
#   Shape sizing (~1M files each by default):
#     MANY_DIRS_PARENTS=200000  MANY_DIRS_FILES_EACH=5
#     FEW_DIRS_PARENTS=200      FEW_DIRS_FILES_EACH=5000
#     ONE_DIR_FILES=1000000
#     CREATE_JOBS=0             generator parallelism (0 = auto).
#     DISK_BUDGET_GIB=64        passed through as DISK_BUDGET_BYTES.
#   Aging (all ignored while AGES is just `fresh`):
#     AGE_JOBS=0                aging parallelism (0 = auto from nproc, cap 64).
#     AGE_SEED=1                seed, so an aged tree is reproducible.
#     AGE_CHURN_FRAC=0.5        fraction deleted and recreated per churn round.
#     AGE_CHURN_ROUNDS=2        churn rounds.
#     AGE_TIMEOUT_SEC=3600      per-tree aging timeout (0 disables).
#     AGED_ASC_MAX=0.95         an aged group whose median readdir_asc_frac is
#                               at or above this failed to age; the summary
#                               fails the run rather than reporting the null.
#   perf mode only (all ignored by prepare/calibrate/matrix):
#     DO_PERF_STAT=1           `perf stat` counting pass, one cold rep per cell.
#     DO_PERF=0                `perf record` sampling pass. Off by default: a
#                              dwarf record of a multi-minute 16-thread walk is
#                              large and slow. Setting it with mode=matrix is a
#                              hard error.
#     PERF_FREQ=997            sampling frequency, as in ecrawl-fixtures.sh.
#     PERF_CALLGRAPH=dwarf     use fp if the binary keeps frame pointers
#                              (cheaper and much smaller).
#     PERF_REPS=1              reps per cell in the perf mode.
#     PERF_SHAPES / PERF_AGES / PERF_CELLS default to SHAPES / AGES / CELLS;
#                              narrow them to keep the sampling pass affordable.
#     PERF_EVENTS=<list>       perf stat event list (see the default below).
#
# Output layout (under <results-dir>):
#   env.txt / env.<mode>.txt       host / binary / shape / protocol snapshot.
#                                  A perf pass in the same job shares this
#                                  directory, so each mode also keeps its own
#                                  copy of the snapshot it ran under.
#   tree.<shape>.<age>.log         generator output per (shape, age)
#   age.<shape>.<age>.log          aging post-pass output (aged groups only)
#   trees.tsv                      one row per (shape, age): walk root, recipe,
#                                  aging cost, and readdir order before/after
#   runs/<shape>.<age>.<cell>.repN.txt   ewalkbench stdout (key=value summary)
#   cells.tsv                      one row per timed run, machine-readable
#   SUMMARY_TABLE.txt              the table to read (…WARM-INVALID.txt if warm)
#   perf/perfstat.<shape>.<cell>.repN.csv        raw `perf stat -x,` counters
#   perf/perfstat.<shape>.<cell>.repN.summary.txt  ewalkbench keys from that run
#   perf/perf.<shape>.<cell>.report.txt          perf report --stdio
#   perf/perf.<shape>.<cell>.report.caller.txt   -g graph,0.5,caller
#   perf/perf.<shape>.<cell>.report.bythread.txt --no-children --sort comm,pid
#   perf/perf.<shape>.<cell>.record-stderr.txt   kept so an empty report stays
#                                                diagnosable
#   perfstat.tsv                   one row per perf-stat run (never a timing row)
#   PERF_REPORT.txt                the perf roll-up to read
#
# Root and Slurm: drop_caches needs root, and Slurm jobs here run unprivileged.
# docs/performance.md's cold protocol uses node9901 with root, reached directly.
#
# Unprivileged warm smoke run (the quickest way to exercise the plumbing). It
# is deliberately explicit about TREE_BASE: the default prefers /scratch/$USER,
# which is the usual node-local path on ordinary Slurm compute nodes but does
# NOT exist on node9901, whose writable local path is /tmp (XFS on /dev/md0).
# /tmp is a real device on both, and tmpfs on the login node, where the tmpfs
# gate will (correctly) refuse to run:
#
#   EWALK_ALLOW_WARM=1 REPS=1 THREADS=4 TREE_BASE=/tmp/$USER/ewalk-smoke \
#   RESULTS_BASE=/tmp/$USER/ewalk-smoke/results LOG_BASE=/tmp/$USER/ewalk-smoke/logs \
#   MANY_DIRS_PARENTS=200 MANY_DIRS_FILES_EACH=5 FEW_DIRS_PARENTS=5 \
#   FEW_DIRS_FILES_EACH=200 ONE_DIR_FILES=2000 \
#     scripts/profile/ewalk-strategy-bench.sh matrix
#
# Same line with `perf` instead of `matrix` smoke-tests the perf plumbing and
# shows whether this host exposes hardware counters at all. Adding
# AGES="fresh renameshuf churn" exercises the aging post-pass and the aged rows.
#
# For the tree build under Slurm:
#   sbatch -p mit_preemptable -N1 -c 32 --mem=64G -t 4:00:00 \
#          -o ~/orcd/scratch/ereport-automated-testing/logs/%j.out \
#          scripts/profile/ewalk-strategy-bench.sh prepare
#
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$REPO_ROOT/scripts/lib/common.sh"
GENERATOR="$REPO_ROOT/scripts/fixtures/generate-ecrawl-adversarial-tree.sh"

# ---- args ------------------------------------------------------------------
MODE=${1:-}
case "$MODE" in
  prepare | calibrate | matrix | perf) ;;
  *)
    echo "usage: $0 <prepare|calibrate|matrix|perf> [results-dir]" >&2
    echo "  (mode is required on purpose: 'matrix' is 90 cold runs)" >&2
    exit 2
    ;;
esac

TS=$(date +%Y%m%d-%H%M%S)
JOB=${SLURM_JOB_ID:-manual-$$}
RESULTS_BASE=${RESULTS_BASE:-"$HOME/orcd/scratch/ereport-automated-testing/results"}
LOG_BASE=${LOG_BASE:-"$HOME/orcd/scratch/ereport-automated-testing/logs"}

# ---- config ----------------------------------------------------------------
THREADS=${THREADS:-16}
REPS=${REPS:-5}
SHAPES=${SHAPES:-"many_dirs few_dirs one_dir"}
# Default `fresh` on purpose: aged trees are opt-in, so an existing invocation
# builds and measures exactly the trees it did before.
AGES=${AGES:-"fresh"}
CELLS=${CELLS:-"dfs-hash dfs-hash-B dfs-ino bfs-hash bfs-ino bfs-spread"}
SORT_WINDOW=${SORT_WINDOW:-}
ROTATE_CELLS=${ROTATE_CELLS:-1}
BASELINE_CELL=${BASELINE_CELL:-dfs-hash}
CONTROL_CELL=${CONTROL_CELL:-dfs-hash-B}
RUN_TIMEOUT_SEC=${RUN_TIMEOUT_SEC:-900}
BUILD=${BUILD:-0}
COPY_BIN=${COPY_BIN:-1}
KEEP_TREES=${KEEP_TREES:-1}
ALLOW_ANY_FSTYPE=${ALLOW_ANY_FSTYPE:-0}
EWALK_ALLOW_WARM=${EWALK_ALLOW_WARM:-0}
TARGET_SEC=${TARGET_SEC:-60}
BUDGET_SEC=${BUDGET_SEC:-21600}
PER_RUN_OVERHEAD_SEC=${PER_RUN_OVERHEAD_SEC:-5}

DO_PERF=${DO_PERF:-0}
DO_PERF_STAT=${DO_PERF_STAT:-1}
PERF_FREQ=${PERF_FREQ:-997}
PERF_CALLGRAPH=${PERF_CALLGRAPH:-dwarf}
PERF_REPS=${PERF_REPS:-1}
PERF_SHAPES=${PERF_SHAPES:-$SHAPES}
PERF_AGES=${PERF_AGES:-$AGES}
PERF_CELLS=${PERF_CELLS:-$CELLS}
# Software events first so they still land when the PMU is not exposed (VMs and
# restricted kernels return <not supported> for cycles/instructions).
# The software events (task-clock, context-switches, page-faults, minor-faults,
# major-faults) come back on any kernel; cycles and instructions are the two
# that go missing when the PMU is hidden, and the perf summary reports on their
# availability separately.
PERF_EVENTS=${PERF_EVENTS:-"task-clock,context-switches,page-faults,minor-faults,major-faults,cycles,instructions"}

MANY_DIRS_PARENTS=${MANY_DIRS_PARENTS:-200000}
MANY_DIRS_FILES_EACH=${MANY_DIRS_FILES_EACH:-5}
FEW_DIRS_PARENTS=${FEW_DIRS_PARENTS:-200}
FEW_DIRS_FILES_EACH=${FEW_DIRS_FILES_EACH:-5000}
ONE_DIR_FILES=${ONE_DIR_FILES:-1000000}
CREATE_JOBS=${CREATE_JOBS:-0}
DISK_BUDGET_GIB=${DISK_BUDGET_GIB:-64}

AGE_JOBS=${AGE_JOBS:-0}
AGE_SEED=${AGE_SEED:-1}
AGE_CHURN_FRAC=${AGE_CHURN_FRAC:-0.5}
AGE_CHURN_ROUNDS=${AGE_CHURN_ROUNDS:-2}
AGE_TIMEOUT_SEC=${AGE_TIMEOUT_SEC:-3600}
AGED_ASC_MAX=${AGED_ASC_MAX:-0.95}

# The contract with ewalkbench. Every one of these must appear in every run's
# summary; a missing key fails the run instead of leaving a blank column.
# `stat_sort_window` and `readdir_asc_frac` are in the contract for the same
# reason the rest are: the window decides what an `-ino` cell can possibly do,
# and readdir_asc_frac is the only evidence that an aged tree is actually aged.
REQUIRED_KEYS="entries dirs elapsed_sec entries_per_sec getdents_calls fstatat_calls chunk_reuse_rate median_ino_delta distinct_ino_buckets stat_sort_window readdir_asc_frac"
# `errors` is recorded and checked but not required: a cell that skipped entries
# it could not stat looks fast for the wrong reason, yet the key is not part of
# the plan's contract, so its absence is a warning rather than a failed run.
N_TSV_FIELDS=20

read -ra SHAPE_LIST <<<"$SHAPES"
read -ra AGE_LIST <<<"$AGES"
read -ra CELL_LIST <<<"$CELLS"
if [[ ${#SHAPE_LIST[@]} -eq 0 || ${#CELL_LIST[@]} -eq 0 || ${#AGE_LIST[@]} -eq 0 ]]; then
  echo "ERROR: SHAPES, AGES and CELLS must all be non-empty" >&2
  exit 2
fi
for a in "${AGE_LIST[@]}"; do
  case "$a" in
    fresh | renameshuf | churn | linkshuf) ;;
    *) echo "ERROR: unknown age '$a' (fresh, renameshuf, churn, linkshuf)" >&2; exit 2 ;;
  esac
done
read -ra PERF_SHAPE_LIST <<<"$PERF_SHAPES"
read -ra PERF_AGE_LIST <<<"$PERF_AGES"
read -ra PERF_CELL_LIST <<<"$PERF_CELLS"

# The one thing that must never happen: a timed rep with a profiler attached.
# Refused rather than ignored, because someone who sets DO_PERF=1 believes they
# are getting a profile, and silently dropping it is how a distorted or missing
# result gets published.
if [[ "$MODE" == "matrix" && "$DO_PERF" == "1" ]]; then
  cat >&2 <<EOF
ERROR: DO_PERF=1 with mode=matrix.

perf must never wrap the timed reps: a dwarf call-graph record inflates and
distorts wall clock, and this benchmark compares wall clock across six cells
against an A/A noise floor only a few percent wide.

Run the matrix uninstrumented, then run the instrumented pass separately:
  $0 matrix
  DO_PERF=1 $0 perf
EOF
  exit 2
fi

COLD=1
[[ "$EWALK_ALLOW_WARM" == "1" ]] && COLD=0

# ---- cells -----------------------------------------------------------------
# label -> ewalkbench flags. An unknown label is fatal: a typo in CELLS must not
# silently produce a cell with no runs in it.
#
# The `-full` labels pass --sort-window all, one window per directory instead of
# 1024 names. They come in hash and ino pairs so the window's own cost (reading
# a whole directory before statting any of it) can be told apart from the sort.
cell_args() {
  local args
  case "$1" in
    dfs-hash | dfs-hash-B) args="--order dfs --stat hash" ;;
    dfs-ino)               args="--order dfs --stat ino" ;;
    bfs-hash)              args="--order bfs --stat hash" ;;
    bfs-ino)               args="--order bfs --stat ino" ;;
    bfs-spread)            args="--order bfs --stat spread" ;;
    dfs-hash-full)         args="--order dfs --stat hash --sort-window all" ;;
    dfs-ino-full)          args="--order dfs --stat ino --sort-window all" ;;
    bfs-hash-full)         args="--order bfs --stat hash --sort-window all" ;;
    bfs-ino-full)          args="--order bfs --stat ino --sort-window all" ;;
    *) return 1 ;;
  esac
  # A cell that names its own window keeps it; SORT_WINDOW is the default for
  # the cells that do not, so the two cannot silently disagree.
  if [[ -n "$SORT_WINDOW" && "$args" != *"--sort-window"* ]]; then
    args="$args --sort-window $SORT_WINDOW"
  fi
  echo "$args"
}
for c in "${CELL_LIST[@]}"; do
  cell_args "$c" >/dev/null || { echo "ERROR: unknown cell '$c' (see CELLS in the header)" >&2; exit 2; }
done
case " ${CELL_LIST[*]} " in *" $BASELINE_CELL "*) ;; *)
  echo "ERROR: BASELINE_CELL='$BASELINE_CELL' is not in CELLS" >&2; exit 2 ;;
esac

# ---- storage ---------------------------------------------------------------
if [[ -z "${TREE_BASE:-}" ]]; then
  if [[ -d /scratch && -w /scratch ]]; then TREE_BASE=/scratch/$USER/ewalk-trees
  else TREE_BASE=/tmp/$USER/ewalk-trees
  fi
fi
mkdir -p "$TREE_BASE/tmp" "$TREE_BASE/bin" || { echo "ERROR: cannot create '$TREE_BASE'" >&2; exit 1; }
TREE_BASE=$(cd "$TREE_BASE" && pwd)
export TMPDIR="$TREE_BASE/tmp"
TREE_FSTYPE=$(fs_type "$TREE_BASE")

# tmpfs has no inode reads to schedule and NFS is not the storage under test, so
# either one silently answers a different question than the one being asked.
case "$TREE_FSTYPE" in
  xfs | ext2/ext3 | ext4 | btrfs | f2fs | zfs) ;;
  *)
    echo "ERROR: TREE_BASE '$TREE_BASE' is on '$TREE_FSTYPE', not node-local block storage." >&2
    if [[ "$TREE_FSTYPE" == "tmpfs" || "$TREE_FSTYPE" == "ramfs" ]]; then
      echo "  A tree in RAM has no inode reads to order, so this experiment would measure nothing." >&2
      echo "  /tmp is tmpfs on the ORCD login node and a real device on compute nodes: run this on a compute node." >&2
    else
      echo "  Point TREE_BASE at node-local disk (e.g. /scratch/\$USER/... or /tmp/\$USER/... on a compute node)." >&2
    fi
    echo "  Set ALLOW_ANY_FSTYPE=1 only if you know this filesystem is local block storage." >&2
    [[ "$ALLOW_ANY_FSTYPE" == "1" ]] || exit 1
    echo "  ALLOW_ANY_FSTYPE=1: continuing anyway." >&2
    ;;
esac

# ---- (shape, age) groups ---------------------------------------------------
# A group is one tree. Fresh keeps the original path so an existing TREE_BASE is
# reused as-is; each aged variant is a tree of its own, because an aged tree is
# a different tree and its rows must never merge with the fresh ones.
GROUP_LIST=()
declare -A GROUP_SHAPE=() GROUP_AGE=() GROUP_TREE_ROOT=() GROUP_WALK_ROOT=() GROUP_EXPECT_FILES=()
for s in "${SHAPE_LIST[@]}"; do
  case "$s" in
    many_dirs | few_dirs | one_dir) ;;
    *) echo "ERROR: unknown shape '$s' (many_dirs, few_dirs, one_dir)" >&2; exit 2 ;;
  esac
  for a in "${AGE_LIST[@]}"; do
    g="$s|$a"
    GROUP_LIST+=("$g")
    GROUP_SHAPE[$g]=$s
    GROUP_AGE[$g]=$a
    if [[ "$a" == "fresh" ]]; then
      GROUP_TREE_ROOT[$g]="$TREE_BASE/$s"
    else
      GROUP_TREE_ROOT[$g]="$TREE_BASE/$s.$a"
    fi
    case "$s" in
      many_dirs)
        GROUP_WALK_ROOT[$g]="${GROUP_TREE_ROOT[$g]}/wide_shallow"
        GROUP_EXPECT_FILES[$g]=$((MANY_DIRS_PARENTS * MANY_DIRS_FILES_EACH))
        ;;
      few_dirs)
        GROUP_WALK_ROOT[$g]="${GROUP_TREE_ROOT[$g]}/wide_shallow"
        GROUP_EXPECT_FILES[$g]=$((FEW_DIRS_PARENTS * FEW_DIRS_FILES_EACH))
        ;;
      one_dir)
        GROUP_WALK_ROOT[$g]="${GROUP_TREE_ROOT[$g]}/single_huge_dir"
        GROUP_EXPECT_FILES[$g]=$ONE_DIR_FILES
        ;;
    esac
  done
done

# Groups for the perf pass: the same trees, filtered by PERF_SHAPES / PERF_AGES.
PERF_GROUP_LIST=()
for g in "${GROUP_LIST[@]}"; do
  case " ${PERF_SHAPE_LIST[*]} " in *" ${GROUP_SHAPE[$g]} "*) ;; *) continue ;; esac
  case " ${PERF_AGE_LIST[*]} " in *" ${GROUP_AGE[$g]} "*) ;; *) continue ;; esac
  PERF_GROUP_LIST+=("$g")
done

# ---- cold-cache gate -------------------------------------------------------
DROP_PATH=/proc/sys/vm/drop_caches

# Probes by actually dropping: -w can be true in a container where the write
# still fails, and a silent failure here turns the whole matrix warm.
cold_probe() {
  [[ -e "$DROP_PATH" ]] || { echo "  $DROP_PATH does not exist on this host" >&2; return 1; }
  [[ -w "$DROP_PATH" ]] || { echo "  $DROP_PATH is not writable (uid=$(id -u); needs root)" >&2; return 1; }
  sync
  echo 3 >"$DROP_PATH" 2>/dev/null || { echo "  writing to $DROP_PATH failed" >&2; return 1; }
  return 0
}

require_cold() {
  if [[ "$COLD" == "1" ]]; then
    echo "==> cold-cache check: dropping caches once as a probe"
    if cold_probe; then
      echo "    OK: caches can be dropped before every rep"
      return 0
    fi
    cat >&2 <<EOF
ERROR: cannot drop the page cache, so this run would be warm.

Warm cache is not a degraded version of this measurement, it is a different
measurement: inode-chunk amplification only exists when the inode has to be
read from disk, so a warm matrix hides the entire effect under test and would
report a null result no matter which strategy is better.

Fix one of these:
  * run on the root-capable host used by docs/performance.md's cold protocol
    (node9901, reached directly rather than through Slurm), or
  * run as root / with CAP_SYS_ADMIN on any node-local-disk host.

To exercise this script's plumbing without a valid result:
  EWALK_ALLOW_WARM=1 $0 $MODE
which labels every artifact WARM-INVALID.
EOF
    exit 1
  fi
  cat <<EOF
==> !!! EWALK_ALLOW_WARM=1: caches are NOT dropped. !!!
    These numbers are WARM and INVALID for the question this benchmark asks.
    They only prove the harness runs. Artifacts are labelled WARM-INVALID.
EOF
}

g_drop_failures=0
drop_caches_before_rep() {
  [[ "$COLD" == "1" ]] || return 0
  sync
  if ! echo 3 >"$DROP_PATH" 2>/dev/null; then
    g_drop_failures=$((g_drop_failures + 1))
    echo "    ERROR: drop_caches failed mid-run; this rep is warm and invalid" >&2
    return 1
  fi
  return 0
}

# Gate before anything is created, so a refused run leaves no half-built results
# directory in the shared results/ tree.
case "$MODE" in calibrate | matrix | perf) require_cold ;; esac

# ---- results dir and log ---------------------------------------------------
RUN_TAG="ewalk-strategy-$JOB"
[[ "$COLD" == "1" ]] || RUN_TAG="$RUN_TAG-WARM-INVALID"
RESULTS_DIR=${2:-"$RESULTS_BASE/$RUN_TAG"}
mkdir -p "$RESULTS_DIR/runs" || { echo "ERROR: cannot create results dir '$RESULTS_DIR'" >&2; exit 1; }
RESULTS_DIR=$(cd "$RESULTS_DIR" && pwd)
CELLS_TSV="$RESULTS_DIR/cells.tsv"

LOG_FILE="$LOG_BASE/$RUN_TAG.log"
if mkdir -p "$LOG_BASE" 2>/dev/null && : >>"$LOG_FILE" 2>/dev/null; then
  exec > >(tee -a "$LOG_FILE") 2>&1
else
  echo "WARNING: cannot write '$LOG_FILE'; console output is not archived under logs/" >&2
fi

# ---- binary ----------------------------------------------------------------
if [[ "$BUILD" == "1" ]]; then
  echo "==> make ewalkbench"
  make -C "$REPO_ROOT" ewalkbench >"$RESULTS_DIR/build.log" 2>&1 || {
    echo "ERROR: 'make ewalkbench' failed:" >&2; tail -30 "$RESULTS_DIR/build.log" >&2; exit 1; }
fi
if [[ -z "${EWALKBENCH:-}" && -f "$REPO_ROOT/ewalkbench" && -x "$REPO_ROOT/ewalkbench" ]]; then
  EWALKBENCH="$REPO_ROOT/ewalkbench"
fi
BIN=$(find_bin ewalkbench EWALKBENCH) || exit 1
if [[ "$COPY_BIN" == "1" ]]; then
  if cp -f "$BIN" "$TREE_BASE/bin/ewalkbench" 2>/dev/null; then
    BIN="$TREE_BASE/bin/ewalkbench"
  else
    echo "WARNING: could not copy the binary to '$TREE_BASE/bin'; running it from '$BIN'" >&2
  fi
fi

# ---- trees -----------------------------------------------------------------
TIMEOUT_BIN=""
command -v timeout >/dev/null 2>&1 && TIMEOUT_BIN=$(command -v timeout)
TREES_TSV=""

# The generator is used unmodified, driven by env vars only. Everything not part
# of the shape under test is switched off: those fixtures are not walked (each
# shape points at one subtree) and would only cost generation time and inodes.
gen_shape() { # group
  local g=$1
  local shape=${GROUP_SHAPE[$g]} age=${GROUP_AGE[$g]}
  local root=${GROUP_TREE_ROOT[$g]}
  local log="$RESULTS_DIR/tree.$shape.$age.log"
  local -a genv=(
    "DISK_BUDGET_BYTES=$((DISK_BUDGET_GIB * 1024 * 1024 * 1024))"
    "AUTO_CAP_FLAT=0"
    "CREATE_JOBS=$CREATE_JOBS"
    "DEPTH_CHAIN=0"
    "DEPTH_SLICE_ENABLE=0"
    "EREPORT_BADGE_FIXTURES=0"
    "BADGE_MARGIN_DILUTION_ENABLE=0"
    "BADGE_MARGIN_NEUTRAL_FILES=0"
    "SYNTH_LINKS_ENABLE=0"
    "SYNTH_REAL_LARGE_ENABLE=0"
    "SYNTH_RANDOM_UID_ENABLE=0"
    "SPARSE_FILE_MIB=0"
  )
  case "$shape" in
    many_dirs) genv+=("FLAT_FILES=0" "WIDE_PARENTS=$MANY_DIRS_PARENTS" "WIDE_FILES_EACH=$MANY_DIRS_FILES_EACH") ;;
    few_dirs)  genv+=("FLAT_FILES=0" "WIDE_PARENTS=$FEW_DIRS_PARENTS"  "WIDE_FILES_EACH=$FEW_DIRS_FILES_EACH") ;;
    one_dir)   genv+=("WIDE_PARENTS=0" "FLAT_FILES=$ONE_DIR_FILES" "SYNTH_FLAT_SHARD_CAP=0") ;;
  esac

  echo "==> tree '$shape' age=$age -> $root  (expect ~${GROUP_EXPECT_FILES[$g]} files)"
  local t0=$SECONDS
  if ! env "${genv[@]}" "$GENERATOR" "$root" >"$log" 2>&1; then
    echo "ERROR: generating '$shape/$age' failed; tail of $log:" >&2
    tail -30 "$log" >&2
    return 1
  fi
  echo "    generator finished in $((SECONDS - t0))s (reuses a matching existing tree)"
}

# ---- aging post-pass -------------------------------------------------------
# The recipes are written out as a python3 program rather than inlined as shell
# because they are per-file operations on millions of files: one `mv` process
# per rename would dominate the cost of the thing being prepared. python3 is
# already required by the generator's bulk-create path, so this adds no
# dependency. os.rename / os.link / os.unlink release the GIL, so the thread
# pool really does overlap.
AGE_PY=""
write_age_py() {
  [[ -n "$AGE_PY" && -f "$AGE_PY" ]] && return 0
  AGE_PY="$RESULTS_DIR/age_tree.py"
  cat >"$AGE_PY" <<'AGEPY' || { echo "ERROR: cannot write '$AGE_PY'" >&2; return 1; }
"""Make a generated tree stop arriving in inode order, and measure that it did.

usage: age_tree.py <walk-root> <recipe> <jobs> <seed> <churn-frac> <churn-rounds>

Prints a key=value summary on stdout (the caller keeps it as the tree's stamp)
and progress on stderr. Exits non-zero on anything that would leave the tree
different from what was asked for, including a change in file count.
"""

import os
import random
import sys
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

ROOT = sys.argv[1]
RECIPE = sys.argv[2]
JOBS = max(1, int(sys.argv[3]))
SEED = int(sys.argv[4])
CHURN_FRAC = float(sys.argv[5])
CHURN_ROUNDS = int(sys.argv[6])


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def list_dirs(top):
    out, stack = [], [top]
    while stack:
        d = stack.pop()
        out.append(d)
        with os.scandir(d) as it:
            for e in it:
                if e.is_dir(follow_symlinks=False):
                    stack.append(e.path)
    return out


def files_of(d):
    with os.scandir(d) as it:
        return [e.name for e in it if not e.is_dir(follow_symlinks=False)]


def subdirs_of(d):
    with os.scandir(d) as it:
        return [e.name for e in it if e.is_dir(follow_symlinks=False)]


def order_stats(dirs):
    """Consecutive same-directory entries, in readdir order, whose inode goes up.

    DirEntry.inode() is d_ino straight from getdents64, so this costs a
    directory read and no stat calls. It is the same quantity ewalkbench
    reports as readdir_asc_frac, measured here before and after so a failed
    aging pass is visible at prepare time rather than as a null in the matrix.
    """
    pairs = asc = nfiles = 0
    for d in dirs:
        prev = None
        with os.scandir(d) as it:
            for e in it:
                if e.is_dir(follow_symlinks=False):
                    continue
                ino = e.inode()
                nfiles += 1
                if prev is not None:
                    pairs += 1
                    if ino > prev:
                        asc += 1
                prev = ino
    return pairs, asc, nfiles


def pmap(fn, items, jw):
    if jw <= 1 or len(items) < 2048:
        for it in items:
            fn(it)
        return
    cs = (len(items) + jw - 1) // jw
    chunks = [items[i:i + cs] for i in range(0, len(items), cs)]

    def run(chunk):
        for it in chunk:
            fn(it)

    with ThreadPoolExecutor(max_workers=len(chunks)) as ex:
        list(ex.map(run, chunks))


def pmap_collect(fn, items, jw):
    if jw <= 1 or len(items) < 64:
        return [fn(x) for x in items]
    cs = (len(items) + jw - 1) // jw
    chunks = [items[i:i + cs] for i in range(0, len(items), cs)]
    with ThreadPoolExecutor(max_workers=len(chunks)) as ex:
        return [r for part in ex.map(lambda c: [fn(x) for x in c], chunks) for r in part]


def fresh_names(rnd, n, prefix, taken):
    """n names that collide with nothing, so no rename can clobber a file."""
    for _ in range(8):
        names = ["%s%016x" % (prefix, rnd.getrandbits(64)) for _ in range(n)]
        if len(set(names)) == n and not (set(names) & taken):
            return names
    raise RuntimeError("could not generate %d unique names with prefix %s" % (n, prefix))


def renameshuf(d, rnd, jw):
    """Rename every file in place to a random name.

    In place, adds no inodes, leaves link counts at 1. It is not a name-hash
    effect: XFS readdir order follows directory data-block offset, and this
    works because the re-inserted entry lands in whatever slot the unlink half
    of the rename freed.
    """
    names = files_of(d)
    if not names:
        return 0
    new = fresh_names(rnd, len(names), "s", set(names))
    pairs = list(zip(names, new))
    rnd.shuffle(pairs)
    pmap(lambda p: os.rename(os.path.join(d, p[0]), os.path.join(d, p[1])), pairs, jw)
    return len(pairs)


def churn(d, rnd, jw):
    """Delete a fraction at random and recreate the same number, R times.

    The honest model of a long-lived filesystem: inode numbers get reused out
    of directory-slot order. Recreated files are empty, so this changes file
    sizes; nothing here reads file contents.
    """
    touched = 0
    for r in range(CHURN_ROUNDS):
        names = files_of(d)
        if not names:
            return touched
        k = int(len(names) * CHURN_FRAC)
        if k <= 0:
            k = 1
        victims = rnd.sample(names, k)
        pmap(lambda nm: os.unlink(os.path.join(d, nm)), victims, jw)
        made = fresh_names(rnd, k, "c%d-" % r, set(names) - set(victims))

        def mk(nm):
            os.close(os.open(os.path.join(d, nm), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))

        pmap(mk, made, jw)
        touched += k
    return touched


def linkshuf(d, rnd, jw):
    """Hard-link every file into a sibling directory in random order.

    A uniform random permutation of readdir order against inode order: an
    upper bound and an instrument check, not a filesystem state anyone has.
    """
    names = files_of(d)
    if not names:
        return 0
    subs = subdirs_of(d)
    tmp = d + ".ewalkshuf"
    if os.path.lexists(tmp):
        raise RuntimeError("linkshuf: %s already exists from an interrupted run" % tmp)
    os.mkdir(tmp)
    shuffled = list(names)
    rnd.shuffle(shuffled)
    width = max(9, len(str(len(shuffled))))
    slot = {nm: i for i, nm in enumerate(shuffled)}
    pmap(lambda nm: os.link(os.path.join(d, nm),
                            os.path.join(tmp, "s%0*d" % (width, slot[nm])),
                            follow_symlinks=False), shuffled, jw)
    for sd in subs:
        os.rename(os.path.join(d, sd), os.path.join(tmp, sd))
    pmap(lambda nm: os.unlink(os.path.join(d, nm)), shuffled, jw)
    os.rmdir(d)
    os.rename(tmp, d)
    return len(shuffled)


RECIPES = {"renameshuf": renameshuf, "churn": churn, "linkshuf": linkshuf}


def main():
    if RECIPE not in RECIPES:
        log("unknown recipe %s" % RECIPE)
        return 2
    fn = RECIPES[RECIPE]

    t = time.time()
    dirs = list_dirs(ROOT)
    pairs_b, asc_b, files_b = order_stats(dirs)
    log("scanned %d dirs / %d files in %.1fs" % (len(dirs), files_b, time.time() - t))

    # Deepest level first, with a barrier between levels. linkshuf renames a
    # directory out of the way and back, so a parent must never be in flight
    # while one of its children is; ordering by depth makes that structural
    # rather than a thing to remember.
    levels = {}
    for d in dirs:
        levels.setdefault(d.count(os.sep), []).append(d)

    t0 = time.time()
    touched = 0
    for dep in sorted(levels, reverse=True):
        lv = sorted(levels[dep])
        if JOBS > 1 and len(lv) >= JOBS:
            # Many directories: one per worker, each aged serially.
            touched += sum(pmap_collect(
                lambda p: fn(p, random.Random(SEED ^ zlib.crc32(p.encode())), 1), lv, JOBS))
        else:
            # Few directories (a megadirectory is one): parallelise inside them.
            for p in lv:
                touched += fn(p, random.Random(SEED ^ zlib.crc32(p.encode())), JOBS)
    age_sec = time.time() - t0

    pairs_a, asc_a, files_a = order_stats(dirs)
    if files_a != files_b:
        log("ERROR: aging changed the file count: %d -> %d" % (files_b, files_a))
        return 1

    out = [
        ("age_recipe", RECIPE),
        ("age_seed", SEED),
        ("age_jobs", JOBS),
        ("age_sec", "%.1f" % age_sec),
        ("age_sec_per_million", "%.1f" % (age_sec * 1e6 / files_a) if files_a else "-"),
        ("age_dirs", len(dirs)),
        ("age_files", files_a),
        ("age_files_touched", touched),
        ("asc_pairs", pairs_a),
        ("asc_frac_before", "%.4f" % (asc_b / pairs_b if pairs_b else 0.0)),
        ("asc_frac_after", "%.4f" % (asc_a / pairs_a if pairs_a else 0.0)),
    ]
    if RECIPE == "churn":
        out += [("churn_frac", CHURN_FRAC), ("churn_rounds", CHURN_ROUNDS)]
    for k, v in out:
        print("%s=%s" % (k, v))
    return 0


if __name__ == "__main__":
    sys.exit(main())
AGEPY
  return 0
}

# Additive: the generator builds an ordinary tree and knows nothing about this.
# Aging runs exactly once per tree, here at prepare time — never between reps
# and never inside a timed region — and stamps the tree root (outside the walk
# root, so it cannot change `entries`) with what it did and what it measured.
#
# The measurement is the point. `renameshuf` and `churn` work by re-inserting
# entries into directory slots freed by an unlink, not by changing a name hash,
# so "did this tree actually stop arriving in inode order" is an empirical
# question the pass answers with a before/after ascending-step fraction.
age_tree() { # group
  local g=$1
  local shape=${GROUP_SHAPE[$g]} age=${GROUP_AGE[$g]}
  local tree=${GROUP_TREE_ROOT[$g]} walk=${GROUP_WALK_ROOT[$g]}
  local stamp="$tree/EWALK_AGE.txt"
  local log="$RESULTS_DIR/age.$shape.$age.log"
  local out="$RESULTS_DIR/age.$shape.$age.keys"
  local jobs=$AGE_JOBS rc

  [[ "$age" == "fresh" ]] && return 0

  if [[ -f "$stamp" ]]; then
    local prev_recipe prev_seed
    prev_recipe=$(sed -n 's/^age_recipe=//p' "$stamp" | head -n1)
    prev_seed=$(sed -n 's/^age_seed=//p' "$stamp" | head -n1)
    if [[ "$prev_recipe" == "$age" && "$prev_seed" == "$AGE_SEED" ]]; then
      echo "    already aged: $(sed -n 's/^asc_frac_before=/asc /p' "$stamp" | head -n1)-> $(sed -n 's/^asc_frac_after=//p' "$stamp" | head -n1) (stamp $stamp)"
      return 0
    fi
    echo "ERROR: '$tree' was aged as '$prev_recipe' seed '$prev_seed', not '$age' seed '$AGE_SEED'." >&2
    echo "  Aging is not repeatable over itself: rm -rf '$tree' and prepare again." >&2
    return 1
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: aging needs python3 on PATH (renaming a million files one 'mv' at a time is not viable)" >&2
    return 1
  fi
  if [[ "$jobs" -le 0 ]]; then
    jobs=$(nproc 2>/dev/null || echo 8)
    [[ "$jobs" -gt 64 ]] && jobs=64
  fi

  echo "==> aging '$shape' with '$age' (jobs=$jobs seed=$AGE_SEED) -> $walk"
  local t0=$SECONDS
  write_age_py || return 1
  if [[ -n "$TIMEOUT_BIN" && "$AGE_TIMEOUT_SEC" -gt 0 ]]; then
    "$TIMEOUT_BIN" "$AGE_TIMEOUT_SEC" python3 "$AGE_PY" \
      "$walk" "$age" "$jobs" "$AGE_SEED" "$AGE_CHURN_FRAC" "$AGE_CHURN_ROUNDS" >"$out" 2>"$log"
  else
    python3 "$AGE_PY" \
      "$walk" "$age" "$jobs" "$AGE_SEED" "$AGE_CHURN_FRAC" "$AGE_CHURN_ROUNDS" >"$out" 2>"$log"
  fi
  rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "ERROR: aging '$shape/$age' failed (rc=$rc); tail of $log:" >&2
    tail -20 "$log" >&2
    echo "  The tree is now in an unknown state: rm -rf '$tree' before retrying." >&2
    return 1
  fi

  { cat "$out"; echo "age_wall_sec=$((SECONDS - t0))"; echo "age_stamped=$(date -Is)"; } >"$stamp" ||
    { echo "ERROR: cannot write the aging stamp '$stamp'" >&2; return 1; }
  echo "    aged in $((SECONDS - t0))s: readdir ascending-step fraction $(sed -n 's/^asc_frac_before=//p' "$stamp") -> $(sed -n 's/^asc_frac_after=//p' "$stamp")"
  return 0
}

# A walk root that does not exist, or is empty, would produce entries=0 for
# every cell and a matrix of identical numbers that looks like a null result.
check_walk_root() { # group
  local g=$1
  local shape=${GROUP_SHAPE[$g]} age=${GROUP_AGE[$g]}
  local target=${GROUP_WALK_ROOT[$g]}
  if [[ ! -d "$target" ]]; then
    echo "ERROR: '$shape/$age' walk root '$target' is missing (run '$0 prepare')" >&2
    return 1
  fi
  if [[ -z $(find "$target" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null) ]]; then
    echo "ERROR: '$shape/$age' walk root '$target' is empty" >&2
    return 1
  fi
  local fst
  fst=$(fs_type "$target")
  if [[ "$fst" != "$TREE_FSTYPE" ]]; then
    echo "WARNING: '$target' is on '$fst' but TREE_BASE is '$TREE_FSTYPE'" >&2
  fi
  return 0
}

# One row per tree, so `prepare` on its own already shows whether aging took.
write_trees_tsv() {
  local g stamp
  TREES_TSV="$RESULTS_DIR/trees.tsv"
  printf 'shape\tage\twalk_root\texpect_files\tage_sec\tage_files\tasc_frac_before\tasc_frac_after\n' >"$TREES_TSV"
  for g in "${GROUP_LIST[@]}"; do
    stamp="${GROUP_TREE_ROOT[$g]}/EWALK_AGE.txt"
    if [[ "${GROUP_AGE[$g]}" == "fresh" || ! -f "$stamp" ]]; then
      printf '%s\t%s\t%s\t%s\t-\t-\t-\t-\n' \
        "${GROUP_SHAPE[$g]}" "${GROUP_AGE[$g]}" "${GROUP_WALK_ROOT[$g]}" "${GROUP_EXPECT_FILES[$g]}" >>"$TREES_TSV"
      continue
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${GROUP_SHAPE[$g]}" "${GROUP_AGE[$g]}" "${GROUP_WALK_ROOT[$g]}" "${GROUP_EXPECT_FILES[$g]}" \
      "$(sed -n 's/^age_sec=//p' "$stamp" | head -n1)" \
      "$(sed -n 's/^age_files=//p' "$stamp" | head -n1)" \
      "$(sed -n 's/^asc_frac_before=//p' "$stamp" | head -n1)" \
      "$(sed -n 's/^asc_frac_after=//p' "$stamp" | head -n1)" >>"$TREES_TSV"
  done
}

prepare_trees() {
  local g rc=0
  for g in "${GROUP_LIST[@]}"; do
    gen_shape "$g" || { rc=1; continue; }
    check_walk_root "$g" || { rc=1; continue; }
    age_tree "$g" || { rc=1; continue; }
    check_walk_root "$g" || rc=1
  done
  write_trees_tsv
  return $rc
}

# ---- preflight -------------------------------------------------------------
# Runs every (order, stat) combination on a three-file tree and checks that all
# REQUIRED_KEYS come back. This is the guard against the failure mode where the
# binary's key names and this script's grep patterns disagree: every column
# would be empty, and an accounting check comparing empty strings would pass.
preflight() {
  local tiny="$TREE_BASE/_preflight" c out missing k v rc
  rm -rf "$tiny"
  mkdir -p "$tiny/sub" || return 1
  : >"$tiny/a"; : >"$tiny/b"; : >"$tiny/sub/c"

  echo "==> preflight: checking the ewalkbench CLI and summary keys"
  for c in "${CELL_LIST[@]}"; do
    local -a args
    read -ra args <<<"$(cell_args "$c")"
    out="$RESULTS_DIR/preflight.$c.txt"
    "$BIN" "${args[@]}" --threads 2 "$tiny" >"$out" 2>&1
    rc=$?
    if [[ $rc -ne 0 ]]; then
      echo "ERROR: preflight '$c' exited $rc:" >&2
      sed 's/^/    /' "$out" >&2
      return 1
    fi
    missing=""
    for k in $REQUIRED_KEYS; do
      v=$(grep -m1 "^$k=" "$out" | cut -d= -f2)
      [[ -n "$v" ]] || missing+="$k "
    done
    if [[ -n "$missing" ]]; then
      echo "ERROR: preflight '$c' summary is missing keys: $missing" >&2
      echo "  This script and ewalkbench disagree on the summary contract. Fix that" >&2
      echo "  before running the matrix; otherwise those columns come back blank." >&2
      sed 's/^/    /' "$out" >&2
      return 1
    fi
    echo "    OK: $c (${args[*]})"
  done
  rm -rf "$tiny"
  return 0
}

# ---- one run ---------------------------------------------------------------
g_failed_runs=0
g_total_elapsed=0

init_cells_tsv() {
  printf 'shape\tage\tcell\trep\tthreads\tcold\tentries\tdirs\telapsed_sec\tentries_per_sec\tgetdents_calls\tfstatat_calls\tchunk_reuse_rate\tmedian_ino_delta\tdistinct_ino_buckets\tstat_sort_window\treaddir_asc_frac\terrors\trc\tstatus\n' \
    >"$CELLS_TSV"
}

run_one() { # group cell rep
  local g=$1 cell=$2 rep=$3
  local shape=${GROUP_SHAPE[$g]} age=${GROUP_AGE[$g]}
  local root=${GROUP_WALK_ROOT[$g]}
  local out="$RESULTS_DIR/runs/$shape.$age.$cell.rep$rep.txt"
  local -a args
  read -ra args <<<"$(cell_args "$cell")"
  local rc status="ok" missing="" k v errs cold_this=$COLD
  local -a vals=()

  drop_caches_before_rep || { status="DROP_CACHES_FAILED"; cold_this=0; }

  printf '    rep%-2s %-12s %-10s %-13s ' "$rep" "$shape" "$age" "$cell"
  if [[ -n "$TIMEOUT_BIN" && "$RUN_TIMEOUT_SEC" -gt 0 ]]; then
    "$TIMEOUT_BIN" "$RUN_TIMEOUT_SEC" "$BIN" "${args[@]}" --threads "$THREADS" "$root" \
      >"$out" 2>"$out.stderr"
  else
    "$BIN" "${args[@]}" --threads "$THREADS" "$root" >"$out" 2>"$out.stderr"
  fi
  rc=$?

  for k in $REQUIRED_KEYS; do
    v=$(grep -m1 "^$k=" "$out" | cut -d= -f2)
    if [[ -z "$v" ]]; then
      missing+="$k,"
      v="NA"
    fi
    vals+=("$v")
  done
  errs=$(grep -m1 '^errors=' "$out" | cut -d= -f2)
  errs=${errs:-NA}

  # A failed drop_caches is already recorded in status and must not be masked by
  # a later, less serious problem with the same run.
  if [[ "$status" == "ok" ]]; then
    if [[ $rc -eq 124 && -n "$TIMEOUT_BIN" ]]; then
      status="TIMEOUT_${RUN_TIMEOUT_SEC}s"
    elif [[ $rc -ne 0 ]]; then
      status="RC=$rc"
    elif [[ -n "$missing" ]]; then
      status="MISSING_KEYS=${missing%,}"
    fi
  fi

  # vals order follows REQUIRED_KEYS:
  # entries dirs elapsed_sec entries_per_sec getdents_calls fstatat_calls
  # chunk_reuse_rate median_ino_delta distinct_ino_buckets stat_sort_window
  # readdir_asc_frac
  local row
  row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
    "$shape" "$age" "$cell" "$rep" "$THREADS" "$cold_this" \
    "${vals[0]}" "${vals[1]}" "${vals[2]}" "${vals[3]}" "${vals[4]}" "${vals[5]}" \
    "${vals[6]}" "${vals[7]}" "${vals[8]}" "${vals[9]}" "${vals[10]}" "$errs" "$rc" "$status")

  # A short row means a value contained a tab or an extraction silently dropped
  # a field; either way the row cannot be trusted as written.
  local nf
  nf=$(awk -F'\t' '{print NF; exit}' <<<"$row")
  if [[ "$nf" != "$N_TSV_FIELDS" ]]; then
    echo "ERROR: malformed row ($nf fields, expected $N_TSV_FIELDS) for $shape/$age/$cell rep$rep" >&2
    status="MALFORMED_ROW"
    row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\t%s\t%s' \
      "$shape" "$age" "$cell" "$rep" "$THREADS" "$cold_this" "$rc" "$status")
  fi
  printf '%s\n' "$row" >>"$CELLS_TSV"

  if [[ "$status" == "ok" ]]; then
    printf 'elapsed=%ss entries=%s eps=%s\n' "${vals[2]}" "${vals[0]}" "${vals[3]}"
    g_total_elapsed=$(awk -v a="$g_total_elapsed" -v b="${vals[2]}" 'BEGIN{printf "%.3f", a+b}')
  else
    g_failed_runs=$((g_failed_runs + 1))
    printf 'FAILED (%s)\n' "$status"
    [[ -s "$out.stderr" ]] && sed 's/^/        /' "$out.stderr" | head -5
  fi
}

# ---- matrix ----------------------------------------------------------------
rotated_cells() { # rep -> cell order for that rep
  local rep=$1 n=${#CELL_LIST[@]} off=0 i
  [[ "$ROTATE_CELLS" == "1" ]] && off=$(((rep - 1) % n))
  for ((i = 0; i < n; i++)); do
    printf '%s ' "${CELL_LIST[$(((i + off) % n))]}"
  done
}

# Reps outer, cells inner: every cell sees the same slice of machine state, so
# drift over a long job cannot be mistaken for a strategy effect. Grouping reps
# by cell is what produced the false 18.6% result this protocol exists to avoid.
run_matrix() {
  local rep g cell projected
  local runs_per_rep=$((${#GROUP_LIST[@]} * ${#CELL_LIST[@]}))
  echo "==> matrix: ${#SHAPE_LIST[@]} shapes x ${#AGE_LIST[@]} ages x ${#CELL_LIST[@]} cells x $REPS reps = $((runs_per_rep * REPS)) runs, interleaved"
  echo "    uninstrumented by design: no perf, no strace, nothing attached to a timed rep"
  echo "    ages: ${AGE_LIST[*]} (reported as separate rows; aging happened once, at prepare time)"
  for ((rep = 1; rep <= REPS; rep++)); do
    echo "  -- rep $rep/$REPS (cell order: $(rotated_cells "$rep"))"
    for g in "${GROUP_LIST[@]}"; do
      for cell in $(rotated_cells "$rep"); do
        run_one "$g" "$cell" "$rep"
      done
    done
    if [[ "$rep" == "1" && "$REPS" -gt 1 ]]; then
      projected=$(awk -v t="$g_total_elapsed" -v r="$REPS" -v n="$runs_per_rep" -v o="$PER_RUN_OVERHEAD_SEC" \
        'BEGIN{printf "%.0f", (t + n*o) * r}')
      echo "  -- rep 1 took ${g_total_elapsed}s of walk time; full matrix projects ~${projected}s of ${BUDGET_SEC}s budget"
      if [[ "$projected" -gt "$BUDGET_SEC" ]]; then
        echo "  -- WARNING: projection exceeds BUDGET_SEC; the job may be killed mid-matrix." >&2
        echo "     Shrink the shapes (see 'calibrate') or lower REPS." >&2
      fi
    fi
  done
}

# ---- calibration -----------------------------------------------------------
run_calibration() {
  local g shape age total=0 elapsed entries eps
  echo "==> calibrate: one cold '$BASELINE_CELL' rep per (shape, age)"
  for g in "${GROUP_LIST[@]}"; do
    run_one "$g" "$BASELINE_CELL" 0
  done

  echo
  {
    echo "ewalk-strategy-bench — CALIBRATION"
    [[ "$COLD" == "1" ]] || echo "  *** WARM (EWALK_ALLOW_WARM=1): throughput here is not cold throughput ***"
    printf '%-12s %-11s %12s %10s %12s %14s %s\n' shape age entries elapsed_s entries_per_s "files_for_${TARGET_SEC}s" suggestion
    for g in "${GROUP_LIST[@]}"; do
      shape=${GROUP_SHAPE[$g]}
      age=${GROUP_AGE[$g]}
      entries=$(awk -F'\t' -v s="$shape" -v a="$age" '$1==s && $2==a && $20=="ok" {print $7; exit}' "$CELLS_TSV")
      elapsed=$(awk -F'\t' -v s="$shape" -v a="$age" '$1==s && $2==a && $20=="ok" {print $9; exit}' "$CELLS_TSV")
      eps=$(awk -F'\t' -v s="$shape" -v a="$age" '$1==s && $2==a && $20=="ok" {print $10; exit}' "$CELLS_TSV")
      if [[ -z "$entries" || -z "$elapsed" ]]; then
        printf '%-12s %-11s %12s %10s %12s %14s %s\n' "$shape" "$age" - - - - "FAILED — see cells.tsv"
        continue
      fi
      total=$(awk -v a="$total" -v b="$elapsed" 'BEGIN{printf "%.3f", a+b}')
      awk -v shape="$shape" -v age="$age" -v e="$entries" -v el="$elapsed" -v eps="$eps" -v tgt="$TARGET_SEC" \
        -v mp="$MANY_DIRS_PARENTS" -v mf="$MANY_DIRS_FILES_EACH" \
        -v fp="$FEW_DIRS_PARENTS" -v ff="$FEW_DIRS_FILES_EACH" -v od="$ONE_DIR_FILES" 'BEGIN{
          rate = (eps != "" && eps+0 > 0) ? eps+0 : ((el+0>0) ? e/el : 0)
          want = (rate > 0) ? rate * tgt : 0
          sug = "-"
          if (want > 0) {
            if (shape == "many_dirs") sug = sprintf("MANY_DIRS_PARENTS=%d (x%d files)", want/mf, mf)
            else if (shape == "few_dirs") sug = sprintf("FEW_DIRS_PARENTS=%d (x%d files)", want/ff, ff)
            else if (shape == "one_dir") sug = sprintf("ONE_DIR_FILES=%d", want)
          }
          printf "%-12s %-11s %12s %10s %12s %14d %s\n", shape, age, e, el, (eps==""?"-":eps), want, sug
        }'
    done
    echo
    awk -v t="$total" -v reps="$REPS" -v ncell="${#CELL_LIST[@]}" -v o="$PER_RUN_OVERHEAD_SEC" \
      -v nshape="${#GROUP_LIST[@]}" -v budget="$BUDGET_SEC" 'BEGIN{
        per_rep = t * ncell + nshape * ncell * o
        full = per_rep * reps
        printf "projected full matrix: %d trees (shape x age) x %d cells x %d reps ≈ %.0fs (%.2fh) vs BUDGET_SEC=%ds\n",
               nshape, ncell, reps, full, full/3600, budget
        if (full > budget)
          printf "OVER BUDGET by %.0fs — shrink the shapes to the sizes above, or lower REPS.\n", full - budget
        else
          printf "fits with %.0fs (%.2fh) to spare.\n", budget - full, (budget-full)/3600
      }'
    echo
    echo "The suggestion column sizes each shape for a ${TARGET_SEC}s cold run at the"
    echo "throughput just measured, which is the point of calibrating: cold runs are"
    echo "far slower than warm ones, and an unsized matrix can outlive the job."
  } | tee "$RESULTS_DIR/CALIBRATION.txt"
}

# ---- summary ---------------------------------------------------------------
# All three select on (shape, age, cell): a fresh tree and an aged tree are
# different trees, so nothing may pool across ages.
med() { # shape age cell column -> median over ok reps
  awk -F'\t' -v s="$1" -v g="$2" -v c="$3" -v col="$4" \
    '$1==s && $2==g && $3==c && $20=="ok" {print $col}' "$CELLS_TSV" |
    sort -g | awk '{a[NR]=$1} END{ if (NR) printf "%s", a[int((NR+1)/2)] }'
}
nok() { # shape age cell -> ok rep count
  awk -F'\t' -v s="$1" -v g="$2" -v c="$3" \
    'BEGIN{n=0} $1==s && $2==g && $3==c && $20=="ok" {n++} END{print n}' "$CELLS_TSV"
}
reps_sorted() { # shape age cell -> sorted elapsed list
  awk -F'\t' -v s="$1" -v g="$2" -v c="$3" '$1==s && $2==g && $3==c && $20=="ok" {print $9}' "$CELLS_TSV" |
    sort -g | tr '\n' ' '
}
# The aging stamp, for the one line of provenance each aged group prints.
stamp_key() { # group key
  local f="${GROUP_TREE_ROOT[$1]}/EWALK_AGE.txt"
  [[ -f "$f" ]] && sed -n "s/^$2=//p" "$f" | head -n1
}

summarize() {
  local table="$RESULTS_DIR/SUMMARY_TABLE.txt"
  [[ "$COLD" == "1" ]] || table="$RESULTS_DIR/SUMMARY_TABLE.WARM-INVALID.txt"
  local g shape age cell base ctrl aa n checks_failed=0

  {
    echo "ewalk-strategy-bench — SUMMARY"
    echo "  results=$RESULTS_DIR"
    echo "  host=$(hostname) threads=$THREADS reps=$REPS fstype=$TREE_FSTYPE tree_base=$TREE_BASE"
    echo "  ages=${AGE_LIST[*]}  (each (shape, age) is its own tree and its own rows; never merged)"
    [[ -n "$SORT_WINDOW" ]] && echo "  sort_window=$SORT_WINDOW applied to every cell that does not name its own"
    if [[ "$COLD" == "1" ]]; then
      echo "  cold=1 (caches dropped before every rep)"
    else
      echo
      echo "  ############################################################"
      echo "  ##  WARM-INVALID: caches were NOT dropped (EWALK_ALLOW_WARM=1)."
      echo "  ##  These numbers only show the harness runs. They cannot answer"
      echo "  ##  the inode-locality question at all — do not quote them."
      echo "  ############################################################"
      echo
    fi
    echo

    for g in "${GROUP_LIST[@]}"; do
      shape=${GROUP_SHAPE[$g]}
      age=${GROUP_AGE[$g]}
      base=$(med "$shape" "$age" "$BASELINE_CELL" 9)
      ctrl=""
      case " ${CELL_LIST[*]} " in *" $CONTROL_CELL "*) ctrl=$(med "$shape" "$age" "$CONTROL_CELL" 9) ;; esac
      echo "shape=$shape  age=$age  root=${GROUP_WALK_ROOT[$g]}"
      if [[ "$age" == "fresh" ]]; then
        echo "  tree as generated. On XFS a fresh tree arrives in inode order, so an"
        echo "  -ino cell here is expected to sort an already-sorted list: a null is the"
        echo "  predicted result, not a failed measurement."
      else
        printf '  aged: recipe=%s seed=%s cost=%ss (%ss/1M) readdir ascending-step %s -> %s over %s files\n' \
          "$age" "$(stamp_key "$g" age_seed)" "$(stamp_key "$g" age_sec)" \
          "$(stamp_key "$g" age_sec_per_million)" "$(stamp_key "$g" asc_frac_before)" \
          "$(stamp_key "$g" asc_frac_after)" "$(stamp_key "$g" age_files)"
        if [[ "$age" == "linkshuf" ]]; then
          echo "  linkshuf is a uniform random permutation: read it as an INSTRUMENT"
          echo "  SENSITIVITY CHECK and an UPPER BOUND on the effect, not as a filesystem"
          echo "  state any real system reaches. churn is the honest aged-filesystem model."
        fi
      fi
      printf '  %-14s %5s %10s %12s %9s %13s %14s %10s %11s %8s\n' \
        cell reps median_s entries_per_s vs_base chunk_reuse median_ino_delta ino_buckets readdir_asc window
      for cell in "${CELL_LIST[@]}"; do
        n=$(nok "$shape" "$age" "$cell")
        if [[ "$n" -eq 0 ]]; then
          printf '  %-14s %5s %10s %12s %9s %13s %14s %10s %11s %8s   NO VALID REPS\n' \
            "$cell" 0 - - - - - - - -
          checks_failed=1
          continue
        fi
        awk -v cell="$cell" -v n="$n" -v m="$(med "$shape" "$age" "$cell" 9)" \
          -v e="$(med "$shape" "$age" "$cell" 10)" -v b="$base" \
          -v cr="$(med "$shape" "$age" "$cell" 13)" -v d="$(med "$shape" "$age" "$cell" 14)" \
          -v bk="$(med "$shape" "$age" "$cell" 15)" -v sw="$(med "$shape" "$age" "$cell" 16)" \
          -v ra="$(med "$shape" "$age" "$cell" 17)" 'BEGIN{
            gap = (b+0 > 0 && m+0 > 0) ? sprintf("%+.1f%%", 100*(b-m)/b) : "-"
            printf "  %-14s %5s %10s %12s %9s %13s %14s %10s %11s %8s\n", cell, n, m, e, gap, cr, d, bk, ra, sw
          }'
      done
      if [[ -n "$base" && -n "$ctrl" ]]; then
        aa=$(awk -v b="$base" -v c="$ctrl" 'BEGIN{ if (b+0>0) { g=100*(b-c)/b; printf "%.1f", (g<0?-g:g) } else printf "-" }')
        echo "  noise floor ($BASELINE_CELL vs $CONTROL_CELL A/A control): ${aa}%  — treat any vs_base below this as noise"
      else
        echo "  noise floor: UNAVAILABLE (no valid $CONTROL_CELL reps) — no gap can be called real"
        checks_failed=1
      fi
      echo "  sorted elapsed_sec per cell (compare distributions, not just medians):"
      for cell in "${CELL_LIST[@]}"; do
        printf '    %-14s %s\n' "$cell" "$(reps_sorted "$shape" "$age" "$cell")"
      done
      echo
    done

    # ---- validity checks --------------------------------------------------
    echo "checks"
    local expected_runs=$((${#GROUP_LIST[@]} * ${#CELL_LIST[@]} * REPS))
    local ok_runs
    ok_runs=$(awk -F'\t' 'NR>1 && $20=="ok" {n++} END{print n+0}' "$CELLS_TSV")
    if [[ "$ok_runs" -eq "$expected_runs" ]]; then
      echo "  PASS runs: $ok_runs/$expected_runs valid"
    else
      echo "  FAIL runs: only $ok_runs/$expected_runs valid — see the status column in cells.tsv"
      awk -F'\t' 'NR>1 && $20!="ok" {printf "       %s %s %s rep%s -> %s\n", $1, $2, $3, $4, $20}' "$CELLS_TSV"
      checks_failed=1
    fi

    # Every cell walks the same tree, so entries must be identical across cells
    # — WITHIN a (shape, age) group. Fresh and aged are deliberately different
    # trees (churn recreates files, the shuffles rename them), so comparing an
    # entries count across ages would fail for the wrong reason; comparing only
    # within a group keeps exactly the guarantee the check was written for.
    # Values are checked as digits first: comparing two empty (or two "NA")
    # columns would otherwise "match" and hide a broken key contract.
    for g in "${GROUP_LIST[@]}"; do
      shape=${GROUP_SHAPE[$g]}
      age=${GROUP_AGE[$g]}
      local vals count distinct
      vals=$(awk -F'\t' -v s="$shape" -v a="$age" '$1==s && $2==a && $20=="ok" {print $7}' "$CELLS_TSV")
      count=$(grep -c '^[0-9][0-9]*$' <<<"$vals")
      distinct=$(grep '^[0-9][0-9]*$' <<<"$vals" | sort -u | tr '\n' ' ' | sed 's/ *$//')
      if [[ "$count" -eq 0 ]]; then
        echo "  FAIL entries[$shape/$age]: no numeric entries counts at all (key contract broken?)"
        checks_failed=1
      elif [[ "$count" -ne $(wc -l <<<"$vals") ]]; then
        echo "  FAIL entries[$shape/$age]: $count of $(wc -l <<<"$vals") values are numeric"
        checks_failed=1
      elif [[ $(wc -w <<<"$distinct") -ne 1 ]]; then
        echo "  FAIL entries[$shape/$age]: cells disagree on the tree they walked: $distinct"
        checks_failed=1
      else
        echo "  PASS entries[$shape/$age]: $distinct in all $count runs"
      fi
    done

    # readdir_asc_frac is a property of the tree, counted while reading and
    # before any reordering, so every cell in a group must report the same
    # number; a disagreement means the tree changed under the matrix. And an
    # aged group that still reads ~1.0 did not get aged — that has to fail
    # loudly, because it is indistinguishable from a null in the timing table.
    for g in "${GROUP_LIST[@]}"; do
      shape=${GROUP_SHAPE[$g]}
      age=${GROUP_AGE[$g]}
      local asc_vals asc_distinct
      asc_vals=$(awk -F'\t' -v s="$shape" -v a="$age" '$1==s && $2==a && $20=="ok" {print $17}' "$CELLS_TSV")
      asc_distinct=$(grep '^[0-9][0-9]*\.[0-9]*$' <<<"$asc_vals" | sort -u | tr '\n' ' ' | sed 's/ *$//')
      if [[ -z "$asc_distinct" ]]; then
        echo "  FAIL readdir_asc[$shape/$age]: no numeric readdir_asc_frac at all (key contract broken?)"
        checks_failed=1
        continue
      fi
      if [[ $(wc -w <<<"$asc_distinct") -ne 1 ]]; then
        echo "  FAIL readdir_asc[$shape/$age]: cells disagree ($asc_distinct) — the tree changed mid-matrix"
        checks_failed=1
        continue
      fi
      if [[ "$age" == "fresh" ]]; then
        echo "  INFO readdir_asc[$shape/fresh]: $asc_distinct ascending (1.0 = readdir already in inode order, so -ino has nothing to recover)"
      elif awk -v v="$asc_distinct" -v m="$AGED_ASC_MAX" 'BEGIN{exit !(v+0 >= m+0)}'; then
        echo "  FAIL aging[$shape/$age]: readdir_asc_frac is $asc_distinct, at or above AGED_ASC_MAX=$AGED_ASC_MAX."
        echo "       The aging pass did not change readdir order, so the -ino cells here are"
        echo "       still sorting an already-sorted list. This is a broken tree, not a null."
        checks_failed=1
      else
        echo "  PASS aging[$shape/$age]: readdir_asc_frac $asc_distinct (< $AGED_ASC_MAX), readdir order really did stop tracking inode order"
      fi
    done

    # A cell that could not stat part of the tree did less work than the others,
    # which reads as a speed-up unless it is called out here.
    local err_runs err_missing
    err_runs=$(awk -F'\t' 'NR>1 && $20=="ok" && $18 ~ /^[0-9]+$/ && $18+0 > 0 {n++} END{print n+0}' "$CELLS_TSV")
    err_missing=$(awk -F'\t' 'NR>1 && $20=="ok" && $18 !~ /^[0-9]+$/ {n++} END{print n+0}' "$CELLS_TSV")
    if [[ "$err_runs" -gt 0 ]]; then
      echo "  FAIL errors: $err_runs run(s) reported walk errors; those cells did not walk the whole tree"
      awk -F'\t' 'NR>1 && $20=="ok" && $18+0 > 0 {printf "       %s %s %s rep%s -> errors=%s\n", $1, $2, $3, $4, $18}' "$CELLS_TSV"
      checks_failed=1
    elif [[ "$err_missing" -gt 0 ]]; then
      echo "  WARN errors: $err_missing run(s) reported no errors= counter (not part of the required contract)"
    else
      echo "  PASS errors: no walk errors in any run"
    fi

    if [[ "$g_drop_failures" -gt 0 ]]; then
      echo "  FAIL drop_caches: failed before $g_drop_failures rep(s); those rows have cold=0"
      checks_failed=1
    elif [[ "$COLD" == "1" ]]; then
      echo "  PASS drop_caches: succeeded before every rep"
    else
      echo "  FAIL cold: EWALK_ALLOW_WARM=1, nothing here is a cold measurement"
      checks_failed=1
    fi

    echo
    if [[ "$checks_failed" -eq 0 && "$COLD" == "1" ]]; then
      echo "verdict: matrix is structurally sound; read vs_base against the noise floor."
    else
      echo "verdict: DO NOT INTERPRET — a check above failed."
    fi
    echo
    echo "Reading it: chunk_reuse_rate and distinct_ino_buckets say whether each"
    echo "strategy actually did what it claims (a -ino cell that did not raise"
    echo "chunk_reuse_rate has not tested inode locality, whatever its timing did),"
    echo "and vs_base is only a result once it clears the A/A noise floor."
    echo
    echo "Two more things the columns are there to stop you concluding. readdir_asc"
    echo "near 1.0 means readdir already handed the walker inode order, so an -ino"
    echo "cell in that group was sorting a sorted list and its null says nothing"
    echo "about inode ordering as a strategy. And a sorted window of W names out of"
    echo "a directory of D leaves inode gaps of about D/W, which has to stay under"
    echo "the 64-inode chunk: at the default window of 1024 that caps out around"
    echo "65,000 entries per directory, so a null at a larger directory is a"
    echo "statement about the window (see the -full cells) and not about the"
    echo "filesystem."
  } >"$table"

  cat "$table"
  echo
  echo "DONE."
  echo "  Cells (tsv):   $CELLS_TSV"
  [[ -n "$TREES_TSV" && -f "$TREES_TSV" ]] && echo "  Trees (tsv):   $TREES_TSV"
  echo "  Summary table: $table"
  [[ -f "$RESULTS_DIR/CALIBRATION.txt" ]] && echo "  Calibration:   $RESULTS_DIR/CALIBRATION.txt"
  return "$checks_failed"
}

# ---- perf pass -------------------------------------------------------------
# Runs only in mode=perf, and writes only perfstat.tsv / PERF_REPORT.txt / perf/.
# Nothing below appends to $CELLS_TSV and none of it calls run_one, so a perf
# run structurally cannot contribute a row to the timing tables — in perf mode
# cells.tsv is never even created.
PERF_DIR=""
PERFSTAT_TSV=""
N_PERFSTAT_FIELDS=19
g_perf_runs=0
g_perf_failed=0

init_perf_dir() {
  PERF_DIR="$RESULTS_DIR/perf"
  PERFSTAT_TSV="$RESULTS_DIR/perfstat.tsv"
  mkdir -p "$PERF_DIR" || return 1
  printf 'shape\tage\tcell\trep\tthreads\tcold\tentries\telapsed_sec\ttask_clock_msec\tcpu_util\tcycles\tinstructions\tipc\tcontext_switches\tpage_faults\tminor_faults\tmajor_faults\trc\tstatus\n' \
    >"$PERFSTAT_TSV"
}

# One `perf stat -x,` counter out of a CSV file. Never returns an empty string:
# a hardware event the kernel would not give us comes back as `unsup`/`notcnt`,
# an event perf never printed as `absent`. Blank cells are what let a broken
# extraction look like a measured zero, so there are none here either.
perf_csv_get() { # csv event
  awk -F, -v ev="$2" '
    /^#/ { next }
    {
      for (i = 1; i <= NF; i++) {
        if ($i == ev || $i == ev ":u" || $i == ev ":k") {
          v = $1
          gsub(/^[ \t]+|[ \t]+$/, "", v)
          if (v ~ /not supported/) { print "unsup"; found = 1; exit }
          if (v ~ /not counted/)   { print "notcnt"; found = 1; exit }
          if (v == "")             { print "empty"; found = 1; exit }
          gsub(/[^0-9.]/, "", v)
          if (v == "") { print "empty"; found = 1; exit }
          print v; found = 1; exit
        }
      }
    }
    END { if (!found) print "absent" }
  ' "$1"
}

is_num() { [[ "$1" =~ ^[0-9]+(\.[0-9]+)?$ ]]; }

# Which privilege levels the counters actually covered. With
# kernel.perf_event_paranoid >= 2 an unprivileged user only gets `:u` events,
# and a cold walk spends nearly all of its cycles in the kernel (getdents64,
# fstatat, XFS inode reads) — so user-only counters describe a sliver of the
# work and IPC must not be read as the walk's IPC. Detected rather than assumed,
# because the same script runs as root on node9901 where it is not restricted.
perf_event_scope() {
  local f any_u=0 any_all=0
  for f in "$PERF_DIR"/perfstat.*.csv; do
    [[ -e "$f" ]] || continue
    grep -q ',task-clock:u,' "$f" && any_u=1
    grep -q ',task-clock,' "$f" && any_all=1
  done
  if [[ "$any_u" == "1" && "$any_all" == "1" ]]; then echo mixed
  elif [[ "$any_u" == "1" ]]; then echo user-only
  elif [[ "$any_all" == "1" ]]; then echo user+kernel
  else echo unknown
  fi
}

perf_stat_one() { # group cell rep
  local g=$1 cell=$2 rep=$3
  local shape=${GROUP_SHAPE[$g]} age=${GROUP_AGE[$g]}
  local root=${GROUP_WALK_ROOT[$g]}
  local base="$PERF_DIR/perfstat.$shape.$age.$cell.rep$rep"
  local -a args
  read -ra args <<<"$(cell_args "$cell")"
  local rc status="ok" cold_this=$COLD
  local entries elapsed task_clock ctxsw pgf minf majf cycles insns ipc cpu_util

  drop_caches_before_rep || { status="DROP_CACHES_FAILED"; cold_this=0; }

  printf '    perf stat %-12s %-11s %-14s ' "$shape" "$age" "$cell"
  perf stat -x, -e "$PERF_EVENTS" -o "$base.csv" -- \
    "$BIN" "${args[@]}" --threads "$THREADS" "$root" \
    >"$base.summary.txt" 2>"$base.stderr.txt"
  rc=$?

  entries=$(grep -m1 '^entries=' "$base.summary.txt" | cut -d= -f2)
  elapsed=$(grep -m1 '^elapsed_sec=' "$base.summary.txt" | cut -d= -f2)
  task_clock=$(perf_csv_get "$base.csv" task-clock)
  ctxsw=$(perf_csv_get "$base.csv" context-switches)
  pgf=$(perf_csv_get "$base.csv" page-faults)
  minf=$(perf_csv_get "$base.csv" minor-faults)
  majf=$(perf_csv_get "$base.csv" major-faults)
  cycles=$(perf_csv_get "$base.csv" cycles)
  insns=$(perf_csv_get "$base.csv" instructions)

  ipc="unavail"
  if is_num "$cycles" && is_num "$insns" && awk -v c="$cycles" 'BEGIN{exit !(c+0 > 0)}'; then
    ipc=$(awk -v i="$insns" -v c="$cycles" 'BEGIN{printf "%.3f", i/c}')
  fi
  cpu_util="unavail"
  if is_num "$task_clock" && is_num "${elapsed:-}" && awk -v e="${elapsed:-0}" 'BEGIN{exit !(e+0 > 0)}'; then
    cpu_util=$(awk -v t="$task_clock" -v e="$elapsed" 'BEGIN{printf "%.2f", (t/1000.0)/e}')
  fi

  if [[ "$status" == "ok" ]]; then
    if [[ $rc -ne 0 ]]; then
      status="RC=$rc"
    elif [[ -z "$entries" || -z "$elapsed" ]]; then
      status="MISSING_KEYS=entries/elapsed_sec"
    elif [[ "$task_clock" == "absent" ]]; then
      # No software event at all means perf produced nothing usable, which is a
      # broken run rather than a host limitation.
      status="NO_PERF_COUNTERS"
    fi
  fi

  local row
  row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
    "$shape" "$age" "$cell" "$rep" "$THREADS" "$cold_this" \
    "${entries:-NA}" "${elapsed:-NA}" "$task_clock" "$cpu_util" "$cycles" "$insns" "$ipc" \
    "$ctxsw" "$pgf" "$minf" "$majf" "$rc" "$status")
  local nf
  nf=$(awk -F'\t' '{print NF; exit}' <<<"$row")
  if [[ "$nf" != "$N_PERFSTAT_FIELDS" ]]; then
    echo "ERROR: malformed perfstat row ($nf fields, expected $N_PERFSTAT_FIELDS)" >&2
    status="MALFORMED_ROW"
    row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\t%s\t%s' \
      "$shape" "$age" "$cell" "$rep" "$THREADS" "$cold_this" "$rc" "$status")
  fi
  printf '%s\n' "$row" >>"$PERFSTAT_TSV"

  g_perf_runs=$((g_perf_runs + 1))
  if [[ "$status" == "ok" ]]; then
    printf 'elapsed=%ss task_clock=%sms ipc=%s\n' "$elapsed" "$task_clock" "$ipc"
  else
    g_perf_failed=$((g_perf_failed + 1))
    printf 'FAILED (%s)\n' "$status"
    [[ -s "$base.stderr.txt" ]] && sed 's/^/        /' "$base.stderr.txt" | head -5
  fi
}

# Sampling pass, mirroring run_perf() in ecrawl-fixtures.sh: same record shape,
# same three report views, same failure note pointing at perf_event_paranoid.
perf_record_one() { # group cell
  local g=$1 cell=$2
  local shape=${GROUP_SHAPE[$g]} age=${GROUP_AGE[$g]}
  local root=${GROUP_WALK_ROOT[$g]}
  local base="$PERF_DIR/perf.$shape.$age.$cell"
  local data="$base.data"
  local -a args
  read -ra args <<<"$(cell_args "$cell")"
  local cg=$PERF_CALLGRAPH

  drop_caches_before_rep || true
  echo "    perf record $shape/$age/$cell: --call-graph $cg -F $PERF_FREQ (timing not representative)"
  if perf record --call-graph "$cg" -F "$PERF_FREQ" -o "$data" -- \
       "$BIN" "${args[@]}" --threads "$THREADS" "$root" \
       >"$base.summary.txt" 2>"$base.record-stderr.txt"; then
    perf report -i "$data" --stdio 2>/dev/null >"$base.report.txt" ||
      echo "perf report failed (see perf.$shape.$age.$cell.record-stderr.txt)" >"$base.report.txt"
    perf report -i "$data" --stdio -g graph,0.5,caller 2>/dev/null \
      >"$base.report.caller.txt" || true
    # perf's per-thread sort key is `pid` (it shows PID:TID); `tid` is not
    # accepted on all builds. Keep stderr so an empty report stays diagnosable.
    perf report -i "$data" --stdio --no-children --sort comm,pid \
      >"$base.report.bythread.txt" 2>"$base.report.bythread-stderr.txt" || true
  else
    echo "perf record failed; check kernel.perf_event_paranoid and permissions." \
      >"$base.report.txt"
    g_perf_failed=$((g_perf_failed + 1))
  fi
  rm -f "$data"
}

run_perf_pass() {
  local rep g cell
  if [[ "$DO_PERF_STAT" != "1" && "$DO_PERF" != "1" ]]; then
    echo "==> perf: both DO_PERF_STAT and DO_PERF are 0; nothing to do" >&2
    return 1
  fi
  cat <<EOF
==> perf mode. Wall times from this pass are NOT comparable to matrix wall
    times and never enter cells.tsv or SUMMARY_TABLE.txt. Read the counters,
    not the clock.
EOF
  if [[ "$DO_PERF_STAT" == "1" ]]; then
    echo "==> perf stat: ${#PERF_GROUP_LIST[@]} trees (shape x age) x ${#PERF_CELL_LIST[@]} cells x $PERF_REPS reps, events: $PERF_EVENTS"
    for ((rep = 1; rep <= PERF_REPS; rep++)); do
      for g in "${PERF_GROUP_LIST[@]}"; do
        for cell in $(rotated_cells "$rep"); do
          case " ${PERF_CELL_LIST[*]} " in *" $cell "*) ;; *) continue ;; esac
          perf_stat_one "$g" "$cell" "$rep"
        done
      done
    done
  fi
  if [[ "$DO_PERF" == "1" ]]; then
    echo "==> perf record: dwarf call graphs are large; PERF_CALLGRAPH=fp is cheaper"
    for g in "${PERF_GROUP_LIST[@]}"; do
      for cell in "${PERF_CELL_LIST[@]}"; do
        perf_record_one "$g" "$cell"
      done
    done
  fi
  return 0
}

perf_med() { # shape age cell column -> median over ok rows
  awk -F'\t' -v s="$1" -v g="$2" -v c="$3" -v col="$4" \
    '$1==s && $2==g && $3==c && $19=="ok" && $col ~ /^[0-9]+(\.[0-9]+)?$/ {print $col}' "$PERFSTAT_TSV" |
    sort -g | awk '{a[NR]=$1} END{ if (NR) printf "%s", a[int((NR+1)/2)] }'
}
# Non-numeric counters (unsup / notcnt / absent) are carried through to the
# table as themselves rather than collapsed to a blank.
perf_tok() { # shape age cell column -> the token seen, when it was not numeric
  awk -F'\t' -v s="$1" -v g="$2" -v c="$3" -v col="$4" \
    '$1==s && $2==g && $3==c && $19=="ok" {print $col; exit}' "$PERFSTAT_TSV"
}
perf_cell() { # shape age cell column -> median if numeric, else the raw token
  local v
  v=$(perf_med "$1" "$2" "$3" "$4")
  [[ -n "$v" ]] && { printf '%s' "$v"; return; }
  v=$(perf_tok "$1" "$2" "$3" "$4")
  printf '%s' "${v:--}"
}

summarize_perf() {
  local report="$RESULTS_DIR/PERF_REPORT.txt"
  [[ "$COLD" == "1" ]] || report="$RESULTS_DIR/PERF_REPORT.WARM-INVALID.txt"
  local g shape age cell hw_ok hw_rows sw_bad checks_failed=0

  {
    echo "########################################################################"
    echo "# ewalk-strategy-bench — perf pass"
    echo "########################################################################"
    echo
    echo "These runs were instrumented. Their wall times are NOT comparable with"
    echo "the matrix and are not part of any timing comparison; they appear only"
    echo "so a counter can be read per entry. The timing answer lives in"
    echo "SUMMARY_TABLE.txt, produced by a separate uninstrumented run."
    if [[ "$COLD" != "1" ]]; then
      echo
      echo "  ############################################################"
      echo "  ##  WARM-INVALID: caches were NOT dropped (EWALK_ALLOW_WARM=1)."
      echo "  ##  Counters from a warm walk describe a different workload."
      echo "  ############################################################"
    fi
    echo
    cat "$RESULTS_DIR/env.txt"
    echo

    if [[ "$DO_PERF_STAT" == "1" ]]; then
      echo "========================================================================"
      echo "perf stat — counters per cell (median over $PERF_REPS rep(s))"
      echo "========================================================================"
      for g in "${PERF_GROUP_LIST[@]}"; do
        shape=${GROUP_SHAPE[$g]}
        age=${GROUP_AGE[$g]}
        echo "shape=$shape age=$age"
        printf '  %-14s %10s %10s %12s %8s %16s %16s %7s %9s %11s %9s\n' \
          cell entries elapsed_s task_clock_ms cpu_util cycles instructions IPC ctx_sw pagefaults maj_flt
        for cell in "${PERF_CELL_LIST[@]}"; do
          local n
          n=$(awk -F'\t' -v s="$shape" -v a="$age" -v c="$cell" 'BEGIN{n=0} $1==s && $2==a && $3==c && $19=="ok" {n++} END{print n}' "$PERFSTAT_TSV")
          if [[ "$n" -eq 0 ]]; then
            printf '  %-14s %10s %10s %12s %8s %16s %16s %7s %9s %11s %9s   NO VALID RUNS\n' \
              "$cell" - - - - - - - - - -
            checks_failed=1
            continue
          fi
          printf '  %-14s %10s %10s %12s %8s %16s %16s %7s %9s %11s %9s\n' \
            "$cell" \
            "$(perf_cell "$shape" "$age" "$cell" 7)" "$(perf_cell "$shape" "$age" "$cell" 8)" \
            "$(perf_cell "$shape" "$age" "$cell" 9)" "$(perf_cell "$shape" "$age" "$cell" 10)" \
            "$(perf_cell "$shape" "$age" "$cell" 11)" "$(perf_cell "$shape" "$age" "$cell" 12)" \
            "$(perf_cell "$shape" "$age" "$cell" 13)" "$(perf_cell "$shape" "$age" "$cell" 14)" \
            "$(perf_cell "$shape" "$age" "$cell" 15)" "$(perf_cell "$shape" "$age" "$cell" 17)"
        done
        echo
      done

      echo "checks"
      if [[ "$g_perf_failed" -eq 0 ]]; then
        echo "  PASS runs: $g_perf_runs/$g_perf_runs valid"
      else
        echo "  FAIL runs: $g_perf_failed of $g_perf_runs failed"
        awk -F'\t' 'NR>1 && $19!="ok" {printf "       %s %s %s rep%s -> %s\n", $1, $2, $3, $4, $19}' "$PERFSTAT_TSV"
        checks_failed=1
      fi
      hw_rows=$(awk -F'\t' 'NR>1 && $19=="ok" {n++} END{print n+0}' "$PERFSTAT_TSV")
      hw_ok=$(awk -F'\t' 'NR>1 && $19=="ok" && $11 ~ /^[0-9]+$/ && $12 ~ /^[0-9]+$/ {n++} END{print n+0}' "$PERFSTAT_TSV")
      if [[ "$hw_ok" -eq 0 ]]; then
        echo "  WARN hardware counters: unavailable on this host (0/$hw_rows rows have cycles+instructions)."
        echo "       IPC cannot be computed here — the PMU is typically hidden on VMs and"
        echo "       behind kernel.perf_event_paranoid. The software events below"
        echo "       (task-clock, context switches, page faults) are still valid, and"
        echo "       task_clock vs elapsed still separates CPU time from stall."
      elif [[ "$hw_ok" -lt "$hw_rows" ]]; then
        echo "  WARN hardware counters: only $hw_ok/$hw_rows rows have cycles+instructions; IPC is partial."
      else
        echo "  PASS hardware counters: cycles+instructions on all $hw_rows rows; IPC is usable."
      fi
      sw_bad=$(awk -F'\t' 'NR>1 && $19=="ok" && $9 !~ /^[0-9]+(\.[0-9]+)?$/ {n++} END{print n+0}' "$PERFSTAT_TSV")
      if [[ "$sw_bad" -gt 0 ]]; then
        echo "  FAIL software counters: $sw_bad row(s) have no numeric task-clock"
        checks_failed=1
      else
        echo "  PASS software counters: task-clock numeric on every valid row"
      fi
      local scope paranoid
      scope=$(perf_event_scope)
      paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)
      case "$scope" in
        user+kernel)
          echo "  PASS counter scope: user+kernel (perf_event_paranoid=${paranoid:-?}); counters cover the syscall path"
          ;;
        user-only)
          echo "  WARN counter scope: USER-SPACE ONLY (:u events, perf_event_paranoid=${paranoid:-?})."
          echo "       A cold walk spends nearly all its cycles in the kernel — getdents64,"
          echo "       fstatat, XFS inode reads — so these cycles/instructions and this IPC"
          echo "       describe only the sliver of work done in user space, and cpu_util is"
          echo "       user CPU, not total. Run as root, or set kernel.perf_event_paranoid=-1,"
          echo "       to get counters that cover the part of the walk under test."
          ;;
        *)
          echo "  WARN counter scope: $scope (perf_event_paranoid=${paranoid:-?}) — check the raw CSVs before comparing cells"
          ;;
      esac
      echo
      echo "Reading it: on a cold walk most of the time is stall, so cpu_util"
      echo "(task_clock/elapsed, against $THREADS threads) and IPC are what separate"
      echo "'this strategy issued fewer I/Os' from 'this strategy merely burned less"
      echo "CPU'. A cell with flat wall time and low IPC is I/O-bound — that is the"
      echo "reading that makes a null wall-clock result interpretable. Counters"
      echo "shown as unsup/notcnt/absent were not delivered by the kernel; they are"
      echo "printed as such so they cannot be mistaken for a measured zero. Read the"
      echo "counter-scope check above first: user-only counters cannot answer the"
      echo "CPU-vs-I/O question for a syscall-dominated walk."
      echo
    fi

    if [[ "$DO_PERF" == "1" ]]; then
      for g in "${PERF_GROUP_LIST[@]}"; do
        shape=${GROUP_SHAPE[$g]}
        age=${GROUP_AGE[$g]}
        for cell in "${PERF_CELL_LIST[@]}"; do
          local base="$PERF_DIR/perf.$shape.$age.$cell"
          echo "========================================================================"
          echo "SHAPE: $shape   AGE: $age   CELL: $cell"
          echo "========================================================================"
          if [[ -s "$base.report.txt" ]]; then
            echo "----- perf.$shape.$age.$cell.report.txt (top 40 lines) -----"
            head -n 40 "$base.report.txt"
            echo
          fi
          if [[ -s "$base.report.bythread.txt" ]]; then
            echo "----- perf.$shape.$age.$cell.report.bythread.txt (top 40 lines) -----"
            head -n 40 "$base.report.bythread.txt"
            echo
          fi
        done
      done
    fi

    echo
    if [[ "$checks_failed" -eq 0 ]]; then
      echo "verdict: perf pass complete. Counters are readable; timings here are not."
    else
      echo "verdict: perf pass had failures — see the checks above before reading counters."
    fi
  } >"$report"

  cat "$report"
  echo
  echo "DONE (perf pass; no timing rows were produced)."
  [[ "$DO_PERF_STAT" == "1" ]] && echo "  perf stat (tsv): $PERFSTAT_TSV"
  echo "  perf report:     $report"
  echo "  perf artifacts:  $PERF_DIR"
  return "$checks_failed"
}

# ---- env snapshot ----------------------------------------------------------
# What ewalkbench reported about itself, rather than what this script assumes it
# did. preflight runs the real binary with the real cell arguments before the
# snapshot is written, so a tool default that changes between runs lands in the
# report instead of staying tribal knowledge. The baseline cell is the one
# quoted because `--stat spread` reports stat_threads=0 whatever was asked for.
observed_key() { # summary key -> value seen in the baseline cell's preflight run
  local key=$1 f v
  for f in "$RESULTS_DIR/preflight.$BASELINE_CELL.txt" "$RESULTS_DIR"/preflight.*.txt; do
    [[ -f "$f" ]] || continue
    v=$(sed -n "s/^$key=//p" "$f" | head -n1)
    [[ -n "$v" ]] && { printf '%s\n' "$v"; return 0; }
  done
  printf '%s\n' "<unknown: no preflight summary yet>"
}

write_env_snapshot() {
  {
    echo "# ewalk-strategy-bench"
    echo "timestamp=$TS"
    echo "mode=$MODE"
    echo "job=$JOB"
    echo "results_dir=$RESULTS_DIR"
    echo "log_file=$LOG_FILE"
    echo "ewalkbench=$BIN"
    echo "threads=$THREADS reps=$REPS rotate_cells=$ROTATE_CELLS"
    echo "cells=$CELLS"
    echo "shapes=$SHAPES"
    echo "ages=$AGES"
    echo "sort_window=${SORT_WINDOW:-<tool default: 1024 names, as ECRAWL_STAT_INODE_ORDER>}"
    echo "stat_threads=$(observed_key stat_threads) (observed; the tool's own summary, not a flag this script passes)"
    echo "stat_min_offload=<tool default: 32 names, as ECRAWL_STAT_BATCH_MIN_OFFLOAD>"
    echo "dir_enqueue_batch=$(observed_key dir_enqueue_batch) (observed; discovered directories published per acquisition of the global queue lock, as ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH. 1 is the one-lock-per-directory form: on a 200,000-directory shape that is bimodal at 0.395 s or 1.100 s, so a 1 here means the timing rows are two populations and their median is an artefact of the split. queue_push_ops in each row is what the batch actually achieved.)"
    echo "locality_counters=$(observed_key locality_counters) (observed; 1 = chunk_reuse_rate, median_ino_delta and distinct_ino_buckets were counted)"
    echo "stat_pool: stat_threads=0 is the inline path logs/ewalk-strategy-manual-567654 was measured with; a nonzero value measures a different walker, so the two sets of rows are not comparable"
    echo "aging: jobs=$AGE_JOBS seed=$AGE_SEED churn_frac=$AGE_CHURN_FRAC churn_rounds=$AGE_CHURN_ROUNDS timeout_sec=$AGE_TIMEOUT_SEC aged_asc_max=$AGED_ASC_MAX"
    echo "baseline_cell=$BASELINE_CELL control_cell=$CONTROL_CELL"
    echo "cold=$COLD (0 means EWALK_ALLOW_WARM=1 and the run is invalid)"
    echo "run_timeout_sec=$RUN_TIMEOUT_SEC keep_trees=$KEEP_TREES"
    echo "tree_base=$TREE_BASE"
    echo "tree_base_fstype=$TREE_FSTYPE"
    echo "tree_base_avail_kb=$(df -Pk "$TREE_BASE" 2>/dev/null | awk 'NR==2{print $4}')"
    echo "tmpdir=$TMPDIR"
    local g k
    for g in "${GROUP_LIST[@]}"; do
      k="tree.${GROUP_SHAPE[$g]}.${GROUP_AGE[$g]}"
      echo "$k.walk_root=${GROUP_WALK_ROOT[$g]}"
      echo "$k.expect_files=${GROUP_EXPECT_FILES[$g]}"
      echo "$k.manifest_files=$(sed -n 's/^total\.files=//p' "${GROUP_TREE_ROOT[$g]}/FIXTURE_MANIFEST.txt" 2>/dev/null | head -n1)"
      if [[ "${GROUP_AGE[$g]}" != "fresh" ]]; then
        echo "$k.age_sec=$(stamp_key "$g" age_sec)"
        echo "$k.asc_frac_before=$(stamp_key "$g" asc_frac_before)"
        echo "$k.asc_frac_after=$(stamp_key "$g" asc_frac_after)"
      fi
    done
    echo "sizing: many_dirs=${MANY_DIRS_PARENTS}x${MANY_DIRS_FILES_EACH} few_dirs=${FEW_DIRS_PARENTS}x${FEW_DIRS_FILES_EACH} one_dir=${ONE_DIR_FILES}"
    if [[ "$MODE" == "perf" ]]; then
      echo "perf: do_perf_stat=$DO_PERF_STAT do_perf=$DO_PERF reps=$PERF_REPS freq=$PERF_FREQ callgraph=$PERF_CALLGRAPH"
      echo "perf_events=$PERF_EVENTS"
      echo "perf_shapes=$PERF_SHAPES"
      echo "perf_ages=$PERF_AGES"
      echo "perf_cells=$PERF_CELLS"
      echo "perf_version=$(perf --version 2>/dev/null | head -n1)"
      echo "perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)"
    else
      echo "perf: not run (the timed path is uninstrumented; use mode=perf)"
    fi
    echo
    echo "## host"
    echo "hostname=$(hostname)"
    echo "uname=$(uname -a)"
    echo "nproc=$(nproc 2>/dev/null || echo '?')"
    echo "uid=$(id -u)"
    echo "cpu_model=$(awk -F: '/model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null | sed 's/^ //')"
    echo "mem_total_kb=$(awk '/MemTotal/{print $2; exit}' /proc/meminfo 2>/dev/null)"
  } >"$RESULTS_DIR/env.txt"
  # A perf pass in the same job shares this directory and would otherwise
  # overwrite the matrix's snapshot, losing the provenance of the timing run.
  cp -f "$RESULTS_DIR/env.txt" "$RESULTS_DIR/env.$MODE.txt" 2>/dev/null || true
}

# ---- main ------------------------------------------------------------------
echo "ewalk-strategy-bench: mode=$MODE job=$JOB host=$(hostname)"
echo "  binary=$BIN"
echo "  trees=$TREE_BASE (fstype=$TREE_FSTYPE)"
echo "  results=$RESULTS_DIR"
echo

rc=0
case "$MODE" in
  prepare)
    # Written twice on purpose: once so a failed prepare still leaves the
    # provenance of what it tried, and once after, so the aging measurements
    # land in the snapshot.
    write_env_snapshot
    prepare_trees || rc=1
    write_env_snapshot
    sed 's/^/  /' "$RESULTS_DIR/env.txt"
    if [[ -f "$TREES_TSV" ]]; then
      echo
      echo "trees (readdir ascending-step fraction is how you tell aging worked):"
      sed 's/^/  /' "$TREES_TSV"
    fi
    if [[ "$rc" -eq 0 ]]; then
      echo "DONE: trees ready under $TREE_BASE (kept for the whole benchmark run)."
    else
      echo "FAILED: at least one tree did not build or age; see tree.*.log / age.*.log in $RESULTS_DIR" >&2
    fi
    ;;
  calibrate | matrix)
    prepare_trees || { echo "ERROR: trees are not ready; refusing to measure" >&2; exit 1; }
    preflight || exit 1
    write_env_snapshot
    sed 's/^/  /' "$RESULTS_DIR/env.txt"
    echo
    init_cells_tsv
    if [[ "$MODE" == "calibrate" ]]; then
      run_calibration
    else
      run_matrix
      echo
      summarize || rc=1
    fi
    [[ "$g_failed_runs" -gt 0 ]] && rc=1
    ;;
  perf)
    if ! command -v perf >/dev/null 2>&1; then
      echo "ERROR: perf not found; the perf mode has nothing to run." >&2
      exit 1
    fi
    prepare_trees || { echo "ERROR: trees are not ready; refusing to measure" >&2; exit 1; }
    preflight || exit 1
    write_env_snapshot
    sed 's/^/  /' "$RESULTS_DIR/env.txt"
    echo
    init_perf_dir || { echo "ERROR: cannot create '$RESULTS_DIR/perf'" >&2; exit 1; }
    run_perf_pass || exit 1
    echo
    summarize_perf || rc=1
    [[ "$g_perf_failed" -gt 0 ]] && rc=1
    ;;
esac

if [[ "$KEEP_TREES" != "1" ]]; then
  echo "KEEP_TREES=$KEEP_TREES: removing $TREE_BASE"
  rm -rf "$TREE_BASE"
fi

exit "$rc"
