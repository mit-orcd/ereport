#!/usr/bin/env bash
#
# Build a medium synthetic tree for indexer comparison and seed Q1–Q5 artifacts.
#
# Usage:
#   scripts/compare-indexers/prepare-synth.sh [synth-root]
#
# Env:
#   SYNTH_PROFILE=medium          (default; tiny for a seconds-long correctness
#                                 fixture, heavy/extreme for larger)
#   DISK_BUDGET_BYTES=...         passed through to the generator
#   SEED_QUERY_DIR=query_seeds    relative dir under synth-root for Q1–Q5 seeds
#
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

SYNTH_ROOT=${1:-${COMPARE_SYNTH_ROOT:-/tmp/indexer-compare-synth}}
SYNTH_PROFILE=${SYNTH_PROFILE:-medium}
SEED_QUERY_DIR=${SEED_QUERY_DIR:-query_seeds}
GEN="$REPO_ROOT/scripts/fixtures/generate-ecrawl-adversarial-tree.sh"

if [[ ! -x "$GEN" ]]; then
  echo "ERROR: missing generator $GEN" >&2
  exit 1
fi

mkdir -p "$SYNTH_ROOT"
SYNTH_ROOT=$(cd "$SYNTH_ROOT" && pwd)

# The generator reuses an existing tree when its FIXTURE_MANIFEST.txt records
# the same parameters, so a second benchmark run against the same root does not
# rebuild. It exits non-zero if the root holds a tree built with different ones.
echo "==> ensuring SYNTH_PROFILE=$SYNTH_PROFILE tree under $SYNTH_ROOT"
SYNTH_PROFILE="$SYNTH_PROFILE" "$GEN" "$SYNTH_ROOT"

SEED_ROOT="$SYNTH_ROOT/$SEED_QUERY_DIR"
rm -rf "$SEED_ROOT"
mkdir -p "$SEED_ROOT/large_subdir" "$SEED_ROOT/named"

# Q1: unique basename
UNIQUE_NAME="q1_unique_$(date +%s)_$$.marker"
: >"$SEED_ROOT/named/$UNIQUE_NAME"

# Q2: slurm-style outputs (glob + trigram-friendly substring)
for i in 1 2 3 4 5; do
  : >"$SEED_ROOT/named/slurm-${i}.out"
done
: >"$SEED_ROOT/named/slurm-not-out.txt"

# Q3: files > SIZE_GT_BYTES (sparse — little disk, large logical size)
# Paper uses 500MB; sparse truncate keeps the budget small.
BIG=$((SIZE_GT_BYTES + 1024))
for i in 1 2 3; do
  truncate -s "$BIG" "$SEED_ROOT/large_subdir/big_${i}.bin"
done
# Control files under the threshold
truncate -s $((100 * 1024 * 1024)) "$SEED_ROOT/large_subdir/mid_100m.bin"
: >"$SEED_ROOT/large_subdir/tiny.txt"

# Q4/Q5 target: many small files under large_subdir/bulk/. The tiny profile
# only needs enough of them for a count to be meaningful.
BULK_DEFAULT=2000
[[ "$SYNTH_PROFILE" != "tiny" ]] || BULK_DEFAULT=200
BULK_N=${BULK_N:-$BULK_DEFAULT}
mkdir -p "$SEED_ROOT/large_subdir/bulk"
seq -w 1 "$BULK_N" | while read -r n; do
  : >"$SEED_ROOT/large_subdir/bulk/f_${n}.dat"
done

# Manifest for query wrappers
MANIFEST="$SYNTH_ROOT/QUERY_SEEDS.txt"
{
  echo "synth_root=$SYNTH_ROOT"
  echo "seed_root=$SEED_ROOT"
  echo "q1_unique_name=$UNIQUE_NAME"
  echo "q1_unique_path=$SEED_ROOT/named/$UNIQUE_NAME"
  echo "q2_glob=slurm-*.out"
  echo "q2_term=slurm-"
  echo "q2_expected=5"
  echo "q3_min_bytes=$SIZE_GT_BYTES"
  echo "q3_expected=3"
  echo "q4_subtree=$SEED_ROOT/large_subdir"
  echo "q5_subtree=$SEED_ROOT/large_subdir"
} >"$MANIFEST"

echo "==> wrote seed manifest $MANIFEST"
echo "synth_root=$SYNTH_ROOT"
cat "$MANIFEST"
