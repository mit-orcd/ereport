#!/usr/bin/env bash
#
# Build a medium synthetic tree for indexer comparison and seed Q1–Q6 artifacts.
#
# Three argument sets are planted for every query, written to the manifest as
# <key>_1..3 with set 1 also written under the plain key.
#
# Usage:
#   scripts/compare-indexers/prepare-synth.sh [synth-root]
#
# Env:
#   SYNTH_PROFILE=medium          (default; tiny for a seconds-long correctness
#                                 fixture, heavy/extreme for larger)
#   DISK_BUDGET_BYTES=...         passed through to the generator
#   SEED_QUERY_DIR=query_seeds    relative dir under synth-root for Q1–Q6 seeds
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
mkdir -p "$SEED_ROOT/large_subdir" "$SEED_ROOT/named" "$SEED_ROOT/infix"

# Three arguments per query, not one. The benchmark asks each question four
# times per repetition -- once cold and three times hot -- and a hot run that
# repeats the cold argument twice more would be timing the answer it already
# has in memory rather than the index.
STAMP="$(date +%s)_$$"

# Q1: three unique basenames.
Q1_NAMES=()
for s in 1 2 3; do
  name="q1_unique_${STAMP}_s${s}.marker"
  Q1_NAMES+=("$name")
  : >"$SEED_ROOT/named/$name"
done

# Q2: three glob families, each with its own literal so the trigram term differs
# per set too, and each with a near-miss control that the glob must exclude.
Q2_GLOBS=('slurm-*.out' 'job-*.err' 'run-*.log')
Q2_TERMS=('slurm-' 'job-' 'run-')
for s in 1 2 3; do
  glob=${Q2_GLOBS[s - 1]}
  prefix=${glob%%\**}
  suffix=${glob#*\*}
  for i in 1 2 3 4 5; do
    : >"$SEED_ROOT/named/${prefix}${i}${suffix}"
  done
  : >"$SEED_ROOT/named/${prefix}not-a-match.txt"
done

# Q3: three thresholds with sparse files planted above each, so the sets have
# different answers. Sparse costs no disk and keeps the logical size honest.
# Set 1 stays on the paper's 500 MB, which is also what the unindexed key means
# to anything reading the manifest by hand.
Q3_MINS=("$SIZE_GT_BYTES" $((250 * 1024 * 1024)) $((750 * 1024 * 1024)))
Q3_EXPECTED=(5 7 2)
for i in 1 2; do
  truncate -s $((800 * 1024 * 1024)) "$SEED_ROOT/large_subdir/huge_${i}.bin"
done
for i in 1 2 3; do
  truncate -s $((SIZE_GT_BYTES + 1024)) "$SEED_ROOT/large_subdir/big_${i}.bin"
done
for i in 1 2; do
  truncate -s $((300 * 1024 * 1024)) "$SEED_ROOT/large_subdir/mid_${i}.bin"
done
# Control files under every threshold
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

# Q4/Q5: three subtrees of different sizes, so no two sets total or count the
# same thing. All three already exist; nothing extra is generated for them.
Q45_SUBTREES=("$SEED_ROOT/large_subdir" "$SEED_ROOT/large_subdir/bulk" "$SEED_ROOT/named")

# Q6, this comparison's own question: a substring anywhere in the name. It is Q2
# with the anchors removed, and that is the whole point -- 'slurm-*.out' has a
# literal prefix a B-tree on names can seek to, while '*token*' does not, so
# Robinhood and a sorted-name scan have to read everything while a trigram index
# looks the token up directly.
#
# The token is long and unlikely to occur anywhere else in the tree, and it sits
# in the middle of the name. The '.dat' tail is what keeps the answer the same
# for every tool: it excludes the control directory below, which would otherwise
# be a legitimate match for -name on the tools that index directories and not
# for the ones that only index files.
Q6_TOKENS=()
Q6_GLOBS=()
for s in 1 2 3; do
  token="zqx${STAMP//_/}s${s}"
  Q6_TOKENS+=("$token")
  Q6_GLOBS+=("*${token}*.dat")
  for i in 1 2 3 4 5 6 7; do
    : >"$SEED_ROOT/infix/data_${token}_00${i}.dat"
  done
  # Two controls. The directory carries the token while the file inside it does
  # not, so a tool matching the whole path instead of the basename is caught
  # rather than credited; the short name holds all but the last character of the
  # token, so a match on a prefix of it is caught too.
  mkdir -p "$SEED_ROOT/infix/${token}_dir"
  : >"$SEED_ROOT/infix/${token}_dir/plain_control.dat"
  : >"$SEED_ROOT/infix/data_${token%?}_control.dat"
done

# Manifest for query wrappers. Every key is written per set, and set 1 is also
# written unindexed: run_queries.sh, prod-protocol.md and any hand-written seed
# file still name the plain keys.
MANIFEST="$SYNTH_ROOT/QUERY_SEEDS.txt"
{
  echo "synth_root=$SYNTH_ROOT"
  echo "seed_root=$SEED_ROOT"
  echo "arg_sets=3"
  for s in 1 2 3; do
    echo "q1_unique_name_${s}=${Q1_NAMES[s - 1]}"
    echo "q1_unique_path_${s}=$SEED_ROOT/named/${Q1_NAMES[s - 1]}"
    echo "q2_glob_${s}=${Q2_GLOBS[s - 1]}"
    echo "q2_term_${s}=${Q2_TERMS[s - 1]}"
    echo "q2_expected_${s}=5"
    echo "q3_min_bytes_${s}=${Q3_MINS[s - 1]}"
    echo "q3_expected_${s}=${Q3_EXPECTED[s - 1]}"
    echo "q4_subtree_${s}=${Q45_SUBTREES[s - 1]}"
    echo "q5_subtree_${s}=${Q45_SUBTREES[s - 1]}"
    echo "q6_glob_${s}=${Q6_GLOBS[s - 1]}"
    echo "q6_term_${s}=${Q6_TOKENS[s - 1]}"
    echo "q6_expected_${s}=7"
  done
  echo "q1_unique_name=${Q1_NAMES[0]}"
  echo "q1_unique_path=$SEED_ROOT/named/${Q1_NAMES[0]}"
  echo "q2_glob=${Q2_GLOBS[0]}"
  echo "q2_term=${Q2_TERMS[0]}"
  echo "q2_expected=5"
  echo "q3_min_bytes=${Q3_MINS[0]}"
  echo "q3_expected=${Q3_EXPECTED[0]}"
  echo "q4_subtree=${Q45_SUBTREES[0]}"
  echo "q5_subtree=${Q45_SUBTREES[0]}"
  echo "q6_glob=${Q6_GLOBS[0]}"
  echo "q6_term=${Q6_TOKENS[0]}"
  echo "q6_expected=7"
} >"$MANIFEST"

echo "==> wrote seed manifest $MANIFEST"
echo "synth_root=$SYNTH_ROOT"
cat "$MANIFEST"
