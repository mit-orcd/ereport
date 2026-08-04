#!/usr/bin/env bash
#
# End-to-end synthetic smoke for indexer comparison.
#
# Usage:
#   scripts/compare-indexers/run_smoke.sh [synth-root] [results-root]
#
# Env:
#   SYNTH_PROFILE=medium tiny: seconds to build, a few thousand entries, and
#                        only find + du alongside the suite. That is the mode
#                        for checking a code change end to end; every other
#                        profile is for measuring.
#   WITH_EXTERNALS=0     1: run the installed external indexers against the tiny
#                        profile too, which is how their wiring gets checked
#                        without waiting for a real tree
#   SKIP_PREPARE=0|1     reuse existing synth-root
#   SKIP_CORRECTNESS=0|1
#   TOOLS_INDEX=...      default: ecrawl find du (+ fd/dua/gufi/xdu if installed)
#   TOOLS_QUERY=...      explicit query tool list (disables auto-detection)
#   REPS=1               keep smoke fast (queries use same REPS)
#   CACHE_MODES="cold hot"  passes per repetition; one name for a single pass
#   DROP_CACHES=0        1: drop page cache before each timed command (root)
#   DROP_CACHES_SCOPE=all        all | first-rep
#   DROP_DB_CACHE=0      1: also restart MariaDB when dropping caches
#   KEEP_ALL_INDEXES=0   1: keep every rep's index instead of the last one
#   DO_NOWRITE=0         1: also time ecrawl walking without storing anything,
#                        the like-for-like row against find/fd/du/dua
#
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# The /tmp default is for the small local smoke test only; a real run passes a
# path on the filesystem under test (benchmark.sh always does).
SYNTH_ROOT=${1:-${COMPARE_SYNTH_ROOT:-/tmp/indexer-compare-synth}}
TS=$(date +%Y%m%d-%H%M%S)
RESULTS_ROOT=${2:-"$COMPARE_DIR/results/smoke-$TS"}
mkdir -p "$RESULTS_ROOT"
RESULTS_ROOT=$(cd "$RESULTS_ROOT" && pwd)

SYNTH_PROFILE=${SYNTH_PROFILE:-medium}
export SYNTH_PROFILE
# Only meaningful with the tiny profile, which otherwise runs find and du alone.
WITH_EXTERNALS=${WITH_EXTERNALS:-0}
export WITH_EXTERNALS
SKIP_PREPARE=${SKIP_PREPARE:-0}
SKIP_CORRECTNESS=${SKIP_CORRECTNESS:-0}
SEED_QUERY_DIR=${SEED_QUERY_DIR:-query_seeds}
REPS=${REPS:-1}
export REPS DROP_CACHES DROP_CACHES_SCOPE DROP_DB_CACHE CACHE_MODES

echo "==> results: $RESULTS_ROOT"

if [[ "$SKIP_PREPARE" != "1" ]]; then
  "$COMPARE_DIR/prepare-synth.sh" "$SYNTH_ROOT"
fi
SYNTH_ROOT=$(cd "$SYNTH_ROOT" && pwd)

# Correctness on the small seeded subtree (fast) + optional whole-tree note.
# On a tiny tree there is no reason to settle for the subtree: checking the
# whole thing costs a second and covers the hard links, symlinks, specials and
# sparse files the seeds do not contain.
CHECK_ROOT="$SYNTH_ROOT/$SEED_QUERY_DIR"
[[ "$SYNTH_PROFILE" != "tiny" ]] || CHECK_ROOT="$SYNTH_ROOT"
if [[ "$SKIP_CORRECTNESS" != "1" ]]; then
  "$COMPARE_DIR/check_correctness.sh" "$CHECK_ROOT" "$RESULTS_ROOT/correctness" || {
    echo "WARN: correctness reported failure (see $RESULTS_ROOT/correctness)" >&2
  }
fi

# Index: always ecrawl + find; add externals if present
TOOLS_INDEX=${TOOLS_INDEX:-}
if [[ -z "$TOOLS_INDEX" && "$SYNTH_PROFILE" == "tiny" && "$WITH_EXTERNALS" != "1" ]]; then
  # A tree this small measures nothing, so there is no reason to drag the
  # external indexers (and a MariaDB) into a run whose only job is to prove the
  # suite still answers correctly. find and du are the references it checks
  # against, and they are always installed.
  #
  # WITH_EXTERNALS=1 asks for the opposite: the same tiny tree, but with every
  # installed indexer, to check that they answer at all. It seeds three sparse
  # 500 MB files and a 1.68 GB subtree, which is what caught XDU reporting
  # allocated blocks where every other tool reports file length.
  TOOLS_INDEX="ecrawl find du"
fi
if [[ -z "$TOOLS_INDEX" ]]; then
  TOOLS_INDEX="ecrawl find"
  # fd/dua: include whenever a path is known, even if the probe failed, so the
  # index script emits skipped rows with the reason instead of omitting them.
  baseline_candidate fd && TOOLS_INDEX+=" fd"
  tool_available du && TOOLS_INDEX+=" du"
  baseline_candidate dua && TOOLS_INDEX+=" dua"
  tool_available gufi && TOOLS_INDEX+=" gufi"
  tool_available xdu && TOOLS_INDEX+=" xdu"
  # Installed but unprovisioned Robinhood would contribute a scan that cannot
  # run and five queries against an empty schema, so leave it out and say why.
  if tool_available robinhood; then
    if rbh_db_ready; then
      TOOLS_INDEX+=" robinhood"
    else
      echo "WARN: robinhood is installed but not usable ($RBH_READY_REASON); leaving it out" >&2
    fi
  fi
fi
export TOOLS="$TOOLS_INDEX"
export INCLUDE_EREPORT_INDEX=1
export DO_NOWRITE=${DO_NOWRITE:-0}

"$COMPARE_DIR/run_index.sh" "$SYNTH_ROOT" "$RESULTS_ROOT/index"

export INDEX_RESULTS_DIR="$RESULTS_ROOT/index"
export ECRAWL_BIN_DIR
ECRAWL_BIN_DIR=$(cat "$RESULTS_ROOT/index/ecrawl_bin_dir.txt" 2>/dev/null || true)
export EREPORT_INDEX_DIR
EREPORT_INDEX_DIR=$(cat "$RESULTS_ROOT/index/ereport_index_dir.txt" 2>/dev/null || true)
export GUFI_INDEX_DIR
GUFI_INDEX_DIR=$(cat "$RESULTS_ROOT/index/gufi_index_dir.txt" 2>/dev/null || true)
export XDU_INDEX_DIR
XDU_INDEX_DIR=$(cat "$RESULTS_ROOT/index/xdu_index_dir.txt" 2>/dev/null || true)

if [[ -n "${TOOLS_QUERY:-}" ]]; then
  TOOLS_Q=$TOOLS_QUERY
elif [[ "$SYNTH_PROFILE" == "tiny" && "$WITH_EXTERNALS" != "1" ]]; then
  TOOLS_Q="find du ecrawl_suite"
else
  TOOLS_Q="find ecrawl_suite"
  baseline_candidate fd && TOOLS_Q+=" fd"
  tool_available du && TOOLS_Q+=" du"
  baseline_candidate dua && TOOLS_Q+=" dua"
  tool_available gufi && [[ -n "${GUFI_INDEX_DIR:-}" ]] && TOOLS_Q+=" gufi"
  tool_available xdu && [[ -n "${XDU_INDEX_DIR:-}" ]] && TOOLS_Q+=" xdu"
  tool_available robinhood && rbh_db_ready && TOOLS_Q+=" robinhood"
fi
export TOOLS="$TOOLS_Q"

"$COMPARE_DIR/run_queries.sh" "$SYNTH_ROOT" "$RESULTS_ROOT/queries"

python3 "$COMPARE_DIR/summarize.py" "$RESULTS_ROOT/index" "$RESULTS_ROOT/queries" \
  --out "$RESULTS_ROOT/SUMMARY_TABLE.txt" >/dev/null

echo "==> smoke complete"
echo "SUMMARY=$RESULTS_ROOT/SUMMARY_TABLE.txt"
cat "$RESULTS_ROOT/SUMMARY_TABLE.txt"

# Charts are a presentation step; a missing plotting stack must not discard a
# completed benchmark run.
CHART_PY=$(resolve_chart_python || true)
if [[ -z "$CHART_PY" ]]; then
  echo "WARN: no interpreter with matplotlib found; skipping charts." >&2
  echo "      Install one with: SETUP_CHARTS=1 scripts/compare-indexers/init.sh" >&2
elif MPLCONFIGDIR="${MPLCONFIGDIR:-$RESULTS_ROOT/.mplconfig}" \
  "$CHART_PY" "$COMPARE_DIR/plot_results.py" "$RESULTS_ROOT" \
  --out-dir "$RESULTS_ROOT/charts"; then
  echo "CHARTS=$RESULTS_ROOT/charts"
else
  echo "WARN: chart generation failed; CSVs and summary table are still complete" >&2
fi
