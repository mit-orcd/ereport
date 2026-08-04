#!/bin/bash
# Usage: ./scripts/profile/profiling.sh [step ...]   e.g. ... profiling.sh step1 step2
#        STEPS="step1 step2" ./scripts/profile/profiling.sh
# Default is step2 alone.
#
# Overridable paths (defaults in parentheses):
#   EREPORT_BIN_DIR      built binaries        (/tmp/ereport)
#   EREPORT_SCRIPTS_DIR  scripts/ root         (the parent of this script's dir)
#   DATA_DIR             synthetic tree root   (/data1/erbmi1/ecrawl-synth)
#   REPORT_DIR           output/fixture root   (/data1/erbmi1/ereport)
#
# Deploying this to a server means copying scripts/ recursively: the default
# sibling lookup expects fixtures/ and profile/ to sit beside each other.
set -u
ulimit -n 100000

self_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
bin_root=${EREPORT_BIN_DIR:-/tmp/ereport}
scripts_root=${EREPORT_SCRIPTS_DIR:-$(cd "$self_dir/.." && pwd)}
data_dir=${DATA_DIR:-/data1/erbmi1/ecrawl-synth}
report_dir=${REPORT_DIR:-/data1/erbmi1/ereport}
bin_dir="$report_dir/bin"
idx_dir="$report_dir/index"

# The profilers only auto-detect ./<tool>, /tmp/<tool> and $PATH — none of which
# match a $bin_root deploy — so name the binaries for them explicitly.
export ECRAWL_BIN="$bin_root/ecrawl"
export ECRAWL_QUERY_BIN="$bin_root/ecrawl_query"
export EREPORT_BIN="$bin_root/ereport"
export EREPORT_INDEX_BIN="$bin_root/ereport_index"

# Fail once, up front, instead of once per profiler deep into the run.
for f in "$bin_root"/{edelete,ecrawl,ecrawl_query,ereport,ereport_index}; do
  [[ -f "$f" && -x "$f" ]] || { echo "ERROR: not an executable: $f (set EREPORT_BIN_DIR)" >&2; exit 2; }
done
for d in "$scripts_root"/{fixtures,profile}; do
  [[ -d "$d" ]] || { echo "ERROR: no such dir: $d (set EREPORT_SCRIPTS_DIR to the scripts/ root)" >&2; exit 2; }
done

function step1() {
  EDELETE_THREADS=96 "$bin_root/edelete" --delete --force "$data_dir"
  EDELETE_THREADS=96 "$bin_root/edelete" --delete --force "$report_dir"
  mkdir -p "$data_dir"
  mkdir -p "$report_dir"

  DISK_BUDGET_BYTES=$((200 * 1024 * 1024 * 1024)) DEPTH_SLICE_ENABLE=1 SYNTH_PROFILE=extreme "$scripts_root/fixtures/generate-ecrawl-adversarial-tree.sh" "$data_dir"
  # Crawled twice on purpose: --verbose turns on the per-call I/O counter atomics,
  # so the quiet run is the one whose capture and timing the later steps use.
  ECRAWL_CRAWL_THREADS=64 "$bin_root/ecrawl" --verbose "$data_dir" "$bin_dir"
  ECRAWL_CRAWL_THREADS=64 "$bin_root/ecrawl" "$data_dir" "$bin_dir"
  ECRAWL_QUERY_THREADS=64 "$bin_root/ecrawl_query" --top 10 "$bin_dir"
  EREPORT_THREADS=64 "$bin_root/ereport" --bucket-details 4 --report-dir "$report_dir" mtime "$bin_dir"
  EREPORT_INDEX_THREADS=64 "$bin_root/ereport_index" --make --index-dir "$idx_dir" "$bin_dir"
}

function step2() {
  DO_STRACE=0 DO_PERF=1 DO_SCHED=1 "$scripts_root/profile/ecrawl-fixtures.sh" "$data_dir" "$report_dir"
  DO_STRACE=0 DO_PERF=1 DO_SCHED=1 "$scripts_root/profile/ecrawl_query-fixtures.sh" "$report_dir"
  DO_STRACE=0 DO_PERF=1 DO_SCHED=1 "$scripts_root/profile/ereport-fixtures.sh" "$report_dir"
  DO_STRACE=0 DO_PERF=1 DO_SCHED=1 "$scripts_root/profile/ereport_index-fixtures.sh" "$report_dir"
}

if (($#)); then steps=("$@"); else read -ra steps <<<"${STEPS:-step2}"; fi

# Validate every step before running any: step1 takes hours, so a typo in a
# later step should fail now rather than after all that work.
for s in "${steps[@]}"; do
  declare -F "$s" >/dev/null || { echo "unknown step '$s' (have: step1 step2)" >&2; exit 2; }
done

for s in "${steps[@]}"; do
  echo "=== $s ==="
  "$s"
done
