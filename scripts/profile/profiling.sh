#!/bin/bash
# Usage: ./scripts/profile/profiling.sh [step ...]   e.g. ... profiling.sh step1 step2
#        STEPS="step1 step2" ./scripts/profile/profiling.sh
# Default is step2 alone.
#
# Overridable paths (defaults in parentheses):
#   EREPORT_BIN_DIR      built binaries        (/tmp/ereport)
#   EREPORT_SCRIPTS_DIR  scripts/ root         (the parent of this script's dir)
#   DATA_DIR             synthetic tree root   (/data1/erbmi1/ecrawl-synt)
#   REPORT_DIR           output/fixture root   (/data1/erbmi1/ereport)
#
# Options:
#   TRIJOURNAL=0         set 1 to exercise crawl-time trigram journals: step1
#                        crawls with --trigram-journal $REPORT_DIR/journals and
#                        builds the index with --journal-dir (a successful --make
#                        deletes the journals it consumed); step2 enables the
#                        per-fixture journals (kept at <bin-root>/<fixture>/journals)
#                        via DO_TRIJOURNAL / EREPORT_INDEX_JOURNALS.
#   STAT_IMPL=fstatat    inode-read syscall for every ecrawl pass in step1/step2:
#                        fstatat (default) | statx (--statx) | iouring
#                        (--iouring). One impl per run; compare across runs.
#                        iouring falls back to statx when the kernel lacks
#                        IORING_OP_STATX (watch io_uring_batches in summaries).
#                        ECRAWL_IOURING_MIN_BATCH (default 8) stats tiny
#                        directory batches inline.
#   step2's fixture scripts take their own env knobs (DO_QUERY, MANYSHARD_COPIES,
#   REPS, ...); anything exported here is inherited by them.
#
# Deploying this to a server means copying scripts/ recursively: the default
# sibling lookup expects fixtures/ and profile/ to sit beside each other.
set -u
ulimit -n 100000

self_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
bin_root=${EREPORT_BIN_DIR:-/tmp/ereport}
scripts_root=${EREPORT_SCRIPTS_DIR:-$(cd "$self_dir/.." && pwd)}
data_dir=${DATA_DIR:-/data1/erbmi1/ecrawl-synt}
report_dir=${REPORT_DIR:-/data1/erbmi1/ereport}
bin_dir="$report_dir/bin"
idx_dir="$report_dir/index"
trijournal=${TRIJOURNAL:-0}
stat_impl=${STAT_IMPL:-fstatat}

# step2's fixture scripts each have their own knob; drive both from TRIJOURNAL.
if [[ "$trijournal" == "1" ]]; then
  export DO_TRIJOURNAL=1 EREPORT_INDEX_JOURNALS=1
fi
case "$stat_impl" in
  fstatat|statx|iouring) export ECRAWL_STAT_IMPL="$stat_impl" ;;
  *) echo "ERROR: STAT_IMPL must be fstatat|statx|iouring (got '$stat_impl')" >&2; exit 2 ;;
esac

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
  local jdir="$report_dir/journals"
  local crawl_args=() make_args=()
  if [[ "$trijournal" == "1" ]]; then
    mkdir -p "$jdir"
    crawl_args=(--trigram-journal "$jdir")
    make_args=(--journal-dir "$jdir")
  fi
  case "$stat_impl" in
    statx) crawl_args+=(--statx) ;;
    iouring) crawl_args+=(--iouring) ;;
  esac
  ECRAWL_CRAWL_THREADS=64 "$bin_root/ecrawl" --verbose "${crawl_args[@]}" "$data_dir" "$bin_dir"
  ECRAWL_CRAWL_THREADS=64 "$bin_root/ecrawl" "${crawl_args[@]}" "$data_dir" "$bin_dir"
  ECRAWL_QUERY_THREADS=64 "$bin_root/ecrawl_query" --top 10 "$bin_dir"
  EREPORT_THREADS=64 "$bin_root/ereport" --bucket-details 4 --report-dir "$report_dir" mtime "$bin_dir"
  EREPORT_INDEX_THREADS=64 "$bin_root/ereport_index" --make --index-dir "$idx_dir" "${make_args[@]}" "$bin_dir"
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
