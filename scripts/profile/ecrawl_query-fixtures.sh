#!/usr/bin/env bash
#
# profile/ecrawl_query-fixtures.sh
#
# SPDX-License-Identifier: MIT
#
# Profile `ecrawl_query` (read-only directory-shape stats over crawl shards)
# over the shared per-fixture crawl output produced by
# profile/ecrawl-fixtures.sh, the same way profile/ereport-fixtures.sh profiles
# `ereport`.
#
# This profiler does NOT crawl. Run profile/ecrawl-fixtures.sh first with the
# same <bin-root> to populate <bin-root>/<fixture>/bin/; this script consumes
# those shards and hard-errors if a selected fixture has none.
#
# Per fixture (see generate-ecrawl-adversarial-tree.sh):
#   ANALYZE phase — run `ecrawl_query --verbose` over <bin-root>/<fixture>/bin
#                   and capture a full profile.
#
# The ANALYZE phase runs up to four instrumented passes:
#   clean   — /usr/bin/time -v + ecrawl_query key=value stats on stdout
#             (records_total, distinct_parent_directories, ...) + our own wall
#             timer. Only trustworthy timing.
#   strace  — strace -f -c syscall histogram (timing NOT representative).
#   perf    — perf record --call-graph dwarf + perf report (CPU profile, plus a
#             per-thread CPU split; build ecrawl_query with `make debug` for
#             best symbols).
#   sched   — optional `perf sched` pass (per-thread runtime / switch / delay)
#             for the big fixtures, to confirm per-section thread concurrency.
#
# NOTE on thread use: ecrawl_query parallelises per parse chunk (a .ckpt
# segment), not per shard file — it caps its worker count at the number of
# chunks. A single big single-UID shard is split into multiple chunks, so it
# now spreads across cores (each shard's catalog is loaded once and shared by
# all its chunks). Only a small single-chunk shard runs single-threaded; the
# per-thread perf views below make the actual spread visible.
#
# Usage:
#   scripts/profile/ecrawl_query-fixtures.sh <bin-root> [results-dir]
#
# Required:
#   <bin-root>     dir produced by profile/ecrawl-fixtures.sh; for each fixture
#                  it must contain <bin-root>/<fixture>/bin/uid_shard_*.bin.
#                  ecrawl_query is read-only, so it produces no kept output.
#
# Optional positional:
#   [results-dir]  where profiling logs/tarball go (default:
#                  ./ecrawl_query-profile-<timestamp>); kept separate from data.
#
# Environment knobs (all optional):
#   ECRAWL_QUERY_BIN=./ecrawl_query  analyzer binary (auto: ./, /tmp/, PATH).
#   ECRAWL_QUERY_THREADS=32    worker threads (passed through; tool caps at the
#                              shard count).
#   ANALYZE_TOP=               if set to N, pass `--top N` to ecrawl_query.
#   FIXTURES="a b c"           subset of fixtures (default: known set, else all
#                              immediate subdirs).
#   INCLUDE_ROOT=0             also analyze the whole-tree _ALL_ROOT_ bin set if
#                              the ecrawl profiler produced it (INCLUDE_ROOT=1).
#   DO_STRACE=1                run the strace pass.
#   DO_PERF=1                  run the perf pass.
#   DO_SCHED=0                 run a `perf sched` pass (per-thread runtime /
#                              switch / delay) — only for fixtures listed in
#                              SCHED_FIXTURES, since the trace is large. Use it
#                              to confirm per-section thread concurrency.
#   SCHED_FIXTURES="single_huge_dir mega_dir1 mega_dir2"  fixtures big enough
#                              for a meaningful perf sched trace.
#   PERF_FREQ=997              perf sampling frequency (Hz).
#   PERF_CALLGRAPH=dwarf       perf call-graph mode (dwarf|fp).
#   REPS=1                     repetitions of the clean analyze pass per fixture.
#   ECRAWL_QUERY_VERBOSE_ARGS=...  extra args appended to ecrawl_query.
#   DO_QUERY=1                 also profile the selective hot-path queries.
#   QUERY_SIZE_GT=524288000    strict byte threshold substituted for @SIZE@.
#   QUERY_TYPE=f               record type substituted for @TYPE@.
#   QUERY_GID=$(id -g)         group id substituted for @GID@.
#   QUERY_OUTPUT_SINK=/dev/null  listed paths destination; use a regular file or
#                              production pipe to measure output backpressure.
#   QUERY_VARIANTS=            newline-separated "name|env|args" list overriding
#                              the default variant set (see QUERY_VARIANTS_DEFAULT
#                              below for the shape and what each default isolates).
#                              Narrow it to shorten a run:
#                                QUERY_VARIANTS=$'subtree-rollup|| --subtree @ROOT@\nsubtree-exact|| --subtree @ROOT@ --exact'
#
# Data layout (under <bin-root>, persistent):
#   <fixture>/bin/             ecrawl uid_shard_*.bin (produced by the ecrawl profiler)
#
# Profiling-log layout (under <results-dir>):
#   env.txt                                    host/build/env snapshot
#   <fixture>/analyze/clean.stats.txt          ecrawl_query key=value (stdout)
#   <fixture>/analyze/clean.progress.txt       ecrawl_query progress (stderr)
#   <fixture>/analyze/clean.time.txt           /usr/bin/time -v
#   <fixture>/analyze/strace.txt               strace -f -c histogram
#   <fixture>/analyze/perf.report.txt          perf report --stdio
#   <fixture>/analyze/perf.report.caller.txt   perf report caller view
#   <fixture>/analyze/perf.report.bythread.txt per-thread CPU split (thread use)
#   <fixture>/analyze/perf.sched.latency.txt   per-thread sched runtime/delay (DO_SCHED)
#   <fixture>/analyze/perf.sched.summary.txt   perf sched timehist summary (DO_SCHED)
#
#   Each selective query variant (DO_QUERY) writes the same file names under its
#   own directory, so all variants coexist in one run:
#   <fixture>/query-<variant>/variant.txt        name, env and resolved argv
#   <fixture>/query-<variant>/clean.stats.txt    totals + row group skip counters
#   <fixture>/query-<variant>/clean.time.txt     /usr/bin/time -v
#   <fixture>/query-<variant>/clean.wall.txt     wall_seconds
#   <fixture>/query-<variant>/perf.report.txt    perf report --stdio
#   <fixture>/query-<variant>/perf.report.caller.txt
#   <fixture>/query-<variant>/perf.report.bythread.txt
#
#   SUMMARY_TABLE.txt                          at-a-glance table
#   COMBINED_REPORT.txt                        everything concatenated
#   ecrawl_query-profile-<timestamp>.tar.gz  tarball (upload this)
#
set -uo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../lib" && pwd)/common.sh"

# ---- args ------------------------------------------------------------------
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <bin-root> [results-dir]" >&2
  exit 2
fi
# Shared shard sets produced by profile/ecrawl-fixtures.sh; this profiler only
# consumes <bin-root>/<fixture>/bin and never crawls.
BIN_ROOT=${1%/}
if [[ ! -d "$BIN_ROOT" ]]; then
  echo "ERROR: bin-root '$BIN_ROOT' is not a directory (run profile/ecrawl-fixtures.sh first)" >&2
  exit 2
fi
BIN_ROOT=$(cd "$BIN_ROOT" && pwd)

TS=$(date +%Y%m%d-%H%M%S)
RESULTS_DIR=${2:-"./ecrawl_query-profile-$TS"}
mkdir -p "$RESULTS_DIR" || { echo "ERROR: cannot create results dir '$RESULTS_DIR'" >&2; exit 1; }
RESULTS_DIR=$(cd "$RESULTS_DIR" && pwd)

# ---- locate binaries -------------------------------------------------------
ECRAWL_QUERY_BIN=$(find_bin ecrawl_query ECRAWL_QUERY_BIN) || exit 1

# ---- config ----------------------------------------------------------------
ECRAWL_QUERY_THREADS=${ECRAWL_QUERY_THREADS:-32}
export ECRAWL_QUERY_THREADS
ANALYZE_TOP=${ANALYZE_TOP:-}
DO_STRACE=${DO_STRACE:-1}
DO_PERF=${DO_PERF:-1}
DO_SCHED=${DO_SCHED:-0}
SCHED_FIXTURES=${SCHED_FIXTURES:-"single_huge_dir mega_dir1 mega_dir2"}
PERF_FREQ=${PERF_FREQ:-997}
PERF_CALLGRAPH=${PERF_CALLGRAPH:-dwarf}
REPS=${REPS:-1}
INCLUDE_ROOT=${INCLUDE_ROOT:-0}
ECRAWL_QUERY_VERBOSE_ARGS=${ECRAWL_QUERY_VERBOSE_ARGS:-}
DO_QUERY=${DO_QUERY:-1}
QUERY_SIZE_GT=${QUERY_SIZE_GT:-524288000}
QUERY_TYPE=${QUERY_TYPE:-f}
QUERY_OUTPUT_SINK=${QUERY_OUTPUT_SINK:-/dev/null}
QUERY_GID=${QUERY_GID:-$(id -g)}

# Query variants, one instrumented pass each: "name|env|args".
#
# `env` is a space-separated VAR=VAL list applied to that pass only (empty for
# most). `args` are appended after the binary, before the bin dir. Placeholders
# are substituted per fixture: @ROOT@ is the crawl's start_path from
# crawl_manifest.txt, @GID@ is QUERY_GID, @SIZE@ is QUERY_SIZE_GT, @TYPE@ is
# QUERY_TYPE. A variant naming @ROOT@ is skipped for a fixture whose manifest has
# no start_path rather than being run against a wrong path.
#
# The set is chosen so each pass isolates one cost, and several exist only as the
# A/B partner of another:
#
#   size-skipall        every row group pruned by the size zone map. Best case:
#                       measures seek-past throughput and the fixed startup cost.
#   size-matchall       nothing prunes, so the decoder actually runs. Worst case,
#                       and the honest counterpart to size-skipall.
#   size-matchall-nolist   same predicate without --list, so the delta against
#                       size-matchall is path reconstruction plus the output write.
#   size-matchall-noskip   same as -nolist with zone maps disabled; the delta is
#                       what row group skipping is worth in seconds, not counters.
#   subtree-rollup      a bare --subtree, answered from the catalog prefix sums
#                       with zero record I/O. This is the headline v8 path.
#   subtree-exact       the same question forced through a full record scan. Only
#                       this pair licenses a claim about the rollup speedup.
#   subtree-list        --subtree with a record predicate, so the DFS range test
#                       runs per record and paths are rebuilt.
#   gid-match           gid is RLE'd and near-constant per uid shard, so its zone
#                       map usually prunes whole groups.
#   perm-any            a bit test, which no zone map can prune: every group is
#                       decoded. The floor for a mode predicate.
QUERY_VARIANTS_DEFAULT=(
  "size-skipall|| --size-gt @SIZE@ --type @TYPE@ --list"
  "size-matchall|| --size-gt 0 --type @TYPE@ --list"
  "size-matchall-nolist|| --size-gt 0 --type @TYPE@"
  "size-matchall-noskip|ECRAWL_QUERY_BLOCK_SKIP=0| --size-gt 0 --type @TYPE@"
  "subtree-rollup|| --subtree @ROOT@"
  "subtree-exact|| --subtree @ROOT@ --exact"
  "subtree-list|| --subtree @ROOT@ --type @TYPE@ --list"
  "gid-match|| --gid @GID@ --type @TYPE@"
  "perm-any|| --perm /0444 --type @TYPE@"
)
if [[ -n "${QUERY_VARIANTS:-}" ]]; then
  IFS=$'\n' read -rd '' -a QVARIANTS <<<"$QUERY_VARIANTS" || true
else
  QVARIANTS=("${QUERY_VARIANTS_DEFAULT[@]}")
fi

if [[ -n "${FIXTURES:-}" ]]; then
  read -ra FIXLIST <<<"$FIXTURES"
else
  FIXLIST=()
  for f in "${KNOWN_FIXTURES[@]}"; do
    [[ -d "$BIN_ROOT/$f/bin" ]] && FIXLIST+=("$f")
  done
  if [[ ${#FIXLIST[@]} -eq 0 ]]; then
    while IFS= read -r d; do FIXLIST+=("$(basename "$d")"); done \
      < <(find "$BIN_ROOT" -mindepth 1 -maxdepth 1 -type d | sort)
  fi
fi
if [[ ${#FIXLIST[@]} -eq 0 ]]; then
  echo "ERROR: no fixtures with bins found under '$BIN_ROOT' (run profile/ecrawl-fixtures.sh first)" >&2
  exit 1
fi

# ---- tool availability -----------------------------------------------------
HAVE_TIME=0; TIME_BIN=""
for c in /usr/bin/time /bin/time; do
  [[ -x "$c" ]] && { TIME_BIN="$c"; HAVE_TIME=1; break; }
done
HAVE_STRACE=0; command -v strace >/dev/null 2>&1 && HAVE_STRACE=1
HAVE_PERF=0;   command -v perf   >/dev/null 2>&1 && HAVE_PERF=1

if [[ "$DO_STRACE" == "1" && "$HAVE_STRACE" != "1" ]]; then
  echo "WARNING: strace not found; skipping strace pass." >&2; DO_STRACE=0
fi
if [[ "$DO_PERF" == "1" && "$HAVE_PERF" != "1" ]]; then
  echo "WARNING: perf not found; skipping perf pass." >&2; DO_PERF=0
fi

# ---- helpers ---------------------------------------------------------------

# Build ecrawl_query argv into RUN_ARGV.
build_argv() {
  local bindir=$1
  RUN_ARGV=("$ECRAWL_QUERY_BIN" --verbose)
  [[ -n "$ANALYZE_TOP" ]] && RUN_ARGV+=(--top "$ANALYZE_TOP")
  # shellcheck disable=SC2206
  [[ -n "$ECRAWL_QUERY_VERBOSE_ARGS" ]] && RUN_ARGV+=($ECRAWL_QUERY_VERBOSE_ARGS)
  RUN_ARGV+=("$bindir")
}

# The crawl root, for variants that need a real --subtree argument. ecrawl always
# writes start_path into the manifest, so no fixture needs a hand-supplied path.
fixture_root() {
  local bindir=$1
  local mf="$bindir/crawl_manifest.txt"
  [[ -f "$mf" ]] || return 1
  local v
  v=$(sed -n 's/^start_path=//p' "$mf" | tail -n1)
  [[ -n "$v" ]] || return 1
  echo "$v"
}

# Expand placeholders and split into QUERY_ARGV. Returns 1 if the variant needs a
# crawl root this fixture does not have.
build_variant_argv() {
  local args=$1 bindir=$2 root=$3
  if [[ "$args" == *"@ROOT@"* && -z "$root" ]]; then return 1; fi
  args=${args//@ROOT@/$root}
  args=${args//@GID@/$QUERY_GID}
  args=${args//@SIZE@/$QUERY_SIZE_GT}
  args=${args//@TYPE@/$QUERY_TYPE}
  # shellcheck disable=SC2206 - deliberate word splitting of the variant's args
  local parts=($args)
  QUERY_ARGV=("$ECRAWL_QUERY_BIN" "${parts[@]}" "$bindir")
  # Without --list the key=value totals are on stdout; with it they move to
  # stderr and stdout carries the matched paths. Capture the totals either way,
  # or a variant silently reports nothing.
  if [[ " $args " == *" --list "* || "$args" == *" --list" ]]; then
    QUERY_LISTS=1
  else
    QUERY_LISTS=0
  fi
  return 0
}

# Prefix for a variant's per-pass environment, as an argv (empty when unset).
build_variant_env() {
  local envs=$1
  ENV_PREFIX=()
  [[ -n "$envs" ]] || return 0
  # shellcheck disable=SC2206 - deliberate word splitting of VAR=VAL pairs
  local kv=($envs)
  ENV_PREFIX=(env "${kv[@]}")
}

run_query_clean() {
  local dest=$1 rep=$2
  local sfx=""; [[ "$REPS" -gt 1 ]] && sfx=".rep${rep}"
  local t0 t1 rc
  echo "    query clean${sfx}: ${ENV_PREFIX[*]:-} ${QUERY_ARGV[*]}"
  t0=$(date +%s.%N)
  if [[ "$QUERY_LISTS" == "1" ]]; then
    if [[ "$HAVE_TIME" == "1" ]]; then
      "${ENV_PREFIX[@]}" "$TIME_BIN" -v -o "$dest/clean.time${sfx}.txt" \
        "${QUERY_ARGV[@]}" >"$QUERY_OUTPUT_SINK" 2>"$dest/clean.stats${sfx}.txt"
    else
      "${ENV_PREFIX[@]}" "${QUERY_ARGV[@]}" >"$QUERY_OUTPUT_SINK" 2>"$dest/clean.stats${sfx}.txt"
    fi
  else
    if [[ "$HAVE_TIME" == "1" ]]; then
      "${ENV_PREFIX[@]}" "$TIME_BIN" -v -o "$dest/clean.time${sfx}.txt" \
        "${QUERY_ARGV[@]}" >"$dest/clean.stats${sfx}.txt" 2>"$dest/clean.progress${sfx}.txt"
    else
      "${ENV_PREFIX[@]}" "${QUERY_ARGV[@]}" \
        >"$dest/clean.stats${sfx}.txt" 2>"$dest/clean.progress${sfx}.txt"
    fi
  fi
  rc=$?
  t1=$(date +%s.%N)
  awk -v a="$t0" -v b="$t1" 'BEGIN{printf "wall_seconds=%.3f\n", b-a}' >"$dest/clean.wall${sfx}.txt"
  echo "rc=$rc" >>"$dest/clean.stats${sfx}.txt"
  echo "    query clean${sfx}: rc=$rc wall=$(cut -d= -f2 "$dest/clean.wall${sfx}.txt")s" \
    "answered_from=$(sed -n 's/^answered_from=//p' "$dest/clean.stats${sfx}.txt" | tail -n1)"
}

run_query_perf() {
  local dest=$1
  [[ "$DO_PERF" == "1" ]] || return 0
  local data="$dest/perf.data"
  local out="$QUERY_OUTPUT_SINK"
  [[ "$QUERY_LISTS" == "1" ]] || out=/dev/null
  echo "    query perf: process-scoped, output -> $out"
  if "${ENV_PREFIX[@]}" perf record --call-graph "$PERF_CALLGRAPH" -F "$PERF_FREQ" -o "$data" \
       "${QUERY_ARGV[@]}" >"$out" 2>"$dest/perf.record-stderr.txt"; then
    perf report -i "$data" --stdio 2>/dev/null >"$dest/perf.report.txt" || true
    perf report -i "$data" --stdio -g graph,0.5,caller 2>/dev/null >"$dest/perf.report.caller.txt" || true
    perf report -i "$data" --stdio --no-children --sort comm,pid \
      >"$dest/perf.report.bythread.txt" 2>"$dest/perf.report.bythread-stderr.txt" || true
  else
    echo "perf record failed; check perf.record-stderr.txt" >"$dest/perf.report.txt"
  fi
  rm -f "$data"
}

run_clean() {
  local dest=$1 bindir=$2 rep=$3
  local sfx=""; [[ "$REPS" -gt 1 ]] && sfx=".rep${rep}"
  build_argv "$bindir"
  echo "    analyze clean${sfx}: ${RUN_ARGV[*]}"
  # Own monotonic wall timer: /usr/bin/time may be absent, and ecrawl_query
  # does not print its own elapsed_sec.
  local t0 t1
  t0=$(date +%s.%N)
  if [[ "$HAVE_TIME" == "1" ]]; then
    "$TIME_BIN" -v -o "$dest/clean.time${sfx}.txt" \
      "${RUN_ARGV[@]}" >"$dest/clean.stats${sfx}.txt" 2>"$dest/clean.progress${sfx}.txt"
  else
    "${RUN_ARGV[@]}" >"$dest/clean.stats${sfx}.txt" 2>"$dest/clean.progress${sfx}.txt"
  fi
  local rc=$?
  t1=$(date +%s.%N)
  awk -v a="$t0" -v b="$t1" 'BEGIN{printf "wall_seconds=%.3f\n", b-a}' >"$dest/clean.wall${sfx}.txt"
  echo "rc=$rc" >>"$dest/clean.progress${sfx}.txt"
  echo "    analyze clean${sfx}: rc=$rc wall=$(cut -d= -f2 "$dest/clean.wall${sfx}.txt")s"
}

run_strace() {
  local dest=$1 bindir=$2
  [[ "$DO_STRACE" == "1" ]] || return 0
  build_argv "$bindir"
  echo "    analyze strace: strace -f -c (timing not representative)"
  strace -f -c -o "$dest/strace.txt" \
    "${RUN_ARGV[@]}" >/dev/null 2>"$dest/strace.analyze-stderr.txt"
}

run_perf() {
  local dest=$1 bindir=$2
  [[ "$DO_PERF" == "1" ]] || return 0
  build_argv "$bindir"
  echo "    analyze perf: perf record --call-graph $PERF_CALLGRAPH -F $PERF_FREQ"
  local data="$dest/perf.data"
  if perf record --call-graph "$PERF_CALLGRAPH" -F "$PERF_FREQ" -o "$data" \
       "${RUN_ARGV[@]}" >/dev/null 2>"$dest/perf.record-stderr.txt"; then
    perf report -i "$data" --stdio 2>/dev/null >"$dest/perf.report.txt" || \
      echo "perf report failed (see perf.record-stderr.txt)" >"$dest/perf.report.txt"
    perf report -i "$data" --stdio -g graph,0.5,caller 2>/dev/null \
      >"$dest/perf.report.caller.txt" || true
    # Per-thread CPU split from the same data (free): how evenly did the
    # configured workers actually burn CPU? Bare TIDs unless threads are named.
    # perf's per-thread sort key is `pid` (shows PID:TID); `tid` isn't accepted
    # on all builds. Keep stderr so an empty report stays diagnosable.
    perf report -i "$data" --stdio --no-children --sort comm,pid \
      >"$dest/perf.report.bythread.txt" 2>"$dest/perf.report.bythread-stderr.txt" || true
  else
    echo "perf record failed; check kernel.perf_event_paranoid and permissions." \
      >"$dest/perf.report.txt"
  fi
  rm -f "$data"
}

# Per-thread scheduling view (runtime / switches / delay) for big fixtures only.
# Confirms how many threads were genuinely on-CPU per section vs. waiting. The
# raw trace is large, so only the text summaries are kept.
run_sched() {
  local dest=$1 bindir=$2 fixture=$3
  [[ "$DO_SCHED" == "1" ]] || return 0
  case " $SCHED_FIXTURES " in *" $fixture "*) ;; *) return 0 ;; esac
  command -v perf >/dev/null 2>&1 || return 0
  build_argv "$bindir"
  echo "    analyze sched: perf sched record (per-thread concurrency; data not kept)"
  local data="$dest/perf.sched.data"
  if perf sched record -o "$data" \
       "${RUN_ARGV[@]}" >/dev/null 2>"$dest/perf.sched.record-stderr.txt"; then
    perf sched latency -i "$data" 2>/dev/null >"$dest/perf.sched.latency.txt" || true
    perf sched timehist -i "$data" --summary-only 2>/dev/null \
      >"$dest/perf.sched.summary.txt" \
      || perf sched timehist -i "$data" -s 2>/dev/null | tail -n 80 \
         >"$dest/perf.sched.summary.txt" || true
  else
    echo "perf sched record failed; needs sched tracepoints (perf_event_paranoid<=1 or root)." \
      >"$dest/perf.sched.latency.txt"
  fi
  rm -f "$data"
}

profile_one() {
  local fixture=$1
  local fxout="$BIN_ROOT/$fixture"
  local bindir="$fxout/bin"
  local dest="$RESULTS_DIR/$fixture"
  local adest="$dest/analyze"
  echo "==> $fixture  ($bindir)"
  local nsh
  nsh=$(shard_count "$bindir")
  if [[ "$nsh" -eq 0 ]]; then
    echo "ERROR: no uid_shard_*.bin under '$bindir' for fixture '$fixture'." >&2
    echo "       Run profile/ecrawl-fixtures.sh with the same <bin-root> first." >&2
    exit 1
  fi
  mkdir -p "$dest" "$adest"
  echo "uid_shard_files=$nsh" >"$dest/bins.txt"
  local r
  for ((r = 1; r <= REPS; r++)); do
    run_clean "$adest" "$bindir" "$r"
  done
  run_strace "$adest" "$bindir"
  run_perf   "$adest" "$bindir"
  run_sched  "$adest" "$bindir" "$fixture"
  if [[ "$DO_QUERY" == "1" ]]; then
    local root=""
    root=$(fixture_root "$bindir" || true)
    [[ -n "$root" ]] || echo "    note: no start_path in manifest; --subtree variants skipped"
    local spec vname venv vargs qdest
    for spec in "${QVARIANTS[@]}"; do
      [[ -n "$spec" ]] || continue
      IFS='|' read -r vname venv vargs <<<"$spec"
      if ! build_variant_argv "$vargs" "$bindir" "$root"; then
        echo "    query $vname: skipped (needs a crawl root)"
        continue
      fi
      build_variant_env "$venv"
      qdest="$dest/query-$vname"
      mkdir -p "$qdest"
      { echo "variant=$vname"; echo "env=${venv}"; echo "argv=${QUERY_ARGV[*]}"; } >"$qdest/variant.txt"
      for ((r = 1; r <= REPS; r++)); do
        run_query_clean "$qdest" "$r"
      done
      run_query_perf "$qdest"
    done
  fi
}

# ---- env snapshot ----------------------------------------------------------
{
  echo "# ecrawl_query fixture profile"
  echo "timestamp=$TS"
  echo "bin_root=$BIN_ROOT"
  echo "bin_root_fstype=$(fs_type "$BIN_ROOT")"
  echo "results_dir=$RESULTS_DIR"
  echo "ecrawl_query_bin=$ECRAWL_QUERY_BIN"
  echo "config: analyze_threads=$ECRAWL_QUERY_THREADS analyze_top=${ANALYZE_TOP:-off}"
  echo "modes: strace=$DO_STRACE perf=$DO_PERF sched=$DO_SCHED reps=$REPS"
  echo "query: enabled=$DO_QUERY size_gt=$QUERY_SIZE_GT type=$QUERY_TYPE gid=$QUERY_GID sink=$QUERY_OUTPUT_SINK"
  echo "query_variants: $(printf '%s ' "${QVARIANTS[@]%%|*}")"
  echo "sched_fixtures: $SCHED_FIXTURES"
  echo "fixtures: ${FIXLIST[*]}"
  echo
  echo "## host"
  echo "uname=$(uname -a)"
  echo "nproc=$(nproc 2>/dev/null || echo '?')"
  echo "ulimit_n=$(ulimit -n)"
  echo "cpu_model=$(awk -F: '/model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null | sed 's/^ //')"
  echo "mem_total_kb=$(awk '/MemTotal/{print $2; exit}' /proc/meminfo 2>/dev/null)"
  echo "perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo '?')"
  echo "yama_ptrace_scope=$(cat /proc/sys/kernel/yama/ptrace_scope 2>/dev/null || echo '?')"
  echo
  echo "## tools"
  echo "time=$TIME_BIN strace=$HAVE_STRACE perf=$HAVE_PERF"
  [[ "$HAVE_STRACE" == "1" ]] && echo "strace_version=$(strace -V 2>&1 | head -n1)"
  [[ "$HAVE_PERF"   == "1" ]] && echo "perf_version=$(perf --version 2>&1 | head -n1)"
  echo
  echo "## ecrawl_query-related env"
  env | grep -E '^ECRAWL_QUERY_|^ECRAWL_REPAIR_' | sort || true
} >"$RESULTS_DIR/env.txt"

echo "ecrawl_query profile starting; results -> $RESULTS_DIR"
sed 's/^/  /' "$RESULTS_DIR/env.txt"
echo

if [[ "$DO_PERF" == "1" ]]; then
  pp=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)
  if [[ "$pp" =~ ^-?[0-9]+$ && "$pp" -gt 1 && "$(id -u)" != "0" ]]; then
    echo "WARNING: perf_event_paranoid=$pp may block perf for non-root; run as root or lower it." >&2
  fi
fi

# ---- run -------------------------------------------------------------------
for f in "${FIXLIST[@]}"; do
  profile_one "$f"
done
if [[ "$INCLUDE_ROOT" == "1" && -d "$BIN_ROOT/_ALL_ROOT_/bin" ]]; then
  profile_one "_ALL_ROOT_"
fi

# ---- combined report -------------------------------------------------------
COMBINED="$RESULTS_DIR/COMBINED_REPORT.txt"
{
  echo "########################################################################"
  echo "# ecrawl_query fixture profile — combined report"
  echo "########################################################################"
  echo
  cat "$RESULTS_DIR/env.txt"
  echo
  for f in "${FIXLIST[@]}"; do
    [[ -d "$RESULTS_DIR/$f" ]] || continue
    echo "========================================================================"
    echo "FIXTURE: $f"
    echo "========================================================================"
    d="$RESULTS_DIR/$f/analyze"
    if [[ -d "$d" ]]; then
      echo "--- full-scan pass (analyze/) ---"
      echo
      for part in clean.stats clean.time; do
        for file in "$d/$part"*.txt; do
          [[ -e "$file" ]] || continue
          echo "----- $(basename "$file") -----"; cat "$file"; echo
        done
      done
      if [[ -s "$d/strace.txt" ]]; then
        echo "----- strace.txt (syscall histogram) -----"; cat "$d/strace.txt"; echo
      fi
      if [[ -s "$d/perf.report.txt" ]]; then
        echo "----- perf.report.txt (top 40 lines) -----"; head -n 40 "$d/perf.report.txt"; echo
      fi
      if [[ -s "$d/perf.report.bythread.txt" ]]; then
        echo "----- perf.report.bythread.txt (top 40 lines) -----"; head -n 40 "$d/perf.report.bythread.txt"; echo
      fi
      if [[ -s "$d/perf.sched.latency.txt" ]]; then
        echo "----- perf.sched.latency.txt -----"; cat "$d/perf.sched.latency.txt"; echo
      fi
    fi
    # The selective queries are the hot paths the columnar work targets, so each
    # variant's profile belongs in the uploaded report next to the full scan.
    for q in "$RESULTS_DIR/$f"/query-*; do
      [[ -d "$q" ]] || continue
      vn=$(basename "$q")
      echo "--- selective query pass ($vn) ---"
      [[ -s "$q/variant.txt" ]] && sed 's/^/    /' "$q/variant.txt"
      echo
      for part in clean.stats clean.time; do
        for file in "$q/$part"*.txt; do
          [[ -e "$file" ]] || continue
          echo "----- $vn/$(basename "$file") -----"; cat "$file"; echo
        done
      done
      if [[ -s "$q/perf.report.txt" ]]; then
        echo "----- $vn/perf.report.txt (top 40 lines) -----"; head -n 40 "$q/perf.report.txt"; echo
      fi
      if [[ -s "$q/perf.report.bythread.txt" ]]; then
        echo "----- $vn/perf.report.bythread.txt (top 40 lines) -----"
        head -n 40 "$q/perf.report.bythread.txt"; echo
      fi
    done
  done
} >"$COMBINED"

# ---- summary table ---------------------------------------------------------
SUMMARY_TABLE="$RESULTS_DIR/SUMMARY_TABLE.txt"
if command -v python3 >/dev/null 2>&1; then
  RESULTS_DIR="$RESULTS_DIR" FIXLIST_STR="${FIXLIST[*]}" \
    QUERY_VARIANT_NAMES="$(printf '%s ' "${QVARIANTS[@]%%|*}")" \
    python3 - <<'PY' >"$SUMMARY_TABLE" 2>/dev/null
import os, re
from pathlib import Path

root = Path(os.environ["RESULTS_DIR"])
fixtures = os.environ.get("FIXLIST_STR", "").split()
variants = os.environ.get("QUERY_VARIANT_NAMES", "").split()
if (root / "_ALL_ROOT_").is_dir() and "_ALL_ROOT_" not in fixtures:
    fixtures.append("_ALL_ROOT_")


def num(s):
    if s is None or s == "":
        return None
    s = s.strip()
    m = {"K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}
    if s and s[-1] in m:
        try:
            return float(s[:-1]) * m[s[-1]]
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def kv(p):
    d = {}
    if p.exists():
        for line in p.read_text(errors="replace").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                d[k.strip()] = v.strip()
    return d


def analyze_kv(fx):
    return pass_kv(fx, "analyze")


def pass_kv(fx, sub):
    for cand in ("clean.stats.rep1.txt", "clean.stats.txt"):
        p = root / fx / sub / cand
        if p.exists():
            return kv(p)
    return {}


def reps_elapsed(fx, sub="analyze"):
    """Wall time from our own timer (clean.wall*), per rep."""
    out = []
    for r in range(1, 100):
        p = root / fx / sub / f"clean.wall.rep{r}.txt"
        if not p.exists():
            break
        v = num(kv(p).get("wall_seconds", ""))
        if v is not None:
            out.append(v)
    if not out:
        p = root / fx / sub / "clean.wall.txt"
        if p.exists():
            v = num(kv(p).get("wall_seconds", ""))
            if v is not None:
                out = [v]
    return out


def strace_top(fx, k=6):
    p = root / fx / "analyze" / "strace.txt"
    if not p.exists():
        return []
    rows = []
    for line in p.read_text(errors="replace").splitlines():
        m = re.match(r"\s*([\d.]+)\s+[\d.]+\s+\d+\s+(\d+)\s+(?:\d+\s+)?(\w+)\s*$", line)
        if m:
            rows.append((float(m.group(1)), m.group(3), int(m.group(2))))
    rows.sort(reverse=True)
    return rows[:k]


def perf_threads(fx, min_pct=1.0, top=5):
    """Per-thread on-CPU spread from perf.report.bythread.txt.

    Returns (n_threads_ge_min, top_pcts): how many distinct threads carried
    >=min_pct of CPU samples, and the busiest per-thread percentages. Workers
    are capped at the parse-chunk count, so a single big shard (many .ckpt
    chunks) still spreads across threads; only a single-chunk shard shows 1.
    """
    p = root / fx / "analyze" / "perf.report.bythread.txt"
    if not p.exists():
        return None
    pcts = []
    for line in p.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"([\d.]+)%\s+\S+\s+\d+", line)
        if m:
            pcts.append(float(m.group(1)))
    if not pcts:
        return None
    pcts.sort(reverse=True)
    n_ge = sum(1 for x in pcts if x >= min_pct)
    return (n_ge, pcts[:top])


def fmt(v, kind="int"):
    if v is None:
        return "-"
    if kind == "int":
        return f"{int(round(v)):,}"
    if kind == "f1":
        return f"{v:.1f}"
    if kind == "f2":
        return f"{v:.2f}"
    return str(v)


lines = []
hdr = kv(root / "env.txt")
lines.append("ecrawl_query profile — SUMMARY TABLE")
lines.append(f"  timestamp={hdr.get('timestamp','?')}  bin_root={hdr.get('bin_root','?')}")
lines.append(f"  host: nproc={hdr.get('nproc','?')} ulimit_n={hdr.get('ulimit_n','?')} fstype={hdr.get('bin_root_fstype','?')}")
for key in ("config:", "modes:"):
    envf = root / "env.txt"
    if envf.exists():
        for l in envf.read_text(errors="replace").splitlines():
            if l.startswith(key):
                lines.append(f"  {l.strip()}")
                break
lines.append("")

# --- main timing table ----------------------------------------------------
cols = ["fixture", "shards", "analyze(s)", "records",
        "parents", "maxNfile", "chunks"]
w = [22, 8, 11, 14, 14, 12, 11]


def row(vals):
    return "  ".join(str(v).ljust(w[i]) for i, v in enumerate(vals))


lines.append("== ANALYZE WALL TIME ==")
lines.append(row(cols))
lines.append(row(["-" * x for x in w]))
for fx in fixtures:
    av = analyze_kv(fx)
    if not av:
        continue
    el = reps_elapsed(fx)
    el_avg = (sum(el) / len(el)) if el else None
    lines.append(row([
        fx,
        kv(root / fx / "bins.txt").get("uid_shard_files", "-"),
        fmt(el_avg, "f2") if el_avg is not None else "-",
        fmt(num(av.get("records_total"))),
        fmt(num(av.get("distinct_parent_directories"))),
        fmt(num(av.get("max_regular_files_under_single_parent"))),
        fmt(num(av.get("parse_chunk_jobs"))),
    ]))
lines.append("")

# --- selective query variants ---------------------------------------------
# blks_dec/blks_skip come from ecrawl_query's own counters: a skipped row group
# is one whose column zone maps proved no record inside could match, so it was
# seeked past instead of decompressed. skip% is the share of groups avoided.
# answered says whether the catalog rollups or a record scan produced the answer.
qcols = ["fixture", "variant", "query(s)", "entries", "scanned",
         "blks_dec", "blks_skip", "skip%", "answered"]
qw = [20, 22, 10, 12, 14, 10, 10, 7, 16]


def qrow(vals):
    return "  ".join(str(v).ljust(qw[i]) for i, v in enumerate(vals))


def variant_stats(fx, vname):
    """(avg wall seconds, key=value dict) for one variant, or (None, {})."""
    sub = f"query-{vname}"
    qv = pass_kv(fx, sub)
    if not qv:
        return None, {}
    el = reps_elapsed(fx, sub)
    return (sum(el) / len(el)) if el else None, qv


qrows = []
for fx in fixtures:
    for vname in variants:
        el_avg, qv = variant_stats(fx, vname)
        if not qv:
            continue
        dec = num(qv.get("blocks_decompressed"))
        skip = num(qv.get("blocks_skipped"))
        pct = None
        if dec is not None and skip is not None and (dec + skip) > 0:
            pct = 100.0 * skip / (dec + skip)
        qrows.append(qrow([
            fx,
            vname,
            fmt(el_avg, "f2") if el_avg is not None else "-",
            fmt(num(qv.get("entries"))),
            fmt(num(qv.get("records_scanned"))),
            fmt(dec),
            fmt(skip),
            fmt(pct, "f1") if pct is not None else "-",
            qv.get("answered_from", "-"),
        ]))
if qrows:
    lines.append("== SELECTIVE QUERY VARIANTS ==")
    lines.append(qrow(qcols))
    lines.append(qrow(["-" * x for x in qw]))
    lines.extend(qrows)
    lines.append("")

# --- A/B deltas -------------------------------------------------------------
# Several variants exist only as each other's control. Doing the subtraction here
# is the difference between a table of numbers and an answer, and it keeps the
# claim honest: if a control is missing the row says so instead of being dropped.
PAIRS = [
    ("subtree-rollup", "subtree-exact",
     "catalog rollup vs forced record scan (the v8 subtree win)"),
    ("size-matchall-nolist", "size-matchall",
     "cost of --list: path reconstruction plus the output write"),
    ("size-matchall-nolist", "size-matchall-noskip",
     "what row group zone-map skipping is worth"),
    ("size-skipall", "size-matchall",
     "everything pruned vs nothing pruned (selectivity spread)"),
]
ab = []
for fast, slow, why in PAIRS:
    rows = []
    for fx in fixtures:
        f_el, f_kv = variant_stats(fx, fast)
        s_el, s_kv = variant_stats(fx, slow)
        if not f_kv and not s_kv:
            continue
        if f_el is None or s_el is None:
            rows.append(f"  {fx}: incomplete ({fast}="
                        f"{'-' if f_el is None else f'{f_el:.2f}s'}, {slow}="
                        f"{'-' if s_el is None else f'{s_el:.2f}s'})")
            continue
        speed = (s_el / f_el) if f_el > 0 else None
        rows.append(f"  {fx}: {fast}={f_el:.3f}s  {slow}={s_el:.3f}s  "
                    + (f"{speed:.1f}x faster" if speed else "n/a"))
    if rows:
        ab.append(f"  {fast} vs {slow} — {why}")
        ab.extend(rows)
        ab.append("")
if ab:
    lines.append("== A/B DELTAS ==")
    lines.extend(ab)

# --- per-thread on-CPU spread (perf) --------------------------------------
# threads_>=1% = how many distinct threads each carried >=1% of CPU samples.
# ecrawl_query caps workers at the parse-chunk count, so a single big shard
# (many .ckpt chunks) still spreads across threads; a single-chunk shard shows 1.
lines.append("== PER-THREAD ON-CPU SPREAD (perf; whole run) ==")
lines.append("   threads>=1% (workers capped at parse-chunk count); top% = busiest threads")
for fx in fixtures:
    pt = perf_threads(fx)
    if not pt:
        continue
    n_ge, tops = pt
    tops_s = " ".join(f"{x:.1f}%" for x in tops)
    shards = kv(root / fx / "bins.txt").get("uid_shard_files", "?")
    lines.append(f"  {fx}: threads>=1%={n_ge} (shards={shards})  top={tops_s}")
lines.append("")

# --- strace top syscalls (analyze pass) -----------------------------------
strace_rows = []
for fx in fixtures:
    top = strace_top(fx)
    if not top:
        continue
    parts = ", ".join(f"{name} {pct:.1f}%/{cnt:,}" for pct, name, cnt in top)
    strace_rows.append(f"  {fx}: {parts}")
# With DO_STRACE=0 there is nothing to show, so skip the header entirely.
if strace_rows:
    lines.append("== STRACE TOP SYSCALLS (analyze pass; %time, count) ==")
    lines.extend(strace_rows)
    lines.append("")

# --- elapsed per rep ------------------------------------------------------
lines.append("== ANALYZE ELAPSED PER REP ==")
for fx in fixtures:
    el = reps_elapsed(fx)
    if el:
        avg = sum(el) / len(el)
        lines.append(f"  {fx}: {[round(x,2) for x in el]} avg={avg:.2f}s")

qper = []
for fx in fixtures:
    for vname in variants:
        el = reps_elapsed(fx, f"query-{vname}")
        if el:
            avg = sum(el) / len(el)
            qper.append(f"  {fx} / {vname}: {[round(x,2) for x in el]} avg={avg:.2f}s")
if qper:
    lines.append("")
    lines.append("== QUERY ELAPSED PER REP ==")
    lines.extend(qper)

print("\n".join(lines))
PY
fi
if [[ -s "$SUMMARY_TABLE" ]]; then
  { cat "$SUMMARY_TABLE"; echo; echo; cat "$COMBINED"; } >"$COMBINED.tmp" && mv "$COMBINED.tmp" "$COMBINED"
else
  echo "(SUMMARY_TABLE skipped: python3 unavailable or parse failed)" >"$SUMMARY_TABLE"
fi

# ---- tarball ---------------------------------------------------------------
TARBALL="$RESULTS_DIR/../$(basename "$RESULTS_DIR").tar.gz"
if tar -czf "$TARBALL" -C "$(dirname "$RESULTS_DIR")" "$(basename "$RESULTS_DIR")" 2>/dev/null; then
  TARBALL=$(cd "$(dirname "$TARBALL")" && pwd)/$(basename "$TARBALL")
  echo
  echo "DONE."
  echo "  Summary table:   $SUMMARY_TABLE"
  echo "  Combined report: $COMBINED"
  echo "  Data (read-only): bins consumed from $BIN_ROOT/<fixture>/bin"
  echo "  Tarball (upload this): $TARBALL"
  echo
  echo "----- SUMMARY_TABLE.txt -----"
  cat "$SUMMARY_TABLE"
else
  echo
  echo "DONE (tarball failed; upload $RESULTS_DIR or COMBINED_REPORT.txt)."
  echo "  Summary table:   $SUMMARY_TABLE"
fi
