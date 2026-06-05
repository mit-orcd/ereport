#!/usr/bin/env bash
#
# profile-ecrawl-fixtures.sh
#
# SPDX-License-Identifier: MIT
#
# Run ecrawl against each sub-directory of a synthetic adversarial tree
# (see generate-ecrawl-adversarial-tree.sh) and capture a full performance
# profile per fixture, so the fast/slow behaviour can be analysed offline.
#
# For each fixture it can run up to three instrumented passes:
#   1. clean   — /usr/bin/time -v + ecrawl --verbose summary. This is the ONLY
#                pass whose timing is trustworthy.
#   2. strace  — strace -f -c (syscall histogram; getdents/openat/close/
#                newfstatat counts). Heavy instrumentation: timing is NOT
#                representative, only the syscall mix is.
#   3. perf    — perf record -g + perf report (CPU profile). Optional; symbols
#                are best if ecrawl is built with -g.
#
# Each pass runs in both --no-write mode (isolates crawl/readdir/donation cost)
# and write mode (exposes uid-shard writer churn) unless disabled.
#
# This is also the PRODUCER of the shared per-fixture shard sets reused by the
# follow-up profilers (profile-ereport*, profile-ecrawl_analyze). The write-mode
# clean pass (rep 1) crawls each fixture into a persistent, kept bin dir:
#       <bin-root>/<fixture>/bin/uid_shard_*.bin
# Point those follow-up scripts at the same <bin-root>; they consume these bins
# and never crawl themselves. (Needs DO_WRITE=1, the default, to populate bins.)
#
# Usage:
#   scripts/profile-ecrawl-fixtures.sh <synth-root> <bin-root> [results-dir]
#
# Required:
#   <synth-root>   path passed to generate-ecrawl-adversarial-tree.sh
#                  (the dir containing single_huge_dir/, mega_dir1/, ...).
#   <bin-root>     parent dir where the kept, reusable shard sets are written:
#                  for each fixture, <bin-root>/<fixture>/bin/uid_shard_*.bin.
#                  Pass the SAME path to the follow-up profilers. Overwritten
#                  (re-crawled fresh) on every write-mode run.
#
# Optional positional:
#   [results-dir]  where to write logs (default: ./ecrawl-profile-<timestamp>).
#
# Environment knobs (all optional):
#   ECRAWL_BIN=./ecrawl        path to the ecrawl binary (auto-detects ./ecrawl,
#                              /tmp/ecrawl, or $PATH).
#   OUTPUT_BASE=<dir>          where THROWAWAY write-mode shard output goes for
#                              extra reps and the strace/perf/sched passes (needs
#                              space; default: <results-dir>/_shard_output).
#                              Cleared between runs. The kept bins go to
#                              <bin-root> instead. Ignored for --no-write.
#   FIXTURES="a b c"           space-separated fixture names to run (default:
#                              auto-detect the known set, else all immediate
#                              subdirs of <synth-root>).
#   INCLUDE_ROOT=1             also profile the whole <synth-root> as one run.
#   DO_NOWRITE=1               run the --no-write pass (default 1).
#   DO_WRITE=1                 run the write pass (default 1).
#   DO_STRACE=1                run the strace -f -c pass (default 1).
#   DO_PERF=0                  run the perf record pass (default 0; set 1 to enable).
#   DO_SCHED=0                 run a `perf sched` pass (per-thread runtime /
#                              switch / delay) — only for fixtures listed in
#                              SCHED_FIXTURES, since the trace is large. Use it
#                              to confirm per-section thread concurrency.
#   SCHED_FIXTURES="single_huge_dir mega_dir1 mega_dir2"  fixtures big enough
#                              for a meaningful perf sched trace.
#   PERF_FREQ=997              perf sampling frequency (Hz).
#   DROP_CACHES=1              when running as root, drop the page cache before
#                              every ecrawl invocation (clean, strace, perf,
#                              sched) via `sync; echo 3 > drop_caches`, so each
#                              run is measured cold — crawl speed is dominated by
#                              readdir/stat metadata fetch, which is meaningless
#                              against a warm cache. Default 1. Set 0 for warm
#                              (in-cache) runs. If not root, the script warns once
#                              and proceeds warm.
#   REPS=1                     repetitions of the clean pass per fixture/mode.
#   ECRAWL_VERBOSE_ARGS=...    extra args appended to ecrawl (e.g. extra flags).
#   Any ECRAWL_* env (ECRAWL_MAX_OPEN_SHARDS, ECRAWL_CRAWL_THREADS, ...) is
#   inherited by every ecrawl run and recorded in env.txt.
#
# Output layout (under <results-dir>):
#   env.txt                              system + build + env snapshot
#   <fixture>/<mode>/clean.summary.txt   ecrawl --verbose stdout (key=value)
#   <fixture>/<mode>/clean.stderr.txt    stderr (stall hints, warnings)
#   <fixture>/<mode>/clean.time.txt      /usr/bin/time -v
#   <fixture>/<mode>/shards.txt          uid_shard_*.bin count (write mode only)
#   <bin-root>/<fixture>/bin/            kept, reusable uid_shard_*.bin (write mode)
#   <fixture>/<mode>/strace.txt          strace -f -c histogram
#   <fixture>/<mode>/perf.report.txt     perf report --stdio (if DO_PERF=1)
#   <fixture>/<mode>/perf.<mode>.report.bythread.txt per-thread CPU split (thread use)
#   <fixture>/<mode>/perf.<mode>.sched.latency.txt   per-thread sched runtime/delay (DO_SCHED)
#   <fixture>/<mode>/perf.<mode>.sched.summary.txt   perf sched timehist summary (DO_SCHED)
#   COMBINED_REPORT.txt                  everything concatenated for easy upload
#   ecrawl-profile-<timestamp>.tar.gz    tarball of the whole results dir
#
set -uo pipefail

# ---- args ------------------------------------------------------------------
if [[ $# -lt 2 ]]; then
  echo "usage: $0 <synth-root> <bin-root> [results-dir]" >&2
  exit 2
fi
SYNTH_ROOT=${1%/}
if [[ ! -d "$SYNTH_ROOT" ]]; then
  echo "ERROR: synth-root '$SYNTH_ROOT' is not a directory" >&2
  exit 2
fi

# Persistent home for the kept, reusable per-fixture shard sets that the
# follow-up profilers consume: <bin-root>/<fixture>/bin/uid_shard_*.bin.
BIN_ROOT=${2%/}
mkdir -p "$BIN_ROOT" || { echo "ERROR: cannot create bin-root '$BIN_ROOT'" >&2; exit 1; }
BIN_ROOT=$(cd "$BIN_ROOT" && pwd)

TS=$(date +%Y%m%d-%H%M%S)
RESULTS_DIR=${3:-"./ecrawl-profile-$TS"}
mkdir -p "$RESULTS_DIR" || { echo "ERROR: cannot create results dir '$RESULTS_DIR'" >&2; exit 1; }
RESULTS_DIR=$(cd "$RESULTS_DIR" && pwd)

# ---- config ----------------------------------------------------------------
ECRAWL_BIN=${ECRAWL_BIN:-}
if [[ -z "$ECRAWL_BIN" ]]; then
  if [[ -x ./ecrawl ]]; then ECRAWL_BIN=$(cd "$(dirname ./ecrawl)" && pwd)/ecrawl
  elif [[ -x /tmp/ecrawl ]]; then ECRAWL_BIN=/tmp/ecrawl
  elif command -v ecrawl >/dev/null 2>&1; then ECRAWL_BIN=$(command -v ecrawl)
  else echo "ERROR: cannot find ecrawl; set ECRAWL_BIN=/path/to/ecrawl" >&2; exit 1
  fi
fi
if [[ ! -x "$ECRAWL_BIN" ]]; then
  echo "ERROR: ECRAWL_BIN '$ECRAWL_BIN' is not executable" >&2
  exit 1
fi

OUTPUT_BASE=${OUTPUT_BASE:-"$RESULTS_DIR/_shard_output"}
DO_NOWRITE=${DO_NOWRITE:-1}
DO_WRITE=${DO_WRITE:-1}
DO_STRACE=${DO_STRACE:-1}
DO_PERF=${DO_PERF:-0}
DO_SCHED=${DO_SCHED:-0}
SCHED_FIXTURES=${SCHED_FIXTURES:-"single_huge_dir mega_dir1 mega_dir2"}
PERF_FREQ=${PERF_FREQ:-997}
DROP_CACHES=${DROP_CACHES:-1}
REPS=${REPS:-1}
INCLUDE_ROOT=${INCLUDE_ROOT:-0}
ECRAWL_VERBOSE_ARGS=${ECRAWL_VERBOSE_ARGS:-}

# Known fixtures emitted by generate-ecrawl-adversarial-tree.sh, ordered so the
# cheap/fast ones run before the multi-minute mega dirs.
KNOWN_FIXTURES=(
  deep_skinny_chain
  depth_slash_profile
  wide_shallow
  ereport_badge_fixtures
  neutral_flat
  single_huge_dir
  mega_dir2
  mega_dir1
)

if [[ -n "${FIXTURES:-}" ]]; then
  read -ra FIXLIST <<<"$FIXTURES"
else
  FIXLIST=()
  for f in "${KNOWN_FIXTURES[@]}"; do
    [[ -d "$SYNTH_ROOT/$f" ]] && FIXLIST+=("$f")
  done
  if [[ ${#FIXLIST[@]} -eq 0 ]]; then
    # Fallback: every immediate subdirectory.
    while IFS= read -r d; do
      FIXLIST+=("$(basename "$d")")
    done < <(find "$SYNTH_ROOT" -mindepth 1 -maxdepth 1 -type d | sort)
  fi
fi
if [[ ${#FIXLIST[@]} -eq 0 ]]; then
  echo "ERROR: no fixtures found under '$SYNTH_ROOT'" >&2
  exit 1
fi

# ---- tool availability -----------------------------------------------------
HAVE_TIME=0; TIME_BIN=""
for c in /usr/bin/time /bin/time; do
  [[ -x "$c" ]] && { TIME_BIN="$c"; HAVE_TIME=1; break; }
done
HAVE_STRACE=0; command -v strace >/dev/null 2>&1 && HAVE_STRACE=1
HAVE_PERF=0;   command -v perf   >/dev/null 2>&1 && HAVE_PERF=1
HAVE_NUMFMT=0; command -v numfmt >/dev/null 2>&1 && HAVE_NUMFMT=1

if [[ "$DO_STRACE" == "1" && "$HAVE_STRACE" != "1" ]]; then
  echo "WARNING: strace not found; skipping strace pass." >&2
  DO_STRACE=0
fi
if [[ "$DO_PERF" == "1" && "$HAVE_PERF" != "1" ]]; then
  echo "WARNING: perf not found; skipping perf pass." >&2
  DO_PERF=0
fi

# ---- helpers ---------------------------------------------------------------
fs_type() { stat -f -c '%T' "$1" 2>/dev/null || echo "?"; }

g_warned_no_drop=0
maybe_drop_caches() {
  [[ "$DROP_CACHES" == "1" ]] || return 0
  if [[ "$(id -u)" == "0" ]]; then
    sync
    if echo 3 >/proc/sys/vm/drop_caches 2>/dev/null; then
      echo "    [cache dropped]"
    else
      echo "    [WARN: could not drop caches]"
    fi
  elif [[ "$g_warned_no_drop" == "0" ]]; then
    # Warn once, not on every pass: with DROP_CACHES on by default this would
    # otherwise print for each fixture/mode/pass.
    echo "    [WARN: DROP_CACHES=1 but not root; caches left warm — crawl timings are in-cache]"
    g_warned_no_drop=1
  fi
}

# Build the ecrawl argv for a given start path / mode into global RUN_ARGV.
build_argv() {
  local mode=$1 start=$2 outdir=$3
  RUN_ARGV=("$ECRAWL_BIN" --verbose)
  if [[ "$mode" == "nowrite" ]]; then
    RUN_ARGV=("$ECRAWL_BIN" --no-write --verbose)
  fi
  # shellcheck disable=SC2206
  [[ -n "$ECRAWL_VERBOSE_ARGS" ]] && RUN_ARGV+=($ECRAWL_VERBOSE_ARGS)
  RUN_ARGV+=("$start")
  [[ "$mode" == "write" ]] && RUN_ARGV+=("$outdir")
}

# clean pass: honest timing + verbose summary + CSVs.
# keep=1 (write-mode rep 1) persists the crawl into <bin-root>/<fixture>/bin so
# the follow-up profilers can reuse it; keep=0 uses a throwaway dir.
run_clean() {
  local fixture=$1 mode=$2 start=$3 dest=$4 outdir=$5 rep=$6 keep=${7:-0}
  local sfx=""
  [[ "$REPS" -gt 1 ]] && sfx=".rep${rep}"

  if [[ "$mode" == "write" ]]; then
    rm -rf "$outdir"; mkdir -p "$outdir"
  fi
  build_argv "$mode" "$start" "$outdir"
  maybe_drop_caches

  echo "    clean/$mode${sfx}: ${RUN_ARGV[*]}"
  if [[ "$HAVE_TIME" == "1" ]]; then
    "$TIME_BIN" -v -o "$dest/clean.time${sfx}.txt" \
      "${RUN_ARGV[@]}" >"$dest/clean.summary${sfx}.txt" 2>"$dest/clean.stderr${sfx}.txt"
  else
    { \
      /usr/bin/env bash -c 'st=$(date +%s.%N); "$@"; rc=$?; en=$(date +%s.%N); \
        echo "wall_seconds=$(awk -v a=$st -v b=$en "BEGIN{printf \"%.3f\", b-a}")" >&2; exit $rc' \
      _ "${RUN_ARGV[@]}" >"$dest/clean.summary${sfx}.txt" 2>"$dest/clean.stderr${sfx}.txt"; }
  fi
  local rc=$?
  echo "rc=$rc" >>"$dest/clean.stderr${sfx}.txt"

  if [[ "$mode" == "write" ]]; then
    local nsh
    nsh=$(find "$outdir" -maxdepth 1 -name 'uid_shard_*.bin' 2>/dev/null | wc -l)
    {
      echo "uid_shard_files_created=$nsh"
      echo "output_dir_size=$(du -sh "$outdir" 2>/dev/null | cut -f1)"
      [[ "$keep" == "1" ]] && echo "kept_bin_dir=$outdir"
    } >"$dest/shards${sfx}.txt"
    if [[ "$keep" == "1" ]]; then
      echo "    kept bins: $nsh shard(s) -> $outdir"
    else
      # Reclaim space immediately; throwaway shards can be large for mega dirs.
      rm -rf "$outdir"
    fi
  fi
}

# strace pass: syscall histogram. Timing intentionally ignored.
run_strace() {
  local mode=$1 start=$2 dest=$3 outdir=$4
  [[ "$DO_STRACE" == "1" ]] || return 0
  if [[ "$mode" == "write" ]]; then rm -rf "$outdir"; mkdir -p "$outdir"; fi
  build_argv "$mode" "$start" "$outdir"
  maybe_drop_caches
  echo "    strace/$mode: strace -f -c (timing not representative)"
  strace -f -c -o "$dest/strace.${mode}.txt" \
    "${RUN_ARGV[@]}" >/dev/null 2>"$dest/strace.${mode}.ecrawl-stderr.txt"
  [[ "$mode" == "write" ]] && rm -rf "$outdir"
}

# perf pass: CPU profile.
run_perf() {
  local mode=$1 start=$2 dest=$3 outdir=$4
  [[ "$DO_PERF" == "1" ]] || return 0
  if [[ "$mode" == "write" ]]; then rm -rf "$outdir"; mkdir -p "$outdir"; fi
  build_argv "$mode" "$start" "$outdir"
  # DWARF call graphs so stacks resolve even when ecrawl is built -O2 without frame pointers.
  # Set PERF_CALLGRAPH=fp if the binary is built with -fno-omit-frame-pointer (cheaper, smaller).
  local cg=${PERF_CALLGRAPH:-dwarf}
  maybe_drop_caches
  echo "    perf/$mode: perf record --call-graph $cg -F $PERF_FREQ"
  local data="$dest/perf.${mode}.data"
  if perf record --call-graph "$cg" -F "$PERF_FREQ" -o "$data" \
       "${RUN_ARGV[@]}" >/dev/null 2>"$dest/perf.${mode}.record-stderr.txt"; then
    perf report -i "$data" --stdio 2>/dev/null >"$dest/perf.${mode}.report.txt" || \
      echo "perf report failed (see perf.${mode}.record-stderr.txt)" >"$dest/perf.${mode}.report.txt"
    # Caller/callee + a futex/lock-focused view to pinpoint the contended critical section.
    perf report -i "$data" --stdio -g graph,0.5,caller 2>/dev/null \
      >"$dest/perf.${mode}.report.caller.txt" || true
    # Per-thread CPU split from the same data (free): how evenly did the crawl /
    # writer threads actually burn CPU? Bare TIDs unless threads are named.
    # perf's per-thread sort key is `pid` (shows PID:TID); `tid` isn't accepted
    # on all builds. Keep stderr so an empty report stays diagnosable.
    perf report -i "$data" --stdio --no-children --sort comm,pid \
      >"$dest/perf.${mode}.report.bythread.txt" 2>"$dest/perf.${mode}.report.bythread-stderr.txt" || true
  else
    echo "perf record failed; check kernel.perf_event_paranoid and permissions." \
      >"$dest/perf.${mode}.report.txt"
  fi
  rm -f "$data"
  [[ "$mode" == "write" ]] && rm -rf "$outdir"
}

# Per-thread scheduling view (runtime / switches / delay) for big fixtures only.
# Confirms how many threads were genuinely on-CPU per section vs. waiting. The
# raw trace is large, so only the text summaries are kept.
run_sched() {
  local fixture=$1 mode=$2 start=$3 dest=$4 outdir=$5
  [[ "$DO_SCHED" == "1" ]] || return 0
  case " $SCHED_FIXTURES " in *" $fixture "*) ;; *) return 0 ;; esac
  command -v perf >/dev/null 2>&1 || return 0
  if [[ "$mode" == "write" ]]; then rm -rf "$outdir"; mkdir -p "$outdir"; fi
  build_argv "$mode" "$start" "$outdir"
  maybe_drop_caches
  echo "    sched/$mode: perf sched record (per-thread concurrency; data not kept)"
  local data="$dest/perf.${mode}.sched.data"
  if perf sched record -o "$data" \
       "${RUN_ARGV[@]}" >/dev/null 2>"$dest/perf.${mode}.sched.record-stderr.txt"; then
    perf sched latency -i "$data" 2>/dev/null >"$dest/perf.${mode}.sched.latency.txt" || true
    perf sched timehist -i "$data" --summary-only 2>/dev/null \
      >"$dest/perf.${mode}.sched.summary.txt" \
      || perf sched timehist -i "$data" -s 2>/dev/null | tail -n 80 \
         >"$dest/perf.${mode}.sched.summary.txt" || true
  else
    echo "perf sched record failed; needs sched tracepoints (perf_event_paranoid<=1 or root)." \
      >"$dest/perf.${mode}.sched.latency.txt"
  fi
  rm -f "$data"
  [[ "$mode" == "write" ]] && rm -rf "$outdir"
}

profile_one() {
  local fixture=$1 start=$2
  echo "==> $fixture  ($start)"
  local modes=()
  [[ "$DO_NOWRITE" == "1" ]] && modes+=("nowrite")
  [[ "$DO_WRITE"   == "1" ]] && modes+=("write")
  if [[ ${#modes[@]} -eq 0 ]]; then
    echo "    (no modes enabled; set DO_NOWRITE=1 or DO_WRITE=1)"; return
  fi
  local mode
  for mode in "${modes[@]}"; do
    local dest="$RESULTS_DIR/$fixture/$mode"
    mkdir -p "$dest"
    local throwaway="$OUTPUT_BASE/$fixture-$mode"
    local persist="$BIN_ROOT/$fixture/bin"
    local r
    for ((r = 1; r <= REPS; r++)); do
      # write-mode rep 1 produces the kept, reusable bin set; everything else
      # (nowrite, extra reps) crawls into the throwaway dir.
      if [[ "$mode" == "write" && "$r" -eq 1 ]]; then
        run_clean "$fixture" "$mode" "$start" "$dest" "$persist" "$r" 1
      else
        run_clean "$fixture" "$mode" "$start" "$dest" "$throwaway" "$r" 0
      fi
    done
    # Instrumented passes always use throwaway output so they never clobber the
    # kept bins (and strace/perf shards are never reused).
    run_strace "$mode" "$start" "$dest" "$throwaway"
    run_perf   "$mode" "$start" "$dest" "$throwaway"
    run_sched  "$fixture" "$mode" "$start" "$dest" "$throwaway"
  done
}

# ---- env snapshot ----------------------------------------------------------
{
  echo "# ecrawl fixture profile"
  echo "timestamp=$TS"
  echo "synth_root=$SYNTH_ROOT"
  echo "synth_root_fstype=$(fs_type "$SYNTH_ROOT")"
  echo "results_dir=$RESULTS_DIR"
  echo "bin_root=$BIN_ROOT"
  echo "output_base=$OUTPUT_BASE"
  echo "output_base_fstype=$(fs_type "$(dirname "$OUTPUT_BASE")")"
  echo "ecrawl_bin=$ECRAWL_BIN"
  echo "modes: nowrite=$DO_NOWRITE write=$DO_WRITE  strace=$DO_STRACE perf=$DO_PERF sched=$DO_SCHED  reps=$REPS drop_caches=$DROP_CACHES"
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
  echo "time=$TIME_BIN strace=$HAVE_STRACE perf=$HAVE_PERF numfmt=$HAVE_NUMFMT"
  [[ "$HAVE_STRACE" == "1" ]] && echo "strace_version=$(strace -V 2>&1 | head -n1)"
  [[ "$HAVE_PERF"   == "1" ]] && echo "perf_version=$(perf --version 2>&1 | head -n1)"
  echo
  echo "## ecrawl-related env"
  env | grep -E '^ECRAWL_' | sort || true
} >"$RESULTS_DIR/env.txt"

echo "ecrawl profile starting; results -> $RESULTS_DIR"
echo "  (see env.txt for host/tool/env snapshot)"
cat "$RESULTS_DIR/env.txt" | sed 's/^/  /'
echo

if [[ "$DO_PERF" == "1" ]]; then
  pp=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)
  if [[ "$pp" =~ ^-?[0-9]+$ && "$pp" -gt 1 && "$(id -u)" != "0" ]]; then
    echo "WARNING: perf_event_paranoid=$pp may block perf for non-root; run as root or lower it." >&2
  fi
fi

# ---- run -------------------------------------------------------------------
mkdir -p "$OUTPUT_BASE"
for f in "${FIXLIST[@]}"; do
  if [[ ! -d "$SYNTH_ROOT/$f" ]]; then
    echo "==> $f  (MISSING under $SYNTH_ROOT; skipped)"
    continue
  fi
  profile_one "$f" "$SYNTH_ROOT/$f"
done
if [[ "$INCLUDE_ROOT" == "1" ]]; then
  profile_one "_ALL_ROOT_" "$SYNTH_ROOT"
fi
rm -rf "$OUTPUT_BASE"

# ---- combined report -------------------------------------------------------
COMBINED="$RESULTS_DIR/COMBINED_REPORT.txt"
{
  echo "########################################################################"
  echo "# ecrawl fixture profile — combined report"
  echo "########################################################################"
  echo
  cat "$RESULTS_DIR/env.txt"
  echo
  for f in "${FIXLIST[@]}"; do
    [[ -d "$RESULTS_DIR/$f" ]] || continue
    for mode in nowrite write; do
      d="$RESULTS_DIR/$f/$mode"
      [[ -d "$d" ]] || continue
      echo "========================================================================"
      echo "FIXTURE: $f   MODE: $mode"
      echo "========================================================================"
      for part in clean.summary clean.time shards; do
        for file in "$d/$part"*.txt; do
          [[ -e "$file" ]] || continue
          echo "----- $(basename "$file") -----"
          cat "$file"
          echo
        done
      done
      if [[ -s "$d/strace.${mode}.txt" ]]; then
        echo "----- strace.${mode}.txt (syscall histogram) -----"
        cat "$d/strace.${mode}.txt"
        echo
      fi
      if [[ -s "$d/perf.${mode}.report.txt" ]]; then
        echo "----- perf.${mode}.report.txt (top 40 lines) -----"
        head -n 40 "$d/perf.${mode}.report.txt"
        echo
      fi
      if [[ -s "$d/perf.${mode}.report.bythread.txt" ]]; then
        echo "----- perf.${mode}.report.bythread.txt (top 40 lines) -----"
        head -n 40 "$d/perf.${mode}.report.bythread.txt"
        echo
      fi
      if [[ -s "$d/perf.${mode}.sched.latency.txt" ]]; then
        echo "----- perf.${mode}.sched.latency.txt -----"
        cat "$d/perf.${mode}.sched.latency.txt"
        echo
      fi
    done
  done
} >"$COMBINED"

# ---- summary table ---------------------------------------------------------
# Compact, at-a-glance table parsed from the per-rep summaries + strace
# histograms. Averages elapsed across reps; pulls the syscalls that
# discriminate the fast/slow theories (getdents/openat/close/newfstatat).
SUMMARY_TABLE="$RESULTS_DIR/SUMMARY_TABLE.txt"
if command -v python3 >/dev/null 2>&1; then
  RESULTS_DIR="$RESULTS_DIR" FIXLIST_STR="${FIXLIST[*]}" python3 - <<'PY' >"$SUMMARY_TABLE" 2>/dev/null
import os, re
from pathlib import Path

root = Path(os.environ["RESULTS_DIR"])
fixtures = os.environ.get("FIXLIST_STR", "").split()
if (root / "_ALL_ROOT_").is_dir() and "_ALL_ROOT_" not in fixtures:
    fixtures.append("_ALL_ROOT_")


def num(s):
    """Parse '356K' / '1.58M' / '12002931' -> float."""
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


def reps_avg(fx, mode, field):
    vals = []
    for r in range(1, 100):
        p = root / fx / mode / f"clean.summary.rep{r}.txt"
        if not p.exists():
            # also support single-run (no rep suffix)
            p = root / fx / mode / "clean.summary.txt"
            if not p.exists():
                break
        v = num(kv(p).get(field, ""))
        if v is not None:
            vals.append(v)
        if not (root / fx / mode / f"clean.summary.rep{r}.txt").exists():
            break
    return (sum(vals) / len(vals)) if vals else None


def first_summary(fx, mode):
    for cand in ("clean.summary.rep1.txt", "clean.summary.txt"):
        p = root / fx / mode / cand
        if p.exists():
            return kv(p)
    return {}


def reps_list(fx, mode, field):
    out = []
    for r in range(1, 100):
        p = root / fx / mode / f"clean.summary.rep{r}.txt"
        if not p.exists():
            break
        v = num(kv(p).get(field, ""))
        if v is not None:
            out.append(v)
    return out


def strace_counts(fx, mode):
    p = root / fx / mode / f"strace.{mode}.txt"
    out = {}
    if not p.exists():
        return out
    txt = p.read_text(errors="replace")
    for name in ("getdents64", "newfstatat", "openat", "close", "futex"):
        m = re.search(
            r"^\s*([\d.]+)\s+[\d.]+\s+\d+\s+(\d+)\s+(?:\d+\s+)?" + name + r"\b",
            txt, re.M)
        if m:
            out[name] = int(m.group(2))
            out[name + "_pct"] = float(m.group(1))
    return out


def perf_threads(fx, mode, min_pct=1.0, top=5):
    """Per-thread on-CPU spread from perf.<mode>.report.bythread.txt.

    Returns (n_threads_ge_min, top_pcts): how many distinct threads carried
    >=min_pct of CPU samples, and the busiest per-thread percentages. Shows
    whether crawl/writer work actually spread across threads or collapsed.
    """
    p = root / fx / mode / f"perf.{mode}.report.bythread.txt"
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
    if kind == "ops":
        return f"{v/1e6:.2f}M" if v >= 1e6 else f"{v/1e3:.1f}K"
    if kind == "f1":
        return f"{v:.1f}"
    if kind == "f2":
        return f"{v:.2f}"
    return str(v)


lines = []
hdr = kv(root / "env.txt")
lines.append("ecrawl profile — SUMMARY TABLE")
lines.append(f"  timestamp={hdr.get('timestamp','?')}  synth_root={hdr.get('synth_root','?')}")
lines.append(f"  host: nproc={hdr.get('nproc','?')} ulimit_n={hdr.get('ulimit_n','?')} fstype={hdr.get('synth_root_fstype','?')}")
_modes = ""
_envf = root / "env.txt"
if _envf.exists():
    for _l in _envf.read_text(errors="replace").splitlines():
        if _l.startswith("modes:"):
            _modes = _l.strip()
            break
if _modes:
    lines.append(f"  {_modes}")
lines.append("")

# --- crawl table (per fixture x mode) -------------------------------------
cols = ["fixture", "mode", "entries", "tasks", "donated", "avgWkr",
        "elapsed(avg)", "ops/s", "opendir", "readdir"]
w = [22, 8, 13, 9, 11, 7, 13, 8, 11, 13]


def row(vals):
    return "  ".join(str(v).ljust(w[i]) for i, v in enumerate(vals))


lines.append("== CRAWL / PARALLELISM ==")
lines.append(row(cols))
lines.append(row(["-" * x for x in w]))
for fx in fixtures:
    for mode in ("nowrite", "write"):
        d = first_summary(fx, mode)
        if not d:
            continue
        el = reps_avg(fx, mode, "elapsed_sec")
        ent = num(d.get("entries"))
        ops = (ent / el) if (ent and el) else None
        lines.append(row([
            fx, mode,
            fmt(ent), fmt(num(d.get("tasks_popped"))),
            fmt(num(d.get("donated_dirs"))),
            fmt(num(d.get("avg_active_workers")), "f1"),
            fmt(el, "f2") + "s" if el is not None else "-",
            fmt(ops, "ops"),
            fmt(num(d.get("io_opendir_calls"))),
            fmt(num(d.get("io_readdir_calls"))),
        ]))
lines.append("")

# --- writer table (write mode only) ---------------------------------------
cols2 = ["fixture", "max_open", "fopen", "fclose", "fflush", "shards",
         "stat", "wr_wait(s)", "elapsed(avg)", "ops/s"]
w2 = [22, 9, 9, 9, 9, 7, 9, 11, 13, 8]


def row2(vals):
    return "  ".join(str(v).ljust(w2[i]) for i, v in enumerate(vals))


lines.append("== WRITER / UID-SHARD CHURN (write mode) ==")
lines.append(row2(cols2))
lines.append(row2(["-" * x for x in w2]))
for fx in fixtures:
    d = first_summary(fx, "write")
    if not d:
        continue
    sh = kv(root / fx / "write" / "shards.rep1.txt").get("uid_shard_files_created")
    if sh is None:
        sh = kv(root / fx / "write" / "shards.txt").get("uid_shard_files_created")
    el = reps_avg(fx, "write", "elapsed_sec")
    ent = num(d.get("entries"))
    ops = (ent / el) if (ent and el) else None
    wrwait = num(d.get("writer_queue_wait_ns"))
    lines.append(row2([
        fx,
        fmt(num(d.get("max_open_shards"))),
        fmt(num(d.get("io_fopen_calls"))),
        fmt(num(d.get("io_fclose_calls"))),
        fmt(num(d.get("io_fflush_calls"))),
        sh if sh is not None else "-",
        fmt(num(d.get("io_stat_calls"))),
        fmt(wrwait / 1e9, "f1") if wrwait is not None else "-",
        fmt(el, "f2") + "s" if el is not None else "-",
        fmt(ops, "ops"),
    ]))
lines.append("")

# --- strace histogram highlights ------------------------------------------
cols3 = ["fixture", "mode", "getdents64", "newfstatat", "openat", "close", "futex%"]
w3 = [22, 8, 12, 12, 11, 11, 8]


def row3(vals):
    return "  ".join(str(v).ljust(w3[i]) for i, v in enumerate(vals))


lines.append("== STRACE SYSCALL HISTOGRAM (counts; timing under strace NOT representative) ==")
lines.append(row3(cols3))
lines.append(row3(["-" * x for x in w3]))
for fx in fixtures:
    for mode in ("nowrite", "write"):
        sc = strace_counts(fx, mode)
        if not sc:
            continue
        lines.append(row3([
            fx, mode,
            fmt(sc.get("getdents64")),
            fmt(sc.get("newfstatat")),
            fmt(sc.get("openat")),
            fmt(sc.get("close")),
            fmt(sc.get("futex_pct"), "f1") if "futex_pct" in sc else "-",
        ]))
lines.append("")

# --- per-thread on-CPU spread (perf) --------------------------------------
# threads_>=1% = how many distinct threads each carried >=1% of CPU samples;
# compare against avg_active_workers / crawl thread count to spot collapse.
lines.append("== PER-THREAD ON-CPU SPREAD (perf; per fixture/mode) ==")
lines.append("   threads>=1% = busy thread count; top% = busiest threads (needs DO_PERF=1)")
for fx in fixtures:
    for mode in ("nowrite", "write"):
        pt = perf_threads(fx, mode)
        if not pt:
            continue
        n_ge, tops = pt
        tops_s = " ".join(f"{x:.1f}%" for x in tops)
        lines.append(f"  {fx}/{mode}: threads>=1%={n_ge}  top={tops_s}")
lines.append("")

# --- elapsed stability (per rep) ------------------------------------------
lines.append("== ELAPSED PER REP (cold-cache stability) ==")
for fx in fixtures:
    for mode in ("nowrite", "write"):
        rl = reps_list(fx, mode, "elapsed_sec")
        if rl:
            avg = sum(rl) / len(rl)
            lines.append(f"  {fx}/{mode}: {[round(x,2) for x in rl]} avg={avg:.2f}s")

print("\n".join(lines))
PY
fi
if [[ -s "$SUMMARY_TABLE" ]]; then
  # Prepend the at-a-glance table to the combined report for easy reading.
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
  if [[ "$DO_WRITE" == "1" ]]; then
    echo "  Reusable bins:   $BIN_ROOT/<fixture>/bin/  (feed this <bin-root> to the follow-up profilers)"
  else
    echo "  Reusable bins:   (none; DO_WRITE=0 so no shards were kept)"
  fi
  echo "  Tarball (upload this): $TARBALL"
  echo
  echo "----- SUMMARY_TABLE.txt -----"
  cat "$SUMMARY_TABLE"
else
  echo
  echo "DONE."
  echo "  Summary table:   $SUMMARY_TABLE"
  echo "  Combined report: $COMBINED"
  echo "  (tarball creation failed; upload the $RESULTS_DIR directory or COMBINED_REPORT.txt)"
fi
