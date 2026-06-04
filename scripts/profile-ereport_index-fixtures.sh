#!/usr/bin/env bash
#
# profile-ereport_index-fixtures.sh
#
# SPDX-License-Identifier: MIT
#
# Profile `ereport_index --make` (the trigram index builder used for path
# search) over each synthetic adversarial sub-directory's crawl output, the
# same way profile-ereport-fixtures.sh profiles `ereport`.
#
# Per fixture (see generate-ecrawl-adversarial-tree.sh):
#   1. CRAWL phase  — run ecrawl to produce that fixture's uid_shard_*.bin set
#                     (skipped if the bin dir already has shards; FORCE_CRAWL=1
#                     to recrawl). Timed so per-subdir crawl speed is tracked.
#   2. INDEX phase  — run `ereport_index --make` over that bin dir (all-users
#                     form) and capture a full profile.
#
# The INDEX phase runs up to three instrumented passes:
#   clean   — /usr/bin/time -v + ereport_index --verbose key=value stats on
#             stdout (elapsed_sec, chunk_prep_sec, index_phase_sec,
#             merge_phase_sec, make_f{open,read,write}_calls). Trustworthy time.
#   strace  — strace -f -c syscall histogram (timing NOT representative).
#   perf    — perf record --call-graph dwarf + perf report (CPU profile; build
#             ereport_index with `make debug` for best symbols).
#
# Usage:
#   scripts/profile-ereport_index-fixtures.sh <synth-root> <out-parent> [results-dir]
#
# Required:
#   <synth-root>   dir containing the fixtures (single_huge_dir/, mega_dir1/, ...).
#   <out-parent>   parent dir for per-fixture data; for each fixture this script
#                  creates and keeps:
#                      <out-parent>/<fixture>/bin/      ecrawl uid_shard_*.bin
#                      <out-parent>/<fixture>/index/    ereport_index output
#                  Bins are reused across runs (FORCE_CRAWL=1 to recrawl).
#
# Optional positional:
#   [results-dir]  where profiling logs/tarball go (default:
#                  ./ereport_index-profile-<timestamp>); kept separate from data.
#
# Environment knobs (all optional):
#   EREPORT_INDEX_BIN=./ereport_index  builder binary (auto: ./, /tmp/, PATH).
#   ECRAWL_BIN=./ecrawl                ecrawl binary  (auto: ./, /tmp/, PATH).
#   FORCE_CRAWL=0              if 1, recrawl every fixture even if shards exist.
#   KEEP_INDEX=1              keep <out-parent>/<fixture>/index (default 1);
#                              set 0 to delete the index after recording its size.
#   EREPORT_INDEX_THREADS=32   parse/trigram worker threads (passed through).
#   EREPORT_INDEX_TRIGRAM_THREADS=...  optional; passed through if set.
#   RAISE_ULIMIT=1             best-effort `ulimit -n` raise + `ulimit -f
#                              unlimited` before building (index builds are
#                              fd- and file-size-hungry). Set 0 to leave as-is.
#   FIXTURES="a b c"           subset of fixtures (default: known set, else all
#                              immediate subdirs).
#   INCLUDE_ROOT=0             also crawl+index the whole <synth-root> as one.
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
#   REPS=1                     repetitions of the clean index pass per fixture.
#   EREPORT_INDEX_VERBOSE_ARGS=...  extra args appended to ereport_index.
#
# Data layout (under <out-parent>, persistent):
#   <fixture>/bin/             ecrawl uid_shard_*.bin for that fixture
#   <fixture>/index/           ereport_index output from the clean pass
#
# Profiling-log layout (under <results-dir>):
#   env.txt                                   host/build/env snapshot
#   <fixture>/crawl.summary.txt|crawl.*.txt   ecrawl /usr/bin/time + --verbose
#   <fixture>/index/clean.stats.txt           ereport_index key=value (stdout)
#   <fixture>/index/clean.progress.txt        ereport_index progress (stderr)
#   <fixture>/index/clean.time.txt            /usr/bin/time -v
#   <fixture>/index/index_size.txt            du of emitted index
#   <fixture>/index/strace.txt                strace -f -c histogram
#   <fixture>/index/perf.report.txt           perf report --stdio
#   <fixture>/index/perf.report.caller.txt    perf report caller view
#   SUMMARY_TABLE.txt                         at-a-glance table
#   COMBINED_REPORT.txt                       everything concatenated
#   ereport_index-profile-<timestamp>.tar.gz  tarball (upload this)
#
set -uo pipefail

# ---- args ------------------------------------------------------------------
if [[ $# -lt 2 ]]; then
  echo "usage: $0 <synth-root> <out-parent> [results-dir]" >&2
  exit 2
fi
SYNTH_ROOT=${1%/}
if [[ ! -d "$SYNTH_ROOT" ]]; then
  echo "ERROR: synth-root '$SYNTH_ROOT' is not a directory" >&2
  exit 2
fi

OUT_PARENT=${2%/}
mkdir -p "$OUT_PARENT" || { echo "ERROR: cannot create out-parent '$OUT_PARENT'" >&2; exit 1; }
OUT_PARENT=$(cd "$OUT_PARENT" && pwd)

TS=$(date +%Y%m%d-%H%M%S)
RESULTS_DIR=${3:-"./ereport_index-profile-$TS"}
mkdir -p "$RESULTS_DIR" || { echo "ERROR: cannot create results dir '$RESULTS_DIR'" >&2; exit 1; }
RESULTS_DIR=$(cd "$RESULTS_DIR" && pwd)

# ---- locate binaries -------------------------------------------------------
find_bin() {
  local name=$1 var=$2
  local v=${!var:-}
  if [[ -n "$v" ]]; then
    [[ -x "$v" ]] || { echo "ERROR: $var '$v' is not executable" >&2; exit 1; }
    echo "$v"; return
  fi
  if   [[ -x "./$name" ]];   then echo "$(cd "$(dirname "./$name")" && pwd)/$name"
  elif [[ -x "/tmp/$name" ]]; then echo "/tmp/$name"
  elif command -v "$name" >/dev/null 2>&1; then command -v "$name"
  else echo "ERROR: cannot find $name; set $var=/path/to/$name" >&2; exit 1
  fi
}
EREPORT_INDEX_BIN=$(find_bin ereport_index EREPORT_INDEX_BIN)
ECRAWL_BIN=$(find_bin ecrawl ECRAWL_BIN)

# ---- config ----------------------------------------------------------------
# Instrumentation passes (strace/perf) rebuild the index into a throwaway dir
# so they never disturb the canonical clean-pass index; this is their home.
INSTR_BASE="$RESULTS_DIR/_instr_index"
FORCE_CRAWL=${FORCE_CRAWL:-0}
EREPORT_INDEX_THREADS=${EREPORT_INDEX_THREADS:-32}
export EREPORT_INDEX_THREADS
[[ -n "${EREPORT_INDEX_TRIGRAM_THREADS:-}" ]] && export EREPORT_INDEX_TRIGRAM_THREADS
DO_STRACE=${DO_STRACE:-1}
DO_PERF=${DO_PERF:-1}
DO_SCHED=${DO_SCHED:-0}
SCHED_FIXTURES=${SCHED_FIXTURES:-"single_huge_dir mega_dir1 mega_dir2"}
PERF_FREQ=${PERF_FREQ:-997}
PERF_CALLGRAPH=${PERF_CALLGRAPH:-dwarf}
REPS=${REPS:-1}
INCLUDE_ROOT=${INCLUDE_ROOT:-0}
KEEP_INDEX=${KEEP_INDEX:-1}
RAISE_ULIMIT=${RAISE_ULIMIT:-1}
EREPORT_INDEX_VERBOSE_ARGS=${EREPORT_INDEX_VERBOSE_ARGS:-}

# Index builds open many trigram-bucket files and write large paths.bin; raise
# soft limits best-effort (never above the hard limit, never fatal).
ULIMIT_N_BEFORE=$(ulimit -n)
ULIMIT_F_BEFORE=$(ulimit -f)
if [[ "$RAISE_ULIMIT" == "1" ]]; then
  ulimit -f unlimited 2>/dev/null || true
  hardn=$(ulimit -Hn 2>/dev/null || echo "")
  if [[ "$hardn" == "unlimited" ]]; then
    ulimit -n 1048576 2>/dev/null || true
  elif [[ "$hardn" =~ ^[0-9]+$ ]]; then
    ulimit -n "$hardn" 2>/dev/null || true
  fi
fi

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
    while IFS= read -r d; do FIXLIST+=("$(basename "$d")"); done \
      < <(find "$SYNTH_ROOT" -mindepth 1 -maxdepth 1 -type d | sort)
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

if [[ "$DO_STRACE" == "1" && "$HAVE_STRACE" != "1" ]]; then
  echo "WARNING: strace not found; skipping strace pass." >&2; DO_STRACE=0
fi
if [[ "$DO_PERF" == "1" && "$HAVE_PERF" != "1" ]]; then
  echo "WARNING: perf not found; skipping perf pass." >&2; DO_PERF=0
fi

# ---- helpers ---------------------------------------------------------------
fs_type() { stat -f -c '%T' "$1" 2>/dev/null || echo "?"; }
shard_count() { find "$1" -maxdepth 1 -name 'uid_shard_*.bin' 2>/dev/null | wc -l; }

# Crawl one fixture into its persistent bin dir (unless shards already exist).
ensure_bins() {
  local fixture=$1 start=$2 bindir=$3 dest=$4
  local n
  n=$(shard_count "$bindir")
  if [[ "$FORCE_CRAWL" != "1" && "$n" -gt 0 ]]; then
    echo "    crawl: reuse $n shard(s) in $bindir"
    { echo "reused=1"; echo "uid_shard_files=$n"; } >"$dest/crawl.summary.txt"
    return 0
  fi
  rm -rf "$bindir"; mkdir -p "$bindir"
  echo "    crawl: $ECRAWL_BIN --verbose $start $bindir"
  local t0 t1
  t0=$(date +%s.%N)
  if [[ "$HAVE_TIME" == "1" ]]; then
    "$TIME_BIN" -v -o "$dest/crawl.time.txt" \
      "$ECRAWL_BIN" --verbose "$start" "$bindir" \
      >"$dest/crawl.summary.txt" 2>"$dest/crawl.stderr.txt"
  else
    "$ECRAWL_BIN" --verbose "$start" "$bindir" \
      >"$dest/crawl.summary.txt" 2>"$dest/crawl.stderr.txt"
  fi
  local rc=$?
  t1=$(date +%s.%N)
  awk -v a="$t0" -v b="$t1" 'BEGIN{printf "wall_seconds=%.3f\n", b-a}' >"$dest/crawl.wall.txt"
  n=$(shard_count "$bindir")
  { echo "reused=0"; echo "crawl_rc=$rc"; echo "uid_shard_files=$n"; } >>"$dest/crawl.summary.txt"
  echo "    crawl: produced $n shard(s) (rc=$rc)"
}

# Build ereport_index argv (all-users --make form) into RUN_ARGV.
# With a bin-dir path (not a system user) as the first positional, --make
# indexes every UID under <index-dir>.
build_argv() {
  local bindir=$1 indexdir=$2
  RUN_ARGV=("$EREPORT_INDEX_BIN" --make --index-dir "$indexdir" --verbose)
  # shellcheck disable=SC2206
  [[ -n "$EREPORT_INDEX_VERBOSE_ARGS" ]] && RUN_ARGV+=($EREPORT_INDEX_VERBOSE_ARGS)
  RUN_ARGV+=("$bindir")
}

run_clean() {
  local dest=$1 bindir=$2 indexdir=$3 rep=$4
  local sfx=""; [[ "$REPS" -gt 1 ]] && sfx=".rep${rep}"
  # Rebuild into a clean index dir so timings are not skewed by stale files;
  # the final clean pass is the canonical, kept output.
  rm -rf "$indexdir"; mkdir -p "$indexdir"
  build_argv "$bindir" "$indexdir"
  echo "    index clean${sfx}: ${RUN_ARGV[*]}"
  # Own monotonic wall timer in addition to ereport_index's elapsed_sec stat.
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
  echo "    index clean${sfx}: rc=$rc wall=$(cut -d= -f2 "$dest/clean.wall${sfx}.txt")s"
  { echo "index_dir=$indexdir"
    echo "index_dir_size=$(du -sh "$indexdir" 2>/dev/null | cut -f1)"
    echo "index_files=$(find "$indexdir" -type f 2>/dev/null | wc -l)"
    for f in tri_keys.bin tri_postings.bin paths.bin path_offsets.bin meta.txt; do
      [[ -e "$indexdir/$f" ]] && echo "size_$f=$(stat -c %s "$indexdir/$f" 2>/dev/null)"
    done
  } >"$dest/index_size${sfx}.txt"
  [[ "$KEEP_INDEX" == "1" ]] || rm -rf "$indexdir"
}

run_strace() {
  local dest=$1 bindir=$2
  [[ "$DO_STRACE" == "1" ]] || return 0
  local indexdir="$INSTR_BASE/strace"
  rm -rf "$indexdir"; mkdir -p "$indexdir"
  build_argv "$bindir" "$indexdir"
  echo "    index strace: strace -f -c (timing not representative)"
  strace -f -c -o "$dest/strace.txt" \
    "${RUN_ARGV[@]}" >/dev/null 2>"$dest/strace.index-stderr.txt"
  rm -rf "$indexdir"
}

run_perf() {
  local dest=$1 bindir=$2
  [[ "$DO_PERF" == "1" ]] || return 0
  local indexdir="$INSTR_BASE/perf"
  rm -rf "$indexdir"; mkdir -p "$indexdir"
  build_argv "$bindir" "$indexdir"
  echo "    index perf: perf record --call-graph $PERF_CALLGRAPH -F $PERF_FREQ"
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
  rm -rf "$indexdir"
}

# Per-thread scheduling view (runtime / switches / delay) for big fixtures only.
# Confirms how many threads were genuinely on-CPU per section vs. waiting. The
# raw trace is large, so only the text summaries are kept.
run_sched() {
  local dest=$1 bindir=$2 fixture=$3
  [[ "$DO_SCHED" == "1" ]] || return 0
  case " $SCHED_FIXTURES " in *" $fixture "*) ;; *) return 0 ;; esac
  command -v perf >/dev/null 2>&1 || return 0
  local indexdir="$INSTR_BASE/sched"
  rm -rf "$indexdir"; mkdir -p "$indexdir"
  build_argv "$bindir" "$indexdir"
  echo "    index sched: perf sched record (per-thread concurrency; data not kept)"
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
  rm -rf "$indexdir"
}

profile_one() {
  local fixture=$1 start=$2
  echo "==> $fixture  ($start)"
  local fxout="$OUT_PARENT/$fixture"
  local bindir="$fxout/bin"
  local indexdir="$fxout/index"
  local dest="$RESULTS_DIR/$fixture"
  local idest="$dest/index"
  mkdir -p "$fxout" "$dest" "$idest"
  ensure_bins "$fixture" "$start" "$bindir" "$dest"
  if [[ "$(shard_count "$bindir")" -eq 0 ]]; then
    echo "    (no shards; skipping index passes)"; return
  fi
  local r
  for ((r = 1; r <= REPS; r++)); do
    run_clean "$idest" "$bindir" "$indexdir" "$r"
  done
  run_strace "$idest" "$bindir"
  run_perf   "$idest" "$bindir"
  run_sched  "$idest" "$bindir" "$fixture"
}

# ---- env snapshot ----------------------------------------------------------
{
  echo "# ereport_index fixture profile"
  echo "timestamp=$TS"
  echo "synth_root=$SYNTH_ROOT"
  echo "synth_root_fstype=$(fs_type "$SYNTH_ROOT")"
  echo "results_dir=$RESULTS_DIR"
  echo "out_parent=$OUT_PARENT"
  echo "ereport_index_bin=$EREPORT_INDEX_BIN"
  echo "ecrawl_bin=$ECRAWL_BIN"
  echo "config: index_threads=$EREPORT_INDEX_THREADS trigram_threads=${EREPORT_INDEX_TRIGRAM_THREADS:-default} force_crawl=$FORCE_CRAWL"
  echo "modes: strace=$DO_STRACE perf=$DO_PERF sched=$DO_SCHED reps=$REPS keep_index=$KEEP_INDEX"
  echo "sched_fixtures: $SCHED_FIXTURES"
  echo "fixtures: ${FIXLIST[*]}"
  echo
  echo "## host"
  echo "uname=$(uname -a)"
  echo "nproc=$(nproc 2>/dev/null || echo '?')"
  echo "ulimit_n_before=$ULIMIT_N_BEFORE ulimit_n_now=$(ulimit -n)"
  echo "ulimit_f_before=$ULIMIT_F_BEFORE ulimit_f_now=$(ulimit -f)"
  echo "cpu_model=$(awk -F: '/model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null | sed 's/^ //')"
  echo "mem_total_kb=$(awk '/MemTotal/{print $2; exit}' /proc/meminfo 2>/dev/null)"
  echo "mem_available_kb=$(awk '/MemAvailable/{print $2; exit}' /proc/meminfo 2>/dev/null)"
  echo "perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo '?')"
  echo "yama_ptrace_scope=$(cat /proc/sys/kernel/yama/ptrace_scope 2>/dev/null || echo '?')"
  echo
  echo "## tools"
  echo "time=$TIME_BIN strace=$HAVE_STRACE perf=$HAVE_PERF"
  [[ "$HAVE_STRACE" == "1" ]] && echo "strace_version=$(strace -V 2>&1 | head -n1)"
  [[ "$HAVE_PERF"   == "1" ]] && echo "perf_version=$(perf --version 2>&1 | head -n1)"
  echo
  echo "## ereport_index-related env"
  env | grep -E '^EREPORT_INDEX_' | sort || true
} >"$RESULTS_DIR/env.txt"

echo "ereport_index profile starting; results -> $RESULTS_DIR"
sed 's/^/  /' "$RESULTS_DIR/env.txt"
echo

if [[ "$DO_PERF" == "1" ]]; then
  pp=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)
  if [[ "$pp" =~ ^-?[0-9]+$ && "$pp" -gt 1 && "$(id -u)" != "0" ]]; then
    echo "WARNING: perf_event_paranoid=$pp may block perf for non-root; run as root or lower it." >&2
  fi
fi

# ---- run -------------------------------------------------------------------
mkdir -p "$INSTR_BASE"
for f in "${FIXLIST[@]}"; do
  if [[ ! -d "$SYNTH_ROOT/$f" ]]; then
    echo "==> $f  (MISSING under $SYNTH_ROOT; skipped)"; continue
  fi
  profile_one "$f" "$SYNTH_ROOT/$f"
done
if [[ "$INCLUDE_ROOT" == "1" ]]; then
  profile_one "_ALL_ROOT_" "$SYNTH_ROOT"
fi
rm -rf "$INSTR_BASE"

# ---- combined report -------------------------------------------------------
COMBINED="$RESULTS_DIR/COMBINED_REPORT.txt"
{
  echo "########################################################################"
  echo "# ereport_index fixture profile — combined report"
  echo "########################################################################"
  echo
  cat "$RESULTS_DIR/env.txt"
  echo
  for f in "${FIXLIST[@]}"; do
    [[ -d "$RESULTS_DIR/$f" ]] || continue
    echo "========================================================================"
    echo "FIXTURE: $f"
    echo "========================================================================"
    for file in "$RESULTS_DIR/$f/crawl.summary.txt" "$RESULTS_DIR/$f/crawl.time.txt"; do
      [[ -e "$file" ]] || continue
      echo "----- $(basename "$file") -----"; cat "$file"; echo
    done
    d="$RESULTS_DIR/$f/index"
    [[ -d "$d" ]] || continue
    for part in clean.stats clean.time index_size; do
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
  done
} >"$COMBINED"

# ---- summary table ---------------------------------------------------------
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


def index_kv(fx):
    for cand in ("clean.stats.rep1.txt", "clean.stats.txt"):
        p = root / fx / "index" / cand
        if p.exists():
            return kv(p)
    return {}


def reps_elapsed(fx):
    """Index wall time, per rep: prefer ereport_index elapsed_sec, else our timer."""
    out = []
    for r in range(1, 100):
        sp = root / fx / "index" / f"clean.stats.rep{r}.txt"
        wp = root / fx / "index" / f"clean.wall.rep{r}.txt"
        if not sp.exists() and not wp.exists():
            break
        v = num(kv(sp).get("elapsed_sec", "")) if sp.exists() else None
        if v is None and wp.exists():
            v = num(kv(wp).get("wall_seconds", ""))
        if v is not None:
            out.append(v)
    if not out:
        sp = root / fx / "index" / "clean.stats.txt"
        wp = root / fx / "index" / "clean.wall.txt"
        v = num(kv(sp).get("elapsed_sec", "")) if sp.exists() else None
        if v is None and wp.exists():
            v = num(kv(wp).get("wall_seconds", ""))
        if v is not None:
            out = [v]
    return out


def strace_top(fx, k=6):
    p = root / fx / "index" / "strace.txt"
    if not p.exists():
        return []
    rows = []
    for line in p.read_text(errors="replace").splitlines():
        m = re.match(r"\s*([\d.]+)\s+[\d.]+\s+\d+\s+(\d+)\s+(?:\d+\s+)?(\w+)\s*$", line)
        if m:
            rows.append((float(m.group(1)), m.group(3), int(m.group(2))))
    rows.sort(reverse=True)
    return rows[:k]


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
lines.append("ereport_index profile — SUMMARY TABLE")
lines.append(f"  timestamp={hdr.get('timestamp','?')}  synth_root={hdr.get('synth_root','?')}")
lines.append(f"  host: nproc={hdr.get('nproc','?')} ulimit_n_now={hdr.get('ulimit_n_now','?')} fstype={hdr.get('synth_root_fstype','?')}")
for key in ("config:", "modes:"):
    envf = root / "env.txt"
    if envf.exists():
        for l in envf.read_text(errors="replace").splitlines():
            if l.startswith(key):
                lines.append(f"  {l.strip()}")
                break
lines.append("")

# --- main timing table ----------------------------------------------------
cols = ["fixture", "shards", "crawl(s)", "index(s)", "idx_size", "paths",
        "trigrams", "chunkprep", "indexph", "mergeph"]
w = [22, 7, 9, 9, 9, 12, 11, 10, 9, 9]


def row(vals):
    return "  ".join(str(v).ljust(w[i]) for i, v in enumerate(vals))


lines.append("== CRAWL + INDEX WALL TIME ==")
lines.append(row(cols))
lines.append(row(["-" * x for x in w]))
for fx in fixtures:
    iv = index_kv(fx)
    if not iv and not (root / fx / "crawl.summary.txt").exists():
        continue
    csum = kv(root / fx / "crawl.summary.txt")
    el = reps_elapsed(fx)
    el_avg = (sum(el) / len(el)) if el else None
    isz = kv(root / fx / "index" / "index_size.txt").get("index_dir_size") \
        or kv(root / fx / "index" / "index_size.rep1.txt").get("index_dir_size")
    cw = num(kv(root / fx / "crawl.wall.txt").get("wall_seconds", ""))
    lines.append(row([
        fx,
        csum.get("uid_shard_files", "-"),
        fmt(cw, "f1") if cw is not None else ("reuse" if csum.get("reused") == "1" else "-"),
        fmt(el_avg, "f2") if el_avg is not None else "-",
        isz or "-",
        fmt(num(iv.get("indexed_paths"))),
        fmt(num(iv.get("unique_trigrams"))),
        fmt(num(iv.get("chunk_prep_sec")), "f2"),
        fmt(num(iv.get("index_phase_sec")), "f2"),
        fmt(num(iv.get("merge_phase_sec")), "f2"),
    ]))
lines.append("")

# --- make I/O counters (indexing hot path) --------------------------------
lines.append("== MAKE I/O COUNTERS (from --verbose stats) ==")
io_cols = ["fixture", "fopen", "fclose", "fread", "fread_MiB", "fwrite", "fwrite_MiB"]
iw = [22, 12, 12, 14, 11, 14, 11]


def irow(vals):
    return "  ".join(str(v).ljust(iw[i]) for i, v in enumerate(vals))


lines.append(irow(io_cols))
lines.append(irow(["-" * x for x in iw]))
for fx in fixtures:
    iv = index_kv(fx)
    if not iv:
        continue
    frb = num(iv.get("make_fread_bytes"))
    fwb = num(iv.get("make_fwrite_bytes"))
    lines.append(irow([
        fx,
        fmt(num(iv.get("make_fopen_calls"))),
        fmt(num(iv.get("make_fclose_calls"))),
        fmt(num(iv.get("make_fread_calls"))),
        fmt(frb / (1024 * 1024), "f1") if frb is not None else "-",
        fmt(num(iv.get("make_fwrite_calls"))),
        fmt(fwb / (1024 * 1024), "f1") if fwb is not None else "-",
    ]))
lines.append("")

# --- per-phase average concurrency ----------------------------------------
# avg threads busy in a phase = phase_cpu_time / phase_wall. Compare against
# the configured worker counts to spot starvation (e.g. lock contention).
def time_pct_cpu(fx):
    for cand in ("clean.time.rep1.txt", "clean.time.txt"):
        p = root / fx / "index" / cand
        if p.exists():
            m = re.search(r"Percent of CPU this job got:\s*([\d.]+)%",
                          p.read_text(errors="replace"))
            if m:
                return float(m.group(1)) / 100.0
    return None


idx_threads_cfg = "?"
for l in (root / "env.txt").read_text(errors="replace").splitlines() \
        if (root / "env.txt").exists() else []:
    m = re.search(r"index_threads=(\d+)", l)
    if m:
        idx_threads_cfg = m.group(1)
        break

lines.append("== PER-PHASE AVG CONCURRENCY (cpu_time / wall; ~threads busy) ==")
lines.append(f"   expected: prep/index ~ index_threads={idx_threads_cfg} (parse+trigram), merge <= 16")
ccols = ["fixture", "prep", "index", "merge", "whole", "time%CPU"]
cw2 = [22, 8, 8, 8, 8, 9]


def crow(vals):
    return "  ".join(str(v).ljust(cw2[i]) for i, v in enumerate(vals))


def conc(uk, sk, wall):
    u = num(iv.get(uk))
    s = num(iv.get(sk))
    if u is None or s is None or wall is None or wall <= 0:
        return "-"
    return f"{(u + s) / wall:.1f}"


lines.append(crow(ccols))
lines.append(crow(["-" * x for x in cw2]))
for fx in fixtures:
    iv = index_kv(fx)
    if not iv:
        continue
    prep_wall = num(iv.get("chunk_prep_sec"))
    ip_wall = num(iv.get("index_phase_sec"))
    mrg_wall = num(iv.get("merge_phase_sec"))
    el = num(iv.get("elapsed_sec"))
    # index_phase_sec spans start->end of indexing (includes prep); subtract it.
    idx_only = (ip_wall - prep_wall) if (ip_wall is not None and prep_wall is not None) else None
    pc = time_pct_cpu(fx)
    lines.append(crow([
        fx,
        conc("cpu_prep_cpu_user_sec", "cpu_prep_cpu_sys_sec", prep_wall),
        conc("cpu_idx_cpu_user_sec", "cpu_idx_cpu_sys_sec", idx_only),
        conc("cpu_mrg_cpu_user_sec", "cpu_mrg_cpu_sys_sec", mrg_wall),
        conc("cpu_make_cpu_user_sec", "cpu_make_cpu_sys_sec", el),
        f"{pc:.1f}x" if pc is not None else "-",
    ]))
lines.append("")

# --- strace top syscalls (index pass) -------------------------------------
lines.append("== STRACE TOP SYSCALLS (index pass; %time, count) ==")
for fx in fixtures:
    top = strace_top(fx)
    if not top:
        continue
    parts = ", ".join(f"{name} {pct:.1f}%/{cnt:,}" for pct, name, cnt in top)
    lines.append(f"  {fx}: {parts}")
lines.append("")

# --- elapsed per rep ------------------------------------------------------
lines.append("== INDEX ELAPSED PER REP ==")
for fx in fixtures:
    el = reps_elapsed(fx)
    if el:
        avg = sum(el) / len(el)
        lines.append(f"  {fx}: {[round(x,2) for x in el]} avg={avg:.2f}s")

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
  echo "  Data (bins+index kept): $OUT_PARENT/<fixture>/{bin,index}  (bins reused; FORCE_CRAWL=1 to recrawl)"
  echo "  Tarball (upload this): $TARBALL"
  echo
  echo "----- SUMMARY_TABLE.txt -----"
  cat "$SUMMARY_TABLE"
else
  echo
  echo "DONE (tarball failed; upload $RESULTS_DIR or COMBINED_REPORT.txt)."
  echo "  Summary table:   $SUMMARY_TABLE"
fi
