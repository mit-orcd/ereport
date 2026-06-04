#!/usr/bin/env bash
#
# profile-ecrawl_analyze-fixtures.sh
#
# SPDX-License-Identifier: MIT
#
# Profile `ecrawl_analyze` (read-only directory-shape stats over crawl shards)
# over each synthetic adversarial sub-directory's crawl output, the same way
# profile-ereport-fixtures.sh profiles `ereport`.
#
# Per fixture (see generate-ecrawl-adversarial-tree.sh):
#   1. CRAWL phase   — run ecrawl to produce that fixture's uid_shard_*.bin set
#                      (skipped if the bin dir already has shards; FORCE_CRAWL=1
#                      to recrawl). Timed so per-subdir crawl speed is tracked.
#   2. ANALYZE phase — run `ecrawl_analyze --verbose` over that bin dir and
#                      capture a full profile.
#
# The ANALYZE phase runs up to four instrumented passes:
#   clean   — /usr/bin/time -v + ecrawl_analyze key=value stats on stdout
#             (records_total, distinct_parent_directories, ...) + our own wall
#             timer. Only trustworthy timing.
#   strace  — strace -f -c syscall histogram (timing NOT representative).
#   perf    — perf record --call-graph dwarf + perf report (CPU profile, plus a
#             per-thread CPU split; build ecrawl_analyze with `make debug` for
#             best symbols).
#   sched   — optional `perf sched` pass (per-thread runtime / switch / delay)
#             for the big fixtures, to confirm per-section thread concurrency.
#
# NOTE on thread use: ecrawl_analyze parallelises per shard file — it caps its
# worker count at the number of uid_shard_*.bin files. Single-shard fixtures
# therefore run effectively single-threaded; the per-thread perf views below
# make that visible. Production crawls with many shards spread across threads.
#
# Usage:
#   scripts/profile-ecrawl_analyze-fixtures.sh <synth-root> <out-parent> [results-dir]
#
# Required:
#   <synth-root>   dir containing the fixtures (single_huge_dir/, mega_dir1/, ...).
#   <out-parent>   parent dir for per-fixture data; for each fixture this script
#                  creates and keeps:
#                      <out-parent>/<fixture>/bin/   ecrawl uid_shard_*.bin
#                  Bins are reused across runs (FORCE_CRAWL=1 to recrawl).
#                  ecrawl_analyze is read-only, so it produces no kept output.
#
# Optional positional:
#   [results-dir]  where profiling logs/tarball go (default:
#                  ./ecrawl_analyze-profile-<timestamp>); kept separate from data.
#
# Environment knobs (all optional):
#   ECRAWL_ANALYZE_BIN=./ecrawl_analyze  analyzer binary (auto: ./, /tmp/, PATH).
#   ECRAWL_BIN=./ecrawl                  ecrawl binary  (auto: ./, /tmp/, PATH).
#   FORCE_CRAWL=0             if 1, recrawl every fixture even if shards exist.
#   ECRAWL_ANALYZE_THREADS=32  worker threads (passed through; tool caps at the
#                              shard count, and falls back to ECRAWL_REPAIR_THREADS
#                              if ECRAWL_ANALYZE_THREADS is unset).
#   ANALYZE_TOP=               if set to N, pass `--top N` to ecrawl_analyze.
#   FIXTURES="a b c"           subset of fixtures (default: known set, else all
#                              immediate subdirs).
#   INCLUDE_ROOT=0             also crawl+analyze the whole <synth-root> as one.
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
#   ECRAWL_ANALYZE_VERBOSE_ARGS=...  extra args appended to ecrawl_analyze.
#
# Data layout (under <out-parent>, persistent):
#   <fixture>/bin/             ecrawl uid_shard_*.bin for that fixture
#
# Profiling-log layout (under <results-dir>):
#   env.txt                                    host/build/env snapshot
#   <fixture>/crawl.summary.txt|crawl.*.txt    ecrawl /usr/bin/time + --verbose
#   <fixture>/analyze/clean.stats.txt          ecrawl_analyze key=value (stdout)
#   <fixture>/analyze/clean.progress.txt       ecrawl_analyze progress (stderr)
#   <fixture>/analyze/clean.time.txt           /usr/bin/time -v
#   <fixture>/analyze/strace.txt               strace -f -c histogram
#   <fixture>/analyze/perf.report.txt          perf report --stdio
#   <fixture>/analyze/perf.report.caller.txt   perf report caller view
#   <fixture>/analyze/perf.report.bythread.txt per-thread CPU split (thread use)
#   <fixture>/analyze/perf.sched.latency.txt   per-thread sched runtime/delay (DO_SCHED)
#   <fixture>/analyze/perf.sched.summary.txt   perf sched timehist summary (DO_SCHED)
#   SUMMARY_TABLE.txt                          at-a-glance table
#   COMBINED_REPORT.txt                        everything concatenated
#   ecrawl_analyze-profile-<timestamp>.tar.gz  tarball (upload this)
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
RESULTS_DIR=${3:-"./ecrawl_analyze-profile-$TS"}
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
ECRAWL_ANALYZE_BIN=$(find_bin ecrawl_analyze ECRAWL_ANALYZE_BIN)
ECRAWL_BIN=$(find_bin ecrawl ECRAWL_BIN)

# ---- config ----------------------------------------------------------------
FORCE_CRAWL=${FORCE_CRAWL:-0}
ECRAWL_ANALYZE_THREADS=${ECRAWL_ANALYZE_THREADS:-32}
export ECRAWL_ANALYZE_THREADS
ANALYZE_TOP=${ANALYZE_TOP:-}
DO_STRACE=${DO_STRACE:-1}
DO_PERF=${DO_PERF:-1}
DO_SCHED=${DO_SCHED:-0}
SCHED_FIXTURES=${SCHED_FIXTURES:-"single_huge_dir mega_dir1 mega_dir2"}
PERF_FREQ=${PERF_FREQ:-997}
PERF_CALLGRAPH=${PERF_CALLGRAPH:-dwarf}
REPS=${REPS:-1}
INCLUDE_ROOT=${INCLUDE_ROOT:-0}
ECRAWL_ANALYZE_VERBOSE_ARGS=${ECRAWL_ANALYZE_VERBOSE_ARGS:-}

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

# Build ecrawl_analyze argv into RUN_ARGV.
build_argv() {
  local bindir=$1
  RUN_ARGV=("$ECRAWL_ANALYZE_BIN" --verbose)
  [[ -n "$ANALYZE_TOP" ]] && RUN_ARGV+=(--top "$ANALYZE_TOP")
  # shellcheck disable=SC2206
  [[ -n "$ECRAWL_ANALYZE_VERBOSE_ARGS" ]] && RUN_ARGV+=($ECRAWL_ANALYZE_VERBOSE_ARGS)
  RUN_ARGV+=("$bindir")
}

run_clean() {
  local dest=$1 bindir=$2 rep=$3
  local sfx=""; [[ "$REPS" -gt 1 ]] && sfx=".rep${rep}"
  build_argv "$bindir"
  echo "    analyze clean${sfx}: ${RUN_ARGV[*]}"
  # Own monotonic wall timer: /usr/bin/time may be absent, and ecrawl_analyze
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
  local fixture=$1 start=$2
  echo "==> $fixture  ($start)"
  local fxout="$OUT_PARENT/$fixture"
  local bindir="$fxout/bin"
  local dest="$RESULTS_DIR/$fixture"
  local adest="$dest/analyze"
  mkdir -p "$fxout" "$dest" "$adest"
  ensure_bins "$fixture" "$start" "$bindir" "$dest"
  if [[ "$(shard_count "$bindir")" -eq 0 ]]; then
    echo "    (no shards; skipping analyze passes)"; return
  fi
  local r
  for ((r = 1; r <= REPS; r++)); do
    run_clean "$adest" "$bindir" "$r"
  done
  run_strace "$adest" "$bindir"
  run_perf   "$adest" "$bindir"
  run_sched  "$adest" "$bindir" "$fixture"
}

# ---- env snapshot ----------------------------------------------------------
{
  echo "# ecrawl_analyze fixture profile"
  echo "timestamp=$TS"
  echo "synth_root=$SYNTH_ROOT"
  echo "synth_root_fstype=$(fs_type "$SYNTH_ROOT")"
  echo "results_dir=$RESULTS_DIR"
  echo "out_parent=$OUT_PARENT"
  echo "ecrawl_analyze_bin=$ECRAWL_ANALYZE_BIN"
  echo "ecrawl_bin=$ECRAWL_BIN"
  echo "config: analyze_threads=$ECRAWL_ANALYZE_THREADS analyze_top=${ANALYZE_TOP:-off} force_crawl=$FORCE_CRAWL"
  echo "modes: strace=$DO_STRACE perf=$DO_PERF sched=$DO_SCHED reps=$REPS"
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
  echo "## ecrawl_analyze-related env"
  env | grep -E '^ECRAWL_ANALYZE_|^ECRAWL_REPAIR_' | sort || true
} >"$RESULTS_DIR/env.txt"

echo "ecrawl_analyze profile starting; results -> $RESULTS_DIR"
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
  if [[ ! -d "$SYNTH_ROOT/$f" ]]; then
    echo "==> $f  (MISSING under $SYNTH_ROOT; skipped)"; continue
  fi
  profile_one "$f" "$SYNTH_ROOT/$f"
done
if [[ "$INCLUDE_ROOT" == "1" ]]; then
  profile_one "_ALL_ROOT_" "$SYNTH_ROOT"
fi

# ---- combined report -------------------------------------------------------
COMBINED="$RESULTS_DIR/COMBINED_REPORT.txt"
{
  echo "########################################################################"
  echo "# ecrawl_analyze fixture profile — combined report"
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
    d="$RESULTS_DIR/$f/analyze"
    [[ -d "$d" ]] || continue
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


def analyze_kv(fx):
    for cand in ("clean.stats.rep1.txt", "clean.stats.txt"):
        p = root / fx / "analyze" / cand
        if p.exists():
            return kv(p)
    return {}


def reps_elapsed(fx):
    """Analyze wall time from our own timer (clean.wall*), per rep."""
    out = []
    for r in range(1, 100):
        p = root / fx / "analyze" / f"clean.wall.rep{r}.txt"
        if not p.exists():
            break
        v = num(kv(p).get("wall_seconds", ""))
        if v is not None:
            out.append(v)
    if not out:
        p = root / fx / "analyze" / "clean.wall.txt"
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
    >=min_pct of CPU samples, and the busiest per-thread percentages. Because
    ecrawl_analyze caps workers at the shard count, single-shard fixtures show
    threads>=1%=1 here.
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
lines.append("ecrawl_analyze profile — SUMMARY TABLE")
lines.append(f"  timestamp={hdr.get('timestamp','?')}  synth_root={hdr.get('synth_root','?')}")
lines.append(f"  host: nproc={hdr.get('nproc','?')} ulimit_n={hdr.get('ulimit_n','?')} fstype={hdr.get('synth_root_fstype','?')}")
for key in ("config:", "modes:"):
    envf = root / "env.txt"
    if envf.exists():
        for l in envf.read_text(errors="replace").splitlines():
            if l.startswith(key):
                lines.append(f"  {l.strip()}")
                break
lines.append("")

# --- main timing table ----------------------------------------------------
cols = ["fixture", "shards", "crawl(s)", "analyze(s)", "records",
        "parents", "maxNfile", "chunks"]
w = [22, 8, 9, 11, 14, 14, 12, 11]


def row(vals):
    return "  ".join(str(v).ljust(w[i]) for i, v in enumerate(vals))


lines.append("== CRAWL + ANALYZE WALL TIME ==")
lines.append(row(cols))
lines.append(row(["-" * x for x in w]))
for fx in fixtures:
    av = analyze_kv(fx)
    if not av and not (root / fx / "crawl.summary.txt").exists():
        continue
    csum = kv(root / fx / "crawl.summary.txt")
    el = reps_elapsed(fx)
    el_avg = (sum(el) / len(el)) if el else None
    cw = num(kv(root / fx / "crawl.wall.txt").get("wall_seconds", ""))
    lines.append(row([
        fx,
        csum.get("uid_shard_files", "-"),
        fmt(cw, "f1") if cw is not None else ("reuse" if csum.get("reused") == "1" else "-"),
        fmt(el_avg, "f2") if el_avg is not None else "-",
        fmt(num(av.get("records_total"))),
        fmt(num(av.get("distinct_parent_directories"))),
        fmt(num(av.get("max_regular_files_under_single_parent"))),
        fmt(num(av.get("parse_chunk_jobs"))),
    ]))
lines.append("")

# --- per-thread on-CPU spread (perf) --------------------------------------
# threads_>=1% = how many distinct threads each carried >=1% of CPU samples.
# ecrawl_analyze caps workers at the shard count, so single-shard fixtures
# show 1; many-shard corpora should approach analyze_threads.
lines.append("== PER-THREAD ON-CPU SPREAD (perf; whole run) ==")
lines.append("   threads>=1% (tool caps workers at shard count); top% = busiest threads")
for fx in fixtures:
    pt = perf_threads(fx)
    if not pt:
        continue
    n_ge, tops = pt
    tops_s = " ".join(f"{x:.1f}%" for x in tops)
    shards = kv(root / fx / "crawl.summary.txt").get("uid_shard_files", "?")
    lines.append(f"  {fx}: threads>=1%={n_ge} (shards={shards})  top={tops_s}")
lines.append("")

# --- strace top syscalls (analyze pass) -----------------------------------
lines.append("== STRACE TOP SYSCALLS (analyze pass; %time, count) ==")
for fx in fixtures:
    top = strace_top(fx)
    if not top:
        continue
    parts = ", ".join(f"{name} {pct:.1f}%/{cnt:,}" for pct, name, cnt in top)
    lines.append(f"  {fx}: {parts}")
lines.append("")

# --- elapsed per rep ------------------------------------------------------
lines.append("== ANALYZE ELAPSED PER REP ==")
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
  echo "  Data (bins kept): $OUT_PARENT/<fixture>/bin  (bins reused; FORCE_CRAWL=1 to recrawl)"
  echo "  Tarball (upload this): $TARBALL"
  echo
  echo "----- SUMMARY_TABLE.txt -----"
  cat "$SUMMARY_TABLE"
else
  echo
  echo "DONE (tarball failed; upload $RESULTS_DIR or COMBINED_REPORT.txt)."
  echo "  Summary table:   $SUMMARY_TABLE"
fi
