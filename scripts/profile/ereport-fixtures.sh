#!/usr/bin/env bash
#
# profile/ereport-fixtures.sh
#
# SPDX-License-Identifier: MIT
#
# Profile `ereport` over the shared per-fixture crawl output produced by
# profile/ecrawl-fixtures.sh.
#
# This profiler does NOT crawl. Run profile/ecrawl-fixtures.sh first with the
# same <bin-root> to populate <bin-root>/<fixture>/bin/; this script consumes
# those shards and hard-errors if a selected fixture has none.
#
# Per fixture (see generate-ecrawl-adversarial-tree.sh):
#   REPORT phase — run ereport over <bin-root>/<fixture>/bin (all-users form,
#                  --bucket-details 4) and capture a full profile.
#
# The REPORT phase runs up to three instrumented passes, mirroring
# profile/ecrawl-fixtures.sh:
#   clean   — /usr/bin/time -v + ereport --verbose key=value phase timings
#             (elapsed_sec, finalize_*, verbose_sort_*). Only trustworthy timing.
#   strace  — strace -f -c syscall histogram (timing NOT representative).
#   perf    — perf record --call-graph dwarf + perf report (CPU profile; build
#             ereport with `make debug` for best symbols).
#
# Usage:
#   scripts/profile/ereport-fixtures.sh <bin-root> [results-dir]
#
# Required:
#   <bin-root>     dir produced by profile/ecrawl-fixtures.sh; for each fixture
#                  it must contain <bin-root>/<fixture>/bin/uid_shard_*.bin.
#                  The emitted HTML is written alongside, and kept, at:
#                      <bin-root>/<fixture>/all_users/  ereport HTML (clean pass)
#
# Optional positional:
#   [results-dir]  where profiling logs/tarball go (default:
#                  ./ereport-profile-<timestamp>); kept separate from the data.
#
# Environment knobs (all optional):
#   EREPORT_BIN=./ereport      ereport binary (auto: ./ereport, /tmp/ereport, PATH).
#   KEEP_REPORTS=1            keep <bin-root>/<fixture>/all_users (default 1);
#                              set 0 to delete the HTML after recording its size.
#   BUCKET_DETAILS=4           --bucket-details N for ereport (1..32).
#   EREPORT_THREADS=32         worker threads (passed through to ereport).
#   FIXTURES="a b c"           subset of fixtures (default: known set, else all
#                              immediate subdirs).
#   INCLUDE_ROOT=0             also report the whole-tree _ALL_ROOT_ bin set if
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
#   REPS=1                     repetitions of the clean report pass per fixture.
#   EREPORT_VERBOSE_ARGS=...   extra args appended to ereport.
#
# Data layout (under <bin-root>, persistent):
#   <fixture>/bin/             ecrawl uid_shard_*.bin (produced by the ecrawl profiler)
#   <fixture>/all_users/       ereport HTML from the clean pass
#
# Profiling-log layout (under <results-dir>):
#   env.txt                                 host/build/env snapshot
#   <fixture>/report/clean.verbose.txt      ereport --verbose key=value (stderr)
#   <fixture>/report/clean.time.txt         /usr/bin/time -v
#   <fixture>/report/clean.stdout.txt       ereport stdout
#   <fixture>/report/report_size.txt        du of emitted HTML
#   <fixture>/report/strace.txt             strace -f -c histogram
#   <fixture>/report/perf.report.txt        perf report --stdio
#   <fixture>/report/perf.report.caller.txt perf report caller view
#   <fixture>/report/perf.report.bythread.txt per-thread CPU split (thread use)
#   <fixture>/report/perf.sched.latency.txt per-thread sched runtime/delay (DO_SCHED)
#   <fixture>/report/perf.sched.summary.txt perf sched timehist summary (DO_SCHED)
#   SUMMARY_TABLE.txt                       at-a-glance table
#   COMBINED_REPORT.txt                     everything concatenated
#   ereport-profile-<timestamp>.tar.gz      tarball (upload this)
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
RESULTS_DIR=${2:-"./ereport-profile-$TS"}
mkdir -p "$RESULTS_DIR" || { echo "ERROR: cannot create results dir '$RESULTS_DIR'" >&2; exit 1; }
RESULTS_DIR=$(cd "$RESULTS_DIR" && pwd)

# ---- locate binaries -------------------------------------------------------
EREPORT_BIN=$(find_bin ereport EREPORT_BIN) || exit 1

# ---- config ----------------------------------------------------------------
# Instrumentation passes (strace/perf) regenerate the report into a throwaway
# dir so they never disturb the canonical clean-pass HTML; this is their home.
INSTR_BASE="$RESULTS_DIR/_instr_report"
BUCKET_DETAILS=${BUCKET_DETAILS:-4}
EREPORT_THREADS=${EREPORT_THREADS:-32}
export EREPORT_THREADS
DO_STRACE=${DO_STRACE:-1}
DO_PERF=${DO_PERF:-1}
DO_SCHED=${DO_SCHED:-0}
SCHED_FIXTURES=${SCHED_FIXTURES:-"single_huge_dir mega_dir1 mega_dir2"}
PERF_FREQ=${PERF_FREQ:-997}
PERF_CALLGRAPH=${PERF_CALLGRAPH:-dwarf}
REPS=${REPS:-1}
INCLUDE_ROOT=${INCLUDE_ROOT:-0}
KEEP_REPORTS=${KEEP_REPORTS:-1}
EREPORT_VERBOSE_ARGS=${EREPORT_VERBOSE_ARGS:-}

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

# Build ereport argv (all-users form) into RUN_ARGV.
build_argv() {
  local bindir=$1 reportdir=$2
  RUN_ARGV=("$EREPORT_BIN" --bucket-details "$BUCKET_DETAILS" --report-dir "$reportdir" --verbose)
  # shellcheck disable=SC2206
  [[ -n "$EREPORT_VERBOSE_ARGS" ]] && RUN_ARGV+=($EREPORT_VERBOSE_ARGS)
  RUN_ARGV+=("$bindir")
}

run_clean() {
  local dest=$1 bindir=$2 reportparent=$3 rep=$4
  local sfx=""; [[ "$REPS" -gt 1 ]] && sfx=".rep${rep}"
  # ereport --report-dir <reportparent> writes the all-users HTML into
  # <reportparent>/all_users/ ; that is the canonical, kept output.
  mkdir -p "$reportparent"
  build_argv "$bindir" "$reportparent"
  echo "    report clean${sfx}: ${RUN_ARGV[*]}"
  # Own monotonic wall timer: /usr/bin/time may be absent, and ereport only
  # prints elapsed_sec on its ~30s verbose peek (missing for sub-30s reports).
  local t0 t1
  t0=$(date +%s.%N)
  if [[ "$HAVE_TIME" == "1" ]]; then
    "$TIME_BIN" -v -o "$dest/clean.time${sfx}.txt" \
      "${RUN_ARGV[@]}" >"$dest/clean.stdout${sfx}.txt" 2>"$dest/clean.verbose${sfx}.txt"
  else
    "${RUN_ARGV[@]}" >"$dest/clean.stdout${sfx}.txt" 2>"$dest/clean.verbose${sfx}.txt"
  fi
  local rc=$?
  t1=$(date +%s.%N)
  awk -v a="$t0" -v b="$t1" 'BEGIN{printf "wall_seconds=%.3f\n", b-a}' >"$dest/clean.wall${sfx}.txt"
  echo "rc=$rc" >>"$dest/clean.verbose${sfx}.txt"
  echo "    report clean${sfx}: rc=$rc wall=$(cut -d= -f2 "$dest/clean.wall${sfx}.txt")s"
  local html="$reportparent/all_users"
  { echo "report_dir=$html"
    echo "report_dir_size=$(du -sh "$html" 2>/dev/null | cut -f1)"
    echo "report_html_files=$(find "$html" -type f 2>/dev/null | wc -l)"
  } >"$dest/report_size${sfx}.txt"
  [[ "$KEEP_REPORTS" == "1" ]] || rm -rf "$html"
}

run_strace() {
  local dest=$1 bindir=$2
  [[ "$DO_STRACE" == "1" ]] || return 0
  local reportdir="$INSTR_BASE/strace"
  rm -rf "$reportdir"; mkdir -p "$reportdir"
  build_argv "$bindir" "$reportdir"
  echo "    report strace: strace -f -c (timing not representative)"
  strace -f -c -o "$dest/strace.txt" \
    "${RUN_ARGV[@]}" >/dev/null 2>"$dest/strace.ereport-stderr.txt"
  rm -rf "$reportdir"
}

run_perf() {
  local dest=$1 bindir=$2
  [[ "$DO_PERF" == "1" ]] || return 0
  local reportdir="$INSTR_BASE/perf"
  rm -rf "$reportdir"; mkdir -p "$reportdir"
  build_argv "$bindir" "$reportdir"
  echo "    report perf: perf record --call-graph $PERF_CALLGRAPH -F $PERF_FREQ"
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
  rm -rf "$reportdir"
}

# Per-thread scheduling view (runtime / switches / delay) for big fixtures only.
# Confirms how many threads were genuinely on-CPU per section vs. waiting. The
# raw trace is large, so only the text summaries are kept.
run_sched() {
  local dest=$1 bindir=$2 fixture=$3
  [[ "$DO_SCHED" == "1" ]] || return 0
  case " $SCHED_FIXTURES " in *" $fixture "*) ;; *) return 0 ;; esac
  command -v perf >/dev/null 2>&1 || return 0
  local reportdir="$INSTR_BASE/sched"
  rm -rf "$reportdir"; mkdir -p "$reportdir"
  build_argv "$bindir" "$reportdir"
  echo "    report sched: perf sched record (per-thread concurrency; data not kept)"
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
  rm -rf "$reportdir"
}

profile_one() {
  local fixture=$1
  local fxout="$BIN_ROOT/$fixture"
  local bindir="$fxout/bin"
  local dest="$RESULTS_DIR/$fixture"
  local rdest="$dest/report"
  echo "==> $fixture  ($bindir)"
  local nsh
  nsh=$(shard_count "$bindir")
  if [[ "$nsh" -eq 0 ]]; then
    echo "ERROR: no uid_shard_*.bin under '$bindir' for fixture '$fixture'." >&2
    echo "       Run profile/ecrawl-fixtures.sh with the same <bin-root> first." >&2
    exit 1
  fi
  mkdir -p "$dest" "$rdest"
  echo "uid_shard_files=$nsh" >"$dest/bins.txt"
  local r
  for ((r = 1; r <= REPS; r++)); do
    run_clean "$rdest" "$bindir" "$fxout" "$r"
  done
  run_strace "$rdest" "$bindir"
  run_perf   "$rdest" "$bindir"
  run_sched  "$rdest" "$bindir" "$fixture"
}

# ---- env snapshot ----------------------------------------------------------
{
  echo "# ereport fixture profile"
  echo "timestamp=$TS"
  echo "bin_root=$BIN_ROOT"
  echo "bin_root_fstype=$(fs_type "$BIN_ROOT")"
  echo "results_dir=$RESULTS_DIR"
  echo "ereport_bin=$EREPORT_BIN"
  echo "config: bucket_details=$BUCKET_DETAILS ereport_threads=$EREPORT_THREADS"
  echo "modes: strace=$DO_STRACE perf=$DO_PERF sched=$DO_SCHED reps=$REPS keep_reports=$KEEP_REPORTS"
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
  echo "## ereport-related env"
  env | grep -E '^EREPORT_' | sort || true
} >"$RESULTS_DIR/env.txt"

echo "ereport profile starting; results -> $RESULTS_DIR"
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
  profile_one "$f"
done
if [[ "$INCLUDE_ROOT" == "1" && -d "$BIN_ROOT/_ALL_ROOT_/bin" ]]; then
  profile_one "_ALL_ROOT_"
fi
rm -rf "$INSTR_BASE"

# ---- combined report -------------------------------------------------------
COMBINED="$RESULTS_DIR/COMBINED_REPORT.txt"
{
  echo "########################################################################"
  echo "# ereport fixture profile — combined report"
  echo "########################################################################"
  echo
  cat "$RESULTS_DIR/env.txt"
  echo
  for f in "${FIXLIST[@]}"; do
    [[ -d "$RESULTS_DIR/$f" ]] || continue
    echo "========================================================================"
    echo "FIXTURE: $f"
    echo "========================================================================"
    d="$RESULTS_DIR/$f/report"
    [[ -d "$d" ]] || continue
    for part in clean.verbose clean.time report_size; do
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


def report_kv(fx, rep=1, reps=False):
    for cand in ((f"clean.verbose.rep{rep}.txt",) if reps else
                 ("clean.verbose.rep1.txt", "clean.verbose.txt")):
        p = root / fx / "report" / cand
        if p.exists():
            return kv(p)
    return {}


def reps_elapsed(fx):
    """Report wall time from our own timer (clean.wall*), per rep."""
    out = []
    for r in range(1, 100):
        p = root / fx / "report" / f"clean.wall.rep{r}.txt"
        if not p.exists():
            break
        v = num(kv(p).get("wall_seconds", ""))
        if v is not None:
            out.append(v)
    if not out:
        p = root / fx / "report" / "clean.wall.txt"
        if p.exists():
            v = num(kv(p).get("wall_seconds", ""))
            if v is not None:
                out = [v]
    return out


def strace_top(fx, k=6):
    p = root / fx / "report" / "strace.txt"
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

    Returns (n_threads_ge_min, top_pcts) where top_pcts is the largest
    per-thread overhead percentages. Lets you see whether a phase that should
    use N workers actually spread CPU across N threads or collapsed onto a few.
    """
    p = root / fx / "report" / "perf.report.bythread.txt"
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
lines.append("ereport profile — SUMMARY TABLE")
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
cols = ["fixture", "shards", "report(s)", "report_html",
        "parse(s)", "bucketHTML(s)", "sortIdx(s)", "indexHTML(s)"]
w = [22, 8, 10, 12, 9, 13, 11, 12]


def row(vals):
    return "  ".join(str(v).ljust(w[i]) for i, v in enumerate(vals))


lines.append("== REPORT WALL TIME ==")
lines.append(row(cols))
lines.append(row(["-" * x for x in w]))
for fx in fixtures:
    rv = report_kv(fx)
    if not rv:
        continue
    el = reps_elapsed(fx)
    el_avg = (sum(el) / len(el)) if el else None
    rsz = kv(root / fx / "report" / "report_size.txt").get("report_dir_size") \
        or kv(root / fx / "report" / "report_size.rep1.txt").get("report_dir_size")
    lines.append(row([
        fx,
        kv(root / fx / "bins.txt").get("uid_shard_files", "-"),
        fmt(el_avg, "f2") if el_avg is not None else "-",
        rsz or "-",
        fmt(num(rv.get("verbose_wall_parse_workers_sec")), "f2"),
        fmt(num(rv.get("verbose_wall_bucket_html_cells_wall_sec")), "f2"),
        fmt(num(rv.get("verbose_sort_path_index_sec")), "f2"),
        fmt(num(rv.get("verbose_wall_index_html_sec")), "f2"),
    ]))
lines.append("")

# --- per-thread on-CPU spread (perf) --------------------------------------
# threads_>=1% = how many distinct threads each carried >=1% of CPU samples;
# compare against ereport_threads to spot phases that collapse onto few threads.
lines.append("== PER-THREAD ON-CPU SPREAD (perf; whole run) ==")
lines.append("   threads>=1% vs configured ereport_threads; top% = busiest threads")
for fx in fixtures:
    pt = perf_threads(fx)
    if not pt:
        continue
    n_ge, tops = pt
    tops_s = " ".join(f"{x:.1f}%" for x in tops)
    lines.append(f"  {fx}: threads>=1%={n_ge}  top={tops_s}")
lines.append("")

# --- strace top syscalls (report pass) ------------------------------------
strace_rows = []
for fx in fixtures:
    top = strace_top(fx)
    if not top:
        continue
    parts = ", ".join(f"{name} {pct:.1f}%/{cnt:,}" for pct, name, cnt in top)
    strace_rows.append(f"  {fx}: {parts}")
# With DO_STRACE=0 there is nothing to show, so skip the header entirely.
if strace_rows:
    lines.append("== STRACE TOP SYSCALLS (report pass; %time, count) ==")
    lines.extend(strace_rows)
    lines.append("")

# --- elapsed per rep ------------------------------------------------------
lines.append("== REPORT ELAPSED PER REP ==")
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
  echo "  Data (HTML kept): $BIN_ROOT/<fixture>/all_users  (bins consumed from $BIN_ROOT/<fixture>/bin)"
  echo "  Tarball (upload this): $TARBALL"
  echo
  echo "----- SUMMARY_TABLE.txt -----"
  cat "$SUMMARY_TABLE"
else
  echo
  echo "DONE (tarball failed; upload $RESULTS_DIR or COMBINED_REPORT.txt)."
  echo "  Summary table:   $SUMMARY_TABLE"
fi
