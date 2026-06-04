#!/usr/bin/env bash
#
# profile-ereport-fixtures.sh
#
# SPDX-License-Identifier: MIT
#
# Per synthetic adversarial sub-directory (see generate-ecrawl-adversarial-tree.sh):
#   1. CRAWL phase  — run ecrawl to produce that fixture's uid_shard_*.bin set
#                     (skipped if the bin dir already has shards; FORCE_CRAWL=1
#                     to recrawl). Timed so per-subdir crawl speed is tracked.
#   2. REPORT phase — run ereport over that bin dir (all-users form,
#                     --bucket-details 4) and capture a full profile.
#
# The REPORT phase runs up to three instrumented passes, mirroring
# profile-ecrawl-fixtures.sh:
#   clean   — /usr/bin/time -v + ereport --verbose key=value phase timings
#             (elapsed_sec, finalize_*, verbose_sort_*). Only trustworthy timing.
#   strace  — strace -f -c syscall histogram (timing NOT representative).
#   perf    — perf record --call-graph dwarf + perf report (CPU profile; build
#             ereport with `make debug` for best symbols).
#
# Usage:
#   scripts/profile-ereport-fixtures.sh <synth-root> <out-parent> [results-dir]
#
# Required:
#   <synth-root>   dir containing the fixtures (single_huge_dir/, mega_dir1/, ...).
#   <out-parent>   parent dir for per-fixture data; for each fixture this script
#                  creates and keeps:
#                      <out-parent>/<fixture>/bin/        ecrawl uid_shard_*.bin
#                      <out-parent>/<fixture>/all_users/  ereport HTML (clean pass)
#                  Bins are reused across runs (FORCE_CRAWL=1 to recrawl).
#
# Optional positional:
#   [results-dir]  where profiling logs/tarball go (default:
#                  ./ereport-profile-<timestamp>); kept separate from the data.
#
# Environment knobs (all optional):
#   EREPORT_BIN=./ereport      ereport binary (auto: ./ereport, /tmp/ereport, PATH).
#   ECRAWL_BIN=./ecrawl        ecrawl binary  (auto: ./ecrawl, /tmp/ecrawl, PATH).
#   FORCE_CRAWL=0              if 1, recrawl every fixture even if shards exist.
#   KEEP_REPORTS=1            keep <out-parent>/<fixture>/all_users (default 1);
#                              set 0 to delete the HTML after recording its size.
#   BUCKET_DETAILS=4           --bucket-details N for ereport (1..32).
#   EREPORT_THREADS=32         worker threads (passed through to ereport).
#   FIXTURES="a b c"           subset of fixtures (default: known set, else all
#                              immediate subdirs).
#   INCLUDE_ROOT=0             also crawl+report the whole <synth-root> as one.
#   DO_STRACE=1                run the strace pass.
#   DO_PERF=1                  run the perf pass.
#   PERF_FREQ=997              perf sampling frequency (Hz).
#   PERF_CALLGRAPH=dwarf       perf call-graph mode (dwarf|fp).
#   REPS=1                     repetitions of the clean report pass per fixture.
#   EREPORT_VERBOSE_ARGS=...   extra args appended to ereport.
#
# Data layout (under <out-parent>, persistent):
#   <fixture>/bin/             ecrawl uid_shard_*.bin for that fixture
#   <fixture>/all_users/       ereport HTML from the clean pass
#
# Profiling-log layout (under <results-dir>):
#   env.txt                                 host/build/env snapshot
#   <fixture>/crawl.time.txt|summary.txt    ecrawl /usr/bin/time + --verbose
#   <fixture>/report/clean.verbose.txt      ereport --verbose key=value (stderr)
#   <fixture>/report/clean.time.txt         /usr/bin/time -v
#   <fixture>/report/clean.stdout.txt       ereport stdout
#   <fixture>/report/report_size.txt        du of emitted HTML
#   <fixture>/report/strace.txt             strace -f -c histogram
#   <fixture>/report/perf.report.txt        perf report --stdio
#   <fixture>/report/perf.report.caller.txt perf report caller view
#   SUMMARY_TABLE.txt                       at-a-glance table
#   COMBINED_REPORT.txt                     everything concatenated
#   ereport-profile-<timestamp>.tar.gz      tarball (upload this)
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
RESULTS_DIR=${3:-"./ereport-profile-$TS"}
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
EREPORT_BIN=$(find_bin ereport EREPORT_BIN)
ECRAWL_BIN=$(find_bin ecrawl ECRAWL_BIN)

# ---- config ----------------------------------------------------------------
# Instrumentation passes (strace/perf) regenerate the report into a throwaway
# dir so they never disturb the canonical clean-pass HTML; this is their home.
INSTR_BASE="$RESULTS_DIR/_instr_report"
FORCE_CRAWL=${FORCE_CRAWL:-0}
BUCKET_DETAILS=${BUCKET_DETAILS:-4}
EREPORT_THREADS=${EREPORT_THREADS:-32}
export EREPORT_THREADS
DO_STRACE=${DO_STRACE:-1}
DO_PERF=${DO_PERF:-1}
PERF_FREQ=${PERF_FREQ:-997}
PERF_CALLGRAPH=${PERF_CALLGRAPH:-dwarf}
REPS=${REPS:-1}
INCLUDE_ROOT=${INCLUDE_ROOT:-0}
KEEP_REPORTS=${KEEP_REPORTS:-1}
EREPORT_VERBOSE_ARGS=${EREPORT_VERBOSE_ARGS:-}

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
  else
    echo "perf record failed; check kernel.perf_event_paranoid and permissions." \
      >"$dest/perf.report.txt"
  fi
  rm -f "$data"
  rm -rf "$reportdir"
}

profile_one() {
  local fixture=$1 start=$2
  echo "==> $fixture  ($start)"
  local fxout="$OUT_PARENT/$fixture"
  local bindir="$fxout/bin"
  local dest="$RESULTS_DIR/$fixture"
  local rdest="$dest/report"
  mkdir -p "$fxout" "$dest" "$rdest"
  ensure_bins "$fixture" "$start" "$bindir" "$dest"
  if [[ "$(shard_count "$bindir")" -eq 0 ]]; then
    echo "    (no shards; skipping report passes)"; return
  fi
  local r
  for ((r = 1; r <= REPS; r++)); do
    run_clean "$rdest" "$bindir" "$fxout" "$r"
  done
  run_strace "$rdest" "$bindir"
  run_perf   "$rdest" "$bindir"
}

# ---- env snapshot ----------------------------------------------------------
{
  echo "# ereport fixture profile"
  echo "timestamp=$TS"
  echo "synth_root=$SYNTH_ROOT"
  echo "synth_root_fstype=$(fs_type "$SYNTH_ROOT")"
  echo "results_dir=$RESULTS_DIR"
  echo "out_parent=$OUT_PARENT"
  echo "ereport_bin=$EREPORT_BIN"
  echo "ecrawl_bin=$ECRAWL_BIN"
  echo "config: bucket_details=$BUCKET_DETAILS ereport_threads=$EREPORT_THREADS force_crawl=$FORCE_CRAWL"
  echo "modes: strace=$DO_STRACE perf=$DO_PERF reps=$REPS keep_reports=$KEEP_REPORTS"
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
    for file in "$RESULTS_DIR/$f/crawl.summary.txt" "$RESULTS_DIR/$f/crawl.time.txt"; do
      [[ -e "$file" ]] || continue
      echo "----- $(basename "$file") -----"; cat "$file"; echo
    done
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
cols = ["fixture", "shards", "crawl(s)", "report(s)", "report_html",
        "parse(s)", "bucketHTML(s)", "sortIdx(s)", "indexHTML(s)"]
w = [22, 8, 9, 10, 12, 9, 13, 11, 12]


def row(vals):
    return "  ".join(str(v).ljust(w[i]) for i, v in enumerate(vals))


lines.append("== CRAWL + REPORT WALL TIME ==")
lines.append(row(cols))
lines.append(row(["-" * x for x in w]))
for fx in fixtures:
    rv = report_kv(fx)
    if not rv and not (root / fx / "crawl.summary.txt").exists():
        continue
    csum = kv(root / fx / "crawl.summary.txt")
    el = reps_elapsed(fx)
    el_avg = (sum(el) / len(el)) if el else None
    rsz = kv(root / fx / "report" / "report_size.txt").get("report_dir_size") \
        or kv(root / fx / "report" / "report_size.rep1.txt").get("report_dir_size")
    cw = num(kv(root / fx / "crawl.wall.txt").get("wall_seconds", ""))
    lines.append(row([
        fx,
        csum.get("uid_shard_files", "-"),
        fmt(cw, "f1") if cw is not None else ("reuse" if csum.get("reused") == "1" else "-"),
        fmt(el_avg, "f2") if el_avg is not None else "-",
        rsz or "-",
        fmt(num(rv.get("verbose_wall_parse_workers_sec")), "f2"),
        fmt(num(rv.get("verbose_wall_bucket_html_cells_wall_sec")), "f2"),
        fmt(num(rv.get("verbose_sort_path_index_sec")), "f2"),
        fmt(num(rv.get("verbose_wall_index_html_sec")), "f2"),
    ]))
lines.append("")

# --- strace top syscalls (report pass) ------------------------------------
lines.append("== STRACE TOP SYSCALLS (report pass; %time, count) ==")
for fx in fixtures:
    top = strace_top(fx)
    if not top:
        continue
    parts = ", ".join(f"{name} {pct:.1f}%/{cnt:,}" for pct, name, cnt in top)
    lines.append(f"  {fx}: {parts}")
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
  echo "  Data (bins+HTML kept): $OUT_PARENT/<fixture>/{bin,all_users}  (bins reused; FORCE_CRAWL=1 to recrawl)"
  echo "  Tarball (upload this): $TARBALL"
  echo
  echo "----- SUMMARY_TABLE.txt -----"
  cat "$SUMMARY_TABLE"
else
  echo
  echo "DONE (tarball failed; upload $RESULTS_DIR or COMBINED_REPORT.txt)."
  echo "  Summary table:   $SUMMARY_TABLE"
fi
