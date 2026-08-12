#!/usr/bin/env bash
# Profile ecrawl vs dut on one tree: cold/hot wall, plus cold nowrite perf/strace.
#
# Root shell on a local node (e.g. node9901), from a git clone under /tmp:
#
#   rsync -a ~/git/ereport/ /tmp/ereport/
#   rsync -a ~/git/dut/dut /tmp/dut          # binary at /tmp/dut
#   cd /tmp/ereport
#   ./scripts/profile/profile-ecrawl-dut.sh
#
# Defaults match the node9901 setup: TREE=/data1/erbmi1/ecrawl-synt, DUT=/tmp/dut.
#
# Outputs under OUT/ (default /tmp/ereport-profile-<utc>/):
#   cold/nowrite/{ecrawl,dut}/   wall + optional perf/strace; ecrawl verbose once
#   hot/nowrite/{ecrawl,dut}/    wall
#   cold/nostat/ecrawl/          wall (--no-stat --count)
#   hot/nostat/ecrawl/           wall
#   Also: OUT.tar.gz
#
# Knobs (env):
#   TREE=/data1/erbmi1/ecrawl-synt
#   OUT=/tmp/ereport-profile-...
#   DUT_BIN=/tmp/dut                 dut binary (file, not a directory)
#   THREADS=16                       dut -t
#   ECRAWL_CRAWL_THREADS=16          same worker budget as dut -t 16
#   DROP_CACHES=1                    allow sync + drop_caches for cold passes
#   RELAX_PTRACE=1                   set yama ptrace_scope=0 so strace works (root)
#   DO_WALL=1 DO_PERF=1 DO_STRACE=1   perf/strace only on cold/nowrite
#   DO_VERBOSE_SUMMARY=1             cold/nowrite/ecrawl --verbose (queue metrics)
#   PERF_FREQ=99 PERF_CALLGRAPH=dwarf
#   BUILD=1                          make -j ecrawl (+ -g)
#   KEEP_PERF_DATA=0
#
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
TREE=${TREE:-/data1/erbmi1/ecrawl-synt}
OUT=${OUT:-/tmp/ereport-profile-$(date -u +%Y%m%d-%H%M%S)}
DUT_BIN=${DUT_BIN:-/tmp/dut}
THREADS=${THREADS:-16}
ECRAWL_CRAWL_THREADS=${ECRAWL_CRAWL_THREADS:-16}
DROP_CACHES=${DROP_CACHES:-1}
RELAX_PTRACE=${RELAX_PTRACE:-1}
DO_WALL=${DO_WALL:-1}
DO_PERF=${DO_PERF:-1}
DO_STRACE=${DO_STRACE:-1}
DO_VERBOSE_SUMMARY=${DO_VERBOSE_SUMMARY:-1}
PERF_FREQ=${PERF_FREQ:-99}
PERF_CALLGRAPH=${PERF_CALLGRAPH:-dwarf}
BUILD=${BUILD:-1}
KEEP_PERF_DATA=${KEEP_PERF_DATA:-0}

export ECRAWL_CRAWL_THREADS

die() { echo "ERROR: $*" >&2; exit 1; }

resolve_dut() {
  # Accept DUT_BIN as a binary, or a directory containing it.
  if [[ -n "$DUT_BIN" ]]; then
    if [[ -x "$DUT_BIN" && -f "$DUT_BIN" ]]; then
      return 0
    fi
    if [[ -x "$DUT_BIN/dut" ]]; then
      DUT_BIN=$DUT_BIN/dut
      return 0
    fi
  fi
  for cand in /tmp/dut /tmp/dut/dut "$HOME/git/dut/dut" "$REPO/../dut/dut"; do
    if [[ -x "$cand" && -f "$cand" ]]; then
      DUT_BIN=$cand
      return 0
    fi
  done
  if command -v dut >/dev/null 2>&1; then
    DUT_BIN=$(command -v dut)
    return 0
  fi
  die "dut not found; copy the binary to /tmp/dut (or set DUT_BIN=)"
}

need_cmd() { command -v "$1" >/dev/null 2>&1 || die "missing command: $1"; }

# cache_mode: cold → sync + drop_caches when allowed; hot → no-op.
maybe_drop_caches() {
  local cache_mode=$1
  if [[ "$cache_mode" != "cold" ]]; then
    return 0
  fi
  if [[ "$DROP_CACHES" != "1" ]]; then
    return 0
  fi
  if [[ ! -w /proc/sys/vm/drop_caches ]]; then
    echo "WARN: cold pass but /proc/sys/vm/drop_caches not writable (need root); continuing warm" >&2
    return 0
  fi
  sync
  echo 3 > /proc/sys/vm/drop_caches
  echo "    drop_caches=ok"
}

relax_ptrace() {
  if [[ "$RELAX_PTRACE" != "1" ]]; then
    return 0
  fi
  if [[ -w /proc/sys/kernel/yama/ptrace_scope ]]; then
    local prev
    prev=$(cat /proc/sys/kernel/yama/ptrace_scope 2>/dev/null || echo "?")
    echo 0 > /proc/sys/kernel/yama/ptrace_scope
    echo "    yama.ptrace_scope: $prev -> 0 (for strace)"
  else
    echo "WARN: cannot write yama/ptrace_scope; strace may fail with EPERM" >&2
  fi
}

write_env() {
  {
    echo "timestamp=$(date -Is)"
    echo "hostname=$(hostname)"
    echo "uid=$(id -u) euid=$(id -u) user=$(id -un)"
    echo "repo=$REPO"
    echo "tree=$TREE"
    echo "out=$OUT"
    echo "ecrawl=$REPO/ecrawl"
    echo "dut=$DUT_BIN"
    echo "threads=$THREADS"
    echo "ECRAWL_CRAWL_THREADS=$ECRAWL_CRAWL_THREADS"
    echo "DROP_CACHES=$DROP_CACHES"
    echo "RELAX_PTRACE=$RELAX_PTRACE"
    echo "PERF_FREQ=$PERF_FREQ"
    echo "PERF_CALLGRAPH=$PERF_CALLGRAPH"
    echo "uname=$(uname -a)"
    findmnt -T "$TREE" -n -o TARGET,SOURCE,FSTYPE 2>/dev/null | sed 's/^/tree_mount=/' || true
    if [[ -x "$REPO/ecrawl" ]]; then
      echo "ecrawl_sha256=$(sha256sum "$REPO/ecrawl" | awk '{print $1}')"
    fi
    if [[ -x "$DUT_BIN" ]]; then
      echo "dut_sha256=$(sha256sum "$DUT_BIN" | awk '{print $1}')"
    fi
  } >"$OUT/env.txt"
}

# run_wall <cache_mode> <label> <dest> <cmd...>
run_wall() {
  local cache_mode=$1 label=$2 dest=$3
  shift 3
  mkdir -p "$dest"
  maybe_drop_caches "$cache_mode"
  echo "==> wall/$cache_mode/$label: $*"
  local t0 t1
  t0=$(date +%s.%N)
  set +e
  "$@" >"$dest/wall.stdout.txt" 2>"$dest/wall.stderr.txt"
  local rc=$?
  set -e
  t1=$(date +%s.%N)
  python3 -c 'import sys; print("%.6f" % (float(sys.argv[2])-float(sys.argv[1])))' "$t0" "$t1" \
    >"$dest/wall.sec.txt"
  echo "rc=$rc" >"$dest/wall.rc.txt"
  echo "    wall_sec=$(cat "$dest/wall.sec.txt") rc=$rc"
}

# run_perf <cache_mode> <label> <dest> <cmd...>
run_perf() {
  local cache_mode=$1 label=$2 dest=$3
  shift 3
  need_cmd perf
  mkdir -p "$dest"
  maybe_drop_caches "$cache_mode"
  echo "==> perf/$cache_mode/$label: perf record --call-graph $PERF_CALLGRAPH -F $PERF_FREQ"
  local data="$dest/perf.data"
  set +e
  perf record --call-graph "$PERF_CALLGRAPH" -F "$PERF_FREQ" -o "$data" -- \
    "$@" >"$dest/perf.stdout.txt" 2>"$dest/perf.record-stderr.txt"
  local rc=$?
  set -e
  echo "rc=$rc" >"$dest/perf.rc.txt"
  if [[ $rc -ne 0 || ! -s "$data" ]]; then
    echo "    perf record failed (rc=$rc); see perf.record-stderr.txt" >&2
    return 0
  fi
  perf script -i "$data" >"$dest/perf.script.txt" 2>"$dest/perf.script-stderr.txt" || true
  gzip -9 -f "$dest/perf.script.txt"
  perf report -i "$data" --stdio >"$dest/perf.report.txt" 2>/dev/null || \
    echo "perf report failed" >"$dest/perf.report.txt"
  perf report -i "$data" --stdio -g graph,0.5,caller >"$dest/perf.report.caller.txt" 2>/dev/null || true
  if [[ "$KEEP_PERF_DATA" != "1" ]]; then
    rm -f "$data"
  fi
  echo "    wrote $dest/perf.script.txt.gz (upload this) and perf.report.txt"
}

# run_strace <cache_mode> <label> <dest> <cmd...>
run_strace() {
  local cache_mode=$1 label=$2 dest=$3
  shift 3
  need_cmd strace
  mkdir -p "$dest"
  maybe_drop_caches "$cache_mode"
  echo "==> strace/$cache_mode/$label: strace -f -c (timing not representative)"
  set +e
  strace -f -c -o "$dest/strace.summary.txt" -- \
    "$@" >"$dest/strace.stdout.txt" 2>"$dest/strace.cmd-stderr.txt"
  local rc=$?
  set -e
  echo "rc=$rc" >"$dest/strace.rc.txt"
  if [[ ! -s "$dest/strace.summary.txt" && -s "$dest/strace.cmd-stderr.txt" ]]; then
    cp "$dest/strace.cmd-stderr.txt" "$dest/strace.summary.txt"
  fi
  if grep -q 'Operation not permitted' "$dest/strace.summary.txt" 2>/dev/null; then
    echo "    WARN: strace EPERM — run as root with RELAX_PTRACE=1" >&2
  fi
  echo "    wrote $dest/strace.summary.txt"
}

wall_sec_or_na() {
  local f=$1
  if [[ -f "$f" ]]; then
    cat "$f"
  else
    echo "n/a"
  fi
}

main() {
  [[ -d "$TREE" ]] || die "TREE does not exist: $TREE"
  need_cmd gcc
  need_cmd make
  need_cmd python3
  need_cmd gzip
  resolve_dut

  if [[ "$(id -u)" -ne 0 ]]; then
    echo "WARN: not root — DROP_CACHES / RELAX_PTRACE / perf may be limited" >&2
  fi

  mkdir -p \
    "$OUT/cold/nowrite/ecrawl" "$OUT/cold/nowrite/dut" \
    "$OUT/hot/nowrite/ecrawl" "$OUT/hot/nowrite/dut" \
    "$OUT/cold/nostat/ecrawl" "$OUT/hot/nostat/ecrawl"
  echo "REPO=$REPO"
  echo "TREE=$TREE"
  echo "OUT=$OUT"
  echo "DUT_BIN=$DUT_BIN"
  echo "ECRAWL_CRAWL_THREADS=$ECRAWL_CRAWL_THREADS"

  relax_ptrace

  if [[ "$BUILD" == "1" ]]; then
    echo "==> building ecrawl (+ -g for perf symbols)"
    make -C "$REPO" -j"$(nproc)" ecrawl CFLAGS="-O2 -Wall -Wextra -Wunused-parameter -pthread -g"
  fi
  [[ -x "$REPO/ecrawl" ]] || die "ecrawl binary missing at $REPO/ecrawl"

  write_env

  # Timed / profiled runs: no --verbose (IO wrappers and counters distort the hot path).
  local -a nowrite_ecrawl_cmd=("$REPO/ecrawl" --no-write "$TREE")
  local -a nostat_ecrawl_cmd=("$REPO/ecrawl" --no-stat --count "$TREE")
  local -a dut_cmd=("$DUT_BIN" -d 0 -n 1 -c -b -t "$THREADS" "$TREE")

  printf '%s\n' "${nowrite_ecrawl_cmd[@]}" >"$OUT/cold/nowrite/ecrawl/cmdline.txt"
  cp "$OUT/cold/nowrite/ecrawl/cmdline.txt" "$OUT/hot/nowrite/ecrawl/cmdline.txt"
  printf '%s\n' "${dut_cmd[@]}" >"$OUT/cold/nowrite/dut/cmdline.txt"
  cp "$OUT/cold/nowrite/dut/cmdline.txt" "$OUT/hot/nowrite/dut/cmdline.txt"
  printf '%s\n' "${nostat_ecrawl_cmd[@]}" >"$OUT/cold/nostat/ecrawl/cmdline.txt"
  cp "$OUT/cold/nostat/ecrawl/cmdline.txt" "$OUT/hot/nostat/ecrawl/cmdline.txt"

  if [[ "$DO_WALL" == "1" ]]; then
    # Cold nowrite: drop before each tool.
    run_wall cold ecrawl "$OUT/cold/nowrite/ecrawl" "${nowrite_ecrawl_cmd[@]}"
    run_wall cold dut "$OUT/cold/nowrite/dut" "${dut_cmd[@]}"
    # Hot nowrite: no drops; reuse cache from the cold passes.
    run_wall hot ecrawl "$OUT/hot/nowrite/ecrawl" "${nowrite_ecrawl_cmd[@]}"
    run_wall hot dut "$OUT/hot/nowrite/dut" "${dut_cmd[@]}"
    # Cold/hot nostat (--no-stat --count); no dut peer.
    run_wall cold nostat "$OUT/cold/nostat/ecrawl" "${nostat_ecrawl_cmd[@]}"
    run_wall hot nostat "$OUT/hot/nostat/ecrawl" "${nostat_ecrawl_cmd[@]}"
  fi

  # Perf/strace only on cold nowrite (primary comparison).
  if [[ "$DO_PERF" == "1" ]]; then
    run_perf cold ecrawl "$OUT/cold/nowrite/ecrawl" "${nowrite_ecrawl_cmd[@]}"
    run_perf cold dut "$OUT/cold/nowrite/dut" "${dut_cmd[@]}"
  fi
  if [[ "$DO_STRACE" == "1" ]]; then
    run_strace cold ecrawl "$OUT/cold/nowrite/ecrawl" "${nowrite_ecrawl_cmd[@]}"
    run_strace cold dut "$OUT/cold/nowrite/dut" "${dut_cmd[@]}"
  fi

  # Verbose summary under cold/nowrite for full end-of-run metrics.
  if [[ "$DO_VERBOSE_SUMMARY" == "1" ]]; then
    echo "==> ecrawl --verbose summary (cold/nowrite; untimed vs dut)"
    maybe_drop_caches cold
    set +e
    "$REPO/ecrawl" --verbose --no-write "$TREE" \
      >"$OUT/cold/nowrite/ecrawl/verbose.stdout.txt" \
      2>"$OUT/cold/nowrite/ecrawl/verbose.stderr.txt"
    echo "rc=$?" >"$OUT/cold/nowrite/ecrawl/verbose.rc.txt"
    set -e
  fi

  local ec_cold dut_cold ec_hot dut_hot ns_cold ns_hot
  ec_cold=$(wall_sec_or_na "$OUT/cold/nowrite/ecrawl/wall.sec.txt")
  dut_cold=$(wall_sec_or_na "$OUT/cold/nowrite/dut/wall.sec.txt")
  ec_hot=$(wall_sec_or_na "$OUT/hot/nowrite/ecrawl/wall.sec.txt")
  dut_hot=$(wall_sec_or_na "$OUT/hot/nowrite/dut/wall.sec.txt")
  ns_cold=$(wall_sec_or_na "$OUT/cold/nostat/ecrawl/wall.sec.txt")
  ns_hot=$(wall_sec_or_na "$OUT/hot/nostat/ecrawl/wall.sec.txt")

  {
    echo "OUT=$OUT"
    echo "upload:"
    echo "  $OUT/cold/nowrite/ecrawl/perf.script.txt.gz"
    echo "  $OUT/cold/nowrite/dut/perf.script.txt.gz"
    echo "  $OUT/cold/nowrite/ecrawl/strace.summary.txt"
    echo "  $OUT/cold/nowrite/dut/strace.summary.txt"
    echo "  $OUT/cold/nowrite/ecrawl/wall.stdout.txt"
    echo "  $OUT/cold/nowrite/dut/wall.stdout.txt"
    echo "  $OUT/hot/nowrite/ecrawl/wall.stdout.txt"
    echo "  $OUT/hot/nowrite/dut/wall.stdout.txt"
    echo "  $OUT/cold/nostat/ecrawl/wall.stdout.txt"
    echo "  $OUT/hot/nostat/ecrawl/wall.stdout.txt"
    echo "  $OUT/env.txt"
    echo "wall_sec cold/nowrite ecrawl=$ec_cold dut=$dut_cold"
    echo "wall_sec hot/nowrite  ecrawl=$ec_hot dut=$dut_hot"
    echo "wall_sec cold/nostat  ecrawl=$ns_cold"
    echo "wall_sec hot/nostat   ecrawl=$ns_hot"
    if [[ "$ec_cold" != "n/a" && "$dut_cold" != "n/a" ]]; then
      python3 -c 'import sys; a=float(sys.argv[1]); b=float(sys.argv[2]); print("ratio cold/nowrite ecrawl/dut=%.3f" % (a/b if b else float("nan")))' \
        "$ec_cold" "$dut_cold"
    fi
  } | tee "$OUT/README.txt"

  # --no-same-owner so extracting as another user (or in a container) does not fail
  local tar="$OUT.tar.gz"
  tar --no-same-owner -C "$(dirname "$OUT")" -czf "$tar" "$(basename "$OUT")" 2>/dev/null \
    || tar -C "$(dirname "$OUT")" -czf "$tar" "$(basename "$OUT")"
  echo "tarball=$tar"
}

main "$@"
