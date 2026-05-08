#!/usr/bin/env bash
# Pretty-print ecrawl_progress.csv (ECRAWL_PROGRESS_LOG): rolling ops_rate vs per-second deltas.
#
# Usage:
#   ./parse-ecrawl-progress.sh ecrawl_progress.csv
#   ./parse-ecrawl-progress.sh --deep ecrawl_progress.csv   # + wait/stat batch deltas (prev row → this row)
#   ./parse-ecrawl-progress.sh --last ecrawl_progress.csv    # header + newest row only (for watch)
#   ./parse-ecrawl-progress.sh --deep --last ecrawl_progress.csv
#   tail -10000 ecrawl_progress.csv | ./parse-ecrawl-progress.sh -
#
# Live tail with watch (1 Hz, matches typical CSV rate):
#   watch -n 1 ./parse-ecrawl-progress.sh --last /path/to/ecrawl_progress.csv
#   watch -n 1 ./parse-ecrawl-progress.sh --deep --last /path/to/ecrawl_progress.csv
#
# Columns from ecrawl (see ecrawl.c): ops_rate = rolling window_entries / seconds_seen (~10s window).
# delta_total_entries is global total_entries gain over the previous 1s sample (often closer to "work/sec").
# wait_* columns are lifetime counters; --deep prints per-interval deltas (approx /s when CSV is 1 Hz).

set -euo pipefail

run_parse() {
  awk -F',' '
BEGIN { hdr = 0 }
NR == 1 {
  for (i = 1; i <= NF; i++) {
    gsub(/\r/, "", $i)
    col[$i] = i
  }
  next
}
{
  if (!hdr) {
    printf "%-12s %16s %18s %14s %12s %12s\n",
      "elapsed_sec", "ops_rate(10s)", "delta_entries/s", "window_ent", "q_depth", "wq_depth"
    hdr = 1
  }
  es = $(col["elapsed_sec"])
  opr = $(col["ops_rate"])
  dt = $(col["delta_total_entries"])
  we = $(col["window_entries"])
  qd = $(col["queue_depth"])
  wq = $(col["writer_queue_depth"])
  gsub(/\r/, "", es)
  gsub(/\r/, "", opr)
  gsub(/\r/, "", dt)
  gsub(/\r/, "", we)
  gsub(/\r/, "", qd)
  gsub(/\r/, "", wq)
  printf "%-12s %16s %18s %14s %12s %12s\n", es, opr, dt, we, qd, wq
}
' "$@"
}

run_parse_deep() {
  awk -F',' '
function ix(name) { return col[name] }
function val(row, name,    j) {
  j = ix(name)
  if (j < 1 || j > NF) return 0
  gsub(/\r/, "", $j)
  return $j + 0
}
BEGIN { hdr = 0; primed = 0 }
NR == 1 {
  for (i = 1; i <= NF; i++) {
    gsub(/\r/, "", $i)
    col[$i] = i
  }
  next
}
{
  if (!hdr) {
    printf "%-12s %16s %18s %12s %12s %10s %10s %10s %10s %10s %14s %12s %12s\n",
      "elapsed_sec", "ops_rate(10s)", "delta_ent/s", "q_depth", "wq_depth",
      "d_st_enq", "d_st_pop", "d_crawl_q", "d_sb_enq", "d_sb_done",
      "window_dirs", "d_readdir", "d_lstat"
    hdr = 1
  }
  es = $(col["elapsed_sec"]); gsub(/\r/, "", es)
  opr = $(col["ops_rate"]); gsub(/\r/, "", opr)
  dt   = val($0, "delta_total_entries")
  qd   = val($0, "queue_depth")
  wq   = val($0, "writer_queue_depth")
  wse  = val($0, "wait_stat_enqueue")
  wsp  = val($0, "wait_stat_pop")
  wcr  = val($0, "wait_crawl_tasks")
  sben = val($0, "stat_batches_enqueued")
  sbdo = val($0, "stat_batches_completed")
  wdi  = val($0, "window_dirs")
  drd  = val($0, "delta_readdir_calls")
  dls  = val($0, "delta_lstat_calls")

  if (!primed) {
    dwse = dwsp = dwcr = dsben = dsbdo = "-"
    primed = 1
  } else {
    dwse  = wse - pwse
    dwsp  = wsp - pwsp
    dwcr  = wcr - pwcr
    dsben = sben - psben
    dsbdo = sbdo - psbdo
  }
  pwse = wse; pwsp = wsp; pwcr = wcr; psben = sben; psbdo = sbdo

  printf "%-12s %16s %18s %12s %12s %10s %10s %10s %10s %10s %14s %12s %12s\n",
    es, opr, dt, qd, wq, dwse, dwsp, dwcr, dsben, dsbdo, wdi, drd, dls
}
' "$@"
}

deep=0
last=0
files=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deep)
      deep=1
      shift
      ;;
    --last)
      last=1
      shift
      ;;
    -*)
      echo "unknown option: $1" >&2
      exit 2
      ;;
    *)
      files+=("$1")
      shift
      ;;
  esac
done

if [[ ${#files[@]} -ne 1 ]]; then
  echo "usage: $0 [--deep] [--last] /path/to/ecrawl_progress.csv (or - for stdin)" >&2
  exit 2
fi

csv="${files[0]}"

if [[ "$last" -eq 1 ]]; then
  f="$csv"
  if [[ "$f" != "-" && ! -r "$f" ]]; then
    echo "cannot read: $f" >&2
    exit 1
  fi
  if [[ "$deep" -eq 1 ]]; then
    { head -n 1 "$f"; tail -n 1 "$f"; } | run_parse_deep -
  else
    { head -n 1 "$f"; tail -n 1 "$f"; } | run_parse -
  fi
  exit 0
fi

if [[ "$deep" -eq 1 ]]; then
  run_parse_deep "$csv"
else
  run_parse "$csv"
fi
