#!/usr/bin/env bash
# Pretty-print ecrawl_progress.csv (ECRAWL_PROGRESS_LOG): rolling ops_rate vs per-second deltas.
#
# Usage:
#   ./parse-ecrawl-progress.sh ecrawl_progress.csv
#   ./parse-ecrawl-progress.sh --last ecrawl_progress.csv    # header + newest row only (for watch)
#   tail -10000 ecrawl_progress.csv | ./parse-ecrawl-progress.sh -
#
# Live tail with watch (1 Hz, matches typical CSV rate):
#   watch -n 1 ./parse-ecrawl-progress.sh --last /path/to/ecrawl_progress.csv
#   watch -n 1 -t -d ./parse-ecrawl-progress.sh --last /path/to/ecrawl_progress.csv   # no banner, highlight changes
#
# Columns from ecrawl (see ecrawl.c): ops_rate = rolling window_entries / seconds_seen (~10s window).
# delta_total_entries is global total_entries gain over the previous 1s sample (often closer to "work/sec").

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

case "${1:-}" in
--last)
  f="${2:?usage: $0 --last /path/to/ecrawl_progress.csv}"
  if [[ ! -r "$f" ]]; then
    echo "cannot read: $f" >&2
    exit 1
  fi
  { head -n 1 "$f"; tail -n 1 "$f"; } | run_parse -
  ;;
*)
  csv="${1:?usage: $0 [--last] /path/to/ecrawl_progress.csv (or - for stdin)}"
  run_parse "$csv"
  ;;
esac
