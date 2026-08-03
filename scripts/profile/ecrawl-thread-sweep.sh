#!/usr/bin/env bash
#
# profile/ecrawl-thread-sweep.sh
#
# SPDX-License-Identifier: MIT
#
# Sweep ecrawl's thread counts over one fixture at a time and print the table
# needed to pick defaults from measurement instead of guesswork.
#
# ecrawl's DEFAULT_CRAWL_THREADS (16) / DEFAULT_STAT_THREADS (8) are constants,
# not derived from the machine, so on a 96-core host a crawl can sit at
# avg_active_workers=16.00 using ~12 of 96 available cores. What the right cap
# is depends on the fixture (directory shape) and on the filesystem — a local
# xfs and an NFS mount do not want the same number — which is exactly why this
# measures rather than assumes. It changes no defaults.
#
# For each (fixture, mode, crawl_threads, stat_threads) cell it runs one clean
# `ecrawl --verbose` pass, cold-cache when possible, and records:
#
#   ops/s          entries / elapsed_sec (the headline)
#   cpu%           "Percent of CPU this job got" from /usr/bin/time -v; compare
#                  against 100 x nproc to see how much of the machine was used
#   avgWkr         avg_active_workers (1 Hz samples of busy crawl workers). Blank
#                  for runs too short to collect two samples.
#   shards         uid_shard_*.bin files the capture actually wrote. Read it with
#                  wr_idle: one shard is one writer thread doing all the zstd, no
#                  matter how many writer threads exist, so a single-owner tree
#                  caps write throughput on one core.
#   wr_idle(s)     writer_queue_wait_ns (write mode). The counter sums both ends of
#                  the batch queue, but wait_writer_push is normally 0, so what it
#                  reports is writer threads waiting for batches -- not crawl
#                  threads held up by the writers. Check wait_writer_push before
#                  reading a large value as back-pressure.
#   tailInl        stat_batches_tail_inlined: batches the crawl thread stat'ed
#                  itself instead of handing to a stat worker. High means the
#                  stat pool is idle and ECRAWL_STAT_THREADS is not the knob.
#   maxRSS         peak RSS, since raising thread counts costs memory too
#
# Usage:
#   scripts/profile/ecrawl-thread-sweep.sh <synth-root> [results-dir]
#
# Required:
#   <synth-root>   the tree generate-ecrawl-adversarial-tree.sh produced (the
#                  dir holding neutral_flat/, ereport_badge_fixtures/, ...).
#
# Optional positional:
#   [results-dir]  where to write logs (default: ./ecrawl-sweep-<timestamp>).
#
# Environment knobs:
#   ECRAWL_BIN=./ecrawl        path to the binary (auto-detect as elsewhere).
#   FIXTURES="a b"             fixtures to sweep (default: neutral_flat
#                              ereport_badge_fixtures — deep/inline-stat
#                              dominated and many-uid/writer heavy).
#   CRAWL_THREADS_LIST="..."   default "16 24 32 48 64 96".
#   STAT_THREADS_LIST="..."    default "8". Cells are the cross product, so keep
#                              this short.
#   STAT_SWEEP_FIXTURES="ereport_badge_fixtures"
#                              fixtures that also get STAT_THREADS_EXTRA, so the
#                              stat pool is only swept where writers are busy.
#   STAT_THREADS_EXTRA="32"    extra stat-thread values for those fixtures.
#   DO_NOWRITE=1 / DO_WRITE=1  which modes to run (both by default).
#   OUTPUT_BASE=<dir>          throwaway write-mode shard output (default:
#                              <results-dir>/_shard_output, removed at the end).
#                              Needs room for a full capture of each fixture.
#   DROP_CACHES=1              drop the page cache before every run when root
#                              (default 1); warns once and proceeds warm if not.
#   REPS=1                     passes per cell; the table averages ops/s.
#
# Output layout (under <results-dir>):
#   env.txt                                  host / tool / env snapshot
#   <fixture>/<mode>/c<ct>_s<st>.summary.txt ecrawl --verbose stdout
#   <fixture>/<mode>/c<ct>_s<st>.time.txt    /usr/bin/time -v
#   cells.tsv                                one row per cell, machine-readable
#   SUMMARY_TABLE.txt                        the table to read
#   ecrawl-sweep-<timestamp>.tar.gz          tarball of the whole results dir
#
set -uo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../lib" && pwd)/common.sh"

# ---- args ------------------------------------------------------------------
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <synth-root> [results-dir]" >&2
  exit 2
fi
SYNTH_ROOT=${1%/}
if [[ ! -d "$SYNTH_ROOT" ]]; then
  echo "ERROR: synth-root '$SYNTH_ROOT' is not a directory" >&2
  exit 2
fi

TS=$(date +%Y%m%d-%H%M%S)
RESULTS_DIR=${2:-"./ecrawl-sweep-$TS"}
mkdir -p "$RESULTS_DIR" || { echo "ERROR: cannot create results dir '$RESULTS_DIR'" >&2; exit 1; }
RESULTS_DIR=$(cd "$RESULTS_DIR" && pwd)

ECRAWL_BIN=$(find_bin ecrawl ECRAWL_BIN) || exit 1

# ---- config ----------------------------------------------------------------
OUTPUT_BASE=${OUTPUT_BASE:-"$RESULTS_DIR/_shard_output"}
DO_NOWRITE=${DO_NOWRITE:-1}
DO_WRITE=${DO_WRITE:-1}
DROP_CACHES=${DROP_CACHES:-1}
REPS=${REPS:-1}
CRAWL_THREADS_LIST=${CRAWL_THREADS_LIST:-"16 24 32 48 64 96"}
STAT_THREADS_LIST=${STAT_THREADS_LIST:-"8"}
STAT_SWEEP_FIXTURES=${STAT_SWEEP_FIXTURES:-"ereport_badge_fixtures"}
STAT_THREADS_EXTRA=${STAT_THREADS_EXTRA:-"32"}

if [[ -n "${FIXTURES:-}" ]]; then
  read -ra FIXLIST <<<"$FIXTURES"
else
  FIXLIST=()
  for f in neutral_flat ereport_badge_fixtures; do
    [[ -d "$SYNTH_ROOT/$f" ]] && FIXLIST+=("$f")
  done
fi
if [[ ${#FIXLIST[@]} -eq 0 ]]; then
  echo "ERROR: no fixtures found under '$SYNTH_ROOT' (set FIXTURES=...)" >&2
  exit 1
fi

HAVE_TIME=0; TIME_BIN=""
for c in /usr/bin/time /bin/time; do
  [[ -x "$c" ]] && { TIME_BIN="$c"; HAVE_TIME=1; break; }
done
if [[ "$HAVE_TIME" != "1" ]]; then
  echo "WARNING: /usr/bin/time not found; the cpu% and maxRSS columns will be empty." >&2
fi

CELLS="$RESULTS_DIR/cells.tsv"
printf 'fixture\tmode\tcrawl_threads\tstat_threads\trep\tentries\telapsed_sec\tops_per_sec\tcpu_pct\tavg_workers\tshards\twriter_wait_sec\ttail_inlined\tmax_rss_kb\trc\n' >"$CELLS"

# ---- helpers ---------------------------------------------------------------
g_warned_no_drop=0
maybe_drop_caches() {
  [[ "$DROP_CACHES" == "1" ]] || return 0
  if [[ "$(id -u)" == "0" ]]; then
    sync
    echo 3 >/proc/sys/vm/drop_caches 2>/dev/null || echo "    [WARN: could not drop caches]"
  elif [[ "$g_warned_no_drop" == "0" ]]; then
    echo "    [WARN: DROP_CACHES=1 but not root; caches left warm — timings are in-cache]"
    g_warned_no_drop=1
  fi
}

kv_get() { # file key
  awk -F= -v k="$2" '$1==k {print $2; exit}' "$1" 2>/dev/null
}

# "Percent of CPU this job got: 1195%" -> 1195
time_cpu_pct() { sed -n 's/.*Percent of CPU this job got: *\([0-9]*\)%.*/\1/p' "$1" 2>/dev/null | head -n1; }
time_max_rss() { sed -n 's/.*Maximum resident set size (kbytes): *\([0-9]*\).*/\1/p' "$1" 2>/dev/null | head -n1; }

run_cell() {
  local fixture=$1 mode=$2 start=$3 dest=$4 outdir=$5 ct=$6 st=$7 rep=$8
  local tag="c${ct}_s${st}"
  [[ "$REPS" -gt 1 ]] && tag="${tag}.rep${rep}"
  local argv=("$ECRAWL_BIN" --verbose)
  local sum="$dest/$tag.summary.txt"
  local tim="$dest/$tag.time.txt"
  local rc entries elapsed ops cpu wkr wait tail rss shards samples

  [[ "$mode" == "nowrite" ]] && argv=("$ECRAWL_BIN" --no-write --verbose)
  argv+=("$start")
  if [[ "$mode" == "write" ]]; then
    rm -rf "$outdir"; mkdir -p "$outdir"
    argv+=("$outdir")
  fi

  maybe_drop_caches
  echo "    $fixture/$mode crawl=$ct stat=$st rep=$rep"
  if [[ "$HAVE_TIME" == "1" ]]; then
    ECRAWL_CRAWL_THREADS="$ct" ECRAWL_STAT_THREADS="$st" \
      "$TIME_BIN" -v -o "$tim" "${argv[@]}" >"$sum" 2>"$dest/$tag.stderr.txt"
  else
    ECRAWL_CRAWL_THREADS="$ct" ECRAWL_STAT_THREADS="$st" \
      "${argv[@]}" >"$sum" 2>"$dest/$tag.stderr.txt"
  fi
  rc=$?
  shards=""
  if [[ "$mode" == "write" ]]; then
    shards=$(find "$outdir" -name 'uid_shard_*.bin' 2>/dev/null | wc -l)
    rm -rf "$outdir"
  fi

  entries=$(kv_get "$sum" entries)
  elapsed=$(kv_get "$sum" elapsed_sec)
  wkr=$(kv_get "$sum" avg_active_workers)
  # Sampled once per second: one sample says nothing about a 1-second run, so drop it
  # rather than print an average of a single edge-of-run observation.
  samples=$(kv_get "$sum" active_workers_samples)
  [[ -z "$samples" || "$samples" -lt 2 ]] && wkr=""
  wait=$(kv_get "$sum" writer_queue_wait_ns)
  tail=$(kv_get "$sum" stat_batches_tail_inlined)
  cpu=$(time_cpu_pct "$tim")
  rss=$(time_max_rss "$tim")
  ops=$(awk -v e="${entries:-}" -v s="${elapsed:-}" 'BEGIN{ if (e!="" && s!="" && s+0>0) printf "%.0f", e/s }')
  wait=$(awk -v w="${wait:-}" 'BEGIN{ if (w!="") printf "%.1f", w/1e9 }')

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$fixture" "$mode" "$ct" "$st" "$rep" "${entries:-}" "${elapsed:-}" "${ops:-}" \
    "${cpu:-}" "${wkr:-}" "${shards:-}" "${wait:-}" "${tail:-}" "${rss:-}" "$rc" >>"$CELLS"
}

stat_values_for() { # fixture -> stat-thread values
  local fixture=$1 vals="$STAT_THREADS_LIST"

  case " $STAT_SWEEP_FIXTURES " in
    *" $fixture "*) vals="$vals $STAT_THREADS_EXTRA" ;;
  esac
  # De-duplicate so an overlapping STAT_THREADS_EXTRA does not run a cell twice.
  echo "$vals" | tr ' ' '\n' | awk 'NF && !seen[$0]++' | tr '\n' ' '
}

sweep_one() {
  local fixture=$1 start=$2
  local modes=() mode ct st r
  [[ "$DO_NOWRITE" == "1" ]] && modes+=("nowrite")
  [[ "$DO_WRITE" == "1" ]] && modes+=("write")
  if [[ ${#modes[@]} -eq 0 ]]; then
    echo "    (no modes enabled; set DO_NOWRITE=1 or DO_WRITE=1)"; return
  fi

  echo "==> $fixture  ($start)"
  for mode in "${modes[@]}"; do
    local dest="$RESULTS_DIR/$fixture/$mode"
    mkdir -p "$dest"
    for ct in $CRAWL_THREADS_LIST; do
      for st in $(stat_values_for "$fixture"); do
        for ((r = 1; r <= REPS; r++)); do
          run_cell "$fixture" "$mode" "$start" "$dest" "$OUTPUT_BASE/$fixture-$mode" "$ct" "$st" "$r"
        done
      done
    done
  done
}

# ---- env snapshot ----------------------------------------------------------
{
  echo "# ecrawl thread sweep"
  echo "timestamp=$TS"
  echo "synth_root=$SYNTH_ROOT"
  echo "synth_root_fstype=$(fs_type "$SYNTH_ROOT")"
  echo "results_dir=$RESULTS_DIR"
  echo "output_base=$OUTPUT_BASE"
  echo "output_base_fstype=$(fs_type "$(dirname "$OUTPUT_BASE")")"
  echo "ecrawl_bin=$ECRAWL_BIN"
  echo "modes: nowrite=$DO_NOWRITE write=$DO_WRITE reps=$REPS drop_caches=$DROP_CACHES"
  echo "crawl_threads_list=$CRAWL_THREADS_LIST"
  echo "stat_threads_list=$STAT_THREADS_LIST"
  echo "stat_sweep_fixtures=$STAT_SWEEP_FIXTURES stat_threads_extra=$STAT_THREADS_EXTRA"
  echo "fixtures: ${FIXLIST[*]}"
  echo
  echo "## host"
  echo "uname=$(uname -a)"
  echo "nproc=$(nproc 2>/dev/null || echo '?')"
  echo "ulimit_n=$(ulimit -n)"
  echo "cpu_model=$(awk -F: '/model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null | sed 's/^ //')"
  echo "mem_total_kb=$(awk '/MemTotal/{print $2; exit}' /proc/meminfo 2>/dev/null)"
  echo
  echo "## ecrawl-related env"
  env | grep -E '^ECRAWL_' | sort || true
} >"$RESULTS_DIR/env.txt"

echo "ecrawl thread sweep starting; results -> $RESULTS_DIR"
sed 's/^/  /' "$RESULTS_DIR/env.txt"
echo

# ---- run -------------------------------------------------------------------
mkdir -p "$OUTPUT_BASE"
for f in "${FIXLIST[@]}"; do
  if [[ ! -d "$SYNTH_ROOT/$f" ]]; then
    echo "==> $f  (MISSING under $SYNTH_ROOT; skipped)"
    continue
  fi
  sweep_one "$f" "$SYNTH_ROOT/$f"
done
rm -rf "$OUTPUT_BASE"

# ---- summary table ---------------------------------------------------------
SUMMARY_TABLE="$RESULTS_DIR/SUMMARY_TABLE.txt"
{
  echo "ecrawl thread sweep — SUMMARY TABLE"
  sed -n 's/^timestamp=/  timestamp=/p;s/^synth_root=/  synth_root=/p;s/^synth_root_fstype=/  fstype=/p;s/^nproc=/  nproc=/p;s/^cpu_model=/  cpu=/p' \
    "$RESULTS_DIR/env.txt"
  echo "  cpu% is of 100% per core, so the machine ceiling is 100 x nproc."
  echo
  awk -F'\t' '
    NR == 1 { next }
    {
      key = $1 "\t" $2 "\t" $3 "\t" $4
      if (!(key in seen)) { order[++n] = key; seen[key] = 1 }
      reps[key]++
      if ($8 != "") { ops[key] += $8; opsn[key]++ }
      if ($7 != "") { el[key] += $7; eln[key]++ }
      # Everything else is reported from the last rep of the cell.
      ent[key] = $6; cpu[key] = $9; wkr[key] = $10; shards[key] = $11
      wait[key] = $12; tail[key] = $13; rss[key] = $14
      if ($15 != "0") bad[key] = $15
    }
    function h(v) { return v == "" ? "-" : v }
    END {
      printf "%-24s %-8s %6s %5s %10s %8s %7s %7s %7s %11s %12s %9s %s\n", \
             "fixture", "mode", "crawl", "stat", "entries", "ops/s", "cpu%", "avgWkr", \
             "shards", "wr_idle(s)", "tailInl", "maxRSS_MB", "elapsed(s)"
      for (i = 1; i <= n; i++) {
        key = order[i]
        split(key, f, "\t")
        o = (opsn[key] > 0) ? sprintf("%.0f", ops[key] / opsn[key]) : "-"
        e = (eln[key] > 0) ? sprintf("%.2f", el[key] / eln[key]) : "-"
        m = (rss[key] != "") ? sprintf("%.0f", rss[key] / 1024) : "-"
        printf "%-24s %-8s %6s %5s %10s %8s %7s %7s %7s %11s %12s %9s %s%s\n", \
               f[1], f[2], f[3], f[4], h(ent[key]), o, h(cpu[key]), h(wkr[key]), \
               h(shards[key]), h(wait[key]), h(tail[key]), m, e, \
               (key in bad) ? "  FAILED(rc=" bad[key] ")" : ""
      }
    }
  ' "$CELLS"
  echo
  echo "Reading it: ops/s is the answer, cpu% says whether more threads bought"
  echo "more work or just more waiting, and wr_idle(s) is writer threads waiting"
  echo "for batches -- it grows when the writers are starved, not when they are"
  echo "the limit. For real back-pressure read wait_writer_push in the summary."
  echo "Check shards before blaming the crawl side: one shard is one writer"
  echo "thread compressing everything, so a single-owner tree is writer-capped"
  echo "at any crawl thread count."
  echo "No defaults change on the strength of one host: the right cap differs"
  echo "between local disk and NFS, so sweep both before touching ecrawl.c."
} >"$SUMMARY_TABLE"

# ---- tarball ---------------------------------------------------------------
TARBALL="$RESULTS_DIR/../$(basename "$RESULTS_DIR").tar.gz"
if tar -czf "$TARBALL" -C "$(dirname "$RESULTS_DIR")" "$(basename "$RESULTS_DIR")" 2>/dev/null; then
  TARBALL=$(cd "$(dirname "$TARBALL")" && pwd)/$(basename "$TARBALL")
  echo
  echo "DONE."
  echo "  Cells (tsv):     $CELLS"
  echo "  Summary table:   $SUMMARY_TABLE"
  echo "  Tarball (upload this): $TARBALL"
else
  echo
  echo "DONE."
  echo "  Cells (tsv):     $CELLS"
  echo "  Summary table:   $SUMMARY_TABLE"
  echo "  (tarball creation failed; upload the $RESULTS_DIR directory)"
fi
echo
echo "----- SUMMARY_TABLE.txt -----"
cat "$SUMMARY_TABLE"
