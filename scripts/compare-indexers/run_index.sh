#!/usr/bin/env bash
#
# Time index / capture builds for each available tool on a tree root.
# Writes paper-style metrics: wall seconds, bytes, sec_per_1M_files, MiB_per_1M_files.
#
# Usage:
#   scripts/compare-indexers/run_index.sh <tree-root> [results-dir]
#
# Env:
#   TOOLS="ecrawl gufi xdu robinhood find fd du dua"
#                                   (find/fd/du/dua = live walk baselines only)
#   REPS=3 DROP_CACHES=0|1 THREADS=16
#   REPS_<TOOL>=n     repetitions for one tool, overriding REPS: REPS_GUFI=1
#                     keeps a 29-minute rollup to a single pass while the cheap
#                     rows still get their three. REPS_EREPORT_INDEX defaults to
#                     1 and cannot exceed REPS_ECRAWL, whose capture it indexes.
#   WORK_ROOT=<dir>   where tool indexes are written (default: <results-dir>/indexes)
#   INCLUDE_EREPORT_INDEX=1   also time ereport_index --make after ecrawl (separate row)
#   DO_NOWRITE=1      extra ecrawl row: full stat walk, no capture written
#   DO_NOSTAT=1       extra ecrawl row: names-only walk (no inode reads), the fd-walk peer
#
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
require_python3

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <tree-root> [results-dir]" >&2
  exit 2
fi

TREE=$(cd "$1" && pwd)
TS=$(date +%Y%m%d-%H%M%S)
OUT=${2:-"$COMPARE_DIR/results/index-$TS"}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
WORK_ROOT=${WORK_ROOT:-"$OUT/indexes"}
mkdir -p "$WORK_ROOT"
TOOLS=${TOOLS:-"ecrawl gufi xdu robinhood find fd du dua"}
INCLUDE_EREPORT_INDEX=${INCLUDE_EREPORT_INDEX:-1}

echo "==> threads: $(thread_plan)"
raise_nofile
echo "==> $NOFILE_NOW"
write_env_snapshot "$OUT/env.txt"

CMD_LOG="$OUT/COMMANDS.txt"
export CMD_LOG
printf '# exact argv of every timed command, in execution order\n# time\tlabel\tcommand\n' >"$CMD_LOG"

# Whatever the harness itself has to say (cache drops, a database that would
# not restart), kept apart from the tools' own stderr.
HARNESS_LOG="$OUT/harness.log"
export HARNESS_LOG
printf '# harness diagnostics, not tool output\n' >"$HARNESS_LOG"

# Entry count for normalization (files+dirs+symlinks ≈ paper “files” scale; use find -type f primarily).
# Two full cold walks before anything is timed, which on a large tree is a long
# silence; announce it so it is not mistaken for a hang.
echo "==> counting entries under $TREE for normalization (two find walks, not timed)"
ENTRY_COUNT=$(count_find "$TREE" -xdev)
FILE_COUNT=$(count_find "$TREE" -xdev -type f)
echo "    entries=$ENTRY_COUNT files=$FILE_COUNT"
echo "entry_count=$ENTRY_COUNT" >"$OUT/tree_stats.txt"
echo "file_count=$FILE_COUNT" >>"$OUT/tree_stats.txt"
echo "tree=$TREE" >>"$OUT/tree_stats.txt"

CSV="$OUT/index_results.csv"
echo "tool,variant,rep,status,elapsed_sec,index_bytes,file_count,sec_per_1M_files,mib_per_1M_files,notes" >"$CSV"

KEEP_ALL_INDEXES=${KEEP_ALL_INDEXES:-0}

# Every rep writes its own index tree; on production-scale trees keeping all of
# them can dwarf the source metadata, so drop the previous rep once it is timed.
prune_prev_index() {
  local prefix=$1 rep=$2
  [[ "$KEEP_ALL_INDEXES" == "1" ]] && return 0
  ((rep > 1)) || return 0
  local prev="$WORK_ROOT/${prefix}_r$((rep - 1))"
  [[ -d "$prev" ]] || return 0
  rm -rf "$prev"
}

append_row() {
  local tool=$1 variant=$2 rep=$3 status=$4 elapsed=$5 bytes=$6 notes=$7
  python3 - "$tool" "$variant" "$rep" "$status" "$elapsed" "$bytes" "$FILE_COUNT" "$notes" <<'PY' >>"$CSV"
import sys, csv
tool, variant, rep, status, elapsed, nbytes, fcount, notes = sys.argv[1:9]
fcount = int(fcount or 0)
try:
    el = float(elapsed) if elapsed not in ("", "None") else float("nan")
except ValueError:
    el = float("nan")
try:
    nb = int(nbytes)
except ValueError:
    nb = 0
sec_per = (el / fcount * 1_000_000) if fcount and el == el else float("nan")
mib_per = (nb / (1024 * 1024) / fcount * 1_000_000) if fcount else float("nan")
w = csv.writer(sys.stdout)
w.writerow([tool, variant, rep, status,
            f"{el:.6f}" if el == el else "",
            nb, fcount,
            f"{sec_per:.6f}" if sec_per == sec_per else "",
            f"{mib_per:.6f}" if mib_per == mib_per else "",
            notes])
PY
}

run_ecrawl() {
  local variant=$1 # write | nowrite | nostat
  local rep=$2
  local dest="$WORK_ROOT/ecrawl_${variant}_r${rep}"
  prune_prev_index "ecrawl_${variant}" "$rep"
  rm -rf "$dest"
  mkdir -p "$dest"
  local tfile="$OUT/ecrawl_${variant}_r${rep}.time.txt"
  local st=0
  set +e
  # --verbose prints the run's full key=value metrics to stdout at exit, which is
  # where the tree-wide file count and byte totals come from: Q4 and Q5 for the
  # crawl root fall out of the capture pass, with no query afterwards. It writes
  # only at exit, so it does not affect the timing.
  if [[ "$variant" == "nostat" ]]; then
    # Names-only walk: stdout is the path stream and the metrics go to stderr, so
    # discard stdout exactly as the fd row does and keep the two walks comparable.
    (
      export ECRAWL_CRAWL_THREADS="$ECRAWL_NOSTAT_CRAWL_THREADS" ECRAWL_STAT_THREADS=0
      time_cmd "$tfile" "$ECRAWL_BIN" --verbose --no-stat "$TREE" \
        >/dev/null 2>"$OUT/ecrawl_${variant}_r${rep}.stderr.txt"
    )
    st=$?
  elif [[ "$variant" == "nowrite" ]]; then
    time_cmd "$tfile" "$ECRAWL_BIN" --verbose --no-write "$TREE" >"$OUT/ecrawl_${variant}_r${rep}.stdout.txt" 2>"$OUT/ecrawl_${variant}_r${rep}.stderr.txt"
    st=$?
  else
    time_cmd "$tfile" "$ECRAWL_BIN" --verbose "$TREE" "$dest" >"$OUT/ecrawl_${variant}_r${rep}.stdout.txt" 2>"$OUT/ecrawl_${variant}_r${rep}.stderr.txt"
    st=$?
  fi
  set -e
  local el bytes notes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  if [[ "$variant" == "write" ]]; then
    bytes=$(ecrawl_index_bytes "$dest")
    notes="ERCBIN08_shards"
    # Keep last write dest for ereport_index
    echo "$dest" >"$OUT/ecrawl_bin_dir.txt"
  elif [[ "$variant" == "nostat" ]]; then
    bytes=0
    # No inode reads, so this row carries no size or ownership data at all: it is a
    # like-for-like peer of the fd walk, not of any row that builds an index.
    notes="names_only_no_inode_reads_compare_to_fd_walk"
  else
    bytes=0
    notes="walk_only_not_comparable_to_full_index"
  fi
  if [[ $st -ne 0 ]]; then
    append_row ecrawl "$variant" "$rep" fail "${el:-}" "$bytes" "exit=$st"
  else
    append_row ecrawl "$variant" "$rep" ok "${el:-}" "$bytes" "$notes"
  fi
}

run_ereport_index() {
  local rep=$1
  local bin_dir
  bin_dir=$(cat "$OUT/ecrawl_bin_dir.txt" 2>/dev/null || true)
  if [[ -z "$bin_dir" || ! -d "$bin_dir" ]]; then
    append_row ereport_index make "$rep" skipped "" 0 "no_ecrawl_bins"
    return 0
  fi
  if ! tool_available ereport_index; then
    append_row ereport_index make "$rep" skipped "" 0 "missing_binary"
    return 0
  fi
  local idx="$WORK_ROOT/ereport_index_r${rep}"
  rm -rf "$idx"
  mkdir -p "$idx"
  local tfile="$OUT/ereport_index_r${rep}.time.txt"
  set +e
  time_cmd "$tfile" "$EREPORT_INDEX_BIN" --make --index-dir "$idx" "$bin_dir" \
    >"$OUT/ereport_index_r${rep}.stdout.txt" 2>"$OUT/ereport_index_r${rep}.stderr.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(dir_bytes "$idx")
  if [[ $st -ne 0 ]]; then
    append_row ereport_index make "$rep" fail "${el:-}" "$bytes" "exit=$st"
  else
    append_row ereport_index make "$rep" ok "${el:-}" "$bytes" "trigram_on_top_of_crawl"
    echo "$idx" >"$OUT/ereport_index_dir.txt"
  fi
}

run_gufi() {
  local variant=$1 # plain | rollup
  local rep=$2
  if ! tool_available gufi; then
    if [[ "$variant" == "rollup" ]]; then
      append_row gufi rollup_index "$rep" skipped "" 0 "gufi_dir2index_not_found"
      append_row gufi rollup_step "$rep" skipped "" 0 "gufi_dir2index_not_found"
    else
      append_row gufi "$variant" "$rep" skipped "" 0 "gufi_dir2index_not_found"
    fi
    return 0
  fi
  local dest="$WORK_ROOT/gufi_${variant}_r${rep}"
  prune_prev_index "gufi_${variant}" "$rep"
  rm -rf "$dest"
  mkdir -p "$dest"
  if [[ "$variant" == "rollup" && ! -x "${GUFI_ROLLUP:-}" ]]; then
    append_row gufi rollup_index "$rep" skipped "" 0 "gufi_rollup_not_found"
    append_row gufi rollup_step "$rep" skipped "" 0 "gufi_rollup_not_found"
    return 0
  fi

  # The rollup variant is two commands, and only the pair is comparable to a
  # one-shot indexer. Record them as separate phases so the total is a sum of
  # things that were each actually measured.
  local phase1=plain
  [[ "$variant" != "rollup" ]] || phase1=rollup_index

  local tfile="$OUT/gufi_${variant}_r${rep}.time.txt"
  set +e
  time_cmd "$tfile" "$GUFI_DIR2INDEX" -n "$THREADS" "$TREE" "$dest" \
    >"$OUT/gufi_${variant}_r${rep}.stdout.txt" 2>"$OUT/gufi_${variant}_r${rep}.stderr.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(dir_bytes "$dest")
  if [[ $st -ne 0 ]]; then
    append_row gufi "$phase1" "$rep" fail "${el:-}" "$bytes" "exit=$st"
    [[ "$variant" != "rollup" ]] ||
      append_row gufi rollup_step "$rep" skipped "" 0 "dir2index_failed"
    return 0
  fi
  append_row gufi "$phase1" "$rep" ok "${el:-}" "$bytes" "sqlite_replica"
  echo "$dest" >"$OUT/gufi_index_dir.txt"
  [[ "$variant" == "rollup" ]] || return 0

  # gufi_rollup takes the GUFI tree, which dir2index placed at
  # <dest>/<basename tree>, and its own thread count -- left at the default it
  # ran the slowest phase of the whole comparison on a fraction of the budget
  # every other tool was given.
  local tfile2="$OUT/gufi_rollup_step_r${rep}.time.txt"
  set +e
  time_cmd "$tfile2" "$GUFI_ROLLUP" -n "$THREADS" "$dest/${TREE##*/}" \
    >>"$OUT/gufi_${variant}_r${rep}.stdout.txt" 2>>"$OUT/gufi_${variant}_r${rep}.stderr.txt"
  st=$?
  set -e
  local el2 bytes_after delta
  el2=$(elapsed_from_time_v "$tfile2" || echo "")
  bytes_after=$(dir_bytes "$dest")
  # What the rollup adds on top of the dir2index tree, so the two phase rows sum
  # to the size on disk instead of counting the replica twice.
  delta=$((bytes_after - bytes))
  ((delta >= 0)) || delta=0
  if [[ $st -ne 0 ]]; then
    append_row gufi rollup_step "$rep" fail "${el2:-}" "$delta" "exit=$st"
  else
    append_row gufi rollup_step "$rep" ok "${el2:-}" "$delta" "rollup_on_top_of_dir2index"
  fi
}

run_xdu() {
  local rep=$1
  if ! tool_available xdu; then
    append_row xdu index "$rep" skipped "" 0 "xdu_not_found"
    return 0
  fi
  local dest="$WORK_ROOT/xdu_r${rep}"
  prune_prev_index xdu "$rep"
  rm -rf "$dest"
  mkdir -p "$dest"
  local tfile="$OUT/xdu_r${rep}.time.txt"
  set +e
  # Without --apparent-size the index stores st_blocks, so every size answer it
  # gives is disk usage while find, du, GUFI and the capture all report file
  # length. On a tree with sparse files that is not a rounding difference: the
  # Q3 threshold matched nothing and Q4 came out far short of du -sb.
  time_cmd "$tfile" "$XDU_BIN" "$TREE" -o "$dest" -j "$THREADS" "${XDU_SIZE_ARGS[@]}" \
    >"$OUT/xdu_r${rep}.stdout.txt" 2>"$OUT/xdu_r${rep}.stderr.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(dir_bytes "$dest")
  if [[ $st -ne 0 ]]; then
    append_row xdu index "$rep" fail "${el:-}" "$bytes" "exit=$st"
  else
    append_row xdu index "$rep" ok "${el:-}" "$bytes" "parquet; $XDU_SIZE_NOTE"
    echo "$dest" >"$OUT/xdu_index_dir.txt"
  fi
}

run_robinhood() {
  local rep=$1
  if ! tool_available robinhood; then
    append_row robinhood scan "$rep" skipped "" 0 "rbh_scan_not_found_set_RBH_SCAN"
    return 0
  fi
  if ! rbh_db_ready; then
    append_row robinhood scan "$rep" skipped "" 0 "$RBH_READY_REASON"
    return 0
  fi
  local rbh_thread_note=""
  # The only row whose cold state includes InnoDB's buffer pool; see
  # maybe_drop_caches, which restarts the server for it and for nothing else.
  local USES_DB=1
  # Site-specific; record a timed invocation if RBH_SCAN_ARGS is provided.
  local dest="$WORK_ROOT/robinhood_r${rep}"
  mkdir -p "$dest"
  local tfile="$OUT/robinhood_r${rep}.time.txt"
  local args=${RBH_SCAN_ARGS:-}
  if [[ -z "$args" ]]; then
    append_row robinhood scan "$rep" skipped "" 0 "set_RBH_SCAN_ARGS_for_site_config"
    return 0
  fi
  # robinhood --scan walks fs_path from its config, ignoring $TREE. Scanning a
  # different tree would be normalized against the wrong file count.
  if [[ -n "${RBH_CONFIG:-}" && -f "${RBH_CONFIG}" ]]; then
    local cfg_path
    cfg_path=$(sed -n 's/^[[:space:]]*fs_path[[:space:]]*=[[:space:]]*"\?\([^";]*\)"\?[[:space:]]*;.*/\1/p' \
      "$RBH_CONFIG" | sed -n '1p')
    if [[ -n "$cfg_path" && "$cfg_path" != "$TREE" ]]; then
      append_row robinhood scan "$rep" skipped "" 0 \
        "config fs_path '$cfg_path' differs from benchmark tree '$TREE'"
      return 0
    fi
    # Robinhood's thread counts live in the config, written once at setup, so a
    # later THREADS change silently leaves it out of step with every other tool.
    local cfg_scan
    cfg_scan=$(sed -n 's/^[[:space:]]*nb_threads_scan[[:space:]]*=[[:space:]]*\([0-9]\+\).*/\1/p' \
      "$RBH_CONFIG" | sed -n '1p')
    if [[ -z "$cfg_scan" ]]; then
      rbh_thread_note="config sets no nb_threads_scan; robinhood ran at its own default"
    elif ((cfg_scan * 2 != THREADS)); then
      rbh_thread_note="config nb_threads_scan=$cfg_scan does not match THREADS=$THREADS; rerun mariadb.sh setup"
    fi
    [[ -z "$rbh_thread_note" ]] || echo "    WARN: robinhood: $rbh_thread_note" >&2
  fi
  # A repeated full-index benchmark must start from an empty database. The
  # bundled helper only touches a database carrying its benchmark marker.
  if [[ "${RBH_AUTO_RESET:-0}" == "1" ]]; then
    if [[ -z "${RBH_DB_HELPER:-}" || ! -x "$RBH_DB_HELPER" ]]; then
      append_row robinhood scan "$rep" fail "" 0 "RBH_AUTO_RESET_without_RBH_DB_HELPER"
      return 0
    fi
    set +e
    PREFIX="${RBH_DB_PREFIX:-$HOME/.local/indexer-compare}" "$RBH_DB_HELPER" reset \
      >"$OUT/robinhood_reset_r${rep}.stdout.txt" 2>"$OUT/robinhood_reset_r${rep}.stderr.txt"
    local reset_st=$?
    set -e
    if [[ $reset_st -ne 0 ]]; then
      append_row robinhood scan "$rep" fail "" 0 "database_reset_exit=$reset_st"
      return 0
    fi
  fi
  # shellcheck disable=SC2086
  set +e
  time_cmd "$tfile" "$RBH_SCAN" $args \
    >"$OUT/robinhood_r${rep}.stdout.txt" 2>"$OUT/robinhood_r${rep}.stderr.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  if [[ -n "${RBH_INDEX_BYTES:-}" ]]; then
    bytes=$RBH_INDEX_BYTES
  elif [[ -n "${RBH_DB_HELPER:-}" && -x "$RBH_DB_HELPER" ]]; then
    bytes=$(PREFIX="${RBH_DB_PREFIX:-$HOME/.local/indexer-compare}" "$RBH_DB_HELPER" bytes \
      2>>"$OUT/robinhood_r${rep}.stderr.txt" || echo 0)
  else
    bytes=$(dir_bytes "${RBH_INDEX_DIR:-$dest}")
  fi
  local note="mariadb_see_notes"
  [[ -z "$rbh_thread_note" ]] || note="$note; $rbh_thread_note"
  if [[ $st -ne 0 ]]; then
    append_row robinhood scan "$rep" fail "${el:-}" "$bytes" "exit=$st"
  else
    append_row robinhood scan "$rep" ok "${el:-}" "$bytes" "$note"
  fi
}

run_find_baseline() {
  local rep=$1
  local tfile="$OUT/find_walk_r${rep}.time.txt"
  set +e
  # Only the timing is used. Keeping the listing would cost ~50GB per rep on a
  # production tree and would charge find for writes du/dua never make.
  time_cmd "$tfile" find "$TREE" -xdev -printf '%p\n' >/dev/null 2>"$OUT/find_walk_r${rep}.stderr.txt"
  local st=$?
  set -e
  local el
  el=$(elapsed_from_time_v "$tfile" || echo "")
  if [[ $st -eq 0 ]]; then
    append_row find walk "$rep" ok "${el:-}" 0 "live_walk_baseline"
  elif [[ $st -eq 1 ]]; then
    # find exits 1 when it cannot descend somewhere or an entry vanishes; the
    # walk still ran, so keep the baseline rather than losing the comparison.
    append_row find walk "$rep" ok "${el:-}" 0 "live_walk_baseline partial_exit=1"
  else
    append_row find walk "$rep" fail "${el:-}" 0 "exit=$st"
  fi
}

run_fd_baseline() {
  local rep=$1
  if ! tool_available fd; then
    append_row fd walk "$rep" skipped "" 0 "$(fd_skip_reason)"
    return 0
  fi
  local tfile="$OUT/fd_walk_r${rep}.time.txt"
  set +e
  # No --type filter, so this walks every entry like the find baseline does.
  time_cmd "$tfile" "$FD_BIN" "${FD_COMMON_ARGS[@]}" . "$TREE" \
    >/dev/null 2>"$OUT/fd_walk_r${rep}.stderr.txt"
  local st=$?
  set -e
  local el
  el=$(elapsed_from_time_v "$tfile" || echo "")
  if [[ $st -eq 0 ]]; then
    append_row fd walk "$rep" ok "${el:-}" 0 "live_walk_baseline_parallel"
  elif [[ $st -eq 1 ]]; then
    append_row fd walk "$rep" ok "${el:-}" 0 "live_walk_baseline_parallel partial_exit=1"
  else
    append_row fd walk "$rep" fail "${el:-}" 0 "exit=$st"
  fi
}

# du and dua walk the tree to total it, which is the same metadata sweep the
# indexers do — just discarded instead of stored.
run_du_baseline() {
  local rep=$1
  local tfile="$OUT/du_walk_r${rep}.time.txt"
  set +e
  time_cmd "$tfile" du -sb "$TREE" \
    >"$OUT/du_walk_r${rep}.stdout.txt" 2>"$OUT/du_walk_r${rep}.stderr.txt"
  local st=$?
  set -e
  local el
  el=$(elapsed_from_time_v "$tfile" || echo "")
  if [[ $st -eq 0 ]]; then
    append_row du walk "$rep" ok "${el:-}" 0 "live_walk_baseline_du_apparent"
  elif [[ $st -eq 1 ]]; then
    append_row du walk "$rep" ok "${el:-}" 0 "live_walk_baseline_du_apparent partial_exit=1"
  else
    append_row du walk "$rep" fail "${el:-}" 0 "exit=$st"
  fi
}

run_dua_baseline() {
  local rep=$1
  if ! tool_available dua; then
    append_row dua walk "$rep" skipped "" 0 "$(dua_skip_reason)"
    return 0
  fi
  local sentinel
  sentinel=$(dua_sentinel "$OUT")
  local tfile="$OUT/dua_walk_r${rep}.time.txt"
  set +e
  time_cmd "$tfile" "$DUA_BIN" "${DUA_AGG_ARGS[@]}" "$TREE" "$sentinel" \
    >"$OUT/dua_walk_r${rep}.stdout.txt" 2>"$OUT/dua_walk_r${rep}.stderr.txt"
  local st=$?
  set -e
  local el
  el=$(elapsed_from_time_v "$tfile" || echo "")
  if [[ $st -eq 0 ]]; then
    # dua reports unreadable paths on stderr and still exits 0, so a partial
    # walk here is only visible in dua_walk_r*.stderr.txt.
    append_row dua walk "$rep" ok "${el:-}" 0 "live_walk_baseline_dua_parallel"
  else
    append_row dua walk "$rep" fail "${el:-}" 0 "exit=$st"
  fi
}

REPS_MAX=$(max_tool_reps $TOOLS)
echo "==> repetitions: $(reps_plan $TOOLS ereport_index)"

for ((rep = 1; rep <= REPS_MAX; rep++)); do
  export CURRENT_REP=$rep
  for t in $TOOLS; do
    t_reps=$(tool_reps "$t")
    # Tools with a smaller count drop out of the later reps; the rest carry on,
    # so each one still meets the cache state it would have met before per-tool
    # counts existed.
    ((rep <= t_reps)) || continue
    # Each tool's own output goes to $OUT/<tool>_*.std{out,err}.txt, so without
    # this the whole phase looks stalled.
    printf '==> rep %d/%d: %s (%s)\n' "$rep" "$t_reps" "$t" "$(date +%H:%M:%S)"
    case "$t" in
      ecrawl)
        run_ecrawl write "$rep"
        if [[ "${DO_NOWRITE:-0}" == "1" ]]; then
          run_ecrawl nowrite "$rep"
        fi
        if [[ "${DO_NOSTAT:-0}" == "1" ]]; then
          run_ecrawl nostat "$rep"
        fi
        # Runs inside ecrawl's branch because it indexes the capture ecrawl just
        # wrote, so REPS_ECRAWL is also its ceiling. Defaults to 1: the input is
        # identical every time, so a second pass measures the same work twice.
        if [[ "$INCLUDE_EREPORT_INDEX" == "1" ]] && ((rep <= $(tool_reps ereport_index))); then
          run_ereport_index "$rep"
        fi
        ;;
      gufi)
        run_gufi plain "$rep"
        if [[ "${GUFI_DO_ROLLUP:-1}" == "1" ]]; then
          run_gufi rollup "$rep"
        fi
        ;;
      xdu) run_xdu "$rep" ;;
      robinhood) run_robinhood "$rep" ;;
      find) run_find_baseline "$rep" ;;
      fd) run_fd_baseline "$rep" ;;
      du) run_du_baseline "$rep" ;;
      dua) run_dua_baseline "$rep" ;;
      *) echo "WARN: unknown tool $t" >&2 ;;
    esac
  done
done

echo "Wrote $CSV"
echo "results=$OUT"
