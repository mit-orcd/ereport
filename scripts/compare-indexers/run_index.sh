#!/usr/bin/env bash
#
# Time index / capture builds for each available tool on a tree root.
# Writes paper-style metrics: wall seconds, bytes, sec_per_1M_files, MiB_per_1M_files.
#
# Usage:
#   scripts/compare-indexers/run_index.sh <tree-root> [results-dir]
#
# Env:
#   TOOLS="ecrawl gufi xdu robinhood find fd du dua dut"
#                               (find/fd/du/dua/dut = live walk baselines only)
#   REPS=3 DROP_CACHES=0|1 THREADS=16
#   CACHE_MODES="cold hot"    per tool (and per ecrawl variant): all cold reps
#                     first, then all hot reps. Each cold rep drops, then runs
#                     the whole pipeline (crawl and index together). The last
#                     cold run is what warms the hot series; later hots stay
#                     warm. Walk-only tools are the same shape without an index.
#   REPS_<TOOL>=n     repetitions for one tool, overriding REPS: REPS_GUFI=1
#                     keeps a 29-minute rollup to a single pass while the cheap
#                     rows still get their three. REPS_EREPORT_INDEX follows
#                     REPS (capped by REPS_ECRAWL) so crawl and index stay a
#                     pair; pin it lower to index only the last N write reps
#                     of each cache series.
#   WORK_ROOT=<dir>   where tool indexes are written (default: <results-dir>/indexes)
#   INCLUDE_EREPORT_INDEX=1   also time ereport_index --make after ecrawl (separate row)
#   DO_NOWRITE=1      extra ecrawl row: stat walk, no capture; answers apparent bytes
#                     (du/dua/dut peer; hardlink_dedup=on)
#   DO_NOSTAT=1       extra ecrawl row: --no-stat --count; answers regular-file count
#                     (find/fd peer)
#   DO_STATX=1        extra ecrawl rows: write+nowrite with --statx (statx(2), minimal mask)
#   DO_IOURING=1      extra ecrawl rows: write+nowrite with --iouring (batched inode reads)
#   ECRAWL_COLD_IOURING=1  default write/nowrite pass --iouring on the cold
#                     cache series (hot stays fstatat). Set 0 for fstatat on both.
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
TOOLS=${TOOLS:-"ecrawl gufi xdu robinhood find fd du dua dut"}
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
echo "tool,variant,rep,cache,status,elapsed_sec,index_bytes,file_count,sec_per_1M_files,mib_per_1M_files,notes" >"$CSV"

KEEP_ALL_INDEXES=${KEEP_ALL_INDEXES:-0}

# Appended to every per-pass filename so the hot pass does not overwrite the
# cold pass's logs. Empty for the first pass, which keeps the name summarize.py
# looks for first when it quotes a tool's stderr.
RUN_TAG=""

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
  local tool=$1 variant=$2 rep=$3 status=$4 elapsed=$5 bytes=$6 notes=$7 cache
  # Taken from the pass rather than passed in by each caller: every row belongs
  # to whichever pass is running, and a caller that had to remember to say so
  # would eventually forget.
  cache=$(cache_label)
  python3 - "$tool" "$variant" "$rep" "$cache" "$status" "$elapsed" "$bytes" "$FILE_COUNT" "$notes" <<'PY' >>"$CSV"
import sys, csv
tool, variant, rep, cache, status, elapsed, nbytes, fcount, notes = sys.argv[1:10]
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
w.writerow([tool, variant, rep, cache, status,
            f"{el:.6f}" if el == el else "",
            nb, fcount,
            f"{sec_per:.6f}" if sec_per == sec_per else "",
            f"{mib_per:.6f}" if mib_per == mib_per else "",
            notes])
PY
}

run_ecrawl() {
  local variant=$1 # write | nowrite | nostat | write_statx | write_iouring | nowrite_statx | nowrite_iouring
  local rep=$2
  local dest="$WORK_ROOT/ecrawl_${variant}_r${rep}"
  prune_prev_index "ecrawl_${variant}" "$rep"
  rm -rf "$dest"
  mkdir -p "$dest"
  local stem="ecrawl_${variant}_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  local st=0
  # Inode-read syscall: named variants pin one; the default write/nowrite use
  # --iouring on a cold pass (ECRAWL_COLD_IOURING=1) and fstatat when hot.
  local stat_flags=()
  case "$variant" in
    *_statx) stat_flags=(--statx) ;;
    *_iouring) stat_flags=(--iouring) ;;
    write|nowrite)
      if [[ "${ECRAWL_COLD_IOURING:-1}" == "1" && "$CACHE_STATE" == "cold" ]]; then
        stat_flags=(--iouring)
      fi
      ;;
  esac
  set +e
  # --verbose prints the run's full key=value metrics to stdout at exit, which is
  # where the tree-wide file count and byte totals come from: Q4 and Q5 for the
  # crawl root fall out of the capture pass, with no query afterwards. It writes
  # only at exit, so it does not affect the timing.
  if [[ "$variant" == "nostat" ]]; then
    # Names-only census: d_type file count, no path stream (find/fd peer).
    # Env prefix, not a subshell: time_cmd records CACHE_DROPPED in this
    # shell, and a subshell used to drop that flag so the row was written
    # as warm after a real drop.
    ECRAWL_CRAWL_THREADS="$ECRAWL_NOSTAT_CRAWL_THREADS" \
      time_cmd "$tfile" "$ECRAWL_BIN" --verbose --no-stat --count "$TREE" \
        >"$OUT/${stem}.stdout.txt" 2>"$OUT/${stem}.stderr.txt"
    st=$?
  elif [[ "$variant" == nowrite* ]]; then
    time_cmd "$tfile" "$ECRAWL_BIN" --verbose --no-write "${stat_flags[@]}" "$TREE" \
      >"$OUT/${stem}.stdout.txt" 2>"$OUT/${stem}.stderr.txt"
    st=$?
  else
    time_cmd "$tfile" "$ECRAWL_BIN" --verbose "${stat_flags[@]}" "$TREE" "$dest" \
      >"$OUT/${stem}.stdout.txt" 2>"$OUT/${stem}.stderr.txt"
    st=$?
  fi
  set -e
  local el bytes notes answer
  el=$(elapsed_from_time_v "$tfile" || echo "")
  if [[ "$variant" == write* ]]; then
    bytes=$(ecrawl_index_bytes "$dest")
    notes="ERCBIN08_shards"
    if [[ "$variant" == "write_statx" ]]; then notes="$notes;statx"; fi
    if [[ "$variant" == "write_iouring" || ( "$variant" == "write" && "${stat_flags[*]}" == *--iouring* ) ]]; then
      notes="$notes;iouring"
    fi
    # Keep last plain-write dest for ereport_index
    if [[ "$variant" == "write" ]]; then
      echo "$dest" >"$OUT/ecrawl_bin_dir.txt"
    fi
  elif [[ "$variant" == "nostat" ]]; then
    bytes=0
    answer=$(kv_from_file files "$OUT/${stem}.stdout.txt")
    notes="answer_files=${answer:-};names_only_regular_file_count"
  else
    bytes=0
    answer=$(kv_from_file total_bytes "$OUT/${stem}.stdout.txt")
    notes="answer_bytes=${answer:-};apparent_size_hardlink_dedup=on"
    if [[ "$variant" == "nowrite_statx" ]]; then notes="$notes;statx"; fi
    if [[ "$variant" == "nowrite_iouring" || ( "$variant" == "nowrite" && "${stat_flags[*]}" == *--iouring* ) ]]; then
      notes="$notes;iouring"
    fi
  fi
  if [[ $st -ne 0 ]]; then
    append_row ecrawl "$variant" "$rep" fail "${el:-}" "$bytes" "exit=$st"
  elif [[ "${stat_flags[*]}" == *--iouring* ]]; then
    # A kernel without IORING_OP_STATX makes ecrawl fall back to plain statx.
    # Extra *_iouring rows skip so they are not mislabelled; the default
    # write/nowrite keep the capture (the index needs it) and note the fallback.
    local batches
    batches=$(kv_from_file io_uring_batches "$OUT/${stem}.stdout.txt")
    if [[ "${batches:-0}" == "0" ]]; then
      if [[ "$variant" == *_iouring ]]; then
        append_row ecrawl "$variant" "$rep" skipped "${el:-}" "$bytes" "iouring_unavailable;fell_back_to_statx"
      else
        notes=${notes%;iouring}
        append_row ecrawl "$variant" "$rep" ok "${el:-}" "$bytes" \
          "${notes};iouring_unavailable;fell_back_to_statx"
      fi
    else
      append_row ecrawl "$variant" "$rep" ok "${el:-}" "$bytes" "$notes"
    fi
  else
    append_row ecrawl "$variant" "$rep" ok "${el:-}" "$bytes" "$notes"
  fi
}

# Which repetitions build a trigram index: ecrawl's last ones, not its first.
# Every ecrawl write rep overwrites ecrawl_bin_dir.txt with its own capture and
# prunes the previous one, so the capture that survives the run -- the one
# run_queries.sh reads -- is ecrawl's final rep. Building from rep 1 while rep 3
# is what gets queried pairs an index with a capture it was not built from: the
# recorded ereport_index_dir.txt named an index whose input had already been
# deleted, and the shard-bound dir-index sidecars correctly reject it, so the
# queries fell back to the slow path and the run showed no improvement.
ereport_index_rep() {
  local rep=$1 crawl_tool=${2:-ecrawl}
  local crawl_reps index_reps
  crawl_reps=$(tool_reps "$crawl_tool")
  index_reps=$(tool_reps ereport_index)
  # There are only as many captures to index as the crawl tool makes.
  ((index_reps <= crawl_reps)) || index_reps=$crawl_reps
  ((rep > crawl_reps - index_reps))
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
  local stem="ereport_index_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  set +e
  time_cmd "$tfile" "$EREPORT_INDEX_BIN" --make --index-dir "$idx" "$bin_dir" \
    >"$OUT/${stem}.stdout.txt" 2>"$OUT/${stem}.stderr.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(dir_bytes "$idx")
  if [[ $st -ne 0 ]]; then
    append_row ereport_index make "$rep" fail "${el:-}" "$bytes" "exit=$st"
  else
    # Which capture it was built from, so the pairing is auditable from the CSV
    # alone: an index and a capture that do not match answer the subtree queries
    # from the slow path and nothing in the timings says why.
    append_row ereport_index make "$rep" ok "${el:-}" "$bytes" \
      "trigram_on_top_of_crawl; input=${bin_dir##*/}"
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
  # gufi_dir2index creates dest itself and exits 1 if the path already exists.
  rm -rf "$dest"
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

  local stem="gufi_${variant}_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  set +e
  time_cmd "$tfile" "$GUFI_DIR2INDEX" -n "$THREADS" "$TREE" "$dest" \
    >"$OUT/${stem}.stdout.txt" 2>"$OUT/${stem}.stderr.txt"
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
  # One pointer per kind of index, because the query phase asks both: the plain
  # one is what dir2index cost, the rolled-up one is what dir2index plus rollup
  # cost, and a single last-one-wins pointer meant every GUFI query bar silently
  # came from the expensive index without saying so.
  if [[ "$variant" == "rollup" ]]; then
    echo "$dest" >"$OUT/gufi_index_dir.txt"
  else
    echo "$dest" >"$OUT/gufi_plain_index_dir.txt"
    # Kept for a run without the rollup variant, where this is the only index.
    [[ -f "$OUT/gufi_rollup_index_dir.txt" ]] || echo "$dest" >"$OUT/gufi_index_dir.txt"
    return 0
  fi

  # gufi_rollup takes the GUFI tree, which dir2index placed at
  # <dest>/<basename tree>, and its own thread count -- left at the default it
  # ran the slowest phase of the whole comparison on a fraction of the budget
  # every other tool was given.
  local tfile2="$OUT/gufi_rollup_step_r${rep}${RUN_TAG}.time.txt"
  set +e
  time_cmd "$tfile2" "$GUFI_ROLLUP" -n "$THREADS" "$dest/${TREE##*/}" \
    >>"$OUT/${stem}.stdout.txt" 2>>"$OUT/${stem}.stderr.txt"
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
    # Only now is this a rolled-up index. Written after the rollup rather than
    # after dir2index because a failed rollup leaves a plain index behind, and
    # the query phase would then credit its treesummary answers to a build that
    # never finished.
    echo "$dest" >"$OUT/gufi_rollup_index_dir.txt"
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
  local stem="xdu_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  set +e
  # Without --apparent-size the index stores st_blocks, so every size answer it
  # gives is disk usage while find, du, GUFI and the capture all report file
  # length. On a tree with sparse files that is not a rounding difference: the
  # Q3 threshold matched nothing and Q4 came out far short of du -sb.
  time_cmd "$tfile" "$XDU_BIN" "$TREE" -o "$dest" -j "$THREADS" "${XDU_SIZE_ARGS[@]}" \
    >"$OUT/${stem}.stdout.txt" 2>"$OUT/${stem}.stderr.txt"
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

# Bytes in MariaDB's data directory, which is how GUFI, XDU and ecrawl are all
# measured. The information_schema figure counts only Robinhood's own tables and
# is kept in the row's note, because the two answer different questions: what the
# rows occupy, and what the server had to write to hold them.
rbh_db_bytes() {
  local helper=${RBH_DB_HELPER:-} out=""
  if [[ -n "${RBH_INDEX_BYTES:-}" ]]; then
    printf '%s' "$RBH_INDEX_BYTES"
    return 0
  fi
  if [[ -n "$helper" && -x "$helper" ]]; then
    out=$(PREFIX="${RBH_DB_PREFIX:-$HOME/.local/indexer-compare}" "$helper" bytes 2>/dev/null || true)
  fi
  [[ -n "$out" ]] || out=$(dir_bytes "${RBH_DB_DATADIR:-${RBH_INDEX_DIR:-}}")
  printf '%s' "${out:-0}"
}

rbh_table_bytes() {
  local helper=${RBH_DB_HELPER:-} out=""
  if [[ -n "$helper" && -x "$helper" ]]; then
    out=$(PREFIX="${RBH_DB_PREFIX:-$HOME/.local/indexer-compare}" "$helper" table-bytes 2>/dev/null || true)
  fi
  printf '%s' "${out:-}"
}

# The second half of what Robinhood costs. The scan above fills index-free
# tables, which is not a database anyone queries, so the three indexes its
# queries need are built here and timed as a phase of their own. run_queries.sh
# drops them again when it is done, so the next repetition's scan is index-free
# and this row is measured against the same starting state every time.
run_robinhood_indexes() {
  local rep=$1
  if ! rbh_db_ready; then
    append_row robinhood indexes "$rep" skipped "" 0 "$RBH_READY_REASON"
    return 0
  fi
  # Re-probed: the scan that just ran is what creates the tables, and the cached
  # answer may be from before it -- write_env_snapshot asks this question at
  # startup, when a reset database still has none.
  RBH_SCHEMA_STATE=""
  if ! rbh_schema_ready; then
    append_row robinhood indexes "$rep" skipped "" 0 "$RBH_SCHEMA_REASON"
    return 0
  fi
  local sql_runner="$OUT/rbh_sql_runner.sh"
  if ! rbh_write_sql_runner "$sql_runner"; then
    append_row robinhood indexes "$rep" skipped "" 0 "no_readable_mysql_credentials_in_the_robinhood_config"
    return 0
  fi
  # An index that is already there costs nothing to create, and a zero here
  # would read as a free index rather than as a leftover from a run that did not
  # get to drop them.
  local present
  present=$(rbh_indexes_present)
  if [[ "$present" != "0" ]]; then
    rbh_drop_indexes ||
      harness_warn "robinhood: could not drop $present leftover index(es) before timing their creation"
  fi
  local stem="robinhood_indexes_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  local before after delta
  before=$(rbh_db_bytes)
  # Same cold-cache treatment as the scan row: on the cold pass the server is
  # restarted first, so the index build starts with an empty buffer pool.
  local USES_DB=1
  set +e
  time_cmd "$tfile" "$sql_runner" "${RBH_INDEX_CREATE[@]}" \
    >"$OUT/${stem}.stdout.txt" 2>"$OUT/${stem}.stderr.txt"
  local st=$?
  set -e
  local el
  el=$(elapsed_from_time_v "$tfile" || echo "")
  after=$(rbh_db_bytes)
  delta=$((after - before))
  ((delta >= 0)) || delta=0
  if [[ $st -ne 0 ]]; then
    append_row robinhood indexes "$rep" fail "${el:-}" "$delta" "exit=$st"
    return 0
  fi
  local note="create_index_on_NAMES(name)_ENTRIES(size)_ENTRIES(type); datadir_delta"
  local tables
  tables=$(rbh_table_bytes)
  [[ -z "$tables" ]] || note="$note; tables_and_indexes=${tables}B"
  append_row robinhood indexes "$rep" ok "${el:-}" "$delta" "$note"
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
  local stem="robinhood_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
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
      >"$OUT/${stem}_reset.stdout.txt" 2>"$OUT/${stem}_reset.stderr.txt"
    local reset_st=$?
    set -e
    if [[ $reset_st -ne 0 ]]; then
      append_row robinhood scan "$rep" fail "" 0 "database_reset_exit=$reset_st"
      return 0
    fi
    # The reset dropped the tables, so any cached "schema present" is now wrong.
    # Read back by lib.sh's rbh_schema_ready, not here.
    # shellcheck disable=SC2034
    RBH_SCHEMA_STATE=""
  fi
  set +e
  # RBH_SCAN_ARGS is a site-configured argument list, so it is meant to split.
  # shellcheck disable=SC2086
  time_cmd "$tfile" "$RBH_SCAN" $args \
    >"$OUT/${stem}.stdout.txt" 2>"$OUT/${stem}.stderr.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(rbh_db_bytes)
  local note="mariadb_see_notes; crawled_into_index-free_tables"
  [[ -z "$rbh_thread_note" ]] || note="$note; $rbh_thread_note"
  if [[ $st -ne 0 ]]; then
    append_row robinhood scan "$rep" fail "${el:-}" "$bytes" "exit=$st"
  else
    append_row robinhood scan "$rep" ok "${el:-}" "$bytes" "$note"
  fi
}

run_find_baseline() {
  local rep=$1
  local stem="find_walk_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  local out="$OUT/${stem}.stdout.txt"
  set +e
  # Regular-file census (names-only peer). Pipe to wc so the answer is one integer
  # rather than a multi-GB path listing.
  time_cmd "$tfile" bash -c 'find "$1" -xdev -type f | wc -l' bash "$TREE" \
    >"$out" 2>"$OUT/${stem}.stderr.txt"
  local st=$?
  set -e
  local el answer notes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  answer=$(first_field_from_file "$out")
  notes="answer_files=${answer:-};names_only_regular_file_count"
  if [[ $st -eq 0 ]]; then
    append_row find walk "$rep" ok "${el:-}" 0 "$notes"
  elif [[ $st -eq 1 ]]; then
    # find exits 1 when it cannot descend somewhere or an entry vanishes; the
    # walk still ran, so keep the baseline rather than losing the comparison.
    append_row find walk "$rep" ok "${el:-}" 0 "${notes};partial_exit=1"
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
  local stem="fd_walk_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  local out="$OUT/${stem}.stdout.txt"
  set +e
  # Regular-file census with the same FD_COMMON_ARGS pin as the query rows.
  time_cmd "$tfile" bash -c '
    fd_bin=$1; tree=$2; shift 2
    "$fd_bin" "$@" -t f . "$tree" | wc -l
  ' bash "$FD_BIN" "$TREE" "${FD_COMMON_ARGS[@]}" \
    >"$out" 2>"$OUT/${stem}.stderr.txt"
  local st=$?
  set -e
  local el answer notes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  answer=$(first_field_from_file "$out")
  notes="answer_files=${answer:-};names_only_regular_file_count"
  if [[ $st -eq 0 ]]; then
    append_row fd walk "$rep" ok "${el:-}" 0 "$notes"
  elif [[ $st -eq 1 ]]; then
    append_row fd walk "$rep" ok "${el:-}" 0 "${notes};partial_exit=1"
  else
    append_row fd walk "$rep" fail "${el:-}" 0 "exit=$st"
  fi
}

# du, dua and dut walk the tree for apparent size (metadata + size peer group).
run_du_baseline() {
  local rep=$1
  local stem="du_walk_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  local out="$OUT/${stem}.stdout.txt"
  set +e
  time_cmd "$tfile" du -sb "$TREE" \
    >"$out" 2>"$OUT/${stem}.stderr.txt"
  local st=$?
  set -e
  local el answer notes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  answer=$(first_field_from_file "$out")
  notes="answer_bytes=${answer:-};apparent_size_hardlinks_may_differ"
  if [[ $st -eq 0 ]]; then
    append_row du walk "$rep" ok "${el:-}" 0 "$notes"
  elif [[ $st -eq 1 ]]; then
    append_row du walk "$rep" ok "${el:-}" 0 "${notes};partial_exit=1"
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
  local stem="dua_walk_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  local out="$OUT/${stem}.stdout.txt"
  set +e
  time_cmd "$tfile" "$DUA_BIN" "${DUA_AGG_ARGS[@]}" "$TREE" "$sentinel" \
    >"$out" 2>"$OUT/${stem}.stderr.txt"
  local st=$?
  set -e
  local el answer notes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  answer=$(dua_bytes_for_path "$out" "$TREE")
  notes="answer_bytes=${answer:-};apparent_size_hardlinks_may_differ"
  if [[ $st -eq 0 ]]; then
    # dua reports unreadable paths on stderr and still exits 0, so a partial
    # walk here is only visible in dua_walk_r*.stderr.txt.
    append_row dua walk "$rep" ok "${el:-}" 0 "$notes"
  else
    append_row dua walk "$rep" fail "${el:-}" 0 "exit=$st"
  fi
}

run_dut_baseline() {
  local rep=$1
  if ! tool_available dut; then
    append_row dut walk "$rep" skipped "" 0 "$(dut_skip_reason)"
    return 0
  fi
  local stem="dut_walk_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  local out="$OUT/${stem}.stdout.txt"
  set +e
  # One line of output, and bounded to this filesystem like find -xdev: see
  # DUT_WALK_ARGS in lib.sh, which is the Q4 vector plus that -x.
  time_cmd "$tfile" "$DUT_BIN" "${DUT_WALK_ARGS[@]}" "$TREE" \
    >"$out" 2>"$OUT/${stem}.stderr.txt"
  local st=$?
  set -e
  local el answer notes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  answer=$(dut_bytes_from_stdout "$out")
  notes="answer_bytes=${answer:-};apparent_size_hardlinks_may_differ"
  if [[ $st -eq 0 ]]; then
    append_row dut walk "$rep" ok "${el:-}" 0 "$notes"
  else
    append_row dut walk "$rep" fail "${el:-}" 0 "exit=$st"
  fi
}

echo "==> repetitions: $(reps_plan $TOOLS ereport_index)"
echo "==> cache series per tool: $CACHE_MODES (all cold reps, then all hot reps)"

# First named mode keeps the untagged log names summarize.py looks up first.
# All cold reps of a unit run before any hot rep, so the last cold is what
# warms the hot series — not a different tool, and not a drop between C1 and H1.
run_cache_series() {
  local label=$1
  local t_reps=$2
  shift 2
  local mode pass=0 rep
  for mode in $CACHE_MODES; do
    pass=$((pass + 1))
    export CACHE_STATE=$mode
    for ((rep = 1; rep <= t_reps; rep++)); do
      export CURRENT_REP=$rep
      RUN_TAG=""
      ((pass == 1)) || RUN_TAG="_$mode"
      printf '==> %s rep %d/%d (%s, %s)\n' "$label" "$rep" "$t_reps" "$mode" "$(date +%H:%M:%S)"
      "$@" "$rep"
    done
  done
}

run_ecrawl_walk_unit() {
  local variant=$1 rep=$2
  begin_timed_unit
  run_ecrawl "$variant" "$rep"
}

run_ecrawl_write_unit() {
  local rep=$1
  begin_timed_unit
  run_ecrawl write "$rep"
  if [[ "$INCLUDE_EREPORT_INDEX" == "1" ]] && ereport_index_rep "$rep"; then
    run_ereport_index "$rep"
  fi
}

run_ecrawl_series() {
  local variants=(write)
  [[ "${DO_NOWRITE:-0}" == "1" ]] && variants+=(nowrite)
  [[ "${DO_STATX:-0}" == "1" ]] && variants+=(write_statx nowrite_statx)
  [[ "${DO_IOURING:-0}" == "1" ]] && variants+=(write_iouring nowrite_iouring)
  [[ "${DO_NOSTAT:-0}" == "1" ]] && variants+=(nostat)
  local v t_reps
  t_reps=$(tool_reps ecrawl)
  for v in "${variants[@]}"; do
    if [[ "$v" == "write" ]]; then
      run_cache_series "ecrawl write+index" "$t_reps" run_ecrawl_write_unit
    else
      run_cache_series "ecrawl $v" "$t_reps" run_ecrawl_walk_unit "$v"
    fi
  done
}

# One tool's whole pipeline, for whichever pass is current.
run_tool_pipeline() {
  local t=$1 rep=$2
  USES_DB=0
  [[ "$t" == "robinhood" ]] && USES_DB=1
  begin_timed_unit
  case "$t" in
    gufi)
      run_gufi plain "$rep"
      if [[ "${GUFI_DO_ROLLUP:-1}" == "1" ]]; then
        run_gufi rollup "$rep"
      fi
      ;;
    xdu) run_xdu "$rep" ;;
    robinhood)
      run_robinhood "$rep"
      # The scan crawled into index-free tables, which is not a database anyone
      # queries. Creating the three indexes is the rest of what Robinhood costs,
      # measured on its own so the split is visible.
      run_robinhood_indexes "$rep"
      ;;
    find) run_find_baseline "$rep" ;;
    fd) run_fd_baseline "$rep" ;;
    du) run_du_baseline "$rep" ;;
    dua) run_dua_baseline "$rep" ;;
    dut) run_dut_baseline "$rep" ;;
    *) echo "WARN: unknown tool $t" >&2 ;;
  esac
}

for t in $TOOLS; do
  case "$t" in
    ecrawl)
      run_ecrawl_series
      continue
      ;;
  esac
  run_cache_series "$t" "$(tool_reps "$t")" run_tool_pipeline "$t"
done

echo "Wrote $CSV"
echo "results=$OUT"
