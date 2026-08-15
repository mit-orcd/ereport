#!/usr/bin/env bash
#
# Run queries Q1–Q6 against available tools. Q1-Q5 are the paper's; Q6 is this
# comparison's own addition, an unanchored substring match.
#
# Usage:
#   scripts/compare-indexers/run_queries.sh <synth-root-or-tree> [results-dir]
#
# Expects QUERY_SEEDS.txt from prepare-synth.sh under <synth-root>, or set:
#   Q1_NAME=... Q2_TERM=slurm- Q2_GLOB='slurm-*.out' Q3_MIN_BYTES=... Q4_SUBTREE=... Q5_SUBTREE=...
#   Q6_GLOB='*token*.dat'
# An exported value applies to every argument set; the manifest's <key>_1..3 are
# what the sets are otherwise taken from.
#
# For the ecrawl suite, set:
#   ECRAWL_BIN_DIR=...          crawl output with uid_shard_*.bin
#   EREPORT_INDEX_DIR=...       ereport_index --make output: the trigram index
#                     Q1/Q2/Q6 search, and the dir-index sidecars Q4 and Q5 look
#                     up their subtree in. Both are bound to the capture they
#                     were built from, so the two must be the same run's pair.
# For GUFI/XDU:
#   GUFI_PLAIN_INDEX_DIR=...    dir2index output (GUFI_INDEX_DIR is still read)
#   GUFI_ROLLUP_INDEX_DIR=...   the same index after gufi_rollup
#   XDU_INDEX_DIR=...
#
# Env: TOOLS="find fd du dua dut ecrawl_suite gufi xdu robinhood" REPS=3
#      REPS_<TOOL>=n overrides REPS for one tool, e.g. REPS_ROBINHOOD=1
#      CACHE_MODES="cold hot"  per tool: all cold reps (drop, then Q1–Q6 on
#                     set 1), then all hot reps (same work, warmed by the last
#                     cold suite). Remaining argument sets run once hot after.
#      ARG_SETS=3     ceiling on how many argument sets the hot pass uses
#
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
require_python3

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <tree-or-synth-root> [results-dir]" >&2
  exit 2
fi

TREE=$(cd "$1" && pwd)
TS=$(date +%Y%m%d-%H%M%S)
OUT=${2:-"$COMPARE_DIR/results/queries-$TS"}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
TOOLS=${TOOLS:-"find fd du dua dut ecrawl_suite gufi xdu robinhood"}

echo "==> threads: $(thread_plan)"
raise_nofile
echo "==> $NOFILE_NOW"

# GUFI is queried once per index the run built, because the two are not the same
# capability: gufi_dir2index alone answers the name, size and type questions,
# while the byte total needs the treesummary rows only gufi_rollup writes -- at
# several times the build cost. The index phase leaves a pointer file per index.
gufi_pointer() {
  local file="${INDEX_RESULTS_DIR:-}/$1"
  [[ -n "${INDEX_RESULTS_DIR:-}" && -f "$file" ]] || return 0
  cat "$file"
}
GUFI_PLAIN_INDEX_DIR=${GUFI_PLAIN_INDEX_DIR:-$(gufi_pointer gufi_plain_index_dir.txt)}
GUFI_ROLLUP_INDEX_DIR=${GUFI_ROLLUP_INDEX_DIR:-$(gufi_pointer gufi_rollup_index_dir.txt)}
# A caller that only knows the old single-index variable (prod-protocol.md, an
# earlier results directory) still gets one series out of it. Which one it is
# depends on what the index phase did, so it stands in for whichever is missing.
if [[ -n "${GUFI_INDEX_DIR:-}" ]]; then
  [[ -n "$GUFI_PLAIN_INDEX_DIR" || -n "$GUFI_ROLLUP_INDEX_DIR" ]] ||
    GUFI_PLAIN_INDEX_DIR=$GUFI_INDEX_DIR
fi

# gufi_find and gufi_du take no thread flag and no index argument: both come
# from a config file whose path is compiled into them. q_gufi_index rewrites it
# per series; write it once here too, so the probe pass below and the banner
# have an index to speak about.
GUFI_CONFIG_NOTE=""
GUFI_PROBE_INDEX_DIR=${GUFI_PLAIN_INDEX_DIR:-$GUFI_ROLLUP_INDEX_DIR}
if have_cmd "${GUFI_FIND:-}" && [[ -n "$GUFI_PROBE_INDEX_DIR" && -d "$GUFI_PROBE_INDEX_DIR" ]]; then
  if GUFI_CONFIG_NOTE=$(gufi_write_config "$GUFI_PROBE_INDEX_DIR"); then
    echo "==> gufi config: $(gufi_config_path) (IndexRoot=$GUFI_PROBE_INDEX_DIR, Threads=$THREADS)"
    [[ -z "$GUFI_ROLLUP_INDEX_DIR" ]] ||
      echo "==> gufi rollup index: $GUFI_ROLLUP_INDEX_DIR (queried as gufi_rollup)"
  else
    echo "WARN: could not write the GUFI config ($GUFI_CONFIG_NOTE); its queries will be skipped" >&2
  fi
fi

# After the GUFI indexes are resolved, so env.txt names both of them: the summary
# reads which index each series was answered from out of this file.
write_env_snapshot "$OUT/env.txt"

# Which unit rbh-du reports in, resolved once from the installed binary.
RBH_DU_ARGS=()
RBH_DU_NOTE="rbh-du_not_probed"
if have_cmd "${RBH_DU:-}"; then
  mapfile -t _rbh_du_probe < <(rbh_du_args)
  [[ -z "${_rbh_du_probe[0]:-}" ]] || RBH_DU_ARGS=("${_rbh_du_probe[0]}")
  RBH_DU_NOTE=${_rbh_du_probe[1]:-rbh-du_-s}
  unset _rbh_du_probe
fi

CMD_LOG="$OUT/COMMANDS.txt"
export CMD_LOG
printf '# exact argv of every timed command, in execution order\n# time\tlabel\tcommand\n' >"$CMD_LOG"

# Whatever the harness itself has to say (cache drops, a database that would
# not restart), kept apart from the tools' own stderr.
HARNESS_LOG="$OUT/harness.log"
export HARNESS_LOG
printf '# harness diagnostics, not tool output\n' >"$HARNESS_LOG"

# Whatever the caller pinned, before the manifest or a set can overwrite it.
record_arg_overrides

# Load seeds
SEEDS="$TREE/QUERY_SEEDS.txt"
if [[ -f "$SEEDS" ]]; then
  # shellcheck disable=SC1090
  set -a
  # Convert key=value to exports
  while IFS= read -r line; do
    [[ "$line" == *=* ]] || continue
    k=${line%%=*}
    v=${line#*=}
    export "$k=$v"
  done <"$SEEDS"
  set +a
fi

# How many argument sets the manifest planted. The measured series is set 1
# only: all cold reps, then all hot reps, so the last cold suite warms the hot
# series. Sets 2..N run once afterwards, still hot, so those bars are not a
# re-answer of set 1.
ARG_SETS_AVAILABLE=$(arg_sets_available)

select_arg_set 1
if [[ -z "$Q1_NAME" ]]; then
  echo "WARN: Q1_NAME unset; Q1 may be empty. Run prepare-synth.sh or export seeds." >&2
fi

# The resolved parameters of every set, not just the seed file: overrides and
# defaults are applied by select_arg_set, and the summary explains Q1-Q6 in terms
# of what actually ran. Set 1 is also written unindexed, which is the shape the
# reporting scripts read for a single-set run.
{
  printf 'tree=%s\n' "$TREE"
  printf 'arg_sets=%s\n' "$ARG_SETS_AVAILABLE"
  for _s in $(seq 1 "$ARG_SETS_AVAILABLE"); do
    select_arg_set "$_s"
    printf 'q1_name_%s=%s\n' "$_s" "${Q1_NAME:-}"
    printf 'q1_path_%s=%s\n' "$_s" "${Q1_PATH:-}"
    printf 'q2_glob_%s=%s\n' "$_s" "${Q2_GLOB:-}"
    printf 'q2_term_%s=%s\n' "$_s" "${Q2_TERM:-}"
    printf 'q1_exact_filter_%s=%s\n' "$_s" "${Q1_ERE:-}"
    printf 'q2_index_term_%s=%s\n' "$_s" "${Q2_INDEX_TERM:-}"
    printf 'q2_exact_filter_%s=%s\n' "$_s" "${Q2_ERE:-}"
    printf 'q3_min_bytes_%s=%s\n' "$_s" "${Q3_MIN:-}"
    printf 'q4_subtree_%s=%s\n' "$_s" "${Q4_SUBTREE:-}"
    printf 'q5_subtree_%s=%s\n' "$_s" "${Q5_SUBTREE:-}"
    printf 'q6_glob_%s=%s\n' "$_s" "${Q6_GLOB:-}"
    printf 'q6_index_term_%s=%s\n' "$_s" "${Q6_INDEX_TERM:-}"
    printf 'q6_exact_filter_%s=%s\n' "$_s" "${Q6_ERE:-}"
    for _key in q2_expected q3_expected q6_expected; do
      _want=$(seed_value "$_key" "$_s")
      [[ -z "$_want" ]] || printf '%s_%s=%s\n' "$_key" "$_s" "$_want"
    done
  done
  select_arg_set 1
  printf 'q1_name=%s\n' "${Q1_NAME:-}"
  printf 'q1_path=%s\n' "${Q1_PATH:-}"
  printf 'q2_glob=%s\n' "${Q2_GLOB:-}"
  printf 'q2_term=%s\n' "${Q2_TERM:-}"
  printf 'q1_exact_filter=%s\n' "${Q1_ERE:-}"
  printf 'q2_index_term=%s\n' "${Q2_INDEX_TERM:-}"
  printf 'q2_exact_filter=%s\n' "${Q2_ERE:-}"
  printf 'q3_min_bytes=%s\n' "${Q3_MIN:-}"
  printf 'q4_subtree=%s\n' "${Q4_SUBTREE:-}"
  printf 'q5_subtree=%s\n' "${Q5_SUBTREE:-}"
  printf 'q6_glob=%s\n' "${Q6_GLOB:-}"
  printf 'q6_index_term=%s\n' "${Q6_INDEX_TERM:-}"
  printf 'q6_exact_filter=%s\n' "${Q6_ERE:-}"
  [[ -z "${q2_expected:-}" ]] || printf 'q2_expected=%s\n' "$q2_expected"
  [[ -z "${q3_expected:-}" ]] || printf 'q3_expected=%s\n' "$q3_expected"
  [[ -z "${q6_expected:-}" ]] || printf 'q6_expected=%s\n' "$q6_expected"
} >"$OUT/query_params.txt"
unset _s _key _want

CSV="$OUT/query_results.csv"
echo "tool,query,rep,cache,arg_set,status,elapsed_sec,result_count,notes" >"$CSV"

# Appended to every per-pass filename so four passes in one repetition do not
# overwrite each other's output. Empty for the first pass, which keeps the name
# summarize.py looks for first when it quotes a command's output.
RUN_TAG=""

append_q() {
  # The pass a row belongs to is a property of the run, not something each of the
  # forty call sites below should have to remember to pass in.
  python3 - "$1" "$2" "$3" "$(cache_label)" "$ARG_SET" "$4" "$5" "$6" "$7" <<'PY' >>"$CSV"
import sys, csv
w = csv.writer(sys.stdout)
w.writerow(sys.argv[1:10])
PY
}

# A tool that cannot run at all still owes the table one row per query: recording
# only Q1 leaves the others missing, which reads as "not attempted" rather than
# "not possible", and the reason is then nowhere.
QUERY_LIST="Q1 Q2 Q3 Q4 Q5 Q6"
skip_all_queries() {
  local tool=$1 rep=$2 reason=$3 q
  for q in $QUERY_LIST; do
    append_q "$tool" "$q" "$rep" skipped "" 0 "$reason"
  done
}

time_count() {
  # Args: tool query rep cmd...  — runs cmd, counts non-empty stdout lines, times via TIMEFORMAT
  local tool=$1 query=$2 rep=$3
  shift 3
  local stem="${tool}_${query}_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  local ofile="$OUT/${stem}.out.txt"
  local efile="$OUT/${stem}.err.txt"
  set +e
  time_cmd "$tfile" "$@" >"$ofile" 2>"$efile"
  local st=$?
  set -e
  local el count
  el=$(elapsed_from_time_v "$tfile" || echo "")
  count=$(grep -c '.' "$ofile" 2>/dev/null || true)
  count=${count:-0}
  if [[ $st -eq 0 ]]; then
    append_q "$tool" "$query" "$rep" ok "${el:-}" "$count" "${NOTE:-}"
  elif [[ $st -eq 1 && "${TOLERATE_EXIT1:-0}" == "1" ]]; then
    append_q "$tool" "$query" "$rep" ok "${el:-}" "$count" "${EXIT1_NOTE:-partial_exit=1}"
  else
    append_q "$tool" "$query" "$rep" fail "${el:-}" "$count" "exit=$st"
  fi
}

# ---- find baselines ----
q_find() {
  local rep=$1
  # Seen by time_count through bash dynamic scoping: find/du exit 1 on
  # unreadable or vanishing paths, which must not void the reference baseline.
  local TOLERATE_EXIT1=1
  [[ -n "$Q1_NAME" ]] && time_count find Q1 "$rep" find "$TREE" -xdev -name "$Q1_NAME"
  time_count find Q2 "$rep" find "$TREE" -xdev -name "$Q2_GLOB"
  time_count find Q3 "$rep" find "$TREE" -xdev -type f -size "+${Q3_MIN}c"
  # find has no aggregate primitive; the `du` rows carry Q4 for this pairing.
  append_q find Q4 "$rep" skipped "" 0 "no_aggregate_primitive_see_du_row"
  time_count find Q5 "$rep" find "$Q5_SUBTREE" -xdev -type f
  # Q6 is Q2 with the leading anchor removed, so find does exactly the same work
  # for it: a full walk either way. That is the reference the indexes are read
  # against, and the difference between Q2 and Q6 is what the indexes make of it.
  if [[ -n "$Q6_GLOB" ]]; then
    time_count find Q6 "$rep" find "$TREE" -xdev -name "$Q6_GLOB"
  else
    append_q find Q6 "$rep" skipped "" 0 "no_q6_glob_in_the_seed_manifest"
  fi
}

# du/dua/dut answer Q4 only: they total bytes and have no name, type or size
# predicates, so the remaining queries are genuinely out of scope for them.
q_du() {
  local rep=$1
  local stem="du_Q4_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  set +e
  time_cmd "$tfile" du -sb "$Q4_SUBTREE" \
    >"$OUT/${stem}.out.txt" 2>"$OUT/${stem}.err.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(awk 'NR == 1 { print $1 }' "$OUT/${stem}.out.txt")
  if [[ $st -eq 0 ]]; then
    append_q du Q4 "$rep" ok "${el:-}" "${bytes:-0}" "du_sb_bytes"
  elif [[ $st -eq 1 ]]; then
    append_q du Q4 "$rep" ok "${el:-}" "${bytes:-0}" "du_sb_bytes partial_exit=1"
  else
    append_q du Q4 "$rep" fail "${el:-}" 0 "exit=$st"
  fi
  local q
  for q in Q1 Q2 Q3 Q5 Q6; do
    append_q du "$q" "$rep" skipped "" 0 "du_totals_bytes_only_no_search_predicates"
  done
}

q_dua() {
  local rep=$1
  if ! tool_available dua; then
    append_q dua Q4 "$rep" skipped "" 0 "$(dua_skip_reason)"
    return 0
  fi
  local sentinel
  sentinel=$(dua_sentinel "$OUT")
  local stem="dua_Q4_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  set +e
  time_cmd "$tfile" "$DUA_BIN" "${DUA_AGG_ARGS[@]}" "$Q4_SUBTREE" "$sentinel" \
    >"$OUT/${stem}.out.txt" 2>"$OUT/${stem}.err.txt"
  local st=$?
  set -e
  local el bytes note
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(dua_bytes_for_path "$OUT/${stem}.out.txt" "$Q4_SUBTREE")
  note="dua_apparent_bytes"
  [[ "$DUA_HAS_BYTES" == "1" ]] || note="dua_build_lacks_--format_bytes_count_unparsed"
  if [[ $st -eq 0 ]]; then
    append_q dua Q4 "$rep" ok "${el:-}" "${bytes:-0}" "$note"
  else
    append_q dua Q4 "$rep" fail "${el:-}" 0 "exit=$st"
  fi
  local q
  for q in Q1 Q2 Q3 Q6; do
    append_q dua "$q" "$rep" skipped "" 0 "dua_totals_bytes_only_no_search_predicates"
  done
  # --stats reports entries traversed (files plus directories), which is not the
  # regular-file count Q5 asks for.
  append_q dua Q5 "$rep" skipped "" 0 "dua_reports_sizes_not_file_counts"
}

q_dut() {
  local rep=$1
  if ! tool_available dut; then
    append_q dut Q4 "$rep" skipped "" 0 "$(dut_skip_reason)"
    return 0
  fi
  local stem="dut_Q4_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  set +e
  time_cmd "$tfile" "$DUT_BIN" "${DUT_ARGS[@]}" "$Q4_SUBTREE" \
    >"$OUT/${stem}.out.txt" 2>"$OUT/${stem}.err.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  # `<total> <shared> <path>`, one line: the first field is the total, and with
  # -s -b and no -x it is du -sb's number, over du -sb's tree, hard links
  # deduplicated the same way.
  bytes=$(awk 'END { print $1 }' "$OUT/${stem}.out.txt")
  if [[ $st -eq 0 ]]; then
    append_q dut Q4 "$rep" ok "${el:-}" "${bytes:-0}" "dut_apparent_bytes"
  else
    append_q dut Q4 "$rep" fail "${el:-}" 0 "exit=$st"
  fi
  local q
  for q in Q1 Q2 Q3 Q6; do
    append_q dut "$q" "$rep" skipped "" 0 "dut_totals_bytes_only_no_search_predicates"
  done
  # -f does count entries, but it counts directories along with files and takes
  # no type predicate, so it does not answer the regular-file count Q5 asks for.
  append_q dut Q5 "$rep" skipped "" 0 "dut_counts_dirs_too_no_type_filter"
}

# The index-then-filter pipeline for Q1/Q2, with the values already substituted
# so the command log shows the line that ran rather than placeholders.
search_filter_script() {
  printf 'set -o pipefail; %q --search --index-dir %q %q | grep -E -- %q' \
    "$EREPORT_INDEX_BIN" "$1" "$2" "$3"
}

# ---- ecrawl suite ----
#
# Q1/Q2 run the trigram index for the smallest candidate set it can produce, then
# a basename-anchored regex to land on find's exact answer: the index matches a
# literal substring anywhere in the path, which is a superset of what -name means.
# Q3/Q4/Q5 read the capture with ecrawl_query, which selects records without
# rebuilding a path for each one; Q4 and Q5 also read the dir-index sidecars the
# same ereport_index --make build wrote, so both halves of the suite depend on
# that phase and on it having indexed this exact capture.
q_suite() {
  local rep=$1
  local idx=${EREPORT_INDEX_DIR:-}
  local bins=${ECRAWL_BIN_DIR:-}
  # Allow sibling index run to leave pointers; also accept env.
  if [[ -z "$idx" && -f "${INDEX_RESULTS_DIR:-}/ereport_index_dir.txt" ]]; then
    idx=$(cat "${INDEX_RESULTS_DIR}/ereport_index_dir.txt")
  fi
  if [[ -z "$bins" && -f "${INDEX_RESULTS_DIR:-}/ecrawl_bin_dir.txt" ]]; then
    bins=$(cat "${INDEX_RESULTS_DIR}/ecrawl_bin_dir.txt")
  fi

  # grep exits 1 on no match, which is an empty answer rather than a failure.
  local TOLERATE_EXIT1=1
  local EXIT1_NOTE="index_prefilter_then_exact_filter; no_match"
  local NOTE="index_prefilter_then_exact_filter"
  if [[ -n "$idx" && -d "$idx" ]] && tool_available ereport_index && [[ -n "$Q1_NAME" ]]; then
    time_count ereport_index Q1 "$rep" bash -c "$(search_filter_script "$idx" "$Q1_NAME" "$Q1_ERE")"
    if [[ -n "$Q2_INDEX_TERM" ]]; then
      time_count ereport_index Q2 "$rep" bash -c \
        "$(search_filter_script "$idx" "$Q2_INDEX_TERM" "$Q2_ERE")"
    else
      append_q ereport_index Q2 "$rep" skipped "" 0 "glob_has_no_literal_run_for_the_trigram_index"
    fi
    # Q6 needs no new machinery: the longest literal run of '*token*.dat' is the
    # token, so the index is handed the most selective term the query contains
    # and does not care that it sits in the middle of the name. That is the
    # difference this query exists to show.
    if [[ -n "$Q6_INDEX_TERM" ]]; then
      time_count ereport_index Q6 "$rep" bash -c \
        "$(search_filter_script "$idx" "$Q6_INDEX_TERM" "$Q6_ERE")"
    else
      append_q ereport_index Q6 "$rep" skipped "" 0 "glob_has_no_literal_run_for_the_trigram_index"
    fi
  else
    append_q ereport_index Q1 "$rep" skipped "" 0 "need_EREPORT_INDEX_DIR"
    append_q ereport_index Q2 "$rep" skipped "" 0 "need_EREPORT_INDEX_DIR"
    append_q ereport_index Q6 "$rep" skipped "" 0 "need_EREPORT_INDEX_DIR"
  fi
  TOLERATE_EXIT1=0
  EXIT1_NOTE=""
  NOTE=""

  if [[ -z "$bins" || ! -d "$bins" ]] || ! tool_available ecrawl_query; then
    local q
    for q in Q3 Q4 Q5; do
      append_q ecrawl_query "$q" "$rep" skipped "" 0 "need_ECRAWL_BIN_DIR_and_ecrawl_query"
    done
    return 0
  fi

  # The two subtree queries read the dir-index sidecars ereport_index --make
  # writes beside the trigram index: Q4 looks its root up instead of
  # materialising every directory row, Q5 additionally scans only the row groups
  # whose DFS sketch can hold a descendant. Not a variant of its own -- the build
  # is already a timed, sized row of its own -- so the rows use it outright, and
  # a run without an index phase still answers both, from the catalog. Q3 has no
  # subtree to resolve and is left alone. The sidecars are bound to the shards
  # they were built from, so this is the same directory the Q1/Q2/Q6 rows search.
  local analyze_idx=()
  [[ -z "$idx" || ! -d "$idx" ]] || analyze_idx=(--index-dir "$idx")

  # Q3 and Q5 print paths, exactly as find does, so the line count is the answer.
  NOTE="capture_scan"
  time_count ecrawl_query Q3 "$rep" "$ECRAWL_QUERY_BIN" --size-gt "$Q3_MIN" --type f --list "$bins"
  time_count ecrawl_query Q5 "$rep" "$ECRAWL_QUERY_BIN" "${analyze_idx[@]}" \
    --subtree "$Q5_SUBTREE" --type f --list "$bins"
  NOTE=""

  # Q4 is one number: apparent bytes with each multiply-linked inode counted
  # once, which is what du -sb reports.
  #
  # Since ERCBIN08 this is the one query that may not read records at all: a bare
  # --subtree aggregate is answered from the catalog's subtree prefix sums, which
  # is O(directories) instead of O(files), and with --index-dir from the one
  # directory row the sidecar points at, which is O(1). It falls back to a full
  # scan when any record under $Q4_SUBTREE has nlink > 1, because crawl-time
  # hardlink credit is attributed to the first link seen anywhere in the tree
  # while a scan dedups within the subtree. The three paths differ by orders of
  # magnitude, and which one runs is a property of the fixture and of whether the
  # index phase ran, so record answered_from (dir_index, catalog_rollup or
  # record_scan) in the note rather than leaving an unexplained swing between
  # trees.
  local stem="ecrawl_query_Q4_r${rep}${RUN_TAG}"
  local tfile="$OUT/${stem}.time.txt"
  set +e
  time_cmd "$tfile" "$ECRAWL_QUERY_BIN" "${analyze_idx[@]}" --subtree "$Q4_SUBTREE" "$bins" \
    >"$OUT/${stem}.out.txt" 2>"$OUT/${stem}.err.txt"
  local st=$?
  set -e
  local el bytes answered scanned
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(sed -n 's/^bytes=//p' "$OUT/${stem}.out.txt" | tail -1 || true)
  answered=$(sed -n 's/^answered_from=//p' "$OUT/${stem}.out.txt" | tail -1 || true)
  scanned=$(sed -n 's/^records_scanned=//p' "$OUT/${stem}.out.txt" | tail -1 || true)
  if [[ $st -ne 0 ]]; then
    append_q ecrawl_query Q4 "$rep" fail "${el:-}" 0 "exit=$st"
  else
    append_q ecrawl_query Q4 "$rep" ok "${el:-}" "${bytes:-0}" \
      "${answered:-answered_from_unknown}; records_scanned=${scanned:-?}; du_sb_semantics"
  fi
}

# One GUFI series per index the run built. rolled_up=1 is the index gufi_rollup
# post-processed, which is the only one gufi_du can answer Q4 from -- and the one
# that cost several times as much to build. Charting both under one name made a
# cheap index look like it answered a question only the expensive one can.
q_gufi_index() {
  local tool=$1 gidx=$2 rolled_up=$3 rep=$4
  if [[ -z "$gidx" || ! -d "$gidx" ]]; then
    skip_all_queries "$tool" "$rep" "need_GUFI_INDEX_DIR"
    return 0
  fi
  if ! have_cmd "$GUFI_FIND"; then
    skip_all_queries "$tool" "$rep" "gufi_find_missing"
    return 0
  fi
  # IndexRoot is compiled into the wrappers' config file, so switching index
  # means rewriting it. Done here, before the queries, rather than once at
  # startup: the two series read two different trees.
  local why
  if ! why=$(gufi_write_config "$gidx"); then
    skip_all_queries "$tool" "$rep" "no_gufi_config: $why"
    return 0
  fi
  # Every path below is relative to IndexRoot, which gufi_find prepends and
  # then refuses to leave. Anything it cannot express is a query GUFI would
  # silently answer with nothing.
  local rel_tree rel_q4 rel_q5
  if ! rel_tree=$(gufi_rel_path "$TREE" "$TREE") ||
    ! rel_q4=$(gufi_rel_path "$Q4_SUBTREE" "$TREE") ||
    ! rel_q5=$(gufi_rel_path "$Q5_SUBTREE" "$TREE"); then
    skip_all_queries "$tool" "$rep" "query_paths_outside_the_indexed_tree"
    return 0
  fi

  local NOTE
  NOTE="index_relative_to_$(gufi_config_path)"
  [[ "$rolled_up" == "1" ]] || NOTE="$NOTE; dir2index_only_no_rollup"
  if [[ -n "$Q1_NAME" ]] && ! blocked "$tool" Q1 "$rep"; then
    time_count "$tool" Q1 "$rep" "$GUFI_FIND" "$rel_tree" -name "$Q1_NAME"
  fi
  blocked "$tool" Q2 "$rep" ||
    time_count "$tool" Q2 "$rep" "$GUFI_FIND" "$rel_tree" -name "$Q2_GLOB"
  blocked "$tool" Q3 "$rep" ||
    time_count "$tool" Q3 "$rep" "$GUFI_FIND" "$rel_tree" -type f -size "+${Q3_MIN}c"
  if blocked "$tool" Q4 "$rep"; then
    :
  elif ! have_cmd "$GUFI_DU"; then
    append_q "$tool" Q4 "$rep" skipped "" 0 "gufi_du_missing"
  elif [[ "$rolled_up" != "1" ]]; then
    # gufi_du -s answers from treesummary rows, and only gufi_rollup writes
    # them: on a plain index it warns, prints 0 and still exits 0. Recording
    # that as an answer would credit the cheap index with the expensive one's
    # capability, so this is a skip with its own reason.
    append_q "$tool" Q4 "$rep" skipped "" 0 \
      "rollup_required_gufi_du_reads_treesummary_rows_only_gufi_rollup_writes"
  else
    local stem="${tool}_Q4_r${rep}${RUN_TAG}"
    local tfile="$OUT/${stem}.time.txt"
    set +e
    # --block-size 1 with --apparent-size is du -sb: file lengths, unrounded,
    # which is the convention every other Q4 row reports.
    time_cmd "$tfile" "$GUFI_DU" -s --apparent-size --block-size 1 "$rel_q4" \
      >"$OUT/${stem}.out.txt" 2>"$OUT/${stem}.err.txt"
    local st=$?
    set -e
    local el bytes
    el=$(elapsed_from_time_v "$tfile" || echo "")
    # `<bytes> <path>`, as du prints it.
    bytes=$(awk 'NF && $1 ~ /^[0-9]+$/ { print $1; exit }' "$OUT/${stem}.out.txt" 2>/dev/null || true)
    if [[ $st -ne 0 ]]; then
      append_q "$tool" Q4 "$rep" fail "${el:-}" 0 "exit=$st"
    elif [[ -z "$bytes" ]]; then
      append_q "$tool" Q4 "$rep" fail "${el:-}" 0 "gufi_du_printed_no_byte_total"
    else
      append_q "$tool" Q4 "$rep" ok "${el:-}" "$bytes" "gufi_du_-s_--apparent-size"
    fi
  fi
  blocked "$tool" Q5 "$rep" || time_count "$tool" Q5 "$rep" "$GUFI_FIND" "$rel_q5" -type f
  if [[ -z "$Q6_GLOB" ]]; then
    append_q "$tool" Q6 "$rep" skipped "" 0 "no_q6_glob_in_the_seed_manifest"
  else
    blocked "$tool" Q6 "$rep" ||
      time_count "$tool" Q6 "$rep" "$GUFI_FIND" "$rel_tree" -name "$Q6_GLOB"
  fi
  NOTE=""
}

q_gufi() {
  local rep=$1 ran=0
  if [[ -n "$GUFI_PLAIN_INDEX_DIR" ]]; then
    q_gufi_index gufi "$GUFI_PLAIN_INDEX_DIR" 0 "$rep"
    ran=1
  fi
  if [[ -n "$GUFI_ROLLUP_INDEX_DIR" ]]; then
    q_gufi_index gufi_rollup "$GUFI_ROLLUP_INDEX_DIR" 1 "$rep"
    ran=1
  fi
  # No pointer from the index phase and no override: say so once under the plain
  # name rather than twice under two.
  ((ran)) || q_gufi_index gufi "" 0 "$rep"
}

q_xdu() {
  local rep=$1
  local xidx=${XDU_INDEX_DIR:-}
  if [[ -z "$xidx" || ! -d "$xidx" ]] || ! have_cmd "$XDU_FIND"; then
    skip_all_queries xdu "$rep" "need_XDU_INDEX_DIR_and_xdu-find"
    return 0
  fi
  local NOTE="$XDU_SIZE_NOTE"
  # Q1: regex escape unique name
  if [[ -n "$Q1_NAME" ]] && ! blocked xdu Q1 "$rep"; then
    time_count xdu Q1 "$rep" "$XDU_FIND" -i "$xidx" -p "/${Q1_NAME}\$"
  fi
  # Q2_ERE is the glob translated to a path-anchored regex, which is the only
  # form xdu-find takes. Spelling one glob's regex out by hand here worked while
  # there was one argument set and answered the wrong question once there were
  # three.
  if [[ -z "$Q2_ERE" ]]; then
    append_q xdu Q2 "$rep" skipped "" 0 "glob_does_not_translate_to_a_path_regex"
  elif ! blocked xdu Q2 "$rep"; then
    time_count xdu Q2 "$rep" "$XDU_FIND" -i "$xidx" -p "$Q2_ERE"
  fi
  # Q3 and Q4 are the two size questions, and this index only holds a size the
  # rest of the comparison would recognise if it was built with
  # --apparent-size. Without it the records carry st_blocks, so a sparse 500 MB
  # file reads as zero: the answers are not wrong by a little, they are in a
  # different unit, and reporting a number here would put a disagreement in the
  # summary that reads like a bug in xdu rather than a gap in the build.
  local xdu_blocks_only=0
  ((${#XDU_SIZE_ARGS[@]} > 0)) || xdu_blocks_only=1
  local unit_skip="index_holds_st_blocks_size_answers_would_be_in_the_wrong_unit"

  # Bare bytes, the same threshold find is given: xdu's K/M/G suffixes do not say
  # whether they are 1024- or 1000-based, and a count cannot be misread.
  if [[ "$xdu_blocks_only" == "1" ]]; then
    append_q xdu Q3 "$rep" skipped "" 0 "$unit_skip"
  elif ! blocked xdu Q3 "$rep"; then
    time_count xdu Q3 "$rep" "$XDU_FIND" -i "$xidx" --min-size "$Q3_MIN"
  fi

  # xdu resolves symlinks as it indexes, so its records hold the canonical path.
  # Anchoring on the path the benchmark was handed matches nothing whenever any
  # component is a symlink -- on this cluster $HOME/orcd/scratch is one, and Q4
  # and Q5 both came back empty while Q1-Q3, which only anchor a basename at the
  # end, were unaffected.
  local pref sub
  sub=$(realpath -- "$Q4_SUBTREE" 2>/dev/null) || sub=$Q4_SUBTREE
  pref=$(printf '%s' "$sub" | sed 's/[.[\*^$()+?{|]/\\&/g')
  # Q4: xdu has no aggregate, so sum the size of every record under the prefix.
  if [[ "$xdu_blocks_only" == "1" ]]; then
    append_q xdu Q4 "$rep" skipped "" 0 "$unit_skip"
  elif ! blocked xdu Q4 "$rep"; then
    local stem4="xdu_Q4_r${rep}${RUN_TAG}"
    local tfile="$OUT/${stem4}.time.txt"
    set +e
    time_cmd "$tfile" "$XDU_FIND" -i "$xidx" -p "^${pref}/" -f size \
      >"$OUT/${stem4}.out.txt" 2>"$OUT/${stem4}.err.txt"
    local st=$?
    set -e
    local el sum
    el=$(elapsed_from_time_v "$tfile" || echo "")
    sum=$(size_field_sum "$OUT/${stem4}.out.txt")
    if [[ $st -ne 0 ]]; then
      append_q xdu Q4 "$rep" fail "${el:-}" 0 "exit=$st"
    elif [[ -z "$sum" && ! -s "$OUT/${stem4}.out.txt" ]]; then
      # Empty and unparsable are different faults: no records under the prefix
      # points at the prefix, not at how xdu prints a size.
      append_q xdu Q4 "$rep" fail "${el:-}" 0 "xdu-find_matched_no_records_under_${pref}"
    elif [[ -z "$sum" ]]; then
      append_q xdu Q4 "$rep" fail "${el:-}" 0 "xdu-find_-f_size_printed_no_integer_sizes"
    else
      append_q xdu Q4 "$rep" ok "${el:-}" "$sum" "sum_sizes_under_prefix; $XDU_SIZE_NOTE"
    fi
  fi

  # --count prints the total, so the answer is the number it printed, not the
  # one line it printed it on.
  if ! blocked xdu Q5 "$rep"; then
    local stem5="xdu_Q5_r${rep}${RUN_TAG}"
    local tfile5="$OUT/${stem5}.time.txt"
    set +e
    time_cmd "$tfile5" "$XDU_FIND" -i "$xidx" -p "^${pref}/" --count \
      >"$OUT/${stem5}.out.txt" 2>"$OUT/${stem5}.err.txt"
    local st5=$?
    set -e
    local el5 count
    el5=$(elapsed_from_time_v "$tfile5" || echo "")
    count=$(grep -oE '[0-9][0-9,]*' "$OUT/${stem5}.out.txt" 2>/dev/null | head -1 | tr -d ',' || true)
    if [[ $st5 -ne 0 ]]; then
      append_q xdu Q5 "$rep" fail "${el5:-}" 0 "exit=$st5"
    elif [[ -z "$count" ]]; then
      append_q xdu Q5 "$rep" fail "${el5:-}" 0 "xdu-find_--count_printed_no_number"
    else
      append_q xdu Q5 "$rep" ok "${el5:-}" "$count" "xdu-find_--count"
    fi
  fi

  # Q6 is the same predicate shape as Q2 with the leading anchor dropped, which
  # for a regex over full paths is one character of difference and no help from
  # the index either way.
  if [[ -z "$Q6_ERE" ]]; then
    append_q xdu Q6 "$rep" skipped "" 0 "no_q6_glob_in_the_seed_manifest"
  elif ! blocked xdu Q6 "$rep"; then
    time_count xdu Q6 "$rep" "$XDU_FIND" -i "$xidx" -p "$Q6_ERE"
  fi
  NOTE=""
}

# fd: the parallel "fast find" baseline test.sh prefers over find(1).
q_fd() {
  local rep=$1
  local TOLERATE_EXIT1=1
  if ! tool_available fd; then
    skip_all_queries fd "$rep" "$(fd_skip_reason)"
    return 0
  fi
  [[ -n "$Q1_NAME" ]] &&
    time_count fd Q1 "$rep" "$FD_BIN" "${FD_COMMON_ARGS[@]}" -t f -g "$Q1_NAME" "$TREE"
  time_count fd Q2 "$rep" "$FD_BIN" "${FD_COMMON_ARGS[@]}" -t f -g "$Q2_GLOB" "$TREE"
  if [[ "$FD_HAS_SIZE" == "1" ]]; then
    time_count fd Q3 "$rep" "$FD_BIN" "${FD_COMMON_ARGS[@]}" -t f --size "+${Q3_MIN}b" . "$TREE"
  else
    append_q fd Q3 "$rep" skipped "" 0 "fd_build_lacks_--size"
  fi
  append_q fd Q4 "$rep" skipped "" 0 "fd_has_no_du_equivalent"
  time_count fd Q5 "$rep" "$FD_BIN" "${FD_COMMON_ARGS[@]}" -t f . "$Q5_SUBTREE"
  if [[ -n "$Q6_GLOB" ]]; then
    time_count fd Q6 "$rep" "$FD_BIN" "${FD_COMMON_ARGS[@]}" -t f -g "$Q6_GLOB" "$TREE"
  else
    append_q fd Q6 "$rep" skipped "" 0 "no_q6_glob_in_the_seed_manifest"
  fi
}

# Runs once per query phase, not once per repetition: creating an index that is
# already there costs a failed statement and a confusing log line.
RBH_INDEX_STATE=""
rbh_ensure_indexes_for_queries() {
  [[ -z "$RBH_INDEX_STATE" ]] || return 0
  local have
  have=$(rbh_indexes_present)
  if [[ "$have" == "3" ]]; then
    RBH_INDEX_STATE="from_the_index_phase"
    return 0
  fi
  if rbh_create_indexes; then
    RBH_INDEX_STATE="created_here_untimed"
    harness_warn "robinhood: created the three query indexes ($have/3 were present); the index phase did not"
  else
    RBH_INDEX_STATE="incomplete"
    harness_warn "robinhood: could not create all three query indexes; rows may reflect table scans"
  fi
}

q_robinhood() {
  local rep=$1
  if ! have_cmd "$RBH_FIND"; then
    skip_all_queries robinhood "$rep" "rbh-find_not_found"
    return 0
  fi
  # Without a reachable database holding Robinhood's tables, every query below
  # would fail the same way, five times per repetition. Say why once instead.
  if ! rbh_schema_ready; then
    skip_all_queries robinhood "$rep" "$RBH_SCHEMA_REASON"
    return 0
  fi
  local cfg_args=(-f "$RBH_CONFIG")
  # Robinhood's own restart is the only cache this row has; see maybe_drop_caches.
  local USES_DB=1
  # The index phase creates the three indexes and times it. When it did not run
  # -- a standalone run_queries.sh, or a results directory whose crawl predates
  # this -- create them here instead, untimed: nobody runs a relational database
  # unindexed, so measuring one would report a configuration that does not exist.
  rbh_ensure_indexes_for_queries
  # ENTRIES is keyed by inode and rbh-find prints one path per entry, so a file
  # with four links is one row here and four for find. On the seeded tree that is
  # a constant six-row shortfall at every Q3 threshold, and it is an accounting
  # difference rather than a miss: every one of the six is a hard link.
  local NOTE="one_name_per_inode"
  if [[ -n "$Q1_NAME" ]] && ! blocked robinhood Q1 "$rep"; then
    time_count robinhood Q1 "$rep" "$RBH_FIND" "${cfg_args[@]}" "$TREE" -name "$Q1_NAME"
  fi
  blocked robinhood Q2 "$rep" ||
    time_count robinhood Q2 "$rep" "$RBH_FIND" "${cfg_args[@]}" "$TREE" -name "$Q2_GLOB"
  blocked robinhood Q3 "$rep" ||
    time_count robinhood Q3 "$rep" "$RBH_FIND" "${cfg_args[@]}" "$TREE" -type f \
      -size "$(rbh_size_arg "$Q3_MIN")"
  if blocked robinhood Q4 "$rep"; then
    :
  elif have_cmd "${RBH_DU:-}"; then
    # Q4 is one number, not a list, so the line count time_count records would
    # be 1 whatever the answer was.
    local stem="robinhood_Q4_r${rep}${RUN_TAG}"
    local tfile="$OUT/${stem}.time.txt"
    set +e
    time_cmd "$tfile" "$RBH_DU" "${cfg_args[@]}" -s "${RBH_DU_ARGS[@]}" "$Q4_SUBTREE" \
      >"$OUT/${stem}.out.txt" 2>"$OUT/${stem}.err.txt"
    local st=$?
    set -e
    local el bytes
    el=$(elapsed_from_time_v "$tfile" || echo "")
    bytes=$(grep -oE '[0-9]+' "$OUT/${stem}.out.txt" 2>/dev/null | head -1 || true)
    if [[ $st -ne 0 ]]; then
      append_q robinhood Q4 "$rep" fail "${el:-}" 0 "exit=$st"
    elif [[ -z "$bytes" ]]; then
      append_q robinhood Q4 "$rep" fail "${el:-}" 0 "rbh-du_printed_no_number"
    else
      append_q robinhood Q4 "$rep" ok "${el:-}" "$bytes" "$RBH_DU_NOTE"
    fi
  else
    append_q robinhood Q4 "$rep" skipped "" 0 "rbh-du_not_found"
  fi
  blocked robinhood Q5 "$rep" ||
    time_count robinhood Q5 "$rep" "$RBH_FIND" "${cfg_args[@]}" "$Q5_SUBTREE" -type f
  # Q6 asks for a substring with nothing anchored at the front, which is the one
  # shape a B-tree on NAMES.name cannot narrow: name_index is there and the
  # optimiser still has to read every row. Same predicate as Q2, same index, and
  # the difference between the two rows is the whole reason Q6 exists.
  NOTE="$NOTE; name_index_cannot_seek_an_unanchored_substring"
  if [[ -z "$Q6_GLOB" ]]; then
    append_q robinhood Q6 "$rep" skipped "" 0 "no_q6_glob_in_the_seed_manifest"
  elif ! blocked robinhood Q6 "$rep"; then
    time_count robinhood Q6 "$rep" "$RBH_FIND" "${cfg_args[@]}" "$TREE" -name "$Q6_GLOB"
  fi
  NOTE=""
}

# ---- probe pass ----
# A predicate an installed tool rejects fails identically in every repetition,
# and on a production tree each of those failures still costs a full scan before
# it reports the same parse error. Ask each external tool once, against the small
# seeded Q5 subtree, whether it accepts the shape of each query; queries it
# refuses become one skipped row carrying the tool's own message.
declare -A PROBE_BLOCK=()

probe_reason=""
# Runs a probe and, on failure, sets probe_reason from the tool's own stderr.
probe() {
  local label=$1
  shift
  local efile="$OUT/probe_${label}.err.txt"
  local st=0
  "$@" >/dev/null 2>"$efile" || st=$?
  log_command "probe_${label}" "$@"
  if [[ $st -eq 0 ]]; then
    probe_reason=""
    return 0
  fi
  local msg
  # First non-empty stderr line, flattened: the CSV reason field is one line.
  msg=$(grep -m1 '.' "$efile" 2>/dev/null | tr -d '\r' | cut -c1-160 || true)
  probe_reason="probe_exit=$st${msg:+: $msg}"
  return 1
}

# queries: the space-separated queries this probe speaks for. Always succeeds;
# the outcome is the block map, so one refused predicate does not stop the run
# from probing the rest.
probe_for() {
  local tool=$1 queries=$2 label=$3
  shift 3
  probe "$label" "$@" && return 0
  local q
  for q in $queries; do
    PROBE_BLOCK["$tool:$q"]="$probe_reason"
  done
  printf 'WARN: %s cannot answer %s: %s\n' "$tool" "${queries// /,}" "$probe_reason" >&2
  return 0
}

probe_gufi() {
  [[ -n "$GUFI_CONFIG_NOTE" ]] && return 0
  have_cmd "$GUFI_FIND" || return 0
  local rel_q5
  rel_q5=$(gufi_rel_path "$Q5_SUBTREE" "$TREE") || return 0
  # Whether gufi_find accepts a predicate is a property of the binary, not of
  # the index behind it, so one probe speaks for both series.
  local series
  for series in gufi gufi_rollup; do
    probe_for "$series" "Q1 Q2 Q6" gufi_name "$GUFI_FIND" "$rel_q5" -name "$PROBE_NO_MATCH"
    probe_for "$series" "Q3" gufi_size "$GUFI_FIND" "$rel_q5" -type f -size "+${Q3_MIN}c"
    probe_for "$series" "Q5" gufi_type "$GUFI_FIND" "$rel_q5" -type f
  done
  # Q4 is only asked of the rolled-up series, and only that index can answer it,
  # so probing it against the plain one would report a capability the run then
  # refuses to use.
  if have_cmd "$GUFI_DU" && [[ -n "$GUFI_ROLLUP_INDEX_DIR" ]]; then
    local rel_q4
    rel_q4=$(gufi_rel_path "$Q4_SUBTREE" "$TREE") || return 0
    gufi_write_config "$GUFI_ROLLUP_INDEX_DIR" >/dev/null || return 0
    probe_for gufi_rollup "Q4" gufi_du "$GUFI_DU" -s --apparent-size --block-size 1 "$rel_q4"
    gufi_write_config "$GUFI_PROBE_INDEX_DIR" >/dev/null || true
  fi
  return 0
}

probe_robinhood() {
  have_cmd "$RBH_FIND" || return 0
  # Probing a schema-less database would only rediscover the missing tables and
  # blame the predicate for it.
  rbh_schema_ready || return 0
  probe_for robinhood "Q1 Q2 Q6" rbh_name "$RBH_FIND" -f "$RBH_CONFIG" "$Q5_SUBTREE" -name "$PROBE_NO_MATCH"
  probe_for robinhood "Q3" rbh_size "$RBH_FIND" -f "$RBH_CONFIG" "$Q5_SUBTREE" -type f \
    -size "$(rbh_size_arg "$Q3_MIN")"
  probe_for robinhood "Q5" rbh_type "$RBH_FIND" -f "$RBH_CONFIG" "$Q5_SUBTREE" -type f
  if have_cmd "${RBH_DU:-}"; then
    probe_for robinhood "Q4" rbh_du "$RBH_DU" -f "$RBH_CONFIG" -s "${RBH_DU_ARGS[@]}" "$Q4_SUBTREE"
  fi
  return 0
}

probe_xdu() {
  local xidx=${XDU_INDEX_DIR:-}
  [[ -n "$xidx" && -d "$xidx" ]] || return 0
  have_cmd "$XDU_FIND" || return 0
  probe_for xdu "Q1 Q2 Q5 Q6" xdu_regex "$XDU_FIND" -i "$xidx" -p "$PROBE_NO_MATCH"
  # No point asking whether the predicate parses when the sizes behind it are in
  # the wrong unit; q_xdu skips both size queries for that reason instead.
  if ((${#XDU_SIZE_ARGS[@]} > 0)); then
    probe_for xdu "Q3" xdu_size "$XDU_FIND" -i "$xidx" --min-size "$Q3_MIN"
    probe_for xdu "Q4" xdu_field "$XDU_FIND" -i "$xidx" -p "$PROBE_NO_MATCH" -f size
  fi
  return 0
}

# A name no seeded file has, so a probe that works returns nothing quickly.
PROBE_NO_MATCH="indexer_compare_probe_no_such_name"

if [[ "${SKIP_PROBE:-0}" != "1" ]]; then
  for t in $TOOLS; do
    case "$t" in
      gufi) printf '==> probing gufi predicates (%s)\n' "$(date +%H:%M:%S)"; probe_gufi ;;
      robinhood) printf '==> probing robinhood predicates (%s)\n' "$(date +%H:%M:%S)"; probe_robinhood ;;
      xdu) printf '==> probing xdu predicates (%s)\n' "$(date +%H:%M:%S)"; probe_xdu ;;
    esac
  done
fi

# Records the skipped row when the probe already showed this query cannot work,
# so the caller can leave the real invocation unattempted.
blocked() {
  local tool=$1 query=$2 rep=$3
  local reason=${PROBE_BLOCK["$tool:$query"]:-}
  [[ -z "$reason" ]] && return 1
  append_q "$tool" "$query" "$rep" skipped "" 0 "$reason"
  return 0
}

echo "==> repetitions: $(reps_plan $TOOLS)"
echo "==> cache series per tool: $CACHE_MODES (all cold reps, then all hot reps, set 1)"

run_one_tool() {
  local t=$1 rep=$2
  USES_DB=0
  [[ "$t" == "robinhood" ]] && USES_DB=1
  begin_timed_unit
  case "$t" in
    find) q_find "$rep" ;;
    fd) q_fd "$rep" ;;
    du) q_du "$rep" ;;
    dua) q_dua "$rep" ;;
    dut) q_dut "$rep" ;;
    ecrawl_suite|suite) q_suite "$rep" ;;
    gufi) q_gufi "$rep" ;;
    xdu) q_xdu "$rep" ;;
    robinhood) q_robinhood "$rep" ;;
    *) echo "WARN: unknown tool $t" >&2 ;;
  esac
}

for t in $TOOLS; do
  t_reps=$(tool_reps "$t")
  pass=0
  for mode in $CACHE_MODES; do
    pass=$((pass + 1))
    export CACHE_STATE=$mode
    select_arg_set 1
    for ((rep = 1; rep <= t_reps; rep++)); do
      export CURRENT_REP=$rep
      RUN_TAG=""
      ((pass == 1)) || RUN_TAG="_${mode}"
      printf '==> %s rep %d/%d %s set %d Q1-Q6 (%s)\n' \
        "$t" "$rep" "$t_reps" "$mode" "$ARG_SET" "$(date +%H:%M:%S)"
      run_one_tool "$t" "$rep"
    done
  done
  # Extra argument sets stay hot-only and run once, after this tool's measured
  # series, so they are not a second answer to set 1 and do not drop the cache
  # that the last hot suite left.
  if [[ "$ARG_SETS_AVAILABLE" -gt 1 ]] && [[ " $CACHE_MODES " == *" hot "* ]]; then
    export CACHE_STATE=hot
    export CURRENT_REP=1
    for ((_s = 2; _s <= ARG_SETS_AVAILABLE; _s++)); do
      select_arg_set "$_s"
      RUN_TAG="_hot_a${ARG_SET}"
      printf '==> %s extra set %d (hot) Q1-Q6 (%s)\n' "$t" "$ARG_SET" "$(date +%H:%M:%S)"
      run_one_tool "$t" 1
    done
  fi
done

# Robinhood's indexes are part of what a run costs, so they do not survive it:
# the next --do crawls index-free tables, times creating them again, and the two
# runs are comparable. Nothing else here leaves state behind for the next run.
if [[ -n "$RBH_INDEX_STATE" ]] && rbh_schema_ready; then
  if rbh_drop_indexes; then
    echo "==> robinhood: dropped the three query indexes (next crawl starts index-free)"
  else
    harness_warn "robinhood: could not drop the query indexes; the next crawl will not be index-free"
  fi
fi

echo "Wrote $CSV"
echo "results=$OUT"
