#!/usr/bin/env bash
#
# Run paper queries Q1–Q5 against available tools.
#
# Usage:
#   scripts/compare-indexers/run_queries.sh <synth-root-or-tree> [results-dir]
#
# Expects QUERY_SEEDS.txt from prepare-synth.sh under <synth-root>, or set:
#   Q1_NAME=... Q2_TERM=slurm- Q2_GLOB='slurm-*.out' Q3_MIN_BYTES=... Q4_SUBTREE=... Q5_SUBTREE=...
#
# For the ecrawl suite, set:
#   ECRAWL_BIN_DIR=...          crawl output with uid_shard_*.bin
#   EREPORT_INDEX_DIR=...       trigram index from ereport_index --make
# For GUFI/XDU:
#   GUFI_INDEX_DIR=... XDU_INDEX_DIR=...
#
# Env: TOOLS="find fd du dua ecrawl_suite gufi xdu robinhood" REPS=3
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
TOOLS=${TOOLS:-"find fd du dua ecrawl_suite gufi xdu robinhood"}

echo "==> threads: $(thread_plan)"
raise_nofile
echo "==> $NOFILE_NOW"
write_env_snapshot "$OUT/env.txt"

# gufi_find and gufi_du take no thread flag and no index argument: both come
# from a config file whose path is compiled into them. Write it now, against the
# index this run built, so the wrappers query that index with the same thread
# budget as every other tool.
GUFI_CONFIG_NOTE=""
if have_cmd "${GUFI_FIND:-}" && [[ -n "${GUFI_INDEX_DIR:-}" && -d "${GUFI_INDEX_DIR:-}" ]]; then
  if GUFI_CONFIG_NOTE=$(gufi_write_config "$GUFI_INDEX_DIR"); then
    echo "==> gufi config: $(gufi_config_path) (IndexRoot=$GUFI_INDEX_DIR, Threads=$THREADS)"
  else
    echo "WARN: could not write the GUFI config ($GUFI_CONFIG_NOTE); its queries will be skipped" >&2
  fi
fi

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

Q1_NAME=${Q1_NAME:-${q1_unique_name:-}}
Q1_PATH=${Q1_PATH:-${q1_unique_path:-}}
Q2_TERM=${Q2_TERM:-${q2_term:-slurm-}}
Q2_GLOB=${Q2_GLOB:-${q2_glob:-slurm-*.out}}
Q3_MIN=${Q3_MIN_BYTES:-${q3_min_bytes:-$SIZE_GT_BYTES}}
Q4_SUBTREE=${Q4_SUBTREE:-${q4_subtree:-$TREE}}
Q5_SUBTREE=${Q5_SUBTREE:-${q5_subtree:-$Q4_SUBTREE}}

if [[ -z "$Q1_NAME" ]]; then
  echo "WARN: Q1_NAME unset; Q1 may be empty. Run prepare-synth.sh or export seeds." >&2
fi

# Derived once, outside the timed region: the literal the trigram index searches
# for, and the regex that trims its substring matches down to find -name's answer.
Q1_ERE=""
[[ -z "$Q1_NAME" ]] || Q1_ERE=$(name_basename_ere "$Q1_NAME")
Q2_INDEX_TERM=""
Q2_ERE=""
if [[ -n "$Q2_GLOB" ]]; then
  { read -r Q2_INDEX_TERM; read -r Q2_ERE; } < <(glob_index_filter "$Q2_GLOB")
  # Below three characters there is no trigram to look up, so the index cannot
  # narrow the candidates and the seed term (if any) is the better prefilter.
  if [[ ${#Q2_INDEX_TERM} -lt 3 && -n "$Q2_TERM" ]]; then
    Q2_INDEX_TERM=$Q2_TERM
  fi
  [[ ${#Q2_INDEX_TERM} -ge 3 ]] || Q2_INDEX_TERM=""
fi

# The resolved parameters, not just the seed file: overrides and defaults are
# applied above, and the summary explains Q1-Q5 in terms of what actually ran.
{
  printf 'tree=%s\n' "$TREE"
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
  [[ -z "${q2_expected:-}" ]] || printf 'q2_expected=%s\n' "$q2_expected"
  [[ -z "${q3_expected:-}" ]] || printf 'q3_expected=%s\n' "$q3_expected"
} >"$OUT/query_params.txt"

CSV="$OUT/query_results.csv"
echo "tool,query,rep,status,elapsed_sec,result_count,notes" >"$CSV"

append_q() {
  python3 - "$1" "$2" "$3" "$4" "$5" "$6" "$7" <<'PY' >>"$CSV"
import sys, csv
w = csv.writer(sys.stdout)
w.writerow(sys.argv[1:8])
PY
}

# A tool that cannot run at all still owes the table five rows: recording only
# Q1 leaves the others missing, which reads as "not attempted" rather than "not
# possible", and the reason is then nowhere.
skip_all_queries() {
  local tool=$1 rep=$2 reason=$3 q
  for q in Q1 Q2 Q3 Q4 Q5; do
    append_q "$tool" "$q" "$rep" skipped "" 0 "$reason"
  done
}

time_count() {
  # Args: tool query rep cmd...  — runs cmd, counts non-empty stdout lines, times via TIMEFORMAT
  local tool=$1 query=$2 rep=$3
  shift 3
  local tfile="$OUT/${tool}_${query}_r${rep}.time.txt"
  local ofile="$OUT/${tool}_${query}_r${rep}.out.txt"
  local efile="$OUT/${tool}_${query}_r${rep}.err.txt"
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
}

# du/dua answer Q4 only: they total bytes and have no name, type or size
# predicates, so the remaining queries are genuinely out of scope for them.
q_du() {
  local rep=$1
  local tfile="$OUT/du_Q4_r${rep}.time.txt"
  set +e
  time_cmd "$tfile" du -sb "$Q4_SUBTREE" \
    >"$OUT/du_Q4_r${rep}.out.txt" 2>"$OUT/du_Q4_r${rep}.err.txt"
  local st=$?
  set -e
  local el bytes
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(awk 'NR == 1 { print $1 }' "$OUT/du_Q4_r${rep}.out.txt")
  if [[ $st -eq 0 ]]; then
    append_q du Q4 "$rep" ok "${el:-}" "${bytes:-0}" "du_sb_bytes"
  elif [[ $st -eq 1 ]]; then
    append_q du Q4 "$rep" ok "${el:-}" "${bytes:-0}" "du_sb_bytes partial_exit=1"
  else
    append_q du Q4 "$rep" fail "${el:-}" 0 "exit=$st"
  fi
  local q
  for q in Q1 Q2 Q3 Q5; do
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
  local tfile="$OUT/dua_Q4_r${rep}.time.txt"
  set +e
  time_cmd "$tfile" "$DUA_BIN" "${DUA_AGG_ARGS[@]}" "$Q4_SUBTREE" "$sentinel" \
    >"$OUT/dua_Q4_r${rep}.out.txt" 2>"$OUT/dua_Q4_r${rep}.err.txt"
  local st=$?
  set -e
  local el bytes note
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(dua_bytes_for_path "$OUT/dua_Q4_r${rep}.out.txt" "$Q4_SUBTREE")
  note="dua_apparent_bytes"
  [[ "$DUA_HAS_BYTES" == "1" ]] || note="dua_build_lacks_--format_bytes_count_unparsed"
  if [[ $st -eq 0 ]]; then
    append_q dua Q4 "$rep" ok "${el:-}" "${bytes:-0}" "$note"
  else
    append_q dua Q4 "$rep" fail "${el:-}" 0 "exit=$st"
  fi
  local q
  for q in Q1 Q2 Q3; do
    append_q dua "$q" "$rep" skipped "" 0 "dua_totals_bytes_only_no_search_predicates"
  done
  # --stats reports entries traversed (files plus directories), which is not the
  # regular-file count Q5 asks for.
  append_q dua Q5 "$rep" skipped "" 0 "dua_reports_sizes_not_file_counts"
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
# Q3/Q4/Q5 read the capture with ecrawl_analyze, which selects records without
# rebuilding a path for each one.
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
  else
    append_q ereport_index Q1 "$rep" skipped "" 0 "need_EREPORT_INDEX_DIR"
    append_q ereport_index Q2 "$rep" skipped "" 0 "need_EREPORT_INDEX_DIR"
  fi
  TOLERATE_EXIT1=0
  EXIT1_NOTE=""
  NOTE=""

  if [[ -z "$bins" || ! -d "$bins" ]] || ! tool_available ecrawl_analyze; then
    local q
    for q in Q3 Q4 Q5; do
      append_q ecrawl_analyze "$q" "$rep" skipped "" 0 "need_ECRAWL_BIN_DIR_and_ecrawl_analyze"
    done
    return 0
  fi

  # Q3 and Q5 print paths, exactly as find does, so the line count is the answer.
  NOTE="capture_scan"
  time_count ecrawl_analyze Q3 "$rep" "$ECRAWL_ANALYZE_BIN" --size-gt "$Q3_MIN" --type f --list "$bins"
  time_count ecrawl_analyze Q5 "$rep" "$ECRAWL_ANALYZE_BIN" --subtree "$Q5_SUBTREE" --type f --list "$bins"
  NOTE=""

  # Q4 is one number: apparent bytes with each multiply-linked inode counted
  # once, which is what du -sb reports.
  #
  # Since ERCBIN08 this is the one query that may not read records at all: a bare
  # --subtree aggregate is answered from the catalog's subtree prefix sums, which
  # is O(directories) instead of O(files). It falls back to a full scan when any
  # record under $Q4_SUBTREE has nlink > 1, because crawl-time hardlink credit is
  # attributed to the first link seen anywhere in the tree while a scan dedups
  # within the subtree. The two paths differ by orders of magnitude, and which one
  # runs is a property of the fixture, so record answered_from in the note rather
  # than leaving an unexplained swing between trees.
  local tfile="$OUT/ecrawl_analyze_Q4_r${rep}.time.txt"
  set +e
  time_cmd "$tfile" "$ECRAWL_ANALYZE_BIN" --subtree "$Q4_SUBTREE" "$bins" \
    >"$OUT/ecrawl_analyze_Q4_r${rep}.out.txt" 2>"$OUT/ecrawl_analyze_Q4_r${rep}.err.txt"
  local st=$?
  set -e
  local el bytes answered scanned
  el=$(elapsed_from_time_v "$tfile" || echo "")
  bytes=$(sed -n 's/^bytes=//p' "$OUT/ecrawl_analyze_Q4_r${rep}.out.txt" | tail -1 || true)
  answered=$(sed -n 's/^answered_from=//p' "$OUT/ecrawl_analyze_Q4_r${rep}.out.txt" | tail -1 || true)
  scanned=$(sed -n 's/^records_scanned=//p' "$OUT/ecrawl_analyze_Q4_r${rep}.out.txt" | tail -1 || true)
  if [[ $st -ne 0 ]]; then
    append_q ecrawl_analyze Q4 "$rep" fail "${el:-}" 0 "exit=$st"
  else
    append_q ecrawl_analyze Q4 "$rep" ok "${el:-}" "${bytes:-0}" \
      "${answered:-answered_from_unknown}; records_scanned=${scanned:-?}; du_sb_semantics"
  fi
}

q_gufi() {
  local rep=$1
  local gidx=${GUFI_INDEX_DIR:-}
  if [[ -z "$gidx" || ! -d "$gidx" ]]; then
    skip_all_queries gufi "$rep" "need_GUFI_INDEX_DIR"
    return 0
  fi
  if ! have_cmd "$GUFI_FIND"; then
    skip_all_queries gufi "$rep" "gufi_find_missing"
    return 0
  fi
  if [[ -n "$GUFI_CONFIG_NOTE" ]]; then
    skip_all_queries gufi "$rep" "no_gufi_config: $GUFI_CONFIG_NOTE"
    return 0
  fi
  # Every path below is relative to IndexRoot, which gufi_find prepends and
  # then refuses to leave. Anything it cannot express is a query GUFI would
  # silently answer with nothing.
  local rel_tree rel_q4 rel_q5
  if ! rel_tree=$(gufi_rel_path "$TREE" "$TREE") ||
    ! rel_q4=$(gufi_rel_path "$Q4_SUBTREE" "$TREE") ||
    ! rel_q5=$(gufi_rel_path "$Q5_SUBTREE" "$TREE"); then
    skip_all_queries gufi "$rep" "query_paths_outside_the_indexed_tree"
    return 0
  fi

  local NOTE="index_relative_to_$(gufi_config_path)"
  if [[ -n "$Q1_NAME" ]] && ! blocked gufi Q1 "$rep"; then
    time_count gufi Q1 "$rep" "$GUFI_FIND" "$rel_tree" -name "$Q1_NAME"
  fi
  blocked gufi Q2 "$rep" ||
    time_count gufi Q2 "$rep" "$GUFI_FIND" "$rel_tree" -name "$Q2_GLOB"
  blocked gufi Q3 "$rep" ||
    time_count gufi Q3 "$rep" "$GUFI_FIND" "$rel_tree" -type f -size "+${Q3_MIN}c"
  if blocked gufi Q4 "$rep"; then
    :
  elif have_cmd "$GUFI_DU"; then
    local tfile="$OUT/gufi_Q4_r${rep}.time.txt"
    set +e
    # --block-size 1 with --apparent-size is du -sb: file lengths, unrounded,
    # which is the convention every other Q4 row reports.
    time_cmd "$tfile" "$GUFI_DU" -s --apparent-size --block-size 1 "$rel_q4" \
      >"$OUT/gufi_Q4_r${rep}.out.txt" 2>"$OUT/gufi_Q4_r${rep}.err.txt"
    local st=$?
    set -e
    local el bytes
    el=$(elapsed_from_time_v "$tfile" || echo "")
    # `<bytes> <path>`, as du prints it.
    bytes=$(awk 'NF && $1 ~ /^[0-9]+$/ { print $1; exit }' "$OUT/gufi_Q4_r${rep}.out.txt" 2>/dev/null || true)
    if [[ $st -ne 0 ]]; then
      append_q gufi Q4 "$rep" fail "${el:-}" 0 "exit=$st"
    elif [[ -z "$bytes" ]]; then
      append_q gufi Q4 "$rep" fail "${el:-}" 0 "gufi_du_printed_no_byte_total"
    else
      append_q gufi Q4 "$rep" ok "${el:-}" "$bytes" "gufi_du_-s_--apparent-size"
    fi
  else
    append_q gufi Q4 "$rep" skipped "" 0 "gufi_du_missing"
  fi
  blocked gufi Q5 "$rep" || time_count gufi Q5 "$rep" "$GUFI_FIND" "$rel_q5" -type f
  NOTE=""
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
  blocked xdu Q2 "$rep" ||
    time_count xdu Q2 "$rep" "$XDU_FIND" -i "$xidx" -p 'slurm-[^/]*\.out$'
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
    local tfile="$OUT/xdu_Q4_r${rep}.time.txt"
    set +e
    time_cmd "$tfile" "$XDU_FIND" -i "$xidx" -p "^${pref}/" -f size \
      >"$OUT/xdu_Q4_r${rep}.out.txt" 2>"$OUT/xdu_Q4_r${rep}.err.txt"
    local st=$?
    set -e
    local el sum
    el=$(elapsed_from_time_v "$tfile" || echo "")
    sum=$(size_field_sum "$OUT/xdu_Q4_r${rep}.out.txt")
    if [[ $st -ne 0 ]]; then
      append_q xdu Q4 "$rep" fail "${el:-}" 0 "exit=$st"
    elif [[ -z "$sum" && ! -s "$OUT/xdu_Q4_r${rep}.out.txt" ]]; then
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
    local tfile5="$OUT/xdu_Q5_r${rep}.time.txt"
    set +e
    time_cmd "$tfile5" "$XDU_FIND" -i "$xidx" -p "^${pref}/" --count \
      >"$OUT/xdu_Q5_r${rep}.out.txt" 2>"$OUT/xdu_Q5_r${rep}.err.txt"
    local st5=$?
    set -e
    local el5 count
    el5=$(elapsed_from_time_v "$tfile5" || echo "")
    count=$(grep -oE '[0-9][0-9,]*' "$OUT/xdu_Q5_r${rep}.out.txt" 2>/dev/null | head -1 | tr -d ',' || true)
    if [[ $st5 -ne 0 ]]; then
      append_q xdu Q5 "$rep" fail "${el5:-}" 0 "exit=$st5"
    elif [[ -z "$count" ]]; then
      append_q xdu Q5 "$rep" fail "${el5:-}" 0 "xdu-find_--count_printed_no_number"
    else
      append_q xdu Q5 "$rep" ok "${el5:-}" "$count" "xdu-find_--count"
    fi
  fi
  NOTE=""
}

# fd: the parallel "fast find" baseline test.sh prefers over find(1).
q_fd() {
  local rep=$1
  local TOLERATE_EXIT1=1
  if ! tool_available fd; then
    skip_all_queries fd "$rep" "fd_not_found_install_fd-find"
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
    local tfile="$OUT/robinhood_Q4_r${rep}.time.txt"
    set +e
    time_cmd "$tfile" "$RBH_DU" "${cfg_args[@]}" -s "${RBH_DU_ARGS[@]}" "$Q4_SUBTREE" \
      >"$OUT/robinhood_Q4_r${rep}.out.txt" 2>"$OUT/robinhood_Q4_r${rep}.err.txt"
    local st=$?
    set -e
    local el bytes
    el=$(elapsed_from_time_v "$tfile" || echo "")
    bytes=$(grep -oE '[0-9]+' "$OUT/robinhood_Q4_r${rep}.out.txt" 2>/dev/null | head -1 || true)
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
  probe_for gufi "Q1 Q2" gufi_name "$GUFI_FIND" "$rel_q5" -name "$PROBE_NO_MATCH"
  probe_for gufi "Q3" gufi_size "$GUFI_FIND" "$rel_q5" -type f -size "+${Q3_MIN}c"
  probe_for gufi "Q5" gufi_type "$GUFI_FIND" "$rel_q5" -type f
  if have_cmd "$GUFI_DU"; then
    local rel_q4
    rel_q4=$(gufi_rel_path "$Q4_SUBTREE" "$TREE") || return 0
    probe_for gufi "Q4" gufi_du "$GUFI_DU" -s --apparent-size --block-size 1 "$rel_q4"
  fi
  return 0
}

probe_robinhood() {
  have_cmd "$RBH_FIND" || return 0
  # Probing a schema-less database would only rediscover the missing tables and
  # blame the predicate for it.
  rbh_schema_ready || return 0
  probe_for robinhood "Q1 Q2" rbh_name "$RBH_FIND" -f "$RBH_CONFIG" "$Q5_SUBTREE" -name "$PROBE_NO_MATCH"
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
  probe_for xdu "Q1 Q2 Q5" xdu_regex "$XDU_FIND" -i "$xidx" -p "$PROBE_NO_MATCH"
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

for ((rep = 1; rep <= REPS; rep++)); do
  export CURRENT_REP=$rep
  for t in $TOOLS; do
    printf '==> rep %d/%d: %s Q1-Q5 (%s)\n' "$rep" "$REPS" "$t" "$(date +%H:%M:%S)"
    case "$t" in
      find) q_find "$rep" ;;
      fd) q_fd "$rep" ;;
      du) q_du "$rep" ;;
      dua) q_dua "$rep" ;;
      ecrawl_suite|suite) q_suite "$rep" ;;
      gufi) q_gufi "$rep" ;;
      xdu) q_xdu "$rep" ;;
      robinhood) q_robinhood "$rep" ;;
      *) echo "WARN: unknown tool $t" >&2 ;;
    esac
  done
done

echo "Wrote $CSV"
echo "results=$OUT"
