#!/usr/bin/env bash
#
# Compare find() baseline counts against ecrawl (and optionally other tools)
# on a quiescent tree.
#
# Usage:
#   scripts/compare-indexers/check_correctness.sh <tree-root> [results-dir]
#
# Env:
#   CHECK_TOOLS=          space-separated. Defaults to the suite (find, ecrawl,
#                         ecrawl_analyze) plus every external that is installed.
#                         ecrawl_analyze reuses the capture the ecrawl check
#                         wrote, so keep it after ecrawl.
#   ECRAWL_BIN=...
#
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <tree-root> [results-dir]" >&2
  exit 2
fi

TREE=$(cd "$1" && pwd)
TS=$(date +%Y%m%d-%H%M%S)
OUT=${2:-"$COMPARE_DIR/results/correctness-$TS"}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
# Deliberately not $TOOLS: everywhere else in this suite that name means "the
# externals to install and benchmark", and benchmark.sh exports it. Reading it
# here replaced the suite checks with a list of externals, so a --smoke run with
# TOOLS="gufi xdu dua" checked neither ecrawl nor ecrawl_analyze and then failed
# on dua as an unknown tool -- silently skipping the checks in the very mode
# meant to exercise them.
CHECK_TOOLS=${CHECK_TOOLS:-}
if [[ -z "$CHECK_TOOLS" ]]; then
  CHECK_TOOLS="find ecrawl ecrawl_analyze"
  for _t in gufi xdu dua; do
    tool_available "$_t" && CHECK_TOOLS+=" $_t"
  done
  unset _t
fi

write_env_snapshot "$OUT/env.txt"
find_baseline "$TREE" "$OUT/find.baseline.txt"
echo "find baseline:" && cat "$OUT/find.baseline.txt"

# Two byte references, because the tools answer two different questions. GUFI
# and xdu hold one record per directory entry, so a hard-linked file counts once
# per link and their totals match find's sum of %s. du and dua deduplicate hard
# links and include the directory inodes, so they match du -sb. On the smoke
# tree those two differ by 33 MB; a single reference would fail one pair or the
# other for reasons that say nothing about the tool.
REF_FILES=$(kv_get "$OUT/find.baseline.txt" files)
REF_LINK_BYTES=$(find "$TREE" -type f -printf '%s\n' 2>/dev/null |
  awk '{ s += $1 } END { printf "%d", s + 0 }')
REF_DU_BYTES=$(du -sb "$TREE" 2>/dev/null | awk 'NR == 1 { print $1 }')

# Compare one number, report it the same way for every tool.
cmp_num() {
  local tool=$1 label=$2 got=$3 want=$4
  if [[ -z "$got" ]]; then
    echo "$tool $label: nothing parsed from output (want $want)" | tee -a "$SUMMARY"
    return 1
  fi
  [[ "$got" == "$want" ]] && return 0
  echo "$tool $label mismatch: got=$got want=$want" | tee -a "$SUMMARY"
  return 1
}

FAIL=0
SUMMARY="$OUT/SUMMARY.txt"
: >"$SUMMARY"

check_ecrawl() {
  local bin_dir="$OUT/ecrawl_bin"
  mkdir -p "$bin_dir"
  if ! tool_available ecrawl; then
    echo "ecrawl: skipped (binary missing)" | tee -a "$SUMMARY"
    return 0
  fi
  "$ECRAWL_BIN" "$TREE" "$bin_dir" >"$OUT/ecrawl.stdout.txt" 2>"$OUT/ecrawl.stderr.txt" || {
    echo "ecrawl: FAIL (nonzero exit)" | tee -a "$SUMMARY"
    FAIL=1
    return 0
  }
  # Prefer key=value lines from verbose/summary if present; else parse stdout.
  local ef ed el eb
  ef=$(kv_get "$OUT/ecrawl.stdout.txt" files || true)
  ed=$(kv_get "$OUT/ecrawl.stdout.txt" dirs || true)
  el=$(kv_get "$OUT/ecrawl.stdout.txt" symlinks || true)
  eb=$(kv_get "$OUT/ecrawl.stdout.txt" total_bytes || true)
  if [[ -z "$ef" ]]; then
    # Non-verbose summary may print a human line; fall back to grep.
    ef=$(grep -Eo 'files=[0-9]+' "$OUT/ecrawl.stdout.txt" | tail -1 | cut -d= -f2 || true)
    ed=$(grep -Eo 'dirs=[0-9]+' "$OUT/ecrawl.stdout.txt" | tail -1 | cut -d= -f2 || true)
    el=$(grep -Eo 'symlinks=[0-9]+' "$OUT/ecrawl.stdout.txt" | tail -1 | cut -d= -f2 || true)
    eb=$(grep -Eo 'total_bytes=[0-9]+' "$OUT/ecrawl.stdout.txt" | tail -1 | cut -d= -f2 || true)
  fi
  {
    echo "files=${ef:-}"
    echo "dirs=${ed:-}"
    echo "symlinks=${el:-}"
    echo "total_bytes=${eb:-}"
  } >"$OUT/ecrawl.counts.txt"

  local ff fd fl fb
  ff=$(kv_get "$OUT/find.baseline.txt" files)
  fd=$(kv_get "$OUT/find.baseline.txt" dirs)
  fl=$(kv_get "$OUT/find.baseline.txt" symlinks)
  fb=$(kv_get "$OUT/find.baseline.txt" unique_file_bytes)

  local ok=1
  [[ "$ef" == "$ff" ]] || { echo "ecrawl files mismatch: got=$ef want=$ff" | tee -a "$SUMMARY"; ok=0; }
  [[ "$ed" == "$fd" ]] || { echo "ecrawl dirs mismatch: got=$ed want=$fd" | tee -a "$SUMMARY"; ok=0; }
  [[ "$el" == "$fl" ]] || { echo "ecrawl symlinks mismatch: got=$el want=$fl" | tee -a "$SUMMARY"; ok=0; }
  # Bytes: ecrawl total_bytes is hardlink-deduped logical; should match find unique bytes on quiet trees.
  if [[ -n "$eb" && -n "$fb" && "$eb" != "$fb" ]]; then
    echo "ecrawl total_bytes mismatch: got=$eb want=$fb (may differ if tree changed mid-run)" | tee -a "$SUMMARY"
    ok=0
  fi
  if [[ $ok -eq 1 ]]; then
    echo "ecrawl: OK files=$ef dirs=$ed symlinks=$el bytes=$eb" | tee -a "$SUMMARY"
  else
    FAIL=1
  fi
}

# The query path the benchmark reports for Q3/Q4/Q5 is only worth timing if it
# returns the right answer, so check it against du and find on the same tree.
check_ecrawl_analyze() {
  local bin_dir="$OUT/ecrawl_bin"
  if ! tool_available ecrawl_analyze; then
    echo "ecrawl_analyze: skipped (binary missing)" | tee -a "$SUMMARY"
    return 0
  fi
  if [[ ! -d "$bin_dir" ]]; then
    echo "ecrawl_analyze: skipped (no capture; run the ecrawl check first)" | tee -a "$SUMMARY"
    return 0
  fi
  local want_bytes got_bytes want_files got_files
  want_bytes=$(du -sb "$TREE" 2>/dev/null | awk 'NR == 1 { print $1 }' || true)
  "$ECRAWL_ANALYZE_BIN" --subtree "$TREE" "$bin_dir" \
    >"$OUT/ecrawl_analyze.subtree.txt" 2>"$OUT/ecrawl_analyze.stderr.txt" || {
    echo "ecrawl_analyze: FAIL (nonzero exit)" | tee -a "$SUMMARY"
    FAIL=1
    return 0
  }
  got_bytes=$(sed -n 's/^bytes=//p' "$OUT/ecrawl_analyze.subtree.txt" | tail -1 || true)
  want_files=$(kv_get "$OUT/find.baseline.txt" files)
  got_files=$("$ECRAWL_ANALYZE_BIN" --subtree "$TREE" --type f --list "$bin_dir" 2>/dev/null | grep -c '.' || true)

  local ok=1
  if [[ -n "$want_bytes" && "$got_bytes" != "$want_bytes" ]]; then
    echo "ecrawl_analyze subtree bytes mismatch: got=$got_bytes want=$want_bytes (du -sb)" | tee -a "$SUMMARY"
    ok=0
  fi
  if [[ "$got_files" != "$want_files" ]]; then
    echo "ecrawl_analyze subtree files mismatch: got=$got_files want=$want_files (find -type f)" | tee -a "$SUMMARY"
    ok=0
  fi

  # Since ERCBIN08 the bare --subtree aggregate above may be answered from the
  # catalog rollups without reading a record. Which path ran is a property of the
  # tree, not the query, so the check above silently exercises only one of them.
  # Run the other explicitly and require they agree: that is the whole contract of
  # the fast path, and this is the only place it gets tested against a real tree.
  local answered exact_bytes
  answered=$(sed -n 's/^answered_from=//p' "$OUT/ecrawl_analyze.subtree.txt" | tail -1 || true)
  if "$ECRAWL_ANALYZE_BIN" --subtree "$TREE" --exact "$bin_dir" \
    >"$OUT/ecrawl_analyze.subtree.exact.txt" 2>"$OUT/ecrawl_analyze.exact.stderr.txt"; then
    exact_bytes=$(sed -n 's/^bytes=//p' "$OUT/ecrawl_analyze.subtree.exact.txt" | tail -1 || true)
    if [[ "$got_bytes" != "$exact_bytes" ]]; then
      echo "ecrawl_analyze rollup/scan disagree: ${answered:-?}=$got_bytes --exact=$exact_bytes" | tee -a "$SUMMARY"
      ok=0
    fi
  else
    echo "ecrawl_analyze: FAIL (--exact nonzero exit)" | tee -a "$SUMMARY"
    ok=0
  fi

  if [[ $ok -eq 1 ]]; then
    echo "ecrawl_analyze: OK bytes=$got_bytes (= du -sb, = --exact) files=$got_files (= find -type f)" \
      " [answered_from=${answered:-?}]" | tee -a "$SUMMARY"
  else
    FAIL=1
  fi
}

note_external() {
  local name=$1
  if tool_available "$name"; then
    echo "$name: installed — compare counts manually or extend this script (see README)" | tee -a "$SUMMARY"
  else
    echo "$name: skipped (not installed)" | tee -a "$SUMMARY"
  fi
}

# GUFI: index the tree into this results directory and ask its own wrappers, so
# the answer comes back through the same path the benchmark times.
check_gufi() {
  if ! tool_available gufi || ! have_cmd "${GUFI_DIR2INDEX:-}" || ! have_cmd "${GUFI_FIND:-}"; then
    echo "gufi: skipped (not installed)" | tee -a "$SUMMARY"
    return 0
  fi
  local idx="$OUT/gufi_index"
  rm -rf "$idx"
  mkdir -p "$idx"
  if ! "$GUFI_DIR2INDEX" -n "$THREADS" "$TREE" "$idx" \
    >"$OUT/gufi.dir2index.txt" 2>"$OUT/gufi.dir2index.err.txt"; then
    echo "gufi: FAIL (gufi_dir2index nonzero exit)" | tee -a "$SUMMARY"
    FAIL=1
    return 0
  fi
  # gufi_write_config points IndexRoot at this index. run_queries.sh rewrites it
  # for the index it builds later, so borrowing the file here is safe.
  local why rel
  if ! why=$(gufi_write_config "$idx"); then
    echo "gufi: skipped (no usable config: $why)" | tee -a "$SUMMARY"
    return 0
  fi
  rel=$(gufi_rel_path "$TREE" "$TREE")

  # gufi_du -s answers from treesummary rows, and only gufi_rollup writes them.
  # On a plain dir2index tree it warns, prints 0 and still exits 0, so the byte
  # check has to depend on the rollup having run rather than on the exit status.
  local rolled=0
  if have_cmd "${GUFI_ROLLUP:-}" &&
    "$GUFI_ROLLUP" -n "$THREADS" "$idx/${TREE##*/}" \
      >"$OUT/gufi.rollup.txt" 2>"$OUT/gufi.rollup.err.txt"; then
    rolled=1
  fi

  local files bytes ok=1
  files=$("$GUFI_FIND" "$rel" -type f 2>"$OUT/gufi.find.err.txt" | grep -c '.' || true)
  cmp_num gufi "files" "$files" "$REF_FILES" || ok=0
  if have_cmd "${GUFI_DU:-}" && ((rolled)); then
    bytes=$("$GUFI_DU" -s --apparent-size --block-size 1 "$rel" 2>"$OUT/gufi.du.err.txt" |
      awk 'NF && $1 ~ /^[0-9]+$/ { print $1; exit }' || true)
    cmp_num gufi "bytes" "$bytes" "$REF_LINK_BYTES" || ok=0
  elif have_cmd "${GUFI_DU:-}"; then
    echo "gufi: bytes not checked (gufi_rollup unavailable, so gufi_du has no treesummary)" |
      tee -a "$SUMMARY"
  fi
  if [[ $ok -eq 1 ]]; then
    echo "gufi: OK files=$files bytes=${bytes:-n/a} (= find -type f)" | tee -a "$SUMMARY"
  else
    FAIL=1
  fi
}

# xdu: same idea, and the one place the canonical-path trap gets caught. xdu
# resolves symlinks as it indexes, so a prefix built from the path we were given
# matches nothing when any component is a symlink.
check_xdu() {
  if ! tool_available xdu || ! have_cmd "${XDU_FIND:-}"; then
    echo "xdu: skipped (not installed)" | tee -a "$SUMMARY"
    return 0
  fi
  local idx="$OUT/xdu_index"
  rm -rf "$idx"
  mkdir -p "$idx"
  if ! "$XDU_BIN" "$TREE" -o "$idx" -j "$THREADS" "${XDU_SIZE_ARGS[@]}" \
    >"$OUT/xdu.index.txt" 2>"$OUT/xdu.index.err.txt"; then
    echo "xdu: FAIL (xdu nonzero exit)" | tee -a "$SUMMARY"
    FAIL=1
    return 0
  fi
  local rt pref count bytes ok=1
  rt=$(realpath -- "$TREE" 2>/dev/null) || rt=$TREE
  pref=$(printf '%s' "$rt" | sed 's/[.[\*^$()+?{|]/\\&/g')
  count=$("$XDU_FIND" -i "$idx" -p "^${pref}/" --count 2>"$OUT/xdu.count.err.txt" |
    grep -oE '[0-9][0-9,]*' | head -1 | tr -d ',' || true)
  cmp_num xdu "files" "$count" "$REF_FILES" || ok=0
  # Without --apparent-size the records hold st_blocks, so the total is disk
  # usage and comparing it to a file length would report a unit as a bug.
  if ((${#XDU_SIZE_ARGS[@]} > 0)); then
    "$XDU_FIND" -i "$idx" -p "^${pref}/" -f size \
      >"$OUT/xdu.sizes.txt" 2>"$OUT/xdu.sizes.err.txt" || true
    bytes=$(size_field_sum "$OUT/xdu.sizes.txt")
    cmp_num xdu "bytes" "$bytes" "$REF_LINK_BYTES" || ok=0
  else
    echo "xdu: bytes not checked ($XDU_SIZE_NOTE)" | tee -a "$SUMMARY"
  fi
  if [[ $ok -eq 1 ]]; then
    echo "xdu: OK files=$count bytes=${bytes:-not_checked} (= find -type f)" | tee -a "$SUMMARY"
  else
    FAIL=1
  fi
}

# dua walks rather than indexes, and deduplicates hard links the way du does, so
# du -sb is its reference and there is no count to check.
check_dua() {
  if ! tool_available dua; then
    echo "dua: skipped ($(dua_skip_reason))" | tee -a "$SUMMARY"
    return 0
  fi
  if [[ "${DUA_HAS_BYTES:-0}" != "1" ]]; then
    echo "dua: bytes not checked (this build has no --format bytes)" | tee -a "$SUMMARY"
    return 0
  fi
  local sentinel bytes
  sentinel=$(dua_sentinel "$OUT")
  if ! "$DUA_BIN" "${DUA_AGG_ARGS[@]}" "$TREE" "$sentinel" \
    >"$OUT/dua.out.txt" 2>"$OUT/dua.err.txt"; then
    echo "dua: FAIL (nonzero exit)" | tee -a "$SUMMARY"
    FAIL=1
    return 0
  fi
  bytes=$(dua_bytes_for_path "$OUT/dua.out.txt" "$TREE")
  if cmp_num dua "bytes" "$bytes" "$REF_DU_BYTES"; then
    echo "dua: OK bytes=$bytes (= du -sb)" | tee -a "$SUMMARY"
  else
    FAIL=1
  fi
}

for t in $CHECK_TOOLS; do
  case "$t" in
    find) echo "find: baseline written" | tee -a "$SUMMARY" ;;
    ecrawl) check_ecrawl ;;
    ecrawl_analyze) check_ecrawl_analyze ;;
    gufi) check_gufi ;;
    xdu) check_xdu ;;
    dua) check_dua ;;
    robinhood) note_external "$t" ;;
    # A name this fixture has no checker for is a gap in the fixture, not a
    # wrong answer from a tool, so it must not fail the run.
    *) echo "$t: not checked (no correctness check for this tool)" | tee -a "$SUMMARY" ;;
  esac
done

echo "results=$OUT"
if [[ $FAIL -ne 0 ]]; then
  echo "CORRECTNESS: FAIL" | tee -a "$SUMMARY"
  exit 1
fi
echo "CORRECTNESS: PASS" | tee -a "$SUMMARY"
exit 0
