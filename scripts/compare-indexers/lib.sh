#!/usr/bin/env bash
# Shared helpers for scripts/compare-indexers/*
# shellcheck disable=SC2034

set -euo pipefail

COMPARE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$COMPARE_DIR/../.." && pwd)

# The tool prefix is normally on a shared filesystem, so a run can pick up
# binaries built against a newer glibc than the machine now running them. Those
# resolve through PATH and then die on startup, one tool at a time; the libc
# difference is the single explanation for all of them. Compared on the libc
# version rather than the hostname deliberately: on a cluster every job lands on
# a different node of the same image, and a per-hostname warning would fire on
# every run until nobody read it.
if [[ -n "${INDEXER_COMPARE_BUILD_LIBC:-}" ]]; then
  _ic_libc=$(getconf GNU_LIBC_VERSION 2>/dev/null || echo unknown)
  if [[ "$_ic_libc" != "$INDEXER_COMPARE_BUILD_LIBC" ]]; then
    printf 'WARN: %s was built on %s against %s and is running on %s with %s; rerun init.sh here if a tool aborts on startup\n' \
      "${INDEXER_COMPARE_PREFIX:-the tool prefix}" \
      "${INDEXER_COMPARE_BUILD_HOST:-an unrecorded host}" "$INDEXER_COMPARE_BUILD_LIBC" \
      "$(hostname)" "$_ic_libc" >&2
  fi
  unset _ic_libc
fi

# Tool binaries (override via env). Missing tools are skipped with status=skipped.
ECRAWL_BIN=${ECRAWL_BIN:-$REPO_ROOT/ecrawl}
EREPORT_BIN=${EREPORT_BIN:-$REPO_ROOT/ereport}
EREPORT_INDEX_BIN=${EREPORT_INDEX_BIN:-$REPO_ROOT/ereport_index}
ECRAWL_ANALYZE_BIN=${ECRAWL_ANALYZE_BIN:-$REPO_ROOT/ecrawl_analyze}

# External indexers — set when installed (paper-ish names; actual PATH may vary).
GUFI_DIR2INDEX=${GUFI_DIR2INDEX:-$(command -v gufi_dir2index 2>/dev/null || true)}
GUFI_FIND=${GUFI_FIND:-$(command -v gufi_find 2>/dev/null || true)}
GUFI_DU=${GUFI_DU:-$(command -v gufi_du 2>/dev/null || true)}
GUFI_ROLLUP=${GUFI_ROLLUP:-$(command -v gufi_rollup 2>/dev/null || true)}
GUFI_QUERY=${GUFI_QUERY:-$(command -v gufi_query 2>/dev/null || true)}

XDU_BIN=${XDU_BIN:-$(command -v xdu 2>/dev/null || true)}
XDU_FIND=${XDU_FIND:-$(command -v xdu-find 2>/dev/null || true)}

# xdu records st_blocks unless told otherwise, and every query then answers in
# disk usage while the rest of the comparison answers in file length. Ask for
# apparent size at index time when the build has the flag, and say so in the row
# when it does not, because that difference is not a rounding error on a tree
# with sparse files.
XDU_SIZE_ARGS=()
XDU_SIZE_NOTE="sizes are st_blocks (build has no --apparent-size)"
if [[ -n "$XDU_BIN" && -x "$XDU_BIN" ]]; then
  _xdu_help=$("$XDU_BIN" --help 2>&1 || true)
  case "$_xdu_help" in
    *--apparent-size*)
      XDU_SIZE_ARGS=(--apparent-size)
      XDU_SIZE_NOTE="apparent_size"
      ;;
  esac
  unset _xdu_help
fi

# Sum the leading size field of `xdu-find -f size` output (`<size>\t<path>`).
# Integers only: if a build prints formatted sizes instead, this yields nothing
# and the caller reports that, rather than letting awk sum the strings to zero
# and pass it off as an answer.
size_field_sum() {
  python3 - "$1" <<'PY'
import sys

total = 0
seen = 0
try:
    with open(sys.argv[1]) as handle:
        for line in handle:
            field = line.split("\t", 1)[0].strip()
            if not field:
                continue
            try:
                total += int(field)
            except ValueError:
                continue
            seen += 1
except OSError:
    seen = 0
print(total if seen else "")
PY
}

THREADS=${THREADS:-${ECRAWL_CRAWL_THREADS:-16}}

# One thread budget for every tool that can be told what to use. Comparing
# tools at their default fan-out compares packaging decisions, not indexers:
# ecrawl ships 16 crawl + 8 stat + 8 writer threads, fd and dua default to
# every logical processor (448 on this class of node), and Robinhood defaults
# to 2 scan threads. THREADS is the *total* per tool, so a tool that runs
# several pools at once splits it. An explicitly set per-tool variable wins,
# and is reported as-is.
#
# ecrawl's three pools run concurrently, so keep its stock 2:1:1 shape
# (crawl:writer:stat) and scale that into the budget.
_t_crawl=$((THREADS / 2))
((_t_crawl >= 1)) || _t_crawl=1
_t_writer=$((THREADS / 4))
((_t_writer >= 1)) || _t_writer=1
_t_stat=$((THREADS - _t_crawl - _t_writer))
((_t_stat >= 0)) || _t_stat=0
export ECRAWL_CRAWL_THREADS=${ECRAWL_CRAWL_THREADS:-$_t_crawl}
export ECRAWL_STAT_THREADS=${ECRAWL_STAT_THREADS:-$_t_stat}
export ECRAWL_WRITER_THREADS=${ECRAWL_WRITER_THREADS:-$_t_writer}
# --no-stat runs neither the stat pool nor the writer pool, so the whole budget goes to
# crawl threads. Leaving it on the 2:1:1 split would hand that row half the parallelism of
# the fd walk it is meant to be compared against, measuring the split rather than the walk.
export ECRAWL_NOSTAT_CRAWL_THREADS=${ECRAWL_NOSTAT_CRAWL_THREADS:-$THREADS}

# ereport_index --make runs parse workers and trigram writers at the same time,
# so the two pools share the budget.
_t_half=$((THREADS / 2))
((_t_half >= 1)) || _t_half=1
_t_trigram=$((THREADS - _t_half))
((_t_trigram >= 1)) || _t_trigram=1
export EREPORT_INDEX_THREADS=${EREPORT_INDEX_THREADS:-$_t_half}
export EREPORT_INDEX_TRIGRAM_THREADS=${EREPORT_INDEX_TRIGRAM_THREADS:-$_t_trigram}
# ereport and ecrawl_analyze queries each run one pool.
export EREPORT_THREADS=${EREPORT_THREADS:-$THREADS}
export ECRAWL_ANALYZE_THREADS=${ECRAWL_ANALYZE_THREADS:-$THREADS}
unset _t_crawl _t_writer _t_stat _t_half _t_trigram

# gufi_dir2index and gufi_rollup take a thread count on the command line, but
# the spelling has moved between releases, so probe --help rather than assume.
# Anchored on whitespace: a bare substring match would find "-n" in prose and
# pass a flag the tool rejects, turning a thread-pinning nicety into a failed
# row. Prints one argument per line, or nothing when the tool cannot be pinned.
# The Python wrappers (gufi_find, gufi_du) have no such flag; their thread count
# comes from the config written by gufi_write_config.
gufi_thread_args() {
  local bin=${1:-}
  [[ -n "$bin" ]] || return 0
  command -v "$bin" >/dev/null 2>&1 || return 0
  local help
  help=$("$bin" --help 2>&1 || true)
  if printf '%s' "$help" | grep -qE -- '(^|[[:space:]])--threads([[:space:],=]|$)'; then
    printf -- '--threads\n%s\n' "$THREADS"
  elif printf '%s' "$help" | grep -qE -- '(^|[[:space:]])-n[[:space:],]'; then
    printf -- '-n\n%s\n' "$THREADS"
  fi
}

# Where gufi_find and gufi_du look for their configuration. The path is compiled
# into them, so ask the installed gufi_config module instead of guessing; the
# fallbacks are the harness's own build and upstream's default.
gufi_config_path() {
  local from_module
  from_module=$(python3 -c 'import gufi_config; print(gufi_config.PATH)' 2>/dev/null || true)
  if [[ -n "$from_module" ]]; then
    printf '%s\n' "$from_module"
    return 0
  fi
  if [[ -n "${GUFI_CONFIG:-}" ]]; then
    printf '%s\n' "$GUFI_CONFIG"
    return 0
  fi
  printf '/etc/GUFI/config\n'
}

# GUFI resolves every path argument against IndexRoot and refuses anything
# outside it, and takes its thread count from the same file. Both are properties
# of the run, not of the install, so write the config once the index exists.
# Prints nothing on success; prints why on failure.
gufi_write_config() {
  local index_root=$1 cfg bin_dir
  cfg=$(gufi_config_path)
  bin_dir=$(dirname "${GUFI_QUERY:-${GUFI_FIND:-/usr/bin/gufi_query}}")
  mkdir -p "$(dirname "$cfg")" 2>/dev/null || {
    printf 'cannot create %s' "$(dirname "$cfg")"
    return 1
  }
  if ! printf '%s\n' \
    '# Written by scripts/compare-indexers for one benchmark run.' \
    "Threads=$THREADS" \
    "Query=${GUFI_QUERY:-$bin_dir/gufi_query}" \
    "Sqlite3=${GUFI_SQLITE3:-$bin_dir/gufi_sqlite3}" \
    "Stat=${GUFI_STAT_BIN:-$bin_dir/gufi_stat_bin}" \
    "IndexRoot=$index_root" \
    'OutputBuffer=4096' >"$cfg" 2>/dev/null; then
    printf 'cannot write %s' "$cfg"
    return 1
  fi
  return 0
}

# gufi_dir2index <tree> <dest> builds the replica at <dest>/<basename tree>, and
# the wrappers prepend IndexRoot to whatever path they are given. So a path in
# the live tree has to be re-expressed relative to the index before GUFI will
# look at it.
gufi_rel_path() {
  local abs=${1%/} tree=${2%/}
  local base=${tree##*/}
  if [[ "$abs" == "$tree" ]]; then
    printf '%s\n' "$base"
  elif [[ "$abs" == "$tree"/* ]]; then
    printf '%s/%s\n' "$base" "${abs#"$tree"/}"
  else
    # Not under the indexed tree: GUFI would silently drop it, so say so.
    return 1
  fi
}

# Reported in the summary so an unpinnable tool is visible rather than a silent
# asymmetry in the numbers.
thread_plan() {
  printf 'budget=%s ecrawl=%s+%s+%s(crawl+stat+writer) ecrawl_nostat=%s(crawl) ereport_index=%s+%s(parse+trigram) ereport=%s ecrawl_analyze=%s' \
    "$THREADS" "$ECRAWL_CRAWL_THREADS" "$ECRAWL_STAT_THREADS" "$ECRAWL_WRITER_THREADS" \
    "$ECRAWL_NOSTAT_CRAWL_THREADS" \
    "$EREPORT_INDEX_THREADS" "$EREPORT_INDEX_TRIGRAM_THREADS" "$EREPORT_THREADS" "$ECRAWL_ANALYZE_THREADS"
}

# find -name matches a glob against the basename; the trigram index matches a
# literal substring anywhere in the path. Pairing them gives find's exact answer
# from the index: search the longest literal run in the glob (the smallest
# candidate set the index can produce for it), then let a basename-anchored
# regex discard the rest. Prints the search term on line 1 and the regex on
# line 2. Character classes are not translated, so a glob containing [ ] is
# reported with an empty term and left to the caller to skip.
glob_index_filter() {
  python3 - "$1" <<'PY'
import re, sys

glob = sys.argv[1]
if "[" in glob or "]" in glob:
    print("")
    print("")
    raise SystemExit(0)
runs = [r for r in re.split(r"[*?]", glob) if r]
print(max(runs, key=len) if runs else "")
out = []
for ch in glob:
    if ch == "*":
        out.append("[^/]*")
    elif ch == "?":
        out.append("[^/]")
    elif ch in ".^$+{}()|\\[]/":
        out.append("\\" + ch)
    else:
        out.append(ch)
print("/" + "".join(out) + "$")
PY
}

# Anchor an exact basename for the same index-then-filter pairing (Q1).
name_basename_ere() {
  python3 - "$1" <<'PY'
import re, sys
print("/" + re.escape(sys.argv[1]) + "$")
PY
}

# Every tool here opens far more files than the stock 1024: ereport_index keeps
# trigram_workers x LRU shards open, GUFI opens a database per directory, and
# ecrawl holds a per-uid shard set per writer. Hitting the limit shows up as a
# confusing mid-run EMFILE rather than as a resource problem, so raise it once,
# up front.
#
# Call this directly, never as $(raise_nofile): a command substitution runs in a
# subshell, where the new limit dies with it. The outcome lands in NOFILE_NOW.
NOFILE_TARGET=${NOFILE_TARGET:-131072}
NOFILE_NOW=""
raise_nofile() {
  local want=${1:-$NOFILE_TARGET} soft hard
  soft=$(ulimit -Sn)
  hard=$(ulimit -Hn)
  if [[ "$soft" == unlimited ]]; then
    NOFILE_NOW="open files unlimited"
    return 0
  fi
  # Only root can raise the hard limit; everyone else is capped by it.
  if [[ "$hard" != unlimited ]] && ((hard < want)); then
    ulimit -Hn "$want" 2>/dev/null || true
    hard=$(ulimit -Hn)
  fi
  local target=$want
  [[ "$hard" == unlimited ]] || ((target <= hard)) || target=$hard
  ((target <= soft)) || ulimit -Sn "$target" 2>/dev/null || true
  soft=$(ulimit -Sn)
  if ((soft < want)); then
    NOFILE_NOW="open files $soft (wanted $want; hard limit $hard -- run as root or raise limits.conf)"
  else
    NOFILE_NOW="open files $soft"
  fi
}

# First line of a failed probe that names the usual cause (a binary built
# against a newer glibc), or empty when stderr has nothing useful.
_probe_loader_hint() {
  local bin=$1 err_file=$2
  local line=""
  line=$(grep -E -m1 'GLIBC_|version `|not found|No such file|cannot open shared object' \
    "$err_file" 2>/dev/null | tr -d '\r' | cut -c1-200 || true)
  if [[ -z "$line" ]] && command -v ldd >/dev/null 2>&1; then
    line=$(ldd "$bin" 2>&1 | grep -E -m1 'not found|GLIBC_' | tr -d '\r' | cut -c1-200 || true)
  fi
  printf '%s' "$line"
}

# Why a path that the harness resolved will not run. A failed -x answers "no" to
# four different questions and each wants a different fix, so name the one that
# applied: "not executable" alone sent a reader looking for a permission bit on
# a dua that was simply no longer there.
_unexecutable_why() {
  local bin=$1 fix=$2
  if [[ -d "$bin" ]]; then
    printf 'is a directory, not a binary: %s' "$bin"
  elif [[ ! -e "$bin" ]]; then
    # Also how an unsearchable parent directory looks from here. Either way the
    # path came from an env.sh written when the binary did resolve, so the
    # prefix has changed under it since.
    printf 'no such file: %s (%s)' "$bin" "$fix"
  elif [[ ! -r "$bin" ]]; then
    printf 'not readable as %s: %s' "$(id -un 2>/dev/null || echo '?')" "$bin"
  else
    printf 'no execute permission: %s (chmod +x it, or %s)' "$bin" "$fix"
  fi
}

# How each baseline is repaired. Named once so the skip note in the CSV and the
# warning benchmark.sh prints before the run give the same instruction.
FD_FIX_HINT="install the fd-find package"
DUA_FIX_HINT="rebuild it with 'TOOLS=dua FORCE_REINSTALL=1 scripts/compare-indexers/init.sh'"

# Prefer a pinned path only while it still exists and is executable; otherwise
# rediscover on PATH. ${VAR:-default} keeps a stale env.sh pin forever even when
# a package binary is sitting at /usr/bin, which is how dua vanished from charts.
_resolve_or_find() {
  local pinned=${1:-}
  shift
  local name
  if [[ -n "$pinned" && -x "$pinned" ]]; then
    printf '%s' "$pinned"
    return 0
  fi
  for name in "$@"; do
    if command -v "$name" >/dev/null 2>&1; then
      command -v "$name"
      return 0
    fi
  done
  # Keep a dead pin so skip notes can still name the path that failed.
  printf '%s' "$pinned"
  return 1
}

# Apply dua aggregate --help feature flags to DUA_AGG_ARGS / DUA_HAS_BYTES.
_dua_apply_help_flags() {
  local help=$1
  case "$help" in
    *--apparent-size*) DUA_AGG_ARGS+=(--apparent-size) ;;
  esac
  case "$help" in
    *--stay-on-filesystem*) DUA_AGG_ARGS+=(--stay-on-filesystem) ;;
  esac
  case "$help" in
    *--format*)
      DUA_AGG_ARGS+=(--format bytes)
      DUA_HAS_BYTES=1
      ;;
  esac
  case "$help" in
    *--no-sort*) DUA_AGG_ARGS+=(--no-sort) ;;
  esac
  case "$help" in
    *--no-total*) DUA_AGG_ARGS+=(--no-total) ;;
  esac
  case "$help" in
    *--threads*) DUA_AGG_ARGS+=(--threads "$THREADS") ;;
  esac
}

# fd is the "fast find" baseline test.sh already prefers over find(1).
# Debian/Ubuntu install the fd-find package as `fdfind`.
FD_BIN=$(_resolve_or_find "${FD_BIN:-}" fd fdfind || true)
# Match test.sh: fd skips hidden files and obeys ignore files unless told not to.
FD_COMMON_ARGS=(--hidden --no-ignore)
FD_HAS_SIZE=0
FD_OK=0
FD_WHY=""
if [[ -n "$FD_BIN" ]]; then
  # Capture --help instead of piping to grep: with pipefail a SIGPIPE'd fd would
  # invert the test result. Require a successful probe the same way dua does —
  # a binary that resolves but dies on a newer glibc must not look "available".
  _fd_err=$(mktemp 2>/dev/null || echo /tmp/ic-fd-probe.$$)
  _fd_st=0
  _fd_help=""
  _fd_candidates=("$FD_BIN")
  _fd_alt=""
  for _fd_name in fd fdfind; do
    _fd_alt=$(command -v "$_fd_name" 2>/dev/null || true)
    [[ -n "$_fd_alt" && "$_fd_alt" != "$FD_BIN" ]] && _fd_candidates+=("$_fd_alt")
  done
  for _fd_cand in "${_fd_candidates[@]}"; do
    [[ -x "$_fd_cand" ]] || continue
    _fd_help=$("$_fd_cand" --help 2>"$_fd_err") || _fd_st=$?
    if [[ -n "$_fd_help" ]]; then
      FD_BIN=$_fd_cand
      FD_OK=1
      break
    fi
  done
  if [[ "$FD_OK" == "1" ]]; then
    case "$_fd_help" in
      # Keep parity with the harness's `find -xdev` when the option exists.
      *--one-file-system*) FD_COMMON_ARGS+=(--one-file-system) ;;
    esac
    case "$_fd_help" in
      *--size*) FD_HAS_SIZE=1 ;;
    esac
    case "$_fd_help" in
      # Otherwise fd defaults to every logical processor while ecrawl, GUFI and
      # XDU are held to THREADS, which would flatter the fd baseline.
      *--threads*) FD_COMMON_ARGS+=(--threads "$THREADS") ;;
    esac
  else
    if [[ -x "$FD_BIN" ]]; then
      FD_WHY=$(grep -m1 '.' "$_fd_err" 2>/dev/null | tr -d '\r' | cut -c1-200 || true)
      if [[ -z "$FD_WHY" ]]; then
        FD_WHY=$(_probe_loader_hint "$FD_BIN" "$_fd_err")
      fi
      [[ -n "$FD_WHY" ]] || FD_WHY="--help exited ${_fd_st} with empty stdout/stderr"
    else
      FD_WHY=$(_unexecutable_why "$FD_BIN" "$FD_FIX_HINT")
    fi
  fi
  rm -f "$_fd_err"
  unset _fd_err _fd_help _fd_st _fd_candidates _fd_alt _fd_name _fd_cand
fi

fd_skip_reason() {
  if [[ -z "${FD_BIN:-}" ]]; then
    printf 'fd_not_found_install_fd-find'
  elif [[ -n "$FD_WHY" ]]; then
    printf 'fd_not_runnable: %s' "$FD_WHY"
  else
    printf 'fd_not_runnable'
  fi
}

# dua (dua-cli) is the Rust du: the parallel "fast du" partner to fd.
DUA_BIN=$(_resolve_or_find "${DUA_BIN:-}" dua || true)
DUA_AGG_ARGS=(aggregate)
DUA_OK=0
DUA_HAS_BYTES=0
# Why it is not usable, in its own words. "present but not runnable" sent the
# last run looking for a bug in the harness when the binary was simply built
# somewhere else and says so on its first line of stderr.
DUA_WHY=""
if [[ -n "$DUA_BIN" ]]; then
  # Probe the resolved path first, then any other dua on PATH. A stale env.sh
  # pin used to win over /usr/bin/dua forever; wrong-glibc prefix builds need
  # the same escape hatch.
  _dua_err=$(mktemp 2>/dev/null || echo /tmp/ic-dua-probe.$$)
  _dua_st=0
  _dua_help=""
  _dua_path=$(command -v dua 2>/dev/null || true)
  _dua_candidates=("$DUA_BIN")
  [[ -n "$_dua_path" && "$_dua_path" != "$DUA_BIN" ]] && _dua_candidates+=("$_dua_path")
  for _dua_cand in "${_dua_candidates[@]}"; do
    [[ -x "$_dua_cand" ]] || continue
    _dua_help=$("$_dua_cand" aggregate --help 2>"$_dua_err") || _dua_st=$?
    if [[ -n "$_dua_help" ]]; then
      DUA_BIN=$_dua_cand
      DUA_OK=1
      break
    fi
  done
  if [[ "$DUA_OK" == "1" ]]; then
    _dua_apply_help_flags "$_dua_help"
  else
    if [[ -x "$DUA_BIN" ]]; then
      DUA_WHY=$(grep -m1 '.' "$_dua_err" 2>/dev/null | tr -d '\r' | cut -c1-200 || true)
      if [[ -z "$DUA_WHY" ]]; then
        DUA_WHY=$(_probe_loader_hint "$DUA_BIN" "$_dua_err")
      fi
      [[ -n "$DUA_WHY" ]] || \
        DUA_WHY="aggregate --help exited ${_dua_st} with empty stdout/stderr"
    else
      DUA_WHY=$(_unexecutable_why "$DUA_BIN" "$DUA_FIX_HINT")
    fi
  fi
  rm -f "$_dua_err"
  unset _dua_err _dua_help _dua_st _dua_path _dua_candidates _dua_cand
fi

# The skipped rows say why in the tool's own words, so a prefix built on another
# node reads as that and not as a missing install.
dua_skip_reason() {
  if [[ -z "${DUA_BIN:-}" ]]; then
    printf 'dua_not_found_install_dua-cli'
  elif [[ -n "$DUA_WHY" ]]; then
    printf 'dua_not_runnable: %s' "$DUA_WHY"
  else
    printf 'dua_not_runnable'
  fi
}

# True when the harness has a path for a baseline tool, whether or not the
# probe succeeded. run_smoke uses this so a present-but-broken dua/fd still
# lands in TOOLS and emits skipped CSV rows instead of vanishing.
baseline_candidate() {
  case "$1" in
    fd) [[ -n "${FD_BIN:-}" ]] ;;
    dua) [[ -n "${DUA_BIN:-}" ]] ;;
    *) tool_available "$1" ;;
  esac
}

# One line per baseline that resolves to a path but will not run, empty when
# both are healthy. Callers print this before the measured work: found here a
# dead baseline costs a chmod, found in the results it costs the whole run.
#
# Re-probed rather than read from FD_OK/DUA_OK, because those were computed when
# lib.sh was sourced and benchmark.sh sources $PREFIX/env.sh afterwards -- and
# that is what actually decides FD_BIN and DUA_BIN for the run.
baseline_health_report() {
  local tool bin fix why probe
  for tool in fd dua; do
    case "$tool" in
      fd) bin=${FD_BIN:-} fix=$FD_FIX_HINT probe=(--help) ;;
      dua) bin=${DUA_BIN:-} fix=$DUA_FIX_HINT probe=(aggregate --help) ;;
    esac
    # No path at all is a different story, and the skipped rows already tell it.
    [[ -n "$bin" ]] || continue
    why=""
    if [[ ! -x "$bin" ]]; then
      why=$(_unexecutable_why "$bin" "$fix")
    elif ! "$bin" "${probe[@]}" >/dev/null 2>&1; then
      why="$(basename "$bin") ${probe[*]} failed; $fix"
    fi
    [[ -z "$why" ]] || printf '%s is unusable: %s\n' "$tool" "$why"
  done
}

# `dua aggregate <one-path>` prints a line per child of that path rather than
# the subtree total. Passing an empty sentinel as a second input switches dua to
# per-input totals, which then match `du -sb` exactly.
dua_sentinel() {
  local dir="$1/.dua-sentinel"
  mkdir -p "$dir"
  printf '%s\n' "$dir"
}

# dua colours its output even through a pipe, so strip escapes before parsing.
# Match on the line's path suffix instead of a field index to survive spaces.
dua_bytes_for_path() {
  local out=$1 path=$2
  [[ -f "$out" ]] || {
    echo 0
    return
  }
  local n
  n=$(awk -v esc="$(printf '\033')" -v suffix=" $path" '
    { gsub(esc "\\[[0-9;]*m", "") }
    substr($0, length($0) - length(suffix) + 1) == suffix { print $1; exit }
  ' "$out" 2>/dev/null || true)
  echo "${n:-0}"
}

# Robinhood: site-specific; common entry points vary by build/backend.
RBH_FIND=${RBH_FIND:-$(command -v rbh-find 2>/dev/null || true)}
RBH_DU=${RBH_DU:-$(command -v rbh-du 2>/dev/null || true)}
RBH_SCAN=${RBH_SCAN:-$(command -v rbh-scan 2>/dev/null || command -v robinhood 2>/dev/null || true)}

# One setting out of a Robinhood config, whichever block it lives in.
rbh_conf_value() {
  local key=$1 file=$2
  [[ -f "$file" ]] || return 1
  sed -n "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*\"\\?\\([^\";]*\\)\"\\?[[:space:]]*;.*/\\1/p" \
    "$file" | sed -n '1p'
}

mysql_client_bin() {
  command -v mariadb 2>/dev/null || command -v mysql 2>/dev/null || true
}

# One statement against Robinhood's own database, with the credentials from its
# config rather than the admin socket, so this works without root.
rbh_sql() {
  local sql=$1 cfg=${RBH_CONFIG:-} cli db user pwfile pw
  [[ -n "$cfg" && -f "$cfg" ]] || return 1
  cli=$(mysql_client_bin)
  [[ -n "$cli" ]] || return 1
  db=$(rbh_conf_value db "$cfg" || true)
  user=$(rbh_conf_value user "$cfg" || true)
  pwfile=$(rbh_conf_value password_file "$cfg" || true)
  [[ -n "$db" && -n "$user" && -n "$pwfile" && -r "$pwfile" ]] || return 1
  pw=$(tr -d '[:space:]' <"$pwfile")
  MYSQL_PWD="$pw" "$cli" --protocol=socket -u "$user" "$db" \
    --batch --skip-column-names -e "$sql" 2>/dev/null
}

# Robinhood needs more than its binaries: a generated config, and a database
# that answers. Probe with the credentials in the config rather than the admin
# socket, so this works without root. Prints why it is not usable, or nothing
# when it is; every caller records that one line instead of letting each query
# rediscover it as a connection error.
rbh_db_unready_reason() {
  local cfg=${RBH_CONFIG:-}
  if [[ -z "$cfg" || ! -f "$cfg" ]]; then
    printf 'no_robinhood_config_run_mariadb.sh_setup'
    return 0
  fi
  local cli
  cli=$(mysql_client_bin)
  if [[ -z "$cli" ]]; then
    printf 'no_mysql_client_installed'
    return 0
  fi
  local db user pwfile pw
  db=$(rbh_conf_value db "$cfg" || true)
  user=$(rbh_conf_value user "$cfg" || true)
  pwfile=$(rbh_conf_value password_file "$cfg" || true)
  if [[ -z "$db" || -z "$user" || -z "$pwfile" || ! -r "$pwfile" ]]; then
    printf 'robinhood_config_has_no_readable_mysql_credentials'
    return 0
  fi
  pw=$(tr -d '[:space:]' <"$pwfile")
  if ! MYSQL_PWD="$pw" "$cli" --protocol=socket -u "$user" "$db" \
    --batch --skip-column-names -e 'SELECT 1;' >/dev/null 2>&1; then
    printf 'mariadb_unreachable_or_credentials_rejected'
    return 0
  fi
  return 0
}

RBH_SCHEMA_STATE=""
RBH_SCHEMA_REASON=""
# A reachable database is not a queryable one. Robinhood creates its tables on
# its first --alter-db scan, so until that has happened rbh-find and rbh-du do
# not return an empty answer, they fail on a missing ENTRIES table. Queries and
# the probe pass ask this; the index step deliberately does not, because a scan
# is exactly what is allowed to find no schema and build one.
rbh_schema_ready() {
  if ! rbh_db_ready; then
    RBH_SCHEMA_REASON=$RBH_READY_REASON
    return 1
  fi
  if [[ -z "$RBH_SCHEMA_STATE" ]]; then
    local n
    n=$(rbh_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'ENTRIES';" || true)
    if [[ "$n" == "1" ]]; then
      RBH_SCHEMA_STATE=1
      RBH_SCHEMA_REASON=""
    else
      RBH_SCHEMA_STATE=0
      RBH_SCHEMA_REASON="database_has_no_schema_the_scan_step_did_not_run"
    fi
  fi
  [[ "$RBH_SCHEMA_STATE" == "1" ]]
}

# find's `-size +Nc` means bytes; Robinhood's parser rejects the c suffix
# outright ("Expected size format: [-|+]<val>[K|M|G|T]") and takes a bare byte
# count instead. Deliberately no K/M/G/T here: whether Robinhood reads those as
# 1024- or 1000-based decides whether the threshold is the same question find
# was asked, and a bare count cannot be misread.
rbh_size_arg() {
  printf '+%s\n' "$1"
}

# rbh-du's unit flag is not spelled the same across builds, and picking wrong
# turns a byte total into a block count that the summary can only flag as a
# disagreement. Ask the binary, and say in the row which form was used.
# Prints the flags, if any, on line 1 and the note on line 2.
rbh_du_args() {
  local help line=""
  help=$("${RBH_DU:-false}" --help 2>&1 || true)
  # The line documenting -b, whatever it says.
  # No match is an answer, not an error: absorb grep's exit 1 so pipefail does
  # not return from the function before it has printed anything.
  line=$(printf '%s\n' "$help" | grep -E -- '(^|[[:space:]])-b([[:space:],]|$)' | head -1 || true)
  if [[ "$line" == *[Bb]yte* ]]; then
    printf -- '-b\n'
    printf 'rbh-du_-s_-b_bytes\n'
  elif [[ -n "$line" ]]; then
    # It exists but means something else (blocks, backend); leave it off and
    # report the default unit so a mismatch is attributable.
    printf -- '\n'
    printf 'rbh-du_-s_default_unit(-b_is_not_bytes_here)\n'
  else
    printf -- '\n'
    printf 'rbh-du_-s_default_unit\n'
  fi
}

RBH_READY_STATE=""
RBH_READY_REASON=""
rbh_db_ready() {
  # A server that died during the run outranks anything probed before it.
  if [[ "${DB_DOWN:-0}" == "1" ]]; then
    RBH_READY_REASON="mariadb_down_after_a_failed_restart"
    return 1
  fi
  if [[ -z "$RBH_READY_STATE" ]]; then
    RBH_READY_REASON=$(rbh_db_unready_reason)
    if [[ -n "$RBH_READY_REASON" ]]; then
      RBH_READY_STATE=0
    else
      RBH_READY_STATE=1
    fi
  fi
  [[ "$RBH_READY_STATE" == "1" ]]
}

DROP_CACHES=${DROP_CACHES:-0}
# all: every timed command starts cold. first-rep: only rep 1 is cold, so later
# reps show warm/steady-state latency the way the paper's query series does.
DROP_CACHES_SCOPE=${DROP_CACHES_SCOPE:-all}
# Page cache alone does not empty MariaDB's buffer pool, which would leave
# Robinhood warm while every other tool is cold.
DROP_DB_CACHE=${DROP_DB_CACHE:-0}
REPS=${REPS:-3}
SIZE_GT_BYTES=${SIZE_GT_BYTES:-$((500 * 1024 * 1024))}

# Repetitions are per tool, because their costs are not comparable: a GUFI
# rollup is 29 minutes a repetition on a 4.4M-entry tree while `find` is
# seconds, so a run that wants error bars on the cheap rows should not have to
# pay for three rollups to get them. REPS is the default and REPS_<TOOL>
# overrides it, spelled in upper case with '-' as '_': REPS_GUFI=1.
#
# Every name that can appear in TOOLS for either phase. Kept explicit so a typo
# is rejected rather than silently ignored, which is the whole failure mode a
# per-tool knob invites. Deliberately not spelled REPS_*, or the check below
# would read this list as a tool of its own.
KNOWN_REPS_TOOLS="ecrawl ecrawl_suite suite ereport_index gufi xdu robinhood find fd du dua"

# ereport_index used to be pinned to rep 1 by a condition in run_index.sh. That
# default is worth keeping -- it builds from ecrawl's capture, so repeating it
# measures the same input twice -- but it is a default now, not a rule.
REPS_EREPORT_INDEX=${REPS_EREPORT_INDEX:-1}

reps_var_name() {
  printf 'REPS_%s' "$(printf '%s' "$1" | tr '[:lower:]-' '[:upper:]_')"
}

# Checked once, here, rather than inside tool_reps: every caller of that runs it
# in a command substitution, where an exit would end the subshell and leave the
# run going with an empty count.
_reps_check_all() {
  local var value tool known ok
  [[ "$REPS" =~ ^[0-9]+$ ]] && ((REPS >= 1)) ||
    { echo "ERROR: REPS must be a whole number of at least 1, not '$REPS'" >&2; exit 1; }
  for var in $(compgen -v | grep '^REPS_' || true); do
    value=${!var}
    [[ -n "$value" ]] || continue
    tool=$(printf '%s' "${var#REPS_}" | tr '[:upper:]' '[:lower:]')
    ok=0
    for known in $KNOWN_REPS_TOOLS; do
      [[ "$tool" == "$known" ]] && ok=1 && break
    done
    if ((!ok)); then
      echo "ERROR: $var names no tool this harness runs." >&2
      echo "       Known tools: $KNOWN_REPS_TOOLS" >&2
      exit 1
    fi
    [[ "$value" =~ ^[0-9]+$ ]] && ((value >= 1)) ||
      { echo "ERROR: $var must be a whole number of at least 1, not '$value'" >&2; exit 1; }
  done
}
_reps_check_all

# How many times this tool runs: REPS unless REPS_<TOOL> says otherwise.
tool_reps() {
  local var value
  var=$(reps_var_name "$1")
  value=${!var:-}
  printf '%s' "${value:-$REPS}"
}

# The outer loop bound: tools are still visited rep-major so that the cache
# state each one sees is the same as it was before per-tool counts existed.
max_tool_reps() {
  local t n max=0
  for t in "$@"; do
    n=$(tool_reps "$t")
    ((n > max)) && max=$n
  done
  ((max > 0)) || max=$REPS
  printf '%s' "$max"
}

# "3" when every tool agrees, otherwise "3 (gufi 1, robinhood 1)". Used for the
# progress banner and for env.txt, so a results directory says what it did
# rather than implying one count for rows that do not share it.
reps_plan() {
  local t n diff=()
  for t in "$@"; do
    n=$(tool_reps "$t")
    [[ "$n" == "$REPS" ]] || diff+=("$t $n")
  done
  if ((${#diff[@]} == 0)); then
    printf '%s' "$REPS"
    return 0
  fi
  local joined=${diff[0]} i
  for ((i = 1; i < ${#diff[@]}; i++)); do
    joined+=", ${diff[i]}"
  done
  printf '%s (%s)' "$REPS" "$joined"
}

have_cmd() {
  local c=$1
  [[ -n "$c" && -x "$c" ]]
}

# find/du exit nonzero on unreadable dirs and on entries that vanish mid-walk.
# Under `set -o pipefail` that status would abort the caller, so absorb it.
count_find() {
  local n
  n=$({ find "$@" 2>/dev/null || true; } | wc -l)
  echo "${n:-0}"
}

tool_available() {
  case "$1" in
    ecrawl) have_cmd "$ECRAWL_BIN" ;;
    ereport) have_cmd "$EREPORT_BIN" ;;
    ereport_index) have_cmd "$EREPORT_INDEX_BIN" ;;
    ecrawl_analyze) have_cmd "$ECRAWL_ANALYZE_BIN" ;;
    gufi) have_cmd "$GUFI_DIR2INDEX" ;;
    gufi_find) have_cmd "$GUFI_FIND" ;;
    gufi_du) have_cmd "$GUFI_DU" ;;
    xdu) have_cmd "$XDU_BIN" && have_cmd "$XDU_FIND" ;;
    robinhood) have_cmd "$RBH_SCAN" || have_cmd "$RBH_FIND" ;;
    find) command -v find >/dev/null 2>&1 ;;
    fd) [[ "$FD_OK" == "1" ]] ;;
    du) command -v du >/dev/null 2>&1 ;;
    dua) [[ "$DUA_OK" == "1" ]] ;;
    *) return 1 ;;
  esac
}

# Harness diagnostics belong to the run, not to the tool that happened to be
# next: time_cmd redirects the tool's stderr to its own file, so anything
# printed from inside it was being filed as that tool's error and quoted in
# FAILURES.txt under its name.
HARNESS_LOG=${HARNESS_LOG:-}
# The script's own stderr, kept aside before any redirection, so a warning
# raised while a tool's stderr is captured still reaches the run's console.
exec 9>&2
harness_warn() {
  local line="WARN: $*"
  if [[ -n "$HARNESS_LOG" ]]; then
    printf '%s %s\n' "$(date +%H:%M:%S)" "$line" >>"$HARNESS_LOG"
  fi
  printf '    %s\n' "$line" >&9
}

# Set once the server could not be brought back, so the rest of the run stops
# asking and the affected rows can say so.
DB_DOWN=0

maybe_drop_caches() {
  if [[ "$DROP_CACHES" != "1" ]]; then
    return 0
  fi
  if [[ "$DROP_CACHES_SCOPE" == "first-rep" && "${CURRENT_REP:-1}" != "1" ]]; then
    return 0
  fi
  if [[ "$(id -u)" -ne 0 ]]; then
    harness_warn "DROP_CACHES=1 but not root; proceeding warm"
    return 0
  fi
  # Only rows that read the database (USES_DB=1, set by the Robinhood callers)
  # need the server cycled. Restarting it before every command bought nothing
  # for find or GUFI and cost a great deal: a dozen restarts inside a minute
  # trips systemd's start rate limit, after which MariaDB stays down and every
  # later Robinhood row fails on a missing socket.
  if [[ "${USES_DB:-0}" == "1" && "$DROP_DB_CACHE" == "1" && "$DB_DOWN" != "1" &&
    -n "${RBH_DB_HELPER:-}" && -x "${RBH_DB_HELPER}" ]]; then
    if ! PREFIX="${RBH_DB_PREFIX:-$HOME/.local/indexer-compare}" "$RBH_DB_HELPER" restart \
      >>"${HARNESS_LOG:-/dev/null}" 2>&1; then
      DB_DOWN=1
      harness_warn "MariaDB did not come back after a restart; remaining database rows are skipped"
    fi
  fi
  sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null ||
    harness_warn "drop_caches failed"
}

# Exact argv of every timed command, quoted so a line can be pasted back into a
# shell. Answers "what was actually measured" without re-deriving it from the
# scripts, which is the only way to audit a result months later.
log_command() {
  [[ -n "${CMD_LOG:-}" ]] || return 0
  local label=$1
  shift
  local quoted
  quoted=$(printf '%q ' "$@")
  printf '%s\t%s\t%s\n' "$(date +%Y-%m-%dT%H:%M:%S)" "$label" "${quoted% }" >>"$CMD_LOG" 2>/dev/null || true
  # A pipeline reads as one escaped blob once argv is quoted, so repeat the
  # script as written. Same command, in the form a person can check.
  if [[ "${1##*/}" == bash && "${2:-}" == "-c" && -n "${3:-}" ]]; then
    printf '\t%s\t%s\n' "(as written)" "$3" >>"$CMD_LOG" 2>/dev/null || true
  fi
}

# Microseconds since the epoch, without forking when bash 5 can answer it.
wall_us() {
  local t s frac
  if [[ -n "${EPOCHREALTIME:-}" ]]; then
    t=${EPOCHREALTIME/,/.}
  else
    t=$(date +%s.%N 2>/dev/null || date +%s)
  fi
  s=${t%%.*}
  frac=${t#*.}
  [[ "$frac" == "$t" ]] && frac=0
  case "$frac" in *[!0-9]*) frac=0 ;; esac
  frac=${frac}000000
  frac=${frac:0:6}
  printf '%s\n' "$((10#${s:-0} * 1000000 + 10#${frac:-0}))"
}

# Wall-clock seconds for a command. Prefer GNU /usr/bin/time -v; else bash date fallback.
# Usage: time_cmd <time-out-file-or-empty> <cmd> [args...]
time_cmd() {
  local out_time=${1:-}
  shift
  local label=${out_time##*/}
  label=${label%.time.txt}
  log_command "${label:-untimed}" "$@"
  maybe_drop_caches
  # Timed tools legitimately exit nonzero (find on unreadable dirs, partial
  # scans). Capture the status through an || list so errexit never fires here,
  # and never mutate the caller's shell options.
  local rc=0 start end
  start=$(wall_us)
  if [[ -x /usr/bin/time ]]; then
    if [[ -n "$out_time" ]]; then
      /usr/bin/time -v -o "$out_time" -- "$@" || rc=$?
    else
      /usr/bin/time -v -- "$@" || rc=$?
    fi
  else
    "$@" || rc=$?
  fi
  end=$(wall_us)
  if [[ -n "$out_time" ]]; then
    local us=$((end - start))
    ((us >= 0)) || us=0
    # GNU time rounds the wall clock to 10 ms, which turns every query on a
    # small tree into 0.00. Our own clock is the one the summaries read;
    # /usr/bin/time -o still supplies max RSS and the rest of the -v block, and
    # has already truncated the file it wrote.
    [[ -x /usr/bin/time ]] ||
      printf 'Elapsed (wall clock) time (h:mm:ss or m:ss): 0:%d.%06d\n' \
        $((us / 1000000)) $((us % 1000000)) >"$out_time"
    printf 'elapsed_sec=%d.%06d\n' $((us / 1000000)) $((us % 1000000)) >>"$out_time"
    # One quiet progress line per timed binary so a multi-hour run shows what
    # just finished without dumping argv. fd 9 is the harness console (see
    # harness_warn); tool stderr may be redirected elsewhere.
    printf '    %d.%03ds  %s\n' $((us / 1000000)) $(((us % 1000000) / 1000)) \
      "${label:-untimed}" >&9
  fi
  return "$rc"
}

elapsed_from_time_v() {
  local f=$1
  if [[ ! -f "$f" ]]; then
    echo ""
    return 1
  fi
  local kv
  kv=$(grep -E '^elapsed_sec=' "$f" 2>/dev/null | tail -1 | cut -d= -f2- || true)
  if [[ -n "$kv" ]]; then
    echo "$kv"
    return 0
  fi
  # Elapsed (wall clock) time: 0:01.23 or 1:02:03  (GNU time) or our 0:%.3f fallback
  local line t
  line=$(grep -E 'Elapsed \(wall clock\) time' "$f" 2>/dev/null | tail -1 || true)
  if [[ -z "$line" ]]; then
    echo ""
    return 1
  fi
  t=$(echo "$line" | awk -F': ' '{print $NF}' | tr -d ' ')
  python3 - "$t" <<'PY'
import sys
s = sys.argv[1].strip()
parts = s.split(":")
try:
    if len(parts) == 3:
        h, m, sec = parts
        print(float(h) * 3600 + float(m) * 60 + float(sec))
    elif len(parts) == 2:
        m, sec = parts
        print(float(m) * 60 + float(sec))
    else:
        print(float(s))
except Exception:
    sys.exit(1)
PY
}

dir_bytes() {
  local d=$1
  if [[ ! -d "$d" ]]; then
    echo 0
    return
  fi
  # Apparent size of index tree (matches paper “index storage” spirit).
  local n
  n=$({ du -sb "$d" 2>/dev/null || true; } | awk 'NR == 1 { print $1 }')
  echo "${n:-0}"
}

ecrawl_index_bytes() {
  local bin_dir=$1
  local total=0
  local f
  shopt -s nullglob
  for f in "$bin_dir"/uid_shard_*.bin "$bin_dir"/uid_shard_*.bin.ckpt "$bin_dir"/crawl_manifest.txt; do
    [[ -f "$f" ]] || continue
    total=$((total + $(stat -c '%s' "$f")))
  done
  shopt -u nullglob
  echo "$total"
}

# find baseline: files, dirs, symlinks, unique regular-file bytes (inode-deduped).
find_baseline() {
  local root=$1
  local out=$2
  local files dirs links bytes
  files=$(count_find "$root" -xdev -type f)
  dirs=$(count_find "$root" -xdev -type d)
  links=$(count_find "$root" -xdev -type l)
  bytes=$({ find "$root" -xdev -type f -printf '%D:%i %s\n' 2>/dev/null || true; } |
    awk '!seen[$1]++ { s += $2 } END { print s+0 }')
  {
    echo "files=$files"
    echo "dirs=$dirs"
    echo "symlinks=$links"
    echo "unique_file_bytes=$bytes"
  } >"$out"
}

kv_get() {
  local file=$1 key=$2
  grep -E "^${key}=" "$file" 2>/dev/null | tail -1 | cut -d= -f2-
}

os_pretty_name() {
  local name=""
  if [[ -r /etc/os-release ]]; then
    name=$(. /etc/os-release 2>/dev/null && printf '%s' "${PRETTY_NAME:-}")
  fi
  printf '%s\n' "${name:-$(uname -s)}"
}

bin_present() {
  local bin=${1:-}
  [[ -n "$bin" ]] || return 1
  command -v "$bin" >/dev/null 2>&1 || [[ -x "$bin" ]]
}

# First line of a tool's version banner. Only for third-party tools; the suite
# binaries take no --version and are identified by their commit instead.
bin_version() {
  local bin=${1:-} out rc flag
  bin_present "$bin" || return 1
  local runner=()
  command -v timeout >/dev/null 2>&1 && runner=(timeout 5)
  for flag in --version -V; do
    rc=0
    out=$(${runner[@]+"${runner[@]}"} "$bin" "$flag" 2>&1) || rc=$?
    # A tool that cannot start (missing shared library) or rejects the flag
    # must not have its error text recorded as a version string.
    [[ $rc -eq 0 ]] || continue
    out=${out%%$'\n'*}
    case "$out" in
      "" | *[Uu]sage* | *nrecognized* | *[Uu]nknown\ option* | *nvalid\ option*) continue ;;
      *)
        printf '%s\n' "$out"
        return 0
        ;;
    esac
  done
  return 1
}

# Prefer the tool's own banner; fall back to the tag init.sh pinned, so a tool
# that prints no version (GUFI) is still identified.
tool_version() {
  local bin=${1:-} pinned=${2:-} v
  if v=$(bin_version "$bin"); then
    printf '%s\n' "$v"
  elif bin_present "$bin"; then
    if [[ -n "$pinned" ]]; then
      printf 'pinned %s at %s (no usable --version)\n' "$pinned" "$bin"
    else
      printf 'present at %s (no usable --version)\n' "$bin"
    fi
  elif [[ -n "$pinned" ]]; then
    printf 'not_installed (init.sh pins %s)\n' "$pinned"
  else
    echo not_installed
  fi
}

repo_commit() {
  local c
  c=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || true)
  # A copied-out source tree has no .git; say so rather than "unknown", because
  # the two call for different follow-up.
  [[ -n "$c" ]] || {
    echo "not_a_git_checkout ($REPO_ROOT)"
    return
  }
  if [[ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null)" ]]; then
    echo "$c-dirty"
  else
    echo "$c"
  fi
}

# ecrawl/ereport/ereport_index carry no version string, so pin them to the commit
# they were built from, plus a hash of the binary itself: that stays meaningful
# when the source tree was copied without .git, or was dirty at build time.
suite_version() {
  local bin=${1:-}
  [[ -n "$bin" && -x "$bin" ]] || {
    echo missing
    return
  }
  local built hash commit
  built=$(date -r "$bin" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || echo unknown)
  hash=$(sha256sum "$bin" 2>/dev/null | cut -c1-12)
  commit=$(repo_commit)
  case "$commit" in
    not_a_git_checkout*) printf 'built %s, sha256 %s (%s)\n' "$built" "${hash:-unknown}" "$commit" ;;
    *) printf 'git %s, built %s, sha256 %s\n' "$commit" "$built" "${hash:-unknown}" ;;
  esac
}

write_env_snapshot() {
  local out=$1
  {
    echo "timestamp=$(date -Iseconds)"
    echo "hostname=$(hostname)"
    echo "tool_build_host=${INDEXER_COMPARE_BUILD_HOST:-unknown}"
    echo "tool_build_libc=${INDEXER_COMPARE_BUILD_LIBC:-unknown}"
    echo "repo_root=$REPO_ROOT"
    echo "uname=$(uname -a)"
    echo "os=$(os_pretty_name)"
    echo "kernel=$(uname -r)"
    echo "libc=$(bin_version ldd || echo unknown)"
    echo "cc=$(bin_version "${CC:-gcc}" || bin_version cc || echo unknown)"
    echo "repo_commit=$(repo_commit)"
    echo "version_ecrawl=$(suite_version "$ECRAWL_BIN")"
    echo "version_ereport=$(suite_version "$EREPORT_BIN")"
    echo "version_ereport_index=$(suite_version "$EREPORT_INDEX_BIN")"
    echo "version_gufi=$(tool_version "${GUFI_DIR2INDEX:-}" "${INDEXER_COMPARE_GUFI_VERSION:-}")"
    echo "version_xdu=$(tool_version "${XDU_BIN:-}" "${INDEXER_COMPARE_XDU_VERSION:-}")"
    echo "version_robinhood=$(tool_version "${RBH_SCAN:-}" "${INDEXER_COMPARE_ROBINHOOD_VERSION:-}")"
    echo "version_mariadb=$(bin_version mysql || bin_version mariadb || echo not_installed)"
    # FD_OK / DUA_OK already distinguish installed from actually runnable.
    if [[ -n "${FD_BIN:-}" && "$FD_OK" != "1" ]]; then
      echo "version_fd=present but not runnable: $FD_BIN"
      echo "fd_unusable=${FD_WHY:-no error text}"
    else
      echo "version_fd=$(tool_version "${FD_BIN:-}")"
    fi
    if [[ -n "${DUA_BIN:-}" && "$DUA_OK" != "1" ]]; then
      echo "version_dua=present but not runnable: $DUA_BIN"
      echo "dua_unusable=${DUA_WHY:-no error text}"
    else
      echo "version_dua=$(tool_version "${DUA_BIN:-}" "${INDEXER_COMPARE_DUA_VERSION:-}")"
    fi
    echo "version_find=$(tool_version find)"
    echo "version_du=$(tool_version du)"
    echo "version_python3=$(tool_version python3)"
    echo "nproc=$(nproc 2>/dev/null || echo '?')"
    echo "threads=$THREADS"
    echo "thread_plan=$(thread_plan)"
    echo "nofile=$(ulimit -Sn)"
    echo "drop_caches=$DROP_CACHES"
    echo "drop_caches_scope=$DROP_CACHES_SCOPE"
    echo "drop_db_cache=$DROP_DB_CACHE"
    echo "reps=$REPS"
    # Only when some tool disagrees with REPS, so a uniform run reads exactly as
    # it always did. Without it a mixed run claims one repetition count for rows
    # that do not share it, and the error bars look inconsistent for no reason.
    _ic_reps_tools=${TOOLS:-}
    [[ "${INCLUDE_EREPORT_INDEX:-0}" != "1" ]] || _ic_reps_tools+=" ereport_index"
    if [[ -n "${_ic_reps_tools// /}" ]]; then
      _ic_reps_detail=""
      for _ic_reps_t in $_ic_reps_tools; do
        _ic_reps_n=$(tool_reps "$_ic_reps_t")
        [[ "$_ic_reps_n" == "$REPS" ]] ||
          _ic_reps_detail+="${_ic_reps_detail:+ }$_ic_reps_t=$_ic_reps_n"
      done
      [[ -z "$_ic_reps_detail" ]] || echo "reps_per_tool=$_ic_reps_detail"
    fi
    echo "ecrawl_bin=$ECRAWL_BIN"
    echo "ereport_bin=$EREPORT_BIN"
    echo "ereport_index_bin=$EREPORT_INDEX_BIN"
    echo "gufi_dir2index=${GUFI_DIR2INDEX:-}"
    echo "xdu_bin=${XDU_BIN:-}"
    echo "rbh_scan=${RBH_SCAN:-}"
    # Both tools take their index, thread count and credentials from a file
    # rather than argv, so which file was in effect is part of the run. Only
    # worth recording for a tool that is actually installed.
    [[ -z "${GUFI_FIND:-}${GUFI_DIR2INDEX:-}" ]] || echo "gufi_config=$(gufi_config_path)"
    echo "gufi_index_root=${GUFI_INDEX_DIR:-}"
    # A GUFI query is a Python wrapper, so where its modules were found is as
    # much part of the run as the binary path.
    [[ -z "${GUFI_FIND:-}${GUFI_DIR2INDEX:-}" ]] || echo "gufi_pythonpath=${PYTHONPATH:-}"
    echo "rbh_config=${RBH_CONFIG:-}"
    echo "rbh_config_fs_path=$(rbh_conf_value fs_path "${RBH_CONFIG:-/nonexistent}" 2>/dev/null || echo '')"
    # Robinhood's tables come from its first scan, so whether they existed when
    # the queries ran is the difference between an answer and a hard failure.
    if [[ -n "${RBH_CONFIG:-}" ]]; then
      if rbh_schema_ready; then
        echo "rbh_schema=present"
      else
        echo "rbh_schema=absent ($RBH_SCHEMA_REASON)"
      fi
    fi
    echo "rbh_du_args=${RBH_DU_ARGS[*]:-}"
    echo "xdu_index_args=${XDU_SIZE_ARGS[*]:-}"
    echo "fd_bin=${FD_BIN:-}"
    echo "fd_args=${FD_COMMON_ARGS[*]}"
    echo "dua_bin=${DUA_BIN:-}"
    echo "dua_args=${DUA_AGG_ARGS[*]}"
    echo "ECRAWL_CRAWL_THREADS=${ECRAWL_CRAWL_THREADS:-}"
    echo "ECRAWL_STAT_THREADS=${ECRAWL_STAT_THREADS:-}"
    echo "ECRAWL_WRITER_THREADS=${ECRAWL_WRITER_THREADS:-}"
    echo "work_root=${WORK_ROOT:-}"
    echo "tmpdir=${TMPDIR:-/tmp}"
    df -T "$PWD" 2>/dev/null | tail -1 | awk '{print "cwd_fstype="$2; print "cwd_mount="$NF}' || true
    # Whether the indexes landed on the filesystem under test or on separate
    # storage changes how the write-heavy rows should be read.
    [[ -z "${WORK_ROOT:-}" ]] ||
      df -T "$WORK_ROOT" 2>/dev/null | tail -1 |
      awk '{print "work_fstype="$2; print "work_mount="$NF}' || true
    [[ -z "${TREE:-}" ]] ||
      df -T "$TREE" 2>/dev/null | tail -1 |
      awk '{print "tree_fstype="$2; print "tree_mount="$NF}' || true
  } >"$out"
}

require_python3() {
  command -v python3 >/dev/null 2>&1 || {
    echo "ERROR: python3 required" >&2
    exit 1
  }
}

# Charts need matplotlib, which often belongs to a different interpreter than
# the `python3` on PATH (platform python vs. RPM/venv builds). Print the first
# interpreter that can import it.
resolve_chart_python() {
  local candidates=()
  [[ -n "${CHART_PYTHON:-}" ]] && candidates+=("$CHART_PYTHON")
  [[ -n "${INDEXER_COMPARE_PREFIX:-}" ]] &&
    candidates+=("$INDEXER_COMPARE_PREFIX/chartvenv/bin/python")
  candidates+=(python3 python3.13 python3.12 python3.11 python3.10 python3.9)
  local py
  for py in "${candidates[@]}"; do
    [[ -x "$py" ]] || command -v "$py" >/dev/null 2>&1 || continue
    if "$py" -c 'import matplotlib' >/dev/null 2>&1; then
      printf '%s\n' "$py"
      return 0
    fi
  done
  return 1
}
