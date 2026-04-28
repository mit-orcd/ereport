#!/usr/bin/env bash
#
# Verification harness for ecrawl + ereport (optional: correlate with live filesystem counts).
#
# Usage:
#   ./test.sh                      # integration test only (temp tree + built binaries)
#   ./test.sh /path/to/tree        # integration + filesystem correlation for that root
#   SKIP_FS=1 ./test.sh /path      # integration only (ignore arg for fs checks)
#   ECRAWL=/abs/ecrawl EREPORT=/abs/ereport ./test.sh
#
# Requires: bash, coreutils, ecrawl + ereport built (default: same directory as this script).

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ECRAWL="${ECRAWL:-$SCRIPT_DIR/ecrawl}"
EREPORT="${EREPORT:-$SCRIPT_DIR/ereport}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'OK: %s\n' "$*"; }

# Last line wins (tools may print stats blocks more than once in verbose modes).
kv_last() {
    local key=$1 file=$2
    grep "^${key}=" "$file" 2>/dev/null | tail -n1 | cut -d= -f2-
}

expect_eq() {
    local label=$1 want=$2 got=$3
    [[ "$got" == "$want" ]] || die "${label}: want '${want}' got '${got}'"
}

need_exe() {
    [[ -x "$1" ]] || die "missing or not executable: $1 (run 'make' in ${SCRIPT_DIR})"
}

# --- Optional: correlate crawl totals with find/fd on a real tree (slow on huge trees) ---
count_files() {
    local root=$1
    if command -v fd >/dev/null 2>&1; then
        fd --hidden --no-ignore -t f . "$root" 2>/dev/null | wc -l | tr -d ' '
    else
        find "$root" -type f 2>/dev/null | wc -l | tr -d ' '
    fi
}

count_dirs() {
    local root=$1
    if command -v fd >/dev/null 2>&1; then
        fd --hidden --no-ignore -t d . "$root" 2>/dev/null | wc -l | tr -d ' '
    else
        find "$root" -type d 2>/dev/null | wc -l | tr -d ' '
    fi
}

count_symlinks() {
    local root=$1
    if command -v fd >/dev/null 2>&1; then
        fd --hidden --no-ignore -t l . "$root" 2>/dev/null | wc -l | tr -d ' '
    else
        find "$root" -type l 2>/dev/null | wc -l | tr -d ' '
    fi
}

sum_unique_regular_bytes() {
    local root=$1
    # Same spirit as test.sh original: unique (dev,inode) sums size once per inode (regular files).
    find "$root" -type f -printf '%D:%i %s\n' 2>/dev/null | sort -u | awk '{s += $2} END {printf "%.0f\n", s + 0}'
}

run_fs_correlation() {
    local root=$1
    [[ -d "$root" ]] || die "not a directory: $root"

    log "filesystem correlation (may be slow on large trees): $root"

    local fc dc lc crawl_files crawl_dirs crawl_symlinks crawl_bytes ere_files ere_dirs ere_links ere_cap fs_files fs_dirs fs_symlinks fs_u_bytes

    fc=$(count_files "$root")
    dc=$(count_dirs "$root")
    lc=$(count_symlinks "$root")
    fs_u_bytes=$(sum_unique_regular_bytes "$root")

    printf '  find/fd: files=%s dirs=%s symlinks=%s unique_regular_bytes=%s\n' "$fc" "$dc" "$lc" "$fs_u_bytes"

    local td crawl_out crawl_log ere_log ere_out
    td=$(mktemp -d "${TMPDIR:-/tmp}/ereport_fs_test.XXXXXX")
    crawl_out="${td}/crawl_out"
    crawl_log="${td}/ecrawl.log"
    ere_log="${td}/ereport.err"
    ere_out="${td}/ereport.out"

    cleanup_fs() {
        rm -rf "$td"
    }
    trap cleanup_fs EXIT

    local root_abs
    root_abs=$(cd "$root" && pwd)

    log "ecrawl -> ${crawl_out}"
    ECREAWL_WORKERS="${ECREAWL_WORKERS:-8}" \
        "$ECRAWL" "$root_abs" "$crawl_out" >"$crawl_log" 2>&1 || {
        tail -n 40 "$crawl_log" >&2 || true
        die "ecrawl failed"
    }

    crawl_files=$(kv_last files "$crawl_log")
    crawl_dirs=$(kv_last dirs "$crawl_log")
    crawl_symlinks=$(kv_last symlinks "$crawl_log")
    crawl_bytes=$(kv_last total_bytes "$crawl_log")

    printf '  ecrawl:  files=%s dirs=%s symlinks=%s total_bytes=%s\n' \
        "$crawl_files" "$crawl_dirs" "$crawl_symlinks" "$crawl_bytes"

    expect_eq "ecrawl.files vs fs file count" "$fc" "$crawl_files"
    expect_eq "ecrawl.dirs vs fs dir count" "$dc" "$crawl_dirs"
    expect_eq "ecrawl.symlinks vs fs symlink count" "$lc" "$crawl_symlinks"
    expect_eq "ecrawl.total_bytes vs sum unique regular bytes" "$fs_u_bytes" "$crawl_bytes"

    log "ereport (single user) cwd=${td}"
    (
        cd "$td" || exit 1
        EREPORT_THREADS="${EREPORT_THREADS:-8}" \
            "$EREPORT" "$(id -un)" mtime "$crawl_out" >"$ere_out" 2>"$ere_log"
    ) || {
        tail -n 60 "$ere_log" >&2 || true
        tail -n 40 "$ere_out" >&2 || true
        die "ereport failed"
    }

    ere_files=$(kv_last files "$ere_out")
    ere_dirs=$(kv_last directories "$ere_out")
    ere_links=$(kv_last links "$ere_out")
    ere_cap=$(kv_last total_capacity_in_files "$ere_out")

    printf '  ereport: files=%s dirs=%s links=%s total_capacity_in_files=%s\n' \
        "$ere_files" "$ere_dirs" "$ere_links" "$ere_cap"

    expect_eq "ereport.files vs ecrawl.files" "$crawl_files" "$ere_files"
    expect_eq "ereport.directories vs ecrawl.dirs" "$crawl_dirs" "$ere_dirs"
    expect_eq "ereport.links vs ecrawl.symlinks" "$crawl_symlinks" "$ere_links"
    expect_eq "ereport.total_capacity_in_files vs ecrawl.total_bytes" "$crawl_bytes" "$ere_cap"

    local scanned matched entries
    scanned=$(kv_last scanned_records "$ere_out")
    matched=$(kv_last matched_records "$ere_out")
    entries=$(kv_last entries "$crawl_log")
    printf '  records: ecrawl entries=%s ereport scanned=%s matched=%s\n' "$entries" "$scanned" "$matched"
    expect_eq "ereport.scanned_records vs ecrawl.entries" "$entries" "$scanned"
    expect_eq "ereport.matched_records vs scanned (single-user tree)" "$scanned" "$matched"

    trap - EXIT
    cleanup_fs
    pass "filesystem correlation for $root"
}

# --- Integration: synthetic tree in /tmp; no dependency on fd/find ---
run_integration() {
    log "integration test (synthetic tree)"

    local td crawl_out crawl_log ere_out ere_err au_out au_err root_abs
    td=$(mktemp -d "${TMPDIR:-/tmp}/ereport_int.XXXXXX")
    crawl_out="${td}/crawl_out"
    crawl_log="${td}/ecrawl.log"
    ere_out="${td}/ereport_single.stdout"
    ere_err="${td}/ereport_single.stderr"
    au_out="${td}/ereport_all.stdout"
    au_err="${td}/ereport_all.stderr"

    cleanup_int() {
        rm -rf "$td"
    }
    trap cleanup_int EXIT

    mkdir -p "${td}/walk/sub"
    echo hello >"${td}/walk/a.txt"
    echo world >"${td}/walk/sub/b.txt"
    ln -s a.txt "${td}/walk/link_a"
    ln "${td}/walk/a.txt" "${td}/walk/a_hard"

    root_abs=$(cd "${td}/walk" && pwd)

    log "ecrawl ${root_abs} -> ${crawl_out}"
    ECREAWL_WORKERS="${ECREAWL_WORKERS:-4}" \
        "$ECRAWL" "$root_abs" "$crawl_out" >"$crawl_log" 2>&1 || {
        tail -n 40 "$crawl_log" >&2 || true
        die "ecrawl failed on synthetic tree"
    }

    local ce cf cd cs cl co
    ce=$(kv_last entries "$crawl_log")
    cf=$(kv_last files "$crawl_log")
    cd=$(kv_last dirs "$crawl_log")
    cs=$(kv_last symlinks "$crawl_log")
    co=$(kv_last other "$crawl_log")
    printf '  ecrawl summary: entries=%s files=%s dirs=%s symlinks=%s other=%s errors=%s\n' \
        "$ce" "$cf" "$cd" "$cs" "$co" "$(kv_last errors "$crawl_log")"

    [[ "${co:-0}" == "0" ]] || die "ecrawl.other expected 0 on synthetic tree (got ${co})"

    local sum
    sum=$((cd + cf + cs + co))
    expect_eq "ecrawl.entries == dirs+files+symlinks+other" "$ce" "$sum"

    log "ereport single-user (cwd=${td})"
    (
        cd "$td" || exit 1
        EREPORT_THREADS="${EREPORT_THREADS:-4}" \
            "$EREPORT" "$(id -un)" mtime "$crawl_out" >"$ere_out" 2>"$ere_err"
    ) || {
        tail -n 80 "$ere_err" >&2 || true
        die "ereport (single user) failed"
    }

    local sf sd sl sm smat sscan scap dist
    sf=$(kv_last files "$ere_out")
    sd=$(kv_last directories "$ere_out")
    sl=$(kv_last links "$ere_out")
    sm=$(kv_last others "$ere_out")
    smat=$(kv_last matched_records "$ere_out")
    sscan=$(kv_last scanned_records "$ere_out")
    scap=$(kv_last total_capacity_in_files "$ere_out")

    printf '  ereport single: files=%s dirs=%s links=%s others=%s scanned=%s matched=%s capacity_files=%s\n' \
        "$sf" "$sd" "$sl" "$sm" "$sscan" "$smat" "$scap"

    expect_eq "single: ereport.files" "$cf" "$sf"
    expect_eq "single: ereport.directories" "$cd" "$sd"
    expect_eq "single: ereport.links" "$cs" "$sl"
    expect_eq "single: ereport.scanned_records" "$ce" "$sscan"
    expect_eq "single: ereport.matched_records" "$sscan" "$smat"

    local tb
    tb=$(kv_last total_bytes "$crawl_log")
    expect_eq "single: total_capacity_in_files vs ecrawl.total_bytes" "$tb" "$scap"

    log "ereport all-users aggregate"
    (
        cd "$td" || exit 1
        EREPORT_THREADS="${EREPORT_THREADS:-4}" \
            "$EREPORT" mtime "$crawl_out" >"$au_out" 2>"$au_err"
    ) || {
        tail -n 80 "$au_err" >&2 || true
        die "ereport (all users) failed"
    }

    dist=$(kv_last distinct_uids "$au_out")
    smat=$(kv_last matched_records "$au_out")
    sscan=$(kv_last scanned_records "$au_out")
    printf '  ereport all_users: distinct_uids=%s scanned=%s matched=%s\n' "$dist" "$sscan" "$smat"

    expect_eq "all_users: distinct_uids (single uid in crawl)" "1" "$dist"
    expect_eq "all_users: scanned_records vs ecrawl.entries" "$ce" "$sscan"
    expect_eq "all_users: matched_records vs scanned" "$sscan" "$smat"

    trap - EXIT
    cleanup_int
    pass "integration test"
}

# --- main ---
ROOT="${1:-}"

need_exe "$ECRAWL"
need_exe "$EREPORT"

run_integration

if [[ -n "${SKIP_FS:-}" ]] || [[ -z "${ROOT}" ]] || [[ ! -d "${ROOT}" ]]; then
    if [[ -n "${ROOT}" ]] && [[ ! -d "${ROOT}" ]]; then
        die "not a directory: ${ROOT}"
    fi
    if [[ -z "${SKIP_FS:-}" ]] && [[ -z "${ROOT}" ]]; then
        log "tip: pass a crawl root directory as \$1 to run filesystem correlation (SKIP_FS=1 to skip)"
    fi
    exit 0
fi

run_fs_correlation "$ROOT"
exit 0
