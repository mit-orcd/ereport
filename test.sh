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

# ANSI colors: disabled for non-tty or NO_COLOR (https://no-color.org/)
_init_colors() {
    if [[ -n "${NO_COLOR:-}" ]] || { [[ ! -t 1 ]] && [[ ! -t 2 ]]; }; then
        R= G= Y= B= C= M= D= BD= Z=
        return
    fi
    R=$'\033[31m'   # red: FAIL / errors
    G=$'\033[32m'   # green: OK / pass
    Y=$'\033[33m'   # yellow: notes / tips / warnings
    B=$'\033[34m'   # blue: timestamps / phase labels
    C=$'\033[36m'   # cyan: numbered check sections [1]–[4]
    M=$'\033[35m'   # magenta: secondary stats emphasis (optional)
    D=$'\033[2m'    # dim: helper / indented lines
    BD=$'\033[1m'
    Z=$'\033[0m'
}
_init_colors

log() {
    local ts msg
    ts="$(date +%H:%M:%S)"
    msg=$*
    case "$msg" in
        note:*)
            printf '%s[%s]%s %s%s%s\n' "$B" "$ts" "$Z" "$Y" "${msg#note: }" "$Z"
            ;;
        tip:*)
            printf '%s[%s]%s %s%s%s\n' "$B" "$ts" "$Z" "$Y" "${msg#tip: }" "$Z"
            ;;
        *)
            printf '%s[%s]%s %s%s\n' "$B" "$ts" "$Z" "$msg" "$Z"
            ;;
    esac
}

die() { printf '%sFAIL:%s %s%s\n' "$R" "$Z" "$*" "$Z" >&2; exit 1; }
pass() { printf '%sOK:%s %s%s\n' "$G" "$Z" "$*" "$Z"; }

# Section titles: cyan bold for filesystem correlation groups [1]–[4]
section_fs() { printf '\n  %s%s%s%s\n' "$C" "$BD" "$1" "$Z"; }
# Yellow bold for integration subsection titles
section_int() { printf '\n  %s%s%s%s\n' "$Y" "$BD" "$1" "$Z"; }

# Last line wins (tools may print stats blocks more than once in verbose modes).
kv_last() {
    local key=$1 file=$2
    grep "^${key}=" "$file" 2>/dev/null | tail -n1 | cut -d= -f2-
}

# For two non-negative integer strings: absolute delta and percent of baseline (want).
# Percent rounds to integer; if that is 0 but pct > 0, use decimals until the first
# non-zero digit after the point (same significant-digit intent as "first digit after comma").
fs_fail_delta() {
    local want=$1 got=$2
    LC_ALL=C awk -v w="$want" -v g="$got" '
    function abs(x) { return x < 0 ? -x : x }
    BEGIN {
        w = int(w + 0)
        g = int(g + 0)
        d = abs(g - w)
        printf "delta_abs=%d", d
        if (w == 0) {
            if (g == 0) print " delta_pct=0%"
            else print " delta_pct=n/a"
            exit
        }
        pct = 100.0 * d / w
        ri = int(pct + 0.5)
        if (ri >= 1) printf " delta_pct=%d%%\n", ri
        else if (pct <= 0) print " delta_pct=0%"
        else {
            order = log(pct) / log(10)
            nd = int(-order) + 1
            if (nd < 1) nd = 1
            if (nd > 15) nd = 15
            printf " delta_pct=%.*f%%\n", nd, pct
        }
    }'
}

# Optional 4th arg: on success, print "OK: label — reason" (why the match matters).
expect_eq() {
    local label=$1 want=$2 got=$3
    local ok_note=${4:-}
    [[ "$got" == "$want" ]] || die "${label}: want '${want}' got '${got}'"
    if [[ -n "$ok_note" ]]; then
        printf '  %sOK:%s %s — %s%s\n' "$G" "$Z" "$label" "$ok_note" "$Z"
    fi
}

# Like expect_eq but records failure and continues (for fs correlation so every check is printed).
expect_eq_continue() {
    local label=$1 want=$2 got=$3
    local ok_note=${4:-}
    if [[ "$got" != "$want" ]]; then
        if [[ "$want" =~ ^[0-9]+$ ]] && [[ "$got" =~ ^[0-9]+$ ]]; then
            printf '  %sFAIL:%s %s: want %s got %s (%s)%s\n' "$R" "$Z" "$label" "$want" "$got" "$(fs_fail_delta "$want" "$got")" "$Z" >&2
        else
            printf '  %sFAIL:%s %s: want %s got %s%s\n' "$R" "$Z" "$label" "$want" "$got" "$Z" >&2
        fi
        return 1
    fi
    if [[ -n "$ok_note" ]]; then
        printf '  %sOK:%s %s — %s%s\n' "$G" "$Z" "$label" "$ok_note" "$Z"
    fi
    return 0
}

# Integer: got <= ceiling (for single-user ⊆ tree / ⊆ all-users checks). Optional 4th: OK reason.
expect_le_continue() {
    local label=$1 ceiling=$2 got=$3
    local ok_note=${4:-}
    if [[ "$got" -gt "$ceiling" ]]; then
        printf '  %sFAIL:%s %s: want <= %s got %s%s\n' "$R" "$Z" "$label" "$ceiling" "$got" "$Z" >&2
        return 1
    fi
    if [[ -n "$ok_note" ]]; then
        printf '  %sOK:%s %s — %s%s\n' "$G" "$Z" "$label" "$ok_note" "$Z"
    fi
    return 0
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
    # Always use find: ecrawl counts the crawl root directory when it runs; `fd -t d . ROOT` omits ROOT
    # from results (off-by-one vs ecrawl). `find ROOT -type d` includes ROOT like ecrawl.
    find "$root" -type d 2>/dev/null | wc -l | tr -d ' '
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
    # Sum st_size once per (dev,inode) so hard links count once, matching ecrawl total_bytes spirit.
    # One awk pass (no sort). RAM scales with unique (dev,inode) keys.
    find "$root" -type f -printf '%D:%i %s\n' 2>/dev/null | awk '!seen[$1]++ { s += $2 } END { printf "%.0f\n", s + 0 }'
}

run_fs_correlation() {
    local root=$1
    local fs_fail=0
    [[ -d "$root" ]] || die "not a directory: $root"

    log "filesystem correlation (may be slow on large trees): $root"
    log "note: live trees drift between the find/fd snapshot and ecrawl; expect exact match only on quiescent data."
    log "note: same find/fd snapshot is compared to ecrawl and (all-users) ereport — ecrawl vs ereport should match even when fs baseline drifts."
    log "note: each passing check prints OK: <check> — <why it matters>"
    log "step: baseline — counting files/dirs/symlinks, then unique regular-file bytes (find | awk dedup; slow = walk + RAM for keys)"

    local fc dc lc crawl_files crawl_dirs crawl_symlinks crawl_bytes entries
    local su_files su_dirs su_links su_cap su_scanned su_matched
    local au_files au_dirs au_links au_cap au_scanned au_matched au_distinct

    fc=$(count_files "$root")
    dc=$(count_dirs "$root")
    lc=$(count_symlinks "$root")
    fs_u_bytes=$(sum_unique_regular_bytes "$root")

    printf '  %sfind/fd:%s files=%s dirs=%s symlinks=%s unique_regular_bytes=%s\n' "$M" "$Z" "$fc" "$dc" "$lc" "$fs_u_bytes"
    printf '%s           (dirs: find -type d incl. crawl root; files/symlinks: fd if installed else find)%s\n' "$D" "$Z"

    local td crawl_out crawl_log ere_su_out ere_su_err ere_all_out ere_all_log
    td=$(mktemp -d "${TMPDIR:-/tmp}/ereport_fs_test.XXXXXX")
    crawl_out="${td}/crawl_out"
    crawl_log="${td}/ecrawl.log"
    ere_su_out="${td}/ereport_single.stdout"
    ere_su_err="${td}/ereport_single.stderr"
    ere_all_out="${td}/ereport_all.stdout"
    ere_all_log="${td}/ereport_all.stderr"

    cleanup_fs() {
        rm -rf "$td"
    }
    trap cleanup_fs EXIT

    local root_abs
    root_abs=$(cd "$root" && pwd)

    log "step: ecrawl → ${crawl_out}"
    ECRAWL_WORKERS="${ECRAWL_WORKERS:-8}" \
        "$ECRAWL" "$root_abs" "$crawl_out" >"$crawl_log" 2>&1 || {
        tail -n 40 "$crawl_log" >&2 || true
        die "ecrawl failed"
    }

    crawl_files=$(kv_last files "$crawl_log")
    crawl_dirs=$(kv_last dirs "$crawl_log")
    crawl_symlinks=$(kv_last symlinks "$crawl_log")
    crawl_bytes=$(kv_last total_bytes "$crawl_log")

    printf '  %secrawl:%s  files=%s dirs=%s symlinks=%s total_bytes=%s\n' "$M" "$Z" \
        "$crawl_files" "$crawl_dirs" "$crawl_symlinks" "$crawl_bytes"

    section_fs "[1] Filesystem baseline vs ecrawl — crawl should match the tree as counted above"
    expect_eq_continue "ecrawl.files vs fs file count" "$fc" "$crawl_files" \
        "ecrawl recorded the same number of regular files as find/fd on this crawl root" || fs_fail=1
    expect_eq_continue "ecrawl.dirs vs fs dir count" "$dc" "$crawl_dirs" \
        "ecrawl recorded the same directory count as find -type d (crawl root included, same as ecrawl seed)" || fs_fail=1
    expect_eq_continue "ecrawl.symlinks vs fs symlink count" "$lc" "$crawl_symlinks" \
        "ecrawl saw the same symlink count as find/fd" || fs_fail=1
    expect_eq_continue "ecrawl.total_bytes vs sum unique regular bytes" "$fs_u_bytes" "$crawl_bytes" \
        "ecrawl total_bytes matches find unique (dev,inode) byte sum for regular files" || fs_fail=1

    entries=$(kv_last entries "$crawl_log")

    # All-users first: always meaningful when ecrawl wrote uid-shard bins. Single-user loads only one shard
    # (uid & (uid_shards-1)); ecrawl omits empty shards — e.g. root maps to shard 0; if no file owner lands
    # there, uid_shard_0000.bin may not exist and ereport single-user cannot run (not a bug).
    log "step: ereport all-users (cwd=${td})"
    (
        cd "$td" || exit 1
        EREPORT_THREADS="${EREPORT_THREADS:-8}" \
            "$EREPORT" mtime "$crawl_out" >"$ere_all_out" 2>"$ere_all_log"
    ) || {
        tail -n 60 "$ere_all_log" >&2 || true
        tail -n 40 "$ere_all_out" >&2 || true
        die "ereport (all users) failed"
    }

    local skip_single=0
    log "step: ereport single-user ($(id -un)) cwd=${td}"
    if (
        cd "$td" || exit 1
        EREPORT_THREADS="${EREPORT_THREADS:-8}" \
            "$EREPORT" "$(id -un)" mtime "$crawl_out" >"$ere_su_out" 2>"$ere_su_err"
    ); then
        :
    else
        skip_single=1
        log "note: skipping ereport single-user checks (ecrawl omits empty uid-shards)."
        grep '^ereport:' "$ere_su_err" 2>/dev/null | tail -n 5 >&2 || true
    fi

    su_files=$(kv_last files "$ere_su_out")
    su_dirs=$(kv_last directories "$ere_su_out")
    su_links=$(kv_last links "$ere_su_out")
    su_cap=$(kv_last total_capacity_in_files "$ere_su_out")
    su_scanned=$(kv_last scanned_records "$ere_su_out")
    su_matched=$(kv_last matched_records "$ere_su_out")

    au_files=$(kv_last files "$ere_all_out")
    au_dirs=$(kv_last directories "$ere_all_out")
    au_links=$(kv_last links "$ere_all_out")
    au_cap=$(kv_last total_capacity_in_files "$ere_all_out")
    au_scanned=$(kv_last scanned_records "$ere_all_out")
    au_matched=$(kv_last matched_records "$ere_all_out")
    au_distinct=$(kv_last distinct_uids "$ere_all_out")

    if [[ "$skip_single" -eq 0 ]]; then
        printf '  %sereport single (%s):%s files=%s dirs=%s links=%s scanned=%s matched=%s total_capacity_in_files=%s\n' \
            "$M" "$(id -un)" "$Z" "$su_files" "$su_dirs" "$su_links" "$su_scanned" "$su_matched" "$su_cap"
    else
        printf '  %sereport single (%s): (skipped — no shard bin for this uid)%s\n' "$Y" "$(id -un)" "$Z"
    fi
    printf '  %sereport all_users:%s files=%s dirs=%s links=%s scanned=%s matched=%s total_capacity_in_files=%s distinct_uids=%s\n' \
        "$M" "$Z" "$au_files" "$au_dirs" "$au_links" "$au_scanned" "$au_matched" "$au_cap" "$au_distinct"
    if [[ "$skip_single" -eq 0 ]]; then
        printf '%s  records:%s ecrawl entries=%s ereport single scanned=%s all-users scanned=%s\n' \
            "$D" "$Z" "$entries" "$su_scanned" "$au_scanned"
    else
        printf '%s  records:%s ecrawl entries=%s ereport single scanned=(skipped) all-users scanned=%s\n' \
            "$D" "$Z" "$entries" "$au_scanned"
    fi

    section_fs "[2] Crawl bins: ereport all-users vs ecrawl — reader must agree with what ecrawl wrote"
    expect_eq_continue "all-users: ereport.files vs ecrawl.files" "$crawl_files" "$au_files" \
        "ereport aggregated the same file count from .bin shards as ecrawl’s stats line" || fs_fail=1
    expect_eq_continue "all-users: ereport.directories vs ecrawl.dirs" "$crawl_dirs" "$au_dirs" \
        "same directory total as ecrawl (tree-wide over crawl records)" || fs_fail=1
    expect_eq_continue "all-users: ereport.links vs ecrawl.symlinks" "$crawl_symlinks" "$au_links" \
        "same symlink total as ecrawl" || fs_fail=1
    expect_eq_continue "all-users: ereport.total_capacity_in_files vs ecrawl.total_bytes" "$crawl_bytes" "$au_cap" \
        "capacity in files matches ecrawl total_bytes" || fs_fail=1
    expect_eq_continue "all-users: ereport.scanned_records vs ecrawl.entries" "$entries" "$au_scanned" \
        "every crawl record was read (scanned == ecrawl entries)" || fs_fail=1
    expect_eq_continue "all-users: ereport.matched_records vs scanned" "$au_scanned" "$au_matched" \
        "all-users mode keeps every scanned row (matched == scanned)" || fs_fail=1

    section_fs "[3] Terminal snapshot vs ereport all-users — report totals match find/fd baseline (same tree)"
    expect_eq_continue "all-users: ereport.files vs fs file count (find/fd)" "$fc" "$au_files" \
        "ereport file total matches the earlier find/fd file count for this path" || fs_fail=1
    expect_eq_continue "all-users: ereport.directories vs fs dir count (find/fd)" "$dc" "$au_dirs" \
        "ereport dir total matches find -type d baseline (incl. crawl root)" || fs_fail=1
    expect_eq_continue "all-users: ereport.links vs fs symlink count (find/fd)" "$lc" "$au_links" \
        "ereport link total matches find/fd symlink count" || fs_fail=1
    expect_eq_continue "all-users: ereport.total_capacity_in_files vs fs unique regular bytes (find)" "$fs_u_bytes" "$au_cap" \
        "ereport capacity matches find unique regular-file bytes (hardlinks counted once)" || fs_fail=1

    if [[ "$skip_single" -eq 0 ]]; then
        section_fs "[4] ereport single-user ($(id -un)) — slice must be consistent vs full crawl"
        expect_eq_continue "single-user: matched_records vs scanned" "$su_scanned" "$su_matched" \
            "after UID filter, every scanned record matches (no dropped rows in slice)" || fs_fail=1
        expect_le_continue "single-user: scanned_records <= ecrawl.entries" "$entries" "$su_scanned" \
            "single-user cannot scan more rows than the full crawl recorded" || fs_fail=1
        expect_le_continue "single-user: scanned_records <= all-users scanned_records" "$au_scanned" "$su_scanned" \
            "this uid’s shard cannot exceed the union of all shards" || fs_fail=1
        expect_le_continue "single-user: files <= all-users files" "$au_files" "$su_files" \
            "uid slice file count ≤ tree-wide file count" || fs_fail=1
        expect_le_continue "single-user: directories <= all-users directories" "$au_dirs" "$su_dirs" \
            "uid slice dir count ≤ tree-wide" || fs_fail=1
        expect_le_continue "single-user: links <= all-users links" "$au_links" "$su_links" \
            "uid slice symlink count ≤ tree-wide" || fs_fail=1
        expect_le_continue "single-user: total_capacity_in_files <= all-users total_capacity_in_files" "$au_cap" "$su_cap" \
            "uid slice capacity ≤ aggregate capacity" || fs_fail=1
    else
        printf '\n%s  [4] ereport single-user — skipped (no uid-shard .bin for this uid)%s\n' "$Y$BD" "$Z"
    fi

    trap - EXIT
    cleanup_fs
    [[ "$fs_fail" -eq 0 ]] || die "filesystem correlation had mismatches for $root (see FAIL lines above)"
    pass "filesystem correlation for $root"
}

# --- Integration: synthetic tree in /tmp; no dependency on find/fd ---
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
    ECRAWL_WORKERS="${ECRAWL_WORKERS:-4}" \
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
    printf '  %secrawl summary:%s entries=%s files=%s dirs=%s symlinks=%s other=%s errors=%s\n' \
        "$M" "$Z" "$ce" "$cf" "$cd" "$cs" "$co" "$(kv_last errors "$crawl_log")"

    [[ "${co:-0}" == "0" ]] || die "ecrawl.other expected 0 on synthetic tree (got ${co})"

    local sum
    sum=$((cd + cf + cs + co))
    section_int "[integration] Synthetic tree — ecrawl internal consistency"
    expect_eq "ecrawl.entries == dirs+files+symlinks+other" "$ce" "$sum" \
        "ecrawl entry count equals components (sanity on crawl accounting)"

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

    printf '  %sereport single:%s files=%s dirs=%s links=%s others=%s scanned=%s matched=%s capacity_files=%s\n' \
        "$M" "$Z" "$sf" "$sd" "$sl" "$sm" "$sscan" "$smat" "$scap"

    section_int "[integration] ereport single-user vs ecrawl (one UID owns the whole synthetic tree)"
    expect_eq "single: ereport.files" "$cf" "$sf" \
        "ereport file count matches ecrawl for this user’s slice (= whole tree here)"
    expect_eq "single: ereport.directories" "$cd" "$sd" \
        "directory totals match"
    expect_eq "single: ereport.links" "$cs" "$sl" \
        "symlink totals match"
    expect_eq "single: ereport.scanned_records" "$ce" "$sscan" \
        "every crawl row was read for this uid"
    expect_eq "single: ereport.matched_records" "$sscan" "$smat" \
        "single-user mode: all scanned rows match the filter"

    local tb
    tb=$(kv_last total_bytes "$crawl_log")
    expect_eq "single: total_capacity_in_files vs ecrawl.total_bytes" "$tb" "$scap" \
        "byte total matches ecrawl total_bytes"

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
    printf '  %sereport all_users:%s distinct_uids=%s scanned=%s matched=%s\n' "$M" "$Z" "$dist" "$sscan" "$smat"

    section_int "[integration] ereport all-users vs ecrawl"
    expect_eq "all_users: distinct_uids (single uid in crawl)" "1" "$dist" \
        "only one uid appears in this tiny crawl"
    expect_eq "all_users: scanned_records vs ecrawl.entries" "$ce" "$sscan" \
        "all-users reads every record"
    expect_eq "all_users: matched_records vs scanned" "$sscan" "$smat" \
        "all-users keeps every row (matched == scanned)"

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
