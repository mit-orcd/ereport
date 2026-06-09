#!/usr/bin/env bash
#
# Verification harness: ecrawl + ereport integration, smoke tests for ecrawl_repair / ecrawl_analyze /
# edelete / ereport_index, optional live-tree correlation with find/fd.
#
# Usage:
#   ./test.sh                      # integration + tool smoke tests (temp tree + built binaries)
#   ./test.sh --edelete-only       # edelete smoke + synthetic probes only (needs ./edelete built)
#   ./test.sh /path/to/tree        # above + filesystem correlation for that root
#   SKIP_FS=1 ./test.sh /path      # integration only (ignore arg for fs checks)
#   ECRAWL=/abs/ecrawl EREPORT=/abs/ereport ./test.sh
#   ECRAWL_REPAIR ECRAWL_ANALYZE EDELETE EREPORT_INDEX override those binaries (same dir as this script by default).
#
# Requires: bash, coreutils, all Makefile targets built (default: same directory as this script).

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ECRAWL="${ECRAWL:-$SCRIPT_DIR/ecrawl}"
EREPORT="${EREPORT:-$SCRIPT_DIR/ereport}"
ECRAWL_REPAIR="${ECRAWL_REPAIR:-$SCRIPT_DIR/ecrawl_repair}"
ECRAWL_ANALYZE="${ECRAWL_ANALYZE:-$SCRIPT_DIR/ecrawl_analyze}"
EDELETE="${EDELETE:-$SCRIPT_DIR/edelete}"
EREPORT_INDEX="${EREPORT_INDEX:-$SCRIPT_DIR/ereport_index}"

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

# --- summary mode (--summary): collect results into copy/paste-friendly tables ---
SUMMARY=0
SUMMARY_FAILS=0
SUMMARY_RENDERED=0
CUR_GROUP="general"
declare -a SUM_ROWS=()
declare -a SUM_METRICS=()
exec 3>&1   # fd 3 = original stdout; the summary table goes here even when test chatter is muted

summary_add()    { SUM_ROWS+=("${1}"$'\037'"${CUR_GROUP}"$'\037'"${2}"$'\037'"${3:-}"); }
summary_metric() { SUM_METRICS+=("${1}"$'\037'"${2:-}"); }

# Box-table renderer (plain text, no ANSI): a Metrics table + a Checks table on fd 3.
render_summary() {
    [[ "$SUMMARY" == 1 ]] || return 0
    [[ "$SUMMARY_RENDERED" == 0 ]] || return 0
    SUMMARY_RENDERED=1
    {
        printf '\n===== test.sh summary (%s) =====\n\n' "$(date '+%Y-%m-%d %H:%M:%S')"
        if [[ ${#SUM_METRICS[@]} -gt 0 ]]; then
            printf '%s\n' "${SUM_METRICS[@]}" | awk -F'\037' '
            function rep(c,n,  s,i){s="";for(i=0;i<n;i++)s=s c;return s}
            { k[NR]=$1; v[NR]=$2; if(length($1)>w1)w1=length($1); if(length($2)>w2)w2=length($2) }
            BEGIN{h1="Metric";h2="Value"}
            END{
              if(length(h1)>w1)w1=length(h1); if(length(h2)>w2)w2=length(h2);
              printf "+%s+%s+\n",rep("-",w1+2),rep("-",w2+2);
              printf "| %-*s | %-*s |\n",w1,h1,w2,h2;
              printf "+%s+%s+\n",rep("-",w1+2),rep("-",w2+2);
              for(i=1;i<=NR;i++) printf "| %-*s | %-*s |\n",w1,k[i],w2,v[i];
              printf "+%s+%s+\n",rep("-",w1+2),rep("-",w2+2);
            }'
            printf '\n'
        fi
        if [[ ${#SUM_ROWS[@]} -gt 0 ]]; then
            printf '%s\n' "${SUM_ROWS[@]}" | awk -F'\037' '
            function rep(c,n,  s,i){s="";for(i=0;i<n;i++)s=s c;return s}
            { r[NR]=$1; g[NR]=$2; c[NR]=$3; d[NR]=$4;
              if(length($1)>w1)w1=length($1); if(length($2)>w2)w2=length($2);
              if(length($3)>w3)w3=length($3); if(length($4)>w4)w4=length($4);
              if($1=="PASS")np++; else if($1=="SKIP")ns++; else nf++ }
            BEGIN{h1="Result";h2="Phase";h3="Check";h4="Detail"}
            END{
              if(length(h1)>w1)w1=length(h1); if(length(h2)>w2)w2=length(h2);
              if(length(h3)>w3)w3=length(h3); if(length(h4)>w4)w4=length(h4);
              printf "+%s+%s+%s+%s+\n",rep("-",w1+2),rep("-",w2+2),rep("-",w3+2),rep("-",w4+2);
              printf "| %-*s | %-*s | %-*s | %-*s |\n",w1,h1,w2,h2,w3,h3,w4,h4;
              printf "+%s+%s+%s+%s+\n",rep("-",w1+2),rep("-",w2+2),rep("-",w3+2),rep("-",w4+2);
              for(i=1;i<=NR;i++) printf "| %-*s | %-*s | %-*s | %-*s |\n",w1,r[i],w2,g[i],w3,c[i],w4,d[i];
              printf "+%s+%s+%s+%s+\n",rep("-",w1+2),rep("-",w2+2),rep("-",w3+2),rep("-",w4+2);
              printf "\nPassed: %d   Failed: %d   Skipped: %d   Total: %d\n", np+0, nf+0, ns+0, NR;
            }'
        fi
    } >&3
}

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

die() {
    printf '%sFAIL:%s %s%s\n' "$R" "$Z" "$*" "$Z" >&2
    if [[ "${SUMMARY:-0}" == 1 ]]; then
        summary_add FAIL "FATAL: $*" "aborted"
        render_summary
    fi
    exit 1
}
pass() { summary_add PASS "$*" "passed"; printf '%sOK:%s %s%s\n' "$G" "$Z" "$*" "$Z"; }

# Section titles: cyan bold for filesystem correlation groups [1]–[4]
section_fs() {
    local n
    n=$(printf '%s' "$1" | sed -n 's/^\[\([^]]*\)\].*/\1/p')
    CUR_GROUP="fs[${n:-?}]"
    [[ "$SUMMARY" == 1 ]] || printf '\n  %s%s%s%s\n' "$C" "$BD" "$1" "$Z"
}
# Yellow bold for integration subsection titles
section_int() {
    CUR_GROUP="integration"
    [[ "$SUMMARY" == 1 ]] || printf '\n  %s%s%s%s\n' "$Y" "$BD" "$1" "$Z"
}

# Last line wins (tools may print stats blocks more than once in verbose modes).
kv_last() {
    local key=$1 file=$2
    grep "^${key}=" "$file" 2>/dev/null | tail -n1 | cut -d= -f2-
}

# Monotonic wall seconds (fractional when date supports %N).
now_sec() {
    date +%s.%N 2>/dev/null || awk -v s="$SECONDS" 'BEGIN{printf "%.3f", s + 0}'
}

# Sets _timed_result and _timed_sec (wall seconds for "$@").
run_timed() {
    local t0 t1
    t0=$(now_sec)
    _timed_result=$("$@")
    t1=$(now_sec)
    _timed_sec=$(LC_ALL=C awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", b - a}')
}

# Speedup line: ecrawl wall vs summed baseline walk steps (both wall seconds).
format_speedup() {
    local my=$1 base=$2
    LC_ALL=C awk -v my="$my" -v base="$base" '
    BEGIN {
        my += 0; base += 0
        if (my <= 0 || base <= 0) { print "n/a"; exit }
        if (my < base) printf "%.1fx faster (ecrawl %.3fs vs baseline %.3fs)", base / my, my, base
        else if (my > base) printf "%.1fx slower (ecrawl %.3fs vs baseline %.3fs)", my / base, my, base
        else printf "same wall time (ecrawl %.3fs)", my
    }'
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
    if [[ "$got" == "$want" ]]; then
        summary_add PASS "$label" "want=$want got=$got"
        if [[ -n "$ok_note" ]]; then
            printf '  %sOK:%s %s — %s%s\n' "$G" "$Z" "$label" "$ok_note" "$Z"
        fi
        return 0
    fi
    summary_add FAIL "$label" "want=$want got=$got"
    if [[ "$SUMMARY" == 1 ]]; then
        printf '  %sFAIL:%s %s: want %s got %s%s\n' "$R" "$Z" "$label" "$want" "$got" "$Z" >&2
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
        return 0
    fi
    die "${label}: want '${want}' got '${got}'"
}

# Like expect_eq but records failure and continues (for fs correlation so every check is printed).
expect_eq_continue() {
    local label=$1 want=$2 got=$3
    local ok_note=${4:-}
    if [[ "$got" != "$want" ]]; then
        summary_add FAIL "$label" "want=$want got=$got"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
        if [[ "$want" =~ ^[0-9]+$ ]] && [[ "$got" =~ ^[0-9]+$ ]]; then
            printf '  %sFAIL:%s %s: want %s got %s (%s)%s\n' "$R" "$Z" "$label" "$want" "$got" "$(fs_fail_delta "$want" "$got")" "$Z" >&2
        else
            printf '  %sFAIL:%s %s: want %s got %s%s\n' "$R" "$Z" "$label" "$want" "$got" "$Z" >&2
        fi
        return 1
    fi
    summary_add PASS "$label" "want=$want got=$got"
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
        summary_add FAIL "$label" "want<=$ceiling got=$got"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
        printf '  %sFAIL:%s %s: want <= %s got %s%s\n' "$R" "$Z" "$label" "$ceiling" "$got" "$Z" >&2
        return 1
    fi
    summary_add PASS "$label" "want<=$ceiling got=$got"
    if [[ -n "$ok_note" ]]; then
        printf '  %sOK:%s %s — %s%s\n' "$G" "$Z" "$label" "$ok_note" "$Z"
    fi
    return 0
}

need_exe() {
    [[ -x "$1" ]] || die "missing or not executable: $1 (run 'make' in ${SCRIPT_DIR})"
}

# Synthetic edelete checks: "." / ".." as argv path components, cwd ".", and dot-prefixed directory names.
# Ensures we never rely on deleting the literal dot-directory entries from readdir (tested in edelete.c)
# and that path normalization / containment behave as expected.
edelete_synthetic_dot_dotdot_tests() {
    local td_root=$1
    local base out err ef

    base="${td_root}/edelete_dot_probe"
    rm -rf "$base"
    mkdir -p "$base"

    log "edelete synthetic: probe tree under ${base}"

    # t1: start path is "." resolved from inside the deepest directory (realpath → absolute).
    mkdir -p "${base}/t1_rel_dot/deep"
    echo x >"${base}/t1_rel_dot/deep/f.txt"
    out="${base}/t1.stdout"
    err="${base}/t1.stderr"
    (cd "${base}/t1_rel_dot/deep" && "$EDELETE" --delete --force . >"$out" 2>"$err") || {
        cat "$err" >&2 || true
        die "edelete dots: t1 (cwd .) failed"
    }
    [[ ! -f "${base}/t1_rel_dot/deep/f.txt" ]] || die "edelete dots: t1 file should be removed"
    [[ ! -d "${base}/t1_rel_dot/deep" ]] || die "edelete dots: t1 leaf dir should be removed"
    ef=$(kv_last errors "$out")
    expect_eq "edelete dots: t1 errors" "0" "${ef:-missing_errors_line}"

    # t2: redundant "./" segments in the start path argument.
    mkdir -p "${base}/t2_dotslash/walk/inner"
    echo y >"${base}/t2_dotslash/walk/inner/g.txt"
    out="${base}/t2.stdout"
    "$EDELETE" --delete --force "${base}/t2_dotslash/walk/./inner/./" >"$out" 2>"${base}/t2.stderr" || die "edelete dots: t2 (./ segments) failed"
    [[ ! -e "${base}/t2_dotslash/walk/inner/g.txt" ]] || die "edelete dots: t2 file should be removed"
    expect_eq "edelete dots: t2 errors" "0" "$(kv_last errors "$out")"

    # t3: ".." in argv collapsing to the intended directory (bash resolves before edelete runs).
    mkdir -p "${base}/t3_dotdot/norm/a/b"
    echo z >"${base}/t3_dotdot/norm/a/b/h.txt"
    out="${base}/t3.stdout"
    "$EDELETE" --delete --force "${base}/t3_dotdot/norm/a/b/../.." >"$out" 2>"${base}/t3.stderr" || die "edelete dots: t3 (.. collapse) failed"
    [[ ! -d "${base}/t3_dotdot/norm" ]] || die "edelete dots: t3 norm/ should be removed"
    expect_eq "edelete dots: t3 errors" "0" "$(kv_last errors "$out")"

    # t4: deleting one subtree must not remove a sibling (exercises containment vs .. semantics).
    mkdir -p "${base}/t4_sibling/keep" "${base}/t4_sibling/delete_me/sub"
    echo keep >"${base}/t4_sibling/keep/preserved.txt"
    echo x >"${base}/t4_sibling/delete_me/sub/x.txt"
    out="${base}/t4.stdout"
    "$EDELETE" --delete --force "${base}/t4_sibling/delete_me" >"$out" 2>"${base}/t4.stderr" || die "edelete dots: t4 sibling containment failed"
    [[ -f "${base}/t4_sibling/keep/preserved.txt" ]] || die "edelete dots: t4 sibling file must survive"
    [[ ! -e "${base}/t4_sibling/delete_me" ]] || die "edelete dots: t4 delete_me subtree should be gone"
    expect_eq "edelete dots: t4 errors" "0" "$(kv_last errors "$out")"

    # t5: dot-prefixed directory names (not the special "." / ".." entries) are visited and removed.
    mkdir -p "${base}/t5_dotnames/.hidden/deep"
    echo h >"${base}/t5_dotnames/.hidden/deep/h.txt"
    out="${base}/t5.stdout"
    "$EDELETE" --delete --force "${base}/t5_dotnames" >"$out" 2>"${base}/t5.stderr" || die "edelete dots: t5 .hidden dir failed"
    [[ ! -d "${base}/t5_dotnames/.hidden" ]] || die "edelete dots: t5 .hidden should be removed"
    expect_eq "edelete dots: t5 errors" "0" "$(kv_last errors "$out")"

    # t6: dry-run with "./" and ".." only in argv path — tree must remain untouched.
    mkdir -p "${base}/t6_dry/sub"
    echo d >"${base}/t6_dry/sub/file.txt"
    out="${base}/t6.stdout"
    "$EDELETE" "${base}/t6_dry/./sub/../" >"$out" 2>"${base}/t6.stderr" || die "edelete dots: t6 dry-run failed"
    [[ -f "${base}/t6_dry/sub/file.txt" ]] || die "edelete dots: t6 dry-run must keep files"
    expect_eq "edelete dots: t6 mode dry-run" "dry-run" "$(kv_last mode "$out")"
    expect_eq "edelete dots: t6 deleted_files stays 0" "0" "$(kv_last deleted_files "$out")"

    # t7: legal names that start with "." but are not "." or ".." (e.g. .local).
    mkdir -p "${base}/t7_dotprefix/.local/bin"
    echo p >"${base}/t7_dotprefix/.local/bin/p.txt"
    out="${base}/t7.stdout"
    "$EDELETE" --delete --force "${base}/t7_dotprefix" >"$out" 2>"${base}/t7.stderr" || die "edelete dots: t7 .local failed"
    [[ ! -f "${base}/t7_dotprefix/.local/bin/p.txt" ]] || die "edelete dots: t7 dot-prefix path should be deleted"
    expect_eq "edelete dots: t7 errors" "0" "$(kv_last errors "$out")"

    pass "edelete synthetic (. .. argv paths, cwd ., dot-named dirs)"
}

edelete_uid_gid_filter_tests() {
    local td_root=$1
    local base out my_uid my_gid other_uid other_gid

    base="${td_root}/edelete_uid_gid"
    rm -rf "$base"
    mkdir -p "$base/mixed"

    my_uid=$(id -u)
    my_gid=$(id -g)
    other_uid=$((my_uid + 1))
    other_gid=$((my_gid + 1))

    echo mine >"${base}/mixed/mine.txt"
    echo other >"${base}/mixed/other.txt"
    chown "${my_uid}:${my_gid}" "${base}/mixed/mine.txt"
    chown "${other_uid}:${other_gid}" "${base}/mixed/other.txt" 2>/dev/null || {
        log "edelete uid/gid: skip (cannot chown to ${other_uid}:${other_gid})"
        return 0
    }

    out="${base}/uid.stdout"
    "$EDELETE" --delete --force --uid "$my_uid" "${base}/mixed" >"$out" 2>"${base}/uid.stderr" || die "edelete uid filter failed"
    [[ -f "${base}/mixed/other.txt" ]] || die "edelete uid filter: other.txt should remain"
    [[ ! -f "${base}/mixed/mine.txt" ]] || die "edelete uid filter: mine.txt should be removed"
    expect_eq "edelete uid filter errors" "0" "$(kv_last errors "$out")"

    echo mine2 >"${base}/mixed/mine2.txt"
    echo other2 >"${base}/mixed/other2.txt"
    chown "${my_uid}:${my_gid}" "${base}/mixed/mine2.txt"
    chown "${other_uid}:${other_gid}" "${base}/mixed/other2.txt"

    out="${base}/gid.stdout"
    "$EDELETE" --delete --force --gid "$other_gid" "${base}/mixed" >"$out" 2>"${base}/gid.stderr" || die "edelete gid filter failed"
    [[ -f "${base}/mixed/mine2.txt" ]] || die "edelete gid filter: mine2.txt should remain"
    [[ ! -f "${base}/mixed/other2.txt" ]] || die "edelete gid filter: other2.txt should be removed"
    expect_eq "edelete gid filter errors" "0" "$(kv_last errors "$out")"

    echo both >"${base}/mixed/both.txt"
    chown "${my_uid}:${other_gid}" "${base}/mixed/both.txt"

    out="${base}/both.stdout"
    "$EDELETE" --delete --force --uid "$my_uid" --gid "$other_gid" "${base}/mixed" >"$out" 2>"${base}/both.stderr" || die "edelete uid+gid filter failed"
    [[ ! -f "${base}/mixed/both.txt" ]] || die "edelete uid+gid filter: both.txt should be removed"
    expect_eq "edelete uid+gid filter errors" "0" "$(kv_last errors "$out")"

    pass "edelete --uid / --gid ownership filters"
}

# Negative ("must not delete") invariants that need no special privileges:
#   - symlinks are unlinked, but their targets outside the start tree are never followed/removed;
#   - the age filter never deletes entries newer than the threshold;
#   - --delete never ascends above the start path (siblings / parents survive).
edelete_safety_negative_tests() {
    local td_root=$1
    local base out

    base="${td_root}/edelete_safety"
    rm -rf "$base"

    # n1: symlinks under the start path point outside it; only the links may go, never the targets.
    mkdir -p "${base}/n1/tree" "${base}/n1/outside/dir_keep"
    echo keep >"${base}/n1/outside/file_keep.txt"
    echo inner >"${base}/n1/outside/dir_keep/inner.txt"
    ln -s ../outside/file_keep.txt "${base}/n1/tree/flink"
    ln -s ../outside/dir_keep "${base}/n1/tree/dlink"
    out="${base}/n1.stdout"
    "$EDELETE" --delete --force "${base}/n1/tree" >"$out" 2>"${base}/n1.stderr" || die "edelete safety: n1 symlink walk failed"
    [[ -f "${base}/n1/outside/file_keep.txt" ]] || die "edelete safety: n1 symlink target file must survive"
    [[ -d "${base}/n1/outside/dir_keep" ]] || die "edelete safety: n1 symlinked dir must survive (no follow)"
    [[ -f "${base}/n1/outside/dir_keep/inner.txt" ]] || die "edelete safety: n1 contents under symlinked dir must survive"
    expect_eq "edelete safety: n1 errors" "0" "$(kv_last errors "$out")"

    # n2: age filter must keep entries newer than the threshold (only the backdated file is eligible).
    mkdir -p "${base}/n2/agetree"
    echo old >"${base}/n2/agetree/old.txt"
    echo fresh >"${base}/n2/agetree/fresh.txt"
    if touch -d "100 days ago" "${base}/n2/agetree/old.txt" 2>/dev/null; then
        out="${base}/n2.stdout"
        "$EDELETE" --delete --force mtime 30 "${base}/n2/agetree" >"$out" 2>"${base}/n2.stderr" || die "edelete safety: n2 age filter failed"
        [[ ! -f "${base}/n2/agetree/old.txt" ]] || die "edelete safety: n2 old.txt (>=30d) should be removed"
        [[ -f "${base}/n2/agetree/fresh.txt" ]] || die "edelete safety: n2 fresh.txt (<30d) must survive"
        expect_eq "edelete safety: n2 deleted_files" "1" "$(kv_last deleted_files "$out")"
        expect_eq "edelete safety: n2 errors" "0" "$(kv_last errors "$out")"
    else
        log "edelete safety: skip n2 (touch -d not supported)"
    fi

    # n3: deleting a leaf subtree must never touch a parent file or a sibling subtree.
    mkdir -p "${base}/n3/keep_sibling" "${base}/n3/delete_me/sub"
    echo parent >"${base}/n3/parent_file.txt"
    echo sib >"${base}/n3/keep_sibling/s.txt"
    echo gone >"${base}/n3/delete_me/sub/g.txt"
    out="${base}/n3.stdout"
    "$EDELETE" --delete --force "${base}/n3/delete_me" >"$out" 2>"${base}/n3.stderr" || die "edelete safety: n3 containment failed"
    [[ ! -e "${base}/n3/delete_me" ]] || die "edelete safety: n3 target subtree should be gone"
    [[ -f "${base}/n3/parent_file.txt" ]] || die "edelete safety: n3 parent file must survive"
    [[ -f "${base}/n3/keep_sibling/s.txt" ]] || die "edelete safety: n3 sibling subtree must survive"
    expect_eq "edelete safety: n3 errors" "0" "$(kv_last errors "$out")"

    pass "edelete safety negatives (symlink targets, age freshness, containment)"
}

# Optional args: temp dir and walk root from run_integration; when omitted, creates its own tree.
run_edelete_tests() {
    local td=${1:-} root_abs=${2:-}
    local own_temp=0

    if [[ -z "$td" ]]; then
        own_temp=1
        log "edelete test suite (synthetic tree)"
        td=$(mktemp -d "${TMPDIR:-/tmp}/ereport_edelete.XXXXXX")
        cleanup_edelete() {
            rm -rf "$td"
        }
        trap cleanup_edelete EXIT
        mkdir -p "${td}/walk/sub"
        echo hello >"${td}/walk/a.txt"
        echo world >"${td}/walk/sub/b.txt"
        root_abs=$(cd "${td}/walk" && pwd)
    fi

    section_int "[edelete] dry-run smoke"
    log "edelete dry-run (synthetic walk tree)"
    "$EDELETE" "$root_abs" >"${td}/edelete.stdout" 2>"${td}/edelete.stderr" || {
        tail -n 40 "${td}/edelete.stderr" >&2 || true
        die "edelete dry-run failed"
    }
    summary_add PASS "edelete --dry-run" "ran ok"

    section_int "[edelete] synthetic . / .. path probes (--delete)"
    edelete_synthetic_dot_dotdot_tests "$td"

    section_int "[edelete] uid/gid ownership filters"
    edelete_uid_gid_filter_tests "$td"

    section_int "[edelete] safety negatives (must-not-delete invariants)"
    edelete_safety_negative_tests "$td"

    if [[ "$own_temp" == 1 ]]; then
        trap - EXIT
        cleanup_edelete
        pass "edelete test suite"
    fi
}

# --- Optional: correlate crawl totals with find/fd on a real tree (slow on huge trees) ---
# find(1) exits 1 if any path errors (e.g. permission denied); with pipefail + set -e that would kill
# the script mid-baseline with no message. Ignore find/fd failure after collecting stdout.
count_files() {
    local root=$1
    if command -v fd >/dev/null 2>&1; then
        { fd --hidden --no-ignore -t f . "$root" 2>/dev/null || true; } | wc -l | tr -d ' '
    else
        { find "$root" -type f 2>/dev/null || true; } | wc -l | tr -d ' '
    fi
}

count_dirs() {
    local root=$1
    # Always use find: ecrawl counts the crawl root directory when it runs; `fd -t d . ROOT` omits ROOT
    # from results (off-by-one vs ecrawl). `find ROOT -type d` includes ROOT like ecrawl.
    { find "$root" -type d 2>/dev/null || true; } | wc -l | tr -d ' '
}

count_symlinks() {
    local root=$1
    if command -v fd >/dev/null 2>&1; then
        { fd --hidden --no-ignore -t l . "$root" 2>/dev/null || true; } | wc -l | tr -d ' '
    else
        { find "$root" -type l 2>/dev/null || true; } | wc -l | tr -d ' '
    fi
}

sum_unique_regular_bytes() {
    local root=$1
    # Sum st_size once per (dev,inode) so hard links count once, matching ecrawl total_bytes spirit.
    # One awk pass (no sort). RAM scales with unique (dev,inode) keys.
    { find "$root" -type f -printf '%D:%i %s\n' 2>/dev/null || true; } |
        awk '!seen[$1]++ { s += $2 } END { printf "%.0f\n", s + 0 }'
}

run_fs_correlation() {
    local root=$1
    local fs_fail=0
    [[ -d "$root" ]] || die "not a directory: $root"

    log "filesystem correlation (may be slow on large trees): $root"
    log "note: live trees drift between the find/fd snapshot and ecrawl; expect exact match only on quiescent data."
    log "note: same find/fd snapshot is compared to ecrawl and (all-users) ereport — ecrawl vs ereport should match even when fs baseline drifts."
    log "note: each passing check prints OK: <check> — <why it matters>"
    log "note: ecrawl runs before find/fd so our tools see a colder cache; find/fd may benefit from warmed metadata."

    local fc dc lc crawl_files crawl_dirs crawl_symlinks crawl_bytes entries
    local su_files su_dirs su_links su_cap su_scanned su_matched
    local au_files au_dirs au_links au_cap au_scanned au_matched au_distinct
    local fc_sec dc_sec lc_sec fs_u_bytes_sec fs_baseline_sec crawl_elapsed
    local fs_walk_files
    if command -v fd >/dev/null 2>&1; then
        fs_walk_files=fd
    else
        fs_walk_files=find
    fi

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

    log "step: ecrawl → ${crawl_out} (first — cold cache)"
    ECRAWL_CRAWL_THREADS="${ECRAWL_CRAWL_THREADS:-8}" \
        "$ECRAWL" "$root_abs" "$crawl_out" >"$crawl_log" 2>&1 || {
        tail -n 40 "$crawl_log" >&2 || true
        die "ecrawl failed"
    }

    crawl_files=$(kv_last files "$crawl_log")
    crawl_dirs=$(kv_last dirs "$crawl_log")
    crawl_symlinks=$(kv_last symlinks "$crawl_log")
    crawl_bytes=$(kv_last total_bytes "$crawl_log")
    crawl_elapsed=$(kv_last elapsed_sec "$crawl_log")

    printf '  %secrawl:%s  files=%s dirs=%s symlinks=%s total_bytes=%s elapsed_sec=%s\n' "$M" "$Z" \
        "$crawl_files" "$crawl_dirs" "$crawl_symlinks" "$crawl_bytes" "${crawl_elapsed:-?}"
    summary_metric "fs root" "$root_abs"
    summary_metric "fs ecrawl: files/dirs/symlinks" "${crawl_files}/${crawl_dirs}/${crawl_symlinks}"
    summary_metric "fs ecrawl: total_bytes / elapsed_sec" "${crawl_bytes} / ${crawl_elapsed:-?}"

    log "step: baseline — counting files/dirs/symlinks, then unique regular-file bytes (find | awk dedup; after ecrawl)"
    run_timed count_files "$root"
    fc="$_timed_result"
    fc_sec="$_timed_sec"
    run_timed count_dirs "$root"
    dc="$_timed_result"
    dc_sec="$_timed_sec"
    run_timed count_symlinks "$root"
    lc="$_timed_result"
    lc_sec="$_timed_sec"
    run_timed sum_unique_regular_bytes "$root"
    fs_u_bytes="$_timed_result"
    fs_u_bytes_sec="$_timed_sec"
    fs_baseline_sec=$(LC_ALL=C awk -v a="$fc_sec" -v b="$dc_sec" -v c="$lc_sec" -v d="$fs_u_bytes_sec" \
        'BEGIN{printf "%.3f", a + b + c + d}')

    printf '  %sfs baseline:%s files=%s dirs=%s symlinks=%s unique_regular_bytes=%s\n' "$M" "$Z" "$fc" "$dc" "$lc" "$fs_u_bytes"
    summary_metric "fs baseline (${fs_walk_files}): files/dirs/symlinks" "${fc}/${dc}/${lc}"
    if [[ "$fs_walk_files" == fd ]]; then
        printf '%s           walk: files=fd dirs=find symlinks=fd unique_regular_bytes=find (dirs incl. crawl root; fd: --hidden --no-ignore)%s\n' "$D" "$Z"
    else
        printf '%s           walk: files=find dirs=find symlinks=find unique_regular_bytes=find (dirs incl. crawl root)%s\n' "$D" "$Z"
    fi
    printf '%s           wall_sec: files=%s dirs=%s symlinks=%s unique_regular_bytes=%s total=%s%s\n' \
        "$D" "$fc_sec" "$dc_sec" "$lc_sec" "$fs_u_bytes_sec" "$fs_baseline_sec" "$Z"
    if [[ -n "${crawl_elapsed:-}" ]]; then
        printf '  %sspeed (files+dirs+symlinks+bytes):%s ecrawl — %s%s\n' \
            "$G" "$Z" "$(format_speedup "$crawl_elapsed" "$fs_baseline_sec")" "$Z"
    fi

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
    # That shard still holds every uid that hashes to the same slot (uid % uid_shards collides), so
    # scanned_records counts all rows read from the file while matched_records counts only rows for the
    # target uid — expect matched <= scanned, not equality, on large multi-owner trees.
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
    summary_metric "fs ereport all-users: scanned/matched" "${au_scanned}/${au_matched}"
    summary_metric "fs ereport all-users: distinct_uids" "$au_distinct"
    summary_metric "fs ereport single-user" "$([[ "$skip_single" -eq 0 ]] && echo "ran ($(id -un))" || echo "skipped (no shard)")"
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
        expect_le_continue "single-user: matched_records <= scanned_records" "$su_scanned" "$su_matched" \
            "shard file contains all uids mapping to that slot; matched counts only target uid" || fs_fail=1
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
    if [[ "$fs_fail" -ne 0 ]]; then
        # In summary mode the per-check FAILs are already recorded and counted;
        # let main render the table and set the exit code instead of dying here.
        [[ "$SUMMARY" == 1 ]] && return 0
        die "filesystem correlation had mismatches for $root (see FAIL lines above)"
    fi
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
    ECRAWL_CRAWL_THREADS="${ECRAWL_CRAWL_THREADS:-4}" \
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
    summary_metric "synthetic ecrawl: entries" "$ce"
    summary_metric "synthetic ecrawl: files/dirs/symlinks" "${cf}/${cd}/${cs}"

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
    summary_metric "synthetic ereport all-users: distinct_uids" "$dist"
    summary_metric "synthetic ereport all-users: scanned/matched" "${sscan}/${smat}"

    section_int "[integration] ereport all-users vs ecrawl"
    expect_eq "all_users: distinct_uids (single uid in crawl)" "1" "$dist" \
        "only one uid appears in this tiny crawl"
    expect_eq "all_users: scanned_records vs ecrawl.entries" "$ce" "$sscan" \
        "all-users reads every record"
    expect_eq "all_users: matched_records vs scanned" "$sscan" "$smat" \
        "all-users keeps every row (matched == scanned)"

    section_int "[integration] ecrawl_repair, ecrawl_analyze, edelete, ereport_index (smoke)"

    log "ecrawl_repair --dry-run (sidecar rules vs crawl_bin_load_ckpt)"
    "$ECRAWL_REPAIR" --dry-run "$crawl_out" >"${td}/repair.stdout" 2>"${td}/repair.stderr" || {
        tail -n 40 "${td}/repair.stderr" >&2 || true
        die "ecrawl_repair --dry-run failed"
    }
    summary_add PASS "ecrawl_repair --dry-run" "ran ok"

    log "ecrawl_analyze (shard scan / parent histogram smoke)"
    "$ECRAWL_ANALYZE" --top 5 "$crawl_out" >"${td}/analyze.stdout" 2>"${td}/analyze.stderr" || {
        tail -n 40 "${td}/analyze.stderr" >&2 || true
        die "ecrawl_analyze failed"
    }
    grep -q '^ecrawl_analyze$' "${td}/analyze.stdout" || die "ecrawl_analyze missing banner line"
    grep -q '^records_total=' "${td}/analyze.stdout" || die "ecrawl_analyze missing records_total"
    grep -q '^parse_chunk_jobs=' "${td}/analyze.stdout" || die "ecrawl_analyze missing parse_chunk_jobs"
    summary_add PASS "ecrawl_analyze smoke" "banner+records_total+parse_chunk_jobs"

    run_edelete_tests "$td" "$root_abs"

    local idx_make="${td}/index_make"
    log "ereport_index --make (trigram index under ${idx_make})"
    EREPORT_INDEX_THREADS="${EREPORT_INDEX_THREADS:-4}" \
        "$EREPORT_INDEX" --make --index-dir "$idx_make" "$(id -un)" "$crawl_out" >"${td}/ei.stdout" 2>"${td}/ei.stderr" || {
        tail -n 80 "${td}/ei.stderr" >&2 || true
        die "ereport_index --make failed"
    }
    [[ -f "${idx_make}/tri_keys.bin" && -f "${idx_make}/paths.bin" ]] ||
        die "ereport_index did not write tri_keys.bin / paths.bin under ${idx_make}"
    summary_add PASS "ereport_index --make" "tri_keys.bin+paths.bin written"

    trap - EXIT
    cleanup_int
    pass "integration test"
}

# --- main ---
ROOT=""
EDELETE_ONLY=0
for arg in "$@"; do
    case "$arg" in
        --summary) SUMMARY=1 ;;
        --edelete-only) EDELETE_ONLY=1 ;;
        -h|--help)
            cat <<EOF
Usage: $0 [--summary] [--edelete-only] [/path/to/tree]
  --summary       collect results into copy/paste-friendly box tables (Metrics + Checks)
                  printed at the end (no ANSI; pastes into slides / screenshots cleanly).
  --edelete-only  run edelete smoke + synthetic probes only (requires ./edelete; no ecrawl/ereport).
  /path/...       optional crawl root for filesystem correlation (SKIP_FS=1 to skip).
EOF
            exit 0
            ;;
        -*) die "unknown option: $arg (try --help)" ;;
        *) ROOT="$arg" ;;
    esac
done

if [[ "$EDELETE_ONLY" == 1 && -n "$ROOT" ]]; then
    die "--edelete-only cannot be combined with a filesystem correlation root path"
fi

# In --summary mode, mute the per-check chatter on stdout (the table is emitted
# to fd 3 at the end); FAIL lines and fatal errors still go to stderr.
run_phase() {
    if [[ "$SUMMARY" == 1 ]]; then "$@" >/dev/null; else "$@"; fi
}

if [[ "$EDELETE_ONLY" == 1 ]]; then
    need_exe "$EDELETE"
    run_phase run_edelete_tests
    render_summary
    exit $(( SUMMARY_FAILS > 0 ? 1 : 0 ))
fi

need_exe "$ECRAWL"
need_exe "$EREPORT"
need_exe "$ECRAWL_REPAIR"
need_exe "$EDELETE"
need_exe "$EREPORT_INDEX"

run_phase run_integration

if [[ -n "${SKIP_FS:-}" ]] || [[ -z "${ROOT}" ]] || [[ ! -d "${ROOT}" ]]; then
    if [[ -n "${ROOT}" ]] && [[ ! -d "${ROOT}" ]]; then
        die "not a directory: ${ROOT}"
    fi
    if [[ -z "${SKIP_FS:-}" ]] && [[ -z "${ROOT}" ]] && [[ "$SUMMARY" != 1 ]]; then
        log "tip: pass a crawl root directory as \$1 to run filesystem correlation (SKIP_FS=1 to skip)"
    fi
else
    run_phase run_fs_correlation "$ROOT"
fi

render_summary
exit $(( SUMMARY_FAILS > 0 ? 1 : 0 ))
