#!/usr/bin/env bash
#
# Verification harness: ecrawl + ereport integration, smoke tests for ecrawl_repair / ecrawl_query /
# edelete / ereport_index, optional live-tree correlation with find/fd.
#
# Usage:
#   ./scripts/test/test.sh                      # integration + tool smoke tests (temp tree + built binaries)
#   ./scripts/test/test.sh --edelete-only       # edelete smoke + synthetic probes only (needs ./edelete built)
#   ./scripts/test/test.sh /path/to/tree        # above + filesystem correlation for that root
#   SKIP_FS=1 ./scripts/test/test.sh /path      # integration only (ignore arg for fs checks)
#   ECRAWL=/abs/ecrawl EREPORT=/abs/ereport ./scripts/test/test.sh
#   ECRAWL_REPAIR ECRAWL_QUERY EDELETE EREPORT_INDEX ECRAWL_MOUNT override those binaries (repo root by default).
#   SKIP_FUSE=1 ./scripts/test/test.sh          # skip the ecrawl_mount live-mount comparison (index check still runs)
#
# Requires: bash, coreutils, all Makefile targets built (default: the repo root, two levels up).

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
ECRAWL="${ECRAWL:-$REPO_ROOT/ecrawl}"
EREPORT="${EREPORT:-$REPO_ROOT/ereport}"
ECRAWL_REPAIR="${ECRAWL_REPAIR:-$REPO_ROOT/ecrawl_repair}"
ECRAWL_QUERY="${ECRAWL_QUERY:-$REPO_ROOT/ecrawl_query}"
EDELETE="${EDELETE:-$REPO_ROOT/edelete}"
EREPORT_INDEX="${EREPORT_INDEX:-$REPO_ROOT/ereport_index}"
# Optional target: only built when FUSE headers were available (see 'make fuse-headers').
ECRAWL_MOUNT="${ECRAWL_MOUNT:-$REPO_ROOT/ecrawl_mount}"

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
    [[ -x "$1" ]] || die "missing or not executable: $1 (run 'make' in ${REPO_ROOT})"
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
# Mountpoint of the ecrawl_mount smoke test, so the integration cleanup can
# unmount before rm -rf: a live read-only mount under $td would otherwise make
# rm walk the whole mounted view.
EMOUNT_MP=""

emount_unmount() {
    local mp=$1 i
    [[ -n "$mp" ]] || return 0
    for i in 1 2 3 4 5; do
        grep -qF " ${mp} fuse" /proc/mounts 2>/dev/null || { EMOUNT_MP=""; return 0; }
        fusermount -u "$mp" 2>/dev/null && { EMOUNT_MP=""; return 0; }
        sleep 0.3
    done
    printf '%swarn:%s could not unmount %s\n' "$Y" "$Z" "$mp" >&2
    return 1
}

emount_cleanup_hook() { emount_unmount "$EMOUNT_MP"; }

# ecrawl_mount smoke test.
#
# This is the one check in the harness that can compare a crawl against the live
# tree it came from through an ordinary POSIX interface, so it diffs find(1)
# output, per-entry stat fields, and du totals rather than any tool's own
# reporting. --dry-run runs everywhere; the mount half needs a usable /dev/fuse.
# ecrawl --no-stat walks by name only, so find(1) is the reference for the path set and
# `grep -iF` is the exact reference for --contains (case-insensitive substring over the full path).
# Uses its own tree so the entry counts the other integration checks assert stay untouched.
run_ecrawl_no_stat_tests() {
    local td=$1
    local nt="${td}/nostat_walk" out="${td}/nostat.out" ref="${td}/nostat.ref" err="${td}/nostat.err"
    local needle hits lstats unknown

    section_int "[integration] ecrawl --no-stat (names-only walk + --contains)"

    # Mixed case, a needle that spans a '/' boundary, and a needle high in the tree so the
    # whole-subtree short-circuit is exercised.
    mkdir -p "${nt}/Alpha/beta/GAMMA" "${nt}/haystack/needlepark/inner" "${nt}/plain/sub"
    echo x >"${nt}/Alpha/beta/GAMMA/README.md"
    echo x >"${nt}/Alpha/beta/report.TXT"
    echo x >"${nt}/haystack/needlepark/inner/note.txt"
    echo x >"${nt}/plain/sub/slurm-1234.out"
    echo x >"${nt}/plain/sub/SLURM-9999.OUT"
    ln -s ../sub "${nt}/plain/link_sub"
    nt=$(cd "$nt" && pwd -P)

    find "$nt" | LC_ALL=C sort >"$ref"
    ECRAWL_CRAWL_THREADS="${ECRAWL_CRAWL_THREADS:-4}" \
        "$ECRAWL" --no-stat "$nt" 2>"$err" | LC_ALL=C sort >"$out" || die "ecrawl --no-stat failed"
    expect_eq_continue "ecrawl --no-stat: path set equals find" \
        "$(cksum <"$ref" | cut -d' ' -f1)" "$(cksum <"$out" | cut -d' ' -f1)" \
        "$(wc -l <"$ref" | tr -d ' ') paths, byte-identical to find"

    for needle in gamma GAMMA "a/b" "k/n" slurm- .txt zzz_no_match; do
        # grep exits 1 on the deliberate zero-match needle, which set -e would treat as fatal.
        find "$nt" | { grep -iF -- "$needle" || true; } | LC_ALL=C sort >"$ref"
        "$ECRAWL" --no-stat --contains "$needle" "$nt" 2>/dev/null | LC_ALL=C sort >"$out" ||
            die "ecrawl --no-stat --contains '$needle' failed"
        hits=$(wc -l <"$ref" | tr -d ' ')
        expect_eq_continue "ecrawl --contains '${needle}' equals find|grep -iF" \
            "$(cksum <"$ref" | cut -d' ' -f1)" "$(cksum <"$out" | cut -d' ' -f1)" "${hits} hits"
    done

    # The point of the mode: d_type drives recursion, so only the crawl root is ever stat'd.
    "$ECRAWL" --no-stat --verbose "$nt" >/dev/null 2>"$err" || die "ecrawl --no-stat --verbose failed"
    lstats=$(kv_last io_lstat_calls "$err")
    unknown=$(kv_last dtype_unknown_fallbacks "$err")
    expect_le_continue "ecrawl --no-stat: inode reads" 1 "${lstats:-999}" "io_lstat_calls=${lstats} (root only)"
    if [[ "$unknown" != "0" ]]; then
        summary_add SKIP "ecrawl --no-stat: d_type available" "dtype_unknown_fallbacks=${unknown}"
        log "note: filesystem does not report d_type; ${unknown} stat fallbacks"
    else
        summary_add PASS "ecrawl --no-stat: d_type available" "dtype_unknown_fallbacks=0"
    fi

    # stdout must stay a clean path list so `ecrawl --no-stat | sort` works.
    if "$ECRAWL" --no-stat --verbose "$nt" 2>/dev/null | grep -qE '^[a-z_]+='; then
        summary_add FAIL "ecrawl --no-stat: summary kept off stdout" "key=value lines leaked into the path stream"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
        printf '  %sFAIL:%s ecrawl --no-stat leaked summary lines into stdout%s\n' "$R" "$Z" "$Z" >&2
    else
        summary_add PASS "ecrawl --no-stat: summary kept off stdout" "paths only"
    fi

    # --no-stat cannot write a capture (no uid/size/inode/mtime), and --contains needs the walk.
    if "$ECRAWL" --no-stat "$nt" "${td}/nostat_out" >/dev/null 2>&1; then
        summary_add FAIL "ecrawl --no-stat rejects an output dir" "accepted"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
    else
        summary_add PASS "ecrawl --no-stat rejects an output dir" "rejected"
    fi
    if "$ECRAWL" --contains foo "$nt" >/dev/null 2>&1; then
        summary_add FAIL "ecrawl --contains requires --no-stat" "accepted"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
    else
        summary_add PASS "ecrawl --contains requires --no-stat" "rejected"
    fi

    rm -rf "$nt" "$out" "$ref" "$err"
    pass "ecrawl --no-stat + --contains"
}

# --statx / --iouring swap the inode-read path (and batch it); the walk's answers and the
# capture's content must be identical to the fstatat baseline. Shard bytes are order-independent
# (record sizes do not depend on emit order), so they are compared exactly; crawl_manifest.txt is
# excluded (it embeds the wall clock).
run_ecrawl_statx_tests() {
    local td=$1
    local st="${td}/statx_walk" err="${td}/statx.err"
    local modes="base statx iouring" m keys k v_base v_new mismatch shard_base shard_new

    section_int "[integration] ecrawl --statx / --iouring (inode-read variants)"

    mkdir -p "${st}/a/b" "${st}/c"
    echo one >"${st}/a/one.txt"
    echo twenty-two-characters >"${st}/a/b/two.txt"
    : >"${st}/c/empty"
    ln "${st}/a/one.txt" "${st}/c/one-hardlink"
    ln -s ../a "${st}/c/link_a"
    st=$(cd "$st" && pwd -P)

    # If this build has no io_uring support the flag must refuse cleanly; skip those legs.
    if ! "$ECRAWL" --no-write --iouring "$st" >/dev/null 2>"$err"; then
        if grep -q "no io_uring support" "$err"; then
            modes="base statx"
            summary_add SKIP "ecrawl --iouring" "built without io_uring support"
            log "note: ecrawl built without io_uring; --iouring legs skipped"
        else
            die "ecrawl --no-write --iouring failed: $(tail -n1 "$err")"
        fi
    fi

    local -A nw_sum=()
    for m in $modes; do
        local flags=() tag=""
        case "$m" in
            statx) flags=(--statx); tag="--statx" ;;
            iouring) flags=(--iouring); tag="--iouring" ;;
        esac
        ECRAWL_CRAWL_THREADS="${ECRAWL_CRAWL_THREADS:-4}" \
            "$ECRAWL" --no-write --verbose "${flags[@]}" "$st" >"${td}/statx_nw_${m}.out" 2>"$err" ||
            die "ecrawl --no-write $tag failed"
        nw_sum[$m]=$(grep -E '^(entries|dirs|files|symlinks|other|total_bytes|total_allocated_bytes|hardlink_files)=' \
            "${td}/statx_nw_${m}.out" | LC_ALL=C sort)
        if [[ "$m" == "iouring" ]]; then
            log "note: --iouring io_uring_batches=$(kv_last io_uring_batches "${td}/statx_nw_${m}.out")" \
                "sync_redos=$(kv_last io_uring_sync_redos "${td}/statx_nw_${m}.out")"
        fi
    done
    for m in $modes; do
        [[ "$m" == "base" ]] && continue
        expect_eq_continue "ecrawl --no-write ($m): walk answers match fstatat baseline" \
            "${nw_sum[base]}" "${nw_sum[$m]}" "entries/files/bytes/hardlinks"
    done

    # Force the inline path: MIN_BATCH above any directory's child count, so every
    # collected batch skips the ring. Answers must still match the fstatat baseline.
    # On a kernel without a working ring, --iouring never enters crawl_dir_entries_iouring
    # (fd < 0); skip the counter checks there.
    if [[ "$modes" == *iouring* ]]; then
        local ring_live=0 batches0 inline0
        batches0=$(kv_last io_uring_batches "${td}/statx_nw_iouring.out")
        inline0=$(kv_last io_uring_inline_stats "${td}/statx_nw_iouring.out")
        if [[ "${batches0:-0}" -gt 0 || "${inline0:-0}" -gt 0 ]]; then
            ring_live=1
        fi
        ECRAWL_CRAWL_THREADS="${ECRAWL_CRAWL_THREADS:-4}" ECRAWL_IOURING_MIN_BATCH=65536 \
            "$ECRAWL" --no-write --verbose --iouring "$st" >"${td}/statx_nw_inline.out" 2>"$err" ||
            die "ecrawl --no-write --iouring (MIN_BATCH=65536) failed"
        expect_eq_continue "ecrawl --iouring MIN_BATCH=65536: walk answers match baseline" \
            "${nw_sum[base]}" \
            "$(grep -E '^(entries|dirs|files|symlinks|other|total_bytes|total_allocated_bytes|hardlink_files)=' \
                "${td}/statx_nw_inline.out" | LC_ALL=C sort)" \
            "inline path"
        if [[ "$ring_live" == "1" ]]; then
            expect_eq_continue "ecrawl --iouring MIN_BATCH=65536: no ring submits" \
                "0" "$(kv_last io_uring_batches "${td}/statx_nw_inline.out")"
            expect_eq_continue "ecrawl --iouring MIN_BATCH=65536: names took inline path" \
                "1" "$(( $(kv_last io_uring_inline_stats "${td}/statx_nw_inline.out") > 0 ))" \
                "io_uring_inline_stats=$(kv_last io_uring_inline_stats "${td}/statx_nw_inline.out")"
        else
            summary_add SKIP "ecrawl --iouring MIN_BATCH=65536: ring counters" \
                "io_uring unavailable at runtime; walk used statx fallback"
        fi
    fi

    # Write mode: counters plus total shard bytes must match the baseline capture.
    for m in $modes; do
        local flags=() tag=""
        case "$m" in
            statx) flags=(--statx); tag="--statx" ;;
            iouring) flags=(--iouring); tag="--iouring" ;;
        esac
        ECRAWL_CRAWL_THREADS="${ECRAWL_CRAWL_THREADS:-4}" \
            "$ECRAWL" --verbose "${flags[@]}" "$st" "${td}/statx_cap_${m}" >"${td}/statx_cap_${m}.out" 2>"$err" ||
            die "ecrawl $tag capture failed"
        nw_sum[$m]=$(grep -E '^(entries|dirs|files|symlinks|other|total_bytes|total_allocated_bytes|hardlink_files)=' \
            "${td}/statx_cap_${m}.out" | LC_ALL=C sort)
        shard_base=$(find "${td}/statx_cap_${m}" -name 'uid_shard_*.bin' -exec stat -c %s {} + |
            awk '{s+=$1} END {print s+0}')
        nw_sum[$m]="${nw_sum[$m]}"$'\n'"shard_bytes=${shard_base}"
        expect_eq_continue "ecrawl capture ($m): stat_impl recorded" \
            "$m" "$(kv_last stat_impl "${td}/statx_cap_${m}.out" | sed 's/fstatat/base/')"
    done
    for m in $modes; do
        [[ "$m" == "base" ]] && continue
        expect_eq_continue "ecrawl capture ($m): counters + shard bytes match baseline" \
            "${nw_sum[base]}" "${nw_sum[$m]}" "incl. uid_shard_*.bin total bytes"
    done

    # --no-stat + --statx: the only inode reads left (crawl root, d_type-less probes) go through
    # statx with a STATX_TYPE-only mask; the path list must still equal find.
    find "$st" | LC_ALL=C sort >"${td}/statx_nostat.ref"
    "$ECRAWL" --no-stat --statx "$st" 2>"$err" | LC_ALL=C sort >"${td}/statx_nostat.out" ||
        die "ecrawl --no-stat --statx failed"
    expect_eq_continue "ecrawl --no-stat --statx: path set equals find" \
        "$(cksum <"${td}/statx_nostat.ref" | cut -d' ' -f1)" \
        "$(cksum <"${td}/statx_nostat.out" | cut -d' ' -f1)" "STATX_TYPE mask path"
    "$ECRAWL" --no-stat --statx --verbose "$st" >/dev/null 2>"$err" ||
        die "ecrawl --no-stat --statx --verbose failed"
    expect_le_continue "ecrawl --no-stat --statx: inode reads" 1 \
        "$(kv_last io_lstat_calls "$err")" "root only"

    rm -rf "$st" "$err" "${td}"/statx_nw_* "${td}"/statx_cap_* "${td}"/statx_nw_inline.out \
        "${td}"/statx_nostat.*
    pass "ecrawl --statx / --iouring"
}

# --progress used to piggyback on the per-directory donate counter, so a bushy
# tree (every dir well under 4096 names) printed nothing. The line must appear
# for every walk mode, on stderr except --count (stdout, with the census).
run_ecrawl_progress_tests() {
    local td=$1
    local tree="${td}/progress_walk" d i nlines

    section_int "[integration] ecrawl --progress (walk-mode combinations)"

    mkdir -p "$tree"
    for d in 0 1 2 3 4 5 6 7 8 9; do
        mkdir -p "${tree}/d${d}"
        for i in 0 1 2 3 4; do
            echo x >"${tree}/d${d}/f${i}"
        done
    done

    expect_progress_line() {
        local tag=$1 file=$2
        if grep -qE 'files=.*entries=.*el=' "$file"; then
            summary_add PASS "ecrawl --progress ${tag}" "live line"
            [[ "$SUMMARY" == 1 ]] || printf '  %sOK:%s ecrawl --progress %s — live line%s\n' "$G" "$Z" "$tag" "$Z"
        else
            summary_add FAIL "ecrawl --progress ${tag}" "no files=/entries= line"
            SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
            printf '  %sFAIL:%s ecrawl --progress %s: no live line in %s%s\n' "$R" "$Z" "$tag" "$file" "$Z" >&2
        fi
    }

    "$ECRAWL" --progress --no-write "$tree" >"${td}/prog_nw.out" 2>"${td}/prog_nw.err" ||
        die "ecrawl --progress --no-write failed"
    expect_progress_line "--no-write" "${td}/prog_nw.err"

    "$ECRAWL" --progress --statx --no-write "$tree" >"${td}/prog_sx.out" 2>"${td}/prog_sx.err" ||
        die "ecrawl --progress --statx --no-write failed"
    expect_progress_line "--statx --no-write" "${td}/prog_sx.err"

    "$ECRAWL" --progress --no-stat "$tree" >"${td}/prog_ns.out" 2>"${td}/prog_ns.err" ||
        die "ecrawl --progress --no-stat failed"
    expect_progress_line "--no-stat" "${td}/prog_ns.err"
    if grep -qE 'files=.*entries=.*el=' "${td}/prog_ns.out"; then
        summary_add FAIL "ecrawl --progress --no-stat: stdout stays paths" "progress leaked onto stdout"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
        printf '  %sFAIL:%s ecrawl --progress --no-stat leaked a live line onto stdout%s\n' "$R" "$Z" "$Z" >&2
    else
        summary_add PASS "ecrawl --progress --no-stat: stdout stays paths" "progress on stderr"
    fi

    "$ECRAWL" --progress --no-stat --count "$tree" >"${td}/prog_ct.out" 2>"${td}/prog_ct.err" ||
        die "ecrawl --progress --no-stat --count failed"
    expect_progress_line "--no-stat --count" "${td}/prog_ct.out"

    "$ECRAWL" --progress --no-stat --contains f0 "$tree" >"${td}/prog_co.out" 2>"${td}/prog_co.err" ||
        die "ecrawl --progress --no-stat --contains failed"
    expect_progress_line "--no-stat --contains" "${td}/prog_co.err"

    "$ECRAWL" --progress "$tree" "${td}/prog_cap" >"${td}/prog_wr.out" 2>"${td}/prog_wr.err" ||
        die "ecrawl --progress (write) failed"
    expect_progress_line "write" "${td}/prog_wr.err"

    # Donation off must not silence progress (the cadence used to be the same counter).
    ECRAWL_DONATE_ENTRY_CHECK_EVERY=0 \
        "$ECRAWL" --progress --no-write "$tree" >"${td}/prog_don0.out" 2>"${td}/prog_don0.err" ||
        die "ecrawl --progress with DONATE_ENTRY_CHECK_EVERY=0 failed"
    expect_progress_line "DONATE_ENTRY_CHECK_EVERY=0" "${td}/prog_don0.err"

    # Each dir has 5 names; a per-dir counter of 8 never trips. Cumulative must.
    ECRAWL_DONATE_ENTRY_CHECK_EVERY=8 \
        "$ECRAWL" --progress --no-write "$tree" >"${td}/prog_cad.out" 2>"${td}/prog_cad.err" ||
        die "ecrawl --progress cadence=8 failed"
    nlines=$(grep -cE 'files=.*entries=.*el=' "${td}/prog_cad.err" || true)
    if [[ "${nlines:-0}" -ge 2 ]]; then
        summary_add PASS "ecrawl --progress bushy cadence=8" "lines=${nlines}"
        [[ "$SUMMARY" == 1 ]] || printf '  %sOK:%s ecrawl --progress bushy cadence=8 — %s lines%s\n' "$G" "$Z" "$nlines" "$Z"
    else
        summary_add FAIL "ecrawl --progress bushy cadence=8" "lines=${nlines} (want >= 2)"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
        printf '  %sFAIL:%s ecrawl --progress bushy cadence=8: %s live lines (want >= 2)%s\n' \
            "$R" "$Z" "$nlines" "$Z" >&2
    fi

    if "$ECRAWL" --progress --no-write --iouring "$tree" >"${td}/prog_io.out" 2>"${td}/prog_io.err"; then
        expect_progress_line "--iouring --no-write" "${td}/prog_io.err"
    else
        summary_add SKIP "ecrawl --progress --iouring" "unavailable"
    fi

    rm -rf "$tree" "${td}/prog_cap" "${td}"/prog_*.out "${td}"/prog_*.err
    pass "ecrawl --progress"
}

# ERCBIN09: the catalog rollup fast path must agree with the record scan wherever
# it claims to apply, must decline where it cannot be exact, and the gid/mode
# columns the columnar format brought back must filter correctly.
run_v8_rollup_tests() {
    local td=$1
    local tree="${td}/v8_tree" out="${td}/v8_crawl" log="${td}/v8.crawl.log"

    section_int "[integration] ERCBIN09 catalog rollups + gid/perm filters"

    # clean/ has no multiply-linked file anywhere below it, so the rollup is
    # provably exact there. linked/y.txt has its twin outside the subtree, which
    # is exactly the case the rollup cannot answer.
    mkdir -p "${tree}/clean/inner" "${tree}/linked" "${tree}/outside" "${tree}/perm"
    head -c 10 /dev/zero >"${tree}/clean/x1.bin"
    head -c 20 /dev/zero >"${tree}/clean/inner/x2.bin"
    head -c 30 /dev/zero >"${tree}/outside/y.bin"
    ln "${tree}/outside/y.bin" "${tree}/linked/y_hard.bin"
    # Explicit modes, and the perm assertions are scoped to this directory, so the
    # expected sets do not shift with the caller's umask.
    head -c 5 /dev/zero >"${tree}/perm/ww.bin"
    head -c 5 /dev/zero >"${tree}/perm/ro.bin"
    head -c 5 /dev/zero >"${tree}/perm/ex.bin"
    chmod 0666 "${tree}/perm/ww.bin"
    chmod 0444 "${tree}/perm/ro.bin"
    chmod 0700 "${tree}/perm/ex.bin"

    local tree_abs
    tree_abs=$(cd "$tree" && pwd -P)

    ECRAWL_CRAWL_THREADS=2 "$ECRAWL" "$tree_abs" "$out" >"$log" 2>&1 || {
        tail -n 40 "$log" >&2 || true
        die "ecrawl failed on the v8 rollup fixture"
    }

    local roll="${td}/v8.rollup" scan="${td}/v8.scan"
    ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --subtree "${tree_abs}/clean" "$out" \
        >"$roll" 2>&1 || die "rollup query failed"
    ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --subtree "${tree_abs}/clean" --exact "$out" \
        >"$scan" 2>&1 || die "exact query failed"

    expect_eq "hardlink-free subtree answers from the catalog" "catalog_rollup" \
        "$(kv_last answered_from "$roll")"
    expect_eq "--exact forces the record scan" "record_scan" "$(kv_last answered_from "$scan")"

    local k
    for k in entries files dirs symlinks other bytes; do
        expect_eq "rollup equals exact scan: ${k}" "$(kv_last "$k" "$scan")" "$(kv_last "$k" "$roll")"
    done
    expect_eq "rollup bytes match du -sb" "$(du -sb "${tree_abs}/clean" | awk '{print $1}')" \
        "$(kv_last bytes "$roll")"

    # A record with nlink > 1 in scope makes crawl-time credit and scan-time
    # dedup legitimately disagree, so the fast path must decline rather than
    # answer approximately.
    local fb="${td}/v8.fallback"
    ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --subtree "${tree_abs}/linked" "$out" \
        >"$fb" 2>&1 || die "hardlink-subtree query failed"
    expect_eq "subtree spanned by a hardlink falls back to the scan" "record_scan" \
        "$(kv_last answered_from "$fb")"
    summary_add PASS "ERCBIN09 rollup fast path" "exact-parity+du-parity+hardlink-fallback"

    # gid and mode are columns the columnar format makes cheap enough to carry.
    local g bogus
    local anyg="${td}/v8.anygid" mineg="${td}/v8.mygid" nog="${td}/v8.nogid"
    g=$(id -g)
    bogus=$((g + 40000))
    ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --type f "$out" >"$anyg" 2>&1 || die "--type f query failed"
    ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --type f --gid "$g" "$out" >"$mineg" 2>&1 || die "--gid query failed"
    ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --type f --gid "$bogus" "$out" >"$nog" 2>&1 ||
        die "--gid miss query failed"
    expect_eq "--gid selects every record in a single-group tree" \
        "$(kv_last entries "$anyg")" "$(kv_last entries "$mineg")"
    expect_eq "--gid on an absent group matches nothing" "0" "$(kv_last entries "$nog")"

    local psub="--subtree ${tree_abs}/perm"
    expect_eq "--perm -0002 finds the world-writable file" "${tree_abs}/perm/ww.bin" \
        "$(ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" $psub --type f --perm -0002 --list "$out" 2>/dev/null)"
    expect_eq "--perm 0444 matches exact bits only" "${tree_abs}/perm/ro.bin" \
        "$(ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" $psub --type f --perm 0444 --list "$out" 2>/dev/null)"
    expect_eq "--perm /0111 finds the one executable file" "${tree_abs}/perm/ex.bin" \
        "$(ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" $psub --type f --perm /0111 --list "$out" 2>/dev/null)"
    summary_add PASS "ERCBIN09 gid/perm filters" "gid-match+gid-miss+perm exact/all/any"

    rm -rf "$tree" "$out"
}

# --- dir-index sidecars: dirs.idx / rowgroups.idx ---------------------------
#
# `ereport_index --make` writes both beside the trigram index (--no-dir-index
# turns the phase off) and `ecrawl_query --index-dir` reads them: a bare
# --subtree aggregate becomes a hash lookup plus a short chain of preads
# (answered_from=dir_index) rather than a full catalog materialisation, and a
# filtered subtree scan builds its chunk list from the row groups whose DFS
# sketch can still hold a descendant.
#
# Both are caches over data the query can already reach another way, so every
# check here is one of three shapes: the sidecar answer equals the route it
# replaces and equals --exact, which always scans; a --list path set is
# identical with and without pruning; or the sidecar is stale/absent and the
# query degrades to catalog_rollup with the same totals instead of failing.

DIRX_AGG_KEYS="entries files dirs symlinks other bytes"

# ecrawl_query into <out>; its stderr lands in <out>.err.
dirx_analyze() {
    local out=$1 bins=$2
    shift 2
    ECRAWL_QUERY_THREADS="${ECRAWL_QUERY_THREADS:-4}" \
        "$ECRAWL_QUERY" "$@" "$bins" >"$out" 2>"${out}.err" || {
        tail -n 20 "${out}.err" >&2 || true
        die "ecrawl_query $* failed"
    }
}

# The stats block prints on stdout for a bare query and on stderr under --list.
dirx_kv() {
    local key=$1 out=$2 v
    v=$(kv_last "$key" "$out")
    [[ -n "$v" ]] || v=$(kv_last "$key" "${out}.err")
    printf '%s' "$v"
}

# Every aggregate the query prints, from two routes that must agree.
dirx_same() {
    local label=$1 ref=$2 got=$3 k
    shift 3
    for k in $DIRX_AGG_KEYS "$@"; do
        expect_eq_continue "${label}: ${k}" "$(dirx_kv "$k" "$ref")" "$(dirx_kv "$k" "$got")" || true
    done
}

# ecrawl files a record by its owner (shard = uid & (uid_shards-1)), so a
# fixture this user owns outright lands in a single shard and the cross-shard
# arithmetic is never exercised: summing one subtree's rollups over every shard
# that holds a piece of it, and adding the subtree's own directory record
# exactly once, in whichever shard actually carries it
# (CRAWL_DIR_FLAG_SELF_RECORD). chown needs privileges a test cannot assume, so
# ownership is faked for the crawl alone -- a preloaded shim answers the stat
# calls ecrawl makes with a uid taken from a "uNNNN_" prefix on the entry's own
# name. Returns non-zero (and the caller skips) when there is no compiler.
dirx_build_uid_shim() {
    local so=$1 src="${so}.c" cc c

    for c in "${CC:-}" cc gcc clang; do
        [[ -n "$c" ]] || continue
        if command -v "$c" >/dev/null 2>&1; then
            cc=$c
            break
        fi
    done
    [[ -n "${cc:-}" ]] || return 1

    cat >"$src" <<'DIRX_SHIM_EOF'
/* Report st_uid/st_gid from a "uNNNN_" prefix on the entry's own name.
 * The name is all that is available: ecrawl stats children as (dirfd, name). */
#define _GNU_SOURCE
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

static unsigned shim_uid(const char *path) {
    const char *b;
    unsigned v = 0;
    int i = 1;

    if (!path) return 0;
    b = strrchr(path, '/');
    b = b ? b + 1 : path;
    if (b[0] != 'u') return 0;
    while (b[i] >= '0' && b[i] <= '9') {
        v = v * 10u + (unsigned)(b[i] - '0');
        i++;
    }
    if (i == 1 || (b[i] != '\0' && b[i] != '_')) return 0;
    return v;
}

static int shim_stat(int dirfd, const char *path, void *stbuf, int flags) {
    struct stat *st = (struct stat *)stbuf;
    unsigned u;
    int rc = (int)syscall(SYS_newfstatat, dirfd, path, st, flags);

    if (rc != 0) return rc;
    u = shim_uid(path);
    if (u) {
        st->st_uid = (uid_t)u;
        st->st_gid = (gid_t)u;
    }
    return 0;
}

int fstatat(int d, const char *p, struct stat *s, int f) { return shim_stat(d, p, s, f); }
int __fxstatat(int v, int d, const char *p, struct stat *s, int f) { (void)v; return shim_stat(d, p, s, f); }
int lstat(const char *p, struct stat *s) { return shim_stat(AT_FDCWD, p, s, AT_SYMLINK_NOFOLLOW); }
int __lxstat(int v, const char *p, struct stat *s) { (void)v; return shim_stat(AT_FDCWD, p, s, AT_SYMLINK_NOFOLLOW); }
int stat(const char *p, struct stat *s) { return shim_stat(AT_FDCWD, p, s, 0); }
int __xstat(int v, const char *p, struct stat *s) { (void)v; return shim_stat(AT_FDCWD, p, s, 0); }
#ifdef __USE_LARGEFILE64
int fstatat64(int d, const char *p, struct stat64 *s, int f) { return shim_stat(d, p, s, f); }
int __fxstatat64(int v, int d, const char *p, struct stat64 *s, int f) { (void)v; return shim_stat(d, p, s, f); }
int lstat64(const char *p, struct stat64 *s) { return shim_stat(AT_FDCWD, p, s, AT_SYMLINK_NOFOLLOW); }
int __lxstat64(int v, const char *p, struct stat64 *s) { (void)v; return shim_stat(AT_FDCWD, p, s, AT_SYMLINK_NOFOLLOW); }
int stat64(const char *p, struct stat64 *s) { return shim_stat(AT_FDCWD, p, s, 0); }
int __xstat64(int v, const char *p, struct stat64 *s) { (void)v; return shim_stat(AT_FDCWD, p, s, 0); }
#endif
DIRX_SHIM_EOF

    "$cc" -shared -fPIC -O1 -o "$so" "$src" >/dev/null 2>&1 || return 1
    [[ -f "$so" ]]
}

# A subtree spread over several uid shards, where the subtree's own directory
# record lives in only one of them. Getting the byte total right across shards
# is the correctness risk a single-uid fixture cannot see, so this one checks
# the three routes against each other and all of them against find/du.
dirx_multishard_tests() {
    local td=$1
    local tree="${td}/dirx_mtree" out="${td}/dirx_mcrawl" idx="${td}/dirx_midx"
    local so="${td}/dirx_uidshim.so"
    local tree_abs top shards i

    section_int "[integration] dir-index sidecars — a subtree spanning uid shards"

    if ! dirx_build_uid_shim "$so"; then
        log "skip: no C compiler for the ownership shim; multi-shard cases need one"
        summary_add SKIP "dir-index multi-shard" "no compiler for the uid shim"
        return 0
    fi

    # u40001_top owns itself; its children are split across three other uids, so
    # every shard holds part of the subtree and exactly one holds its own record.
    mkdir -p "$tree/u40001_top/u40002_a" "$tree/u40001_top/u40003_b" \
        "$tree/u40001_top/u40001_c/u40002_deep" "$tree/u40004_side"
    for ((i = 0; i < 12; i++)); do
        head -c $((100 + i)) /dev/zero >"$tree/u40001_top/u40002_a/u40002_f${i}.dat"
        head -c $((200 + i)) /dev/zero >"$tree/u40001_top/u40003_b/u40003_f${i}.dat"
        head -c $((300 + i)) /dev/zero >"$tree/u40001_top/u40001_c/u40001_f${i}.dat"
        head -c $((400 + i)) /dev/zero >"$tree/u40001_top/u40001_c/u40002_deep/u40002_g${i}.dat"
        head -c $((500 + i)) /dev/zero >"$tree/u40004_side/u40004_f${i}.dat"
    done
    head -c 777 /dev/zero >"$tree/u40001_top/u40002_direct.dat"
    ln -s u40002_a "$tree/u40001_top/u40003_link"
    tree_abs=$(cd "$tree" && pwd -P)
    top="${tree_abs}/u40001_top"

    LD_PRELOAD="$so" ECRAWL_CRAWL_THREADS=4 "$ECRAWL" "$tree_abs" "$out" \
        >"${td}/dirx_mcrawl.log" 2>&1 || {
        tail -n 20 "${td}/dirx_mcrawl.log" >&2 || true
        die "ecrawl failed on the multi-uid fixture"
    }
    shards=$(find "$out" -maxdepth 1 -name 'uid_shard_*.bin' | wc -l | tr -d ' ')
    if [[ "$shards" -lt 3 ]]; then
        # The shim did not take (a libc that routes stat elsewhere, say), and a
        # one-shard capture would silently test nothing here.
        log "skip: ownership shim produced ${shards} shard(s); need at least 3"
        summary_add SKIP "dir-index multi-shard" "shim produced ${shards} shard(s)"
        rm -rf "$tree" "$out"
        return 0
    fi
    summary_metric "dir-index multi-shard fixture: uid shards" "$shards"

    EREPORT_INDEX_THREADS="${EREPORT_INDEX_THREADS:-4}" \
        "$EREPORT_INDEX" --make --index-dir "$idx" "$out" \
        >"${td}/dirx_mmake.out" 2>"${td}/dirx_mmake.err" || {
        tail -n 20 "${td}/dirx_mmake.err" >&2 || true
        die "ereport_index --make failed on the multi-uid fixture"
    }

    local plain="${td}/dirx_m.plain" sidecar="${td}/dirx_m.idx" exact="${td}/dirx_m.exact"
    dirx_analyze "$plain" "$out" --subtree "$top"
    dirx_analyze "$sidecar" "$out" --subtree "$top" --index-dir "$idx"
    dirx_analyze "$exact" "$out" --subtree "$top" --exact
    expect_eq_continue "multi-shard: the sidecar answered" "dir_index" \
        "$(dirx_kv answered_from "$sidecar")" "root row found in the shard that owns it" || true
    dirx_same "multi-shard: dir_index vs catalog_rollup" "$plain" "$sidecar" || true
    dirx_same "multi-shard: dir_index vs --exact scan" "$exact" "$sidecar" || true

    # find/du are the outside opinion: they cannot be fooled by an accounting
    # rule the three routes might share, and the subtree's own directory --
    # counted in one shard and skipped in the others -- is exactly what a
    # double count or a dropped record would show up in.
    expect_eq_continue "multi-shard: entries vs find" "$(find "$top" | wc -l | tr -d ' ')" \
        "$(dirx_kv entries "$sidecar")" "self record counted exactly once across shards" || true
    expect_eq_continue "multi-shard: dirs vs find -type d" \
        "$(find "$top" -type d | wc -l | tr -d ' ')" "$(dirx_kv dirs "$sidecar")" || true
    expect_eq_continue "multi-shard: files vs find -type f" \
        "$(find "$top" -type f | wc -l | tr -d ' ')" "$(dirx_kv files "$sidecar")" || true
    expect_eq_continue "multi-shard: bytes vs du -sb" "$(du -sb "$top" | awk '{print $1}')" \
        "$(dirx_kv bytes "$sidecar")" "byte total is right across shards" || true

    # The crawl root: every shard holds a piece, and the root's own record sits
    # in the shard of whoever owns the directory the test made.
    dirx_analyze "${td}/dirx_mroot.plain" "$out" --subtree "$tree_abs"
    dirx_analyze "${td}/dirx_mroot.idx" "$out" --subtree "$tree_abs" --index-dir "$idx"
    dirx_same "multi-shard crawl root: dir_index vs catalog_rollup" \
        "${td}/dirx_mroot.plain" "${td}/dirx_mroot.idx" || true
    expect_eq_continue "multi-shard crawl root: entries vs find" \
        "$(find "$tree_abs" | wc -l | tr -d ' ')" "$(dirx_kv entries "${td}/dirx_mroot.idx")" || true

    # A subtree whose root and contents belong to different shards, listed:
    # the chunk list is built per shard, so a lost shard shows up as lost paths.
    dirx_analyze "${td}/dirx_ml.plain" "$out" --subtree "$top" --type f --list
    dirx_analyze "${td}/dirx_ml.idx" "$out" --subtree "$top" --type f --list --index-dir "$idx"
    expect_eq_continue "multi-shard --list: identical path sets" \
        "$(sort "${td}/dirx_ml.plain" | cksum)" "$(sort "${td}/dirx_ml.idx" | cksum)" \
        "$(wc -l <"${td}/dirx_ml.plain" | tr -d ' ') paths" || true

    summary_add PASS "dir-index multi-shard" "${shards} shards, self-record attribution + find/du parity"

    # ereport across the same shards. Its bitmap is built per shard and the
    # subtree's own directory record sits in only one of them, so a report that
    # counted it in each shard -- or in none -- differs from the string-prefix
    # run here and nowhere else.
    ereport_dirx_tests "$td" "$out" "$idx" "$top" multishard 0

    rm -rf "$tree" "$out" "$idx"
}

# What the sidecars are keyed on is the path the capture stored, which is not
# always the path on disk: ecrawl --record-root relabels it at crawl time and
# ereport_index --path-rewrite relabels it again for the trigram index. The
# hashes must follow the stored spelling, because that is what a query without
# a sidecar matches against.
dirx_stored_path_tests() {
    local td=$1
    local tree="${td}/dirx_rr_tree" out="${td}/dirx_rr_crawl" idx="${td}/dirx_rr_idx"
    local idx2="${td}/dirx_rw_idx" idx3="${td}/dirx_dup_idx"
    local out_b="${td}/dirx_rr_crawl_b"
    local stored="/dirx-virtual/srv07" i

    section_int "[integration] dir-index sidecars — stored paths (--record-root, --path-rewrite)"

    mkdir -p "$tree/sub/inner" "$tree/other"
    for ((i = 0; i < 8; i++)); do
        head -c $((64 + i)) /dev/zero >"$tree/sub/f${i}.dat"
        head -c $((32 + i)) /dev/zero >"$tree/sub/inner/g${i}.dat"
        head -c $((16 + i)) /dev/zero >"$tree/other/h${i}.dat"
    done
    local tree_abs
    tree_abs=$(cd "$tree" && pwd -P)

    ECRAWL_CRAWL_THREADS=2 "$ECRAWL" --record-root "$stored" "$tree_abs" "$out" \
        >"${td}/dirx_rr.crawl.log" 2>&1 || {
        tail -n 20 "${td}/dirx_rr.crawl.log" >&2 || true
        die "ecrawl --record-root failed"
    }
    EREPORT_INDEX_THREADS=2 "$EREPORT_INDEX" --make --index-dir "$idx" "$out" \
        >"${td}/dirx_rr.make.out" 2>"${td}/dirx_rr.make.err" || {
        tail -n 20 "${td}/dirx_rr.make.err" >&2 || true
        die "ereport_index --make failed on a --record-root capture"
    }

    dirx_analyze "${td}/dirx_rr.plain" "$out" --subtree "${stored}/sub"
    dirx_analyze "${td}/dirx_rr.idx" "$out" --subtree "${stored}/sub" --index-dir "$idx"
    dirx_analyze "${td}/dirx_rr.exact" "$out" --subtree "${stored}/sub" --exact
    expect_eq_continue "--record-root: the stored path is what the sidecar indexed" "dir_index" \
        "$(dirx_kv answered_from "${td}/dirx_rr.idx")" || true
    dirx_same "--record-root: dir_index vs catalog_rollup" "${td}/dirx_rr.plain" "${td}/dirx_rr.idx" || true
    dirx_same "--record-root: dir_index vs --exact scan" "${td}/dirx_rr.exact" "${td}/dirx_rr.idx" || true
    expect_eq_continue "--record-root: bytes vs du -sb of the real tree" \
        "$(du -sb "${tree_abs}/sub" | awk '{print $1}')" "$(dirx_kv bytes "${td}/dirx_rr.idx")" || true

    # The path the tree actually has on disk was never stored, so neither route
    # can find it -- and they have to be missing it in the same way.
    dirx_analyze "${td}/dirx_rr.real.plain" "$out" --subtree "${tree_abs}/sub"
    dirx_analyze "${td}/dirx_rr.real.idx" "$out" --subtree "${tree_abs}/sub" --index-dir "$idx"
    expect_eq_continue "--record-root: the on-disk path is not in the capture" "0" \
        "$(dirx_kv entries "${td}/dirx_rr.real.idx")" || true
    dirx_same "--record-root: an absent path agrees either way" \
        "${td}/dirx_rr.real.plain" "${td}/dirx_rr.real.idx" || true

    # --path-rewrite relabels what the trigram index stores. The sidecars are
    # built from the catalogs, which it does not touch, so a subtree query keeps
    # using the stored spelling -- the same one it needs without an index dir.
    EREPORT_INDEX_THREADS=2 "$EREPORT_INDEX" --make --path-rewrite "${stored}=/dirx-relabelled" \
        --index-dir "$idx2" "$out" >"${td}/dirx_rw.make.out" 2>"${td}/dirx_rw.make.err" || {
        tail -n 20 "${td}/dirx_rw.make.err" >&2 || true
        die "ereport_index --make --path-rewrite failed"
    }
    dirx_analyze "${td}/dirx_rw.idx" "$out" --subtree "${stored}/sub" --index-dir "$idx2"
    expect_eq_continue "--path-rewrite: sidecar still keyed on the stored path" "dir_index" \
        "$(dirx_kv answered_from "${td}/dirx_rw.idx")" || true
    dirx_same "--path-rewrite: totals unchanged" "${td}/dirx_rr.plain" "${td}/dirx_rw.idx" || true
    dirx_analyze "${td}/dirx_rw.relabelled" "$out" --subtree "/dirx-relabelled/sub" --index-dir "$idx2"
    expect_eq_continue "--path-rewrite: the relabelled path answers nothing (as without the sidecar)" \
        "0" "$(dirx_kv entries "${td}/dirx_rw.relabelled")" || true

    # Two input directories contributing the same shard basename: the sidecars
    # are keyed on that name, so the phase declines rather than indexing one
    # shard under the other's identity. The trigram index is still written.
    ECRAWL_CRAWL_THREADS=2 "$ECRAWL" "${tree_abs}/other" "$out_b" \
        >"${td}/dirx_dup.crawl.log" 2>&1 || die "ecrawl failed on the second capture"
    EREPORT_INDEX_THREADS=2 "$EREPORT_INDEX" --make --index-dir "$idx3" "$out" "$out_b" \
        >"${td}/dirx_dup.make.out" 2>"${td}/dirx_dup.make.err" || {
        tail -n 20 "${td}/dirx_dup.make.err" >&2 || true
        die "ereport_index --make over two crawl dirs failed"
    }
    expect_eq_continue "duplicate shard names: dir index declined" "0" \
        "$(kv_last dir_index "${td}/dirx_dup.make.out")" || true
    if [[ -e "${idx3}/dirs.idx" || -e "${idx3}/rowgroups.idx" ]]; then
        summary_add FAIL "duplicate shard names: no sidecar written" "found one"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
    else
        summary_add PASS "duplicate shard names: no sidecar written" "declined"
    fi
    grep -q 'duplicate shard name' "${td}/dirx_dup.make.err" ||
        die "ereport_index skipped the dir index over duplicate shard names without saying so"
    [[ -f "${idx3}/tri_keys.bin" ]] ||
        die "ereport_index dropped the trigram index when it declined the sidecars"
    summary_add PASS "duplicate shard names" "declined, trigram index still written"

    rm -rf "$tree" "$out" "$out_b" "$idx" "$idx2" "$idx3"
}

# nlink > 1 anywhere in scope makes crawl-time hardlink credit and scan-time
# dedup legitimately disagree, so both rollup routes must decline, not answer.
dirx_hardlink_tests() {
    local td=$1
    local tree="${td}/dirx_hl_tree" out="${td}/dirx_hl_crawl" idx="${td}/dirx_hl_idx"
    local tree_abs i

    mkdir -p "$tree/sub/x"
    for ((i = 0; i < 5; i++)); do head -c $((1000 + i)) /dev/zero >"$tree/sub/x/h${i}.dat"; done
    ln "$tree/sub/x/h0.dat" "$tree/sub/x/h0_link.dat"
    tree_abs=$(cd "$tree" && pwd -P)

    ECRAWL_CRAWL_THREADS=2 "$ECRAWL" "$tree_abs" "$out" >"${td}/dirx_hl.crawl.log" 2>&1 ||
        die "ecrawl failed on the hardlink fixture"
    EREPORT_INDEX_THREADS=2 "$EREPORT_INDEX" --make --index-dir "$idx" "$out" \
        >"${td}/dirx_hl.make.out" 2>"${td}/dirx_hl.make.err" ||
        die "ereport_index --make failed on the hardlink fixture"

    dirx_analyze "${td}/dirx_hl.plain" "$out" --subtree "${tree_abs}/sub"
    dirx_analyze "${td}/dirx_hl.idx" "$out" --subtree "${tree_abs}/sub" --index-dir "$idx"
    expect_eq_continue "hardlink in scope: catalog rollup declines" "record_scan" \
        "$(dirx_kv answered_from "${td}/dirx_hl.plain")" || true
    expect_eq_continue "hardlink in scope: dirs.idx declines too" "record_scan" \
        "$(dirx_kv answered_from "${td}/dirx_hl.idx")" \
        "the sidecar reads the same subtree_nlink_gt1_count guard" || true
    dirx_same "hardlink fallback: totals agree" "${td}/dirx_hl.plain" "${td}/dirx_hl.idx" || true

    rm -rf "$tree" "$out" "$idx"
}

# `ereport --subtree` over the same two sidecars. It uses them differently from
# ecrawl_query: there is no aggregate to look up, so dirs.idx only resolves the
# subtree root per shard and the scan then decides membership from a bitmap over
# parent_dir_id instead of rebuilding each record's path and comparing strings,
# with rowgroups.idx removing the groups that cannot hold a descendant.
#
# The report is the whole product, so the report is the check: the two runs must
# write byte-identical trees. Any record the bitmap admits and the string prefix
# does not (or the reverse) moves a number on some page -- including the
# subtree's own directory record, which `path == prefix` accepts and which lives
# in exactly one shard.
ereport_dirx_tests() {
    local td=$1 out=$2 idx=$3 target=$4 tag=$5 want_prune=$6
    shift 6
    local who=("$@") # empty for the all-users aggregate
    local v extra pdir idir pout iout k kept total

    for v in histogram details; do
        extra=""
        [[ "$v" == "details" ]] && extra="--bucket-details 2"
        pdir="${td}/ere_${tag}_${v}_plain"
        idir="${td}/ere_${tag}_${v}_idx"
        pout="${pdir}.stdout"
        iout="${idir}.stdout"
        mkdir -p "$pdir" "$idir"

        # shellcheck disable=SC2086
        EREPORT_THREADS="${EREPORT_THREADS:-4}" "$EREPORT" $extra --report-dir "$pdir" \
            --subtree "$target" "${who[@]}" mtime "$out" >"$pout" 2>"${pdir}.stderr" || {
            tail -n 40 "${pdir}.stderr" >&2 || true
            die "ereport --subtree (${tag}/${v}, no index dir) failed"
        }
        # shellcheck disable=SC2086
        EREPORT_THREADS="${EREPORT_THREADS:-4}" "$EREPORT" $extra --report-dir "$idir" \
            --subtree "$target" --index-dir "$idx" "${who[@]}" mtime "$out" \
            >"$iout" 2>"${idir}.stderr" || {
            tail -n 40 "${idir}.stderr" >&2 || true
            die "ereport --subtree (${tag}/${v}, --index-dir) failed"
        }

        expect_eq_continue "ereport --subtree [${tag}/${v}]: the dir index was used" "dir_index" \
            "$(kv_last subtree_from "$iout")" || true

        if diff -r "$pdir" "$idir" >"${td}/ere_${tag}_${v}.diff" 2>&1; then
            summary_add PASS "ereport --subtree [${tag}/${v}]: byte-identical report" \
                "$(find "$idir" -type f | wc -l | tr -d ' ') file(s)"
            printf '  %sOK:%s ereport --subtree [%s/%s] report identical with and without the dir index (%s file(s))%s\n' \
                "$G" "$Z" "$tag" "$v" "$(find "$idir" -type f | wc -l | tr -d ' ')" "$Z"
        else
            summary_add FAIL "ereport --subtree [${tag}/${v}]: byte-identical report" \
                "$(head -n 6 "${td}/ere_${tag}_${v}.diff" | tr '\n' ' ')"
            SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
            printf '  %sFAIL:%s ereport --subtree [%s/%s] reports differ:%s\n' "$R" "$Z" "$tag" "$v" "$Z" >&2
            head -n 20 "${td}/ere_${tag}_${v}.diff" >&2 || true
        fi

        # Every stat the run prints, not just the ones the HTML happens to show.
        # scanned_records is in here on purpose: pruning must not change what the
        # report says it accounted for, only how much of it was decoded.
        for k in scanned_input_files scanned_records matched_records files directories links others \
            non_files total_capacity_in_files total_capacity_in_others bad_input_files; do
            expect_eq_continue "ereport --subtree [${tag}/${v}]: ${k}" \
                "$(kv_last "$k" "$pout")" "$(kv_last "$k" "$iout")" || true
        done

        kept=$(kv_last rowgroups_kept "$iout")
        total=$(kv_last rowgroups_total "$iout")
        if [[ "$want_prune" != "1" ]]; then
            # A capture below one row group per shard has nothing to prune, and
            # the pruned chunk builder then declines in favour of the checkpoint
            # segments. The bitmap route above is still what was compared.
            summary_add PASS "ereport --subtree [${tag}/${v}]: row groups" \
                "${kept:-n/a}/${total:-n/a} (pruning not required for this fixture)"
        elif [[ -n "$kept" && -n "$total" && "$kept" -lt "$total" ]]; then
            summary_add PASS "ereport --subtree [${tag}/${v}]: row groups pruned" "kept ${kept}/${total}"
            summary_metric "ereport --subtree [${tag}/${v}] row groups kept" "${kept}/${total}"
        else
            # Not a wrong answer, but then the pruned chunk builder never ran and
            # the comparison above was one route against itself.
            summary_add FAIL "ereport --subtree [${tag}/${v}]: row groups pruned" \
                "kept=${kept:-none} of ${total:-none}"
            SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
            printf '  %sFAIL:%s ereport --subtree [%s/%s] pruned nothing (kept=%s total=%s)%s\n' \
                "$R" "$Z" "$tag" "$v" "${kept:-none}" "${total:-none}" "$Z" >&2
        fi
    done

    # An index dir that is not there: the run must not merely survive it, it must
    # produce the report it would have produced without the flag.
    pdir="${td}/ere_${tag}_histogram_plain"
    idir="${td}/ere_${tag}_noidx"
    mkdir -p "$idir"
    EREPORT_THREADS="${EREPORT_THREADS:-4}" "$EREPORT" --report-dir "$idir" --subtree "$target" \
        --index-dir "${td}/ere_no_such_index" "${who[@]}" mtime "$out" \
        >"${idir}.stdout" 2>"${idir}.stderr" || {
        tail -n 40 "${idir}.stderr" >&2 || true
        die "ereport --subtree with a missing index dir failed"
    }
    expect_eq_continue "ereport --subtree [${tag}]: missing index dir leaves no trace" "" \
        "$(kv_last subtree_from "${idir}.stdout")" || true
    if diff -r "$pdir" "$idir" >"${td}/ere_${tag}_noidx.diff" 2>&1; then
        summary_add PASS "ereport --subtree [${tag}]: missing index dir degrades silently" "identical report"
    else
        summary_add FAIL "ereport --subtree [${tag}]: missing index dir degrades silently" "report differs"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
        head -n 20 "${td}/ere_${tag}_noidx.diff" >&2 || true
    fi

    # A subtree the capture never held. The sidecar cannot place it in any shard,
    # so this is the "absent from every shard" fallback, and an empty report has
    # to come out the same way it does without a dir index.
    idir="${td}/ere_${tag}_absent_idx"
    pdir="${td}/ere_${tag}_absent_plain"
    mkdir -p "$idir" "$pdir"
    EREPORT_THREADS="${EREPORT_THREADS:-4}" "$EREPORT" --report-dir "$pdir" \
        --subtree "${target}/dirx_no_such_dir" "${who[@]}" mtime "$out" \
        >"${pdir}.stdout" 2>"${pdir}.stderr" || true
    EREPORT_THREADS="${EREPORT_THREADS:-4}" "$EREPORT" --report-dir "$idir" \
        --subtree "${target}/dirx_no_such_dir" --index-dir "$idx" "${who[@]}" mtime "$out" \
        >"${idir}.stdout" 2>"${idir}.stderr" || true
    expect_eq_continue "ereport --subtree [${tag}]: an absent subtree matches nothing either way" \
        "$(kv_last matched_records "${pdir}.stdout")" "$(kv_last matched_records "${idir}.stdout")" || true

    rm -rf "${td}/ere_${tag}"_*
}

run_dir_index_tests() {
    local td=$1
    local tree="${td}/dirx_tree" out="${td}/dirx_crawl" idx="${td}/dirx_idx" nidx="${td}/dirx_idx_off"
    local bulk_dirs=${DIRX_BULK_DIRS:-40} bulk_files=${DIRX_BULK_FILES:-700}
    local tree_abs target d i

    section_int "[integration] dir-index sidecars (dirs.idx / rowgroups.idx)"

    # Big enough for several row groups -- ecrawl flushes one about every MiB of
    # raw records -- with the queried subtree small and clustered so pruning has
    # something to remove. Crawled single-threaded so which records share a row
    # group does not depend on how the thread pool happened to interleave.
    mkdir -p "$tree/bulk"
    for ((i = 0; i < bulk_dirs; i++)); do
        d=$(printf '%s/bulk/d%02d' "$tree" "$i")
        mkdir -p "$d"
        (cd "$d" && seq -f 'f%04g' 1 "$bulk_files" | xargs -r touch)
    done
    mkdir -p "$tree/target/s0" "$tree/target/s1" "$tree/target/deep/a/b/c"
    (cd "$tree/target/s0" && seq -f 'g%03g.dat' 1 200 | xargs -r touch)
    (cd "$tree/target/s1" && seq -f 'h%03g.log' 1 120 | xargs -r touch)
    head -c 5000 /dev/zero >"$tree/target/s1/big.bin"
    head -c 300 /dev/zero >"$tree/target/deep/a/b/c/leaf.bin"
    ln -s s0 "$tree/target/link_s0"
    tree_abs=$(cd "$tree" && pwd -P)
    target="${tree_abs}/target"

    log "ecrawl (dir-index fixture: ${bulk_dirs}x${bulk_files} bulk files + target subtree)"
    ECRAWL_CRAWL_THREADS=1 ECRAWL_WRITER_THREADS=1 \
        "$ECRAWL" "$tree_abs" "$out" >"${td}/dirx.crawl.log" 2>&1 || {
        tail -n 20 "${td}/dirx.crawl.log" >&2 || true
        die "ecrawl failed on the dir-index fixture"
    }

    log "ereport_index --make (dir-index sidecars under ${idx})"
    EREPORT_INDEX_THREADS="${EREPORT_INDEX_THREADS:-4}" \
        "$EREPORT_INDEX" --make --index-dir "$idx" "$out" \
        >"${td}/dirx.make.out" 2>"${td}/dirx.make.err" || {
        tail -n 20 "${td}/dirx.make.err" >&2 || true
        die "ereport_index --make failed on the dir-index fixture"
    }
    [[ -f "${idx}/dirs.idx" && -f "${idx}/rowgroups.idx" ]] ||
        die "ereport_index --make wrote no dirs.idx / rowgroups.idx under ${idx}"
    expect_eq_continue "meta.txt records the dir index" "1" "$(kv_last dir_index "${idx}/meta.txt")" || true

    local dirs_n groups_n dirs_b groups_b
    dirs_n=$(kv_last dir_index_dirs "${td}/dirx.make.out")
    groups_n=$(kv_last rowgroup_index_groups "${td}/dirx.make.out")
    dirs_b=$(stat -c %s "${idx}/dirs.idx")
    groups_b=$(stat -c %s "${idx}/rowgroups.idx")
    summary_metric "dirs.idx: bytes / dirs / bytes-per-dir" \
        "${dirs_b} / ${dirs_n} / $(LC_ALL=C awk -v b="$dirs_b" -v n="${dirs_n:-0}" 'BEGIN{printf "%.1f", (n?b/n:0)}')"
    summary_metric "rowgroups.idx: bytes / groups / bytes-per-group" \
        "${groups_b} / ${groups_n} / $(LC_ALL=C awk -v b="$groups_b" -v n="${groups_n:-0}" 'BEGIN{printf "%.1f", (n?b/n:0)}')"
    summary_metric "dir-index build seconds" "$(kv_last dir_index_sec "${td}/dirx.make.out")"

    # 1. The bare aggregate, three ways: the sidecar, the catalog rollup it
    #    replaces, and a record scan, which is the definition of the answer.
    local plain="${td}/dirx.q4.plain" sidecar="${td}/dirx.q4.idx" exact="${td}/dirx.q4.exact"
    dirx_analyze "$plain" "$out" --subtree "$target"
    dirx_analyze "$sidecar" "$out" --subtree "$target" --index-dir "$idx"
    dirx_analyze "$exact" "$out" --subtree "$target" --exact
    expect_eq_continue "subtree aggregate: no index dir uses the catalog" "catalog_rollup" \
        "$(dirx_kv answered_from "$plain")" || true
    expect_eq_continue "subtree aggregate: --index-dir uses dirs.idx" "dir_index" \
        "$(dirx_kv answered_from "$sidecar")" || true
    expect_eq_continue "subtree aggregate: --exact still scans records" "record_scan" \
        "$(dirx_kv answered_from "$exact")" || true
    dirx_same "dir_index vs catalog_rollup" "$plain" "$sidecar" hardlink_dupes records_scanned || true
    dirx_same "dir_index vs --exact scan" "$exact" "$sidecar" hardlink_dupes || true
    expect_eq_continue "dir_index bytes vs du -sb" "$(du -sb "$target" | awk '{print $1}')" \
        "$(dirx_kv bytes "$sidecar")" || true

    # The point of the sidecar: the catalog route reads every directory row in
    # the capture to find one, and this route reads a parent chain.
    expect_le_continue "dir_index reads fewer directory rows than the rollup" \
        "$(dirx_kv directories_examined "$plain")" "$(dirx_kv directories_examined "$sidecar")" \
        "$(dirx_kv directories_examined "$sidecar") rows vs $(dirx_kv directories_examined "$plain")" || true

    # 2. Same again on a deep leaf and on the crawl root, where the parent chain
    #    is longest and shortest respectively.
    local p label
    for p in "${target}/deep/a/b/c" "${target}/s0" "$tree_abs"; do
        label=${p#"$tree_abs"}
        label=${label:-(crawl root)}
        dirx_analyze "${td}/dirx.qd.plain" "$out" --subtree "$p"
        dirx_analyze "${td}/dirx.qd.idx" "$out" --subtree "$p" --index-dir "$idx"
        expect_eq_continue "subtree ${label}: answered from dirs.idx" "dir_index" \
            "$(dirx_kv answered_from "${td}/dirx.qd.idx")" || true
        dirx_same "subtree ${label}" "${td}/dirx.qd.plain" "${td}/dirx.qd.idx" || true
    done

    # 3. A path the capture never saw: not an error, and not a different answer
    #    depending on whether the sidecar was consulted.
    dirx_analyze "${td}/dirx.miss.plain" "$out" --subtree "${target}/no_such_dir"
    dirx_analyze "${td}/dirx.miss.idx" "$out" --subtree "${target}/no_such_dir" --index-dir "$idx"
    expect_eq_continue "absent subtree: no entries" "0" "$(dirx_kv entries "${td}/dirx.miss.idx")" || true
    expect_eq_continue "absent subtree: same route either way" \
        "$(dirx_kv answered_from "${td}/dirx.miss.plain")" \
        "$(dirx_kv answered_from "${td}/dirx.miss.idx")" || true
    dirx_same "absent subtree" "${td}/dirx.miss.plain" "${td}/dirx.miss.idx" || true

    # 4. The filtered scan, which is where rowgroups.idx earns its keep: the
    #    chunk list comes from the surviving groups instead of every checkpoint
    #    segment. Fewer bytes read, identical answer.
    local filt
    for filt in "--type f" "--size-gt 100 --type f" "--type d" "--gid $(id -g)"; do
        # shellcheck disable=SC2086
        dirx_analyze "${td}/dirx.q5.plain" "$out" --subtree "$target" $filt
        # shellcheck disable=SC2086
        dirx_analyze "${td}/dirx.q5.idx" "$out" --subtree "$target" $filt --index-dir "$idx"
        # records_scanned is deliberately not compared: reading fewer records for
        # the same answer is the whole point of the row-group sketch.
        dirx_same "filtered scan [${filt}]" "${td}/dirx.q5.plain" "${td}/dirx.q5.idx" || true
    done

    local rg_total rg_kept
    dirx_analyze "${td}/dirx.q5.idx" "$out" --subtree "$target" --type f --index-dir "$idx"
    rg_total=$(dirx_kv rowgroups_total "${td}/dirx.q5.idx")
    rg_kept=$(dirx_kv rowgroups_kept "${td}/dirx.q5.idx")
    expect_le_continue "fixture holds more than one row group" "${rg_total:-0}" 2 \
        "rowgroups_total=${rg_total:-0}" || true
    if [[ -n "$rg_total" && -n "$rg_kept" && "$rg_kept" -lt "$rg_total" ]]; then
        summary_add PASS "row-group pruning engaged" "kept ${rg_kept}/${rg_total} groups"
        printf '  %sOK:%s row-group pruning kept %s of %s groups (interval %s, bitmap %s)%s\n' \
            "$G" "$Z" "$rg_kept" "$rg_total" "$(dirx_kv rowgroups_kept_interval "${td}/dirx.q5.idx")" \
            "$(dirx_kv rowgroups_kept_bitmap "${td}/dirx.q5.idx")" "$Z"
    else
        # Not a wrong answer, but the pruned chunk builder then never ran and the
        # parity checks above compared one route with itself.
        summary_add FAIL "row-group pruning engaged" "kept=${rg_kept:-none} of ${rg_total:-none}"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
        printf '  %sFAIL:%s row-group pruning removed nothing (kept=%s total=%s)%s\n' \
            "$R" "$Z" "${rg_kept:-none}" "${rg_total:-none}" "$Z" >&2
    fi
    summary_metric "row groups kept for the target subtree" "${rg_kept:-?}/${rg_total:-?}"

    # 5. --list: the totals can agree while the path set does not, so compare
    #    the paths themselves, pruned against unpruned.
    dirx_analyze "${td}/dirx.list.plain" "$out" --subtree "$target" --type f --list
    dirx_analyze "${td}/dirx.list.idx" "$out" --subtree "$target" --type f --list --index-dir "$idx"
    expect_eq_continue "--list: pruned and unpruned path sets identical" \
        "$(sort "${td}/dirx.list.plain" | cksum)" "$(sort "${td}/dirx.list.idx" | cksum)" \
        "$(wc -l <"${td}/dirx.list.plain" | tr -d ' ') paths" || true
    expect_le_continue "--list: pruning did not read more records" \
        "$(dirx_kv records_scanned "${td}/dirx.list.plain")" \
        "$(dirx_kv records_scanned "${td}/dirx.list.idx")" \
        "$(dirx_kv records_scanned "${td}/dirx.list.idx") records vs $(dirx_kv records_scanned "${td}/dirx.list.plain")" || true

    # 6. Staleness. A sidecar names shards by size, mtime, catalog offset and
    #    catalog entry count; any of those moving means the dir_ids and row
    #    offsets it recorded may now point at something else, so the run must
    #    fall back rather than answer from it. Each case below has to come back
    #    with the catalog rollup's own numbers, not merely "not dir_index".
    local shard
    shard=$(find "$out" -maxdepth 1 -name 'uid_shard_*.bin' | LC_ALL=C sort | head -n1)
    [[ -n "$shard" ]] || die "dir-index fixture wrote no uid shard"

    cp -p "$shard" "${td}/dirx.shard.bak"
    touch "$shard"
    dirx_analyze "${td}/dirx.stale.mtime" "$out" --subtree "$target" --index-dir "$idx"
    expect_eq_continue "stale shard (mtime moved): falls back" "catalog_rollup" \
        "$(dirx_kv answered_from "${td}/dirx.stale.mtime")" || true
    dirx_same "stale shard (mtime moved): same totals" "$plain" "${td}/dirx.stale.mtime" || true
    cp -p "${td}/dirx.shard.bak" "$shard"

    # A shard that grew: the records and catalog the fallback needs are intact,
    # so the query still has to be exactly right -- which a genuinely truncated
    # shard could not show, since truncation takes the catalog with it.
    printf 'x' >>"$shard"
    dirx_analyze "${td}/dirx.stale.size" "$out" --subtree "$target" --index-dir "$idx"
    expect_eq_continue "stale shard (size moved): falls back" "catalog_rollup" \
        "$(dirx_kv answered_from "${td}/dirx.stale.size")" || true
    dirx_same "stale shard (size moved): same totals" "$plain" "${td}/dirx.stale.size" || true
    cp -p "${td}/dirx.shard.bak" "$shard"
    dirx_analyze "${td}/dirx.restored" "$out" --subtree "$target" --index-dir "$idx"
    expect_eq_continue "restored shard: the sidecar is used again" "dir_index" \
        "$(dirx_kv answered_from "${td}/dirx.restored")" || true

    cp "${idx}/dirs.idx" "${td}/dirx.dirs.bak"
    head -c 200 "${td}/dirx.dirs.bak" >"${idx}/dirs.idx"
    dirx_analyze "${td}/dirx.trunc.dirs" "$out" --subtree "$target" --index-dir "$idx"
    expect_eq_continue "truncated dirs.idx: falls back" "catalog_rollup" \
        "$(dirx_kv answered_from "${td}/dirx.trunc.dirs")" || true
    dirx_same "truncated dirs.idx: same totals" "$plain" "${td}/dirx.trunc.dirs" || true
    cp "${td}/dirx.dirs.bak" "${idx}/dirs.idx"

    # rowgroups.idx is the optional half: without it the aggregate still comes
    # from dirs.idx and only the scan loses its pruning.
    cp "${idx}/rowgroups.idx" "${td}/dirx.rg.bak"
    head -c 120 "${td}/dirx.rg.bak" >"${idx}/rowgroups.idx"
    dirx_analyze "${td}/dirx.trunc.rg" "$out" --subtree "$target" --index-dir "$idx"
    expect_eq_continue "truncated rowgroups.idx: the aggregate still uses dirs.idx" "dir_index" \
        "$(dirx_kv answered_from "${td}/dirx.trunc.rg")" || true
    dirx_analyze "${td}/dirx.trunc.rg.list" "$out" --subtree "$target" --type f --list --index-dir "$idx"
    expect_eq_continue "truncated rowgroups.idx: the scan still lists the same paths" \
        "$(sort "${td}/dirx.list.plain" | cksum)" "$(sort "${td}/dirx.trunc.rg.list" | cksum)" || true
    cp "${td}/dirx.rg.bak" "${idx}/rowgroups.idx"

    dirx_analyze "${td}/dirx.noidx" "$out" --subtree "$target" --index-dir "${td}/dirx_no_such_index"
    expect_eq_continue "absent index dir: falls back" "catalog_rollup" \
        "$(dirx_kv answered_from "${td}/dirx.noidx")" || true
    dirx_same "absent index dir: same totals" "$plain" "${td}/dirx.noidx" || true

    # 7. The phase is on by default and off on request.
    EREPORT_INDEX_THREADS="${EREPORT_INDEX_THREADS:-4}" \
        "$EREPORT_INDEX" --make --no-dir-index --index-dir "$nidx" "$out" \
        >"${td}/dirx.nomake.out" 2>"${td}/dirx.nomake.err" || {
        tail -n 20 "${td}/dirx.nomake.err" >&2 || true
        die "ereport_index --make --no-dir-index failed"
    }
    if [[ -e "${nidx}/dirs.idx" || -e "${nidx}/rowgroups.idx" ]]; then
        summary_add FAIL "--no-dir-index writes no sidecar" "found one"
        SUMMARY_FAILS=$((SUMMARY_FAILS + 1))
    else
        summary_add PASS "--no-dir-index writes no sidecar" "neither file written"
    fi
    expect_eq_continue "--no-dir-index: meta.txt says so" "0" "$(kv_last dir_index "${nidx}/meta.txt")" || true
    dirx_analyze "${td}/dirx.nodirx" "$out" --subtree "$target" --index-dir "$nidx"
    expect_eq_continue "--no-dir-index: a query against that index dir falls back" "catalog_rollup" \
        "$(dirx_kv answered_from "${td}/dirx.nodirx")" || true

    # 8. ereport's own use of the same sidecars.
    section_int "[integration] dir-index sidecars — ereport --subtree --index-dir"
    ereport_dirx_tests "$td" "$out" "$idx" "$target" single 1

    rm -rf "$tree" "$out" "$idx" "$nidx"

    dirx_hardlink_tests "$td"
    dirx_multishard_tests "$td"
    dirx_stored_path_tests "$td"
}

run_ecrawl_mount_tests() {
    local td=$1 root_abs=$2 crawl_out=$3 want_entries=$4
    local dr="${td}/emount.dryrun" drerr="${td}/emount.dryrun.err"
    local mp="${td}/emount_mp" mroot
    local got p a b n=0 bad=0

    section_int "[integration] ecrawl_mount (smoke)"

    if [[ ! -x "$ECRAWL_MOUNT" ]]; then
        log "skip: ${ECRAWL_MOUNT} not built (needs FUSE headers: make fuse-headers)"
        summary_add SKIP "ecrawl_mount" "binary not built"
        return 0
    fi

    log "ecrawl_mount --dry-run (index build, no mount)"
    "$ECRAWL_MOUNT" --dry-run "$crawl_out" >"$dr" 2>"$drerr" || {
        tail -n 40 "$drerr" >&2 || true
        die "ecrawl_mount --dry-run failed"
    }
    grep -q '^ecrawl_mount$' "$dr" || die "ecrawl_mount --dry-run missing banner line"
    grep -q '^index_memory_bytes=' "$dr" || die "ecrawl_mount --dry-run missing index_memory_bytes"
    # Every record ecrawl wrote must land in the index exactly once.
    expect_eq "ecrawl_mount: records_total vs ecrawl.entries" "$want_entries" "$(kv_last records_total "$dr")" \
        "index covers every record"
    expect_eq "ecrawl_mount: records_skipped" "0" "$(kv_last records_skipped "$dr")"
    expect_eq "ecrawl_mount: shards_unreadable" "0" "$(kv_last shards_unreadable "$dr")"
    summary_add PASS "ecrawl_mount --dry-run" "records_total=${want_entries}"

    if [[ -n "${SKIP_FUSE:-}" ]]; then
        log "skip: live mount comparison (SKIP_FUSE set)"
        summary_add SKIP "ecrawl_mount live mount" "SKIP_FUSE=1"
        return 0
    fi
    if [[ ! -e /dev/fuse ]] || ! command -v fusermount >/dev/null 2>&1; then
        log "skip: live mount comparison (no /dev/fuse or fusermount)"
        summary_add SKIP "ecrawl_mount live mount" "no fuse on this host"
        return 0
    fi

    mkdir -p "$mp"
    log "ecrawl_mount ${crawl_out} -> ${mp}"
    if ! "$ECRAWL_MOUNT" "$crawl_out" "$mp" >"${td}/emount.out" 2>"${td}/emount.err"; then
        tail -n 20 "${td}/emount.err" >&2 || true
        log "skip: mount failed on this host (unprivileged FUSE may be disabled)"
        summary_add SKIP "ecrawl_mount live mount" "mount refused"
        return 0
    fi
    EMOUNT_MP="$mp"
    # Paths are stored absolute, so the crawled tree appears under its own path.
    mroot="${mp}${root_abs}"

    # 1. namespace: the mounted tree must have exactly the crawled paths.
    if ! diff <(cd "$root_abs" && find . | sort) <(cd "$mroot" && find . | sort) >"${td}/emount.pathdiff"; then
        head -n 20 "${td}/emount.pathdiff" >&2 || true
        emount_unmount "$mp"
        die "ecrawl_mount namespace differs from the live tree"
    fi
    expect_eq "ecrawl_mount: namespace matches live tree" "0" "0" "find output identical"

    # 2. per-entry metadata that a crawl does record. atime is deliberately not
    #    compared: the crawl froze it, while walking the live tree here keeps
    #    moving it, so any atime check would be racy rather than meaningful.
    while IFS= read -r p; do
        [[ -n "$p" ]] || continue
        a=$(stat -c '%s %Y %Z %u %h %i %F' "${root_abs}/${p}" 2>/dev/null) || continue
        b=$(stat -c '%s %Y %Z %u %h %i %F' "${mroot}/${p}" 2>/dev/null) || {
            printf 'missing in mount: %s\n' "$p" >&2
            bad=1
            continue
        }
        n=$((n + 1))
        if [[ "$a" != "$b" ]]; then
            printf 'stat mismatch %s\n  live: %s\n  mnt : %s\n' "$p" "$a" "$b" >&2
            bad=1
        fi
    done < <(cd "$root_abs" && find . -printf '%P\n')
    [[ "$n" -gt 0 ]] || { emount_unmount "$mp"; die "ecrawl_mount compared no entries"; }
    if [[ "$bad" != 0 ]]; then
        emount_unmount "$mp"
        die "ecrawl_mount metadata differs from the live tree"
    fi
    expect_eq "ecrawl_mount: stat fields match on all ${n} entries" "0" "$bad" \
        "size/mtime/ctime/uid/nlink/inode/type"

    # 3. du: sizes are exact, so apparent-size totals must agree byte for byte.
    expect_eq "ecrawl_mount: du --apparent-size total" \
        "$(du --apparent-size -sb "$root_abs" | awk '{print $1}')" \
        "$(du --apparent-size -sb "$mroot" | awk '{print $1}')" \
        "byte-exact apparent size"

    # 4. read() yields zeros for the recorded length: size-derived tools stay
    #    correct even though contents were never captured.
    got=$(wc -c <"${mroot}/a.txt")
    expect_eq "ecrawl_mount: read length equals st_size" "$(wc -c <"${root_abs}/a.txt")" "$got"
    got=$(tr -d '\0' <"${mroot}/a.txt" | wc -c)
    expect_eq "ecrawl_mount: file content is zeros" "0" "$got"

    # 5. symlink type survives even though the target does not.
    expect_eq "ecrawl_mount: symlink type preserved" "link_a" "$(cd "$mroot" && find . -type l -printf '%f\n')"
    if readlink "${mroot}/link_a" >/dev/null 2>&1; then
        emount_unmount "$mp"
        die "ecrawl_mount readlink should fail (targets are not recorded)"
    fi
    expect_eq "ecrawl_mount: readlink fails (target not recorded)" "0" "0" "EIO as documented"

    # 6. the mount is genuinely read-only. The redirection itself is what must
    #    fail, so it runs in a subshell whose stderr is discarded.
    if ( : >"${mroot}/should_not_exist" ) 2>/dev/null; then
        emount_unmount "$mp"
        die "ecrawl_mount accepted a write"
    fi
    expect_eq "ecrawl_mount: writes refused" "0" "0" "EROFS"

    # 7. statfs reports the crawl's own totals.
    expect_eq "ecrawl_mount: statfs inode count" "$want_entries" \
        "$(stat -f -c '%c' "$mroot")"

    emount_unmount "$mp" || die "ecrawl_mount: unmount failed"
    summary_add PASS "ecrawl_mount live mount" "namespace+stat+du+read+symlink+ro"
}

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
        emount_cleanup_hook
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

    section_int "[integration] ecrawl_repair, ecrawl_query, edelete, ereport_index (smoke)"

    log "ecrawl_repair --dry-run (sidecar rules vs crawl_bin_load_ckpt)"
    "$ECRAWL_REPAIR" --dry-run "$crawl_out" >"${td}/repair.stdout" 2>"${td}/repair.stderr" || {
        tail -n 40 "${td}/repair.stderr" >&2 || true
        die "ecrawl_repair --dry-run failed"
    }
    summary_add PASS "ecrawl_repair --dry-run" "ran ok"

    log "ecrawl_query (shard scan / parent histogram smoke)"
    "$ECRAWL_QUERY" --top 5 "$crawl_out" >"${td}/analyze.stdout" 2>"${td}/analyze.stderr" || {
        tail -n 40 "${td}/analyze.stderr" >&2 || true
        die "ecrawl_query failed"
    }
    grep -q '^ecrawl_query$' "${td}/analyze.stdout" || die "ecrawl_query missing banner line"
    grep -q '^records_total=' "${td}/analyze.stdout" || die "ecrawl_query missing records_total"
    grep -q '^parse_chunk_jobs=' "${td}/analyze.stdout" || die "ecrawl_query missing parse_chunk_jobs"
    summary_add PASS "ecrawl_query smoke" "banner+records_total+parse_chunk_jobs"

    log "ecrawl_query selective query block-skip parity"
    # Skipping a block on its header summary must be invisible in the answer:
    # same paths, same totals, just less decompression.
    local qskip_out="${td}/query.skip.out" qskip_err="${td}/query.skip.err"
    local qfull_out="${td}/query.full.out" qfull_err="${td}/query.full.err"
    ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --size-gt 5 --type f --list "$crawl_out" \
        >"$qskip_out" 2>"$qskip_err" || die "block-skip query failed"
    ECRAWL_QUERY_THREADS=1 ECRAWL_QUERY_BLOCK_SKIP=0 \
        "$ECRAWL_QUERY" --size-gt 5 --type f --list "$crawl_out" \
        >"$qfull_out" 2>"$qfull_err" || die "full-decompression query failed"
    cmp <(sort "$qskip_out") <(sort "$qfull_out") >/dev/null ||
        die "block-skip and full-decompression listed paths differ"
    local qkey
    for qkey in entries files dirs symlinks other bytes hardlink_dupes records_scanned; do
        expect_eq "query block-skip parity: ${qkey}" "$(kv_last "$qkey" "$qfull_err")" \
            "$(kv_last "$qkey" "$qskip_err")"
    done
    expect_eq "query strict > boundary (size 6 excluded)" "0" \
        "$(ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --size-gt 6 --type f "$crawl_out" 2>/dev/null |
            awk -F= '$1=="entries"{print $2}')"

    local qallskip="${td}/query.allskip"
    ECRAWL_QUERY_THREADS=1 "$ECRAWL_QUERY" --size-gt 1000000000 --type f "$crawl_out" \
        >"$qallskip" 2>/dev/null || die "all-block skip query failed"
    [[ "$(kv_last blocks_skipped "$qallskip")" -gt 0 ]] || die "block-skip query skipped no blocks"
    expect_eq "query avoids decompression when every block is impossible" "0" \
        "$(kv_last blocks_decompressed "$qallskip")"
    # records_scanned must still be right when the records were never decompressed.
    expect_eq "skipped-record accounting matches a full scan" \
        "$(kv_last records_scanned "$qfull_err")" "$(kv_last records_scanned "$qallskip")"
    summary_add PASS "ecrawl_query block skipping" "parity+strict-boundary+all-skip+accounting"

    run_v8_rollup_tests "$td"

    run_dir_index_tests "$td"

    run_ecrawl_no_stat_tests "$td"

    run_ecrawl_statx_tests "$td"

    run_ecrawl_progress_tests "$td"

    run_edelete_tests "$td" "$root_abs"

    local idx_make="${td}/index_make"
    log "ereport_index --make (trigram index under ${idx_make})"
    EREPORT_INDEX_THREADS="${EREPORT_INDEX_THREADS:-4}" \
        "$EREPORT_INDEX" --make --index-dir "$idx_make" "$(id -un)" "$crawl_out" >"${td}/ei.stdout" 2>"${td}/ei.stderr" || {
        tail -n 80 "${td}/ei.stderr" >&2 || true
        die "ereport_index --make failed"
    }
    [[ -f "${idx_make}/tri_keys.bin" && -f "${idx_make}/paths.bin" && -f "${idx_make}/path_isdir.bin" && -f "${idx_make}/path_kids.bin" ]] ||
        die "ereport_index did not write tri_keys.bin / paths.bin / path_isdir.bin / path_kids.bin under ${idx_make}"
    summary_add PASS "ereport_index --make" "tri_keys.bin+paths.bin+path_isdir.bin+path_kids.bin written"

    expect_eq "ereport_index: paths.bin magic" "EPATH002" "$(head -c 8 "${idx_make}/paths.bin")" \
        "paths.bin uses the compressed EPATH002 layout"
    expect_eq "ereport_index: meta version" "4" "$(kv_last ereport_index_version "${idx_make}/meta.txt")" \
        "meta.txt records the children-CSR index version"
    expect_eq "ereport_index: trigrams scope" "basename" "$(kv_last trigrams "${idx_make}/meta.txt")" \
        "meta.txt records basename-only trigrams"

    # Round trip through both compressed files: postings decode, then a path comes back out of paths.bin.
    log "ereport_index --search (compressed index round trip)"
    "$EREPORT_INDEX" --search --index-dir "$idx_make" sub >"${td}/ei_search.out" 2>"${td}/ei_search.err" || {
        tail -n 40 "${td}/ei_search.err" >&2 || true
        die "ereport_index --search failed"
    }
    local ei_hits=0 ei_line
    while IFS= read -r ei_line; do
        [[ -n "$ei_line" ]] || continue
        [[ "$ei_line" == *sub* ]] || die "ereport_index --search returned a path without the term: ${ei_line}"
        [[ -e "$ei_line" ]] || die "ereport_index --search returned a path that does not exist: ${ei_line}"
        ei_hits=$((ei_hits + 1))
    done <"${td}/ei_search.out"
    [[ "$ei_hits" -ge 1 ]] || die "ereport_index --search found no path containing 'sub'"
    summary_add PASS "ereport_index --search" "${ei_hits} path(s) decoded from the compressed index"

    # Basename-only trigrams still find descendants under a matching directory segment.
    log "ereport_index segment-once (directory hit expands to descendant file)"
    local seg_root="${td}/segonce_tree" seg_crawl="${td}/segonce_crawl" seg_idx="${td}/segonce_idx"
    mkdir -p "${seg_root}/unique_dir_xyz/nested"
    printf 'x\n' >"${seg_root}/unique_dir_xyz/nested/leaf_only.dat"
    "$ECRAWL" "$seg_root" "$seg_crawl" >/dev/null 2>"${td}/segonce_crawl.err" || {
        tail -n 40 "${td}/segonce_crawl.err" >&2 || true
        die "ecrawl failed for segment-once fixture"
    }
    EREPORT_INDEX_THREADS="${EREPORT_INDEX_THREADS:-4}" \
        "$EREPORT_INDEX" --make --index-dir "$seg_idx" --no-dir-index "$(id -un)" "$seg_crawl" \
        >"${td}/segonce_make.out" 2>"${td}/segonce_make.err" || {
        tail -n 40 "${td}/segonce_make.err" >&2 || true
        die "ereport_index --make failed on segment-once fixture"
    }
    "$EREPORT_INDEX" --search --index-dir "$seg_idx" unique_dir_xyz >"${td}/segonce_search.out" 2>"${td}/segonce_search.err" || {
        tail -n 40 "${td}/segonce_search.err" >&2 || true
        die "ereport_index --search failed on segment-once fixture"
    }
    [[ -f "${seg_idx}/path_kids.bin" ]] ||
        die "ereport_index --make did not write path_kids.bin for the segment-once fixture"
    grep -q 'leaf_only\.dat$' "${td}/segonce_search.out" ||
        die "ereport_index --search unique_dir_xyz did not expand to nested/leaf_only.dat"
    summary_add PASS "ereport_index segment-once expand" "directory hit returns descendant file"

    # A basename-only file hit must not pull siblings sitting under a different directory.
    log "ereport_index basename-only hit does not expand siblings"
    local sib_root="${td}/sib_tree" sib_crawl="${td}/sib_crawl" sib_idx="${td}/sib_idx"
    mkdir -p "${sib_root}/other_dir"
    printf 'a\n' >"${sib_root}/unique_file_abc"
    printf 'b\n' >"${sib_root}/other_dir/sibling.dat"
    "$ECRAWL" "$sib_root" "$sib_crawl" >/dev/null 2>"${td}/sib_crawl.err" || {
        tail -n 40 "${td}/sib_crawl.err" >&2 || true
        die "ecrawl failed for basename-only sibling fixture"
    }
    EREPORT_INDEX_THREADS="${EREPORT_INDEX_THREADS:-4}" \
        "$EREPORT_INDEX" --make --index-dir "$sib_idx" --no-dir-index "$(id -un)" "$sib_crawl" \
        >"${td}/sib_make.out" 2>"${td}/sib_make.err" || {
        tail -n 40 "${td}/sib_make.err" >&2 || true
        die "ereport_index --make failed on basename-only sibling fixture"
    }
    "$EREPORT_INDEX" --search --index-dir "$sib_idx" unique_file_abc >"${td}/sib_search.out" 2>"${td}/sib_search.err" || {
        tail -n 40 "${td}/sib_search.err" >&2 || true
        die "ereport_index --search failed on basename-only sibling fixture"
    }
    grep -q 'unique_file_abc$' "${td}/sib_search.out" ||
        die "ereport_index --search unique_file_abc missed the file itself"
    if grep -q 'sibling\.dat$' "${td}/sib_search.out"; then
        die "ereport_index --search unique_file_abc pulled sibling.dat"
    fi
    summary_add PASS "ereport_index basename-only hit" "file hit does not pull siblings"

    "$EREPORT_INDEX" --search --index-dir "$idx_make" zzqqxxnotpresent >"${td}/ei_search_none.out" 2>/dev/null || true
    [[ ! -s "${td}/ei_search_none.out" ]] ||
        die "ereport_index --search returned matches for a term that is not in the tree"
    summary_add PASS "ereport_index --search (no match)" "absent term returns nothing"

    # An older index version must be rejected rather than misread.
    local idx_stale="${td}/index_stale"
    cp -r "$idx_make" "$idx_stale"
    sed -i 's/^ereport_index_version=4$/ereport_index_version=2/' "${idx_stale}/meta.txt"
    if "$EREPORT_INDEX" --search --index-dir "$idx_stale" sub >/dev/null 2>"${td}/ei_stale.err"; then
        die "ereport_index --search accepted an index whose meta.txt claims version 2"
    fi
    grep -q "rebuild" "${td}/ei_stale.err" ||
        die "ereport_index --search rejected the stale index without telling the user to rebuild"
    summary_add PASS "ereport_index version gate" "a v2 index is refused with a rebuild hint"

    run_ecrawl_mount_tests "$td" "$root_abs" "$crawl_out" "$ce"

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
