#!/usr/bin/env bash
# Wall sweep: ecrawl --no-write across crawl-thread counts (hot).
#
# Tuned for ZFS / hosts where page-cache drop is unavailable: warm the tree with
# WARMUP_RUNS untimed walks, then time a hot-only matrix (no drop_caches).
#
#   rsync -a ~/git/ereport/ /tmp/ereport/
#   cd /tmp/ereport
#   TREE=/path/on/zfs ./scripts/profile/profile-ecrawl-thread-ratios.sh
#
# Defaults: 3 warmups (24 threads), then 5 measured hot cells.
# No dut, no strace, no linux perf record.
#
# Outputs under OUT/ (default /tmp/ereport-thread-ratios-<utc>/):
#   warmup/{1..N}/{wall.*,cmdline.txt}     untimed priming walks
#   results.tsv
#   c<C>/hot/{wall.*,cmdline.txt}
#   env.txt  README.txt  OUT.tar.gz
#
# Knobs:
#   TREE  OUT  BUILD=1
#   WARMUP_RUNS=3                 0 = skip warmups
#   WARMUP_THREADS=24             crawl threads used for warmups
#   DROP_CACHES=0                 default off (ZFS); set 1 only if cold + root
#   CACHE_MODES=hot               default hot-only; XFS cold: CACHE_MODES='cold hot' DROP_CACHES=1 WARMUP_RUNS=0
#   THREAD_COUNTS='24 16 8 4 2'
#
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
TREE=${TREE:-/data1/erbmi1/ecrawl-synt}
OUT=${OUT:-/tmp/ereport-thread-ratios-$(date -u +%Y%m%d-%H%M%S)}
DROP_CACHES=${DROP_CACHES:-0}
BUILD=${BUILD:-1}
WARMUP_RUNS=${WARMUP_RUNS:-3}
WARMUP_THREADS=${WARMUP_THREADS:-24}

DEFAULT_THREAD_COUNTS=(
  "24"
  "16"
  "8"
  "4"
  "2"
)
# shellcheck disable=SC2206
THREAD_COUNTS=( ${THREAD_COUNTS:-${DEFAULT_THREAD_COUNTS[*]}} )
# shellcheck disable=SC2206
CACHE_MODES=( ${CACHE_MODES:-hot} )

die() { echo "ERROR: $*" >&2; exit 1; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "missing command: $1"; }

# cache_mode: cold → sync + drop_caches when allowed; hot → no-op.
maybe_drop_caches() {
  local cache_mode=$1
  if [[ "$cache_mode" != "cold" ]]; then
    return 0
  fi
  if [[ "$DROP_CACHES" != "1" ]]; then
    return 0
  fi
  if [[ ! -w /proc/sys/vm/drop_caches ]]; then
    echo "WARN: cold pass but /proc/sys/vm/drop_caches not writable (need root); continuing warm" >&2
    return 0
  fi
  sync
  echo 3 > /proc/sys/vm/drop_caches
  echo "    drop_caches=ok"
}

cell_dir() {
  local c=$1 cache=$2
  printf 'c%d/%s' "$c" "$cache"
}

# Pull a key=value from wall.stdout (first match).
kv() {
  local file=$1 key=$2
  awk -F= -v k="$key" '$1 == k { print $2; exit }' "$file" 2>/dev/null || true
}

validate_args() {
  local c
  [[ ${#THREAD_COUNTS[@]} -eq 5 ]] || die "need exactly 5 thread counts, got ${#THREAD_COUNTS[@]} (override THREAD_COUNTS=)"
  [[ ${#CACHE_MODES[@]} -ge 1 ]] || die "need at least one CACHE_MODES entry"
  for c in "${THREAD_COUNTS[@]}"; do
    [[ "$c" =~ ^[0-9]+$ ]] || die "bad thread count '$c'"
    [[ "$c" -ge 1 ]] || die "thread count must be >= 1 in '$c'"
  done
  local m
  for m in "${CACHE_MODES[@]}"; do
    [[ "$m" == "cold" || "$m" == "hot" ]] || die "CACHE_MODES entries must be cold or hot, got '$m'"
  done
  [[ "$WARMUP_RUNS" =~ ^[0-9]+$ ]] || die "WARMUP_RUNS must be a non-negative integer"
  [[ "$WARMUP_THREADS" =~ ^[0-9]+$ ]] || die "WARMUP_THREADS must be a non-negative integer"
}

write_env() {
  {
    echo "timestamp=$(date -Is)"
    echo "hostname=$(hostname)"
    echo "uid=$(id -u) user=$(id -un)"
    echo "repo=$REPO"
    echo "tree=$TREE"
    echo "out=$OUT"
    echo "ecrawl=$REPO/ecrawl"
    echo "DROP_CACHES=$DROP_CACHES"
    echo "WARMUP_RUNS=$WARMUP_RUNS"
    echo "WARMUP_THREADS=$WARMUP_THREADS"
    echo "thread_counts=${THREAD_COUNTS[*]}"
    echo "cache_modes=${CACHE_MODES[*]}"
    echo "cells=$(( ${#THREAD_COUNTS[@]} * ${#CACHE_MODES[@]} ))"
    echo "uname=$(uname -a)"
    findmnt -T "$TREE" -n -o TARGET,SOURCE,FSTYPE 2>/dev/null | sed 's/^/tree_mount=/' || true
    if [[ -x "$REPO/ecrawl" ]]; then
      echo "ecrawl_sha256=$(sha256sum "$REPO/ecrawl" | awk '{print $1}')"
    fi
  } >"$OUT/env.txt"
}

# run_cell <crawl> <cache_mode> [dest_override]
run_cell() {
  local crawl=$1 cache_mode=$2
  local dest=${3:-"$OUT/$(cell_dir "$crawl" "$cache_mode")"}
  mkdir -p "$dest"

  export ECRAWL_CRAWL_THREADS=$crawl

  {
    echo "cache=$cache_mode"
    echo "ECRAWL_CRAWL_THREADS=$crawl"
    printf '%s\n' "$REPO/ecrawl" --no-write "$TREE"
  } >"$dest/cmdline.txt"

  maybe_drop_caches "$cache_mode"
  echo "==> ${cache_mode} c${crawl}"

  local t0 t1 rc
  t0=$(date +%s.%N)
  set +e
  "$REPO/ecrawl" --no-write "$TREE" >"$dest/wall.stdout.txt" 2>"$dest/wall.stderr.txt"
  rc=$?
  set -e
  t1=$(date +%s.%N)
  python3 -c 'import sys; print("%.6f" % (float(sys.argv[2])-float(sys.argv[1])))' "$t0" "$t1" \
    >"$dest/wall.sec.txt"
  echo "rc=$rc" >"$dest/wall.rc.txt"
  echo "    wall_sec=$(cat "$dest/wall.sec.txt") rc=$rc"
}

run_warmups() {
  local n=$WARMUP_RUNS
  if [[ "$n" -le 0 ]]; then
    echo "==> warmup skipped (WARMUP_RUNS=0)"
    return 0
  fi
  local i dest
  echo "==> warmup: $n × --no-write (c${WARMUP_THREADS}), not in results.tsv"
  for i in $(seq 1 "$n"); do
    dest="$OUT/warmup/$i"
    echo "==> warmup $i/$n"
    # hot: never drop during warmup
    run_cell "$WARMUP_THREADS" hot "$dest"
  done
}

append_tsv_row() {
  local tsv=$1 crawl=$2 cache_mode=$3
  local dest="$OUT/$(cell_dir "$crawl" "$cache_mode")"
  local wall_sec rc elapsed entries
  wall_sec=$(cat "$dest/wall.sec.txt")
  rc=$(sed -n 's/^rc=//p' "$dest/wall.rc.txt")
  elapsed=$(kv "$dest/wall.stdout.txt" elapsed_sec)
  entries=$(kv "$dest/wall.stdout.txt" entries)
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$crawl" "$cache_mode" "$wall_sec" "$rc" "${elapsed:-}" "${entries:-}" >>"$tsv"
}

main() {
  [[ -d "$TREE" ]] || die "TREE does not exist: $TREE"
  need_cmd gcc
  need_cmd make
  need_cmd python3
  need_cmd gzip
  validate_args

  if [[ "$DROP_CACHES" == "1" ]] && [[ "$(id -u)" -ne 0 ]]; then
    echo "WARN: DROP_CACHES=1 but not root — cold drops may fail" >&2
  fi

  local ncells=$(( ${#THREAD_COUNTS[@]} * ${#CACHE_MODES[@]} ))
  mkdir -p "$OUT"
  echo "REPO=$REPO TREE=$TREE OUT=$OUT"
  echo "thread_counts: ${THREAD_COUNTS[*]}"
  echo "cache_modes: ${CACHE_MODES[*]}  (cells=$ncells)"
  echo "warmup: runs=$WARMUP_RUNS threads=$WARMUP_THREADS DROP_CACHES=$DROP_CACHES"

  if [[ "$BUILD" == "1" ]]; then
    echo "==> building ecrawl"
    make -C "$REPO" -j"$(nproc)" ecrawl
  fi
  [[ -x "$REPO/ecrawl" ]] || die "ecrawl binary missing at $REPO/ecrawl"

  write_env

  run_warmups

  local tsv="$OUT/results.tsv"
  printf 'crawl\tcache\twall_sec\trc\telapsed_sec\tentries\n' \
    >"$tsv"

  local crawl cache
  for crawl in "${THREAD_COUNTS[@]}"; do
    for cache in "${CACHE_MODES[@]}"; do
      run_cell "$crawl" "$cache"
      append_tsv_row "$tsv" "$crawl" "$cache"
    done
  done

  {
    echo "OUT=$OUT"
    echo "results=$tsv"
    echo "thread_counts=${THREAD_COUNTS[*]}"
    echo "cache_modes=${CACHE_MODES[*]}"
    echo "warmup_runs=$WARMUP_RUNS warmup_threads=$WARMUP_THREADS"
    echo "cells=$ncells"
    echo
    if [[ -d "$OUT/warmup" ]]; then
      echo "warmup wall_sec:"
      for _wu in $(seq 1 "$WARMUP_RUNS"); do
        if [[ -f "$OUT/warmup/$_wu/wall.sec.txt" ]]; then
          echo "  $_wu  $(cat "$OUT/warmup/$_wu/wall.sec.txt")"
        fi
      done
      echo
    fi
    column -t -s $'\t' "$tsv" 2>/dev/null || cat "$tsv"
    echo
    echo "fastest by cache (wall_sec):"
    python3 - <<'PY' "$tsv"
import sys
path = sys.argv[1]
rows = []
with open(path) as f:
    hdr = next(f).rstrip("\n").split("\t")
    for line in f:
        d = dict(zip(hdr, line.rstrip("\n").split("\t")))
        rows.append(d)
caches = []
for r in rows:
    if r["cache"] not in caches:
        caches.append(r["cache"])
for cache in caches:
    sub = [r for r in rows if r["cache"] == cache]
    sub.sort(key=lambda r: float(r["wall_sec"]))
    print(f"-- {cache} --")
    print("crawl\twall_sec")
    for r in sub[:5]:
        print(f"{r['crawl']}\t{r['wall_sec']}")
PY
  } | tee "$OUT/README.txt"

  local tar="$OUT.tar.gz"
  tar --no-same-owner -C "$(dirname "$OUT")" -czf "$tar" "$(basename "$OUT")" 2>/dev/null \
    || tar -C "$(dirname "$OUT")" -czf "$tar" "$(basename "$OUT")"
  echo "tarball=$tar"
}

main "$@"
