#!/usr/bin/env bash
#
# ecrawl-mem-arc-snapshot.sh
#
# SPDX-License-Identifier: MIT
#
# Capture a read-only snapshot of why a *running* ecrawl crawl on ZFS is slowing
# down: the crawler's resident memory vs system RAM vs the ZFS ARC, plus a short
# ARC / disk time series. Use this when a long production crawl decays to a low
# stat/s rate and a CPU (perf cycles) profile points at ZFS metadata lookups
# (zfs_zget / zap_lockdir / arc_read) — the cycles profile shows where CPU goes,
# but this snapshot shows whether the ARC metadata working set has been squeezed
# (often by ecrawl's own RSS) into cold random-read thrash.
#
# Everything here is read-only: it only reads /proc, /sys, ZFS kstats, and runs
# arcstat / iostat / zpool in reporting mode. It changes nothing.
#
# What to read out of the log:
#   - VmRSS / VmHWM (ecrawl) vs `free -g`     -> RAM ecrawl steals from ARC
#   - arcstats size vs c vs c_max             -> ARC pinned at cap?
#   - arcstats *metadata* / dnode usage       -> metadata cache starved?
#   - arcstat miss% / dm% (demand-metadata)   -> cold-metadata thrash
#   - iostat r_await + %util on pool NVMe     -> random small-read wall
#   - zpool list -v FRAG / CAP                -> pool aging context
#
# Usage:
#   scripts/tools/ecrawl-mem-arc-snapshot.sh [pid] [log_file]
#
# Args (both optional):
#   pid       ecrawl PID. Default: auto-detect the oldest `ecrawl` process.
#   log_file  Output path (appended). Default: ./ecrawl-mem-arc.log
#
# Env knobs:
#   SAMPLE_INTERVAL  seconds between time-series samples (default 5)
#   SAMPLE_COUNT     number of time-series samples (default 6)
#
# ARC hit-rate tooling differs by ZFS build, so the live-hit-rate section probes
# (in order) arcstat then zarcstat; if neither exists it derives ARC / demand-
# metadata / L2ARC hit% straight from /proc/spl/kstat/zfs/arcstats deltas over the
# sample window. A one-shot arc_summary / zarcsummary dump is appended when present.
#
# Run it a few times over the crawl (e.g. every ~30 min) to see the trend; each
# run appends a timestamped block.

set -uo pipefail

PID="${1:-}"
LOG="${2:-./ecrawl-mem-arc.log}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-5}"
SAMPLE_COUNT="${SAMPLE_COUNT:-6}"

if [ -z "$PID" ]; then
    PID="$(pgrep -o -x ecrawl 2>/dev/null || true)"
fi

{
    echo "==================================================================="
    echo "===== $(date -Is)  ecrawl pid=${PID:-<not found>} ====="
    echo "==================================================================="

    if [ -n "$PID" ] && [ -r "/proc/$PID/status" ]; then
        echo "----- ecrawl process memory (/proc/$PID/status) -----"
        grep -E 'VmRSS|VmHWM|VmData|VmSize|Threads' "/proc/$PID/status"
    else
        echo "----- ecrawl process memory: pid not found / not readable -----"
        echo "  hint: ps -eo pid,comm,args | grep -i [e]crawl"
    fi

    echo "----- system memory (free -g) -----"
    free -g

    echo "----- ARC summary (full kstat: size, c, c_max, hits, misses, demand_metadata_misses, evict_*) -----"
    if [ -r /proc/spl/kstat/zfs/arcstats ]; then
        cat /proc/spl/kstat/zfs/arcstats
    else
        echo "  /proc/spl/kstat/zfs/arcstats not present (ZFS not loaded?)"
    fi

    echo "----- ZFS module tunables -----"
    for t in zfs_arc_max zfs_arc_meta_balance zfs_arc_dnode_limit_percent \
             zfs_prefetch_disable zfs_arc_meta_limit_percent; do
        if [ -r "/sys/module/zfs/parameters/$t" ]; then
            printf '  %s = ' "$t"; cat "/sys/module/zfs/parameters/$t"
        else
            printf '  %s = (n/a)\n' "$t"
        fi
    done

    echo "----- live ARC hit rate (watch: miss%, dm% = demand-metadata miss, arcsz vs c) -----"
    # Tool name varies by ZFS build: arcstat (most), zarcstat (some 2.x packagings).
    ARCSTAT_BIN=""
    for b in arcstat zarcstat; do
        if command -v "$b" >/dev/null 2>&1; then ARCSTAT_BIN="$b"; break; fi
    done
    if [ -n "$ARCSTAT_BIN" ]; then
        echo "  ($ARCSTAT_BIN ${SAMPLE_INTERVAL}s x${SAMPLE_COUNT})"
        "$ARCSTAT_BIN" "$SAMPLE_INTERVAL" "$SAMPLE_COUNT" 2>/dev/null \
            || echo "  $ARCSTAT_BIN present but failed to run"
    else
        # No arcstat tool: derive a hit-rate delta straight from the kstat over the window.
        KS=/proc/spl/kstat/zfs/arcstats
        if [ -r "$KS" ]; then
            echo "  (arcstat/zarcstat not found; computing delta from $KS over $((SAMPLE_INTERVAL * SAMPLE_COUNT))s)"
            read_ks() { awk -v k="$1" '$1==k{print $3}' "$KS"; }
            h0=$(read_ks hits);  m0=$(read_ks misses)
            dmh0=$(read_ks demand_metadata_hits); dmm0=$(read_ks demand_metadata_misses)
            l2h0=$(read_ks l2_hits); l2m0=$(read_ks l2_misses)
            sleep "$((SAMPLE_INTERVAL * SAMPLE_COUNT))"
            h1=$(read_ks hits);  m1=$(read_ks misses)
            dmh1=$(read_ks demand_metadata_hits); dmm1=$(read_ks demand_metadata_misses)
            l2h1=$(read_ks l2_hits); l2m1=$(read_ks l2_misses)
            awk -v h=$((h1-h0)) -v m=$((m1-m0)) \
                -v dmh=$((dmh1-dmh0)) -v dmm=$((dmm1-dmm0)) \
                -v l2h=$((l2h1-l2h0)) -v l2m=$((l2m1-l2m0)) 'BEGIN{
                  tot=h+m; dmtot=dmh+dmm; l2tot=l2h+l2m;
                  printf "  ARC      : hits=%d misses=%d  miss%%=%.1f\n", h, m, (tot? 100.0*m/tot:0);
                  printf "  demand-md: hits=%d misses=%d  miss%%=%.1f\n", dmh, dmm, (dmtot?100.0*dmm/dmtot:0);
                  printf "  L2ARC    : hits=%d misses=%d  hit%%=%.1f  (low L2 hit%% => cold regions miss L2 and fall to pool disks)\n", l2h, l2m, (l2tot?100.0*l2h/l2tot:0);
                }'
        else
            echo "  no arcstat/zarcstat and no $KS — cannot sample ARC hit rate"
        fi
    fi

    echo "----- one-shot ARC summary (arc_summary / zarcsummary, if present) -----"
    ARCSUM_BIN=""
    for b in arc_summary zarcsummary arcsummary; do
        if command -v "$b" >/dev/null 2>&1; then ARCSUM_BIN="$b"; break; fi
    done
    if [ -n "$ARCSUM_BIN" ]; then
        "$ARCSUM_BIN" 2>/dev/null || echo "  $ARCSUM_BIN present but failed to run"
    else
        echo "  arc_summary/zarcsummary not found (raw kstat above already has size/c/c_max/meta/l2_*)"
    fi

    echo "----- iostat ${SAMPLE_INTERVAL}s x${SAMPLE_COUNT} (NVMe r/s, r_await, %util on pool disks) -----"
    iostat -x "$SAMPLE_INTERVAL" "$SAMPLE_COUNT" 2>/dev/null || echo "  iostat not installed (sysstat)"

    echo "----- pool layout / fragmentation -----"
    zpool list -v 2>/dev/null || echo "  zpool not available"
    zpool status 2>/dev/null || true

    echo
} >> "$LOG" 2>&1

echo "ecrawl-mem-arc-snapshot: appended snapshot for pid=${PID:-<none>} to $LOG"
