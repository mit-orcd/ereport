#!/usr/bin/env bash
#
# Build directory layouts that tend to crawl slowly with ecrawl's current model:
#   - One very large *flat* directory of regular files (single-threaded readdir+fstatat;
#     nothing to donate off the stack).
#   - An optional skinny-deep chain (little parallelism until the wide part is done).
#   - Optional wide-shallow fanout (many sibling dirs with moderate file counts) to
#     contrast with the single megadir.
#
# Usage:
#   ./scripts/generate-ecrawl-adversarial-tree.sh [output_root]
#
# Presets (optional — only fill vars you did not already export):
#   SYNTH_PROFILE=           # unset or empty: quick smoke (~100k files in megadir; ecrawl may finish in ~1s on fast local disk)
#   SYNTH_PROFILE=medium     # ~2M flat files + wide_shallow + depth_slash_profile (still bounded by DISK_BUDGET_BYTES)
#   SYNTH_PROFILE=heavy      # ~12M flat + larger wide + depth slice (aims for long ecrawl --no-write on megadir + extras)
#   SYNTH_PROFILE=extreme    # Same scale defaults as heavy for single_huge_dir/chain/wide/depth_slice, plus extra flat megadirs:
#                            #   mega_dir1/ — SYNTH_EXTREME_MEGA_DIR1_FILES (default 20M) regular files in one directory
#                            #                (names f000000000…f019999999 for 20M; zero-padded width grows if count exceeds 1e9-1).
#                            #                Always unsharded (true single-directory megadir); creation ignores SYNTH_FLAT_SHARD_CAP.
#                            #   mega_dir2/ — SYNTH_EXTREME_MEGA_DIR2_TOP_FILES (default 2M) top-level files f000000000…,
#                            #                plus SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS (default 1M) subdirs d000000000…,
#                            #                each containing exactly one regular file named `file` (3M files + 1M dirs under mega_dir2).
#                            # Extreme adds large inode/metadata load — raise DISK_BUDGET_BYTES (default 100GiB estimate cap),
#                            # lower ASSUMED_BYTES_PER_FLAT_FILE if your fs metadata is cheaper in practice, or set AUTO_CAP_FLAT=1
#                            # (caps single_huge_dir FLAT_FILES only; mega_dir estimates still count toward the budget — if the tree
#                            # alone exceeds DISK_BUDGET_BYTES, raise the budget or unset SYNTH_PROFILE=extreme / export mega counts to 0).
#                            # Tunables (optional exports; extreme preset sets defaults):
#                            #   SYNTH_EXTREME_MEGA_DIR1_FILES=20000000
#                            #   SYNTH_EXTREME_MEGA_DIR2_TOP_FILES=2000000
#                            #   SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS=1000000
#   For real stress, push FLAT_FILES into the millions; a single huge directory is what pins one crawl thread on readdir.
#
# Environment (defaults are safe for a quick local test):
#   FLAT_FILES=100000          # files under single_huge_dir/
#   DEPTH_CHAIN=700             # dir levels deep_skinny_chain/0/1/2/… (0 = skip). Each segment grows (/999 …),
#                               so long chains hit PATH_MAX (~4096); values above the safe limit are clamped.
#   IGNORE_DEPTH_PATH_MAX=0    # set 1 to skip clamping (mkdir will fail with “File name too long” if exceeded).
#   BATCH_CREATE=1             # use python3 bulk create for flat dir if available
#   CREATE_JOBS=0              # parallel file creation when using python3: 0 = auto (I/O-friendly thread count),
#                               or set e.g. 16; 1 = serial. (Ignored for bash-only fallbacks.)
#
# Disk budget (avoid accidentally filling storage; empty files still cost metadata):
#   DISK_BUDGET_BYTES          # default: 100 GiB upper bound on *estimated* footprint
#   ASSUMED_BYTES_PER_FLAT_FILE  # default 4096 (pessimistic: tiny file may cost a block)
#   ASSUMED_BYTES_PER_CHAIN_DIR    # default 4096 per chain directory
#   AUTO_CAP_FLAT=1            # default: clamp FLAT_FILES to fit DISK_BUDGET_BYTES (set 0 to error instead)
#   SPARSE_FILE_MIB=0          # if >0, each flat file is sparse-truncated to this many MiB
#                               (logical size; sparse blocks often use little space — still capped in estimate)
#
# Optional wide tree under wide_shallow/:
#   WIDE_PARENTS=0             # number of sibling dirs (0 = skip)
#   WIDE_FILES_EACH=1000       # regular files per sibling dir
#
# Optional depth/slash profile (matches ecrawl_analyze "depth_bin_*" = count of '/' in stored path
# relative to crawl root — one leading segment depth_slash_profile/ adds one slash):
#   DEPTH_SLICE_ENABLE=0       # set 1 to create depth_slash_profile/…
#   DEPTH_PEAK_LO=12 DEPTH_PEAK_HI=16     # "mass" bins (inclusive)
#   DEPTH_PEAK_FILES_PER_BIN=50000
#   DEPTH_PLATEAU_LO=41 DEPTH_PLATEAU_HI=47   # deep plateau bins (inclusive), default ~1648 files/bin
#   DEPTH_PLATEAU_FILES_PER_BIN=1648
#   Keep PEAK and PLATEAU ranges disjoint. Bins < 3 are skipped.
#
# Ecrawl rough runtime hints (for --no-write; NFS/production varies wildly):
#   ECRAWL_FLAT_LOW=1000       # pessimistic flat-dir entries/s
#   ECRAWL_FLAT_HIGH=30000     # optimistic flat-dir entries/s
#
# Optional ereport heat-map fixtures under ereport_badge_fixtures/ (see BADGE_* vars):
#   skew_cell/       — one megadir + one deep branch (same small size bucket) stamped together for Skew + C-led drill
#   heatmap_grid/    — age×size lattice (6×6): mtimes/atimes per ereport age row; sparse logical sizes per column fall **inside**
#                      ereport size_bucket_for() bands so each heat-map cell gets bytes/files (defaults: random counts up to tens of thousands).
#                      Sparse seeks make multi‑GiB **logical** sizes (what ecrawl/ereport sum); totals can reach **PB** class while disk stays small.
#                      single_huge_dir/ files are &lt;4K and mostly sit in the youngest age row — lower FLAT_FILES if that cell should not
#                      visually swamp the &lt;4K column in file-share.
#                      Oldest row maps to index.html “3+ years” (ereport age bucket 5): dirs heatmap_grid/…/ab05/sb*/, mtimes
#                      BADGE_HEATMAP_OLDEST_DAYS in the past (default 2000 ≥ 3×365). Needs ereport mtime or atime —
#                      effective uses max(a,m,c) and Linux refreshes ctime when utime() sets a/m → stamped cells often read as C-led.
#                      Default BADGE_HEATMAP_DEEP_PREFIX_LEVELS adds heatmap_grid/_d00/… under **showcase** cells so paths have ≥12 slashes (Deep badge exercise); neutral cells stay shallow (see BADGE_HEATMAP_BADGE_CELL_FRAC).
#   dense_multi_age/ — single megadir (>=8192 files) sharded under megadir/sNNNN/… so **Dense** rarely triggers (by design)
#   dense_flat_cell/ — unsharded megadir under one heat-map cell for a reliable **Dense-only** slice (see BADGE_DENSE_FLAT_*; default skips that cell in heatmap_grid)
#   dense_flat_cell_BS/ — additional Dense-only slices, one per (age_band B, size_sb S) pair in BADGE_DENSE_FLAT_EXTRA_PAIRS;
#                          each is its own unsharded megadir (≥8192 children, shallow paths). Heatmap skips matching cells by default
#                          so each extra slice stays Dense-only. Defaults pick four cells outside skew_cell rows (ab02/ab03), the
#                          deep_only ab*/sb00 row, and the primary dense_flat_cell (ab05/sb01).
#   deep_multi_age/  — deep paths only, grouped by utime (still overlaps Dense in rows that also have megadir files)
#   deep_only_cell/  — optional (BADGE_DEEP_ONLY_N>0): deep paths under deep_only_cell/deep_branch/… with utime for one age row
#                      only; dense_multi_age skips that row so no megadir in that band. By default heatmap_grid **does not** fill
#                      heatmap_grid/ab{band}/sb00 so deep bytes can exceed ereport’s Deep threshold (&gt;=30% of slice bytes from paths
#                      with &gt;=12 slashes); set BADGE_HEATMAP_FILL_DEEP_ONLY_S0=1 to populate that cell anyway. Keep BADGE_SKEW_STAMP_DAYS
#                      out of that band’s day range (AGE_DAYS / BADGE_DEEP_ONLY_BAND) or skew_cell’s megadir will add Dense there.
#
#   EREPORT_BADGE_FIXTURES=1      # default 1
#   BADGE_DENSE_N=8500          # skew_cell megadir (>= 8192 for Dense)
#   BADGE_DEEP_N=5200           # skew_cell deep leaf files (>=~30% skew bytes vs dense for Skew pill)
#   BADGE_EXTRA_SKEW_CELL=1      # second skew_cell_b/ (same counts, BADGE_EXTRA_SKEW_STAMP_DAYS age) → extra Skew slice
#   BADGE_EXTRA_SKEW_STAMP_DAYS=265
#   BADGE_FILE_BYTES=512        # skew + multi-age small files: >0 and <4096 (same size bucket)
#   BADGE_DEEP_SEGMENTS=20      # dirs under skew_cell/deep_branch/… (stored path depth / slashes)
#   BADGE_SKEW_STAMP_DAYS=120   # skew_cell only: days ago for atime=mtime (default keeps skew_cell out of “1–3y” row used for Deep-only)
#   BADGE_DEEP_ONLY_N=4000      # 0 disables deep_only_cell/ + restores megadir in all 6 age bands; else deep-only fixture count
#   BADGE_DEEP_ONLY_BAND=4      # age row index 0..5 where Deep-only land (default 4 = ereport “1–3 years”; megadir skipped there)
#
#   BADGE_HEATMAP_GRID=1              # 0 skips heatmap_grid/ (then age×size cells may lack dedicated fixtures)
#   BADGE_HEATMAP_RANDOM=1            # 1 (default): random file count per inner cell in [BADGE_GRID_FILES_MIN, BADGE_GRID_FILES_MAX]
#   BADGE_GRID_FILES_MIN=400          # with BADGE_HEATMAP_RANDOM=1 — light cells (still thousands-scale spread vs max)
#   BADGE_GRID_FILES_MAX=78000        # with BADGE_HEATMAP_RANDOM=1 — hot cells (ten-thousands deltas between buckets; sparse extents)
#   BADGE_HEATMAP_RANDOM_SEED=        # optional: set integer for reproducible counts (same seed → same layout)
#   BADGE_HEATMAP_BADGE_CELL_FRAC=0.30   # fraction of the 6×6 lattice treated as “showcase” cells (deep _d-prefix, full counts).
#                                       # Default ~30% ⇒ ~11 cells keep badge-heavy layout; ~70% are “neutral” (shallow ab**/sb**, ≤19 files/cell
#                                       # by default — below ereport PATH_SHAPE_MIN_BUCKET_FILES=20 — no Dense megadir). Skipped cells
#                                       # (dense_flat / deep_only sb00 / extras) are excluded from fillable slots; frac applies to 36-slot count:
#                                       # n_showcase = round(36×frac) capped by fillable cells. Use 1.0 for legacy “every filled cell is showcase”.
#   BADGE_HEATMAP_SHOWCASE_SEED=       # optional int: which cells are showcase (deterministic). Empty → BADGE_HEATMAP_RANDOM_SEED, else 1704067200.
#   BADGE_HEATMAP_NEUTRAL_MAX_FILES=19    # neutral cells: max files per cell (must be &lt;20 = PATH_SHAPE_MIN_BUCKET_FILES in ereport.c)
#   BADGE_HEATMAP_NEUTRAL_MIN_FILES=3     # neutral cells: min files per cell (random mode); uniform mode clamps grid_n into this range
#   BADGE_GRID_FILES_PER_CELL=35      # used only when BADGE_HEATMAP_RANDOM=0 — uniform count per cell
#   BADGE_HEATMAP_FILL_DEEP_ONLY_S0=  # when BADGE_DEEP_ONLY_N>0: unset/0 skips heatmap &lt;4K cell on BADGE_DEEP_ONLY_BAND (Deep drill); 1 fills it
#   BADGE_HEATMAP_SKIP_DENSE_FLAT_CELL=1  # when dense_flat_cell enabled: omit matching heatmap cell (see header above)
#   BADGE_HEATMAP_SKIP_DENSE_FLAT_CELL=1  # when BADGE_DENSE_FLAT_ENABLE=1: leave ab{BAND}/sb{SB} empty in heatmap_grid so dense_flat_cell isn’t mixed with deep-prefix bytes (clean **Dense-only** slice)
#   BADGE_HEATMAP_OLDEST_DAYS=2000     # age for heatmap row ab05 / multi_age oldest band (≥1095 required for “3+ years”)
#   BADGE_HEATMAP_DEEP_PREFIX_LEVELS=8 # extra nested dirs under heatmap_grid/ (_d00/…) before **showcase** ab**/sb** only (neutral cells stay shallow under heatmap_grid/ab**/sb**); 0 = all shallow
#   BADGE_MULTI_DENSE_N=8192          # files in dense_multi_age/megadir (>=8192 total; sharded so no dir hits Dense)
#   BADGE_MULTI_DEEP_N=5500           # files under deep_multi_age/… (deep path slices)
#   BADGE_GRID_MAX_FILES_PER_DIR=4096 # heatmap: shard files into per-cell subdirs (stay below 8192 Dense threshold)
#   BADGE_GRID_S0_FRAC=0.18           # heatmap &lt;4K column only: scale random/uniform counts (more Deep/Skew vs shallow noise)
#   BADGE_HEATMAP_CORPUS_BLEND_NUM=85 BADGE_HEATMAP_CORPUS_BLEND_DEN=100   # multiply heatmap_grid files/cell by NUM/DEN (default 85:100); raise dilution / lower inner share vs margins
#   BADGE_MEGADIR_SHARD_CAP=4096      # dense_multi_age: files under megadir/sNNNN/… (same Dense avoidance)
#
#   BADGE_DENSE_FLAT_ENABLE=1       # set 0 to skip dense_flat_cell/
#   BADGE_DENSE_FLAT_N=8500         # files directly under …/flat_megadir/ (≥8192)
#   BADGE_DENSE_FLAT_BAND=5         # heat-map age row 0..5 (default ab05 “3+ years”) — avoid ab02/ab03 where skew_cell/skew_cell_b land (~120d / ~265d)
#   BADGE_DENSE_FLAT_SB=1           # heat-map size column 0..5 (default sb01 “4K–1M” sparse logical size; avoids deep_only_cell on sb00 + youngest single_huge_dir sb00)
#
#   BADGE_DENSE_FLAT_EXTRA_ENABLE=1                    # set 0 to skip extra dense_flat_cell_*/ slices
#   BADGE_DENSE_FLAT_EXTRA_N=8500                      # files per extra cell (≥8192; same Dense threshold)
#   BADGE_DENSE_FLAT_EXTRA_PAIRS="0:5,1:3,4:2,5:4"   # comma-separated BAND:SB pairs (each 0..5); four extra Dense-only slices by default
#                                                     # — ab00/sb05 (≥10G youngest), ab01/sb03 (100M–1G recent),
#                                                     # — ab04/sb02 (1M–100M ~1–3y), ab05/sb04 (1G–10G oldest);
#                                                     # avoid ab02/ab03 (skew rows), ab04/sb00 (deep_only),
#                                                     # and the primary BADGE_DENSE_FLAT_BAND/SB cell.
#   BADGE_HEATMAP_SKIP_DENSE_FLAT_EXTRA_CELLS=1        # 0 to fill those heatmap cells anyway (mixes Dense-only with grid noise)
#
#   BADGE_HEATMAP_ROW_DAY_BIAS=     # optional: six comma-separated ints added to grid/dense_flat ages (days), e.g.
#                                   # "0,8,0,15,0,0" nudges mtimes within the same ereport age buckets for broader spread.
#
# Heat-map row totals, column totals, and grand corner call the same emit_heat_map_badges() as inner cells (ereport.c):
#   C-led share ≥ EREPORT_HEAT_CTIME_LED_MIN_SHARE (default 30%) on slice bytes; Deep share ≥30% of slice bytes from paths with ≥12 '/'
#   (and ≥20 files in slice); Dense when max immediate-child count among parents that contribute files to that slice is ≥8192.
#   Row/col badges aggregate shape_deep_bytes and merged dense-parent maps across that row/column; grand corner merges the whole heat map,
#   so one crawl-wide megadir (classic single_huge_dir) forces Dense on the corner even when inner cells are sharded.
#
# Margin dilution (optional — lowers aggregate Deep/C-led/Dense vs inner demo cells without editing ereport):
#   BADGE_MARGIN_DILUTION_ENABLE=1       # default 1: emit neutral_flat/ (set 0 for legacy: no dilution tree)
#   BADGE_MARGIN_NEUTRAL_FILES=520000    # when ENABLE=1: many shallow regular files under neutral_flat/ (0 disables neutral tree only)
#   BADGE_MARGIN_NEUTRAL_PARENT_CAP=4096 # max files per leaf dir (<8192 Dense); also caps shard bucket fan-out under each age band
#   BADGE_MARGIN_NEUTRAL_DEPTH=2         # extra shallow dir levels (x0_XX/x1_XX/…) before bandNN/h… — spreads parents; no ≥12-'/' paths
#   BADGE_MARGIN_NEUTRAL_BYTES=512       # payload per file (<4K bucket); keeps dilution cheap vs sparse heat-map giants
#   BADGE_MARGIN_NEUTRAL_UTIME=1         # 1: stamp atime=mtime cycling ereport age rows (matches heatmap_grid AGE_DAYS + BADGE_HEATMAP_ROW_DAY_BIAS)
#   SYNTH_FLAT_SHARD_CAP=4096            # default 4096: shard FLAT_FILES under single_huge_dir/sNNNN/… (<8192 entries per leaf dir).
#                                       # Set **0** for a **true single-directory megadir**: all FLAT_FILES live directly under
#                                       single_huge_dir/ as f000000000 … (same multi-threaded python3 creation when BATCH_CREATE=1).
#                                       Example — ~20M files in one directory for ecrawl readdir stress:
#                                         FLAT_FILES=20000000 SYNTH_FLAT_SHARD_CAP=0 ./scripts/generate-ecrawl-adversarial-tree.sh /tmp/x
#                                       (Raise DISK_BUDGET_BYTES / inode limits as needed; naming supports up to 999999999 files.)
#
# Optional heat-map footprint vs dilution (inner cells can stay badge-rich at lower absolute counts):
#   BADGE_HEATMAP_CORPUS_BLEND_NUM=85 BADGE_HEATMAP_CORPUS_BLEND_DEN=100   # default scales heatmap_grid counts slightly down vs margins
#
# Note: skew_cell / dense_flat_cell / BADGE_DENSE_FLAT_EXTRA_* use intentional unsharded megadirs (≥8192). They still raise crawl-wide max parent fan-out,
#   so the heat-map grand corner may keep a Dense badge even when single_huge_dir is sharded — dilution mainly softens Deep/C-led aggregate ratios and removes
#   megadir-driven Dense from the flat workload path. **Intentional megadirs can still force Dense on row/column/grand aggregates**; only removing or sharding those fixtures clears it completely.
#
# Random numeric ownership (ecrawl records st_uid from stat; no passwd entry required). Needs root or CAP_CHOWN for arbitrary UIDs:
#   SYNTH_RANDOM_UID_ENABLE=1       # default 1: after generation, sample-chown regular files under scope (skipped automatically if probe chown fails)
#   SYNTH_RANDOM_UID_SCOPE=badge    # badge = only $ROOT/ereport_badge_fixtures (default); all = entire $ROOT (slow on huge FLAT_FILES)
#   SYNTH_RANDOM_UID_FRACTION=0.12 # Bernoulli probability per file while walking (cap below)
#   SYNTH_RANDOM_UID_MAX_CHOWN=40000  # stop after this many successful chowns (limits work on massive trees)
#   SYNTH_RANDOM_UID_MIN=100000 SYNTH_RANDOM_UID_MAX=199999  # inclusive UID pool draw range when probe succeeds
#   SYNTH_RANDOM_UID_UNIQUE_MAX=5000 # at most this many distinct numeric UIDs are used (sampled without replacement from [MIN,MAX])
#   SYNTH_RANDOM_UID_SEED=        # optional int for reproducible picks (fraction positions still walk-dependent unless scope is stable)
#
# Time basis (ereport CLI: mtime | atime | ctime | effective):
#   Fixtures call utime(path, (atime, mtime)) with paired values. On Linux that stamp updates ctime to ~now, so ctime ≫ max(a,m)
#   and ereport marks most bytes as C-led (≥180d newer). That is expected — not a crawl bug. To quiet purple badges in HTML:
#   EREPORT_HEAT_CTIME_LED_MIN_SHARE=0.45 ./ereport …
#   — mtime or atime basis: age buckets follow the stamped times (spread across ~5 years via grid + multi-age lanes).
#   — effective basis: max(a,m,c) is usually dominated by new ctime → most files land in the youngest age column; use mtime/atime
#     to exercise the heat map and badge drills.
#
# Recommended HTML emit (mtime basis, bucket drill-down):
#   EREPORT_HEAT_CTIME_LED_MIN_SHARE=0.45 ./ereport --bucket-details 1 <uid|name> mtime '$ROOT'
#
# Example stress (still under default budget if ASSUMED_BYTES_PER_FLAT_FILE=4096):
#   FLAT_FILES=12000000 DEPTH_CHAIN=700 ./scripts/generate-ecrawl-adversarial-tree.sh /tmp/ecrawl_adv
#
set -euo pipefail

ROOT=${1:-./ecrawl_adversarial_scratch}
SYNTH_PROFILE=${SYNTH_PROFILE:-}

case "$SYNTH_PROFILE" in
 medium)
   echo "generate-ecrawl-adversarial-tree: SYNTH_PROFILE=medium (override any default by exporting vars before this script)" >&2
   FLAT_FILES=${FLAT_FILES:-2000000}
   DEPTH_CHAIN=${DEPTH_CHAIN:-200}
   WIDE_PARENTS=${WIDE_PARENTS:-64}
   WIDE_FILES_EACH=${WIDE_FILES_EACH:-8000}
   DEPTH_SLICE_ENABLE=${DEPTH_SLICE_ENABLE:-1}
   DEPTH_PEAK_FILES_PER_BIN=${DEPTH_PEAK_FILES_PER_BIN:-20000}
   ;;
 heavy)
   echo "generate-ecrawl-adversarial-tree: SYNTH_PROFILE=heavy — large tree; generation and ecrawl --no-write can take a long time" >&2
   FLAT_FILES=${FLAT_FILES:-12000000}
   DEPTH_CHAIN=${DEPTH_CHAIN:-400}
   WIDE_PARENTS=${WIDE_PARENTS:-128}
   WIDE_FILES_EACH=${WIDE_FILES_EACH:-20000}
   DEPTH_SLICE_ENABLE=${DEPTH_SLICE_ENABLE:-1}
   DEPTH_PEAK_FILES_PER_BIN=${DEPTH_PEAK_FILES_PER_BIN:-80000}
   ;;
 extreme)
   echo "generate-ecrawl-adversarial-tree: SYNTH_PROFILE=extreme — heavy-class tree plus mega_dir1/mega_dir2; very large inode footprint" >&2
   FLAT_FILES=${FLAT_FILES:-12000000}
   DEPTH_CHAIN=${DEPTH_CHAIN:-400}
   WIDE_PARENTS=${WIDE_PARENTS:-128}
   WIDE_FILES_EACH=${WIDE_FILES_EACH:-20000}
   DEPTH_SLICE_ENABLE=${DEPTH_SLICE_ENABLE:-1}
   DEPTH_PEAK_FILES_PER_BIN=${DEPTH_PEAK_FILES_PER_BIN:-80000}
   SYNTH_EXTREME_MEGA_DIR1_FILES=${SYNTH_EXTREME_MEGA_DIR1_FILES:-20000000}
   SYNTH_EXTREME_MEGA_DIR2_TOP_FILES=${SYNTH_EXTREME_MEGA_DIR2_TOP_FILES:-2000000}
   SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS=${SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS:-1000000}
   ;;
 "")
   ;;
 *)
   echo "ERROR: unknown SYNTH_PROFILE='$SYNTH_PROFILE' (use medium, heavy, extreme, or unset)" >&2
   exit 2
   ;;
esac

if [[ "$SYNTH_PROFILE" != "extreme" ]]; then
  SYNTH_EXTREME_MEGA_DIR1_FILES=0
  SYNTH_EXTREME_MEGA_DIR2_TOP_FILES=0
  SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS=0
fi

FLAT_FILES=${FLAT_FILES:-100000}
DEPTH_CHAIN=${DEPTH_CHAIN:-700}
BATCH_CREATE=${BATCH_CREATE:-1}
CREATE_JOBS=${CREATE_JOBS:-0}
IGNORE_DEPTH_PATH_MAX=${IGNORE_DEPTH_PATH_MAX:-0}

DISK_BUDGET_BYTES=${DISK_BUDGET_BYTES:-$((100 * 1024 * 1024 * 1024))}
ASSUMED_BYTES_PER_FLAT_FILE=${ASSUMED_BYTES_PER_FLAT_FILE:-4096}
ASSUMED_BYTES_PER_CHAIN_DIR=${ASSUMED_BYTES_PER_CHAIN_DIR:-4096}
AUTO_CAP_FLAT=${AUTO_CAP_FLAT:-1}
SPARSE_FILE_MIB=${SPARSE_FILE_MIB:-0}

WIDE_PARENTS=${WIDE_PARENTS:-0}
WIDE_FILES_EACH=${WIDE_FILES_EACH:-1000}

DEPTH_SLICE_ENABLE=${DEPTH_SLICE_ENABLE:-0}
DEPTH_PEAK_LO=${DEPTH_PEAK_LO:-12}
DEPTH_PEAK_HI=${DEPTH_PEAK_HI:-16}
DEPTH_PEAK_FILES_PER_BIN=${DEPTH_PEAK_FILES_PER_BIN:-50000}
DEPTH_PLATEAU_LO=${DEPTH_PLATEAU_LO:-41}
DEPTH_PLATEAU_HI=${DEPTH_PLATEAU_HI:-47}
DEPTH_PLATEAU_FILES_PER_BIN=${DEPTH_PLATEAU_FILES_PER_BIN:-1648}

ECRAWL_FLAT_LOW=${ECRAWL_FLAT_LOW:-1000}
ECRAWL_FLAT_HIGH=${ECRAWL_FLAT_HIGH:-30000}

EREPORT_BADGE_FIXTURES=${EREPORT_BADGE_FIXTURES:-1}
BADGE_DENSE_N=${BADGE_DENSE_N:-8500}
BADGE_DEEP_N=${BADGE_DEEP_N:-5200}
BADGE_FILE_BYTES=${BADGE_FILE_BYTES:-512}
BADGE_DEEP_SEGMENTS=${BADGE_DEEP_SEGMENTS:-20}
BADGE_SKEW_STAMP_DAYS=${BADGE_SKEW_STAMP_DAYS:-120}

BADGE_DEEP_ONLY_N=${BADGE_DEEP_ONLY_N:-4000}
BADGE_DEEP_ONLY_BAND=${BADGE_DEEP_ONLY_BAND:-4}

BADGE_HEATMAP_GRID=${BADGE_HEATMAP_GRID:-1}
BADGE_HEATMAP_RANDOM=${BADGE_HEATMAP_RANDOM:-1}
BADGE_GRID_FILES_MIN=${BADGE_GRID_FILES_MIN:-400}
BADGE_GRID_FILES_MAX=${BADGE_GRID_FILES_MAX:-78000}
BADGE_GRID_FILES_PER_CELL=${BADGE_GRID_FILES_PER_CELL:-35}
BADGE_HEATMAP_OLDEST_DAYS=${BADGE_HEATMAP_OLDEST_DAYS:-2000}
BADGE_MULTI_DENSE_N=${BADGE_MULTI_DENSE_N:-8192}
BADGE_MULTI_DEEP_N=${BADGE_MULTI_DEEP_N:-5500}

# Heat-map / megadir layout (ereport Dense = parent has >=8192 immediate children):
BADGE_GRID_MAX_FILES_PER_DIR=${BADGE_GRID_MAX_FILES_PER_DIR:-4096}
BADGE_MEGADIR_SHARD_CAP=${BADGE_MEGADIR_SHARD_CAP:-4096}
BADGE_GRID_S0_FRAC=${BADGE_GRID_S0_FRAC:-0.18}
BADGE_HEATMAP_DEEP_PREFIX_LEVELS=${BADGE_HEATMAP_DEEP_PREFIX_LEVELS:-8}
BADGE_EXTRA_SKEW_CELL=${BADGE_EXTRA_SKEW_CELL:-1}
BADGE_EXTRA_SKEW_STAMP_DAYS=${BADGE_EXTRA_SKEW_STAMP_DAYS:-265}

BADGE_DENSE_FLAT_ENABLE=${BADGE_DENSE_FLAT_ENABLE:-1}
BADGE_DENSE_FLAT_N=${BADGE_DENSE_FLAT_N:-8500}
BADGE_DENSE_FLAT_BAND=${BADGE_DENSE_FLAT_BAND:-5}
BADGE_DENSE_FLAT_SB=${BADGE_DENSE_FLAT_SB:-1}

BADGE_DENSE_FLAT_EXTRA_ENABLE=${BADGE_DENSE_FLAT_EXTRA_ENABLE:-1}
BADGE_DENSE_FLAT_EXTRA_N=${BADGE_DENSE_FLAT_EXTRA_N:-8500}
BADGE_DENSE_FLAT_EXTRA_PAIRS=${BADGE_DENSE_FLAT_EXTRA_PAIRS:-"0:5,1:3,4:2,5:4"}
BADGE_HEATMAP_SKIP_DENSE_FLAT_EXTRA_CELLS=${BADGE_HEATMAP_SKIP_DENSE_FLAT_EXTRA_CELLS:-1}

# Heat-map inner cells: showcase vs neutral (see header BADGE_HEATMAP_BADGE_CELL_FRAC)
BADGE_HEATMAP_BADGE_CELL_FRAC=${BADGE_HEATMAP_BADGE_CELL_FRAC:-0.30}
BADGE_HEATMAP_SHOWCASE_SEED=${BADGE_HEATMAP_SHOWCASE_SEED:-}
BADGE_HEATMAP_NEUTRAL_MAX_FILES=${BADGE_HEATMAP_NEUTRAL_MAX_FILES:-19}
BADGE_HEATMAP_NEUTRAL_MIN_FILES=${BADGE_HEATMAP_NEUTRAL_MIN_FILES:-3}
BADGE_HEATMAP_CORPUS_BLEND_NUM=${BADGE_HEATMAP_CORPUS_BLEND_NUM:-85}
BADGE_HEATMAP_CORPUS_BLEND_DEN=${BADGE_HEATMAP_CORPUS_BLEND_DEN:-100}

# Heat-map margin dilution / flat sharding (see header)
BADGE_MARGIN_DILUTION_ENABLE=${BADGE_MARGIN_DILUTION_ENABLE:-1}
BADGE_MARGIN_NEUTRAL_FILES=${BADGE_MARGIN_NEUTRAL_FILES:-520000}
BADGE_MARGIN_NEUTRAL_PARENT_CAP=${BADGE_MARGIN_NEUTRAL_PARENT_CAP:-4096}
BADGE_MARGIN_NEUTRAL_DEPTH=${BADGE_MARGIN_NEUTRAL_DEPTH:-2}
BADGE_MARGIN_NEUTRAL_BYTES=${BADGE_MARGIN_NEUTRAL_BYTES:-512}
BADGE_MARGIN_NEUTRAL_UTIME=${BADGE_MARGIN_NEUTRAL_UTIME:-1}
SYNTH_FLAT_SHARD_CAP=${SYNTH_FLAT_SHARD_CAP:-4096}

SYNTH_RANDOM_UID_ENABLE=${SYNTH_RANDOM_UID_ENABLE:-1}
SYNTH_RANDOM_UID_SCOPE=${SYNTH_RANDOM_UID_SCOPE:-badge}
SYNTH_RANDOM_UID_FRACTION=${SYNTH_RANDOM_UID_FRACTION:-0.12}
SYNTH_RANDOM_UID_MAX_CHOWN=${SYNTH_RANDOM_UID_MAX_CHOWN:-40000}
SYNTH_RANDOM_UID_MIN=${SYNTH_RANDOM_UID_MIN:-100000}
SYNTH_RANDOM_UID_MAX=${SYNTH_RANDOM_UID_MAX:-199999}
SYNTH_RANDOM_UID_UNIQUE_MAX=${SYNTH_RANDOM_UID_UNIQUE_MAX:-5000}

mkdir -p "$ROOT"

flat_dir="$ROOT/single_huge_dir"
chain_root="$ROOT/deep_skinny_chain"
wide_root="$ROOT/wide_shallow"
depth_slice_root="$ROOT/depth_slash_profile"
badge_fixtures_parent="$ROOT/ereport_badge_fixtures"

if [[ "$EREPORT_BADGE_FIXTURES" == "1" ]]; then
  if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: EREPORT_BADGE_FIXTURES=1 requires python3." >&2
    exit 1
  fi
  if [[ "$BADGE_DENSE_N" -lt 8192 ]]; then
    echo "ERROR: BADGE_DENSE_N=$BADGE_DENSE_N must be >= 8192 (PATH_SHAPE_DENSE_MIN_CHILDREN in ereport)." >&2
    exit 1
  fi
  if [[ "$BADGE_FILE_BYTES" -lt 1 || "$BADGE_FILE_BYTES" -ge 4096 ]]; then
    echo "ERROR: BADGE_FILE_BYTES=$BADGE_FILE_BYTES must be >= 1 and < 4096 (same cell size bucket + non-zero Deep bytes)." >&2
    exit 1
  fi
  if [[ "$BADGE_MULTI_DENSE_N" -lt 8192 ]]; then
    echo "ERROR: BADGE_MULTI_DENSE_N=$BADGE_MULTI_DENSE_N must be >= 8192 when EREPORT_BADGE_FIXTURES=1." >&2
    exit 1
  fi
  if [[ "${BADGE_DENSE_FLAT_ENABLE:-1}" == "1" ]]; then
    if [[ "$BADGE_DENSE_FLAT_N" -lt 8192 ]]; then
      echo "ERROR: BADGE_DENSE_FLAT_N=$BADGE_DENSE_FLAT_N must be >= 8192." >&2
      exit 1
    fi
    if [[ "$BADGE_DENSE_FLAT_BAND" -lt 0 || "$BADGE_DENSE_FLAT_BAND" -gt 5 ]]; then
      echo "ERROR: BADGE_DENSE_FLAT_BAND=$BADGE_DENSE_FLAT_BAND must be 0..5." >&2
      exit 1
    fi
    if [[ "$BADGE_DENSE_FLAT_SB" -lt 0 || "$BADGE_DENSE_FLAT_SB" -gt 5 ]]; then
      echo "ERROR: BADGE_DENSE_FLAT_SB=$BADGE_DENSE_FLAT_SB must be 0..5." >&2
      exit 1
    fi
  fi
  if [[ "${BADGE_DENSE_FLAT_EXTRA_ENABLE:-1}" == "1" && -n "${BADGE_DENSE_FLAT_EXTRA_PAIRS:-}" ]]; then
    if [[ "$BADGE_DENSE_FLAT_EXTRA_N" -lt 8192 ]]; then
      echo "ERROR: BADGE_DENSE_FLAT_EXTRA_N=$BADGE_DENSE_FLAT_EXTRA_N must be >= 8192." >&2
      exit 1
    fi
    IFS=',' read -ra _ex_pairs <<<"$BADGE_DENSE_FLAT_EXTRA_PAIRS"
    _ex_seen=":"
    for _p in "${_ex_pairs[@]}"; do
      _p="${_p// /}"
      [[ -z "$_p" ]] && continue
      if [[ "$_p" != *:* ]]; then
        echo "ERROR: BADGE_DENSE_FLAT_EXTRA_PAIRS entry '$_p' must be 'BAND:SB' (e.g. 0:5,1:3)." >&2
        exit 1
      fi
      _b="${_p%%:*}"
      _s="${_p##*:}"
      if ! [[ "$_b" =~ ^[0-9]+$ && "$_s" =~ ^[0-9]+$ ]]; then
        echo "ERROR: BADGE_DENSE_FLAT_EXTRA_PAIRS entry '$_p' must be integers." >&2
        exit 1
      fi
      if [[ "$_b" -lt 0 || "$_b" -gt 5 || "$_s" -lt 0 || "$_s" -gt 5 ]]; then
        echo "ERROR: BADGE_DENSE_FLAT_EXTRA_PAIRS entry '$_p' must be in 0..5." >&2
        exit 1
      fi
      if [[ "$_ex_seen" == *":${_b}:${_s}:"* ]]; then
        echo "ERROR: BADGE_DENSE_FLAT_EXTRA_PAIRS has duplicate cell '$_p'." >&2
        exit 1
      fi
      _ex_seen="${_ex_seen}${_b}:${_s}:"
      if [[ "${BADGE_DENSE_FLAT_ENABLE:-1}" == "1" \
            && "$_b" -eq "$BADGE_DENSE_FLAT_BAND" \
            && "$_s" -eq "$BADGE_DENSE_FLAT_SB" ]]; then
        echo "ERROR: BADGE_DENSE_FLAT_EXTRA_PAIRS entry '$_p' overlaps primary dense_flat_cell ab${BADGE_DENSE_FLAT_BAND}/sb${BADGE_DENSE_FLAT_SB}." >&2
        exit 1
      fi
    done
    unset _ex_pairs _ex_seen _p _b _s
  fi
  if [[ "$BADGE_DEEP_ONLY_N" -gt 0 ]]; then
    if [[ "$BADGE_DEEP_ONLY_BAND" -lt 0 || "$BADGE_DEEP_ONLY_BAND" -gt 5 ]]; then
      echo "ERROR: BADGE_DEEP_ONLY_BAND=$BADGE_DEEP_ONLY_BAND must be 0..5." >&2
      exit 1
    fi
    if [[ "$BADGE_DEEP_ONLY_N" -lt 100 ]]; then
      echo "ERROR: BADGE_DEEP_ONLY_N=$BADGE_DEEP_ONLY_N too small; use >= 100 for reliable Deep badge (bytes share + file count)." >&2
      exit 1
    fi
  fi
  if [[ "$BADGE_HEATMAP_GRID" == "1" ]]; then
    if ! BADGE_HEATMAP_BADGE_CELL_FRAC="$BADGE_HEATMAP_BADGE_CELL_FRAC" python3 -c 'import os,sys
try:
    x = float(os.environ["BADGE_HEATMAP_BADGE_CELL_FRAC"])
except Exception:
    sys.exit(1)
sys.exit(0 if 0.0 <= x <= 1.0 else 1)
'; then
      echo "ERROR: BADGE_HEATMAP_BADGE_CELL_FRAC must be a number in [0,1] (got $BADGE_HEATMAP_BADGE_CELL_FRAC)" >&2
      exit 1
    fi
    if [[ "$BADGE_HEATMAP_NEUTRAL_MAX_FILES" -lt 1 || "$BADGE_HEATMAP_NEUTRAL_MAX_FILES" -ge 20 ]]; then
      echo "ERROR: BADGE_HEATMAP_NEUTRAL_MAX_FILES=$BADGE_HEATMAP_NEUTRAL_MAX_FILES must be 1..19 (ereport PATH_SHAPE_MIN_BUCKET_FILES=20)." >&2
      exit 1
    fi
    if [[ "$BADGE_HEATMAP_NEUTRAL_MIN_FILES" -lt 1 || "$BADGE_HEATMAP_NEUTRAL_MIN_FILES" -gt "$BADGE_HEATMAP_NEUTRAL_MAX_FILES" ]]; then
      echo "ERROR: BADGE_HEATMAP_NEUTRAL_MIN_FILES=$BADGE_HEATMAP_NEUTRAL_MIN_FILES must be 1..$BADGE_HEATMAP_NEUTRAL_MAX_FILES." >&2
      exit 1
    fi
    if [[ "$BADGE_HEATMAP_CORPUS_BLEND_DEN" -lt 1 || "$BADGE_HEATMAP_CORPUS_BLEND_NUM" -lt 1 ]]; then
      echo "ERROR: BADGE_HEATMAP_CORPUS_BLEND_NUM/DEN must be >= 1." >&2
      exit 1
    fi
    if [[ "$BADGE_GRID_MAX_FILES_PER_DIR" -lt 1 || "$BADGE_GRID_MAX_FILES_PER_DIR" -gt 8191 ]]; then
      echo "ERROR: BADGE_GRID_MAX_FILES_PER_DIR=$BADGE_GRID_MAX_FILES_PER_DIR must be 1..8191 (<8192 Dense threshold)." >&2
      exit 1
    fi
    if [[ "$BADGE_HEATMAP_DEEP_PREFIX_LEVELS" -lt 0 || "$BADGE_HEATMAP_DEEP_PREFIX_LEVELS" -gt 64 ]]; then
      echo "ERROR: BADGE_HEATMAP_DEEP_PREFIX_LEVELS=$BADGE_HEATMAP_DEEP_PREFIX_LEVELS must be 0..64." >&2
      exit 1
    fi
    if [[ "$BADGE_HEATMAP_RANDOM" == "1" ]]; then
      if [[ "$BADGE_GRID_FILES_MIN" -lt 1 ]]; then
        echo "ERROR: BADGE_GRID_FILES_MIN=$BADGE_GRID_FILES_MIN must be >= 1." >&2
        exit 1
      fi
      if [[ "$BADGE_GRID_FILES_MAX" -lt "$BADGE_GRID_FILES_MIN" ]]; then
        echo "ERROR: BADGE_GRID_FILES_MAX must be >= BADGE_GRID_FILES_MIN." >&2
        exit 1
      fi
      if [[ "$BADGE_GRID_FILES_MAX" -gt 999999 ]]; then
        echo "ERROR: BADGE_GRID_FILES_MAX=$BADGE_GRID_FILES_MAX too large (max 999999)." >&2
        exit 1
      fi
    fi
  fi
  if [[ "$BADGE_MEGADIR_SHARD_CAP" -lt 1 || "$BADGE_MEGADIR_SHARD_CAP" -gt 8191 ]]; then
    echo "ERROR: BADGE_MEGADIR_SHARD_CAP=$BADGE_MEGADIR_SHARD_CAP must be 1..8191." >&2
    exit 1
  fi
  if [[ "$BADGE_HEATMAP_OLDEST_DAYS" -lt 1095 ]]; then
    echo "ERROR: BADGE_HEATMAP_OLDEST_DAYS=$BADGE_HEATMAP_OLDEST_DAYS must be >= 1095 (ereport ‘3+ years’ is days >= 3×365)." >&2
    exit 1
  fi
  if [[ "${BADGE_DENSE_FLAT_ENABLE:-1}" == "1" && "$BADGE_DEEP_ONLY_N" -gt 0 ]]; then
    if [[ "$BADGE_DENSE_FLAT_BAND" -eq "$BADGE_DEEP_ONLY_BAND" && "$BADGE_DENSE_FLAT_SB" -eq 0 ]]; then
      echo "WARNING: dense_flat_cell shares ab${BADGE_DENSE_FLAT_BAND}/sb00 with deep_only_cell row; expect Skew/Dense rather than a clean Deep-only drill." >&2
    fi
  fi
  # Deep badge needs deep_bytes / bucket_bytes >= 0.30 with equal per-file sizes → BADGE_DEEP_N / (BADGE_DEEP_N + BADGE_DENSE_N) >= 0.3
  _badge_need_deep=$(((3 * BADGE_DENSE_N + 6) / 7))
  if [[ "$BADGE_DEEP_N" -lt "$_badge_need_deep" ]]; then
    echo "WARNING: BADGE_DEEP_N=$BADGE_DEEP_N may be too small for Deep (>=30% bytes); suggest at least $_badge_need_deep with BADGE_DENSE_N=$BADGE_DENSE_N." >&2
  fi
fi

if [[ "$SYNTH_FLAT_SHARD_CAP" != "0" ]]; then
  if [[ "$SYNTH_FLAT_SHARD_CAP" -lt 1 || "$SYNTH_FLAT_SHARD_CAP" -gt 8191 ]]; then
    echo "ERROR: SYNTH_FLAT_SHARD_CAP=$SYNTH_FLAT_SHARD_CAP must be 0 or 1..8191 (<8192 ereport Dense threshold)." >&2
    exit 1
  fi
  if [[ "$BATCH_CREATE" != "1" ]] || ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: SYNTH_FLAT_SHARD_CAP requires BATCH_CREATE=1 and python3." >&2
    exit 1
  fi
fi

if [[ "$SYNTH_PROFILE" == "extreme" ]]; then
  if [[ "$BATCH_CREATE" != "1" ]] || ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: SYNTH_PROFILE=extreme requires BATCH_CREATE=1 and python3 (mega_dir1/mega_dir2 use bulk creation)." >&2
    exit 1
  fi
  if [[ "$SYNTH_EXTREME_MEGA_DIR1_FILES" -lt 0 || "$SYNTH_EXTREME_MEGA_DIR2_TOP_FILES" -lt 0 || "$SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS" -lt 0 ]]; then
    echo "ERROR: SYNTH_EXTREME_MEGA_* counts must be >= 0." >&2
    exit 1
  fi
fi

if [[ "${BADGE_MARGIN_DILUTION_ENABLE:-0}" == "1" ]]; then
  if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: BADGE_MARGIN_DILUTION_ENABLE=1 requires python3." >&2
    exit 1
  fi
  if [[ "$BADGE_MARGIN_NEUTRAL_FILES" -lt 0 ]]; then
    echo "ERROR: BADGE_MARGIN_NEUTRAL_FILES=$BADGE_MARGIN_NEUTRAL_FILES must be >= 0." >&2
    exit 1
  fi
  if [[ "$BADGE_MARGIN_NEUTRAL_PARENT_CAP" -lt 1 || "$BADGE_MARGIN_NEUTRAL_PARENT_CAP" -gt 8191 ]]; then
    echo "ERROR: BADGE_MARGIN_NEUTRAL_PARENT_CAP=$BADGE_MARGIN_NEUTRAL_PARENT_CAP must be 1..8191." >&2
    exit 1
  fi
  if [[ "$BADGE_MARGIN_NEUTRAL_DEPTH" -lt 0 || "$BADGE_MARGIN_NEUTRAL_DEPTH" -gt 32 ]]; then
    echo "ERROR: BADGE_MARGIN_NEUTRAL_DEPTH=$BADGE_MARGIN_NEUTRAL_DEPTH must be 0..32." >&2
    exit 1
  fi
  if [[ "$BADGE_MARGIN_NEUTRAL_BYTES" -lt 1 || "$BADGE_MARGIN_NEUTRAL_BYTES" -ge 4096 ]]; then
    echo "ERROR: BADGE_MARGIN_NEUTRAL_BYTES=$BADGE_MARGIN_NEUTRAL_BYTES must be >= 1 and < 4096." >&2
    exit 1
  fi
fi

# Max levels for deep_skinny_chain/0/1/2/… so path to leaf stays under PATH_MAX (numeric names grow: /999 …).
depth_chain_cap_levels() {
  local root="$1"
  local want="$2"
  local max_pm seg step acc safety d chain_root

  max_pm=$(getconf PATH_MAX "$root" 2>/dev/null || echo 4096)
  [[ "$max_pm" =~ ^[0-9]+$ ]] || max_pm=4096
  chain_root="${root%/}/deep_skinny_chain"
  acc=${#chain_root}
  # Reserve slash + leaf file name ('/x') plus margin for filesystem quirks.
  tail_room=96
  d=0
  while [[ "$d" -lt "$want" ]]; do
    printf -v seg '%d' "$d"
    step=$((1 + ${#seg}))
    if [[ $((acc + step + tail_room + 2)) -gt "$max_pm" ]]; then
      echo "$d"
      return
    fi
    acc=$((acc + step))
    d=$((d + 1))
  done
  echo "$d"
}

if [[ "$DEPTH_CHAIN" -gt 0 && "$IGNORE_DEPTH_PATH_MAX" != "1" ]]; then
  _dc_cap=$(depth_chain_cap_levels "$ROOT" "$DEPTH_CHAIN")
  if [[ "$_dc_cap" -eq 0 ]]; then
    echo "ERROR: deep_skinny_chain cannot fit under PATH_MAX with this ROOT (path already too long)." >&2
    exit 1
  fi
  if [[ "$DEPTH_CHAIN" -gt "$_dc_cap" ]]; then
    echo "WARNING: DEPTH_CHAIN=$DEPTH_CHAIN exceeds PATH_MAX-safe depth ${_dc_cap} for this root (numeric segments like …/2540/… lengthen paths); clamping. Use IGNORE_DEPTH_PATH_MAX=1 to skip (mkdir usually fails)." >&2
    DEPTH_CHAIN=$_dc_cap
  fi
fi

if [[ "$DEPTH_SLICE_ENABLE" == "1" ]]; then
  if [[ "$DEPTH_PEAK_LO" -gt "$DEPTH_PEAK_HI" ]] || [[ "$DEPTH_PLATEAU_LO" -gt "$DEPTH_PLATEAU_HI" ]]; then
    echo "ERROR: DEPTH_PEAK_* or DEPTH_PLATEAU_* range inverted." >&2
    exit 1
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: DEPTH_SLICE_ENABLE=1 requires python3." >&2
    exit 1
  fi
fi

depth_slice_file_count() {
  local peak_bins plateau_bins
  if [[ "$DEPTH_SLICE_ENABLE" != "1" ]]; then
    echo 0
    return
  fi
  peak_bins=$((DEPTH_PEAK_HI - DEPTH_PEAK_LO + 1))
  plateau_bins=$((DEPTH_PLATEAU_HI - DEPTH_PLATEAU_LO + 1))
  echo $((peak_bins * DEPTH_PEAK_FILES_PER_BIN + plateau_bins * DEPTH_PLATEAU_FILES_PER_BIN))
}

# Unique directories created (shared dslice/ counted once).
depth_slice_dir_estimate() {
  local sum=1 b k
  if [[ "$DEPTH_SLICE_ENABLE" != "1" ]]; then
    echo 0
    return
  fi
  for ((b = DEPTH_PEAK_LO; b <= DEPTH_PEAK_HI; b++)); do
    if [[ "$b" -lt 3 ]]; then continue; fi
    k=$((b - 3))
    sum=$((sum + 1 + k))
  done
  for ((b = DEPTH_PLATEAU_LO; b <= DEPTH_PLATEAU_HI; b++)); do
    if [[ "$b" -lt 3 ]]; then continue; fi
    k=$((b - 3))
    sum=$((sum + 1 + k))
  done
  echo "$sum"
}

depth_slice_assumed_bytes() {
  local slice_f slice_d sparse_slice
  slice_f=$(depth_slice_file_count)
  slice_d=$(depth_slice_dir_estimate)
  sparse_slice=0
  if [[ "$SPARSE_FILE_MIB" -gt 0 && "$slice_f" -gt 0 ]]; then
    sparse_slice=$((slice_f * SPARSE_FILE_MIB * 1024 * 1024))
  fi
  echo $((slice_f * ASSUMED_BYTES_PER_FLAT_FILE + slice_d * ASSUMED_BYTES_PER_CHAIN_DIR + sparse_slice))
}

dense_flat_extra_pair_count() {
  local _arr _p n=0
  if [[ "${BADGE_DENSE_FLAT_EXTRA_ENABLE:-1}" != "1" ]] || [[ -z "${BADGE_DENSE_FLAT_EXTRA_PAIRS:-}" ]]; then
    echo 0
    return
  fi
  IFS=',' read -ra _arr <<<"$BADGE_DENSE_FLAT_EXTRA_PAIRS"
  for _p in "${_arr[@]}"; do
    _p="${_p// /}"
    [[ -n "$_p" ]] && n=$((n + 1))
  done
  echo "$n"
}

badge_fixtures_file_count() {
  local grid_fc multi_fc dense_flat_extra_pairs dense_flat_extra_fc
  if [[ "$EREPORT_BADGE_FIXTURES" != "1" ]]; then
    echo 0
    return
  fi
  grid_fc=0
  if [[ "$BADGE_HEATMAP_GRID" == "1" ]]; then
    if [[ "$BADGE_HEATMAP_RANDOM" == "1" ]]; then
      grid_fc=$((36 * BADGE_GRID_FILES_MAX))
    else
      grid_fc=$((36 * BADGE_GRID_FILES_PER_CELL))
    fi
  fi
  multi_fc=$((BADGE_MULTI_DENSE_N + BADGE_MULTI_DEEP_N + BADGE_DEEP_ONLY_N))
  skew_fc=$((BADGE_DENSE_N + BADGE_DEEP_N))
  if [[ "${BADGE_EXTRA_SKEW_CELL:-1}" == "1" ]]; then
    skew_fc=$((skew_fc + BADGE_DENSE_N + BADGE_DEEP_N))
  fi
  dense_flat_fc=0
  if [[ "${BADGE_DENSE_FLAT_ENABLE:-1}" == "1" ]]; then
    dense_flat_fc=$((BADGE_DENSE_FLAT_N))
  fi
  dense_flat_extra_pairs=$(dense_flat_extra_pair_count)
  dense_flat_extra_fc=$((dense_flat_extra_pairs * BADGE_DENSE_FLAT_EXTRA_N))
  echo $((skew_fc + grid_fc + multi_fc + dense_flat_fc + dense_flat_extra_fc))
}

# Conservative metadata + payload estimate for ereport_badge_fixtures (see header).
badge_fixtures_assumed_bytes() {
  local fc dc grid_fc grid_bytes multi_fc seg_multi dense_flat_extra_pairs dense_flat_extra_fc dense_flat_extra_dirs
  if [[ "$EREPORT_BADGE_FIXTURES" != "1" ]]; then
    echo 0
    return
  fi
  skew_trees=1
  [[ "${BADGE_EXTRA_SKEW_CELL:-1}" == "1" ]] && skew_trees=2
  fc=$((skew_trees * (BADGE_DENSE_N + BADGE_DEEP_N)))
  dc=$((skew_trees * (4 + BADGE_DEEP_SEGMENTS)))
  grid_fc=0
  grid_bytes=0
  if [[ "$BADGE_HEATMAP_GRID" == "1" ]]; then
    if [[ "$BADGE_HEATMAP_RANDOM" == "1" ]]; then
      grid_fc=$((36 * BADGE_GRID_FILES_MAX))
    else
      grid_fc=$((36 * BADGE_GRID_FILES_PER_CELL))
    fi
    grid_bytes=$((grid_fc * ASSUMED_BYTES_PER_FLAT_FILE))
  fi
  multi_fc=$((BADGE_MULTI_DENSE_N + BADGE_MULTI_DEEP_N + BADGE_DEEP_ONLY_N))
  seg_multi=$((4 + BADGE_DEEP_SEGMENTS))
  if [[ "$BADGE_DEEP_ONLY_N" -gt 0 ]]; then
    seg_multi=$((seg_multi + 2 + BADGE_DEEP_SEGMENTS))
  fi
  dense_flat_fc=0
  if [[ "${BADGE_DENSE_FLAT_ENABLE:-1}" == "1" ]]; then
    dense_flat_fc=$((BADGE_DENSE_FLAT_N))
  fi
  dense_flat_dirs=4
  dense_flat_extra_pairs=$(dense_flat_extra_pair_count)
  dense_flat_extra_fc=$((dense_flat_extra_pairs * BADGE_DENSE_FLAT_EXTRA_N))
  # 4 dirs (root/abXX/sbYY/flat_megadir) per extra cell — sparse files use BADGE_FILE_BYTES allocated.
  dense_flat_extra_dirs=$((dense_flat_extra_pairs * 4))
  echo $(((fc + multi_fc + dense_flat_fc + dense_flat_extra_fc) * BADGE_FILE_BYTES +
    (dc + seg_multi + dense_flat_dirs + dense_flat_extra_dirs) * ASSUMED_BYTES_PER_CHAIN_DIR + grid_bytes))
}

# Shallow neutral_flat/ tree under $ROOT (not under ereport_badge_fixtures/) — dilutes heat-map margin totals vs badge fixtures (default BADGE_MARGIN_DILUTION_ENABLE=1).
margin_neutral_file_count() {
  if [[ "${BADGE_MARGIN_DILUTION_ENABLE:-0}" != "1" ]]; then
    echo 0
    return
  fi
  local n=${BADGE_MARGIN_NEUTRAL_FILES:-0}
  if [[ "$n" -lt 0 ]]; then
    echo 0
    return
  fi
  echo "$n"
}

margin_neutral_assumed_bytes() {
  local fc
  fc=$(margin_neutral_file_count)
  echo $((fc * BADGE_MARGIN_NEUTRAL_BYTES))
}

# Regular files only (SYNTH_PROFILE=extreme mega_dir1 + mega_dir2).
extreme_mega_file_count() {
  if [[ "$SYNTH_PROFILE" != "extreme" ]]; then
    echo 0
    return
  fi
  echo $((SYNTH_EXTREME_MEGA_DIR1_FILES + SYNTH_EXTREME_MEGA_DIR2_TOP_FILES + SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS))
}

# Conservative footprint for extreme mega dirs (metadata model matches ASSUMED_BYTES_*).
extreme_mega_assumed_bytes() {
  local files dirs sparse_extra
  if [[ "$SYNTH_PROFILE" != "extreme" ]]; then
    echo 0
    return
  fi
  files=$((SYNTH_EXTREME_MEGA_DIR1_FILES + SYNTH_EXTREME_MEGA_DIR2_TOP_FILES + SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS))
  dirs=$((2 + SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS))
  sparse_extra=0
  if [[ "$SPARSE_FILE_MIB" -gt 0 && "$files" -gt 0 ]]; then
    sparse_extra=$((files * SPARSE_FILE_MIB * 1024 * 1024))
  fi
  echo $((files * ASSUMED_BYTES_PER_FLAT_FILE + dirs * ASSUMED_BYTES_PER_CHAIN_DIR + sparse_extra))
}

estimate_bytes() {
  local flat_bytes chain_bytes wide_bytes sparse_extra sparse_flat slice_bytes badge_bytes margin_bytes extreme_bytes

  flat_bytes=$((FLAT_FILES * ASSUMED_BYTES_PER_FLAT_FILE))
  chain_bytes=$((DEPTH_CHAIN * ASSUMED_BYTES_PER_CHAIN_DIR))
  wide_bytes=0
  if [[ "$WIDE_PARENTS" -gt 0 ]]; then
    wide_bytes=$((WIDE_PARENTS * (ASSUMED_BYTES_PER_CHAIN_DIR + WIDE_FILES_EACH * ASSUMED_BYTES_PER_FLAT_FILE)))
  fi

  sparse_extra=0
  if [[ "$SPARSE_FILE_MIB" -gt 0 ]]; then
    sparse_flat=$((FLAT_FILES * SPARSE_FILE_MIB * 1024 * 1024))
    sparse_extra=$((sparse_flat))
    if [[ "$WIDE_PARENTS" -gt 0 ]]; then
      sparse_extra=$((sparse_extra + WIDE_PARENTS * WIDE_FILES_EACH * SPARSE_FILE_MIB * 1024 * 1024))
    fi
  fi

  slice_bytes=$(depth_slice_assumed_bytes)
  badge_bytes=$(badge_fixtures_assumed_bytes)
  margin_bytes=$(margin_neutral_assumed_bytes)
  extreme_bytes=$(extreme_mega_assumed_bytes)

  echo $((flat_bytes + chain_bytes + wide_bytes + sparse_extra + slice_bytes + badge_bytes + margin_bytes + extreme_bytes))
}

# Bytes used by wide tree only (for AUTO_CAP headroom).
wide_tree_assumed_bytes() {
  local wide_bytes sparse_extra
  wide_bytes=0
  if [[ "$WIDE_PARENTS" -gt 0 ]]; then
    wide_bytes=$((WIDE_PARENTS * (ASSUMED_BYTES_PER_CHAIN_DIR + WIDE_FILES_EACH * ASSUMED_BYTES_PER_FLAT_FILE)))
  fi
  sparse_extra=0
  if [[ "$SPARSE_FILE_MIB" -gt 0 && "$WIDE_PARENTS" -gt 0 ]]; then
    sparse_extra=$((WIDE_PARENTS * WIDE_FILES_EACH * SPARSE_FILE_MIB * 1024 * 1024))
  fi
  echo $((wide_bytes + sparse_extra))
}

human_bytes() {
  local b=$1
  if command -v numfmt >/dev/null 2>&1; then
    numfmt --to=iec-i --suffix=B "$b" 2>/dev/null || echo "${b} B"
  else
    if ((b >= 1073741824)); then printf '~%d GiB\n' $((b / 1073741824))
    elif ((b >= 1048576)); then printf '~%d MiB\n' $((b / 1048576))
    else printf '%s B\n' "$b"
    fi
  fi
}

human_duration() {
  local sec=$1
  local h m
  if ((sec < 0)); then echo "?"; return; fi
  if ((sec < 1)); then echo "<1s"; return; fi
  if ((sec < 3600)); then m=$((sec / 60)); if ((sec < 120)); then echo "~${sec}s"; else echo "~${m} min"; fi; return; fi
  if ((sec < 86400)); then h=$((sec / 3600)); m=$(((sec % 3600) / 60)); echo "~${h}h ${m}m"; return; fi
  h=$((sec / 3600)); echo "~$((h / 24))d $((h % 24))h"
}

rough_ecrawl_flat_seconds() {
  local rate=$1
  if [[ "$rate" -lt 1 ]]; then rate=1; fi
  echo $((FLAT_FILES / rate))
}

est=$(estimate_bytes)
if [[ "$est" -gt "$DISK_BUDGET_BYTES" ]]; then
  if [[ "$AUTO_CAP_FLAT" == "1" ]]; then
    chain_bytes=$((DEPTH_CHAIN * ASSUMED_BYTES_PER_CHAIN_DIR))
    wide_bytes_nonflat=$(wide_tree_assumed_bytes)
    slice_bytes_nonflat=$(depth_slice_assumed_bytes)
    badge_bytes_nonflat=$(badge_fixtures_assumed_bytes)
    margin_bytes_nonflat=$(margin_neutral_assumed_bytes)
    extreme_bytes_nonflat=$(extreme_mega_assumed_bytes)
    headroom=$((DISK_BUDGET_BYTES - chain_bytes - wide_bytes_nonflat - slice_bytes_nonflat - badge_bytes_nonflat - margin_bytes_nonflat - extreme_bytes_nonflat))
    if [[ "$headroom" -lt 0 ]]; then headroom=0; fi
    sparse_per=$((SPARSE_FILE_MIB * 1024 * 1024))
    denom=$((ASSUMED_BYTES_PER_FLAT_FILE + sparse_per))
    if [[ "$denom" -lt 1 ]]; then denom=1; fi
    new_flat=$((headroom / denom))
    echo "AUTO_CAP_FLAT: reducing FLAT_FILES $FLAT_FILES -> $new_flat to stay within disk budget." >&2
    FLAT_FILES=$new_flat
    est=$(estimate_bytes)
    if [[ "$est" -gt "$DISK_BUDGET_BYTES" ]]; then
      echo "ERROR: non-flat trees (DEPTH_CHAIN / wide_shallow / depth_slash_profile / ereport_badge_fixtures / SYNTH_PROFILE=extreme mega_dir* / SPARSE_FILE_MIB) consume the whole disk budget; reduce those or raise DISK_BUDGET_BYTES." >&2
      exit 1
    fi
  else
    echo "ERROR: estimated footprint $(human_bytes "$est") exceeds budget $(human_bytes "$DISK_BUDGET_BYTES")." >&2
    echo "  Lower FLAT_FILES / DEPTH_CHAIN / WIDE_* / DEPTH_* / SPARSE_FILE_MIB / SYNTH_EXTREME_MEGA_*, raise DISK_BUDGET_BYTES, set EREPORT_BADGE_FIXTURES=0, or set AUTO_CAP_FLAT=1." >&2
    exit 1
  fi
fi

if [[ "${SYNTH_RANDOM_UID_ENABLE:-0}" == "1" ]]; then
  if ! SYNTH_RANDOM_UID_FRACTION="$SYNTH_RANDOM_UID_FRACTION" python3 -c 'import os
x = float(os.environ["SYNTH_RANDOM_UID_FRACTION"])
raise SystemExit(0 if 0 < x <= 1 else 1)
' 2>/dev/null; then
    echo "ERROR: SYNTH_RANDOM_UID_FRACTION must be a number in (0, 1] (got ${SYNTH_RANDOM_UID_FRACTION})" >&2
    exit 2
  fi
  if [[ "$SYNTH_RANDOM_UID_MAX_CHOWN" -lt 1 ]]; then
    echo "ERROR: SYNTH_RANDOM_UID_MAX_CHOWN must be >= 1" >&2
    exit 2
  fi
  if [[ "$SYNTH_RANDOM_UID_MIN" -gt "$SYNTH_RANDOM_UID_MAX" ]]; then
    echo "ERROR: SYNTH_RANDOM_UID_MIN must be <= SYNTH_RANDOM_UID_MAX" >&2
    exit 2
  fi
  if [[ "$SYNTH_RANDOM_UID_UNIQUE_MAX" -lt 1 ]]; then
    echo "ERROR: SYNTH_RANDOM_UID_UNIQUE_MAX must be >= 1" >&2
    exit 2
  fi
fi

echo "Output root: $ROOT"
if [[ "$SYNTH_FLAT_SHARD_CAP" == "0" ]]; then
  echo "  single_huge_dir:    $FLAT_FILES regular files (worst case: one dir, no subdir donation)"
else
  echo "  single_huge_dir:    $FLAT_FILES regular files sharded ≤$SYNTH_FLAT_SHARD_CAP per leaf dir under single_huge_dir/s… (SYNTH_FLAT_SHARD_CAP)"
fi
echo "  deep_skinny_chain:  depth $DEPTH_CHAIN (0 skips)"
if [[ "$WIDE_PARENTS" -gt 0 ]]; then
  echo "  wide_shallow:       $WIDE_PARENTS dirs × $WIDE_FILES_EACH files (parallelism-friendly contrast)"
else
  echo "  wide_shallow:       (skipped; set WIDE_PARENTS>0 to create)"
fi
if [[ "$SPARSE_FILE_MIB" -gt 0 ]]; then
  echo "  SPARSE_FILE_MIB:    $SPARSE_FILE_MIB MiB logical per flat file (sparse; actual disk may be far smaller)"
fi
if [[ "$DEPTH_SLICE_ENABLE" == "1" ]]; then
  echo "  depth_slash_profile: peak bins ${DEPTH_PEAK_LO}-${DEPTH_PEAK_HI} × ${DEPTH_PEAK_FILES_PER_BIN} files/bin;"
  echo "                       plateau bins ${DEPTH_PLATEAU_LO}-${DEPTH_PLATEAU_HI} × ${DEPTH_PLATEAU_FILES_PER_BIN} files/bin"
  echo "                       (stored paths have exactly B slashes for bin B — same rule as ecrawl_analyze depth_bin_B)"
else
  echo "  depth_slash_profile: (skipped; DEPTH_SLICE_ENABLE=1 to mimic analyze depth histogram shapes)"
fi
if [[ "$EREPORT_BADGE_FIXTURES" == "1" ]]; then
  echo "  ereport_badge_fixtures: skew_cell (${BADGE_DENSE_N}+${BADGE_DEEP_N} × ${BADGE_FILE_BYTES}B, ~${BADGE_SKEW_STAMP_DAYS}d)$([[ "${BADGE_EXTRA_SKEW_CELL:-1}" == "1" ]] && echo -n " + skew_cell_b(~${BADGE_EXTRA_SKEW_STAMP_DAYS}d)" || echo -n "") + heatmap_grid ($([[ "$BADGE_HEATMAP_GRID" != "1" ]] && echo off || { [[ "$BADGE_HEATMAP_RANDOM" == "1" ]] && echo "on, random ${BADGE_GRID_FILES_MIN}–${BADGE_GRID_FILES_MAX}/cell showcase, ≤${BADGE_GRID_MAX_FILES_PER_DIR}/leaf, sb00×${BADGE_GRID_S0_FRAC} on showcase, _d=${BADGE_HEATMAP_DEEP_PREFIX_LEVELS}, BADGE_HEATMAP_BADGE_CELL_FRAC=${BADGE_HEATMAP_BADGE_CELL_FRAC}" || echo "on, uniform ${BADGE_GRID_FILES_PER_CELL}/cell showcase, _d=${BADGE_HEATMAP_DEEP_PREFIX_LEVELS}, BADGE_HEATMAP_BADGE_CELL_FRAC=${BADGE_HEATMAP_BADGE_CELL_FRAC}"; })) + multi_age (${BADGE_MULTI_DENSE_N}+${BADGE_MULTI_DEEP_N}, megadir shards ≤${BADGE_MEGADIR_SHARD_CAP})$([[ "$BADGE_DEEP_ONLY_N" -gt 0 ]] && echo " + deep_only(${BADGE_DEEP_ONLY_N} in band ${BADGE_DEEP_ONLY_BAND})" || echo "")$([[ "$BADGE_DEEP_ONLY_N" -gt 0 && "${BADGE_HEATMAP_FILL_DEEP_ONLY_S0:-0}" != "1" ]] && echo -n "; heatmap skips ab$(printf '%02d' "$BADGE_DEEP_ONLY_BAND")/sb00 for Deep drill" || true)$([[ "${BADGE_DENSE_FLAT_ENABLE:-1}" == "1" ]] && echo -n " + dense_flat_cell(${BADGE_DENSE_FLAT_N} files, ab${BADGE_DENSE_FLAT_BAND}/sb${BADGE_DENSE_FLAT_SB})" || echo -n "")$([[ "${BADGE_DENSE_FLAT_EXTRA_ENABLE:-1}" == "1" && -n "${BADGE_DENSE_FLAT_EXTRA_PAIRS:-}" ]] && echo -n " + dense_flat_cell_extra($(dense_flat_extra_pair_count)×${BADGE_DENSE_FLAT_EXTRA_N} @${BADGE_DENSE_FLAT_EXTRA_PAIRS})" || echo -n "") ; see header"
else
  echo "  ereport_badge_fixtures: (skipped; set EREPORT_BADGE_FIXTURES=1 for heat-map badge drill data)"
fi
_mnf=$(margin_neutral_file_count)
if [[ "$_mnf" -gt 0 ]]; then
  echo "  margin dilution:      neutral_flat/ × $_mnf files (~${BADGE_MARGIN_NEUTRAL_BYTES}B, depth=${BADGE_MARGIN_NEUTRAL_DEPTH}, ≤${BADGE_MARGIN_NEUTRAL_PARENT_CAP}/leaf; BADGE_MARGIN_DILUTION_ENABLE=1)"
elif [[ "${BADGE_MARGIN_DILUTION_ENABLE:-0}" == "1" ]]; then
  echo "  margin dilution:      enabled but BADGE_MARGIN_NEUTRAL_FILES=0 (no neutral_flat tree)"
fi
if [[ "$EREPORT_BADGE_FIXTURES" == "1" && "$FLAT_FILES" -ge 8192 && "$SYNTH_FLAT_SHARD_CAP" == "0" ]]; then
  echo "  heat-map hint:        FLAT_FILES≥8192 in one directory tends to force Dense on grand total — set SYNTH_FLAT_SHARD_CAP=4096 and/or BADGE_MARGIN_DILUTION_ENABLE=1." >&2
fi
if [[ "$SYNTH_PROFILE" == "extreme" ]]; then
  echo "  mega_dir1:            ${SYNTH_EXTREME_MEGA_DIR1_FILES} regular files (flat dir; fNNNNNNNNN; SHARD_CAP=0)" >&2
  echo "  mega_dir2:            ${SYNTH_EXTREME_MEGA_DIR2_TOP_FILES} top-level files (f…) + ${SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS} dirs (d…/file)" >&2
fi

echo ""
echo "Estimated footprint (conservative metadata model; see ASSUMED_BYTES_* and SPARSE_FILE_MIB): $(human_bytes "$est")"
echo "Disk budget cap: $(human_bytes "$DISK_BUDGET_BYTES")"

sec_fast=$(rough_ecrawl_flat_seconds "$ECRAWL_FLAT_HIGH")
sec_slow=$(rough_ecrawl_flat_seconds "$ECRAWL_FLAT_LOW")
if [[ "$sec_slow" -lt "$sec_fast" ]]; then sec_slow=$sec_fast; fi

chain_leaf_extra=0
[[ "$DEPTH_CHAIN" -gt 0 ]] && chain_leaf_extra=1
approx_other_files=$((WIDE_PARENTS * WIDE_FILES_EACH + chain_leaf_extra + $(depth_slice_file_count) + $(badge_fixtures_file_count) + $(margin_neutral_file_count) + $(extreme_mega_file_count)))
echo ""
echo "Rough ecrawl --no-write duration (flat megadir only; highly approximate):"
echo "  assume ${ECRAWL_FLAT_LOW}–${ECRAWL_FLAT_HIGH} entries/s over $FLAT_FILES files in single_huge_dir"
echo "  → about $(human_duration "$sec_fast") (optimistic) … $(human_duration "$sec_slow") (pessimistic wall clock)"
echo "  Other files here (approx): $approx_other_files outside single_huge_dir (+ $DEPTH_CHAIN dirs in deep_skinny_chain);"
echo "    depth_slash_profile adds $(depth_slice_file_count) files when enabled — deep dirs dominate mkdir time, not flat scan rate."
echo "  Tweak ECRAWL_FLAT_LOW / ECRAWL_FLAT_HIGH if you have a measured rate from your fs."

if [[ "$FLAT_FILES" -gt 500000 ]]; then
  echo "WARNING: FLAT_FILES=$FLAT_FILES will take noticeable time and many inodes." >&2
fi
_extreme_mfc=$(extreme_mega_file_count)
if [[ "$_extreme_mfc" -gt 500000 ]]; then
  echo "WARNING: SYNTH_PROFILE=extreme mega dirs total $_extreme_mfc regular files (mega_dir1+mega_dir2) — long runtime and huge inode use; raise DISK_BUDGET_BYTES as needed." >&2
fi
unset _extreme_mfc

mkdir -p "$flat_dir"

# Python bulk helper: flat files under root; shard_cap 0 = single directory f{width}d names (width grows if n needs it).
_synth_python_flat_megadir() {
  local dest=$1
  local n=$2
  local shard_cap=$3
  SYNTH_FLAT_SHARD_CAP="$shard_cap" \
    python3 - "$dest" "$n" "$SPARSE_FILE_MIB" "$CREATE_JOBS" <<'PY'
import concurrent.futures as cf
import math
import os
import sys

root, n, sparse_mib, jobs_cli = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])

shard_cap = int(os.environ.get("SYNTH_FLAT_SHARD_CAP", "0") or "0")
if shard_cap < 0 or shard_cap > 8191:
    raise SystemExit("SYNTH_FLAT_SHARD_CAP must be 0..8191")


def worker_cap(cli: int) -> int:
    c = os.cpu_count() or 8
    if cli <= 0:
        return min(64, max(8, c * 2))
    return max(1, cli)


jw = worker_cap(jobs_cli)
sparse = sparse_mib > 0
sz = sparse_mib * 1024 * 1024 if sparse else 0
f_width = max(9, int(math.floor(math.log10(max(n - 1, 1)))) + 1)


def touch_flat(path: str) -> None:
    if sparse:
        with open(path, "wb") as f:
            if sz > 0:
                f.seek(sz - 1)
                f.write(b"\0")
    else:
        with open(path, "wb"):
            pass


def touch_one(i: int) -> None:
    if shard_cap <= 0:
        p = os.path.join(root, f"f{i:0{f_width}d}")
    else:
        sub = os.path.join(root, f"s{i // shard_cap:06d}")
        os.makedirs(sub, exist_ok=True)
        p = os.path.join(sub, f"f{i:0{f_width}d}")
    touch_flat(p)


def flat_range(lo: int, hi: int) -> None:
    for i in range(lo, hi):
        touch_one(i)


if jw <= 1 or n < 2048:
    for i in range(n):
        touch_one(i)
        if (i + 1) % 100000 == 0:
            print(f"  ... {i + 1}/{n}", file=sys.stderr)
else:
    jobs_disp = "auto" if jobs_cli <= 0 else str(jobs_cli)
    print(f"  parallel workers≈{jw} (CREATE_JOBS={jobs_disp})", file=sys.stderr)
    cs = max(1, (n + jw - 1) // jw)
    ranges = [(i, min(n, i + cs)) for i in range(0, n, cs)]

    def run_chunk(rg):
        flat_range(rg[0], rg[1])

    with cf.ThreadPoolExecutor(max_workers=jw) as ex:
        list(ex.map(run_chunk, ranges))
    print(f"  ... flat done {n}/{n} ({len(ranges)} chunks)", file=sys.stderr)
PY
}

if [[ "$BATCH_CREATE" == "1" ]] && command -v python3 >/dev/null 2>&1; then
  echo "Creating $FLAT_FILES files under $flat_dir (python3, CREATE_JOBS=$CREATE_JOBS)..."
  _synth_python_flat_megadir "$flat_dir" "$FLAT_FILES" "$SYNTH_FLAT_SHARD_CAP"
else
  if [[ "$SYNTH_FLAT_SHARD_CAP" != "0" ]]; then
    echo "ERROR: SYNTH_FLAT_SHARD_CAP=$SYNTH_FLAT_SHARD_CAP requires BATCH_CREATE=1 and python3." >&2
    exit 1
  fi
  echo "Creating $FLAT_FILES files under $flat_dir (bash loop; slow for large N)..."
  i=0
  while [[ "$i" -lt "$FLAT_FILES" ]]; do
    printf -v name 'f%09d' "$i"
    if [[ "$SPARSE_FILE_MIB" -gt 0 ]]; then
      # Sparse-ish: seek then write one byte (bash cannot truncate easily without dd)
      dd if=/dev/zero of="$flat_dir/$name" bs=1 count=0 seek=$((SPARSE_FILE_MIB * 1024 * 1024)) status=none 2>/dev/null || true
      # Some dd versions need count=0 seek=... — if file missing, touch then truncate via dd
      if [[ ! -e "$flat_dir/$name" ]]; then
        : >"$flat_dir/$name"
      fi
    else
      : >"$flat_dir/$name"
    fi
    i=$((i + 1))
    if ((i % 10000 == 0)); then
      echo "  ... $i/$FLAT_FILES" >&2
    fi
  done
fi

if [[ "$SYNTH_EXTREME_MEGA_DIR1_FILES" -gt 0 ]]; then
  mega1_root="$ROOT/mega_dir1"
  mkdir -p "$mega1_root"
  echo "Creating mega_dir1: $SYNTH_EXTREME_MEGA_DIR1_FILES flat files under $mega1_root (python3, unsharded megadir, CREATE_JOBS=$CREATE_JOBS)..."
  _synth_python_flat_megadir "$mega1_root" "$SYNTH_EXTREME_MEGA_DIR1_FILES" "0"
fi

if [[ "$SYNTH_EXTREME_MEGA_DIR2_TOP_FILES" -gt 0 || "$SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS" -gt 0 ]]; then
  mega2_root="$ROOT/mega_dir2"
  mkdir -p "$mega2_root"
  echo "Creating mega_dir2: $SYNTH_EXTREME_MEGA_DIR2_TOP_FILES top-level f* + $SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS d*/file (python3, CREATE_JOBS=$CREATE_JOBS)..."
  python3 - "$mega2_root" "$SYNTH_EXTREME_MEGA_DIR2_TOP_FILES" "$SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS" "$SPARSE_FILE_MIB" "$CREATE_JOBS" <<'PY'
import concurrent.futures as cf
import math
import os
import sys

root, top_n, nested_n, sparse_mib, jobs_cli = (
    sys.argv[1],
    int(sys.argv[2]),
    int(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
)


def worker_cap(cli: int) -> int:
    c = os.cpu_count() or 8
    if cli <= 0:
        return min(64, max(8, c * 2))
    return max(1, cli)


jw = worker_cap(jobs_cli)
sparse = sparse_mib > 0
sz = sparse_mib * 1024 * 1024 if sparse else 0


def touch_flat(path: str) -> None:
    if sparse:
        with open(path, "wb") as f:
            if sz > 0:
                f.seek(sz - 1)
                f.write(b"\0")
    else:
        with open(path, "wb"):
            pass


def name_width(n: int) -> int:
    if n <= 0:
        return 9
    return max(9, int(math.floor(math.log10(max(n - 1, 1)))) + 1)


f_w = name_width(top_n)
d_w = name_width(nested_n)


def top_chunk(lo: int, hi: int) -> None:
    for i in range(lo, hi):
        touch_flat(os.path.join(root, f"f{i:0{f_w}d}"))


def nested_chunk(lo: int, hi: int) -> None:
    for i in range(lo, hi):
        d = os.path.join(root, f"d{i:0{d_w}d}")
        os.makedirs(d, exist_ok=True)
        touch_flat(os.path.join(d, "file"))


def run_parallel(tag: str, n: int, fn) -> None:
    if n <= 0:
        return
    if jw <= 1 or n < 2048:
        fn(0, n)
        print(f"  ... mega_dir2 {tag} done {n}/{n}", file=sys.stderr)
        return
    jobs_disp = "auto" if jobs_cli <= 0 else str(jobs_cli)
    print(f"  mega_dir2 {tag}: parallel workers≈{jw} (CREATE_JOBS={jobs_disp})", file=sys.stderr)
    cs = max(1, (n + jw - 1) // jw)
    ranges = [(i, min(n, i + cs)) for i in range(0, n, cs)]

    def run_piece(rg):
        lo, hi = rg
        fn(lo, hi)

    with cf.ThreadPoolExecutor(max_workers=jw) as ex:
        list(ex.map(run_piece, ranges))
    print(f"  ... mega_dir2 {tag} done {n}/{n} ({len(ranges)} chunks)", file=sys.stderr)


run_parallel("top_files", top_n, top_chunk)
run_parallel("nested_dirs", nested_n, nested_chunk)
PY
fi

if [[ "$DEPTH_CHAIN" -gt 0 ]]; then
  echo "Creating depth-$DEPTH_CHAIN chain under $chain_root..."
  cur="$chain_root"
  mkdir -p "$cur"
  d=0
  while [[ "$d" -lt "$DEPTH_CHAIN" ]]; do
    next="$cur/$d"
    mkdir -p "$next"
    cur="$next"
    d=$((d + 1))
    if ((d % 500 == 0)); then
      echo "  ... depth $d/$DEPTH_CHAIN" >&2
    fi
  done
  # Short leaf name — path is already near PATH_MAX when DEPTH_CHAIN is large.
  : >"$cur/x"
fi

if [[ "$WIDE_PARENTS" -gt 0 ]]; then
  echo "Creating wide_shallow: $WIDE_PARENTS dirs × $WIDE_FILES_EACH files (CREATE_JOBS=$CREATE_JOBS)..."
  if [[ "$BATCH_CREATE" == "1" ]] && command -v python3 >/dev/null 2>&1; then
    python3 - "$wide_root" "$WIDE_PARENTS" "$WIDE_FILES_EACH" "$SPARSE_FILE_MIB" "$CREATE_JOBS" <<'PY'
import concurrent.futures as cf
import os
import sys

root, parents, each, sparse_mib, jobs_cli = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])


def worker_cap(cli: int) -> int:
    c = os.cpu_count() or 8
    if cli <= 0:
        return min(64, max(8, c * 2))
    return max(1, cli)


jw = worker_cap(jobs_cli)
sparse = sparse_mib > 0
sz = sparse_mib * 1024 * 1024 if sparse else 0


def touch_wide(path: str) -> None:
    if sparse:
        with open(path, "wb") as f:
            if sz > 0:
                f.seek(sz - 1)
                f.write(b"\0")
    else:
        with open(path, "wb"):
            pass


def one_bucket(p: int) -> None:
    d = os.path.join(root, f"bucket_{p:05d}")
    os.makedirs(d, exist_ok=True)
    for i in range(each):
        touch_wide(os.path.join(d, f"w{i:07d}"))


if jw <= 1 or parents < 2:
    for p in range(parents):
        one_bucket(p)
        if (p + 1) % 100 == 0 or p + 1 == parents:
            print(f"  ... wide parents {p + 1}/{parents}", file=sys.stderr)
else:
    print(f"  parallel workers≈{min(jw, parents)} (CREATE_JOBS={'auto' if jobs_cli <= 0 else jobs_cli})", file=sys.stderr)
    with cf.ThreadPoolExecutor(max_workers=min(jw, parents)) as ex:
        list(ex.map(one_bucket, range(parents)))
    print(f"  ... wide_shallow done {parents} buckets", file=sys.stderr)
PY
  else
    p=0
    while [[ "$p" -lt "$WIDE_PARENTS" ]]; do
      printf -v bucket 'bucket_%05d' "$p"
      mkdir -p "$wide_root/$bucket"
      i=0
      while [[ "$i" -lt "$WIDE_FILES_EACH" ]]; do
        printf -v wname 'w%07d' "$i"
        if [[ "$SPARSE_FILE_MIB" -gt 0 ]]; then
          dd if=/dev/zero of="$wide_root/$bucket/$wname" bs=1 count=0 seek=$((SPARSE_FILE_MIB * 1024 * 1024)) status=none 2>/dev/null || : >"$wide_root/$bucket/$wname"
        else
          : >"$wide_root/$bucket/$wname"
        fi
        i=$((i + 1))
      done
      p=$((p + 1))
    done
  fi
fi

if [[ "$DEPTH_SLICE_ENABLE" == "1" ]]; then
  echo "Creating depth_slash_profile (analyze-style slash counts; CREATE_JOBS=$CREATE_JOBS)..."
  mkdir -p "$depth_slice_root"
  python3 - "$depth_slice_root" "$DEPTH_PEAK_LO" "$DEPTH_PEAK_HI" "$DEPTH_PEAK_FILES_PER_BIN" "$DEPTH_PLATEAU_LO" "$DEPTH_PLATEAU_HI" "$DEPTH_PLATEAU_FILES_PER_BIN" "$SPARSE_FILE_MIB" "$CREATE_JOBS" <<'PY'
import concurrent.futures as cf
import os
import sys

root, plo, phi, pn, zlo, zhi, zn, sparse_mib, jobs_cli = (
    sys.argv[1],
    int(sys.argv[2]),
    int(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
    int(sys.argv[6]),
    int(sys.argv[7]),
    int(sys.argv[8]),
    int(sys.argv[9]),
)


def worker_cap(cli: int) -> int:
    c = os.cpu_count() or 8
    if cli <= 0:
        return min(64, max(8, c * 2))
    return max(1, cli)


jw = worker_cap(jobs_cli)
sparse = sparse_mib > 0
sz = sparse_mib * 1024 * 1024 if sparse else 0


def touch(path: str) -> None:
    if sparse:
        with open(path, "wb") as f:
            if sz > 0:
                f.seek(sz - 1)
                f.write(b"\0")
    else:
        with open(path, "wb"):
            pass


def base_dir_for_bin(base_root: str, b: int) -> str:
    """Directory to place files so rel path from crawl root is depth_slash_profile/dslice/bNNN/u... (exactly b slashes total)."""
    if b < 3:
        raise ValueError(f"bin {b} too small; need >= 3")
    k = b - 3
    parts = [base_root, "dslice", f"b{b:03d}"]
    parts.extend(f"u{j:02d}" for j in range(k))
    out = os.path.join(*parts)
    os.makedirs(out, exist_ok=True)
    return out


def emit_range(lo: int, hi: int, n_per: int, tag: str) -> None:
    paths = []
    bins_done = []
    for b in range(lo, hi + 1):
        if b < 3:
            print(f"  skip bin {b} (< 3 slashes)", file=sys.stderr)
            continue
        base = base_dir_for_bin(root, b)
        bins_done.append(b)
        for i in range(n_per):
            paths.append(os.path.join(base, f"f{i:08d}"))
    if not paths:
        return
    print(f"  ... {tag}: {len(paths)} files in bins {bins_done[0]}–{bins_done[-1]} (u-chain up to {bins_done[-1] - 3})", file=sys.stderr)
    if jw <= 1 or len(paths) < 4096:
        for p in paths:
            touch(p)
    else:
        print(f"     parallel workers≈{jw} (CREATE_JOBS={'auto' if jobs_cli <= 0 else jobs_cli})", file=sys.stderr)
        with cf.ThreadPoolExecutor(max_workers=jw) as ex:
            list(ex.map(touch, paths))


emit_range(plo, phi, pn, "peak")
emit_range(zlo, zhi, zn, "plateau")
PY
fi

if [[ "$EREPORT_BADGE_FIXTURES" == "1" ]]; then
  echo "Creating ereport badge fixtures under $badge_fixtures_parent (skew_cell + optional grid + multi_age; CREATE_JOBS=$CREATE_JOBS)..."
  BADGE_GRID_MAX_FILES_PER_DIR="$BADGE_GRID_MAX_FILES_PER_DIR" \
    BADGE_HEATMAP_DEEP_PREFIX_LEVELS="$BADGE_HEATMAP_DEEP_PREFIX_LEVELS" \
    BADGE_GRID_S0_FRAC="$BADGE_GRID_S0_FRAC" \
    BADGE_MEGADIR_SHARD_CAP="$BADGE_MEGADIR_SHARD_CAP" \
    BADGE_EXTRA_SKEW_CELL="$BADGE_EXTRA_SKEW_CELL" \
    BADGE_EXTRA_SKEW_STAMP_DAYS="$BADGE_EXTRA_SKEW_STAMP_DAYS" \
    BADGE_DENSE_FLAT_ENABLE="$BADGE_DENSE_FLAT_ENABLE" \
    BADGE_DENSE_FLAT_N="$BADGE_DENSE_FLAT_N" \
    BADGE_DENSE_FLAT_BAND="$BADGE_DENSE_FLAT_BAND" \
    BADGE_DENSE_FLAT_SB="$BADGE_DENSE_FLAT_SB" \
    BADGE_DENSE_FLAT_EXTRA_ENABLE="$BADGE_DENSE_FLAT_EXTRA_ENABLE" \
    BADGE_DENSE_FLAT_EXTRA_N="$BADGE_DENSE_FLAT_EXTRA_N" \
    BADGE_DENSE_FLAT_EXTRA_PAIRS="$BADGE_DENSE_FLAT_EXTRA_PAIRS" \
    BADGE_HEATMAP_SKIP_DENSE_FLAT_EXTRA_CELLS="${BADGE_HEATMAP_SKIP_DENSE_FLAT_EXTRA_CELLS:-1}" \
    BADGE_HEATMAP_SKIP_DENSE_FLAT_CELL="${BADGE_HEATMAP_SKIP_DENSE_FLAT_CELL:-1}" \
    BADGE_HEATMAP_FILL_DEEP_ONLY_S0="${BADGE_HEATMAP_FILL_DEEP_ONLY_S0:-}" \
    BADGE_HEATMAP_BADGE_CELL_FRAC="$BADGE_HEATMAP_BADGE_CELL_FRAC" \
    BADGE_HEATMAP_SHOWCASE_SEED="$BADGE_HEATMAP_SHOWCASE_SEED" \
    BADGE_HEATMAP_NEUTRAL_MAX_FILES="$BADGE_HEATMAP_NEUTRAL_MAX_FILES" \
    BADGE_HEATMAP_NEUTRAL_MIN_FILES="$BADGE_HEATMAP_NEUTRAL_MIN_FILES" \
    BADGE_HEATMAP_CORPUS_BLEND_NUM="$BADGE_HEATMAP_CORPUS_BLEND_NUM" \
    BADGE_HEATMAP_CORPUS_BLEND_DEN="$BADGE_HEATMAP_CORPUS_BLEND_DEN" \
    BADGE_HEATMAP_RANDOM_SEED="${BADGE_HEATMAP_RANDOM_SEED:-}" \
    python3 - "$badge_fixtures_parent" "$BADGE_DENSE_N" "$BADGE_DEEP_N" "$BADGE_FILE_BYTES" "$BADGE_DEEP_SEGMENTS" "$CREATE_JOBS" \
    "$BADGE_GRID_FILES_PER_CELL" "$BADGE_HEATMAP_GRID" "$BADGE_MULTI_DENSE_N" "$BADGE_MULTI_DEEP_N" \
    "$BADGE_SKEW_STAMP_DAYS" "$BADGE_HEATMAP_OLDEST_DAYS" "$BADGE_DEEP_ONLY_N" "$BADGE_DEEP_ONLY_BAND" \
    "$BADGE_HEATMAP_RANDOM" "$BADGE_GRID_FILES_MIN" "$BADGE_GRID_FILES_MAX" <<'PY'
import concurrent.futures as cf
import os
import random
import sys
import time

(
    parent,
    dense_n,
    deep_n,
    file_bytes,
    deep_segments,
    jobs_cli,
    grid_n,
    heatmap_grid,
    multi_dense_n,
    multi_deep_n,
    skew_stamp_days,
    heatmap_oldest_days,
    deep_only_n,
    deep_only_band,
    heatmap_random,
    grid_files_min,
    grid_files_max,
) = (
    sys.argv[1],
    int(sys.argv[2]),
    int(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
    int(sys.argv[6]),
    int(sys.argv[7]),
    sys.argv[8],
    int(sys.argv[9]),
    int(sys.argv[10]),
    int(sys.argv[11]),
    int(sys.argv[12]),
    int(sys.argv[13]),
    int(sys.argv[14]),
    sys.argv[15],
    int(sys.argv[16]),
    int(sys.argv[17]),
)

_seed = os.environ.get("BADGE_HEATMAP_RANDOM_SEED", "").strip()
if _seed != "":
    random.seed(int(_seed))

grid_max_per_dir = int(os.environ.get("BADGE_GRID_MAX_FILES_PER_DIR", "4096"))
mega_shard_cap = int(os.environ.get("BADGE_MEGADIR_SHARD_CAP", "4096"))
if not (1 <= grid_max_per_dir < 8192):
    raise SystemExit("BADGE_GRID_MAX_FILES_PER_DIR must be 1..8191")
if not (1 <= mega_shard_cap < 8192):
    raise SystemExit("BADGE_MEGADIR_SHARD_CAP must be 1..8191")

try:
    grid_s0_frac = float(os.environ.get("BADGE_GRID_S0_FRAC", "0.18"))
except ValueError:
    grid_s0_frac = 0.18
grid_s0_frac = max(0.0, min(1.0, grid_s0_frac))

try:
    heatmap_deep_prefix_levels = int(os.environ.get("BADGE_HEATMAP_DEEP_PREFIX_LEVELS", "8").strip())
except ValueError:
    raise SystemExit("BADGE_HEATMAP_DEEP_PREFIX_LEVELS must be an integer")
if heatmap_deep_prefix_levels < 0 or heatmap_deep_prefix_levels > 64:
    raise SystemExit("BADGE_HEATMAP_DEEP_PREFIX_LEVELS must be 0..64")

# Midpoints for rows ab00..ab04; ab05 uses heatmap_oldest_days (must be >= 3*365 for ereport “3+ years”).
AGE_DAYS = (15, 45, 120, 270, 550, heatmap_oldest_days)


def parse_row_bias(raw: str):
    raw = raw.strip()
    if not raw:
        return [0] * 6
    parts = raw.split(",")
    if len(parts) != 6:
        raise SystemExit("BADGE_HEATMAP_ROW_DAY_BIAS must be exactly six comma-separated integers")
    out = []
    for p in parts:
        p = p.strip()
        if p == "":
            raise SystemExit("BADGE_HEATMAP_ROW_DAY_BIAS: empty component")
        out.append(int(p, 10))
    return out


row_bias = parse_row_bias(os.environ.get("BADGE_HEATMAP_ROW_DAY_BIAS", ""))

blend_num = int(os.environ.get("BADGE_HEATMAP_CORPUS_BLEND_NUM", "85"))
blend_den = int(os.environ.get("BADGE_HEATMAP_CORPUS_BLEND_DEN", "100"))
if blend_den < 1:
    blend_den = 1
if blend_num < 1:
    blend_num = 1
corpus_blend = min(1.0, blend_num / float(blend_den))

try:
    badge_cell_frac = float(os.environ.get("BADGE_HEATMAP_BADGE_CELL_FRAC", "0.30"))
except ValueError:
    badge_cell_frac = 0.30
badge_cell_frac = max(0.0, min(1.0, badge_cell_frac))

neutral_max = int(os.environ.get("BADGE_HEATMAP_NEUTRAL_MAX_FILES", "19"))
neutral_min = int(os.environ.get("BADGE_HEATMAP_NEUTRAL_MIN_FILES", "3"))
if not (1 <= neutral_max < 20):
    raise SystemExit("BADGE_HEATMAP_NEUTRAL_MAX_FILES must be 1..19 (< PATH_SHAPE_MIN_BUCKET_FILES)")
if not (1 <= neutral_min <= neutral_max):
    raise SystemExit("BADGE_HEATMAP_NEUTRAL_MIN_FILES must be 1..BADGE_HEATMAP_NEUTRAL_MAX_FILES")

def worker_cap(cli: int) -> int:
    c = os.cpu_count() or 8
    if cli <= 0:
        return min(64, max(8, c * 2))
    return max(1, cli)


jw = worker_cap(jobs_cli)
blob = b"\0" * file_bytes if file_bytes > 0 else b""


def write_one(path: str) -> None:
    with open(path, "wb") as f:
        if blob:
            f.write(blob)


def dense_range(lo: int, hi: int, dense_dir: str) -> None:
    for i in range(lo, hi):
        write_one(os.path.join(dense_dir, f"f{i:05d}"))


def deep_range(lo: int, hi: int, leaf: str) -> None:
    for i in range(lo, hi):
        write_one(os.path.join(leaf, f"u{i:05d}"))


def logical_size_for_sb(sb: int) -> int:
    """Sparse logical sizes that fall inside ereport size_bucket_for() bands (ereport.c)."""
    return (
        4000,
        512 * 1024,
        52 * 1024 * 1024,
        200 * 1024 * 1024,
        5 * 1024**3,
        12 * 1024**3,
    )[sb]


def write_sized_sparse(path: str, sz: int) -> None:
    if sz <= 0:
        with open(path, "wb"):
            pass
        return
    with open(path, "wb") as f:
        f.seek(sz - 1)
        f.write(b"\0")


def heatmap_cell_files(lo: int, hi: int, cell: str, sz: int, ts: float, cap: int) -> None:
    for i in range(lo, hi):
        sub = os.path.join(cell, f"h{i // cap:05d}")
        os.makedirs(sub, exist_ok=True)
        p = os.path.join(sub, f"g{i:07d}")
        write_sized_sparse(p, sz)
        try:
            os.utime(p, (ts, ts))
        except OSError as e:
            print(f"  warn: utime {p}: {e}", file=sys.stderr)


def emit_skew_fixture(sub_name: str, stamp_days: int) -> None:
    skew_root = os.path.join(parent, sub_name)
    dense_dir = os.path.join(skew_root, "dense_parent")
    os.makedirs(dense_dir, exist_ok=True)
    cur = os.path.join(skew_root, "deep_branch")
    for j in range(deep_segments):
        cur = os.path.join(cur, f"d{j:02d}")
        os.makedirs(cur, exist_ok=True)
    leaf_dir = cur
    print(
        f"  ... {sub_name} dense_parent: {dense_n} files; deep_branch leaf {leaf_dir} ({deep_segments} segments)",
        file=sys.stderr,
    )
    if jw <= 1 or dense_n < 2048:
        dense_range(0, dense_n, dense_dir)
    else:
        cs = max(1, (dense_n + jw - 1) // jw)
        ranges = [(i, min(dense_n, i + cs)) for i in range(0, dense_n, cs)]
        print(f"     dense parallel workers≈{jw} (CREATE_JOBS={'auto' if jobs_cli <= 0 else jobs_cli})", file=sys.stderr)
        with cf.ThreadPoolExecutor(max_workers=jw) as ex:
            list(ex.map(lambda rg: dense_range(rg[0], rg[1], dense_dir), ranges))
    if jw <= 1 or deep_n < 2048:
        deep_range(0, deep_n, leaf_dir)
    else:
        cs = max(1, (deep_n + jw - 1) // jw)
        ranges = [(i, min(deep_n, i + cs)) for i in range(0, deep_n, cs)]
        print(f"     deep parallel workers≈{jw}", file=sys.stderr)
        with cf.ThreadPoolExecutor(max_workers=jw) as ex:
            list(ex.map(lambda rg: deep_range(rg[0], rg[1], leaf_dir), ranges))
    skew_ts = time.time() - stamp_days * 86400
    stamped = 0
    for dirpath, _, filenames in os.walk(skew_root):
        for fn in filenames:
            p = os.path.join(dirpath, fn)
            try:
                os.utime(p, (skew_ts, skew_ts))
                stamped += 1
            except OSError as e:
                print(f"  warn: utime {p}: {e}", file=sys.stderr)
    print(
        f"  ... {sub_name}: stamped atime=mtime (~{stamp_days}d ago) on {stamped} files "
        f"(Linux: utime refreshes ctime → strong C-led vs mtime/atime basis)",
        file=sys.stderr,
    )


def dense_multi_shard_path(dm: str, idx: int, cap: int) -> str:
    sub = os.path.join(dm, f"s{idx // cap:04d}")
    os.makedirs(sub, exist_ok=True)
    return os.path.join(sub, f"m{idx:05d}")


os.makedirs(parent, exist_ok=True)
emit_skew_fixture("skew_cell", skew_stamp_days)
if os.environ.get("BADGE_EXTRA_SKEW_CELL", "1").strip() == "1":
    emit_skew_fixture(
        "skew_cell_b",
        int(os.environ.get("BADGE_EXTRA_SKEW_STAMP_DAYS", "265")),
    )

now = time.time()

dense_flat_en = os.environ.get("BADGE_DENSE_FLAT_ENABLE", "1").strip() == "1"
dense_flat_n = int(os.environ.get("BADGE_DENSE_FLAT_N", "8500"))
dense_flat_band = int(os.environ.get("BADGE_DENSE_FLAT_BAND", "5"))
dense_flat_sb = int(os.environ.get("BADGE_DENSE_FLAT_SB", "1"))
if dense_flat_en:
    if dense_flat_n < 8192:
        raise SystemExit("BADGE_DENSE_FLAT_N must be >= 8192")
    if not (0 <= dense_flat_band <= 5 and 0 <= dense_flat_sb <= 5):
        raise SystemExit("BADGE_DENSE_FLAT_BAND / BADGE_DENSE_FLAT_SB must be 0..5")
    df_root = os.path.join(
        parent,
        "dense_flat_cell",
        f"ab{dense_flat_band:02d}",
        f"sb{dense_flat_sb:02d}",
        "flat_megadir",
    )
    os.makedirs(df_root, exist_ok=True)
    sz_df = logical_size_for_sb(dense_flat_sb)
    ts_df = now - (AGE_DAYS[dense_flat_band] + row_bias[dense_flat_band]) * 86400

    def dense_flat_one(i: int) -> None:
        p = os.path.join(df_root, f"d{i:05d}")
        write_sized_sparse(p, sz_df)
        try:
            os.utime(p, (ts_df, ts_df))
        except OSError as e:
            print(f"  warn: utime {p}: {e}", file=sys.stderr)

    if jw <= 1 or dense_flat_n < 2048:
        for di in range(dense_flat_n):
            dense_flat_one(di)
    else:
        cs = max(1, (dense_flat_n + jw - 1) // jw)
        ranges = [(i, min(dense_flat_n, i + cs)) for i in range(0, dense_flat_n, cs)]

        def dense_flat_chunk(rg):
            lo, hi = rg
            for di in range(lo, hi):
                dense_flat_one(di)

        with cf.ThreadPoolExecutor(max_workers=jw) as ex:
            list(ex.map(dense_flat_chunk, ranges))
    print(
        f"  ... dense_flat_cell: {dense_flat_n} sparse files under {df_root} "
        f"(single flat megadir → Dense badge; shallow paths)",
        file=sys.stderr,
    )

# Extra Dense-only slices: each (band, sb) pair gets its own dense_flat_cell_BS/abBB/sbSS/flat_megadir
extra_dense_flat_en = os.environ.get("BADGE_DENSE_FLAT_EXTRA_ENABLE", "1").strip() == "1"
extra_dense_flat_n = int(os.environ.get("BADGE_DENSE_FLAT_EXTRA_N", "8500"))
extra_dense_flat_pairs = []
if extra_dense_flat_en:
    raw_extra = os.environ.get("BADGE_DENSE_FLAT_EXTRA_PAIRS", "").strip()
    if raw_extra:
        if extra_dense_flat_n < 8192:
            raise SystemExit("BADGE_DENSE_FLAT_EXTRA_N must be >= 8192")
        seen_extra = set()
        for tok in raw_extra.split(","):
            tok = tok.strip()
            if not tok:
                continue
            if ":" not in tok:
                raise SystemExit(f"BADGE_DENSE_FLAT_EXTRA_PAIRS entry '{tok}' must be 'BAND:SB'")
            b_s, s_s = tok.split(":", 1)
            try:
                eb_i, es_i = int(b_s), int(s_s)
            except ValueError:
                raise SystemExit(f"BADGE_DENSE_FLAT_EXTRA_PAIRS entry '{tok}' must be integers")
            if not (0 <= eb_i <= 5 and 0 <= es_i <= 5):
                raise SystemExit(f"BADGE_DENSE_FLAT_EXTRA_PAIRS entry '{tok}' must be 0..5")
            if (eb_i, es_i) in seen_extra:
                raise SystemExit(f"BADGE_DENSE_FLAT_EXTRA_PAIRS has duplicate cell '{tok}'")
            if dense_flat_en and eb_i == dense_flat_band and es_i == dense_flat_sb:
                raise SystemExit(
                    f"BADGE_DENSE_FLAT_EXTRA_PAIRS '{tok}' overlaps primary dense_flat_cell"
                )
            seen_extra.add((eb_i, es_i))
            extra_dense_flat_pairs.append((eb_i, es_i))

for (eb, es) in extra_dense_flat_pairs:
    edf_root = os.path.join(
        parent,
        f"dense_flat_cell_{eb}{es}",
        f"ab{eb:02d}",
        f"sb{es:02d}",
        "flat_megadir",
    )
    os.makedirs(edf_root, exist_ok=True)
    sz_e = logical_size_for_sb(es)
    ts_e = now - (AGE_DAYS[eb] + row_bias[eb]) * 86400

    def _make_extra_writer(root_dir, sz_v, ts_v):
        def _w(i):
            p = os.path.join(root_dir, f"d{i:05d}")
            write_sized_sparse(p, sz_v)
            try:
                os.utime(p, (ts_v, ts_v))
            except OSError as e:
                print(f"  warn: utime {p}: {e}", file=sys.stderr)
        return _w

    extra_w = _make_extra_writer(edf_root, sz_e, ts_e)
    if jw <= 1 or extra_dense_flat_n < 2048:
        for di in range(extra_dense_flat_n):
            extra_w(di)
    else:
        cs_e = max(1, (extra_dense_flat_n + jw - 1) // jw)
        ranges_e = [
            (i, min(extra_dense_flat_n, i + cs_e))
            for i in range(0, extra_dense_flat_n, cs_e)
        ]

        def _extra_chunk(rg, w=extra_w):
            lo, hi = rg
            for di in range(lo, hi):
                w(di)

        with cf.ThreadPoolExecutor(max_workers=jw) as ex:
            list(ex.map(_extra_chunk, ranges_e))
    print(
        f"  ... dense_flat_cell_{eb}{es}: {extra_dense_flat_n} sparse files under {edf_root} "
        f"(extra Dense-only slice ab{eb:02d}/sb{es:02d})",
        file=sys.stderr,
    )

if heatmap_grid == "1":
    grid_root = os.path.join(parent, "heatmap_grid")
    heatmap_cells_root = grid_root
    for di in range(heatmap_deep_prefix_levels):
        heatmap_cells_root = os.path.join(heatmap_cells_root, f"_d{di:02d}")
        os.makedirs(heatmap_cells_root, exist_ok=True)
    if heatmap_deep_prefix_levels > 0:
        print(
            f"  ... heatmap_grid: {heatmap_deep_prefix_levels} _d-prefix levels for showcase cells only "
            f"(neutral cells use shallow heatmap_grid/ab**/sb**)",
            file=sys.stderr,
        )
    total_g = 0
    use_rand = heatmap_random == "1"
    fill_deep_s0 = os.environ.get("BADGE_HEATMAP_FILL_DEEP_ONLY_S0", "").strip() == "1"
    skip_heatmap_deep_s0 = deep_only_n > 0 and not fill_deep_s0
    skip_dense_flat_cell = (
        dense_flat_en
        and os.environ.get("BADGE_HEATMAP_SKIP_DENSE_FLAT_CELL", "1").strip() == "1"
    )
    skip_extra_dense_flat = (
        bool(extra_dense_flat_pairs)
        and os.environ.get("BADGE_HEATMAP_SKIP_DENSE_FLAT_EXTRA_CELLS", "1").strip() == "1"
    )
    extra_skip_set = set(extra_dense_flat_pairs) if skip_extra_dense_flat else set()
    if skip_heatmap_deep_s0:
        print(
            f"  ... heatmap_grid: skip ab{deep_only_band:02d}/sb00 (Deep drill; BADGE_HEATMAP_FILL_DEEP_ONLY_S0=1 to fill)",
            file=sys.stderr,
        )
    if skip_dense_flat_cell:
        print(
            f"  ... heatmap_grid: skip ab{dense_flat_band:02d}/sb{dense_flat_sb:02d} "
            f"(leave dense_flat_cell unmixed; BADGE_HEATMAP_SKIP_DENSE_FLAT_CELL=0 to fill)",
            file=sys.stderr,
        )
    if skip_extra_dense_flat:
        cells_str = ", ".join(f"ab{b:02d}/sb{s:02d}" for (b, s) in extra_dense_flat_pairs)
        print(
            f"  ... heatmap_grid: skip {cells_str} "
            f"(extra dense_flat_cell_*; BADGE_HEATMAP_SKIP_DENSE_FLAT_EXTRA_CELLS=0 to fill)",
            file=sys.stderr,
        )

    fillable = []
    for ab in range(6):
        for sb in range(6):
            if skip_heatmap_deep_s0 and ab == deep_only_band and sb == 0:
                continue
            if skip_dense_flat_cell and ab == dense_flat_band and sb == dense_flat_sb:
                continue
            if (ab, sb) in extra_skip_set:
                continue
            fillable.append((ab, sb))

    seed_showcase = os.environ.get("BADGE_HEATMAP_SHOWCASE_SEED", "").strip()
    if seed_showcase == "":
        seed_showcase = os.environ.get("BADGE_HEATMAP_RANDOM_SEED", "").strip()
    if seed_showcase == "":
        seed_showcase = "1704067200"
    try:
        pick_seed_i = int(seed_showcase)
    except ValueError:
        raise SystemExit(
            "BADGE_HEATMAP_SHOWCASE_SEED (or BADGE_HEATMAP_RANDOM_SEED fallback) must be an integer"
        )

    rng_pick = random.Random(pick_seed_i)
    fillable_shuffled = list(fillable)
    rng_pick.shuffle(fillable_shuffled)
    n_showcase_target = min(len(fillable_shuffled), max(0, int(round(36 * badge_cell_frac))))
    showcase_set = set(fillable_shuffled[:n_showcase_target])
    print(
        f"  ... heatmap_grid: showcase picks {len(showcase_set)}/{len(fillable)} fillable cells "
        f"(target round(36×{badge_cell_frac})={n_showcase_target}; pick_seed={seed_showcase})",
        file=sys.stderr,
    )

    for ab in range(6):
        for sb in range(6):
            if skip_heatmap_deep_s0 and ab == deep_only_band and sb == 0:
                continue
            if skip_dense_flat_cell and ab == dense_flat_band and sb == dense_flat_sb:
                continue
            if (ab, sb) in extra_skip_set:
                continue
            is_showcase = (ab, sb) in showcase_set
            if is_showcase:
                cell = os.path.join(heatmap_cells_root, f"ab{ab:02d}", f"sb{sb:02d}")
            else:
                cell = os.path.join(grid_root, f"ab{ab:02d}", f"sb{sb:02d}")
            os.makedirs(cell, exist_ok=True)
            ts = now - (AGE_DAYS[ab] + row_bias[ab]) * 86400
            sz = logical_size_for_sb(sb)
            if is_showcase:
                n_here = random.randint(grid_files_min, grid_files_max) if use_rand else grid_n
                if sb == 0 and grid_s0_frac < 1.0:
                    n_here = max(grid_files_min, int(n_here * grid_s0_frac))
                if corpus_blend < 1.0:
                    floor_n = grid_files_min if use_rand else max(1, grid_n)
                    n_here = max(floor_n, int(n_here * corpus_blend))
            else:
                if use_rand:
                    n_here = random.randint(neutral_min, neutral_max)
                else:
                    n_here = min(neutral_max, max(neutral_min, grid_n))
                if corpus_blend < 1.0:
                    n_here = max(neutral_min, min(neutral_max, int(n_here * corpus_blend)))
            if jw <= 1 or n_here < 2048:
                heatmap_cell_files(0, n_here, cell, sz, ts, grid_max_per_dir)
            else:
                cs = max(1, (n_here + jw - 1) // jw)
                ranges = [(i, min(n_here, i + cs)) for i in range(0, n_here, cs)]
                with cf.ThreadPoolExecutor(max_workers=jw) as ex:
                    list(
                        ex.map(
                            lambda rg: heatmap_cell_files(rg[0], rg[1], cell, sz, ts, grid_max_per_dir),
                            ranges,
                        )
                    )
            total_g += n_here
    mode = f"random {grid_files_min}–{grid_files_max} files/cell (seed={'set' if _seed != '' else 'nondeterministic'})" if use_rand else f"uniform {grid_n}/cell"
    print(
        f"  ... heatmap_grid: {total_g} files (6×6, {mode}); ≤{grid_max_per_dir} files/leaf dir; "
        f"sb00×{grid_s0_frac} on showcase; neutral shallow ≤{neutral_max} files/cell",
        file=sys.stderr,
    )

dm = os.path.join(parent, "dense_multi_age", "megadir")
os.makedirs(dm, exist_ok=True)
if deep_only_n <= 0:
    bounds_m = [round(i * multi_dense_n / 6) for i in range(7)]
    for g in range(6):
        lo, hi = bounds_m[g], bounds_m[g + 1]
        ts = now - AGE_DAYS[g] * 86400
        for i in range(lo, hi):
            p = dense_multi_shard_path(dm, i, mega_shard_cap)
            write_one(p)
            try:
                os.utime(p, (ts, ts))
            except OSError as e:
                print(f"  warn: utime {p}: {e}", file=sys.stderr)
    print(
        f"  ... dense_multi_age/megadir: {multi_dense_n} files × {file_bytes}B, 6 utime bands (shards ≤{mega_shard_cap})",
        file=sys.stderr,
    )
else:
    if deep_only_band < 0 or deep_only_band > 5:
        raise SystemExit("deep_only_band must be 0..5")
    dense_bands = [g for g in range(6) if g != deep_only_band]
    n5 = len(dense_bands)
    base = multi_dense_n // n5
    rem = multi_dense_n % n5
    sizes = [base + (1 if j < rem else 0) for j in range(n5)]
    cursor = 0
    for si, g in enumerate(dense_bands):
        sz = sizes[si]
        lo, hi = cursor, cursor + sz
        cursor = hi
        ts = now - AGE_DAYS[g] * 86400
        for i in range(lo, hi):
            p = dense_multi_shard_path(dm, i, mega_shard_cap)
            write_one(p)
            try:
                os.utime(p, (ts, ts))
            except OSError as e:
                print(f"  warn: utime {p}: {e}", file=sys.stderr)
    print(
        f"  ... dense_multi_age/megadir: {multi_dense_n} files × {file_bytes}B, 5 bands (skipped row {deep_only_band}; shards ≤{mega_shard_cap})",
        file=sys.stderr,
    )

deep_root = os.path.join(parent, "deep_multi_age")
curm = os.path.join(deep_root, "deep_branch")
for j in range(deep_segments):
    curm = os.path.join(curm, f"d{j:02d}")
    os.makedirs(curm, exist_ok=True)
leaf_m = curm
bounds_d = [round(i * multi_deep_n / 6) for i in range(7)]
for g in range(6):
    lo, hi = bounds_d[g], bounds_d[g + 1]
    ts = now - AGE_DAYS[g] * 86400
    for i in range(lo, hi):
        p = os.path.join(leaf_m, f"u{i:05d}")
        write_one(p)
        try:
            os.utime(p, (ts, ts))
        except OSError as e:
            print(f"  warn: utime {p}: {e}", file=sys.stderr)
print(
    f"  ... deep_multi_age: {multi_deep_n} files × {file_bytes}B under {leaf_m}",
    file=sys.stderr,
)

if deep_only_n > 0:
    do_root = os.path.join(parent, "deep_only_cell")
    cur_do = os.path.join(do_root, "deep_branch")
    for j in range(deep_segments):
        cur_do = os.path.join(cur_do, f"d{j:02d}")
        os.makedirs(cur_do, exist_ok=True)
    leaf_do = cur_do
    ts_do = now - AGE_DAYS[deep_only_band] * 86400
    if jw <= 1 or deep_only_n < 2048:

        def dor(lo: int, hi: int) -> None:
            for i in range(lo, hi):
                p = os.path.join(leaf_do, f"v{i:05d}")
                write_one(p)
                try:
                    os.utime(p, (ts_do, ts_do))
                except OSError as e:
                    print(f"  warn: utime {p}: {e}", file=sys.stderr)

        dor(0, deep_only_n)
    else:
        cs = max(1, (deep_only_n + jw - 1) // jw)

        def dor_chunk(rg):
            lo, hi = rg
            for i in range(lo, hi):
                p = os.path.join(leaf_do, f"v{i:05d}")
                write_one(p)
                try:
                    os.utime(p, (ts_do, ts_do))
                except OSError as e:
                    print(f"  warn: utime {p}: {e}", file=sys.stderr)

        ranges = [(i, min(deep_only_n, i + cs)) for i in range(0, deep_only_n, cs)]
        with cf.ThreadPoolExecutor(max_workers=jw) as ex:
            list(ex.map(dor_chunk, ranges))
    print(
        f"  ... deep_only_cell: {deep_only_n} files × {file_bytes}B under {leaf_do} (band {deep_only_band}, no megadir in row → Deep pill)",
        file=sys.stderr,
    )
PY
  echo "ereport hint: Linux updates ctime when utime() sets atime/mtime — broad C-led in HTML is normal for these fixtures; set EREPORT_HEAT_CTIME_LED_MIN_SHARE to require a larger share before showing the purple badge." >&2
fi

if [[ "${BADGE_MARGIN_DILUTION_ENABLE:-0}" == "1" ]]; then
  _mn_run=$(margin_neutral_file_count)
  if [[ "$_mn_run" -gt 0 ]]; then
    echo "Creating margin dilution under $ROOT/neutral_flat ($_mn_run × ~${BADGE_MARGIN_NEUTRAL_BYTES}B files; CREATE_JOBS=$CREATE_JOBS)..."
    BADGE_HEATMAP_ROW_DAY_BIAS="${BADGE_HEATMAP_ROW_DAY_BIAS:-}" \
    BADGE_MARGIN_NEUTRAL_UTIME="$BADGE_MARGIN_NEUTRAL_UTIME" \
      python3 - "$ROOT" "$_mn_run" "$BADGE_MARGIN_NEUTRAL_PARENT_CAP" "$BADGE_MARGIN_NEUTRAL_DEPTH" \
      "$BADGE_MARGIN_NEUTRAL_BYTES" "$CREATE_JOBS" "$BADGE_HEATMAP_OLDEST_DAYS" <<'NEUTRAL_PY'
import concurrent.futures as cf
import os
import sys
import time

root, n_files, parent_cap, extra_depth, file_bytes, jobs_cli, heatmap_oldest = (
    sys.argv[1],
    int(sys.argv[2]),
    int(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
    int(sys.argv[6]),
    int(sys.argv[7]),
)

if not (1 <= parent_cap < 8192):
    raise SystemExit("BADGE_MARGIN_NEUTRAL_PARENT_CAP must be 1..8191")
if extra_depth < 0 or extra_depth > 32:
    raise SystemExit("BADGE_MARGIN_NEUTRAL_DEPTH must be 0..32")


def parse_row_bias(raw: str):
    raw = raw.strip()
    if not raw:
        return [0] * 6
    parts = raw.split(",")
    if len(parts) != 6:
        raise SystemExit("BADGE_HEATMAP_ROW_DAY_BIAS must be exactly six comma-separated integers")
    out = []
    for p in parts:
        p = p.strip()
        if p == "":
            raise SystemExit("BADGE_HEATMAP_ROW_DAY_BIAS: empty component")
        out.append(int(p, 10))
    return out


AGE_DAYS = (15, 45, 120, 270, 550, heatmap_oldest)
row_bias = parse_row_bias(os.environ.get("BADGE_HEATMAP_ROW_DAY_BIAS", ""))


def worker_cap(cli: int) -> int:
    c = os.cpu_count() or 8
    if cli <= 0:
        return min(64, max(8, c * 2))
    return max(1, cli)


jw = worker_cap(jobs_cli)
blob = b"\0" * file_bytes if file_bytes > 0 else b""
neutral_root = os.path.join(root, "neutral_flat")
os.makedirs(neutral_root, exist_ok=True)
stamp_utime = os.environ.get("BADGE_MARGIN_NEUTRAL_UTIME", "1").strip() == "1"


def path_for(i: int) -> str:
    cur = neutral_root
    for d in range(extra_depth):
        cur = os.path.join(cur, f"x{d}_{(i >> (8 * d)) & 0xFF:02x}")
        os.makedirs(cur, exist_ok=True)
    band = i % 6
    cur = os.path.join(cur, f"band{band:02d}")
    os.makedirs(cur, exist_ok=True)
    k = i // 6
    sub = os.path.join(cur, f"h{k // parent_cap:05d}")
    os.makedirs(sub, exist_ok=True)
    return os.path.join(sub, f"n{i:09d}")


def write_one(i: int) -> None:
    p = path_for(i)
    with open(p, "wb") as f:
        if blob:
            f.write(blob)
    if stamp_utime:
        band = i % 6
        ts = time.time() - (AGE_DAYS[band] + row_bias[band]) * 86400
        try:
            os.utime(p, (ts, ts))
        except OSError as e:
            print(f"  warn: utime {p}: {e}", file=sys.stderr)


if jw <= 1 or n_files < 2048:
    for i in range(n_files):
        write_one(i)
        if (i + 1) % 100000 == 0:
            print(f"  ... neutral_flat {i + 1}/{n_files}", file=sys.stderr)
else:
    print(
        f"  parallel workers≈{jw} (CREATE_JOBS={'auto' if jobs_cli <= 0 else jobs_cli})",
        file=sys.stderr,
    )
    cs = max(1, (n_files + jw - 1) // jw)
    ranges = [(i, min(n_files, i + cs)) for i in range(0, n_files, cs)]

    def run_chunk(rg):
        lo, hi = rg
        for j in range(lo, hi):
            write_one(j)

    with cf.ThreadPoolExecutor(max_workers=jw) as ex:
        list(ex.map(run_chunk, ranges))
    print(f"  ... neutral_flat done {n_files}/{n_files}", file=sys.stderr)
NEUTRAL_PY
  fi
fi

if [[ "${SYNTH_RANDOM_UID_ENABLE:-0}" == "1" ]]; then
  case "${SYNTH_RANDOM_UID_SCOPE:-badge}" in
    badge)
      _synth_uid_root="$badge_fixtures_parent"
      ;;
    all)
      _synth_uid_root="$ROOT"
      ;;
    *)
      echo "ERROR: SYNTH_RANDOM_UID_SCOPE must be badge or all (got ${SYNTH_RANDOM_UID_SCOPE})" >&2
      exit 2
      ;;
  esac
  if [[ "${SYNTH_RANDOM_UID_SCOPE:-badge}" == "badge" && "$EREPORT_BADGE_FIXTURES" != "1" ]]; then
    echo "SYNTH_RANDOM_UID: scope=badge but EREPORT_BADGE_FIXTURES=0; skipping." >&2
  elif [[ ! -d "$_synth_uid_root" ]]; then
    echo "SYNTH_RANDOM_UID: missing directory $_synth_uid_root; skipping." >&2
  else
    SYNTH_RANDOM_UID_ROOT="$_synth_uid_root" \
    SYNTH_RANDOM_UID_FRACTION="$SYNTH_RANDOM_UID_FRACTION" \
    SYNTH_RANDOM_UID_MAX_CHOWN="$SYNTH_RANDOM_UID_MAX_CHOWN" \
    SYNTH_RANDOM_UID_MIN="$SYNTH_RANDOM_UID_MIN" \
    SYNTH_RANDOM_UID_MAX="$SYNTH_RANDOM_UID_MAX" \
    SYNTH_RANDOM_UID_UNIQUE_MAX="$SYNTH_RANDOM_UID_UNIQUE_MAX" \
    SYNTH_RANDOM_UID_SEED="${SYNTH_RANDOM_UID_SEED:-}" \
      python3 <<'PY'
import os
import random
import stat
import sys

root = os.environ["SYNTH_RANDOM_UID_ROOT"]
frac = float(os.environ["SYNTH_RANDOM_UID_FRACTION"])
max_chown = int(os.environ["SYNTH_RANDOM_UID_MAX_CHOWN"])
lo = int(os.environ["SYNTH_RANDOM_UID_MIN"])
hi = int(os.environ["SYNTH_RANDOM_UID_MAX"])
unique_max = int(os.environ["SYNTH_RANDOM_UID_UNIQUE_MAX"])
seed = os.environ.get("SYNTH_RANDOM_UID_SEED", "").strip()
if seed != "":
    random.seed(int(seed))

if lo > hi:
    print("SYNTH_RANDOM_UID: MIN > MAX; skipping.", file=sys.stderr)
    raise SystemExit(0)

span = hi - lo + 1
pool_n = min(unique_max, span)
uid_pool = random.sample(range(lo, hi + 1), pool_n)

probe = os.path.join(root, ".synth_uid_probe_%d" % os.getpid())
try:
    fd = os.open(probe, os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o600)
    os.close(fd)
except OSError as e:
    print(f"SYNTH_RANDOM_UID: cannot create probe in {root}: {e}; skipping.", file=sys.stderr)
    raise SystemExit(0)

try:
    os.chown(probe, lo, -1)
except OSError:
    try:
        os.unlink(probe)
    except OSError:
        pass
    print(
        "SYNTH_RANDOM_UID: arbitrary UID chown denied (run as root or with CAP_CHOWN). "
        "ecrawl records numeric st_uid from stat when files are owned by those UIDs; skipping random assignment.",
        file=sys.stderr,
    )
    raise SystemExit(0)

try:
    os.unlink(probe)
except OSError:
    pass

n_ok = 0
n_seen = 0
for dirpath, _dirnames, filenames in os.walk(root):
    for fn in filenames:
        if n_ok >= max_chown:
            break
        if fn.startswith(".synth_uid_probe_"):
            continue
        path = os.path.join(dirpath, fn)
        try:
            st = os.lstat(path)
        except OSError:
            continue
        if not stat.S_ISREG(st.st_mode):
            continue
        n_seen += 1
        if random.random() >= frac:
            continue
        uid = random.choice(uid_pool)
        try:
            os.chown(path, uid, -1)
            n_ok += 1
        except OSError:
            pass
    if n_ok >= max_chown:
        break

print(
    f"SYNTH_RANDOM_UID: chowned {n_ok} files under {root} (seen {n_seen} regular files, p≈{frac}, "
    f"uid pool size={pool_n} drawn from [{lo},{hi}], cap={max_chown})",
    file=sys.stderr,
)
PY
  fi
fi

cat <<EOF

Done.

Why this is adversarial for ecrawl:
  - single_huge_dir: one worker walks the entire directory stream; every file needs
    fstatat + emit; stack donation does not split file entries across threads. Use
    SYNTH_FLAT_SHARD_CAP=0 so all FLAT_FILES sit in that one directory (multi-threaded
    python3 creation); higher cap shards into single_huge_dir/sNNNN/ subdirs.
  - deep_skinny_chain: mostly a single path of subdirectories (limited breadth until
    you fan back into other trees).
  - wide_shallow (optional): many top-level buckets — workers can donate sibling dirs.
  - depth_slash_profile (optional): paths sized so stored slash counts match ecrawl_analyze
    depth_bin_* (see DEPTH_PEAK_* / DEPTH_PLATEAU_*): peak mass + deep plateau.

To parallelize naturally, split files across *many sibling directories* instead of
one directory.

Dry-run ecrawl (no write):
  ecrawl --no-write '$ROOT'

Heat-map drill data in ereport (crawl shards + HTML emit):
  EREPORT_HEAT_CTIME_LED_MIN_SHARE=0.45 ./ereport --bucket-details 1 <uid|name> mtime '$ROOT'
  Prefer mtime or atime: fixtures pair atime=mtime for age spread. On Linux, utime() refreshes ctime → most stamped bytes look C-led vs (a,m); raise EREPORT_HEAT_CTIME_LED_MIN_SHARE if purple badges dominate.
  effective basis collapses most ages to “young” because ctime dominates max(a,m,c).
  Expect Skew under skew_cell/ (+ optional skew_cell_b/): megadir+d deep same slice; heatmap uses subdirs so inner cells are not all Dense; dense_multi_age is sharded (no megadir Dense). **dense_flat_cell/** (default ab05×sb01) is an unsharded shallow megadir — heatmap skips that cell when BADGE_HEATMAP_SKIP_DENSE_FLAT_CELL=1 so Deep-prefix noise does not swallow the amber **Dense-only** slice; skew stays on younger rows.
  **dense_flat_cell_BS/** (BADGE_DENSE_FLAT_EXTRA_PAIRS, default 4 extra slices) drops similar unsharded shallow megadirs in age×size cells outside the skew/deep_only rows; heatmap skips them when BADGE_HEATMAP_SKIP_DENSE_FLAT_EXTRA_CELLS=1 so each lands as a clean Dense-only band on the aggregate heat map.
  heatmap_grid: BADGE_HEATMAP_BADGE_CELL_FRAC (default 0.30) picks ~round(36×frac) **showcase** cells with _d-prefix + full counts; others are **neutral** (shallow heatmap_grid/ab**/sb**, ≤BADGE_HEATMAP_NEUTRAL_MAX_FILES default 19). Use FRAC=1.0 for legacy all-showcase grid. Deterministic picks: BADGE_HEATMAP_SHOWCASE_SEED or BADGE_HEATMAP_RANDOM_SEED or built-in default.
  BADGE_HEATMAP_DEEP_PREFIX_LEVELS applies only to showcase paths; neutral stays shallow (no teal Deep from prefix layout).
  sb00 scaling BADGE_GRID_S0_FRAC applies to showcase cells only; deep_only still skips one sb00 heatmap cell unless BADGE_HEATMAP_FILL_DEEP_ONLY_S0=1.
  SYNTH_RANDOM_UID_* (default on): random numeric owners under ereport_badge_fixtures for richer all_users UID rolls; requires root/CAP_CHOWN. At most SYNTH_RANDOM_UID_UNIQUE_MAX (default 5000) distinct UIDs are drawn from [MIN,MAX].
  heatmap_grid uses **random** files/cell by default (BADGE_HEATMAP_RANDOM=1); set BADGE_HEATMAP_RANDOM_SEED for reproducibility or =0 for uniform BADGE_GRID_FILES_PER_CELL.
  **Sparse heat-map margins (defaults on):** BADGE_MARGIN_DILUTION_ENABLE=1, BADGE_MARGIN_NEUTRAL_FILES=520000 (neutral_flat/), SYNTH_FLAT_SHARD_CAP=4096, BADGE_HEATMAP_CORPUS_BLEND_NUM/DEN=85/100 — softens row/column/grand Dense/C-led/Deep aggregates. Intentional dense_flat_cell / skew megadirs can still force Dense on affected margins; set BADGE_DENSE_FLAT_ENABLE=0 or shrink extras to quiet corners further.

EOF
