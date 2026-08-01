#!/usr/bin/env bash
#
# Simulate a Linux /home storage server for ecrawl/ereport scale testing:
#   <root>/home/0001 … /home/<USERS>   (one directory per user, zero-padded)
# Each user home gets a realistic, randomly-shaped tree of subdirectories and
# files (varied depth/fanout), files owned by that user's numeric uid/gid, and
# (optionally) sparse logical sizes so the *apparent* capacity reaches PB-class
# while the *physical* flash footprint stays tiny (one tail block + inode each).
#
# This is NOT the adversarial single-megadir generator
# (see generate-ecrawl-adversarial-tree.sh) — it models normal home dirs.
#
# Usage:
#   sudo ./scripts/fixtures/generate-home-storage-tree.sh /mnt/flash/storage
#   DRY_RUN=1 ./scripts/fixtures/generate-home-storage-tree.sh /mnt/flash/storage   # plan only
#
# Per-user ownership requires root / CAP_CHOWN (HOME_CHOWN=1, default). Without
# it every file would land on one uid and ereport would see a single user; the
# script refuses unless you set HOME_CHOWN=0 on purpose.
#
# Key knobs (env; defaults target the 6000-user / 1.2B-file / ~6 PB ask):
#   USERS=6000                 # home dirs /home/0001 … /home/6000
#   HOME_PARENT=home           # parent under <root> (so paths are <root>/home/NNNN/…)
#   TOTAL_FILES=1200000000     # regular files spread across all users (≈200k/user)
#   TARGET_APPARENT_PB=6       # apparent (logical, sparse) total in decimal PB; 0 = empty files
#   USER_FILE_SIGMA=0.6        # lognormal spread of per-user file counts (0 = every user equal)
#   AVG_FILES_PER_DIR=120      # average regular files per leaf directory
#   MAX_FILES_PER_DIR=2000     # cap per leaf dir (stays < ereport Dense 8192)
#   MAX_DEPTH=6                # max subdir depth under each home
#   SIZE_SIGMA=2.0             # lognormal spread of per-file logical size (heavy tail)
#   SIZE_MIN_BYTES=1024
#   SIZE_MAX_BYTES=68719476736 # 64 GiB clamp on the big tail
#   UID_BASE=100000 GID_BASE=100000   # uid/gid = base + (user_index-1)
#   HOME_CHOWN=1               # chown homes/files to per-user uid:gid (needs root)
#   STAMP_TIMES=1              # set atime=mtime to a random age (spreads ereport age buckets)
#   AGE_MAX_DAYS=1825          # oldest random age (~5 years)
#   CREATE_JOBS=0              # worker threads (0 = auto). Users are built in parallel.
#   SEED=                      # integer for reproducible layout/sizes (empty = random, printed)
#   DRY_RUN=0                  # 1 = print plan + estimates and exit (creates nothing)
#
# Physical budget (REAL flash; sparse logical bytes are NOT counted here):
#   DISK_BUDGET_BYTES          # default 100 TiB. Estimate = TOTAL_FILES×PHYS_BYTES_PER_FILE + dirs.
#   PHYS_BYTES_PER_FILE=4608   # ~1 data block (tail byte) + inode per sparse file
#   PHYS_BYTES_PER_DIR=4096
#
# Pilot first (1/100 scale, ~minutes) to measure throughput, then extrapolate:
#   sudo USERS=60 TOTAL_FILES=12000000 ./scripts/fixtures/generate-home-storage-tree.sh /mnt/flash/pilot
#
# Report afterwards (per-user + all-users; mtime basis spreads the age heat map):
#   ecrawl /mnt/flash/storage /tmp/home_bin
#   ereport --bucket-details 2 --report-dir /tmp/home_report /tmp/home_bin   # all users
set -euo pipefail

ROOT=${1:?usage: $0 <output_root> (e.g. /mnt/flash/storage)}

USERS=${USERS:-6000}
HOME_PARENT=${HOME_PARENT:-home}
TOTAL_FILES=${TOTAL_FILES:-1200000000}
TARGET_APPARENT_PB=${TARGET_APPARENT_PB:-6}
USER_FILE_SIGMA=${USER_FILE_SIGMA:-0.6}
AVG_FILES_PER_DIR=${AVG_FILES_PER_DIR:-120}
MAX_FILES_PER_DIR=${MAX_FILES_PER_DIR:-2000}
MAX_DEPTH=${MAX_DEPTH:-6}
SIZE_SIGMA=${SIZE_SIGMA:-2.0}
SIZE_MIN_BYTES=${SIZE_MIN_BYTES:-1024}
SIZE_MAX_BYTES=${SIZE_MAX_BYTES:-$((64 * 1024 * 1024 * 1024))}
UID_BASE=${UID_BASE:-100000}
GID_BASE=${GID_BASE:-100000}
HOME_CHOWN=${HOME_CHOWN:-1}
STAMP_TIMES=${STAMP_TIMES:-1}
AGE_MAX_DAYS=${AGE_MAX_DAYS:-1825}
CREATE_JOBS=${CREATE_JOBS:-0}
SEED=${SEED:-}
DRY_RUN=${DRY_RUN:-0}

PHYS_BYTES_PER_FILE=${PHYS_BYTES_PER_FILE:-4608}
PHYS_BYTES_PER_DIR=${PHYS_BYTES_PER_DIR:-4096}
DISK_BUDGET_BYTES=${DISK_BUDGET_BYTES:-$((100 * 1024 * 1024 * 1024 * 1024))}

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required." >&2
  exit 1
fi

is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }
for v in USERS TOTAL_FILES AVG_FILES_PER_DIR MAX_FILES_PER_DIR MAX_DEPTH \
         SIZE_MIN_BYTES SIZE_MAX_BYTES UID_BASE GID_BASE AGE_MAX_DAYS \
         CREATE_JOBS PHYS_BYTES_PER_FILE PHYS_BYTES_PER_DIR DISK_BUDGET_BYTES; do
  if ! is_uint "${!v}"; then
    echo "ERROR: $v must be a non-negative integer (got '${!v}')." >&2
    exit 2
  fi
done

if [[ "$USERS" -lt 1 ]]; then echo "ERROR: USERS must be >= 1." >&2; exit 2; fi
if [[ "$TOTAL_FILES" -lt "$USERS" ]]; then
  echo "ERROR: TOTAL_FILES ($TOTAL_FILES) must be >= USERS ($USERS)." >&2
  exit 2
fi
if [[ "$AVG_FILES_PER_DIR" -lt 1 ]]; then echo "ERROR: AVG_FILES_PER_DIR must be >= 1." >&2; exit 2; fi
if [[ "$MAX_FILES_PER_DIR" -lt 1 || "$MAX_FILES_PER_DIR" -gt 8191 ]]; then
  echo "ERROR: MAX_FILES_PER_DIR must be 1..8191 (< ereport Dense threshold 8192)." >&2
  exit 2
fi
if [[ "$MAX_DEPTH" -lt 1 || "$MAX_DEPTH" -gt 32 ]]; then echo "ERROR: MAX_DEPTH must be 1..32." >&2; exit 2; fi
if [[ "$SIZE_MIN_BYTES" -lt 1 || "$SIZE_MAX_BYTES" -lt "$SIZE_MIN_BYTES" ]]; then
  echo "ERROR: need 1 <= SIZE_MIN_BYTES <= SIZE_MAX_BYTES." >&2
  exit 2
fi
if ! TARGET_APPARENT_PB="$TARGET_APPARENT_PB" python3 -c 'import os,sys
try: x=float(os.environ["TARGET_APPARENT_PB"])
except Exception: sys.exit(1)
sys.exit(0 if x>=0 else 1)'; then
  echo "ERROR: TARGET_APPARENT_PB must be a number >= 0 (got '$TARGET_APPARENT_PB')." >&2
  exit 2
fi
if [[ -n "$SEED" ]] && ! is_uint "$SEED"; then echo "ERROR: SEED must be an integer." >&2; exit 2; fi

# Physical (real flash) estimate — sparse logical bytes are intentionally excluded.
leaf_dirs=$(((TOTAL_FILES + AVG_FILES_PER_DIR - 1) / AVG_FILES_PER_DIR))
est_dirs=$((leaf_dirs * 16 / 10 + USERS * 4))
phys_bytes=$((TOTAL_FILES * PHYS_BYTES_PER_FILE + est_dirs * PHYS_BYTES_PER_DIR))
est_inodes=$((TOTAL_FILES + est_dirs))
avg_files_user=$((TOTAL_FILES / USERS))

human() {
  local b=$1
  if command -v numfmt >/dev/null 2>&1; then numfmt --to=iec-i --suffix=B "$b" 2>/dev/null && return; fi
  if ((b >= 1099511627776)); then printf '~%d TiB\n' $((b / 1099511627776));
  elif ((b >= 1073741824)); then printf '~%d GiB\n' $((b / 1073741824));
  else printf '%d B\n' "$b"; fi
}

uw=${#USERS}

echo "Home-storage simulation plan"
echo "  output root:        $ROOT"
echo "  users:              $USERS  ($ROOT/$HOME_PARENT/$(printf "%0${uw}d" 1) … $(printf "%0${uw}d" "$USERS"))"
echo "  total files:        $TOTAL_FILES  (~$avg_files_user / user avg, lognormal sigma=$USER_FILE_SIGMA)"
echo "  per-dir files:      ~$AVG_FILES_PER_DIR avg, ≤$MAX_FILES_PER_DIR; subdir depth ≤$MAX_DEPTH"
echo "  ownership:          uid/gid = ${UID_BASE}.. (per user); HOME_CHOWN=$HOME_CHOWN"
echo "  apparent capacity:  ~${TARGET_APPARENT_PB} PB target (sparse logical; size sigma=$SIZE_SIGMA, clamp $(human "$SIZE_MIN_BYTES")..$(human "$SIZE_MAX_BYTES"))"
echo "  estimated dirs:     ~$est_dirs   estimated inodes: ~$est_inodes"
echo "  PHYSICAL flash est: $(human "$phys_bytes")  (budget $(human "$DISK_BUDGET_BYTES"))"
echo "  time stamps:        STAMP_TIMES=$STAMP_TIMES (ages 0..${AGE_MAX_DAYS}d)"
[[ -n "$SEED" ]] && echo "  seed:               $SEED"

if [[ "$phys_bytes" -gt "$DISK_BUDGET_BYTES" ]]; then
  echo "ERROR: estimated PHYSICAL footprint $(human "$phys_bytes") exceeds DISK_BUDGET_BYTES $(human "$DISK_BUDGET_BYTES")." >&2
  echo "  (Apparent/sparse PB is not counted here.) Lower TOTAL_FILES, raise DISK_BUDGET_BYTES, or check PHYS_BYTES_PER_FILE." >&2
  exit 1
fi

if [[ "$DRY_RUN" == "1" ]]; then
  echo ""
  echo "DRY_RUN=1: no files created. Re-run without DRY_RUN to build."
  exit 0
fi

if [[ "$TOTAL_FILES" -ge 100000000 ]]; then
  echo "WARNING: $TOTAL_FILES files is a multi-hour job and ~$est_inodes inodes — verify 'df -i $ROOT' has headroom (XFS dynamic-inode preferred)." >&2
fi

mkdir -p "$ROOT"

HS_ROOT="$ROOT" \
HS_HOME_PARENT="$HOME_PARENT" \
HS_USERS="$USERS" \
HS_TOTAL_FILES="$TOTAL_FILES" \
HS_TARGET_APPARENT_PB="$TARGET_APPARENT_PB" \
HS_USER_FILE_SIGMA="$USER_FILE_SIGMA" \
HS_AVG_FILES_PER_DIR="$AVG_FILES_PER_DIR" \
HS_MAX_FILES_PER_DIR="$MAX_FILES_PER_DIR" \
HS_MAX_DEPTH="$MAX_DEPTH" \
HS_SIZE_SIGMA="$SIZE_SIGMA" \
HS_SIZE_MIN_BYTES="$SIZE_MIN_BYTES" \
HS_SIZE_MAX_BYTES="$SIZE_MAX_BYTES" \
HS_UID_BASE="$UID_BASE" \
HS_GID_BASE="$GID_BASE" \
HS_HOME_CHOWN="$HOME_CHOWN" \
HS_STAMP_TIMES="$STAMP_TIMES" \
HS_AGE_MAX_DAYS="$AGE_MAX_DAYS" \
HS_CREATE_JOBS="$CREATE_JOBS" \
HS_SEED="$SEED" \
  python3 <<'PY'
import concurrent.futures as cf
import math
import os
import random
import sys
import threading
import time

E = os.environ
ROOT = E["HS_ROOT"]
HOME_PARENT = E["HS_HOME_PARENT"]
USERS = int(E["HS_USERS"])
TOTAL_FILES = int(E["HS_TOTAL_FILES"])
TARGET_PB = float(E["HS_TARGET_APPARENT_PB"])
USER_FILE_SIGMA = float(E["HS_USER_FILE_SIGMA"])
AVG_FPD = int(E["HS_AVG_FILES_PER_DIR"])
MAX_FPD = int(E["HS_MAX_FILES_PER_DIR"])
MAX_DEPTH = int(E["HS_MAX_DEPTH"])
SIZE_SIGMA = float(E["HS_SIZE_SIGMA"])
SIZE_MIN = int(E["HS_SIZE_MIN_BYTES"])
SIZE_MAX = int(E["HS_SIZE_MAX_BYTES"])
UID_BASE = int(E["HS_UID_BASE"])
GID_BASE = int(E["HS_GID_BASE"])
HOME_CHOWN = E["HS_HOME_CHOWN"].strip() == "1"
STAMP_TIMES = E["HS_STAMP_TIMES"].strip() == "1"
AGE_MAX_DAYS = int(E["HS_AGE_MAX_DAYS"])
JOBS_CLI = int(E["HS_CREATE_JOBS"])
_seed_raw = E.get("HS_SEED", "").strip()

SEED = int(_seed_raw) if _seed_raw else random.SystemRandom().randrange(1 << 62)
print(f"  seed (use SEED={SEED} to reproduce)", file=sys.stderr)

home_root = os.path.join(ROOT, HOME_PARENT)
os.makedirs(home_root, exist_ok=True)

# Per-file mean logical size to hit the apparent target.
apparent_target = int(TARGET_PB * 1e15)
mean_bytes = (apparent_target / TOTAL_FILES) if (TARGET_PB > 0 and TOTAL_FILES > 0) else 0.0
size_mu = math.log(mean_bytes) - (SIZE_SIGMA * SIZE_SIGMA) / 2.0 if mean_bytes > 0 else 0.0
sized = mean_bytes > 0

TOP_NAMES = [
    "projects", "data", "Documents", "Downloads", "scratch", "results",
    "archive", "src", "work", "datasets", "experiments", "backup", "tmp",
    ".cache", ".local", ".config", "notes", "papers", "code", "logs",
]
SUB_NAMES = [
    "run", "set", "batch", "part", "sample", "seq", "img", "raw", "proc",
    "out", "stage", "grp", "blk", "node", "shard", "chunk", "sub", "dir",
    "v", "build", "case", "trial", "session", "lane",
]


def worker_cap(cli: int) -> int:
    c = os.cpu_count() or 8
    if cli <= 0:
        return min(64, max(8, c * 2))
    return max(1, cli)


JW = worker_cap(JOBS_CLI)
UW = len(str(USERS))
NOW = time.time()


# Per-user file counts: lognormal weights normalized to exactly TOTAL_FILES.
def make_counts() -> list:
    rg = random.Random(SEED ^ 0xA5A5A5A5)
    if USER_FILE_SIGMA <= 0:
        counts = [TOTAL_FILES // USERS] * USERS
    else:
        w = [rg.lognormvariate(0.0, USER_FILE_SIGMA) for _ in range(USERS)]
        s = sum(w) or 1.0
        counts = [max(0, round(x / s * TOTAL_FILES)) for x in w]
    diff = TOTAL_FILES - sum(counts)
    i = 0
    while diff != 0:
        j = i % USERS
        if diff > 0:
            counts[j] += 1
            diff -= 1
        elif counts[j] > 0:
            counts[j] -= 1
            diff += 1
        i += 1
    return counts


COUNTS = make_counts()

if HOME_CHOWN:
    probe = os.path.join(home_root, f".hs_chown_probe_{os.getpid()}")
    ok = False
    try:
        fd = os.open(probe, os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o600)
        os.close(fd)
        os.chown(probe, UID_BASE, GID_BASE)
        ok = True
    except OSError as e:
        print(
            f"ERROR: HOME_CHOWN=1 but cannot chown in {home_root}: {e}\n"
            "  Run as root / with CAP_CHOWN for per-user ownership, or set HOME_CHOWN=0 "
            "to build a single-owner tree on purpose.",
            file=sys.stderr,
        )
    finally:
        try:
            os.unlink(probe)
        except OSError:
            pass
    if not ok:
        raise SystemExit(3)

_prog_lock = threading.Lock()
_done_files = 0
_done_users = 0
blob_cache = {}


def sample_size(rng) -> int:
    v = int(rng.lognormvariate(size_mu, SIZE_SIGMA))
    if v < SIZE_MIN:
        v = SIZE_MIN
    elif v > SIZE_MAX:
        v = SIZE_MAX
    return v


def write_file(path: str, sz: int) -> None:
    # Sparse: one tail byte allocates a single block; st_size reports full logical size.
    with open(path, "wb") as f:
        if sz > 0:
            f.seek(sz - 1)
            f.write(b"\0")


def sub_component(rng, level: int) -> str:
    span = 16 if level == 1 else 8
    return f"{rng.choice(SUB_NAMES)}{rng.randint(0, span - 1)}"


def build_user(idx: int) -> int:
    global _done_files, _done_users
    rng = random.Random((SEED ^ ((idx + 1) * 2654435761)) & ((1 << 62) - 1))
    uid = UID_BASE + idx
    gid = GID_BASE + idx
    home = os.path.join(home_root, f"{idx + 1:0{UW}d}")
    os.makedirs(home, exist_ok=True)
    if HOME_CHOWN:
        try:
            os.chown(home, uid, gid)
        except OSError:
            pass

    n = COUNTS[idx]
    made = 0
    made_dirs = {home}
    fpd_mu = math.log(AVG_FPD) if AVG_FPD > 0 else 0.0

    while made < n:
        depth = rng.randint(1, MAX_DEPTH)
        comps = [rng.choice(TOP_NAMES)]
        for lvl in range(1, depth):
            comps.append(sub_component(rng, lvl))

        cur = home
        for c in comps:
            cur = os.path.join(cur, c)
            if cur not in made_dirs:
                try:
                    os.mkdir(cur)
                    if HOME_CHOWN:
                        os.chown(cur, uid, gid)
                except FileExistsError:
                    pass
                except OSError:
                    pass
                made_dirs.add(cur)
        leaf = cur

        k = int(rng.lognormvariate(fpd_mu, 0.7)) if AVG_FPD > 0 else 1
        if k < 1:
            k = 1
        if k > MAX_FPD:
            k = MAX_FPD
        if k > n - made:
            k = n - made

        for j in range(k):
            p = os.path.join(leaf, f"f{made + j:08d}")
            sz = sample_size(rng) if sized else 0
            try:
                write_file(p, sz)
                if STAMP_TIMES:
                    ts = NOW - rng.random() * AGE_MAX_DAYS * 86400.0
                    os.utime(p, (ts, ts))
                if HOME_CHOWN:
                    os.chown(p, uid, gid)
            except OSError as e:
                print(f"  warn: {p}: {e}", file=sys.stderr)
        made += k

    with _prog_lock:
        _done_files += made
        _done_users += 1
        if _done_users % 100 == 0 or _done_users == USERS:
            pct = 100.0 * _done_files / TOTAL_FILES if TOTAL_FILES else 100.0
            print(
                f"  ... users {_done_users}/{USERS}, files {_done_files}/{TOTAL_FILES} ({pct:.1f}%)",
                file=sys.stderr,
            )
    return made


print(
    f"Building {USERS} home dirs under {home_root} "
    f"(workers≈{JW}, CREATE_JOBS={'auto' if JOBS_CLI <= 0 else JOBS_CLI})...",
    file=sys.stderr,
)
if sized:
    print(
        f"  per-file logical size: lognormal mean≈{mean_bytes/1e6:.2f} MB "
        f"(target apparent ≈{TARGET_PB} PB across {TOTAL_FILES} files)",
        file=sys.stderr,
    )

t0 = time.time()
if JW <= 1 or USERS < 2:
    for u in range(USERS):
        build_user(u)
else:
    with cf.ThreadPoolExecutor(max_workers=JW) as ex:
        list(ex.map(build_user, range(USERS)))
dt = time.time() - t0
rate = (_done_files / dt) if dt > 0 else 0.0
print(
    f"Done: {_done_files} files, {USERS} users in {dt:.0f}s ({rate:,.0f} files/s).",
    file=sys.stderr,
)
PY

cat <<EOF

Done.

Layout:
  $ROOT/$HOME_PARENT/$(printf "%0${uw}d" 1) … $(printf "%0${uw}d" "$USERS")  — one home per user
  each home: nested subdirs (depth ≤$MAX_DEPTH, ≤$MAX_FILES_PER_DIR files/dir),
  files owned by uid/gid ${UID_BASE}.. , sparse logical sizes (apparent ~${TARGET_APPARENT_PB} PB).

Crawl + report:
  ecrawl $ROOT /tmp/home_bin
  ereport --bucket-details 2 --report-dir /tmp/home_report /tmp/home_bin     # all 6000 users
  ereport --report-dir /tmp/home_report_u1 /tmp/home_bin $((UID_BASE))        # single user 0001

Notes:
  - Physical flash use is metadata + ~1 block/file; sparse logical (st_size) is what
    ecrawl/ereport sum, so 'df -h' stays small while reports show PB-class capacity.
  - 'df -i $ROOT' must have room for ~$est_inodes inodes.
EOF
