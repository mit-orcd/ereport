#!/usr/bin/env bash
#
# One command to run the indexer comparison, and one to remove everything it
# created. Wraps init.sh, mariadb.sh, prepare-synth.sh and run_smoke.sh so a
# full run is a single invocation instead of a five-step sequence.
#
# Usage:
#   benchmark.sh --do     <benchmark-tree> [options]
#   benchmark.sh --undo   <benchmark-tree> [options]
#   benchmark.sh --adopt  <benchmark-tree>
#   benchmark.sh --charts <results-dir>
#
# --adopt records an existing tree built by prepare-synth.sh directly, so that
# --undo will tear it down. It requires the tree to carry prepare-synth.sh's
# QUERY_SEEDS.txt and query_seeds/ fingerprint.
#
# --charts redraws the presentation layer of a finished run and nothing else:
# it rereads the CSVs already in <results-dir> and rewrites SUMMARY_TABLE.txt,
# FAILURES.txt and charts/ (PNG and PDF). Nothing is crawled, indexed or
# queried, so it takes seconds and needs neither the benchmark tree nor the
# external tools -- which is the point, since a measured run costs hours and
# the figures get revised far more often than the numbers behind them.
#
# Options:
#   --small           fast test cycle instead of a benchmark: rebuild this repo's
#                     binaries, generate a ~2.5k-entry tree, and compare against
#                     find and du only. No external indexers, no MariaDB, no
#                     package installs, one repetition, ~30 seconds end to end.
#                     For checking that a code change still answers Q1-Q6; the
#                     timings mean nothing at that size and say so.
#   --smoke           --small's tiny tree, but with every installed external
#                     indexer running against it, MariaDB included when
#                     Robinhood is present. Rebuilds the suite and the tool
#                     prefix, one repetition, warm caches, results/smoke. This
#                     is how GUFI's config and modules, Robinhood's schema and
#                     XDU's size unit get checked without spending an evening on
#                     a real tree. Not a measurement.
#   --quick           a full run with the external indexers, but one repetition
#                     and no GUFI rollup variant: minutes instead of hours, for
#                     checking that every tool answers Q1-Q6 correctly before
#                     spending a night on the measured run. Results land in
#                     results/quick. Timings are real but single-sample.
#   --work <dir>      scratch storage for the indexes every tool builds and for
#                     TMPDIR. This is the bulk of what a run writes -- GUFI
#                     alone creates a database per directory -- so put it on the
#                     big filesystem, not in $HOME. Defaults to <benchmark-tree>-work.
#   --results <dir>   where the CSVs, summary and charts go (small, and kept by
#                     --undo). Defaults to a timestamped directory next to this
#                     script, or results/small with --small, which is reused so
#                     the path stays the same from cycle to cycle.
#   --reps <spec>     how many times each tool runs. A bare number sets them
#                     all ("--reps 1"); "<tool>=<n>" sets one; the two combine,
#                     comma or space separated, and later entries win:
#                       --reps 1                      every tool once
#                       --reps gufi=1,robinhood=1     those two once, rest at REPS
#                       --reps 3,gufi=1               spell the default too
#                     Tools differ by orders of magnitude -- a GUFI rollup is 29
#                     minutes a repetition where find is seconds -- so this is
#                     how the cheap rows get error bars without paying for three
#                     of everything. A bare number counts as an explicit REPS,
#                     so --small, --quick and --smoke will not override it; a
#                     <tool>=<n> entry leaves those mode defaults alone.
#   --yes, -y         answer every prompt with yes; required off a terminal
#   --no-robinhood    skip MariaDB and Robinhood entirely
#   --keep-tools      --undo: keep the built tools and their source clones
#   --purge-results   --undo: also delete the results directories
#
# So a run touches three paths: the tree being crawled, the scratch it writes
# indexes to, and the results. --undo deletes the first two and keeps the third.
# All three are created if they do not exist, parents included; step 0 prints
# the filesystem each one landed on, which is where an unmounted mount point
# shows itself.
#
# Both actions are re-runnable: --do skips work that is already done, so it
# doubles as the way to resume an interrupted setup.
#
# --undo deletes the benchmark tree and the scratch with this repo's edelete
# ('--delete --force', EDELETE_THREADS threads) when it is built and runnable,
# because rm -rf takes hours on a tree of tens of millions of files. It falls
# back to rm -rf, and always uses rm -rf for the small paths. EDELETE_BIN
# overrides the binary.
#
# Env (all optional, passed through to the underlying scripts):
#   PREFIX, SRC_ROOT, JOBS, TOOLS, PKG_ARGS, INSTALL_PACKAGES, SETUP_CHARTS,
#   WORK_ROOT, RESULTS_ROOT, SYNTH_PROFILE, REPS, REPS_<TOOL>, CACHE_MODES,
#   DROP_CACHES, DROP_CACHES_SCOPE, DROP_DB_CACHE, ARG_SETS, KEEP_ALL_INDEXES,
#   TOOLS_INDEX, TOOLS_QUERY, RBH_DB_DATADIR, EDELETE_BIN, EDELETE_THREADS
#
# REPS_<TOOL> is the env spelling of --reps: REPS_GUFI=1 and --reps gufi=1 do
# the same thing. REPS_EREPORT_INDEX follows REPS so crawl and index stay a
# pair; REPS_ECRAWL is still its cap.
#
# WORK_ROOT and RESULTS_ROOT are the env spellings of --work and --results;
# WORK_ROOT names the index directory itself, --work names a parent that also
# gets a tmp/ for TMPDIR.
#
# Every default --small changes can still be overridden by setting the variable:
# SYNTH_PROFILE, REPS, TOOLS, DROP_CACHES, CACHE_MODES and INSTALL_PACKAGES are
# only defaulted when they are unset.
#
# CACHE_MODES is "cold hot" for a measured run: each tool finishes all its cold
# reps (drop, then the whole pipeline) before any hot rep, so the last cold run
# is what warms the hot series. --small, --smoke and --quick cut back to one
# pass, because they check answers rather than measure them.
#
# RBH_DB_DATADIR puts MariaDB's tables on the storage under test, defaulting to
# <work>/mariadb, so Robinhood's index is not the only one on the OS disk.
#
# REPS defaults to 3 here (run_smoke.sh alone defaults to 1) because the charts
# need repetitions to show error bars. DROP_CACHES defaults to 1 when running as
# root and 0 otherwise. DO_NOWRITE defaults to 1 so ecrawl's stat-walk row sits
# beside du/dua/dut; DO_NOSTAT defaults to 1 so the names-only row sits beside
# find/fd. Each adds one extra traversal per rep; set either to 0 to skip.
# Cold write / --no-write crawls pass --iouring (ECRAWL_COLD_IOURING=1); hot
# stays on fstatat. That is the measured inode-read path, not an extra row.
# DO_STATX / DO_IOURING stay 0: they add write+nowrite variants that measure
# --statx / --iouring as their own CSV rows, which a default run does not need.
# On dnf hosts PKG_ARGS defaults to --disableplugin=etckeeper so a site hook
# cannot stall the install on a password prompt; set PKG_ARGS to override.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
# Prefer the shared ORCD scratch prefix when it already exists so a root or
# login-node run does not quietly rebuild under $HOME/.local/indexer-compare
# (and then fail probes against a glibc that does not match the compute node).
_ic_scratch_prefix="$HOME/orcd/scratch/ereport-automated-testing/prefix"
_ic_scratch_src="$HOME/orcd/scratch/ereport-automated-testing/src"
if [[ -z "${PREFIX:-}" ]]; then
  if [[ -d "$_ic_scratch_prefix" ]]; then
    PREFIX=$_ic_scratch_prefix
  else
    PREFIX="$HOME/.local/indexer-compare"
  fi
fi
if [[ -z "${SRC_ROOT:-}" ]]; then
  if [[ -d "$_ic_scratch_src" ]]; then
    SRC_ROOT=$_ic_scratch_src
  elif [[ -d "$_ic_scratch_prefix" ]]; then
    SRC_ROOT=$_ic_scratch_src
  else
    SRC_ROOT="$HOME/.cache/indexer-compare-src"
  fi
fi
unset _ic_scratch_prefix _ic_scratch_src
# Which knobs the caller set, recorded before anything is defaulted, so --small
# can supply its own defaults without overriding an explicit choice.
FROM_ENV_TOOLS=${TOOLS+1}
FROM_ENV_REPS=${REPS+1}
FROM_ENV_PROFILE=${SYNTH_PROFILE+1}
FROM_ENV_DROP=${DROP_CACHES+1}
FROM_ENV_CACHE_MODES=${CACHE_MODES+1}
FROM_ENV_INSTALL=${INSTALL_PACKAGES+1}
# An explicit empty TOOLS means "install nothing external", so honour it rather
# than falling back to the default set.
TOOLS=${TOOLS-"gufi xdu robinhood dua dut"}
REPS=${REPS:-3}
# Cold and hot both get measured; the correctness modes below drop back to one
# pass. Defaulted here as well as in lib.sh so this script can report it.
CACHE_MODES=${CACHE_MODES:-"cold hot"}
# The per-tool thread budget, shared with lib.sh's default and baked into the
# Robinhood config by mariadb.sh, which is why it is set here rather than left
# to each script.
THREADS=${THREADS:-16}
export THREADS

# Records what a --do run created, so --undo never guesses. Living inside the
# tree also means --undo refuses to delete a tree this script did not build.
STATE_NAME=.indexer-compare-run

# Every tool here outruns the stock 1024 open files: ereport_index keeps
# trigram writers x LRU shards open, GUFI opens a database per directory, and
# ecrawl holds per-uid shards per writer. Raised here, in the parent, so the
# whole run inherits one limit. lib.sh repeats this for the step-by-step
# scripts, which are not started from here.
NOFILE_TARGET=${NOFILE_TARGET:-131072}
export NOFILE_TARGET
raise_nofile() {
  local soft hard
  soft=$(ulimit -Sn)
  hard=$(ulimit -Hn)
  [[ "$soft" != unlimited ]] || return 0
  if [[ "$hard" != unlimited ]] && ((hard < NOFILE_TARGET)); then
    ulimit -Hn "$NOFILE_TARGET" 2>/dev/null || true
    hard=$(ulimit -Hn)
  fi
  local target=$NOFILE_TARGET
  [[ "$hard" == unlimited ]] || ((target <= hard)) || target=$hard
  ((target <= soft)) || ulimit -Sn "$target" 2>/dev/null || true
}

ACTION=""
TREE=""
WORK_DIR=""
RESULTS_DIR=${RESULTS_ROOT:-}
ASSUME_YES=0
WITH_ROBINHOOD=1
KEEP_TOOLS=0
PURGE_RESULTS=0
SMALL=0
SMOKE=0
QUICK=0
# The suite under test. Built by --small so a cycle always measures the working
# tree rather than whatever was compiled last; edelete is included because
# --undo uses it.
SUITE_TARGETS="ecrawl ereport ereport_index ecrawl_query edelete"

log() { printf '\n=== %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  # The whole header block, so a new option is documented in one place only.
  awk 'NR < 3 {next} /^[^#]/ {exit} {sub(/^#[ ]?/, ""); print}' "${BASH_SOURCE[0]}"
}

# Mirrors lib.sh's KNOWN_REPS_TOOLS. Repeated rather than sourced because this
# script deliberately does not source lib.sh, and a typo has to be caught here:
# rejected at the command line it costs a retype, accepted it costs a whole run
# at a repetition count nobody chose.
REPS_TOOL_NAMES="ecrawl ecrawl_suite suite ereport_index gufi xdu robinhood find fd du dua dut"

reps_valid() { [[ "$1" =~ ^[0-9]+$ ]] && (($1 >= 1)); }

# --reps <spec>: "1", "gufi=1", or "3,gufi=1,robinhood=1". A bare number is the
# default for every tool (REPS itself); <tool>=<n> overrides one, exported as
# REPS_<TOOL> because that is what the run scripts read.
parse_reps_spec() {
  local spec=${1//,/ } entry tool value known ok
  [[ -n "${spec// /}" ]] ||
    die "--reps needs a value: a count, <tool>=<count>, or both"
  for entry in $spec; do
    if [[ "$entry" != *=* ]]; then
      reps_valid "$entry" ||
        die "--reps $entry: expected a count of at least 1, or <tool>=<count>"
      REPS=$entry
      # Only a bare number pins the global, so "--smoke --reps gufi=1" still
      # gets smoke's REPS=1 for everything else.
      FROM_ENV_REPS=1
      continue
    fi
    tool=$(printf '%s' "${entry%%=*}" | tr '[:upper:]-' '[:lower:]_')
    value=${entry#*=}
    ok=0
    for known in $REPS_TOOL_NAMES; do
      [[ "$tool" == "$known" ]] && ok=1 && break
    done
    ((ok)) || die "$(
      printf -- '--reps: no tool called %s\n' "$tool"
      printf '       known tools: %s' "$REPS_TOOL_NAMES"
    )"
    reps_valid "$value" ||
      die "--reps $entry: the count must be a whole number of at least 1"
    export "$(printf 'REPS_%s' "$(printf '%s' "$tool" | tr '[:lower:]' '[:upper:]')")=$value"
  done
}

# "3", or "3 (gufi 1, robinhood 1)" when tools disagree. ereport_index is absent
# unless asked for explicitly: its default of 1 lives in lib.sh, and run_index.sh
# prints the full plan including it once the tool list is settled.
reps_summary() {
  local var tool out=""
  for var in $(compgen -v | grep '^REPS_' || true); do
    [[ "$var" != REPS_TOOL_NAMES ]] || continue
    [[ -n "${!var}" && "${!var}" != "$REPS" ]] || continue
    tool=$(printf '%s' "${var#REPS_}" | tr '[:upper:]' '[:lower:]')
    out+="${out:+, }$tool ${!var}"
  done
  printf '%s%s' "$REPS" "${out:+ ($out)}"
}

confirm() {
  local prompt=$1
  [[ "$ASSUME_YES" == "0" ]] || return 0
  [[ -t 0 ]] || die "$prompt -- refusing to assume an answer; rerun with --yes"
  local reply
  read -r -p "$prompt [y/N] " reply || reply=n
  [[ "$reply" == [Yy]* ]]
}

# Absolute paths only, and never a top-level directory. Checked while the plan
# is printed so that a bad path cannot abort the teardown halfway through.
check_removable() {
  local path=${1%/} what=$2
  [[ -n "$path" ]] || return 0
  [[ "$path" == /* ]] || die "refusing to remove relative path '$path' ($what)"
  [[ "$path" == */*/* ]] ||
    die "refusing to remove top-level path '$path' ($what); remove it by hand if that is really intended"
}

# The trees this builds are tens of millions of entries, which single-threaded
# rm -rf unlinks for hours. edelete is the parallel walker from this repo and
# ends the same way: files unlinked, then empty directories removed bottom-up,
# including the start directory.
EDELETE_BIN=${EDELETE_BIN:-"$SCRIPT_DIR/../../edelete"}
EDELETE_OK=""
edelete_usable() {
  if [[ -z "$EDELETE_OK" ]]; then
    EDELETE_OK=0
    if [[ -x "$EDELETE_BIN" ]]; then
      # Linked against jemalloc it can be present and still fail to start, and
      # a teardown is the wrong place to find that out. A bare invocation is a
      # usage error (2) and touches nothing; 126/127 is the loader failing.
      "$EDELETE_BIN" >/dev/null 2>&1
      local rc=$?
      ((rc == 126 || rc == 127)) || EDELETE_OK=1
    fi
  fi
  [[ "$EDELETE_OK" == "1" ]]
}

# mode=bulk uses edelete on directories: the benchmark tree and the index
# scratch run to tens of millions of entries, which single-threaded rm -rf
# unlinks for hours. edelete walks in parallel and finishes the same way --
# files unlinked, then empty directories removed bottom-up, start directory
# included. Everything else stays on rm -rf, which is quieter and plenty for a
# few thousand files.
safe_rm() {
  local path=${1%/} what=$2 mode=${3:-quick}
  [[ -n "$path" ]] || return 0
  check_removable "$path" "$what"
  if [[ ! -e "$path" ]]; then
    info "already gone: $path"
    return 0
  fi
  info "removing $what: $path"
  if [[ "$mode" == "bulk" && -d "$path" ]] && edelete_usable; then
    # Progress goes to the terminal on purpose: this is the slowest step, and a
    # silent hour looks like a hang.
    if EDELETE_THREADS=${EDELETE_THREADS:-$THREADS} \
      "$EDELETE_BIN" --delete --force "$path"; then
      [[ -e "$path" ]] || return 0
      info "edelete left $path behind; finishing with rm -rf"
    else
      info "edelete failed on $path; falling back to rm -rf"
    fi
  fi
  rm -rf -- "$path"
}

# Mount point and filesystem type, so a path that landed somewhere unintended
# says so instead of being discovered at the end of a long run.
fs_note() {
  local path=$1 mount type
  mount=$(df -P "$path" 2>/dev/null | awk 'NR==2 {print $6}')
  type=$(stat -f -c %T "$path" 2>/dev/null)
  [[ -z "$mount" ]] || printf '%s on %s' "${type:-fs}" "$mount"
}

# Create the whole path. The risk this used to refuse over -- a mount point that
# is not mounted, so the run silently fills the root filesystem instead of the
# array -- is real, but refusing only caught it when the parent was missing too.
# Reporting the filesystem each path actually landed on catches it either way.
ensure_tree_dir() {
  local tree=$1
  [[ -n "$tree" ]] || die "a benchmark tree path is required"
  [[ ! -d "$tree" ]] || return 0
  local abs=$tree existing levels=0
  [[ "$abs" == /* ]] || abs="$PWD/$abs"
  existing=$abs
  while [[ ! -d "$existing" && "$existing" == /?* ]]; do
    existing=$(dirname "$existing")
    levels=$((levels + 1))
  done
  mkdir -p "$tree" ||
    die "cannot create the benchmark tree $tree; check permissions and the mount point"
  if ((levels > 1)); then
    info "created $tree and $((levels - 1)) missing parent(s) under $existing"
  else
    info "created $tree"
  fi
}

# Anything written under the tree gets crawled by every tool on the next
# repetition, which corrupts the counts and can make a run grow without bound.
reject_inside_tree() {
  local path=${1%/} tree=${2%/} what=$3
  [[ "$path" != "$tree" && "$path" != "$tree"/* ]] ||
    die "$what ($path) is inside the benchmark tree; the tools would index their own output"
  [[ "$tree" != "$path"/* ]] ||
    die "the benchmark tree ($tree) is inside the $what ($path); --undo could not tell them apart"
}

# Bulk output goes here: index replicas plus TMPDIR. Sized for the tree, not for
# $HOME, which is where the results directory lives by default.
resolve_scratch() {
  local tree=$1 work
  if [[ -n "$WORK_DIR" ]]; then
    work=$WORK_DIR
  else
    # A sibling of the tree, so scratch lands on the filesystem the caller
    # already pointed at rather than on whatever $HOME happens to be.
    work="${tree%/}-work"
  fi
  [[ "$work" == /* ]] || work="$PWD/$work"
  mkdir -p "$work" ||
    die "cannot create the scratch directory $work; pass --work <dir> on a writable filesystem"
  work=$(cd "$work" && pwd)
  reject_inside_tree "$work" "$tree" "scratch directory"

  # WORK_ROOT set in the environment keeps its older, narrower meaning: the
  # index directory itself.
  SCRATCH_INDEXES=${WORK_ROOT:-"$work/indexes"}
  SCRATCH_TMP="$work/tmp"
  mkdir -p "$SCRATCH_INDEXES" "$SCRATCH_TMP"
  SCRATCH_INDEXES=$(cd "$SCRATCH_INDEXES" && pwd)
  SCRATCH_TMP=$(cd "$SCRATCH_TMP" && pwd)
  reject_inside_tree "$SCRATCH_INDEXES" "$tree" "index directory"
  SCRATCH_ROOT=$work
}

# Free space and free inodes, because GUFI writes one database per directory and
# runs out of inodes long before it runs out of bytes.
space_note() {
  local path=$1 bytes inodes
  bytes=$(df -Ph "$path" 2>/dev/null | awk 'NR==2 {print $4}')
  inodes=$(df -Pi "$path" 2>/dev/null | awk 'NR==2 {print $4}')
  [[ -z "$bytes" ]] || printf '%s free, %s free inodes' "$bytes" "${inodes:-?}"
}

want_robinhood() {
  [[ "$WITH_ROBINHOOD" == "1" ]] || return 1
  case " $TOOLS " in
    *" robinhood "*) return 0 ;;
    *) return 1 ;;
  esac
}

# Is Robinhood in a state where it can produce comparable numbers? Two ways it
# is not, both of which used to surface only as a wall of failed rows: the
# database does not answer, and the config points at a different tree (a scan
# walks fs_path from the config, not the path this script was given, so the
# counts would be normalised against the wrong file total). Prints a one-line
# verdict either way; the probe itself lives in lib.sh, so this is the same
# check the run scripts make.
rbh_status() {
  local tree=$1 reason configured schema
  reason=$(bash -c 'source "$1/lib.sh"; rbh_db_ready || printf "%s" "$RBH_READY_REASON"' \
    _ "$SCRIPT_DIR" 2>/dev/null) || reason="probe_failed"
  if [[ -n "$reason" ]]; then
    printf 'not usable (%s)' "$reason"
    return 1
  fi
  configured=$(sed -n 's/^[[:space:]]*fs_path[[:space:]]*=[[:space:]]*"\?\([^";]*\)"\?[[:space:]]*;.*/\1/p' \
    "${RBH_CONFIG:-/dev/null}" 2>/dev/null | sed -n '1p')
  if [[ -n "$configured" && "${configured%/}" != "${tree%/}" ]]; then
    printf 'configured for %s, not %s' "$configured" "$tree"
    return 1
  fi
  # Not fatal: the index step scans with --alter-db and builds the schema.
  schema=$(bash -c 'source "$1/lib.sh"; rbh_schema_ready && printf schema || printf "no schema yet"' \
    _ "$SCRIPT_DIR" 2>/dev/null) || schema="schema unknown"
  printf 'database reachable (%s), configured for %s' "$schema" "${configured:-$tree}"
}

# --small is a different question, not a smaller benchmark: does the code still
# answer Q1-Q6 correctly. So it drops everything that only matters for
# measurement -- the external indexers, their packages, MariaDB, repetitions,
# cold caches, the second cache pass -- and keeps the references that are always
# installed.
apply_small_defaults() {
  [[ -z "$FROM_ENV_PROFILE" ]] && SYNTH_PROFILE=tiny
  [[ -z "$FROM_ENV_REPS" ]] && REPS=1
  [[ -z "$FROM_ENV_TOOLS" ]] && TOOLS=""
  [[ -z "$FROM_ENV_DROP" ]] && DROP_CACHES=0
  # One pass, not two: a second pass exists to measure the cost of a warm cache,
  # and this mode is not measuring anything.
  [[ -z "$FROM_ENV_CACHE_MODES" ]] && CACHE_MODES=cold
  # Nothing here needs a system package, and a prompt would defeat the point.
  [[ -z "$FROM_ENV_INSTALL" ]] && INSTALL_PACKAGES=0
  export SYNTH_PROFILE INSTALL_PACKAGES
  WITH_ROBINHOOD=0
  RESULTS_DEFAULT="$SCRIPT_DIR/results/small"
}

# Checking that the external tools answer correctly needs one pass, not a
# benchmark. On a 4.4M-entry tree a full run spent 1.6 hours in the index phase
# and 91% of that in GUFI's rollup, three times over, for numbers this mode is
# not trying to measure. One repetition without the rollup variant leaves every
# tool building its index once, which is what a correctness check needs.
apply_quick_defaults() {
  [[ -z "$FROM_ENV_REPS" ]] && REPS=1
  [[ -z "$FROM_ENV_DROP" ]] && DROP_CACHES=0
  [[ -z "$FROM_ENV_CACHE_MODES" ]] && CACHE_MODES=cold
  [[ -n "${GUFI_DO_ROLLUP:-}" ]] || GUFI_DO_ROLLUP=0
  export GUFI_DO_ROLLUP
  RESULTS_DEFAULT="$SCRIPT_DIR/results/quick"
}

# --smoke is --small plus the external indexers: the tiny tree, but every tool
# that is installed runs against it. Nothing here is a measurement. It exists
# because the wiring each external tool needs -- GUFI's Python modules and
# config, Robinhood's database schema, XDU's size unit -- can only be checked by
# running them, and doing that on a real tree costs an evening. The tiny tree
# still carries three sparse 500 MB files and a 1.68 GB subtree, which is
# exactly what exposed XDU indexing allocated blocks.
apply_smoke_defaults() {
  [[ -z "$FROM_ENV_PROFILE" ]] && SYNTH_PROFILE=tiny
  [[ -z "$FROM_ENV_REPS" ]] && REPS=1
  [[ -z "$FROM_ENV_DROP" ]] && DROP_CACHES=0
  [[ -z "$FROM_ENV_CACHE_MODES" ]] && CACHE_MODES=cold
  WITH_EXTERNALS=1
  export SYNTH_PROFILE WITH_EXTERNALS
  RESULTS_DEFAULT="$SCRIPT_DIR/results/smoke"
}

# In --small mode this replaces init.sh; in --smoke it runs alongside it. Either
# way the suite has to match the source that is being tested.
build_suite() {
  local jobs=${JOBS:-$(nproc 2>/dev/null || echo 4)}
  info "make -j$jobs $SUITE_TARGETS"
  # shellcheck disable=SC2086
  make -C "$REPO_ROOT" -j"$jobs" $SUITE_TARGETS ||
    die "building the suite failed; fix the compile error and rerun"
  local missing=() target
  for target in $SUITE_TARGETS; do
    [[ -x "$REPO_ROOT/$target" ]] || missing+=("$target")
  done
  ((${#missing[@]} == 0)) || die "make reported success but ${missing[*]} is missing"
}

do_run() {
  local tree=$1
  ensure_tree_dir "$tree"
  tree=$(cd "$tree" && pwd)
  local started=$SECONDS

  resolve_scratch "$tree"
  # Robinhood's index is MariaDB's data directory, and it belongs on the storage
  # under test like every other tool's. Set before init.sh, which provisions the
  # database on a first run and would otherwise put the tables wherever the
  # package does -- the operating system's disk.
  if want_robinhood; then
    export RBH_DB_DATADIR="$SCRATCH_ROOT/mariadb"
  fi
  raise_nofile
  log "0/5 paths and limits"
  if [[ "$SMALL" == "1" ]]; then
    # The find/du-only tool list comes from the tiny profile in run_smoke.sh, so
    # a caller who overrode the profile gets the auto-detected list instead.
    if [[ "${SYNTH_PROFILE:-}" == "tiny" ]]; then
      info "mode:    --small (SYNTH_PROFILE=tiny, find and du only, no external indexers)"
    else
      info "mode:    --small, but SYNTH_PROFILE=$SYNTH_PROFILE was given: tree and tool list are not the small ones"
    fi
  fi
  if [[ "$QUICK" == "1" ]]; then
    info "mode:    --quick (all tools, REPS=$REPS, GUFI_DO_ROLLUP=$GUFI_DO_ROLLUP) -- correctness pass, single-sample timings"
  fi
  if [[ "$SMOKE" == "1" ]]; then
    info "mode:    --smoke (SYNTH_PROFILE=$SYNTH_PROFILE with every installed indexer) -- wiring check, the timings mean nothing"
  fi
  info "tree:    $tree  ($(fs_note "$tree"); $(space_note "$tree"))"
  info "scratch: $SCRATCH_ROOT  ($(fs_note "$SCRATCH_ROOT"); $(space_note "$SCRATCH_ROOT"))"
  info "  indexes -> $SCRATCH_INDEXES"
  info "  TMPDIR  -> $SCRATCH_TMP"
  [[ -z "${RBH_DB_DATADIR:-}" ]] || info "  mariadb -> $RBH_DB_DATADIR"
  info "threads: $THREADS per tool (every tool that can be told)"
  info "reps:    $(reps_summary)"
  info "cache:   $CACHE_MODES per repetition"
  # These modes each want a single repetition, but only when REPS was not set.
  # An exported REPS=3 left over from a measured run therefore made a "quick"
  # check take three times as long with nothing to say why.
  if [[ -n "$FROM_ENV_REPS" && "$REPS" != "1" ]] &&
    [[ "$SMALL$QUICK$SMOKE" == *1* ]]; then
    info "  this mode defaults to 1, but REPS=$REPS was given, so it stands"
    info "  pass --reps 1 (or unset REPS) for a single pass"
  fi
  local soft=$(ulimit -Sn)
  if [[ "$soft" != unlimited ]] && ((soft < NOFILE_TARGET)); then
    info "open files: $soft -- wanted $NOFILE_TARGET, capped by the hard limit $(ulimit -Hn)"
    info "  run as root, or raise nofile in /etc/security/limits.conf, to lift it"
  else
    info "open files: $soft"
  fi

  if [[ "$SMALL" == "1" ]]; then
    log "1/5 building the suite from $REPO_ROOT"
    build_suite
  else
    if [[ "$SMOKE" == "1" ]]; then
      log "1/5 building the suite from $REPO_ROOT and the tools into $PREFIX"
      # --smoke checks the external tools, and the fixes they need live in the
      # install: GUFI's config path is compiled in, and a dua from another host
      # has to be rebuilt here. So the prefix is rebuilt too, not just used.
      build_suite
    else
      log "1/5 building tools into $PREFIX"
    fi
    # The database has to exist before init.sh writes env.sh, because that is
    # where RBH_CONFIG and RBH_SCAN_ARGS come from: provisioning afterwards left
    # every Robinhood row running without a config. init.sh skips the
    # provisioning when a config for this tree is already there.
    local mariadb_env=(SETUP_MARIADB=0)
    if want_robinhood; then
      mariadb_env=(SETUP_MARIADB=1 RBH_FS_PATH="$tree")
    fi
    if ! env "${mariadb_env[@]}" PREFIX="$PREFIX" SRC_ROOT="$SRC_ROOT" TOOLS="$TOOLS" \
      "$SCRIPT_DIR/init.sh"; then
      die "init.sh failed; fix the reported dependency and rerun the same command"
    fi
  fi

  # Still sourced in --small mode: an earlier full run may have left a chart
  # interpreter here, and nothing else in it can hurt.
  # shellcheck source=/dev/null
  [[ -f "$PREFIX/env.sh" ]] && source "$PREFIX/env.sh"

  # Asked here, once env.sh has had its say on FD_BIN and DUA_BIN, because this
  # is the last moment it is cheap: a baseline that will not start is one line
  # now, or a column of skipped rows noticed after the run is already spent.
  # Same subshell trick as rbh_status; env.sh exports both paths, so the probe
  # sees what the run scripts will see.
  local health health_line
  health=$(bash -c 'source "$1/lib.sh"; baseline_health_report' _ "$SCRIPT_DIR" 2>/dev/null) ||
    health=""
  if [[ -n "$health" ]]; then
    while IFS= read -r health_line; do
      info "$health_line"
    done <<<"$health"
    info "  those rows will be skipped; the other tools are unaffected"
  fi

  # The generated config is the completion artifact: the ownership marker is
  # written early in setup, so it says "claimed", not "finished".
  local rbh_config="$PREFIX/etc/robinhood.d/indexer-compare.conf"
  if want_robinhood; then
    if [[ ! -f "$rbh_config" ]]; then
      # init.sh normally does this; this covers a prefix built before --do
      # learned to ask for it.
      log "2/5 provisioning MariaDB and the Robinhood config for $tree"
      PREFIX="$PREFIX" THREADS="$THREADS" RBH_DB_DATADIR="$RBH_DB_DATADIR" \
        "$SCRIPT_DIR/mariadb.sh" setup "$tree"
      export RBH_CONFIG="$rbh_config"
      export RBH_SCAN_ARGS="-f $rbh_config --scan --once --alter-db"
      export RBH_AUTO_RESET=1
    else
      log "2/5 checking the Robinhood database"
      # A database provisioned before setup learned to create the schema has
      # tables only if some earlier run scanned with --alter-db. Asking for it
      # here is a no-op when they are already there.
      PREFIX="$PREFIX" RBH_CONFIG="$rbh_config" "$SCRIPT_DIR/mariadb.sh" schema ||
        info "  could not create the Robinhood schema; the index step will try again"
      # Asked on every run, not only when provisioning: a database set up before
      # the move existed, or one whose --work has changed, would otherwise keep
      # writing to the operating system's disk for the rest of its life.
      PREFIX="$PREFIX" "$SCRIPT_DIR/mariadb.sh" datadir "$RBH_DB_DATADIR" ||
        info "  could not move MariaDB's data directory to $RBH_DB_DATADIR; its rows are not on the storage under test"
    fi
    info "robinhood datadir: $(PREFIX="$PREFIX" "$SCRIPT_DIR/mariadb.sh" datadir 2>/dev/null || echo unknown)"
    # Whether it was provisioned now or five runs ago, the run is only fair if
    # Robinhood can actually reach its database. Finding that out here costs one
    # query; finding it out later costs a whole run of failing rows.
    local rbh_state
    if rbh_state=$(rbh_status "$tree"); then
      info "robinhood: $rbh_state"
    else
      info "robinhood: $rbh_state"
      info "  continuing without it; the other tools are unaffected"
      TOOLS=$(printf '%s' "$TOOLS" | tr ' ' '\n' | grep -vx robinhood | tr '\n' ' ')
      WITH_ROBINHOOD=0
    fi
  else
    log "2/5 skipping MariaDB and Robinhood"
  fi

  if [[ -f "$tree/QUERY_SEEDS.txt" ]] && grep -q '^arg_sets=' "$tree/QUERY_SEEDS.txt"; then
    log "3/5 reusing the existing tree at $tree"
    info "delete it with '$0 --undo $tree' to build a fresh one"
  elif [[ -f "$tree/QUERY_SEEDS.txt" ]]; then
    # A tree seeded before the queries had argument sets. Reseeding recreates
    # query_seeds/ -- a few thousand small files -- and leaves the tree itself
    # alone: prepare-synth.sh reuses a generated tree whose parameters match.
    log "3/5 reseeding the query arguments at $tree"
    info "its manifest predates the three argument sets the hot passes need"
    "$SCRIPT_DIR/prepare-synth.sh" "$tree"
  else
    log "3/5 building the synthetic tree at $tree"
    "$SCRIPT_DIR/prepare-synth.sh" "$tree"
  fi

  if [[ -z "${DROP_CACHES:-}" ]]; then
    if [[ "$(id -u)" -eq 0 ]]; then
      DROP_CACHES=1
    else
      DROP_CACHES=0
      info "not root: leaving caches warm (DROP_CACHES=0)"
    fi
  fi
  # Dropping the page cache leaves InnoDB's buffer pool populated, which would
  # flatter Robinhood against every other tool.
  if [[ -z "${DROP_DB_CACHE:-}" ]]; then
    if want_robinhood && [[ "$DROP_CACHES" == "1" ]]; then
      DROP_DB_CACHE=1
    else
      DROP_DB_CACHE=0
    fi
  fi

  local ts results
  ts=$(date +%Y%m%d-%H%M%S)
  results=${RESULTS_DIR:-${RESULTS_DEFAULT:-"$SCRIPT_DIR/results/run-$ts"}}
  mkdir -p "$results" ||
    die "cannot create the results directory $results; pass --results <dir> on a writable filesystem"
  results=$(cd "$results" && pwd)
  reject_inside_tree "$results" "$tree" "results directory"

  # Written before the run so --undo works even on an interrupted benchmark.
  {
    printf 'created=%s\n' "$(date -Iseconds)"
    printf 'tree=%s\n' "$tree"
    printf 'prefix=%s\n' "$PREFIX"
    printf 'src_root=%s\n' "$SRC_ROOT"
    printf 'mariadb=%s\n' "$(want_robinhood && echo 1 || echo 0)"
    # Removed by 'mariadb.sh cleanup', not by the file sweep: the server has to
    # be off it first.
    [[ -z "${RBH_DB_DATADIR:-}" ]] || printf 'rbh_datadir=%s\n' "$RBH_DB_DATADIR"
    printf 'work_root=%s\n' "$SCRATCH_INDEXES"
    printf 'tmp_root=%s\n' "$SCRATCH_TMP"
  } >"$tree/$STATE_NAME.tmp"
  if [[ -f "$tree/$STATE_NAME" ]]; then
    grep '^results=' "$tree/$STATE_NAME" >>"$tree/$STATE_NAME.tmp" || true
  fi
  printf 'results=%s\n' "$results" >>"$tree/$STATE_NAME.tmp"
  mv "$tree/$STATE_NAME.tmp" "$tree/$STATE_NAME"

  # Walk peers come in two classes: find/fd (names-only) and du/dua/dut (stat +
  # size). ecrawl needs a row in each — --no-stat and --no-write — each one extra
  # traversal per rep.
  local nowrite=${DO_NOWRITE:-1}
  local nostat=${DO_NOSTAT:-1}
  local statx=${DO_STATX:-0}
  local iouring=${DO_IOURING:-0}
  local cold_iouring=${ECRAWL_COLD_IOURING:-1}

  log "4/5 benchmarking (reps=$(reps_summary) cache=$CACHE_MODES DROP_CACHES=$DROP_CACHES DROP_DB_CACHE=$DROP_DB_CACHE DO_NOWRITE=$nowrite DO_NOSTAT=$nostat DO_STATX=$statx DO_IOURING=$iouring ECRAWL_COLD_IOURING=$cold_iouring)"
  info "results: $results"
  info "each timed binary prints one line: <seconds>  <label>"
  SKIP_PREPARE=1 REPS="$REPS" DROP_CACHES="$DROP_CACHES" DROP_DB_CACHE="$DROP_DB_CACHE" \
    CACHE_MODES="$CACHE_MODES" \
    DO_NOWRITE="$nowrite" DO_NOSTAT="$nostat" \
    DO_STATX="$statx" DO_IOURING="$iouring" \
    ECRAWL_COLD_IOURING="$cold_iouring" \
    WORK_ROOT="$SCRATCH_INDEXES" TMPDIR="$SCRATCH_TMP" \
    WITH_EXTERNALS="${WITH_EXTERNALS:-0}" \
    "$SCRIPT_DIR/run_smoke.sh" "$tree" "$results"

  log "5/5 done in $((SECONDS - started))s"
  info "summary: $results/SUMMARY_TABLE.txt"
  info "charts:  $results/charts"
  info "tear down with: $0 --undo $tree"
}

# Redraw the derived artifacts of a finished run from its CSVs. Everything here
# is a pure function of index_results.csv and query_results.csv, so it can be
# repeated as often as the presentation changes without spending the hours a
# measured run costs. Deliberately rewrites the summary as well as the charts:
# they are read side by side, and a fresh figure beside a stale table is worse
# than either alone.
charts_only() {
  local results=$1
  [[ -n "$results" ]] || die "--charts needs a results directory"
  [[ -d "$results" ]] || die "$results does not exist"
  results=$(cd "$results" && pwd)

  # Accept both layouts: run_smoke.sh writes index/ and queries/ subdirectories,
  # but a hand-assembled directory may hold the CSVs at the top level.
  local sources=() csv found=0
  for csv in "$results/index/index_results.csv" "$results/queries/query_results.csv" \
    "$results/index_results.csv" "$results/query_results.csv"; do
    [[ -f "$csv" ]] || continue
    found=1
    local dir
    dir=$(dirname "$csv")
    [[ " ${sources[*]-} " == *" $dir "* ]] || sources+=("$dir")
  done
  ((found == 1)) || die "$(
    printf 'no benchmark CSVs under %s.\n' "$results"
    printf '       Expected index/index_results.csv or queries/query_results.csv\n'
    printf '       from a completed run; --charts only redraws, it cannot measure.'
  )"

  log "redrawing from $results"
  info "sources: ${sources[*]}"

  if python3 "$SCRIPT_DIR/summarize.py" "${sources[@]}" \
    --out "$results/SUMMARY_TABLE.txt" \
    --failures-out "$results/FAILURES.txt" >/dev/null; then
    info "summary: $results/SUMMARY_TABLE.txt"
  else
    die "summarize.py failed; the CSVs may be truncated"
  fi

  # A full run reaches the chart venv through env.sh, which run_smoke.sh has
  # already sourced by the time it plots. This action runs on its own, so it has
  # to find the same interpreter itself.
  # shellcheck source=/dev/null
  [[ -f "$PREFIX/env.sh" ]] && source "$PREFIX/env.sh"
  export INDEXER_COMPARE_PREFIX="${INDEXER_COMPARE_PREFIX:-$PREFIX}"
  # Same subshell trick as rbh_status: lib.sh is meant to be sourced by the run
  # scripts, not by this one.
  local chart_py
  chart_py=$(bash -c 'source "$1/lib.sh"; resolve_chart_python' _ "$SCRIPT_DIR" 2>/dev/null) ||
    chart_py=""
  if [[ -z "$chart_py" ]]; then
    die "$(
      printf 'no interpreter with matplotlib, so there is nothing to draw with.\n'
      printf '       Provide one with: SETUP_CHARTS=1 %s/init.sh\n' "$SCRIPT_DIR"
      printf '       The summary table above was still rewritten.'
    )"
  fi
  info "interpreter: $chart_py"
  if MPLCONFIGDIR="${MPLCONFIGDIR:-$results/.mplconfig}" \
    "$chart_py" "$SCRIPT_DIR/plot_results.py" "$results" --out-dir "$results/charts"; then
    info "charts:  $results/charts"
  else
    die "chart generation failed; the summary table was still rewritten"
  fi
}

# Evidence that a directory was generated by this harness rather than holding
# real data. Any one of these is enough; an empty directory qualifies because
# deleting it costs nothing.
tree_evidence() {
  local tree=$1 d
  if [[ -f "$tree/QUERY_SEEDS.txt" && -d "$tree/query_seeds" ]]; then
    printf 'prepare-synth.sh seeds (QUERY_SEEDS.txt, query_seeds/)'
    return 0
  fi
  for d in single_huge_dir deep_skinny_chain wide_shallow depth_slash_profile \
    ereport_badge_fixtures links_and_specials real_large_files mega_dir1; do
    if [[ -d "$tree/$d" ]]; then
      printf 'generator layout (%s/)' "$d"
      return 0
    fi
  done
  if [[ -z "$(ls -A "$tree" 2>/dev/null)" ]]; then
    printf 'directory is empty'
    return 0
  fi
  return 1
}

# A tree built by the step-by-step flow has no state file. Adopting one is safe
# only when it looks like this harness built it, rather than on the say-so of
# whoever typed the command.
adopt_tree() {
  local tree=$1
  [[ -d "$tree" ]] || die "$tree does not exist"
  tree=$(cd "$tree" && pwd)
  local evidence
  if ! evidence=$(tree_evidence "$tree"); then
    die "$(
      printf '%s does not look like a generated benchmark tree, so refusing to adopt it.\n' "$tree"
      printf '       Expected prepare-synth.sh seeds (QUERY_SEEDS.txt and query_seeds/),\n'
      printf '       a generator directory such as single_huge_dir/, or an empty directory.\n'
      printf '       Its top level holds:\n'
      ls -A "$tree" 2>/dev/null | head -8 | sed 's/^/         /'
    )"
  fi
  if [[ -f "$tree/$STATE_NAME" ]]; then
    log "$tree is already adopted"
    return 0
  fi

  local mariadb=0
  [[ -f "$PREFIX/etc/robinhood.d/indexer-compare.conf" ]] && mariadb=1
  {
    printf 'created=%s\n' "$(date -Iseconds)"
    printf 'adopted=1\n'
    printf 'tree=%s\n' "$tree"
    printf 'prefix=%s\n' "$PREFIX"
    printf 'src_root=%s\n' "$SRC_ROOT"
    printf 'mariadb=%s\n' "$mariadb"
    # Only paths that exist: adopting cannot know where an earlier run wrote,
    # and recording a guess would have --undo delete something it never made.
    local guess=${WORK_DIR:-"${tree%/}-work"}
    [[ ! -d "${WORK_ROOT:-$guess/indexes}" ]] || printf 'work_root=%s\n' "${WORK_ROOT:-$guess/indexes}"
    [[ ! -d "$guess/tmp" ]] || printf 'tmp_root=%s\n' "$guess/tmp"
  } >"$tree/$STATE_NAME"

  log "adopted $tree"
  info "evidence: $evidence"
  info "wrote $tree/$STATE_NAME"
  info "prefix=$PREFIX src_root=$SRC_ROOT mariadb=$mariadb"
  info "results from earlier runs are not recorded, and --undo keeps results anyway"
  info "now run: $0 --undo $tree"
}

undo_run() {
  local tree=$1
  [[ -d "$tree" ]] ||
    die "$tree does not exist; if only the tools are left, remove $PREFIX and $SRC_ROOT by hand"
  tree=$(cd "$tree" && pwd)
  local state="$tree/$STATE_NAME"
  [[ -f "$state" ]] || die "$(
    printf "%s is absent, so this tree was not created by '%s --do'; refusing to delete %s.\n" \
      "$state" "$0" "$tree"
    printf "       If it was built by prepare-synth.sh directly, adopt it first with:\n"
    printf '         %s --adopt %s' "$0" "$tree"
  )"

  local s_prefix="" s_src="" s_work="" s_tmp="" s_mariadb=0 s_datadir=""
  local s_results=()
  local key value
  while IFS='=' read -r key value; do
    case "$key" in
      prefix) s_prefix=$value ;;
      src_root) s_src=$value ;;
      work_root) s_work=$value ;;
      tmp_root) s_tmp=$value ;;
      mariadb) s_mariadb=$value ;;
      rbh_datadir) s_datadir=$value ;;
      # Every --do appends its results path, and --small reuses one, so the same
      # directory is normally in there once per cycle.
      results)
        [[ " ${s_results[*]-} " == *" $value "* ]] || s_results+=("$value")
        ;;
    esac
  done <"$state"
  s_prefix=${s_prefix:-$PREFIX}
  s_src=${s_src:-$SRC_ROOT}

  log "teardown plan for $tree"
  if [[ "$s_mariadb" == "1" ]]; then
    info "drop the benchmark database and its generated Robinhood config"
    # Part of the database cleanup rather than the file sweep below: MariaDB has
    # to be pointed back at its packaged data directory before this can go.
    [[ -z "$s_datadir" ]] ||
      info "put MariaDB's data directory back and delete the copy at $s_datadir"
  fi
  if [[ -n "$s_work" ]]; then
    check_removable "$s_work" "index working directory"
    info "delete index working directory $s_work"
  fi
  if [[ -n "$s_tmp" ]]; then
    check_removable "$s_tmp" "scratch TMPDIR"
    info "delete scratch TMPDIR $s_tmp"
  fi
  # Only the two subdirectories this script created, never the --work parent
  # itself: it may be a shared scratch mount that predates the benchmark.
  [[ -z "$s_work$s_tmp" ]] || info "the directory containing them is left in place"
  check_removable "$tree" "benchmark tree"
  info "delete the benchmark tree $tree"
  if [[ "$KEEP_TOOLS" == "1" ]]; then
    info "keep the built tools in $s_prefix and the clones in $s_src"
  else
    check_removable "$s_prefix" "installed tools"
    check_removable "$s_src" "source clones"
    info "delete the built tools in $s_prefix and the clones in $s_src"
  fi
  if [[ "$PURGE_RESULTS" == "1" ]]; then
    local r
    for r in ${s_results[@]+"${s_results[@]}"}; do
      check_removable "$r" "results"
      info "delete results $r"
    done
  elif [[ ${#s_results[@]} -gt 0 ]]; then
    info "keep ${#s_results[@]} results directory/ies (pass --purge-results to delete them)"
  fi
  info "MariaDB packages and the running service are left alone"
  if edelete_usable; then
    info "the tree and the scratch go through 'edelete --delete --force' (${EDELETE_THREADS:-$THREADS} threads); the rest through rm -rf"
  else
    info "edelete is not runnable ($EDELETE_BIN), so everything goes through rm -rf"
    info "  build it with 'make edelete' first to delete a large tree in parallel"
  fi

  confirm "Proceed?" || die "aborted; nothing was removed"

  if [[ "$s_mariadb" == "1" && -f "$s_prefix/etc/robinhood.d/.indexer-compare-db" ]]; then
    log "dropping the benchmark database"
    PREFIX="$s_prefix" "$SCRIPT_DIR/mariadb.sh" cleanup ||
      info "WARN: database cleanup failed; check it by hand before reusing this host"
  fi

  log "removing files"
  [[ -z "$s_work" ]] || safe_rm "$s_work" "index working directory" bulk
  [[ -z "$s_tmp" ]] || safe_rm "$s_tmp" "scratch TMPDIR" bulk
  if [[ "$PURGE_RESULTS" == "1" ]]; then
    local r
    for r in ${s_results[@]+"${s_results[@]}"}; do
      safe_rm "$r" "results"
    done
  fi
  safe_rm "$tree" "benchmark tree" bulk
  if [[ "$KEEP_TOOLS" != "1" ]]; then
    safe_rm "$s_prefix" "installed tools"
    safe_rm "$s_src" "source clones"
  fi

  log "teardown complete"
  if [[ "$PURGE_RESULTS" != "1" && ${#s_results[@]} -gt 0 ]]; then
    local r
    for r in "${s_results[@]}"; do
      [[ -d "$r" ]] && info "results kept: $r"
    done
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --do | --undo | --adopt)
      [[ -z "$ACTION" ]] || die "use only one of --do, --undo, --adopt or --charts"
      ACTION=${1#--}
      [[ $# -ge 2 && "$2" != -* ]] || die "$1 needs a benchmark tree path"
      TREE=$2
      shift 2
      ;;
    --charts)
      [[ -z "$ACTION" ]] || die "use only one of --do, --undo, --adopt or --charts"
      ACTION=charts
      [[ $# -ge 2 && "$2" != -* ]] || die "--charts needs a results directory"
      TREE=$2
      shift 2
      ;;
    --work | --scratch)
      [[ $# -ge 2 && -n "$2" ]] || die "$1 needs a directory"
      WORK_DIR=$2
      shift 2
      ;;
    --results)
      [[ $# -ge 2 && -n "$2" ]] || die "$1 needs a directory"
      RESULTS_DIR=$2
      shift 2
      ;;
    --small | --tiny)
      SMALL=1
      shift
      ;;
    --smoke)
      SMOKE=1
      shift
      ;;
    --quick)
      QUICK=1
      shift
      ;;
    --reps)
      [[ $# -ge 2 && -n "$2" ]] || die "--reps needs a count or <tool>=<count>"
      parse_reps_spec "$2"
      shift 2
      ;;
    --yes | -y)
      ASSUME_YES=1
      shift
      ;;
    --no-robinhood)
      WITH_ROBINHOOD=0
      shift
      ;;
    --keep-tools)
      KEEP_TOOLS=1
      shift
      ;;
    --purge-results)
      PURGE_RESULTS=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *) die "unknown argument '$1'; see --help" ;;
  esac
done

[[ -n "$ACTION" ]] || {
  usage >&2
  exit 1
}

if (($(("$SMALL" + "$SMOKE" + "$QUICK")) > 1)); then
  die "--small, --smoke and --quick are different runs; pick one"
fi

RESULTS_DEFAULT=""
[[ "$SMALL" == "0" ]] || apply_small_defaults
[[ "$SMOKE" == "0" ]] || apply_smoke_defaults
[[ "$QUICK" == "0" ]] || apply_quick_defaults

case "$ACTION" in
  do) do_run "$TREE" ;;
  undo) undo_run "$TREE" ;;
  adopt) adopt_tree "$TREE" ;;
  charts) charts_only "$TREE" ;;
esac
