#!/usr/bin/env bash
#
# Build/install the three open-source indexers used by the PEARC '26 paper:
#   Robinhood 3.2.0, GUFI 0.6.10, XDU 0.4.1
# plus dua-cli, the parallel du used as a traditional-tool baseline.
#
# This installs into a private prefix. SETUP_MARIADB=1 additionally installs
# and starts MariaDB and creates a disposable Robinhood benchmark database.
#
# Usage:
#   scripts/compare-indexers/init.sh                      (asks before installing deps)
#   TOOLS="gufi xdu" scripts/compare-indexers/init.sh
#   INSTALL_PACKAGES=1 scripts/compare-indexers/init.sh   (unattended)
#   INSTALL_PACKAGES=1 SETUP_MARIADB=1 \
#     RBH_FS_PATH=/tmp/indexer-compare-synth scripts/compare-indexers/init.sh
#
# Environment:
#   PREFIX=$HOME/.local/indexer-compare
#   SRC_ROOT=$HOME/.cache/indexer-compare-src
#   TOOLS="gufi xdu robinhood dua"
#   JOBS=$(nproc)          build parallelism for every tool; lower it to share
#                          a busy host
#   FORCE_REINSTALL=0      1: rebuild every tool even when the pinned version is
#                          already installed in PREFIX. Reruns are otherwise
#                          cheap: a tool is built once and then left alone, so
#                          this script is safe to call before every benchmark.
#   CARGO_TARGET_DIR=      where cargo builds XDU. Defaults beside the sources,
#                          which is the wrong disk when SRC_ROOT is on a network
#                          filesystem; point it at local scratch there.
#   CARGO_JOBS=            rustc parallelism, overriding the memory-derived
#                          default. The Rust builds are memory-bound rather than
#                          CPU-bound, so by default they run at JOBS or at what
#                          the memory budget affords, whichever is smaller.
#   CARGO_MEM_PER_JOB_GB=4 memory assumed per rustc process by that estimate
#   INSTALL_PACKAGES=ask   prompt before installing dependencies with dnf/apt;
#                          1 installs without asking, 0 never installs. A
#                          non-interactive run behaves like 0.
#   PKG_ARGS=              extra dnf/apt-get arguments. Defaults to
#                          --disableplugin=etckeeper on dnf hosts, because that
#                          plugin can stall a transaction on an ssh password
#                          prompt when a site configures it to push /etc to a
#                          management node. Set it (even to empty) to replace
#                          that default; /etc changes then go unrecorded by
#                          etckeeper as usual.
#   SETUP_MARIADB=0        1: provision a disposable Robinhood database
#   SETUP_CHARTS=1         0: skip providing a matplotlib interpreter
#   RBH_FS_PATH=           tree configured for Robinhood when SETUP_MARIADB=1
#   SUDO=sudo              use "" when already root
#   GUFI_VERSION=0.6.10
#   XDU_VERSION=v0.4.1
#   ROBINHOOD_VERSION=3.2.0
#   DUA_VERSION=2.39.1
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PREFIX=${PREFIX:-"$HOME/.local/indexer-compare"}
SRC_ROOT=${SRC_ROOT:-"$HOME/.cache/indexer-compare-src"}
TOOLS=${TOOLS-"gufi xdu robinhood dua"}
JOBS=${JOBS:-$(nproc 2>/dev/null || echo 4)}
INSTALL_PACKAGES=${INSTALL_PACKAGES:-ask}
# etckeeper's dnf plugin commits /etc after every transaction, and where a site
# has given it a push remote it blocks on an ssh password prompt and then fails
# the transaction, which is fatal to an otherwise unattended install. It has
# nothing to do with building these tools, so skip it by default. dnf5 spells the
# option differently and apt has no equivalent, so this is dnf-only; setting
# PKG_ARGS (even to empty) replaces it.
default_pkg_args() {
  local dnf_path
  dnf_path=$(command -v dnf 2>/dev/null) || return 0
  case "$(readlink -f "$dnf_path" 2>/dev/null || printf '%s' "$dnf_path")" in
    *dnf5) printf '%s' '--disable-plugin=etckeeper' ;;
    *) printf '%s' '--disableplugin=etckeeper' ;;
  esac
}
[[ -n "${PKG_ARGS+set}" ]] || PKG_ARGS=$(default_pkg_args)
read -r -a PKG_ARGS_ARR <<<"$PKG_ARGS"
SETUP_MARIADB=${SETUP_MARIADB:-0}
SETUP_CHARTS=${SETUP_CHARTS:-1}
RBH_FS_PATH=${RBH_FS_PATH:-}
CHART_VENV="$PREFIX/chartvenv"

GUFI_VERSION=${GUFI_VERSION:-0.6.10}
XDU_VERSION=${XDU_VERSION:-v0.4.1}
ROBINHOOD_VERSION=${ROBINHOOD_VERSION:-3.2.0}
DUA_VERSION=${DUA_VERSION:-2.39.1}

GUFI_REPO=${GUFI_REPO:-https://github.com/mar-file-system/GUFI.git}
XDU_REPO=${XDU_REPO:-https://github.com/glentner/xdu.git}
ROBINHOOD_REPO=${ROBINHOOD_REPO:-https://github.com/cea-hpc/robinhood.git}

if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=${SUDO:-}
else
  SUDO=${SUDO:-sudo}
fi

log() { printf '==> %s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

want_tool() {
  case " $TOOLS " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

# Rebuilding these is the expensive part of every run: GUFI takes ten minutes
# with its bundled SQLite, and XDU pulls in arrow and parquet. Nothing about
# them changes between runs, so an install of the pinned version is left alone
# and only FORCE_REINSTALL=1 replaces it.
STAMP_DIR="$PREFIX/var/lib/indexer-compare"
FORCE_REINSTALL=${FORCE_REINSTALL:-0}

# A binary that resolves is not a binary that runs: a prefix on shared storage
# can hold one built against a newer glibc. ldd reports that as "not found"
# without executing anything, which also works for a tool with no --version.
binary_runnable() {
  local bin=$1
  [[ -x "$bin" ]] || return 1
  ! ldd "$bin" 2>/dev/null | grep -q 'not found'
}

# name, pinned version, then every binary the install must have left behind.
tool_installed() {
  local name=$1 version=$2
  shift 2
  [[ "$FORCE_REINSTALL" == "1" ]] && return 1
  local bin
  for bin in "$@"; do
    binary_runnable "$bin" || return 1
  done
  local stamp="$STAMP_DIR/$name.version" have=""
  [[ -f "$stamp" ]] && have=$(<"$stamp")
  if [[ "$have" == "$version" ]]; then
    log "$name $version already installed in $PREFIX (FORCE_REINSTALL=1 to rebuild)"
    return 0
  fi
  if [[ -z "$have" ]]; then
    # Installed by a run that predates the stamp. Rebuilding it would cost the
    # ten minutes this check exists to avoid, so take it and record it.
    log "$name is installed but unstamped; assuming $version (FORCE_REINSTALL=1 to rebuild)"
    mark_installed "$name" "$version"
    return 0
  fi
  log "$name $have is installed but $version is pinned; rebuilding"
  return 1
}

mark_installed() {
  mkdir -p "$STAMP_DIR"
  printf '%s\n' "$2" >"$STAMP_DIR/$1.version"
}

run_root() {
  if [[ -n "$SUDO" ]]; then
    "$SUDO" "$@"
  else
    "$@"
  fi
}

# Ask before doing anything to the host. Unattended callers pin INSTALL_PACKAGES
# to 0 or 1 and are never prompted.
confirm() {
  local prompt=$1 reply=""
  if [[ ! -t 0 ]]; then
    log "not a terminal, so not asking; set INSTALL_PACKAGES=1 to install without prompting"
    return 1
  fi
  read -r -p "$prompt [Y/n] " reply || true
  case "$reply" in
    "" | [Yy] | [Yy][Ee][Ss]) return 0 ;;
    *) return 1 ;;
  esac
}

install_packages() {
  [[ "$INSTALL_PACKAGES" != "0" ]] || return 0

  local mgr pkgs optional
  if command -v dnf >/dev/null 2>&1; then
    mgr=dnf
    pkgs=(
      git gcc gcc-c++ make cmake python3 pkgconf-pkg-config
      libattr-devel pcre2-devel zlib-devel curl
    )
    optional=(time python3-matplotlib fd-find dua-cli)
    # GUFI's CMake enables its RPM packaging step only when rpmbuild exists, so
    # this is a nice-to-have rather than a build requirement.
    want_tool gufi && optional+=(rpm-build)
    if want_tool robinhood; then
      pkgs+=(
        autoconf automake libtool flex bison glib2-devel
        mariadb-connector-c-devel
      )
      optional+=(jemalloc-devel)
    fi
  elif command -v apt-get >/dev/null 2>&1; then
    mgr=apt-get
    pkgs=(
      git build-essential cmake python3 python3-venv pkg-config
      libattr1-dev libpcre2-dev zlib1g-dev curl
    )
    optional=(time python3-matplotlib fd-find dua-cli)
    if want_tool robinhood; then
      pkgs+=(
        autoconf automake libtool flex bison libglib2.0-dev
        default-libmysqlclient-dev
      )
      optional+=(libjemalloc-dev)
    fi
  elif [[ "$INSTALL_PACKAGES" == "1" ]]; then
    die "INSTALL_PACKAGES=1 but neither dnf nor apt-get is available"
  else
    log "no dnf or apt-get here; skipping dependency install"
    return 0
  fi

  if [[ "$INSTALL_PACKAGES" != "1" ]]; then
    printf '\nBuild dependencies for TOOLS="%s" (%s%s):\n' \
      "$TOOLS" "$mgr" "${SUDO:+ via $SUDO}"
    printf '  required: %s\n' "${pkgs[*]}"
    printf '  optional: %s\n' "${optional[*]}"
    printf '  Skipping means installing these yourself; the build stops at the first one missing.\n'
    if ! confirm "Install them now?"; then
      log "skipping dependency install"
      return 0
    fi
  fi

  log "installing build dependencies with $mgr"
  local args=(install -y ${PKG_ARGS_ARR[@]+"${PKG_ARGS_ARR[@]}"})
  [[ "$mgr" == "apt-get" ]] && run_root apt-get update
  run_root "$mgr" "${args[@]}" "${pkgs[@]}"

  # Charts and GNU time are conveniences, and packages such as
  # python3-matplotlib live in EPEL rather than the base repositories. Install
  # them one at a time so an unavailable name cannot abort the whole setup.
  local pkg
  for pkg in "${optional[@]}"; do
    if ! run_root "$mgr" "${args[@]}" "$pkg" >/dev/null 2>&1; then
      log "note: optional package '$pkg' is unavailable; continuing without it"
    fi
  done
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 ||
    die "missing '$1' (rerun and accept the dependency install, or install it manually)"
}

# Report a missing -devel package here rather than as a CMake stack trace
# hundreds of configure lines later.
require_pkgconfig() {
  local module=$1 pkg=$2
  pkg-config --exists "$module" 2>/dev/null ||
    die "missing pkg-config module '$module' (install $pkg, or rerun and accept the dependency install)"
}

# A site-wide insteadOf rule silently redirects these public https clones to an
# internal mirror, where git then blocks on an interactive ssh password prompt.
# Name the rule instead of letting the build appear to hang.
git_transport_hint() {
  local rewrites
  rewrites=$(git config --show-origin --get-regexp 'url\..*\.insteadof' 2>/dev/null || true)
  [[ -z "$rewrites" ]] ||
    printf 'note: git is configured to rewrite remote URLs:\n%s\n' "$rewrites" >&2
}

# Never prompt: an unreachable or rewritten remote must fail, not wait for input.
git_batch() {
  GIT_TERMINAL_PROMPT=0 \
    GIT_SSH_COMMAND=${GIT_SSH_COMMAND:-"ssh -oBatchMode=yes"} \
    git "$@"
}

# Clone a pinned tag, or safely move an existing clean checkout to that tag.
checkout_tag() {
  local name=$1 repo=$2 tag=$3 dest=$4

  if [[ ! -d "$dest/.git" ]]; then
    log "cloning $name $tag from $repo"
    if ! git_batch clone --depth 1 --branch "$tag" "$repo" "$dest"; then
      rm -rf "$dest"
      git_transport_hint
      die "cloning $name from $repo failed; set ${name^^}_REPO to a reachable mirror or clone it into $dest yourself"
    fi
    return
  fi

  local origin
  origin=$(git -C "$dest" remote get-url origin 2>/dev/null || true)
  [[ "$origin" == "$repo" ]] ||
    die "$dest exists but origin is '$origin', expected '$repo'"

  # Already on the pinned tag, so there is no version to switch and a rerun can
  # just rebuild. Builds leave artifacts in these trees, so this is the normal
  # path for every rerun after the first.
  local want have
  want=$(git -C "$dest" rev-parse --verify -q "refs/tags/$tag^{commit}" || true)
  have=$(git -C "$dest" rev-parse --verify -q HEAD || true)
  if [[ -n "$want" && "$want" == "$have" ]]; then
    log "$name already checked out at $tag"
    return
  fi

  # Only edits to tracked files can be lost by switching versions; untracked
  # build output from an earlier run must not block the switch.
  [[ -z "$(git -C "$dest" status --porcelain --untracked-files=no)" ]] ||
    die "$dest has local edits to tracked files; refusing to switch versions"

  log "updating $name checkout to $tag"
  if ! git_batch -C "$dest" fetch --depth 1 origin "refs/tags/$tag:refs/tags/$tag"; then
    git_transport_hint
    die "fetching $tag for $name from $repo failed"
  fi
  git -C "$dest" checkout --detach "$tag"
}

GUFI_CONFIG_FILE="$PREFIX/etc/GUFI/config"
# Filled in by install_gufi, or looked up again when this run did not build it.
GUFI_PYTHON_LIB=""

install_gufi() {
  if tool_installed gufi "$GUFI_VERSION" \
    "$PREFIX/bin/gufi_dir2index" "$PREFIX/bin/gufi_find" "$PREFIX/bin/gufi_du"; then
    GUFI_PYTHON_LIB=$(find_gufi_python_lib) ||
      die "GUFI is installed in $PREFIX but gufi_common.py is not; rerun with FORCE_REINSTALL=1"
    return 0
  fi
  require_cmd git
  require_cmd cmake
  require_cmd make
  require_cmd python3
  require_cmd pkg-config
  require_pkgconfig libpcre2-8 "pcre2-devel (Debian: libpcre2-dev)"
  require_pkgconfig zlib "zlib-devel (Debian: zlib1g-dev)"

  local src="$SRC_ROOT/GUFI"
  # Build outside the checkout so the clone stays pristine for version switches.
  local build="$SRC_ROOT/GUFI-build-compare"
  checkout_tag GUFI "$GUFI_REPO" "$GUFI_VERSION" "$src"
  rm -rf "$src/build-compare"

  log "configuring GUFI $GUFI_VERSION"
  # gufi_find and gufi_du are Python wrappers around gufi_query, and the path of
  # the config they read is compiled in (upstream default /etc/GUFI/config).
  # Point it inside the prefix so the harness can rewrite it per run -- it
  # carries the index root every query is resolved against -- and so --undo
  # takes it away with everything else.
  # Two defaults have to go, or the install fails on a host that would run GUFI
  # perfectly well. DEP_AI builds sqlite-vec and sqlite-lembed, which vendors
  # llama.cpp: nothing here queries an embedding, and llama.cpp emits AVX512-BF16
  # that an older binutils cannot assemble ("no such instruction: vdpbf16ps").
  # BASH_COMPLETION installs into /etc/bash_completion.d, an absolute path
  # outside the prefix that an unprivileged install cannot write.
  cmake -S "$src" -B "$build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DSERVER_CONFIG="$GUFI_CONFIG_FILE" \
    -DDEP_AI=OFF \
    -DBASH_COMPLETION=OFF \
    -DDEP_BUILD_THREADS="$JOBS"
  log "building/installing GUFI"
  cmake --build "$build" --parallel "$JOBS"
  cmake --install "$build"

  GUFI_PYTHON_LIB=$(find_gufi_python_lib) ||
    die "GUFI installed but gufi_common.py is nowhere under $PREFIX; gufi_find and gufi_du import it by bare name and would die on ModuleNotFoundError"
  log "GUFI python modules in $GUFI_PYTHON_LIB"
  mark_installed gufi "$GUFI_VERSION"
}

# gufi_find and gufi_du are Python and import gufi_common by bare name, so
# PYTHONPATH has to name the directory GUFI actually installed it in. Upstream
# has moved it before, and guessing $PREFIX/lib is how every query ended in
# ModuleNotFoundError on this host, so ask the filesystem instead.
find_gufi_python_lib() {
  local hit
  hit=$(find "$PREFIX" -name gufi_common.py -type f -print -quit 2>/dev/null)
  [[ -n "$hit" ]] || return 1
  printf '%s' "$(dirname "$hit")"
}

# The memory this process is actually allowed, in KiB, or empty when nothing
# caps it. Under Slurm the limit lives on a parent cgroup rather than the leaf,
# and /proc/meminfo still reports the whole node, so walk the hierarchy up and
# keep the smallest limit found.
cgroup_mem_limit_kb() {
  local rel dir raw best=""
  rel=$(awk -F: '$1=="0"{print $3; exit}' /proc/self/cgroup 2>/dev/null) || true
  if [[ -n "$rel" && -d /sys/fs/cgroup$rel ]]; then
    dir=/sys/fs/cgroup$rel
    while [[ "$dir" == /sys/fs/cgroup* ]]; do
      raw=$(cat "$dir/memory.max" 2>/dev/null) || raw=""
      if [[ "$raw" =~ ^[0-9]+$ ]] && { [[ -z "$best" ]] || ((raw / 1024 < best)); }; then
        best=$((raw / 1024))
      fi
      [[ "$dir" == /sys/fs/cgroup ]] && break
      dir=$(dirname "$dir")
    done
  fi
  if [[ -z "$best" ]]; then
    raw=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null) || raw=""
    # cgroup v1 spells "unlimited" as a number near 2^63.
    [[ "$raw" =~ ^[0-9]+$ ]] && ((raw < 1 << 62)) && best=$((raw / 1024))
  fi
  printf '%s' "$best"
}

# How many rustc processes this machine can feed. Cargo takes --jobs as a core
# count, but building arrow and parquet is memory-bound, not CPU-bound: each
# unit can hold several GB of LLVM IR, so --jobs $(nproc) on a big node starts
# hundreds of them and the node swaps itself to a standstill. Size the build by
# the smaller of the two resources instead.
cargo_jobs() {
  if [[ -n "${CARGO_JOBS:-}" ]]; then
    printf '%s' "$CARGO_JOBS"
    return 0
  fi

  local per_job_gb=${CARGO_MEM_PER_JOB_GB:-4}
  local budget_kb avail_kb limit_kb n
  avail_kb=$(awk '/^MemAvailable:/{print $2; exit}' /proc/meminfo 2>/dev/null) || avail_kb=""
  limit_kb=$(cgroup_mem_limit_kb)
  budget_kb=$avail_kb
  if [[ -n "$limit_kb" ]] && { [[ -z "$budget_kb" ]] || ((limit_kb < budget_kb)); }; then
    budget_kb=$limit_kb
  fi
  # Nothing readable to go on: trust the caller rather than inventing a number.
  [[ -n "$budget_kb" ]] || { printf '%s' "$JOBS"; return 0; }

  n=$((budget_kb / (per_job_gb * 1024 * 1024)))
  ((n < 1)) && n=1
  ((n > JOBS)) && n=$JOBS
  printf '%s' "$n"
}

ensure_rust() {
  if command -v cargo >/dev/null 2>&1; then
    return 0
  fi

  require_cmd curl
  log "cargo not found; installing rustup (minimal profile) under the current user"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y --profile minimal
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
  command -v cargo >/dev/null 2>&1 || die "rustup completed but cargo is unavailable"
}

# XDU pulls in libduckdb-sys, which compiles a large amount of C++ through the
# cc crate with $CXX and then leaves rustc to link the result by invoking plain
# "cc". Those are not always the same toolchain: a GCC module installs gcc, g++,
# c++ and cpp but no cc, so g++ can be the module's GCC 12 while cc is still the
# system /usr/bin/cc from GCC 11. DuckDB then references libstdc++ internals the
# older static libstdc++.a has never heard of, and the link dies on undefined
# std::__exception_ptr and std::__throw_bad_array_new_length symbols. Pin the
# linker to the C driver sitting beside $CXX so one toolchain does both halves.
cxx_matched_linker() {
  local cxx dir drv
  cxx=$(command -v "${CXX:-c++}" 2>/dev/null) || return 1
  dir=$(dirname "$cxx")
  for drv in "$dir/gcc" "$dir/cc" "$dir/clang"; do
    [[ -x "$drv" ]] && { printf '%s' "$drv"; return 0; }
  done
  return 1
}

# CARGO_TARGET_<TRIPLE>_LINKER, which needs the host triple upper-cased with
# dashes turned into underscores.
cargo_linker_var() {
  local triple
  triple=$(rustc -vV 2>/dev/null | awk '/^host:/{print $2}') || return 1
  [[ -n "$triple" ]] || return 1
  printf 'CARGO_TARGET_%s_LINKER' "$(printf '%s' "$triple" | tr 'a-z-' 'A-Z_')"
}

install_xdu() {
  tool_installed xdu "$XDU_VERSION" "$PREFIX/bin/xdu" "$PREFIX/bin/xdu-find" && return 0
  require_cmd git
  ensure_rust

  local src="$SRC_ROOT/xdu"
  checkout_tag XDU "$XDU_REPO" "$XDU_VERSION" "$src"

  # XDU 0.4.1 uses Rust edition 2024; current stable supports it.
  if command -v rustup >/dev/null 2>&1; then
    log "ensuring a current stable Rust toolchain for XDU"
    rustup toolchain install stable --profile minimal
  fi

  local cjobs
  cjobs=$(cargo_jobs)
  log "building/installing XDU $XDU_VERSION with $cjobs cargo job(s)"
  ((cjobs < JOBS)) &&
    log "  (capped from JOBS=$JOBS to fit memory; raise with CARGO_JOBS= or CARGO_MEM_PER_JOB_GB=)"
  (
    cd "$src"
    # Keep cargo's target/ out of the checkout so the clone stays pristine. The
    # default puts it beside the sources, which is the wrong disk when SRC_ROOT
    # is on a network filesystem -- a cargo build of arrow and parquet there
    # stalled for over an hour with no progress -- so an exported
    # CARGO_TARGET_DIR pointing at local scratch wins.
    export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$SRC_ROOT/xdu-build-compare}"

    local linker linker_var
    if linker=$(cxx_matched_linker) && linker_var=$(cargo_linker_var); then
      export CC="${CC:-$linker}" CXX="${CXX:-$(command -v c++)}"
      export "$linker_var=$linker"
      log "  C++ $("$CXX" -dumpversion 2>/dev/null || echo '?') from $CXX"
      log "  linking with $("$linker" -dumpversion 2>/dev/null || echo '?') from $linker"
    else
      log "  WARN: cannot pin the linker to \$CXX; a link failure on undefined std:: symbols means they disagree"
    fi

    if command -v rustup >/dev/null 2>&1; then
      cargo +stable install --jobs "$cjobs" --locked --path . --root "$PREFIX" --force
    else
      cargo install --jobs "$cjobs" --locked --path . --root "$PREFIX" --force
    fi
  )
  mark_installed xdu "$XDU_VERSION"
}

install_dua() {
  # A dua that resolves is not a dua that runs: this prefix usually lives in a
  # shared home, so the binary in it may have been built on a node with a newer
  # glibc and now dies on startup. Check the prefix's own copy first, and say
  # what it said before replacing it, because the same trap catches every tool
  # here and the error is the only thing that identifies it.
  local prefix_dua="$PREFIX/bin/dua" err=""
  if [[ -x "$prefix_dua" && "$FORCE_REINSTALL" != "1" ]]; then
    if "$prefix_dua" --version >/dev/null 2>&1; then
      log "dua already installed: $prefix_dua"
      mark_installed dua "$DUA_VERSION"
      return 0
    fi
    err=$("$prefix_dua" --version 2>&1 >/dev/null | grep -m1 '.' || true)
    log "WARN: $prefix_dua does not run here (${err:-no error text}); rebuilding it"
  elif command -v dua >/dev/null 2>&1 && dua --version >/dev/null 2>&1; then
    log "dua already installed: $(command -v dua)"
    return 0
  fi

  ensure_rust
  log "building/installing dua-cli $DUA_VERSION"
  cargo install dua-cli --version "$DUA_VERSION" --jobs "$(cargo_jobs)" --locked --root "$PREFIX" --force
  "$prefix_dua" --version >/dev/null 2>&1 ||
    die "built $prefix_dua but it still does not run: $("$prefix_dua" --version 2>&1 >/dev/null | grep -m1 '.' || echo 'no error text')"
  mark_installed dua "$DUA_VERSION"
}

install_robinhood() {
  tool_installed robinhood "$ROBINHOOD_VERSION" \
    "$PREFIX/sbin/robinhood" "$PREFIX/bin/rbh-find" && return 0
  require_cmd git
  require_cmd make
  require_cmd gcc
  require_cmd autoconf
  require_cmd automake
  require_cmd libtool
  require_cmd flex
  require_cmd bison
  require_cmd pkg-config

  local src="$SRC_ROOT/robinhood"
  checkout_tag Robinhood "$ROBINHOOD_REPO" "$ROBINHOOD_VERSION" "$src"

  log "bootstrapping Robinhood $ROBINHOOD_VERSION"
  (
    cd "$src"
    sh autogen.sh
    # POSIX scanner build: no Lustre/Shook dependency. Keep jemalloc disabled
    # because this private benchmark install should run on hosts without its
    # runtime package. MariaDB/MySQL development headers are still required.
    ./configure \
      --prefix="$PREFIX" \
      --sysconfdir="$PREFIX/etc" \
      --disable-lustre \
      --disable-shook \
      --disable-jemalloc
    make -j"$JOBS"
    make install
  )
  mark_installed robinhood "$ROBINHOOD_VERSION"
}

# The interpreter carrying matplotlib is frequently not the `python3` on PATH
# (platform python vs. RPM builds), so fall back to a dedicated venv.
install_chart_env() {
  [[ "$SETUP_CHARTS" == "1" ]] || return 0

  local py
  for py in "$CHART_VENV/bin/python" python3 python3.13 python3.12 python3.11 python3.10 python3.9; do
    [[ -x "$py" ]] || command -v "$py" >/dev/null 2>&1 || continue
    if "$py" -c 'import matplotlib' >/dev/null 2>&1; then
      log "charts will use $py (matplotlib already installed)"
      return 0
    fi
  done

  for py in python3.13 python3.12 python3.11 python3.10 python3.9 python3; do
    command -v "$py" >/dev/null 2>&1 || continue
    "$py" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 7) else 1)' 2>/dev/null || continue
    log "creating chart virtualenv with $py"
    rm -rf "$CHART_VENV"
    if "$py" -m venv "$CHART_VENV" >/dev/null 2>&1 &&
      "$CHART_VENV/bin/pip" install --quiet --disable-pip-version-check matplotlib >/dev/null 2>&1; then
      log "charts will use $CHART_VENV/bin/python"
      return 0
    fi
    rm -rf "$CHART_VENV"
  done

  log "note: no interpreter with matplotlib; run_smoke.sh will skip charts"
}

RBH_CONFIG_FILE="$PREFIX/etc/robinhood.d/indexer-compare.conf"

# The tree Robinhood is configured to scan, which is the one it walks no matter
# what the benchmark is pointed at.
rbh_configured_path() {
  [[ -f "$RBH_CONFIG_FILE" ]] || return 1
  sed -n 's/^[[:space:]]*fs_path[[:space:]]*=[[:space:]]*"\?\([^";]*\)"\?[[:space:]]*;.*/\1/p' \
    "$RBH_CONFIG_FILE" | sed -n '1p'
}

setup_mariadb() {
  [[ "$SETUP_MARIADB" == "1" ]] || return 0
  want_tool robinhood ||
    die "SETUP_MARIADB=1 requires robinhood in TOOLS"
  [[ -n "$RBH_FS_PATH" ]] ||
    die "SETUP_MARIADB=1 requires RBH_FS_PATH=<existing benchmark tree>"
  # Re-running setup rotates the password and rewrites the config, so skip it
  # when the existing one already points at this tree. That makes a resumed
  # install cheap and leaves a scanned database intact.
  local configured
  if configured=$(rbh_configured_path) && [[ "$configured" == "$RBH_FS_PATH" ]]; then
    log "Robinhood config already provisioned for $RBH_FS_PATH"
    return 0
  fi
  [[ -z "${configured:-}" ]] ||
    log "Robinhood config points at $configured, not $RBH_FS_PATH; reprovisioning"
  PREFIX="$PREFIX" SUDO="$SUDO" INSTALL_MARIADB_PACKAGES=1 \
    "$SCRIPT_DIR/mariadb.sh" setup "$RBH_FS_PATH"
}

write_env_file() {
  local env_file="$PREFIX/env.sh"
  mkdir -p "$PREFIX"
  # A run that skipped the GUFI build still has to export the right path, so
  # look for the modules an already-installed GUFI left behind.
  if [[ -z "$GUFI_PYTHON_LIB" ]]; then
    GUFI_PYTHON_LIB=$(find_gufi_python_lib || true)
  fi
  cat >"$env_file" <<EOF
# Source this before running scripts/compare-indexers:
export INDEXER_COMPARE_PREFIX="$PREFIX"
# This prefix usually sits in a shared filesystem, and a binary built against
# one machine's glibc dies on startup on an older one. The host is recorded for
# the record; the libc version is what lib.sh compares, because a cluster's
# nodes share an image and warning per hostname would cry wolf on every job.
export INDEXER_COMPARE_BUILD_HOST="$(hostname)"
export INDEXER_COMPARE_BUILD_LIBC="$(getconf GNU_LIBC_VERSION 2>/dev/null || echo unknown)"
export PATH="$PREFIX/bin:$PREFIX/sbin:\${PATH}"
export LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:\${LD_LIBRARY_PATH:-}"
# Where this install actually put gufi_common.py and gufi_config.py. The
# gufi_find/gufi_du wrappers import them by bare name, so without this every
# GUFI query dies on ModuleNotFoundError before it reads its first database.
export PYTHONPATH="${GUFI_PYTHON_LIB:-$PREFIX/lib}\${PYTHONPATH:+:\${PYTHONPATH}}"

# Harness overrides (only used when the binaries exist):
export GUFI_DIR2INDEX="$PREFIX/bin/gufi_dir2index"
export GUFI_FIND="$PREFIX/bin/gufi_find"
export GUFI_DU="$PREFIX/bin/gufi_du"
export GUFI_ROLLUP="$PREFIX/bin/gufi_rollup"
export GUFI_QUERY="$PREFIX/bin/gufi_query"
export GUFI_SQLITE3="$PREFIX/bin/gufi_sqlite3"
export GUFI_STAT_BIN="$PREFIX/bin/gufi_stat_bin"
# Compiled into the wrappers by -DSERVER_CONFIG; the harness rewrites it before
# each query run so IndexRoot names the index that was just built.
export GUFI_CONFIG="$GUFI_CONFIG_FILE"
export XDU_BIN="$PREFIX/bin/xdu"
export XDU_FIND="$PREFIX/bin/xdu-find"
export RBH_SCAN="$PREFIX/sbin/robinhood"
export RBH_FIND="$PREFIX/bin/rbh-find"
export RBH_DU="$PREFIX/bin/rbh-du"
export RBH_DB_HELPER="$SCRIPT_DIR/mariadb.sh"
export RBH_DB_PREFIX="$PREFIX"

# Recorded in each run's env.txt, so results identify the tool versions even
# when a tool prints no version banner of its own.
export INDEXER_COMPARE_GUFI_VERSION="$GUFI_VERSION"
export INDEXER_COMPARE_XDU_VERSION="$XDU_VERSION"
export INDEXER_COMPARE_ROBINHOOD_VERSION="$ROBINHOOD_VERSION"
export INDEXER_COMPARE_DUA_VERSION="$DUA_VERSION"
EOF
  # Only pin DUA_BIN to this prefix when it holds a dua; otherwise leave lib.sh
  # to find the distribution package on PATH.
  if [[ -x "$PREFIX/bin/dua" ]]; then
    printf 'export DUA_BIN="%s"\n' "$PREFIX/bin/dua" >>"$env_file"
  fi
  if [[ -x "$CHART_VENV/bin/python" ]]; then
    printf 'export CHART_PYTHON="%s"\n' "$CHART_VENV/bin/python" >>"$env_file"
  fi
  # Keyed on the generated config, not on the ownership marker: the marker says
  # this helper may drop the database, while the config is what a scan and every
  # query actually need. Keying it on the marker meant a prefix built before the
  # database existed exported neither, and every Robinhood row then ran without
  # a config and failed.
  if [[ -f "$RBH_CONFIG_FILE" ]]; then
    cat >>"$env_file" <<EOF
export RBH_CONFIG="$RBH_CONFIG_FILE"
export RBH_SCAN_ARGS="-f $RBH_CONFIG_FILE --scan --once --alter-db"
export RBH_AUTO_RESET=1
EOF
  fi
  log "wrote $env_file"
}

verify() {
  local failed=0
  local path
  for path in \
    "$PREFIX/bin/gufi_dir2index" \
    "$PREFIX/bin/gufi_find" \
    "$PREFIX/bin/gufi_du" \
    "$PREFIX/bin/xdu" \
    "$PREFIX/bin/xdu-find" \
    "$PREFIX/sbin/robinhood" \
    "$PREFIX/bin/rbh-find" \
    "$PREFIX/bin/rbh-du"; do
    if [[ -x "$path" ]]; then
      printf '  OK      %s\n' "$path"
    else
      printf '  MISSING %s\n' "$path"
      case "$path" in
        *gufi*) want_tool gufi && failed=1 ;;
        *xdu*) want_tool xdu && failed=1 ;;
        *robinhood*|*rbh-find*) want_tool robinhood && failed=1 ;;
      esac
    fi
  done
  if want_tool dua; then
    local dua_path
    dua_path=$([[ -x "$PREFIX/bin/dua" ]] && printf '%s' "$PREFIX/bin/dua" ||
      command -v dua 2>/dev/null || true)
    if [[ -n "$dua_path" ]] && "$dua_path" --version >/dev/null 2>&1; then
      printf '  OK      %s\n' "$dua_path"
    else
      printf '  MISSING dua (%s)\n' "${dua_path:-not found}"
      failed=1
    fi
  fi
  return "$failed"
}

main() {
  mkdir -p "$PREFIX" "$SRC_ROOT"
  install_packages
  setup_mariadb
  install_chart_env

  want_tool gufi && install_gufi
  want_tool xdu && install_xdu
  want_tool robinhood && install_robinhood
  want_tool dua && install_dua

  write_env_file
  log "installation summary"
  verify || die "one or more requested binaries were not installed"

  local tree=${RBH_FS_PATH:-/tmp/indexer-compare-synth}
  cat <<EOF

Installation complete.

Activate:
  source "$PREFIX/env.sh"

If MariaDB was not provisioned above, run this with the tree you are about to
benchmark. It becomes fs_path in Robinhood's config, and robinhood --scan walks
that path rather than the one given to the benchmark, so it must match the
argument to run_smoke.sh below or the Robinhood rows are skipped:
  PREFIX="$PREFIX" scripts/compare-indexers/mariadb.sh setup $tree

After the Robinhood benchmark, remove its database and generated credentials:
  PREFIX="$PREFIX" scripts/compare-indexers/mariadb.sh cleanup

Then run:
  scripts/compare-indexers/run_smoke.sh $tree
EOF
}

main "$@"
