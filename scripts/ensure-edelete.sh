#!/usr/bin/env bash
# ensure-edelete.sh — print the path of a usable edelete binary, cloning and
# building https://github.com/mit-orcd/ecopy into ereport's own cache when
# needed.
#
# edelete moved out of this repo to mit-orcd/ecopy. Benchmark/profiling
# teardown uses it for parallel bulk deletion, but it is optional: callers
# fall back to rm -rf when this script fails.
#
# Resolution order:
#   1. $EDELETE_BIN, when set — explicit override; if it is not usable the
#      script fails (a typo'd override should not silently clone something).
#   2. $ECOPY_DIR/edelete — existing clone; `make edelete` if the binary is
#      missing. Never auto-pulled: update by hand with git -C "$ECOPY_DIR" pull.
#   3. Fresh clone of $ECOPY_REPO into $ECOPY_DIR, then `make edelete`.
#
# Env:
#   EDELETE_BIN   explicit binary path (wins over everything)
#   ECOPY_DIR     clone location. Default: the shared ORCD scratch toolchain
#                 dir when present, else a per-user cache:
#                   ~/orcd/scratch/ereport-automated-testing/ecopy
#                   ~/.cache/ereport/ecopy
#   ECOPY_REPO    git URL (default: https://github.com/mit-orcd/ecopy)
#   ECOPY_REF     ref to checkout right after cloning (default: remote HEAD)
#
# stdout: the binary path, exit 0. Any failure: message on stderr, exit 1.
# A resolved binary is smoke-tested with a bare invocation: rc 2 is the usage
# message (fine); 126/127 means the loader failed (not fine).

set -u

log() { echo "ensure-edelete: $*" >&2; }

usable() {
  local bin=$1 rc
  [[ -n "$bin" && -x "$bin" ]] || return 1
  "$bin" >/dev/null 2>&1
  rc=$?
  ((rc != 126 && rc != 127))
}

if [[ -n "${EDELETE_BIN:-}" ]]; then
  if usable "$EDELETE_BIN"; then
    echo "$EDELETE_BIN"
    exit 0
  fi
  log "EDELETE_BIN is set but not usable: $EDELETE_BIN"
  exit 1
fi

ECOPY_REPO=${ECOPY_REPO:-https://github.com/mit-orcd/ecopy}
if [[ -z "${ECOPY_DIR:-}" ]]; then
  if [[ -d "$HOME/orcd/scratch/ereport-automated-testing" ]]; then
    ECOPY_DIR="$HOME/orcd/scratch/ereport-automated-testing/ecopy"
  else
    ECOPY_DIR="$HOME/.cache/ereport/ecopy"
  fi
fi

if [[ ! -d "$ECOPY_DIR/.git" ]]; then
  # Clone into a temp sibling and rename, so a concurrent or interrupted
  # clone never leaves a half-checked-out tree at $ECOPY_DIR.
  tmp="$ECOPY_DIR.tmp.$$"
  rm -rf -- "$tmp"
  mkdir -p "$(dirname "$ECOPY_DIR")"
  log "cloning $ECOPY_REPO -> $ECOPY_DIR"
  if ! git clone --quiet "$ECOPY_REPO" "$tmp"; then
    rm -rf -- "$tmp"
    log "clone failed (no network on this host? pre-clone from a login node:"
    log "  git clone $ECOPY_REPO $ECOPY_DIR)"
    exit 1
  fi
  if [[ -n "${ECOPY_REF:-}" ]] && ! git -C "$tmp" checkout --quiet "$ECOPY_REF"; then
    rm -rf -- "$tmp"
    log "checkout of ECOPY_REF=$ECOPY_REF failed"
    exit 1
  fi
  if [[ -d "$ECOPY_DIR/.git" ]]; then
    rm -rf -- "$tmp"   # someone won the race while we cloned
  else
    mv -- "$tmp" "$ECOPY_DIR"
  fi
fi

bin="$ECOPY_DIR/edelete"
if [[ ! -x "$bin" ]]; then
  log "building edelete in $ECOPY_DIR"
  if ! make -C "$ECOPY_DIR" --quiet edelete >&2; then
    log "build failed; see above"
    exit 1
  fi
fi

if usable "$bin"; then
  echo "$bin"
  exit 0
fi
log "built binary is not runnable: $bin"
exit 1
