#!/usr/bin/env bash
# crawl.sh — run ecrawl under a minimal landrun sandbox
#
# usage: ./crawl.sh <start-path> [output-dir] [extra ecrawl args...]
set -euo pipefail

ECRAWL=/usr/bin/ecrawl

command -v landrun >/dev/null || {
  curl -fsSLo /root/.local/bin/landrun https://github.com/Zouuup/landrun/releases/download/v0.1.13/landrun-linux-amd64
  chmod +x /root/.local/bin/landrun
}

if [ $# -lt 1 ]; then
  echo "usage: crawl.sh <start-path> [output-dir] [extra ecrawl args...]" >&2
  exit 2
fi

target=$1
out=${2:-/var/tmp/test-crawls}

shift
if [ $# -gt 0 ]; then shift; fi   # drop start-path + optional output-dir, keep extra args

target=$(readlink -f -- "$target")
[ -e "$target" ] || { echo "crawl.sh: target '$target' not found" >&2; exit 1; }

mkdir -p -- "$out"
out=$(readlink -f -- "$out")

exec landrun \
  --rox "$ECRAWL" \
  --rox /lib64 \
  --ro /etc/ld.so.cache \
  --ro "$target" \
  --rwx "$out" \
  --env ECRAWL_CRAWL_THREADS=8 --env ECRAWL_WRITER_THREADS=8  \
  -- "$ECRAWL" --progress "$target" "$out" "$@"
