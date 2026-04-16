#!/usr/bin/env bash

set -euo pipefail

root="${1:-/data1}"
printf -v root_q '%q' "$root"

run_check() {
  local label=$1
  local cmd=$2

  printf '\n[%s]\n' "$label"
  time bash -o pipefail -lc "$cmd"
}

run_check "files_count" \
  "fd --hidden --no-ignore -t f . ${root_q} | wc -l"

run_check "dirs_count" \
  "fd --hidden --no-ignore -t d . ${root_q} | wc -l"

run_check "symlinks_count" \
  "fd --hidden --no-ignore -t l . ${root_q} | wc -l"

run_check "hardlink_files_count" \
  "find ${root_q} -type f -links +1 | wc -l"

run_check "hardlink_unique_regular_bytes" \
  "find ${root_q} -type f -links +1 -printf '%D:%i %s\\n' | sort -u | awk '{s+=\$2} END {printf \"%.0f\\n\", s+0}'"

run_check "regular_unique_bytes_ecrawl_semantics" \
  "find ${root_q} -type f -printf '%D:%i %s\\n' | sort -u | awk '{s+=\$2} END {printf \"%.0f\\n\", s+0}'"

run_check "dirs_apparent_bytes" \
  "find ${root_q} -type d -printf '%s\\n' | awk '{s+=\$1} END {printf \"%.0f\\n\", s+0}'"

run_check "symlinks_apparent_bytes" \
  "find ${root_q} -type l -printf '%s\\n' | awk '{s+=\$1} END {printf \"%.0f\\n\", s+0}'"

run_check "other_apparent_bytes" \
  "find ${root_q} ! -type f ! -type d ! -type l -printf '%s\\n' | awk '{s+=\$1} END {printf \"%.0f\\n\", s+0}'"

run_check "du_apparent_bytes" \
  "du -bs --apparent-size ${root_q}"
