#!/usr/bin/env bash
#
# Helpers shared by the profilers in scripts/profile/. Source it, do not execute:
#   source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../lib" && pwd)/common.sh"
#
# Only genuinely identical code lives here. Each profiler keeps its own
# run_clean / run_perf / run_strace / run_sched / build_argv / profile_one,
# because those differ per tool by design.

# Locate a built binary: $VAR override, then ./<name>, /tmp/<name>, $PATH.
#
# Fails with return, not exit: this runs inside $(...), where exit would only end
# the subshell and leave the run going with an empty path, producing a
# structurally complete report full of rc=127. Callers must use || exit.
find_bin() {
  local name=$1 var=$2
  local v=${!var:-}
  if [[ -n "$v" ]]; then
    [[ -f "$v" && -x "$v" ]] || { echo "ERROR: $var '$v' is not an executable file" >&2; return 1; }
    echo "$v"; return 0
  fi
  # Tests -f as well as -x because a deploy dir can share the tool's name
  # (/tmp/ereport), and a directory is -x, so -x alone picks the dir as the binary.
  if   [[ -f "./$name" && -x "./$name" ]];   then echo "$(cd "$(dirname "./$name")" && pwd)/$name"
  elif [[ -f "/tmp/$name" && -x "/tmp/$name" ]]; then echo "/tmp/$name"
  elif command -v "$name" >/dev/null 2>&1; then command -v "$name"
  else echo "ERROR: cannot find $name; set $var=/path/to/$name" >&2; return 1
  fi
}

fs_type() { stat -f -c '%T' "$1" 2>/dev/null || echo "?"; }
shard_count() { find "$1" -maxdepth 1 -name 'uid_shard_*.bin' 2>/dev/null | wc -l; }

# Fixtures emitted by scripts/fixtures/generate-ecrawl-adversarial-tree.sh,
# ordered so the cheap/fast ones run before the multi-minute mega dirs.
KNOWN_FIXTURES=(
  deep_skinny_chain
  depth_slash_profile
  wide_shallow
  ereport_badge_fixtures
  neutral_flat
  single_huge_dir
  mega_dir2
  mega_dir1
)
