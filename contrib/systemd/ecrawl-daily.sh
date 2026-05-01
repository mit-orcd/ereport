#!/usr/bin/env bash
# ecrawl-daily.sh — read ecrawl job list from a config file, run ecrawl for each tree,
# then rsync each job output_dir’s crawl data to RSYNC_DEST (optional). After each directory’s
# rsync succeeds, the same ecrawl artifact filenames are removed locally (see remove patterns below).
#
# RSYNC_DEST is the remote (or local) *parent* for crawl mirrors: output_dir /path/foo syncs to
# DEST/foo/ (basename of output_dir). Requires non-empty output_dir on each job line.
#
# On any non-zero exit, ecrawl artifact files under each touched output_dir are also deleted (by name:
# uid_shard_*.bin, uid_shard_*.bin.ckpt, crawl_manifest.txt, uid.txt, gid.txt). Cleanup only runs
# when the directory resolves with readlink -f to a real path that is not /, ., or ...
#
# Usage:
#   ecrawl-daily.sh [/path/to/ecrawl-daily.conf]
#
# Default config: /etc/ereport/ecrawl-daily.conf

set -uo pipefail

CONF=${1:-/etc/ereport/ecrawl-daily.conf}

ECRAWL_BIN=${ECRAWL_BIN:-}
RSYNC_DEST=${RSYNC_DEST:-}
RSYNC_RSH=${RSYNC_RSH:-}
RSYNC_DELETE=${RSYNC_DELETE:-0}

ECRAWL_DAILY_CLEANUP_OUTPUT_DIRS=()
CRAWL_RSYNC_OUTPUT_DIRS=()

die() {
	echo "ecrawl-daily: $*" >&2
	exit 1
}

register_cleanup_output_dir() {
	local d=$1 x
	[[ -n "$d" ]] || return 0
	for x in "${ECRAWL_DAILY_CLEANUP_OUTPUT_DIRS[@]}"; do
		[[ "$x" == "$d" ]] && return 0
	done
	ECRAWL_DAILY_CLEANUP_OUTPUT_DIRS+=("$d")
}

register_rsync_crawl_dir() {
	local d=$1 x
	[[ -n "$d" ]] || return 0
	for x in "${CRAWL_RSYNC_OUTPUT_DIRS[@]}"; do
		[[ "$x" == "$d" ]] && return 0
	done
	CRAWL_RSYNC_OUTPUT_DIRS+=("$d")
}

crawl_output_dir_safe_for_file_cleanup() {
	local raw=$1 canon noslash
	[[ -n "$raw" ]] || return 1
	case "$raw" in
	"/"|"."|".."|"/."|"/..") return 1 ;;
	esac
	noslash=${raw//\/}
	[[ -n "$noslash" ]] || return 1

	canon=$(readlink -f -- "$raw" 2>/dev/null) || return 1
	[[ -n "$canon" ]] || return 1
	case "$canon" in
	"/"|"."|"..") return 1 ;;
	esac
	[[ -d "$canon" ]] || return 1
	return 0
}

remove_ecrawl_artifact_files_in_dir() {
	local dir=$1 canon
	[[ -d "$dir" ]] || return 0
	if ! crawl_output_dir_safe_for_file_cleanup "$dir"; then
		echo "ecrawl-daily: refusing artifact cleanup under unsafe path: $dir" >&2
		return 0
	fi
	canon=$(readlink -f -- "$dir" 2>/dev/null) || return 0
	# Names produced by ecrawl for uid-sharded layout (see ecrawl.c).
	find "$canon" -maxdepth 1 -type f \( \
		-name 'uid_shard_*.bin' -o \
		-name 'uid_shard_*.bin.ckpt' -o \
		-name 'crawl_manifest.txt' -o \
		-name 'uid.txt' -o \
		-name 'gid.txt' \
		\) -delete 2>/dev/null || true
}

cleanup_failed_run_outputs() {
	local status=$?
	[[ $status -eq 0 ]] && return 0
	local d
	for d in "${ECRAWL_DAILY_CLEANUP_OUTPUT_DIRS[@]}"; do
		[[ -n "$d" && -d "$d" ]] || continue
		echo "ecrawl-daily: removing ecrawl artifact files after failure (exit $status) under: $d" >&2
		remove_ecrawl_artifact_files_in_dir "$d"
	done
}

set_directive() {
	local k=$1
	local v=$2
	case "$k" in
	ECRAWL_BIN) ECRAWL_BIN=$v ;;
	RSYNC_DEST) RSYNC_DEST=$v ;;
	RSYNC_RSH) RSYNC_RSH=$v ;;
	RSYNC_DELETE) RSYNC_DELETE=$v ;;
	*)
		echo "ecrawl-daily: ignoring unknown directive '$k' (allowed: ECRAWL_BIN, RSYNC_DEST, RSYNC_RSH, RSYNC_DELETE)" >&2
		;;
	esac
}

ssh_cmd_base() {
	if [[ -n "$RSYNC_RSH" ]]; then
		# shellcheck disable=SC2206
		echo $RSYNC_RSH
	else
		echo ssh
	fi
}

parse_rsync_ssh_dest() {
	local dest=$1
	# Absolute local paths must not be treated as host:path (avoids "/tmp:a" mis-parsing).
	[[ "${dest:0:1}" != "/" ]] || return 1
	if [[ "$dest" =~ ^(([^@]+)@)?([^:]+):(.*)$ ]]; then
		RSYNC_SSH_USER="${BASH_REMATCH[2]}"
		RSYNC_SSH_HOST="${BASH_REMATCH[3]}"
		RSYNC_REMOTE_PATH="${BASH_REMATCH[4]}"
		if [[ -n "$RSYNC_SSH_USER" ]]; then
			RSYNC_SSH_TARGET="${RSYNC_SSH_USER}@${RSYNC_SSH_HOST}"
		else
			RSYNC_SSH_TARGET="$RSYNC_SSH_HOST"
		fi
		return 0
	fi
	return 1
}

ensure_remote_deploy_parent() {
	local ssh_target=$1 parent=$2
	local q
	q=$(printf '%q' "$parent")
	# shellcheck disable=SC2206
	local ssh_cmd=( $(ssh_cmd_base) )
	echo "ecrawl-daily: mkdir -p (remote) ${ssh_target}:${parent}"
	"${ssh_cmd[@]}" "$ssh_target" "mkdir -p -- $q"
}

sync_crawl_outputs() {
	if [[ -z "$RSYNC_DEST" ]]; then
		echo "ecrawl-daily: RSYNC_DEST unset; skipping crawl output rsync."
		return 0
	fi
	if [[ ${#CRAWL_RSYNC_OUTPUT_DIRS[@]} -eq 0 ]]; then
		echo "ecrawl-daily: RSYNC_DEST set but no job output_dir paths registered (use a non-empty output_dir column); skipping crawl rsync." >&2
		return 0
	fi

	local -a rsync_cmd=(rsync -a)
	if [[ "$RSYNC_DELETE" == "1" || "$RSYNC_DELETE" == "yes" || "$RSYNC_DELETE" == "true" ]]; then
		rsync_cmd+=(--delete)
	fi
	if [[ -n "$RSYNC_RSH" ]]; then
		rsync_cmd+=(-e "$RSYNC_RSH")
	fi

	local remote_parent canon base dest
	if parse_rsync_ssh_dest "$RSYNC_DEST"; then
		remote_parent="${RSYNC_REMOTE_PATH%/}"
		ensure_remote_deploy_parent "$RSYNC_SSH_TARGET" "$remote_parent"
	else
		remote_parent="${RSYNC_DEST%/}"
		mkdir -p -- "$remote_parent" || die "cannot create RSYNC_DEST parent: $remote_parent"
	fi

	for local_dir in "${CRAWL_RSYNC_OUTPUT_DIRS[@]}"; do
		[[ -d "$local_dir" ]] || die "crawl output directory missing for rsync: $local_dir"
		canon=$(readlink -f -- "$local_dir" 2>/dev/null) || die "cannot resolve crawl output dir: $local_dir"
		base=$(basename -- "$canon")
		[[ -n "$base" ]] || die "invalid basename for crawl output dir: $local_dir"

		if parse_rsync_ssh_dest "$RSYNC_DEST"; then
			dest="${RSYNC_SSH_TARGET}:${remote_parent}/${base}/"
		else
			mkdir -p -- "${remote_parent}/${base}" || die "cannot create ${remote_parent}/${base}"
			dest="${remote_parent}/${base}/"
		fi
		echo "ecrawl-daily: rsync crawl output ${canon}/ -> ${dest}"
		if ! "${rsync_cmd[@]}" "${canon}/" "$dest"; then
			echo "ecrawl-daily: rsync failed for ${canon}" >&2
			return 1
		fi
		echo "ecrawl-daily: removing local crawl artifacts after successful rsync under ${canon}"
		remove_ecrawl_artifact_files_in_dir "$canon"
	done
}

main() {
	local jobs_fail=0 start_path output_dir record_root line section=directives key val
	local -a ecrawl_cmd job_lines

	[[ -r "$CONF" ]] || die "cannot read config: $CONF"
	trap cleanup_failed_run_outputs EXIT

	while IFS= read -r line || [[ -n "$line" ]]; do
		[[ "$line" =~ ^[[:space:]]*# ]] && continue
		[[ -z "${line//[$' \t']/}" ]] && continue
		if [[ "$line" == "---jobs---" ]]; then
			section=jobs
			continue
		fi
		if [[ "$section" == directives ]]; then
			if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
				key="${BASH_REMATCH[1]}"
				val="${BASH_REMATCH[2]}"
				val="${val#"${val%%[![:space:]]*}"}"
				val="${val%"${val##*[![:space:]]}"}"
				set_directive "$key" "$val"
			else
				echo "ecrawl-daily: skipping malformed directive line in $CONF" >&2
			fi
			continue
		fi
		if [[ "$line" != *$'\t'* ]]; then
			die "job lines must be tab-separated (start_path<TAB>output_dir<TAB>record_root): $line"
		fi
		job_lines+=("$line")
	done <"$CONF"

	if [[ -z "$ECRAWL_BIN" ]]; then
		if command -v ecrawl &>/dev/null; then
			ECRAWL_BIN=$(command -v ecrawl)
		else
			die "ECRAWL_BIN not set and ecrawl not in PATH"
		fi
	fi
	[[ -x "$ECRAWL_BIN" ]] || die "ecrawl binary not executable: $ECRAWL_BIN"

	if [[ ${#job_lines[@]} -eq 0 ]]; then
		echo "ecrawl-daily: no jobs after '---jobs---' in $CONF" >&2
		jobs_fail=1
	fi

	for line in "${job_lines[@]}"; do
		IFS=$'\t' read -r start_path output_dir record_root <<<"$line" || true
		start_path="${start_path#"${start_path%%[![:space:]]*}"}"
		start_path="${start_path%"${start_path##*[![:space:]]}"}"
		[[ -n "$start_path" ]] || die "empty start_path in job line"

		ecrawl_cmd=("$ECRAWL_BIN")
		record_root="${record_root#"${record_root%%[![:space:]]*}"}"
		record_root="${record_root%"${record_root##*[![:space:]]}"}"
		if [[ -n "$record_root" ]]; then
			ecrawl_cmd+=(--record-root "$record_root")
		fi
		ecrawl_cmd+=("$start_path")
		output_dir="${output_dir#"${output_dir%%[![:space:]]*}"}"
		output_dir="${output_dir%"${output_dir##*[![:space:]]}"}"
		if [[ -n "$output_dir" ]]; then
			mkdir -p -- "$output_dir" || die "cannot create output directory: $output_dir"
			register_cleanup_output_dir "$output_dir"
			register_rsync_crawl_dir "$output_dir"
			ecrawl_cmd+=("$output_dir")
		fi

		echo "ecrawl-daily: ${ecrawl_cmd[*]}"
		if "${ecrawl_cmd[@]}"; then
			:
		else
			local ec=$?
			echo "ecrawl-daily: ecrawl failed for start_path=$start_path (exit $ec)" >&2
			jobs_fail=1
		fi
	done

	sync_crawl_outputs || jobs_fail=1

	[[ "$jobs_fail" -eq 0 ]] || exit 1
}

main "$@"
