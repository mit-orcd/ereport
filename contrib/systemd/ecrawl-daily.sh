#!/usr/bin/env bash
# ecrawl-daily.sh — read ecrawl job list from a config file, run ecrawl for each tree,
# then rsync each job output_dir’s crawl data to RSYNC_DEST (optional).
#
# RSYNC_DEST is the remote (or local) *parent* for crawl mirrors: output_dir /path/foo syncs to
# DEST/foo/ (basename of output_dir). Requires non-empty output_dir on each job line.
#
# Optional EREPORT_INSTALL_DEST=host:/path repeats the old toolchain deploy: rsync ecrawl/ereport/…
# from EREPORT_SOURCE_DIR via staging + archive + promote (see RSYNC_STAGING_DIR_NAME).
#
# On any non-zero exit, ecrawl artifact files under each touched output_dir are deleted (by name:
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
EREPORT_SOURCE_DIR=${EREPORT_SOURCE_DIR:-}
RSYNC_DEST=${RSYNC_DEST:-}
EREPORT_INSTALL_DEST=${EREPORT_INSTALL_DEST:-}
RSYNC_RSH=${RSYNC_RSH:-}
RSYNC_DELETE=${RSYNC_DELETE:-0}
RSYNC_STAGING_DIR_NAME=${RSYNC_STAGING_DIR_NAME:-.ecrawl-daily-staging}
RSYNC_ARCHIVE_DIR_NAME=${RSYNC_ARCHIVE_DIR_NAME:-archive}
EREPORT_RSYNC_ALL=${EREPORT_RSYNC_ALL:-0}

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
	EREPORT_SOURCE_DIR) EREPORT_SOURCE_DIR=$v ;;
	RSYNC_DEST) RSYNC_DEST=$v ;;
	EREPORT_INSTALL_DEST) EREPORT_INSTALL_DEST=$v ;;
	RSYNC_RSH) RSYNC_RSH=$v ;;
	RSYNC_DELETE) RSYNC_DELETE=$v ;;
	RSYNC_STAGING_DIR_NAME) RSYNC_STAGING_DIR_NAME=$v ;;
	RSYNC_ARCHIVE_DIR_NAME) RSYNC_ARCHIVE_DIR_NAME=$v ;;
	EREPORT_RSYNC_ALL) EREPORT_RSYNC_ALL=$v ;;
	*)
		echo "ecrawl-daily: ignoring unknown directive '$k' (allowed: ECRAWL_BIN, EREPORT_SOURCE_DIR, EREPORT_RSYNC_ALL, RSYNC_DEST, EREPORT_INSTALL_DEST, RSYNC_RSH, RSYNC_DELETE, RSYNC_STAGING_DIR_NAME, RSYNC_ARCHIVE_DIR_NAME)" >&2
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

atomic_promote_remote_parent() {
	local ssh_target=$1 parent=$2
	local q q_st q_arch
	q=$(printf '%q' "$parent")
	q_st=$(printf '%q' "$RSYNC_STAGING_DIR_NAME")
	q_arch=$(printf '%q' "$RSYNC_ARCHIVE_DIR_NAME")
	# shellcheck disable=SC2206
	local ssh_cmd=( $(ssh_cmd_base) )
	"${ssh_cmd[@]}" "$ssh_target" "REMOTE_PARENT=$q STAGING=$q_st ARCHIVE=$q_arch bash -s" <<'EOS'
set -euo pipefail
P=$REMOTE_PARENT
STAGING_DIR=$STAGING
ARCHIVE_DIR=$ARCHIVE
staging="$P/$STAGING_DIR"
arch_root="$P/$ARCHIVE_DIR"
ts=$(date +%Y%m%d-%H%M%S)
[[ -d "$staging" ]] || { echo "ecrawl-daily(remote): staging missing: $staging" >&2; exit 1; }
mkdir -p "$arch_root/$ts"
while IFS= read -r -d '' item; do
	mv -- "$item" "$arch_root/$ts/"
done < <(find "$P" -mindepth 1 -maxdepth 1 ! -name "$ARCHIVE_DIR" ! -name "$STAGING_DIR" -print0)
while IFS= read -r -d '' item; do
	mv -- "$item" "$P/"
done < <(find "$staging" -mindepth 1 -maxdepth 1 -print0)
rmdir "$staging" 2>/dev/null || true
EOS
}

atomic_promote_local_parent() {
	local parent=$1
	local staging arch_root ts
	parent="${parent%/}"
	staging="$parent/$RSYNC_STAGING_DIR_NAME"
	arch_root="$parent/$RSYNC_ARCHIVE_DIR_NAME"
	ts=$(date +%Y%m%d-%H%M%S)
	[[ -d "$staging" ]] || die "staging directory missing: $staging"
	mkdir -p "$arch_root/$ts" || die "cannot create $arch_root/$ts"
	while IFS= read -r -d '' item; do
		mv -- "$item" "$arch_root/$ts/" || die "failed to archive: $item"
	done < <(find "$parent" -mindepth 1 -maxdepth 1 \
		! -name "$RSYNC_ARCHIVE_DIR_NAME" \
		! -name "$RSYNC_STAGING_DIR_NAME" \
		-print0)
	while IFS= read -r -d '' item; do
		mv -- "$item" "$parent/" || die "failed to promote: $item"
	done < <(find "$staging" -mindepth 1 -maxdepth 1 -print0)
	rmdir "$staging" 2>/dev/null || true
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
		"${rsync_cmd[@]}" "${canon}/" "$dest"
	done
}

sync_ereport_toolchain_install() {
	if [[ -z "$EREPORT_INSTALL_DEST" ]]; then
		echo "ecrawl-daily: EREPORT_INSTALL_DEST unset; skipping ereport toolchain install rsync."
		return 0
	fi
	[[ -n "$EREPORT_SOURCE_DIR" ]] || die "EREPORT_SOURCE_DIR must be set when EREPORT_INSTALL_DEST is set"
	[[ -d "$EREPORT_SOURCE_DIR" ]] || die "EREPORT_SOURCE_DIR is not a directory: $EREPORT_SOURCE_DIR"
	[[ -n "$RSYNC_STAGING_DIR_NAME" ]] || die "RSYNC_STAGING_DIR_NAME must not be empty"
	[[ -n "$RSYNC_ARCHIVE_DIR_NAME" ]] || die "RSYNC_ARCHIVE_DIR_NAME must not be empty"
	[[ "$RSYNC_STAGING_DIR_NAME" != "$RSYNC_ARCHIVE_DIR_NAME" ]] || die "staging and archive directory names must differ"

	local -a rsync_cmd=(rsync -a)
	if [[ "$RSYNC_DELETE" == "1" || "$RSYNC_DELETE" == "yes" || "$RSYNC_DELETE" == "true" ]]; then
		rsync_cmd+=(--delete)
	fi
	if [[ -n "$RSYNC_RSH" ]]; then
		rsync_cmd+=(-e "$RSYNC_RSH")
	fi

	local parent staging_dest
	if parse_rsync_ssh_dest "$EREPORT_INSTALL_DEST"; then
		parent="${RSYNC_REMOTE_PATH%/}"
		staging_dest="${RSYNC_SSH_TARGET}:${parent}/${RSYNC_STAGING_DIR_NAME}/"
		ensure_remote_deploy_parent "$RSYNC_SSH_TARGET" "$parent"
	else
		parent="${EREPORT_INSTALL_DEST%/}"
		staging_dest="${parent}/${RSYNC_STAGING_DIR_NAME}/"
		mkdir -p -- "$parent" || die "cannot create EREPORT_INSTALL_DEST parent: $parent"
	fi

	local -a rsync_sources=()
	if [[ "$EREPORT_RSYNC_ALL" == "1" || "$EREPORT_RSYNC_ALL" == "yes" || "$EREPORT_RSYNC_ALL" == "true" ]]; then
		rsync_sources=("$EREPORT_SOURCE_DIR/")
		echo "ecrawl-daily: rsync entire ${EREPORT_SOURCE_DIR}/ -> ${staging_dest} (staging)"
	else
		local name path
		for name in ecrawl ereport ereport_index ecrawl_repair edelete; do
			path="$EREPORT_SOURCE_DIR/$name"
			[[ -f "$path" ]] || die "missing built binary $path (run make in ereport, or set EREPORT_RSYNC_ALL=1 to rsync the whole directory)"
			rsync_sources+=("$path")
		done
		echo "ecrawl-daily: rsync ereport toolchain (${#rsync_sources[@]} files) from ${EREPORT_SOURCE_DIR}/ -> ${staging_dest} (staging)"
	fi
	rsync_cmd+=("${rsync_sources[@]}" "$staging_dest")

	"${rsync_cmd[@]}"

	echo "ecrawl-daily: promote staged ereport toolchain into ${EREPORT_INSTALL_DEST%%/}"
	if parse_rsync_ssh_dest "$EREPORT_INSTALL_DEST"; then
		atomic_promote_remote_parent "$RSYNC_SSH_TARGET" "$parent"
	else
		atomic_promote_local_parent "$parent"
	fi
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

	if [[ -z "$EREPORT_SOURCE_DIR" ]]; then
		EREPORT_SOURCE_DIR=$(dirname -- "$(readlink -f "$ECRAWL_BIN" 2>/dev/null || realpath "$ECRAWL_BIN" 2>/dev/null || echo "$ECRAWL_BIN")")
	fi

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
	sync_ereport_toolchain_install || jobs_fail=1

	[[ "$jobs_fail" -eq 0 ]] || exit 1
}

main "$@"
