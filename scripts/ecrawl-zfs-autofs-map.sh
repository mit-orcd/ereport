#!/usr/bin/env bash
#
# ecrawl-zfs-autofs-map.sh -- on a ZFS/NFS server, map its datasets to the paths the cluster mounts
# them at, as ereport --path-rewrite OLD=NEW rules.
#
# The autofs maps live in LDAP (Bright: ou=automount,dc=cm,dc=cluster). auto.master names the top-level
# mount points (/orcd -> auto.orcd, /- -> auto.direct ...); nested maps (-fstype=autofs ... ldap:ou=...)
# add one component each; the leaves are NFS entries "host:/export". Walking that graph gives, for
# every export of this host, the absolute path a cluster node sees it at:
#
#     cn=002,ou=auto.orcd.data.dandi   -fstype=nfs,... hstor004-n2:/group/dandi/002
#         -> /orcd/data/dandi/002
#
# The local side comes from `zfs list`: the export /group/dandi/002 is the dataset whose mountpoint
# ends in /group/dandi/002 (/data2/group/dandi/002), or, when exportfs shows an NFSv4 pseudo-root
# (fsid=0), <root>/group/dandi/002. Each pair becomes one rule:
#
#     /data2/group/dandi/002=/orcd/data/dandi/002
#
# Usage:
#   scripts/ecrawl-zfs-autofs-map.sh                     # rules, one per line, on stdout
#   scripts/ecrawl-zfs-autofs-map.sh --args              # "--path-rewrite OLD=NEW ..." for a command line
#   scripts/ecrawl-zfs-autofs-map.sh --tsv               # dataset, mountpoint, export, autofs path
#   scripts/ecrawl-zfs-autofs-map.sh --write CRAWL_DIR   # CRAWL_DIR/path_rewrites.txt (ereport reads it)
#
# Options:
#   --host NAME[,ALIAS...]  host as it appears in the LDAP entries (default: `hostname -s` with a
#                           trailing -mgmt removed; NAME.ib and NAME.<domain> always match too)
#   --base DN               LDAP base (default: BASE from /etc/openldap/ldap.conf, else dc=cm,dc=cluster)
#   --ldap-uri URI          LDAP server (default: the client configuration)
#   --allow-missing         exit 0 even when an export of this host matched no dataset
#   --ldif FILE             parse this ldapsearch output instead of querying (testing / offline)
#   --zfs-list FILE         parse this `zfs list -H -o name,mountpoint -r` output instead of running it
#   --exports FILE          parse this `exportfs -v` output instead of running it (empty file: none)
#
# Exit status: 0 with every export mapped, 1 when one was not (unless --allow-missing), 2 on usage or
# tool errors. Datasets no autofs entry points at (the pool root, scratch) are listed as comments so
# the unmapped part of the namespace is visible; they keep their local path in the report.
#
# Run as any user for stdout; root (or write access to the crawl directory) for --write.
#
# SPDX-License-Identifier: MIT

set -euo pipefail

prog=${0##*/}
mode=rules
write_dir=
host_spec=
base=
ldap_uri=
allow_missing=0
ldif_file=
zfs_file=
exports_file=

usage() {
	sed -n '2,/^# SPDX/p' "$0" | sed -e 's/^# \{0,1\}//' -e '/^SPDX/d' >&2
	exit 2
}

die() {
	echo "$prog: $*" >&2
	exit 2
}

while [[ $# -gt 0 ]]; do
	case $1 in
	--args) mode=args ;;
	--tsv) mode=tsv ;;
	--write)
		[[ $# -ge 2 ]] || die "--write needs a crawl directory"
		mode=write
		write_dir=$2
		shift
		;;
	--host)
		[[ $# -ge 2 ]] || die "--host needs a name"
		host_spec=$2
		shift
		;;
	--base)
		[[ $# -ge 2 ]] || die "--base needs a DN"
		base=$2
		shift
		;;
	--ldap-uri)
		[[ $# -ge 2 ]] || die "--ldap-uri needs a URI"
		ldap_uri=$2
		shift
		;;
	--allow-missing) allow_missing=1 ;;
	--ldif)
		[[ $# -ge 2 ]] || die "--ldif needs a file"
		ldif_file=$2
		shift
		;;
	--zfs-list)
		[[ $# -ge 2 ]] || die "--zfs-list needs a file"
		zfs_file=$2
		shift
		;;
	--exports)
		[[ $# -ge 2 ]] || die "--exports needs a file"
		exports_file=$2
		shift
		;;
	-h | --help) usage ;;
	*) die "unknown argument: $1 (try --help)" ;;
	esac
	shift
done

# --- host aliases -------------------------------------------------------------------------------
if [[ -z $host_spec ]]; then
	host_spec=$(hostname -s 2>/dev/null || hostname)
	host_spec=${host_spec%-mgmt}
fi
# comma-separated aliases; the first is the canonical name used in messages
host_main=${host_spec%%,*}

# --- LDAP ---------------------------------------------------------------------------------------
if [[ -z $base ]]; then
	if [[ -r /etc/openldap/ldap.conf ]]; then
		base=$(awk 'toupper($1)=="BASE"{print $2; exit}' /etc/openldap/ldap.conf || true)
	fi
	[[ -n $base ]] || base="dc=cm,dc=cluster"
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/ecrawl-zfs-autofs-map.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

if [[ -n $ldif_file ]]; then
	cp -- "$ldif_file" "$tmp/automount.ldif"
else
	command -v ldapsearch >/dev/null 2>&1 || die "ldapsearch not found (openldap-clients)"
	ldap_args=(-LLL -o ldif-wrap=no -x)
	[[ -n $ldap_uri ]] && ldap_args+=(-H "$ldap_uri")
	if ! ldapsearch "${ldap_args[@]}" -b "ou=automount,$base" \
		'(|(objectClass=automount)(objectClass=automountMap))' dn automountInformation \
		>"$tmp/automount.ldif" 2>"$tmp/ldap.err"; then
		cat "$tmp/ldap.err" >&2
		die "ldapsearch failed (base ou=automount,$base)"
	fi
fi

# --- ZFS ----------------------------------------------------------------------------------------
if [[ -n $zfs_file ]]; then
	cp -- "$zfs_file" "$tmp/zfs.tsv"
else
	command -v zfs >/dev/null 2>&1 || die "zfs not found; is this a ZFS server? (or pass --zfs-list FILE)"
	zfs list -H -o name,mountpoint -t filesystem -r >"$tmp/zfs.tsv" 2>"$tmp/zfs.err" || {
		cat "$tmp/zfs.err" >&2
		die "zfs list failed"
	}
fi

# --- NFS exports (optional: pseudo-root for NFSv4) ---------------------------------------------
if [[ -n $exports_file ]]; then
	cp -- "$exports_file" "$tmp/exports.txt"
elif command -v exportfs >/dev/null 2>&1; then
	exportfs -v >"$tmp/exports.txt" 2>/dev/null || : >"$tmp/exports.txt"
else
	: >"$tmp/exports.txt"
fi

# --- resolve ------------------------------------------------------------------------------------
# One awk pass: parse the LDIF into map/key -> information, walk auto.master down through the nested
# maps, keep the NFS leaves of this host, match each export to a dataset mountpoint, print TSV rows:
#   L <dataset> <mountpoint> <export> <autofs path>      mapped
#   M - - <export> <autofs path>                          export with no (or an ambiguous) dataset
#   U <dataset> <mountpoint> - -                          dataset no autofs entry of this host points at
awk -v host_spec="$host_spec" -v ldif="$tmp/automount.ldif" -v zfs="$tmp/zfs.tsv" -v exports="$tmp/exports.txt" '
function trim(s) { sub(/^[ \t]+/, "", s); sub(/[ \t]+$/, "", s); return s }

# "ldap:ou=auto.orcd,ou=automount,dc=..." or "ldap:auto.orcd" or "auto.orcd" -> map name
function map_of(info,    m, t, n, i) {
	n = split(info, t, /[ \t]+/)
	for (i = 1; i <= n; i++) {
		if (t[i] ~ /^-/) continue
		m = t[i]
		sub(/^ldap:/, "", m)
		sub(/^\/\/[^\/]*\//, "", m)          # ldap://server/ou=...
		if (match(m, /ou=[^,]+/)) return substr(m, RSTART + 3, RLENGTH - 3)
		if (m ~ /^auto\./) return m
	}
	return ""
}

function host_ok(h,    i) {
	for (i = 1; i <= nalias; i++) {
		if (h == alias[i] || h == alias[i] ".ib" || index(h, alias[i] ".") == 1) return 1
	}
	return 0
}

function joinp(prefix, key) {
	if (key ~ /^\//) return key
	if (prefix == "/" || prefix == "") return "/" key
	return prefix "/" key
}

# Walk map `m`, mounted at `prefix`. Leaves: every host:/export token of an NFS entry.
function walk(m, prefix, depth,    k, info, n, t, i, h, e, p, sub_) {
	if (depth > 16 || (m, prefix) in seen) return
	seen[m, prefix] = 1
	for (k in keys) {
		split(k, kk, SUBSEP)
		if (kk[1] != m) continue
		info = ent[k]
		p = joinp(prefix, kk[2])
		if (info ~ /-fstype=autofs/) {
			sub_ = map_of(info)
			if (sub_ != "") walk(sub_, p, depth + 1)
			continue
		}
		n = split(info, t, /[ \t]+/)
		for (i = 1; i <= n; i++) {
			if (t[i] ~ /^-/) continue
			if (t[i] !~ /^[A-Za-z0-9._-]+:\//) continue
			h = t[i]; sub(/:.*/, "", h)
			e = t[i]; sub(/^[^:]*:/, "", e)
			if (h == "" || !host_ok(h)) continue
			sub(/\/+$/, "", e); if (e == "") e = "/"
			leaf[e] = p
		}
	}
}

BEGIN {
	nalias = split(host_spec, alias, /,/)

	# LDIF -> ent[map, key] = automountInformation
	map = ""; key = ""
	while ((getline line < ldif) > 0) {
		if (line ~ /^dn: /) {
			dn = substr(line, 5); map = ""; key = ""
			if (match(dn, /^cn=[^,]*(\\,[^,]*)*/)) { key = substr(dn, 4, RLENGTH - 3); gsub(/\\,/, ",", key) }
			rest = dn; sub(/^cn=[^,]*(\\,[^,]*)*,/, "", rest)
			if (match(rest, /^ou=[^,]+/)) map = substr(rest, 4, RLENGTH - 3)
		} else if (line ~ /^dn:: /) {
			printf "W base64 dn skipped: %s\n", substr(line, 6) > "/dev/stderr"; map = ""; key = ""
		} else if (line ~ /^automountInformation: / && map != "" && key != "") {
			ent[map, key] = trim(substr(line, 23)); keys[map, key] = 1
		}
	}
	close(ldif)

	# auto.master (direct maps under /-, indirect under their mount point)
	found_master = 0
	for (k in keys) {
		split(k, kk, SUBSEP)
		if (kk[1] != "auto.master") continue
		found_master = 1
		sub_ = map_of(ent[k])
		if (sub_ == "") continue
		if (kk[2] == "/-") walk(sub_, "", 1)
		else walk(sub_, kk[2], 1)
	}
	if (!found_master) print "W no auto.master entries found under the LDAP base; nothing to map" > "/dev/stderr"

	# datasets
	nds = 0
	while ((getline line < zfs) > 0) {
		n = split(line, f, /\t/)
		if (n < 2) continue
		if (f[2] == "none" || f[2] == "legacy" || f[2] == "-" || f[2] !~ /^\//) continue
		mp = f[2]; sub(/\/+$/, "", mp); if (mp == "") mp = "/"
		nds++; ds_name[nds] = f[1]; ds_mp[nds] = mp; mp_ds[mp] = f[1]
	}
	close(zfs)

	# NFSv4 pseudo-root: the export carrying fsid=0 / fsid=root
	root = ""
	while ((getline line < exports) > 0) {
		if (line ~ /^[ \t]/ || line !~ /^\//) continue
		if (line ~ /fsid=(0|root)[,)]/) { root = line; sub(/[ \t].*/, "", root); sub(/\/+$/, "", root); break }
	}
	close(exports)

	# match exports to mountpoints
	n_missing = 0
	for (e in leaf) {
		hit = ""; nhit = 0
		if (root != "" && ((root e) in mp_ds)) { hit = root e; nhit = 1 }
		if (nhit == 0) {
			for (i = 1; i <= nds; i++) {
				mp = ds_mp[i]
				if (mp == e) { hit = mp; nhit = 1; break }
				# e starts with "/", so a suffix match is already on a component boundary
				if (length(mp) > length(e) && substr(mp, length(mp) - length(e) + 1) == e) {
					hit = mp; nhit++
				}
			}
		}
		if (nhit == 1) { used[hit] = 1; printf "L\t%s\t%s\t%s\t%s\n", mp_ds[hit], hit, e, leaf[e] }
		else { n_missing++; printf "M\t-\t-\t%s\t%s\t%s\n", e, leaf[e], (nhit > 1 ? "ambiguous" : "no dataset") }
	}
	for (i = 1; i <= nds; i++) if (!(ds_mp[i] in used)) printf "U\t%s\t%s\t-\t-\n", ds_name[i], ds_mp[i]
}' | sort -t "$(printf '\t')" -k1,1 -k3,3 >"$tmp/rows.tsv"

n_rules=$(awk -F'\t' '$1=="L"' "$tmp/rows.tsv" | wc -l)
n_missing=$(awk -F'\t' '$1=="M"' "$tmp/rows.tsv" | wc -l)

if [[ $n_rules -eq 0 && $n_missing -eq 0 ]]; then
	echo "$prog: no autofs entry in LDAP names host $host_spec (try --host)" >&2
fi

emit_comments() {
	awk -F'\t' -v host="$host_main" '
		$1=="M" { printf "# unmapped export %s:%s (%s) -> %s\n", host, $4, $6, $5 }
		$1=="U" { printf "# no autofs entry for dataset %s at %s\n", $2, $3 }' "$tmp/rows.tsv"
}

emit_rules() {
	awk -F'\t' '$1=="L" { print $3 "=" $5 }' "$tmp/rows.tsv"
}

case $mode in
rules)
	emit_comments
	emit_rules
	;;
args)
	emit_comments >&2
	awk -F'\t' '$1=="L" { printf "%s--path-rewrite %s=%s", (n++ ? " " : ""), $3, $5 } END { if (n) printf "\n" }' "$tmp/rows.tsv"
	;;
tsv)
	printf 'dataset\tmountpoint\texport\tautofs_path\n'
	awk -F'\t' '$1=="L" { printf "%s\t%s\t%s\t%s\n", $2, $3, $4, $5 }' "$tmp/rows.tsv"
	emit_comments >&2
	;;
write)
	[[ -d $write_dir ]] || die "$write_dir is not a directory"
	[[ -f $write_dir/crawl_manifest.txt ]] || die "$write_dir has no crawl_manifest.txt (not an ecrawl output directory)"
	manifest_host=$(awk -F= '$1=="hostname"{print $2; exit}' "$write_dir/crawl_manifest.txt" || true)
	if [[ -n $manifest_host && $manifest_host != "$host_main" && $manifest_host != "$host_main-mgmt" ]]; then
		echo "$prog: warning: crawl_manifest.txt says hostname=$manifest_host, mapping host $host_main" >&2
	fi
	out=$write_dir/path_rewrites.txt
	{
		echo "# ereport --path-rewrite rules for crawl directory $write_dir"
		echo "# generated by $prog on $(hostname) at $(date -u +%Y-%m-%dT%H:%M:%SZ) for host $host_main"
		echo "# OLD=NEW per line: local dataset mountpoint -> cluster autofs path"
		emit_comments
		emit_rules
	} >"$out.tmp"
	mv -f -- "$out.tmp" "$out"
	echo "$prog: wrote $n_rules rule(s) to $out" >&2
	;;
esac

if [[ $n_missing -gt 0 ]]; then
	echo "$prog: $n_missing export(s) of $host_main matched no dataset (see comments)" >&2
	[[ $allow_missing -eq 1 ]] || exit 1
fi
exit 0
