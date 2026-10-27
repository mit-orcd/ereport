#!/bin/bash
# discover.sh -- discover NFS servers, exports, and mounts from the cluster's
# automount LDAP tree; store them in storage_discovery.sqlite3; enrich the DB
# with per-filer filesystem facts: probe.sh snapshot per filer (clush fanout
# via run_probes.sh over the cluster's ssh trust chain) + offline parse.py.
set -euo pipefail
cd "$(dirname "$0")"                     # every artifact lives next to this script

AUTOMOUNT_SSH=${AUTOMOUNT_SSH:-root@core017}                 # node with LDAP access
AUTOMOUNT_BASEDN=${AUTOMOUNT_BASEDN:-ou=automount,dc=cm,dc=cluster}
LIMIT=""                                                     # --pull-facts hosts
TAGS=""                                                      # --pull-facts tags

require_uv() {
    command -v uv >/dev/null 2>&1 && return
    echo "--ldap: uv not found; installing uv into ~/.local/bin" >&2
    curl -fsSL https://astral.sh/uv/install.sh | sh >&2
    export PATH="$HOME/.local/bin:$PATH"
    command -v uv >/dev/null 2>&1 || { echo "error: uv install failed" >&2; exit 1; }
}

fetch_ldap() {                           # fresh read-only LDAP dump (idempotent)
    local tmp=raw-ldap-automount.ldif.$$
    echo "--ldap: querying $AUTOMOUNT_SSH $AUTOMOUNT_BASEDN ..." >&2
    ssh -o BatchMode=yes "$AUTOMOUNT_SSH" \
        ldapsearch -x -LLL -o ldif-wrap=no -o nettimeout=10 \
                   -b "$AUTOMOUNT_BASEDN" automountInformation > "$tmp"
    mv "$tmp" raw-ldap-automount.ldif
    echo "--ldap: $(grep -c '^automountInformation: ' raw-ldap-automount.ldif) mount entries" >&2
}

inventory_hosts() {                      # inventory.yml hosts, optional tag filter
    awk -v want="${1:-}" '
        BEGIN { n = split(want, wanted, ",") }
        /^  [A-Za-z0-9_.-]+:$/ { host = $1; sub(":", "", host); next }
        host != "" && /^    tags:/ {
            gsub(/[][ ]/, ""); sub("tags:", "")
            m = split($0, tags, ",")
            for (i = 1; i <= n; i++)
                for (j = 1; j <= m; j++)
                    if (wanted[i] == tags[j]) found = 1
            if (n == 0 || found) print host
            host = ""; found = 0
        }' inventory.yml
}

probe_facts() {                          # one clush fanout: snapshot per filer
    local hosts="" known
    if [ -n "$LIMIT" ]; then
        known=$(inventory_hosts)
        for host in $LIMIT; do
            grep -qx -- "$host" <<<"$known" ||
                { echo "error: $host is not a probe target in inventory.yml" >&2
                  exit 1; }
        done
        hosts=$LIMIT
        echo "--pull-facts: $LIMIT" >&2
    elif [ -n "$TAGS" ]; then
        hosts=$(inventory_hosts "$TAGS")
        [ -n "$hosts" ] ||
            { echo "error: no inventory host carries tag $TAGS" >&2; exit 1; }
        echo "--pull-facts: $(wc -w <<<"$hosts") hosts from inventory.yml" \
             "(tags: ${TAGS//,/, })" >&2
    else
        hosts=$(inventory_hosts | tr '\n' ' ')
        echo "--pull-facts: $(wc -w <<<"$hosts") hosts from inventory.yml" >&2
    fi
    ./run_probes.sh $hosts
}

case ${1:-} in
--ldap)
    require_uv
    fetch_ldap
    uv run parse_ldap.py
    ;;
--pull-facts)
    require_uv
    shift
    while [ $# -gt 0 ]; do
        case $1 in
            --limit) LIMIT="${2//,/ }"; shift 2 ;;
            --tag) TAGS="$2"; shift 2 ;;
            *) echo "error: unknown option $1" >&2; exit 1 ;;
        esac
    done
    probe_facts                          # bash only: dumb snapshot files
    uv run parse.py                      # offline parse -> DB
    ;;
*)
    echo "usage: discover.sh --ldap" >&2
    echo "       discover.sh --pull-facts [--tag T1,T2] [--limit HOST1,HOST2]" >&2
    echo "  --tag   select inventory hosts by tag (hdd, fstor, zfs, ...)" >&2
    echo "  --limit exact probe targets; wins over --tag" >&2
    exit 1
    ;;
esac
