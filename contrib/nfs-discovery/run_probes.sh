#!/bin/bash
# Probe every host named by inventory.yml (or the hosts given as arguments):
# one clush fanout, desktop -> xcat -> root@<probe>, one snapshot per host
# under probe/.  Read-only on the filers; only probe.sh runs there.
set -uo pipefail
JUMPHOST=${JUMPHOST:-root@xcat}
cd "$(dirname "$0")"

if [ $# -gt 0 ]; then
    hosts=$(echo "$@" | tr ' ' ',')
else
    hosts=$(sed -n 's/^  \([A-Za-z0-9_.-]*\):$/\1/p' inventory.yml \
            | tr '\n' ',')
fi
hosts=${hosts%,}
[ -n "$hosts" ] || { echo "error: inventory.yml names no hosts" >&2; exit 1; }

mkdir -p probe

clush -R exec -f 8 -t 15 -u 120 -w "$hosts" --outdir probe \
    "ssh -o BatchMode=yes $JUMPHOST ssh -o BatchMode=yes -o ConnectTimeout=10 root@%host -- bash -s" \
    < probe.sh
