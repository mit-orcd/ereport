#!/bin/bash
# Probe one filer, read-only.  The ##marker lines are parse.py's contract:
# change them on both sides together.  ##ok appears only when every fatal
# section succeeded, so parse.py can tell a broken probe from a filer with
# nothing mounted instead of clearing facts.
export LC_ALL=C
set -o pipefail

ok=1
mark() { echo "##error $1"; ok=0; }          # fatal: facts must not be written
rows() { IFS= read -r h && { echo "$h"; sort "-k$1"; }; }  # keep header, sort body

echo "##host";       hostname || mark host
echo "##df";         df -T | rows 7 || mark df
echo "##mountinfo";  cat /proc/self/mountinfo || mark mountinfo
echo "##mounts";     sort -k2 < /proc/mounts || mark mounts
echo "##exports";    showmount -e localhost 2>&1 | rows 1 || mark exports
echo "##exportopts"; exportfs -v 2>&1 || true          # corroboration only
echo "##zfs";        if command -v zfs >/dev/null 2>&1; then
                         zfs list -H -p -o name,mountpoint,used,avail,quota,mounted \
                             | sort -k1 || mark zfs
                     fi
echo "##zpool";      if command -v zpool >/dev/null 2>&1; then
                         zpool status 2>&1 || true       # corroboration only
                     fi
echo "##inodes";     df -i | rows 6 || mark inodes
echo "##exportfile"; cat /etc/exports 2>/dev/null || true
[ "$ok" = 1 ] && echo "##ok"
