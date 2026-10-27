#!/bin/bash
export LC_ALL=C
echo "##host";       hostname
echo "##df";         df -T | { IFS= read -r h && echo "$h"; sort -k7; }
echo "##mounts";     sort -k2 < /proc/mounts
echo "##exports";    showmount -e localhost 2>&1 | { IFS= read -r h && echo "$h"; sort -k1; }
echo "##zfs";        command -v zfs >/dev/null 2>&1 && zfs list -H -p -o name,mountpoint,used,avail | sort -k1
echo "##inodes";     df -i | { IFS= read -r h && echo "$h"; sort -k6; }
echo "##exportfile"; cat /etc/exports 2>/dev/null
