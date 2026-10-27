#!/usr/bin/env python3
"""Parse the automount LDAP dump into storage_discovery.sqlite3.

Input : raw-ldap-automount.ldif     (written by discover.sh --ldap)
Output: storage_discovery.sqlite3, table shares

One row per automount share. The filer is stored as the clean host name
(hostlist.clean_host), so 'core009.ib' and 'fstor010-p2.ib' become
'core009' and 'fstor010' - the same name probe/<host> snapshots use.
A rebuild keeps every fact column already collected and leaves new rows NULL.
New filers are added to inventory.yml; missing ones are kept.
"""
import sqlite3
import sys
from pathlib import Path

import hostlist

HERE = Path(__file__).resolve().parent
LDIF = HERE / "raw-ldap-automount.ldif"
DB = HERE / "storage_discovery.sqlite3"

SCHEMA = """
CREATE TABLE IF NOT EXISTS shares (
    host       TEXT NOT NULL,      -- clean host name
    share      TEXT NOT NULL,      -- path the filer exports
    mountpoint TEXT NOT NULL,      -- client-side path exposing the share
    backing    TEXT,               -- fs type behind the share         (facts)
    usage      INTEGER,            -- capacity in use, %              (facts)
    local_fs   TEXT,               -- filer-local path carrying it     (facts)
    size       INTEGER,            -- fs capacity behind it, bytes     (facts)
    nfiles     INTEGER,            -- files in use on that fs          (facts)
    pool       TEXT,               -- zfs pool of the backing fs       (facts)
    PRIMARY KEY (host, share, mountpoint)
) WITHOUT ROWID
"""


def records(text):
    """Yield {attribute: value} for each LDIF entry (folded values joined)."""
    entry, key = {}, None
    for line in text.splitlines() + [""]:
        if line.startswith(" ") and key:          # continuation of a value
            entry[key] += line[1:]
        elif line:
            key, _, entry[key] = line.partition(": ")
        elif entry:
            yield entry
            entry, key = {}, None


def mountpoint(dn):
    """Client-side path from a DN's leaf key and map container:
       cn=001,ou=auto.orcd.data.ase,... -> /orcd/data/ase/001
       cn=/programs,ou=auto.direct,...  -> /programs"""
    parts = dn.split(",")
    if not parts[0].startswith("cn="):
        return None
    key = parts[0][len("cn="):]
    if key.startswith("/"):
        return key
    if len(parts) < 2 or not parts[1].startswith("ou=auto."):
        return None
    return "/" + parts[1][len("ou=auto."):].replace(".", "/") + "/" + key


def endpoints(info):
    """(filer, exported path) for each 'filer:/path' token; options and
    nested-autofs map references are skipped."""
    return [(filer, path) for filer, _, path in
            (token.partition(":") for token in info.split())
            if filer and not filer.startswith("-") and path.startswith("/")]


def main():
    if not LDIF.exists():
        sys.exit(f"error: {LDIF} missing - run discover.sh --ldap first")
    rows = set()
    for entry in records(LDIF.read_text()):
        where = mountpoint(entry.get("dn", ""))
        if where and "automountInformation" in entry:
            rows.update((hostlist.clean_host(host), share, where)
                        for host, share in endpoints(entry["automountInformation"]))

    con = sqlite3.connect(DB)
    try:
        con.execute(SCHEMA)
        facts = {(h, s, m): rest for h, s, m, *rest in con.execute(
            "SELECT host, share, mountpoint, backing, usage, local_fs,"
            " size, nfiles, pool FROM shares")}
        con.execute("DELETE FROM shares")
        con.executemany(
            "INSERT INTO shares VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(h, s, m, *facts.get((h, s, m), (None,) * 6))
             for h, s, m in sorted(rows)])
        con.commit()
        known = hostlist.load() if hostlist.INVENTORY.exists() else {}
        ldap = {host for (host,) in con.execute(
            "SELECT DISTINCT host FROM shares")}
        added = sorted(ldap - set(known))
        for host in ldap:
            known[host] = hostlist.merge_tags(host, known.get(host, []))
        hostlist.save(known)
        stale = sorted(set(known) - ldap)
        count, hosts, with_facts = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT host),"
            " COALESCE(SUM(backing IS NOT NULL), 0) FROM shares").fetchone()
        print(f"shares: {count} rows, {hosts} hosts, {with_facts} with facts")
        print(f"inventory: {len(known)} hosts")
        if added:
            print(f"inventory: added {', '.join(added)}")
        if stale:
            print(f"inventory: kept {len(stale)} without shares:"
                  f" {', '.join(stale)}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
