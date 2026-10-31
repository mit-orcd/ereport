#!/usr/bin/env python3
"""Parse probe/<host> snapshots (from probe.sh) into
storage_discovery.sqlite3 - purely local and offline; probing is done by
discover.sh via run_probes.sh (clush fanout) running probe.sh over ssh.

Column rules (never guessed, never defaulted):
    backing    fs kind behind the share, or the original export for
               mount-table-backed re-exports, or 're-export'
    usage      capacity in use, %
    local_fs   filer-local path/remote that actually carries the share;
               the share path itself when that path is the local mountpoint
               (NULL only when the share is unresolved)
    size       capacity of the fs carrying the share, bytes: df total, or
               zfs used+avail when only a dataset matched (zfs avail is
               pool-wide on un-quotaed datasets; df wins wherever a df
               row matched)
    nfiles     files in use on that fs: df -i IUsed.  Re-exports carry
               none; stage-3 shares inherit their ancestor fs, exactly
               like usage
    pool       zfs pool of the fs that carries the share: first segment
               of the dataset's name in the ##zfs table (data1/amins ->
               data1); NULL whenever that fs is not zfs or no ##zfs row
               names its mountpoint
    leaf       1 when no other share's local path sits beneath this one,
               so walking it never revisits another row's data; 0 when one
               does; NULL when the share has no local path (re-export or
               unresolved)
    crawl      'y' when this row's bytes are not read by another row,
               'n' when they are: a duplicate carrier (same device and
               relative path, e.g. /data1 and a bind of its root) or a
               subtree of a crawled whole-fs row.  A scan filters on
               crawl = 'y' and never reads the same tree twice.

Decision rules above the stages:
    A bare 'host:/' automount rides the filer's single fsid=0 pseudo-root
    frame ('##exportfile'), never the OS root df row behind it.  A share
    whose server-side path is unattested (no exact row, no mounted rows
    beneath it, no fsid=0-framed re-root) stays NULL instead of guessing.
    A fresh snapshot that cannot resolve a share clears its stored facts.
"""
import sqlite3
import sys
from pathlib import Path

import hostlist

HERE = Path(__file__).resolve().parent
DB = HERE / "storage_discovery.sqlite3"
PROBE_DIR = HERE / "probe"


def sections(text):
    """Split the '##marker' delimited probe output into {name: body}."""
    result = {}
    current, body = None, []
    for line in text.splitlines() + ["##end"]:     # sentinel flushes the last
        if line.startswith("##"):
            if current is not None:
                result[current] = "\n".join(body).strip()
            current, body = line[2:], []
        else:
            body.append(line)
    return result


def norm(path):
    """Single slashes, no trailing slash except for '/' itself."""
    while "//" in path:
        path = path.replace("//", "/")
    return path.rstrip("/") or "/"


def unescape(field):
    """Kernel mount tables escape space, tab, newline and backslash as
    \\ooo; a mountpoint is one token, so turn them back into bytes."""
    return (field.replace("\\040", " ").replace("\\011", "\t")
                 .replace("\\012", "\n").replace("\\134", "\\"))


def inside(parent, child):
    """True when child is strictly beneath parent (path boundaries; '/'
    contains every other path)."""
    if parent == "/":
        return child != "/"
    return child != parent and child.startswith(parent + "/")


def suffix_of(cand, path):
    """True when path ends with cand at a path boundary (or equals it)."""
    return path == cand or (path.endswith(cand)
                            and path[len(path) - len(cand) - 1] == "/")


def df_devices(section):
    """{mountpoint: device} from the raw '##df' section."""
    devices = {}
    for line in section.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 7:
            continue
        devices["/".join(fields[6:]) or "/"] = fields[0]
    return devices


def mount_pairs(section):
    """(device, mountpoint, fstype) for device-backed mounts from
    '##mounts'.  df -T omits additional mountpoints of a device it
    already listed (the /compute_orcd_00N frames); those carry the
    device's own fs facts."""
    rows = []
    for line in section.splitlines():
        fields = line.split()
        if fields and fields[0].startswith("/dev/") and len(fields) >= 3:
            rows.append((fields[0], fields[1], fields[2]))
    return rows


def mountinfo(section):
    """{mountpoint: (device, fsroot)} from '##mountinfo'.

    device is the zfs dataset name, otherwise major:minor - every mount of
    the same filesystem carries the same pair.  fsroot is the subtree the
    mount exposes, so a bind of /data1/compute/x is ('/', '/data1') with
    fsroot /compute/x rather than a second whole-fs mount.  Empty when the
    snapshot predates the marker, and identity falls back to the path."""
    info = {}
    for line in section.splitlines():
        fields = line.split()
        if "-" not in fields:
            continue
        sep = fields.index("-")
        if sep < 3 or len(fields) < sep + 3:
            continue
        fstype, source = fields[sep + 1], unescape(fields[sep + 2])
        info[unescape(fields[4])] = (
            source if fstype == "zfs" else fields[2], norm(unescape(fields[3])))
    return info


def fs_path(fsroot, local_fs, base):
    """The path inside its filesystem that a share's carrier names.
    'fsroot' is where the mount sits in that filesystem, '(local_fs - base)'
    the share's offset from the mountpoint."""
    rel = norm(local_fs)[len(norm(base)):] if local_fs != base else ""
    return norm(fsroot.rstrip("/") + "/" + rel.lstrip("/"))


def carrier_key(local_fs, base, mi):
    """(device, path inside it) naming the tree a share serves.

    Equal tuples mean the same bytes: /data1 and a bind of the fs root are
    both (dev, '/'), a bind of /data1/compute/x is (dev, '/compute/x').
    Without a ##mountinfo row for the matched mountpoint the fallback is
    the carrier path itself, which is all a pre-v2 snapshot attests."""
    if base is None or base not in mi:
        return ("path", norm(wildcard_base(local_fs)))
    device, fsroot = mi[base]
    return (device, fs_path(fsroot, local_fs, base))


def fsid0_roots(section):
    """NFSv4 pseudo-root paths from '##exportfile': export lines whose
    options carry fsid=0.  A bare 'host:/' automount resolves against the
    single such frame; several frames stay ambiguous."""
    roots = set()
    for line in section.splitlines():
        if line.lstrip().startswith("#"):
            continue
        fields = line.split()
        if fields and fields[0].startswith("/") \
                and "fsid=0" in " ".join(fields[1:]):
            roots.add(fields[0])
    return sorted(roots)


def df_rows(section):
    """(mountpoint, fstype, used%, total-bytes) rows from 'df -T'."""
    rows = []
    for line in section.splitlines()[1:]:          # skip the header
        fields = line.split()
        if len(fields) < 7:
            continue
        try:
            usage = int(fields[5].rstrip("%"))
        except ValueError:
            usage = None
        try:
            total = int(fields[2]) * 1024          # 1K blocks -> bytes
        except ValueError:
            total = None
        rows.append(("/".join(fields[6:]) or "/", fields[1], usage, total))
    return rows


def inode_rows(section):
    """{mountpoint: files in use} from the 'df -i' section ('##inodes').

    Positional IUsed (column 3): the IUse% field may be '-'.  df wraps
    long device names onto their own continuation line; those merge with
    the row that follows."""
    inodes = {}
    wrapped = ""
    for line in section.splitlines()[1:]:          # skip the header
        fields = line.split()
        if len(fields) == 1:                       # bare wrapped device name
            wrapped = fields[0]
            continue
        if wrapped and len(fields) < 6:
            fields = [wrapped] + fields            # continuation row body
        wrapped = ""
        try:
            inodes["/".join(fields[5:]) or "/"] = int(fields[2])
        except (ValueError, IndexError):
            continue
    return inodes


def exports(section):
    """(export_path, client_list) pairs from the showmount section.

    The runtime export table equals /etc/exports + mounts injected on the
    fly (ZFS sharenfs, runtime-managed legacy filers): root frames carry
    CIDR client lists, injected per-dataset shares carry explicit hosts."""
    pairs = []
    for line in section.splitlines()[1:]:          # skip the header
        fields = line.split()
        if fields and fields[0].startswith("/"):
            pairs.append((fields[0], " ".join(fields[1:])))
    return pairs


def broad_root(pairs):
    """Most permissive exported path (its client list contains CIDRs).

    Root frames are the fsid=0/crossmnt stanza from /etc/exports; injected
    per-dataset shares list explicit hosts, not CIDRs, so they never win
    this pick (the export root is the re-rooting pivot for share paths)."""
    roots = [p for p, clients in pairs if "/" in clients]
    return min(roots, key=len) if roots else None


def zfs_map(section):
    """(name, mountpoint, used, avail) triples from the zfs section."""
    parsed = []
    for line in section.splitlines():
        fields = line.split("\t") if "\t" in line else line.split()
        if len(fields) < 4:
            continue
        try:
            parsed.append((fields[0], fields[1], int(fields[2]), int(fields[3])))
        except ValueError:
            continue
    return parsed


def pool_of(zfsm, mountpoint, fstype):
    """Pool name of one zfs fs from the '##zfs' map: the first segment
    of the dataset's name (data1/system/ldap -> data1).  Only a zfs
    backing carries a pool; exact mountpoint match only - no attested
    row, no pool."""
    if mountpoint is None or fstype != "zfs":
        return None
    for name, mph, _used, _avail in zfsm:
        if mph == mountpoint:
            return name.split("/")[0] if name else None
    return None


def is_leaf(path, carriers):
    """1 when no other share's local path sits strictly beneath path, so
    walking this share never revisits another row's data; 0 when one does.

    The carrier is the server-side path (local_fs) - never the client-side
    export alias."""
    return int(not any(inside(path, other) for other in carriers))


def wildcard_base(share):
    """The path a '&' share stands for: pool wildcard shares end in the
    automount's child-dir wildcard - server-side they mean the parent
    dataset (/pool/008/& -> /pool/008)."""
    return share[:-1].rstrip("/") if share.endswith("&") else share


def candidates(share, root):
    """Path forms the share might carry on the server: the share, its
    bare parent, an underscore alias - plus those two re-rooted under
    the export table's root frame when there is one.

    A share that already carries the root (its own export alias) is never
    doubled: the share itself stays the re-rooted form, and its parent
    only joins when it is not the root again."""
    share = norm(wildcard_base(share))
    flat = "/" + share.strip("/").replace("/", "_")
    parent = share.rsplit("/", 1)[0] or "/"
    forms = [share, parent, flat]
    if root is None or share == "/" or norm(root) == "/":
        return forms
    if share == norm(root):             # the share IS the export root itself
        return forms + [share]
    if share.startswith(norm(root) + "/"):
        # already carries the root: never double it; its parent only joins
        # when it is not the root again
        return forms + [share] + ([parent] if parent != norm(root) else [])
    rerooted_parent = norm(root + parent)
    extras = [norm(root + share)]
    if rerooted_parent != norm(root):   # a share never collapses to the root
        extras.append(rerooted_parent)
    return forms + extras


def resolve_share(share, df, root, zfsm, inodes, mounts, fsid0=()):
    """(backing, usage, local_fs, size, nfiles, pool, base) for one share,
    decided locally from snapshot - all None when no stage matches;
    never guessed.

    local_fs shows where the share actually lives on the filer (the
    share path itself when that path is the local mountpoint, 'host:/export'
    when a mount-table re-export matched).  size/nfiles ride the df row that
    matched; stage 3 works off the deepest ancestor's fs exactly like
    usage does; stage 4's size is zfs used+avail and nfiles comes from
    the dataset's own df -i row.  base is the mountpoint carrying local_fs
    (None for a re-export): with ##mountinfo it names the filesystem and
    the subtree a share's bytes really are."""
    share = norm(wildcard_base(share))
    cands = candidates(share, root)
    rerooted = cands[3:]        # candidates() appends the root+ forms last

    # 0. NFSv4 pseudo-root: a framed bare 'host:/' mount resolves to the
    #    frame's data fs (##exportfile fsid=0), never to the OS root
    #    behind it; with no frame or several, the share stays unresolved
    if share == "/" and len(fsid0) == 1 and fsid0[0] != "/":
        for mountpoint, fstype, usage, total in df:
            if mountpoint == fsid0[0]:
                return (fstype, usage, mountpoint, total,
                        inodes.get(mountpoint),
                        pool_of(zfsm, mountpoint, fstype), mountpoint)
    # 1. exact local mountpoint == the share (never the OS root for '/')
    if share != "/":
        for mountpoint, fstype, usage, total in df:
            if mountpoint == share:
                return (fstype, usage, mountpoint, total,
                        inodes.get(mountpoint),
                        pool_of(zfsm, mountpoint, fstype), mountpoint)
    # 2. exact mountpoint == a re-rooted form: the share's own path first,
    #    then its parent dataset - whatever order df printed them in
    for form in rerooted[:2]:
        for mountpoint, fstype, usage, total in df:
            if mountpoint == form:
                return (fstype, usage, mountpoint, total,
                        inodes.get(mountpoint),
                        pool_of(zfsm, mountpoint, fstype), mountpoint)
    # 3. share lives inside an existing fs (re-rooted forms only; the
    #    deepest ancestor fs carries its backing, same as `df -T` would).
    #    The path must be attested: df rows strictly beneath it, or the
    #    export root is its single fsid=0 frame and carries a df row
    #    (the re-root convention for fleet filers).  The OS root '/'
    #    participates only beneath attested paths; an unattested share
    #    stays unresolved rather than borrowing foreign facts.
    for cand in rerooted:
        beneath = [row for row in df if inside(cand, row[0])]
        framed = len(fsid0) == 1 and root and norm(root) in fsid0 \
            and cand.startswith(norm(root) + "/") \
            and any(row[0] == norm(root) for row in df)
        if not (beneath or framed):
            continue
        ancestors = [row for row in df if inside(row[0], cand)]
        if not ancestors and beneath:      # plain directory on the OS root
            ancestors = [row for row in df if row[0] == "/"]
        if ancestors:
            row = max(ancestors, key=lambda r: len(r[0]))
            _, fstype, usage, total = row
            return (fstype, usage, cand, total, inodes.get(row[0]),
                    pool_of(zfsm, row[0], fstype), row[0])
    # 4. zfs dataset whose mountpoint carries the share: exact mountpoint
    #    forms first, then the deepest path-boundary suffix - whatever
    #    order zfs printed its tables in.  pool is the dataset name's
    #    first segment (data1/pool/008 -> data1).
    exact = [row for row in zfsm if row[1] in cands]
    if exact:
        _name, mountpoint, used, avail = max(exact, key=lambda r: len(r[1]))
        usage = round(100 * used / (used + avail)) if used + avail else None
        served = mountpoint
        return ("zfs", usage, served,
                used + avail if used + avail else None,
                inodes.get(mountpoint), pool_of(zfsm, mountpoint, "zfs"),
                mountpoint)
    deepest = [row for row in zfsm
               if any(cand != "/" and suffix_of(cand, row[1])
                      for cand in cands)]
    if deepest:
        _name, mountpoint, used, avail = max(deepest, key=lambda r: len(r[1]))
        usage = round(100 * used / (used + avail)) if used + avail else None
        served = mountpoint
        return ("zfs", usage, served,
                used + avail if used + avail else None,
                inodes.get(mountpoint), pool_of(zfsm, mountpoint, "zfs"),
                mountpoint)
    # 5. mount-table re-export: an NFS-mounted export that names the share
    #    (automount hooks and local device binds are neither)
    for row in mounts:
        fields = row.split()
        if len(fields) < 3 or not fields[2].startswith("nfs"):
            continue
        source, target = fields[0], fields[1]
        source_path = source.split(":", 1)[1] if ":" in source else ""
        for cand in cands:
            if cand == "/":                  # no specific evidence for /
                continue
            if target == cand or target.endswith(cand) or \
               (source_path.startswith("/")
                and (source_path == cand
                     or source_path.endswith("/" + cand.lstrip("/")))):
                return source, None, source, None, None, None, None
    return None, None, None, None, None, None, None


def crawl_flags(rows):
    """Set each row's crawl flag: 'n' when its bytes are already read by
    another row, 'y' otherwise.

    Two rows are the same tree when their carrier keys match (the export
    alias case); one of them is kept, whole-fs mount first, then
    non-'/legacy/', then shortest mountpoint.  A row is also dropped when
    its tree sits strictly inside a row that is itself crawled (leaf = 1),
    so a bind of /data1/compute/x under a whole-fs /data1 share is not read
    twice.  A row with no carrier has no duplicate evidence and stays 'y'."""
    groups = {}
    for row in rows:
        row["crawl"] = "y"
        if row["key"] is not None:
            groups.setdefault(row["key"], []).append(row)
    for group in groups.values():
        keep = min(group, key=lambda r: (
            r["local_fs"] != r["base"],      # the whole-fs mount is canonical
            "/legacy/" in r["mountpoint"],
            len(r["mountpoint"]), r["mountpoint"]))
        for row in group:
            if row is not keep:
                row["crawl"] = "n"
    for row in rows:
        if row["device"] is None or row["leaf"] != 1:
            continue
        for other in rows:
            if other is not row and other["crawl"] == "y" \
                    and other["device"] == row["device"] \
                    and other["leaf"] == 1 \
                    and inside(other["fspath"], row["fspath"]):
                row["crawl"] = "n"
                break


def update_db(con, host, resolutions):
    """Write one host's per-row resolutions.  A fresh snapshot that cannot
    resolve a stored row contradicts the facts it carries: clear them
    instead of letting stale values pose as current."""
    updated = cleared = 0
    clear = ("UPDATE shares SET backing = NULL, usage = NULL,"
             " local_fs = NULL, size = NULL, nfiles = NULL, pool = NULL,"
             " leaf = NULL, crawl = NULL"
             " WHERE host = ? AND share = ? AND mountpoint = ?"
             " AND (backing IS NOT NULL OR usage IS NOT NULL"
             " OR local_fs IS NOT NULL OR size IS NOT NULL"
             " OR nfiles IS NOT NULL OR pool IS NOT NULL"
             " OR leaf IS NOT NULL OR crawl IS NOT NULL)")
    for (share, mountpoint, backing, usage, local_fs, size, nfiles, pool,
         leaf, crawl) in resolutions:
        if backing is not None:
            changed = con.execute(
                "UPDATE shares SET backing = ?, usage = ?, local_fs = ?,"
                " size = ?, nfiles = ?, pool = ?, leaf = ?, crawl = ?"
                " WHERE host = ? AND share = ? AND mountpoint = ?",
                (backing, usage, local_fs, size, nfiles, pool, leaf, crawl,
                 host, share, mountpoint)).rowcount
            updated += 1 if changed else 0
            continue
        cleared += con.execute(clear, (host, share, mountpoint)).rowcount
    return updated, cleared


def fs_tag(kinds):
    """'zfs', 'xfs' or 'mixed' from the fs kinds resolved for one host."""
    return "mixed" if len(kinds) > 1 else next(iter(kinds), None)


def incomplete(text):
    """True for a v2 snapshot whose fatal sections failed (##error, no ##ok).
    A legacy snapshot carries neither marker and stays parseable."""
    return "##ok" not in text and \
        any(line.startswith("##error") for line in text.splitlines())


def parse_snapshot(path, keys):
    """[per-row resolutions] from one snapshot; None if unusable.

    keys are the (share, mountpoint) rows stored for this host.  A snapshot
    whose fatal probe section failed is incomplete: it never reaches the DB,
    so a broken probe cannot clear facts."""
    text = path.read_text()
    if "##host" not in text or incomplete(text):
        return None
    snap = sections(text)
    pairs = exports(snap.get("exports", ""))
    root = broad_root(pairs)
    df = df_rows(snap.get("df", ""))
    inodes = inode_rows(snap.get("inodes", ""))
    devices = df_devices(snap.get("df", ""))
    carriers = {}                            # device -> its df row
    for row in df:
        carriers.setdefault(devices.get(row[0]), row)
    for device, mountpoint, fstype in mount_pairs(snap.get("mounts", "")):
        if mountpoint in devices or device not in carriers:
            continue                         # not a hidden mate of a df row
        mate = carriers[device]
        df.append((mountpoint, fstype, mate[2], mate[3]))
        inodes.setdefault(mountpoint, inodes.get(mate[0]))
    zfsm = zfs_map(snap.get("zfs", ""))
    mounts = snap.get("mounts", "").splitlines()
    fsid0 = fsid0_roots(snap.get("exportfile", ""))
    mi = mountinfo(snap.get("mountinfo", ""))
    shares = sorted({share for share, _mountpoint in keys})
    facts = {share: resolve_share(share, df, root, zfsm, inodes, mounts,
                                  fsid0) for share in shares}
    # carrier: the filer-local path carrying the share - local_fs, which is
    # the share itself when that path is the local mountpoint.  Re-exports
    # name another server's tree, so they are out.
    paths = {share: norm(wildcard_base(local_fs))
             for share, (backing, _usage, local_fs, _size, _n, _pool, _base)
             in facts.items() if backing in ("zfs", "xfs")}
    leaves = {share: is_leaf(paths[share], paths.values())
              for share in paths}
    rows = []
    for share, mountpoint in keys:
        (backing, usage, local_fs, size, nfiles, pool, base) = facts[share]
        device = fspath = key = None
        if share in paths:
            key = carrier_key(local_fs, base, mi)
            device = key[0]
            fspath = key[1]
        rows.append({"share": share, "mountpoint": mountpoint,
                     "backing": backing, "usage": usage, "local_fs": local_fs,
                     "size": size, "nfiles": nfiles, "pool": pool,
                     "leaf": leaves.get(share), "key": key,
                     "device": device, "fspath": fspath, "base": base,
                     "crawl": "y"})
    crawl_flags(rows)
    return [(r["share"], r["mountpoint"], r["backing"], r["usage"],
             r["local_fs"], r["size"], r["nfiles"], r["pool"], r["leaf"],
             r["crawl"]) for r in rows]


def main():
    con = sqlite3.connect(DB)
    try:
        hosts = hostlist.load()
        wanted = sys.argv[1:] or None      # host names
        snapshots = {box: PROBE_DIR / box for box in sorted(hosts)
                     if (PROBE_DIR / box).is_file()
                     and (not wanted or box in wanted)}
        parsed = set()
        for box, snap in snapshots.items():
            text = snap.read_text(errors="replace")
            if "##host" not in text:
                print(f"{box}: snapshot unusable (no ##host marker)",
                      flush=True)
                continue
            if incomplete(text):
                print(f"{box}: snapshot incomplete (##error, no ##ok),"
                      " facts kept", flush=True)
                continue
            rows = list(con.execute(
                "SELECT share, mountpoint FROM shares WHERE host = ?"
                " ORDER BY share, mountpoint", (box,)))
            updated, cleared = update_db(con, box, parse_snapshot(snap, rows))
            con.commit()
            parsed.add(box)
            unresolved = [s for (s,) in con.execute(
                "SELECT share FROM shares WHERE host = ?"
                " AND backing IS NULL ORDER BY share", (box,))]
            note = f", unresolved {len(unresolved)}" if unresolved else ""
            cleared_note = f", {cleared} cleared" if cleared else ""
            print(f"{box} [{snap.name}]: {updated}/{len(rows)} rows parsed"
                  f"{cleared_note}{note}"
                  + ("" if rows else " (no LDAP shares)"), flush=True)
        unlisted = sorted(p.name for p in PROBE_DIR.iterdir()
                          if p.is_file() and p.name not in snapshots)
        db_hosts = {h for (h,) in con.execute(
            "SELECT DISTINCT host FROM shares")}
        unclassified = sorted(db_hosts - set(hosts))
        selected = (set(hosts) & set(wanted)) if wanted else set(hosts)
        pending = selected - parsed
        kinds = {}
        for host, backing in con.execute(
                "SELECT DISTINCT host, backing FROM shares"
                " WHERE backing IN ('zfs', 'xfs')"):
            kinds.setdefault(host, set()).add(backing)
        tagged = {host: hostlist.merge_tags(host, tags,
                                            fs_tag(kinds.get(host, ())))
                  for host, tags in hosts.items()}
        changed = sum(tagged[host] != hosts[host] for host in hosts)
        if changed:
            hostlist.save(tagged)
            print(f"inventory: {changed} hosts retagged")
        share_count, host_count = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT host) FROM shares"
            " WHERE backing IS NOT NULL").fetchone()
        size_count, nfiles_count = con.execute(
            "SELECT COALESCE(SUM(size IS NOT NULL), 0),"
            " COALESCE(SUM(nfiles IS NOT NULL), 0) FROM shares"
            " WHERE backing IS NOT NULL").fetchone()
        parent_count = con.execute(
            "SELECT COUNT(*) FROM shares WHERE leaf = 0").fetchone()[0]
        skip_count = con.execute(
            "SELECT COUNT(*) FROM shares WHERE crawl = 'n'").fetchone()[0]
        print(f"DB: {share_count} shares carry backing facts across"
              f" {host_count} hosts; size on {size_count},"
              f" nfiles on {nfiles_count},"
              f" leaf = 0 on {parent_count},"
              f" crawl = n on {skip_count}"
              f" ({len(pending)} hosts lack a usable snapshot)")
        print(f"inventory: {len(hosts)} hosts,"
              f" {len(unclassified)} unclassified"
              f"{', e.g. ' + unclassified[0] if unclassified else ''}")
        if unlisted and not wanted:
            print(f"ignored {len(unlisted)} snapshot(s) no host names:"
                  f" {', '.join(unlisted)}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
