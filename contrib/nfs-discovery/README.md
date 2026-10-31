# ORCD NFS storage discovery

Answer "what NFS storage does the cluster expose, and how full is it?" —
from the automount LDAP tree + read-only filer probes, into
`storage_discovery.sqlite3` (table `shares`).

## Quickstart

    ./discover.sh --ldap                                # refresh DB from LDAP
    ./discover.sh --pull-facts                          # probe every inventory host
    ./discover.sh --pull-facts --tag hdd                # only the HDD (hstor) boxes
    ./discover.sh --pull-facts --limit HOST1,HOST2      # exact probe targets

    sqlite3 storage_discovery.sqlite3 \
      'SELECT host, share, backing, usage FROM shares ORDER BY usage DESC;'

    # everything, as CSV
    sqlite3 -header -csv storage_discovery.sqlite3 'SELECT * FROM shares;'

## Inventory: which filers exist, and what they are

`inventory.yml` is generated and authoritative for **which hosts to
probe**; facts still come only from `probe/<host>`.  `discover.sh --ldap`
adds every filer the automount tree names, and `--pull-facts` refreshes
the tags from probe facts.  A host that leaves LDAP is kept (and reported)
until removed by hand, so a transient miss cannot shrink the host list.
One entry per physical machine, keyed by its clean host name:

    hosts:
      hstor013-n1:
        tags: [hstor, hdd, zfs]
      eofe-data001:
        tags: [eofe, xfs]
      fcstor002:
        tags: [fcstor]

    key     the host name, also the ssh target, the probe/<host>
            snapshot name and shares.host: the LDAP/##host name with its
            .ib / .inband / .lb wrapper suffix, its '-mgmt' qualifier and
            a '-p2' generation marker removed (fstor010.ib + fstor010-p2.ib
            -> fstor010)
    tags    class (core/fstor/hstor/eofe/fcstor/nfs, from the name),
            media (hdd/flash, naming convention), fs (zfs/xfs/mixed, from
            the resolved shares.backing values).  A tag with no evidence
            is omitted - never 'unknown'; an existing fs tag is kept while
            no facts resolve it.

LDAP names are not listed here: `parse_ldap.py` derives the clean host with
the same rule (`hostlist.clean_host`) and writes it straight into
`shares.host`, so `core009.ib`'s 17 shares are stored under `core009`
without the name appearing in the file.  The file is read and rewritten by
the parsers: a malformed entry fails loudly at load time, a `--tag` no
host carries errors, and the parse run reports any `shares.host` missing
from the inventory and any `probe/<host>` file no inventory host names.

## Columns in `shares`

    host        clean host name of the filer that backs the row (core009),
                derived from the automount name by hostlist.clean_host and
                used for probe/<host>
    share       the nfs_share: what clients ask the filer for, taken from
                the LDAP automount value (core009.ib:/arupclab002).  An
                alias - on newer filers it need not exist server-side;
                pool map values end in '&' (clients see '*').
    mountpoint  client-side path where the automount exposes it
                (/nfs/arupclab002)
    backing     zfs/xfs once local_fs is identified directly; 'host:/path'
                when the row rides a re-export from another server
    local_fs    the filer-local path (or remote) that actually carries
                the bytes (/data/arupclab002); the share path itself when
                that path is already the local mountpoint.  bare host:/ v4
                shares name their fsid=0 frame's data fs (fstor013's
                / -> /data1).  NULL only when the share is unresolved
    usage       % used of that filesystem (NULL when unknown)
    size        capacity of that filesystem, bytes (zfs: used+avail,
                pool-wide on un-quotaed datasets)
    nfiles      inodes in use on that filesystem (df -i IUsed)
    pool        zfs pool of the fs carrying the share: first segment of
                the dataset's name from the ##zfs list (data1/amins ->
                data1); NULL for xfs filers and unattested paths
    leaf        1 when no other share's local path sits beneath this one,
                so walking it never revisits another row's data (the local
                path is local_fs); 0 when a share does sit beneath it, e.g.
                a whole-filesystem '/' share above the per-directory
                shares; NULL when the share has no local path (re-export,
                unresolved)
    crawl       'y' when a scan should read this row's bytes, 'n' when
                another row already covers them: a duplicate carrier (same
                device and path inside it - /data1 and a bind of its root)
                or a subtree bind under a crawled whole-fs row.  Scan with
                crawl = 'y' to never read the same tree twice.

Facts trace to probe/<host> snapshot evidence only: exact mountpoint rows,
framed re-roots, mounted-children-attested paths, device-mated mount
rows, identified re-exports.  Unattested paths stay NULL instead of
borrowing foreign facts, and a fresh snapshot that cannot resolve a share
clears its stored facts.  `leaf` marks shares whose local path has no
other share beneath it, so a walk that follows the flag visits every byte
once: fstor004's `/` is 0 because `/data1/compute/orcd/004` is shared
below it, while a pool wildcard (`/data1/pool/014/&`) is 1 because nothing
else covers its children.  Re-exports and unresolved rows have no local
path and stay NULL.
Unreachable filers stay NULL — rerun or `--limit` the ones that are up.
