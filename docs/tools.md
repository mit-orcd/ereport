# Tool reference

One section per binary: what it does, usage, common examples, then the flags. Every binary prints its flag list with `--help` (or with no arguments). Env-var tuning lives in [environment-variables.md](environment-variables.md); on-disk formats in [binary-format.md](binary-format.md).

[`ecrawl`](#ecrawl) · [`ecrawl_query`](#ecrawl_query) · [`ecrawl_mount`](#ecrawl_mount) · [`edelete`](#edelete) · [`edump`](#edump) · [`ereport`](#ereport) · [`ereport_index`](#ereport_index) · [`eserve.py`](#eservepy) · [Source layout](#source-layout)

## `ecrawl`

Parallel filesystem crawler. Walks a tree and writes uid-sharded binary metadata records (one `uid_shard_NNN.bin` per owner shard, plus `crawl_manifest.txt`, `uid.txt`, `gid.txt`).

```bash
./ecrawl [options] <start-path> [output-dir]
```

```bash
./ecrawl /data/lab crawl-out                          # crawl → ./crawl-out/
./ecrawl /data/lab                                    # output dir auto-named <host>_<mon>-<dd>-<yyyy>_<hh>-<mm>-<ss>
./ecrawl --progress /data/lab crawl-out               # live files=/entries=/bytes= line on stderr
./ecrawl --no-write /data/lab                         # counts and byte totals only, no output
./ecrawl --no-stat /data/lab | sort                   # names only, never reads an inode
./ecrawl --no-stat --count /data/lab                  # file/dir/symlink census, no paths
./ecrawl --no-stat --contains slurm- /data/lab        # case-insensitive full-path substring (= find | grep -iF)
./ecrawl --no-stat --print0 /data/lab | xargs -0 ls -l
ECRAWL_CRAWL_THREADS=8 ./ecrawl /data/lab crawl-out
```

| Flag | Effect |
|------|--------|
| `--progress` | live count line (~2 updates/s) on stderr, or stdout with `--count` |
| `--verbose` | full end-of-run diagnostics (queue counters, `manifest=`, allocated bytes, sparse-file count) |
| `--no-write` | crawl and report, write nothing; hardlink dedup still on so `total_bytes` matches write mode |
| `--no-stat` | names-only walk driven by `d_type`; streams paths to stdout, summary to stderr; implies `--no-write` |
| `--count` | with `--no-stat`: tally types only, print no paths |
| `--contains TEXT` | with `--no-stat`: keep paths whose full path contains `TEXT` (case-insensitive; not a glob) |
| `--print0` | with `--no-stat`: NUL-separated output |
| `--statx`, `--iouring` | alternative stat paths; usually no faster, `--iouring` regresses badly on NFS — leave off |

Byte accounting: `total_bytes` is unique regular-file `st_size` (each hardlinked inode counted once, like `du`); `total_allocated_bytes` is the same over `st_blocks`; directory, symlink and other apparent bytes are reported separately. Paths are stored canonical (`realpath`); relabel at report time with `--path-rewrite OLD=NEW` on `ereport` / `ereport_index`.

If `fstatat` finds a directory where `d_type` said otherwise (bad `d_type` or a rename race), `ecrawl` does not descend, counts it in `stat_batch_unexpected_dir_total` and prints a `WARN` block on stderr with example paths — totals may be incomplete when that appears.

## `ecrawl_query`

Read-only queries over a crawl. Without filters it prints directory-shape statistics; with any filter it selects records and prints `key=value` totals (`entries`, `files`, `dirs`, `symlinks`, `other`, `bytes`, `records_scanned`, `answered_from`, `elapsed_sec`). `bytes` is apparent size with hardlinks counted once, so it matches `du -sb`.

```bash
./ecrawl_query [options] <crawl-dir>
```

```bash
./ecrawl_query crawl-out                                        # shape stats: file-per-dir and depth histograms, top parents
./ecrawl_query --top,deep 50 crawl-out                          # deepest directories
./ecrawl_query --subtree /data/lab/jones crawl-out              # bytes + counts under a directory (usually no record reads)
./ecrawl_query --size-gt 524288000 --type f --list crawl-out    # files over 500 MB, one path per line
./ecrawl_query --type f --perm -0002 --list crawl-out           # world-writable files
./ecrawl_query --uid 142698 --type f crawl-out                  # one user's files (opens one shard)
./ecrawl_query --uid 142698 --list --level 1 crawl-out          # that user's top-level directories
./ecrawl_query --uid 142698 --list --level 1 --sum crawl-out    # ... as files,dirs,symlinks,other,bytes,path rows
```

| Flag | Effect |
|------|--------|
| `--top[,dense][,deep] N` | shape mode: top `N` parents by regular-file count (`dense`) and/or depth (`deep`); default `dense`, N=32 |
| `--subtree DIR` | records at or under `DIR`, `DIR` included |
| `--size-gt N` | `st_size > N` bytes |
| `--type C` | one of `f d l c b p s o` |
| `--uid N`, `--gid N` | numeric owner / group; `--uid` opens only that uid's shard |
| `--perm MODE` | octal, `find -perm` forms: `0644` exact, `-0002` all bits, `/0022` any bit |
| `--list` | print matching paths on stdout; totals move to stderr |
| `--level N` | with `--list`: collapse to level-1 roots (matches with no matching ancestor) or `N-1` components below them |
| `--sum` | with `--list`: prefix each path with `files,dirs,symlinks,other,bytes` for the records at or under it |
| `--exact` | always scan records; never answer from rollups or sidecars |
| `--index-dir DIR` | use `dirs.idx` / `rowgroups.idx` written by `ereport_index --make` (auto-detected in the crawl dir when omitted) |
| `-v` | one line per parsed chunk |

Filters AND together. `answered_from` tells you which route ran:

- `catalog_rollup` — a bare `--subtree` answered from the per-directory rollups the crawl stored; no records read.
- `dir_index` — the same via the sidecars: a hash lookup instead of parsing every catalog row. Filtered scans also use the sidecars to skip row groups that cannot reach the subtree and to test membership without loading catalogs (`catalogs_loaded=0`).
- `record_scan` — the full scan. Forced by `--exact`, by any non-subtree filter, and whenever a subtree contains a hardlink (`nlink > 1`), because crawl-time hardlink credit and scan-time dedup can legitimately differ.

Every route gives the same answer; sidecars that are missing, truncated or stale for the shards are ignored silently. Row groups whose zone maps cannot match `--size-gt` / `--type` / `--uid` / `--gid` are skipped without decompressing.

## `ecrawl_mount`

Mounts a crawl as a read-only FUSE filesystem so `find`, `ls`, `du`, `stat`, `tree`, `rsync -n` work on it without the source tree. Linux only; optional build target (see [build-and-deploy.md](build-and-deploy.md#optional-fuse-for-ecrawl_mount)).

```bash
./ecrawl_mount [options] <crawl-dir> <mountpoint>
./ecrawl_mount --dry-run <crawl-dir>            # build the index, print stats, don't mount
```

```bash
./ecrawl_mount crawl-out ~/mnt
find ~/mnt -mtime +365 -size +1G
du -s --apparent-size ~/mnt/data/lab            # byte-exact
fusermount -u ~/mnt
./ecrawl_mount --subtree /data/lab crawl-out ~/mnt    # mount only that directory as the root (smaller index)
```

| Flag | Effect |
|------|--------|
| `--subtree PATH` / `-o subtree=PATH` | index and mount only this directory, as the mount root |
| `-o gid=N` | `st_gid` to report (default 0) |
| `-o threads=N` | index build threads (default 32) |
| `--dry-run` | print `records_total`, `directories`, `index_memory_bytes`, `elapsed_sec` and exit |
| `-f`, `-d`, `-s`, `-v` | foreground, FUSE debug, single-threaded loop, index progress |

Exact: `st_size`, `st_mtime/atime/ctime`, `st_uid`, `st_nlink`, `st_ino`, entry type — so `du --apparent-size` and hardlink dedup behave as on the live tree. Synthesized: mode bits (`0555` dirs / `0444` files), `st_gid`, `st_blocks`; `read()` returns zeros, `readlink()` fails with `EIO`. Use `ecrawl_query --perm/--gid` for the real bits.

The whole namespace is built in memory at mount time (~90 bytes per record: ~9 GB for 100M files); startup is dominated by zstd decompression. FUSE 2 protocol overhead makes traversal several times slower than the live tree — this is for ad-hoc exploration, not bulk analytics. Root can enable `mount -t ecrawl none /mnt -o path=<crawl-dir>` with `ln -s ecrawl_mount /sbin/mount.ecrawl`.

## `edelete`

Parallel deleter for non-directory entries. Dry-run by default; never follows symlinks; removes directories only when they become empty (`--delete`, deepest first, never above the start path).

```bash
./edelete [options] <path>                          # everything under path
./edelete [options] <atime|mtime|ctime> <days> <path>   # only entries older than N days
```

```bash
./edelete /scratch/staging                          # dry run: prints would_delete=
./edelete mtime 90 /scratch/job123                  # dry run, age-filtered
./edelete --delete mtime 90 /scratch/job123         # asks you to type YES
./edelete --delete --force ctime 14 /cache/tmp      # no prompt (scripting)
./edelete --uid 1234 --delete /scratch/shared       # only that owner's entries
```

| Flag | Effect |
|------|--------|
| `--delete` | actually unlink (prompts for `YES` on stdin) |
| `--force` | with `--delete`: skip the prompt |
| `--uid N`, `--gid N` | restrict to entries with that owner / group (both must match when both set) |

Summary keys: `mode`, `would_delete`, `deleted_files`, `removed_empty_dirs`, `errors`, `elapsed_sec`, throughput.

Quota'd XFS: every `unlink` of one owner's files serializes on that owner's dquot mutex, so more threads make it *slower* (kernel time in `osq_lock` / `mutex_spin_on_owner`). Cap `EDELETE_MAX_UNLINK_INFLIGHT` (2–4 is often best when deleting one user's tree); traversal parallelism (`EDELETE_THREADS`) can stay high. Find the knee with:

```bash
for n in 1 2 4 8 16; do
  EDELETE_THREADS=$n EDELETE_MAX_UNLINK_INFLIGHT=$n ./edelete --delete --force <path> 2>/dev/null \
  | awk -F= -v n=$n '/^deleted_files=/{d=$2} /^elapsed_sec=/{e=$2} END{printf "inflight=%s rate=%.0f/s\n", n, e>0?d/e:0}'
done
```

## `edump`

Recreates a crawl as a real tree under a new root: same shape, scrambled `xxxx-xxxx` names, files filled with a repeating seed-derived block. Hardlinks are relinked; symlink targets are dummies of the recorded length; sparse files are materialized at logical size (a warning names the count).

```bash
./edump [--seed N] [--writers N] [--block-size N] [--only PATH] [--progress|--no-progress] <crawl-dir> <output-dir>
```

```bash
./edump crawl-out /data1/replica                            # full tree; output dir must be empty
./edump --writers 32 crawl-out /data1/replica               # fast NVMe: more writers
./edump --only /data/lab/jones crawl-out /data1/jones       # one subtree, with PATH becoming output-dir
```

| Flag | Effect |
|------|--------|
| `--seed N` | name/content seed (default 1); same seed + crawl → identical dump at any writer count |
| `--writers N` | parallel writers (default 8, or `EDUMP_WRITERS`) |
| `--block-size N` | content block; must be a multiple of 4096 to keep `O_DIRECT` |
| `--only PATH` | dump only the subtree at `PATH` (absolute, in the crawled namespace); names match the full dump's |
| `--progress` / `--no-progress` | force the live `files/dirs/other/volume` line on or off (default: on when stderr is a TTY) |
| `--name-self-test` | check the id→name map is injective and exit |

Files ≥ 4 KiB are written with `O_DIRECT` where the filesystem allows it, which avoids the page-cache writeback throttle on fast arrays. Summary keys: `dirs`, `files`, `hardlinks`, `symlinks`, `skipped`, `bytes_written`, `collisions`, `elapsed_sec`. Exit status is nonzero if any write failed; the counters then describe how far it got, not the whole tree.

## `ereport`

Builds a static HTML report from one or more crawls: an age × size heat map with drill-down pages, a sunburst chart, and a search box that works when served by `eserve.py` with an index.

```bash
./ereport [options] [user] [atime|mtime|ctime|effective] [crawl-dir ...]
```

```bash
./ereport mtime crawl-out                              # all users → ./all_users/
./ereport alice mtime crawl-out                        # one user (login or uid) → ./alice/
./ereport alice crawl-out                              # single user, effective time (max of atime/mtime/ctime)
./ereport mtime crawl_srv01 crawl_srv02 crawl_srv03    # merge several servers' crawls into one report
./ereport --bucket-details 3 mtime crawl-out           # per-bucket directory rollup tables, 3 levels deep
./ereport --subtree /data/lab/jones mtime crawl-out    # report on one directory of an existing crawl
./ereport --sunburst-buckets mtime crawl-out           # sunburst with age/size filter chips
EREPORT_THREADS=16 ./ereport mtime crawl-out
```

Output (`./<user>/` or `./all_users/`): `index.html` (heat map + statistics), `bucket_aX_sY.html` (one per cell), `sunburst.html` + `sunburst.json`, and on all-users runs `users/<name>.html` per uid for the sunburst picker.

| Flag | Effect |
|------|--------|
| `--bucket-details N` | read paths and add `N` (1…32) levels of directory rollup tables plus Dense/Deep/Skew drill-downs to each bucket page; slower and more memory |
| `--subtree PATH` | restrict everything to records at or under `PATH` (directory boundary); paths stay absolute |
| `--index-dir DIR` | with `--subtree`: use the `dirs.idx` / `rowgroups.idx` sidecars to skip row groups outside the subtree; output is byte-identical |
| `--path-rewrite OLD=NEW` | relabel a path prefix in the report (e.g. one root per storage server) |
| `--no-sunburst` | skip `sunburst.html` / `sunburst.json` |
| `--sunburst-depth N` | levels broken out below the displayed root (default 6) |
| `--sunburst-buckets` | per-node age × size matrices, enabling the filter chips and bucket coloring; one extra pass over the bins |
| `--no-sunburst-users` | all-users runs: skip the per-user sunburst pages |

Options go before `user` / time basis. A first token that is not a time keyword and does not resolve as a user is taken as the first crawl directory (all-users, effective time). With no crawl directory, `./` is read. When merging directories they must share the same `uid_shards` layout.

Sunburst: click a wedge to zoom, the center to go back, toggle bytes/files. The root is the deepest directory that still holds all content; a node's total is `du -sb` of the directory minus its own record. Per node the top 12 children by bytes or files survive (≥ 0.1% of the parent), the rest fold into `(other)`. `sunburst.json` is `{name, path, bytes, files, children?}` recursively — internal nodes carry *self* values, leaves carry subtree totals, so summing every `bytes` gives the grand total; with `--sunburst-buckets` each node adds `bucket_bytes` / `bucket_files` (36 values, `[age][size]` row-major). To feed Plotly: `px.sunburst(ids=paths, parents=parent_paths, values=subtree_totals, branchvalues="total")`.

Search box: shown only when the URL has `?search` (e.g. `index.html?search=1`); needs `ereport_index --make` and `eserve.py` — browser `fetch` does not work from `file://`.

`docs/images/` has screenshots of the heat map and a bucket page.

## `ereport_index`

Builds and searches a trigram index over crawled paths. Search is a case-insensitive substring match within one path segment (never across `/`): `doc` matches `/x/acme-docs`, `/x/doc`, and everything under a matching directory. Minimum 3 characters.

```bash
./ereport_index --make   [--index-dir DIR] [--subtree PATH] [--path-rewrite OLD=NEW] [--no-dir-index] [user] [crawl-dir ...]
./ereport_index --search [--index-dir DIR] [--json] [--skip N] [--limit N] <term>
./ereport_index --resume-merge --index-dir DIR
```

```bash
ulimit -n 65535; ulimit -f unlimited                            # before large --make runs
./ereport_index --make crawl-out                                # all users → ./all_users/index/
./ereport_index --make alice crawl-out                          # one user → ./alice/index/
./ereport_index --make --index-dir /srv/search crawl_a crawl_b  # merged, explicit location
./ereport_index --make --subtree /data/lab/jones crawl-out      # index one directory only
./ereport_index --search --index-dir all_users/index doc
./ereport_index --search --index-dir alice/index doc --json --limit 20
./ereport_index --resume-merge --index-dir /srv/search          # finish a merge that died (OOM, kill)
```

| Flag | Effect |
|------|--------|
| `--index-dir DIR` | where to write / read the index (default `./<user>/index` or `./all_users/index`; `--search` defaults to `./index`) |
| `--subtree PATH` | index only records at or under `PATH`; stored paths stay absolute |
| `--path-rewrite OLD=NEW` | relabel a path prefix in the stored paths (does not touch the sidecars) |
| `--no-dir-index` | skip writing `dirs.idx` / `rowgroups.idx` |
| `--json` | one JSON object: `total`, `skip`, `limit`, `search_ms`, `index_keys`, `indexed_paths`, `paths[]`; `--limit` defaults to 50 |

User vs all-users: if the first token after the options is a login or uid, it is the user and the rest are crawl directories; otherwise every token is a crawl directory and the index covers all uids.

Two phases: parse (chunk the shards, extract basename trigrams, stream `paths.bin`) and merge (sort per-bucket temp files into `tri_keys.bin` + `tri_postings.bin`, lex-sort `paths.bin`). Merge parallelism is bounded by RAM — `min(MemAvailable, cgroup limit) × EREPORT_INDEX_MERGE_RAM_FRAC` — so a session cgroup far below host RAM limits it regardless of thread count. If `--make` dies during merge, `--resume-merge` finishes it without re-reading the crawl (it cannot when a single-threaded merge was interrupted; rerun `--make`). During `--make` a line is appended every 8 s to `<index-dir>/ereport_index.log` with queue and merge memory estimates.

Limits: `--make` opens many descriptors (`ulimit -n 65535`), and `paths.bin` can exceed a finite `ulimit -f` (bash's unit is KiB — `ulimit -f 200000` is ~195 MiB). A warning prints at start when the file-size limit is below 64 GiB.

Stdout after `--make`: phase timings (`chunk_prep_sec`, `index_phase_sec`, `merge_phase_sec`, `dir_index_sec`, `elapsed_sec`), scale (`scanned_records`, `indexed_paths`, `unique_trigrams`), merge sizing (`merge_workers`, `merge_max_bucket_mib`, `merge_parallel_ram_budget_mib`), per-phase CPU (`cpu_*`), and queue-wait counters (`writeq_*_waits`, `trigramq_*_waits` — high values point at `EREPORT_INDEX_WRITEQ_MAX_BATCHES` / `EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH`). File layout and versions: [binary-format.md](binary-format.md#trigram-index-ereport_index).

## `eserve.py`

Static HTTP server for the report directories plus `GET …/search`, which runs `ereport_index --search`.

```bash
make serve                                              # 127.0.0.1:8000, serves SERVE_ROOT=.
make serve-public SERVE_ROOT=./all_users SERVE_PORT=8080   # 0.0.0.0
make serve SERVE_ROOT=./report SERVE_INDEX_DIR=/srv/search # index outside the tree
python3 eserve.py --bind 127.0.0.1 --port 8000 [--index-dir DIR] [DIR]
```

Needs `python3` and an `ereport_index` binary: next to `eserve.py`, on `PATH`, or `EREPORT_INDEX_BIN=/path`. The index defaults to `<report>/index` next to `index.html` (`SERVE_ROOT/<user>/index` for `GET /<user>/search`); `--index-dir` / `EREPORT_SEARCH_INDEX_DIR` point every search at one directory instead.

Make variables: `SERVE_ROOT` (`.`), `SERVE_PORT` (`8000`), `SERVE_BIND` (`127.0.0.1`, `make serve` only), `SERVE_INDEX_DIR` (empty), `PYTHON3`.

API: `GET /search?q=…&skip=0&limit=50` when `SERVE_ROOT` is the report directory, or `GET /<user>/search?q=…` when it contains user folders. Response is the `ereport_index --json` object.

## Source layout

| File | Role |
|------|------|
| `ecrawl.c` | crawler; parallel walk with work donation, uid-sharded columnar writer |
| `ecrawl_query.c` | read-only queries over shards (shape stats, filters, `--list`, sidecar routes) |
| `ecrawl_mount.c` | FUSE 2 read-only view; in-memory namespace index |
| `edelete.c` | parallel deleter (standalone: `path_canon.h`, `path_utils`) |
| `edump.c` | tree recreation from a crawl |
| `ereport.c`, `ereport_sunburst.[ch]` | HTML report; sunburst aggregation and emitters |
| `ereport_index.c`, `trigram_extract.[ch]` | trigram index build/search; basename trigram extraction |
| `eserve.py` | HTTP server and `/search` bridge |
| `crawl_bin_format.h` | on-disk structs: file header, row groups, column chunks, catalog entries |
| `crawl_bin_block.[ch]` | row-group writer/reader: column encodings, zone maps, projection |
| `crawl_bin_codec.[ch]` | per-column codecs (`RAW`, `FOR_BITPACK`, `RLE`, `CONST`) |
| `crawl_bin_catalog.[ch]` | catalog tail loader, path reconstruction, DFS/subtree helpers |
| `crawl_bin_chunks.[ch]`, `crawl_ckpt.h` | `.ckpt`-driven chunk boundaries for parallel readers |
| `crawl_sidecar.[ch]` | `dirs.idx` / `rowgroups.idx` reader shared by `ecrawl_query` and `ereport` |
| `crawl_result.[ch]` | open a crawl directory: manifest, finalized shards, header validation |
| `crawl_fpcache.[ch]` | per-thread `FILE*` cache for many-shard scans |
| `path_utils.[ch]`, `path_canon.h` | path canonicalization helpers |
| `alloc_tuning.h`, `compat_qsort_r.h`, `ecrawl_wire.h` | glibc `mallopt` tuning, portable `qsort_r`, crawler wire structs |
| `test_crawl_*.c`, `test_query_subtree_dups.c` | unit tests and a fixture generator run by `make check` / `test.sh` |
