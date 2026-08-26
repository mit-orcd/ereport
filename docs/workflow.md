# Typical workflow and output semantics

The shortest path from crawl → HTML → search index → HTTP, plus how to read the byte/capacity totals each tool reports.

## Typical workflow

For multiple crawl outputs (e.g. several servers), run `ecrawl` once per output directory, then pass all of those directories to one `ereport` / `ereport_index --make` command so the report and search index stay unified. To relabel a crawl's stored paths (say to one root per server), use `--path-rewrite OLD=NEW` at report/index time rather than re-crawling. For an all-users aggregate report (`./ereport ctime …`), build the matching index with `ereport_index --make dir1 dir2 …` where the first argument is not a valid username/uid on this host—pass the same `bin_dir` list as for `ereport` (for example directory names only).

### 1. Crawl a filesystem

```bash
./ecrawl /path/to/filesystem-tree
```

This writes binary shard files into an auto-generated output directory unless you provide one explicitly.

### 2. Build a per-user report

```bash
./ereport alice atime host-a_apr-17-2026_15-03-01
```

This writes:

```text
alice/index.html
alice/bucket_a0_s0.html
...
```

### 3. Optionally build a search index

One crawl output directory (default index location is `./<username>/index/`):

```bash
./ereport_index --make alice host-a_apr-17-2026_15-03-01
```

Several crawl output directories (merged index for the same user):

```bash
./ereport_index --make alice crawl_srv01 crawl_srv02 crawl_srv03
```

Omit directories to use `./` as the only input path: `./ereport_index --make alice`.

All-users index (same crawl inputs as `./ereport ctime …`). Omit a username: list crawl dirs first so the first token is not resolved as a login or uid:

```bash
./ereport_index --make crawl_srv01 crawl_srv02
./ereport_index --search --index-dir all_users/index foo
```

By default this writes under `./all_users/index/` (unless `--index-dir` points elsewhere):

```text
all_users/index/meta.txt
all_users/index/path_offsets.bin
all_users/index/paths.bin
all_users/index/tri_keys.bin
all_users/index/tri_postings.bin
```

For a single-user index (example user `alice`), the same filenames appear under `alice/index/`.

### 4. Serve the results over HTTP

Pick `SERVE_ROOT` depending on how you want URLs to look:

Option A — Serve the user directory directly (`index.html` at site root):

```bash
make serve-public SERVE_ROOT=./alice SERVE_PORT=8000
```

Open `http://127.0.0.1:8000/index.html`. Search requests go to `http://127.0.0.1:8000/search?q=…` (handled by `eserve.py`).

Option B — Serve a parent directory (URL includes username):

```bash
make serve-public SERVE_ROOT=. SERVE_PORT=8000
```

Open `http://127.0.0.1:8000/alice/index.html`. Search requests resolve to `http://127.0.0.1:8000/alice/search?q=…`.

Ensure `ereport_index` is built (`make ereport_index`) or set `EREPORT_INDEX_BIN` before starting the server.

## Output semantics

### `ecrawl` byte totals

`ecrawl` reports:

- `total_bytes`: unique regular-file logical bytes (`st_size`, hardlink-deduped)
- `st_blocks_bytes_unit`: multiplier for `st_blocks` (512 on typical Linux/glibc builds)
- `total_allocated_bytes`: unique regular-file on-disk bytes (`st_blocks × st_blocks_bytes_unit`, same inode dedup as `total_bytes`)
- `files_sparse_heuristic`: count of deduped regular files where allocated bytes < logical size (heuristic for sparse / preallocated files)
- `dir_apparent_bytes`: apparent size of directories
- `symlink_apparent_bytes`: apparent size of symlinks
- `other_apparent_bytes`: apparent size of other matched types
- `apparent_bytes_total`: sum of all of the above

This means:

- `total_bytes` is closer to deduped logical file size (what you read if you read every byte; sparse files can look huge)
- `total_allocated_bytes` is closer to `du`-style block usage for regular files (still crawl-wide, not per-mount quota semantics)
- `apparent_bytes_total` is closer to `du --apparent-size` over all entry types in the sum

### `ereport` capacity totals

`ereport` currently reports:

- `total_capacity_in_files`
- `total_capacity_in_others`

where `total_capacity_in_files` is based on matched file records and hard-link-aware byte accounting from the crawl input.
