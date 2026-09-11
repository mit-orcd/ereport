# Typical workflow and output semantics

The common path from crawl → HTML report → search index → HTTP, then the variations (per-user, multi-server), then how to read the byte/capacity totals each tool reports. The [README](../README.md) quick start is this same path compressed to five commands.

## The common path

### 1. Crawl a filesystem

```bash
./ecrawl /path/to/filesystem-tree crawl-out
```

Writes binary shard files into `crawl-out`; omit the directory and a timestamped one is created in the current working directory.

### 2. Build the HTML report

```bash
./ereport mtime crawl-out
```

All-users report under `./all_users/`:

```text
all_users/index.html
all_users/bucket_a0_s0.html
...
all_users/sunburst.html    # clickable "where the bytes live" chart, linked from index.html
all_users/sunburst.json    # the same tree as data (schema: docs/tools.md#ereport)
all_users/users/<name>.html  # per-user sunburst pages, reachable from the
all_users/users/<name>.json  #   User picker on the aggregate sunburst page
```

Add `--sunburst-buckets` to the `ereport` command and the sunburst page gains
age-range and size-range filter chips (the report's 6×6 bucket axes) plus a
color-by-dominant-bucket selector, backed by an exact per-node bucket matrix in
`sunburst.json`. The per-user pages carry their own matrices too. Every
sunburst page has a collapsible legend below the chart explaining the color
schemes; `--no-sunburst-users` suppresses the per-user pages and picker.

### 3. Build the search index (optional)

```bash
./ereport_index --make crawl-out
```

Writes under `./all_users/index/` (unless `--index-dir` points elsewhere):

```text
all_users/index/meta.txt
all_users/index/path_offsets.bin
all_users/index/paths.bin
all_users/index/tri_keys.bin
all_users/index/tri_postings.bin
```

Command-line search (the report's search box does the same over HTTP):

```bash
./ereport_index --search --index-dir all_users/index foo
```

### 4. Serve over HTTP

```bash
make serve SERVE_ROOT=./all_users SERVE_PORT=8000
```

Open `http://127.0.0.1:8000/index.html`. The search box talks to `eserve.py`, which needs `ereport_index` built (`make ereport_index`), on `PATH`, or named by `EREPORT_INDEX_BIN`.

## Variations

### Per-user report and index

Put a username or uid first:

```bash
./ereport alice mtime crawl-out          # → ./alice/
./ereport_index --make alice crawl-out   # → ./alice/index/
```

Omitting the time basis selects `effective` (max of atime/mtime/ctime per file). `./ereport_index --make alice` with no directory reads crawl input from `./`.

### Merging several servers into one report

Run `ecrawl` once per server, then pass every crawl directory to one command so the report and search index stay unified:

```bash
./ereport mtime crawl_srv01 crawl_srv02 crawl_srv03
./ereport_index --make crawl_srv01 crawl_srv02 crawl_srv03
```

Every directory must use the same shard layout and `uid_shards` count. For an all-users index, the first argument after `--make` must not resolve as a login/uid on this host — listing crawl directories first takes care of that. To relabel a crawl's stored paths (say one root per server), use `--path-rewrite OLD=NEW` at report/index time rather than re-crawling.

### Serving a per-user report

Pick `SERVE_ROOT` depending on how you want URLs to look:

Option A — serve the user directory directly (`index.html` at site root):

```bash
make serve-public SERVE_ROOT=./alice SERVE_PORT=8000
```

Open `http://127.0.0.1:8000/index.html`; search requests go to `http://127.0.0.1:8000/search?q=…`.

Option B — serve a parent directory (URL includes the username):

```bash
make serve-public SERVE_ROOT=. SERVE_PORT=8000
```

Open `http://127.0.0.1:8000/alice/index.html`; search requests resolve to `http://127.0.0.1:8000/alice/search?q=…`.

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
