# ereport

Small C tools for crawling filesystem metadata, writing compact binary crawl data, and turning that data into static HTML reports.

The current toolchain is:

- `ecrawl`: parallel filesystem crawler that writes compact binary records
- `ereport`: report generator that reads crawl output and writes `index.html`, bucket drilldown pages, and a search box that uses the trigram index when you serve the tree over HTTP
- `ereport_index`: search-index helper for path-element substring search
- `eserve.py`: HTTP server for static reports plus **server-side path search** (calls `ereport_index` on the trigram index)

## Build

```bash
make
```

Build individual binaries:

```bash
make ecrawl
make ereport
make ereport_index
```

Clean:

```bash
make clean
```

## Testing

```bash
make check              # tiny in-memory fixture only (fast)
make check-tree         # ./test_setup.sh then ./test.sh on ./test (needs ecrawl/ereport built)
```

- **`test.sh`** — Parses **`key=value`** stats from **`ecrawl`** and **`ereport`** and checks **`entries` ↔ `scanned_records`**, file/dir/link counts, and byte totals. With **no arguments** it uses a tiny tree under `/tmp`. With a **directory** (e.g. **`./test`** after **`test_setup.sh`**), it also correlates **`find`/`fd`** counts on that tree before crawling it. Use **`SKIP_FS=1`** to skip that filesystem pass when a path is given.
- **`test_setup.sh`** — Removes and recreates **`./test`** (default: **`…/ereport/test`**) with a **deep** chain (**`deep/seg001/…`**), a **wide** branch layout (**`wide/b00/…`**), symlinks, hardlinks, and root files. Tune size with **`DEPTH`**, **`BRANCHES`**, **`FILES_WIDE`**.
- **`test_full.sh`** — Runs **`test_setup.sh`** and then **`./test.sh`** on that tree (same as **`make check-tree`**).

Manual sequence:

```bash
./test_setup.sh
./test.sh "$(pwd)/test"
```

## What The Tools Do

### `ecrawl`

`ecrawl` walks a local filesystem tree and writes binary metadata records. It supports:

- parallel crawl workers
- uid-sharded output files
- optional `--no-write` benchmarking mode
- live status output
- separate accounting for:
  - unique regular-file bytes
  - directory apparent bytes
  - symlink apparent bytes
  - other apparent bytes

Default write-mode output, when no output directory is provided, is auto-named like:

```text
hostname_apr-17-2026_15-03-01
```

Basic usage:

```bash
./ecrawl [--no-write] [--verbose] [--record-root <abs-path>] <start-path> [output-dir]
```

Positional arguments are only **`start-path`** (required, absolute) and optionally **`output-dir`**. If **`output-dir`** is omitted, a timestamped directory name is created in the current working directory.

Optional environment variables (no CLI flags for these):

| Variable | Meaning |
|----------|---------|
| **`ECRAWL_WORKERS`** | Crawl worker threads (**1…16**, default **16**). |
| **`ECRAWL_WRITER_THREADS`** | Writer threads for uid-sharded `.bin` output (default **8**). |
| **`ECRAWL_UID_SHARDS`** | Number of uid shards; must be a **power of two** (default **8192**). |
| **`ECRAWL_MAX_OPEN_SHARDS`** | Per-writer shard file cache target (default **256**); automatically capped against the process open-file limit. |

Examples:

```bash
./ecrawl /data1
./ecrawl --no-write /data1
./ecrawl --no-write --verbose /data1
ECRAWL_WORKERS=8 ./ecrawl /data1
./ecrawl /data1 fstor004_apr-17-2026_15-03-01
./ecrawl --record-root /storage/srv07 /mnt/server07 crawl_srv07
ECRAWL_UID_SHARDS=4096 ECRAWL_WRITER_THREADS=4 ./ecrawl /data1 /tmp/out
ECRAWL_MAX_OPEN_SHARDS=1024 ./ecrawl /data1 /tmp/out
```

Notes:

- `--no-write` crawls and reports metrics without writing shard files.
- `--verbose` enables the full end-of-run diagnostics.
- **`--record-root <abs-path>`** rewrites stored paths: each record’s path becomes `<record-root>/<path-relative-to-start-path>` instead of the live mount path. Use one distinct root per storage server so merged reports and search hits stay identifiable (for example `/storage/srv07/...` vs `/storage/srv08/...`). The crawl still walks **`start-path`** on disk; only the strings written into `.bin` files change. Requires `--record-root` to be an absolute path.

After every run (including non-verbose), stdout includes lightweight queue contention counters (relaxed atomics only; cheap to collect):

- `uid_shards`: uid shard count used for the output layout.
- `max_open_shards`: effective per-writer shard file cache after any open-file-limit auto-cap.
- `writer_failed`: `1` means at least one writer batch failed; the process exits nonzero in this case.
- `wait_crawl_tasks`: worker wakeups waiting on the crawl task queue (idle workers).
- `wait_writer_push`: worker wakeups waiting on a **full** uid-shard writer queue (writers falling behind).
- `wait_writer_pop`: writer wakeups waiting on an **empty** queue (workers not feeding writers fast enough).

Interpret these as **counts of blocking episodes**, not wall-clock time.

### `ereport`

`ereport` reads crawl output and builds an HTML report under **`./<username>/`** (resolved login name), unless that name is unusable—then it falls back to `./tmp/`. If you **omit the username** and pass only the time basis (`./ereport atime …`), it aggregates **all UIDs** present in the crawl and writes under **`./all_users/`**.

Outputs:

- `./<username>/index.html` — heat map, **path search** box (uses server-side index when served via `eserve`), full statistics below the table
- `./<username>/bucket_aX_sY.html` — per age/size cell; **brief summary HTML** unless you pass **`--bucket-details N`** (see below). With **`--bucket-details`**, each page lists directory rollup tables for **N** path levels below the shared prefix inside that bucket.
- `./all_users/` — same layout; **`./all_users/bucket_aX_sY.html`** is a brief summary unless **`--bucket-details`** is used (heat-map totals on `index.html` always match the crawl).

Place **`--bucket-details N`** (`N` = **1…32**) **first**, before the username (if any) and time basis. Omit it for fast runs and small bucket HTML (no path reads for drill-down tables).

**Search UI (important):** There is **only one** search field. Results stay **below that box**—not in a sliding drawer—so it stays obvious what you are editing. After three characters you get a preview list under the input; press **Enter** for paged results in the same panel. Use **Hide** to collapse the panel without clearing your query.

Usage:

```bash
./ereport [--bucket-details N] <username|uid> <atime|mtime|ctime> [bin_dir ...]
./ereport [--bucket-details N] <atime|mtime|ctime> [bin_dir ...]   # all users → ./all_users/
```

If you **omit every `bin_dir`**, `ereport` reads crawl `.bin` files from the **current working directory** (`./`).

The **first argument** is treated as a time basis (`atime`, `mtime`, or `ctime`) only when it matches exactly—otherwise it is interpreted as a username or numeric UID (so you cannot name an account literally `atime` without passing it as a UID).

Thread count (parallel **bin readers** / `worker_main` pool, plus one **stats** thread): set **`EREPORT_THREADS`** (default **32**). Thread count is **not** set on the command line.

**Multiple crawl directories:** pass several **`bin_dir`** paths (each an `ecrawl` output folder). Every directory must use the same layout (legacy vs `uid_shards`) and the same **`uid_shards`** count when manifests are present. For each directory, `ereport` loads that user’s shard file when you specify a user (uid-sharded layout), **or every shard file** when aggregating all users; legacy layouts still load all matching bins. So twenty servers mean twenty directories and twenty shard files merged into one report (per-user), or all shards from every directory (all-users mode).

Examples:

```bash
./ereport milechin atime fstor005-mgmt_apr-17-2026_15-07-17
EREPORT_THREADS=16 ./ereport milechin atime /tmp/crawl_out
EREPORT_THREADS=8 ./ereport 82831 mtime /tmp/crawl_out
EREPORT_THREADS=16 ./ereport milechin atime crawl_srv01 crawl_srv02 crawl_srv03
./ereport milechin atime crawl_a crawl_b crawl_c
./ereport atime /tmp/crawl_out
EREPORT_THREADS=16 ./ereport mtime crawl_srv01 crawl_srv02
EREPORT_THREADS=64 ./ereport ctime /path/to/crawl
./ereport --bucket-details 3 milechin mtime crawl_out
./ereport --bucket-details 3 mtime crawl_srv01 crawl_srv02
```

Parse chunks scale with input `.bin` size so parallel workers are not capped by a tiny chunk count.

**Bucket drill-down:** By default, **`ereport` does not read path strings** into per-bucket tables—the parser seeks past path bytes and **`bucket_aX_sY.html`** files stay short summaries. Pass **`--bucket-details N`** so **`ereport` reads paths** and emits **N** directory-level rollup tables per bucket page (`N` between **1** and **32**). This applies to **single-user** and **all-users** runs; larger **`N`** and all-users crawls cost more I/O and memory.

Runtime behavior:

- **`ereport` scans crawl directories**, then **maps chunk boundaries** inside each `.bin` shard (reading record headers only). That mapping runs with **`EREPORT_THREADS` parallel scanners**; stdout shows **`chunk-map files:X/Y`** until every shard has been scanned, then the usual records/sec line appears while workers parse chunks.
- prints a live progress line during processing
- prints final run stats to `stdout`
- writes warnings/errors to `stderr`
- uses local progress counters with chunked flushes to avoid per-record atomics in the hot path

Interactive search in `index.html` requires **`ereport_index --make`** (see below), **`eserve`** running with **`ereport_index`** available, and opening the report **over HTTP** (browser `fetch` does not work reliably from `file://`).

### `ereport_index`

`ereport_index` builds and searches an on-disk trigram index over crawl path strings—either for **one resolved Unix user** or for **every UID** when **no user** is selected (see **`--make`** disambiguation below; same idea as `ereport` all-users mode: all uid-shard files, no UID filter on records).

The search is a case-insensitive substring match on **individual path segments** (slashes separate segments; matches do not span `/`). For example:

```text
erb
```

matches paths such as:

```text
/path/foo/erbmi1/...
/path/foo/ferbm/...
/path/foo/erb/...
```

Queries must be **at least three characters** (trigram filtering).

Usage:

```bash
./ereport_index --make [--index-dir <path>] [username|uid] [bin_dir ...]
./ereport_index --search <term> [index_dir] [--json] [--skip N] [--limit M]
```

**`--make` user vs all-users:** If the **first** argument after optional **`--index-dir`** is a valid login name or numeric uid on this system, it names the report user and any further arguments are crawl directories (default **`./`**). If that first token is **not** a known user (for example it is a crawl output directory name), **every** argument—including the first—is treated as a **`bin_dir`**, and the index is built for **all UIDs** under **`./all_users/index/`** unless **`--index-dir`** overrides the location (same merge semantics as **`ereport`** aggregate output). **`./ereport_index --make`** with nothing after **`--make`** indexes **`./`** for all users.

You can pass **multiple** **`bin_dir`** paths (same merged crawl directories as for **`ereport`**); they are merged into one index.

Examples:

```bash
./ereport_index --make milechin fstor005-mgmt_apr-17-2026_15-07-17
./ereport_index --make /path/to/crawl_out
./ereport_index --make crawl_srv01 crawl_srv02
./ereport_index --make --index-dir /var/lib/ereport-search milechin crawl_a crawl_b
./ereport_index --search erb milechin/index
./ereport_index --search erb all_users/index
./ereport_index --search erb milechin/index --json --skip 0 --limit 20   # JSON body for APIs
```

Default behavior:

- **`--make [--index-dir <path>]`** — without **`--index-dir`**, **`--make <user> …`** writes under **`./<username>/index/`** (`paths.bin`, `tri_keys.bin`, etc.). With **`--index-dir`**, those files go directly under **`<path>`** (the directory is created if needed).
- **`--make`** with only crawl directories (first token not a system user) writes under **`./all_users/index/`** unless **`--index-dir`** is set.
- **`--make`** with only **`<username|uid>`** and no **`bin_dir`** arguments reads crawl input from **`./`** (same idea as `ereport`).
- **`--search`** defaults **`index_dir`** to **`./index`** when omitted (relative to current working directory).
- **`EREPORT_INDEX_THREADS`** — optional; if set to an integer in **1…4096**, sets parallelism for **`--make`** (default **32** when unset or invalid): **chunk-boundary mapping** runs with up to this many scanners across distinct input `.bin` files (capped by file count), and **parse/index** uses the same count for parallel chunk workers. The trigram **merge** phase uses a **separate** parallelism model (CPU-based, capped), not this variable. Raising thread count increases peak RAM mostly by having more workers fill **bounded** writer queues (see **`EREPORT_INDEX_WRITEQ_MAX_BATCHES`**); very high values are rarely useful if storage is the bottleneck.
- **`EREPORT_INDEX_WRITEQ_MAX_BATCHES`** — optional override (**4…4096**) for how many **write batches** may wait on the single writer thread during **`--make`**. Default scales with **`EREPORT_INDEX_THREADS`** (about **threads/4**, clamped **6…48**). Raising this raises peak memory if workers outpace the writer; lowering it adds backpressure (workers block until the writer drains).

JSON search output is one UTF-8 JSON object per line:

```json
{"total":123,"skip":0,"limit":50,"paths":["...","..."]}
```

If **`--json`** is given without **`--limit`**, **limit defaults to 50**.

### Index build pipeline (`--make`)

Roughly two phases:

1. **Scan / index** — Input `.bin` files are listed, then **chunk boundaries are mapped in parallel** (same idea as `ereport`, using **`EREPORT_INDEX_THREADS`**, capped by how many bin files exist). Parallel workers then read each chunk (record headers first). For a **single-user** build, rows whose UID does not match skip reading the path string (`fseek` past it). For an **all-users** **`--make`** (no resolved username as the first argument), every matched layout row’s path is indexed (no UID filter). Paths are trigram-extracted and batched to a dedicated writer thread that appends `paths.bin`, `path_offsets.bin`, and per-bucket temp trigram files. Chunk input files use large stdio buffers; trigram code lists use a cheap hybrid sort/dedup for uniqueness.

2. **Merge** — Temp per-bucket trigram files are sorted and merged into `tri_keys.bin` + `tri_postings.bin`. When enough buckets have data, merge runs **multiple worker threads** that write per-bucket segment files, then a single thread **stitches** postings offsets and concatenates blobs. Reads prefer **`mmap`** with `malloc`+`read` fallback; sorting uses **LSD radix** on packed records. During indexing, the builder records which trigram buckets were touched so merge can skip `stat`-ing thousands of empty bucket paths. All heavy merge I/O uses large buffers.

### `--make` summary metrics (stdout)

Typical keys include:

- **Where time goes:** `index_phase_sec` (everything up to closing `paths.bin` / `path_offsets.bin`, i.e. scan + parallel index + writer draining temp buckets) vs `merge_phase_sec` vs `wall_after_index_sec` (merge + `meta.txt` and similar; should be ≈ `merge_phase_sec` plus tiny overhead). `elapsed_sec` is end-to-end. **`avg_paths_per_sec` divides by `elapsed_sec`**, so it understates peak index throughput if merge is fast; use **`index_paths_per_sec`** (paths ÷ `index_phase_sec`) for scan/index rate.
- Throughput and scale: `scanned_records`, `indexed_paths`, `trigram_records`, `unique_trigrams`, `index_workers`, `writeq_max_batches`, `write_batch_flush_at` (queue backpressure and paths-per-batch flush tuning).
- Merge: `merge_phase_sec`, `merge_workers`, `merge_buckets_nonempty`, `merge_buckets_skipped`, `merge_trigram_records_read`, byte counts for temp reads and final `tri_*` outputs, derived `*_per_sec` rates.
- Pipeline: `writeq_writer_waits` — count of writer-thread waits on an empty write queue (low is good for the scan phase).

### Index format and on-disk layout

Current index format:

- disk-native and binary
- lowercased trigram postings for search
- original full paths stored separately for result lookup
- intended to be read directly by a later server-side helper

Current files under `<username>/index/`:

- `meta.txt`
- `path_offsets.bin`
- `paths.bin`
- `tri_keys.bin`
- `tri_postings.bin`

During merge, transient `tmp_trigrams_*.bin` files are removed as buckets are processed. Parallel merge may create short-lived `merge_seg_*` segment files under the same directory; successful runs delete them after the stitch step.

### `eserve.py`

HTTP server for generated HTML/CSS and bucket pages. It also implements **`GET …/search`**, which runs **`ereport_index --search`** against the trigram index beside the report.

**Requirements**

- Python **3** on PATH (`python3`).
- **`ereport_index`** available:
  - sibling **`./ereport_index`** next to **`eserve.py`** (after **`make ereport_index`**), **or**
  - on **`PATH`**, **or**
  - override with **`EREPORT_INDEX_BIN=/absolute/path/to/ereport_index`**.

**Make targets** (from the repo root):

```bash
make serve              # bind 127.0.0.1, port 8000 by default
make serve-public       # bind 0.0.0.0 (all interfaces), same default port
```

**Variables** (passed to `make`):

| Variable      | Default   | Meaning                          |
|---------------|-----------|----------------------------------|
| `SERVE_ROOT`  | `.`       | Directory whose files are served |
| `SERVE_PORT`  | `8000`    | TCP port                         |
| `SERVE_BIND`  | `127.0.0.1` (`serve` only) | Listen address         |

Examples:

```bash
# Serve one user’s report tree (contains index.html + index/ + bucket_*.html)
make serve-public SERVE_ROOT=./milechin SERVE_PORT=8080

# Serve a parent directory that contains a user folder
make serve-public SERVE_ROOT=. SERVE_PORT=8000
# Then open http://<host>:8000/milechin/index.html
```

**Search HTTP API** (used by `index.html` via relative `fetch`):

| URL pattern | When to use |
|-------------|-------------|
| **`GET /search?q=…&skip=…&limit=…`** | `SERVE_ROOT` **is** the report directory (e.g. `./milechin`) so `index.html` is at the root of the site. |
| **`GET /<user>/search?q=…`** | `SERVE_ROOT` **contains** the user folder (e.g. SERVE_ROOT=. and files live under `./milechin/`). |

Parameters:

- **`q`** — search string (**≥ 3** characters after trimming).
- **`skip`** — offset into the ranked match list (default `0`).
- **`limit`** — page size (default `50`; capped server-side).

Successful responses are **`application/json`** from **`ereport_index`** (`total`, `skip`, `limit`, `paths`). Errors return JSON or plain HTTP errors depending on failure mode.

Direct **`python3`** invocation:

```bash
python3 eserve.py --bind 127.0.0.1 --port 8000 /path/to/serve
python3 eserve.py --bind 0.0.0.0 --port 8080 ./milechin
```

## Typical Workflow

For **multiple crawl outputs** (e.g. several servers), run **`ecrawl`** once per output directory (often with distinct **`--record-root`** values), then pass **all** of those directories to one **`ereport`** / **`ereport_index --make`** command so the report and search index stay unified. For an **all-users** aggregate report (`./ereport ctime …`), build the matching index with **`ereport_index --make dir1 dir2 …`** where the **first** argument is **not** a valid username/uid on this host—pass the same **`bin_dir`** list as for **`ereport`** (for example directory names only).

### 1. Crawl a filesystem

```bash
./ecrawl /data1
```

This writes binary shard files into an auto-generated output directory unless you provide one explicitly.

### 2. Build a per-user report

```bash
./ereport milechin atime fstor004_apr-17-2026_15-03-01
```

This writes:

```text
milechin/index.html
milechin/bucket_a0_s0.html
...
```

### 3. Optionally build a search index

One crawl output directory (default index location is **`./<username>/index/`**):

```bash
./ereport_index --make milechin fstor004_apr-17-2026_15-03-01
```

Several crawl output directories (merged index for the same user):

```bash
./ereport_index --make milechin crawl_srv01 crawl_srv02 crawl_srv03
```

Omit directories to use **`./`** as the only input path: `./ereport_index --make milechin`.

**All-users** index (same crawl inputs as `./ereport ctime …`, output beside **`./all_users/`**). Omit a username: list crawl dirs first so the first token is **not** resolved as a login or uid:

```bash
./ereport_index --make crawl_srv01 crawl_srv02
./ereport_index --search foo all_users/index
```

This writes:

```text
milechin/index/meta.txt
milechin/index/path_offsets.bin
milechin/index/paths.bin
milechin/index/tri_keys.bin
milechin/index/tri_postings.bin
```

### 4. Serve the results over HTTP

Pick **`SERVE_ROOT`** depending on how you want URLs to look:

**Option A — Serve the user directory directly** (`index.html` at site root):

```bash
make serve-public SERVE_ROOT=./milechin SERVE_PORT=8000
```

Open **`http://127.0.0.1:8000/index.html`**. Search requests go to **`http://127.0.0.1:8000/search?q=…`** (handled by `eserve.py`).

**Option B — Serve a parent directory** (URL includes username):

```bash
make serve-public SERVE_ROOT=. SERVE_PORT=8000
```

Open **`http://127.0.0.1:8000/milechin/index.html`**. Search requests resolve to **`http://127.0.0.1:8000/milechin/search?q=…`**.

Ensure **`ereport_index`** is built (`make ereport_index`) or set **`EREPORT_INDEX_BIN`** before starting the server.

## Validation Helpers

`test.sh` is a validation script used during development to compare crawl/report counts and byte totals against `find`, `fd`, and `du` style checks.

Example:

```bash
./test.sh /data1/milechin
```

It is mainly for development and benchmarking, not part of the normal end-user workflow.

## Output Semantics

### `ecrawl` byte totals

`ecrawl` reports:

- `total_bytes`: unique regular-file bytes
- `dir_apparent_bytes`: apparent size of directories
- `symlink_apparent_bytes`: apparent size of symlinks
- `other_apparent_bytes`: apparent size of other matched types
- `apparent_bytes_total`: sum of all of the above

This means:

- `total_bytes` is closer to deduped regular-file data volume
- `apparent_bytes_total` is closer to `du --apparent-size`

### `ereport` capacity totals

`ereport` currently reports:

- `total_capacity_in_files`
- `total_capacity_in_others`

where `total_capacity_in_files` is based on matched file records and hard-link-aware byte accounting from the crawl input.

## Environment variables (quick reference)

| Variable | Tool / context | Role |
|----------|----------------|------|
| **`ECRAWL_WORKERS`** | `ecrawl` | Crawl worker threads (1…16, default 16). |
| **`ECRAWL_WRITER_THREADS`** | `ecrawl` | Uid-shard writer threads (default 8). |
| **`ECRAWL_UID_SHARDS`** | `ecrawl` | Uid shard count, power of two (default 8192). |
| **`ECRAWL_MAX_OPEN_SHARDS`** | `ecrawl` | Per-writer shard file cache target, auto-capped by `RLIMIT_NOFILE` (default 256). |
| **`EREPORT_THREADS`** | `ereport` | Parallel bin chunk readers (default 32). |
| **`EREPORT_INDEX_THREADS`** | `ereport_index --make` | Parallel chunk-boundary mapping (across bin files) and parallel scan/index workers (default 32). |
| **`EREPORT_INDEX_WRITEQ_MAX_BATCHES`** | `ereport_index --make` | Max writer-queue depth (default scales with thread count; bounds RAM when many workers feed one writer). |
| **`EREPORT_INDEX_BIN`** | `eserve.py` | Absolute path to `ereport_index` if not on `PATH` / next to `eserve.py`. |

## Notes

- The code assumes local filesystem crawl data written by `ecrawl` format version `2`.
- `uid_shard_*.bin` layout is preferred and automatically detected via `crawl_manifest.txt`.
- For **per-user** runs, `ereport` and `ereport_index --make` read only the uid-shard files relevant to that user when uid-sharded input is available. **All-users** runs load **every** shard file (same as merging full-cluster crawls).
- **`ECRAWL_UID_SHARDS`** for a crawl run should match across every output directory you later pass together to **`ereport`** / **`ereport_index --make`** (merged reports assume consistent shard layout).
- The **`Makefile`** targets **`serve`** and **`serve-public`** wrap **`eserve.py`** and forward **`SERVE_ROOT`**, **`SERVE_PORT`**, and **`SERVE_BIND`**.

## License

This project is licensed under the **MIT License**. See **[license.txt](license.txt)**. Copyright is held by **Michel Erb** (2026).
