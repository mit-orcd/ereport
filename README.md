# ereport

Small C tools for crawling filesystem metadata, writing compact binary crawl data, and turning that data into static HTML reports.

The current toolchain is:

- `ecrawl`: parallel filesystem crawler that writes compact binary records
- `ereport`: report generator that reads crawl output and writes `index.html`, bucket drilldown pages, and a search box that uses the trigram index when you serve the tree over HTTP
- `ereport_index`: search-index helper for path-element substring search
- `ereport_simple`: older/simple report variant
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
make ereport_simple
```

Clean:

```bash
make clean
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
./ecrawl [--no-write] [--verbose] [--workers N] [--record-root <abs-path>] <start-path> [split-depth] [output-dir] [uid-shards] [writer-threads]
```

Examples:

```bash
./ecrawl /data1
./ecrawl --no-write /data1
./ecrawl --no-write --verbose --workers 8 /data1
./ecrawl /data1 2 fstor004_apr-17-2026_15-03-01
./ecrawl --record-root /storage/srv07 /mnt/server07 3 crawl_srv07
```

Notes:

- `--no-write` crawls and reports metrics without writing shard files.
- `--verbose` enables the full end-of-run diagnostics.
- `--workers N` sets crawl worker count at runtime, up to the compiled maximum.
- **`--record-root <abs-path>`** rewrites stored paths: each record’s path becomes `<record-root>/<path-relative-to-start-path>` instead of the live mount path. Use one distinct root per storage server so merged reports and search hits stay identifiable (for example `/storage/srv07/...` vs `/storage/srv08/...`). The crawl still walks **`start-path`** on disk; only the strings written into `.bin` files change. Requires `--record-root` to be an absolute path.

After every run (including non-verbose), stdout includes lightweight queue contention counters (relaxed atomics only; cheap to collect):

- `wait_crawl_tasks`: worker wakeups waiting on the crawl task queue (idle workers).
- `wait_writer_push`: worker wakeups waiting on a **full** uid-shard writer queue (writers falling behind).
- `wait_writer_pop`: writer wakeups waiting on an **empty** queue (workers not feeding writers fast enough).

Interpret these as **counts of blocking episodes**, not wall-clock time.

### `ereport`

`ereport` reads crawl output and builds a per-user HTML report under **`./<username>/`** (resolved login name), unless that name is unusable—then it falls back to `./tmp/`.

Outputs:

- `./<username>/index.html` — heat map, **path search** box (uses server-side index when served via `eserve`), full statistics below the table
- `./<username>/bucket_aX_sY.html` — bucket drilldown pages  
  Bucket links open in a **right-hand drawer**. Search results appear in a **panel directly below** the search box (live preview after three characters; press **Enter** for full paging with **Prev/Next**).

**Search UI (important):** There is **only one** search field. Results stay **below that box**—not in a sliding drawer—so it stays obvious what you are editing. After three characters you get a preview list under the input; press **Enter** for paged results in the same panel. Use **Hide** to collapse the panel without clearing your query.

Usage:

```bash
./ereport <username|uid> <atime|mtime|ctime> [bin_dir ...]
```

Thread count (parallel **bin readers** / `worker_main` pool, plus one **stats** thread):

- Set **`EREPORT_THREADS`** (default **32**). There is no `-t` / `--threads` CLI flag.

**Multiple crawl directories:** pass several **`bin_dir`** paths (each an `ecrawl` output folder). Every directory must use the same layout (legacy vs `uid_shards`) and the same **`uid_shards`** count when manifests are present. For each directory, `ereport` loads that user’s shard file (uid-sharded layout) or all matching bins (legacy)—so twenty servers mean twenty directories and twenty shard files merged into one report.

Examples:

```bash
./ereport milechin atime fstor005-mgmt_apr-17-2026_15-07-17
EREPORT_THREADS=16 ./ereport milechin atime /tmp/crawl_out
EREPORT_THREADS=8 ./ereport 82831 mtime /tmp/crawl_out
EREPORT_THREADS=16 ./ereport milechin atime crawl_srv01 crawl_srv02 crawl_srv03
./ereport milechin atime crawl_a crawl_b crawl_c
```

Parse chunks scale with input `.bin` size so parallel workers are not capped by a tiny chunk count.

Runtime behavior:

- prints a live progress line during processing
- prints final run stats to `stdout`
- writes warnings/errors to `stderr`
- uses local progress counters with chunked flushes to avoid per-record atomics in the hot path

Interactive search in `index.html` requires **`ereport_index --make`** (see below), **`eserve`** running with **`ereport_index`** available, and opening the report **over HTTP** (browser `fetch` does not work reliably from `file://`).

### `ereport_index`

`ereport_index` builds and searches an on-disk trigram index for one user’s crawl data.

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
./ereport_index --make <username|uid> [bin_dir ...]
./ereport_index --search <term> [index_dir] [--json] [--skip N] [--limit M]
```

Examples:

```bash
./ereport_index --make milechin fstor005-mgmt_apr-17-2026_15-07-17
./ereport_index --search erb milechin/index
./ereport_index --search erb milechin/index --json --skip 0 --limit 20   # JSON body for APIs
```

Default behavior:

- **`--make`** writes under **`./<username>/index/`** (`paths.bin`, `tri_keys.bin`, etc.).
- **`--search`** defaults **`index_dir`** to **`./index`** when omitted (relative to current working directory).
- **`EREPORT_INDEX_THREADS`** — optional; if set to an integer in **1…4096**, overrides the default worker count for **`--make`** (otherwise compiled defaults apply).

JSON search output is one UTF-8 JSON object per line:

```json
{"total":123,"skip":0,"limit":50,"paths":["...","..."]}
```

If **`--json`** is given without **`--limit`**, **limit defaults to 50**.

### Index build pipeline (`--make`)

Roughly two phases:

1. **Scan / index** — Parallel workers read crawl `.bin` chunks (record headers first). Rows whose UID does not match the target user skip reading the path string entirely (`fseek` past it). Matching paths are trigram-extracted and batched to a dedicated writer thread that appends `paths.bin`, `path_offsets.bin`, and per-bucket temp trigram files. Chunk input files use large stdio buffers; trigram code lists use a cheap hybrid sort/dedup for uniqueness.

2. **Merge** — Temp per-bucket trigram files are sorted and merged into `tri_keys.bin` + `tri_postings.bin`. When enough buckets have data, merge runs **multiple worker threads** that write per-bucket segment files, then a single thread **stitches** postings offsets and concatenates blobs. Reads prefer **`mmap`** with `malloc`+`read` fallback; sorting uses **LSD radix** on packed records. During indexing, the builder records which trigram buckets were touched so merge can skip `stat`-ing thousands of empty bucket paths. All heavy merge I/O uses large buffers.

### `--make` summary metrics (stdout)

Typical keys include:

- **Where time goes:** `index_phase_sec` (everything up to closing `paths.bin` / `path_offsets.bin`, i.e. scan + parallel index + writer draining temp buckets) vs `merge_phase_sec` vs `wall_after_index_sec` (merge + `meta.txt` and similar; should be ≈ `merge_phase_sec` plus tiny overhead). `elapsed_sec` is end-to-end. **`avg_paths_per_sec` divides by `elapsed_sec`**, so it understates peak index throughput if merge is fast; use **`index_paths_per_sec`** (paths ÷ `index_phase_sec`) for scan/index rate.
- Throughput and scale: `scanned_records`, `indexed_paths`, `trigram_records`, `unique_trigrams`, `index_workers`.
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

## Notes

- The code assumes local filesystem crawl data written by `ecrawl` format version `2`.
- `uid_shard_*.bin` layout is preferred and automatically detected via `crawl_manifest.txt`.
- `ereport` and `ereport_index` both read only the shard files relevant to the requested user when uid-sharded input is available.
- The **`Makefile`** targets **`serve`** and **`serve-public`** wrap **`eserve.py`** and forward **`SERVE_ROOT`**, **`SERVE_PORT`**, and **`SERVE_BIND`**.
