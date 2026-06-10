# ereport

Small C tools for crawling filesystem metadata into compact binary records and turning that data into static HTML reports with fast path search.

## Tools

| Tool | Role |
|------|------|
| [`ecrawl`](docs/tools.md#ecrawl) | Parallel filesystem crawler; writes compact, uid-sharded binary metadata records. |
| [`ecrawl_repair`](docs/tools.md#ecrawl_repair) | Rebuilds `*.bin.ckpt` sidecars, truncates incomplete shard tails, quarantines unrepairable shards. |
| [`ecrawl_analyze`](docs/tools.md#ecrawl_analyze) | Read-only directory-shape stats (parent, path-depth, and top-parent histograms). |
| [`edelete`](docs/tools.md#edelete) | Parallel deleter for non-directory paths — everything under a path, or only entries older than an age threshold. Dry-run by default; never follows symlinks. |
| [`ereport`](docs/tools.md#ereport) | Turns crawl output into `index.html`, an age×size heat map with bucket drill-down pages, and a path-search box. |
| [`ereport_index`](docs/tools.md#ereport_index) | Builds and searches the trigram index behind path-substring search. |
| [`eserve.py`](docs/tools.md#eservepy) | HTTP server for the static reports plus server-side path search. |

## Quick start

```bash
# Build everything
make

# 1. Crawl a tree (writes shards into ./crawl-out)
./ecrawl /path/to/tree crawl-out

# 2. Build an all-users HTML report (heat map + bucket pages) under ./all_users/
./ereport mtime crawl-out

# 3. (optional) Build the trigram search index under ./all_users/index/
./ereport_index --make crawl-out

# 4. Serve it over HTTP
make serve SERVE_ROOT=./all_users SERVE_PORT=8000
# open http://127.0.0.1:8000/index.html
```

Per-user instead of all-users: put a username or uid first, e.g. `./ereport alice mtime crawl-out` and `./ereport_index --make alice crawl-out` (output lands under `./alice/`). Merging several servers? Pass every crawl directory to one `ereport` / `ereport_index --make` command. See [docs/workflow.md](docs/workflow.md).

## Tools at a glance

### `ecrawl` — crawl a tree
Walks a local filesystem tree and writes uid-sharded binary metadata records (logical/allocated bytes, sparse heuristics, per-type accounting). Start with:

```bash
./ecrawl /path/to/tree [output-dir]      # add --no-write to benchmark without writing shards
```

Full flags, the stat-worker pipeline, and tuning knobs: [docs/tools.md#ecrawl](docs/tools.md#ecrawl).

### `edelete` — delete non-directory paths
Parallel walker that deletes files/symlinks/etc. (and empties directories) under a path, optionally filtered by age and owner. It previews by default and only deletes with `--delete`.

```bash
./edelete /tmp/scratch                    # dry-run preview
./edelete --delete mtime 90 /tmp/scratch  # delete entries older than 90 days
```

Safety model, `--uid`/`--gid` filters, and XFS unlink-contention tuning: [docs/tools.md#edelete](docs/tools.md#edelete).

### `ereport` — build the HTML report
Reads crawl shards and emits an `index.html` heat map (age × size), per-cell `bucket_*.html` pages, and a search box. All-users or single-user.

```bash
./ereport mtime crawl-out                 # all-users → ./all_users/
./ereport alice mtime crawl-out           # single user → ./alice/
```

Time-basis rules, `--bucket-details`, `--subtree`, and multi-crawl merging: [docs/tools.md#ereport](docs/tools.md#ereport).

### `ereport_index` — build/search the path index
Builds an on-disk trigram index over crawl path strings and answers case-insensitive substring searches (≥3 chars). Powers the search box in `index.html` when served via `eserve.py`.

```bash
./ereport_index --make crawl-out                          # all-users index
./ereport_index --search --index-dir all_users/index doc  # query
```

Build pipeline, `--resume-merge`, `ulimit` guidance, and JSON output: [docs/tools.md#ereport_index](docs/tools.md#ereport_index).

### `ecrawl_repair` / `ecrawl_analyze` — maintenance & inspection

```bash
./ecrawl_repair crawl-out                 # rebuild missing/stale .ckpt sidecars
./ecrawl_analyze --top 50 crawl-out       # directory-shape stats (read-only)
```

Details: [docs/tools.md#ecrawl_repair](docs/tools.md#ecrawl_repair) · [docs/tools.md#ecrawl_analyze](docs/tools.md#ecrawl_analyze).

### `eserve.py` — serve reports + search

```bash
make serve SERVE_ROOT=./all_users SERVE_PORT=8000
```

Serves the static report and routes `GET …/search` to `ereport_index --search`. Details: [docs/tools.md#eservepy](docs/tools.md#eservepy).

## Documentation

- [Tool reference](docs/tools.md) — full usage, flags, examples, and per-tool behavior for every binary and `eserve.py`, plus the source layout.
- [Typical workflow & output semantics](docs/workflow.md) — multi-server crawls, merged reports, and how byte/capacity totals are computed.
- [Environment variables & thread defaults](docs/environment-variables.md) — every tuning knob and the per-binary default thread counts.
- [Crawl shard binary format](docs/binary-format.md) — `ERCBIN05` header, record stream, catalog tail, and `.ckpt` sidecars.
- [Performance & profiling](docs/performance.md) — why it is fast, adversarial-tree generation, and the profiling harness.
- [Testing](docs/testing.md) — `test.sh` / `make check` and validation helpers.
- [Build & deploy](docs/build-and-deploy.md) — jemalloc linking and the systemd daily-crawl units.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE). Copyright is held by Michel Erb (2026).
