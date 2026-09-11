# ereport

Small C tools for crawling filesystem metadata into compact binary records and turning that data into static HTML reports with fast path search.

## Why ereport?

`du` and `find` re-walk the whole tree on every run — hours on a large filesystem. ereport splits the job:

- **Crawl once.** `ecrawl` walks the tree in parallel and keeps the result as compact binary records — much faster than a serial `du`, and reusable.
- **Report without re-walking.** `ereport` turns a crawl into a static HTML report: an age × size heat map with drill-down pages, all-users or per-user.
- **Find any path fast.** `ereport_index` answers case-insensitive substring searches over every crawled path — even billions — from a search box in the report.
- **Serve anywhere.** The report is static HTML; `eserve.py` serves it and routes the search box.

## Quick start

```bash
make                                            # build everything

./ecrawl /path/to/tree crawl-out                # 1. crawl      → shards in ./crawl-out/
./ereport mtime crawl-out                       # 2. report     → ./all_users/
./ereport_index --make crawl-out                # 3. (optional) search index → ./all_users/index/
make serve SERVE_ROOT=./all_users SERVE_PORT=8000   # 4. serve
```

Open <http://127.0.0.1:8000/index.html> — heat map plus a path-search box.

Variations: per-user report (`./ereport alice mtime crawl-out`, index with `./ereport_index --make alice crawl-out`), or merge several servers' crawls by passing every crawl directory to one command. Details: [docs/workflow.md](docs/workflow.md).

## Tools

| Tool | Role |
|------|------|
| [`ecrawl`](docs/tools.md#ecrawl) | Parallel filesystem crawler; writes compact, uid-sharded binary metadata records. |
| [`ereport`](docs/tools.md#ereport) | Turns crawl output into `index.html`, an age×size heat map with bucket drill-down pages, a sunburst chart (optionally filterable by age/size buckets), and a path-search box. |
| [`ereport_index`](docs/tools.md#ereport_index) | Builds and searches the trigram index behind path-substring search. |
| [`eserve.py`](docs/tools.md#eservepy) | HTTP server for the static reports plus server-side path search. |
| [`ecrawl_query`](docs/tools.md#ecrawl_query) | Read-only directory-shape stats (parent, path-depth, and top-parent histograms). |
| [`ecrawl_mount`](docs/tools.md#ecrawl_mount) | Mounts a crawl as a read-only FUSE filesystem, so `find`/`ls`/`du` work on it without the source tree. Linux only. |
| [`edelete`](docs/tools.md#edelete) | Parallel deleter for non-directory paths, optionally filtered by age and owner. Dry-run by default. |

Full flags, examples, and per-tool behavior: [docs/tools.md](docs/tools.md).

## Documentation

- [Tool reference](docs/tools.md) — full usage, flags, examples, and per-tool behavior for every binary and `eserve.py`, plus the source layout.
- [Typical workflow & output semantics](docs/workflow.md) — multi-server crawls, merged reports, and how byte/capacity totals are computed.
- [Environment variables & thread defaults](docs/environment-variables.md) — every tuning knob and the per-binary default thread counts.
- [Crawl shard binary format](docs/binary-format.md) — `ERCBIN09` header, columnar zstd-compressed row groups with per-column zone maps, an equally columnar catalog tail with DFS ordering and subtree rollups, and `.ckpt` sidecars.
- [Performance & profiling](docs/performance.md) — why it is fast, adversarial-tree generation, and the profiling harness.
- [Testing](docs/testing.md) — `scripts/test/test.sh` / `make check` and validation helpers.
- [Build & deploy](docs/build-and-deploy.md) — jemalloc linking and the systemd daily-crawl units.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE). Copyright is held by Michel Erb (2026).
