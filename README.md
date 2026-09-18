# ereport

C tools that crawl filesystem metadata once into compact binary records, then answer questions from the records — HTML reports, path search, subtree totals, `find`-style queries — without walking the tree again.

## Quick start

```bash
make                                              # builds every binary (needs libzstd; jemalloc and FUSE are optional)

./ecrawl /path/to/tree crawl-out                  # 1. crawl      → crawl-out/uid_shard_*.bin
./ereport mtime crawl-out                         # 2. report     → ./all_users/index.html
./ereport_index --make crawl-out                  # 3. search index (optional) → ./all_users/index/
make serve SERVE_ROOT=./all_users SERVE_PORT=8000 # 4. serve
```

Open <http://127.0.0.1:8000/index.html?search=1>: an age × size heat map with per-cell drill-down pages, a sunburst of where the bytes live, and a path search box.

Common variations:

```bash
./ereport alice mtime crawl-out                              # one user → ./alice/
./ereport_index --make alice crawl-out                       # ... and their index → ./alice/index/
./ereport mtime crawl_srv01 crawl_srv02 crawl_srv03          # merge several servers' crawls into one report
./ereport --subtree /path/to/tree/lab/jones mtime crawl-out  # report on one directory of an existing crawl
./ereport --bucket-details 3 mtime crawl-out                 # directory rollup tables on every bucket page
./ecrawl_query --subtree /path/to/tree/lab/jones crawl-out   # du -sb of a subtree, from the crawl
./ecrawl_query --size-gt 1073741824 --type f --list crawl-out   # find -size +1G, from the crawl
```

Serving one user's report: `make serve SERVE_ROOT=./alice` puts `index.html` at the site root; `make serve SERVE_ROOT=.` serves every user under `/<user>/index.html`. `make serve-public` binds `0.0.0.0`.

## Tools

| Tool | Role |
|------|------|
| [`ecrawl`](docs/tools.md#ecrawl) | Parallel crawler; writes uid-sharded binary records. `--no-stat` is a fast names-only `find`. |
| [`ereport`](docs/tools.md#ereport) | Static HTML report: heat map, bucket pages, sunburst, search box. |
| [`ereport_index`](docs/tools.md#ereport_index) | Trigram index for case-insensitive path-substring search over billions of paths. |
| [`eserve.py`](docs/tools.md#eservepy) | Serves the report and answers the search box. |
| [`ecrawl_query`](docs/tools.md#ecrawl_query) | `du` / `find`-style queries over a crawl: subtree totals, size/type/owner/perm filters, path lists. |
| [`ecrawl_mount`](docs/tools.md#ecrawl_mount) | Read-only FUSE mount of a crawl so `find`, `ls`, `du` work without the source tree. Linux only. |
| [`edelete`](docs/tools.md#edelete) | Parallel deleter with age and owner filters. Dry-run by default. |
| [`edump`](docs/tools.md#edump) | Recreates a crawl as a real tree with scrambled names and synthetic contents. |

## How the totals are defined

- `ecrawl` `total_bytes`: unique regular-file `st_size` — each hardlinked inode counted once, like `du -sb`. `total_allocated_bytes` is the same over `st_blocks × 512`; `files_sparse_heuristic` counts files where allocated < logical. Directory, symlink and other apparent bytes are listed separately; `apparent_bytes_total` sums them all.
- `ereport` heat-map bytes use the same hardlink-aware accounting over the records it matched (`total_capacity_in_files`; other types in `total_capacity_in_others`).
- `ecrawl_query --subtree` `bytes` equals `du -sb` of that directory.

## Documentation

- [Tool reference](docs/tools.md) — usage, examples and flags for every binary.
- [Environment variables](docs/environment-variables.md) — thread counts and tuning knobs.
- [Build & deploy](docs/build-and-deploy.md) — zstd/jemalloc/FUSE, and the systemd daily-crawl units.
- [Testing](docs/testing.md) — `make check`, the correlation harness, the indexer comparison.
- [Binary format](docs/binary-format.md) — shard, catalog, sidecar and index layouts.
- [Performance](docs/performance.md) — why it is fast, adversarial trees, profiling.

## License

MIT — see [LICENSE](LICENSE). Copyright Michel Erb (2026).
