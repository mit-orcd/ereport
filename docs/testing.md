# Testing and validation

```bash
make check              # scripts/test/test.sh: integration + ecrawl_repair / edelete / ereport_index smokes (tiny /tmp tree; fast)
make check-tree         # scripts/test/test_setup.sh then test.sh on ./test (needs all binaries built)
```

## Scripts

The harnesses live in [`scripts/test/`](../scripts/test/). Prefer `make check` / `make check-tree`, or call the scripts directly.

- `scripts/test/test.sh` — runs two phases:
  - Integration (always): `ecrawl` on a tiny `/tmp` tree, then `ereport` single-user (`mtime`, counts vs `ecrawl`) and all-users (incl. `distinct_uids`), then smoke tests on the same tree — `ecrawl_repair --dry-run`, `ecrawl_query`, `edelete` (dry-run), `ereport_index --make` (checks `tri_keys.bin` / `paths.bin` exist), and `ecrawl_mount` (see below).
  - `./scripts/test/test.sh --edelete-only` — edelete smoke + synthetic probes only (needs `./edelete` built; skips ecrawl/ereport and filesystem correlation).
  - Filesystem correlation (only with a directory argument): one `find`/`fd` baseline — file/dir/symlink counts and unique regular-file bytes via `find %D:%i` (not `du`) — compared against `ecrawl` and against `ereport` all-users, plus `ecrawl` vs `ereport` all-users (`entries` ↔ `scanned_records`, etc.). Single-user checks are subset/consistency checks, skipped when no shard maps to that UID (uid-shard crawls omit empty shards). All checks print; any failure fails the step.
  - Notes: expect strict equality only on quiescent trees (a busy tree drifts before `ecrawl` finishes); directory counts use `find -type d` so the crawl root is included (`fd` omits it); a `find` exit status of 1 on unreadable subdirs is tolerated; `SKIP_FS=1` skips only the correlation phase; `--summary` prints a copy/paste results table; `--keep-html[=DIR]` snapshots every HTML report the run generates into DIR (default `./ereport-test-html`) before the phase temp dirs are torn down, so the report UI can be browsed afterwards. Override binaries/threads via `ECRAWL`, `EREPORT`, `ECRAWL_REPAIR`, `ECRAWL_QUERY`, `EDELETE`, `EREPORT_INDEX`, `ECRAWL_MOUNT`, `ECRAWL_CRAWL_THREADS`, `EREPORT_THREADS`, `EREPORT_INDEX_THREADS`.

### dir-index sidecar section

`ereport_index --make` writes `dirs.idx` and `rowgroups.idx` next to the trigram index, and `ecrawl_query --index-dir` uses them to answer a bare `--subtree` aggregate without reading every directory row and to build a filtered scan's chunk list from the row groups that can still hold a match. Both are caches over data the query can already reach another way, so every check has one of three shapes: the sidecar answer equals the route it replaces *and* equals `--exact`, which always scans; a `--list` path set is identical with and without pruning; or the sidecar is stale or absent and the query degrades to `catalog_rollup` with the same totals instead of failing.

- Three-way agreement on the target subtree, a deep leaf, `s0`, and the crawl root: `answered_from` is `catalog_rollup` / `dir_index` / `record_scan` as expected, and `entries`, `files`, `dirs`, `symlinks`, `other` and `bytes` match across all three, with `bytes` also checked against `du -sb`.
- `--list` path sets, compared pruned against unpruned; `records_scanned` must not go *up* under pruning.
- Filtered scans (`--type f`, `--size-gt`, `--type d`, `--gid`) agree on every aggregate. `records_scanned` is deliberately not compared: reading fewer records for the same answer is the point.
- Staleness, each of which must come back with the catalog rollup's own numbers rather than merely "not `dir_index`": a shard whose mtime moved, a shard that grew by a byte, a truncated `dirs.idx`, and an index directory that does not exist. Restoring the shard puts the sidecar back in use. A truncated `rowgroups.idx` is the optional half — the aggregate still comes from `dirs.idx` and only the scan loses its pruning.
- The hardlink guard: `nlink > 1` in scope makes crawl-time hardlink credit and scan-time dedup legitimately disagree, so both rollup routes must decline to `record_scan`.
- A subtree the capture never saw: not an error, and the same answer either way.
- `--no-dir-index` writes neither file, records `dir_index=0` in `meta.txt`, and a query against that index directory falls back.

Three cases need fixtures the rest of the harness does not build:

- **Several uid shards.** `ecrawl` files a record by its owner, so a fixture this user owns outright lands in one shard and the cross-shard arithmetic — summing one subtree's rollups over every shard holding a piece of it, and adding the subtree's own directory record exactly once, in whichever shard actually carries it — is never exercised. `chown` needs privileges a test cannot assume, so ownership is faked for the crawl alone: a preloaded shim answers the `stat` calls `ecrawl` makes with a uid taken from a `uNNNN_` prefix on the entry's own name. The section skips itself when there is no compiler for the shim, or when the shim did not take and the capture came out with fewer than three shards, since a one-shard capture would silently test nothing. `find` and `du -sb` are the outside opinion here: they cannot be fooled by an accounting rule the three routes might share.
- **Stored paths.** The sidecars are keyed on the path the capture stored (the canonical on-disk path). `ereport_index --path-rewrite` relabels what the *trigram* index stores and does not touch the catalogs, so a subtree query keeps using the stored spelling — the relabelled path answers nothing, exactly as it would without a sidecar.
- **Duplicate shard basenames.** The sidecars are keyed on that name, so passing two crawl directories that contribute the same one makes the phase decline rather than index one shard under the other's identity. It has to say so on stderr, write neither file, and still write the trigram index.

### `ecrawl_mount` section

This is the only check that compares a crawl against the live tree it came from through an ordinary POSIX interface rather than through a tool's own reporting, which makes it the strongest end-to-end check in the harness. It runs in two halves:

- `--dry-run` (needs no kernel support, so it runs wherever the binary exists): asserts the index covers every record — `records_total` must equal `ecrawl`'s `entries` — with `records_skipped` and `shards_unreadable` at zero.
- Live mount (needs `/dev/fuse` and `fusermount`): mounts the crawl and checks the mounted view against the source tree — `find` output identical, `stat` fields equal per entry (`size`, `mtime`, `ctime`, `uid`, `nlink`, `inode`, type), `du --apparent-size` byte-exact, `read()` returning the right number of zero bytes, symlink type preserved while `readlink` fails, writes refused with `EROFS`, and `statfs` reporting the crawl's record count.

`atime` is deliberately excluded from the per-entry comparison: the crawl froze it, while walking the live tree during the test keeps moving it, so comparing it would be racy rather than meaningful.

Skips are reported rather than treated as failures, since `ecrawl_mount` is an optional target: the section is skipped on macOS (Linux-only target), when the binary was not built, when `SKIP_FUSE=1` is set, when the host has no `/dev/fuse` or `fusermount`, or when the mount itself is refused (unprivileged FUSE disabled). The mountpoint lives under the integration temp directory and the cleanup trap unmounts it before removing that directory, so a failed run cannot leave a stale mount behind.
- `scripts/test/test_setup.sh` — Removes and recreates `./test` (default: `…/ereport/test`) with a deep chain (`deep/seg001/…`), a wide branch layout (`wide/b00/…`), symlinks, hardlinks, and root files. Tune size with `DEPTH`, `BRANCHES`, `FILES_WIDE`.
- `scripts/test/test_full.sh` — Runs `test_setup.sh` and then `test.sh` on that tree (same as `make check-tree`).

Manual sequence:

```bash
./scripts/test/test_setup.sh
./scripts/test/test.sh "$(pwd)/test"
```

## Validation helpers

`scripts/test/test.sh` behavior is summarized above (integration vs optional filesystem correlation; `find`/`fd` vs `ecrawl` and vs `ereport` all-users; `ecrawl` vs `ereport`; single-user subset checks).

Example:

```bash
./scripts/test/test.sh /path/to/test-correlation-root
```

Used during development and benchmarking; not part of the normal end-user workflow.

## Indexer comparison (Robinhood / GUFI / XDU vs ecrawl suite)

Paper-style capture + Q1–Q6 harness lives under [`scripts/compare-indexers/`](../scripts/compare-indexers/README.md), measuring every phase cold and hot (Q6 is this comparison's own extra query, an unanchored substring):

```bash
# Fast cycle, well under a minute: rebuilds the suite, generates a 2.5k-entry
# tree, checks correctness over all of it, compares against find and du only.
# For checking that a code change still answers Q1-Q6 correctly.
scripts/compare-indexers/benchmark.sh --do /tmp/small --small

# The same comparison without the driver, on a tree that already exists.
SYNTH_PROFILE=tiny scripts/compare-indexers/run_smoke.sh /tmp/tinytree /tmp/tinyres

# Hours: the benchmark tree and every installed indexer.
make ecrawl ereport ereport_index
scripts/compare-indexers/run_smoke.sh /tmp/indexer-compare-synth
```

See also [`scripts/compare-indexers/capability-matrix.md`](../scripts/compare-indexers/capability-matrix.md) and [`prod-protocol.md`](../scripts/compare-indexers/prod-protocol.md).
