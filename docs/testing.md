# Testing and validation

```bash
make check              # scripts/test/test.sh: integration + ecrawl_repair / edelete / ereport_index smokes (tiny /tmp tree; fast)
make check-tree         # scripts/test/test_setup.sh then test.sh on ./test (needs all binaries built)
```

## Scripts

The harnesses live in [`scripts/test/`](../scripts/test/). `./test.sh` at the repo root is a shim that execs `scripts/test/test.sh`, so either path works.

- `scripts/test/test.sh` — runs two phases:
  - Integration (always): `ecrawl` on a tiny `/tmp` tree, then `ereport` single-user (`mtime`, counts vs `ecrawl`) and all-users (incl. `distinct_uids`), then smoke tests on the same tree — `ecrawl_repair --dry-run`, `ecrawl_analyze`, `edelete` (dry-run), `ereport_index --make` (checks `tri_keys.bin` / `paths.bin` exist), and `ecrawl_mount` (see below).
  - `./test.sh --edelete-only` — edelete smoke + synthetic probes only (needs `./edelete` built; skips ecrawl/ereport and filesystem correlation).
  - Filesystem correlation (only with a directory argument): one `find`/`fd` baseline — file/dir/symlink counts and unique regular-file bytes via `find %D:%i` (not `du`) — compared against `ecrawl` and against `ereport` all-users, plus `ecrawl` vs `ereport` all-users (`entries` ↔ `scanned_records`, etc.). Single-user checks are subset/consistency checks, skipped when no shard maps to that UID (uid-shard crawls omit empty shards). All checks print; any failure fails the step.
  - Notes: expect strict equality only on quiescent trees (a busy tree drifts before `ecrawl` finishes); directory counts use `find -type d` so the crawl root is included (`fd` omits it); a `find` exit status of 1 on unreadable subdirs is tolerated; `SKIP_FS=1` skips only the correlation phase; `--summary` prints a copy/paste results table. Override binaries/threads via `ECRAWL`, `EREPORT`, `ECRAWL_REPAIR`, `ECRAWL_ANALYZE`, `EDELETE`, `EREPORT_INDEX`, `ECRAWL_MOUNT`, `ECRAWL_CRAWL_THREADS`, `EREPORT_THREADS`, `EREPORT_INDEX_THREADS`.

### `ecrawl_mount` section

This is the only check that compares a crawl against the live tree it came from through an ordinary POSIX interface rather than through a tool's own reporting, which makes it the strongest end-to-end check in the harness. It runs in two halves:

- `--dry-run` (needs no kernel support, so it runs wherever the binary exists): asserts the index covers every record — `records_total` must equal `ecrawl`'s `entries` — with `records_skipped` and `shards_unreadable` at zero.
- Live mount (needs `/dev/fuse` and `fusermount`): mounts the crawl and checks the mounted view against the source tree — `find` output identical, `stat` fields equal per entry (`size`, `mtime`, `ctime`, `uid`, `nlink`, `inode`, type), `du --apparent-size` byte-exact, `read()` returning the right number of zero bytes, symlink type preserved while `readlink` fails, writes refused with `EROFS`, and `statfs` reporting the crawl's record count.

`atime` is deliberately excluded from the per-entry comparison: the crawl froze it, while walking the live tree during the test keeps moving it, so comparing it would be racy rather than meaningful.

Skips are reported rather than treated as failures, since `ecrawl_mount` is an optional target: the section is skipped when the binary was not built, when `SKIP_FUSE=1` is set, when the host has no `/dev/fuse` or `fusermount`, or when the mount itself is refused (unprivileged FUSE disabled). The mountpoint lives under the integration temp directory and the cleanup trap unmounts it before removing that directory, so a failed run cannot leave a stale mount behind.
- `scripts/test/test_setup.sh` — Removes and recreates `./test` (default: `…/ereport/test`) with a deep chain (`deep/seg001/…`), a wide branch layout (`wide/b00/…`), symlinks, hardlinks, and root files. Tune size with `DEPTH`, `BRANCHES`, `FILES_WIDE`.
- `scripts/test/test_full.sh` — Runs `test_setup.sh` and then `test.sh` on that tree (same as `make check-tree`).

Manual sequence:

```bash
./scripts/test/test_setup.sh
./test.sh "$(pwd)/test"
```

## Validation helpers

`test.sh` behavior is summarized above (integration vs optional filesystem correlation; `find`/`fd` vs `ecrawl` and vs `ereport` all-users; `ecrawl` vs `ereport`; single-user subset checks).

Example:

```bash
./test.sh /path/to/test-correlation-root
```

Used during development and benchmarking; not part of the normal end-user workflow.

## Indexer comparison (Robinhood / GUFI / XDU vs ecrawl suite)

Paper-style capture + Q1–Q5 harness lives under [`scripts/compare-indexers/`](../scripts/compare-indexers/README.md):

```bash
# Fast cycle, well under a minute: rebuilds the suite, generates a 2.5k-entry
# tree, checks correctness over all of it, compares against find and du only.
# For checking that a code change still answers Q1-Q5 correctly.
scripts/compare-indexers/benchmark.sh --do /tmp/small --small

# The same comparison without the driver, on a tree that already exists.
SYNTH_PROFILE=tiny scripts/compare-indexers/run_smoke.sh /tmp/tinytree /tmp/tinyres

# Hours: the benchmark tree and every installed indexer.
make ecrawl ereport ereport_index
scripts/compare-indexers/run_smoke.sh /tmp/indexer-compare-synth
```

See also [`scripts/compare-indexers/capability-matrix.md`](../scripts/compare-indexers/capability-matrix.md) and [`prod-protocol.md`](../scripts/compare-indexers/prod-protocol.md).
