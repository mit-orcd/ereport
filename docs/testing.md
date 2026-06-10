# Testing and validation

```bash
make check              # ./test.sh: integration + ecrawl_repair / edelete / ereport_index smokes (tiny /tmp tree; fast)
make check-tree         # ./test_setup.sh then ./test.sh on ./test (needs all binaries built)
```

## Scripts

- `test.sh` — runs two phases:
  - Integration (always): `ecrawl` on a tiny `/tmp` tree, then `ereport` single-user (`mtime`, counts vs `ecrawl`) and all-users (incl. `distinct_uids`), then smoke tests on the same tree — `ecrawl_repair --dry-run`, `ecrawl_analyze`, `edelete` (dry-run), and `ereport_index --make` (checks `tri_keys.bin` / `paths.bin` exist).
  - `./test.sh --edelete-only` — edelete smoke + synthetic probes only (needs `./edelete` built; skips ecrawl/ereport and filesystem correlation).
  - Filesystem correlation (only with a directory argument): one `find`/`fd` baseline — file/dir/symlink counts and unique regular-file bytes via `find %D:%i` (not `du`) — compared against `ecrawl` and against `ereport` all-users, plus `ecrawl` vs `ereport` all-users (`entries` ↔ `scanned_records`, etc.). Single-user checks are subset/consistency checks, skipped when no shard maps to that UID (uid-shard crawls omit empty shards). All checks print; any failure fails the step.
  - Notes: expect strict equality only on quiescent trees (a busy tree drifts before `ecrawl` finishes); directory counts use `find -type d` so the crawl root is included (`fd` omits it); a `find` exit status of 1 on unreadable subdirs is tolerated; `SKIP_FS=1` skips only the correlation phase; `--summary` prints a copy/paste results table. Override binaries/threads via `ECRAWL`, `EREPORT`, `ECRAWL_REPAIR`, `ECRAWL_ANALYZE`, `EDELETE`, `EREPORT_INDEX`, `ECRAWL_CRAWL_THREADS`, `EREPORT_THREADS`, `EREPORT_INDEX_THREADS`.
- `test_setup.sh` — Removes and recreates `./test` (default: `…/ereport/test`) with a deep chain (`deep/seg001/…`), a wide branch layout (`wide/b00/…`), symlinks, hardlinks, and root files. Tune size with `DEPTH`, `BRANCHES`, `FILES_WIDE`.
- `test_full.sh` — Runs `test_setup.sh` and then `./test.sh` on that tree (same as `make check-tree`).

Manual sequence:

```bash
./test_setup.sh
./test.sh "$(pwd)/test"
```

## Validation helpers

`test.sh` behavior is summarized above (integration vs optional filesystem correlation; `find`/`fd` vs `ecrawl` and vs `ereport` all-users; `ecrawl` vs `ereport`; single-user subset checks).

Example:

```bash
./test.sh /path/to/test-correlation-root
```

Used during development and benchmarking; not part of the normal end-user workflow.
