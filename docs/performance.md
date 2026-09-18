# Performance and profiling

Why the tools are fast, how to generate adversarial test trees, and the profiling harness used to find and verify optimizations. Detailed A/B experiments live in the git history of this file.

## Design

Common to every binary: compact binary I/O, parallelism along natural boundaries (shard, chunk, bucket), and bounded pipelines instead of second passes or big shared locks.

- **Columnar, zstd-compressed shards** (`ERCBIN09`). Each row group stores one chunk per column with a min/max zone map, so a query decodes only the columns it names and skips groups that cannot match. The catalog tail stores per-directory subtree rollups in DFS order, so a bare `--subtree` total is a lookup, not a scan. Layout: [binary-format.md](binary-format.md).
- **Checkpoint sidecars** (`*.bin.ckpt`). `ecrawl` records record-aligned offsets at a fixed stride, so readers split a shard into parallel chunks without a preliminary scan.
- **Directory-index sidecars** (`dirs.idx`, `rowgroups.idx`). Written by `ereport_index --make`; let `ecrawl_query` and `ereport --subtree` resolve a directory by hash and skip row groups whose DFS sketch cannot reach it.

`ecrawl`
- Flat worker pool: each crawl thread `readdir`s a directory and `fstatat`s its entries inline; no cross-thread stat batching (measured to rarely win). Work-stealing donation keeps wide and deep trees busy across threads.
- Uid-sharded output via dedicated writer threads with bounded queues and large buffered I/O; records are sorted by `(parent_dir_id, name)` inside a row group so the codecs see one run per directory.
- The write path costs roughly one extra second per million files over `--no-write`; most of it is the catalog, not the column encode. Reading inodes via `XFS_IOC_BULKSTAT` was rejected: it needs `CAP_SYS_ADMIN`, and `ecrawl` runs as an ordinary user.

`ereport`
- Chunk lists from `.ckpt`, parallel parse, per-thread summaries merged after the workers finish. The 36 heat-map pages are emitted in parallel, each with a small cache-resident per-worker aggregation table.
- Without `--bucket-details` the parser never reads path strings.

`ereport_index --make`
- Basename-only trigrams ("segment-once"); directory hits are expanded at search time by a prefix scan of the lex-sorted `paths.bin` rather than re-posting parent names onto every child.
- Parse workers → one ordered paths writer → bounded trigram queue → trigram workers → per-bucket temp files, then a RAM-budgeted parallel merge. Queue-wait counters in the `--make` output say which stage is the bottleneck.

`ecrawl_query`
- Row groups are skipped on zone maps, only named columns are decoded, per-thread `FILE*` caches avoid glibc's global stream-list lock on thousand-shard captures, and `--list --level/--sum` sort per task and k-way merge.

`edelete`
- Same walk as `ecrawl`; concurrent `unlink` is capped separately because quota'd XFS serializes unlinks of one owner on that owner's dquot mutex — see [tools.md#edelete](tools.md#edelete).

## Synthetic adversarial trees

`scripts/fixtures/generate-ecrawl-adversarial-tree.sh <root>` builds stress layouts: a flat megadir, a deep chain, wide fan-out, optional `ecrawl_query` depth slices and `ereport` badge fixtures. Pick the scale with `SYNTH_PROFILE` (unset = quick smoke, `medium`, `heavy`, `extreme`).

```bash
SYNTH_PROFILE=medium ./scripts/fixtures/generate-ecrawl-adversarial-tree.sh /tmp/adv
SYNTH_PROFILE=extreme DISK_BUDGET_BYTES=$((200 << 30)) ./scripts/fixtures/generate-ecrawl-adversarial-tree.sh /scratch/adv
```

`extreme` adds `mega_dir1/` (~20M files in one directory) and `mega_dir2/` (2M files + 1M single-file subdirectories); it needs `python3` for bulk creation. Generation refuses to exceed `DISK_BUDGET_BYTES` (default ~100 GiB). A finished tree writes `FIXTURE_MANIFEST.txt`; rerunning with the same parameters is a no-op, `FORCE=1` rebuilds, and different parameters against an existing root are an error. All knobs are documented in the script's header comment.

## Profiling

`scripts/profile/` measures each tool per fixture — wall clock, `strace -f -c` syscall histograms, `perf record --call-graph dwarf`, optional `perf sched` — and packs the results with a `SUMMARY_TABLE.txt` into a tarball. Build with `make debug` for symbols; `perf` needs root or a lowered `kernel.perf_event_paranoid`.

`ecrawl-fixtures.sh` is the producer: it crawls every fixture and keeps the shards at `<bin-root>/<fixture>/bin/`. The other three only read that `<bin-root>`.

```bash
DO_PERF=1 ./scripts/profile/ecrawl-fixtures.sh /tmp/adv /data1/bins     # --no-write and write modes
./scripts/profile/ereport-fixtures.sh        /data1/bins                # ereport --bucket-details 4
./scripts/profile/ereport_index-fixtures.sh  /data1/bins                # --make: chunk_prep / index / merge split
./scripts/profile/ecrawl_query-fixtures.sh   /data1/bins
```

Knobs shared by all four: `DO_STRACE`, `DO_PERF`, `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`, plus each tool's own env vars. `scripts/profile/profiling.sh` chains them. Read a profile with:

```bash
perf report -i perf.data --stdio > report.txt
perf report -i perf.data --stdio --no-children --sort comm,pid > report.bythread.txt
```

The method is: generate a shape, profile it, name the bottleneck from the counters, change one thing, re-profile, keep it only if the numbers move, and guard it with `scripts/test/test.sh`. Comparing against Robinhood, GUFI and XDU on the same trees is [`scripts/compare-indexers/`](../scripts/compare-indexers/README.md).
