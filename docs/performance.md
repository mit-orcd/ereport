# Performance and profiling

Design concepts that make the tools fast, how to generate adversarial test trees, and the profiling harness used to find and verify optimizations.

## Why this is fast (design concepts)

The tools are fast because they combine compact binary I/O, parallelism along natural boundaries, and bounded pipelines instead of naïvely scanning everything twice or holding giant locks over shared mutable state.

### Shared ideas (`ecrawl`, `edelete`, `ereport`, `ereport_index`)

- Path arguments — Directory and crawl-root arguments are normalized to canonical absolute paths with `realpath(3)` once the path exists (see `path_canon.h`). Relative inputs are supported; symlink components are resolved. Output directories that are created on demand are canonicalized after `mkdir` where applicable.
- Binary crawl records — Paths and metadata use a fixed header plus a zstd block-compressed record stream and (when finalized) a catalog tail (file magic `ERCBIN07`, format version 7). Layout and rejection rules are in [binary-format.md](binary-format.md).
- Checkpoint sidecars (`*.bin.ckpt`) — While crawling, `ecrawl` records record-aligned byte offsets at a fixed stride so `ereport` and `ereport_index` can split each shard into valid segments without a preliminary full-file scan. See [binary-format.md](binary-format.md#checkpoint-sidecars-binckpt).
- Embarrassingly parallel units — Work is split by shard file, chunk, age×size bucket, or trigram bucket so threads rarely contend on the same byte or the same mutex for long.

### `edelete`: parallel deletion

- Same crawl parallelism pattern as `ecrawl` — task queue, worker threads, local directory stacks, and work donation so wide trees stay busy across threads.
- Does not follow symlinks — traversal uses `lstat` / `fstatat(..., AT_SYMLINK_NOFOLLOW)`; symlink inodes can be `unlink`’d if eligible, but targets are not walked through links.
- Deletes non-directory paths only — directories are removed with `rmdir` only when they become empty, deepest first, without ascending above the start path or removing `/`.

See [tools.md#edelete](tools.md#edelete) for the unlink-contention tuning note (quota'd XFS).

### `ecrawl`: crawling and capture

- Parallel crawl threads feed a task queue; multiple threads traverse the tree concurrently while respecting directory boundaries.
- Parallel stat pool (when `ECRAWL_STAT_THREADS` > 0, default 8) — Each crawl worker does one `readdir` per open directory; trusted non-directory names are batched to stat workers (see [tools.md#directory-scanning-and-stat-workers](tools.md#directory-scanning-and-stat-workers)). Set `ECRAWL_STAT_THREADS=0` for legacy inline-only `fstatat` (often faster on very low-latency metadata).
- Uid-sharded output — Records hash to many `uid_shard_*.bin` files so writes spread across descriptors and writer threads; you avoid one giant append-only file and reduce lock contention on a single sink.
- Separate writer threads — Crawl threads batch work to bounded writer queues; dedicated writers flush shards with large buffered I/O instead of every thread hitting the filesystem independently for every record. `ECRAWL_WRITER_QUEUE_BATCHES` caps pending record batches per writer (default 64, range 4…4096).
- Checkpoint rows during write — Sidecars capture sparse offsets so later tools can parallel-read without rescanning from zero.

### `ereport`: reports from crawl bins

- Parallel chunk mapping — Uses `*.ckpt` to build chunk lists (byte ranges that align with record starts). Chunk count scales with file size, so `EREPORT_THREADS` has enough units of work.
- Parallel chunk parsing — Workers consume disjoint chunks; summaries and bucket histograms merge after workers finish (merge step is not on the per-record hot path across all threads).
- Parallel bucket HTML — The 36 heat-map cells map to 36 independent output files; emission fans out across threads up to that cap. Per-page `aggregate_totals_for_page_n` uses a RAM-budgeted worker matrix; if the worker×row partial matrix cannot be allocated, worker count is reduced until `calloc` succeeds (avoids a single-threaded fallback that pins one CPU on huge path-row maps).
- Cheap mode by default — Without `--bucket-details`, the parser seeks past path strings for histogram-only passes, keeping I/O and CPU down when you only need aggregates.

### `ereport_index`: trigram index build (`--make`)

- Same chunk boundaries as `ereport` — Parallel chunk readers; rows can skip path bytes when building for a single UID (`fseek` past unmatched records).
- Ordered path stream — A single paths writer thread appends `paths.bin` / `path_offsets.bin` in strict path-id order so the index remains coherent while many trigram workers run.
- Producer–consumer queues — Parse workers → paths writer → trigram job queue (bounded) → trigram workers → `tmp_trigrams_*.bin`. Bounded queues apply backpressure instead of unbounded RAM; tuning env vars trade memory vs blocking (see metrics like `writeq_parse_waits` / `trigramq_paths_waits`).
- Sliced bulk enqueue — Trigram jobs from a write batch are enqueued in slices that fit current queue depth, so partial capacity is used instead of waiting until an entire batch fits.
- Batched trigram writes — For each path, trigram codes are sorted by trigram bucket, then `fwrite` runs per contiguous bucket run under one mutex acquisition per slice — far fewer lock rounds and syscalls than one write per trigram.
- Lazy open + LRU on bucket files — Only up to `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS` `tmp_trigrams_*.bin` handles stay open; cold buckets are closed and reopened on demand so you do not need thousands of `FILE*` simultaneously.
- Merge phase — Temp bucket files are sorted (radix on packed records), optionally merged with parallel workers subject to a RAM budget, with large buffered I/O and `mmap` where helpful — separate from index-phase throughput but tuned for large disks.

Together, these choices aim to keep CPU, mutexes, and syscalls off the critical path per byte of crawl data, and to use disk bandwidth (especially on NVMe) with large buffered writes instead of tiny random appends per logical record.

## Synthetic adversarial trees

`scripts/fixtures/generate-ecrawl-adversarial-tree.sh` builds stress layouts for `ecrawl` (flat megadir, optional depth chain, wide fan-out, optional `ecrawl_analyze` depth slices, optional ereport badge fixtures). Choose scale with `SYNTH_PROFILE` (unset = quick smoke, `medium`, `heavy`, `extreme`).

`SYNTH_PROFILE=extreme` layers two extra megadirs on top of the heavy-class baseline:

- `mega_dir1/` — about 20M regular files in one directory by default (`SYNTH_EXTREME_MEGA_DIR1_FILES`; always unsharded).
- `mega_dir2/` — `SYNTH_EXTREME_MEGA_DIR2_TOP_FILES` (default 2M) top-level `f…` files plus `SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS` (default 1M) subdirectories `d…/`, each with a single regular file `file` (~3M files + 1M dirs under `mega_dir2/`).

`python3` + bulk create: extreme requires `BATCH_CREATE=1` (the default) and `python3` on `PATH` — `mega_dir1` / `mega_dir2` use the same threaded bulk creator as large flat dirs and the script errors out if bulk mode or `python3` is unavailable.

Disk budget (`DISK_BUDGET_BYTES`): generation refuses when the estimated footprint exceeds the cap (default ~100 GiB). Extreme trees are metadata-heavy; 100 GiB is only an order-of-magnitude guardrail—raise `DISK_BUDGET_BYTES`, tune `ASSUMED_BYTES_PER_FLAT_FILE`, `AUTO_CAP_FLAT`, or lower `SYNTH_EXTREME_*` counts for your filesystem. All presets and tunables (`DISK_BUDGET_BYTES`, `FLAT_FILES`, badge fixtures, etc.) are documented in the comment header at the top of `scripts/fixtures/generate-ecrawl-adversarial-tree.sh`.

Example (larger cap only — adjust paths and budgets to match your host):

```bash
SYNTH_PROFILE=extreme DISK_BUDGET_BYTES=$((200 * 1024 * 1024 * 1024)) \
  ./scripts/fixtures/generate-ecrawl-adversarial-tree.sh /tmp/ecrawl-adversarial
```

## Profiling and performance work

Companion scripts profile the tools per fixture and capture a full performance picture — wall-clock timings, `strace -f -c` syscall histograms, `perf record --call-graph dwarf` CPU profiles, and optional `perf sched` thread-concurrency traces (`DO_SCHED`, gated to `SCHED_FIXTURES`) — into an uploadable tarball with a `SUMMARY_TABLE.txt`. Build with `make debug` (or otherwise ensure `-g`) for the best `perf` symbols.

They live in [`scripts/profile/`](../scripts/profile/) and share one set of crawl outputs. `profile/ecrawl-fixtures.sh` is the producer: it crawls each fixture and keeps the reusable shards at `<bin-root>/<fixture>/bin/`. The other three are consumers — they read that same `<bin-root>`, never crawl, and hard-error if a fixture's bins are missing. So always run the `ecrawl` profiler first, then point the others at the same `<bin-root>`.

- `scripts/profile/ecrawl-fixtures.sh <synth-root> <bin-root> [results-dir]` — runs `ecrawl` against each fixture in `--no-write` and write modes, isolating crawl/`readdir`/donation cost vs. uid-shard writer churn. The write-mode pass keeps each fixture's shards at `<bin-root>/<fixture>/bin/` for the consumers below (needs `DO_WRITE=1`, the default). Knobs: `DO_NOWRITE` / `DO_WRITE` / `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`, and any inherited `ECRAWL_*` (e.g. `ECRAWL_MAX_OPEN_SHARDS`).
- `scripts/profile/ereport-fixtures.sh <bin-root> [results-dir]` — profiles `ereport` (all-users, `--bucket-details 4`) over `<bin-root>/<fixture>/bin/`, writing HTML to `<bin-root>/<fixture>/all_users/`. Knobs: `BUCKET_DETAILS`, `EREPORT_THREADS`, `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`, `KEEP_REPORTS`.
- `scripts/profile/ereport_index-fixtures.sh <bin-root> [results-dir]` — profiles `ereport_index --make` (all-users) over `<bin-root>/<fixture>/bin/`, writing the index to `<bin-root>/<fixture>/index/`. The summary splits time into `chunk_prep` / `index_phase` / `merge_phase` and tabulates the `make_f{open,read,write}` I/O counters that usually drive index-build cost. Knobs: `EREPORT_INDEX_THREADS`, `EREPORT_INDEX_TRIGRAM_THREADS`, `RAISE_ULIMIT`, `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`, `KEEP_INDEX`.
- `scripts/profile/ecrawl_analyze-fixtures.sh <bin-root> [results-dir]` — profiles the read-only `ecrawl_analyze` directory-shape stats over `<bin-root>/<fixture>/bin/` (produces no kept output). Knobs: `ECRAWL_ANALYZE_THREADS`, `ANALYZE_TOP`, `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`.

```bash
# 1) ecrawl (producer): profile every fixture, both modes, with perf — keeps bins under <bin-root>
DO_PERF=1 ./scripts/profile/ecrawl-fixtures.sh /tmp/ecrawl-adversarial /data1/ecrawl-bins

# 2) consumers: reuse the same <bin-root>; no recrawl
./scripts/profile/ereport-fixtures.sh        /data1/ecrawl-bins
./scripts/profile/ereport_index-fixtures.sh  /data1/ecrawl-bins
./scripts/profile/ecrawl_analyze-fixtures.sh /data1/ecrawl-bins
```

Each run prints the results dir and a `…tar.gz` to upload; full options are in each script's comment header. For `perf`, run as root or lower `kernel.perf_event_paranoid`.

To turn a recorded `perf.data` into a readable text report (the "stdio" report):

```bash
perf report -i perf.data --stdio > report.txt
perf report -i perf.data --stdio -g graph,0.5,caller > report.caller.txt   # caller-oriented call graph
perf report -i perf.data --stdio --no-children --sort comm,pid > report.bythread.txt
```

### Indexer comparison (Robinhood / GUFI / XDU)

To compare open-source file indexers against the ecrawl suite using the same paper methodology (index time/size per 1M files, queries Q1–Q5), use [`scripts/compare-indexers/`](../scripts/compare-indexers/README.md). It reuses the adversarial tree generator and reports a `SUMMARY_TABLE.txt`. For fixture-level `perf`/`strace` on ecrawl alone, keep using `scripts/profile/ecrawl-fixtures.sh` below.

### Test- and data-driven development

Performance here is dominated by filesystem *shape*, not just file count: one 20M-entry directory, a million tiny directories, a deep skinny chain, and a high-UID-diversity tree each stress a different part of the pipeline. Guessing where the time goes is unreliable, so the workflow is deliberately data-driven:

1. Generate adversarial shapes with `scripts/fixtures/generate-ecrawl-adversarial-tree.sh` so every pathological case is reproducible on demand.
2. Profile each shape with the scripts above and read the `SUMMARY_TABLE.txt` — timings, syscall histograms, and CPU call-graphs — to name the actual bottleneck (a specific syscall, lock, or callsite) instead of guessing.
3. Change one thing, re-run the same profile, and compare. Keep the change only when the numbers move; the tarballs are the evidence trail.
4. Guard the win with `scripts/test/test.sh` (use `--summary` for a copy/paste results table) so a correctness or throughput regression surfaces immediately.

This loop turned several hunches into measured fixes — raising the writer's open-shard cap to cut uid-shard `open`/`close` churn ~90%, replacing a per-record `ftello()` in `ereport` that issued an `lseek` per record, and moving NSS lookups out of a global lock in `ecrawl` (which roughly halved a many-UID crawl). None were obvious from reading the code; the profiles pointed straight at them.

### Where AI fits

Most of this cycle is mechanical and detail-heavy, which is exactly where an AI coding assistant compresses the turnaround:

- Scaffolding the harness — the profiling scripts (consistent flags, strace/perf passes, a shared producer/consumer bin layout, throwaway vs. kept output dirs, a Python summary parser) are tedious boilerplate an assistant can draft in one pass and keep consistent across every tool.
- Reading the evidence — pasting a `perf report` head or a `strace -c` histogram and asking "what's the hot path and why" turns raw counters into a ranked hypothesis fast, often down to the source line behind a symbol.
- Implementing the fix — once a bottleneck is named (an O(N) registry scan, a per-record seek), the assistant can apply the change, preserve the surrounding invariants, and update tests and docs in the same edit.
- Closing the loop — it can re-run profiles and diff the before/after `SUMMARY_TABLE.txt` to confirm the change actually helped.

The human still owns the decisions — which trade-offs are acceptable, when a number is "good enough," and whether a change is safe to ship — but the measure → diagnose → fix → re-measure loop runs far faster when the assistant handles the scaffolding and the first pass at interpretation. Treat its diagnoses as hypotheses to confirm against the data, not as ground truth.
