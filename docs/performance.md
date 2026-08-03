# Performance and profiling

Design concepts that make the tools fast, how to generate adversarial test trees, and the profiling harness used to find and verify optimizations.

## Why this is fast (design concepts)

The tools are fast because they combine compact binary I/O, parallelism along natural boundaries, and bounded pipelines instead of naïvely scanning everything twice or holding giant locks over shared mutable state.

### Shared ideas (`ecrawl`, `edelete`, `ereport`, `ereport_index`)

- Path arguments — Directory and crawl-root arguments are normalized to canonical absolute paths with `realpath(3)` once the path exists (see `path_canon.h`). Relative inputs are supported; symlink components are resolved. Output directories that are created on demand are canonicalized after `mkdir` where applicable.
- Binary crawl records — Paths and metadata use a fixed header plus a columnar, zstd-compressed record region and (when finalized) a catalog tail (file magic `ERCBIN08`, format version 8). Each row group stores a field per column chunk with a min/max zone map, so a query decodes only the columns it names and skips groups that cannot match. Layout and rejection rules are in [binary-format.md](binary-format.md).
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

### ERCBIN08 capture-write cost

The compare-indexers harness times `ecrawl` both ways, as the `ecrawl/write` and `ecrawl/nowrite` rows of `SUMMARY_TABLE.txt`. The gap `write − nowrite` is the inferred **capture-write** cost (column encode + per-column zstd + catalog finalize). Walk-only rows (`fd`, `find`, `du`, `ecrawl --no-write`) share the same crawl/stat path and skip that producer work.

On the same host and cold-cache protocol (`node9901`, 16-thread budget, `drop_caches=1`, ~3.5M-file synth tree):

| format (CSV notes) | write sec/1M | nowrite sec/1M | inferred store (write − nowrite) | capture MiB/1M |
|---|---:|---:|---:|---:|
| `ERCBIN06_shards` (2026-07-31) | 3.545 | 2.519 | ~1.03 | 25.6 |
| `ERCBIN08_shards` (2026-08-02) | 5.086 | 2.307 | ~2.78 | 43.2 |

`fd` / `find` / `du` stayed within noise across those two runs. `ecrawl`'s write row therefore moved because the capture format got more expensive to produce, not because the machine or the walk got colder. Layout rationale (1 MiB row groups, per-column codecs) is in [binary-format.md](binary-format.md).

Where that time actually goes is not where the layout description suggests. A `perf record` of a write-mode crawl (Rocky 8 compute node, ~3.3M files, 8 crawl + 4 stat + 4 writer threads) attributes user-space cycles roughly as:

| symbol | share of write-mode profile |
|---|---:|
| `shard_cat_ensure_dir` | ~7.6% |
| `writer_process_batch_frame` | ~4.5% |
| `crawl_bin_codec_encode_u64` | ~4.4% |
| `writer_thread_main` | ~2.8% |
| `shard_cat_ht_insert` | ~1.9% |
| `realloc` | ~1.8% |

So the columnar encode is a few percent, and the **catalog** is the larger consumer. Two further constraints on any writer optimization:

- A crawl run by a normal user produces **one** uid shard, because records shard by uid. The per-shard column buffers only multiply on a multi-uid tree (the benchmark tree is generated as root with random owners), so a single-uid reproduction cannot exercise writer memory pressure at all. Peak RSS on the real runs was 499 MB for v8 against 354 MB for v6.
- Rewriting the writer to stage records row-major and transpose them at flush was tried and measured: instructions moved -0.2%, cycles +2.8%, `dTLB-load-misses` -25%, capture size +0.06%. No wall-clock win on a single-shard tree, so it was not kept. Append into fourteen parallel column arrays is already prefetch-friendly when few shards are open; the trade only plausibly pays with hundreds of shards live, which needs a root-generated multi-uid tree to demonstrate.

### Rejected: widening the stat pool with core count

A 96-core production profile showed `single_huge_dir` spending 96.4% of its CPU inside `fstatat` with the stat pool at its default of 8 threads, which reads like an obvious case for scaling `ECRAWL_STAT_THREADS` with the machine. Measuring it says otherwise.

Sweeping 8/16/32/64 stat threads over a 4M-file `single_huge_dir` on local XFS, 15 interleaved reps per cell, with **two cells declared identically at 8 threads as an A/A control**:

| stat threads | median sec | vs 8 |
|---|---:|---:|
| 8 | 2.228 | — |
| 16 | 2.082 | −6.6% |
| 8 (A/A control) | 1.806 | −18.9% |
| 32 | 3.124 | +40.2% |

The control is the result. Two runs of the same configuration differ by 18.9% depending only on where they sit in the rep cycle, so the 6.6% that 16 threads appears to win is not measurable here. What does clear the noise floor is that 32 threads is consistently worse — its *fastest* run is slower than the median at 8. Two earlier sweeps, one under a small memory allocation to force dentry reclaim, agreed on that.

`DEFAULT_STAT_THREADS` stays at 8. The profile's 96% is blocking syscall time, so the useful pool width is set by inode-read latency, not by core count, and a warm local filesystem is not the regime the production number came from — anyone revisiting this needs a cold, high-latency metadata path and an A/A control in the harness.

Stat *ordering* is the same story: `ECRAWL_STAT_INODE_ORDER` and `ECRAWL_STAT_RANDOM_QUEUE` were measured in all four combinations against the 24.3% the profile spends in `xfs_iget_cache_miss`, and every combination landed inside the same noise band. Both defaults stand. Reordering can only pay when the inode cache is cold, and dropping caches needs root.

### Rejected: reading inodes with `XFS_IOC_BULKSTAT`

A production profile (96-core EPYC, XFS) put **66.9%** of `ecrawl` inside `xfs_vn_lookup` — resolving names that `getdents64` had already returned with `d_ino` — and another 24.3% in `xfs_iget_cache_miss`. `XFS_IOC_BULKSTAT` reads inode metadata straight from the inode btree in inode order with no directory lookup at all, which looks like the obvious way to delete that work.

It is not usable here. The kernel gates the ioctl on `CAP_SYS_ADMIN`, and `ecrawl` runs as an ordinary user — it is a tool for looking at your own data, and it shards output by uid. A probe on an XFS scratch filesystem (kernel 5.14, `scripts/` under the automated-testing scratch tree) returns `EPERM` on the first call, before any of the performance questions can be asked. Running the crawler as root to avoid a lookup is not a trade worth making.

The same probe does bound what the lookups cost, by walking a 10,050-entry tree twice:

| pass | µs per entry |
|---|---:|
| `getdents64`, names only | 0.23 |
| `getdents64` + `fstatat` | 1.48 |

So inode reads are roughly 6x the cost of the walk that finds the names, which is the same gap `ecrawl --no-stat` exploits and the ceiling any lookup-avoiding scheme could aim at. Reaching it needs a mechanism an unprivileged process can actually call.

### `ereport`: reports from crawl bins

- Parallel chunk mapping — Uses `*.ckpt` to build chunk lists (byte ranges that align with record starts). Chunk count scales with file size, so `EREPORT_THREADS` has enough units of work.
- Parallel chunk parsing — Workers consume disjoint chunks; summaries and bucket histograms merge after workers finish (merge step is not on the per-record hot path across all threads).
- Parallel bucket HTML — The 36 heat-map cells map to 36 independent output files; emission fans out across threads up to that cap. Per-page `aggregate_totals_for_page_n` gives each worker a small open-addressed table keyed by row pointer instead of a worker×row matrix: a worker's slice of the records only ever touches a handful of the rows, so a table sized to that working set stays in cache, and one relaxed atomic add per touched row folds it into the shared totals at the end. Rows past the table's load limit go straight to those atomics, so a slice that touches everything still finishes without a fallback.
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

### Measured: profile-driven round two

Five changes taken from the 2026-08-02 fixture profiles, each A/B'd against the previous commit on one node with one set of inputs and alternating reps. Where a result was small, the harness declares the same configuration twice as an A/A control, so the noise floor is measured rather than assumed — that control is what turned two of these from "looks like a win" into a decision.

| change | fixture | effect |
|---|---|---:|
| size-gate the catalog `mmap` | 8,000-shard corpus | **−23.8%** |
| parent-map arena + `dir_id` keying | 4M-record whole tree | **−58.5%** |
| entry-driven work donation | `depth_slash_profile` | **−45.7%** |
| `wb_arena` chunk freelist | 4M-record index build | **−4.1%** |
| path length in `path_row_t` | `ereport --bucket-details 4` | within noise |

Notes on the two that need them:

- **Catalog `mmap` gate.** Mapping every catalog regardless of size was a regression on many-small-shard corpora: `munmap` in a multi-threaded process sends a TLB shootdown IPI to every CPU the process has touched, and that cost is per call, not per byte. Below 1 MiB the catalog now loads with plain reads. Measured on 8,000 shards of ~27 KB each: 0.21 s → 0.16 s with no overlap between the two distributions, and byte-identical reports.
- **Work donation on entries.** Donation used to key only off the local stack growing, which a deep narrow chain never does — one subdirectory per directory never reaches the floor of 8. `depth_slash_profile` ran with `tasks_popped=2` and `donated_dirs=6`; it now reports 6 and 16, and on the whole tree `avg_active_workers` rose from 6–13 to 15–31 at unchanged wall time.

Two items from the same round were measured and **not** taken: the stat-pool width and stat ordering defaults, both above.

### Rejected: catalog slot defaulting, and a cap on the fpcache buffer

The 2026-08-03 `ecrawl_analyze` profile put `catalog_ensure_slots` at 5.97% self time on `neutral_flat` and `crawl_fpcache_fopen` at ~31% inclusive on a many-shard capture. Neither converted into recoverable time. Both attempts are recorded here because the instruction counts show precisely *why*, and because the same reasoning would otherwise be tried again.

Setup for every number below: one exclusive node (`node5103`, `mit_preemptable`, AMD EPYC 9654, Rocky 8.10), 32 threads, the `r6/binroot` v8 captures on NFS, page cache warmed once per build, 31 alternating reps, default allocator. The baseline is the working tree with *only* the change under test reversed — not `HEAD`, because the tree already carried unrelated edits to `crawl_bin_block.c`, `ecrawl.c` and `ecrawl_analyze.c` that an A/B against `HEAD` would have credited here. `diff -r` confirmed the two trees differed in one file. "Noise" means the gap did not exceed twice the standard error of the difference; with 31 reps that threshold is about 1.2% of task-clock, so an effect larger than that would have been visible.

`catalog_ensure_slots` defaulted 18 arrays one `dir_id` at a time and `catalog_store_entry` then overwrote all 18, so on a dense shard every one of those stores is dead. Two ways to stop paying for them were measured on `neutral_flat` (782,402 parents, one shard):

| | instructions | task-clock | cycles | page-faults | peak RSS |
|---|---:|---:|---:|---:|---:|
| bulk `memset` per array | **−14,085,195 (−1.43%)** | +1.12% | +1.31% | −0.01% | +0.15% |
| skip the slot entirely | **−15,675,222 (−1.60%)** | −0.72% | +0.13% | −0.00% | +0.05% |

Every column but the first is noise. The instruction column is not: its spread is ±0.011%, and the first row lands within 0.014% of the arithmetic (782,402 directories × 18 arrays = 14,083,236 stores). The mechanism is exactly as described, and it is worth nothing.

The first attempt bulk-defaulted slots `1..n` up front, which both loaders can do because they know the entry count before they parse. That only *vectorized* the stores — the same bytes are still written, as one AVX-512 `memset` per array instead of scalar stores striped across 18 — so the page-fault count does not move and neither does the time. The second attempt stopped writing them at all: defaulting only the gap strictly below `dir_id`, on the contract that the caller stores that entry immediately. It removes 1.6% of all instructions the program executes and still does not move the clock, because `catalog_store_entry` pulls those same cache lines in regardless. The defaulting stores were never the cost — they were absorbed by lines that the very next instruction was going to dirty anyway. What the pass is actually bound by is visible in its profile: `__memmove_avx512` at 27.5% and `analyze_scan_fp_until` at 21.6%, with `crawl_bin_catalog_load_sel` at 7.2%.

On `wide_shallow` and `single_huge_dir` the instruction delta is under 13,000 and every metric is noise, which is the expected shape — 491 parents have no defaulting worth skipping.

**The fpcache buffer cap is rejected outright.** `ecrawl_analyze` requests a 1 MiB `setvbuf` buffer on every real open; capping it at the file's own size was expected to cut resident memory on a many-shard capture. Measured on 1,000 synthetic 12 KB shards it changed nothing: instructions +0.01%, task-clock +1.01%, page-faults +0.14%, peak RSS +3.76%, all noise. The premise was wrong twice over. A 1 MiB buffer that stdio only fills to 12 KB costs 12 KB of resident memory rather than 1 MiB, because the untouched pages never become resident — peak RSS for that entire run is ~10 MB. And the `munmap` TLB-shootdown cost that motivated it cannot arise, because `tune_allocator()` raises glibc's `mmap` threshold from 128 KiB to 32 MiB and nothing in the tree sets `EREPORT_ALLOC_TUNE=0`.

What survives from this round is `test_crawl_catalog`, 390 checks wired into `make check`. It drives synthetic catalog blobs through both the mapped and the stdio loader and asserts that a slot no entry ever claims still reads as unwritten — in particular that `imm_child_min_eff_time` stays `UINT64_MAX` instead of becoming the epoch, which is the one default that is not zero and the one failure that would be silent rather than a crash. It covers sparse ids, ids arriving out of order, ids past the entry count, all four `CRAWL_CAT_*` gate combinations, and a reused catalog struct, and it passes identically on the baseline and on both attempts. `ecrawl_analyze`'s own key=value output was byte-identical across all three fixtures as well.

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

Reuse (`FIXTURE_MANIFEST.txt`): a finished tree records the parameters that built it, a fingerprint over them, and per-subdirectory counts. Re-running against the same root with the same parameters exits 0 without rebuilding, which is what makes it practical to keep an `extreme` tree on scratch and point run after run at it. `FORCE=1` rebuilds over the top; `MANIFEST_VERIFY=1` additionally walks the finished tree and records measured counts, at the cost of a second full traversal. Re-running with *different* parameters against an existing tree is an error rather than an overlay, since mixing two trees produces one that matches neither — `rm -rf` the root for a clean rebuild. The manifest is written only on success, so an interrupted run leaves none and the next run rebuilds. Knobs that do not change the tree (the disk-budget model, `BATCH_CREATE` / `CREATE_JOBS`, `ECRAWL_FLAT_LOW` / `ECRAWL_FLAT_HIGH`, the output path, and the script's own hash) are excluded from the fingerprint so they never force a rebuild.

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
