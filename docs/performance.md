# Performance and profiling

Design concepts that make the tools fast, how to generate adversarial test trees, and the profiling harness used to find and verify optimizations.

## Why this is fast (design concepts)

The tools are fast because they combine compact binary I/O, parallelism along natural boundaries, and bounded pipelines instead of naïvely scanning everything twice or holding giant locks over shared mutable state.

### Shared ideas (`ecrawl`, `edelete`, `ereport`, `ereport_index`)

- Path arguments — Directory and crawl-root arguments are normalized to canonical absolute paths with `realpath(3)` once the path exists (see `path_canon.h`). Relative inputs are supported; symlink components are resolved. Output directories that are created on demand are canonicalized after `mkdir` where applicable.
- Binary crawl records — Paths and metadata use a fixed header plus a columnar, zstd-compressed record region and (when finalized) an equally columnar catalog tail (file magic `ERCBIN09`, format version 9). Each row group stores a field per column chunk with a min/max zone map, so a query decodes only the columns it names and skips groups that cannot match. Layout and rejection rules are in [binary-format.md](binary-format.md).
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
- Stat workers can also take crawl tasks when their own queue is empty (`ECRAWL_STAT_HELPS_CRAWL`, default 0), lifting the fixed split between the two pools. Off because it measured no faster — see [letting stat workers also crawl](#rejected-letting-stat-workers-also-crawl), which is also where the stat pool's zero-batch case on small-directory trees is documented.
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

### Rejected: letting stat workers also crawl

`ecrawl` runs three fixed thread pools and picks the crawl/stat split before anything is known about the tree, so it seemed likely that some tree shape would leave one pool starved. One does, and dramatically. From a 1.3M-entry `neutral_flat` run (782K directories, two or three entries each) with `ECRAWL_STAT_THREADS` at its default of 8:

```
stat_batches_enqueued=0
stat_batches_tail_inlined=520000
```

The stat pool receives **zero** batches for the entire run. Every batch is an end-of-directory batch below `ECRAWL_STAT_BATCH_MIN_OFFLOAD` (32), so it is deliberately statted on the crawl thread instead of paying a queue round-trip for three names — the right call per batch, but the consequence is that eight threads exist and are handed nothing while crawl threads do 520,000 inline stat batches.

Letting an idle stat worker take a crawl task fixes the imbalance and changes no wall-clock time. Six fixtures on the extreme tree, seven reps per cell **interleaved** off/off/on rather than grouped, with **two cells declared identically as an A/A control**:

| fixture | off | off (A/A) | helpers on | A/A gap | apparent gain |
|---|---:|---:|---:|---:|---:|
| mega_dir1 | 10.206 | 10.625 | 10.356 | 4.1% | −1.5% |
| mega_dir2 | 3.647 | 3.597 | 3.571 | 1.4% | 2.1% |
| single_huge_dir | 2.056 | 1.945 | 2.042 | 5.4% | 0.7% |
| neutral_flat | 0.787 | 0.701 | 0.749 | 10.9% | 4.8% |
| wide_shallow | 0.501 | 0.539 | 0.515 | 7.6% | −2.8% |
| depth_slash_profile | 0.128 | 0.123 | 0.129 | 3.9% | −0.8% |

Every difference is inside the control gap, and the raw reps overlap almost completely — on `neutral_flat` the two identical configs span 0.668–0.815 and 0.624–0.816, wider than any gap to the helpers-on cell. Helpers were doing real work while this happened: 2,146 crawl tasks taken on `neutral_flat`, 47,836 on `mega_dir2`.

An earlier run on the medium tree did show 18.6% and 11.2% gains against A/A gaps of 4.3% and 3.6%, and it was wrong: that harness ran all reps of one config before moving to the next, so config differences absorbed any drift in machine state. Grouped reps are the same trap the stat-thread sweep below documents. Interleaving reversed the answer, which is worth remembering — a harness bug here looks exactly like a win.

The framing was the real mistake. "Eight idle threads" is not wasted capacity when cores are spare: a thread parked in `pthread_cond_wait` costs nothing, and the run is bound by I/O and by the pending-batch drain, neither of which more crawlers relieve. Thread idleness only becomes a cost when runnable threads exceed cores.

The code is kept behind `ECRAWL_STAT_HELPS_CRAWL`, **default 0**, for the case these measurements cannot reach: crawl threads configured at or below the core count while the stat pool sits idle, where the extra crawlers would be real parallelism rather than surplus. It is verified rather than merely present — `entries`, `dirs`, `total_bytes`, and `errors` identical across configs on eight fixtures in write mode; the recorded path set, read back out of the capture with `ereport_index --make` plus `--search`, byte-identical between configs on five directory-heavy fixtures (1,302,401 paths on `neutral_flat` with 378 helper tasks); and `scripts/test/test.sh` green with the switch both ways.

Two details make it safe to turn on:

- A helper stats **inline** while crawling (`tls_stat_inline_only`) rather than queueing batches. If every stat worker were crawling and waiting for the pool to drain its own batches, those batches would have no one left to run them. Statting inline makes a helper independent of the pool it came from.
- `should_donate_work` derives idle capacity from `crawl_threads_started`. Helpers count in `g_active_workers`, so without counting them in the denominator too, `idle` goes negative and the conservative all-busy branch is selected permanently — throttling donation exactly when there are *more* threads available to consume it.

Shutdown is the risk worth naming, since quiescence is "seeding done, queue empty, nobody active". A helper takes tasks through the same `g_active_workers` increment as a crawl worker, so the queue cannot latch closed while one holds work, and stat batches are always drained inside the directory that created them, so nothing can push after the last thread goes idle. `discovered_dir_batch_flush` already reports a push to a closed queue as an error instead of dropping it, so a flaw in that reasoning surfaces as a visible error rather than a missing subtree.

One trap for anyone re-testing this: comparing `ecrawl --no-stat | sort` against `find | sort` is the obvious path-set check and it cannot see this change at all, because `--no-stat` forces `ECRAWL_STAT_THREADS` to 0 — no stat pool, no helpers.

### Rejected: statting inline when the stat queue is full

The other direction of the same imbalance looked just as plausible: a crawl thread reading a megadirectory should be able to outrun the stat pool, fill the bounded queue, and block in `stat_batch_enqueue` waiting for a slot. Since inode reads cost roughly 6x a dirent read (see the [`BULKSTAT` probe](#rejected-reading-inodes-with-xfs_ioc_bulkstat) below), a single reader ought to saturate several statters easily.

It never happens. Sweeping the stat pool from 1 to 16 threads over `mega_dir1` — 12,000,001 files in one directory, so exactly one crawl thread feeding the whole pool — `wait_stat_enqueue` is **0** in every cell, and the queue never reaches its 64 slots even when a single stat thread stretches the run past 10s:

| stat threads | sec | `stat_queue_depth_max` | `wait_stat_enqueue` |
|---|---:|---:|---:|
| 1 | 10.54 | 59 | 0 |
| 2 | 6.39 | 54 | 0 |
| 4 | 4.87 | 51 | 0 |
| 8 | 5.88 | 48 | 0 |
| 16 | 5.04 | 48 | 0 |

The reason is `STAT_PENDING_DRAIN_EVERY_READDIRS`: every 65536 dirents, a crawl thread waits for all of its outstanding batches. At the default batch size of 1024 names that is 64 batches — exactly `ECRAWL_STAT_QUEUE_BATCHES`. The reader is stopped by its own drain before the queue it feeds can fill, so the blocking enqueue is structurally unreachable rather than merely rare, and a change that avoids blocking there has nothing to avoid. Across eight fixtures the inline path fired 11 times in total.

The code is kept behind `ECRAWL_STAT_FULL_INLINE`, **default 0**, for cold or remote metadata, where a stat costs far more than a dirent read and the queue plausibly does fill. Turn it on only with `wait_stat_enqueue > 0` as evidence.

The sweep does expose a real bottleneck, just not that one: the drain is a *barrier* rather than backpressure, because the reader waits for all 64 batches to finish instead of enough of them to make room, then refills from empty. Hence the plateau above — one reader on one directory sees no benefit past 4 stat threads, and 16 threads is no better than 2. Compare `single_huge_dir`, the same 12M entries spread over 2,931 directories so several crawl threads read at once, which goes from 10.81s to 1.13s across the same sweep. Draining only as far as the next free slot is the more promising thing to attack next.

Caveat on all of the above: these runs are warm-cache on node-local RAID, because `drop_caches` needs root. Warm cache is the regime where coordination overhead is visible and storage latency is not, which is what makes it the right place to measure a *scheduling* change — and the wrong place to conclude anything about cold, high-latency metadata.

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

### Measured: `ecrawl_query`'s per-directory cost

Round two's profiles showed `ecrawl_query` paying by the directory, not by the record: `single_huge_dir` did 2,000,490 records over 491 parents in 0.02 s, while `neutral_flat` did 1,302,401 records over 782,402 parents in 0.36 s. That is ~16 ns per record against ~1.9 us per distinct directory, and the difference was one string-keyed hash map — `parent_map_get_or_add` at 26% self time, `pthread_mutex_lock` at 22%.

The map was there to merge the same directory across shards. Inside a single shard it buys nothing, because `dir_id` is already a bijection with the path (`ecrawl.c` hands ids out one per directory as it meets them), and `neutral_flat` is one shard. So the work went in two tiers: first six edits that only remove work, then a restructure that keys the scan on `dir_id` and defers paths.

| fixture | tier 1 | tier 2 on top | total | A/A control |
|---|---:|---:|---:|---:|
| `neutral_flat` (782 K parents, 1 shard) | **−41.9%** | **−17.6%** | **−52.1%** | 0.0% |
| `single_huge_dir` (2 M records, 491 parents) | **−50.6%** | −4.9% | **−53.0%** | 7.3% |
| 1000-shard corpus | **−25.6%** | +2.4% | **−23.8%** | 4.1% |
| `wide_shallow` | **−38.3%** | −3.4% | **−40.4%** | 8.5% |
| `deep_skinny_chain` | **−16.7%** | −2.9% | **−19.0%** | 2.4% |

One node, 32 threads, five interleaved reps of ten runs each, with the baseline declared a second time as an A/A control. Only `neutral_flat` clears that control on the tier 2 column, which is the expected shape: tier 2 removes per-directory cost, and `neutral_flat` is the only corpus here that is dominated by distinct directories. On the others tier 2 is worth roughly nothing on its own and the total is tier 1's. Every build printed a byte-identical report on every fixture and every rep.

**Tier 1, six edits that only remove work.** The shape pass loaded `CRAWL_CAT_SUBTREE` — nine `uint64_t` arrays and a byte array, ~73 B per directory — and read none of it, so it now asks for it only when `--subtree` will actually use it. It projected the name bytes column and used them for one `memchr` looking for a slash that `crawl_bin_format.h` promises is not there; dropping the column turns the largest column on disk into part of a coalesced skip seek and removed the 5.5% `__memmove_avx512` that was copying it. The path length that `crawl_bin_catalog_dir_path_len` already computed was thrown away and re-derived with `strlen`. The hash was byte-at-a-time FNV-1a over an ~80-byte path, an 80-long dependent multiply chain per insert. The bucket array was a fixed 262,144 entries, which at 782 K parents is load factor 3.0 and three random arena chases per lookup; it is now sized from the record count. And the counter bump was one atomic per record on a line every worker shares, where records arrive in runs sharing a parent — counting the run in plain locals and flushing on change is what `single_huge_dir` and `wide_shallow` were paying for.

**Tier 2, `dir_id` on the hot path.** The scan now bumps `counts[parent_dir_id]` in a dense per-shard array and touches no hash table, no lock and no string: after this, `pthread_mutex_lock` is gone from the profile entirely. Paths are built exactly once, in a fold that walks a shard's counters in ascending `dir_id` after the last chunk of it has been scanned. Ascending order is what makes that cheap — a parent always has a lower id than its children, so a path is its parent's path plus one component, and caching the last parent's prefix turns a run of siblings into one chain walk instead of one per directory. A single-shard corpus skips the map entirely, since its `dir_id` is already the key and every node the fold produces is unique by construction.

Two things this round had to get right rather than improve on. `crawl_bin_catalog_dir_path_len` silently drops the components it did not reach past a 128-component limit, so "parent's path plus a name" gives a 129-component path where the direct walk gives 128 — `deep_skinny_chain` is exactly that shape, and the fold now defers to the walk past the limit rather than print a better answer than the baseline.

The other was the fold's own scheduling, and it cost the thousand-shard corpus its whole tier 1 win: as a phase of its own it is a serial tail after work that tier 1 had spread across the scan, measuring **+29.4%** against tier 1 where the shipped version measures +2.4%. A shard small enough to fold in one pass is now folded by the worker that finishes its last chunk, so that work is overlapped again and the catalog goes back to the allocator immediately rather than every catalog being held until the end. Bigger shards still wait for the post-scan fold, which can split one of them across threads — which is what `neutral_flat`, a single 782 K-directory shard, needs.

### Rejected: catalog slot defaulting, and a cap on the fpcache buffer

The 2026-08-03 `ecrawl_query` profile put `catalog_ensure_slots` at 5.97% self time on `neutral_flat` and `crawl_fpcache_fopen` at ~31% inclusive on a many-shard capture. Neither converted into recoverable time. Both attempts are recorded here because the instruction counts show precisely *why*, and because the same reasoning would otherwise be tried again.

Setup for every number below: one exclusive node (`node5103`, `mit_preemptable`, AMD EPYC 9654, Rocky 8.10), 32 threads, the `r6/binroot` v8 captures on NFS, page cache warmed once per build, 31 alternating reps, default allocator. The baseline is the working tree with *only* the change under test reversed — not `HEAD`, because the tree already carried unrelated edits to `crawl_bin_block.c`, `ecrawl.c` and `ecrawl_query.c` that an A/B against `HEAD` would have credited here. `diff -r` confirmed the two trees differed in one file. "Noise" means the gap did not exceed twice the standard error of the difference; with 31 reps that threshold is about 1.2% of task-clock, so an effect larger than that would have been visible.

`catalog_ensure_slots` defaulted 18 arrays one `dir_id` at a time and `catalog_store_entry` then overwrote all 18, so on a dense shard every one of those stores is dead. Two ways to stop paying for them were measured on `neutral_flat` (782,402 parents, one shard):

| | instructions | task-clock | cycles | page-faults | peak RSS |
|---|---:|---:|---:|---:|---:|
| bulk `memset` per array | **−14,085,195 (−1.43%)** | +1.12% | +1.31% | −0.01% | +0.15% |
| skip the slot entirely | **−15,675,222 (−1.60%)** | −0.72% | +0.13% | −0.00% | +0.05% |

Every column but the first is noise. The instruction column is not: its spread is ±0.011%, and the first row lands within 0.014% of the arithmetic (782,402 directories × 18 arrays = 14,083,236 stores). The mechanism is exactly as described, and it is worth nothing.

The first attempt bulk-defaulted slots `1..n` up front, which both loaders can do because they know the entry count before they parse. That only *vectorized* the stores — the same bytes are still written, as one AVX-512 `memset` per array instead of scalar stores striped across 18 — so the page-fault count does not move and neither does the time. The second attempt stopped writing them at all: defaulting only the gap strictly below `dir_id`, on the contract that the caller stores that entry immediately. It removes 1.6% of all instructions the program executes and still does not move the clock, because `catalog_store_entry` pulls those same cache lines in regardless. The defaulting stores were never the cost — they were absorbed by lines that the very next instruction was going to dirty anyway. What the pass is actually bound by is visible in its profile: `__memmove_avx512` at 27.5% and `analyze_scan_fp_until` at 21.6%, with `crawl_bin_catalog_load_sel` at 7.2%.

On `wide_shallow` and `single_huge_dir` the instruction delta is under 13,000 and every metric is noise, which is the expected shape — 491 parents have no defaulting worth skipping.

**The fpcache buffer cap is rejected outright.** `ecrawl_query` requests a 1 MiB `setvbuf` buffer on every real open; capping it at the file's own size was expected to cut resident memory on a many-shard capture. Measured on 1,000 synthetic 12 KB shards it changed nothing: instructions +0.01%, task-clock +1.01%, page-faults +0.14%, peak RSS +3.76%, all noise. The premise was wrong twice over. A 1 MiB buffer that stdio only fills to 12 KB costs 12 KB of resident memory rather than 1 MiB, because the untouched pages never become resident — peak RSS for that entire run is ~10 MB. And the `munmap` TLB-shootdown cost that motivated it cannot arise, because `tune_allocator()` raises glibc's `mmap` threshold from 128 KiB to 32 MiB and nothing in the tree sets `EREPORT_ALLOC_TUNE=0`.

**Replicated on a second machine, against the repo's own captures.** Both changes were rebuilt from a frozen pair of trees and re-measured on `node5500` (`mit_testing`, Intel Xeon Platinum 8570, Rocky 8.10, glibc 2.28, gcc 12.2), 32 threads, the `r6/binroot` captures copied to node-local XFS, 11 alternating reps for wall clock and 7 for `perf stat` and peak RSS, default allocator. `diff -r` over the two source trees, before either was built, listed exactly four files: `crawl_fpcache.c`, `crawl_fpcache.h`, `crawl_bin_catalog.c`, `crawl_bin_catalog.h` — the pre-existing `CRAWL_BIN_CATALOG_MAX_PATH_PARTS` work sits in both copies unchanged. Every corpus printed a byte-identical report from both builds.

On `neutral_flat` the bulk-`memset` form takes instructions from 982,238,454 to 968,161,199. That delta, 14,077,255, lands within 0.04% of the same 782,402 × 18 arithmetic on a different vendor's core, so the instruction saving is real and portable. Page faults went 29,971 to 29,972. Wall clock was −4.8% against a 15% min-max spread on both builds; on an eight-link `neutral_flat` corpus, whose 0.18 s runs are steady to 8%, it was +2.2%. The one place the removed work shows is the loader's own self time — `catalog_ensure_slots` is `static` and inlines, so it has no symbol of its own — where `crawl_bin_catalog_load_sel` goes 4.61% → 2.64% on `neutral_flat` and 3.61% → 1.99% on the eight-link corpus. The loader does get cheaper. It is 4% of the run, and the rest of the run absorbs it.

The buffer cap can only bind on a shard smaller than the 1 MiB request, and of the captures here only `deep_skinny_chain` (30,570 B) is; `ereport_badge_fixtures` is 2.7 MiB and `neutral_flat` 120 MiB, so on those the cap is a no-op by construction and both measure 0.0%. On 1,019 links of the small shard the cap takes the buffer to 32 KiB and still changes nothing: wall +0.6%, instructions −0.2%, page-faults −0.3%, peak RSS 8,480 → 8,840 KiB, every one of them inside a 9–13% spread. Forcing the case the cap was meant to foreclose — `EREPORT_ALLOC_TUNE=0`, so glibc's 128 KiB threshold is back and a 1 MiB buffer really is its own `mmap` — moved it −3.0% against a 9.5% spread, which is still nothing. A 1,019-link badge corpus says where the time in a many-shard run actually goes: 66% `crawl_bin_block_reader_next` and 26% `analyze_scan_fp_until`, both record decode, with no `open`-side symbol above 0.15%. Those links share one inode on local disk, which is the cheapest `open` there is, so this bounds the buffer question rather than the per-shard `open` cost on a real multi-user capture.

What survives from this round is `test_crawl_catalog`, 390 checks wired into `make check`. It drives synthetic catalog blobs through both the mapped and the stdio loader and asserts that a slot no entry ever claims still reads as unwritten — in particular that `imm_child_min_eff_time` stays `UINT64_MAX` instead of becoming the epoch, which is the one default that is not zero and the one failure that would be silent rather than a crash. It covers sparse ids, ids arriving out of order, ids past the entry count, all four `CRAWL_CAT_*` gate combinations, and a reused catalog struct, and it passes identically on the baseline and on both attempts. `ecrawl_query`'s own key=value output was byte-identical across all three fixtures as well.

### Rejected: reading the catalog's `imm_child_*` rollups instead of rebuilding them

`ecrawl` already writes per-directory child rollups into every shard catalog — `imm_child_files`, `imm_child_dirs`, `imm_child_symlinks`, `imm_child_bytes`, `imm_child_min/max_eff_time`, `imm_child_ctime_led_count` — and `ereport` under `--bucket-details` builds what looks like the same thing from scratch: `fanout_parent_stat_accumulate` hashes every matched record's reconstructed parent path and accumulates the same counts. Reading columns that are already on disk instead of recomputing them from 9.75M records is the obvious trade. It is exact in one case and worth nothing in that case, and it cannot be done at all in the others.

**Where it is exact.** The rollups are unconditional: `ecrawl` counts every child of a directory, with no notion of uid, of a subtree, or of which time basis the report will pick. So they answer `ereport`'s question only for an all-users run with no `--subtree` on the effective-time basis. Any uid filter, any `--subtree`, or `--time-basis mtime`/`atime` makes the on-disk number a different quantity than the one the table prints, so those runs keep the record-side accumulation regardless. That is most runs.

**What it measures in the case where it is legal.** Three fixtures on `node5103`, seven interleaved reps per cell, four builds from the same tree differing only in which side computes the numbers. `fanoutonly` deletes the record-side accumulation and leaves the table wrong (a correctness ceiling, not a candidate); `statsonly` deletes it and drops the table; `catfanout` deletes it and rebuilds the table from the catalog, which is the actual proposal:

| fixture | base wall | catfanout | delta | base spread |
|---|---:|---:|---:|---:|
| neutral_flat (782,402 parents) | 2.189 | 2.159 | **−1.4%** | 6.3% |
| wide_shallow | 0.317 | 0.315 | −0.6% | 5.1% |
| single_huge_dir | 0.939 | 0.929 | −1.0% | 7.6% |

The most directory-heavy fixture in the set moves 1.4% inside a 6.3% run-to-run spread; the others do not move at all. Peak RSS goes the wrong way on `neutral_flat`, 1,182 MB to 1,188 MB. The reports are byte-identical (`catfanout: IDENTICAL` on all three fixtures, against `DIFFERS` for the two ceiling builds, which is what confirms the arithmetic agrees).

The ceiling explains the outcome. Deleting the record-side map outright is worth **−19.0%** on `neutral_flat` — `fini_dirmaps_dense` alone goes 0.438 s to 0.276 s — so there is real time there. Rebuilding the same table from 782 K catalog rows costs all of it back, because the Dense-parents table is keyed by *path* and the catalog is keyed by `dir_id`: recovering the key means walking each directory's parent chain and materializing its path, which is the same work the record side was doing, just moved and no longer amortized over the runs of consecutive same-parent records that make the record-side version cheap.

**Two structural blockers, either of which is fatal on its own.** `shard_cat_finalize` (`ecrawl.c:2920`) rolls `subtree_files` / `subtree_dirs` / `subtree_symlinks` *up the tree in place*, so what reaches disk is the subtree total, not the immediate per-type breakdown the Dense-parents table prints. The immediate counts exist only in memory during the crawl. `fanout_parent_stat_accumulate` therefore cannot be removed at all, only duplicated. And `ereport` frees each shard catalog the moment its parse workers join, precisely to bound peak RSS; reading rollups after the scan means holding every catalog to the end of the run, which is the memory regression the early free was added to prevent.

One column is dead on arrival regardless: `imm_child_ctime_led_count` has no consumer even in principle. `ereport` tracks ctime-led records per age-by-size cell, never per directory, so there is no report in which a per-directory ctime-led count is the number being printed.

Raw data: `~/orcd/scratch/ereport-automated-testing/results/ereport-rollups3-19608592` (`SUMMARY.txt`, `PARITY.txt`, `runs.csv`), with the two earlier rounds alongside it.

### Rejected: a larger row group raw target

Compressing the catalog for v9 moved the cost: the record region is now 93.6% of a mixed capture against the catalog's 5.4%, so `CRAWL_BIN_ROWGROUP_RAW_TARGET` sits on top of essentially all of the remaining space. Raising it should pay twice, by amortizing the per-frame overhead of a zstd frame per column over more records and by lengthening the window each frame gets to find redundancy in. It does pay, by 6.5% at 8 MiB, and every other number moves the wrong way.

Setup for everything below: one node (`node1607`, 32 CPUs requested, `--mem=96G`, `mit_normal`), fixture trees and `TMPDIR` on node-local `/scratch`, `ECRAWL_QUERY_THREADS=16`, medians of 9 reps with the arms **interleaved inside each rep**, on the two Phase 1 trees — `dirheavy` (2M directories, one file each, 4,000,006 records) and `mixed` (600k directories, 8 files each, 5,400,006 records). A **second, independently built 1 MiB tree is declared as an A/A control** (the `A/A` column), so the noise floor is measured rather than assumed. Every arm built with zero warnings, and every query answer is identical across arms: sorted, counter-filtered `ecrawl_query` output over eight query shapes matched the 1 MiB control byte for byte on both trees.

Total capture size, on-disk `bin` + `ckpt`:

| tree | 1 MiB | 2 MiB | 4 MiB | 8 MiB | A/A |
|---|---:|---:|---:|---:|---:|
| dirheavy | 31.61 MiB | 31.17 MiB (−1.4%) | 31.33 MiB (−0.9%) | **29.55 MiB (−6.5%)** | 31.60 MiB (−0.03%) |
| mixed | 31.17 MiB | 30.95 MiB (−0.7%) | 30.86 MiB (−1.0%) | **29.93 MiB (−4.0%)** | 31.13 MiB (−0.1%) |

The curve is not monotone — 4 MiB is *worse* than 2 MiB on `dirheavy` — and the per-column breakdown says why the whole range is so narrow. On `dirheavy`, going from 1 MiB to 8 MiB: `inode` 14.87 → 14.85 MiB, `name_bytes` 4.57 → 4.42 MiB, `parent_dir_id` 3.94 → 2.37 MiB, and no other column exceeds 64 KiB. Practically the entire win is one column, and the ceiling on any win is set by the column that will not move: `inode` is 62.9% of the record region at 1 MiB and 68.2% at 8 MiB, it encodes `FOR_BITPACK` at a 1.06x ratio, and a longer window does nothing for it because a stream of unrelated inode numbers has no redundancy to find at any length. `mixed` is the same shape with `inode` at 68.7% and unmoved.

Query medians, seconds:

| tree | shape | 1 MiB | 2 MiB | 4 MiB | 8 MiB | A/A |
|---|---|---:|---:|---:|---:|---:|
| dirheavy | `--subtree` rollup | 0.0715 | 0.0719 | 0.0717 | 0.0725 | 0.0717 |
| dirheavy | `--top,dense,deep 64` | 0.2359 | 0.2380 | 0.2343 | 0.2323 | 0.2393 |
| dirheavy | `--subtree --type f --size-gt 100 --list` | 0.0817 | 0.0827 | 0.0836 | 0.0862 | 0.0818 |
| dirheavy | `--index-dir` subtree | 0.0042 | 0.0042 | 0.0040 | 0.0041 | 0.0040 |
| dirheavy | `--size-gt 500M` (all groups skip) | 0.0058 | 0.0051 | 0.0049 | 0.0048 | 0.0056 |
| dirheavy | `--size-gt 63` (partial skip) | 0.0062 | 0.0064 | 0.0073 | **0.0097 (+56%)** | 0.0060 |
| dirheavy | narrow `--subtree --list` | 0.1470 | 0.1497 | 0.1551 | **0.1585 (+7.8%)** | 0.1483 |
| dirheavy | same, `ECRAWL_QUERY_BLOCK_SKIP=0` | 0.0192 | 0.0205 | 0.0235 | **0.0263 (+37%)** | 0.0194 |
| mixed | `--subtree` rollup | 0.0278 | 0.0287 | 0.0283 | 0.0279 | 0.0277 |
| mixed | `--top,dense,deep 64` | 0.0845 | 0.0832 | 0.0837 | 0.0819 | 0.0840 |
| mixed | `--subtree --type f --size-gt 100 --list` | 0.0504 | 0.0525 | 0.0566 | **0.0639 (+27%)** | 0.0509 |
| mixed | `--index-dir` subtree | 0.0041 | 0.0041 | 0.0041 | 0.0040 | 0.0041 |
| mixed | `--size-gt 500M` (all groups skip) | 0.0058 | 0.0053 | 0.0052 | 0.0048 | 0.0059 |
| mixed | `--size-gt 25` (partial skip) | 0.0213 | 0.0223 | 0.0246 | **0.0300 (+41%)** | 0.0214 |
| mixed | narrow `--subtree --list` | 0.0505 | 0.0527 | 0.0574 | **0.0616 (+22%)** | 0.0508 |
| mixed | same, `ECRAWL_QUERY_BLOCK_SKIP=0` | 0.0219 | 0.0229 | 0.0256 | **0.0314 (+43%)** | 0.0224 |

Two identical builds differ by at most 4.8%, and every row over 1.5% is a query that finishes in under 6 ms where a tenth of a millisecond is already a percent; the bolded regressions are an order of magnitude above that floor, while the two shapes that get *faster* — the fully-skippable `--size-gt 500M` and the whole-catalog `--top,dense,deep` — move by about as much as the control does. The rollup and the sidecar-answered subtree do not touch the record region at all and correctly do not move.

**The regression is not the zone maps going blind, which is what the experiment was designed to catch.** On `dirheavy`'s partial-skip query the skip rate barely falls: 468 of 469 groups skipped at 1 MiB, 61 of 62 at 8 MiB, 99.8% against 98.4%, with `records_scanned` identical at 4,000,006 in every arm. One group survives in both cases. It costs 56% more because that one group is 65,536 records at 8 MiB against 8,544 at 1 MiB — the *granularity of a miss* grew eightfold even though the *fraction* of misses did not. The `ECRAWL_QUERY_BLOCK_SKIP=0` row isolates the rest: with skipping switched off entirely, 8 MiB is still 37% and 43% slower, because `parse_chunk_jobs` falls from 46 to 31 on `dirheavy` and 57 to 42 on `mixed` — the row group is the parallel unit, and coarsening it costs concurrency on top of read amplification. Both effects are structural, and neither is fixed by choosing a better predicate.

Write throughput does not move. `dirheavy` crawl medians are 4.4336 / 4.3974 / 4.5219 / 4.5133 s against 4.4461 s for the A/A arm, and the nine reps of the 1 MiB arm alone span 4.3494–4.4921 s, which covers every cell; `mixed` is 2.1315 / 2.0119 / 2.0735 / 2.0716 against 2.0516, with the control arm the slowest of the five.

**Writer memory is the hard stop, and it is invisible on the obvious fixture.** Peak RSS of the crawl on these trees is 731.8 / 734.6 / 738.1 / 755.9 MiB across the sweep — flat, and meaningless: records shard by uid, both trees are single-uid, and one live shard buffers one row group no matter what the target is. Measuring the real thing needs many uids, which unprivileged means an `LD_PRELOAD` stat shim that takes `st_uid` from a `uNNN_` name prefix, and a fixture where every shard can actually *fill* an 8 MiB group — 1024 owner directories of 22,900 files with 255-byte names, 23,450,625 entries, sized from the 112 + `name_len` accounting so a full group is ~22.9k records. Under-fill the shards and the large targets look free. Peak RSS, MiB, at `--mem=160G` on `node1604`:

| live shards | 1 MiB | 2 MiB | 4 MiB | 8 MiB |
|---:|---:|---:|---:|---:|
| 2 | 189.8 | 186.2 | 192.2 | 199.3 |
| 9 | 339.6 | 357.8 | 378.8 | 446.0 |
| 33 | 400.6 | 438.2 | 541.2 | 701.2 |
| 129 | 608.7 | 755.4 | 1100.0 | 1703.0 |
| 1024 | 2318.4 | 3494.1 | 6179.5 | **10691.8** |

At the default `ECRAWL_UID_SHARDS` of 1024, 8 MiB costs 10.4 GiB of resident memory against 2.3 GiB — 4.6x, and the naive `8 MiB × 1024 = 8 GiB` arithmetic turns out to *understate* it, because `writer_reserve` rounds the column arrays up to a power of two and the name buffer is charged on top. A crawler that needs 10 GiB on a multi-user filesystem is not a capture-size trade, it is a different tool.

**Above ~7.6 MiB the constant stops doing anything at all.** A record contributes `(CRAWL_COL__COUNT - 1) * 8 + name_len` = 112 + `name_len` decoded bytes to the flush accumulator, so `CRAWL_BIN_ROWGROUP_MAX_RECORDS` binds at 65536 × that: exactly 7.0 MiB for nameless records, and 7.67 MiB on `dirheavy` (mean name 10.73 B, 122.73 B per record) or 7.53 MiB on `mixed` (8.54 B, 120.54 B). Walking the group headers confirms the crossover empirically rather than arithmetically — how each group closed, by arm:

| tree | target | groups | closed by record cap | closed by byte target | closed at EOF |
|---|---:|---:|---:|---:|---:|
| dirheavy | 1 MiB | 469 | 0 | 468 | 1 |
| dirheavy | 2 MiB | 235 | 0 | 234 | 1 |
| dirheavy | 4 MiB | 118 | 0 | 117 | 1 |
| dirheavy | 8 MiB | 62 | **61** | 0 | 1 |
| mixed | 8 MiB | 83 | **82** | 0 | 1 |

So the 8 MiB arm is not an 8 MiB arm: every full group closes at 65,536 records, and it is really a 7.67 / 7.53 MiB arm whose behaviour the byte target no longer controls. The 255-byte-name RSS fixture is the other side of the same identity — 366.99 B per record puts its crossover at 22.94 MiB, and there not one group in any arm closed on the cap.

**And `make check` fails at 4 MiB and above.** The dir-index sidecar test crawls a 28,372-record fixture and asserts that it lands in more than one row group, because `rowgroups.idx` pruning is only exercised when there is a group to prune: with a single group the pruned chunk builder never runs and the parity checks around it compare one route with itself. Sweeping the harness as-is: 1 MiB passes with pruning keeping 1 of 4 groups, 2 MiB passes keeping 1 of 2, and 4 MiB and 8 MiB both fail with the fixture collapsed into a single group. That is a gate, not a nuisance — a target that silently deletes the coverage protecting the sketch would have to ship with a bigger fixture, which is more test runtime bought to make a change that is already losing on size-versus-speed.

`CRAWL_BIN_ROWGROUP_RAW_TARGET` stays at 1 MiB. 2 MiB is the only arm that clears the harness, and it buys 1.4% and 0.7% of capture size for 4.2% on `mixed --list`, 3.2% and 4.7% on the two partial-skip queries and 4.6–6.8% with skipping disabled, against an A/A floor near 1% — a small loss for a small win, which is still a loss under "smaller with no regression". What would change the answer is a change to what the row group *is*, not to how big it is: sub-group zone maps or a page index would let a coarse group keep fine-grained skipping, and splitting the parse unit from the compression unit would give back the concurrency the `BLOCK_SKIP=0` row measures. Until one of those exists, the three effects are locked together and 1 MiB is where they balance.

Raw data: `~/orcd/scratch/ereport-automated-testing/results/rgtarget2-sweep-19671435` (per-arm `space-*.txt` column breakdowns, per-rep query output, parity files), `rgtarget2-rss-19671436` (peak-RSS sweep) and `rgtarget2-checks-19672395` (`make check` per target), with the job logs under `logs/`. An earlier pass over the same question — same trees, same eight query shapes, no A/A control and no harness gate — is alongside them as `ercbin09-rgsweep-19662025` and `ercbin09-rgrss-19662026`; it reached the same verdict on the same mechanism, measuring the 8 MiB size win at −6.5% on `dirheavy` against −6.5% here, and the partial-skip regression at +52% against +56% here.

## Synthetic adversarial trees

`scripts/fixtures/generate-ecrawl-adversarial-tree.sh` builds stress layouts for `ecrawl` (flat megadir, optional depth chain, wide fan-out, optional `ecrawl_query` depth slices, optional ereport badge fixtures). Choose scale with `SYNTH_PROFILE` (unset = quick smoke, `medium`, `heavy`, `extreme`).

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
- `scripts/profile/ecrawl_query-fixtures.sh <bin-root> [results-dir]` — profiles the read-only `ecrawl_query` directory-shape stats over `<bin-root>/<fixture>/bin/` (produces no kept output). Knobs: `ECRAWL_QUERY_THREADS`, `ANALYZE_TOP`, `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`.

```bash
# 1) ecrawl (producer): profile every fixture, both modes, with perf — keeps bins under <bin-root>
DO_PERF=1 ./scripts/profile/ecrawl-fixtures.sh /tmp/ecrawl-adversarial /data1/ecrawl-bins

# 2) consumers: reuse the same <bin-root>; no recrawl
./scripts/profile/ereport-fixtures.sh        /data1/ecrawl-bins
./scripts/profile/ereport_index-fixtures.sh  /data1/ecrawl-bins
./scripts/profile/ecrawl_query-fixtures.sh /data1/ecrawl-bins
```

Each run prints the results dir and a `…tar.gz` to upload; full options are in each script's comment header. For `perf`, run as root or lower `kernel.perf_event_paranoid`.

To turn a recorded `perf.data` into a readable text report (the "stdio" report):

```bash
perf report -i perf.data --stdio > report.txt
perf report -i perf.data --stdio -g graph,0.5,caller > report.caller.txt   # caller-oriented call graph
perf report -i perf.data --stdio --no-children --sort comm,pid > report.bythread.txt
```

### Indexer comparison (Robinhood / GUFI / XDU)

To compare open-source file indexers against the ecrawl suite using the same paper methodology (build time and index size, queries Q1–Q6 cold and hot), use [`scripts/compare-indexers/`](../scripts/compare-indexers/README.md). It reuses the adversarial tree generator and reports a `SUMMARY_TABLE.txt`. For fixture-level `perf`/`strace` on ecrawl alone, keep using `scripts/profile/ecrawl-fixtures.sh` below.

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
