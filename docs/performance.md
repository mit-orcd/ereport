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
- Flat worker pool — Every non-directory name is `fstatat`'d inline on the crawl thread that read the directory; there is no cross-thread stat batching. The former stat pool was removed after profiling showed it rarely wins and complicates the threading model.
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
- Basename-only trigrams — Each path emits trigrams for its final component only (segment-once). Parent names are indexed when those directories appear as their own records; `--search` expands directory hits by a prefix scan of lex-sorted `paths.bin` instead of re-posting parents onto every child during `--make`.
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

### Measured: sorting each row group by (parent_dir_id, name)

The section above rejected the larger row group but measured the prize inside it. The whole 8 MiB size win was one column — `parent_dir_id` 3.94 → 2.37 MiB on `dirheavy` — and it came from longer runs, not from a longer compression window. Sorting a group by `parent_dir_id` before encoding it produces those runs directly, at 1 MiB, without the 10.7 GiB of writer RSS and the 56% partial-skip regression that made the 8 MiB arm unshippable. It is kept: **−17.1% on `mixed`, −0.83% on `dirheavy`, −9.5% on a long-name tree**, with query medians flat or faster on every shape, write throughput inside the A/A spread, and peak RSS flat at 1024 live shards. The win is a function of records per directory, and there is one narrow fan-out where it is a 1.42% loss; the curve is at the end of this section.

**The audit came first, and the reason nothing depends on record order is structural.** Sorting happens at flush, so it permutes rows inside a buffer that is about to be encoded and never moves a record between groups: the flush threshold counts decoded bytes (112 + `name_len` per record), which do not depend on order. Group membership, `record_count`, `type_mask` and every per-column `min`/`max` are therefore identical to what an unsorted writer produces, and so is every group boundary and file offset. That disposes of the `.ckpt` sidecars, which record block-aligned *physical* offsets into the record region and are only used to split a shard into segments; of `rowgroups.idx` and `dirs.idx`, which are built at index time from what a group actually holds and from catalog paths respectively; and of the catalog's `dir_id` assignment, DFS numbering and subtree rollups, which are driven by directory arrival in a post-pass on final close and never read a row group. `ecrawl_mount` already sorts its directory listings by `(parent, name)` in memory, and `ecrawl_query --list` output was already compared sorted because crawl threads interleave, so record order was never deterministic to begin with. Mid-crawl resume is covered by the same argument rather than by a test — the harness exercises `.ckpt` loading but has no restart case, so that leg is by construction, not by measurement.

**The ordering is described, not promised.** [binary-format.md](binary-format.md) states the writer's order — `parent_dir_id`, then name bytes by `memcmp` over the common prefix with the shorter name first, then arrival position — and states equally plainly that a reader must not rely on it. Sorting is per group, so one directory's children can still straddle a boundary and arrive in either order, which makes any reader that assumed sibling adjacency wrong anyway; and a later writer that wanted a different order should not have to bump the format version to get it. The tie-break exists so the order is reproducible, not so it can be depended on.

Setup: `node1610`, 32 CPUs requested, `--mem=96G`, `mit_normal`, fixture trees and `TMPDIR` on node-local `/scratch`, `ECRAWL_QUERY_THREADS=16`, medians of 9 reps with the three arms interleaved inside each rep, on the same `dirheavy` and `mixed` trees Phase 1 and 2 used. The third arm is **a second, independently built unsorted binary as an A/A control**. All three arms built with zero warnings, and all 48 sorted-output comparisons — 12 query shapes × 2 trees × 2 comparisons — matched byte for byte, including the 2,000,012-line and 4,800,012-line whole-tree `--list` shapes.

Total capture size, on-disk `bin` + `ckpt`:

| tree | records | unsorted | sorted | A/A |
|---|---:|---:|---:|---:|
| dirheavy | 4,000,006 | 31.65 MiB | **31.39 MiB (−0.83%)** | 31.66 MiB (+0.03%) |
| mixed | 5,400,006 | 31.06 MiB | **25.74 MiB (−17.1%)** | 31.04 MiB (−0.07%) |

The two trees disagree about *which* column pays, and the reason is a crossover the encoder is already choosing across correctly. `CRAWL_ENC_RLE` costs 16 bytes per run — a `(value, run_length)` pair of `uint64` — while `CRAWL_ENC_FOR_BITPACK` costs `bit_width` bits per record, about 2.4 bytes here. So RLE wins on `parent_dir_id` once the mean run exceeds ~6.5 records, which after sorting is just the mean number of records per directory. `mixed` has 9, `dirheavy` has 2:

| tree | column | unsorted | sorted | encoding, unsorted → sorted |
|---|---|---:|---:|---|
| dirheavy | `inode` | 14.95 MiB | 14.95 MiB | `FOR_BITPACK` → `FOR_BITPACK` |
| dirheavy | `name_bytes` | 4.55 MiB | **4.03 MiB (−11.4%)** | `BYTES` → `BYTES` |
| dirheavy | `parent_dir_id` | 3.92 MiB | **4.15 MiB (+5.9%)** | `FOR_BITPACK` → `FOR_BITPACK` |
| mixed | `inode` | 19.88 MiB | 19.86 MiB | `FOR_BITPACK` → `FOR_BITPACK` |
| mixed | `parent_dir_id` | 7.87 MiB | **724.28 KiB (−91.0%)** | `FOR_BITPACK` → `RLE` (605 of 621 groups) |
| mixed | `name_bytes` | 992.63 KiB | **2.91 MiB (+200%)** | `BYTES` → `BYTES` |

`mixed`'s `parent_dir_id` is the Phase 2 prize collected in full and then some: 7.87 MiB to 724 KiB, where the 8 MiB row group only managed 40% on the other tree. `dirheavy` stays on `FOR_BITPACK` because two records per directory would cost 32 bytes per run against 2.4 — the encoder correctly declines — and then loses 5.9% at the zstd stage, because arrival order repeats each bucket's parent id many times over while the sorted stream is monotone and nearly distinct, so sorting removes literal matches zstd was living on without giving the run coder enough to replace them. A delta encoder would collect that back; `CRAWL_ENC_DELTA` is what the sorted stream is now shaped for and does not exist yet.

**The cheap columns collapse, which is where the query win comes from.** On `dirheavy` six columns flip from `FOR_BITPACK` to `RLE` — `mode` raw 6.68 MiB → 14.72 KiB, `size` 2.41 MiB → 14.75 KiB, and the same for `atime`, `name_len`, `nlink`, `type` — dropping the record region's decoded size from 82.16 MiB to 67.16 MiB for only 0.26 MiB of compressed savings. Those columns were already near-free on disk (450x, 257x ratios) but were still being bit-unpacked four million values at a time on every scan. Sorted, they expand from a handful of runs.

**Zone maps do not tighten, and the reason is the same structural argument as the audit.** The hypothesis going in was that sorted groups would cover narrow, near-disjoint `parent_dir_id` intervals and skip far more. They cannot, because intra-group sorting does not change *which* records are in a group and a zone map is a property of that set. Measured rather than assumed: on all twelve shapes, in both trees, `blocks_decompressed`, `blocks_skipped`, `records_scanned` and `directories_examined` are identical across all three arms — 469 groups on `dirheavy` and 621 on `mixed`, `records_scanned` 4,000,006 and 5,400,006, 468/469 and 620/621 skipped on the shapes that skip, 2,000,008 and 600,008 directories examined on the rollups. Only `parse_chunk_jobs` moves, 46 → 47 on `dirheavy`, because the sorted capture is smaller. Narrower intervals need records *assigned* to groups by key, not sorted inside them, and that means buffering across group boundaries — the memory cost the 8 MiB arm was rejected for.

One thing the sweep set out to do and failed at: the `sel*` shapes put the size predicate at several points of the tree's own distribution trying to land a skip rate strictly between the 99.8% Phase 2 was stuck at and 0%, and none of them did. Every shape came out at 0%, 99.8% or 100%. The invariance above is the stronger statement — the counters are identical on every shape rather than close on one — but a genuinely intermediate skip rate remains unmeasured.

Query medians, seconds, `ECRAWL_QUERY_THREADS=16`, `MID` = mean bytes per file:

| tree | shape | skip | unsorted | sorted | A/A |
|---|---|---:|---:|---:|---:|
| dirheavy | `--subtree` rollup | — | 0.0717 | 0.0713 | 0.0714 |
| dirheavy | `--top,dense,deep 64` | — | 0.2394 | **0.1857 (−22.4%)** | 0.2396 |
| dirheavy | `--subtree --type f --size-gt 100 --list` | 99.8% | 0.0805 | 0.0805 | 0.0805 |
| dirheavy | `--index-dir` subtree | — | 0.0042 | 0.0042 | 0.0043 |
| dirheavy | `--size-gt 500M --type f --list` | 100% | 0.0059 | 0.0062 | 0.0060 |
| dirheavy | `--size-gt MID --type f` | 99.8% | 0.0064 | 0.0064 | 0.0065 |
| dirheavy | `--size-gt 0 --type f` | 0% | 0.0219 | **0.0178 (−18.7%)** | 0.0211 |
| dirheavy | `--size-gt 4095` | 99.8% | 0.0066 | 0.0066 | 0.0065 |
| dirheavy | narrow `--subtree --type f --list` | 0% | 0.0994 | 0.0961 | 0.0990 |
| dirheavy | mid `--subtree --type f --list` | 0% | 0.1016 | 0.0977 | 0.1013 |
| dirheavy | whole-tree `--subtree --type f --list` | 0% | 0.3033 | 0.3115 | 0.2949 |
| dirheavy | `--size-gt MID`, `ECRAWL_QUERY_BLOCK_SKIP=0` | off | 0.0230 | **0.0197 (−14.3%)** | 0.0218 |
| mixed | `--subtree` rollup | — | 0.0281 | 0.0279 | 0.0268 |
| mixed | `--top,dense,deep 64` | — | 0.0860 | **0.0671 (−22.0%)** | 0.0865 |
| mixed | `--subtree --type f --size-gt 100 --list` | 0% | 0.0506 | 0.0501 | 0.0509 |
| mixed | `--index-dir` subtree | — | 0.0044 | 0.0043 | 0.0042 |
| mixed | `--size-gt 500M --type f --list` | 100% | 0.0064 | 0.0062 | 0.0062 |
| mixed | `--size-gt MID --type f` | 0% | 0.0220 | **0.0188 (−14.5%)** | 0.0229 |
| mixed | `--size-gt 0 --type f` | 0% | 0.0262 | **0.0189 (−27.9%)** | 0.0245 |
| mixed | `--size-gt 4095` | 99.8% | 0.0067 | 0.0066 | 0.0066 |
| mixed | narrow `--subtree --type f --list` | 0% | 0.0507 | 0.0501 | 0.0514 |
| mixed | mid `--subtree --type f --list` | 0% | 0.0510 | 0.0506 | 0.0509 |
| mixed | whole-tree `--subtree --type f --list` | 0% | 0.5278 | 0.5382 | 0.5195 |
| mixed | `--size-gt MID`, `ECRAWL_QUERY_BLOCK_SKIP=0` | off | 0.0231 | **0.0197 (−14.7%)** | 0.0249 |

Two identical builds differ by up to 7.8% here — the sub-30 ms scans, where a tenth of a millisecond is already half a percent — so read anything under that as flat. The bolded rows are the decode-bound scans, and they move with the RLE collapse above rather than with anything about skipping: the shape that skips 99.8% of its groups is unchanged in every arm, while the shape that decodes all 621 is 28% faster. The rollup and the `--index-dir` subtree never touch the record region and correctly do not move. The whole-tree `--list` shapes are the one place sorted is nominally slowest (+2.7% and +2.0%), both inside an A/A spread of 2.8% and 1.6% on arms whose per-rep ranges overlap; they are also output-bound, printing two and 4.8 million lines. No shape regresses beyond the control.

Write throughput is where the risk was, and it does not move. `dirheavy` crawl medians are 4.4471 s unsorted, 4.5085 s sorted, 4.4907 s control — sorted is 1.4% above unsorted and 0.4% above a control built from the same source. `mixed` is 2.2163 / 2.1297 / 2.1088, with sorted *faster* than unsorted. The nine reps of the unsorted `dirheavy` arm alone span 4.3113–5.9175 s (4.31–4.74 discarding one outlier), which covers every cell in the row. The sort is a two-pass LSD radix over the frame-of-reference `parent_dir_id` — usually two byte passes, since a group spans a narrow slice of the shard's dir ids — plus insertion or bottom-up merge on names within each equal-parent run, and at about 8,500 records per group it disappears into the zstd pass that follows it.

Peak RSS is flat, which took one design decision. The sort needs 24 bytes of index per buffered record plus a gather copy of the group's name blob, and the obvious place to put that is the writer — but a writer is per shard, so at the default `ECRAWL_UID_SHARDS` of 1024 it would be a quarter of a gigabyte at the ~8,500 records a group holds on these trees and 1.6 GiB at the 65536-record cap, all of it live for the microseconds a flush takes. It is thread-local instead — a writer thread flushes one shard at a time — so the high-water mark is charged once per thread rather than once per shard. Peak RSS on the single-uid trees is 734.8 / 733.5 / 732.9 MiB on `dirheavy` and 360.6 / 359.0 / 358.6 MiB on `mixed`, and on the many-uid fixture the 8 MiB row group died on (the `LD_PRELOAD` `st_uid` shim, 1024 owner directories of 4096 files with 200-byte names, 4,195,329 entries):

| live shards | unsorted | sorted | A/A |
|---:|---:|---:|---:|
| 2 | 106.8 MiB | 108.7 | 114.0 |
| 9 | 271.6 | 275.2 | 272.1 |
| 33 | 323.4 | 320.6 | 319.7 |
| 129 | 522.5 | 508.3 | 515.3 |
| 1024 | 2235.0 | 2229.4 | 2222.3 |

At 1024 shards the sorted arm is 5.6 MiB below unsorted and 7.1 MiB above the control — inside the 12.7 MiB the two unsorted builds put between themselves.

`make check` passes on both arms, 0 FAIL lines, with the dir-index fixture still landing in 4 row groups and pruning still keeping 1 of them, so the coverage Phase 2 found so easy to delete is intact. One unit test needed updating: `test_crawl_block_filter.c`'s round-trip asserted append order on records that all share `parent_dir_id`, which the sort legitimately invalidates, so its expectations were reordered to the byte-wise name order and a `check_group_order()` case was added with three distinct parents and shared-prefix names — `a`, `ab`, `b` under one, `a`, `aB`, `ab` under another — asserting the exact permutation, the multiset, and that the zone map still reports the true extremes. No harness script was touched.

**`name_bytes` moves in both directions, which is the number that calibrates Phase 5.** Adjacency helps where names are long and share prefixes inside a directory and hurts where the arrival stream was already periodic. A third tree isolates the good case — 256 directories of 4096 files with 200-byte names, 1,048,833 records — and there sorted `name_bytes` is 2.01 MiB against 2.54 MiB unsorted and 2.96 MiB for the control, a −9.5% capture on the same comparison. Note what that control says: the *unsorted* arm's `name_bytes` varies by 16% between two identical builds, because crawl arrival order is nondeterministic and zstd's result depends on it, while the sorted arm's does not. Sorting buys reproducible capture size on top of smaller capture size. It also takes `name_bytes` on that tree to 99.35x, so zstd is already capturing nearly all the prefix redundancy that front-coding would target, and Phase 5's remaining headroom on this shape is small.

**The `mixed` counter-case is a fan-out effect, and it crosses over at about 12 records per directory.** Holding the entry count near 1.2M and the `mixed` layout fixed — `bucket_NNNNN` directories of `w0000000`… files, the same eight-name alphabet in every directory — and sweeping only the fan-out:

| files/dir | dirs | `name_bytes` unsorted → sorted | `parent_dir_id` unsorted → sorted | total |
|---:|---:|---|---|---:|
| 2 | 600,000 | 1.85 → 2.44 MiB (+32%) | 2.99 → 2.34 MiB | −0.19% |
| 4 | 300,000 | 962 → 1516 KiB (+58%) | 2.43 → 2.03 MiB | **+1.42%** |
| 8 | 150,000 | 520 → 1044 KiB (+100%) | 1.69 MiB → 239 KiB | −11.9% |
| 16 | 75,000 | 295 → 203 KiB (−31%) | 468 → 86 KiB | −7.8% |
| 32 | 37,500 | 168 → 110 KiB (−34%) | 57.1 → 47.8 KiB | −1.4% |

The A/A control tracks the unsorted arm to within 0.5% at every point, so the reversal is real. It also confirms the 16-bytes-per-run arithmetic above from the other side: `parent_dir_id` stays on `FOR_BITPACK` at fan-out 4 and flips to `RLE` in 140 of 156 groups at fan-out 8, bracketing the predicted ~6.5. Reading the `name_bytes` reversal: the directory records' own names are the thing sorting concentrates. They share one parent, so they collect into a single run of distinct incrementing strings, where unsorted each one rode along inside a long repeating `bucket_NNNNN w0000000 w0000001` template that zstd matched almost entirely. Directory names are 45% of the name bytes at fan-out 2 and 4.6% at 32, which is the same order as where the curve turns. That is a reading of the curve, not a measurement of the two sub-streams separately.

Two things follow. `mixed` at fan-out 8 sits exactly on the worst point for `name_bytes` and still wins 17.1% overall, because `parent_dir_id`'s RLE crossover lands at almost the same fan-out and is worth several times more. And there is one shape — fan-out 4, where `parent_dir_id` has not yet flipped to RLE but `name_bytes` has already lost 58% — **where sorting makes the capture 1.42% bigger**. It is a narrow window on a synthetic tree whose names repeat exactly across directories, and it costs nothing but size, but "smaller" is not universal and the curve above is where it is not.

The peak win is at fan-out 8 (−11.9% here, −17.1% on `mixed` itself); by 32 it narrows to −1.4%, because `parent_dir_id` is already RLE without sorting by then and there is nothing left to collect.

**Recommendation: keep the sort.** −17.1% on the tree shape where most captures live, −0.83% where it barely pays, +1.42% in one narrow synthetic window, and every gate holds — identical query answers on 48 comparisons, `make check` green with the dir-index fixture still spanning 4 groups and pruning still keeping 1, query medians flat or up to 22% faster, write throughput and peak RSS inside their own controls. Both per-column regressions are zstd losing literal matches to a more ordered stream, and both are what the later phases are for: `CRAWL_ENC_DELTA` for the now-monotone `parent_dir_id`, front-coding for `name_bytes`. Neither is worth pre-empting with a store-if-smaller guard at the group level, which would double the encode cost to protect columns worth 4 MiB and 2 MiB of a 26 MiB capture.

Raw data: `~/orcd/scratch/ereport-automated-testing/results/recsort-sweep-19673578` (per-arm `space-*.txt`, per-rep query output with counters, parity files), `recsort-rss-19675022` (peak-RSS sweep under the uid shim), `recsort-check-19675077` (`make check` both arms), `recsort-names-19676435` (the long-name tree) and `recsort-fanout-19678085` (the fan-out crossover), with job logs under `logs/`.

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
