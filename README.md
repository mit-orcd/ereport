# ereport

Small C tools for crawling filesystem metadata, writing compact binary crawl data, and turning that data into static HTML reports.

The current toolchain is:

- `ecrawl`: parallel filesystem crawler that writes compact binary metadata records.
- `ecrawl_repair`: rebuilds `*.bin.ckpt` sidecars, truncates incomplete shard tails, and quarantines unrepairable shards.
- `ecrawl_analyze`: read-only scan that prints directory-shape statistics (parent, path-depth, and top-parent histograms).
- `edelete`: parallel walker that deletes non-directory paths — everything under a path, or only entries older than an `atime`/`mtime`/`ctime` threshold (dry-run by default; does not follow symlinks).
- `ereport`: reads crawl output and writes `index.html`, bucket drill-down pages, and a path-search box.
- `ereport_index`: builds the trigram index used for path-substring search.
- `eserve.py`: HTTP server for the static reports plus server-side path search.

How to read this file: Each tool has a detailed section under [What The Tools Do](#what-the-tools-do) (usage, examples, env tables). [Default thread counts](#default-thread-counts-per-binary) summarizes parallelism defaults. [Typical Workflow](#typical-workflow) is the shortest path from crawl → HTML → search index → HTTP. [Environment variables (quick reference)](#environment-variables-quick-reference) lists every tuning knob in one place.

### Contents

- [Build](#build) · [Testing](#testing) · [Synthetic adversarial trees](#synthetic-adversarial-trees) · [Profiling and performance work](#profiling-and-performance-work) · [systemd](#systemd-daily-ecrawl-and-binary-sync) · [Why this is fast](#why-this-is-fast) · [Crawl shard binary format](#crawl-shard-binary-format) · [What The Tools Do](#what-the-tools-do) · [Sample HTML fixtures](#sample-html-fixtures-and-screenshots) · [Typical Workflow](#typical-workflow) · [Validation / tests](#validation-helpers) · [Output semantics](#output-semantics) · [Environment variables](#environment-variables-quick-reference) · [Source layout](#source-layout) · [License](#license)

## Default thread counts (per binary)

Each program has its own defaults. There is no single global “thread count” for the whole toolchain: `ecrawl`, `ereport`, and `ereport_index` read different environment variables and use different built-in numbers when those variables are unset.

The Min logical CPUs and Min RAM columns below are practical floors for running with default thread counts on a mostly idle machine: enough CPUs that the default parallelism isn't badly oversubscribed, and enough RAM for modest workloads. They are not guarantees for huge trees — reduce the thread-count env vars on smaller hosts, and expect large `ereport_index --make` merges to need far more (tens to hundreds of GiB peak; the merge budget tracks host/cgroup `MemAvailable`).

| Program | Parallelism role | Override (env) | Built-in default | Min logical CPUs | Min RAM |
|---------|------------------|----------------|------------------|------------------|---------|
| `ecrawl` | Walk / queue directory work | `ECRAWL_CRAWL_THREADS` | 16 crawl threads (minimum 1; no fixed maximum) | 4 | 4 GiB |
| `ecrawl` | Parallel `fstatat` on batched non-directory names (per-directory `readdir` stays on crawl workers) | `ECRAWL_STAT_THREADS` | 8 stat threads (`0` disables pool; legacy inline stat) | 4 | 4 GiB |
| `ecrawl` | Flush uid-sharded `.bin` output | `ECRAWL_WRITER_THREADS` | 8 writer threads | 4 | 4 GiB |
| `ecrawl_repair` | Parallel rescans; optional `truncate` on incomplete tail; checkpoint rebuild / verify | `ECRAWL_REPAIR_THREADS` | 16 | 4 | 4 GiB |
| `ecrawl_analyze` | Parallel shard scan for stats only (no writes) | `ECRAWL_ANALYZE_THREADS` (falls back to `ECRAWL_REPAIR_THREADS`) | 16 (maximum 4096) | 4 | 4 GiB |
| `edelete` | Parallel directory walk; optional `unlink` (bounded concurrency in `--delete`) | `EDELETE_THREADS`, `EDELETE_MAX_UNLINK_INFLIGHT` | 16 threads; 256 max concurrent `unlink` (`0` = unlimited) | 4 | 4 GiB |
| `ereport` | Map/parse `.bin` chunks, emit up to 36 `bucket_*.html` files, live stderr stats | `EREPORT_THREADS` | 32 | 8 | 8 GiB |
| `ereport_index` | `--make`: parallel chunk-boundary scan, parse workers; trigram temp writers default to the same count unless `EREPORT_INDEX_TRIGRAM_THREADS` is set. `--search`: parallel postings load and path filtering when the query and candidate set are large enough | `EREPORT_INDEX_THREADS` (and optionally `EREPORT_INDEX_TRIGRAM_THREADS`) | 32 | 16 | 16 GiB |

Not controlled by those knobs: `ereport_index --make` merge workers (cap 16, chosen from RAM budget), and `--resume-merge` merge workers—see `EREPORT_INDEX_MERGE_` vars in the table below.

Details and ranges for each variable appear under each tool’s section and in Environment variables (quick reference) at the end of this file.

## Build

```bash
make
```

Build individual binaries:

```bash
make ecrawl
make ereport
make ereport_index
make ecrawl_repair
make ecrawl_analyze
make edelete
```

Clean:

```bash
make clean
```

### Optional: link native binaries against jemalloc

The Makefile auto-detects jemalloc via `pkg-config` and, when `jemalloc-devel` / `libjemalloc-dev` is installed, links all native targets built by `make all` — `ereport`, `ereport_index`, `ecrawl`, `edelete`, `ecrawl_repair`, `ecrawl_analyze`, and `enfsprobe` / `enfsprobe-dist` — against `-ljemalloc`. `enfsprobe-static` is unchanged (fully static link + jemalloc is fragile). Install the dev package so `pkg-config` can find it:

```bash
sudo dnf install jemalloc-devel    # RHEL / Fedora / EL8+ (EPEL)
sudo apt install libjemalloc-dev   # Debian / Ubuntu
make clean && make
ldd ./ereport_index | grep jemalloc   # verify libjemalloc.so.2 is linked
```

A single `build: jemalloc …` line prints once when you run `make` / `make all`. No source `#include` is required — linking `-ljemalloc` transparently interposes `malloc` / `calloc` / `realloc` / `free`. Without the dev package the link line is byte-for-byte identical to a vanilla build, so the change is a no-op on hosts that lack jemalloc. Override the auto-detect by passing `JEMALLOC_LIBS=` (empty) on the make command line to force-disable, or `JEMALLOC_LIBS=-ljemalloc` to force-enable.

On a 14.9M-path crawl with `EREPORT_INDEX_THREADS=64`, linking against jemalloc made `ereport_index --make` ~27% faster end-to-end (index phase ~31% faster, merge phase unchanged); `ecrawl` showed no measurable benefit on adversarial trees. When enabled, the deployment host needs `libjemalloc.so.2` at runtime (the RHEL `jemalloc` package suffices; the dev package is only needed at build time).

## systemd: daily `ecrawl` and binary sync

Optional units under `contrib/systemd/` run `ecrawl` on paths listed in `/etc/ereport/ecrawl-daily.conf`, then `rsync` each job’s `output_dir` (crawl shard data) under `RSYNC_DEST` (typically `RSYNC_DEST/<basename(output_dir)>/`, or directly into `RSYNC_DEST` when its last path component already matches that basename); after each successful sync the script deletes matching crawl artifact files locally (see `contrib/systemd/ecrawl-daily.conf.example`).

Install (adjust paths if you install elsewhere):

```bash
sudo install -d /etc/ereport /usr/local/lib/ereport
sudo install -m0644 contrib/systemd/ecrawl-daily.conf.example /etc/ereport/ecrawl-daily.conf
# edit /etc/ereport/ecrawl-daily.conf
sudo install -m0755 contrib/systemd/ecrawl-daily.sh /usr/local/lib/ereport/ecrawl-daily.sh
sudo install -m0644 contrib/systemd/ecrawl-daily.service /etc/systemd/system/
sudo install -m0644 contrib/systemd/ecrawl-daily.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ecrawl-daily.timer
```

Set `User=` / `Group=` in `ecrawl-daily.service` if the job must not run as root. The same block appears as comments at the top of `contrib/systemd/ecrawl-daily.service`.

## Why this is fast (design concepts)

The tools are fast because they combine compact binary I/O, parallelism along natural boundaries, and bounded pipelines instead of naïvely scanning everything twice or holding giant locks over shared mutable state.

### Shared ideas (`ecrawl`, `edelete`, `ereport`, `ereport_index`)

- Path arguments — Directory and crawl-root arguments are normalized to canonical absolute paths with `realpath(3)` once the path exists (see `path_canon.h`). Relative inputs are supported; symlink components are resolved. Output directories that are created on demand are canonicalized after `mkdir` where applicable.
- Binary crawl records — Paths and metadata use a fixed header plus a record stream and (when finalized) a catalog tail (file magic `ERCBIN05`, format version 5). Layout and rejection rules are spelled out under [Crawl shard binary format](#crawl-shard-binary-format) below.
- Checkpoint sidecars (`*.bin.ckpt`) — While crawling, `ecrawl` records record-aligned byte offsets at a fixed stride into `uid_shard_*.bin.ckpt`. `ereport` and `ereport_index` load those offsets to split each shard into valid segments without a preliminary full-file scan to find boundaries. That enables many threads to work on different byte ranges of the same file safely (no record torn across workers). Checkpoint offsets apply only to the record region (from just after the file header up to `catalog_offset` on finalized shards—see below). If sidecars are missing or stale (for example an interrupted crawl), run `ecrawl_repair` on the crawl output directory to rebuild them—and to truncate an incomplete last record when possible—see `ecrawl_repair` below.
- Embarrassingly parallel units — Work is split by shard file, chunk, age×size bucket, or trigram bucket so threads rarely contend on the same byte or the same mutex for long.

### Crawl shard binary format

Each `uid_shard_*.bin` file uses this layout.

What changed (high level): Current shards are format version 5 (`ERCBIN05`), replacing older `ERCBIN03` / version 3 files. Version 5 adds a `catalog_offset` field to the fixed header and appends a directory catalog after the record stream. Each catalog row carries immediate-child byte and time aggregates for that directory within the shard (child subtree totals are not rolled into parents on disk). `ecrawl` writes `catalog_offset == 0` until the shard is finalized; `ereport`, `ereport_index --make`, and `ecrawl_analyze` paths that load the catalog require a nonzero, in-range `catalog_offset` and a parseable catalog blob.

File header (32 bytes, packed): `magic[8]` (`ERCBIN05`), `version` (`uint32_t`, must match 5), `reserved` `uint32_t`, `catalog_offset` `uint64_t` (byte offset from BOF to the catalog blob), `reserved64` `uint64_t`.

Record region: `[sizeof(header), catalog_offset)` on finalized shards — a concatenation of variable-length records. Each record starts with `bin_record_hdr_t` (`parent_dir_id`, `name_len`, `type`, `mode`, `uid`, `gid`, `size`, `inode`, `dev_major`, `dev_minor`, `nlink`, `atime`, `mtime`, `ctime`) followed by `name_len` bytes of UTF-8 for a single path component (see `crawl_bin_format.h`).

Catalog tail: `[catalog_offset, EOF)` — begins with `uint64_t n_entries`, then `n_entries` packed `bin_dir_catalog_entry_t` rows (`dir_id`, `parent_dir_id`, `depth`, `name_len`, reserved padding, then `imm_child_bytes`, `imm_child_count`, `imm_child_ctime_led_count`, `imm_child_min_eff_time`, `imm_child_max_eff_time`). `imm_child_*` fields sum only records whose on-disk `parent_dir_id` equals this row’s `dir_id` (scoped to the shard). `imm_child_bytes` follows `ecrawl` accounting (regular files: hardlink-aware credit; other types: apparent `st_size`). `imm_child_min_eff_time` / `imm_child_max_eff_time` use `max(atime, mtime, ctime)` per child; `imm_child_min_eff_time` is `UINT64_MAX` when `imm_child_count == 0`. `imm_child_ctime_led_count` uses the same “ctime-led” rule as `ereport` (`ctime` strictly greater than `max(atime, mtime)` and at least 180 days newer). Each row ends with `name_len` UTF-8 bytes (directory component only; root uses `name_len == 0`).

`catalog_offset`: On a completed crawl `ecrawl` sets this to the first byte of the catalog (≥ header size, ≤ file size). `catalog_offset == 0` means the shard was never finalized (still writing or interrupted).

Incomplete / invalid shards: `ereport` and `ereport_index` reject a shard when `catalog_offset == 0`, `catalog_offset` is out of range, magic/version mismatch, or `crawl_bin_catalog_load()` fails (truncated catalog, bogus counts, etc.). Chunk mapping in `crawl_bin_chunks` caps the record region at `catalog_offset` when it is nonzero; when it is zero, loaders may still align checkpoints against EOF for diagnostics, but report/index consumers treat the shard as unusable until `ecrawl_repair` (or a fresh crawl) fixes it. `ecrawl_repair` behavior for bad tails and `corrupt_shards/` quarantine is unchanged in spirit—see `ecrawl_repair` above.

### `edelete`: parallel deletion (optional age filter)

- Same crawl parallelism pattern as `ecrawl` — task queue, worker threads, local directory stacks, and work donation so wide trees stay busy across threads.
- Does not follow symlinks — traversal uses `lstat` / `fstatat(..., AT_SYMLINK_NOFOLLOW)`; symlink inodes can be `unlink`’d if eligible, but targets are not walked through links.
- Deletes non-directory paths only — regular files, symlinks, FIFOs, sockets, etc.; directories are opened and enumerated, then removed with `rmdir` only after `--delete` when they become empty (deepest first, without ascending above the start path or removing `/`).
- Default is dry-run — counts `would_delete` and prints `mode=dry-run`; `--delete` prints a summary of resolved path, filter, thread settings, and `verbose`, then requires typing `YES` on stdin before any `unlink` and empty-dir cleanup—unless `--force` is also passed (`--delete --force` skips the prompt for scripting). There is no separate `--dry-run` flag: omit `--delete` to preview.
- Live status line — rolling `entries/s(10s)` and totals on stdout (similar spirit to `ecrawl`); `--verbose` is currently parsed but does not change output.

Usage:

```bash
./edelete [--delete] [--force] [--verbose] [--uid <uid>] [--gid <gid>] <path>
./edelete [--delete] [--force] [--verbose] [--uid <uid>] [--gid <gid>] <atime|mtime|ctime> <days> <path>
```

Optional `--uid` and/or `--gid` restrict eligibility to entries whose `st_uid` / `st_gid` match; when both are set, both must match.

The start path is resolved to an absolute path (`realpath(3)`). With one argument, every non-directory under `path` is eligible (still dry-run unless `--delete`). With three arguments, `days` selects entries whose chosen timestamp is at least `days × 86400` seconds older than wall-clock now.

Environment variables:

| Variable | Meaning |
|----------|---------|
| `EDELETE_THREADS` | Parallel crawl workers (default 16, minimum 1). |
| `EDELETE_MAX_UNLINK_INFLIGHT` | In `--delete` mode, max concurrent `unlink(2)` calls across all threads (default 256; `0` = unlimited). |

Examples:

```bash
./edelete /tmp/staging_area
./edelete --delete /tmp/empty_me
./edelete mtime 90 /storage/scratch/job123
EDELETE_THREADS=32 ./edelete --delete ctime 14 /mnt/cache/tmp
EDELETE_MAX_UNLINK_INFLIGHT=128 ./edelete --delete atime 30 /big/tree
```

Performance tuning — unlink contention on quota'd XFS:

- On an XFS filesystem with disk quotas enabled, every `unlink(2)` must reserve and later apply quota for the file owner's dquot, all under a single per-dquot kernel mutex (`xfs_trans_dqresv` / `xfs_trans_apply_dquot_deltas`, plus `xfs_qm_dqattach` on a cold cache). When many threads delete files belonging to the **same uid/gid**, they serialize on that one mutex. In profiles this shows up as the kernel burning ~70%+ of CPU in `osq_lock` / `mutex_spin_on_owner` — i.e. spinning, not unlinking. More unlink threads then make it *slower*, not faster.
- The lever is `EDELETE_MAX_UNLINK_INFLIGHT`: lowering it caps how many threads pile onto the dquot mutex. On a quota'd XFS deleting one owner's tree, a small cap (2–4) typically matches or beats the default with a fraction of the CPU. Keep `EDELETE_THREADS` high if you like — traversal and `fstatat` parallelize fine; it is specifically concurrent `unlink` on the same dquot that serializes.
- Concurrency *does* help when deletions span many distinct owners (different dquots = different mutexes), or on filesystems without quotas (or non-XFS), where this contention is absent.
- Quick sweep to find the knee on your filesystem (compare `deleted_files` ÷ `elapsed_sec`):

```bash
for n in 1 2 4 8 16; do
  EDELETE_THREADS="$n" EDELETE_MAX_UNLINK_INFLIGHT="$n" \
    ./edelete --delete --force <path> 2>/dev/null \
    | awk -F= '/^deleted_files=/{d=$2} /^elapsed_sec=/{e=$2} END{printf "inflight=%s deleted=%s sec=%s rate=%.0f/s\n", "'"$n"'", d, e, e>0?d/e:0}'
done
```

edelete does **not** auto-detect this contention or adjust concurrency on its own (see the note below on why) — it is a deliberate manual knob.

Final stdout summary includes `delete_all` (`1` when the one-argument form was used), `basis` / `age_days` when the age filter is used, `force` (`1` if `--force` was passed), `mode`, scan counts, `deleted_files`, `removed_empty_dirs`, `would_delete`, `errors`, throughput metrics, and donation counters.

### `ecrawl`: crawling and capture

- Parallel crawl threads feed a task queue; multiple threads traverse the tree concurrently while respecting directory boundaries.
- Parallel stat pool (when `ECRAWL_STAT_THREADS` > 0, default 8) — Each crawl worker does one `readdir` per open directory. Entry classification and batching are described under [Directory scanning and stat workers](#directory-scanning-and-stat-workers) in the `ecrawl` tool section (trusted `d_type` batching, inline prefix `ECRAWL_STAT_BATCH_AFTER_RELIABLE_NONDIRS`, `DT_DIR`/`DT_UNKNOWN` never batched, unexpected-dir `WARN` on stderr). Set `ECRAWL_STAT_THREADS=0` for legacy inline-only `fstatat` on crawl threads (often faster on very low-latency metadata). `--verbose` adds `stat_batches_*`, `stat_batch_unexpected_dir_total`, `stat_queue_depth_max`, `wait_stat_pop`, `wait_stat_enqueue`.
- Uid-sharded output — Records hash to many `uid_shard_*.bin` files so writes spread across descriptors and writer threads; you avoid one giant append-only file and reduce lock contention on a single sink.
- Separate writer threads — Crawl threads batch work to bounded writer queues; dedicated writers flush shards with large buffered I/O instead of every thread hitting the filesystem independently for every record. `ECRAWL_WRITER_QUEUE_BATCHES` caps pending record batches per writer (default 64, range 4…4096).
- Checkpoint rows during write — Sidecars capture sparse offsets so later tools can parallel-read without rescanning from zero.

### `ereport`: reports from crawl bins

- Parallel chunk mapping — Uses `*.ckpt` to build chunk lists (byte ranges that align with record starts). Chunk count scales with file size, so `EREPORT_THREADS` has enough units of work.
- Parallel chunk parsing — Workers consume disjoint chunks; summaries and bucket histograms merge after workers finish (merge step is not on the per-record hot path across all threads).
- Parallel bucket HTML — The 36 heat-map cells map to 36 independent output files; emission fans out across threads up to that cap. Per-page `aggregate_totals_for_page_n` uses a RAM-budgeted worker matrix; if the worker×row partial matrix cannot be allocated, worker count is reduced until `calloc` succeeds (avoids a single-threaded fallback that pins one CPU on huge path-row maps).
- Cheap mode by default — Without `--bucket-details`, the parser seeks past path strings for histogram-only passes, keeping I/O and CPU down when you only need aggregates.

### `ereport_index`: trigram index build (`--make`)

- Same chunk boundaries as `ereport` — Parallel chunk readers; rows can skip path bytes when building for a single UID ( `fseek` past unmatched records).
- Ordered path stream — A single paths writer thread appends `paths.bin` / `path_offsets.bin` in strict path-id order so the index remains coherent while many trigram workers run.
- Producer–consumer queues — Parse workers → paths writer → trigram job queue (bounded) → trigram workers → `tmp_trigrams_*.bin`. Bounded queues apply backpressure instead of unbounded RAM; tuning env vars trades memory vs blocking (see metrics like `writeq_parse_waits` / `trigramq_paths_waits`).
- Sliced bulk enqueue — Trigram jobs from a write batch are enqueued in slices that fit current queue depth, so partial capacity is used instead of waiting until an entire batch fits.
- Batched trigram writes — For each path, trigram codes are sorted by trigram bucket, then `fwrite` runs per contiguous bucket run under one mutex acquisition per slice — far fewer lock rounds and syscalls than one write per trigram.
- Lazy open + LRU on bucket files — Only up to `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS` `tmp_trigrams_*.bin` handles stay open; cold buckets are closed and reopened on demand so you do not need thousands of `FILE*` simultaneously.
- Merge phase — Temp bucket files are sorted (radix on packed records), optionally merged with parallel workers subject to a RAM budget, with large buffered I/O and `mmap` where helpful — separate from index-phase throughput but tuned for large disks.

Together, these choices aim to keep CPU, mutexes, and syscalls off the critical path per byte of crawl data, and to use disk bandwidth (especially on NVMe) with large buffered writes instead of tiny random appends per logical record.

## Testing

```bash
make check              # ./test.sh: integration + ecrawl_repair / edelete / ereport_index smokes (tiny /tmp tree; fast)
make check-tree         # ./test_setup.sh then ./test.sh on ./test (needs all binaries built)
```

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

## Synthetic adversarial trees

`scripts/generate-ecrawl-adversarial-tree.sh` builds stress layouts for `ecrawl` (flat megadir, optional depth chain, wide fan-out, optional `ecrawl_analyze` depth slices, optional ereport badge fixtures). Choose scale with `SYNTH_PROFILE` (unset = quick smoke, `medium`, `heavy`, `extreme`).

`SYNTH_PROFILE=extreme` layers two extra megadirs on top of the heavy-class baseline:

- `mega_dir1/` — about 20M regular files in one directory by default (`SYNTH_EXTREME_MEGA_DIR1_FILES`; always unsharded).
- `mega_dir2/` — `SYNTH_EXTREME_MEGA_DIR2_TOP_FILES` (default 2M) top-level `f…` files plus `SYNTH_EXTREME_MEGA_DIR2_NESTED_PAIR_DIRS` (default 1M) subdirectories `d…/`, each with a single regular file `file` (~3M files + 1M dirs under `mega_dir2/`).

`python3` + bulk create: extreme requires `BATCH_CREATE=1` (the default) and `python3` on `PATH` — `mega_dir1` / `mega_dir2` use the same threaded bulk creator as large flat dirs and the script errors out if bulk mode or `python3` is unavailable.

Disk budget (`DISK_BUDGET_BYTES`): generation refuses when the estimated footprint exceeds the cap (default ~100 GiB). Extreme trees are metadata-heavy; 100 GiB is only an order-of-magnitude guardrail—raise `DISK_BUDGET_BYTES`, tune `ASSUMED_BYTES_PER_FLAT_FILE`, `AUTO_CAP_FLAT`, or lower `SYNTH_EXTREME_*` counts for your filesystem. All presets and tunables (`DISK_BUDGET_BYTES`, `FLAT_FILES`, badge fixtures, etc.) are documented in the comment header at the top of `scripts/generate-ecrawl-adversarial-tree.sh`.

Example (larger cap only — adjust paths and budgets to match your host):

```bash
SYNTH_PROFILE=extreme DISK_BUDGET_BYTES=$((200 * 1024 * 1024 * 1024)) \
  ./scripts/generate-ecrawl-adversarial-tree.sh /tmp/ecrawl-adversarial
```

## Profiling and performance work

Companion scripts profile the tools per fixture and capture a full performance picture — wall-clock timings, `strace -f -c` syscall histograms, `perf record --call-graph dwarf` CPU profiles, and optional `perf sched` thread-concurrency traces (`DO_SCHED`, gated to `SCHED_FIXTURES`) — into an uploadable tarball with a `SUMMARY_TABLE.txt`. Build with `make debug` (or otherwise ensure `-g`) for the best `perf` symbols.

They share one set of crawl outputs. `profile-ecrawl-fixtures.sh` is the producer: it crawls each fixture and keeps the reusable shards at `<bin-root>/<fixture>/bin/`. The other three are consumers — they read that same `<bin-root>`, never crawl, and hard-error if a fixture's bins are missing. So always run the `ecrawl` profiler first, then point the others at the same `<bin-root>`.

- `scripts/profile-ecrawl-fixtures.sh <synth-root> <bin-root> [results-dir]` — runs `ecrawl` against each fixture in `--no-write` and write modes, isolating crawl/`readdir`/donation cost vs. uid-shard writer churn. The write-mode pass keeps each fixture's shards at `<bin-root>/<fixture>/bin/` for the consumers below (needs `DO_WRITE=1`, the default). Knobs: `DO_NOWRITE` / `DO_WRITE` / `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`, and any inherited `ECRAWL_*` (e.g. `ECRAWL_MAX_OPEN_SHARDS`).
- `scripts/profile-ereport-fixtures.sh <bin-root> [results-dir]` — profiles `ereport` (all-users, `--bucket-details 4`) over `<bin-root>/<fixture>/bin/`, writing HTML to `<bin-root>/<fixture>/all_users/`. Knobs: `BUCKET_DETAILS`, `EREPORT_THREADS`, `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`, `KEEP_REPORTS`.
- `scripts/profile-ereport_index-fixtures.sh <bin-root> [results-dir]` — profiles `ereport_index --make` (all-users) over `<bin-root>/<fixture>/bin/`, writing the index to `<bin-root>/<fixture>/index/`. The summary splits time into `chunk_prep` / `index_phase` / `merge_phase` and tabulates the `make_f{open,read,write}` I/O counters that usually drive index-build cost. Knobs: `EREPORT_INDEX_THREADS`, `EREPORT_INDEX_TRIGRAM_THREADS`, `RAISE_ULIMIT`, `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`, `KEEP_INDEX`.
- `scripts/profile-ecrawl_analyze-fixtures.sh <bin-root> [results-dir]` — profiles the read-only `ecrawl_analyze` directory-shape stats over `<bin-root>/<fixture>/bin/` (produces no kept output). Knobs: `ECRAWL_ANALYZE_THREADS`, `ANALYZE_TOP`, `DO_STRACE` / `DO_PERF` / `DO_SCHED`, `REPS`, `FIXTURES`, `SCHED_FIXTURES`.

```bash
# 1) ecrawl (producer): profile every fixture, both modes, with perf — keeps bins under <bin-root>
DO_PERF=1 ./scripts/profile-ecrawl-fixtures.sh /tmp/ecrawl-adversarial /data1/ecrawl-bins

# 2) consumers: reuse the same <bin-root>; no recrawl
./scripts/profile-ereport-fixtures.sh        /data1/ecrawl-bins
./scripts/profile-ereport_index-fixtures.sh  /data1/ecrawl-bins
./scripts/profile-ecrawl_analyze-fixtures.sh /data1/ecrawl-bins
```

Each run prints the results dir and a `…tar.gz` to upload; full options are in each script's comment header. For `perf`, run as root or lower `kernel.perf_event_paranoid`.

### Test- and data-driven development

Performance here is dominated by filesystem *shape*, not just file count: one 20M-entry directory, a million tiny directories, a deep skinny chain, and a high-UID-diversity tree each stress a different part of the pipeline. Guessing where the time goes is unreliable, so the workflow is deliberately data-driven:

1. Generate adversarial shapes with `generate-ecrawl-adversarial-tree.sh` so every pathological case is reproducible on demand.
2. Profile each shape with the scripts above and read the `SUMMARY_TABLE.txt` — timings, syscall histograms, and CPU call-graphs — to name the actual bottleneck (a specific syscall, lock, or callsite) instead of guessing.
3. Change one thing, re-run the same profile, and compare. Keep the change only when the numbers move; the tarballs are the evidence trail.
4. Guard the win with `test.sh` (use `--summary` for a copy/paste results table) so a correctness or throughput regression surfaces immediately.

This loop turned several hunches into measured fixes — raising the writer's open-shard cap to cut uid-shard `open`/`close` churn ~90%, replacing a per-record `ftello()` in `ereport` that issued an `lseek` per record, and moving NSS lookups out of a global lock in `ecrawl` (which roughly halved a many-UID crawl). None were obvious from reading the code; the profiles pointed straight at them.

### Where AI fits

Most of this cycle is mechanical and detail-heavy, which is exactly where an AI coding assistant compresses the turnaround:

- Scaffolding the harness — the profiling scripts (consistent flags, strace/perf passes, a shared producer/consumer bin layout, throwaway vs. kept output dirs, a Python summary parser) are tedious boilerplate an assistant can draft in one pass and keep consistent across every tool.
- Reading the evidence — pasting a `perf report` head or a `strace -c` histogram and asking "what's the hot path and why" turns raw counters into a ranked hypothesis fast, often down to the source line behind a symbol.
- Implementing the fix — once a bottleneck is named (an O(N) registry scan, a per-record seek), the assistant can apply the change, preserve the surrounding invariants, and update tests and docs in the same edit.
- Closing the loop — it can re-run profiles and diff the before/after `SUMMARY_TABLE.txt` to confirm the change actually helped.

The human still owns the decisions — which trade-offs are acceptable, when a number is "good enough," and whether a change is safe to ship — but the measure → diagnose → fix → re-measure loop runs far faster when the assistant handles the scaffolding and the first pass at interpretation. Treat its diagnoses as hypotheses to confirm against the data, not as ground truth.

## What The Tools Do

### `ecrawl`

`ecrawl` walks a local filesystem tree and writes binary metadata records. It supports:

- parallel crawl threads
- uid-sharded output files
- optional `--no-write` benchmarking mode
- live status output
- separate accounting for:
  - unique regular-file logical bytes (`st_size`, hardlink-deduped)
  - allocated regular-file bytes from `st_blocks` (POSIX/Linux 512-byte units, same dedup policy), plus a sparse heuristic file count when allocated < logical
  - directory apparent bytes
  - symlink apparent bytes
  - other apparent bytes

Default write-mode output, when no output directory is provided, is auto-named like:

```text
hostname_apr-17-2026_15-03-01
```

Basic usage:

```bash
./ecrawl [--no-write] [--verbose] [--record-root <abs-path>] <start-path> [output-dir]
```

Positional arguments are only `start-path` (required) and optionally `output-dir`. `start-path` must exist; it is canonicalized with `realpath(3)` (relative or absolute). After the output directory is created, it is canonicalized the same way. If `output-dir` is omitted, a timestamped directory name is created in the current working directory.

#### Directory scanning and stat workers

Applies when `ECRAWL_STAT_THREADS` > 0 (the default). `ECRAWL_STAT_THREADS=0` skips this pool: every child name is `fstatat`’d on the crawl thread that read the directory (same semantics, no cross-thread batch queue).

| Step | What happens |
|------|----------------|
| 1. `readdir` | One crawl worker reads each directory stream sequentially; skips `.` and `..`. |
| 2. Obvious subdirectories | If `d_type` is `DT_DIR`, the child path is pushed onto that worker’s directory stack (and may be donated to other crawl threads). No stat worker involved. |
| 3. Trusted non-directory types | If `d_type` is one of `DT_REG`, `DT_LNK`, `DT_FIFO`, `DT_SOCK`, `DT_CHR`, `DT_BLK`, `DT_WHT`, the name is either `fstatat`’d inline on the crawl thread or queued for stat workers, depending on how many such entries were already seen in this directory (see `ECRAWL_STAT_BATCH_AFTER_RELIABLE_NONDIRS`, default `0`). `0` means always send these names to stat workers (no inline prefix). A positive value `N` handles the first `N` trusted non-dirs inline per directory, then batches the rest. |
| 4. Everything else (`DT_UNKNOWN`, etc.) | `fstatat` on the crawl thread; if it is a directory, behave like step 2; otherwise emit like a file. These names are never placed in stat-worker batches (the batch path assumes the dentry already looked like a non-directory). |
| 5. Workers + flush points | Stat threads `fstatat` batched names against a `dup`'d directory fd. Pending batches are capped globally (`ECRAWL_STAT_QUEUE_BATCHES`). When `readdir` finishes that directory, the crawl thread waits for its pending batches for that folder, then continues. |

Unexpected directory inside a batch: If `fstatat` still reports a directory for a batched name (rare: wrong `d_type` or rename race), `ecrawl` counts `stat_batch_unexpected_dir_total`, prints a `WARN` block on stderr after the run (up to 100 example paths; message notes truncation), and does not descend into those paths—so totals may be incomplete if that warning appears.

Optional environment variables (no CLI flags for these; see also [quick reference](#environment-variables-quick-reference)):

| Variable | Meaning |
|----------|---------|
| `ECRAWL_CRAWL_THREADS` | Crawl threads (minimum 1, default 16; no fixed maximum—practical limits are RAM and OS thread capacity). |
| `ECRAWL_WRITER_THREADS` | Writer threads for uid-sharded `.bin` output (default 8). |
| `ECRAWL_WRITER_QUEUE_BATCHES` | Max pending record batches per uid-shard writer queue when writing output (default 64, range 4…4096); larger values buffer more ~1 MiB batches in RAM. Ignored with `--no-write`. |
| `ECRAWL_UID_SHARDS` | Number of uid shards; must be a power of two (default 1024). |
| `ECRAWL_MAX_OPEN_SHARDS` | Per-writer shard file cache target (default 128 = every shard a writer owns at the default 1024 shards / 8 writers, so many-UID workloads avoid LRU open/close churn); automatically capped against the process open-file limit. |
| `ECRAWL_STAT_THREADS` | Stat worker threads for batched `fstatat` (default 8; `0` disables the pool). |
| `ECRAWL_STAT_BATCH_ENTRIES` | Directory names per stat batch (default 1024, range 64…65536). |
| `ECRAWL_STAT_BATCH_AFTER_RELIABLE_NONDIRS` | Per directory, trusted non-dir `d_type` entries handled inline before stat batching (default `0` = batch from the first entry; set `N` > 0 for an inline prefix of `N` names). Max 2097152. |
| `ECRAWL_STAT_BATCH_MIN_OFFLOAD` | End-of-directory stat batches with fewer than this many names run inline on the crawl thread (default 32; `0` = always enqueue tail batches to the stat pool). Mid-directory flushes at `ECRAWL_STAT_BATCH_ENTRIES` always offload when the stat pool is enabled. |
| `ECRAWL_STAT_QUEUE_BATCHES` | Max pending stat batches globally (default 64, range 4…4096); bounds `dup(dirfd)` backlog and crawl-thread blocking when the pool is full. |
| `ECRAWL_STAT_RANDOM_QUEUE` | `0` = FIFO stat-batch dequeue; non-zero (default `1`) = pseudo-random dequeue among pending batches. |
| `ECRAWL_DONATE_CHECK_EVERY` | During `readdir`, check whether to donate local directory-stack work every `N` `DT_DIR` pushes (default 64; `1` = check after every directory child). |
| `ECRAWL_DONATE_CHUNK_FORCE_MAX` | When the local stack exceeds `ECRAWL_FORCE_DONATE_AT`, donate up to this many directories per queue push (default 2048). |
| `ECRAWL_FORCE_DONATE_AT` | Spill local directory stack to the global task queue when it holds more than this many pending dirs (default 4096). |
| `ECRAWL_DONATE_ALL_BUSY_MIN_STACK` | When every crawl thread already holds a popped task, still allow proactive donation if the local stack is at least this deep and the global queue is below `started × ECRAWL_DONATE_ALL_BUSY_MAX_QDEPTH_MULT` (default 64 dirs; range `donate_floor`…65536). |
| `ECRAWL_DONATE_ALL_BUSY_MAX_QDEPTH_MULT` | Caps global task-queue depth for that “all busy” donation path (default 4; range 1…256). |
| `ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH` | Coalesce `fstatat`-discovered subdir enqueues into fewer global queue pushes (default 48 paths per flush; range 1…4096). |
| `ECRAWL_STALL_HINT_SECONDS` | After the rolling window is warm (~10 seconds), emit one stderr line if `window_entries` stays 0 for this many consecutive seconds (default `5`; `0` disables). Another hint is allowed only after `window_entries` goes non-zero again. |

Examples:

```bash
./ecrawl /path/to/filesystem-tree
./ecrawl --no-write /path/to/filesystem-tree
./ecrawl --no-write --verbose /path/to/filesystem-tree
ECRAWL_CRAWL_THREADS=8 ./ecrawl /path/to/filesystem-tree
./ecrawl /path/to/filesystem-tree host-a_apr-17-2026_15-03-01
./ecrawl --record-root /storage/srv-a /mnt/server-a crawl_srv_a
ECRAWL_UID_SHARDS=4096 ECRAWL_WRITER_THREADS=4 ./ecrawl /path/to/filesystem-tree /tmp/crawl-output
ECRAWL_MAX_OPEN_SHARDS=1024 ./ecrawl /path/to/filesystem-tree /tmp/crawl-output
ECRAWL_STAT_THREADS=0 ./ecrawl /path/to/filesystem-tree
ECRAWL_STAT_BATCH_AFTER_RELIABLE_NONDIRS=0 ./ecrawl /path/to/filesystem-tree
ECRAWL_STAT_RANDOM_QUEUE=0 ECRAWL_STAT_QUEUE_BATCHES=128 ./ecrawl /path/to/filesystem-tree /tmp/crawl-out
```

Notes:

- `--no-write` crawls and reports metrics without writing shard files.
- `--verbose` enables the full end-of-run diagnostics.
- `ECRAWL_DEBUG_LOG` (megadir CSV) and `ECRAWL_PROGRESS_LOG` (1 Hz CSV) were removed; use `--verbose` end-of-run metrics and the built-in contention counters instead.
- `--record-root <path>` rewrites stored paths: each record’s path becomes `<record-root>/<path-relative-to-start-path>` instead of the live mount path. Use one distinct root per storage server so merged reports and search hits stay identifiable (for example `/storage/srv-a/...` vs `/storage/srv-b/...`). The crawl still walks `start-path` on disk; only the strings written into `.bin` files change. The root is turned into an absolute path (relative roots use the current working directory); if that path exists on disk it is also canonicalized with `realpath(3)`.

After every run (including non-verbose), stdout includes lightweight queue contention counters (relaxed atomics only; cheap to collect):

- `uid_shards`: uid shard count used for the output layout.
- `max_open_shards`: effective per-writer shard file cache after any open-file-limit auto-cap.
- `writer_failed`: `1` means at least one writer batch failed; the process exits nonzero in this case.
- `task_queue_pushes`: crawl threads pushing directory tasks onto the global task queue (donations + batched discovered subdirs).
- `queue_lock_waits` / `wait_crawl_tasks`: same underlying counter — increments once per `pthread_cond_wait` episode when a crawl thread waits on an empty global task queue (TLS-batched to the global atomics). Not “mutex acquire count” on push.
- `donate_calls`: directory-stack donation attempts (TLS-batched; folded into the global counter when threads exit).
- `writer_queue_wait_ns`: cumulative nanoseconds crawl threads spent blocked on full writer queues.
- `wait_crawl_tasks`: duplicate of the crawl-queue wait counter above (printed under both names for CSV / human readers).
- `wait_writer_push`: crawl-thread wakeups waiting on a full uid-shard writer queue (writers falling behind).
- `wait_writer_pop`: writer wakeups waiting on an empty queue (crawl threads not feeding writers fast enough).

With `--verbose`, full metrics also include `wait_stat_pop` / `wait_stat_enqueue` (same idea for the stat batch pool), `stat_queue_depth_max`, `stat_batches_*`, `stat_batch_unexpected_dir_total`, `donate_all_busy_*`, `discovered_dir_enqueue_batch`, and crawl `manifest=` path plus `st_blocks_bytes_unit`, `total_allocated_bytes`, `files_sparse_heuristic` (same keys as `crawl_manifest.txt`).

Interpret these as counts of blocking episodes, not wall-clock time. Summary `WARN` lines for unexpected batched directories always go to stderr, even when stdout is concise.

### `ecrawl_repair`

Use `ecrawl_repair` when a crawl directory has `uid_shard_*.bin` files but `ereport` or `ereport_index --make` cannot map chunks because `.ckpt` sidecars are missing or invalid—or when you want to confirm sidecars match `crawl_bin_load_ckpt()` in `crawl_bin_chunks.c` (the shared loader used by `ereport` and `ereport_index`) before running readers.

It does not modify the `ecrawl` or `ereport` programs; it operates only on crawl output files in the directory you pass. Behavior highlights:

- Parallel shard rescans — Set `ECRAWL_REPAIR_THREADS` (default 16, minimum 1).
- Incomplete tail — If the record stream fails because the last record is truncated (common after a crashed crawl), `ecrawl_repair` `truncate`s the shard `.bin` to the last complete record boundary, rescans, and writes `*.bin.ckpt`. If `truncate` fails, the shard is treated like other corrupt files (see below).
- `*.bin.ckpt` — Written (or overwritten) next to each repaired shard using the same on-disk layout `ecrawl` uses.
- Corrupt / unusable shards — Shards that cannot be salvaged (bad container header, damaged middle of the file, or truncate failure) are `rename`’d into `corrupt_shards/` under the crawl directory (optional `*.bin.ckpt` beside them moves too when possible). `ereport` only scans `uid_shard_*.bin` in the crawl directory root, so quarantined files are excluded until you move or fix them manually.
- Summary line — After processing, stderr prints aggregate tail-truncation stats (shard count, bytes removed, original vs new totals in bytes and human-readable KiB/MiB/…) when any truncation occurred—or would-be stats with `--dry-run`.
- `--dry-run` — Does not `truncate`, `rename` to `corrupt_shards/`, or write `.ckpt`; still reports what would happen.
- `--verbose` — Per-shard progress on stdout (and `ok` on successful exit).
- Exit status — On a normal run (not `--dry-run`), exit 0 means every remaining top-level `uid_shard_*.bin` has an `ereport`-compatible sidecar; exit nonzero if operational errors occurred, verification failed, or every shard was quarantined so `ereport` would see no shard files.

Usage:

```bash
./ecrawl_repair [--dry-run] [--verbose] <crawl-output-dir>
ECRAWL_REPAIR_THREADS=32 ./ecrawl_repair /path/to/crawl-out
```

### `ecrawl_analyze`

`ecrawl_analyze` reads `uid_shard_*.bin` shards in a crawl output directory (read-only—no shard or `.ckpt` writes) and prints aggregate directory-shape metrics on stdout.

Behavior highlights:

- Chunk boundaries — Parse jobs follow `*.bin.ckpt` segment boundaries when sidecars are valid (same spirit as `ereport` / `ereport_index` chunk mapping). If a checkpoint is missing or unusable, that shard is scanned as a single range from after the file header through EOF.
- Parallelism — Worker count comes from `ECRAWL_ANALYZE_THREADS` (default 16, range 1…4096). If `ECRAWL_ANALYZE_THREADS` is unset, `ECRAWL_REPAIR_THREADS` is used when set, so existing repair-tuning scripts can drive analyze without a second variable.
- Progress — When stderr is a TTY, a one-line status (bytes scanned, chunk and record rates, elapsed time, ETA) updates about once per second; the final report is always plain text on stdout.
- `--top N` — List the top `N` parent directories (1…100000; default 32). The default dimension is `dense` (most regular files); each row shows `nfile`, `ndir`, `nsym`, `nother`, and the parent path. Select dimensions with `--top,DIM[,DIM] N` (order-independent): `dense` = top parents by regular-file count, `deep` = deepest parent directories by path slash count. For example `--top,deep 20` lists only the deepest directories, `--top,dense,deep 20` prints both lists. The `deep` table adds a leading `depth` column.
- `--verbose` / `-v` — While scanning, prints one line per successfully parsed chunk (shard path, byte range, record count), mutex-serialized so lines are not interleaved mid-line; chunk failures are reported on stderr (often corrupt data or checkpoint mismatch).

Stdout summary (stable `key=value` / section headers) includes: shard and chunk job counts, `records_total`, distinct-parent counts, a histogram of regular files per parent (bucketed counts among parents that have at least one file), a slash-count (depth) histogram over stored paths, and the selected top lists (`top_parents_by_regular_file_count` for `dense`, `top_parents_by_depth` for `deep`).

Usage:

```bash
./ecrawl_analyze [--verbose] [--top[,dim...] N] <crawl-output-dir>
```

Examples:

```bash
./ecrawl_analyze /path/to/crawl-out
./ecrawl_analyze --top 100 /path/to/crawl-out
./ecrawl_analyze --top,deep 50 /path/to/crawl-out          # deepest directories only
./ecrawl_analyze --top,dense,deep 50 /path/to/crawl-out    # both top lists
ECRAWL_ANALYZE_THREADS=32 ./ecrawl_analyze -v /path/to/crawl-out
```

### `ereport`

`ereport` reads crawl output and builds an HTML report under `./<username>/` (resolved login name), unless that name is unusable—then it falls back to `./tmp/`. If you omit the username and pass only the time basis (`./ereport atime …`), it aggregates all UIDs present in the crawl and writes under `./all_users/`. If the first token is not a time keyword and does not resolve as a login/UID, it is treated as the first `bin_dir` and the run is all-users with effective age buckets (`max(atime,mtime,ctime)` per file). For single-user runs, omitting the time argument also selects effective.

Outputs:

- `./<username>/index.html` — heat map, path search box (uses server-side index when served via `eserve`), full statistics below the table
- `./<username>/bucket_aX_sY.html` — per age/size cell; brief summary HTML unless you pass `--bucket-details N` (see below). With `--bucket-details`, each page lists directory rollup tables for N path levels below the shared prefix inside that bucket.
- `./all_users/` — same layout; `./all_users/bucket_aX_sY.html` is a brief summary unless `--bucket-details` is used (heat-map totals on `index.html` always match the crawl).

Place `--bucket-details N` (`N` = 1…32) first, before the username (if any) and time basis. Omit it for fast runs and small bucket HTML (no path reads for drill-down tables).

Search UI:

- One search field; results render below it (no sliding drawer), so it stays obvious what you're editing.
- Typing ≥3 characters shows a preview list; press Enter for paged results in the same panel. Hide collapses the panel without clearing the query.
- In-flight preview requests are aborted when the query changes, so fast typing leaves no stale matches on screen.
- Result lines show timing plus corpus scale: `indexed_paths` from the index's `meta.txt` ("~N paths indexed") when present, otherwise `index_keys` (distinct trigrams in `tri_keys.bin`, "~N trigrams").

Heat map (`index.html`):

- Each age×size cell is split diagonally: the upper-right triangle shows data volume and share of total bytes (blue intensity); the lower-left shows file count (with a rounded count like *2M* when large) and share of total files (rose intensity).
- Inner cell colors scale so the strongest inner cell reaches full saturation; row and column totals scale against the full corpus (100%) so margin labels stay comparable to "fraction of everything."
- Size-bucket (column) headers use a light blue tint; age-bucket (row) headers a light rose tint; the "Age × Size" corner and "Total" label cells stay neutral.
- The table uses `class="heatmap"` so these styles don't collide with generic `th` rules elsewhere.

Usage:

```bash
./ereport [--bucket-details N] [--subtree PATH] <username|uid> [<atime|mtime|ctime|effective>] [bin_dir ...]
./ereport [--bucket-details N] [--subtree PATH] [<atime|mtime|ctime|effective>] [bin_dir ...]   # all users → ./all_users/
```

If you omit every `bin_dir`, `ereport` reads crawl `.bin` files from the current working directory (`./`).

The first argument is treated as a time basis (`atime`, `mtime`, `ctime`, or `effective`) only when it matches exactly—otherwise it is interpreted as a username or numeric UID, or (if that fails) as the start of the `bin_dir` list for an all-users run with effective time. You cannot name an account literally `atime` without resolving that ambiguity (e.g. numeric UID).

Thread count: set `EREPORT_THREADS` (default 32). This controls parallel `.bin` chunk readers during the scan, parallel emission of `bucket_aX_sY.html` (36 heat-map cells), and the stats thread. It is not set on the command line.

Multiple crawl directories: pass several `bin_dir` paths (each an `ecrawl` output folder). Every directory must use the same layout (unsharded flat bin set vs `uid_shards`) and the same `uid_shards` count when manifests are present. For each directory, `ereport` loads that user’s shard file when you specify a user (uid-sharded layout), or every shard file when aggregating all users; unsharded layouts still load all matching bins. So twenty servers mean twenty directories and twenty shard files merged into one report (per-user), or all shards from every directory (all-users mode).

Examples:

```bash
./ereport alice atime host-b-mgmt_apr-17-2026_15-07-17
EREPORT_THREADS=16 ./ereport alice atime /tmp/crawl-out
EREPORT_THREADS=8 ./ereport 82831 mtime /tmp/crawl-out
EREPORT_THREADS=16 ./ereport alice atime crawl_srv01 crawl_srv02 crawl_srv03
./ereport alice atime crawl_a crawl_b crawl_c
./ereport atime /tmp/crawl-out
EREPORT_THREADS=16 ./ereport mtime crawl_srv01 crawl_srv02
EREPORT_THREADS=64 ./ereport ctime /path/to/crawl
./ereport --bucket-details 3 alice mtime crawl_out
./ereport --bucket-details 3 mtime crawl_srv01 crawl_srv02
./ereport alice /tmp/crawl-out                               # single-user, effective time (default)
./ereport effective /tmp/crawl-out                          # all-users, explicit effective
./ereport /tmp/crawl-out                                    # all-users effective if path is not a user name
./ereport --subtree /orcd/data/ki/001/lab/jones mtime crawl_out   # analyze only that subtree of an existing crawl
```

Parse chunks scale with input `.bin` size so parallel workers are not capped by a tiny chunk count.

Bucket drill-down:

- By default `ereport` does not read path strings: the parser seeks past path bytes and `bucket_aX_sY.html` pages stay short summaries.
- `--bucket-details N` (N = 1–32) makes it read paths and emit N directory-level rollup tables per bucket page; applies to single- and all-users runs (larger N and all-users cost more I/O and memory).
- Each level lists directories sorted by bucket bytes (largest first); past 200 directories at a depth, only the top 200 rows are written (heat-map and bucket-header totals still reflect the full bucket).
- With `--bucket-details`, pages also include a path-shape drill-down (Dense / Deep / Skew) with collapsible, sortable, slice-first sections.

Subtree scoping:

- `--subtree PATH` (absolute) restricts the whole analysis to records at or under `PATH`, as if only that directory had been crawled — useful for zooming into one lab/group inside a larger `--record-root` crawl without re-crawling. Place it before the username (if any) and time basis. The subtree directory itself is included, and matching is on a directory boundary (so `…/jones` does not match `…/jones2`).
- Full absolute paths are kept in the report (records are filtered, not rewritten); all heat-map totals, badges, distinct-user counts, and bucket-detail tables are scoped to the subtree. It forces per-record path reconstruction, so it is a bit slower than the default histogram-only fast path even without `--bucket-details`.
- `manifest_*` lines (e.g. `total_allocated_bytes`) come from `crawl_manifest.txt` and still describe the whole crawl, not the subtree.

Runtime behavior:

- `ereport` scans crawl directories, then maps chunk boundaries inside each `.bin` shard (reading record headers only). That mapping runs with `EREPORT_THREADS` parallel scanners; stdout shows `chunk-map files:X/Y` until every shard has been scanned, then the usual records/sec line appears while workers parse chunks. If mapping is slow, an occasional stderr advisory may print after a completed progress line so it does not glue to the `chunk-map` status text.
- After parsing finishes, the status line switches to finalizing with sub-steps: merging shard summaries (in-process merges of per-thread summaries and bucket-detail maps), writing bucket HTML (n/36) while `bucket_*.html` files are emitted, then writing index.html. Verbose mode can surface lookup substeps (repartition → path_shape scan → margins) via atomic `finalize_lookup_stage`.
- Final run stats go to stdout; warnings/errors to stderr. Progress uses local counters with chunked flushes to avoid per-record atomics on the hot path. When `crawl_manifest.txt` in the input `bin_dir` set lists `total_allocated_bytes` / `files_sparse_heuristic`, `ereport` aggregates those across merged crawls and prints `manifest_*` lines plus HTML/drilldown copy for allocated space and the sparse estimate (full-corpus crawl totals, not UID-filtered heat-map bytes).

Interactive search in `index.html` requires `ereport_index --make` (see below), `eserve` running with `ereport_index` available, and opening the report over HTTP (browser `fetch` does not work reliably from `file://`).

### Sample HTML fixtures and screenshots

The checked-in `all_users/` tree is a static report generated from a synthetic adversarial layout (with `--bucket-details`), kept in the repo so you can browse the UI without running a crawl. File paths inside those HTML files use placeholders such as `demo-volume/example-user/…` instead of real home directories, volume names, or host-specific prefixes.

- `all_users/index.html` — aggregate heat map, search box (needs HTTP + index for live search), and corpus-wide statistics for all UIDs in the crawl.
- `all_users/bucket_aX_sY.html` — Bucket details for one age×size cell: header metadata, optional Dense / Deep / Skew badges, and directory rollup tables when drill-down was enabled at generation time.

Open the HTML directly in a browser, or serve the tree with `eserve.py` / `make serve` as described below.

The images below are illustrative mockups of the layout (fonts and spacing may differ slightly from the real CSS). For pixel-accurate rendering, use the fixture files above.

![All-users index (heat map and search)](docs/images/ereport-all-users-index.png)

![Bucket details page (per age×size cell)](docs/images/ereport-bucket-details.png)

### `ereport_index`

`ereport_index` builds and searches an on-disk trigram index over crawl path strings—either for one resolved Unix user or for every UID when no user is selected (see `--make` disambiguation below; same idea as `ereport` all-users mode: all uid-shard files, no UID filter on records).

The search is a case-insensitive substring match on individual path segments (slashes separate segments; matches do not span `/`). For example:

```text
doc
```

matches paths such as:

```text
/path/foo/alice/...
/path/foo/acme-docs/...
/path/foo/doc/...
```

Queries must be at least three characters (trigram filtering).

Usage:

```bash
./ereport_index --make [--index-dir <path>] [--subtree <abs-path>] [username|uid] [bin_dir ...]
./ereport_index --resume-merge --index-dir <path>
./ereport_index --search [--index-dir <path>] <term> [--json] [--skip N] [--limit M]
```

`--make` user vs all-users: If the first argument after optional `--index-dir` is a valid login name or numeric uid on this system, it names the report user and any further arguments are crawl directories (default `./`). If that first token is not a known user (for example it is a crawl output directory name), every argument—including the first—is treated as a `bin_dir`, and the index is built for all UIDs under `./all_users/index/` unless `--index-dir` overrides the location (same merge semantics as `ereport` aggregate output). `./ereport_index --make` with nothing after `--make` indexes `./` for all users.

`--subtree <abs-path>` (may precede or follow `--index-dir`, must come before the username/`bin_dir` arguments) indexes only records whose reconstructed full path is at or under that absolute directory, mirroring `ereport --subtree`. Full absolute paths are kept in the index, so a search over a subtree index returns the same paths as the full index would, just restricted to that directory. Matching is on a directory boundary (so `…/jones` does not match `…/jones2`).

You can pass multiple `bin_dir` paths (same merged crawl directories as for `ereport`); they are merged into one index.

Examples:

```bash
./ereport_index --make alice host-b-mgmt_apr-17-2026_15-07-17
./ereport_index --make /path/to/crawl-out
./ereport_index --make crawl_srv01 crawl_srv02
./ereport_index --make --index-dir /var/lib/example-search alice crawl_a crawl_b
./ereport_index --make --subtree /orcd/data/ki/001/lab/jones /path/to/crawl-out   # index only that subtree
./ereport_index --resume-merge --index-dir /var/lib/example-search
./ereport_index --search --index-dir alice/index doc
./ereport_index --search --index-dir all_users/index doc
./ereport_index --search --index-dir /var/lib/my-index doc
./ereport_index --search --index-dir alice/index doc --json --skip 0 --limit 20   # JSON body for APIs
```

Default behavior:

- `--make [--index-dir <path>]` — without `--index-dir`, `--make <user> …` writes under `./<username>/index/` (`paths.bin`, `tri_keys.bin`, etc.). With `--index-dir`, those files go directly under `<path>` (the directory is created if needed).
- `--make` with only crawl directories (first token not a system user) writes under `./all_users/index/` unless `--index-dir` is set.
- `--make` with only `<username|uid>` and no `bin_dir` arguments reads crawl input from `./` (same idea as `ereport`).
- `--search [--index-dir <path>] <term>` — optional `--index-dir` (same flag as `--make`). If omitted, the index directory defaults to `./index` relative to the current working directory.
- `EREPORT_INDEX_THREADS` — optional; if set to an integer in 1…4096, sets parallelism for `--make` (default 32 when unset or invalid): chunk-boundary mapping runs with up to this many scanners across distinct input `.bin` files (capped by file count), and parse/index uses the same count for parallel chunk readers (parse workers). Trigram writers default to the same count; override with `EREPORT_INDEX_TRIGRAM_THREADS` (below). This does not set trigram merge worker count. Merge uses up to 16 workers by default, capped by available RAM (each merge worker may hold about 2× the largest `tmp_trigrams_*.bin` bucket in memory during sort). Tune merge parallelism with `EREPORT_INDEX_MERGE_RAM_FRAC` / `EREPORT_INDEX_MERGE_MEMORY_MB` (see below). Raising thread count increases peak RAM mostly by having more workers fill bounded queues (paths writer depth is `EREPORT_INDEX_WRITEQ_MAX_BATCHES`).
- `EREPORT_INDEX_TRIGRAM_THREADS` — optional; parallel writers appending to `tmp_trigrams_*.bin` during `--make`. Defaults to `EREPORT_INDEX_THREADS` (same integer range 1…4096). Use when trigram temp I/O is the bottleneck and you can afford more concurrent bucket files (subject to `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS` / `ulimit -n`).
- `EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH` — optional; bounded queue of path jobs between the paths writer and trigram workers (range 512…262144). When unset, default depth scales with `EREPORT_INDEX_TRIGRAM_THREADS` (64× workers, minimum 4096, capped at 16384) so high parallelism does not starve workers as easily; override explicitly for more headroom (uses more RAM).
- `EREPORT_INDEX_WRITE_BATCH_PATHS` — optional; target number of paths per batch handed to the paths writer (default 4096, range 512…65536). The effective flush size is also scaled down when `EREPORT_INDEX_THREADS` is high (see `write_batch_flush_at` in `--make` stats).
- `EREPORT_INDEX_WRITEQ_MAX_BATCHES` — optional override (4…4096) for how many write batches may wait on the single writer thread during `--make`. Default scales with `EREPORT_INDEX_THREADS` (about threads/3, clamped 6…96). Raising this raises peak memory if workers outpace the writer; lowering it adds backpressure (workers block until the writer drains).
- `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS` — optional cap (32…4096) on how many `tmp_trigrams_*` bucket `FILE*` handles each trigram worker may keep open (LRU); unset defaults to 4096, then split across workers using an assumed fd budget (see `ulimit` below). Lower this if you hit `EMFILE`.

#### Open files (`ulimit`)

`ereport_index --make` uses many descriptors (parallel trigram shards, merge I/O, crawl inputs). Before large builds run:

```bash
ulimit -n 65535
```

(65535 or higher.) If `Too many open files` persists, raise `ulimit -n` further, lower `EREPORT_INDEX_THREADS`, or set `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS` lower. If `ulimit -n` cannot go high enough, raise the hard limit (often `/etc/security/limits.conf` or `systemd` `LimitNOFILE=`) and open a new shell.

File size (`ulimit -f`) — If `--make` dies with `File size limit exceeded` / core dump from `SIGXFSZ`, the process hit `RLIMIT_FSIZE`: the max size of any single file this process may grow (not open-file count, and not “free disk”). Use `ulimit -f unlimited` before large `--make` runs unless you deliberately cap output size.

Do not mirror `ulimit -n` onto `-f`. In bash on Linux, the number after `ulimit -f` is kilobytes (each unit is 1024 bytes). So `ulimit -f 200000` is only about 200000 × 1024 ≈ 195 MiB per file — `paths.bin` alone exceeds that quickly at ~1M paths. `ulimit -n 200000` raises the descriptor count; it does not remove a file-size cap.

Check the effective limit with `ulimit -f` / `ulimit -a` (or `prlimit`). `ereport_index` also prints a one-line stderr warning at `--make` / `--resume-merge` start when the soft limit is finite and below 64 GiB.

JSON search output is one UTF-8 JSON object per line (fields mirror `ereport_index --help`):

```json
{"total":123,"skip":0,"limit":50,"search_ms":4,"index_keys":63000,"indexed_paths":2000000,"paths":["...","..."]}
```

- `index_keys` — number of distinct trigram keys in `tri_keys.bin`.
- `indexed_paths` — corpus size from `meta.txt` (`indexed_paths=`), aligned with `paths.bin` entry count (`0` if `meta.txt` is missing or unreadable).
- `search_ms` — server-side search duration for that request.

If `--json` is given without `--limit`, limit defaults to 50.

### Index build pipeline (`--make`)

Roughly two phases:

1. Scan / index — Input `.bin` files are listed, then chunk boundaries are mapped in parallel (same idea as `ereport`, using `EREPORT_INDEX_THREADS`, capped by how many bin files exist). Parallel workers then read each chunk (record headers first). For a single-user build, rows whose UID does not match skip reading the path string (`fseek` past it). For an all-users `--make` (no resolved username as the first argument), every matched layout row’s path is indexed (no UID filter). Parsed paths are batched to a paths writer thread that appends `paths.bin` and `path_offsets.bin` in strict order, then enqueues linked trigram jobs from each write batch to the trigram queue in slices that fit current free depth (fewer mutex rounds than per-path enqueue, without stalling until an entire batch fits). Multiple trigram writer threads append `tmp_trigrams_*.bin` shard files in parallel (lazy `FILE*` handles and LRU eviction per `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS`; requires sufficient `ulimit -n`). For each path, trigram codes are sorted by bucket and written with batched `fwrite`s per bucket (fewer lock rounds and syscalls than one write per trigram). Chunk input files use large stdio buffers; trigram code lists use a cheap hybrid sort/dedup for uniqueness. The trigram job queue depth defaults to 4096 parsed paths between the paths writer and trigram workers; override with `EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH` (see above).

2. Merge — Temp per-bucket trigram files are sorted and merged into `tri_keys.bin` + `tri_postings.bin`. When enough buckets have data, merge runs multiple worker threads that write per-bucket segment files, then a single thread stitches postings offsets and concatenates blobs. Reads prefer `mmap` with `malloc`+`read` fallback; sorting uses LSD radix on packed records. During indexing, the builder records which trigram buckets were touched so merge can skip `stat`-ing thousands of empty bucket paths. All heavy merge I/O uses large buffers.

At `--make` start, `ereport_index` prints one stderr line with effective parse workers, trigram workers, trigram job queue depth, and write batch path cap. Final stdout stats include `trigram_queue_depth` and `write_batch_paths` for the run.

Merge RAM cap (OOM avoidance): Parallel merge worker count is limited so that `workers × (≈2 × largest bucket file + overhead)` stays under a budget derived from `min(MemAvailable, cgroup v2 memory.max)` × `EREPORT_INDEX_MERGE_RAM_FRAC` (default 55%). Override the budget with `EREPORT_INDEX_MERGE_MEMORY_MB=<MiB>` (explicit cap) or `EREPORT_INDEX_MERGE_RAM_FRAC` (e.g. `0.6`). Session cgroups often enforce a limit far below host RAM—raising threads without raising the cgroup cap can still OOM.

#### `ereport_index.log` (memory snapshot)

During `--make` and during `--resume-merge`, a background thread appends one line every 8s to `ereport_index.log` under the index directory (`wb` = paths-writer queue bytes, `tj` = trigram job queue, `cq` = chunk-queue estimate, `mp` = estimated merge RAM peak). On resume-merge, only `mp` is meaningful (other fields stay 0).

#### `--resume-merge` (interrupted merge)

If `--make` finished the indexing phase (`paths.bin` + `path_offsets.bin` exist) but died during merge (OOM, kill, etc.), you can finish trigram output without re-scanning crawl data:

```bash
./ereport_index --resume-merge --index-dir /path/to/index
```

Resume deletes any partial `tri_keys.bin` / `tri_postings.bin`, removes orphan `merge_seg_k_*` / `merge_seg_p_*` halves left by a crash, converts remaining `tmp_trigrams_*.bin` to segment files, then stitches all segment pairs into new `tri_*` files and writes `meta.txt`.

Cannot resume when leftover `tmp_trigrams_*.bin` exist and a non-empty `tri_keys.bin` exists but there are no `merge_seg_*.bin` files (that pattern means single-thread merge was interrupted; data lived only in the partial `tri_*` stream). In that case run a full `--make` again.

`EREPORT_INDEX_THREADS` does not apply to resume-merge merge workers (same merge RAM rules as above).

### `--make` summary metrics (stdout)

Typical keys include:

- Where time goes: `chunk_prep_sec` (parallel chunk-boundary mapping only) vs `index_phase_sec` (wall clock from run start through closing `paths.bin` / `path_offsets.bin`, including scan, prep, directory setup, parallel parse + writers + draining queues) vs `merge_phase_sec` vs `wall_after_index_sec` (merge + `meta.txt` and similar; should be ≈ `merge_phase_sec` plus tiny overhead). `elapsed_sec` is end-to-end. `avg_paths_per_sec` divides by `elapsed_sec`, so it understates peak index throughput if merge is fast; use `index_paths_per_sec` (paths ÷ `index_phase_sec`) for overall index-stage rate.
- CPU by phase (Linux `getrusage`, all threads summed): on successful `--make`, lines prefixed `cpu_prep_`, `cpu_idx_`, `cpu_mrg_`, `cpu_make_` report user/sys CPU seconds, voluntary/involuntary context switches, and minor/major page faults between phase boundaries (`_cpu_user_sec`, `_cpu_sys_sec`, `_ctx_sw_vol`, etc.).
- Stdio/POSIX I/O counts for `--make`: `make_fread_*`, `make_fwrite_*`, `make_fopen_calls`, `make_fclose_calls`, `make_open_calls`, `make_read_*`, `make_mmap_calls`, `make_munmap_calls`, `make_trigram_append_batches` (once per batched append to `tmp_trigrams` for a bucket—much smaller than `trigram_records`)—useful for tuning syscalls / temp I/O behavior.
- Throughput and scale: `scanned_records`, `indexed_paths`, `trigram_records`, `unique_trigrams`, `index_workers`, `index_trigram_workers`, `writeq_max_batches`, `write_batch_flush_at` (queue backpressure and paths-per-batch flush tuning).
- Merge: `merge_phase_sec`, `merge_workers` (after RAM cap), `merge_workers_cpu` (CPU-based choice before cap), `merge_max_bucket_mib`, `merge_parallel_ram_budget_mib`, `merge_buckets_nonempty`, `merge_buckets_skipped`, `merge_trigram_records_read`, byte counts for temp reads and final `tri_*` outputs, derived `*_per_sec` rates.
- Queue wait counters (each increment only when a thread blocks on `pthread_cond_wait`; no overhead on the non-blocking fast path): `writeq_writer_waits` (paths writer starved for parse batches), `writeq_parse_waits` (parse workers blocked because the paths writer queue is full), `trigramq_paths_waits` (paths writer blocked because the trigram job queue is full), `trigramq_worker_waits` (trigram workers idle waiting for jobs). High counts usually mean raising `EREPORT_INDEX_WRITEQ_MAX_BATCHES` or `EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH`, tuning `EREPORT_INDEX_THREADS` / `EREPORT_INDEX_TRIGRAM_THREADS`, or faster trigram temp I/O (subject to RAM and disk).

Successful `--resume-merge` prints a shorter set (`mode=resume_merge`, `index_dir`, `indexed_paths`, `unique_trigrams`, merge timings and sizes, `elapsed_sec`).

### Index format and on-disk layout

Current index format:

- disk-native and binary
- lowercased trigram postings for search
- original full paths stored separately for result lookup
- intended to be read directly by a later server-side helper

Current files under `<username>/index/`:

- `meta.txt` — small key/value record written at end of `--make` / `--resume-merge` (includes `indexed_paths=` corpus size and format/version fields)
- `path_offsets.bin`
- `paths.bin`
- `tri_keys.bin`
- `tri_postings.bin`

During merge, transient `tmp_trigrams_*.bin` files are removed as buckets are processed. Parallel merge may create short-lived `merge_seg_k_*` / `merge_seg_p_*` segment files under the same directory; successful runs delete them after the stitch step. `--resume-merge` also drops orphan half-segment files if a crash left only one of the pair.

### `eserve.py`

HTTP server for generated HTML/CSS and bucket pages. It also implements `GET …/search`, which runs `ereport_index --search` against the configured trigram index (default: next to the report; see Search index location below).

Requirements

- Python 3 on PATH (`python3`).
- `ereport_index` available:
  - sibling `./ereport_index` next to `eserve.py` (after `make ereport_index`), or
  - on `PATH`, or
  - override with `EREPORT_INDEX_BIN=/absolute/path/to/ereport_index`.
- Search index location (directory containing `tri_keys.bin`, etc.):
  - default: `<report>/index` next to `index.html` (or `SERVE_ROOT/<user>/index` for `GET /<user>/search`);
  - override: `--index-dir DIR` or `EREPORT_SEARCH_INDEX_DIR` (CLI wins). The same directory is used for every `…/search` request and may live outside `SERVE_ROOT`.

Make targets (from the repo root):

```bash
make serve              # bind 127.0.0.1, port 8000 by default
make serve-public       # bind 0.0.0.0 (all interfaces), same default port
```

Variables (passed to `make`):

| Variable      | Default   | Meaning                          |
|---------------|-----------|----------------------------------|
| `SERVE_ROOT`  | `.`       | Directory whose files are served (last argument to `eserve.py`) |
| `SERVE_PORT`  | `8000`    | TCP port (both `serve` and `serve-public`) |
| `SERVE_BIND`  | `127.0.0.1` | For `make serve` only: `eserve.py --bind`. `make serve-public` always uses `0.0.0.0` and does not read `SERVE_BIND`. |
| `SERVE_INDEX_DIR` | (empty) | If set, passed to `eserve.py --index-dir`. Use `SERVE_INDEX_DIR=/abs/path` (one `=`); `==/path` is a common typo—`eserve.py` strips leading `=` so absolute index paths still work. |
| `PYTHON3`     | `python3` | Python interpreter used to run `eserve.py` |

Examples:

```bash
# Serve one user’s report tree (contains index.html + index/ + bucket_*.html)
make serve-public SERVE_ROOT=./alice SERVE_PORT=8080

# Serve a parent directory that contains a user folder
make serve-public SERVE_ROOT=. SERVE_PORT=8000
# Then open http://<host>:8000/alice/index.html

# Custom port + index outside the tree (same as eserve.py --index-dir)
make serve SERVE_PORT=9000 SERVE_INDEX_DIR=/data/my_index SERVE_ROOT=./report
```

Search HTTP API (used by `index.html` via relative `fetch`):

| URL pattern | When to use |
|-------------|-------------|
| `GET /search?q=…&skip=…&limit=…` | `SERVE_ROOT` is the report directory (e.g. `./alice`) so `index.html` is at the root of the site. |
| `GET /<user>/search?q=…` | `SERVE_ROOT` contains the user folder (e.g. SERVE_ROOT=. and files live under `./alice/`). |

Parameters:

- `q` — search string (≥ 3 characters after trimming).
- `skip` — offset into the ranked match list (default `0`).
- `limit` — page size (default `50`; capped server-side).

Successful responses are `application/json` from `ereport_index`: `total`, `skip`, `limit`, `search_ms`, `index_keys`, `indexed_paths`, and `paths` (string array). Errors return JSON or plain HTTP errors depending on failure mode.

Direct `python3` invocation:

```bash
python3 eserve.py --bind 127.0.0.1 --port 8000 /path/to/serve
python3 eserve.py --bind 0.0.0.0 --port 8080 ./alice
# Index built elsewhere (single index for all /search and /<user>/search calls):
python3 eserve.py --index-dir /data/report_index /path/to/serve
```

## Typical Workflow

For multiple crawl outputs (e.g. several servers), run `ecrawl` once per output directory (often with distinct `--record-root` values), then pass all of those directories to one `ereport` / `ereport_index --make` command so the report and search index stay unified. For an all-users aggregate report (`./ereport ctime …`), build the matching index with `ereport_index --make dir1 dir2 …` where the first argument is not a valid username/uid on this host—pass the same `bin_dir` list as for `ereport` (for example directory names only).

### 1. Crawl a filesystem

```bash
./ecrawl /path/to/filesystem-tree
```

This writes binary shard files into an auto-generated output directory unless you provide one explicitly.

### 2. Build a per-user report

```bash
./ereport alice atime host-a_apr-17-2026_15-03-01
```

This writes:

```text
alice/index.html
alice/bucket_a0_s0.html
...
```

### 3. Optionally build a search index

One crawl output directory (default index location is `./<username>/index/`):

```bash
./ereport_index --make alice host-a_apr-17-2026_15-03-01
```

Several crawl output directories (merged index for the same user):

```bash
./ereport_index --make alice crawl_srv01 crawl_srv02 crawl_srv03
```

Omit directories to use `./` as the only input path: `./ereport_index --make alice`.

All-users index (same crawl inputs as `./ereport ctime …`). Omit a username: list crawl dirs first so the first token is not resolved as a login or uid:

```bash
./ereport_index --make crawl_srv01 crawl_srv02
./ereport_index --search --index-dir all_users/index foo
```

By default this writes under `./all_users/index/` (unless `--index-dir` points elsewhere):

```text
all_users/index/meta.txt
all_users/index/path_offsets.bin
all_users/index/paths.bin
all_users/index/tri_keys.bin
all_users/index/tri_postings.bin
```

For a single-user index (example user `alice`), the same filenames appear under `alice/index/`.

### 4. Serve the results over HTTP

Pick `SERVE_ROOT` depending on how you want URLs to look:

Option A — Serve the user directory directly (`index.html` at site root):

```bash
make serve-public SERVE_ROOT=./alice SERVE_PORT=8000
```

Open `http://127.0.0.1:8000/index.html`. Search requests go to `http://127.0.0.1:8000/search?q=…` (handled by `eserve.py`).

Option B — Serve a parent directory (URL includes username):

```bash
make serve-public SERVE_ROOT=. SERVE_PORT=8000
```

Open `http://127.0.0.1:8000/alice/index.html`. Search requests resolve to `http://127.0.0.1:8000/alice/search?q=…`.

Ensure `ereport_index` is built (`make ereport_index`) or set `EREPORT_INDEX_BIN` before starting the server.

## Validation Helpers

`test.sh` behavior is summarized under Testing above (integration vs optional filesystem correlation; `find`/`fd` vs `ecrawl` and vs `ereport` all-users; `ecrawl` vs `ereport`; single-user subset checks).

Example:

```bash
./test.sh /path/to/test-correlation-root
```

Used during development and benchmarking; not part of the normal end-user workflow.

## Output Semantics

### `ecrawl` byte totals

`ecrawl` reports:

- `total_bytes`: unique regular-file logical bytes (`st_size`, hardlink-deduped)
- `st_blocks_bytes_unit`: multiplier for `st_blocks` (512 on typical Linux/glibc builds)
- `total_allocated_bytes`: unique regular-file on-disk bytes (`st_blocks × st_blocks_bytes_unit`, same inode dedup as `total_bytes`)
- `files_sparse_heuristic`: count of deduped regular files where allocated bytes < logical size (heuristic for sparse / preallocated files)
- `dir_apparent_bytes`: apparent size of directories
- `symlink_apparent_bytes`: apparent size of symlinks
- `other_apparent_bytes`: apparent size of other matched types
- `apparent_bytes_total`: sum of all of the above

This means:

- `total_bytes` is closer to deduped logical file size (what you read if you read every byte; sparse files can look huge)
- `total_allocated_bytes` is closer to `du`-style block usage for regular files (still crawl-wide, not per-mount quota semantics)
- `apparent_bytes_total` is closer to `du --apparent-size` over all entry types in the sum

### `ereport` capacity totals

`ereport` currently reports:

- `total_capacity_in_files`
- `total_capacity_in_others`

where `total_capacity_in_files` is based on matched file records and hard-link-aware byte accounting from the crawl input.

## Environment variables (quick reference)

Defaults below are the built-in values when the variable is unset—each tool uses its own defaults (see Default thread counts (per binary) above).

| Variable | Tool / context | Role |
|----------|----------------|------|
| `ECRAWL_CRAWL_THREADS` | `ecrawl` | Crawl threads (minimum 1, default 16; no fixed maximum). |
| `ECRAWL_WRITER_THREADS` | `ecrawl` | Uid-shard writer threads (default 8). |
| `ECRAWL_WRITER_QUEUE_BATCHES` | `ecrawl` | Pending record batches per writer queue when writing shards (default 64, range 4…4096). |
| `ECRAWL_UID_SHARDS` | `ecrawl` | Uid shard count, power of two (default 1024). |
| `ECRAWL_MAX_OPEN_SHARDS` | `ecrawl` | Per-writer shard file cache target, auto-capped by `RLIMIT_NOFILE` (default 1024). |
| `ECRAWL_STAT_THREADS` | `ecrawl` | Stat worker threads for batched `fstatat` (default 8; `0` disables). |
| `ECRAWL_STAT_BATCH_ENTRIES` | `ecrawl` | Names per stat batch (default 1024, range 64…65536). |
| `ECRAWL_STAT_BATCH_AFTER_RELIABLE_NONDIRS` | `ecrawl` | Trusted non-dir `d_type` entries per directory handled inline before batching (default `0` = always batch; `N` > 0 = inline prefix of `N`). |
| `ECRAWL_STAT_BATCH_MIN_OFFLOAD` | `ecrawl` | Min names in an end-of-directory stat batch before offloading to stat workers (default 32; `0` = always enqueue). |
| `ECRAWL_STAT_QUEUE_BATCHES` | `ecrawl` | Max pending stat batches (default 64, range 4…4096). |
| `ECRAWL_STAT_RANDOM_QUEUE` | `ecrawl` | `0` = FIFO stat-batch dequeue; non-zero (default `1`) = pseudo-random. |
| `ECRAWL_DONATE_CHECK_EVERY` | `ecrawl` | Donate-check period during `readdir` in `DT_DIR` pushes (default 64). |
| `ECRAWL_DONATE_CHUNK_FORCE_MAX` | `ecrawl` | Max dirs donated per queue push on force spill (default 2048). |
| `ECRAWL_FORCE_DONATE_AT` | `ecrawl` | Local stack size that triggers force donation (default 4096). |
| `ECRAWL_DONATE_ALL_BUSY_MIN_STACK` | `ecrawl` | Min local stack depth before donating when every crawl thread holds a task (default 64). |
| `ECRAWL_DONATE_ALL_BUSY_MAX_QDEPTH_MULT` | `ecrawl` | Skip “all busy” donation when `g_queue_depth ≥ crawl_threads × mult` (default 4). |
| `ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH` | `ecrawl` | Batch size for enqueueing `fstatat`-discovered subdirs to the global queue (default 48). |
| `ECRAWL_STALL_HINT_SECONDS` | `ecrawl` | Stderr hint when the rolling `window_entries` stays at 0 for N consecutive seconds after warmup (default `5`; `0` = off). |
| `ECRAWL_REPAIR_THREADS` | `ecrawl_repair` | Parallel shard rescans, tail salvage `truncate`, checkpoint rebuild (default 16, minimum 1). |
| `ECRAWL_ANALYZE_THREADS` | `ecrawl_analyze` | Parallel shard scan for stats only (default 16, minimum 1, maximum 4096). If unset, `ECRAWL_REPAIR_THREADS` is used when set. |
| `EDELETE_THREADS` | `edelete` | Parallel walk workers (default 16, minimum 1). |
| `EDELETE_MAX_UNLINK_INFLIGHT` | `edelete` `--delete` | Max concurrent `unlink` syscalls across all workers (default 256; `0` = unlimited). |
| `EREPORT_THREADS` | `ereport` | Parallel `.bin` chunk readers, parallel `bucket_*.html` emission, and stats thread (default 32). |
| `EREPORT_INDEX_THREADS` | `ereport_index --make` / `--search` | Parallel chunk-boundary mapping, index parse workers, and (for `--search`) parallel postings load + path filtering when the query and candidate set are large enough (default 32). Does not set merge worker count. Trigram temp writers default to this count unless `EREPORT_INDEX_TRIGRAM_THREADS` is set. |
| `EREPORT_INDEX_TRIGRAM_THREADS` | `ereport_index --make` | Parallel writers to `tmp_trigrams_*.bin` (default: same as `EREPORT_INDEX_THREADS` when unset). |
| `EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH` | `ereport_index --make` | Bounded queue between paths writer and trigram workers (default scales with trigram thread count; range 512…262144). |
| `EREPORT_INDEX_WRITE_BATCH_PATHS` | `ereport_index --make` | Base paths-per-batch to the writer (default 4096, range 512…65536; scaled when thread count is high). |
| `EREPORT_INDEX_WRITEQ_MAX_BATCHES` | `ereport_index --make` | Max depth of batches waiting on the paths writer (default scales with thread count). |
| `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS` | `ereport_index --make` | Per-worker LRU cap on `tmp_trigrams_*` shard `FILE*` handles (32…4096; default 4096). Use a high `ulimit -n` for large `--make` (see README). |
| `EREPORT_INDEX_MERGE_MEMORY_MB` | `ereport_index --make` / merge / resume-merge | Explicit merge RAM budget (MiB) for limiting parallel merge workers (optional). |
| `EREPORT_INDEX_MERGE_RAM_FRAC` | `ereport_index --make` / merge / resume-merge | Fraction of `min(MemAvailable, cgroup memory.max)` used as that budget (default 0.55). |
| `EREPORT_INDEX_BIN` | `eserve.py` | Absolute path to `ereport_index` if not on `PATH` / next to `eserve.py`. |
| `EREPORT_SEARCH_INDEX_DIR` | `eserve.py` | Trigram index directory (`tri_keys.bin`). Overridden by `--index-dir`. |

## Source layout

- `edelete.c` — standalone parallel walker / deletion utility (`path_canon.h` only).
- `ecrawl_analyze.c` — read-only `uid_shard_*.bin` analyzer (parent and depth histograms); links `crawl_bin_chunks.o` for shared chunk parsing.
- `crawl_bin_format.h` — magic, format version, `bin_file_header_t`, `bin_record_hdr_t`, `bin_dir_catalog_entry_t` (immediate-child aggregates).
- `crawl_bin_catalog.h` / `crawl_bin_catalog.c` — load catalog tails (`crawl_bin_catalog_load()`, path helpers).
- `crawl_bin_chunks.h` / `crawl_bin_chunks.c` — checkpoint-driven chunk boundaries (`crawl_bin_load_ckpt()`, `crawl_bin_build_chunks_for_file()`).
- `crawl_ckpt.h` — shared on-disk checkpoint layout for `uid_shard_*.bin.ckpt` sidecars; included by `ecrawl`, `ereport`, `ereport_index`, `ecrawl_repair`, and `ecrawl_analyze`.
- HTML emitters in `ereport.c` follow a common argument order where practical: output path / `FILE*` target first, then `username`, `all_users`, `distinct_uids`, `basis_str`, then function-specific fields (e.g. age/size bucket indices, detail levels).

## Notes

- The code assumes local filesystem crawl data in `ERCBIN05` / format version 5 (nonzero `catalog_offset` and trailing catalog). Per-shard `uid_shard_*.bin.ckpt` sidecars still record sparse byte offsets within the record region for parallel chunk mapping in `ereport` / `ereport_index --make`. Use `ecrawl_repair` to regenerate missing sidecars without re-crawling, and optionally `truncate` shards whose last record was cut off mid-write.
- `uid_shard_*.bin` layout is preferred and automatically detected via `crawl_manifest.txt`.
- For per-user runs, `ereport` and `ereport_index --make` read only the uid-shard files relevant to that user when uid-sharded input is available. All-users runs load every shard file (same as merging full-cluster crawls).
- `ECRAWL_UID_SHARDS` for a crawl run should match across every output directory you later pass together to `ereport` / `ereport_index --make` (merged reports assume consistent shard layout).
- The `Makefile` targets `serve` and `serve-public` run `$(PYTHON3) eserve.py`: they forward `SERVE_ROOT`, `SERVE_PORT`, and optionally `SERVE_INDEX_DIR`; `SERVE_BIND` applies only to `serve` (`serve-public` binds `0.0.0.0` in the recipe).

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE). Copyright is held by Michel Erb (2026).
