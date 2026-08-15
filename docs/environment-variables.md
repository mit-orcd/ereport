# Environment variables and thread defaults

Every tuning knob in one place, plus the per-binary default thread counts. Each variable is also described in context under the relevant tool in [tools.md](tools.md).

## Default thread counts (per binary)

Each program has its own defaults. There is no single global “thread count” for the whole toolchain: `ecrawl`, `ereport`, and `ereport_index` read different environment variables and use different built-in numbers when those variables are unset.

The Min logical CPUs and Min RAM columns below are practical floors for running with default thread counts on a mostly idle machine: enough CPUs that the default parallelism isn't badly oversubscribed, and enough RAM for modest workloads. They are not guarantees for huge trees — reduce the thread-count env vars on smaller hosts, and expect large `ereport_index --make` merges to need far more (tens to hundreds of GiB peak; the merge budget tracks host/cgroup `MemAvailable`).

| Program | Parallelism role | Override (env) | Built-in default | Min logical CPUs | Min RAM |
|---------|------------------|----------------|------------------|------------------|---------|
| `ecrawl` | Walk / queue directory work, inline `fstatat` | `ECRAWL_CRAWL_THREADS` | 16 crawl threads (minimum 1; no fixed maximum) | 4 | 4 GiB |
| `ecrawl` | Flush uid-sharded `.bin` output | `ECRAWL_WRITER_THREADS` | 8 writer threads | 4 | 4 GiB |
| `ecrawl_repair` | Parallel rescans; optional `truncate` on incomplete tail; checkpoint rebuild / verify | `ECRAWL_REPAIR_THREADS` | 16 | 4 | 4 GiB |
| `ecrawl_query` | Parallel shard scan for stats only (no writes) | `ECRAWL_QUERY_THREADS` | 16 (maximum 4096) | 4 | 4 GiB |
| `ecrawl_mount` | Build the in-memory namespace index at mount time (catalog merge, parallel record scan, per-directory sort) | `ECRAWL_MOUNT_THREADS` (or `-o threads=N`) | 32 (maximum 4096) | 8 | 8 GiB |
| `edelete` | Parallel directory walk; optional `unlink` (bounded concurrency in `--delete`) | `EDELETE_THREADS`, `EDELETE_MAX_UNLINK_INFLIGHT` | 16 threads; 256 max concurrent `unlink` (`0` = unlimited) | 4 | 4 GiB |
| `ereport` | Map/parse `.bin` chunks, emit up to 36 `bucket_*.html` files, live stderr stats | `EREPORT_THREADS` | 32 | 8 | 8 GiB |
| `ereport_index` | `--make`: parallel chunk-boundary scan, parse workers; trigram temp writers default to the same count unless `EREPORT_INDEX_TRIGRAM_THREADS` is set. `--search`: parallel postings load and path filtering when the query and candidate set are large enough | `EREPORT_INDEX_THREADS` (and optionally `EREPORT_INDEX_TRIGRAM_THREADS`) | 32 | 16 | 16 GiB |

Not controlled by those knobs: `ereport_index --make` merge workers (cap 16, chosen from RAM budget), and `--resume-merge` merge workers—see the `EREPORT_INDEX_MERGE_` vars in the table below.

## Quick reference

Defaults below are the built-in values when the variable is unset—each tool uses its own defaults (see the table above).

| Variable | Tool / context | Role |
|----------|----------------|------|
| `ECRAWL_CRAWL_THREADS` | `ecrawl` | Crawl threads (minimum 1, default 16; no fixed maximum). |
| `ECRAWL_WRITER_THREADS` | `ecrawl` | Uid-shard writer threads (default 8). |
| `ECRAWL_WRITER_QUEUE_BATCHES` | `ecrawl` | Pending record batches per writer queue when writing shards (default 64, range 4…4096). |
| `ECRAWL_UID_SHARDS` | `ecrawl` | Uid shard count, power of two (default 512). |
| `ECRAWL_MAX_OPEN_SHARDS` | `ecrawl` | Per-writer shard file cache target, auto-capped by `RLIMIT_NOFILE` (default 64). |
| `ECRAWL_DONATE_CHECK_EVERY` | `ecrawl` | Donate-check period during `readdir` in `DT_DIR` pushes (default 64). |
| `ECRAWL_DONATE_ENTRY_CHECK_EVERY` | `ecrawl` | Donate-check period during `readdir` in dirents, so a deep chain that pushes one subdirectory per directory still sheds work to idle peers (default 4096; `0` disables). |
| `ECRAWL_DONATE_CHUNK_FORCE_MAX` | `ecrawl` | Max dirs donated per queue push on force spill (default 2048). |
| `ECRAWL_FORCE_DONATE_AT` | `ecrawl` | Local stack size that triggers force donation (default 4096). |
| `ECRAWL_DONATE_ALL_BUSY_MIN_STACK` | `ecrawl` | Min local stack depth before donating when every crawl thread holds a task (default 64). |
| `ECRAWL_DONATE_ALL_BUSY_MAX_QDEPTH_MULT` | `ecrawl` | Skip “all busy” donation when `g_queue_depth ≥ crawl_threads × mult` (default 4). |
| `ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH` | `ecrawl` | Batch size for enqueueing `fstatat`-discovered subdirs to the global queue (default 48). |
| `ECRAWL_STALL_HINT_SECONDS` | `ecrawl` | Stderr hint when the rolling `window_entries` stays at 0 for N consecutive seconds after warmup (default `5`; `0` = off). |
| `ECRAWL_REPAIR_THREADS` | `ecrawl_repair` | Parallel shard rescans, tail salvage `truncate`, checkpoint rebuild (default 16, minimum 1). |
| `ECRAWL_QUERY_THREADS` | `ecrawl_query` | Parallel shard scan for stats only (default 16, minimum 1, maximum 4096). |
| `ECRAWL_MOUNT_THREADS` | `ecrawl_mount` | Index build threads: parallel record scan, scatter, and per-directory name sorts (default 32, range 1…4096). `-o threads=N` overrides it. Does not affect the FUSE event loop, which libfuse sizes itself (`-s` forces single-threaded). |
| `EDELETE_THREADS` | `edelete` | Parallel walk workers (default 16, minimum 1). |
| `EDELETE_MAX_UNLINK_INFLIGHT` | `edelete` `--delete` | Max concurrent `unlink` syscalls across all workers (default 256; `0` = unlimited). |
| `EREPORT_THREADS` | `ereport` | Parallel `.bin` chunk readers, parallel `bucket_*.html` emission, and stats thread (default 32). |
| `EREPORT_INDEX_THREADS` | `ereport_index --make` / `--search` | Parallel chunk-boundary mapping, index parse workers, and (for `--search`) parallel postings load + path filtering when the query and candidate set are large enough (default 32). Does not set merge worker count. Trigram temp writers default to this count unless `EREPORT_INDEX_TRIGRAM_THREADS` is set. |
| `EREPORT_INDEX_TRIGRAM_THREADS` | `ereport_index --make` | Parallel writers to `tmp_trigrams_*.bin` (default: same as `EREPORT_INDEX_THREADS` when unset). |
| `EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH` | `ereport_index --make` | Bounded queue between paths writer and trigram workers (default scales with trigram thread count; range 512…262144). |
| `EREPORT_INDEX_TRIGRAM_FRAME_BYTES` | `ereport_index --make` | Source bytes per frame written to `tmp_trigrams_*.bin`, rounded down to whole records (default 65536, range 4096…16777216). One frame is buffered per *open* (worker × bucket) shard, so this multiplies with `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS`: larger frames mean fewer, better-compressing writes and fewer reads at merge, at more resident buffer. |
| `EREPORT_INDEX_WRITE_BATCH_PATHS` | `ereport_index --make` | Base paths-per-batch to the writer (default 4096, range 512…65536; scaled when thread count is high). |
| `EREPORT_INDEX_WRITEQ_MAX_BATCHES` | `ereport_index --make` | Max depth of batches waiting on the paths writer (default scales with thread count). |
| `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS` | `ereport_index --make` | Per-worker LRU cap on `tmp_trigrams_*` shard `FILE*` handles (32…4096; default 4096). Use a high `ulimit -n` for large `--make`. |
| `EREPORT_INDEX_MERGE_MEMORY_MB` | `ereport_index --make` / merge / resume-merge | Explicit merge RAM budget (MiB) for limiting parallel merge workers (optional). |
| `EREPORT_INDEX_MERGE_RAM_FRAC` | `ereport_index --make` / merge / resume-merge | Fraction of `min(MemAvailable, cgroup memory.max)` used as that budget (default 0.55). |
| `EREPORT_INDEX_MERGE_WORKERS` | `ereport_index --make` / merge / resume-merge | Cap on concurrent merge workers (1…4096; default 16, and never more than online CPUs or nonempty buckets). RAM admission still decides how many run at once, so raising this only helps when the budget has room. Measured on 8M paths in one directory (34 nonempty buckets, 64 CPUs), `merge_phase_sec` improved from 4.31 s to 2.87 s between 8 and 16 workers and then stopped moving: past 16 the merge is bound by per-bucket decode and sort, not pool size. |
| `EREPORT_INDEX_MERGE_SORT_THREADS` | `ereport_index --make` / merge / resume-merge | Per-bucket cap on threads for the within-bucket parallel sort (1…4096; default 1 = serial). Worth setting only when the merge is CPU-bound rather than temp-read-bound: on local NVMe it cut `merge_phase_sec` from 0.70 s to 0.33 s at 16, but on a 953M-path build it made the merge slower. |
| `EREPORT_INDEX_BIN` | `eserve.py` | Absolute path to `ereport_index` if not on `PATH` / next to `eserve.py`. |
| `EREPORT_SEARCH_INDEX_DIR` | `eserve.py` | Trigram index directory (`tri_keys.bin`). Overridden by `--index-dir`. |
