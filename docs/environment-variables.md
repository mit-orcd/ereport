# Environment variables

Every tuning knob, in one place. Each binary reads its own variables; there is no global thread count. Defaults are the built-in values when a variable is unset.

## Thread counts

| Program | Variable | Default | What it parallelizes |
|---------|----------|---------|----------------------|
| `ecrawl` | `ECRAWL_CRAWL_THREADS` | 16 | directory walk + inline `fstatat` (minimum 1) |
| `ecrawl` | `ECRAWL_WRITER_THREADS` | 8 | flushing uid-sharded `.bin` output |
| `ecrawl` | `ECRAWL_ID_RESOLVE_THREADS` | 16 | uid/gid name lookups at the end of the run (1 = serial) |
| `ecrawl_query` | `ECRAWL_QUERY_THREADS` | 16 | shard scan (maximum 4096) |
| `ecrawl_mount` | `ECRAWL_MOUNT_THREADS` | 32 | in-memory index build at mount time; `-o threads=N` overrides (1…4096) |
| `edelete` | `EDELETE_THREADS` | 16 | directory walk |
| `edelete` | `EDELETE_MAX_UNLINK_INFLIGHT` | 256 | concurrent `unlink` calls in `--delete` (`0` = unlimited) |
| `edump` | `EDUMP_WRITERS` | 8 | file recreation; `--writers N` overrides (1…4096) |
| `ereport` | `EREPORT_THREADS` | 32 | `.bin` parsing and `bucket_*.html` emission |
| `ereport_index` | `EREPORT_INDEX_THREADS` | 32 | `--make` parse workers; `--search` postings load |
| `ereport_index` | `EREPORT_INDEX_TRIGRAM_THREADS` | = `EREPORT_INDEX_THREADS` | `tmp_trigrams_*.bin` writers |
| `ereport_index` | `EREPORT_INDEX_MERGE_WORKERS` | 16 | concurrent merge workers; RAM admission may run fewer |
| `ereport_index` | `EREPORT_INDEX_MERGE_SORT_THREADS` | 1 | threads per within-bucket sort; only helps when the merge is CPU-bound |

Rough floors for the defaults on an idle host: 4 CPUs / 4 GiB for `ecrawl`, `ecrawl_query`, `edelete`, `edump`; 8 CPUs / 8 GiB for `ereport` and `ecrawl_mount`; 16 CPUs / 16 GiB for `ereport_index`. Large `ereport_index --make` merges can need tens to hundreds of GiB; the merge budget tracks `MemAvailable`.

## `ecrawl`

| Variable | Default | Role |
|----------|---------|------|
| `ECRAWL_UID_SHARDS` | 512 | uid shard count (power of two) |
| `ECRAWL_MAX_OPEN_SHARDS` | 64 | per-writer open shard-file cache, capped by `RLIMIT_NOFILE` |
| `ECRAWL_WRITER_QUEUE_BATCHES` | 64 | pending record batches per writer queue (4…4096) |
| `ECRAWL_ZSTD_LEVEL` | 3 | zstd level for record blocks and the catalog (1…22) |
| `ECRAWL_GETDENTS_BUF` | 1 MiB | raw `getdents64` buffer (4 KiB…64 MiB); `0` = use libc `readdir` |
| `ECRAWL_IOURING_DEPTH` | 256 | `--iouring`: SQEs per directory batch (16…4096) |
| `ECRAWL_IOURING_MIN_BATCH` | 8 | `--iouring`: directories with fewer names are `statx`'d synchronously (1…depth) |
| `ECRAWL_STALL_HINT_SECONDS` | 5 | stderr hint after N seconds with zero progress (`0` = off) |
| `ECRAWL_DONATE_CHECK_EVERY` | 64 | work-stealing: donate check period per `DT_DIR` push |
| `ECRAWL_DONATE_ENTRY_CHECK_EVERY` | 4096 | work-stealing: donate check period per dirent (`0` = off) |
| `ECRAWL_DONATE_CHUNK_FORCE_MAX` | 2048 | work-stealing: max dirs donated per forced spill |
| `ECRAWL_FORCE_DONATE_AT` | 4096 | work-stealing: local stack size that forces a spill |
| `ECRAWL_DONATE_ALL_BUSY_MIN_STACK` | 64 | work-stealing: min local depth before donating when all threads are busy |
| `ECRAWL_DONATE_ALL_BUSY_MAX_QDEPTH_MULT` | 4 | work-stealing: skip donation when queue depth ≥ threads × mult |
| `ECRAWL_DISCOVERED_DIR_ENQUEUE_BATCH` | 48 | subdirs per global-queue push |

The `DONATE`/`ENQUEUE` knobs are for pathological trees (one huge directory, a very deep chain); the defaults are right for ordinary filesystems.

## `ecrawl_query`

| Variable | Default | Role |
|----------|---------|------|
| `ECRAWL_QUERY_CHUNK_BYTES` | sized from capture bytes ÷ threads, ≤ 4 MiB | bytes per parse job (minimum 4096) |
| `ECRAWL_QUERY_BLOCK_SKIP` | on | `0` disables zone-map row-group skipping (parity testing only; results never change) |

## `edelete`

| Variable | Default | Role |
|----------|---------|------|
| `EDELETE_FANOUT_MIN_BYTES` | 64 MiB | files at least this large are unlinked via the work queue instead of inline (`0` = off) |

## `ereport`

| Variable | Default | Role |
|----------|---------|------|
| `EREPORT_BUCKET_CELL_CONCURRENCY` | auto | how many of the 36 age×size pages are built at once (1…1024) |
| `EREPORT_HEAT_CTIME_LED_MIN_SHARE` | 0.30 | min share (0…1] of a cell's bytes that must be ctime-led before the heat map badges it |
| `EREPORT_MEMSTATS` | unset | set to print a memory breakdown after parsing |
| `EREPORT_ALLOC_TUNE` | on | `0` skips the glibc `mallopt` tuning (all C binaries) |

## `ereport_index`

| Variable | Default | Role |
|----------|---------|------|
| `EREPORT_INDEX_ZSTD_LEVEL` | 3 | zstd level for `tri_keys.bin` / `paths.bin` |
| `EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH` | scales with threads | queue between paths writer and trigram workers (512…262144) |
| `EREPORT_INDEX_TRIGRAM_FRAME_BYTES` | 64 KiB | source bytes per `tmp_trigrams_*.bin` frame (4 KiB…16 MiB); one frame is buffered per open worker×bucket shard |
| `EREPORT_INDEX_WRITE_BATCH_PATHS` | 4096 | paths per batch to the writer (512…65536) |
| `EREPORT_INDEX_WRITEQ_MAX_BATCHES` | scales with threads | max batches waiting on the paths writer |
| `EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS` | 4096 | per-worker LRU cap on open `tmp_trigrams_*` handles (32…4096); raise `ulimit -n` for big builds |
| `EREPORT_INDEX_MERGE_MEMORY_MB` | from RAM | explicit merge RAM budget (MiB) |
| `EREPORT_INDEX_MERGE_RAM_FRAC` | 0.55 | fraction of `min(MemAvailable, cgroup limit)` used as that budget |
| `EREPORT_INDEX_MERGE_SORT_SLICE_MB` | 2048 | max aux bytes per radix-sort slice for oversized buckets (≥ 64) |
| `EREPORT_INDEX_MEMSTATS` | unset | set to print RSS / catalog memory after `--make` |

## `eserve.py`

| Variable | Role |
|----------|------|
| `EREPORT_INDEX_BIN` | path to `ereport_index` if not on `PATH` or next to `eserve.py` |
| `EREPORT_SEARCH_INDEX_DIR` | trigram index directory; `--index-dir` overrides |
