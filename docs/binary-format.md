# Crawl shard binary format

Each `uid_shard_*.bin` file uses this layout.

What changed (high level): Current shards are format version 5 (`ERCBIN05`), replacing older `ERCBIN03` / version 3 files. Version 5 adds a `catalog_offset` field to the fixed header and appends a directory catalog after the record stream. Each catalog row carries immediate-child byte and time aggregates for that directory within the shard (child subtree totals are not rolled into parents on disk). `ecrawl` writes `catalog_offset == 0` until the shard is finalized; `ereport`, `ereport_index --make`, and `ecrawl_analyze` paths that load the catalog require a nonzero, in-range `catalog_offset` and a parseable catalog blob.

File header (32 bytes, packed): `magic[8]` (`ERCBIN05`), `version` (`uint32_t`, must match 5), `reserved` `uint32_t`, `catalog_offset` `uint64_t` (byte offset from BOF to the catalog blob), `reserved64` `uint64_t`.

Record region: `[sizeof(header), catalog_offset)` on finalized shards — a concatenation of variable-length records. Each record starts with `bin_record_hdr_t` (`parent_dir_id`, `name_len`, `type`, `mode`, `uid`, `gid`, `size`, `inode`, `dev_major`, `dev_minor`, `nlink`, `atime`, `mtime`, `ctime`) followed by `name_len` bytes of UTF-8 for a single path component (see `crawl_bin_format.h`).

Catalog tail: `[catalog_offset, EOF)` — begins with `uint64_t n_entries`, then `n_entries` packed `bin_dir_catalog_entry_t` rows (`dir_id`, `parent_dir_id`, `depth`, `name_len`, reserved padding, then `imm_child_bytes`, `imm_child_count`, `imm_child_ctime_led_count`, `imm_child_min_eff_time`, `imm_child_max_eff_time`). `imm_child_*` fields sum only records whose on-disk `parent_dir_id` equals this row’s `dir_id` (scoped to the shard). `imm_child_bytes` follows `ecrawl` accounting (regular files: hardlink-aware credit; other types: apparent `st_size`). `imm_child_min_eff_time` / `imm_child_max_eff_time` use `max(atime, mtime, ctime)` per child; `imm_child_min_eff_time` is `UINT64_MAX` when `imm_child_count == 0`. `imm_child_ctime_led_count` uses the same “ctime-led” rule as `ereport` (`ctime` strictly greater than `max(atime, mtime)` and at least 180 days newer). Each row ends with `name_len` UTF-8 bytes (directory component only; root uses `name_len == 0`).

`catalog_offset`: On a completed crawl `ecrawl` sets this to the first byte of the catalog (≥ header size, ≤ file size). `catalog_offset == 0` means the shard was never finalized (still writing or interrupted).

Incomplete / invalid shards: `ereport` and `ereport_index` reject a shard when `catalog_offset == 0`, `catalog_offset` is out of range, magic/version mismatch, or `crawl_bin_catalog_load()` fails (truncated catalog, bogus counts, etc.). Chunk mapping in `crawl_bin_chunks` caps the record region at `catalog_offset` when it is nonzero; when it is zero, loaders may still align checkpoints against EOF for diagnostics, but report/index consumers treat the shard as unusable until `ecrawl_repair` (or a fresh crawl) fixes it. `ecrawl_repair` behavior for bad tails and `corrupt_shards/` quarantine is described under [`ecrawl_repair`](tools.md#ecrawl_repair).

## Checkpoint sidecars (`*.bin.ckpt`)

While crawling, `ecrawl` records record-aligned byte offsets at a fixed stride into `uid_shard_*.bin.ckpt`. `ereport` and `ereport_index` load those offsets to split each shard into valid segments without a preliminary full-file scan to find boundaries. That enables many threads to work on different byte ranges of the same file safely (no record torn across workers). Checkpoint offsets apply only to the record region (from just after the file header up to `catalog_offset` on finalized shards). If sidecars are missing or stale (for example an interrupted crawl), run [`ecrawl_repair`](tools.md#ecrawl_repair) on the crawl output directory to rebuild them—and to truncate an incomplete last record when possible.

## Operational notes

- The code assumes local filesystem crawl data in `ERCBIN05` / format version 5 (nonzero `catalog_offset` and trailing catalog). Per-shard `uid_shard_*.bin.ckpt` sidecars still record sparse byte offsets within the record region for parallel chunk mapping in `ereport` / `ereport_index --make`. Use `ecrawl_repair` to regenerate missing sidecars without re-crawling, and optionally `truncate` shards whose last record was cut off mid-write.
- `uid_shard_*.bin` layout is preferred and automatically detected via `crawl_manifest.txt`.
- For per-user runs, `ereport` and `ereport_index --make` read only the uid-shard files relevant to that user when uid-sharded input is available. All-users runs load every shard file (same as merging full-cluster crawls).
- `ECRAWL_UID_SHARDS` for a crawl run should match across every output directory you later pass together to `ereport` / `ereport_index --make` (merged reports assume consistent shard layout).
