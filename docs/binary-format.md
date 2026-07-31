# Crawl shard binary format

Each `uid_shard_*.bin` file uses this layout. The authoritative definition is [`crawl_bin_format.h`](../crawl_bin_format.h); this page explains it.

## Version history

Current shards are format version 7 (`ERCBIN07`).

- v5 (`ERCBIN05`) added `catalog_offset` to the fixed header and appended a directory catalog after the record stream. Records were stored raw, and the record header still carried `mode` and `gid`.
- v6 compressed the record region: it became a sequence of independently zstd-compressed blocks instead of raw records. The record header was also trimmed to the fields readers actually consume, **dropping `mode` and `gid`** along with an unused pad byte.
- v7 added a summary of each block's contents to that block's own header, so a reader can prove a block cannot match a size or type predicate and skip the frame without decompressing it.

Readers require an exact magic and version match, so older shards must be re-crawled rather than upgraded.

Because `mode` and `gid` are no longer stored, nothing downstream can report real permissions or groups. That is why [`ecrawl_mount`](tools.md#ecrawl_mount) has to synthesize them.

## File header (32 bytes, packed)

`magic[8]` (`ERCBIN07`), `version` (`uint32_t`, must equal 7), `reserved` `uint32_t`, `catalog_offset` `uint64_t` (byte offset from BOF to the catalog blob), `reserved64` `uint64_t`.

`catalog_offset` is the finalization marker. On a completed crawl `ecrawl` sets it to the first byte of the catalog (≥ header size, ≤ file size). `catalog_offset == 0` means the shard was never finalized — still being written, or interrupted — and readers must reject it.

## Record region: `[sizeof(header), catalog_offset)`

A back-to-back sequence of compressed blocks. Each block is a 24-byte `bin_block_hdr_t` followed by `comp_size` bytes holding a zstd frame that decompresses to `raw_size` bytes:

| Field | Type | Meaning |
|---|---|---|
| `raw_size` | `uint32_t` | decompressed payload size |
| `comp_size` | `uint32_t` | compressed frame size on disk |
| `max_record_size` | `uint64_t` | largest `bin_record_hdr_t.size` in this block |
| `record_count` | `uint32_t` | records in this block |
| `type_mask` | `uint16_t` | OR of `crawl_bin_type_bit()` over this block |
| `reserved16` | `uint16_t` | — |

The decompressed payload is a concatenation of whole records; a record never spans a block boundary. Blocks are self-describing and contiguous, so a reader walks them with header reads alone, needing no side index, and every chunk boundary handed to a parallel worker is a block boundary. `ecrawl` flushes a block once about 256 KiB of uncompressed records have accumulated (`CRAWL_BIN_BLOCK_RAW_TARGET`).

The v7 summary fields exist to let a selective query skip work. `max_record_size` and `type_mask` must cover *every* record in the block, since understating either would silently drop query results; `record_count` lets a skipping reader still report an accurate scanned-record total. They are stored inline rather than in a sidecar precisely so they cannot go stale relative to the frame they describe. Overhead is about 0.006% of a 256 KiB block. See `crawl_bin_block_reader_set_filter()` in [`crawl_bin_block.h`](../crawl_bin_block.h), and the `ecrawl_analyze` block-skip parity checks in `test.sh`.

### Record header (75 bytes, packed)

`parent_dir_id` `uint64_t`, `name_len` `uint16_t`, `type` `uint8_t`, `uid` `uint64_t`, `size` `uint64_t`, `inode` `uint64_t`, `dev_major` `uint32_t`, `dev_minor` `uint32_t`, `nlink` `uint64_t`, `atime` `uint64_t`, `mtime` `uint64_t`, `ctime` `uint64_t` — followed by `name_len` bytes of UTF-8 for a single path component (no slashes).

`type` is a `find(1)`-style letter from `fdlcbpso`. Full paths are not stored: they are reconstructed by walking `parent_dir_id` through the catalog. There is no `mode` and no `gid`; readers derive the file kind from `type`, and the hardlink-aware byte credit uses `inode`, `dev_major`/`dev_minor`, and `nlink`.

## Catalog tail: `[catalog_offset, EOF)`

Begins with `uint64_t n_entries`, then `n_entries` packed `bin_dir_catalog_entry_t` rows: `dir_id`, `parent_dir_id`, `depth`, `name_len`, reserved padding, then `imm_child_bytes`, `imm_child_count`, `imm_child_ctime_led_count`, `imm_child_min_eff_time`, `imm_child_max_eff_time`. Each row ends with `name_len` UTF-8 bytes (directory component only).

`dir_id` is 1-based and unique per shard; `parent_dir_id == 0` only for the synthetic root row (`dir_id == 1`). A directory gets a row when it is the recorded parent of at least one entry, so a directory with no children of its own may have no row. Directory identity is per shard, so the same logical path has different `dir_id`s in different shards — merging them into one namespace is the bulk of the work in [`ecrawl_mount`](tools.md#ecrawl_mount).

The `imm_child_*` aggregates sum only records whose on-disk `parent_dir_id` equals this row's `dir_id`, scoped to this shard; child directories' rollups are not propagated up (deliberately — readers do that if they need it). `imm_child_bytes` follows `ecrawl` accounting (regular files: hardlink-aware credit; other types: apparent `st_size`). `imm_child_min_eff_time` / `imm_child_max_eff_time` use per-record effective time `max(atime, mtime, ctime)`, and `imm_child_min_eff_time` is `UINT64_MAX` when `imm_child_count == 0`. `imm_child_ctime_led_count` uses the same “ctime-led” rule as `ereport`: `ctime` strictly greater than `max(atime, mtime)` and at least 180 days newer.

## Incomplete / invalid shards

`ereport` and `ereport_index` reject a shard when `catalog_offset == 0`, `catalog_offset` is out of range, magic/version mismatch, or `crawl_bin_catalog_load()` fails (truncated catalog, bogus counts). Chunk mapping in `crawl_bin_chunks` caps the record region at `catalog_offset` when nonzero; when it is zero, loaders may still align checkpoints against EOF for diagnostics, but report and index consumers treat the shard as unusable until `ecrawl_repair` (or a fresh crawl) fixes it. `ecrawl_mount` skips unfinalized shards and reports how many it skipped. Bad-tail handling and `corrupt_shards/` quarantine are described under [`ecrawl_repair`](tools.md#ecrawl_repair).

## Checkpoint sidecars (`*.bin.ckpt`)

While crawling, `ecrawl` records block-aligned byte offsets at a fixed stride into `uid_shard_*.bin.ckpt`. Readers load those offsets to split each shard into valid segments without a preliminary full-file scan for boundaries, which lets many threads work on different byte ranges of the same file safely with no record torn across workers. Checkpoint offsets apply only to the record region (from just after the file header up to `catalog_offset` on finalized shards). If sidecars are missing or stale — for example after an interrupted crawl — run [`ecrawl_repair`](tools.md#ecrawl_repair) on the crawl output directory to rebuild them, and to truncate an incomplete last record when possible.

## Operational notes

- The code assumes local filesystem crawl data in `ERCBIN07` / format version 7 (nonzero `catalog_offset` and a trailing catalog). Use `ecrawl_repair` to regenerate missing sidecars without re-crawling.
- `uid_shard_*.bin` layout is preferred and automatically detected via `crawl_manifest.txt`.
- Shards are assigned by `uid & (uid_shards - 1)`, so one directory's children scatter across many shard files whenever its entries have different owners.
- For per-user runs, `ereport` and `ereport_index --make` read only the uid-shard files relevant to that user when uid-sharded input is available. All-users runs load every shard file, as do merged full-cluster crawls.
- `ECRAWL_UID_SHARDS` for a crawl run should match across every output directory you later pass together to `ereport` / `ereport_index --make`; merged reports assume a consistent shard layout.
