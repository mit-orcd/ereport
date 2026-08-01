# Crawl shard binary format

Each `uid_shard_*.bin` file uses this layout. The authoritative definition is [`crawl_bin_format.h`](../crawl_bin_format.h); this page explains it.

## Version

Shards are format version 8, magic `ERCBIN08`. Readers require an exact magic and version match and there is no upgrade path: a capture written by an older build must be re-crawled. Nothing in the tree reads an earlier version, so this page describes v8 only.

## File header (32 bytes, packed)

`magic[8]` (`ERCBIN08`), `version` (`uint32_t`, must equal 8), `reserved` `uint32_t`, `catalog_offset` `uint64_t` (byte offset from BOF to the catalog blob), `reserved64` `uint64_t`.

`catalog_offset` is the finalization marker. On a completed crawl `ecrawl` sets it to the first byte of the catalog (≥ header size, ≤ file size). `catalog_offset == 0` means the shard was never finalized — still being written, or interrupted — and readers must reject it.

## Record region: `[sizeof(header), catalog_offset)`

A back-to-back sequence of **row groups**. Each is laid out as

```
bin_rowgroup_hdr_t
bin_colchunk_hdr_t[column_count]        the column directory
<column 0 payload> … <column n-1 payload>
```

with the payloads in directory order, so a reader that wants two columns reads the header plus directory and then seeks straight to those two payloads. Row groups are self-describing and contiguous — `crawl_bin_rowgroup_total_bytes()` gives the stride to the next group from the header alone — so a reader walks them with header reads and no side index, and every chunk boundary handed to a parallel worker is a group boundary. `ecrawl` flushes a group at about 1 MiB of uncompressed records or 65536 records, whichever comes first (`CRAWL_BIN_ROWGROUP_RAW_TARGET`, `CRAWL_BIN_ROWGROUP_MAX_RECORDS`). The 1 MiB target is what gives the codecs enough runway to pay off: a few thousand records per group is too short for run-length and frame-of-reference encoding to matter.

`bin_rowgroup_hdr_t` (32 bytes): `record_count` `uint32_t`, `column_count` `uint32_t`, `comp_bytes` `uint64_t` (payload bytes after the directory), `raw_bytes` `uint64_t`, `type_mask` `uint16_t` (OR of `crawl_bin_type_bit()` over the group), then reserved.

`bin_colchunk_hdr_t` (32 bytes): `column_id` `uint8_t`, `encoding` `uint8_t`, `bit_width` `uint8_t`, reserved, `comp_bytes` `uint32_t`, `raw_bytes` `uint64_t`, `min_value` `uint64_t`, `max_value` `uint64_t`.

### Columns and encodings

The fifteen column ids are on-disk identifiers and are never renumbered, only appended: `parent_dir_id`, `name_len`, `type`, `uid`, `gid`, `mode`, `size`, `inode`, `dev_major`, `dev_minor`, `nlink`, `atime`, `mtime`, `ctime`, `name_bytes`. All except `name_bytes` are `uint64_t` value columns; `name_bytes` is the concatenated UTF-8 component names, sliced by the `name_len` column. `type` is a `find(1)`-style letter from `fdlcbpso`. Full paths are still not stored — they are reconstructed by walking `parent_dir_id` through the catalog.

Every chunk is encoded and then zstd-compressed; the encoding says what the frame decompresses to. The writer picks whichever is smallest for the data it just buffered:

| Encoding | Payload | Typical column |
|---|---|---|
| `CRAWL_ENC_CONST` | empty; every value is `min_value` | `uid` within a uid shard, `dev_*` |
| `CRAWL_ENC_RLE` | `(value, run_length)` `uint64` pairs | `gid`, `mode`, `type`, `nlink` |
| `CRAWL_ENC_FOR_BITPACK` | `value - min_value` packed at `bit_width` bits | `atime`/`mtime`/`ctime`, `parent_dir_id` |
| `CRAWL_ENC_RAW` | `record_count` little-endian `uint64` | high-entropy columns such as `inode` |
| `CRAWL_ENC_BYTES` | opaque blob | `name_bytes` only |

### Zone maps and projection

`min_value` / `max_value` on every chunk form a zone map, so a reader can prove no record in a group can satisfy a range predicate and seek past the group without decompressing it. The summary must cover *every* record in the group — understating a range would silently drop query results — and it is inline rather than in a sidecar precisely so it cannot go stale relative to the data it describes. For `name_bytes`, which has no order, both are 0.

Projection is the other half: `crawl_bin_block_reader_set_projection()` decodes only the requested columns, and requesting `name_bytes` implicitly pulls in `name_len` because the lengths slice the blob. `crawl_bin_block_reader_next()` is a row-reconstruction shim that rebuilds a `bin_record_hdr_t` from the decoded columns, leaving unprojected fields zero, so a consumer can adopt projection without restructuring its loop. See [`crawl_bin_block.h`](../crawl_bin_block.h), the codec round-trip tests in `test_crawl_codec.c`, and the projection and block-skip parity checks in `test_crawl_block_filter.c` and `test.sh`.

### Record header (in memory only)

`bin_record_hdr_t` — `parent_dir_id`, `name_len`, `type`, `uid`, `gid`, `mode`, `size`, `inode`, `dev_major`, `dev_minor`, `nlink`, `atime`, `mtime`, `ctime` — is the record exchanged between the writer, the readers and the shim. Since v8 it is **not** written to disk verbatim; each field lives in its own column chunk. Hardlink-aware byte credit uses `inode`, `dev_major`/`dev_minor` and `nlink`.

## Catalog tail: `[catalog_offset, EOF)`

Begins with `uint64_t n_entries`, then `n_entries` packed `bin_dir_catalog_entry_t` rows: `dir_id`, `parent_dir_id`, `depth`, `name_len`, `flags`, then the `imm_child_*` aggregates, then the v8 `dfs_*` / `subtree_*` / `self_bytes` fields described below. Each row ends with `name_len` UTF-8 bytes (directory component only). `crawl_bin_catalog_load_sel()` lets a reader load only the groups it needs — the tree fields alone, plus `CRAWL_CAT_IMM_CHILD` and/or `CRAWL_CAT_SUBTREE` — which matters at a billion files, where the optional arrays are 40 and 72 bytes per directory. `ereport`, `ereport_index` and `ecrawl_mount` load none of them.

`dir_id` is 1-based and unique per shard; `parent_dir_id == 0` only for the synthetic root row (`dir_id == 1`). A directory gets a row when it is the recorded parent of at least one entry, so a directory with no children of its own may have no row. Directory identity is per shard, so the same logical path has different `dir_id`s in different shards — merging them into one namespace is the bulk of the work in [`ecrawl_mount`](tools.md#ecrawl_mount).

The `imm_child_*` aggregates sum only records whose on-disk `parent_dir_id` equals this row's `dir_id`, scoped to this shard. `imm_child_bytes` follows `ecrawl` accounting (regular files: hardlink-aware credit; other types: apparent `st_size`). `imm_child_min_eff_time` / `imm_child_max_eff_time` use per-record effective time `max(atime, mtime, ctime)`, and `imm_child_min_eff_time` is `UINT64_MAX` when `imm_child_count == 0`. `imm_child_ctime_led_count` uses the same “ctime-led” rule as `ereport`: `ctime` strictly greater than `max(atime, mtime)` and at least 180 days newer.

### DFS ordering and subtree rollups (v8)

A single O(directories) post-pass at the end of the crawl fills the remaining fields. It runs only on the final close, never on LRU shard eviction, because a shard can be closed and reopened many times mid-crawl.

`dir_id` is assigned in crawl arrival order and is referenced by every record's `parent_dir_id`, so it cannot be renumbered without rewriting the whole record region. `dfs_index` is therefore a *permutation* rather than a renumbering: it gives each directory its position in DFS pre-order, which makes “is X at or under D” the O(1) range test

```
dfs_index[D] <= dfs_index[X] < dfs_index[D] + dfs_subtree_dirs[D]
```

with no per-shard bitmap and without touching a single record. `ecrawl_analyze` uses it in place of the bitmap `--subtree` previously built.

`subtree_bytes` / `subtree_count` are `imm_child_bytes` / `imm_child_count` summed over that DFS range, with `subtree_files` / `subtree_dirs` / `subtree_symlinks` breaking the count down by record type. So a `--subtree` aggregate becomes a lookup: cost is O(directories) instead of O(files), and no record is read at all.

Two subtleties are worth stating plainly:

- The `subtree_*` sums cover records *under* the directory and exclude the directory's own record, which by construction hangs off its parent. `self_bytes` carries that one record's credit on the directory's own row so a query can add it back and match `du -sb`, which counts the directory it was given. The `CRAWL_DIR_FLAG_SELF_RECORD` flag says this shard actually holds that record — a shard is per-uid, so a directory owned by another user still gets a catalog row here to give its children a path, and without the flag a cross-uid subtree would count its root once per shard.
- `subtree_nlink_gt1_count` is the number of records in the subtree with `nlink > 1`. **When it is zero, `subtree_bytes` provably equals what a full scan would compute.** When it is nonzero the two can legitimately differ, because crawl-time hardlink credit is attributed to the first link visited anywhere in the tree while a scan dedups within the queried subtree. Readers must fall back to the scan rather than present the rollup as exact; `ecrawl_analyze` does, and reports which ran via `answered_from=`.

Note `dfs_subtree_dirs` counts *catalog* directories (path components) while `subtree_dirs` counts directory *records* in this shard; they differ for the per-uid reason above.

## Incomplete / invalid shards

`ereport` and `ereport_index` reject a shard when `catalog_offset == 0`, `catalog_offset` is out of range, magic/version mismatch, or `crawl_bin_catalog_load()` fails (truncated catalog, bogus counts). Chunk mapping in `crawl_bin_chunks` caps the record region at `catalog_offset` when nonzero; when it is zero, loaders may still align checkpoints against EOF for diagnostics, but report and index consumers treat the shard as unusable until `ecrawl_repair` (or a fresh crawl) fixes it. `ecrawl_mount` skips unfinalized shards and reports how many it skipped. Bad-tail handling and `corrupt_shards/` quarantine are described under [`ecrawl_repair`](tools.md#ecrawl_repair).

## Checkpoint sidecars (`*.bin.ckpt`)

While crawling, `ecrawl` records block-aligned byte offsets at a fixed stride into `uid_shard_*.bin.ckpt`. Readers load those offsets to split each shard into valid segments without a preliminary full-file scan for boundaries, which lets many threads work on different byte ranges of the same file safely with no record torn across workers. Checkpoint offsets apply only to the record region (from just after the file header up to `catalog_offset` on finalized shards). If sidecars are missing or stale — for example after an interrupted crawl — run [`ecrawl_repair`](tools.md#ecrawl_repair) on the crawl output directory to rebuild them, and to truncate an incomplete last record when possible.

## Operational notes

- The code assumes local filesystem crawl data in `ERCBIN08` / format version 8 (nonzero `catalog_offset` and a trailing catalog). Use `ecrawl_repair` to regenerate missing sidecars without re-crawling.
- `uid_shard_*.bin` layout is preferred and automatically detected via `crawl_manifest.txt`.
- Shards are assigned by `uid & (uid_shards - 1)`, so one directory's children scatter across many shard files whenever its entries have different owners.
- For per-user runs, `ereport` and `ereport_index --make` read only the uid-shard files relevant to that user when uid-sharded input is available. All-users runs load every shard file, as do merged full-cluster crawls.
- `ECRAWL_UID_SHARDS` for a crawl run should match across every output directory you later pass together to `ereport` / `ereport_index --make`; merged reports assume a consistent shard layout.
