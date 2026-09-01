# Crawl shard binary format

Each `uid_shard_*.bin` file uses this layout. The authoritative definition is [`crawl_bin_format.h`](../crawl_bin_format.h); this page explains it.

## Version

Shards are format version 9, magic `ERCBIN09`. Readers require an exact magic and version match and there is no upgrade path: a capture written by an older build must be re-crawled. Nothing in the tree reads an earlier version, so this page describes v9 only.

## File header (32 bytes, packed)

`magic[8]` (`ERCBIN09`), `version` (`uint32_t`, must equal 9), `reserved` `uint32_t`, `catalog_offset` `uint64_t` (byte offset from BOF to the catalog blob), `reserved64` `uint64_t`.

`catalog_offset` is the finalization marker. On a completed crawl `ecrawl` sets it to the first byte of the catalog (≥ header size, ≤ file size). `catalog_offset == 0` means the shard was never finalized — still being written, or interrupted — and readers must reject it.

## Record region: `[sizeof(header), catalog_offset)`

A back-to-back sequence of **row groups**. Each is laid out as

```
bin_rowgroup_hdr_t
bin_colchunk_hdr_t[column_count]        the column directory
<column 0 payload> … <column n-1 payload>
```

with the payloads in directory order, so a reader that wants two columns reads the header plus directory and then seeks straight to those two payloads. Row groups are self-describing and contiguous — `crawl_bin_rowgroup_total_bytes()` gives the stride to the next group from the header alone — so a reader walks them with header reads and no side index, and every chunk boundary handed to a parallel worker is a group boundary. `ecrawl` flushes a group at about 1 MiB of uncompressed records or 65536 records, whichever comes first (`CRAWL_BIN_ROWGROUP_RAW_TARGET`, `CRAWL_BIN_ROWGROUP_MAX_RECORDS`). The 1 MiB target is what gives the codecs enough runway to pay off: a few thousand records per group is too short for run-length and frame-of-reference encoding to matter.

**Record order inside a group is current writer behaviour, not part of the format.** Just before encoding, `ecrawl` sorts the buffered group by `parent_dir_id`, then by name bytes (`memcmp` over the common prefix, shorter name first), then by arrival position, because the codecs are order-sensitive and one run per directory is worth several MiB on a capture with more than a couple of records per directory — see [performance.md](performance.md#measured-sorting-each-row-group-by-parent_dir_id-name). The order is deterministic given the group's contents, and nothing in the tree depends on it: `.ckpt` offsets are physical, the sidecars are built from what a group actually holds, and `ecrawl_query` and `ecrawl_mount` impose their own order on output. Readers should keep it that way. Sorting is per group and never moves a record between groups, so one directory's children can still straddle a boundary and arrive in either order, and a later writer is free to order a group differently or not at all.

That layout is intentionally heavier at **write** time than the older single-zstd block frames (v6/v7): each flush runs a codec pass and a `ZSTD_compress` per column. `--no-write` never enters this path, so a compare-indexers figure where `fd` / `find` / `du` / `ecrawl --no-write` stay flat while solid `ecrawl` (write) slows is the expected signature of a producer-format change, not a colder walk. See [performance.md](performance.md#ercbin08-capture-write-cost).

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
| `CRAWL_ENC_BYTES_STORED` | opaque blob with no zstd frame around it | catalog `name_bytes` only, when name compression is off |

### Zone maps and projection

`min_value` / `max_value` on every chunk form a zone map, so a reader can prove no record in a group can satisfy a range predicate and seek past the group without decompressing it. The summary must cover *every* record in the group — understating a range would silently drop query results — and it is inline rather than in a sidecar precisely so it cannot go stale relative to the data it describes. For `name_bytes`, which has no order, both are 0.

Projection is the other half: `crawl_bin_block_reader_set_projection()` decodes only the requested columns, and requesting `name_bytes` implicitly pulls in `name_len` because the lengths slice the blob. `crawl_bin_block_reader_next()` is a row-reconstruction shim that rebuilds a `bin_record_hdr_t` from the decoded columns, leaving unprojected fields zero, so a consumer can adopt projection without restructuring its loop. See [`crawl_bin_block.h`](../crawl_bin_block.h), the codec round-trip tests in `test_crawl_codec.c`, and the projection and block-skip parity checks in `test_crawl_block_filter.c` and `test.sh`.

### Record header (in memory only)

`bin_record_hdr_t` — `parent_dir_id`, `name_len`, `type`, `uid`, `gid`, `mode`, `size`, `inode`, `dev_major`, `dev_minor`, `nlink`, `atime`, `mtime`, `ctime` — is the record exchanged between the writer, the readers and the shim. Since v8 it is **not** written to disk verbatim; each field lives in its own column chunk. Hardlink-aware byte credit uses `inode`, `dev_major`/`dev_minor` and `nlink`.

## Catalog tail: `[catalog_offset, EOF)`

Since v9 the catalog is columnar and compressed, the same way the record region is:

```
uint64_t n_entries                    dir_ids are dense, 1 … n_entries
bin_catalog_hdr_t                     chunk_count, chunk_dirs
<chunk 0> … <chunk chunk_count-1>
uint64_t chunk_off[chunk_count]       absolute file offsets
```

and each chunk is a `bin_catchunk_hdr_t` (`dir_count`, `column_count`, `comp_bytes`, `raw_bytes`), then a `bin_colchunk_hdr_t[column_count]` directory, then the payloads in directory order — a row group in all but name, so the codec, the zone maps and the skip-what-you-did-not-ask-for reader logic are shared. `CRAWL_BIN_CATALOG_CHUNK_DIRS` (4096) directories go in a chunk; only the last may hold fewer.

v8 wrote this region as 136 raw bytes plus a name per directory, uncompressed, which on a directory-heavy capture was four fifths of the file.

Chunk *k* holds dir_ids `[k*chunk_dirs + 1, (k+1)*chunk_dirs]`, which is what lets the `dir_id` column be dropped: it is the row's position. The other nineteen columns are `parent_dir_id`, `depth`, `name_len`, `name_bytes`, the five `imm_child_*`, `flags`, and the nine `dfs_*` / `subtree_*` / `self_bytes` fields described below. `name_bytes` is the concatenated component names, sliced by `name_len`, exactly as in the record region.

Columns are ordered so the always-loaded tree group is a contiguous prefix, and `crawl_bin_catalog_load_sel()` lets a reader load only the groups it needs — the tree fields alone, plus `CRAWL_CAT_IMM_CHILD` and/or `CRAWL_CAT_SUBTREE`. A group it did not ask for is skipped by its `comp_bytes` and never decompressed. `ereport`, `ereport_index` and `ecrawl_mount` ask for neither.

Two readers exist. `crawl_bin_catalog_load_sel()` materializes whole columns into per-`dir_id` arrays, and walks the chunks back to back rather than through `chunk_off[]`, so it does not depend on where the file ends. `crawl_bin_catalog_read_row()` reads one row: `chunk_off[]` locates the row's chunk from its `dir_id` alone, and the decoded chunk is cached in the caller's walk, so climbing a parent chain usually decodes nothing after the first level. That is the path `dirs.idx` uses, and the reason the chunk offset table exists at all; it trails the chunks because the writer only knows where a chunk landed once it has written it.

`dir_id` is 1-based and unique per shard; `parent_dir_id == 0` only for the synthetic root row (`dir_id == 1`). A directory gets a row when it is the recorded parent of at least one entry, so a directory with no children of its own may have no row. Directory identity is per shard, so the same logical path has different `dir_id`s in different shards — merging them into one namespace is the bulk of the work in [`ecrawl_mount`](tools.md#ecrawl_mount).

The `imm_child_*` aggregates sum only records whose on-disk `parent_dir_id` equals this row's `dir_id`, scoped to this shard. `imm_child_bytes` follows `ecrawl` accounting (regular files: hardlink-aware credit; other types: apparent `st_size`). `imm_child_min_eff_time` / `imm_child_max_eff_time` use per-record effective time `max(atime, mtime, ctime)`, and `imm_child_min_eff_time` is `UINT64_MAX` when `imm_child_count == 0`. `imm_child_ctime_led_count` uses the same “ctime-led” rule as `ereport`: `ctime` strictly greater than `max(atime, mtime)` and at least 180 days newer.

### DFS ordering and subtree rollups (v8)

A single O(directories) post-pass at the end of the crawl fills the remaining fields. It runs only on the final close, never on LRU shard eviction, because a shard can be closed and reopened many times mid-crawl.

`dir_id` is assigned in crawl arrival order and is referenced by every record's `parent_dir_id`, so it cannot be renumbered without rewriting the whole record region. `dfs_index` is therefore a *permutation* rather than a renumbering: it gives each directory its position in DFS pre-order, which makes “is X at or under D” the O(1) range test

```
dfs_index[D] <= dfs_index[X] < dfs_index[D] + dfs_subtree_dirs[D]
```

with no per-shard bitmap and without touching a single record. `ecrawl_query` uses it in place of the bitmap `--subtree` previously built.

`subtree_bytes` / `subtree_count` are `imm_child_bytes` / `imm_child_count` summed over that DFS range, with `subtree_files` / `subtree_dirs` / `subtree_symlinks` breaking the count down by record type. So a `--subtree` aggregate becomes a lookup: cost is O(directories) instead of O(files), and no record is read at all.

Two subtleties are worth stating plainly:

- The `subtree_*` sums cover records *under* the directory and exclude the directory's own record, which by construction hangs off its parent. `self_bytes` carries that one record's credit on the directory's own row so a query can add it back and match `du -sb`, which counts the directory it was given. The `CRAWL_DIR_FLAG_SELF_RECORD` flag says this shard actually holds that record — a shard is per-uid, so a directory owned by another user still gets a catalog row here to give its children a path, and without the flag a cross-uid subtree would count its root once per shard.
- `subtree_nlink_gt1_count` is the number of records in the subtree with `nlink > 1`. **When it is zero, `subtree_bytes` provably equals what a full scan would compute.** When it is nonzero the two can legitimately differ, because crawl-time hardlink credit is attributed to the first link visited anywhere in the tree while a scan dedups within the queried subtree. Readers must fall back to the scan rather than present the rollup as exact; `ecrawl_query` does, and reports which ran via `answered_from=`.

Note `dfs_subtree_dirs` counts *catalog* directories (path components) while `subtree_dirs` counts directory *records* in this shard; they differ for the per-uid reason above.

## Incomplete / invalid shards

`ereport` and `ereport_index` reject a shard when `catalog_offset == 0`, `catalog_offset` is out of range, magic/version mismatch, or `crawl_bin_catalog_load()` fails (truncated catalog, bogus counts). Chunk mapping in `crawl_bin_chunks` caps the record region at `catalog_offset` when nonzero; when it is zero, loaders may still align checkpoints against EOF for diagnostics, but report and index consumers treat the shard as unusable until a fresh crawl replaces it. `ecrawl_mount` skips unfinalized shards and reports how many it skipped.

## Checkpoint sidecars (`*.bin.ckpt`)

While crawling, `ecrawl` records block-aligned byte offsets at a fixed stride into `uid_shard_*.bin.ckpt`. Readers load those offsets to split each shard into valid segments without a preliminary full-file scan for boundaries, which lets many threads work on different byte ranges of the same file safely with no record torn across workers. Checkpoint offsets apply only to the record region (from just after the file header up to `catalog_offset` on finalized shards). A missing or stale sidecar (for example after an interrupted crawl) is not rebuilt in place: `ecrawl_query` scans that shard as a single range from after the file header through EOF; `ereport` / `ereport_index` reject a shard that has no usable catalog tail. Re-crawl to get a complete capture.

## Directory-index sidecars (`dirs.idx`, `rowgroups.idx`)

Two derived files that `ereport_index --make` writes into its `--index-dir`, and `ecrawl_query --index-dir DIR` reads. They are not part of a shard and hold no data of their own: everything in them can be recomputed from the shards, and a reader that cannot find or verify them behaves exactly as it did before they existed. `--no-dir-index` skips writing them.

They exist because both routes to a subtree answer were linear in the *capture*, not in the subtree. The v8 catalog rollups already reduce a bare `--subtree` aggregate to O(directories), but reaching the one row that holds the answer still meant parsing every catalog row in every shard — 21726 directories examined to answer with 2 on the tree these were measured against. A filtered subtree scan had the same shape one level down: chunk boundaries came from the `.ckpt` stride, which knows nothing about which directories a byte range covers, so every record was decoded and tested.

### Shared skeleton

Both files are little-endian, packed, and laid out the same way:

| | |
|---|---|
| `crawl_sidecar_hdr_t` | 48 bytes at offset 0: magic, version, `shard_count`, `shard_dir_off`, `names_off`, `names_bytes`, `entry_total` |
| per-shard payloads | in shard order, appended as each shard's worker finishes |
| shard descriptor array | at `shard_dir_off` |
| shard basename blob | at `names_off`, `names_bytes` long, not NUL-separated (each descriptor carries its own length) |

The descriptor array trails the payloads because the writer only knows where a payload landed after writing it. Shards are stored sorted by basename, which is the order a reader gets from sorting its own directory listing, so the identity check is an index-wise comparison. A build whose input directories contribute two shards with the same basename writes no sidecars at all: a reader keyed on basename could not tell them apart.

### Identity binding

Every descriptor opens with a `crawl_sidecar_shard_id_t` recording five facts about the shard it was built from: basename, `st_size`, mtime (sec and nsec), the `catalog_offset` from the shard's own header, and the `n_entries` count that opens the catalog blob — plus the `max_dir_id` the catalog reached. A reader re-stats each shard, re-reads its 32-byte header and that one `uint64`, and compares. Any mismatch on any shard retires the sidecar for the whole run.

This matters more than it looks. `dir_id` is per shard and is handed out in crawl arrival order, so a sidecar's dir_ids and DFS positions are meaningless against a different file — and nothing else in the index dir is staleness-checked. The rule is therefore reject, never repair.

### `dirs.idx` — `EDIRX002`

Per shard: a hash table over full stored paths.

- `crawl_dirx_shard_t`: the identity binding, then `hash_count`, `hash_off`.
- At `hash_off`: `hash_count` × `crawl_dirx_entry_t` (`path_hash`, `dir_id`), sorted by `(path_hash, dir_id)`.

`path_hash` is FNV-1a over the path exactly as `crawl_bin_catalog_dir_path()` spells it: absolute, no trailing slash, the synthetic root as the empty string. No path bytes are stored. A lookup binary-searches to the hash, and each candidate is only accepted after its row has been read and its parent chain walked back into a path that compares byte-equal to the query — so a 64-bit collision costs one wasted read and can never produce a wrong answer.

Rows come out of the shard itself. `EDIRX001` also stored a `dir_id → catalog row offset` map, because a v8 catalog row was variable length and an ancestor's offset could not be computed from its id; it cost about 8 bytes per directory. In v9 a `dir_id` names its own chunk, so the map is gone and `crawl_bin_catalog_read_row()` resolves the hit against the shard's chunk table.

Directories whose path will not rebuild — a broken parent chain — are left out of the table. `subtree_find_dirs` skips them too, so the two routes still agree about which directories exist.

### `rowgroups.idx` — `ERGIX001`

Per shard: one sketch per row group of where that group's records sit in the shard's DFS order.

- `crawl_rgix_shard_t`: the identity binding, then `dfs_domain` (DFS positions live in `[0, dfs_domain)`), `group_count`, `groups_off`.
- At `groups_off`: `group_count` × `crawl_rgix_group_t` — `file_offset`, `group_bytes`, `record_count`, `flags`, `dfs_min`, `dfs_max`, and a 1024-bit bucket bitmap (128 bytes), 168 bytes per group.

A subtree is a contiguous DFS range, so a group can be skipped when the range misses `[dfs_min, dfs_max]`, or when no bucket the range covers is set. Bucket assignment is `dfs * 1024 / dfs_domain` (`crawl_rgix_bucket_of`), shared by writer and reader.

Both sketches are stored because `dir_id` follows crawl arrival order and correlates with DFS position only loosely, which makes the plain interval very wide in practice. Measured on a 21726-directory, 1.13M-record capture with 129 row groups, for a single leaf directory the interval kept 73 groups and the bitmap kept 2; for one mid-level directory, 74 against 5. Neither test implies the other — a bucket can be lit by a directory just outside the range, and an interval can straddle a range it never actually visits — so keeping only the groups both accept prunes more than either alone (92 and 82 individually, 76 together, on a large subtree). Both are conservative supersets, so the intersection is still a superset and pruning cannot drop a record the scan should have seen.

`flags & CRAWL_RGIX_GRP_UNKNOWN` marks a group whose sketch is incomplete because a record named a `dir_id` the catalog does not have. Such a group proves nothing and is always kept. `dfs_min > dfs_max` means the group had no in-catalog parents at all.

`group_bytes` may span a run of adjacent groups when the writer stepped over empty ones; both ends are always row-group boundaries, which is all a chunked reader requires, so a survivor list turns straight into scan chunks.

## Operational notes

- The code assumes local filesystem crawl data in `ERCBIN09` / format version 9 (nonzero `catalog_offset` and a trailing catalog). Interrupted crawls leave shards without a catalog tail; re-crawl rather than patching sidecars.
- `uid_shard_*.bin` layout is preferred and automatically detected via `crawl_manifest.txt`.
- Shards are assigned by `uid & (uid_shards - 1)`, so one directory's children scatter across many shard files whenever its entries have different owners.
- For per-user runs, `ereport` and `ereport_index --make` read only the uid-shard files relevant to that user when uid-sharded input is available. All-users runs load every shard file, as do merged full-cluster crawls.
- `ECRAWL_UID_SHARDS` for a crawl run should match across every output directory you later pass together to `ereport` / `ereport_index --make`; merged reports assume a consistent shard layout.
