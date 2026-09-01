# Investigation roadmap: NUL bytes in ecrawl catalog names

Status: **OPEN — root cause not yet identified.** Symptom characterized in detail;
several latent bugs fixed along the way; writer-side tripwire in place.
Last updated: 2026-09-01 (after the post-union n1 full-option sweep).

This document is the handoff for any future session picking up the hunt. It records
what is known, what was ruled out, what was fixed, and what to do next. Do not
re-derive the "Established facts" section — trust it and build on it.

## Symptom

Reconstructed paths read back from the production capture
`hstor006-n1-mgmt_aug-26-2026_14-38-44` (a 9.6 h crawl of n1) contain **embedded NUL
bytes**. Structurally they are trailing NUL padding inside *catalog* directory-name
components: a directory whose catalog entry stores `(name_len=N, name_bytes)` with
the correct N but the name bytes all zero joins into paths as `.../foo\0/bar/...`.

Every tool that prints paths with `%s` (including old `ecrawl_query --list`) silently
truncated these paths at the first NUL, so the corruption was invisible until the
length-aware `--list --level --sum` emit produced 521,532 garbage rows from shard 362
(uid 142698). That emit path is now fully length-aware (memchr + explicit lengths);
printing still cuts at the first NUL, matching historical `%s` behavior.

## Established facts (empirical, do not re-derive)

- The zero bytes are **physically on disk** in the catalog region of
  `uid_shard_362.bin`. `cat_chunk_extract.py` parses the chunk headers and dumps
  NAME_BYTES without any catalog-reader code: zeros are in the file. The reader
  (`crawl_bin_catalog.c`) is exonerated.
- Scale in shard 362 (`probe_cat2.c` survey): **16,633 bad entries** (name_len > 0
  with NUL bytes present) out of ~78 M named directories.
- The corruption is **length-selective**: bad names have `name_len` in **32..54**
  (first survey said 32..43; the wider scan extended it). Long-named *good* entries
  exist in the same length range, so length alone is not the trigger.
- The corruption is **clustered**: bad entries sit at depths ~21–25 and concentrate
  under a few parents (some parents have 135+ bad siblings). This is not a uniform
  random process.
- **61% of zeroed entries have a "twin"** (`probe_twin.c`): a same-parent,
  same-length sibling whose name bytes are intact.
- **Duplicate directory entries exist** (`probe_dup.c`, `probe_rootkids.c`):
  shard 362 contains four distinct `data1` directories under the synthetic root
  (pid=1); shard 000 has 5 exact-duplicate (pid, name) groups. Duplicates mean the
  catalog hash table lost track of an existing entry and the directory was
  re-created with a fresh dir_id.
- The **n2 capture (40 min crawl) is clean**: bad=0 in all its shards. Its shards
  are small (177 K / 94 K / 734 K dirs) vs n1's 78 M dirs in shard 362 alone. The
  trigger is scale/duration-dependent or specific to the n1 crawl's conditions.
- The capture also contains two records that reconstruct as `/` (name_len=0,
  uid 142698, type d, 6 bytes total). Unexplained but benign; possibly a
  restart/checkpoint edge in the 9.6 h crawl.
- **The corruption silently broke `--subtree` queries** (found 2026-09-01 via
  the full-option perf sweep; **query-side union landed**). Duplicate catalog
  directory entries meant a subtree path used to resolve to ONE dir_id branch,
  and the DFS-range block filter then skipped records attached under the
  duplicate branches. Measured on the n1 capture before the union: `--subtree
  /data1/group/jbt/001/from_om/weka/arsalans --exact` returned 12,642,318
  entries while the path-string-based `--uid 142698 --list --level 1 --sum`
  attributed 122,623,571 records to the same path; `--subtree /data1 --exact`
  returned 687,808,524 of the true 978,235,888 (30% of all records invisible).
  `--subtree` now collects every dir_id whose reconstructed path equals the
  query and unions their DFS ranges (catalog rollup, record scan, and sidecar
  prune). Post-fix on the same n1 capture (node9901, 2026-09-01): `--subtree
  /data1 --exact` reports entries=978,231,323 of records_scanned=978,235,888
  (the remaining 4,565 are the two `/` records plus 4,563 skipped by the
  parent-dir hull); `--uid 142698 --subtree
  /data1/group/jbt/001/from_om/weka/arsalans --exact` reports entries=122,623,571,
  matching the `--list --level 1 --sum` rollup. The 12:20 full-option sweep
  (no `--uid` on `--exact`) reproduced the same 122,623,571; the hardlink
  subtree `home/arsalans` also gained the second catalog clone (77,202 →
  78,445). `--list`-family queries were already path-string-based and
  unaffected. Zeroed names still will not `memcmp`-match a normal subtree
  string. Writer-side healing of those names is a follow-on.

## Root-cause hypothesis (current best)

A **zeroing event hits jemalloc 48-byte size-class runs**. Both things that get
corrupted live in that class:

- catalog name buffers for 32–54-char names (allocation = length prefix + bytes),
- catalog hash-table entries.

One event explains both observations: names zeroed in place, *and* HT entries
zeroed/lost, which breaks chains and makes `shard_cat_ensure_dir` re-create already
existing directories (the duplicates). The length selectivity (32–54) is the
size-class signature; the clustering is consistent with whole runs/regions being
zeroed at a point in time.

Candidate vectors not yet excluded: a wild `memset`/`bzero` from an unrelated
component, a bad `realloc` move, an `munmap`+re-`mmap` region handed out by jemalloc
while a stale pointer still points into it, or a use-after-free whose freed run is
reused and cleared.

## Code-path findings that constrain repro design (2026-09-01)

- **LRU eviction does NOT round-trip the catalog.** `writer_close_lru_shard`
  writes the catalog tail + ckpt sidecar and closes the fp, but the in-memory
  catalog survives (`cat_live` stays 1); reopen takes the hot path and skips
  `crawl_bin_catalog_load` entirely. The disk-reload path
  (`shard_cat_load_from_disk_catalog`, which itself looks clean: `strndup`'d
  names, `strdup`'d keys) only runs when `cat_live==0`, which mid-run happens
  only after a `reopen_fail` — i.e. a **transient I/O error on reopen** destroys
  the in-memory catalog and forces a disk reload on next access. On weka
  (production) such errors are plausible; on node9901 NVMe they never happen, so
  eviction-churn repro variants exercise nothing interesting. A targeted variant
  would need fault injection on shard reopen.
- **No cross-run resume**: startup deletes leftover shard files ("interrupted
  crawls are not resumed"), so the reload path is cold in normal operation and
  the production corruption happened in-memory during the single 9.6 h run.
- **Size-class arithmetic**: HT entries are ~40 B (jemalloc 48 B class); name
  blobs are `malloc(name_len+1)`, so nlen 32–46 → 48 B class, nlen 47–54 → 64 B
  class. The 48 B-class hypothesis covers the observed 32–43 core exactly; the
  44–54 tail either belongs to a second affected class or the boundary needs
  re-measuring (a bad-entry nlen histogram from `probe_cat2`-style data would
  settle it).

## What was ruled out

- **Reader-side bug**: zeros are on disk (see above).
- **ASan-detectable heap errors at small/medium scale**: `ecrawl_asan` (-O1 -g
  -fsanitize=address) ran clean over two stress rounds plus a 40-iteration churn
  loop on synthetic trees (`gen_tree.py`: name lengths 32–54, deep nesting,
  shard-LRU-eviction churn via `ECRAWL_MAX_OPEN_SHARDS`, inode sharding via
  `ECRAWL_SHARD_BY_INO`). No NULs produced, no ASan reports.
- **Full-speed jemalloc build** (`ecrawl_prod`, -O2 + jemalloc): same synthetic
  stress, also clean.
- **Signature-shaped tree at near-production scale** (2026-09-01, round 3):
  `~/gen_deep_tree.py` built `/data1/erbmi1/deep-tree` on node9901 — 36.2 M dirs,
  all one uid (single shard), wide levels at depth 21–22 (200- and 180-way
  fanout), every component 32–54 chars. 15 tripwire-instrumented crawls
  (`~/deep-crawl-loop.sh`, 32 threads, ~1 min each) all clean: no tripwire hits,
  `probe_cat2` bad=0 every run. Shape + half-production scale + fast local-NVMe
  crawls are not sufficient. Still untested: production-duration crawls (9.6 h vs
  1 min = ~600x smaller event window per crawl), weka latency/error patterns.
- **TSan**: not run — `libtsan` is missing on node9901's gcc 11.5 install. Still
  available as a vector if a data race is suspected (build on a host with a
  complete TSan runtime).

## Fixes landed during this investigation (already in the tree)

These were found while hunting and are fixed regardless of the main root cause:

1. `crawl_identity_init` (ecrawl.c): `ci->uring` was not zero-initialized when
   io_uring was inactive → wild `munmap`/`free` in `uring_stat_destroy` at worker
   exit (ASan DEADLYSIGNAL). Fixed with `memset`. Benign in production only because
   fresh thread stacks happen to be zero-filled.
2. `shard_cat_grow_arrays` (ecrawl.c): quadratic same-size `realloc` loop (a writer
   spun 9+ min under ASan, which always copies) — added an early return when no
   growth is needed. Also fixed the OOM path leaving half the arrays
   dangling/updated (each realloc now commits immediately).
3. `shard_cat_ensure_dir` (ecrawl.c): `ht_insert` failure left a half-initialized
   slot; now rolls the slot back fully (burned dir_id, consistent state).
4. `crawl_bin_catalog_dir_path_len` (crawl_bin_catalog.c): silently truncated paths
   deeper than `CRAWL_BIN_CATALOG_MAX_PATH_PARTS`; now fails loudly (`return -1`).

Validation: Slurm jobs 21725032 (growfix) and 21727703 (oomfix) ran `test.sh` green.

## Instrumentation

- `ECRAWL_CAT_TRIPWIRE=1` (ecrawl.c): checked catalog name components for NUL bytes
  at creation time and again at every write (eviction + final close), reporting
  dir_id/pid/name_len/phase. **Removed from the tree on 2026-09-01** (kept out of
  the feature commit per user decision). To re-arm: `git apply -R
  ~/ecrawl-cat-tripwire.patch` (78 lines, three sites: create-time check in
  `shard_cat_ensure_dir`, the `shard_cat_nul_scan` function, its call in
  `shard_flush_ckpt_before_close`). A tripwire-enabled binary also survives in
  node9901's `/tmp/ereport-hl` until that dir is rebuilt or the host reboots.
- `ECRAWL_SHARD_BY_INO=1` (test-only): forces sharding by inode instead of uid.
  Never committed; lived only in node9901 scratch.
- `ECRAWL_MAX_OPEN_SHARDS=N`: caps open shard handles to force LRU eviction churn
  (pre-existing, committed).

## Next steps (in order)

1. **Tripwire a production-scale crawl.** Re-apply the tripwire first
   (`git apply -R ~/ecrawl-cat-tripwire.patch`), then run that `ecrawl` against a
   large, long tree (n1-like scale: tens of millions of dirs per shard, multi-hour;
   match the original crawl's 32 crawl / 8 writer threads, 512 shards, default
   max_open_shards=64). A trip fires at the moment of corruption and names the
   phase (create vs eviction-write vs final-write). This is the single most
   informative experiment available.
2. **Aggressive-purge repro** (2026-09-01, round 4 — **clean**): the deep-tree
   loop rerun with `MALLOC_CONF=tcache:false,dirty_decay_ms:0,muzzy_decay_ms:0`
   so freed runs are purged (MADV_DONTNEED) immediately — if the mechanism were
   use-after-free followed by purge, this widened the window from ~10 s of decay
   to zero. 15/15 iterations clean (no tripwire, probe bad=0). Result log:
   `/data1/erbmi1/deep-captures-purge.log` on node9901. The UAF+purge vector is
   not confirmed at this scale/duration; remaining differentiators are
   production crawl duration (9.6 h vs 1 min) and the weka environment.
3. **Fault-injection variant**: since the disk-reload path only runs after a
   reopen failure (see findings above), a repro that injects reopen errors
   (LD_PRELOAD on fopen/fread, or an env-gated test hook) would exercise
   `shard_cat_load_from_disk_catalog` mid-run at scale.
4. **Heap forensics on the bad capture.** Map the on-disk offsets of zeroed name
   blobs back to allocation order (offsets are append-ordered) to test whether the
   bad entries were contiguous in *time* at the writer. Also produce the bad-entry
   nlen histogram to settle the 48 B vs 64 B class question.
5. **TSan build** on a host with a working runtime, if (1)–(4) point at a race.
6. When the root cause is found: fix, then decide whether a one-off pass should
   detect (and where possible heal via twins) zeroed catalog names in existing
   captures.
7. **Query-side duplicate tolerance — landed.** `--subtree` unions every
   catalog (and sidecar) dir_id whose reconstructed path equals the query, so
   a corrupt capture with duplicate directory entries no longer silently
   undercounts. Writer-side NUL / hash-table corruption remains open; until
   that is fixed, `probe_dup` still describes how damaged a capture is, but
  it is no longer a "do not run --subtree" check. The 12:20 n1 sweep confirmed
  the unioned counts and that `subtree_find_dirs` is ~1% of samples; remaining
  `--subtree` cost is catalog load (follow-up 11). Zeroed names still do not
  match a normal subtree string.

## Artifacts and where they live

- Probes (durable, in home): `~/probe_cat2.c` (corruption survey),
  `~/probe_twin.c` (twin rate), `~/probe_dup.c` (duplicate groups),
  `~/probe_rootkids.c` (children of pid=1), `~/probe_rootrec.c` (the `/` records),
  `~/harvest_nul.py` (distinct zeroed-name prefixes), `~/cat_chunk_extract.py`
  (reader-independent on-disk check), `~/gen_tree.py` (small synthetic stress
  trees), `~/gen_deep_tree.py` (signature-shaped 36 M-dir tree),
  `~/deep-crawl-loop.sh` (tripwire crawl loop + per-run probe survey).
- Synthetic trees on node9901: `/data1/erbmi1/deep-tree` (36.2 M dirs, round-3/4
  repro tree), `/data1/erbmi1/repro-tree` (~1 M dirs, rounds 1–2),
  `/data1/erbmi1/deep-captures*.log` (loop results).
- Capture under analysis: `/data1/ereport/hstor006-n1-mgmt_aug-26-2026_14-38-44/`
  on node9901 (also `~/orcd/scratch/ereport/hstor006-n1-mgmt_aug-26-2026_14-38-44/`
  on ORCD). Shard of interest: `uid_shard_362.bin` (uid 142698 = arsalans).
- Query profiles (gitignored `logs/`): `logs/ecrawl_query_prof.tgz` (12:20
  post-union pack; extracted `logs/prof_new/`) and `logs/prof/` (10:42
  pre-union). Recipe: `ecrawl_query.txt`. `--index-dir` profiles were not
  included.
- node9901 scratch: `/tmp/ereport-hl` (repo copy + build), `/tmp/ereport-asan`
  (ASan build). **/tmp does not survive reboots** — rebuild from home if gone.
- Build/run on node9901: `rsync -a --delete --exclude=.git ~/git/ereport/
  /tmp/ereport-hl/ && cd /tmp/ereport-hl && make -j32` (gcc 11.5). SSH via the
  `efs-test-ssh` skill wrapper.
- Builds/tests on ORCD go through Slurm per the `orcd-slurm-test` skill
  (mit_quicktest / mit_normal only).

## Related but separate work

- The `ecrawl_query --list --level` emit was parallelized (2026-09-01) after perf
  showed the single-threaded hash emit dominating: 65 s → ~12 s at ~8–9 CPUs on the
  n1 capture, output byte-identical. Unrelated to the corruption itself, but the
  length-aware emit it builds on is what made the NULs visible in the first place.

### ecrawl_query performance follow-ups (post-fix perf review, logs/*txt, 2026-09-01)

Post-fix profiles (`--uid 142698 --list --level 1` ± `--sum`, 16 threads, node9901;
elapsed 10.6 s both): threads evenly loaded (~4% each, no serial tail). Cycle
breakdown: emit 62.8% (Pass 1 = 54.7%, of which `query_hash_path` = 37.8%;
Pass 2 = 8.2%), scan 36.5% (`crawl_bin_catalog_dir_path_len` 10.4%, block read +
zstd 7%, `realloc` copies 6.5–7.5%, catalog load 3.6%). The `--sum` profile is
identical in shape; the global hardlink-dedup mutex shows no contention at this
scale (4,540 dupes) and would only matter for hardlink-heavy captures.

Ordered by expected value:

1. **Skip Pass 1 for unfiltered queries.** The mset exists so Pass 2 can find the
   shallowest *listed* ancestor (`query_level_root_nc`) — needed only when filters
   (`--uid`/`--gid`/subtree/name) make the listing sparse. With no filters every
   ancestor is listed, the level root is always the first component, and Pass 1
   (54.7% of all cycles) is pure waste. Gate on "no filters active".
2. **Kill the `realloc` copies in worker output/`lrec` buffers (6.5–7.5%).**
   `query_out_append`/`query_lrec_append` double from a small initial cap, so a
   ~1 GB/worker output is copied ~2× (17 GB total here). Use chunked segment
   buffers (no copying) or pre-size from shard record counts.
3. **Path reconstruction (10.4% + 4.7% memmove).** A `query_path_cache` already
   exists (ecrawl_query.c:3602); measure its hit rate — DFS-ordered records should
   hit often. If misses dominate, key the cache on consecutive records' parent
   dir_id instead.
4. **`query_list_emit_sum` (`--list --sum` without `--level`) is still serial**
   (global sort of all matching records, ecrawl_query.c:2696). Parallelize like
   the level emit if that flag combination ever matters.
5. **Hash micro-optimization (secondary).** `query_hash_path` is already
   word-at-a-time; SSE4.2 CRC32 or xxh3 would shave Pass 1 further, and the
   `memchr` line-split could be fused with hashing into one pass over the bytes.
   Both are moot for unfiltered queries if (1) lands.
6. **zstd decode (~6%)** is inherent and already parallel; nothing to do.

### Full-option perf sweep (logs/ecrawl_query_prof.tgz, 2026-09-01)

Two packs of the same recipe (`ecrawl_query.txt` v2, `ECRAWL_QUERY_THREADS=16`,
n1 capture `hstor006-n1-mgmt_aug-26-2026_14-38-44` on node9901, dwarf / 997 Hz):

- **Pre-union** `logs/prof/` (~10:42) — first sweep; some filter walls were
  missing from the bundle and were guessed from sample counts.
- **Post-union** `logs/ecrawl_query_prof.tgz` (12:20, extracted to
  `logs/prof_new/`) — complete `out.*.txt` + `stats.*.txt`. This is the
  source of truth for walls. None of follow-ups (1)–(12) were implemented
  between the two packs; union is a correctness change.

`--subtree` "does not exist here; matching the capture literally" is expected
on node9901. Duplicate-union stderr fired: 4× `weka/arsalans` and 2×
`home/arsalans` in `uid_shard_362.bin`. Recipe §16 (`--index-dir` sidecar
profiles) was not run. Leftover `logs/report.level1.txt` /
`logs/report.sum.txt` at the logs/ root are a 07:55 pair, not this sweep.

Post-union `elapsed_sec` (stdout for filter/subtree, stderr stats for
`--list`; default/`--top,deep` emit shape histograms with no elapsed):

| query | wall | entries | answered_from | records_scanned | notes |
|---|---|---|---|---|---|
| default | — (127 CPU-s) | shape / 978,235,888 rec | fold | — | `parent_map_get_or_add` 57% |
| `--top,deep` 50 | — (136 CPU-s) | same fold | fold | — | same, 53% |
| `--uid` 142698 | 0.223 s | 127,821,098 | record_scan | 128,175,044 | VERIFY matched; 1 shard |
| `--gid` 167149 | 0.765 s | 468,818,647 | record_scan | 978,235,888 | 496M zonemap skips |
| subtree `home/arsalans` | 6.391 s | **78,445** (was 77,202) | record_scan | 590,739,564 | nlink>1, no rollup; k=2 |
| subtree `om5/arsalans` | **9.323 s** | 1 (2 bytes) | **catalog_rollup** | 0 | 139,836,804 dirs examined |
| subtree `--exact` `weka/arsalans` | 6.688 s | **122,623,571** (was 12,642,318) | record_scan | 595,000,297 | same scanned, 10× matches |
| `--size-gt` 1G | 0.068 s | 92,131 | record_scan | 978,235,888 | 953M skipped |
| `--type` l | 0.197 s | 4,400,807 | record_scan | 978,235,888 | 890M skipped |
| `--perm` -0002 | 1.491 s | 22,649,446 | record_scan | 978,235,888 | 0 skipped; 23 CPU-s ≠ 23 s wall |
| `--uid --list` | 4.190 s | 127,821,098 | record_scan | 128,175,044 | stdout `/dev/null` |
| `--uid --list --level` 1 | 10.651 s | 127,821,098 | record_scan | 128,175,044 | |
| `--uid --list --level` 1 `--sum` | 10.703 s | 127,821,098 | record_scan | 128,175,044 | |
| `--uid --list --sum` (no `--level`) | **116.065 s** | 127,821,098 | record_scan | 128,175,044 | worst query |
| `--uid --list --level` 3 | 10.847 s | 127,821,098 | record_scan | 128,175,044 | |
| `--list --level` 1 (no uid) | 51.462 s | 978,235,888 | record_scan | 978,235,888 | |

Union vs pre-union walls are noise except the two trees that gained DFS
ranges: `--exact` 6.167 → 6.688 s, `home/arsalans` 6.080 → 6.391 s.
`subtree_find_dirs` is ~1% of samples. `records_scanned` stayed 595,000,297
(`--exact`) and 590,739,564 (home); `blocks_decompressed` rose 4,480 → 25,638
and 3,230 → 7,699 — same shards, denser hull hits, not a new I/O class.

The 12:20 pack also replaces the first-sweep CPU-sample guesses. `--perm`
is 1.49 s wall (23 CPU-s across 16 threads), not a 23 s query; `--uid` is
0.223 s wall (3 CPU-s). Filter totals are already fast; zonemaps explain
size/type/gid; `--perm` has no zonemap.

Thread balance (bythread overhead): `--list --sum` 80.8% on one PID (serial
`qsort`); subtree rollup 100% one PID (catalog load, no workers); `--exact`
43% on one PID then even scan; default one fat PID at 12% plus ~4.8% × 16
workers (fold lock); `--level` 49 PIDs, even ~4–6%.

New / confirmed hot paths (children % from the post-union reports). None of
these are checked off:

7. **`--list --sum` (no `--level`) is the worst query: 116.1 s, `qsort_r`
   60.5% of cycles, 81% of samples on one thread.** Serial global sort of
   127.8M paths in `query_list_emit_sum`. Parallelize: per-thread sort +
   k-way merge, or hash-aggregate by path first and sort only unique dirs.
   Owns ~70 s of the 116 s wall.
8. **Unfiltered `--list --level` 1: Pass 1 = 69% of all cycles** (51.5 s
   wall, `query_hash_path` 54%). Confirms follow-up (1): skip the membership
   pass when no filters are active. Filtered `--uid --level` 1/3/`--sum` are
   still ~10.7 s with Pass 1 at 54–56% (~6 s); skip-Pass-1 helps those too
   but less.
9. **Default/`--top,deep` are fold-bound: `parent_map_get_or_add` = 57% /
   53%**, mutex lock 22% / 21%, `parent_chain_find` 21% / 20%, fold worker
   76% / 71%. Catalog load is only 3%. The stripe-locked shared parent map
   serializes 133.9M inserts. Options: per-worker maps + parallel merge, or
   the lock-free CAS insert pattern now proven in the emit mset.
10. **`crawl_bin_codec_decode_u64` = 50–58% of every filter scan**
    (perm/uid/gid/sizegt/type_l), zstd 12–20%. The byte-at-a-time varint
    decoder is the universal *scan* bottleneck, but the walls are already
    0.07–1.5 s. A 1-byte fast path would help `--perm` most (full decode,
    0 skipped). Do this after (7)–(9) and (11).
11. **Every `--subtree` query is catalog-load-bound, including the rollup
    shortcut.** `crawl_bin_catalog_load_sel` is 92% of the 9.32 s
    `om5/arsalans` rollup (`decode_u64` 49% + zstd 39%, 1 PID, 0 records
    scanned), 83% of the 6.39 s hardlink `home/arsalans` scan (rollup
    correctly skipped, `nlink>1`), and 55% of the 6.69 s `--exact` (then
    the 122M-record scan). Loading 140M catalog rows to answer one directory
    is **slower than scanning 122M matching records**. The
    `dirs.idx`/`rowgroups.idx` sidecars (`--index-dir`) exist for exactly
    this and were not profiled (recipe §16 still commented). Alternatively
    make catalog loads lazier. Union itself is not the cost.
12. **Plain `--list` is path-reconstruction-bound: `dir_path_len` 33% +
    memmove 16% + reader 24%** (4.19 s). The path builder prepends
    components with memmove, making each path O(depth²) byte-moves; building
    back-to-front into the buffer end is O(depth). Measure `query_path_cache`
    hit rate before rewriting (follow-up 3).

Ranked by measured wall those still own: (7) 70 of 116 s → (8) 36 of 51 s
unfiltered / ~6 of 11 s filtered → (11) 8.6 of 9.3 s rollup, 3.7 of 6.7 s
`--exact` → (9) default fold → (12) ~2 of 4.2 s `--list` → (2) realloc 7% of
`--level` → (10) sub-second filter scans. (5) is moot if (1)/(8) land; (6)
zstd is inherent.
