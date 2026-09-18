# Investigation roadmap: NUL bytes in ecrawl catalog names

Status: **OPEN — root cause not yet identified.** Symptom characterized in detail;
several latent bugs fixed along the way; writer-side tripwire in place.
Last updated: 2026-09-01 (n1 catalog forensics + TSan 1.2 B crawl).

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
- Scale in shard 362 (`probe_cat2.c` / `probe_forensics.c`): **16,633 bad entries**
  (name_len > 0 with NUL bytes present) out of ~78.7 M named directories in that
  shard. Capture-wide (all 264 n1 shards, 2026-09-01): **38,464 bad / 139,836,522
  named** in **75 of 264** shards. Top dirty shards: 362 (16,633), 143 (10,860),
  424 (2,358), 000 (2,136), 429 (1,950), 503 (1,925). 38,321 all-zero, 143 mixed.
- The corruption is **length-selective and 48-byte-class-selective**. Capture-wide
  **99.63%** of bad names are jemalloc 48 B class (`malloc(nlen+1)` with nlen ≤ 47).
  Sharp onset at **nlen=32** (23,447 bad = 61% of all bad, 0.213% of nlen=32 names);
  nlen 33–35 sit at ~0.04%; only 124 bad names have nlen < 32. cls64 = 78 and
  cls_gt64 = 63 are small sibling clusters of long names (e.g. shard 482 nlen
  48–61; shards 298/501 nlen=64). Shard 362's bad nlen range is **32–43 only**
  (all 16,633 in cls48). The earlier "32–54" figure mixed in the *named*
  histogram's long tail; long-named *good* entries exist in every length, so
  length alone is not the trigger.
- The corruption is **clustered by parent/sibling, not by allocation time**.
  Capture-wide 95% of consecutive-bad dir_id runs have length 1, so this is not
  one jemalloc-run wipe of thousands of adjacent allocations. Two regimes:
  (1) **nlen=32 sibling bursts** — shard 143: 10,830 of 10,860 bad are nlen=32,
  10,009 under one parent at depth 12, max consecutive dir_id run 555; shard 000:
  1,967 nlen=32, 1,931 siblings, depth 11, max run 128. (2) **scattered groups
  in shard 362** — depths 21–25 (peak 24: 10,375), 1,952 parents, max 138 bad
  siblings, 12,256 of 12,728 runs isolated. Mixed names on 362 (40-sample hex
  dump) are **16-byte-lane partial wipes** of otherwise-ASCII sim names
  (`runNo12--X-88.530_Y-…`, `motorcycle-Idx…`): every sample has lead0 ∈
  {0,16,32}; half have exactly 16 or 32 zero bytes. All-zero is the same
  pattern covering the whole `name_len` window.
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

A **zeroing event hits jemalloc 48-byte size-class slots**, in **16-byte
granules**. Both things that get corrupted live in that class:

- catalog name buffers for nlen 32–47 (`malloc(nlen+1)` → 33–48 B → 48 B class),
- catalog hash-table entries (~40 B).

One event still explains both observations: names zeroed in place (full 48 B
slot → all-zero name; 16 or 32 B from an aligned start → the mixed 16-byte-lane
pattern), *and* HT entries zeroed/lost, which breaks chains and makes
`shard_cat_ensure_dir` re-create already existing directories (the duplicates).
The nlen=32 onset is the size-class signature (33 B request). Sibling bursts
of 32-char names are consistent with many same-class allocations sitting in
one run; the 362 scatter says the event is not "wipe one whole run in dir_id
order".

Candidate vectors not yet excluded: a wild 16/32/48-byte `memset`/`bzero` from
an unrelated component, a bad `realloc` move, an `munmap`+re-`mmap` region
handed out by jemalloc while a stale pointer still points into it, or a
use-after-free whose freed run is reused and cleared. A data race in the
production writer is now unlikely (TSan, below).

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
- **Size-class arithmetic** (settled by `probe_forensics` nlen_bad, all 264
  shards): HT entries are ~40 B (jemalloc 48 B class); name blobs are
  `malloc(name_len+1)`, so nlen 32–47 → 48 B class, nlen 48–63 → 64 B class,
  nlen ≥ 64 → larger. 99.63% of bad names are 48 B class. The 64 B / larger
  tail (141 names) is real but small and sibling-clustered.

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
- **TSan on gcc 11 (node9901 / fstor007): no production writer race.** `libtsan`
  **is** present at `/usr/lib/gcc/x86_64-redhat-linux/11/libtsan.so` (the earlier
  "missing on gcc 11.5" note was wrong). Two tripwire+TSan crawls, glibc malloc
  (no jemalloc), 32 crawl / 8 writer threads, 512 shards, `max_open_shards=64`:
  - Synt 20 M entries / 986 k dirs (`/data1/erbmi1/ecrawl-synt`, node9901,
    `tsan-crawl-20260901-134428`): exit 0, bad=0, tripwire empty, no TSan
    reports, `good_nlen>=32=0`, 0 shard evictions.
  - **1.219 B entries / 19.17 M dirs** (`/data1/erbmi1/home-storage-tree` on
    **fstor007-mgmt**, `tsan-crawl-20260901-140356`, copied to gitignored
    `logs/`): 4,836 s, `writer_failed=0`, probe **bad=0** on all 512 shards,
    tripwire empty, **`good_nlen>=32=0`**, 0 evictions / 0 reopens.
    `ecrawl_exit=66` is TSan's "found a warning" status. The **one** report is
    a data race on tripwire-only `g_cat_tripwire` (unsynchronized lazy
    `getenv` cache at `shard_cat_tripwire_enabled`); both threads write `1`.
    Harmless, not in the committed tree. Re-arm with a once-init in `main` (or
    `_Atomic`) so the next TSan run is clean.
  These crawls **do not exercise the n1 signature**: TSan cannot use jemalloc,
  and both trees have **zero** directory names of length ≥ 32, so the 48 B
  name class is empty. They do make a production data race an unlikely cause
  of the NULs.

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

1. **Tripwire a production-scale crawl (jemalloc, not TSan).** Re-apply the
   tripwire first (`git apply -R ~/ecrawl-cat-tripwire.patch`; init
   `g_cat_tripwire` once in `main` so TSan stays quiet if you mix the two), then
   run **jemalloc** `ecrawl` against a large, long tree with **nlen ≥ 32
   directory names** (n1-like: tens of millions of dirs per shard, multi-hour;
   32 crawl / 8 writer threads, 512 shards, default max_open_shards=64).
   `home-storage-tree` (1.2 B files) is the wrong shape: 19 M dirs, **zero**
   names ≥ 32 chars. A trip fires at the moment of corruption and names the
   phase (create vs eviction-write vs final-write). This is still the single
   most informative experiment available.
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
4. **Heap forensics on the bad capture — done (2026-09-01).** `~/probe_forensics.c`
   over all 264 n1 shards (`/data1/erbmi1/nul-forensics-20260901-155824/`, summaries
   in gitignored `logs/nul-forensics/`). Catalog flush is dir_id order, so
   consecutive-bad runs **are** the time-contiguous test: 95% are isolated;
   nlen=32 sibling bursts are the exception. nlen histogram settled 48 B vs 64 B
   (99.63% cls48). Mixed-hex on shard 362 shows 16-byte-lane partial wipes.
   Remaining: a live-heap dump during a reproducing crawl (needs (1)).
5. **TSan — done for a race hunt** on 20 M synt and 1.2 B `home-storage-tree`
   (see ruled-out). No production race; tripwire getenv race only. Does not
   replace (1): need jemalloc + nlen≥32 names + duration.
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
  `~/probe_forensics.c` (per-shard nlen/class/clustering; `--tsv`),
  `~/dump_mixed.c` (hex of mixed-NUL names), `~/nul-forensics-run.sh`,
  `~/probe_twin.c` (twin rate), `~/probe_dup.c` (duplicate groups),
  `~/probe_rootkids.c` (children of pid=1), `~/probe_rootrec.c` (the `/` records),
  `~/harvest_nul.py` (distinct zeroed-name prefixes), `~/cat_chunk_extract.py`
  (reader-independent on-disk check), `~/gen_tree.py` (small synthetic stress
  trees), `~/gen_deep_tree.py` (signature-shaped 36 M-dir tree),
  `~/deep-crawl-loop.sh` (tripwire crawl loop + per-run probe survey).
- n1 forensics: `/data1/erbmi1/nul-forensics-20260901-155824/` on node9901
  (all-shard `.txt` + shard 362 `.tsv` + mixed hex); gitignored copies of dirty
  shards + `CAPTURE_WIDE.txt` in `logs/nul-forensics/`.
- TSan crawls: `logs/tsan-crawl-20260901-134428/` (20 M synt) and the 1.2 B
  pack currently unpacked at `logs/{env.txt,ecrawl.stdout,probe_summary.txt,tsan/,cap/}`
  (`tsan-crawl-20260901-140356` on fstor007; 11 GB of shards in `logs/cap/`).
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
