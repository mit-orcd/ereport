# ereport

Small C tools for crawling filesystem metadata, writing compact binary crawl data, and turning that data into static HTML reports.

The current toolchain is:

- `ecrawl`: parallel filesystem crawler that writes compact binary records
- `ecrawl_repair`: optional utility that rescans `uid_shard_*.bin` shards, writes **`*.bin.ckpt`** sidecars, **truncates** shards at the last good record when the tail is incomplete, quarantines shards that cannot be fixed, and prints a short **tail-truncation summary** on stderr when anything was shortened (or would be, with **`--dry-run`**)
- `edelete`: parallel filesystem walker (same task-queue / donation model as **`ecrawl`**) that can **`unlink`** non-directory paths—either **everything under a path** (one argument) or only entries older than a chosen **`atime`/`mtime`/`ctime`** threshold (**three** arguments); default run is **dry-run**, with **`--delete`** performing real deletes and **`rmdir`** for directories that become empty (still under the start path). Symlinks are **not** followed.
- `ereport`: report generator that reads crawl output and writes `index.html`, bucket drilldown pages, and a search box that uses the trigram index when you serve the tree over HTTP
- `ereport_index`: search-index helper for path-element substring search
- `eserve.py`: HTTP server for static reports plus **server-side path search** (calls `ereport_index` on the trigram index)

## Default thread counts (per binary)

**Each program has its own defaults.** There is no single global “thread count” for the whole toolchain: `ecrawl`, `ereport`, and `ereport_index` read **different** environment variables and use **different** built-in numbers when those variables are unset.

**Minimum CPUs / RAM** below are practical **floors** for running **with default thread counts** on a dedicated or mostly idle machine: enough logical CPUs that default parallelism is not absurdly oversubscribed, and enough RAM that typical modest workloads are unlikely to fail from memory pressure alone. They are **not** guarantees for huge trees or maximal queue depth—reduce env thread counts on smaller hosts, and expect **`ereport_index --make`** large merges to need **well above** the baseline RAM (merge budget follows host/cgroup **MemAvailable**; wide parallel index runs often use **tens to hundreds of GiB** peak).

| Program | Parallelism role | Override (env) | Built-in default | Min logical CPUs | Min RAM |
|---------|------------------|----------------|------------------|------------------|---------|
| **`ecrawl`** | Walk / queue directory work | **`ECRAWL_CRAWL_THREADS`** | **16** crawl threads (minimum **1**; no fixed maximum) | **4** | **4 GiB** |
| **`ecrawl`** | Flush uid-sharded `.bin` output | **`ECRAWL_WRITER_THREADS`** | **8** writer threads | **4** | **4 GiB** |
| **`ecrawl_repair`** | Parallel rescans; optional **`truncate`** on incomplete tail; checkpoint rebuild / verify | **`ECRAWL_REPAIR_THREADS`** | **16** | **4** | **4 GiB** |
| **`edelete`** | Parallel directory walk; optional **`unlink`** (bounded concurrency in **`--delete`**) | **`EDELETE_THREADS`**, **`EDELETE_MAX_UNLINK_INFLIGHT`** | **16** threads; **256** max concurrent **`unlink`** (**`0`** = unlimited) | **4** | **4 GiB** |
| **`ereport`** | Map/parse `.bin` chunks, emit up to **36** `bucket_*.html` files, live stderr stats | **`EREPORT_THREADS`** | **32** | **8** | **8 GiB** |
| **`ereport_index`** | **`--make`:** parallel chunk-boundary scan, parse workers; **trigram** temp writers default to the **same** count unless **`EREPORT_INDEX_TRIGRAM_THREADS`** is set. **`--search`:** parallel postings load and path filtering when the query and candidate set are large enough | **`EREPORT_INDEX_THREADS`** (and optionally **`EREPORT_INDEX_TRIGRAM_THREADS`**) | **32** | **16** | **16 GiB** |

**Not controlled by those knobs:** `ereport_index --make` **merge** workers (cap **16**, chosen from RAM budget), and **`--resume-merge`** merge workers—see **`EREPORT_INDEX_MERGE_`** vars in the table below.

Details and ranges for each variable appear under each tool’s section and in **Environment variables (quick reference)** at the end of this file.

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
make edelete
```

Clean:

```bash
make clean
```

## systemd: daily `ecrawl` and binary sync

Optional units under **`contrib/systemd/`** run **`ecrawl`** on paths listed in **`/etc/ereport/ecrawl-daily.conf`**, then **`rsync`** each job’s **`output_dir`** (crawl shard data) under **`RSYNC_DEST`** using that directory’s basename as the remote subdirectory (see **`contrib/systemd/ecrawl-daily.conf.example`**).

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

Set **`User=`** / **`Group=`** in **`ecrawl-daily.service`** if the job must not run as root. The same block appears as comments at the top of **`contrib/systemd/ecrawl-daily.service`**.

## Why this is fast (design concepts)

The tools are fast because they combine **compact binary I/O**, **parallelism along natural boundaries**, and **bounded pipelines** instead of naïvely scanning everything twice or holding giant locks over shared mutable state.

### Shared ideas (`ecrawl`, `edelete`, `ereport`, `ereport_index`)

- **Path arguments** — Directory and crawl-root arguments are normalized to **canonical absolute paths** with **`realpath(3)`** once the path exists (see **`path_canon.h`**). Relative inputs are supported; symlink components are resolved. Output directories that are created on demand are canonicalized after **`mkdir`** where applicable.
- **Binary crawl records** — Paths and metadata are stored in a tight on-disk format (file magic **`ERCBIN03`**, format version **3**). Readers parse record headers first and skip or read payloads in bulk instead of parsing text line-by-line.
- **Checkpoint sidecars (`*.bin.ckpt`)** — While crawling, **`ecrawl`** records **record-aligned byte offsets** at a fixed stride into `uid_shard_*.bin.ckpt`. **`ereport`** and **`ereport_index`** load those offsets to split each shard into **valid segments** without a preliminary full-file scan to find boundaries. That enables **many threads** to work on **different byte ranges** of the same file safely (no record torn across workers). If sidecars are missing or stale (for example an interrupted crawl), run **`ecrawl_repair`** on the crawl output directory to rebuild them—and to **truncate** an incomplete last record when possible—see **`ecrawl_repair`** below.
- **Embarrassingly parallel units** — Work is split by **shard file**, **chunk**, **age×size bucket**, or **trigram bucket** so threads rarely contend on the same byte or the same mutex for long.

### `edelete`: parallel deletion (optional age filter)

- **Same crawl parallelism pattern as `ecrawl`** — task queue, worker threads, local directory stacks, and **work donation** so wide trees stay busy across threads.
- **Does not follow symlinks** — traversal uses **`lstat`** / **`fstatat(..., AT_SYMLINK_NOFOLLOW)`**; symlink inodes can be **`unlink`**’d if eligible, but targets are not walked through links.
- **Deletes non-directory paths only** — regular files, symlinks, FIFOs, sockets, etc.; directories are opened and enumerated, then removed with **`rmdir`** only after **`--delete`** when they become empty (deepest first, without ascending above the start path or removing **`/`**).
- **Default is dry-run** — counts **`would_delete`** and prints **`mode=dry-run`**; **`--delete`** prints a summary of resolved path, filter, thread settings, and **`verbose`**, then requires typing **`YES`** on stdin before any **`unlink`** and empty-dir cleanup—unless **`--force`** is also passed (**`--delete --force`** skips the prompt for scripting).
- **Live status line** — rolling **`entries/s(10s)`** and totals on stdout (similar spirit to **`ecrawl`**); **`--verbose`** is currently parsed but does not change output.

Usage:

```bash
./edelete [--delete] [--force] [--verbose] <path>
./edelete [--delete] [--force] [--verbose] <atime|mtime|ctime> <days> <path>
```

The start path is resolved to an absolute path (**`realpath(3)`**). With **one** argument, every non-directory under **`path`** is eligible (still **dry-run** unless **`--delete`**). With **three** arguments, **`days`** selects entries whose chosen timestamp is at least **`days × 86400`** seconds older than wall-clock now.

Environment variables:

| Variable | Meaning |
|----------|---------|
| **`EDELETE_THREADS`** | Parallel crawl workers (default **16**, minimum **1**). |
| **`EDELETE_MAX_UNLINK_INFLIGHT`** | In **`--delete`** mode, max concurrent **`unlink(2)`** calls **across all threads** (default **256**; **`0`** = unlimited). |

Examples:

```bash
./edelete /tmp/staging_area
./edelete --delete /tmp/empty_me
./edelete mtime 90 /storage/scratch/job123
EDELETE_THREADS=32 ./edelete --delete ctime 14 /mnt/cache/tmp
EDELETE_MAX_UNLINK_INFLIGHT=128 ./edelete --delete atime 30 /big/tree
```

Final stdout summary includes **`delete_all`** (**`1`** when the one-argument form was used), **`basis`** / **`age_days`** when the age filter is used, **`force`** (**`1`** if **`--force`** was passed), **`mode`**, scan counts, **`deleted_files`**, **`removed_empty_dirs`**, **`would_delete`**, **`errors`**, throughput metrics, and donation counters.

### `ecrawl`: crawling and capture

- **Parallel crawl threads** feed a **task queue**; multiple threads traverse the tree concurrently while respecting directory boundaries.
- **Uid-sharded output** — Records hash to many **`uid_shard_*.bin`** files so writes spread across descriptors and writer threads; you avoid one giant append-only file and reduce lock contention on a single sink.
- **Separate writer threads** — Crawl threads batch work to **bounded writer queues**; dedicated writers flush shards with large buffered I/O instead of every thread hitting the filesystem independently for every record.
- **Checkpoint rows during write** — Sidecars capture sparse offsets so **later tools** can parallel-read without rescanning from zero.

### `ereport`: reports from crawl bins

- **Parallel chunk mapping** — Uses **`*.ckpt`** to build **chunk lists** (byte ranges that align with record starts). Chunk count scales with file size, so **`EREPORT_THREADS`** has enough units of work.
- **Parallel chunk parsing** — Workers consume disjoint chunks; summaries and bucket histograms merge after workers finish (merge step is **not** on the per-record hot path across all threads).
- **Parallel bucket HTML** — The **36** heat-map cells map to **36** independent output files; emission fans out across threads up to that cap.
- **Cheap mode by default** — Without **`--bucket-details`**, the parser **seeks past** path strings for histogram-only passes, keeping I/O and CPU down when you only need aggregates.

### `ereport_index`: trigram index build (`--make`)

- **Same chunk boundaries as `ereport`** — Parallel chunk readers; rows can skip path bytes when building for a single UID ( **`fseek`** past unmatched records).
- **Ordered path stream** — A single **paths writer** thread appends **`paths.bin`** / **`path_offsets.bin`** in **strict path-id order** so the index remains coherent while **many** trigram workers run.
- **Producer–consumer queues** — Parse workers → paths writer → **trigram job queue** (bounded) → trigram workers → **`tmp_trigrams_*.bin`**. Bounded queues apply **backpressure** instead of unbounded RAM; tuning env vars trades memory vs blocking (see metrics like **`writeq_parse_waits`** / **`trigramq_paths_waits`**).
- **Sliced bulk enqueue** — Trigram jobs from a write batch are enqueued in **slices** that fit current queue depth, so partial capacity is used instead of waiting until an entire batch fits.
- **Batched trigram writes** — For each path, trigram codes are **sorted by trigram bucket**, then **`fwrite`** runs **per contiguous bucket run** under one mutex acquisition per slice — far fewer lock rounds and syscalls than one write per trigram.
- **Lazy open + LRU on bucket files** — Only up to **`EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS`** **`tmp_trigrams_*.bin`** handles stay open; cold buckets are closed and reopened on demand so you do not need thousands of `FILE*` simultaneously.
- **Merge phase** — Temp bucket files are sorted (**radix** on packed records), optionally merged with **parallel workers** subject to a **RAM budget**, with large buffered I/O and **`mmap`** where helpful — separate from index-phase throughput but tuned for large disks.

Together, these choices aim to keep **CPU**, **mutexes**, and **syscalls** off the critical path per byte of crawl data, and to use **disk bandwidth** (especially on NVMe) with large buffered writes instead of tiny random appends per logical record.

## Testing

```bash
make check              # ./test.sh: integration + ecrawl_repair / edelete / ereport_index smokes (tiny /tmp tree; fast)
make check-tree         # ./test_setup.sh then ./test.sh on ./test (needs all binaries built)
```

- **`test.sh`** — Always runs **integration** first: **`ecrawl`** on a tiny synthetic tree under `/tmp`, then **`ereport`** **single-user** (`mtime`, counts vs **`ecrawl`**), then **`ereport`** **all-users** (including **`distinct_uids`**), then **smoke tests** on the same tree: **`ecrawl_repair --dry-run`**, **`edelete`** (default dry-run), **`ereport_index --make`** with a temp **`--index-dir`** (checks **`tri_keys.bin`** / **`paths.bin`** exist). With a **directory argument**, it runs **filesystem correlation**: one **`find`/`fd`** baseline (file/dir/symlink counts and **unique regular-file bytes** via **`find`** `%D:%i`, not **`du`**) is compared to **`ecrawl`** and again to **`ereport` all-users** (terminal-style snapshot vs crawl-derived report totals); **`ecrawl`** is also compared to **`ereport` all-users** (**`entries` ↔ `scanned_records`**, etc.); **single-user** checks are **consistency and subset** vs **`ecrawl`** / all-users when **`ereport` single-user** can load that UID’s shard (**uid-shard** crawls omit empty shards — e.g. **root** uses shard **0**; if nothing maps there, those checks are skipped). **All-users** runs first so correlation still validates **`ecrawl` ↔ `ereport`**. **All** checks print; any failure fails the step. On **busy live trees**, the baseline can drift before **`ecrawl`** completes—expect strict equality mainly on **quiescent** data. **`find`** often exits with status **1** when some subdirectories are unreadable; **`test.sh`** still uses the paths **`find`** printed so the correlation phase does not abort mid-baseline (without this, **`set -e`** / **pipefail** could exit the script with no error line). **Directory counts** use **`find -type d`** (not **`fd`**) for the baseline so the **crawl root** is included—**`ecrawl`** seeds that path and counts it; **`fd`** omits the search-root directory and would disagree by one on a stable tree. **`SKIP_FS=1`** skips only the directory correlation when a path is given. **`ECRAWL`**, **`EREPORT`**, **`ECRAWL_REPAIR`**, **`EDELETE`**, **`EREPORT_INDEX`**, **`ECRAWL_CRAWL_THREADS`**, **`EREPORT_THREADS`**, **`EREPORT_INDEX_THREADS`** override defaults.
- **`test_setup.sh`** — Removes and recreates **`./test`** (default: **`…/ereport/test`**) with a **deep** chain (**`deep/seg001/…`**), a **wide** branch layout (**`wide/b00/…`**), symlinks, hardlinks, and root files. Tune size with **`DEPTH`**, **`BRANCHES`**, **`FILES_WIDE`**.
- **`test_full.sh`** — Runs **`test_setup.sh`** and then **`./test.sh`** on that tree (same as **`make check-tree`**).

Manual sequence:

```bash
./test_setup.sh
./test.sh "$(pwd)/test"
```

## What The Tools Do

### `ecrawl`

`ecrawl` walks a local filesystem tree and writes binary metadata records. It supports:

- parallel crawl threads
- uid-sharded output files
- optional `--no-write` benchmarking mode
- live status output
- separate accounting for:
  - unique regular-file bytes
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

Positional arguments are only **`start-path`** (required) and optionally **`output-dir`**. **`start-path`** must exist; it is canonicalized with **`realpath(3)`** (relative or absolute). After the output directory is created, it is canonicalized the same way. If **`output-dir`** is omitted, a timestamped directory name is created in the current working directory.

Optional environment variables (no CLI flags for these):

| Variable | Meaning |
|----------|---------|
| **`ECRAWL_CRAWL_THREADS`** | Crawl threads (minimum **1**, default **16**; no fixed maximum—practical limits are RAM and OS thread capacity). |
| **`ECRAWL_WRITER_THREADS`** | Writer threads for uid-sharded `.bin` output (default **8**). |
| **`ECRAWL_UID_SHARDS`** | Number of uid shards; must be a **power of two** (default **8192**). |
| **`ECRAWL_MAX_OPEN_SHARDS`** | Per-writer shard file cache target (default **256**); automatically capped against the process open-file limit. |

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
```

Notes:

- `--no-write` crawls and reports metrics without writing shard files.
- `--verbose` enables the full end-of-run diagnostics.
- **`--record-root <path>`** rewrites stored paths: each record’s path becomes `<record-root>/<path-relative-to-start-path>` instead of the live mount path. Use one distinct root per storage server so merged reports and search hits stay identifiable (for example `/storage/srv-a/...` vs `/storage/srv-b/...`). The crawl still walks **`start-path`** on disk; only the strings written into `.bin` files change. The root is turned into an absolute path (relative roots use the current working directory); if that path exists on disk it is also canonicalized with **`realpath(3)`**.

After every run (including non-verbose), stdout includes lightweight queue contention counters (relaxed atomics only; cheap to collect):

- `uid_shards`: uid shard count used for the output layout.
- `max_open_shards`: effective per-writer shard file cache after any open-file-limit auto-cap.
- `writer_failed`: `1` means at least one writer batch failed; the process exits nonzero in this case.
- `wait_crawl_tasks`: crawl-thread wakeups waiting on the crawl task queue (idle crawl threads).
- `wait_writer_push`: crawl-thread wakeups waiting on a **full** uid-shard writer queue (writers falling behind).
- `wait_writer_pop`: writer wakeups waiting on an **empty** queue (crawl threads not feeding writers fast enough).

Interpret these as **counts of blocking episodes**, not wall-clock time.

### `ecrawl_repair`

Use **`ecrawl_repair`** when a crawl directory has **`uid_shard_*.bin`** files but **`ereport`** or **`ereport_index --make`** cannot map chunks because **`.ckpt`** sidecars are missing or invalid—or when you want to confirm sidecars match **`crawl_bin_load_ckpt()`** in **`crawl_bin_chunks.c`** (the shared loader used by **`ereport`** and **`ereport_index`**) before running readers.

It **does not modify the `ecrawl` or `ereport` programs`**; it operates **only** on crawl output files in the directory you pass. Behavior highlights:

- **Parallel shard rescans** — Set **`ECRAWL_REPAIR_THREADS`** (default **16**, minimum **1**).
- **Incomplete tail** — If the record stream fails because the **last record** is truncated (common after a crashed crawl), **`ecrawl_repair`** **`truncate`**s the shard **`.bin`** to the last complete record boundary, rescans, and writes **`*.bin.ckpt`**. If **`truncate`** fails, the shard is treated like other corrupt files (see below).
- **`*.bin.ckpt`** — Written (or overwritten) next to each repaired shard using the same on-disk layout **`ecrawl`** uses.
- **Corrupt / unusable shards** — Shards that cannot be salvaged (bad container header, damaged middle of the file, or truncate failure) are **`rename`**’d into **`corrupt_shards/`** under the crawl directory (optional **`*.bin.ckpt`** beside them moves too when possible). **`ereport`** only scans **`uid_shard_*.bin`** in the crawl directory root, so quarantined files are excluded until you move or fix them manually.
- **Summary line** — After processing, stderr prints aggregate **tail-truncation** stats (shard count, bytes removed, original vs new totals in bytes and human-readable **KiB/MiB/…**) when any truncation occurred—or **would-be** stats with **`--dry-run`**.
- **`--dry-run`** — Does not **`truncate`**, **`rename`** to **`corrupt_shards/`**, or write **`.ckpt`**; still reports what would happen.
- **`--verbose`** — Per-shard progress on stdout (and **`ok`** on successful exit).
- **Exit status** — On a normal run (not **`--dry-run`**), exit **0** means every remaining top-level **`uid_shard_*.bin`** has an **`ereport`-compatible** sidecar; exit **nonzero** if operational errors occurred, verification failed, or every shard was quarantined so **`ereport`** would see **no** shard files.

Usage:

```bash
./ecrawl_repair [--dry-run] [--verbose] <crawl-output-dir>
ECRAWL_REPAIR_THREADS=32 ./ecrawl_repair /path/to/crawl-out
```

### `ereport`

`ereport` reads crawl output and builds an HTML report under **`./<username>/`** (resolved login name), unless that name is unusable—then it falls back to `./tmp/`. If you **omit the username** and pass only the time basis (`./ereport atime …`), it aggregates **all UIDs** present in the crawl and writes under **`./all_users/`**. If the **first token is not** a time keyword and **does not** resolve as a login/UID, it is treated as the first **`bin_dir`** and the run is **all-users** with **effective** age buckets (**`max(atime,mtime,ctime)`** per file). For **single-user** runs, omitting the time argument also selects **effective**.

Outputs:

- `./<username>/index.html` — heat map, **path search** box (uses server-side index when served via `eserve`), full statistics below the table
- `./<username>/bucket_aX_sY.html` — per age/size cell; **brief summary HTML** unless you pass **`--bucket-details N`** (see below). With **`--bucket-details`**, each page lists directory rollup tables for **N** path levels below the shared prefix inside that bucket.
- `./all_users/` — same layout; **`./all_users/bucket_aX_sY.html`** is a brief summary unless **`--bucket-details`** is used (heat-map totals on `index.html` always match the crawl).

Place **`--bucket-details N`** (`N` = **1…32**) **first**, before the username (if any) and time basis. Omit it for fast runs and small bucket HTML (no path reads for drill-down tables).

**Search UI (important):** There is **only one** search field. Results stay **below that box**—not in a sliding drawer—so it stays obvious what you are editing. After three characters you get a preview list under the input; press **Enter** for paged results in the same panel. Use **Hide** to collapse the panel without clearing your query. In-flight preview requests are **aborted** when the query changes so fast typing does not leave **stale** matches on screen. The preview and paged-result lines include timing plus corpus scale: **`indexed_paths`** from the index’s **`meta.txt`** (shown as “~N paths indexed”) when that value is present; otherwise they fall back to **`index_keys`** (distinct trigrams in **`tri_keys.bin`**, shown as “~N trigrams”).

**Heat map (index.html):** Each age×size cell is split **diagonally**: the **upper-right** triangle shows **data volume** and **share of total bytes** (blue intensity); the **lower-left** shows **file count**, a short rounded count in parentheses when large **(e.g. 2M)**, and **share of total files** (rose intensity), with the parenthetical line on a **second** line in a small badge like the byte **%** badge. **Inner** bucket colors scale so the **strongest inner cell** reaches full saturation; **row totals** and **column totals** scale against the **full corpus (100%)** instead, so the margin labels stay comparable to “fraction of everything.” **Column** headers (size buckets) use a light **blue** tint; **row** headers (age buckets) use a light **rose** tint; the **“Age × Size”** corner and the **“Total”** row/column **label** cells stay **neutral** (no axis tint). The table uses **`class="heatmap"`** so those styles do not fight generic **`th`** rules elsewhere.

Usage:

```bash
./ereport [--bucket-details N] <username|uid> [<atime|mtime|ctime|effective>] [bin_dir ...]
./ereport [--bucket-details N] [<atime|mtime|ctime|effective>] [bin_dir ...]   # all users → ./all_users/
```

If you **omit every `bin_dir`**, `ereport` reads crawl `.bin` files from the **current working directory** (`./`).

The **first argument** is treated as a time basis (`atime`, `mtime`, `ctime`, or **`effective`**) only when it matches exactly—otherwise it is interpreted as a username or numeric UID, or (if that fails) as the start of the **`bin_dir`** list for an **all-users** run with **effective** time. You cannot name an account literally `atime` without resolving that ambiguity (e.g. numeric UID).

Thread count: set **`EREPORT_THREADS`** (default **32**). This controls parallel **`.bin` chunk readers** during the scan, **parallel emission of `bucket_aX_sY.html`** (36 heat-map cells), and the **stats** thread. It is **not** set on the command line.

**Multiple crawl directories:** pass several **`bin_dir`** paths (each an `ecrawl` output folder). Every directory must use the same layout (**unsharded** flat bin set vs **`uid_shards`**) and the same **`uid_shards`** count when manifests are present. For each directory, `ereport` loads that user’s shard file when you specify a user (uid-sharded layout), **or every shard file** when aggregating all users; **unsharded** layouts still load all matching bins. So twenty servers mean twenty directories and twenty shard files merged into one report (per-user), or all shards from every directory (all-users mode).

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
```

Parse chunks scale with input `.bin` size so parallel workers are not capped by a tiny chunk count.

**Bucket drill-down:** By default, **`ereport` does not read path strings** into per-bucket tables—the parser seeks past path bytes and **`bucket_aX_sY.html`** files stay short summaries. Pass **`--bucket-details N`** so **`ereport` reads paths** and emits **N** directory-level rollup tables per bucket page (`N` between **1** and **32**). This applies to **single-user** and **all-users** runs; larger **`N`** and all-users crawls cost more I/O and memory. Each level table lists directories **sorted by bucket bytes** (largest first); if more than **200** directories exist at that depth, only the **top 200** rows are written and a short note appears under the section heading (totals in the heat map and bucket header still reflect the full bucket).

Runtime behavior:

- **`ereport` scans crawl directories**, then **maps chunk boundaries** inside each `.bin` shard (reading record headers only). That mapping runs with **`EREPORT_THREADS` parallel scanners**; stdout shows **`chunk-map files:X/Y`** until every shard has been scanned, then the usual records/sec line appears while workers parse chunks. If mapping is slow, an occasional **stderr** advisory may print after a completed progress line so it does not glue to the **`chunk-map`** status text.
- After parsing finishes, the status line switches to **finalizing** with sub-steps: **merging shard summaries** (in-process merges of per-thread summaries and bucket-detail maps), **writing bucket HTML (n/36)** while **`bucket_*.html`** files are emitted, then **writing index.html**.
- prints a live progress line during processing
- prints final run stats to `stdout`
- writes warnings/errors to `stderr`
- uses local progress counters with chunked flushes to avoid per-record atomics in the hot path

Interactive search in `index.html` requires **`ereport_index --make`** (see below), **`eserve`** running with **`ereport_index`** available, and opening the report **over HTTP** (browser `fetch` does not work reliably from `file://`).

### `ereport_index`

`ereport_index` builds and searches an on-disk trigram index over crawl path strings—either for **one resolved Unix user** or for **every UID** when **no user** is selected (see **`--make`** disambiguation below; same idea as `ereport` all-users mode: all uid-shard files, no UID filter on records).

The search is a case-insensitive substring match on **individual path segments** (slashes separate segments; matches do not span `/`). For example:

```text
doc
```

matches paths such as:

```text
/path/foo/alice/...
/path/foo/acme-docs/...
/path/foo/doc/...
```

Queries must be **at least three characters** (trigram filtering).

Usage:

```bash
./ereport_index --make [--index-dir <path>] [username|uid] [bin_dir ...]
./ereport_index --resume-merge --index-dir <path>
./ereport_index --search [--index-dir <path>] <term> [--json] [--skip N] [--limit M]
```

**`--make` user vs all-users:** If the **first** argument after optional **`--index-dir`** is a valid login name or numeric uid on this system, it names the report user and any further arguments are crawl directories (default **`./`**). If that first token is **not** a known user (for example it is a crawl output directory name), **every** argument—including the first—is treated as a **`bin_dir`**, and the index is built for **all UIDs** under **`./all_users/index/`** unless **`--index-dir`** overrides the location (same merge semantics as **`ereport`** aggregate output). **`./ereport_index --make`** with nothing after **`--make`** indexes **`./`** for all users.

You can pass **multiple** **`bin_dir`** paths (same merged crawl directories as for **`ereport`**); they are merged into one index.

Examples:

```bash
./ereport_index --make alice host-b-mgmt_apr-17-2026_15-07-17
./ereport_index --make /path/to/crawl-out
./ereport_index --make crawl_srv01 crawl_srv02
./ereport_index --make --index-dir /var/lib/example-search alice crawl_a crawl_b
./ereport_index --resume-merge --index-dir /var/lib/example-search
./ereport_index --search --index-dir alice/index doc
./ereport_index --search --index-dir all_users/index doc
./ereport_index --search --index-dir /var/lib/my-index doc
./ereport_index --search --index-dir alice/index doc --json --skip 0 --limit 20   # JSON body for APIs
```

Default behavior:

- **`--make [--index-dir <path>]`** — without **`--index-dir`**, **`--make <user> …`** writes under **`./<username>/index/`** (`paths.bin`, `tri_keys.bin`, etc.). With **`--index-dir`**, those files go directly under **`<path>`** (the directory is created if needed).
- **`--make`** with only crawl directories (first token not a system user) writes under **`./all_users/index/`** unless **`--index-dir`** is set.
- **`--make`** with only **`<username|uid>`** and no **`bin_dir`** arguments reads crawl input from **`./`** (same idea as `ereport`).
- **`--search [--index-dir <path>] <term>`** — optional **`--index-dir`** (same flag as **`--make`**). If omitted, the index directory defaults to **`./index`** relative to the current working directory.
- **`EREPORT_INDEX_THREADS`** — optional; if set to an integer in **1…4096**, sets parallelism for **`--make`** (default **32** when unset or invalid): **chunk-boundary mapping** runs with up to this many scanners across distinct input `.bin` files (capped by file count), and **parse/index** uses the same count for **parallel chunk readers** (parse workers). **Trigram writers** default to the same count; override with **`EREPORT_INDEX_TRIGRAM_THREADS`** (below). **This does not set trigram merge worker count.** Merge uses up to **16** workers by default, capped by **available RAM** (each merge worker may hold about **2× the largest `tmp_trigrams_*.bin` bucket** in memory during sort). Tune merge parallelism with **`EREPORT_INDEX_MERGE_RAM_FRAC`** / **`EREPORT_INDEX_MERGE_MEMORY_MB`** (see below). Raising thread count increases peak RAM mostly by having more workers fill **bounded** queues (paths writer depth is **`EREPORT_INDEX_WRITEQ_MAX_BATCHES`**).
- **`EREPORT_INDEX_TRIGRAM_THREADS`** — optional; parallel writers appending to **`tmp_trigrams_*.bin`** during **`--make`**. Defaults to **`EREPORT_INDEX_THREADS`** (same integer range **1…4096**). Use when trigram temp I/O is the bottleneck and you can afford more concurrent bucket files (subject to **`EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS`** / **`ulimit -n`**).
- **`EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH`** — optional; bounded queue of path jobs between the paths writer and trigram workers (range **512…262144**). When unset, default depth scales with **`EREPORT_INDEX_TRIGRAM_THREADS`** (**64×** workers, minimum **4096**, capped at **16384**) so high parallelism does not starve workers as easily; override explicitly for more headroom (uses more RAM).
- **`EREPORT_INDEX_WRITE_BATCH_PATHS`** — optional; target number of paths per batch handed to the paths writer (default **4096**, range **512…65536**). The effective flush size is also scaled down when **`EREPORT_INDEX_THREADS`** is high (see **`write_batch_flush_at`** in **`--make`** stats).
- **`EREPORT_INDEX_WRITEQ_MAX_BATCHES`** — optional override (**4…4096**) for how many **write batches** may wait on the single writer thread during **`--make`**. Default scales with **`EREPORT_INDEX_THREADS`** (about **threads/3**, clamped **6…96**). Raising this raises peak memory if workers outpace the writer; lowering it adds backpressure (workers block until the writer drains).
- **`EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS`** — optional cap (**32…4096**) on how many **`tmp_trigrams_*`** bucket **`FILE*`** handles each trigram worker may keep open (LRU); unset defaults to **4096**, then split across workers using an assumed fd budget (see **`ulimit`** below). Lower this if you hit **`EMFILE`**.

#### Open files (`ulimit`)

**`ereport_index --make`** uses many descriptors (parallel trigram shards, merge I/O, crawl inputs). Before large builds run:

```bash
ulimit -n 65535
```

(**65535** or higher.) If **`Too many open files`** persists, raise **`ulimit -n`** further, lower **`EREPORT_INDEX_THREADS`**, or set **`EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS`** lower. If **`ulimit -n`** cannot go high enough, raise the **hard** limit (often **`/etc/security/limits.conf`** or **`systemd`** **`LimitNOFILE=`**) and open a new shell.

JSON search output is one UTF-8 JSON object per line (fields mirror **`ereport_index --help`**):

```json
{"total":123,"skip":0,"limit":50,"search_ms":4,"index_keys":63000,"indexed_paths":2000000,"paths":["...","..."]}
```

- **`index_keys`** — number of distinct trigram keys in **`tri_keys.bin`**.
- **`indexed_paths`** — corpus size from **`meta.txt`** (**`indexed_paths=`**), aligned with **`paths.bin`** entry count (**`0`** if **`meta.txt`** is missing or unreadable).
- **`search_ms`** — server-side search duration for that request.

If **`--json`** is given without **`--limit`**, **limit defaults to 50**.

### Index build pipeline (`--make`)

Roughly two phases:

1. **Scan / index** — Input `.bin` files are listed, then **chunk boundaries are mapped in parallel** (same idea as `ereport`, using **`EREPORT_INDEX_THREADS`**, capped by how many bin files exist). Parallel workers then read each chunk (record headers first). For a **single-user** build, rows whose UID does not match skip reading the path string (`fseek` past it). For an **all-users** **`--make`** (no resolved username as the first argument), every matched layout row’s path is indexed (no UID filter). Parsed paths are batched to a **paths writer** thread that appends **`paths.bin`** and **`path_offsets.bin`** in **strict order**, then enqueues **linked trigram jobs from each write batch** to the trigram queue in **slices** that fit current free depth (fewer mutex rounds than per-path enqueue, without stalling until an entire batch fits). **Multiple trigram writer threads** append **`tmp_trigrams_*.bin`** shard files in parallel (lazy **`FILE*`** handles and **LRU eviction** per **`EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS`**; requires sufficient **`ulimit -n`**). For each path, trigram codes are **sorted by bucket** and written with **batched `fwrite`s** per bucket (fewer lock rounds and syscalls than one write per trigram). Chunk input files use large stdio buffers; trigram code lists use a cheap hybrid sort/dedup for uniqueness. The **trigram job queue** depth defaults to **4096** parsed paths between the paths writer and trigram workers; override with **`EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH`** (see above).

2. **Merge** — Temp per-bucket trigram files are sorted and merged into `tri_keys.bin` + `tri_postings.bin`. When enough buckets have data, merge runs **multiple worker threads** that write per-bucket segment files, then a single thread **stitches** postings offsets and concatenates blobs. Reads prefer **`mmap`** with `malloc`+`read` fallback; sorting uses **LSD radix** on packed records. During indexing, the builder records which trigram buckets were touched so merge can skip `stat`-ing thousands of empty bucket paths. All heavy merge I/O uses large buffers.

At **`--make`** start, **`ereport_index`** prints one stderr line with effective **parse workers**, **trigram workers**, **trigram job queue depth**, and **write batch path cap**. Final stdout stats include **`trigram_queue_depth`** and **`write_batch_paths`** for the run.

**Merge RAM cap (OOM avoidance):** Parallel merge worker count is limited so that `workers × (≈2 × largest bucket file + overhead)` stays under a **budget** derived from **`min(MemAvailable, cgroup v2 memory.max)`** × **`EREPORT_INDEX_MERGE_RAM_FRAC`** (default **55%**). Override the budget with **`EREPORT_INDEX_MERGE_MEMORY_MB=<MiB>`** (explicit cap) or **`EREPORT_INDEX_MERGE_RAM_FRAC`** (e.g. `0.6`). Session cgroups often enforce a limit far below host RAM—raising threads without raising the cgroup cap can still OOM.

#### `ereport_index.log` (memory snapshot)

During **`--make`** and during **`--resume-merge`**, a background thread appends one line every **8s** to **`ereport_index.log`** under the index directory (`wb` = paths-writer queue bytes, `tj` = trigram job queue, `cq` = chunk-queue estimate, `mp` = estimated merge RAM peak). On **resume-merge**, only **`mp`** is meaningful (other fields stay **0**).

#### `--resume-merge` (interrupted merge)

If **`--make`** finished the **indexing** phase (`paths.bin` + `path_offsets.bin` exist) but died during **merge** (OOM, kill, etc.), you can finish trigram output without re-scanning crawl data:

```bash
./ereport_index --resume-merge --index-dir /path/to/index
```

Resume **deletes** any partial **`tri_keys.bin`** / **`tri_postings.bin`**, removes orphan **`merge_seg_k_*` / `merge_seg_p_*`** halves left by a crash, converts remaining **`tmp_trigrams_*.bin`** to segment files, then **stitches** all segment pairs into new **`tri_*`** files and writes **`meta.txt`**.

**Cannot resume** when leftover **`tmp_trigrams_*.bin`** exist **and** a non-empty **`tri_keys.bin`** exists **but** there are **no** **`merge_seg_*.bin`** files (that pattern means **single-thread** merge was interrupted; data lived only in the partial `tri_*` stream). In that case run a full **`--make`** again.

**`EREPORT_INDEX_THREADS`** does not apply to resume-merge merge workers (same merge RAM rules as above).

### `--make` summary metrics (stdout)

Typical keys include:

- **Where time goes:** `chunk_prep_sec` (parallel chunk-boundary mapping only) vs `index_phase_sec` (wall clock from run start through closing `paths.bin` / `path_offsets.bin`, including scan, prep, directory setup, parallel parse + writers + draining queues) vs `merge_phase_sec` vs `wall_after_index_sec` (merge + `meta.txt` and similar; should be ≈ `merge_phase_sec` plus tiny overhead). `elapsed_sec` is end-to-end. **`avg_paths_per_sec` divides by `elapsed_sec`**, so it understates peak index throughput if merge is fast; use **`index_paths_per_sec`** (paths ÷ `index_phase_sec`) for overall index-stage rate.
- **CPU by phase (Linux `getrusage`, all threads summed):** on successful **`--make`**, lines prefixed `cpu_prep_`, `cpu_idx_`, `cpu_mrg_`, `cpu_make_` report user/sys CPU seconds, voluntary/involuntary context switches, and minor/major page faults between phase boundaries (`_cpu_user_sec`, `_cpu_sys_sec`, `_ctx_sw_vol`, etc.).
- **Stdio/POSIX I/O counts for `--make`:** `make_fread_*`, `make_fwrite_*`, `make_fopen_calls`, `make_fclose_calls`, `make_open_calls`, `make_read_*`, `make_mmap_calls`, `make_munmap_calls`, `make_trigram_append_batches` (once per **batched** append to `tmp_trigrams` for a bucket—much smaller than `trigram_records`)—useful for tuning syscalls / temp I/O behavior.
- Throughput and scale: `scanned_records`, `indexed_paths`, `trigram_records`, `unique_trigrams`, `index_workers`, `index_trigram_workers`, `writeq_max_batches`, `write_batch_flush_at` (queue backpressure and paths-per-batch flush tuning).
- Merge: `merge_phase_sec`, `merge_workers` (after RAM cap), `merge_workers_cpu` (CPU-based choice before cap), `merge_max_bucket_mib`, `merge_parallel_ram_budget_mib`, `merge_buckets_nonempty`, `merge_buckets_skipped`, `merge_trigram_records_read`, byte counts for temp reads and final `tri_*` outputs, derived `*_per_sec` rates.
- **Queue wait counters** (each increment only when a thread blocks on `pthread_cond_wait`; **no overhead on the non-blocking fast path**): `writeq_writer_waits` (paths writer starved for parse batches), `writeq_parse_waits` (parse workers blocked because the paths writer queue is full), `trigramq_paths_waits` (paths writer blocked because the trigram job queue is full), `trigramq_worker_waits` (trigram workers idle waiting for jobs). High counts usually mean raising **`EREPORT_INDEX_WRITEQ_MAX_BATCHES`** or **`EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH`**, tuning **`EREPORT_INDEX_THREADS`** / **`EREPORT_INDEX_TRIGRAM_THREADS`**, or faster trigram temp I/O (subject to RAM and disk).

Successful **`--resume-merge`** prints a shorter set (`mode=resume_merge`, `index_dir`, `indexed_paths`, `unique_trigrams`, merge timings and sizes, `elapsed_sec`).

### Index format and on-disk layout

Current index format:

- disk-native and binary
- lowercased trigram postings for search
- original full paths stored separately for result lookup
- intended to be read directly by a later server-side helper

Current files under `<username>/index/`:

- `meta.txt` — small key/value record written at end of **`--make`** / **`--resume-merge`** (includes **`indexed_paths=`** corpus size and format/version fields)
- `path_offsets.bin`
- `paths.bin`
- `tri_keys.bin`
- `tri_postings.bin`

During merge, transient `tmp_trigrams_*.bin` files are removed as buckets are processed. Parallel merge may create short-lived `merge_seg_k_*` / `merge_seg_p_*` segment files under the same directory; successful runs delete them after the stitch step. **`--resume-merge`** also drops orphan half-segment files if a crash left only one of the pair.

### `eserve.py`

HTTP server for generated HTML/CSS and bucket pages. It also implements **`GET …/search`**, which runs **`ereport_index --search`** against the configured trigram index (default: next to the report; see **Search index location** below).

**Requirements**

- Python **3** on PATH (`python3`).
- **`ereport_index`** available:
  - sibling **`./ereport_index`** next to **`eserve.py`** (after **`make ereport_index`**), **or**
  - on **`PATH`**, **or**
  - override with **`EREPORT_INDEX_BIN=/absolute/path/to/ereport_index`**.
- **Search index location** (directory containing **`tri_keys.bin`**, etc.):
  - default: **`<report>/index`** next to **`index.html`** (or **`SERVE_ROOT/<user>/index`** for **`GET /<user>/search`**);
  - override: **`--index-dir DIR`** or **`EREPORT_SEARCH_INDEX_DIR`** (CLI wins). The same directory is used for every **`…/search`** request and may live **outside** **`SERVE_ROOT`**.

**Make targets** (from the repo root):

```bash
make serve              # bind 127.0.0.1, port 8000 by default
make serve-public       # bind 0.0.0.0 (all interfaces), same default port
```

**Variables** (passed to `make`):

| Variable      | Default   | Meaning                          |
|---------------|-----------|----------------------------------|
| `SERVE_ROOT`  | `.`       | Directory whose files are served (last argument to **`eserve.py`**) |
| `SERVE_PORT`  | `8000`    | TCP port (both **`serve`** and **`serve-public`**) |
| `SERVE_BIND`  | `127.0.0.1` | For **`make serve`** only: **`eserve.py --bind`**. **`make serve-public`** always uses **`0.0.0.0`** and does not read **`SERVE_BIND`**. |
| `SERVE_INDEX_DIR` | (empty) | If set, passed to **`eserve.py --index-dir`**. Use **`SERVE_INDEX_DIR=/abs/path`** (one `=`); **`==/path`** is a common typo—**`eserve.py`** strips leading `=` so absolute index paths still work. |
| `PYTHON3`     | `python3` | Python interpreter used to run **`eserve.py`** |

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

**Search HTTP API** (used by `index.html` via relative `fetch`):

| URL pattern | When to use |
|-------------|-------------|
| **`GET /search?q=…&skip=…&limit=…`** | `SERVE_ROOT` **is** the report directory (e.g. `./alice`) so `index.html` is at the root of the site. |
| **`GET /<user>/search?q=…`** | `SERVE_ROOT` **contains** the user folder (e.g. SERVE_ROOT=. and files live under `./alice/`). |

Parameters:

- **`q`** — search string (**≥ 3** characters after trimming).
- **`skip`** — offset into the ranked match list (default `0`).
- **`limit`** — page size (default `50`; capped server-side).

Successful responses are **`application/json`** from **`ereport_index`**: **`total`**, **`skip`**, **`limit`**, **`search_ms`**, **`index_keys`**, **`indexed_paths`**, and **`paths`** (string array). Errors return JSON or plain HTTP errors depending on failure mode.

Direct **`python3`** invocation:

```bash
python3 eserve.py --bind 127.0.0.1 --port 8000 /path/to/serve
python3 eserve.py --bind 0.0.0.0 --port 8080 ./alice
# Index built elsewhere (single index for all /search and /<user>/search calls):
python3 eserve.py --index-dir /data/report_index /path/to/serve
```

## Typical Workflow

For **multiple crawl outputs** (e.g. several servers), run **`ecrawl`** once per output directory (often with distinct **`--record-root`** values), then pass **all** of those directories to one **`ereport`** / **`ereport_index --make`** command so the report and search index stay unified. For an **all-users** aggregate report (`./ereport ctime …`), build the matching index with **`ereport_index --make dir1 dir2 …`** where the **first** argument is **not** a valid username/uid on this host—pass the same **`bin_dir`** list as for **`ereport`** (for example directory names only).

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

One crawl output directory (default index location is **`./<username>/index/`**):

```bash
./ereport_index --make alice host-a_apr-17-2026_15-03-01
```

Several crawl output directories (merged index for the same user):

```bash
./ereport_index --make alice crawl_srv01 crawl_srv02 crawl_srv03
```

Omit directories to use **`./`** as the only input path: `./ereport_index --make alice`.

**All-users** index (same crawl inputs as `./ereport ctime …`, output beside **`./all_users/`**). Omit a username: list crawl dirs first so the first token is **not** resolved as a login or uid:

```bash
./ereport_index --make crawl_srv01 crawl_srv02
./ereport_index --search --index-dir all_users/index foo
```

This writes:

```text
alice/index/meta.txt
alice/index/path_offsets.bin
alice/index/paths.bin
alice/index/tri_keys.bin
alice/index/tri_postings.bin
```

### 4. Serve the results over HTTP

Pick **`SERVE_ROOT`** depending on how you want URLs to look:

**Option A — Serve the user directory directly** (`index.html` at site root):

```bash
make serve-public SERVE_ROOT=./alice SERVE_PORT=8000
```

Open **`http://127.0.0.1:8000/index.html`**. Search requests go to **`http://127.0.0.1:8000/search?q=…`** (handled by `eserve.py`).

**Option B — Serve a parent directory** (URL includes username):

```bash
make serve-public SERVE_ROOT=. SERVE_PORT=8000
```

Open **`http://127.0.0.1:8000/alice/index.html`**. Search requests resolve to **`http://127.0.0.1:8000/alice/search?q=…`**.

Ensure **`ereport_index`** is built (`make ereport_index`) or set **`EREPORT_INDEX_BIN`** before starting the server.

## Validation Helpers

`test.sh` behavior is summarized under **Testing** above (integration vs optional filesystem correlation; **`find`/`fd`** vs **`ecrawl`** and vs **`ereport` all-users**; **`ecrawl`** vs **`ereport`**; **single-user** subset checks).

Example:

```bash
./test.sh /path/to/test-correlation-root
```

Used during development and benchmarking; not part of the normal end-user workflow.

## Output Semantics

### `ecrawl` byte totals

`ecrawl` reports:

- `total_bytes`: unique regular-file bytes
- `dir_apparent_bytes`: apparent size of directories
- `symlink_apparent_bytes`: apparent size of symlinks
- `other_apparent_bytes`: apparent size of other matched types
- `apparent_bytes_total`: sum of all of the above

This means:

- `total_bytes` is closer to deduped regular-file data volume
- `apparent_bytes_total` is closer to `du --apparent-size`

### `ereport` capacity totals

`ereport` currently reports:

- `total_capacity_in_files`
- `total_capacity_in_others`

where `total_capacity_in_files` is based on matched file records and hard-link-aware byte accounting from the crawl input.

## Environment variables (quick reference)

Defaults below are the **built-in** values when the variable is **unset**—each tool uses **its own** defaults (see **Default thread counts (per binary)** above).

| Variable | Tool / context | Role |
|----------|----------------|------|
| **`ECRAWL_CRAWL_THREADS`** | `ecrawl` | Crawl threads (minimum **1**, default **16**; no fixed maximum). |
| **`ECRAWL_WRITER_THREADS`** | `ecrawl` | Uid-shard writer threads (default **8**). |
| **`ECRAWL_UID_SHARDS`** | `ecrawl` | Uid shard count, power of two (default 8192). |
| **`ECRAWL_MAX_OPEN_SHARDS`** | `ecrawl` | Per-writer shard file cache target, auto-capped by `RLIMIT_NOFILE` (default 256). |
| **`ECRAWL_REPAIR_THREADS`** | `ecrawl_repair` | Parallel shard rescans, tail salvage **`truncate`**, checkpoint rebuild (default **16**, minimum **1**). |
| **`EDELETE_THREADS`** | `edelete` | Parallel walk workers (default **16**, minimum **1**). |
| **`EDELETE_MAX_UNLINK_INFLIGHT`** | `edelete` **`--delete`** | Max concurrent **`unlink`** syscalls across all workers (default **256**; **`0`** = unlimited). |
| **`EREPORT_THREADS`** | `ereport` | Parallel **`.bin` chunk readers**, parallel **`bucket_*.html`** emission, and stats thread (default **32**). |
| **`EREPORT_INDEX_THREADS`** | `ereport_index --make` / **`--search`** | Parallel chunk-boundary mapping, index **parse** workers, and (for **`--search`**) parallel postings load + path filtering when the query and candidate set are large enough (default **32**). Does **not** set merge worker count. Trigram temp writers default to this count unless **`EREPORT_INDEX_TRIGRAM_THREADS`** is set. |
| **`EREPORT_INDEX_TRIGRAM_THREADS`** | `ereport_index --make` | Parallel writers to **`tmp_trigrams_*.bin`** (default: **same as `EREPORT_INDEX_THREADS`** when unset). |
| **`EREPORT_INDEX_TRIGRAM_QUEUE_DEPTH`** | `ereport_index --make` | Bounded queue between paths writer and trigram workers (default scales with trigram thread count; range **512…262144**). |
| **`EREPORT_INDEX_WRITE_BATCH_PATHS`** | `ereport_index --make` | Base paths-per-batch to the writer (default **4096**, range **512…65536**; scaled when thread count is high). |
| **`EREPORT_INDEX_WRITEQ_MAX_BATCHES`** | `ereport_index --make` | Max depth of batches waiting on the **paths** writer (default scales with thread count). |
| **`EREPORT_INDEX_MAX_OPEN_TRIGRAM_BUCKETS`** | `ereport_index --make` | Per-worker LRU cap on **`tmp_trigrams_*`** shard **`FILE*`** handles (**32…4096**; default **4096**). Use a high **`ulimit -n`** for large **`--make`** (see README). |
| **`EREPORT_INDEX_MERGE_MEMORY_MB`** | `ereport_index --make` / merge / **resume-merge** | Explicit merge RAM **budget** (MiB) for limiting parallel merge workers (optional). |
| **`EREPORT_INDEX_MERGE_RAM_FRAC`** | `ereport_index --make` / merge / **resume-merge** | Fraction of `min(MemAvailable, cgroup memory.max)` used as that budget (default **0.55**). |
| **`EREPORT_INDEX_BIN`** | `eserve.py` | Absolute path to `ereport_index` if not on `PATH` / next to `eserve.py`. |
| **`EREPORT_SEARCH_INDEX_DIR`** | `eserve.py` | Trigram index directory (**`tri_keys.bin`**). Overridden by **`--index-dir`**. |

## Source layout

- **`edelete.c`** — standalone parallel walker / deletion utility (**`path_canon.h`** only).
- **`crawl_ckpt.h`** — shared on-disk checkpoint layout for **`uid_shard_*.bin.ckpt`** sidecars; included by **`ecrawl`**, **`ereport`**, **`ereport_index`**, and **`ecrawl_repair`**.
- HTML **emitters** in **`ereport.c`** follow a common argument order where practical: output path / `FILE*` target first, then **`username`**, **`all_users`**, **`distinct_uids`**, **`basis_str`**, then function-specific fields (e.g. age/size bucket indices, detail levels).

## Notes

- The code assumes local filesystem crawl data written by `ecrawl` format version **3** (per-shard **`uid_shard_*.bin.ckpt`** sidecars record sparse byte offsets for parallel chunk mapping in `ereport` / `ereport_index --make`). Use **`ecrawl_repair`** to regenerate missing sidecars without re-crawling, and optionally **`truncate`** shards whose **last record** was cut off mid-write.
- `uid_shard_*.bin` layout is preferred and automatically detected via `crawl_manifest.txt`.
- For **per-user** runs, `ereport` and `ereport_index --make` read only the uid-shard files relevant to that user when uid-sharded input is available. **All-users** runs load **every** shard file (same as merging full-cluster crawls).
- **`ECRAWL_UID_SHARDS`** for a crawl run should match across every output directory you later pass together to **`ereport`** / **`ereport_index --make`** (merged reports assume consistent shard layout).
- The **`Makefile`** targets **`serve`** and **`serve-public`** run **`$(PYTHON3) eserve.py`**: they forward **`SERVE_ROOT`**, **`SERVE_PORT`**, and optionally **`SERVE_INDEX_DIR`**; **`SERVE_BIND`** applies only to **`serve`** (**`serve-public`** binds **`0.0.0.0`** in the recipe).

## License

This project is licensed under the **MIT License**. See **[LICENSE](LICENSE)**. Copyright is held by **Michel Erb** (2026).
