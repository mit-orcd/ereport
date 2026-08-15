# Build and deploy

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
make ecrawl_query
make edelete
make ecrawl_mount      # optional; needs FUSE headers (see below)
```

Clean:

```bash
make clean
```

### Optional: FUSE for `ecrawl_mount`

`ecrawl_mount` is built only when FUSE 2.x headers are present, so a host without them produces a link line identical to a vanilla build. `make` reports which way it resolved:

```
build: fuse enabled (-l:libfuse.so.2)
build: fuse not found; ecrawl_mount will not be built (try: make fuse-headers)
```

Detection prefers `pkg-config --libs fuse` from `fuse-devel`:

```bash
sudo dnf install fuse-devel     # RHEL / Fedora / EL8+
sudo apt install libfuse-dev    # Debian / Ubuntu
```

Without root, `make fuse-headers` unpacks only the headers from the matching distro RPM into `$(FUSE_PREFIX)` (default `~/.local/fuse-devel`) using `curl` + `rpm2cpio` + `cpio`, then links against the system `libfuse.so.2` — which is already present as part of the base `fuse-libs` package:

```bash
make fuse-headers && make ecrawl_mount
```

The header version is pinned to the distro's `libfuse.so.2` so the ABI matches exactly; override `FUSE_DEVEL_URL` / `FUSE_DEVEL_RPM` on another distro, or `FUSE_PREFIX` to unpack elsewhere. At runtime the host needs `/dev/fuse` and the setuid `fusermount` helper from the `fuse` package; no root is needed to mount. See [tools.md#ecrawl_mount](tools.md#ecrawl_mount).

### Optional: link native binaries against jemalloc

The Makefile auto-detects jemalloc via `pkg-config` and, when `jemalloc-devel` / `libjemalloc-dev` is installed, links all native targets built by `make all` — `ereport`, `ereport_index`, `ecrawl`, `edelete`, `ecrawl_repair`, `ecrawl_query`, and `ecrawl_mount` — against `-ljemalloc`. Install the dev package so `pkg-config` can find it:

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
