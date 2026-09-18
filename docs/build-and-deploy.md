# Build and deploy

## Build

```bash
make                    # every binary; prints one line each for jemalloc and FUSE detection
make ecrawl ereport     # individual targets
make clean
```

Required: gcc, pthreads, libzstd (`zstd-devel` / `libzstd-dev`; Homebrew `zstd` on macOS). Optional: jemalloc and FUSE 2, both auto-detected with `pkg-config`.

### Optional: jemalloc

With `jemalloc-devel` (RHEL/Fedora, EPEL) or `libjemalloc-dev` (Debian/Ubuntu) installed, every native binary links `-ljemalloc`; without it the build is a byte-identical glibc-malloc build. Force with `make JEMALLOC_LIBS=-ljemalloc` or disable with `make JEMALLOC_LIBS=`. On a 14.9M-path crawl it made `ereport_index --make` ~27% faster; `ecrawl` gains nothing. Deployment hosts then need `libjemalloc.so.2` at runtime.

### Optional: FUSE for `ecrawl_mount`

Linux only (skipped on macOS). Built when FUSE 2.x headers are found:

```bash
sudo dnf install fuse-devel      # or: sudo apt install libfuse-dev
make ecrawl_mount
```

Without root on RHEL/Rocky, where `libfuse.so.2` is in the base `fuse-libs` package but the headers are not:

```bash
make fuse-headers && make ecrawl_mount    # unpacks only the headers of the matching RPM into ~/.local/fuse-devel
```

Override `FUSE_DEVEL_URL` / `FUSE_DEVEL_RPM` for another distro, `FUSE_PREFIX` for another location. Mounting needs `/dev/fuse` and the setuid `fusermount` helper; no root.

## systemd: daily crawl + sync

`contrib/systemd/` runs `ecrawl` daily on the paths in `/etc/ereport/ecrawl-daily.conf`, `rsync`s each output directory to `RSYNC_DEST`, and deletes the local copy after a successful sync.

```bash
sudo contrib/systemd/install.sh --enable     # units + wrapper + example config, then enable the timer
sudo systemctl edit ecrawl-daily.service     # drop-ins for User=, Environment=, ... (survive reinstalls)
```

`install.sh` never overwrites an existing config. Any `ECRAWL_*` line in the config (other than `ECRAWL_BIN`) is exported into the crawl's environment, so `ECRAWL_CRAWL_THREADS=16` works there. The unit sets `LimitNOFILE=65536` and has no `[Install]` section: it runs from `ecrawl-daily.timer` only. See `contrib/systemd/ecrawl-daily.conf.example` for the config keys.
