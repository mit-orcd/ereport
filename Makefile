# Compiler
CC = gcc
PYTHON3 ?= python3

# Flags
CFLAGS = -O2 -Wall -Wextra -Wunused-parameter -pthread
# macOS: sources set _XOPEN_SOURCE/_GNU_SOURCE, which hides BSD extensions (qsort_r,
# major/minor, O_DIRECTORY) unless _DARWIN_C_SOURCE is also on the command line.
ifeq ($(shell uname -s),Darwin)
CFLAGS += -D_DARWIN_C_SOURCE
endif
SERVE_ROOT ?= .
SERVE_PORT ?= 8000
SERVE_BIND ?= 127.0.0.1
# Optional: directory with trigram index (tri_keys.bin); forwarded as eserve.py --index-dir
SERVE_INDEX_DIR ?=

# Targets
TARGETS = ereport ereport_index ecrawl ecrawl_query edelete

UNAME_S := $(shell uname -s)

# zstd: required for the v6 uid_shard_*.bin block-compressed record format.
# Auto-detected via pkg-config; falls back to a bare -lzstd link (libzstd.so is a
# base-system library on RHEL). On macOS zstd comes from Homebrew, which keeps it
# keg-only (not in the default compiler search path), so without pkg-config point
# at the brew prefix explicitly. If libzstd is truly absent the link fails, which
# is the intended hard requirement.
ZSTD_CFLAGS ?= $(shell pkg-config --cflags libzstd 2>/dev/null)
ZSTD_LIBS ?= $(shell pkg-config --libs libzstd 2>/dev/null)
ifeq ($(strip $(ZSTD_LIBS)),)
ifeq ($(UNAME_S),Darwin)
ZSTD_BREW := $(firstword $(wildcard /opt/homebrew/opt/zstd /usr/local/opt/zstd))
ifneq ($(strip $(ZSTD_BREW)),)
ZSTD_CFLAGS := -I$(ZSTD_BREW)/include
ZSTD_LIBS := -L$(ZSTD_BREW)/lib -lzstd
else
ZSTD_LIBS := -lzstd
endif
else
ZSTD_LIBS := -lzstd
endif
endif

# Optional FUSE 2.x for ecrawl_mount (read-only mount of a crawl result).
# Preferred source is fuse.pc from fuse-devel. RHEL/Rocky ship libfuse.so.2 in
# the base fuse-libs package but put the headers in fuse-devel, which needs
# root to install; `make fuse-headers` unpacks just the headers of the matching
# RPM into FUSE_PREFIX so an unprivileged user can still build. In that case we
# link -l:libfuse.so.2 against the system library rather than -lfuse, because
# the devel-only libfuse.so symlink is not present.
# ecrawl_mount is Linux-only. macFUSE mounts of this filesystem can wedge the VFS
# badly enough to hang umount/mount, so the FUSE target is not supported on macOS.
ifeq ($(UNAME_S),Darwin)
FUSE_NOTE := ecrawl_mount is not supported on macOS; skipping (Linux-only FUSE target)
else
FUSE_PREFIX ?= $(HOME)/.local/fuse-devel
FUSE_CFLAGS ?= $(shell pkg-config --cflags fuse 2>/dev/null)
FUSE_LIBS ?= $(shell pkg-config --libs fuse 2>/dev/null)
ifeq ($(strip $(FUSE_LIBS)),)
ifneq ($(wildcard $(FUSE_PREFIX)/usr/include/fuse.h),)
FUSE_CFLAGS := -I$(FUSE_PREFIX)/usr/include
FUSE_LIBS := -l:libfuse.so.2
endif
endif

# FUSE 2.9 API; the 2.x high-level (path-based) interface is what ecrawl_mount uses.
FUSE_CPPFLAGS := -DFUSE_USE_VERSION=29 -D_FILE_OFFSET_BITS=64

# ecrawl_mount is optional, so decide by asking the compiler whether <fuse.h> actually
# resolves rather than by trusting that a fuse.pc implies usable headers. A .pc that sets
# Libs but omits Cflags, or fuse-libs installed without fuse-devel, would otherwise
# add the target and then fail the build.
FUSE_HAVE_HEADER := $(shell $(CC) $(FUSE_CPPFLAGS) $(FUSE_CFLAGS) -E -include fuse.h -x c /dev/null >/dev/null 2>&1 && echo 1)
# Headers present but no .pc: RHEL/Rocky ship libfuse.so.2 in base fuse-libs, and the
# devel-only libfuse.so symlink -lfuse needs is absent, so name the soname directly.
ifeq ($(strip $(FUSE_LIBS)),)
ifeq ($(strip $(FUSE_HAVE_HEADER)),1)
FUSE_LIBS := -l:libfuse.so.2
endif
endif

ifeq ($(strip $(FUSE_HAVE_HEADER))$(if $(strip $(FUSE_LIBS)),1,),11)
TARGETS += ecrawl_mount
FUSE_NOTE := fuse enabled ($(strip $(FUSE_LIBS)))
else
FUSE_NOTE := fuse headers not found; skipping optional ecrawl_mount (get them with: make fuse-headers)
endif
endif

# Header-only install of fuse-devel into FUSE_PREFIX, no root required. The
# extracted headers must match the installed libfuse.so.2 ABI, so the version
# is pinned to what the distro ships rather than tracking upstream.
FUSE_DEVEL_RPM ?= fuse-devel-2.9.7-19.el8.x86_64.rpm
FUSE_DEVEL_URL ?= http://10.1.10.195/install/engaging/rocky-8.10/repos/baseos/Packages/f/$(FUSE_DEVEL_RPM)
# The fuse-headers recipe lives below `all` on purpose: make takes the first
# target in the file as the default goal, so a rule here would break bare `make`.

# Optional jemalloc for native C binaries.
# Auto-detected via pkg-config; needs jemalloc-devel (RHEL/Fedora) or libjemalloc-dev
# (Debian/Ubuntu). The runtime-only package ships just libjemalloc.so.2 with no .pc,
# so pkg-config correctly reports "not available" and the build silently falls back
# to glibc malloc. No source #include is required: linking -ljemalloc interposes
# malloc/free/calloc/realloc transparently. Override by setting JEMALLOC_LIBS= to ""
# to force-disable, or to a literal "-ljemalloc" to force-enable.
JEMALLOC_CFLAGS ?= $(shell pkg-config --cflags jemalloc 2>/dev/null)
JEMALLOC_LIBS ?= $(shell pkg-config --libs jemalloc 2>/dev/null)
ifneq ($(strip $(JEMALLOC_LIBS)),)
JEMALLOC_NOTE := jemalloc enabled ($(strip $(JEMALLOC_LIBS)))
else
JEMALLOC_NOTE := jemalloc not found; using glibc malloc
endif

# Default target (listed first so bare `make` builds everything, not only jemalloc-note).
all: jemalloc-note $(TARGETS)

jemalloc-note:
	@echo "build: $(JEMALLOC_NOTE)"
	@echo "build: $(FUSE_NOTE)"

# Header-only install of fuse-devel into FUSE_PREFIX, no root required, for
# hosts that ship libfuse.so.2 (base system) but not the devel package.
fuse-headers:
	@if test -f "$(FUSE_PREFIX)/usr/include/fuse.h"; then \
	  echo "fuse-headers: already present at $(FUSE_PREFIX)/usr/include/fuse.h"; \
	else \
	  set -e; \
	  mkdir -p "$(FUSE_PREFIX)"; \
	  td=$$(mktemp -d); \
	  echo "fuse-headers: fetching $(FUSE_DEVEL_URL)"; \
	  curl -sSf -o "$$td/fuse-devel.rpm" "$(FUSE_DEVEL_URL)"; \
	  rpm2cpio "$$td/fuse-devel.rpm" | cpio -idm --quiet -D "$(FUSE_PREFIX)"; \
	  rm -rf "$$td"; \
	  echo "fuse-headers: installed into $(FUSE_PREFIX); re-run make to build ecrawl_mount"; \
	fi

path_utils.o: path_utils.c path_utils.h
	$(CC) $(CFLAGS) -c path_utils.c -o path_utils.o

crawl_bin_catalog.o: crawl_bin_catalog.c crawl_bin_catalog.h crawl_bin_codec.h crawl_bin_format.h
	$(CC) $(CFLAGS) $(ZSTD_CFLAGS) -c crawl_bin_catalog.c -o crawl_bin_catalog.o

crawl_result.o: crawl_result.c crawl_result.h crawl_bin_format.h
	$(CC) $(CFLAGS) -c crawl_result.c -o crawl_result.o

crawl_bin_chunks.o: crawl_bin_chunks.c crawl_bin_chunks.h crawl_bin_format.h crawl_ckpt.h
	$(CC) $(CFLAGS) -c crawl_bin_chunks.c -o crawl_bin_chunks.o

crawl_fpcache.o: crawl_fpcache.c crawl_fpcache.h
	$(CC) $(CFLAGS) -c crawl_fpcache.c -o crawl_fpcache.o

crawl_bin_codec.o: crawl_bin_codec.c crawl_bin_codec.h crawl_bin_format.h
	$(CC) $(CFLAGS) -c crawl_bin_codec.c -o crawl_bin_codec.o

# dirs.idx / rowgroups.idx reader, shared by ecrawl_query --index-dir and ereport --index-dir.
crawl_sidecar.o: crawl_sidecar.c crawl_sidecar.h crawl_bin_format.h crawl_bin_catalog.h crawl_bin_chunks.h
	$(CC) $(CFLAGS) -c crawl_sidecar.c -o crawl_sidecar.o

crawl_bin_block.o: crawl_bin_block.c crawl_bin_block.h crawl_bin_codec.h crawl_bin_format.h crawl_bin_chunks.h
	$(CC) $(CFLAGS) $(ZSTD_CFLAGS) -c crawl_bin_block.c -o crawl_bin_block.o

# Basename trigram extraction for ereport_index.
trigram_extract.o: trigram_extract.c trigram_extract.h
	$(CC) $(CFLAGS) -c trigram_extract.c -o trigram_extract.o

test_crawl_codec: test_crawl_codec.c crawl_bin_codec.o
	$(CC) $(CFLAGS) -o $@ test_crawl_codec.c crawl_bin_codec.o

test_crawl_block_filter: test_crawl_block_filter.c crawl_bin_block.o crawl_bin_codec.o
	$(CC) $(CFLAGS) $(ZSTD_CFLAGS) -o $@ test_crawl_block_filter.c crawl_bin_block.o crawl_bin_codec.o $(ZSTD_LIBS)

test_crawl_catalog: test_crawl_catalog.c crawl_bin_catalog.o crawl_bin_codec.o
	$(CC) $(CFLAGS) $(ZSTD_CFLAGS) -o $@ test_crawl_catalog.c crawl_bin_catalog.o crawl_bin_codec.o $(ZSTD_LIBS)

test_query_subtree_dups: test_query_subtree_dups.c crawl_bin_catalog.o crawl_bin_block.o crawl_bin_codec.o
	$(CC) $(CFLAGS) $(ZSTD_CFLAGS) -o $@ test_query_subtree_dups.c crawl_bin_catalog.o crawl_bin_block.o crawl_bin_codec.o $(ZSTD_LIBS)

ecrawl: ecrawl.c alloc_tuning.h crawl_ckpt.h path_canon.h path_utils.h path_utils.o crawl_bin_catalog.o crawl_bin_block.h crawl_bin_block.o crawl_bin_codec.o ecrawl_wire.h
	$(CC) $(CFLAGS) $(JEMALLOC_CFLAGS) $(ZSTD_CFLAGS) -o $@ ecrawl.c path_utils.o crawl_bin_catalog.o crawl_bin_block.o crawl_bin_codec.o $(ZSTD_LIBS) $(JEMALLOC_LIBS)

edelete: edelete.c path_canon.h path_utils.h path_utils.o
	$(CC) $(CFLAGS) $(JEMALLOC_CFLAGS) -o $@ edelete.c path_utils.o $(JEMALLOC_LIBS)

ecrawl_query: ecrawl_query.c alloc_tuning.h crawl_ckpt.h path_canon.h crawl_bin_chunks.h crawl_bin_chunks.o crawl_bin_catalog.o crawl_bin_block.h crawl_bin_block.o crawl_bin_codec.o crawl_fpcache.h crawl_fpcache.o crawl_sidecar.h crawl_sidecar.o
	$(CC) $(CFLAGS) $(JEMALLOC_CFLAGS) $(ZSTD_CFLAGS) -o $@ ecrawl_query.c crawl_bin_chunks.o crawl_bin_catalog.o crawl_bin_block.o crawl_bin_codec.o crawl_fpcache.o crawl_sidecar.o $(ZSTD_LIBS) $(JEMALLOC_LIBS)

ereport: ereport.c alloc_tuning.h crawl_ckpt.h path_canon.h path_utils.h path_utils.o crawl_bin_chunks.h crawl_bin_chunks.o crawl_bin_catalog.o crawl_bin_block.h crawl_bin_block.o crawl_bin_codec.o crawl_fpcache.h crawl_fpcache.o crawl_sidecar.h crawl_sidecar.o
	$(CC) $(CFLAGS) $(JEMALLOC_CFLAGS) $(ZSTD_CFLAGS) -o $@ ereport.c path_utils.o crawl_bin_chunks.o crawl_bin_catalog.o crawl_bin_block.o crawl_bin_codec.o crawl_fpcache.o crawl_sidecar.o $(ZSTD_LIBS) $(JEMALLOC_LIBS)

ereport_index: ereport_index.c alloc_tuning.h crawl_ckpt.h path_canon.h crawl_bin_chunks.h crawl_bin_chunks.o crawl_bin_catalog.o crawl_bin_block.h crawl_bin_block.o crawl_bin_codec.o crawl_fpcache.h crawl_fpcache.o trigram_extract.h trigram_extract.o
	$(CC) $(CFLAGS) $(JEMALLOC_CFLAGS) $(ZSTD_CFLAGS) -o $@ ereport_index.c crawl_bin_chunks.o crawl_bin_catalog.o crawl_bin_block.o crawl_bin_codec.o crawl_fpcache.o trigram_extract.o $(ZSTD_LIBS) $(JEMALLOC_LIBS)

ecrawl_mount: ecrawl_mount.c crawl_result.h crawl_result.o crawl_bin_chunks.h crawl_bin_chunks.o crawl_bin_catalog.o crawl_bin_block.h crawl_bin_block.o crawl_bin_codec.o
	$(CC) $(CFLAGS) $(JEMALLOC_CFLAGS) $(ZSTD_CFLAGS) $(FUSE_CPPFLAGS) $(FUSE_CFLAGS) -o $@ ecrawl_mount.c crawl_result.o crawl_bin_chunks.o crawl_bin_catalog.o crawl_bin_block.o crawl_bin_codec.o $(FUSE_LIBS) $(ZSTD_LIBS) $(JEMALLOC_LIBS)

# Debug build
debug: CFLAGS = -O0 -g -Wall -Wextra -pthread
debug: clean all

# Clean
clean:
	rm -f $(TARGETS) ecrawl_mount test_crawl_block_filter test_crawl_codec test_crawl_catalog test_query_subtree_dups *.o crawl_bin_catalog.o crawl_bin_block.o crawl_bin_codec.o crawl_result.o crawl_sidecar.o
	rm -rf __pycache__

# SERVE_BIND applies here only; serve-public always uses 0.0.0.0 (see README eserve.py section).
serve:
	$(PYTHON3) eserve.py --bind $(SERVE_BIND) --port $(SERVE_PORT) $(if $(SERVE_INDEX_DIR),--index-dir "$(SERVE_INDEX_DIR)") $(SERVE_ROOT)

serve-public:
	$(PYTHON3) eserve.py --bind 0.0.0.0 --port $(SERVE_PORT) $(if $(SERVE_INDEX_DIR),--index-dir "$(SERVE_INDEX_DIR)") $(SERVE_ROOT)

# Self-test: tiny temp tree + key=value stat cross-checks (ecrawl + ereport)
check: $(TARGETS) test_crawl_codec test_crawl_block_filter test_crawl_catalog test_query_subtree_dups
	./test_crawl_codec
	./test_crawl_block_filter
	./test_crawl_catalog
	./scripts/test/test.sh

# Larger fixture under ./test (see scripts/test/test_setup.sh), then same correlation as check
check-tree: $(TARGETS)
	./scripts/test/test_full.sh

.PHONY: all clean debug serve serve-public check check-tree jemalloc-note fuse-headers
