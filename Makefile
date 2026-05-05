# Compiler
CC = gcc
PYTHON3 ?= python3

# Flags
CFLAGS = -O2 -Wall -Wextra -Wunused-parameter -pthread
SERVE_ROOT ?= .
SERVE_PORT ?= 8000
SERVE_BIND ?= 127.0.0.1
# Optional: directory with trigram index (tri_keys.bin); forwarded as eserve.py --index-dir
SERVE_INDEX_DIR ?=

# Targets
TARGETS = ereport ereport_index ecrawl ecrawl_repair edelete

# Optional NFS probe (needs libnfs, e.g. dnf install libnfs-devel)
LIBNFS_CFLAGS ?= $(shell pkg-config --cflags libnfs 2>/dev/null)
LIBNFS_LIBS ?= $(shell pkg-config --libs libnfs 2>/dev/null)
ifneq ($(strip $(LIBNFS_LIBS)),)
TARGETS += enfsprobe
# Some libnfs.pc files only set Libs (-lnfs) and omit Cflags; then we must not
# use #include <libnfs.h> without -I — use <nfsc/libnfs.h> instead.
ifeq ($(strip $(LIBNFS_CFLAGS)),)
ENFSPROBE_CPPFLAGS := -DENFSPROBE_NFSC_LIBNFS_H=1
ENFSPROBE_LIBNFS_CFLAGS :=
else
ENFSPROBE_CPPFLAGS :=
ENFSPROBE_LIBNFS_CFLAGS := $(LIBNFS_CFLAGS)
endif
ENFSPROBE_LIBNFS_LIBS := $(LIBNFS_LIBS)
else
ENFSPROBE_CPPFLAGS := -DENFSPROBE_NFSC_LIBNFS_H=1
ENFSPROBE_LIBNFS_CFLAGS :=
ENFSPROBE_LIBNFS_LIBS := -lnfs
endif

# dlopen/dlsym: optional nfs_set_version at runtime (missing on EL7 libnfs)
ENFSPROBE_LIBDL := -ldl

# Default target
all: $(TARGETS)

path_utils.o: path_utils.c path_utils.h
	$(CC) $(CFLAGS) -c path_utils.c -o path_utils.o

crawl_bin_chunks.o: crawl_bin_chunks.c crawl_bin_chunks.h crawl_bin_format.h crawl_ckpt.h
	$(CC) $(CFLAGS) -c crawl_bin_chunks.c -o crawl_bin_chunks.o

ecrawl: ecrawl.c crawl_ckpt.h path_canon.h path_utils.h path_utils.o
	$(CC) $(CFLAGS) -o $@ ecrawl.c path_utils.o

edelete: edelete.c path_canon.h path_utils.h path_utils.o
	$(CC) $(CFLAGS) -o $@ edelete.c path_utils.o

ecrawl_repair: ecrawl_repair.c crawl_ckpt.h path_canon.h
	$(CC) $(CFLAGS) -o $@ ecrawl_repair.c

ereport: ereport.c crawl_ckpt.h path_canon.h path_utils.h path_utils.o crawl_bin_chunks.h crawl_bin_chunks.o
	$(CC) $(CFLAGS) -o $@ ereport.c path_utils.o crawl_bin_chunks.o

ereport_index: ereport_index.c crawl_ckpt.h path_canon.h crawl_bin_chunks.h crawl_bin_chunks.o
	$(CC) $(CFLAGS) -o $@ ereport_index.c crawl_bin_chunks.o

enfsprobe: enfsprobe.c
	$(CC) $(CFLAGS) $(ENFSPROBE_CPPFLAGS) $(ENFSPROBE_LIBNFS_CFLAGS) -o $@ enfsprobe.c $(ENFSPROBE_LIBNFS_LIBS) $(ENFSPROBE_LIBDL)

# Fully static libnfs (needs libnfs.a — often unavailable on RHEL; try Fedora or build libnfs from source).
enfsprobe-static: enfsprobe.c
	$(CC) $(CFLAGS) $(ENFSPROBE_CPPFLAGS) $(ENFSPROBE_LIBNFS_CFLAGS) -o $@ enfsprobe.c \
	  -Wl,-Bstatic -lnfs -Wl,-Bdynamic $(ENFSPROBE_LIBDL)

# Standalone directory: binary + libnfs.so.13; copy enfsprobe-dist/ to hosts without libnfs RPM.
enfsprobe-dist: enfsprobe.c
	rm -rf $@
	mkdir -p $@
	$(CC) $(CFLAGS) $(ENFSPROBE_CPPFLAGS) $(ENFSPROBE_LIBNFS_CFLAGS) \
	  -Wl,-rpath,'$$ORIGIN' -o $@/enfsprobe enfsprobe.c $(ENFSPROBE_LIBNFS_LIBS) $(ENFSPROBE_LIBDL)
	SO=; \
	for d in $$(pkg-config --variable=libdir libnfs 2>/dev/null) /usr/lib64 /usr/lib; do \
	  test -n "$$d" || continue; \
	  if test -e "$$d/libnfs.so.13"; then SO=$$(realpath "$$d/libnfs.so.13"); break; fi; \
	done; \
	if test -z "$$SO" || test ! -f "$$SO"; then \
	  echo "enfsprobe-dist: could not find libnfs.so.13 to bundle"; exit 1; \
	fi; \
	cp -a "$$SO" $@/libnfs.so.13
	@echo "enfsprobe-dist: built $@/ (copy the directory; target still needs glibc, not libnfs RPM)"

# Debug build
debug: CFLAGS = -O0 -g -Wall -Wextra -pthread
debug: clean all

# Clean
clean:
	rm -f $(TARGETS) enfsprobe enfsprobe-static *.o
	rm -rf __pycache__ enfsprobe-dist

# SERVE_BIND applies here only; serve-public always uses 0.0.0.0 (see README eserve.py section).
serve:
	$(PYTHON3) eserve.py --bind $(SERVE_BIND) --port $(SERVE_PORT) $(if $(SERVE_INDEX_DIR),--index-dir "$(SERVE_INDEX_DIR)") $(SERVE_ROOT)

serve-public:
	$(PYTHON3) eserve.py --bind 0.0.0.0 --port $(SERVE_PORT) $(if $(SERVE_INDEX_DIR),--index-dir "$(SERVE_INDEX_DIR)") $(SERVE_ROOT)

# Self-test: tiny temp tree + key=value stat cross-checks (ecrawl + ereport)
check: $(TARGETS)
	./test.sh

# Larger fixture under ./test (see test_setup.sh), then same correlation as check
check-tree: $(TARGETS)
	./test_full.sh

.PHONY: all clean debug serve serve-public check check-tree enfsprobe-static enfsprobe-dist
