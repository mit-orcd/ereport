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

# Default target
all: $(TARGETS)

ecrawl: ecrawl.c crawl_ckpt.h
	$(CC) $(CFLAGS) -o $@ ecrawl.c

edelete: edelete.c
	$(CC) $(CFLAGS) -o $@ $<

ecrawl_repair: ecrawl_repair.c crawl_ckpt.h
	$(CC) $(CFLAGS) -o $@ ecrawl_repair.c

ereport: ereport.c crawl_ckpt.h
	$(CC) $(CFLAGS) -o $@ ereport.c

ereport_index: ereport_index.c crawl_ckpt.h
	$(CC) $(CFLAGS) -o $@ ereport_index.c

# Debug build
debug: CFLAGS = -O0 -g -Wall -Wextra -pthread
debug: clean all

# Clean
clean:
	rm -f $(TARGETS) *.o
	rm -rf __pycache__

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

.PHONY: all clean debug serve serve-public check check-tree
