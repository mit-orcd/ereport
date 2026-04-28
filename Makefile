# Compiler
CC = gcc
PYTHON3 ?= python3

# Flags
CFLAGS = -O2 -Wall -Wextra -pthread
SERVE_ROOT ?= .
SERVE_PORT ?= 8000
SERVE_BIND ?= 127.0.0.1

# Targets
TARGETS = ereport ereport_index ecrawl

# Default target
all: $(TARGETS)

ecrawl: ecrawl.c
	$(CC) $(CFLAGS) -o $@ $<

ereport: ereport.c
	$(CC) $(CFLAGS) -o $@ $<

ereport_index: ereport_index.c
	$(CC) $(CFLAGS) -o $@ $<

# Debug build
debug: CFLAGS = -O0 -g -Wall -Wextra -pthread
debug: clean all

# Clean
clean:
	rm -f $(TARGETS) *.o
	rm -rf __pycache__

serve:
	$(PYTHON3) eserve.py --bind $(SERVE_BIND) --port $(SERVE_PORT) $(SERVE_ROOT)

serve-public:
	$(PYTHON3) eserve.py --bind 0.0.0.0 --port $(SERVE_PORT) $(SERVE_ROOT)

# Self-test: tiny temp tree + key=value stat cross-checks (ecrawl + ereport)
check: $(TARGETS)
	./test.sh

# Larger fixture under ./test (see test_setup.sh), then same correlation as check
check-tree: $(TARGETS)
	./test_full.sh

.PHONY: all clean debug serve serve-public check check-tree
