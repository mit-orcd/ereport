# Compiler
CC = gcc
PYTHON3 ?= python3

# Flags
CFLAGS = -O2 -Wall -Wextra -pthread
SERVE_ROOT ?= .
SERVE_PORT ?= 8000
SERVE_BIND ?= 127.0.0.1

# Targets
TARGETS = ereport_simple ereport ecrawl

# Default target
all: $(TARGETS)

ecrawl: ecrawl.c
	$(CC) $(CFLAGS) -o $@ $<

ereport_simple: ereport_simple.c
	$(CC) $(CFLAGS) -o $@ $<

ereport: ereport.c
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

.PHONY: all clean debug serve serve-public
