# Compiler
CC = gcc

# Flags
CFLAGS = -O2 -Wall -Wextra -pthread

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
	./eserve.py .

serve-public:
	./eserve.py --bind 0.0.0.0 .

.PHONY: all clean debug serve serve-public
