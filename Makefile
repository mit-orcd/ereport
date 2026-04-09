# Compiler
CC = gcc

# Flags
CFLAGS = -O2 -Wall -Wextra -pthread

# Targets
TARGETS = ereport_simple ereport ecrawl ecrawl_nfs
OLD_TARGETS = nfs_report nfs_report_ng local_crawl nfs_crawl

# Default target
all: $(TARGETS)

ecrawl: ecrawl.c
	$(CC) $(CFLAGS) -o $@ $<


ereport_simple: ereport_simple.c
	$(CC) $(CFLAGS) -o $@ $<

ereport: ereport.c
	$(CC) $(CFLAGS) -o $@ $<

# Special rule for ecrawl_nfs (needs libnfs)
ecrawl_nfs: ecrawl_nfs.c
	$(CC) $(CFLAGS) -o $@ $< -lnfs

# Debug build
debug: CFLAGS = -O0 -g -Wall -Wextra -pthread
debug: clean all

# Clean
clean:
	rm -f $(TARGETS) $(OLD_TARGETS) *.o thread_*.txt thread_*.bin

# Dependencies hint
deps:
	@echo "On Debian/Ubuntu:"
	@echo "  sudo apt-get install libnfs-dev"
	@echo ""
	@echo "On RHEL/CentOS:"
	@echo "  sudo yum install libnfs-devel"


serve:
	./eserve.py .

serve-public:
	./eserve.py --bind 0.0.0.0 .

.PHONY: all clean debug deps serve serve-public