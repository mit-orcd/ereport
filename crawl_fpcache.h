/*
 * Per-thread parking cache for read-only shard FILE handles.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 *
 * glibc registers every stream on one process-wide list and takes a single lock
 * (_IO_list_all) in _IO_link_in and _IO_un_link, so fopen and fclose serialize
 * across all threads no matter which file they touch. On a capture with a
 * thousand uid shards that dominated the readers: 56% of ecrawl_query's cycles
 * on the 1019-shard fixture sat in native_queued_spin_lock_slowpath under
 * __fopen_internal and fclose, because each shard is opened once for its catalog
 * and again for its records.
 *
 * The fix is to stop closing: crawl_fpcache_fclose parks the handle in a small
 * per-thread slot table, and the next crawl_fpcache_fopen of the same path from
 * the same thread rewinds it and hands it back with no syscall and no lock. The
 * back-to-back "load the catalog, then scan a chunk of the same shard" pattern
 * both readers use turns two opens into one.
 *
 * Only read-only modes are cached, and only for files that do not change during a
 * run (crawl shard binaries and their .ckpt sidecars). Anything else falls
 * through to plain fopen/fclose.
 */
#ifndef CRAWL_FPCACHE_H
#define CRAWL_FPCACHE_H

#include <stdint.h>
#include <stdio.h>

/*
 * Drop-in fopen/fclose. A cached handle is rewound to offset 0 with its error and
 * EOF flags cleared, so a caller that reads a file header straight after opening
 * behaves exactly as it would with a fresh stream.
 */
FILE *crawl_fpcache_fopen(const char *path, const char *mode);
int crawl_fpcache_fclose(FILE *fp);

/*
 * Stdio buffer size for cached streams, applied with setvbuf when the handle is
 * really opened. Call once at startup: a caller must not setvbuf a handle it got
 * from this cache, because a reused handle has already done I/O. 0 (the default)
 * leaves stdio's own default buffering.
 */
void crawl_fpcache_set_bufsz(size_t bytes);

/*
 * Close this thread's parked handles. Registered as a TLS destructor, so worker
 * threads release their fds on exit without every pool having to call it; exposed
 * for the main thread and for phase boundaries that want the fds back early.
 */
void crawl_fpcache_release_thread(void);

/* Process-wide counters: opens that reached the kernel, and opens served from a parked handle. */
uint64_t crawl_fpcache_real_opens(void);
uint64_t crawl_fpcache_hits(void);

#endif /* CRAWL_FPCACHE_H */
