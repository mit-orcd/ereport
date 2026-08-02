/*
 * alloc_tuning.h — one place for the process-wide glibc malloc settings the
 * multi-threaded phases of ecrawl / ereport / ereport_index depend on.
 *
 * SPDX-License-Identifier: MIT
 *
 * Two defaults hurt these tools, both visible in the 2026-07-31 profiles:
 *
 *   M_MMAP_THRESHOLD (128 KiB) — every allocation at or above it is a fresh
 *     mmap and every free is an munmap, so the buffers these tools cycle
 *     through (arena chunks, per-worker record and compression scratch, path
 *     index blocks) each cost a page-table teardown plus an IPI to every core
 *     holding the mapping. That is the `flush_tlb_func` /
 *     `smp_call_function_many_cond` time in the ereport profile.
 *
 *   M_TRIM_THRESHOLD (128 KiB) — free() returns the top of the heap to the
 *     kernel that eagerly, so a phase that frees and reallocates at the same
 *     size pays repeated `sysmalloc` growth (13.95% of ereport_index CPU).
 *
 * Raising both keeps that memory in the process, which is what a batch pipeline
 * wants: peak RSS is set by what is in flight, not by what has been freed.
 *
 * Set EREPORT_ALLOC_TUNE=0 to leave the allocator at its defaults (for
 * before/after profiling). No effect when jemalloc is interposed.
 */
#ifndef ALLOC_TUNING_H
#define ALLOC_TUNING_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#ifdef __GLIBC__
#include <malloc.h>
#endif
#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

#define ALLOC_TUNE_TRIM_THRESHOLD (128 * 1024 * 1024)
#define ALLOC_TUNE_MMAP_THRESHOLD (32 * 1024 * 1024)
/* Below this an allocation cannot hold a whole huge page once aligned, so the hint would be a no-op. */
/* -D the minimum huge to A/B without the hint. */
#ifndef ALLOC_HUGEPAGE_MIN_BYTES
#define ALLOC_HUGEPAGE_MIN_BYTES (4u * 1024u * 1024u)
#endif
#define ALLOC_HUGEPAGE_SIZE (2u * 1024u * 1024u)
/* Below this the fault count is too small to be worth a syscall. -D it huge to A/B without prefaulting. */
#ifndef ALLOC_PREFAULT_MIN_BYTES
#define ALLOC_PREFAULT_MIN_BYTES (1u * 1024u * 1024u)
#endif

static inline void tune_allocator(void) {
#ifdef __GLIBC__
    const char *off = getenv("EREPORT_ALLOC_TUNE");

    if (off && strcmp(off, "0") == 0) return;
#ifdef M_TRIM_THRESHOLD
    (void)mallopt(M_TRIM_THRESHOLD, ALLOC_TUNE_TRIM_THRESHOLD);
#endif
#ifdef M_MMAP_THRESHOLD
    (void)mallopt(M_MMAP_THRESHOLD, ALLOC_TUNE_MMAP_THRESHOLD);
#endif
#endif
}

/*
 * Ask for transparent huge pages over the huge-page-aligned interior of a large allocation. Worth doing
 * for the multi-megabyte hash tables these tools populate once and then probe randomly: a 2 MiB page is
 * one fault and one TLB entry instead of 512 of each. Advisory — a no-op where THP is off or already
 * "always", and it deliberately ignores errors.
 */
static inline void alloc_hint_hugepages(void *p, size_t bytes) {
#if defined(__linux__) && defined(MADV_HUGEPAGE)
    uintptr_t start = (uintptr_t)p;
    uintptr_t aligned, end;

    if (!p || bytes < (size_t)ALLOC_HUGEPAGE_MIN_BYTES) return;
    aligned = (start + (ALLOC_HUGEPAGE_SIZE - 1)) & ~(uintptr_t)(ALLOC_HUGEPAGE_SIZE - 1);
    end = (start + bytes) & ~(uintptr_t)(ALLOC_HUGEPAGE_SIZE - 1);
    if (end <= aligned) return;
    (void)madvise((void *)aligned, (size_t)(end - aligned), MADV_HUGEPAGE);
#else
    (void)p;
    (void)bytes;
#endif
}

/*
 * Populate the page tables of a large allocation now, instead of one fault at a time as it fills.
 * MADV_HUGEPAGE on its own does not do this: the pages a fresh calloc hands back still fault on
 * first write, and for the zero page that fault is a copy (wp_page_copy) whose page-table update
 * sends a TLB shootdown IPI to every core running the process. On a 96-core box populating a
 * multi-megabyte hash table that way cost ereport ~30% of its cycles in the fault handler with
 * another ~8% in smp_call_function_many_cond. One madvise replaces the whole storm.
 *
 * MADV_POPULATE_WRITE is Linux 5.14+ and its value is spelled out because a toolchain older than
 * the running kernel would otherwise hide it. Older kernels answer EINVAL, which falls back to a
 * page-stride read-modify-write: it stores back exactly what it read, so it is safe whatever the
 * buffer holds.
 */
/* MADV_HUGEPAGE stands in for "this translation unit sees madvise()": glibc gates the declaration and
 * the MADV_* constants on the same feature macros, so a unit compiled without them uses the touch loop. */
#if defined(__linux__) && defined(MADV_HUGEPAGE)
#define ALLOC_HAVE_MADVISE 1
#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif
#endif

static inline void alloc_prefault(void *p, size_t bytes) {
#if defined(__linux__)
    long pagesz;
    uintptr_t start = (uintptr_t)p;
    uintptr_t aligned, end;

    if (!p || bytes < (size_t)ALLOC_PREFAULT_MIN_BYTES) return;

    pagesz = sysconf(_SC_PAGESIZE);
    if (pagesz <= 0) pagesz = 4096;

    /* madvise needs a page-aligned start; the head and tail partial pages are left to fault. */
    aligned = (start + (uintptr_t)pagesz - 1u) & ~((uintptr_t)pagesz - 1u);
    end = (start + bytes) & ~((uintptr_t)pagesz - 1u);
    if (end <= aligned) return;

#ifdef ALLOC_HAVE_MADVISE
    if (madvise((void *)aligned, (size_t)(end - aligned), MADV_POPULATE_WRITE) == 0) return;
#endif

    {
        volatile unsigned char *q = (volatile unsigned char *)aligned;
        size_t span = (size_t)(end - aligned);
        size_t off;

        for (off = 0; off < span; off += (size_t)pagesz) q[off] = q[off];
    }
#else
    (void)p;
    (void)bytes;
#endif
}

#endif /* ALLOC_TUNING_H */
