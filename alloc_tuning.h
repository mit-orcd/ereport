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
#endif

#define ALLOC_TUNE_TRIM_THRESHOLD (128 * 1024 * 1024)
#define ALLOC_TUNE_MMAP_THRESHOLD (32 * 1024 * 1024)
/* Below this an allocation cannot hold a whole huge page once aligned, so the hint would be a no-op. */
#define ALLOC_HUGEPAGE_MIN_BYTES (4u * 1024u * 1024u)
#define ALLOC_HUGEPAGE_SIZE (2u * 1024u * 1024u)

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

#endif /* ALLOC_TUNING_H */
