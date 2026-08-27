/* ereport_qsort_r: glibc-style qsort_r(base, n, size, cmp(a, b, arg), arg) on every platform.
 *
 * glibc and the BSDs (including macOS) both call it qsort_r but disagree on the calling
 * convention: glibc passes the context pointer last and gives the comparator
 * (a, b, ctx); BSD passes the thunk before the comparator and gives it (ctx, a, b).
 * On BSD a small trampoline adapts the call so all comparators in this repo keep the
 * glibc signature. Callers on glibc must still define _GNU_SOURCE before <stdlib.h>.
 */
#ifndef EREPORT_COMPAT_QSORT_R_H
#define EREPORT_COMPAT_QSORT_R_H

#include <stdlib.h>

typedef int (*ereport_qsort_cmp_t)(const void *, const void *, void *);

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) || defined(__DragonFly__)
typedef struct {
    ereport_qsort_cmp_t cmp;
    void *arg;
} ereport_qsort_wrap_t;

static int ereport_qsort_tramp(void *ctx, const void *a, const void *b) {
    const ereport_qsort_wrap_t *w = (const ereport_qsort_wrap_t *)ctx;
    return w->cmp(a, b, w->arg);
}

static void ereport_qsort_r(void *base, size_t n, size_t size, ereport_qsort_cmp_t cmp, void *arg) {
    ereport_qsort_wrap_t w = { cmp, arg };
    qsort_r(base, n, size, &w, ereport_qsort_tramp);
}
#else
static void ereport_qsort_r(void *base, size_t n, size_t size, ereport_qsort_cmp_t cmp, void *arg) {
    qsort_r(base, n, size, cmp, arg);
}
#endif

#endif /* EREPORT_COMPAT_QSORT_R_H */
