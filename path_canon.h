/*
 * Canonical absolute paths via realpath(3) for existing paths.
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef PATH_CANON_H
#define PATH_CANON_H

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/*
 * Resolve an existing path to a canonical absolute path. On failure prints
 * label (if non-NULL) then path and strerror, and returns -1.
 */
static inline int path_resolve_existing(const char *in, char out[PATH_MAX], const char *err_label) {
    if (!in || !out || in[0] == '\0') {
        errno = EINVAL;
        if (err_label && err_label[0] != '\0')
            fprintf(stderr, "%s(empty path)\n", err_label);
        else
            fprintf(stderr, "path is empty\n");
        return -1;
    }
    if (!realpath(in, out)) {
        if (err_label && err_label[0] != '\0')
            fprintf(stderr, "%s%s: %s\n", err_label, in, strerror(errno));
        else
            fprintf(stderr, "%s: %s\n", in, strerror(errno));
        return -1;
    }
    return 0;
}

/* Replace an existing directory path buffer with its realpath. */
static inline int path_resolve_inplace(char *path, size_t path_sz, const char *err_label) {
    char tmp[PATH_MAX];
    int n;

    if (path_resolve_existing(path, tmp, err_label) != 0) return -1;
    n = snprintf(path, path_sz, "%s", tmp);
    if (n < 0 || (size_t)n >= path_sz) {
        errno = ENAMETOOLONG;
        if (err_label && err_label[0] != '\0')
            fprintf(stderr, "%sresolved path too long\n", err_label);
        return -1;
    }
    return 0;
}

/* Same as path_resolve_inplace but returns -1 without printing (best-effort). */
static inline int path_try_resolve_inplace(char *path, size_t path_sz) {
    char tmp[PATH_MAX];

    if (!realpath(path, tmp)) return -1;
    if (snprintf(path, path_sz, "%s", tmp) >= (int)path_sz) return -1;
    return 0;
}

#endif /* PATH_CANON_H */
