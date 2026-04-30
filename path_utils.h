/*
 * Shared path helpers (join, strip trailing separators, prefix containment).
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef PATH_UTILS_H
#define PATH_UTILS_H

#include <stddef.h>

/* Strip trailing '/' while length > 1 (preserves "/"). */
void path_rstrip_slashes(char *s);

/* Strip trailing '/' and '\\' while length > 1. */
void path_rstrip_path_separators(char *s);

/*
 * Join base + name into out. Returns 0 on success.
 * If base is exactly "/", result is "/name".
 */
int path_join_fast(const char *base, size_t base_len, const char *name, size_t name_len, char *out, size_t out_sz);

/* malloc path; sets *out_len to strlen(path). */
int path_join_alloc(const char *base, size_t base_len, const char *name, size_t name_len, char **out_path,
                    size_t *out_len);

/*
 * True if path equals root or is strictly under root as a path prefix
 * (root "/" matches any path except "/").
 */
int path_is_under_root(const char *path, const char *root);

#endif /* PATH_UTILS_H */
