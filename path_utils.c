/*
 * Shared path helpers — see path_utils.h
 *
 * SPDX-License-Identifier: MIT
 */

#include "path_utils.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

void path_rstrip_slashes(char *s) {
    size_t n;

    if (!s) return;
    n = strlen(s);
    while (n > 1U && s[n - 1U] == '/') {
        s[n - 1U] = '\0';
        n--;
    }
}

void path_rstrip_path_separators(char *s) {
    size_t len;

    if (!s) return;
    len = strlen(s);
    while (len > 1U && (s[len - 1U] == '/' || s[len - 1U] == '\\')) {
        s[--len] = '\0';
    }
}

int path_join_fast(const char *base, size_t base_len, const char *name, size_t name_len, char *out, size_t out_sz) {
    size_t need;

    if (!base || !name || !out || out_sz == 0) return -1;

    if (base_len == 1U && base[0] == '/') {
        need = 1U + name_len + 1U;
        if (need > out_sz) return -1;
        out[0] = '/';
        if (name_len > 0U) memcpy(out + 1, name, name_len);
        out[1U + name_len] = '\0';
        return 0;
    }

    need = base_len + 1U + name_len + 1U;
    if (need > out_sz) return -1;
    memcpy(out, base, base_len);
    out[base_len] = '/';
    if (name_len > 0U) memcpy(out + base_len + 1U, name, name_len);
    out[base_len + 1U + name_len] = '\0';
    return 0;
}

int path_join_alloc(const char *base, size_t base_len, const char *name, size_t name_len, char **out_path,
                    size_t *out_len) {
    char *path;
    size_t need;

    if (!base || !name || !out_path || !out_len) {
        errno = EINVAL;
        return -1;
    }

    if (base_len == 1U && base[0] == '/') need = 1U + name_len + 1U;
    else need = base_len + 1U + name_len + 1U;

    path = (char *)malloc(need);
    if (!path) return -1;
    if (path_join_fast(base, base_len, name, name_len, path, need) != 0) {
        free(path);
        errno = ENAMETOOLONG;
        return -1;
    }

    *out_path = path;
    *out_len = need - 1U;
    return 0;
}

int path_is_under_root(const char *path, const char *root) {
    size_t lr;

    if (!path || !root) return 0;
    if (strcmp(root, "/") == 0) return strcmp(path, "/") != 0;
    lr = strlen(root);
    if (strcmp(path, root) == 0) return 1;
    if (strncmp(path, root, lr) != 0) return 0;
    return path[lr] == '/';
}
