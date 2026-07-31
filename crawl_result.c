/*
 * crawl_result — see crawl_result.h.
 *
 * SPDX-License-Identifier: MIT
 */
#define _FILE_OFFSET_BITS 64
#define _DEFAULT_SOURCE

#include "crawl_result.h"

#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "crawl_bin_format.h"

#define SHARD_PREFIX "uid_shard_"
#define SHARD_PREFIX_LEN (sizeof(SHARD_PREFIX) - 1)
#define SHARD_SUFFIX ".bin"
#define SHARD_SUFFIX_LEN (sizeof(SHARD_SUFFIX) - 1)

void crawl_result_init(crawl_result_t *cr) {
    if (cr) memset(cr, 0, sizeof(*cr));
}

void crawl_result_free(crawl_result_t *cr) {
    size_t i;

    if (!cr) return;
    for (i = 0; i < cr->shard_count; i++) free(cr->shards[i].path);
    free(cr->shards);
    free(cr->dir);
    free(cr->start_path);
    free(cr->record_root);
    memset(cr, 0, sizeof(*cr));
}

const char *crawl_result_stored_root(const crawl_result_t *cr) {
    if (!cr) return NULL;
    if (cr->record_root && cr->record_root[0] != '\0') return cr->record_root;
    if (cr->start_path && cr->start_path[0] != '\0') return cr->start_path;
    return NULL;
}

static int is_power_of_two_u32(uint32_t v) { return v != 0U && (v & (v - 1U)) == 0U; }

/*
 * "uid_shard_0042.bin" -> 42. Rejects anything with trailing bytes, so the
 * ".bin.ckpt" sidecars are not mistaken for shards.
 */
static int parse_shard_name(const char *name, uint32_t *shard_out) {
    size_t len = strlen(name);
    const char *digits = name + SHARD_PREFIX_LEN;
    size_t ndigits;
    unsigned long v;
    char *endp;

    if (len <= SHARD_PREFIX_LEN + SHARD_SUFFIX_LEN) return -1;
    if (strncmp(name, SHARD_PREFIX, SHARD_PREFIX_LEN) != 0) return -1;
    if (strcmp(name + len - SHARD_SUFFIX_LEN, SHARD_SUFFIX) != 0) return -1;

    ndigits = len - SHARD_PREFIX_LEN - SHARD_SUFFIX_LEN;
    if (ndigits == 0 || ndigits > 10) return -1;
    for (size_t i = 0; i < ndigits; i++) {
        if (digits[i] < '0' || digits[i] > '9') return -1;
    }

    errno = 0;
    v = strtoul(digits, &endp, 10);
    if (errno != 0 || endp != digits + ndigits || v > UINT32_MAX) return -1;

    *shard_out = (uint32_t)v;
    return 0;
}

static char *dup_trimmed(const char *s) {
    size_t len = strlen(s);
    char *out;

    while (len > 0 && (s[len - 1] == '\r' || s[len - 1] == ' ' || s[len - 1] == '\t')) len--;
    out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, s, len);
    out[len] = '\0';
    return out;
}

/*
 * Absent manifest is not an error: pre-manifest and hand-assembled crawl
 * directories still scan fine, they just do not advertise a layout.
 */
static int read_manifest(const char *dir, crawl_result_t *cr) {
    char path[PATH_MAX];
    char line[4096];
    FILE *fp;
    int saw_uid_shard_layout = 0;

    if (snprintf(path, sizeof(path), "%s/crawl_manifest.txt", dir) >= (int)sizeof(path)) {
        fprintf(stderr, "crawl_result: manifest path too long under %s\n", dir);
        return -1;
    }
    fp = fopen(path, "r");
    if (!fp) {
        if (errno == ENOENT) return 0;
        fprintf(stderr, "crawl_result: %s: %s\n", path, strerror(errno));
        return -1;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *nl = strchr(line, '\n');
        if (nl) *nl = '\0';

        if (strcmp(line, "layout=uid_shards") == 0) {
            saw_uid_shard_layout = 1;
        } else if (strncmp(line, "uid_shards=", 11) == 0) {
            unsigned long v = strtoul(line + 11, NULL, 10);
            if (v > 0 && v <= UINT32_MAX) cr->uid_shards = (uint32_t)v;
        } else if (strncmp(line, "uid_shard_digits=", 17) == 0) {
            long v = strtol(line + 17, NULL, 10);
            if (v > 0 && v <= 10) cr->uid_shard_digits = (int)v;
        } else if (strncmp(line, "format_version=", 15) == 0) {
            unsigned long v = strtoul(line + 15, NULL, 10);
            if (v <= UINT32_MAX) cr->format_version = (uint32_t)v;
        } else if (strncmp(line, "start_path=", 11) == 0) {
            free(cr->start_path);
            cr->start_path = dup_trimmed(line + 11);
            if (!cr->start_path) goto oom;
        } else if (strncmp(line, "record_root=", 12) == 0) {
            free(cr->record_root);
            cr->record_root = dup_trimmed(line + 12);
            if (!cr->record_root) goto oom;
        }
    }
    fclose(fp);

    if (!saw_uid_shard_layout) {
        cr->uid_shards = 0;
    } else if (!is_power_of_two_u32(cr->uid_shards)) {
        fprintf(stderr, "crawl_result: invalid uid_shards value in %s\n", path);
        return -1;
    }
    return 0;

oom:
    fclose(fp);
    fprintf(stderr, "crawl_result: out of memory reading %s\n", path);
    return -1;
}

/*
 * Validate one candidate shard. Returns 1 when usable (fields filled), 0 when
 * it should be skipped, -1 on a hard error.
 */
static int inspect_shard(const char *dir, const char *name, uint32_t shard, crawl_result_shard_t *out,
                         size_t *skipped_incomplete, size_t *skipped_unreadable) {
    char path[PATH_MAX];
    bin_file_header_t fh;
    struct stat st;
    FILE *fp;

    if (snprintf(path, sizeof(path), "%s/%s", dir, name) >= (int)sizeof(path)) {
        fprintf(stderr, "crawl_result: shard path too long: %s/%s\n", dir, name);
        return -1;
    }

    fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "crawl_result: %s: %s\n", path, strerror(errno));
        (*skipped_unreadable)++;
        return 0;
    }
    if (fstat(fileno(fp), &st) != 0 || fread(&fh, sizeof(fh), 1, fp) != 1) {
        fprintf(stderr, "crawl_result: %s: short or unreadable header\n", path);
        fclose(fp);
        (*skipped_unreadable)++;
        return 0;
    }
    fclose(fp);

    if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) {
        fprintf(stderr, "crawl_result: %s: not an %s v%u shard\n", path, CRAWL_BIN_MAGIC, FORMAT_VERSION);
        (*skipped_unreadable)++;
        return 0;
    }
    /* A writer still holds this shard open; its records are not yet addressable. */
    if (fh.catalog_offset == 0ULL) {
        (*skipped_incomplete)++;
        return 0;
    }
    if (fh.catalog_offset < sizeof(fh) || fh.catalog_offset > (uint64_t)st.st_size) {
        fprintf(stderr, "crawl_result: %s: catalog_offset %" PRIu64 " outside file of %" PRIu64 " bytes\n", path,
                (uint64_t)fh.catalog_offset, (uint64_t)st.st_size);
        (*skipped_unreadable)++;
        return 0;
    }

    out->path = strdup(path);
    if (!out->path) {
        fprintf(stderr, "crawl_result: out of memory\n");
        return -1;
    }
    out->shard = shard;
    out->file_size = (uint64_t)st.st_size;
    out->catalog_offset = fh.catalog_offset;
    return 1;
}

static int cmp_shard(const void *a, const void *b) {
    const crawl_result_shard_t *x = a, *y = b;

    if (x->shard < y->shard) return -1;
    if (x->shard > y->shard) return 1;
    return 0;
}

int crawl_result_open(const char *dir, crawl_result_t *cr) {
    DIR *dp;
    struct dirent *de;
    size_t cap = 0;

    if (!dir || !cr) return -1;
    crawl_result_init(cr);

    cr->dir = strdup(dir);
    if (!cr->dir) {
        fprintf(stderr, "crawl_result: out of memory\n");
        return -1;
    }
    if (read_manifest(dir, cr) != 0) goto fail;

    dp = opendir(dir);
    if (!dp) {
        fprintf(stderr, "crawl_result: %s: %s\n", dir, strerror(errno));
        goto fail;
    }
    while ((de = readdir(dp)) != NULL) {
        uint32_t shard;
        int rc;

        if (parse_shard_name(de->d_name, &shard) != 0) continue;

        if (cr->shard_count == cap) {
            size_t ncap = cap ? cap * 2 : 64;
            crawl_result_shard_t *ns = realloc(cr->shards, ncap * sizeof(*ns));
            if (!ns) {
                fprintf(stderr, "crawl_result: out of memory\n");
                closedir(dp);
                goto fail;
            }
            cr->shards = ns;
            cap = ncap;
        }

        rc = inspect_shard(dir, de->d_name, shard, &cr->shards[cr->shard_count], &cr->skipped_incomplete,
                           &cr->skipped_unreadable);
        if (rc < 0) {
            closedir(dp);
            goto fail;
        }
        if (rc > 0) cr->shard_count++;
    }
    closedir(dp);

    /* readdir order is arbitrary; shard order keeps scans and logs reproducible. */
    if (cr->shard_count > 1) qsort(cr->shards, cr->shard_count, sizeof(*cr->shards), cmp_shard);
    return 0;

fail:
    crawl_result_free(cr);
    return -1;
}
