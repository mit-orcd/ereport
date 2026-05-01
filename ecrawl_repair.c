/*
 * ecrawl_repair.c — Write missing uid_shard_*.bin.ckpt sidecars by scanning shard binaries.
 *
 * SPDX-License-Identifier: MIT
 *
 * Build: gcc -O2 -Wall -Wextra -o ecrawl_repair ecrawl_repair.c
 *
 * Usage: ecrawl_repair [--dry-run] [--verbose] <crawl-output-dir>
 *
 * Recreates checkpoint offsets the same way ecrawl does when reopening a shard without
 * a valid sidecar (full sequential scan, stride CRAWL_CKPT_STRIDE_BYTES).
 *
 * Parallelism: environment variable ECRAWL_REPAIR_THREADS (default 16, minimum 1).
 *
 * Incomplete tail (truncated last record): the shard .bin is shortened to the last complete record
 * boundary, then rescanned and given a .ckpt.
 *
 * Other corrupt uid_shard_*.bin shards are renamed into <crawl-dir>/corrupt_shards/ (and matching
 * .bin.ckpt sidecars when present). Dry-run does not truncate, move, or write.
 *
 * If the 16-byte file header magic/version is not the current v3 (or known ERCBIN02 v2), we still run a full
 * record-structure scan. When that scan succeeds, the header is rewritten to ERCBIN03 / v3 before writing .ckpt
 * (same reserved field as read from disk). If the header is wrong and the scan fails, the shard is quarantined.
 *
 * After a normal run, every remaining top-level uid_shard_*.bin is checked for a sidecar compatible with
 * crawl_bin_load_ckpt() in crawl_bin_chunks.c (same rules as ereport / ereport_index). Exit status is 0 only
 * if none failed and that check passes, so readers can load checkpoint sidecars for all shards still in the crawl directory.
 */

#define _FILE_OFFSET_BITS 64
#define _DEFAULT_SOURCE

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "crawl_ckpt.h"
#include "path_canon.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* Older ecrawl shards: same bin_record_hdr_t as current; ecrawl_repair rewrites header after scan. */
#define REPAIR_LEGACY_MAGIC_V2 "ERCBIN02"
#define REPAIR_LEGACY_VERSION_V2 2u

typedef enum {
    REPAIR_HDR_REJECT = 0,
    REPAIR_HDR_CURRENT = 1,
    REPAIR_HDR_LEGACY_V2 = 2
} repair_bin_hdr_kind_t;

static repair_bin_hdr_kind_t repair_bin_hdr_kind(const bin_file_header_t *h) {
    if (h->version == FORMAT_VERSION && memcmp(h->magic, CRAWL_BIN_MAGIC, (size_t)CRAWL_BIN_MAGIC_LEN) == 0)
        return REPAIR_HDR_CURRENT;
    if (h->version == REPAIR_LEGACY_VERSION_V2 &&
        memcmp(h->magic, REPAIR_LEGACY_MAGIC_V2, (size_t)CRAWL_BIN_MAGIC_LEN) == 0)
        return REPAIR_HDR_LEGACY_V2;
    return REPAIR_HDR_REJECT;
}

#define DEFAULT_REPAIR_THREADS 16U
#define REPAIR_THREADS_MAX 4096U
#define CORRUPT_SUBDIR "corrupt_shards"

static pthread_mutex_t g_verbose_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_quarantine_mutex = PTHREAD_MUTEX_INITIALIZER;

static _Atomic uint64_t g_trunc_shard_count;
static _Atomic uint64_t g_trunc_bytes_removed;
static _Atomic uint64_t g_trunc_orig_bytes_total;
static _Atomic uint64_t g_trunc_new_bytes_total;

static void trunc_stats_reset(void) {
    atomic_store_explicit(&g_trunc_shard_count, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_trunc_bytes_removed, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_trunc_orig_bytes_total, 0ULL, memory_order_relaxed);
    atomic_store_explicit(&g_trunc_new_bytes_total, 0ULL, memory_order_relaxed);
}

static void trunc_stats_add(uint64_t orig_sz, uint64_t new_sz) {
    atomic_fetch_add_explicit(&g_trunc_shard_count, 1ULL, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_trunc_bytes_removed, orig_sz - new_sz, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_trunc_orig_bytes_total, orig_sz, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_trunc_new_bytes_total, new_sz, memory_order_relaxed);
}

static void format_bytes_human(uint64_t n, char *out, size_t out_sz) {
    static const char *const suf[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    double v = (double)n;
    unsigned si = 0;

    while (v >= 1024.0 && si + 1U < sizeof(suf) / sizeof(suf[0])) {
        v /= 1024.0;
        si++;
    }
    if (si == 0) {
        snprintf(out, out_sz, "%" PRIu64 " B", n);
    } else {
        snprintf(out, out_sz, "%.2f %s", v, suf[si]);
    }
}

static void print_truncation_summary(int dry_run) {
    uint64_t nsh = atomic_load_explicit(&g_trunc_shard_count, memory_order_relaxed);
    uint64_t removed = atomic_load_explicit(&g_trunc_bytes_removed, memory_order_relaxed);
    uint64_t orig = atomic_load_explicit(&g_trunc_orig_bytes_total, memory_order_relaxed);
    uint64_t newb = atomic_load_explicit(&g_trunc_new_bytes_total, memory_order_relaxed);
    char rem_buf[48], orig_buf[48], new_buf[48];

    if (nsh == 0ULL) return;

    format_bytes_human(removed, rem_buf, sizeof(rem_buf));
    format_bytes_human(orig, orig_buf, sizeof(orig_buf));
    format_bytes_human(newb, new_buf, sizeof(new_buf));

    fprintf(stderr,
            "ecrawl_repair: %stail truncation — %" PRIu64 " shard(s); removed %" PRIu64 " bytes (%s); "
            "original total %" PRIu64 " bytes (%s) -> new total %" PRIu64 " bytes (%s)\n",
            dry_run ? "would-be " : "", nsh, removed, rem_buf, orig, orig_buf, newb, new_buf);
}

static int g_verbose;
static int g_dry_run;

static int is_uid_shard_bin_name(const char *name);

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--dry-run] [--verbose] <crawl-output-dir>\n"
            "  Writes uid_shard_*.bin.ckpt next to ecrawl v%u shard files (full scan).\n"
            "  Non-current bin headers are rewritten to %s v%u after a successful record scan.\n"
            "  Parallel threads: ECRAWL_REPAIR_THREADS (default %u).\n"
            "  Truncates tail when the record stream fails at an incomplete last record; else corrupt "
            "shards go to %s/.\n"
            "  Verifies remaining shards have ereport-valid .ckpt before exit 0.\n",
            prog,
            FORMAT_VERSION,
            CRAWL_BIN_MAGIC,
            FORMAT_VERSION,
            DEFAULT_REPAIR_THREADS,
            CORRUPT_SUBDIR);
}

static unsigned parse_repair_threads_env(void) {
    const char *e = getenv("ECRAWL_REPAIR_THREADS");
    unsigned long v;
    char *end;

    if (!e || e[0] == '\0') return DEFAULT_REPAIR_THREADS;
    errno = 0;
    v = strtoul(e, &end, 10);
    if (errno || end == e || *end != '\0' || v < 1UL || v > (unsigned long)REPAIR_THREADS_MAX) return DEFAULT_REPAIR_THREADS;
    return (unsigned)v;
}

static int ckpt_sidecar_path(const char *bin_path, char *out, size_t out_sz) {
    int n = snprintf(out, out_sz, "%s.ckpt", bin_path);
    return (n < 0 || (size_t)n >= out_sz) ? -1 : 0;
}

/*
 * Returns 0 on success. On failure, *corrupt_out is 1 if the shard looks corrupt/unreadable as a
 * crawl bin (quarantine); 0 if the failure is environmental (e.g. cannot open file).
 */
static int read_bin_file_header(const char *path, uint64_t file_sz, bin_file_header_t *hdr_out, int *corrupt_out) {
    FILE *fp;

    *corrupt_out = 0;
    if (file_sz < sizeof(bin_file_header_t)) {
        fprintf(stderr, "%s: file too small\n", path);
        *corrupt_out = 1;
        return -1;
    }
    fp = fopen(path, "rb");
    if (!fp) {
        perror(path);
        return -1;
    }
    if (fread(hdr_out, sizeof(*hdr_out), 1, fp) != 1) {
        fprintf(stderr, "%s: cannot read header\n", path);
        fclose(fp);
        *corrupt_out = 1;
        return -1;
    }
    fclose(fp);
    return 0;
}

static int validate_bin_prefix_current_only(const char *path, uint64_t file_sz, int *corrupt_out) {
    bin_file_header_t hdr;
    repair_bin_hdr_kind_t k;

    *corrupt_out = 0;
    if (read_bin_file_header(path, file_sz, &hdr, corrupt_out) != 0) return -1;
    k = repair_bin_hdr_kind(&hdr);
    if (k != REPAIR_HDR_CURRENT) {
        fprintf(stderr, "%s: not an ecrawl uid-shard binary (magic/version)\n", path);
        *corrupt_out = (k == REPAIR_HDR_REJECT) ? 1 : 0;
        return -1;
    }
    return 0;
}

static int rewrite_shard_header_to_current(const char *path, const bin_file_header_t *old_hdr) {
    FILE *fp;
    bin_file_header_t nh;

    fp = fopen(path, "r+b");
    if (!fp) {
        perror(path);
        return -1;
    }
    memset(&nh, 0, sizeof(nh));
    memcpy(nh.magic, CRAWL_BIN_MAGIC, (size_t)CRAWL_BIN_MAGIC_LEN);
    nh.version = FORMAT_VERSION;
    nh.reserved = old_hdr->reserved;
    if (fseeko(fp, (off_t)0, SEEK_SET) != 0 || fwrite(&nh, sizeof(nh), 1, fp) != 1 || fflush(fp) != 0) {
        int e = errno ? errno : EIO;
        fclose(fp);
        errno = e;
        perror(path);
        return -1;
    }
    if (fclose(fp) != 0) {
        perror(path);
        return -1;
    }
    return 0;
}

/*
 * On corrupt scan (*corrupt_out=1), if salvage_exclusive_end_out is non-NULL, sets it to the byte
 * offset immediately after the last fully validated record (or sizeof(bin_file_header_t) if none),
 * suitable for truncate() when salvage_exclusive_end_out < file_sz.
 */
static int rebuild_offsets_scan(const char *bin_path, uint64_t file_sz, uint64_t **offs_out, size_t *n_out,
                                int *corrupt_out, uint64_t *salvage_exclusive_end_out) {
    FILE *fp;
    uint64_t *buf = NULL;
    size_t n, cap;
    uint64_t seg0;
    uint64_t last_good_exclusive;
    off_t pos;

    *offs_out = NULL;
    *n_out = 0;
    *corrupt_out = 0;
    if (salvage_exclusive_end_out) *salvage_exclusive_end_out = sizeof(bin_file_header_t);
    fp = fopen(bin_path, "rb");
    if (!fp) {
        perror(bin_path);
        return -1;
    }

    buf = (uint64_t *)malloc(16 * sizeof(*buf));
    if (!buf) {
        fclose(fp);
        return -1;
    }
    n = 1;
    cap = 16;
    buf[0] = sizeof(bin_file_header_t);
    seg0 = sizeof(bin_file_header_t);
    last_good_exclusive = sizeof(bin_file_header_t);

    if (fseeko(fp, (off_t)sizeof(bin_file_header_t), SEEK_SET) != 0) {
        if (salvage_exclusive_end_out) *salvage_exclusive_end_out = last_good_exclusive;
        goto corrupt;
    }
    pos = (off_t)sizeof(bin_file_header_t);

    while ((uint64_t)pos < file_sz) {
        uint64_t rec_start = (uint64_t)pos;
        bin_record_hdr_t rh;

        if (rec_start - seg0 >= CRAWL_CKPT_STRIDE_BYTES) {
            if (n == cap) {
                size_t ncap = cap * 2;
                uint64_t *p = (uint64_t *)realloc(buf, ncap * sizeof(*p));
                if (!p) goto oom;
                buf = p;
                cap = ncap;
            }
            buf[n++] = rec_start;
            seg0 = rec_start;
        }
        if (fread(&rh, sizeof(rh), 1, fp) != 1) {
            if (salvage_exclusive_end_out) *salvage_exclusive_end_out = last_good_exclusive;
            goto corrupt;
        }
        pos = ftello(fp);
        if (pos < 0) {
            if (salvage_exclusive_end_out) *salvage_exclusive_end_out = last_good_exclusive;
            goto corrupt;
        }
        if (rh.path_len) {
            if ((uint64_t)pos + rh.path_len > file_sz) {
                if (salvage_exclusive_end_out) *salvage_exclusive_end_out = last_good_exclusive;
                goto corrupt;
            }
            if (fseeko(fp, (off_t)rh.path_len, SEEK_CUR) != 0) {
                if (salvage_exclusive_end_out) *salvage_exclusive_end_out = last_good_exclusive;
                goto corrupt;
            }
            pos = ftello(fp);
            if (pos < 0) {
                if (salvage_exclusive_end_out) *salvage_exclusive_end_out = last_good_exclusive;
                goto corrupt;
            }
        }
        last_good_exclusive = (uint64_t)pos;
    }
    if ((uint64_t)pos != file_sz) {
        fprintf(stderr, "%s: scan did not consume whole file (corrupt or wrong format?)\n", bin_path);
        errno = EINVAL;
        if (salvage_exclusive_end_out) *salvage_exclusive_end_out = last_good_exclusive;
        goto corrupt;
    }
    fclose(fp);
    if (salvage_exclusive_end_out) *salvage_exclusive_end_out = file_sz;
    *offs_out = buf;
    *n_out = n;
    return 0;
oom:
    fclose(fp);
    free(buf);
    return -1;
corrupt:
    *corrupt_out = 1;
    fclose(fp);
    free(buf);
    return -1;
}

static int salvage_truncate_shard(const char *path, uint64_t new_sz) {
    if (truncate(path, (off_t)new_sz) != 0) {
        perror(path);
        return -1;
    }
    return 0;
}

static int write_ckpt_file(const char *bin_path, const uint64_t *offs, size_t n) {
    char ckpath[PATH_MAX];
    crawl_ckpt_file_hdr_t ch;
    FILE *fp;

    if (ckpt_sidecar_path(bin_path, ckpath, sizeof(ckpath)) != 0) {
        fprintf(stderr, "%s: ckpt path too long\n", bin_path);
        return -1;
    }
    fp = fopen(ckpath, "wb");
    if (!fp) {
        perror(ckpath);
        return -1;
    }
    memset(&ch, 0, sizeof(ch));
    memcpy(ch.magic, CRAWL_CKPT_MAGIC, CRAWL_CKPT_MAGIC_LEN);
    ch.version = CRAWL_CKPT_ONDISK_VERSION;
    ch.stride_bytes = CRAWL_CKPT_STRIDE_BYTES;
    ch.num_offsets = (uint64_t)n;
    if (fwrite(&ch, sizeof(ch), 1, fp) != 1 || fwrite(offs, sizeof(uint64_t), n, fp) != n) {
        int e = errno ? errno : EIO;
        fclose(fp);
        errno = e;
        perror(ckpath);
        return -1;
    }
    if (fflush(fp) != 0 || fclose(fp) != 0) {
        perror(ckpath);
        return -1;
    }
    return 0;
}

/*
 * Same acceptance rules as crawl_bin_load_ckpt() (crawl_bin_chunks.c) — required for chunk mapping to succeed.
 */
static int sidecar_ok_for_ereport(const char *bin_path, uint64_t file_sz) {
    char ckpath[PATH_MAX];
    crawl_ckpt_file_hdr_t ch;
    FILE *fp;
    uint64_t *buf = NULL;
    size_t i;

    if (ckpt_sidecar_path(bin_path, ckpath, sizeof(ckpath)) != 0) return -1;
    fp = fopen(ckpath, "rb");
    if (!fp) return -1;
    if (fread(&ch, sizeof(ch), 1, fp) != 1) {
        fclose(fp);
        return -1;
    }
    if (memcmp(ch.magic, CRAWL_CKPT_MAGIC, CRAWL_CKPT_MAGIC_LEN) != 0 || ch.version != CRAWL_CKPT_ONDISK_VERSION ||
        ch.stride_bytes != CRAWL_CKPT_STRIDE_BYTES || ch.num_offsets == 0 || ch.num_offsets > (uint64_t)(SIZE_MAX / sizeof(uint64_t))) {
        fclose(fp);
        return -1;
    }
    buf = (uint64_t *)malloc((size_t)ch.num_offsets * sizeof(*buf));
    if (!buf) {
        fclose(fp);
        return -1;
    }
    if (fread(buf, sizeof(uint64_t), (size_t)ch.num_offsets, fp) != (size_t)ch.num_offsets || fclose(fp) != 0) {
        free(buf);
        return -1;
    }
    if (buf[0] != sizeof(bin_file_header_t)) {
        free(buf);
        return -1;
    }
    for (i = 1; i < (size_t)ch.num_offsets; i++) {
        if (buf[i] <= buf[i - 1] || buf[i] > file_sz) {
            free(buf);
            return -1;
        }
    }
    free(buf);
    return 0;
}

/*
 * Top-level crawl dir only: each uid_shard_*.bin regular file must have a valid sidecar for ereport.
 */
static int verify_top_level_shards_ereport_ready(const char *dir_path) {
    DIR *dp;
    struct dirent *de;
    char full[PATH_MAX];
    struct stat st;
    int bad = 0;
    int corrupt_flag;

    dp = opendir(dir_path);
    if (!dp) {
        perror(dir_path);
        return -1;
    }
    while ((de = readdir(dp)) != NULL) {
        if (!is_uid_shard_bin_name(de->d_name)) continue;
        if (snprintf(full, sizeof(full), "%s/%s", dir_path, de->d_name) >= (int)sizeof(full)) {
            fprintf(stderr, "%s/%s: path too long (ereport check)\n", dir_path, de->d_name);
            bad++;
            continue;
        }
        if (lstat(full, &st) != 0) {
            fprintf(stderr, "%s: stat failed (ereport check)\n", full);
            bad++;
            continue;
        }
        if (!S_ISREG(st.st_mode)) continue;

        if (validate_bin_prefix_current_only(full, (uint64_t)st.st_size, &corrupt_flag) != 0) {
            fprintf(stderr,
                    "%s: not a readable ecrawl v%u shard (ereport would skip); remove or fix before ereport\n",
                    full, FORMAT_VERSION);
            bad++;
            continue;
        }
        if (sidecar_ok_for_ereport(full, (uint64_t)st.st_size) != 0) {
            fprintf(stderr,
                    "%s: missing or invalid .ckpt for ereport — chunk mapping would fail (expected sidecar next "
                    "to this .bin)\n",
                    full);
            bad++;
        }
    }
    closedir(dp);
    return bad;
}

static int count_regular_uid_shard_bins(const char *dir_path) {
    DIR *dp;
    struct dirent *de;
    char full[PATH_MAX];
    struct stat st;
    int n = 0;

    dp = opendir(dir_path);
    if (!dp) return -1;
    while ((de = readdir(dp)) != NULL) {
        if (!is_uid_shard_bin_name(de->d_name)) continue;
        if (snprintf(full, sizeof(full), "%s/%s", dir_path, de->d_name) >= (int)sizeof(full)) continue;
        if (lstat(full, &st) != 0) continue;
        if (S_ISREG(st.st_mode)) n++;
    }
    closedir(dp);
    return n;
}

static int is_uid_shard_bin_name(const char *name) {
    const char *p;
    unsigned long v;
    char *end;

    if (strncmp(name, "uid_shard_", 10) != 0) return 0;
    p = name + 10;
    if (*p == '\0' || !isdigit((unsigned char)*p)) return 0;
    errno = 0;
    v = strtoul(p, &end, 10);
    (void)v;
    if (errno || !end || strcmp(end, ".bin") != 0) return 0;
    return 1;
}

static int cmp_strptr(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static void ops_fail_inc(_Atomic int *n) { atomic_fetch_add_explicit(n, 1, memory_order_relaxed); }

static void corrupt_quarantine_inc(_Atomic int *n) { atomic_fetch_add_explicit(n, 1, memory_order_relaxed); }

/*
 * Move bin (and sibling .bin.ckpt if present) into bin_dir/CORRUPT_SUBDIR/. Collision-safe names.
 * Uses g_quarantine_mutex — call only when holding no other locks besides internal.
 */
static void quarantine_corrupt_shard(const char *bin_dir, const char *full_bin_path, const char *base_name) {
    char subdir[PATH_MAX];
    char dest_bin[PATH_MAX];
    char ck_src[PATH_MAX];
    char ck_dst[PATH_MAX];
    struct stat st;
    unsigned suf;

    if (g_dry_run) {
        fprintf(stderr, "%s: corrupt shard (dry-run — not moved; would go under %s/)\n", full_bin_path,
                CORRUPT_SUBDIR);
        return;
    }

    if (snprintf(subdir, sizeof(subdir), "%s/%s", bin_dir, CORRUPT_SUBDIR) >= (int)sizeof(subdir)) {
        fprintf(stderr, "%s: corrupt shard — quarantine path too long\n", full_bin_path);
        return;
    }

    pthread_mutex_lock(&g_quarantine_mutex);

    if (mkdir(subdir, 0775) != 0 && errno != EEXIST) {
        fprintf(stderr, "%s: mkdir %s: %s\n", full_bin_path, subdir, strerror(errno));
        pthread_mutex_unlock(&g_quarantine_mutex);
        return;
    }

    for (suf = 0; suf < 10000U; suf++) {
        int nb;
        if (suf == 0)
            nb = snprintf(dest_bin, sizeof(dest_bin), "%s/%s", subdir, base_name);
        else
            nb = snprintf(dest_bin, sizeof(dest_bin), "%s/%s.%u", subdir, base_name, suf);
        if (nb < 0 || (size_t)nb >= sizeof(dest_bin)) {
            fprintf(stderr, "%s: quarantine destination path too long\n", full_bin_path);
            pthread_mutex_unlock(&g_quarantine_mutex);
            return;
        }
        if (lstat(dest_bin, &st) == 0) continue;
        if (errno != ENOENT) {
            fprintf(stderr, "%s: stat %s: %s\n", full_bin_path, dest_bin, strerror(errno));
            pthread_mutex_unlock(&g_quarantine_mutex);
            return;
        }
        if (rename(full_bin_path, dest_bin) == 0) goto moved_bin;
        fprintf(stderr, "%s: rename -> %s: %s\n", full_bin_path, dest_bin, strerror(errno));
        pthread_mutex_unlock(&g_quarantine_mutex);
        return;
    }

    fprintf(stderr, "%s: could not find unused name under %s\n", full_bin_path, subdir);
    pthread_mutex_unlock(&g_quarantine_mutex);
    return;

moved_bin:
    if (ckpt_sidecar_path(full_bin_path, ck_src, sizeof(ck_src)) != 0) {
        pthread_mutex_unlock(&g_quarantine_mutex);
        fprintf(stderr, "%s: moved corrupt shard to %s\n", full_bin_path, dest_bin);
        return;
    }
    /* Sidecar path was next to original bin; bin was renamed, ck_src still valid on disk. */
    if (lstat(ck_src, &st) != 0) {
        pthread_mutex_unlock(&g_quarantine_mutex);
        fprintf(stderr, "%s: moved corrupt shard to %s\n", full_bin_path, dest_bin);
        return;
    }
    if (snprintf(ck_dst, sizeof(ck_dst), "%s.ckpt", dest_bin) >= (int)sizeof(ck_dst)) {
        fprintf(stderr, "warn: %s exists — could not build ckpt dest path\n", ck_src);
        pthread_mutex_unlock(&g_quarantine_mutex);
        fprintf(stderr, "%s: moved corrupt shard to %s\n", full_bin_path, dest_bin);
        return;
    }
    if (rename(ck_src, ck_dst) != 0)
        fprintf(stderr, "warn: moved corrupt bin but not sidecar %s: %s\n", ck_src, strerror(errno));
    pthread_mutex_unlock(&g_quarantine_mutex);

    if (g_verbose) {
        pthread_mutex_lock(&g_verbose_mutex);
        printf("%s -> %s (corrupt)\n", full_bin_path, dest_bin);
        pthread_mutex_unlock(&g_verbose_mutex);
    } else {
        fprintf(stderr, "%s: moved corrupt shard to %s\n", full_bin_path, dest_bin);
    }
}

static int process_one_shard(const char *dir_path, const char *name, _Atomic int *ops_fail,
                             _Atomic int *corrupt_quarantined) {
    char full[PATH_MAX];
    struct stat st;
    uint64_t *offs = NULL;
    size_t n_off = 0;
    int n;
    int corrupt;
    bin_file_header_t bin_hdr;
    repair_bin_hdr_kind_t hdr_kind;
    int header_unrecognized;

    n = snprintf(full, sizeof(full), "%s/%s", dir_path, name);
    if (n < 0 || (size_t)n >= sizeof(full)) {
        fprintf(stderr, "%s/%s: path too long\n", dir_path, name);
        ops_fail_inc(ops_fail);
        return -1;
    }
    if (lstat(full, &st) != 0) {
        perror(full);
        ops_fail_inc(ops_fail);
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "%s: skip (not a regular file)\n", full);
        return 0;
    }

    if (read_bin_file_header(full, (uint64_t)st.st_size, &bin_hdr, &corrupt) != 0) {
        if (corrupt) {
            quarantine_corrupt_shard(dir_path, full, name);
            corrupt_quarantine_inc(corrupt_quarantined);
        } else {
            ops_fail_inc(ops_fail);
        }
        return -1;
    }
    hdr_kind = repair_bin_hdr_kind(&bin_hdr);
    header_unrecognized = (hdr_kind == REPAIR_HDR_REJECT);

    {
        uint64_t salvage_end = 0;

        if (rebuild_offsets_scan(full, (uint64_t)st.st_size, &offs, &n_off, &corrupt, &salvage_end) != 0) {
            int can_truncate =
                corrupt && salvage_end >= sizeof(bin_file_header_t) && salvage_end < (uint64_t)st.st_size;

            if (can_truncate) {
                uint64_t orig_sz = (uint64_t)st.st_size;

                if (g_dry_run) {
                    trunc_stats_add(orig_sz, salvage_end);
                    fprintf(stderr,
                            "%s: incomplete tail — would truncate %" PRIu64 " -> %" PRIu64 " bytes then write "
                            ".ckpt\n",
                            full, orig_sz, salvage_end);
                    return 0;
                }
                if (g_verbose) {
                    pthread_mutex_lock(&g_verbose_mutex);
                    printf("%s: truncating %" PRIu64 " -> %" PRIu64 " bytes (salvage valid prefix)\n", full,
                           orig_sz, salvage_end);
                    pthread_mutex_unlock(&g_verbose_mutex);
                } else {
                    fprintf(stderr, "%s: truncating %" PRIu64 " -> %" PRIu64 " bytes (incomplete last record)\n",
                            full, orig_sz, salvage_end);
                }
                if (salvage_truncate_shard(full, salvage_end) != 0) {
                    ops_fail_inc(ops_fail);
                    quarantine_corrupt_shard(dir_path, full, name);
                    corrupt_quarantine_inc(corrupt_quarantined);
                    return -1;
                }
                trunc_stats_add(orig_sz, salvage_end);
                if (stat(full, &st) != 0) {
                    perror(full);
                    ops_fail_inc(ops_fail);
                    return -1;
                }
                salvage_end = 0;
                corrupt = 0;
                if (rebuild_offsets_scan(full, (uint64_t)st.st_size, &offs, &n_off, &corrupt, &salvage_end) !=
                    0) {
                    if (corrupt) {
                        if (header_unrecognized) {
                            fprintf(stderr,
                                    "%s: unrecognized bin header (magic/version) and record scan failed after "
                                    "truncate — not a repairable shard\n",
                                    full);
                        }
                        quarantine_corrupt_shard(dir_path, full, name);
                        corrupt_quarantine_inc(corrupt_quarantined);
                    } else {
                        ops_fail_inc(ops_fail);
                    }
                    return -1;
                }
            } else {
                if (corrupt) {
                    if (header_unrecognized) {
                        fprintf(stderr,
                                "%s: unrecognized bin header (magic/version) and record scan failed — not a "
                                "repairable shard\n",
                                full);
                    }
                    quarantine_corrupt_shard(dir_path, full, name);
                    corrupt_quarantine_inc(corrupt_quarantined);
                } else {
                    ops_fail_inc(ops_fail);
                }
                return -1;
            }
        }
    }

    if (g_verbose) {
        pthread_mutex_lock(&g_verbose_mutex);
        printf("%s -> %zu checkpoint offsets\n", full, n_off);
        pthread_mutex_unlock(&g_verbose_mutex);
    }

    if (hdr_kind == REPAIR_HDR_LEGACY_V2 || header_unrecognized) {
        if (g_dry_run) {
            if (hdr_kind == REPAIR_HDR_LEGACY_V2)
                fprintf(stderr, "%s: would rewrite bin header %s v%u -> %s v%u\n", full, REPAIR_LEGACY_MAGIC_V2,
                        REPAIR_LEGACY_VERSION_V2, CRAWL_BIN_MAGIC, FORMAT_VERSION);
            else
                fprintf(stderr, "%s: would rewrite unrecognized bin header -> %s v%u\n", full, CRAWL_BIN_MAGIC,
                        FORMAT_VERSION);
        } else if (rewrite_shard_header_to_current(full, &bin_hdr) != 0) {
            ops_fail_inc(ops_fail);
            free(offs);
            return -1;
        } else if (g_verbose) {
            pthread_mutex_lock(&g_verbose_mutex);
            printf("%s: normalized bin header to %s v%u\n", full, CRAWL_BIN_MAGIC, FORMAT_VERSION);
            pthread_mutex_unlock(&g_verbose_mutex);
        }
    }

    if (g_dry_run) {
        free(offs);
        return 0;
    }

    if (stat(full, &st) != 0) {
        perror(full);
        ops_fail_inc(ops_fail);
        free(offs);
        return -1;
    }

    if (write_ckpt_file(full, offs, n_off) != 0) {
        ops_fail_inc(ops_fail);
        free(offs);
        return -1;
    }
    if (sidecar_ok_for_ereport(full, (uint64_t)st.st_size) != 0) {
        fprintf(stderr, "%s: wrote .ckpt but ereport compatibility check failed\n", full);
        ops_fail_inc(ops_fail);
        free(offs);
        return -1;
    }
    free(offs);
    return 0;
}

typedef struct {
    const char *dir_path;
    char **names;
    size_t name_count;
    _Atomic size_t cursor;
    _Atomic int ops_failures;
    _Atomic int corrupt_quarantined;
} ckpt_pool_t;

static void *ckpt_worker_main(void *arg) {
    ckpt_pool_t *p = (ckpt_pool_t *)arg;

    for (;;) {
        size_t i = atomic_fetch_add_explicit(&p->cursor, 1, memory_order_relaxed);
        if (i >= p->name_count) break;
        process_one_shard(p->dir_path, p->names[i], &p->ops_failures, &p->corrupt_quarantined);
    }
    return NULL;
}

int main(int argc, char **argv) {
    const char *dir_path = NULL;
    static char dir_path_abs[PATH_MAX];
    DIR *dp;
    struct dirent *de;
    char **names = NULL;
    size_t name_count = 0, name_cap = 0;
    unsigned nthreads = parse_repair_threads_env();
    ckpt_pool_t pool;
    pthread_t *threads = NULL;
    unsigned ti;
    int ops_failures;
    int corrupt_n;
    int ereport_ready_fail;
    int i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--dry-run") == 0)
            g_dry_run = 1;
        else if (strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0)
            g_verbose = 1;
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown option: %s\n", argv[i]);
            usage(argv[0]);
            return 2;
        } else if (!dir_path) {
            dir_path = argv[i];
        } else {
            fprintf(stderr, "extra argument: %s\n", argv[i]);
            usage(argv[0]);
            return 2;
        }
    }

    if (!dir_path) {
        usage(argv[0]);
        return 2;
    }

    if (path_resolve_existing(dir_path, dir_path_abs, "ecrawl_repair: ") != 0) return 2;
    dir_path = dir_path_abs;

    trunc_stats_reset();

    dp = opendir(dir_path);
    if (!dp) {
        perror(dir_path);
        return 1;
    }

    while ((de = readdir(dp)) != NULL) {
        if (!is_uid_shard_bin_name(de->d_name)) continue;
        if (name_count == name_cap) {
            size_t nc = name_cap ? name_cap * 2 : 64;
            char **p = (char **)realloc(names, nc * sizeof(*p));
            if (!p) {
                perror("realloc");
                closedir(dp);
                for (i = 0; i < (int)name_count; i++) free(names[i]);
                free(names);
                return 1;
            }
            names = p;
            name_cap = nc;
        }
        names[name_count] = strdup(de->d_name);
        if (!names[name_count]) {
            perror("strdup");
            closedir(dp);
            for (i = 0; i < (int)name_count; i++) free(names[i]);
            free(names);
            return 1;
        }
        name_count++;
    }
    closedir(dp);

    if (name_count == 0) {
        fprintf(stderr, "%s: no uid_shard_*.bin files found\n", dir_path);
        return 1;
    }

    qsort(names, name_count, sizeof(names[0]), cmp_strptr);

    if ((size_t)nthreads > name_count) nthreads = (unsigned)name_count;
    if (nthreads < 1) nthreads = 1;

    pool.dir_path = dir_path;
    pool.names = names;
    pool.name_count = name_count;
    atomic_store_explicit(&pool.cursor, 0, memory_order_relaxed);
    atomic_store_explicit(&pool.ops_failures, 0, memory_order_relaxed);
    atomic_store_explicit(&pool.corrupt_quarantined, 0, memory_order_relaxed);

    threads = (pthread_t *)calloc(nthreads, sizeof(*threads));
    if (!threads) {
        perror("calloc");
        for (i = 0; i < (int)name_count; i++) free(names[i]);
        free(names);
        return 1;
    }

    for (ti = 0; ti < nthreads; ti++) {
        if (pthread_create(&threads[ti], NULL, ckpt_worker_main, &pool) != 0) {
            perror("pthread_create");
            for (; ti > 0U; ti--) pthread_join(threads[ti - 1U], NULL);
            free(threads);
            for (i = 0; i < (int)name_count; i++) free(names[i]);
            free(names);
            return 1;
        }
    }

    for (ti = 0; ti < nthreads; ti++) pthread_join(threads[ti], NULL);
    free(threads);

    ops_failures = atomic_load_explicit(&pool.ops_failures, memory_order_relaxed);
    corrupt_n = atomic_load_explicit(&pool.corrupt_quarantined, memory_order_relaxed);

    for (i = 0; i < (int)name_count; i++) free(names[i]);
    free(names);

    print_truncation_summary(g_dry_run);

    ereport_ready_fail = 0;
    if (!g_dry_run) {
        int remaining_bins;

        ereport_ready_fail = verify_top_level_shards_ereport_ready(dir_path);
        if (ereport_ready_fail < 0) {
            ops_failures++;
            ereport_ready_fail = 1;
        } else if (ereport_ready_fail > 0) {
            fprintf(stderr,
                    "ereport: %d uid_shard file(s) in %s are not usable (fix above or re-run this tool)\n",
                    ereport_ready_fail, dir_path);
        }

        remaining_bins = count_regular_uid_shard_bins(dir_path);
        if (remaining_bins < 0)
            ops_failures++;
        else if (remaining_bins == 0 && corrupt_n > 0) {
            fprintf(stderr,
                    "no uid_shard_*.bin remain under %s after quarantining %d corrupt shard(s); "
                    "./ereport will find no shard files here\n",
                    dir_path, corrupt_n);
            ereport_ready_fail = 1;
        }
    }

    if (ops_failures > 0) fprintf(stderr, "done with %d operational error(s)\n", ops_failures);
    if (corrupt_n > 0)
        fprintf(stderr, "quarantined %d corrupt uid_shard .bin (under %s/)\n", corrupt_n, CORRUPT_SUBDIR);

    if (ops_failures > 0 || ereport_ready_fail != 0) return 1;
    if (g_verbose || g_dry_run) printf("ok\n");
    return 0;
}
