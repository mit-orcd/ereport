/*
 * Round-trip and validation tests for the crawl-time trigram journal format.
 *
 * A journal is a cache bound to its capture shard by five facts (basename,
 * size, mtime, catalog_offset, catalog_entries), so what these tests are after
 * is twofold: nothing is lost or shifted between writer and reader (paths,
 * uids, types, and the delta-varint code lists across block boundaries), and
 * every fact mismatch, an incomplete publication, or a corrupt body makes the
 * reader say "fall back" rather than hand back wrong rows.
 *
 * The shard on disk is a minimal fake — a valid bin_file_header_t plus the
 * uint64 catalog entry count at catalog_offset — because validation only ever
 * reads those two fields; the record region's contents are irrelevant to it.
 *
 * SPDX-License-Identifier: MIT
 */
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "crawl_bin_format.h"
#include "crawl_trijournal.h"

static int g_fail = 0;
static int g_checks = 0;

static void ok(const char *what) {
    g_checks++;
    printf("ok   %s\n", what);
}

static void fail(const char *what, const char *detail) {
    g_checks++;
    g_fail++;
    printf("FAIL %s: %s\n", what, detail);
}

static void check_u64(const char *what, uint64_t got, uint64_t want) {
    char buf[160];

    if (got == want) {
        ok(what);
        return;
    }
    snprintf(buf, sizeof(buf), "got %llu want %llu", (unsigned long long)got, (unsigned long long)want);
    fail(what, buf);
}

static void check_int(const char *what, long got, long want) {
    char buf[160];

    if (got == want) {
        ok(what);
        return;
    }
    snprintf(buf, sizeof(buf), "got %ld want %ld", got, want);
    fail(what, buf);
}

#define TEST_CAT_OFFSET 4096u
#define TEST_CAT_ENTRIES 42u

/* Minimal shard: header at 0, entry count at catalog_offset, zero padding between. */
static int write_fake_shard(const char *path) {
    bin_file_header_t fh;
    uint64_t n = TEST_CAT_ENTRIES;
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);

    if (fd < 0) return -1;
    memset(&fh, 0, sizeof(fh));
    memcpy(fh.magic, CRAWL_BIN_MAGIC, CRAWL_BIN_MAGIC_LEN);
    fh.version = FORMAT_VERSION;
    fh.catalog_offset = TEST_CAT_OFFSET;
    if (write(fd, &fh, sizeof(fh)) != (ssize_t)sizeof(fh)) goto err;
    if (ftruncate(fd, (off_t)TEST_CAT_OFFSET) != 0) goto err;
    if (lseek(fd, (off_t)TEST_CAT_OFFSET, SEEK_SET) < 0) goto err;
    if (write(fd, &n, sizeof(n)) != (ssize_t)sizeof(n)) goto err;
    if (close(fd) != 0) return -1;
    return 0;
err:
    (void)close(fd);
    return -1;
}

static void binding_from_stat(const char *shard_path, trij_binding_t *b) {
    struct stat st;

    memset(b, 0, sizeof(*b));
    if (stat(shard_path, &st) != 0) return;
    b->shard_size = (uint64_t)st.st_size;
    b->shard_mtime_sec = (uint64_t)st.st_mtim.tv_sec;
    b->shard_mtime_nsec = (uint64_t)st.st_mtim.tv_nsec;
    b->catalog_offset = TEST_CAT_OFFSET;
    b->catalog_entries = TEST_CAT_ENTRIES;
    b->max_dir_id = 7;
}

/* Deterministic entry pattern; codes strictly increasing as the writer contract requires. */
static size_t entry_make(unsigned int i, char *path, size_t path_cap, uint64_t *uid, uint8_t *type,
                         uint32_t *codes, size_t code_cap) {
    size_t code_count = i % 5;
    size_t j;
    int n;

    if (i == 3) {
        /* one long path to exercise multi-byte path_len varints */
        n = snprintf(path, path_cap, "/root/deep/");
        while ((size_t)n < 2000 && (size_t)n + 8 < path_cap) {
            memcpy(path + n, "sub_dir/", 8);
            n += 8;
        }
        path[n] = '\0';
    } else {
        n = snprintf(path, path_cap, "/root/dir_%u/report_file_%u.txt", i % 9, i);
    }
    *uid = 1000 + (i % 7);
    *type = (i % 11 == 0) ? (uint8_t)'d' : (uint8_t)'f';
    if (code_count > code_cap) code_count = code_cap;
    for (j = 0; j < code_count; j++) codes[j] = i * 8 + (uint32_t)j * 2 + 1;
    return code_count;
}

#define ROUND_ENTRIES 5000u /* ~300 KB uncompressed: crosses the 256 KB block target */

static void test_round_trip(const char *jdir, const char *shard_path) {
    trij_writer_t w;
    trij_reader_t r;
    trij_binding_t binding;
    trij_hdr_t hdr;
    char path[4096];
    uint64_t uid;
    uint8_t type;
    uint32_t codes[8];
    unsigned int i;
    uint64_t seen = 0;
    int mismatches = 0;
    int rc;

    binding_from_stat(shard_path, &binding);
    memset(&w, 0, sizeof(w));
    if (trij_writer_create(&w, jdir, "uid_shard_0007.bin") != 0) {
        fail("round-trip create", "trij_writer_create failed");
        return;
    }
    for (i = 0; i < ROUND_ENTRIES; i++) {
        size_t cc = entry_make(i, path, sizeof(path), &uid, &type, codes, 8);
        if (trij_writer_append(&w, path, strlen(path), uid, type, codes, cc) != 0) {
            fail("round-trip append", "trij_writer_append failed");
            trij_writer_abort(&w);
            return;
        }
    }
    /* exercise the fd close/reopen cycle used by the pool's LRU */
    trij_writer_close_fd(&w);
    rc = trij_writer_finalize(&w, &binding);
    check_int("round-trip finalize", rc, 0);
    if (rc != 0) return;

    memset(&r, 0, sizeof(r));
    rc = trij_reader_open_validate(&r, jdir, shard_path, &hdr);
    check_int("round-trip open_validate", rc, 1);
    if (rc != 1) return;
    check_u64("hdr record_count", hdr.record_count, ROUND_ENTRIES);
    check_u64("hdr shard_size", hdr.shard_size, binding.shard_size);
    check_u64("hdr catalog_offset", hdr.catalog_offset, TEST_CAT_OFFSET);
    check_u64("hdr catalog_entries", hdr.catalog_entries, TEST_CAT_ENTRIES);
    check_u64("hdr max_dir_id", hdr.max_dir_id, 7);

    for (;;) {
        const char *gpath;
        size_t gpath_len;
        uint64_t guid;
        uint8_t gtype;
        const uint32_t *gcodes;
        size_t gcc;
        char epath[4096];
        uint64_t euid;
        uint8_t etype;
        uint32_t ecodes[8];
        size_t ecc;
        int got = trij_reader_next(&r, &gpath, &gpath_len, &guid, &gtype, &gcodes, &gcc);

        if (got == 0) break;
        if (got < 0) {
            fail("round-trip iteration", "trij_reader_next returned corrupt");
            break;
        }
        if (seen < ROUND_ENTRIES) {
            ecc = entry_make((unsigned int)seen, epath, sizeof(epath), &euid, &etype, ecodes, 8);
            if (gpath_len != strlen(epath) || memcmp(gpath, epath, gpath_len) != 0 || guid != euid ||
                gtype != etype || gcc != ecc ||
                (gcc && memcmp(gcodes, ecodes, gcc * sizeof(uint32_t)) != 0))
                mismatches++;
        }
        seen++;
    }
    check_u64("round-trip entry count", seen, ROUND_ENTRIES);
    check_int("round-trip entry mismatches", mismatches, 0);
    check_u64("reader entries_read", r.entries_read, ROUND_ENTRIES);
    trij_reader_close(&r);
}

/* Expect validation to reject after `mutate` changes the shard or the binding. */
static void expect_reject(const char *what, const char *jdir, const char *shard_path,
                          void (*mutate)(const char *shard_path, trij_binding_t *binding)) {
    trij_writer_t w;
    trij_reader_t r;
    trij_binding_t binding;
    char path[64];
    uint64_t uid;
    uint8_t type;
    uint32_t codes[8];
    int rc;

    if (write_fake_shard(shard_path) != 0) {
        fail(what, "shard rewrite failed");
        return;
    }
    binding_from_stat(shard_path, &binding);
    memset(&w, 0, sizeof(w));
    if (trij_writer_create(&w, jdir, "uid_shard_0007.bin") != 0) {
        fail(what, "create failed");
        return;
    }
    entry_make(1, path, sizeof(path), &uid, &type, codes, 8);
    (void)trij_writer_append(&w, path, strlen(path), uid, type, codes, 1);
    if (mutate) mutate(shard_path, &binding);
    if (trij_writer_finalize(&w, &binding) != 0) {
        /* a mutate that breaks finalize's own stat is also a valid rejection */
        ok(what);
        return;
    }
    memset(&r, 0, sizeof(r));
    rc = trij_reader_open_validate(&r, jdir, shard_path, NULL);
    check_int(what, rc, 0);
    if (rc == 1) trij_reader_close(&r);
}

static void mutate_size(const char *shard_path, trij_binding_t *binding) {
    int fd;
    (void)binding;
    fd = open(shard_path, O_WRONLY | O_APPEND);
    if (fd >= 0) {
        (void)write(fd, "x", 1);
        (void)close(fd);
    }
}

static void mutate_mtime(const char *shard_path, trij_binding_t *binding) {
    struct timespec ts[2];
    (void)binding;
    ts[0].tv_sec = 1000000000;
    ts[0].tv_nsec = 12345;
    ts[1] = ts[0];
    (void)utimensat(AT_FDCWD, shard_path, ts, 0);
}

static void mutate_cat_offset(const char *shard_path, trij_binding_t *binding) {
    (void)shard_path;
    binding->catalog_offset += 8;
}

static void mutate_cat_entries(const char *shard_path, trij_binding_t *binding) {
    (void)shard_path;
    binding->catalog_entries += 1;
}

static void test_incomplete(const char *jdir, const char *shard_path) {
    trij_writer_t w;
    trij_reader_t r;
    char path[64];
    uint64_t uid;
    uint8_t type;
    uint32_t codes[8];
    int rc;

    /* a .tmp left by a crashed run (never finalized) must be ignored; clear any
     * published journal from an earlier case so only the .tmp remains */
    {
        char jpath[4096];
        if (trij_journal_path(jpath, sizeof(jpath), jdir, "uid_shard_0007.bin", 0) == 0)
            (void)unlink(jpath);
    }
    memset(&w, 0, sizeof(w));
    if (trij_writer_create(&w, jdir, "uid_shard_0007.bin") != 0) {
        fail("incomplete create", "create failed");
        return;
    }
    entry_make(2, path, sizeof(path), &uid, &type, codes, 8);
    (void)trij_writer_append(&w, path, strlen(path), uid, type, codes, 2);
    trij_writer_close_fd(&w); /* simulate crash: .tmp stays, no finalize, no rename */
    free(w.path_tmp);
    free(w.path_final);
    free(w.block);

    memset(&r, 0, sizeof(r));
    rc = trij_reader_open_validate(&r, jdir, shard_path, NULL);
    check_int("leftover .tmp ignored", rc, 0);
    if (rc == 1) trij_reader_close(&r);
}

static void test_empty_journal(const char *jdir, const char *shard_path) {
    trij_writer_t w;
    trij_reader_t r;
    trij_binding_t binding;
    const char *p;
    size_t plen, cc;
    uint64_t uid;
    uint8_t type;
    const uint32_t *codes;
    int rc;

    binding_from_stat(shard_path, &binding);
    memset(&w, 0, sizeof(w));
    if (trij_writer_create(&w, jdir, "uid_shard_0007.bin") != 0) {
        fail("empty create", "create failed");
        return;
    }
    rc = trij_writer_finalize(&w, &binding);
    check_int("empty finalize", rc, 0);
    if (rc != 0) return;
    memset(&r, 0, sizeof(r));
    rc = trij_reader_open_validate(&r, jdir, shard_path, NULL);
    check_int("empty open_validate", rc, 1);
    if (rc != 1) return;
    rc = trij_reader_next(&r, &p, &plen, &uid, &type, &codes, &cc);
    check_int("empty journal clean EOF", rc, 0);
    trij_reader_close(&r);
}

static void test_missing_journal(const char *jdir, const char *shard_path) {
    trij_reader_t r;
    char jpath[4096];
    int rc;

    if (trij_journal_path(jpath, sizeof(jpath), jdir, "uid_shard_0007.bin", 0) != 0) {
        fail("missing setup", "path build failed");
        return;
    }
    (void)unlink(jpath);
    memset(&r, 0, sizeof(r));
    rc = trij_reader_open_validate(&r, jdir, shard_path, NULL);
    check_int("missing journal falls back", rc, 0);
    if (rc == 1) trij_reader_close(&r);
}

static void test_corrupt_body(const char *jdir, const char *shard_path) {
    trij_writer_t w;
    trij_reader_t r;
    trij_binding_t binding;
    char jpath[4096];
    char path[64];
    uint64_t uid;
    uint8_t type;
    uint32_t codes[8];
    struct stat st;
    int fd;
    unsigned int i;
    int rc;
    int saw_corrupt = 0;

    binding_from_stat(shard_path, &binding);
    memset(&w, 0, sizeof(w));
    if (trij_writer_create(&w, jdir, "uid_shard_0007.bin") != 0) {
        fail("corrupt create", "create failed");
        return;
    }
    for (i = 0; i < 2000; i++) {
        size_t cc = entry_make(i, path, sizeof(path), &uid, &type, codes, 8);
        (void)trij_writer_append(&w, path, strlen(path), uid, type, codes, cc);
    }
    if (trij_writer_finalize(&w, &binding) != 0) {
        fail("corrupt finalize", "finalize failed");
        return;
    }

    /* flip a byte in the middle of the body: inside the first zstd frame */
    if (trij_journal_path(jpath, sizeof(jpath), jdir, "uid_shard_0007.bin", 0) != 0 ||
        stat(jpath, &st) != 0 || st.st_size < 256) {
        fail("corrupt setup", "journal missing or too small");
        return;
    }
    fd = open(jpath, O_RDWR);
    if (fd < 0) {
        fail("corrupt setup", "open failed");
        return;
    }
    {
        off_t off = st.st_size / 2;
        unsigned char b;
        if (pread(fd, &b, 1, off) != 1 || pwrite(fd, &(unsigned char){(unsigned char)(b ^ 0x5a)}, 1, off) != 1) {
            fail("corrupt setup", "flip failed");
            (void)close(fd);
            return;
        }
    }
    (void)close(fd);

    memset(&r, 0, sizeof(r));
    rc = trij_reader_open_validate(&r, jdir, shard_path, NULL);
    check_int("corrupt body still validates (header intact)", rc, 1);
    if (rc != 1) return;
    for (;;) {
        const char *p;
        size_t plen, cc;
        const uint32_t *gcodes;
        int got = trij_reader_next(&r, &p, &plen, &uid, &type, &gcodes, &cc);
        if (got < 0) {
            saw_corrupt = 1;
            break;
        }
        if (got == 0) break;
    }
    check_int("corrupt body detected during replay", saw_corrupt, 1);
    trij_reader_close(&r);
}

static void test_path_overflow(const char *jdir) {
    char buf[16];

    check_int("trij_journal_path small buffer", trij_journal_path(buf, sizeof(buf), jdir, "uid_shard_0007.bin", 0),
              -1);
}

int main(void) {
    char tmpl[] = "/tmp/test_trij_XXXXXX";
    char *dir;
    char jdir[PATH_MAX];
    char shard[PATH_MAX];

    dir = mkdtemp(tmpl);
    if (!dir) {
        perror("mkdtemp");
        return 1;
    }
    snprintf(jdir, sizeof(jdir), "%s/journals", dir);
    snprintf(shard, sizeof(shard), "%s/uid_shard_0007.bin", dir);
    if (mkdir(jdir, 0700) != 0) {
        perror("mkdir");
        return 1;
    }
    if (write_fake_shard(shard) != 0) {
        perror("write_fake_shard");
        return 1;
    }

    test_round_trip(jdir, shard);
    test_empty_journal(jdir, shard);
    test_incomplete(jdir, shard);
    expect_reject("reject size mismatch", jdir, shard, mutate_size);
    expect_reject("reject mtime mismatch", jdir, shard, mutate_mtime);
    expect_reject("reject catalog_offset mismatch", jdir, shard, mutate_cat_offset);
    expect_reject("reject catalog_entries mismatch", jdir, shard, mutate_cat_entries);
    test_missing_journal(jdir, shard);
    test_corrupt_body(jdir, shard);
    test_path_overflow(jdir);

    printf("test_crawl_trijournal: %d checks, %d failures\n", g_checks, g_fail);
    return g_fail ? 1 : 0;
}
