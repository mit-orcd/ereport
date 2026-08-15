/*
 * crawl_trijournal — crawl-time trigram journal format (see crawl_trijournal.h).
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 Michel Erb — see LICENSE.
 */

#define _XOPEN_SOURCE 700

#include "crawl_trijournal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <zstd.h>

#define TRIJ_ZSTD_LEVEL 1
/* Ceiling for one decompressed block; the writer targets TRIJ_BLOCK_TARGET_BYTES. */
#define TRIJ_MAX_BLOCK_BYTES ((size_t)64 * 1024 * 1024)

/* ------------------------------------------------------------------ varint */

static unsigned char *trij_put_varint(unsigned char *p, uint64_t v) {
    while (v >= 0x80U) {
        *p++ = (unsigned char)(v | 0x80U);
        v >>= 7;
    }
    *p++ = (unsigned char)v;
    return p;
}

/* *end is one past the decodable region; NULL return = truncated or overlong. */
static const unsigned char *trij_get_varint(const unsigned char *p, const unsigned char *end,
                                            uint64_t *out) {
    uint64_t v = 0;
    unsigned shift = 0;

    while (p < end && shift < 64) {
        unsigned char b = *p++;
        v |= (uint64_t)(b & 0x7FU) << shift;
        if (!(b & 0x80U)) {
            *out = v;
            return p;
        }
        shift += 7;
    }
    return NULL;
}

static size_t trij_varint_len(uint64_t v) {
    size_t n = 1;
    while (v >= 0x80U) {
        v >>= 7;
        n++;
    }
    return n;
}

/* ------------------------------------------------------------------- misc */

int trij_journal_path(char *buf, size_t cap, const char *dir, const char *shard_basename, int tmp) {
    int n = snprintf(buf, cap, "%s/%s.tij%s", dir, shard_basename, tmp ? ".tmp" : "");
    if (n < 0 || (size_t)n >= cap) return -1;
    return 0;
}

static int trij_write_all(int fd, const void *buf, size_t len) {
    const unsigned char *p = (const unsigned char *)buf;
    while (len > 0) {
        ssize_t w = write(fd, p, len);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += (size_t)w;
        len -= (size_t)w;
    }
    return 0;
}

static int trij_pwrite_all(int fd, const void *buf, size_t len, off_t off) {
    const unsigned char *p = (const unsigned char *)buf;
    while (len > 0) {
        ssize_t w = pwrite(fd, p, len, off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += (size_t)w;
        off += w;
        len -= (size_t)w;
    }
    return 0;
}

static int trij_read_all(int fd, void *buf, size_t len) {
    unsigned char *p = (unsigned char *)buf;
    while (len > 0) {
        ssize_t r = read(fd, p, len);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return 1; /* clean EOF partway: caller decides */
        p += (size_t)r;
        len -= (size_t)r;
    }
    return 0;
}

static int trij_grow(void **buf, size_t *cap, size_t need) {
    void *p;
    size_t nc;

    if (*cap >= need) return 0;
    nc = *cap ? *cap : 4096U;
    while (nc < need) nc <<= 1;
    p = realloc(*buf, nc);
    if (!p) return -1;
    *buf = p;
    *cap = nc;
    return 0;
}

/* ----------------------------------------------------------------- writer */

int trij_writer_create(trij_writer_t *w, const char *journal_dir, const char *shard_basename) {
    char tmp[4096];
    char final[4096];
    trij_hdr_t hdr;
    size_t name_len = strlen(shard_basename);

    memset(w, 0, sizeof(*w));
    w->fd = -1;
    if (name_len == 0 || name_len > 1024) return -1;
    if (trij_journal_path(tmp, sizeof(tmp), journal_dir, shard_basename, 1) != 0) return -1;
    if (trij_journal_path(final, sizeof(final), journal_dir, shard_basename, 0) != 0) return -1;

    w->fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (w->fd < 0) return -1;

    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, CRAWL_TRIJ_MAGIC, CRAWL_TRIJ_MAGIC_LEN);
    hdr.version = TRIJOURNAL_VERSION;
    hdr.flags = 0; /* complete only via trij_writer_finalize */
    hdr.name_len = (uint32_t)name_len;
    if (trij_write_all(w->fd, &hdr, sizeof(hdr)) != 0 ||
        trij_write_all(w->fd, shard_basename, name_len) != 0) {
        trij_writer_abort(w);
        return -1;
    }

    w->path_tmp = strdup(tmp);
    w->path_final = strdup(final);
    if (!w->path_tmp || !w->path_final) {
        trij_writer_abort(w);
        return -1;
    }
    w->name_len = (uint32_t)name_len;
    w->file_off = (uint64_t)sizeof(hdr) + (uint64_t)name_len;
    w->bytes_written = w->file_off;
    return 0;
}

int trij_writer_reopen(trij_writer_t *w) {
    if (w->fd >= 0) return 0;
    if (!w->path_tmp) return -1;
    /* No O_APPEND: every write is a pwrite at a tracked offset (Linux pwrite
     * under O_APPEND ignores the offset, which would corrupt the header). */
    w->fd = open(w->path_tmp, O_WRONLY);
    if (w->fd < 0) return -1;
    return 0;
}

void trij_writer_close_fd(trij_writer_t *w) {
    if (w->fd >= 0) {
        (void)close(w->fd);
        w->fd = -1;
    }
}

static int trij_writer_flush_block(trij_writer_t *w) {
    trij_block_hdr_t bh;
    size_t bound;
    unsigned char *comp;
    size_t clen;
    int rc = -1;

    if (w->block_entries == 0) return 0;
    if (w->block_len > UINT32_MAX) {
        w->failed = 1;
        return -1;
    }

    bound = ZSTD_compressBound(w->block_len);
    comp = (unsigned char *)malloc(bound);
    if (!comp) {
        w->failed = 1;
        return -1;
    }
    clen = ZSTD_compress(comp, bound, w->block, w->block_len, TRIJ_ZSTD_LEVEL);
    if (ZSTD_isError(clen) || clen > UINT32_MAX) goto out;

    memset(&bh, 0, sizeof(bh));
    bh.n_entries = w->block_entries;
    bh.comp_len = (uint32_t)clen;
    bh.uncomp_len = (uint32_t)w->block_len;
    if (trij_writer_reopen(w) != 0) goto out;
    /* Record the block's seek target before advancing file_off: the v2 block
     * table written at finalize is what ranged (parallel) replay seeks by. */
    if (w->blocks_count == w->blocks_cap) {
        size_t ncap = w->blocks_cap ? w->blocks_cap * 2 : 64;
        trij_block_ent_t *nb = (trij_block_ent_t *)realloc(w->blocks, ncap * sizeof(*nb));
        if (!nb) goto out;
        w->blocks = nb;
        w->blocks_cap = ncap;
    }
    w->blocks[w->blocks_count].file_off = w->file_off;
    w->blocks[w->blocks_count].comp_len = (uint32_t)clen;
    w->blocks[w->blocks_count].n_entries = w->block_entries;
    w->blocks_count++;
    if (trij_pwrite_all(w->fd, &bh, sizeof(bh), (off_t)w->file_off) != 0) goto out;
    if (trij_pwrite_all(w->fd, comp, clen, (off_t)(w->file_off + sizeof(bh))) != 0) goto out;
    w->file_off += (uint64_t)sizeof(bh) + (uint64_t)clen;
    w->bytes_written += (uint64_t)sizeof(bh) + (uint64_t)clen;
    rc = 0;
out:
    free(comp);
    if (rc != 0) {
        w->failed = 1;
    } else {
        w->block_len = 0;
        w->block_entries = 0;
    }
    return rc;
}

int trij_writer_append(trij_writer_t *w, const char *path, size_t path_len, uint64_t uid,
                       uint8_t type, const uint32_t *codes, size_t code_count) {
    size_t need;
    size_t i;
    unsigned char *p;

    if (w->failed) return -1;
    if (path_len == 0 || path_len > TRIJ_MAX_PATH_BYTES || code_count > TRIJ_MAX_CODES) {
        w->failed = 1;
        return -1;
    }

    /* Upper bound: two u64 varints, one type byte, u32 codes at 5 bytes each. */
    need = w->block_len + trij_varint_len(path_len) + path_len + trij_varint_len(uid) + 1 +
           trij_varint_len(code_count) + 5 * code_count;
    if (trij_grow((void **)&w->block, &w->block_cap, need) != 0) {
        w->failed = 1;
        return -1;
    }

    p = w->block + w->block_len;
    p = trij_put_varint(p, (uint64_t)path_len);
    memcpy(p, path, path_len);
    p += path_len;
    p = trij_put_varint(p, uid);
    *p++ = type;
    p = trij_put_varint(p, (uint64_t)code_count);
    if (code_count > 0) {
        p = trij_put_varint(p, (uint64_t)codes[0]);
        for (i = 1; i < code_count; i++)
            p = trij_put_varint(p, (uint64_t)(codes[i] - codes[i - 1] - 1U));
    }
    w->block_len = (size_t)(p - w->block);
    w->block_entries++;
    w->record_count++;

    if (w->block_len >= TRIJ_BLOCK_TARGET_BYTES) return trij_writer_flush_block(w);
    return 0;
}

int trij_writer_finalize(trij_writer_t *w, const trij_binding_t *binding) {
    trij_hdr_t hdr;

    if (w->failed) return -1;
    if (trij_writer_flush_block(w) != 0) return -1;
    if (trij_writer_reopen(w) != 0) {
        w->failed = 1;
        return -1;
    }

    /* The block table goes after the last block, before the header rewrite:
     * a crash anywhere in here leaves flags=0 in the on-disk header and the
     * .tmp is never renamed, so readers never see a partial table. */
    if (w->blocks_count > 0) {
        size_t table_len = w->blocks_count * sizeof(*w->blocks);
        if (trij_pwrite_all(w->fd, w->blocks, table_len, (off_t)w->file_off) != 0) {
            w->failed = 1;
            return -1;
        }
        w->bytes_written += (uint64_t)table_len;
    }

    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, CRAWL_TRIJ_MAGIC, CRAWL_TRIJ_MAGIC_LEN);
    hdr.version = TRIJOURNAL_VERSION;
    hdr.flags = TRIJ_FLAG_COMPLETE;
    hdr.record_count = w->record_count;
    hdr.shard_size = binding->shard_size;
    hdr.shard_mtime_sec = binding->shard_mtime_sec;
    hdr.shard_mtime_nsec = binding->shard_mtime_nsec;
    hdr.catalog_offset = binding->catalog_offset;
    hdr.catalog_entries = binding->catalog_entries;
    hdr.max_dir_id = binding->max_dir_id;
    hdr.block_table_off = w->blocks_count ? w->file_off : 0;
    hdr.block_count = (uint64_t)w->blocks_count;
    /* The basename was written at create; only the fixed header is rewritten. */
    hdr.name_len = w->name_len;
    if (trij_pwrite_all(w->fd, &hdr, sizeof(hdr), 0) != 0) {
        w->failed = 1;
        return -1;
    }
    if (fdatasync(w->fd) != 0) {
        w->failed = 1;
        return -1;
    }
    if (close(w->fd) != 0) {
        w->fd = -1;
        w->failed = 1;
        return -1;
    }
    w->fd = -1;
    if (rename(w->path_tmp, w->path_final) != 0) {
        w->failed = 1;
        return -1;
    }
    return 0;
}

void trij_writer_abort(trij_writer_t *w) {
    if (w->fd >= 0) {
        (void)close(w->fd);
        w->fd = -1;
    }
    if (w->path_tmp) {
        (void)unlink(w->path_tmp);
        free(w->path_tmp);
        w->path_tmp = NULL;
    }
    free(w->path_final);
    w->path_final = NULL;
    free(w->block);
    w->block = NULL;
    w->block_cap = 0;
    w->block_len = 0;
    w->block_entries = 0;
    free(w->blocks);
    w->blocks = NULL;
    w->blocks_count = 0;
    w->blocks_cap = 0;
}

/* ----------------------------------------------------------------- reader */

int trij_reader_open_validate(trij_reader_t *r, const char *journal_dir, const char *shard_path,
                              trij_hdr_t *hdr_out) {
    char jpath[4096];
    const char *base = strrchr(shard_path, '/');
    size_t base_len;
    char *name = NULL;
    int sfd = -1;
    int valid = 0;

    base = base ? base + 1 : shard_path;
    base_len = strlen(base);
    memset(r, 0, sizeof(*r));
    r->fd = -1;

    if (trij_journal_path(jpath, sizeof(jpath), journal_dir, base, 0) != 0) return 0;
    r->fd = open(jpath, O_RDONLY);
    if (r->fd < 0) return 0; /* missing journal: normal fallback */

    if (trij_read_all(r->fd, &r->hdr, sizeof(r->hdr)) != 0) goto out;
    if (memcmp(r->hdr.magic, CRAWL_TRIJ_MAGIC, CRAWL_TRIJ_MAGIC_LEN) != 0) goto out;
    if (r->hdr.version != TRIJOURNAL_VERSION) goto out;
    if (!(r->hdr.flags & TRIJ_FLAG_COMPLETE)) goto out;
    if (r->hdr.name_len == 0 || r->hdr.name_len > 1024) goto out;
    if ((uint64_t)r->hdr.name_len != (uint64_t)base_len) goto out;

    name = (char *)malloc(base_len + 1);
    if (!name) goto out;
    if (trij_read_all(r->fd, name, base_len) != 0) goto out;
    name[base_len] = '\0';
    if (memcmp(name, base, base_len) != 0) goto out;

    /* The five-fact live check against the shard itself. */
    sfd = open(shard_path, O_RDONLY);
    if (sfd < 0) goto out;
    {
        struct stat st;
        bin_file_header_t fh;
        uint64_t n_entries = 0;

        if (fstat(sfd, &st) != 0 || !S_ISREG(st.st_mode)) goto out;
        if ((uint64_t)st.st_size != r->hdr.shard_size) goto out;
        if ((uint64_t)st.st_mtim.tv_sec != r->hdr.shard_mtime_sec) goto out;
        if ((uint64_t)st.st_mtim.tv_nsec != r->hdr.shard_mtime_nsec) goto out;
        if (pread(sfd, &fh, sizeof(fh), 0) != (ssize_t)sizeof(fh)) goto out;
        if (!crawl_bin_hdr_magic_ok(fh.magic, fh.version, FORMAT_VERSION)) goto out;
        if (fh.catalog_offset != r->hdr.catalog_offset) goto out;
        if (r->hdr.catalog_offset < sizeof(fh) || r->hdr.catalog_offset > r->hdr.shard_size) goto out;
        if (r->hdr.shard_size - r->hdr.catalog_offset < sizeof(uint64_t)) goto out;
        if (pread(sfd, &n_entries, sizeof(n_entries), (off_t)r->hdr.catalog_offset) !=
            (ssize_t)sizeof(n_entries))
            goto out;
        if (n_entries != r->hdr.catalog_entries) goto out;
    }

    /* Bound sequential replay to the published blocks: the v2 block table sits
     * after the last block, so "read until physical EOF" would parse table bytes
     * as a block header. A set_block_range call overrides this bound. */
    r->blocks_left = r->hdr.block_count;
    valid = 1;
out:
    if (sfd >= 0) (void)close(sfd);
    free(name);
    if (!valid) {
        if (r->fd >= 0) {
            (void)close(r->fd);
            r->fd = -1;
        }
        return 0;
    }
    if (hdr_out) *hdr_out = r->hdr;
    return 1;
}

int trij_reader_next(trij_reader_t *r, const char **path, size_t *path_len, uint64_t *uid,
                     uint8_t *type, const uint32_t **codes, size_t *code_count) {
    const unsigned char *p;
    const unsigned char *end;
    uint64_t v;

    if (r->eof) return 0;

    if (r->block_entries_left == 0) {
        trij_block_hdr_t bh;
        int rr;

        if (r->blocks_left == 0) {
            r->eof = 1;
            return 0;
        }
        rr = trij_read_all(r->fd, &bh, sizeof(bh));
        if (rr > 0) {
            r->eof = 1;
            return 0;
        }
        if (rr < 0) return -1;
        if (bh.n_entries == 0 || bh.uncomp_len == 0 || bh.uncomp_len > TRIJ_MAX_BLOCK_BYTES ||
            bh.comp_len == 0 || bh.comp_len > ZSTD_compressBound(bh.uncomp_len))
            return -1;
        if (trij_grow((void **)&r->cbuf, &r->cbuf_cap, bh.comp_len) != 0) return -1;
        if (trij_grow((void **)&r->ubuf, &r->ubuf_cap, bh.uncomp_len) != 0) return -1;
        if (trij_read_all(r->fd, r->cbuf, bh.comp_len) != 0) return -1;
        {
            size_t d = ZSTD_decompress(r->ubuf, bh.uncomp_len, r->cbuf, bh.comp_len);
            if (ZSTD_isError(d) || d != bh.uncomp_len) return -1;
        }
        r->ubuf_len = bh.uncomp_len;
        r->ubuf_pos = 0;
        r->block_entries_left = bh.n_entries;
        r->blocks_left--;
    }

    p = r->ubuf + r->ubuf_pos;
    end = r->ubuf + r->ubuf_len;

    p = trij_get_varint(p, end, &v);
    if (!p || v == 0 || v > TRIJ_MAX_PATH_BYTES || (size_t)(end - p) < (size_t)v) return -1;
    *path = (const char *)p;
    *path_len = (size_t)v;
    p += v;

    p = trij_get_varint(p, end, &v);
    if (!p) return -1;
    *uid = v;

    if (p >= end) return -1;
    *type = *p++;

    p = trij_get_varint(p, end, &v);
    if (!p || v > TRIJ_MAX_CODES) return -1;
    *code_count = (size_t)v;
    if (v > 0) {
        uint64_t code;
        size_t i;

        if (trij_grow((void **)&r->codes, &r->codes_cap, (size_t)v * sizeof(uint32_t)) != 0)
            return -1;
        p = trij_get_varint(p, end, &code);
        if (!p || code > 0x00FFFFFFU) return -1;
        r->codes[0] = (uint32_t)code;
        for (i = 1; i < (size_t)v; i++) {
            uint64_t gap;

            p = trij_get_varint(p, end, &gap);
            if (!p) return -1;
            code += gap + 1U;
            if (code > 0x00FFFFFFU) return -1;
            r->codes[i] = (uint32_t)code;
        }
        *codes = r->codes;
    } else {
        *codes = NULL;
    }

    r->ubuf_pos = (size_t)(p - r->ubuf);
    r->block_entries_left--;
    r->entries_read++;
    return 1;
}

int trij_reader_load_block_table(trij_reader_t *r) {
    uint64_t i;
    size_t len;

    if (r->blocks) return 0; /* idempotent */
    if (r->fd < 0) return -1;
    if (r->hdr.block_count == 0) return 0; /* empty journal: no blocks to seek to */
    if (r->hdr.block_count > (UINT64_MAX / sizeof(trij_block_ent_t))) return -1;
    if (r->hdr.block_table_off < sizeof(trij_hdr_t)) return -1;
    len = (size_t)r->hdr.block_count * sizeof(trij_block_ent_t);
    r->blocks = (trij_block_ent_t *)malloc(len);
    if (!r->blocks) return -1;
    {
        const unsigned char *p = (const unsigned char *)r->blocks;
        size_t left = len;
        off_t off = (off_t)r->hdr.block_table_off;

        while (left > 0) {
            ssize_t n = pread(r->fd, (void *)p, left, off);
            if (n < 0) {
                if (errno == EINTR) continue;
                goto bad;
            }
            if (n == 0) goto bad; /* table truncated */
            p += (size_t)n;
            off += n;
            left -= (size_t)n;
        }
    }
    r->block_count = r->hdr.block_count;
    /* Table sanity: ascending in-file offsets, each block header inside the file
     * region before the table itself, entry counts summing to record_count. */
    {
        uint64_t entries = 0;
        for (i = 0; i < r->block_count; i++) {
            uint64_t boff = r->blocks[i].file_off;
            if (boff < sizeof(trij_hdr_t) || boff + sizeof(trij_block_hdr_t) > r->hdr.block_table_off)
                goto bad;
            if (i > 0 && boff <= r->blocks[i - 1].file_off) goto bad;
            if ((uint64_t)r->blocks[i].comp_len > r->hdr.block_table_off - boff) goto bad;
            entries += r->blocks[i].n_entries;
        }
        if (entries != r->hdr.record_count) goto bad;
    }
    return 0;
bad:
    free(r->blocks);
    r->blocks = NULL;
    r->block_count = 0;
    return -1;
}

int trij_reader_set_block_range(trij_reader_t *r, uint64_t first, uint64_t nblocks) {
    if (!r->blocks || first > r->block_count || nblocks > r->block_count - first) return -1;
    if (nblocks == 0) {
        r->blocks_left = 0;
        r->block_entries_left = 0;
        r->eof = 0; /* a later non-empty range on this reader must still read */
        return 0;
    }
    if (lseek(r->fd, (off_t)r->blocks[first].file_off, SEEK_SET) < 0) return -1;
    r->blocks_left = nblocks;
    r->block_entries_left = 0;
    r->ubuf_pos = 0;
    r->ubuf_len = 0;
    r->eof = 0;
    return 0;
}

void trij_reader_close(trij_reader_t *r) {
    if (r->fd >= 0) {
        (void)close(r->fd);
        r->fd = -1;
    }
    free(r->cbuf);
    r->cbuf = NULL;
    r->cbuf_cap = 0;
    free(r->ubuf);
    r->ubuf = NULL;
    r->ubuf_cap = 0;
    free(r->codes);
    r->codes = NULL;
    r->codes_cap = 0;
    free(r->blocks);
    r->blocks = NULL;
    r->block_count = 0;
}
