/* Throwaway inspection tool: dump paths.bin in path_id order to verify
 * whether a directory's descendants are contiguous in path_id space.
 * Not part of the build; compiled by hand for the Option-1 design check. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <zstd.h>

#define PATHS_HDR_BYTES 40
#define PATHS_CHUNK_RAW ((size_t)256 * 1024)

typedef struct __attribute__((packed)) {
    uint64_t logical_start;
    uint64_t file_off;
    uint32_t stored_len;
    uint32_t raw_len;
} ent_t;

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: %s paths.bin path_offsets.bin [maxdump]\n", argv[0]); return 2; }
    FILE *pf = fopen(argv[1], "rb");
    FILE *of = fopen(argv[2], "rb");
    if (!pf || !of) { perror("open"); return 1; }
    unsigned char hdr[PATHS_HDR_BYTES];
    if (fread(hdr, 1, sizeof(hdr), pf) != sizeof(hdr)) { fprintf(stderr, "hdr\n"); return 1; }
    uint64_t chunk_count, table_off;
    memcpy(&chunk_count, hdr + 16, 8);
    memcpy(&table_off, hdr + 24, 8);
    ent_t *tab = malloc(chunk_count * sizeof(ent_t));
    if (fseeko(pf, (off_t)table_off, SEEK_SET) || fread(tab, sizeof(ent_t), chunk_count, pf) != chunk_count) {
        fprintf(stderr, "table\n"); return 1;
    }
    /* offsets file: array of u64, path i = [off[i], off[i+1]) */
    fseeko(of, 0, SEEK_END);
    uint64_t npaths = (uint64_t)ftello(of) / 8 - 1;
    fseeko(of, 0, SEEK_SET);
    uint64_t *offs = malloc((npaths + 1) * 8);
    if (fread(offs, 8, npaths + 1, of) != npaths + 1) { fprintf(stderr, "offs\n"); return 1; }

    uint64_t maxdump = (argc > 3) ? strtoull(argv[3], NULL, 10) : npaths;
    ZSTD_DCtx *dctx = ZSTD_createDCtx();
    unsigned char *raw = malloc(PATHS_CHUNK_RAW);
    unsigned char *stored = malloc(PATHS_CHUNK_RAW * 2);
    uint64_t cached = UINT64_MAX;
    char *path = malloc(65536);

    for (uint64_t pid = 0; pid < npaths && pid < maxdump; pid++) {
        uint64_t o0 = offs[pid], o1 = offs[pid + 1];
        uint64_t len = o1 - o0;
        /* find chunk */
        uint64_t lo = 0, hi = chunk_count, ci = UINT64_MAX;
        while (lo < hi) { uint64_t mid = lo + (hi - lo) / 2; if (tab[mid].logical_start <= o0) lo = mid + 1; else hi = mid; }
        if (lo) { lo--; if (o0 - tab[lo].logical_start < tab[lo].raw_len) ci = lo; }
        if (ci == UINT64_MAX) { printf("%6llu <nochunk>\n", (unsigned long long)pid); continue; }
        if (cached != ci) {
            if (tab[ci].stored_len == tab[ci].raw_len) {
                if (fseeko(pf, (off_t)tab[ci].file_off, SEEK_SET) || fread(raw, 1, tab[ci].raw_len, pf) != tab[ci].raw_len) { fprintf(stderr, "read chunk\n"); return 1; }
            } else {
                if (fseeko(pf, (off_t)tab[ci].file_off, SEEK_SET) || fread(stored, 1, tab[ci].stored_len, pf) != tab[ci].stored_len) { fprintf(stderr, "read stored\n"); return 1; }
                size_t d = ZSTD_decompressDCtx(dctx, raw, PATHS_CHUNK_RAW, stored, tab[ci].stored_len);
                if (ZSTD_isError(d) || d != tab[ci].raw_len) { fprintf(stderr, "decomp\n"); return 1; }
            }
            cached = ci;
        }
        uint64_t inchunk = o0 - tab[ci].logical_start;
        memcpy(path, raw + inchunk, len);
        path[len] = 0;
        printf("%6llu %s\n", (unsigned long long)pid, path);
    }
    return 0;
}
