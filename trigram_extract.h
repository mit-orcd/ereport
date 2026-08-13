#ifndef TRIGRAM_EXTRACT_H
#define TRIGRAM_EXTRACT_H

#include <stddef.h>
#include <stdint.h>

/*
 * Basename trigram extraction, shared by ereport_index (index time) and ecrawl
 * (crawl-time trigram journals). Trigrams are 24-bit rolling-window codes over
 * the ASCII-lowercased final path component; parents are indexed when those
 * entries appear as their own basenames.
 */

/* Scratch buffers grown by trigram_ensure_buf; passing NULL scratch mallocs per call. */
typedef struct {
    char *codes_buf;
    size_t codes_cap;
    char *lower_seg_buf;
    size_t lower_seg_cap;
} trigram_scratch_t;

int trigram_ensure_buf(char **buf, size_t *cap, size_t need);

int trigram_cmp_u32(const void *a, const void *b);

/*
 * Emit the sorted, de-duplicated trigram codes for path's basename. On success
 * *out_codes points at the scratch buffer (or heap memory the caller owns when
 * scratch is NULL); it is valid until the next call with that scratch and must
 * not be freed. A basename shorter than 3 bytes yields *out_codes == NULL,
 * *out_count == 0.
 */
int trigram_extract_path(const char *path, uint32_t **out_codes, size_t *out_count,
                         trigram_scratch_t *scratch);

#endif /* TRIGRAM_EXTRACT_H */
