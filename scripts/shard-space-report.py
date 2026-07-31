#!/usr/bin/env python3
"""Break down on-disk space of ecrawl v6 uid_shard_*.bin shards.

For each shard the file is:  [32B header][compressed record blocks][catalog].
This reports how many bytes go to the compressed record region vs. the
(uncompressed, per-shard-duplicated) catalog, the zstd compression ratio of
the record region, and how much is wasted in partial "runt" final blocks.

The point of the tool is to decide whether lowering the default shard count is
worthwhile: catalog bytes and runt bytes scale with shard count, the compressed
record payload does not. No decompression is performed - block raw sizes are
read straight from the self-describing block headers.

Usage:
  scripts/shard-space-report.py <crawl-output-dir> [more-dirs-or-bins ...]
  scripts/shard-space-report.py --per-shard <crawl-output-dir>
"""

import argparse
import os
import struct
import sys

MAGIC = b"ERCBIN07"
HEADER_FMT = "<8sIIQQ"          # magic, version, reserved, catalog_offset, reserved64
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # 32
# raw_size, comp_size, max_record_size, record_count, type_mask, reserved16
BLOCK_FMT = "<IIQIHH"
BLOCK_SIZE = struct.calcsize(BLOCK_FMT)     # 24
CATALOG_ENTRY_SIZE = 64         # bin_dir_catalog_entry_t fixed part (name bytes follow)


class ShardStats:
    __slots__ = ("path", "file_bytes", "ckpt_bytes", "rec_comp_bytes", "rec_raw_bytes",
                 "catalog_bytes", "n_blocks", "runt_raw_bytes", "n_runt_shards",
                 "n_catalog_entries")

    def __init__(self, path):
        self.path = path
        self.file_bytes = 0
        self.ckpt_bytes = 0          # size of the matching .bin.ckpt sidecar
        self.rec_comp_bytes = 0
        self.rec_raw_bytes = 0
        self.catalog_bytes = 0
        self.n_blocks = 0
        self.runt_raw_bytes = 0      # raw bytes sitting in sub-target final blocks
        self.n_runt_shards = 0       # shards whose final block never hit the target
        self.n_catalog_entries = 0


def find_shards(targets):
    bins = []
    for t in targets:
        if os.path.isdir(t):
            for root, _dirs, files in os.walk(t):
                for f in files:
                    if f.startswith("uid_shard_") and f.endswith(".bin"):
                        bins.append(os.path.join(root, f))
        elif os.path.isfile(t):
            bins.append(t)
        else:
            print(f"warning: not found: {t}", file=sys.stderr)
    return sorted(bins)


def analyze_shard(path, block_target):
    st = ShardStats(path)
    st.file_bytes = os.path.getsize(path)
    ckpt = path + ".ckpt"
    if os.path.isfile(ckpt):
        st.ckpt_bytes = os.path.getsize(ckpt)
    with open(path, "rb") as fp:
        hdr = fp.read(HEADER_SIZE)
        if len(hdr) < HEADER_SIZE:
            raise ValueError("truncated header")
        magic, version, _res, catalog_off, _res64 = struct.unpack(HEADER_FMT, hdr)
        if magic != MAGIC:
            raise ValueError(f"bad magic {magic!r} (expected {MAGIC!r})")
        if catalog_off == 0:
            raise ValueError("incomplete shard (catalog_offset == 0)")
        if catalog_off > st.file_bytes:
            raise ValueError("catalog_offset past EOF")

        st.catalog_bytes = st.file_bytes - catalog_off

        # Walk self-describing block frames in [HEADER_SIZE, catalog_off).
        pos = HEADER_SIZE
        last_raw = 0
        while pos < catalog_off:
            fp.seek(pos)
            bh = fp.read(BLOCK_SIZE)
            if len(bh) < BLOCK_SIZE:
                raise ValueError("truncated block header")
            raw_size, comp_size = struct.unpack(BLOCK_FMT, bh)[:2]
            st.n_blocks += 1
            st.rec_raw_bytes += raw_size
            st.rec_comp_bytes += comp_size
            last_raw = raw_size
            pos += BLOCK_SIZE + comp_size
        if pos != catalog_off:
            raise ValueError("block region did not land on catalog_offset")
        # The final block is "runt" if it never reached the flush target.
        if st.n_blocks > 0 and last_raw < block_target:
            st.runt_raw_bytes = last_raw
            st.n_runt_shards = 1

        # Catalog entry count (uint64 right after catalog_offset).
        fp.seek(catalog_off)
        nbuf = fp.read(8)
        if len(nbuf) == 8:
            st.n_catalog_entries = struct.unpack("<Q", nbuf)[0]
    return st


def human(n):
    f = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(f) < 1024.0 or unit == "TiB":
            return f"{f:.2f} {unit}" if unit != "B" else f"{int(f)} B"
        f /= 1024.0
    return f"{f:.2f} TiB"


def pct(part, whole):
    return (100.0 * part / whole) if whole else 0.0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("targets", nargs="+",
                    help="crawl-output dir(s) and/or individual uid_shard_*.bin files")
    ap.add_argument("--per-shard", action="store_true",
                    help="print one line per shard before the aggregate")
    ap.add_argument("--block-target", type=int, default=(1 << 18),
                    help="raw bytes per full block, for runt detection (default 262144)")
    args = ap.parse_args()

    shards = find_shards(args.targets)
    if not shards:
        print("no uid_shard_*.bin files found", file=sys.stderr)
        return 2

    tot = ShardStats("<total>")
    n_ok = 0
    n_err = 0
    if args.per_shard:
        print(f"{'shard':<40} {'file':>12} {'rec_comp':>12} {'catalog':>10} "
              f"{'ckpt':>8} {'cat%':>6} {'ratio':>6} {'blocks':>7}")
    for path in shards:
        try:
            st = analyze_shard(path, args.block_target)
        except Exception as e:  # noqa: BLE001 - report and continue
            print(f"  skip {path}: {e}", file=sys.stderr)
            n_err += 1
            continue
        n_ok += 1
        tot.file_bytes += st.file_bytes
        tot.ckpt_bytes += st.ckpt_bytes
        tot.rec_comp_bytes += st.rec_comp_bytes
        tot.rec_raw_bytes += st.rec_raw_bytes
        tot.catalog_bytes += st.catalog_bytes
        tot.n_blocks += st.n_blocks
        tot.runt_raw_bytes += st.runt_raw_bytes
        tot.n_runt_shards += st.n_runt_shards
        tot.n_catalog_entries += st.n_catalog_entries
        if args.per_shard:
            ratio = (st.rec_raw_bytes / st.rec_comp_bytes) if st.rec_comp_bytes else 0.0
            print(f"{os.path.basename(path):<40} {human(st.file_bytes):>12} "
                  f"{human(st.rec_comp_bytes):>12} {human(st.catalog_bytes):>10} "
                  f"{human(st.ckpt_bytes):>8} {pct(st.catalog_bytes, st.file_bytes):>5.1f}% "
                  f"{ratio:>5.2f}x {st.n_blocks:>7}")

    header_bytes = n_ok * HEADER_SIZE
    ratio = (tot.rec_raw_bytes / tot.rec_comp_bytes) if tot.rec_comp_bytes else 0.0
    on_disk_total = tot.file_bytes + tot.ckpt_bytes
    # Real on-disk bytes that scale ~linearly with shard count (1x per shard).
    scaling_overhead = tot.catalog_bytes + header_bytes + tot.ckpt_bytes

    print("\n=== aggregate ===")
    print(f"shards parsed            : {n_ok}" + (f"  ({n_err} skipped)" if n_err else ""))
    print(f"on-disk total (bin+ckpt) : {human(on_disk_total)}")
    print(f"  compressed records     : {human(tot.rec_comp_bytes)} "
          f"({pct(tot.rec_comp_bytes, on_disk_total):.1f}%)")
    print(f"  catalog (uncompressed) : {human(tot.catalog_bytes)} "
          f"({pct(tot.catalog_bytes, on_disk_total):.1f}%)")
    print(f"  file headers (32B each): {human(header_bytes)} "
          f"({pct(header_bytes, on_disk_total):.1f}%)")
    print(f"  .ckpt sidecars         : {human(tot.ckpt_bytes)} "
          f"({pct(tot.ckpt_bytes, on_disk_total):.1f}%)")
    print(f"record region ratio      : {ratio:.2f}x "
          f"(raw {human(tot.rec_raw_bytes)} -> comp {human(tot.rec_comp_bytes)})")
    print(f"blocks total             : {tot.n_blocks}  "
          f"(avg {tot.n_blocks / n_ok:.1f}/shard)")
    print(f"catalog rows total       : {tot.n_catalog_entries} "
          f"(avg {tot.n_catalog_entries / n_ok:.0f}/shard; sum incl. cross-shard duplication)")
    print(f"runt tail blocks         : {tot.n_runt_shards}/{n_ok} shards, "
          f"{pct(tot.runt_raw_bytes, tot.rec_raw_bytes):.1f}% of raw record bytes "
          f"in sub-{args.block_target}B final blocks (compress less efficiently)")

    print("\n=== shard-count sensitivity ===")
    print(f"per-shard on-disk overhead (catalog + headers + ckpt): {human(scaling_overhead)} "
          f"({pct(scaling_overhead, on_disk_total):.1f}% of on-disk total)")
    print("  This scales ~1x per shard; the compressed record payload does not.")
    print(f"  Rough projection if you change shard count from N to N':")
    print(f"    new overhead ~= current x (N'/N), payload ~unchanged.")
    if n_ok > 0:
        per_shard = scaling_overhead / n_ok
        print(f"    measured ~{human(per_shard)} of scaling overhead per shard "
              f"(catalog duplication dominates on multi-user trees).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
