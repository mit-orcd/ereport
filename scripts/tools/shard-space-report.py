#!/usr/bin/env python3
"""Break down on-disk space of ecrawl uid_shard_*.bin shards (ERCBIN08).

For each shard the file is:  [32B header][columnar row groups][catalog].
This reports how many bytes go to the compressed record region vs. the
(uncompressed, per-shard-duplicated) catalog, the zstd compression ratio of
the record region, and how much is wasted in partial "runt" final row groups.

Because the record region is columnar, it also breaks the compressed bytes down
per column and reports which encoding each column chose, which is what tells you
whether a column is earning its place: a column that RLEs to a few hundred bytes
per row group costs nothing, while a high-entropy one like inode dominates.

The point of the tool is to decide whether lowering the default shard count is
worthwhile: catalog bytes and runt bytes scale with shard count, the compressed
record payload does not. No decompression is performed - raw sizes and per-column
sizes are read straight from the self-describing row group headers.

Usage:
  scripts/tools/shard-space-report.py <crawl-output-dir> [more-dirs-or-bins ...]
  scripts/tools/shard-space-report.py --per-shard <crawl-output-dir>
  scripts/tools/shard-space-report.py --columns <crawl-output-dir>
"""

import argparse
import os
import struct
import sys

MAGIC = b"ERCBIN08"
HEADER_FMT = "<8sIIQQ"          # magic, version, reserved, catalog_offset, reserved64
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # 32
# record_count, column_count, comp_bytes, raw_bytes, type_mask, reserved16, reserved32
ROWGROUP_FMT = "<IIQQHHI"
ROWGROUP_SIZE = struct.calcsize(ROWGROUP_FMT)   # 32
# column_id, encoding, bit_width, reserved8, comp_bytes, raw_bytes, min_value, max_value
COLCHUNK_FMT = "<BBBBIQQQ"
COLCHUNK_SIZE = struct.calcsize(COLCHUNK_FMT)   # 32
# bin_dir_catalog_entry_t fixed part (name bytes follow): tree fields 24,
# imm_child_* 40, v8 dfs_/subtree_/self_bytes 72.
CATALOG_ENTRY_SIZE = 136
ROWGROUP_RAW_TARGET = 1 << 20   # CRAWL_BIN_ROWGROUP_RAW_TARGET

COLUMN_NAMES = [
    "parent_dir_id", "name_len", "type", "uid", "gid", "mode", "size", "inode",
    "dev_major", "dev_minor", "nlink", "atime", "mtime", "ctime", "name_bytes",
]
ENCODING_NAMES = ["RAW", "FOR_BITPACK", "RLE", "CONST", "BYTES"]


def column_name(cid):
    return COLUMN_NAMES[cid] if cid < len(COLUMN_NAMES) else f"col{cid}"


def encoding_name(enc):
    return ENCODING_NAMES[enc] if enc < len(ENCODING_NAMES) else f"enc{enc}"


class ColumnStats:
    __slots__ = ("comp_bytes", "raw_bytes", "chunks", "encodings")

    def __init__(self):
        self.comp_bytes = 0
        self.raw_bytes = 0
        self.chunks = 0
        self.encodings = {}

    def add(self, comp, raw, enc):
        self.comp_bytes += comp
        self.raw_bytes += raw
        self.chunks += 1
        self.encodings[enc] = self.encodings.get(enc, 0) + 1

    def merge(self, other):
        self.comp_bytes += other.comp_bytes
        self.raw_bytes += other.raw_bytes
        self.chunks += other.chunks
        for enc, n in other.encodings.items():
            self.encodings[enc] = self.encodings.get(enc, 0) + n


class ShardStats:
    __slots__ = ("path", "file_bytes", "ckpt_bytes", "rec_comp_bytes", "rec_raw_bytes",
                 "catalog_bytes", "n_groups", "dir_bytes", "runt_raw_bytes",
                 "n_runt_shards", "n_catalog_entries", "n_records", "columns")

    def __init__(self, path):
        self.path = path
        self.file_bytes = 0
        self.ckpt_bytes = 0          # size of the matching .bin.ckpt sidecar
        self.rec_comp_bytes = 0      # column payload bytes only
        self.rec_raw_bytes = 0
        self.catalog_bytes = 0
        self.n_groups = 0
        self.dir_bytes = 0           # row group headers + column directories
        self.runt_raw_bytes = 0      # raw bytes sitting in sub-target final groups
        self.n_runt_shards = 0       # shards whose final group never hit the target
        self.n_catalog_entries = 0
        self.n_records = 0
        self.columns = {}

    def column(self, cid):
        cs = self.columns.get(cid)
        if cs is None:
            cs = ColumnStats()
            self.columns[cid] = cs
        return cs

    def merge_columns(self, other):
        for cid, cs in other.columns.items():
            self.column(cid).merge(cs)


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


def analyze_shard(path, group_target):
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

        # Walk self-describing row groups in [HEADER_SIZE, catalog_off). Each is
        # a header, then column_count chunk headers, then the payloads in the
        # same order, so the directory alone accounts for every byte.
        pos = HEADER_SIZE
        last_raw = 0
        while pos < catalog_off:
            fp.seek(pos)
            gh = fp.read(ROWGROUP_SIZE)
            if len(gh) < ROWGROUP_SIZE:
                raise ValueError("truncated row group header")
            rec_count, col_count, comp_bytes, raw_bytes = struct.unpack(ROWGROUP_FMT, gh)[:4]
            if col_count > 64:
                raise ValueError(f"implausible column_count {col_count}")
            dir_bytes = col_count * COLCHUNK_SIZE
            cdir = fp.read(dir_bytes)
            if len(cdir) < dir_bytes:
                raise ValueError("truncated column directory")
            for i in range(col_count):
                off = i * COLCHUNK_SIZE
                cid, enc, _bw, _r8, c_comp, c_raw = struct.unpack_from(
                    COLCHUNK_FMT, cdir, off)[:6]
                st.column(cid).add(c_comp, c_raw, enc)

            st.n_groups += 1
            st.n_records += rec_count
            st.dir_bytes += ROWGROUP_SIZE + dir_bytes
            st.rec_raw_bytes += raw_bytes
            st.rec_comp_bytes += comp_bytes
            last_raw = raw_bytes
            pos += ROWGROUP_SIZE + dir_bytes + comp_bytes
        if pos != catalog_off:
            raise ValueError("row group region did not land on catalog_offset")
        # The final group is "runt" if it never reached the flush target.
        if st.n_groups > 0 and last_raw < group_target:
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


def print_column_table(tot):
    if not tot.columns:
        return
    print("\n=== record region by column ===")
    print(f"{'column':<15} {'comp':>11} {'share':>7} {'raw':>11} {'ratio':>7} "
          f"{'B/rec':>7}  encodings")
    for cid in sorted(tot.columns, key=lambda c: -tot.columns[c].comp_bytes):
        cs = tot.columns[cid]
        ratio = (cs.raw_bytes / cs.comp_bytes) if cs.comp_bytes else 0.0
        per_rec = (cs.comp_bytes / tot.n_records) if tot.n_records else 0.0
        encs = ", ".join(f"{encoding_name(e)}x{n}"
                         for e, n in sorted(cs.encodings.items(), key=lambda kv: -kv[1]))
        print(f"{column_name(cid):<15} {human(cs.comp_bytes):>11} "
              f"{pct(cs.comp_bytes, tot.rec_comp_bytes):>6.1f}% {human(cs.raw_bytes):>11} "
              f"{ratio:>6.2f}x {per_rec:>7.2f}  {encs}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("targets", nargs="+",
                    help="crawl-output dir(s) and/or individual uid_shard_*.bin files")
    ap.add_argument("--per-shard", action="store_true",
                    help="print one line per shard before the aggregate")
    ap.add_argument("--columns", action="store_true",
                    help="print the per-column size and encoding breakdown")
    ap.add_argument("--group-target", "--block-target", type=int, default=ROWGROUP_RAW_TARGET,
                    dest="group_target",
                    help=f"raw bytes per full row group, for runt detection "
                         f"(default {ROWGROUP_RAW_TARGET})")
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
              f"{'ckpt':>8} {'cat%':>6} {'ratio':>6} {'groups':>7}")
    for path in shards:
        try:
            st = analyze_shard(path, args.group_target)
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
        tot.n_groups += st.n_groups
        tot.dir_bytes += st.dir_bytes
        tot.n_records += st.n_records
        tot.runt_raw_bytes += st.runt_raw_bytes
        tot.n_runt_shards += st.n_runt_shards
        tot.n_catalog_entries += st.n_catalog_entries
        tot.merge_columns(st)
        if args.per_shard:
            ratio = (st.rec_raw_bytes / st.rec_comp_bytes) if st.rec_comp_bytes else 0.0
            print(f"{os.path.basename(path):<40} {human(st.file_bytes):>12} "
                  f"{human(st.rec_comp_bytes):>12} {human(st.catalog_bytes):>10} "
                  f"{human(st.ckpt_bytes):>8} {pct(st.catalog_bytes, st.file_bytes):>5.1f}% "
                  f"{ratio:>5.2f}x {st.n_groups:>7}")

    header_bytes = n_ok * HEADER_SIZE
    ratio = (tot.rec_raw_bytes / tot.rec_comp_bytes) if tot.rec_comp_bytes else 0.0
    on_disk_total = tot.file_bytes + tot.ckpt_bytes
    # Real on-disk bytes that scale ~linearly with shard count (1x per shard).
    scaling_overhead = tot.catalog_bytes + header_bytes + tot.ckpt_bytes

    print("\n=== aggregate ===")
    print(f"shards parsed            : {n_ok}" + (f"  ({n_err} skipped)" if n_err else ""))
    print(f"records                  : {tot.n_records}")
    print(f"on-disk total (bin+ckpt) : {human(on_disk_total)}")
    print(f"  compressed columns     : {human(tot.rec_comp_bytes)} "
          f"({pct(tot.rec_comp_bytes, on_disk_total):.1f}%)")
    print(f"  row group directories  : {human(tot.dir_bytes)} "
          f"({pct(tot.dir_bytes, on_disk_total):.1f}%)")
    print(f"  catalog (uncompressed) : {human(tot.catalog_bytes)} "
          f"({pct(tot.catalog_bytes, on_disk_total):.1f}%)")
    print(f"  file headers (32B each): {human(header_bytes)} "
          f"({pct(header_bytes, on_disk_total):.1f}%)")
    print(f"  .ckpt sidecars         : {human(tot.ckpt_bytes)} "
          f"({pct(tot.ckpt_bytes, on_disk_total):.1f}%)")
    print(f"record region ratio      : {ratio:.2f}x "
          f"(raw {human(tot.rec_raw_bytes)} -> comp {human(tot.rec_comp_bytes)})")
    if tot.n_records:
        print(f"bytes per record on disk : {tot.rec_comp_bytes / tot.n_records:.2f} "
              f"(columns only, excludes directories)")
    print(f"row groups total         : {tot.n_groups}  "
          f"(avg {tot.n_groups / n_ok:.1f}/shard)" if n_ok else "")
    print(f"catalog rows total       : {tot.n_catalog_entries} "
          f"(avg {tot.n_catalog_entries / n_ok:.0f}/shard; sum incl. cross-shard duplication)")
    print(f"runt tail groups         : {tot.n_runt_shards}/{n_ok} shards, "
          f"{pct(tot.runt_raw_bytes, tot.rec_raw_bytes):.1f}% of raw record bytes "
          f"in sub-{args.group_target}B final groups (compress less efficiently)")

    if args.columns:
        print_column_table(tot)

    print("\n=== shard-count sensitivity ===")
    print(f"per-shard on-disk overhead (catalog + headers + ckpt): {human(scaling_overhead)} "
          f"({pct(scaling_overhead, on_disk_total):.1f}% of on-disk total)")
    print("  This scales ~1x per shard; the compressed record payload does not.")
    print("  Rough projection if you change shard count from N to N':")
    print("    new overhead ~= current x (N'/N), payload ~unchanged.")
    if n_ok > 0:
        per_shard = scaling_overhead / n_ok
        print(f"    measured ~{human(per_shard)} of scaling overhead per shard "
              f"(catalog duplication dominates on multi-user trees).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
