# Testing

```bash
make check                              # unit tests + scripts/test/test.sh on a tiny /tmp tree (fast)
make check-tree                         # build a larger fixture under ./test, then test.sh against it
./scripts/test/test.sh /path/to/tree    # test.sh plus find/du correlation against a real tree
```

`make check` runs the C unit tests (`test_crawl_codec`, `test_crawl_block_filter`, `test_crawl_catalog`) and then `scripts/test/test.sh`, which crawls a small generated tree and cross-checks every tool against it: `ereport` single- and all-users counts against `ecrawl`, `ecrawl_query` routes (`catalog_rollup` / `dir_index` / `record_scan`) against each other and against `du -sb`, `edelete` dry-run, `ereport_index --make` output, `edump` round-trip, and — when `/dev/fuse` is available — an `ecrawl_mount` live mount compared with the source tree via `find`, `stat` and `du`. Each section explains what it checks and why in a comment block at the top of that section in `test.sh`.

With a directory argument, `test.sh` also builds a `find`/`fd` baseline (counts and unique regular-file bytes via `%D:%i`) and compares it with `ecrawl` and `ereport`. Expect exact equality only on a quiescent tree.

Useful switches: `--summary` (results table), `--keep-html[=DIR]` (keep the generated reports for browsing), `--edelete-only`, `SKIP_FS=1` (skip the correlation phase), `SKIP_FUSE=1`, and `ECRAWL=` / `EREPORT=` / `ECRAWL_QUERY=` / … to point at other binaries. Run `test.sh` on a compute node, not a cluster login node.

## Indexer comparison

[`scripts/compare-indexers/`](../scripts/compare-indexers/README.md) benchmarks the suite against Robinhood, GUFI, XDU and the classic `find`/`du` tools on a synthetic tree, cold and hot:

```bash
scripts/compare-indexers/benchmark.sh --do /tmp/small --small     # < 1 min: correctness of Q1–Q6 after a code change
scripts/compare-indexers/benchmark.sh --do /data/synth            # hours: full benchmark with every installed indexer
```
