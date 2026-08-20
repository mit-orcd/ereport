#!/usr/bin/env python3
"""Create the comparison figures from the harness CSV results.

Usage:
  plot_results.py <smoke-results> [<smoke-results> ...] [--out-dir DIR]

Each input may be a run_smoke result root or a directory containing
index_results.csv/query_results.csv. Multiple inputs become dataset series,
matching the paper's Set 1 / Set 2 comparison.
"""

import argparse
import csv
import math
import statistics
import sys
import textwrap
from pathlib import Path

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.patches import Patch
    from matplotlib.ticker import FuncFormatter, LogLocator
    from matplotlib.transforms import ScaledTranslation
except ImportError:
    sys.stderr.write(
        "ERROR: matplotlib is required for charts. Rerun init.sh with "
        "INSTALL_PACKAGES=1, or install python3-matplotlib.\n"
    )
    raise SystemExit(2)


# gufi_dir2index runs twice per repetition: once for the plain index, and once
# more into a second directory so the rollup pass has something to chew on
# without destroying the first. Identical command either way (see the run's
# COMMANDS.txt), so the two variants are one measurement with twice the
# repetitions, not two builds to be charted side by side.
GUFI_DIR2INDEX = (("gufi", "plain"), ("gufi", "rollup_index"))

# Each indexer is one or more measured phases, and a phase pools every variant
# that ran the same command. Naming the phases beats a single opaque "suite"
# bar: a reader comparing against a one-shot indexer needs the total, and a
# reader tuning ecrawl needs to know which half to attack.
PIPELINES = [
    (
        "ecrawl + ereport_index",
        [
            ((("ecrawl", "write"),), "crawl"),
            ((("ereport_index", "make"),), "index"),
        ],
        "ecrawl",  # label when only the first phase ran
    ),
    (
        # Two stages, not two alternatives: the scan crawls into index-free
        # tables, which is not a database anyone queries, and the three CREATE
        # INDEX statements are what make them queryable. Charting the scan alone
        # billed Robinhood for half of what it costs.
        "Robinhood",
        [
            ((("robinhood", "scan"),), "crawl"),
            ((("robinhood", "indexes"),), "index"),
        ],
        "Robinhood (scan only)",
    ),
    (
        # statx variants: the same capture as ecrawl, only the inode-read
        # syscall differs. Crawl-only pipelines: the ereport_index half is
        # byte-identical to the baseline's, so re-running it would measure
        # nothing new.
        "ecrawl\n(--statx)",
        [((("ecrawl", "write_statx"),), "crawl")],
        "ecrawl\n(--statx)",
    ),
    (
        "ecrawl\n(--io_uring)",
        [((("ecrawl", "write_iouring"),), "crawl")],
        "ecrawl\n(--io_uring)",
    ),
    ("GUFI (dir2index)", [(GUFI_DIR2INDEX, "index")], None),
    (
        # An alternative to the row above, never an addition to it: rollup is a
        # second pass over the index that copies each directory's rows up into
        # its ancestors so a query opens fewer databases.
        "GUFI + rollup",
        [(GUFI_DIR2INDEX, "index"), ((("gufi", "rollup_step"),), "rollup")],
        None,
    ),
    ("XDU", [((("xdu", "index"),), "index")], None),
]
INDEX_ORDER = [
    "ecrawl + ereport_index",
    "ecrawl",
    "ecrawl\n(--statx)",
    "ecrawl\n(--io_uring)",
    "Robinhood",
    "Robinhood (scan only)",
    "GUFI (dir2index)",
    "GUFI + rollup",
    "XDU",
]
# Figures 3–4 are "unindexed tree to queryable index". The statx / io_uring
# write rows are crawl-only (ereport_index was not re-run), so ranking them
# against the full pipeline is a shorter bar for less work.
BUILD_ORDER = [
    label
    for label in INDEX_ORDER
    if label not in ("ecrawl\n(--statx)", "ecrawl\n(--io_uring)")
]

# Walk-only rows store nothing. They are not one peer group: find/fd (and
# ecrawl --no-stat --count) return a regular-file count with no inode reads,
# while du/dua/dut (and ecrawl --no-write) return apparent size. Figures 1–2
# draw those as two panels.
WALK_VARIANTS = ("walk", "nowrite", "nostat", "nowrite_statx", "nowrite_iouring")
WALK_LABELS = {
    ("ecrawl", "nostat"): "ecrawl\n(--no-stat --count)",
    ("ecrawl", "nowrite"): "ecrawl\n(--no-write)",
    ("ecrawl", "nowrite_statx"): "ecrawl\n(--no-write --statx)",
    ("ecrawl", "nowrite_iouring"): "ecrawl\n(--no-write --io_uring)",
    ("find", "walk"): "find",
    ("fd", "walk"): "fd",
    ("du", "walk"): "du",
    ("dua", "walk"): "dua",
    ("dut", "walk"): "dut",
}
# Names-only peers: timed job returns a regular-file count (answer_files=).
WALK_NAMES_ORDER = ["ecrawl\n(--no-stat --count)", "fd", "find"]
# Metadata + size peers: timed job returns apparent bytes (answer_bytes=).
WALK_META_ORDER = [
    "ecrawl\n(--no-write)",
    "ecrawl\n(--no-write --statx)",
    "ecrawl\n(--no-write --io_uring)",
    "dut",
    "dua",
    "du",
]
WALK_NAMES_LABELS = frozenset(WALK_NAMES_ORDER)
WALK_META_LABELS = frozenset(WALK_META_ORDER)
QUERY_ORDER = [
    "ecrawl_query",
    "ereport",
    "ereport_index",
    "Robinhood",
    "GUFI",
    "GUFI + rollup",
    "XDU",
    "ecrawl (live walk)",
    "find",
    "fd",
    "du",
    "dua",
    "dut",
]
# Q6 is not one of the paper's five. It asks for a substring with nothing
# anchored at either end, which is the shape a B-tree on names cannot seek into,
# and its title says so wherever it is drawn.
QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"]
QUERY_TITLES = {
    "Q1": "exact-name lookup",
    "Q2": "name pattern across the tree",
    "Q3": "files over a size threshold",
    "Q4": "total bytes in a subtree",
    "Q5": "list files in a subtree",
    "Q6": "name contains a token anywhere  [extra, not in the paper]",
}

# Indexers get saturated colour; the traditional walkers stay neutral, so the
# two classes of tool separate at a glance. Okabe-Ito, which stays legible for
# the common colour vision deficiencies and in greyscale print.
COLORS = {
    "ecrawl + ereport_index": "#0072B2",
    "ecrawl": "#0072B2",
    "ecrawl\n(--no-stat --count)": "#0072B2",
    "ecrawl\n(--no-write)": "#1A6FA8",
    # Same family, darker bases: lightness is reserved for cold vs hot.
    # Pale tints as the cold colour made --io_uring's pair look like one bar.
    "ecrawl\n(--statx)": "#1A6FA8",
    "ecrawl\n(--io_uring)": "#0A4E7A",
    "ecrawl\n(--no-write --statx)": "#1A6FA8",
    "ecrawl\n(--no-write --io_uring)": "#0A4E7A",
    "ereport": "#0072B2",
    "ecrawl_query": "#0072B2",
    # A walker, but the project's own: keep the ecrawl blue so it reads as the
    # family fd is measured against, not as one of the neutral walkers.
    "ecrawl (live walk)": "#0072B2",
    "ereport_index": "#56B4E9",
    "Robinhood": "#D55E00",
    "Robinhood (scan only)": "#D55E00",
    "GUFI": "#009E73",
    "GUFI (dir2index)": "#009E73",
    "GUFI + rollup": "#8FA01F",
    "XDU": "#CC79A7",
    "find": "#7A7A7A",
    "fd": "#B09070",
    "du": "#ADADAD",
    "dua": "#D9B48A",
    "dut": "#8C7B6B",
}
WALKERS = ("find", "fd", "du", "dua", "dut")

# Q4 sums bytes rather than counting entries, and the tools disagree slightly
# on which inodes to include (hard links, the sentinel directory). A fraction
# of a percent is that disagreement; anything larger is a different answer.
BYTES_TOLERANCE = 0.005
GRID = {"color": "#CCCCCC", "linestyle": ":", "linewidth": 0.7}

# Switch to a log x-axis once the spread makes a linear one useless. At 10x the
# smallest bar is a tenth of the largest; past that a linear panel stops
# ranking the tools and only shows which one is slowest. These runs routinely
# span three decades (GUFI's rollup against a trigram index), so without this
# every fast tool collapses into a sliver at the origin.
LOG_RANGE = 10.0

# US Letter pages. Index charts (figures 1-5) are landscape: a ranking is wide
# and short. Query charts (figure 6) stay portrait so three panels stack with
# room between them. Content is pinned to the top with print margins; the axes
# grow to fill most of the printable height so short rankings do not leave a
# large blank band under the legend.
LETTER_PORTRAIT = (8.5, 11.0)
LETTER_LANDSCAPE = (11.0, 8.5)
LETTER_MARGIN = 0.45  # inches of clear paper around the content band
# Fraction of the printable landscape height the index content band should use.
INDEX_PAGE_FILL = 0.96
# Minimum physical inches per tool row. Split labels need a bit more.
# Figure 5 adds a per-file line under the phase split, so those rows are taller.
INDEX_ROW_IN = 0.34
INDEX_ROW_SPLIT_IN = 0.62
INDEX_ROW_SIZE_IN = 0.95
# Physical inches per bar inside a query panel, plus a fixed pad for its title
# and the gap that keeps one panel's tick labels out of the next panel's bars.
QUERY_BAR_IN = 0.28
QUERY_PANEL_PAD_IN = 0.70
QUERY_PANEL_GAP_IN = 0.45


def _page_band(page, content_in):
    """Top-aligned content band on a page, as figure-fraction y bounds.

    Returns (y_bottom, y_top, page_height) so callers can place header, axes and
    footer inside the band and leave the rest of the page empty.
    """
    page_h = page[1]
    content_in = min(content_in, page_h - 2 * LETTER_MARGIN)
    y_top = 1.0 - LETTER_MARGIN / page_h
    y_bottom = y_top - content_in / page_h
    return y_bottom, y_top, page_h


def read_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


# Reading order rather than alphabetical: cold is what the work costs against
# untouched storage, hot what it costs once the metadata is in memory, and warm a
# cold pass the run had no privileges to actually drop caches for.
CACHE_ORDER = {"cold": 0, "warm": 1, "hot": 2}


def cache_states(rows):
    """Which passes these rows came from, in reading order. A single empty string
    for a CSV written before the column existed."""
    return sorted(
        {row.get("cache") or "" for row in rows},
        key=lambda name: (CACHE_ORDER.get(name, 3), name),
    ) or [""]


def metric_mean(dataset, key, tool, metric):
    if not dataset:
        return None
    return dataset.get(key, {}).get(tool, {}).get(metric, (None, None))[0]


def tool_rank_mean(datasets, key, tool, metric):
    """That tool's ranking number: its cold mean, else warm, else any series.

    A tool recorded as warm (no privilege to drop) still has to compete with
    the cold bars; ranking only the cold dataset would bury it.
    """
    by_cache = {}
    for dataset in datasets:
        mean = metric_mean(dataset, key, tool, metric)
        if mean is not None:
            by_cache.setdefault(dataset.get("cache") or "", mean)
    for want in ("cold", "warm", "hot", ""):
        if want in by_cache:
            return by_cache[want]
    if by_cache:
        return next(iter(by_cache.values()))
    return None


def sort_tools_best_first(tools, datasets, key, metric, higher_better):
    """Best at the start of the list (top after invert_yaxis), by each tool's cold."""

    def sort_key(tool):
        mean = tool_rank_mean(datasets, key, tool, metric)
        if mean is None:
            return (1, 0.0)
        value = -mean if higher_better else mean
        return (0, value)

    return sorted(tools, key=sort_key)


def rows_for_cache(rows, cache):
    return [row for row in rows if (row.get("cache") or "") == cache]


def multiple_trees(datasets):
    """Whether the series are different trees rather than different passes over
    one. Elapsed seconds do not carry across trees; across passes they are the
    whole point of the comparison."""
    return len({d.get("source") for d in datasets}) > 1


def subtitle_for(datasets):
    """The tree and the passes over it. One tree measured twice is one name and
    two states, not the same name printed twice."""
    if len(datasets) == 1:
        return datasets[0]["label"]
    if not multiple_trees(datasets):
        states = [d["cache"] for d in datasets if d.get("cache")]
        if states:
            return "{0} · {1}".format(
                datasets[0].get("base_label", datasets[0]["label"]), " and ".join(states)
            )
    return "; ".join(d["label"] for d in datasets)


def series_label(dataset, datasets):
    """How a series is named in the key. With one tree the only thing that
    distinguishes the series is the pass, and repeating the tree's name in front
    of each of them says nothing."""
    if not multiple_trees(datasets) and dataset.get("cache"):
        return dataset["cache"]
    return dataset["label"]


def series_title(datasets):
    """What the shading means, in the reader's terms."""
    trees = multiple_trees(datasets)
    passes = len({d.get("cache") for d in datasets}) > 1
    if trees and passes:
        return "Dataset and cache state (lighter shade = later)"
    if trees:
        return "Dataset (lighter shade = later)"
    return "Cache state (lighter shade = later)"


def locate_csv(root, name):
    candidates = [
        root / name,
        root / "index" / name,
        root / "queries" / name,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def kv_file(path):
    values = {}
    if not path.is_file():
        return values
    with path.open() as stream:
        for line in stream:
            key, sep, value = line.rstrip("\n").partition("=")
            if sep:
                values[key] = value
    return values


def find_kv(root, name):
    for candidate in (root / name, root / "index" / name, root / "queries" / name):
        if candidate.is_file():
            return kv_file(candidate)
    return {}


def tree_file_count(index_csv):
    stats_path = index_csv.parent / "tree_stats.txt" if index_csv else Path()
    try:
        return int(kv_file(stats_path).get("file_count", "0"))
    except ValueError:
        return 0


# Below this the fixed cost of starting a process outweighs the walk, so the
# bars rank start-up time rather than the tools.
TIMEABLE_MIN_FILES = 100000


def too_small_caveat(count):
    if not count or count >= TIMEABLE_MIN_FILES:
        return ""
    return (
        "CORRECTNESS FIXTURE, NOT A BENCHMARK: {:,} files is small enough that "
        "process start-up dominates every bar.".format(count)
    )


def dataset_label(root, index_csv, explicit):
    if explicit:
        return explicit
    stats_path = index_csv.parent / "tree_stats.txt" if index_csv else Path()
    stats = kv_file(stats_path)
    tree = stats.get("tree", "")
    name = Path(tree).name if tree else root.name
    count = tree_file_count(index_csv)
    if count >= 1000000:
        return "{} ({:.1f}M files)".format(name, count / 1000000.0)
    if count:
        return "{} ({:,} files)".format(name, count)
    return name


def run_conditions(env, reps):
    """One line of the conditions a reader needs to judge the numbers."""
    bits = []
    host = env.get("hostname")
    os_name = env.get("os")
    if host:
        bits.append(host if not os_name else "{} ({})".format(host, os_name))
    if env.get("nproc"):
        bits.append("{} cpus".format(env["nproc"]))
    if env.get("threads"):
        bits.append("{} threads per tool".format(env["threads"]))
    if env.get("drop_caches") == "1":
        bits.append("cold caches")
    elif env.get("drop_caches") == "0":
        bits.append("warm caches")
    if reps:
        note = "{} repetition{}".format(reps, "" if reps == "1" else "s")
        # Per-tool counts: without them the conditions line promises a sample
        # size that some of the bars beside it were never given.
        detail = (env.get("reps_per_tool") or "").split()
        if detail:
            note += " ({})".format(", ".join(detail))
        bits.append(note)
    return "  \u00b7  ".join(bits)


def floats(rows, field):
    values = []
    for row in rows:
        if row.get("status") != "ok" or not row.get(field):
            continue
        try:
            values.append(float(row[field]))
        except ValueError:
            pass
    return values


def mean_std(values):
    if not values:
        return None, None
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.pstdev(values)


def combine_stats(parts):
    means = []
    variances = []
    for values in parts:
        mean, std = mean_std(values)
        if mean is None:
            return None, None
        means.append(mean)
        variances.append(std * std)
    return sum(means), math.sqrt(sum(variances))


def group_index(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault((row.get("tool"), row.get("variant")), []).append(row)
    return grouped


def phase_rows(grouped, keys):
    """Every repetition of a phase, pooling variants that ran the same command."""
    selected = []
    for key in keys:
        selected.extend(grouped.get(key, []))
    return selected


def row_file_count(rows):
    """How many files the run walked. The same for every row, since every tool
    was pointed at the same tree."""
    for row in rows:
        try:
            count = int(row.get("file_count") or 0)
        except ValueError:
            continue
        if count > 0:
            return count
    return 0


def rate_stats(count, mean, std):
    """Files per second, with the spread carried across from the seconds.

    The rate is one measured quantity inverted, so a 2% spread in seconds is a
    2% spread in files per second; recomputing it per repetition would say the
    same thing with more arithmetic.
    """
    if not count or not mean:
        return None, None
    rate = count / mean
    return rate, rate * (std / mean) if std else 0.0


def mib(rows):
    return [value / (1024.0 * 1024.0) for value in floats(rows, "index_bytes")]


def index_metrics(rows):
    """Per pipeline: elapsed time, the rate it works out to, what it left on
    disk, and every phase that was measured separately."""
    grouped = group_index(rows)
    count = row_file_count(rows)
    result = {}
    for label, phases, solo_label in PIPELINES:
        present = []
        for keys, phase_name in phases:
            selected = phase_rows(grouped, keys)
            times = floats(selected, "elapsed_sec")
            sizes = mib(selected)
            if times or sizes:
                present.append((phase_name, times, sizes))
        if not present:
            continue
        # A pipeline that only got through its first phase is that first phase,
        # and saying so is more honest than showing a partial total.
        if solo_label and len(present) < len(phases) and present[0][0] == phases[0][1]:
            label = solo_label
        if label in result:
            continue
        time = (
            combine_stats([p[1] for p in present])
            if all(p[1] for p in present)
            else mean_std(present[0][1])
        )
        size = (
            combine_stats([p[2] for p in present])
            if all(p[2] for p in present)
            else mean_std(present[0][2])
        )
        entry = {"time": time, "size": size, "rate": rate_stats(count, *time)}
        if size[0] and count:
            entry["per_file"] = size[0] * 1024.0 * 1024.0 / count
        if len(present) > 1:
            entry["components"] = [
                (name, mean_std(times)[0], mean_std(sizes)[0])
                for name, times, sizes in present
            ]
        result[label] = entry
    return result


def walk_floor(rows):
    """Fastest names-only walk: the floor no indexer can go below to see every name."""
    grouped = group_index(rows)
    best = None
    for (tool, variant), selected in grouped.items():
        label = WALK_LABELS.get((tool, variant))
        if label not in WALK_NAMES_LABELS:
            continue
        times = floats(selected, "elapsed_sec")
        if not times:
            continue
        mean = statistics.mean(times)
        if best is None or mean < best[1]:
            best = (label, mean)
    return best


def walk_metrics(rows):
    """Traversal rows that store nothing, keyed by chart label for both walk panels."""
    grouped = group_index(rows)
    count = row_file_count(rows)
    result = {}
    for (tool, variant), selected in grouped.items():
        if variant not in WALK_VARIANTS:
            continue
        label = WALK_LABELS.get((tool, variant)) or "{} {}".format(tool, variant)
        times = floats(selected, "elapsed_sec")
        if not times:
            continue
        time = mean_std(times)
        result[label] = {"time": time, "rate": rate_stats(count, *time)}
    return result


# Named after the binary that ran, so a panel says which half of the suite
# answered the query rather than crediting the pair for either one's work.
QUERY_TOOL_LABELS = {
    "ecrawl_suite": "ereport",
    "ereport_index": "ereport_index",
    "ereport": "ereport",
    "ecrawl_query": "ecrawl_query",
    # The index-free name search: ecrawl --no-stat --contains plus the exact
    # grep, fd's live-search peer. No capture, no trigram index.
    "ecrawl_walk": "ecrawl (live walk)",
    "robinhood": "Robinhood",
    "gufi": "GUFI",
    # The same wrappers reading the index gufi_rollup produced. A separate series
    # because it is a separate index, built at several times the cost, and it can
    # answer a question the plain one cannot.
    "gufi_rollup": "GUFI + rollup",
    "xdu": "XDU",
    "find": "find",
    "fd": "fd",
    "du": "du",
    "dua": "dua",
    "dut": "dut",
}


# Why a bar's answer differs from the reference when the difference is
# definitional. Keyed by the chart's own labels. These rows are drawn and timed
# like any other -- the tool answered the question its index can answer -- and
# the phrase goes on the bar so the hatching is never read as a defect.
# summarize.py carries the same table at length; README.md has the numbers.
ANSWER_SEMANTICS = {
    ("Robinhood", "Q4"): "disk usage, not apparent bytes",
    ("GUFI", "Q4"): "file bytes only, no directory inodes",
    ("GUFI + rollup", "Q4"): "file bytes only, no directory inodes",
    ("XDU", "Q4"): "file bytes only, no directory inodes",
}
for _query in ("Q1", "Q2", "Q3", "Q5", "Q6"):
    ANSWER_SEMANTICS[("Robinhood", _query)] = "counts inodes, not names"


def row_arg_set(row):
    """Which argument set a row answered. Blank in a CSV written before the sets
    existed, which is set 1 by definition."""
    return (row.get("arg_set") or "1").strip() or "1"


def query_metrics(rows):
    """Per (tool, query): timing, the answers returned, and why it is missing."""
    timings = {}
    counts = {}
    states = {}
    notes = {}
    for row in rows:
        label = QUERY_TOOL_LABELS.get(row.get("tool"))
        query = row.get("query")
        if not label or query not in QUERIES:
            continue
        status = row.get("status")
        if status != "ok":
            # A tool that is merely unsuited to a query is not a failure, and
            # the figure should not imply that it is.
            states.setdefault((label, query), status)
            notes.setdefault((label, query), row.get("notes") or "")
            continue
        states[(label, query)] = "ok"
        try:
            timings.setdefault((label, query), []).append(float(row["elapsed_sec"]))
        except (KeyError, ValueError):
            pass
        # One answer per argument set: each set asks for a different name and a
        # different threshold, so a single count per tool would be checked
        # against another set's reference and read as a disagreement.
        try:
            counts[(label, query, row_arg_set(row))] = int(row["result_count"])
        except (KeyError, ValueError, TypeError):
            pass

    stats = {key: mean_std(values) for key, values in timings.items()}
    return {"stats": stats, "counts": counts, "states": states, "notes": notes}


REFERENCE_PREFERENCE = {
    "Q1": ["find", "fd"],
    "Q2": ["find", "fd"],
    "Q3": ["find", "fd"],
    "Q4": ["du", "dua"],
    "Q5": ["find", "fd"],
    "Q6": ["find", "fd"],
}


def reference_counts(queries):
    """What the answer should be, per query and argument set, taken from the tool
    the paper treats as truth."""
    counts = queries["counts"]
    arg_sets = {key[2] for key in counts} or {"1"}
    reference = {}
    for query in QUERIES:
        for arg_set in arg_sets:
            for tool in REFERENCE_PREFERENCE[query]:
                if (tool, query, arg_set) in counts:
                    reference[(query, arg_set)] = (tool, counts[(tool, query, arg_set)])
                    break
    return reference


def wrong_answer(queries, reference, tool, query):
    """The first argument set this tool got wrong, as (answered, expected, ref).

    Checked per set because each one has its own right answer; one wrong set is
    enough to make the timing next to it meaningless.
    """
    counts = queries["counts"]
    for (label, q, arg_set), value in sorted(counts.items()):
        if label != tool or q != query:
            continue
        expected = reference.get((query, arg_set))
        if not expected or expected[0] == tool:
            continue
        if count_disagrees(query, value, expected[1]):
            return value, expected[1], expected[0]
    return None


def count_disagrees(query, value, expected):
    if expected in (None, 0) and value in (None, 0):
        return False
    if value is None or expected is None:
        return False
    if query == "Q4":
        return abs(value - expected) > max(1.0, abs(expected) * BYTES_TOLERANCE)
    return value != expected


def ordered_keys(order, datasets, key, extras=True):
    """Names from `order` that any dataset has data for.

    Unlisted names are appended so a tool added to the harness still charts
    instead of silently vanishing, unless `extras` is false (build time/rate
    must not pick up crawl-only write variants).
    """
    present = set()
    for dataset in datasets:
        present.update(dataset.get(key, {}).keys())
    listed = [name for name in order if name in present]
    if not extras:
        return listed
    return listed + sorted(present.difference(listed))


def ordered_present(order, datasets, section):
    present = set()
    for dataset in datasets:
        if section == "index":
            present.update(dataset["index"].keys())
        else:
            present.update(key[0] for key in dataset["queries"]["stats"].keys())
    return [name for name in order if name in present]


def fmt_tick_seconds(value):
    """Decade ticks: 10 ms, 1 s, 100 s -- no trailing precision to read past."""
    if value >= 1:
        return "{:g} s".format(value)
    return "{:g} ms".format(value * 1000)


def fmt_seconds(value):
    if value >= 100:
        return "{:.0f} s".format(value)
    if value >= 10:
        return "{:.1f} s".format(value)
    if value >= 1:
        return "{:.2f} s".format(value)
    if value >= 0.001:
        return "{:.0f} ms".format(value * 1000)
    return "{:.1f} ms".format(value * 1000)


def fmt_mib(value):
    if value >= 1024 * 1024:
        return "{:.1f} TiB".format(value / (1024 * 1024))
    if value >= 1024:
        return "{:.1f} GiB".format(value / 1024)
    if value >= 1:
        return "{:.0f} MiB".format(value)
    return "{:.2f} MiB".format(value)


def fmt_rate(value):
    """Files per second. Two significant figures is all these deserve: the run
    to run spread is percents, and a reader is comparing magnitudes."""
    if value >= 1e6:
        return "{:.1f}M files/s".format(value / 1e6)
    if value >= 1e5:
        return "{:.0f}k files/s".format(value / 1e3)
    if value >= 1e3:
        return "{:.1f}k files/s".format(value / 1e3)
    return "{:.0f} files/s".format(value)


def fmt_bytes(value):
    if value >= 1024 * 1024:
        return "{:.1f} MiB".format(value / (1024 * 1024))
    if value >= 1024:
        return "{:.1f} KiB".format(value / 1024)
    return "{:.0f} B".format(value)


# Cold is the tool colour. Later passes lighten toward white. 0.45 left a pale
# base (io_uring) looking like one bar; 0.62 is a clear dark/light pair.
CACHE_SHADES = (0.0, 0.62, 0.78, 0.88)


def cache_shades(n):
    shades = list(CACHE_SHADES[:n])
    while len(shades) < n:
        shades.append(CACHE_SHADES[-1])
    return shades


def shade(color, amount):
    """Lighten toward white. Keeps a tool recognisable across datasets while
    still separating the datasets, which a second hue would not."""
    if not amount:
        return color
    rgb = matplotlib.colors.to_rgb(color)
    return tuple(channel + (1.0 - channel) * amount for channel in rgb)


def style_axis(axis, xlabel=None, log=False):
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.spines["left"].set_visible(False)
    axis.tick_params(axis="y", length=0)
    axis.set_axisbelow(True)
    axis.xaxis.grid(True, which="major", **GRID)
    if log:
        axis.xaxis.grid(True, which="minor", color="#EAEAEA", linestyle=":", linewidth=0.5)
    if xlabel:
        axis.set_xlabel(xlabel)


def _label_transform(axis, dx_pt=4.0, dy_pt=0.0):
    """Data coords plus a point offset. invert_yaxis cannot desync x/y."""
    shift = ScaledTranslation(dx_pt / 72.0, dy_pt / 72.0, axis.figure.dpi_scale_trans)
    return axis.transData + shift


def bar_label(axis, y, x, text, log, color="#222222", weight="normal", sub=None):
    """Value at the end of the bar, optically centered on that bar.

    A note under the value is the same text object, so a three-line size
    label stays one block on this y instead of growing up and down from it
    and landing on the next bar.
    """
    offset = x * 1.12 if log else x
    if sub:
        text = "{}\n{}".format(text, sub)
    # ~0.3 em: enough to cancel descent, not enough to drop onto the next bar.
    trans = _label_transform(axis, 4.0, -2.5)
    axis.text(
        offset,
        y,
        text,
        transform=trans,
        va="center",
        ha="left",
        fontsize=8.5 if not sub else 8.0,
        color=color,
        fontweight=weight,
        linespacing=1.35,
        clip_on=False,
        zorder=6,
    )


def datasets_with_metric(datasets, key, metric):
    """Drop a cache series that has no bar on this figure.

    A warm-only walk row would otherwise open a third slot on the build
    charts, which have no warm values, and stretch every label off its bar.
    """
    return [
        dataset
        for dataset in datasets
        if any(
            dataset.get(key, {}).get(tool, {}).get(metric, (None, None))[0] is not None
            for tool in dataset.get(key, {})
        )
    ]


def fits_inside(fig, axis, value, text, fontsize=7.5):
    """Whether a phase split can sit inside its bar instead of trailing it."""
    left = axis.get_xlim()[0]
    x0, x1 = axis.transData.transform([(left, 0), (value, 0)])[:, 0]
    width_pt = (x1 - x0) * 72.0 / fig.dpi
    # DejaVu Sans averages a little over half the point size per character.
    return width_pt > 0.55 * fontsize * len(text) + 14


def _segment_bounds(low, right, values, log):
    """Boundary x-positions for a stacked bar's segments.

    On a linear axis the boundaries are just the cumulative values, which are
    already proportional to the shares. On a log axis that would compress the
    later phases (a 50/50 split reads as 80/20), so instead each boundary is
    placed where the segment's *on-screen* width is proportional to its value:
    cumulative fraction f lands at low * (right/low)**f. That splits the bar's
    rendered span [low, right] by value while the right edge stays at the
    truthful total.
    """
    if not log:
        bounds = [0.0]
        for v in values:
            bounds.append(bounds[-1] + v)
        return bounds
    total = sum(values)
    bounds = [low]
    acc = 0.0
    for v in values:
        acc += v
        frac = acc / total if total else 0.0
        bounds.append(low * (right / low) ** frac)
    return bounds


def _segment_value_labels(axis, y, parts, total, dataset_index, n_datasets,
                          bar_h, log, formatter):
    """One value label per phase, centred on its own segment of the bar.

    The labels sit just off the bar -- above the upper bar of a multi-cache
    row, below the lower one -- so they land in the gaps between rows rather
    than on a neighbouring bar. Each is centred on its segment's proportional
    span (geometric mean on a log axis), then nudged back on-canvas when a
    thin sliver phase would otherwise push it off the axis edge.
    """
    offset = bar_h * 0.43 + 0.05
    above = dataset_index <= (n_datasets - 1) / 2.0
    label_y = y - offset if above else y + offset
    low, high = axis.get_xlim()
    bounds = _segment_bounds(low, total, [v for _, v in parts], log)
    dpi = axis.figure.dpi

    def to_px(x):
        return axis.transData.transform([(x, 0.0)])[0][0]

    left_px, right_px = to_px(low), to_px(high)
    for part_no, (name, value) in enumerate(parts):
        a, b = bounds[part_no], bounds[part_no + 1]
        center = math.sqrt(a * b) if log and a > 0 else (a + b) / 2.0
        label = "{} {}".format(formatter(value), name)
        # A thin sliver phase would push its label off the axis edge; clamp the
        # centre so the text stays on the canvas beside its segment instead.
        half_px = (0.55 * 7.0 * len(label) + 6.0) * dpi / 72.0 / 2.0
        centre_px = min(max(to_px(center), left_px + half_px), right_px - half_px)
        center = axis.transData.inverted().transform([(centre_px, 0.0)])[0][0]
        axis.text(
            center,
            label_y,
            label,
            va="center",
            ha="center",
            fontsize=7,
            color="#444444",
            clip_on=False,
            zorder=6,
        )


def index_figure(datasets, out_dir, spec):
    """One figure, one horizontal ranking.

    Every index chart is the same picture with a different column of numbers
    behind it, so they share this renderer: bar geometry, whiskers, value
    labels, the sort, and the decision to go logarithmic all stay consistent
    from figure to figure, which is the only way the four can be read together.
    """
    key = spec["key"]
    metric = spec.get("metric", "time")
    formatter = spec.get("formatter", fmt_seconds)
    datasets = datasets_with_metric(datasets, key, metric)
    # Rates go the other way round: the best bar is the longest one, not the
    # shortest, and everything that assumes "less is better" has to be told.
    higher_better = spec.get("better") == "higher"

    tools = ordered_keys(
        spec["order"], datasets, key, extras=not spec.get("only_order")
    )
    # A tool with no value for *this* column would otherwise take a row label
    # and draw nothing beside it, which reads as a zero rather than a gap.
    tools = [
        tool
        for tool in tools
        if any(
            dataset[key].get(tool, {}).get(metric, (None, None))[0] is not None
            for dataset in datasets
        )
    ]
    if not tools:
        return [], None
    multi = len(datasets) > 1

    # Cold best at the top. Hot bars share that order so a cache win cannot
    # reshuffle the ranking.
    tools = sort_tools_best_first(tools, datasets, key, metric, higher_better)
    shades = cache_shades(len(datasets))

    # A phase split adds a second line of text per bar, which needs the room.
    split_rows = spec.get("split", True) and any(
        "components" in dataset[key].get(tool, {})
        for dataset in datasets
        for tool in tools
    )
    if spec.get("per_file"):
        split_rows = split_rows or any(
            dataset[key].get(tool, {}).get("per_file")
            for dataset in datasets
            for tool in tools
        )
    floor = datasets[0].get("walk_floor") if spec.get("floor") else None
    # Letter landscape: rankings are wide and short. Axes start at a
    # label-driven minimum and grow later to fill the printable page.
    page = LETTER_LANDSCAPE
    printable = page[1] - 2 * LETTER_MARGIN
    # Leave room for the title + subtitle + conditions above the panel title;
    # landscape used to park that block on top of the axes.
    header_in = 1.20 if any(d.get("conditions") for d in datasets) else 0.85
    if datasets[0].get("caveat"):
        header_in += 0.18
    row_in = INDEX_ROW_SPLIT_IN if split_rows else INDEX_ROW_IN
    if split_rows and spec.get("per_file") and multi:
        row_in = INDEX_ROW_SIZE_IN
    min_axes_in = row_in * len(tools) + (0.35 if floor else 0.12)
    # Tick labels and the x-axis title hang below the axes box; keep a dedicated
    # band so the legend cannot collide with them on landscape pages.
    xlabel_band_in = 0.75
    # Legend room at the bottom is estimated up front, before axes placement.
    caption_guess_in = 0.55 if multi else 0.0
    band_pad_in = 0.08
    fixed_in = header_in + xlabel_band_in + caption_guess_in
    axes_in = max(min_axes_in,
                  INDEX_PAGE_FILL * printable - fixed_in - band_pad_in)
    content_in = fixed_in + axes_in + band_pad_in
    y_bottom, y_top, height = _page_band(page, content_in)
    fig = plt.figure(figsize=page)
    axis = fig.add_axes(
        [0.16,
         y_bottom + (caption_guess_in + xlabel_band_in) / height,
         0.78,
         axes_in / height],
    )
    bar_h = 0.72 / len(datasets)
    positions = list(range(len(tools)))

    xlabel = spec["xlabel"]
    values = [
        dataset[key].get(tool, {}).get(metric, (None, None))[0]
        for dataset in datasets
        for tool in tools
    ]
    positive = [v for v in values if v and v > 0]
    # Every panel gets the same treatment. Build time used to be pinned to a
    # linear axis, which made a run containing GUFI's rollup unreadable: at
    # 495 s against 4.6 s the other three tools were slivers on the axis.
    log = bool(positive) and max(positive) / min(positive) >= LOG_RANGE
    # On a log axis the segments are drawn with proportional on-screen widths
    # (see _segment_bounds), anchored at this left edge; it is also the axis
    # minimum, so the first phase starts exactly where the axis does. The floor
    # line is the point of the panel it appears on, and on a log axis it usually
    # sits left of every bar, so it has to pull the limit out with it or it is
    # simply clipped away.
    seg_low = None
    if log and positive:
        seg_low = min(positive) / 4.0
        if floor:
            seg_low = min(seg_low, floor[1] / 1.8)
    part_index = 0 if metric == "time" else 1
    pending = []
    for dataset_index, dataset in enumerate(datasets):
        offset = (dataset_index - (len(datasets) - 1) / 2.0) * bar_h
        for row, tool in enumerate(tools):
            entry = dataset[key].get(tool, {})
            mean, std = entry.get(metric, (None, None))
            y = positions[row] + offset
            if mean is None:
                continue
            base = shade(COLORS.get(tool, "#4C72B0"), shades[dataset_index])
            # Rates do not add, so a rate figure never segments its bars: two
            # phases at 400k and 1.7M files/s do not make a 2.1M files/s
            # pipeline, and drawing them end to end would say they do.
            rename = spec.get("part_names", {})
            parts = [
                (rename.get(name, name), values[part_index])
                for name, *values in entry.get("components", [])
            ] if spec.get("split", True) else []
            parts = parts if all(v is not None for _, v in parts) else []
            # A log axis cannot be read additively, so the phases usually go
            # in the label there instead of pretending the segments sum by
            # length. A spec can still ask for the segments (the storage and
            # build figures do): the boundaries are then placed so each
            # segment's on-screen width is proportional to its phase's value
            # (see _segment_bounds), keeping the shares readable on the log
            # axis while the right edge stays at the truthful total.
            segmented = bool(parts) and (not log or spec.get("segment_log"))
            if segmented:
                bounds = _segment_bounds(seg_low, mean, [v for _, v in parts], log)
                for part_no, (_, value) in enumerate(parts):
                    axis.barh(
                        y,
                        bounds[part_no + 1] - bounds[part_no],
                        left=bounds[part_no],
                        height=bar_h * 0.86,
                        color=shade(base, min(0.78, 0.42 * part_no)),
                        edgecolor="white",
                        linewidth=0.8,
                        zorder=3,
                    )
            else:
                axis.barh(
                    y,
                    mean,
                    height=bar_h * 0.86,
                    color=base,
                    edgecolor="white",
                    linewidth=0.6,
                    zorder=3,
                )
            if std:
                axis.errorbar(
                    mean, y, xerr=std, fmt="none", ecolor="#333333",
                    elinewidth=1.0, capsize=2.5, zorder=4,
                )
            split = None
            if parts:
                split = " + ".join(
                    "{} {}".format(formatter(value), name) for name, value in parts
                )
            text = formatter(mean) if mean else "0"
            note = None
            if spec.get("per_file") and entry.get("per_file"):
                # On the total line, not a third line: cold/hot already share
                # one block, and a phase split under that is two lines, not
                # three stacked into the next bar.
                text = "{}  ·  {} per file".format(
                    text, fmt_bytes(entry["per_file"])
                )
            # Labels are placed once the axis limits are final, because whether
            # a split fits inside its bar depends on them.
            pending.append(
                (row, dataset_index, y, mean, std, text, split, note, segmented, parts)
            )

    axis.set_yticks(positions)
    axis.set_yticklabels(tools, fontsize=9)
    # The walk-only line is annotated above the first bar, so it needs a band of
    # its own up there.
    axis.set_ylim(-(1.0 if floor else 0.7), len(tools) - 0.3)
    axis.invert_yaxis()
    axis.set_title(spec["panel_title"], fontsize=10, fontweight="bold",
                   loc="left", pad=4)
    if log:
        axis.set_xscale("log")
        axis.xaxis.set_major_locator(LogLocator(base=10.0))
        # Decades of MiB do not land on binary units, so "9.8 GiB" as a tick is
        # noise; plain magnitudes read better and the bar labels already carry
        # the human-scale number.
        axis.xaxis.set_major_formatter(
            FuncFormatter(lambda v, _: "{:,.0f}".format(v) if v >= 1 else "{:g}".format(v))
        )
        xlabel += " (log scale)"
    style_axis(axis, xlabel, log)
    # Headroom for the value labels, which sit outside the bars. On a log axis
    # also drop the left edge below the smallest bar, or the smallest index
    # looks like it took no space at all.
    low, high = axis.get_xlim()
    if log and seg_low:
        low = seg_low
    if log:
        # Split / per-file notes are long; give them page width before clip.
        extra = 5.5 if spec.get("per_file") else (4.2 if split_rows else 2.6)
        axis.set_xlim(low, high * extra)
    else:
        axis.set_xlim(low, high * (1.55 if split_rows else 1.22))

    # Same note on every series of a row is one label on the tool row.
    # Index size is not a cache-state quantity: cold and hot differ by a
    # last-rep byte or two, which used to force a three-line label onto each
    # bar and stack them on top of each other. Always one label there, parked
    # past the longer bar. Other figures still split when the texts differ.
    # Per-segment labels need every bar's own phase list, so a row cannot
    # collapse to one shared label the way the size figure does.
    seg_labels = spec.get("segment_labels")
    placed = []
    if multi:
        by_row = {}
        for item in pending:
            by_row.setdefault(item[0], []).append(item)
        for row in sorted(by_row):
            items = by_row[row]
            keys = [
                (text, split, note, segmented)
                for _r, _i, _y, _m, _s, text, split, note, segmented, _p in items
            ]
            share = not seg_labels and (
                spec.get("per_file") or (len(items) > 1 and len(set(keys)) == 1)
            )
            if share:
                pick = max(items, key=lambda it: it[3] if it[3] is not None else -1)
                _r, di, _y, mean, std, text, split, note, segmented, parts = pick
                placed.append(
                    (positions[row], mean, std, text, split, note, segmented, parts, di)
                )
            else:
                for _r, di, y, mean, std, text, split, note, segmented, parts in items:
                    placed.append(
                        (y, mean, std, text, split, note, segmented, parts, di)
                    )
    else:
        for _r, di, y, mean, std, text, split, note, segmented, parts in pending:
            placed.append((y, mean, std, text, split, note, segmented, parts, di))

    for y, mean, std, text, split, note, segmented, parts, di in placed:
        end = mean + (std or 0)
        sub = note or None
        if segmented and seg_labels and parts:
            # Each phase value sits on its own segment, so the end of the bar
            # keeps just the total.
            _segment_value_labels(
                axis, y, parts, mean, di, len(datasets), bar_h, log, formatter
            )
        elif split:
            # A segmented bar's numbers go underneath the value, where they
            # cannot fight the segment boundary; the per-file note takes a
            # second line there rather than replacing them.
            if segmented or not log:
                sub = split if sub is None else "{}\n{}".format(split, sub)
            elif fits_inside(fig, axis, mean, split):
                # An unsegmented log bar with room takes the split inside in
                # white.
                inside = axis.get_yaxis_transform() + ScaledTranslation(
                    0, -2.5 / 72.0, axis.figure.dpi_scale_trans
                )
                axis.text(
                    0.008,
                    y,
                    split,
                    transform=inside,
                    va="center",
                    ha="left",
                    fontsize=7.5,
                    color="white",
                    zorder=5,
                    clip_on=False,
                )
            else:
                # Too short to hold the split inside: keep it on the value's
                # own line so the label stays centred on the thin bar instead
                # of a second line spilling off the bottom of it.
                text = "{}  ·  {}".format(text, split)
        bar_label(axis, y, end, text, log, sub=sub)

    if floor:
        name, value = floor
        axis.axvline(value, color="#B00020", linestyle="--", linewidth=1.1, zorder=2)
        # Above the first bar rather than beside it: the fastest indexer sits at
        # the bottom, which is exactly where this line is closest to a bar.
        axis.annotate(
            "fastest names-only walk ({}, {})".format(name, fmt_seconds(value)),
            xy=(value, -0.82),
            xytext=(4, 0),
            textcoords="offset points",
            fontsize=8,
            color="#B00020",
            va="center",
            ha="left",
        )

    subtitle = subtitle_for(datasets)
    conditions = datasets[0].get("conditions", "")
    caveat = datasets[0].get("caveat", "")
    # Legend title + swatches sit at the bottom; size this band from the legend
    # itself so its title cannot climb into the axis above.
    legend_in = 0.55 if multi else 0.0
    caption_in = legend_in
    # Grow the axes so short rankings (figures 1/2/4) use the same page fill as
    # denser ones (3/5).
    fixed_in = header_in + xlabel_band_in + caption_in
    axes_budget = printable - fixed_in - band_pad_in
    axes_in = max(min_axes_in, min(INDEX_PAGE_FILL * printable - fixed_in - band_pad_in,
                                   axes_budget))
    content_in = fixed_in + axes_in + band_pad_in
    y_bottom, y_top, height = _page_band(page, content_in)
    axis.set_position(
        [0.16,
         y_bottom + (caption_in + xlabel_band_in) / height,
         0.78,
         axes_in / height],
    )

    fig.text(0.05, y_top,
             "Figure {}: {}".format(spec["number"], spec["heading"]),
             fontsize=12, fontweight="bold", va="top")
    fig.text(
        0.05,
        y_top - 0.28 / height,
        subtitle + ("\n" + conditions if conditions else ""),
        fontsize=8,
        color="#555555",
        va="top",
    )
    if caveat:
        fig.text(
            0.05,
            y_top - (0.28 + (0.28 if conditions else 0.14)) / height,
            caveat,
            fontsize=8,
            color="#b2182b",
            fontweight="bold",
            va="top",
        )
    if multi:
        # Neutral swatches: the bars carry the tool's own hue, so a coloured key
        # would suggest a mapping that does not exist.
        proxies = [
            Patch(facecolor=shade("#555555", shades[i]), edgecolor="white",
                  label=series_label(dataset, datasets))
            for i, dataset in enumerate(datasets)
        ]
        fig.legend(
            handles=proxies,
            title=series_title(datasets),
            frameon=False,
            fontsize=8,
            loc="lower left",
            bbox_to_anchor=(0.05, y_bottom),
            ncol=len(proxies),
        )
    outputs = []
    for suffix in ("png", "pdf"):
        path = out_dir / ("{}.{}".format(spec["name"], suffix))
        fig.savefig(str(path), dpi=200 if suffix == "png" else None)
        outputs.append(path)
    # Caller closes after the combined PDF has been written.
    return outputs, fig


# Two questions, each asked twice: how long did it take, and how fast is that.
# The pair is deliberate -- elapsed seconds are what actually happened and what
# the summary table prints, while files per second is the number that means
# anything on a tree of a different size -- and every figure is the same
# picture with a different column of numbers behind it.
WALK_PANELS = [
    {
        "panel_title": "Regular-file count (names-only)",
        "order": WALK_NAMES_ORDER,
    },
    {
        "panel_title": "Apparent size (metadata + bytes)",
        "order": WALK_META_ORDER,
    },
]

INDEX_FIGURES = [
    {
        "name": "figure1_walk_time",
        "number": 1,
        "heading": "walking, elapsed time",
        "key": "walk",
        "panels": WALK_PANELS,
        "metric": "time",
        "formatter": fmt_seconds,
        "xlabel": "Elapsed seconds",
    },
    {
        "name": "figure2_walk_rate",
        "number": 2,
        "heading": "walking, throughput",
        "key": "walk",
        "panels": WALK_PANELS,
        "metric": "rate",
        "better": "higher",
        "formatter": fmt_rate,
        "xlabel": "Files per second",
    },
    {
        "name": "figure3_build_time",
        "number": 3,
        "heading": "building, elapsed time",
        "panel_title": "How long it takes to build a queryable index",
        "key": "index",
        "order": BUILD_ORDER,
        "only_order": True,
        "metric": "time",
        "formatter": fmt_seconds,
        "xlabel": "Elapsed seconds",
        "floor": True,
        # Build bars split into their crawl/index phases even on the log axis,
        # each phase value sitting on its own segment (Figure 5 does the same
        # for size). The reader tuning a pipeline needs the per-phase numbers,
        # not just the total.
        "segment_log": True,
        "segment_labels": True,
        # The dashed line explains itself where it is drawn.
    },
    {
        "name": "figure4_build_rate",
        "number": 4,
        "heading": "building, throughput",
        "panel_title": "Files per second, building the index",
        "key": "index",
        "order": BUILD_ORDER,
        "only_order": True,
        "metric": "rate",
        "better": "higher",
        "formatter": fmt_rate,
        "xlabel": "Files per second",
        # Rates do not add, so the phases stay off this one: the bar is the
        # whole pipeline's throughput, and Figure 3 is where its parts live.
        "split": False,
    },
    {
        "name": "figure5_index_size",
        "number": 5,
        "heading": "index storage",
        "panel_title": "What the index costs to keep",
        "key": "index",
        "order": INDEX_ORDER,
        "metric": "size",
        "formatter": fmt_mib,
        "xlabel": "MiB kept on disk",
        "per_file": True,
        # Storage is where the crawl/index split matters most, so the bars
        # segment even on the log axis this figure always ends up with.
        "segment_log": True,
        "part_names": {"crawl": "crawl metadata"},
    },
]


def _walk_panel_tools(datasets, key, order, metric):
    """Tools from this peer group's `order` only (do not pull in the other panel)."""
    present = set()
    for dataset in datasets:
        present.update(dataset.get(key, {}).keys())
    return [
        tool
        for tool in order
        if tool in present
        and any(
            dataset[key].get(tool, {}).get(metric, (None, None))[0] is not None
            for dataset in datasets
        )
    ]


def _sort_walk_tools(tools, datasets, key, metric, higher_better):
    return sort_tools_best_first(tools, datasets, key, metric, higher_better)


def _packed_ys(position, count, span=0.72):
    """Bar centers for one tool, packed around the y-tick with no empty slots."""
    slot = span / max(count, 1)
    return [position + (i - (count - 1) / 2.0) * slot for i in range(count)]


def _draw_walk_panel(axis, datasets, tools, key, metric, formatter, higher_better,
                     shades, xlabel, panel_title):
    """One peer-group ranking; independent x-scale from the sibling panel."""
    multi = len(datasets) > 1
    positions = list(range(len(tools)))
    values = [
        dataset[key].get(tool, {}).get(metric, (None, None))[0]
        for dataset in datasets
        for tool in tools
    ]
    positive = [v for v in values if v and v > 0]
    log = bool(positive) and max(positive) / min(positive) >= LOG_RANGE
    pending = []
    for row, tool in enumerate(tools):
        present = []
        for dataset_index, dataset in enumerate(datasets):
            mean, std = dataset[key].get(tool, {}).get(metric, (None, None))
            if mean is not None:
                present.append((dataset_index, mean, std))
        bar_h = 0.72 / max(len(present), 1)
        for y, (dataset_index, mean, std) in zip(
            _packed_ys(positions[row], len(present)), present
        ):
            base = shade(COLORS.get(tool, "#4C72B0"), shades[dataset_index])
            axis.barh(
                y,
                mean,
                height=bar_h * 0.86,
                color=base,
                edgecolor="white",
                linewidth=0.6,
                zorder=3,
            )
            if std:
                axis.errorbar(
                    mean, y, xerr=std, fmt="none", ecolor="#333333",
                    elinewidth=1.0, capsize=2.5, zorder=4,
                )
            pending.append((y, dataset_index, mean, std, formatter(mean) if mean else "0"))

    axis.set_yticks(positions)
    axis.set_yticklabels(tools, fontsize=9)
    axis.set_ylim(-0.7, len(tools) - 0.3)
    axis.invert_yaxis()
    axis.set_title(panel_title, fontsize=10, fontweight="bold", loc="left", pad=4)
    axis_xlabel = xlabel
    if log:
        axis.set_xscale("log")
        axis.xaxis.set_major_locator(LogLocator(base=10.0))
        axis.xaxis.set_major_formatter(
            FuncFormatter(lambda v, _: "{:,.0f}".format(v) if v >= 1 else "{:g}".format(v))
        )
        axis_xlabel += " (log scale)"
    style_axis(axis, axis_xlabel, log)
    low, high = axis.get_xlim()
    if log and positive:
        low = min(positive) / 4.0
        # Rate labels ("16.8M files/s") are wider than time labels ("1.13 s").
        # 2.6x left the longest rate sitting in the mid-gap on top of the
        # other panel's y-tick names.
        axis.set_xlim(low, high * (4.0 if higher_better else 2.6))
    else:
        axis.set_xlim(low, high * 1.22)
    for y, _dataset_index, mean, std, text in pending:
        end = mean + (std or 0)
        bar_label(axis, y, end, text, log)


def _walk_ytick_gutter(tools, page_width_in):
    """Figure fraction reserved left of a walk panel for its y-tick names.

    Each panel has its own names. Sizing only the page-left gutter (and leaving
    a 4% mid-gap) puts the right panel's "ecrawl --no-write --io_uring" on top
    of the left panel's bars.
    """
    longest = max(
        (max(len(line) for line in tool.splitlines()) for tool in tools),
        default=0,
    )
    tick_pt = 9.0
    label_in = longest * tick_pt * 0.62 / 72.0
    # One-line names (same as figures 3–5) are wider than the old two-line
    # split; 0.24 clipped "ecrawl --no-write --io_uring" into the left bars.
    return min(0.30, max(0.08, (label_in + 0.18) / page_width_in))


def _walk_panel_positions(left_margin, mid_gap, right_margin=0.04):
    panel_width = (1.0 - left_margin - right_margin - mid_gap) / 2.0
    return [
        left_margin,
        left_margin + panel_width + mid_gap,
        panel_width,
    ]


def _fit_walk_panel_gutters(fig, axes, min_left=0.08, min_mid=0.04):
    """Widen gutters from the rendered boxes so a name cannot cross into the
    sibling panel. Never shrink below the character-count layout: a short
    measurement is how the right-hand names ended up on the left-hand bars."""
    if not any(axis is not None for axis in axes):
        return
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    fig_w = fig.bbox.width
    pad = 0.012
    widths = []
    for axis in axes:
        if axis is None:
            widths.append(0.0)
            continue
        ax_left = axis.get_position().x0 * fig_w
        extra = 0.0
        for lab in axis.get_yticklabels():
            extra = max(
                extra,
                max(0.0, ax_left - lab.get_window_extent(renderer=renderer).x0),
            )
        widths.append(extra / fig_w + pad)
    left_margin = max(min_left, widths[0])
    mid_gap = max(min_mid, widths[1] if len(widths) > 1 else 0.04)
    if axes[0] is not None:
        ax_right = axes[0].get_position().x1 * fig_w
        overflow = 0.0
        for artist in axes[0].texts:
            overflow = max(
                overflow,
                max(0.0, artist.get_window_extent(renderer=renderer).x1 - ax_right),
            )
        if overflow:
            mid_gap += overflow / fig_w + pad
    usable = 1.0 - left_margin - 0.04 - mid_gap
    if usable < 0.36:
        return
    x0_left, x0_right, panel_width = _walk_panel_positions(left_margin, mid_gap)
    xs = (x0_left, x0_right)
    for i, axis in enumerate(axes):
        if axis is None:
            continue
        pos = axis.get_position()
        axis.set_position([xs[i], pos.y0, panel_width, pos.height])


def walk_figure(datasets, out_dir, spec):
    """Figures 1–2: two side-by-side peer groups (names-only vs metadata+size)."""
    key = spec["key"]
    metric = spec.get("metric", "time")
    formatter = spec.get("formatter", fmt_seconds)
    datasets = datasets_with_metric(datasets, key, metric)
    higher_better = spec.get("better") == "higher"
    panels = spec["panels"]
    panel_tools = []
    for panel in panels:
        tools = _walk_panel_tools(datasets, key, panel["order"], metric)
        tools = _sort_walk_tools(tools, datasets, key, metric, higher_better)
        panel_tools.append(tools)
    if not any(panel_tools):
        return [], None

    multi = len(datasets) > 1
    shades = cache_shades(len(datasets))

    page = LETTER_LANDSCAPE
    printable = page[1] - 2 * LETTER_MARGIN
    header_in = 1.20 if any(d.get("conditions") for d in datasets) else 0.85
    if datasets[0].get("caveat"):
        header_in += 0.18
    xlabel_band_in = 0.55
    caption_guess_in = 0.55 if multi else 0.0
    band_pad_in = 0.08
    max_rows = max((len(t) for t in panel_tools if t), default=1)
    min_axes_in = INDEX_ROW_IN * max_rows + 0.20
    fixed_in = header_in + xlabel_band_in + caption_guess_in
    axes_in = max(min_axes_in,
                  INDEX_PAGE_FILL * printable - fixed_in - band_pad_in)
    content_in = fixed_in + axes_in + band_pad_in
    y_bottom, y_top, height = _page_band(page, content_in)
    fig = plt.figure(figsize=page)

    # Each panel keeps its own y-tick gutter. The left margin is the names-only
    # names; the mid-gap is the metadata panel's names. A single page-left
    # gutter left the right-hand labels sitting on the left-hand bars.
    left_tools = panel_tools[0] if panel_tools else []
    right_tools = panel_tools[1] if len(panel_tools) > 1 else []
    left_margin = _walk_ytick_gutter(left_tools, page[0])
    mid_gap = _walk_ytick_gutter(right_tools, page[0]) if right_tools else 0.04
    x0_left, x0_right, panel_width = _walk_panel_positions(left_margin, mid_gap)
    panel_x0 = (x0_left, x0_right)
    axes_bottom = y_bottom + (caption_guess_in + xlabel_band_in) / height
    axes_height = axes_in / height
    axes = []
    for i, tools in enumerate(panel_tools):
        if not tools:
            axes.append(None)
            continue
        axis = fig.add_axes([panel_x0[i], axes_bottom, panel_width, axes_height])
        _draw_walk_panel(
            axis, datasets, tools, key, metric, formatter, higher_better,
            shades, spec["xlabel"], panels[i]["panel_title"],
        )
        axes.append(axis)

    subtitle = subtitle_for(datasets)
    conditions = datasets[0].get("conditions", "")
    caveat = datasets[0].get("caveat", "")
    legend_in = 0.55 if multi else 0.0
    caption_in = legend_in
    fixed_in = header_in + xlabel_band_in + caption_in
    axes_budget = printable - fixed_in - band_pad_in
    axes_in = max(min_axes_in, min(INDEX_PAGE_FILL * printable - fixed_in - band_pad_in,
                                   axes_budget))
    content_in = fixed_in + axes_in + band_pad_in
    y_bottom, y_top, height = _page_band(page, content_in)
    axes_bottom = y_bottom + (caption_in + xlabel_band_in) / height
    axes_height = axes_in / height
    for i, axis in enumerate(axes):
        if axis is None:
            continue
        axis.set_position([panel_x0[i], axes_bottom, panel_width, axes_height])
    _fit_walk_panel_gutters(fig, axes, min_left=left_margin, min_mid=mid_gap)

    fig.text(0.05, y_top,
             "Figure {}: {}".format(spec["number"], spec["heading"]),
             fontsize=12, fontweight="bold", va="top")
    fig.text(
        0.05,
        y_top - 0.28 / height,
        subtitle + ("\n" + conditions if conditions else ""),
        fontsize=8,
        color="#555555",
        va="top",
    )
    if caveat:
        fig.text(
            0.05,
            y_top - (0.28 + (0.28 if conditions else 0.14)) / height,
            caveat,
            fontsize=8,
            color="#b2182b",
            fontweight="bold",
            va="top",
        )
    if multi:
        proxies = [
            Patch(facecolor=shade("#555555", shades[i]), edgecolor="white",
                  label=series_label(dataset, datasets))
            for i, dataset in enumerate(datasets)
        ]
        fig.legend(
            handles=proxies,
            title=series_title(datasets),
            frameon=False,
            fontsize=8,
            loc="lower left",
            bbox_to_anchor=(0.05, y_bottom),
            ncol=len(proxies),
        )
    outputs = []
    for suffix in ("png", "pdf"):
        path = out_dir / ("{}.{}".format(spec["name"], suffix))
        fig.savefig(str(path), dpi=200 if suffix == "png" else None)
        outputs.append(path)
    return outputs, fig


def plot_index(datasets, out_dir):
    outputs = []
    figures = []
    for spec in INDEX_FIGURES:
        if spec.get("panels"):
            paths, fig = walk_figure(datasets, out_dir, spec)
        else:
            paths, fig = index_figure(datasets, out_dir, spec)
        if not paths or fig is None:
            continue
        outputs.extend(paths)
        figures.append(fig)
    return outputs, figures


def query_figure(dataset, queries, rows_per_query, bounds, page_no, pages):
    """One Letter portrait page: three query panels for a single cache state.

    Cold and hot each get their own page. Panels fill the sheet with enough
    gap that one panel's tick labels cannot land on the next panel's bars, and
    only the bottom panel draws an x-axis.
    """
    lo, hi = bounds
    page = LETTER_PORTRAIT
    conditions = dataset.get("conditions", "")
    caveat = dataset.get("caveat", "")
    header_in = 1.05 if conditions else 0.85
    if caveat:
        header_in += 0.20
    # Room below the bottom panel for its x-axis label.
    caption_in = 0.40
    panel_inches = [
        QUERY_BAR_IN * rows_per_query[q] + QUERY_PANEL_PAD_IN for q in queries
    ]
    gap_in = QUERY_PANEL_GAP_IN * max(0, len(queries) - 1)
    # Use the full printable height when the panels under-fill it, so three
    # rankings are spaced rather than glued into the top third of the page.
    printable = page[1] - 2 * LETTER_MARGIN - header_in - caption_in
    needed = sum(panel_inches) + gap_in
    if needed < printable and needed > 0:
        grow = (printable - needed) / len(panel_inches)
        panel_inches = [p + grow for p in panel_inches]
        needed = sum(panel_inches) + gap_in
    content_in = header_in + needed + caption_in
    y_bottom, y_top, height = _page_band(page, content_in)
    fig = plt.figure(figsize=page)
    avg_panel = sum(panel_inches) / len(panel_inches)
    gs = fig.add_gridspec(
        len(queries),
        1,
        height_ratios=panel_inches,
        left=0.16,
        right=0.96,
        top=y_top - header_in / height,
        bottom=y_bottom + caption_in / height,
        hspace=QUERY_PANEL_GAP_IN / avg_panel if avg_panel else 0.35,
    )
    axes = [[fig.add_subplot(gs[i, 0])] for i in range(len(queries))]
    if len(queries) > 1:
        for i in range(1, len(queries)):
            axes[i][0].sharex(axes[0][0])

    stats = dataset["queries"]["stats"]
    states = dataset["queries"]["states"]
    notes_by_key = dataset["queries"].get("notes", {})
    reference = reference_counts(dataset["queries"])

    for row, query in enumerate(queries):
        axis = axes[row][0]
        # Fastest on this page at the top. invert_yaxis puts y=0 at the top,
        # matching the walk and build figures. Each cache state ranks itself:
        # freezing hot to the cold order left a slower bar above a faster one.
        entries = sorted(
            (t for t in QUERY_ORDER if (t, query) in stats),
            key=lambda tool: stats[(tool, query)][0],
        )
        positions = list(range(len(entries)))
        # Reason -> the tools it applies to, for the bars that missed the
        # reference because they answer a different question.
        semantics = {}
        mismatch_notes = []
        for y, tool in zip(positions, entries):
            mean, std = stats[(tool, query)]
            mismatch = wrong_answer(dataset["queries"], reference, tool, query)
            wrong = mismatch is not None
            axis.barh(
                y,
                mean,
                height=0.62,
                color=COLORS.get(tool, "#4C72B0"),
                edgecolor="#B00020" if wrong else "white",
                linewidth=1.3 if wrong else 0.6,
                hatch="//" if wrong else None,
                alpha=0.55 if wrong else 1.0,
                zorder=3,
            )
            if std:
                axis.errorbar(
                    mean, y, xerr=min(std, mean * 0.95), fmt="none",
                    ecolor="#333333", elinewidth=1.0, capsize=2.5, zorder=4,
                )
            text = fmt_seconds(mean)
            if wrong:
                # Keep the bar label short; the full disagreement goes in the
                # panel note so a long "answered N, not M" cannot run into the
                # next bar or the panel above.
                answered, expected_value, ref_tool = mismatch
                text += "  \u2717"
                mismatch_notes.append(
                    "{} answered {:,}, not {:,} (per {})".format(
                        tool, answered, expected_value, ref_tool
                    )
                )
                why = ANSWER_SEMANTICS.get((tool, query))
                if why:
                    semantics.setdefault(why, []).append(tool)
            bar_label(
                axis,
                y,
                mean + (std or 0),
                text,
                True,
                color="#B00020" if wrong else "#222222",
                weight="bold" if wrong else "normal",
            )

        axis.set_yticks(positions)
        axis.set_yticklabels(entries, fontsize=9)
        axis.set_ylim(-0.75, max(1, len(entries)) - 0.25)
        axis.invert_yaxis()
        axis.set_xscale("log")
        axis.set_xlim(lo, hi)
        style_axis(axis, None, True)
        axis.xaxis.set_major_formatter(
            FuncFormatter(lambda v, _: fmt_tick_seconds(v))
        )
        # Only the bottom panel keeps tick labels and the axis name; drawing
        # them under every panel is what put "10 ms" on top of the bars above.
        if row < len(queries) - 1:
            axis.tick_params(axis="x", labelbottom=False, length=3)
        title = "{}  \u00b7  {}".format(query, QUERY_TITLES.get(query, ""))

        # Say why a tool has no bar. Unsupported and broken are different
        # findings, and a gap alone cannot tell them apart.
        missing = {"unsupported": [], "failed": [], "rollup": []}
        for tool in QUERY_ORDER:
            state = states.get((tool, query))
            if state in (None, "ok"):
                continue
            if state == "fail":
                missing["failed"].append(tool)
            elif "rollup_required" in (notes_by_key.get((tool, query)) or ""):
                # Not "no equivalent query": the tool has the query, and the
                # index this series was built by cannot serve it. Reading that
                # as a missing feature is exactly how the cheap GUFI index
                # came to look like it answered the aggregate.
                missing["rollup"].append(tool)
            else:
                missing["unsupported"].append(tool)
        notes = list(mismatch_notes)
        for why, who in semantics.items():
            notes.append("{}: {}".format(", ".join(who), why))
        if missing["rollup"]:
            notes.append(
                "needs the rolled-up index: " + ", ".join(missing["rollup"])
            )
        if missing["unsupported"]:
            notes.append("no equivalent query: " + ", ".join(missing["unsupported"]))
        if missing["failed"]:
            notes.append("failed: " + ", ".join(missing["failed"]))
        # Notes sit under the title, not in the plot: top-right annotations
        # collided with short bars' value labels and with long bars that reach
        # the right edge.
        axis.set_title(
            title,
            fontsize=10,
            fontweight="bold",
            loc="left",
            pad=14 if notes else 6,
        )
        if notes:
            axis.text(
                0.0,
                1.01,
                "  \u00b7  ".join(notes),
                transform=axis.transAxes,
                fontsize=7.5,
                color="#666666",
                va="bottom",
                ha="left",
                clip_on=False,
            )

    axes[-1][0].set_xlabel("Elapsed time (log scale)", fontsize=9, labelpad=6)

    cache = dataset.get("cache") or ""
    # Tree name alone: the cache state is already in the page heading, and
    # repeating "· cold" under a "cold" title says nothing.
    subtitle = dataset.get("base_label") or dataset["label"]
    if cache and subtitle.endswith(" · " + cache):
        subtitle = subtitle[: -(len(cache) + 3)]
    query_span = "\u2013".join(
        [queries[0], queries[-1]] if len(queries) > 1 else [queries[0]]
    )
    heading = "Figure 6: query performance"
    if pages > 1:
        state = cache if cache else "pass"
        heading += "  ({} of {}, {} \u00b7 {})".format(
            page_no, pages, state, query_span
        )
    elif cache:
        heading += "  ({})".format(cache)
    fig.text(0.05, y_top, heading,
             fontsize=12, fontweight="bold", va="top")
    fig.text(
        0.05,
        y_top - 0.28 / height,
        subtitle + ("\n" + conditions if conditions else ""),
        fontsize=8,
        color="#555555",
        va="top",
    )
    if caveat:
        fig.text(
            0.05,
            y_top - (0.28 + (0.28 if conditions else 0.14)) / height,
            caveat,
            fontsize=8,
            color="#b2182b",
            fontweight="bold",
            va="top",
        )

    return fig


# Three query panels to a Letter page. Cold and hot each get their own page,
# so six queries and two cache states become four printable sheets.
QUERIES_PER_PAGE = 3


def plot_queries(datasets, out_dir):
    usable = [d for d in datasets if d["queries"]["stats"]]
    if not usable:
        return [], []

    # One panel per query, sized by how many tools answered it on that pass.
    rows_per_query = {}
    for query in QUERIES:
        depth = max(
            len([t for t in QUERY_ORDER if (t, query) in d["queries"]["stats"]])
            for d in usable
        )
        if depth:
            rows_per_query[query] = depth
    if not rows_per_query:
        return [], []
    queries = [q for q in QUERIES if q in rows_per_query]

    # One x-axis for every page, or a query on page two would look faster than
    # one on page one for no reason but its own axis.
    everything = [
        m for d in usable for (m, _s) in d["queries"]["stats"].values() if m
    ]
    bounds = (min(everything) / 3.0, max(everything) * 3.0)

    query_chunks = [
        queries[start:start + QUERIES_PER_PAGE]
        for start in range(0, len(queries), QUERIES_PER_PAGE)
    ]
    # Chunk first, then cache state: Q1-Q3 cold, Q1-Q3 hot, Q4-Q6 cold,
    # Q4-Q6 hot. Consecutive pages then compare the same questions cold vs hot.
    pages = [
        (dataset, chunk)
        for chunk in query_chunks
        for dataset in usable
    ]
    figures = [
        query_figure(
            dataset,
            chunk,
            {
                q: len([t for t in QUERY_ORDER if (t, q) in dataset["queries"]["stats"]])
                or rows_per_query[q]
                for q in chunk
            },
            bounds,
            number,
            len(pages),
        )
        for number, (dataset, chunk) in enumerate(pages, start=1)
    ]

    # Drop stale page PNGs from an earlier layout (e.g. the old two-column pair)
    # so a four-page render cannot leave a fifth orphan behind.
    for stale in out_dir.glob("figure6_queries*.png"):
        stale.unlink()

    outputs = []
    for number, fig in enumerate(figures, start=1):
        stem = "figure6_queries" if len(figures) == 1 else "figure6_queries_p{}".format(number)
        path = out_dir / (stem + ".png")
        fig.savefig(str(path), dpi=200)
        outputs.append(path)
    # One PDF, one Letter page per group — what a printer and a paper both want.
    pdf_path = out_dir / "figure6_queries.pdf"
    with PdfPages(str(pdf_path)) as pdf:
        for fig in figures:
            pdf.savefig(fig)
    outputs.append(pdf_path)
    # Caller closes after the combined PDF has been written.
    return outputs, figures


# ---------------------------------------------------------------------------
# Figure 7: time to answer = crawl/scan + index build + the query itself.

# What the first answer costs beyond the query, per query-tool label: the
# index-side phases as (group_index keys, phase name), in spend order. The
# walkers map to no phases -- their query row already is the whole walk.
# dir2index runs twice per repetition (once for the plain index, once for the
# rollup's input), so each GUFI series is billed for its own run, not the pool.
ANSWER_BUILD_PHASES = {
    "ereport_index": [
        ((("ecrawl", "write"),), "crawl"),
        ((("ereport_index", "make"),), "index"),
    ],
    "Robinhood": [
        ((("robinhood", "scan"),), "crawl"),
        ((("robinhood", "indexes"),), "index"),
    ],
    "GUFI": [((("gufi", "plain"),), "index")],
    "GUFI + rollup": [
        ((("gufi", "rollup_index"),), "index"),
        ((("gufi", "rollup_step"),), "rollup"),
    ],
    "XDU": [((("xdu", "index"),), "index")],
}


def answer_build_phases(label, query):
    """Index-side phases a query tool pays before its first answer, in order."""
    if label in ("ecrawl_query", "ereport"):
        # Q3 reads only the capture; Q4 and Q5 also open the dir-index
        # sidecars that the same ereport_index --make build wrote.
        phases = [((("ecrawl", "write"),), "crawl")]
        if query in ("Q4", "Q5"):
            phases.append(((("ereport_index", "make"),), "index"))
        return phases
    return ANSWER_BUILD_PHASES.get(label, [])


def answer_bar(dataset, grouped, label, query):
    """One time-to-answer bar: (segments, total), segments as (phase, mean,
    std) in spend order ending with the query itself. None when an index-side
    phase has no successful rows -- without it the tool could not have
    produced this answer, so a query-only bar would be a lie."""
    stat = dataset["queries"]["stats"].get((label, query))
    if not stat or not stat[0]:
        return None
    segments = []
    for keys, phase in answer_build_phases(label, query):
        mean, std = mean_std(floats(phase_rows(grouped, keys), "elapsed_sec"))
        if mean is None:
            return None
        segments.append((phase, mean, std or 0.0))
    segments.append(("query", stat[0], stat[1] or 0.0))
    total = (
        sum(seg[1] for seg in segments),
        math.sqrt(sum(seg[2] * seg[2] for seg in segments)),
    )
    return segments, total


def answer_bars(dataset):
    """Every drawable (tool, query) bar for one cache-state dataset."""
    grouped = group_index(dataset.get("index_rows", []))
    bars = {}
    for query in QUERIES:
        for tool in QUERY_ORDER:
            bar = answer_bar(dataset, grouped, tool, query)
            if bar is not None:
                bars[(tool, query)] = bar
    return bars


def answer_figure(dataset, queries, rows_per_query, bounds, page_no, pages):
    """One Letter portrait page: three query panels of stacked time-to-answer
    bars for a single cache state, laid out like Figure 6 so the two read
    side by side."""
    lo, hi = bounds
    page = LETTER_PORTRAIT
    conditions = dataset.get("conditions", "")
    caveat = dataset.get("caveat", "")
    header_in = 1.05 if conditions else 0.85
    if caveat:
        header_in += 0.20
    # Room below the bottom panel for its x-axis label.
    caption_in = 0.40
    panel_inches = [
        QUERY_BAR_IN * rows_per_query[q] + QUERY_PANEL_PAD_IN for q in queries
    ]
    gap_in = QUERY_PANEL_GAP_IN * max(0, len(queries) - 1)
    printable = page[1] - 2 * LETTER_MARGIN - header_in - caption_in
    needed = sum(panel_inches) + gap_in
    if needed < printable and needed > 0:
        grow = (printable - needed) / len(panel_inches)
        panel_inches = [p + grow for p in panel_inches]
        needed = sum(panel_inches) + gap_in
    content_in = header_in + needed + caption_in
    y_bottom, y_top, height = _page_band(page, content_in)
    fig = plt.figure(figsize=page)
    avg_panel = sum(panel_inches) / len(panel_inches)
    gs = fig.add_gridspec(
        len(queries),
        1,
        height_ratios=panel_inches,
        left=0.16,
        right=0.96,
        top=y_top - header_in / height,
        bottom=y_bottom + caption_in / height,
        hspace=QUERY_PANEL_GAP_IN / avg_panel if avg_panel else 0.35,
    )
    axes = [[fig.add_subplot(gs[i, 0])] for i in range(len(queries))]
    if len(queries) > 1:
        for i in range(1, len(queries)):
            axes[i][0].sharex(axes[0][0])

    bars = dataset["answers"]
    states = dataset["queries"]["states"]
    notes_by_key = dataset["queries"].get("notes", {})
    reference = reference_counts(dataset["queries"])

    for row, query in enumerate(queries):
        axis = axes[row][0]
        # Fastest answer on this page at the top, same as Figure 6.
        entries = sorted(
            (t for t in QUERY_ORDER if (t, query) in bars),
            key=lambda tool: bars[(tool, query)][1][0],
        )
        positions = list(range(len(entries)))
        semantics = {}
        mismatch_notes = []
        for y, tool in zip(positions, entries):
            segments, (mean, std) = bars[(tool, query)]
            mismatch = wrong_answer(dataset["queries"], reference, tool, query)
            wrong = mismatch is not None
            base = COLORS.get(tool, "#4C72B0")
            left = 0.0
            for part_no, (_phase, value, _std) in enumerate(segments):
                axis.barh(
                    y,
                    value,
                    left=left,
                    height=0.62,
                    color=shade(base, min(0.78, 0.42 * part_no)),
                    edgecolor="#B00020" if wrong else "white",
                    linewidth=1.3 if wrong else 0.6,
                    hatch="//" if wrong else None,
                    alpha=0.55 if wrong else 1.0,
                    zorder=3,
                )
                left += value
            if std:
                axis.errorbar(
                    mean, y, xerr=min(std, mean * 0.95), fmt="none",
                    ecolor="#333333", elinewidth=1.0, capsize=2.5, zorder=4,
                )
            text = fmt_seconds(mean)
            if wrong:
                answered, expected_value, ref_tool = mismatch
                text += "  ✗"
                mismatch_notes.append(
                    "{} answered {:,}, not {:,} (per {})".format(
                        tool, answered, expected_value, ref_tool
                    )
                )
                why = ANSWER_SEMANTICS.get((tool, query))
                if why:
                    semantics.setdefault(why, []).append(tool)
            bar_label(
                axis,
                y,
                mean + (std or 0),
                text,
                True,
                color="#B00020" if wrong else "#222222",
                weight="bold" if wrong else "normal",
            )

        axis.set_yticks(positions)
        axis.set_yticklabels(entries, fontsize=9)
        axis.set_ylim(-0.75, max(1, len(entries)) - 0.25)
        axis.invert_yaxis()
        axis.set_xscale("log")
        axis.set_xlim(lo, hi)
        style_axis(axis, None, True)
        axis.xaxis.set_major_formatter(
            FuncFormatter(lambda v, _: fmt_tick_seconds(v))
        )
        if row < len(queries) - 1:
            axis.tick_params(axis="x", labelbottom=False, length=3)

        title = "{}  ·  {}".format(query, QUERY_TITLES.get(query, ""))

        # Say why a tool has no bar, split the same way Figure 6 splits it,
        # plus the one cause Figure 6 cannot have: a query that ran against
        # an index whose build never succeeded here.
        missing = {"unsupported": [], "failed": [], "rollup": [], "build": []}
        for tool in QUERY_ORDER:
            state = states.get((tool, query))
            if state == "fail":
                missing["failed"].append(tool)
            elif state not in (None, "ok"):
                if "rollup_required" in (notes_by_key.get((tool, query)) or ""):
                    missing["rollup"].append(tool)
                else:
                    missing["unsupported"].append(tool)
            elif (tool, query) not in bars:
                missing["build"].append(tool)
        notes = list(mismatch_notes)
        for why, who in semantics.items():
            notes.append("{}: {}".format(", ".join(who), why))
        if missing["rollup"]:
            notes.append(
                "needs the rolled-up index: " + ", ".join(missing["rollup"])
            )
        if missing["unsupported"]:
            notes.append("no equivalent query: " + ", ".join(missing["unsupported"]))
        if missing["failed"]:
            notes.append("failed: " + ", ".join(missing["failed"]))
        if missing["build"]:
            notes.append("no successful build row: " + ", ".join(missing["build"]))
        axis.set_title(
            title,
            fontsize=10,
            fontweight="bold",
            loc="left",
            pad=14 if notes else 6,
        )
        if notes:
            axis.text(
                0.0,
                1.01,
                "  ·  ".join(notes),
                transform=axis.transAxes,
                fontsize=7.5,
                color="#666666",
                va="bottom",
                ha="left",
                clip_on=False,
            )

    axes[-1][0].set_xlabel("Elapsed time (log scale)", fontsize=9, labelpad=6)

    cache = dataset.get("cache") or ""
    subtitle = dataset.get("base_label") or dataset["label"]
    if cache and subtitle.endswith(" · " + cache):
        subtitle = subtitle[: -(len(cache) + 3)]
    query_span = "–".join(
        [queries[0], queries[-1]] if len(queries) > 1 else [queries[0]]
    )
    heading = "Figure 7: time to answer (crawl + index + query)"
    if pages > 1:
        state = cache if cache else "pass"
        heading += "  ({} of {}, {} · {})".format(page_no, pages, state, query_span)
    elif cache:
        heading += "  ({})".format(cache)
    fig.text(0.05, y_top, heading, fontsize=12, fontweight="bold", va="top")
    fig.text(
        0.05,
        y_top - 0.28 / height,
        subtitle + ("\n" + conditions if conditions else ""),
        fontsize=8,
        color="#555555",
        va="top",
    )
    if caveat:
        fig.text(
            0.05,
            y_top - (0.28 + (0.28 if conditions else 0.14)) / height,
            caveat,
            fontsize=8,
            color="#b2182b",
            fontweight="bold",
            va="top",
        )

    return fig


def plot_time_to_answer(datasets, out_dir):
    for dataset in datasets:
        dataset["answers"] = answer_bars(dataset)
    usable = [d for d in datasets if d["answers"]]
    if not usable:
        return [], []

    rows_per_query = {}
    for query in QUERIES:
        depth = max(
            len([t for t in QUERY_ORDER if (t, query) in d["answers"]])
            for d in usable
        )
        if depth:
            rows_per_query[query] = depth
    if not rows_per_query:
        return [], []
    queries = [q for q in QUERIES if q in rows_per_query]

    # One x-axis for every page, and shared with nothing else: totals live a
    # decade above Figure 6's query-only bars.
    everything = [
        total[0]
        for d in usable
        for _segments, total in d["answers"].values()
        if total[0]
    ]
    bounds = (min(everything) / 3.0, max(everything) * 3.0)

    query_chunks = [
        queries[start:start + QUERIES_PER_PAGE]
        for start in range(0, len(queries), QUERIES_PER_PAGE)
    ]
    pages = [
        (dataset, chunk)
        for chunk in query_chunks
        for dataset in usable
    ]
    figures = [
        answer_figure(
            dataset,
            chunk,
            {
                q: len([t for t in QUERY_ORDER if (t, q) in dataset["answers"]])
                or rows_per_query[q]
                for q in chunk
            },
            bounds,
            number,
            len(pages),
        )
        for number, (dataset, chunk) in enumerate(pages, start=1)
    ]

    for stale in out_dir.glob("figure7_time_to_answer*.png"):
        stale.unlink()

    outputs = []
    for number, fig in enumerate(figures, start=1):
        stem = "figure7_time_to_answer" if len(figures) == 1 else "figure7_time_to_answer_p{}".format(number)
        path = out_dir / (stem + ".png")
        fig.savefig(str(path), dpi=200)
        outputs.append(path)
    pdf_path = out_dir / "figure7_time_to_answer.pdf"
    with PdfPages(str(pdf_path)) as pdf:
        for fig in figures:
            pdf.savefig(fig)
    outputs.append(pdf_path)
    return outputs, figures


# ---- "What didn't go well" summary page ----------------------------------
#
# Every figure before this one ranks the tools that worked. This page lists
# the ones that did not: queries a tool answered with the wrong number (it ran
# cleanly, the answer just is not the reference's) and queries that failed
# outright, each next to the exact command that produced it so the row can be
# reproduced. Skips are counted, not itemised: a tool with no primitive for a
# question not running it is the design, not a defect.

CMD_FS = 8.0
# DejaVu Sans Mono advances ~0.602 em per glyph; close enough to wrap the
# highlighted command without a renderer round-trip.
CMD_CHAR_EM = 0.602
CMD_BINARY = "#0B3D91"
CMD_FLAG = "#7B2CBF"
CMD_PATH = "#1B7A43"
CMD_OP = "#8C8C8C"
CMD_TEXT = "#222222"


def _cmd_token_style(token, index):
    """Colour and weight for one shell token: the binary, then operators,
    flags, paths and plain arguments."""
    if index == 0:
        return CMD_BINARY, "bold"
    if token in ("|", "||", "&&", ";") or ">" in token:
        return CMD_OP, "normal"
    if token.startswith("-"):
        return CMD_FLAG, "normal"
    if "/" in token:
        return CMD_PATH, "normal"
    return CMD_TEXT, "normal"


def _load_commands(source):
    """label -> exact argv, from the queries/ and index/ COMMANDS.txt logs."""
    commands = {}
    for sub in ("queries", "index"):
        path = Path(source) / sub / "COMMANDS.txt"
        if not path.is_file():
            continue
        try:
            lines = path.read_text().splitlines()
        except OSError:
            continue
        for line in lines:
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t", 2)
            if len(parts) < 3:
                continue
            commands.setdefault(parts[1].strip(), parts[2].strip())
    return commands


def _query_command(commands, csv_tool, query, arg_set, cache):
    """The exact argv a (tool, query, argument set) ran, if it was logged.
    Extra argument sets only run in the hot pass, so their label carries the
    set number; set 1 runs cold and hot."""
    labels = []
    if arg_set and arg_set != "1":
        labels.append("{}_{}_r1_hot_a{}".format(csv_tool, query, arg_set))
    labels.append(
        "{}_{}_r1{}".format(csv_tool, query, "_hot" if cache == "hot" else "")
    )
    labels.append("{}_{}_r1".format(csv_tool, query))
    for label in labels:
        if label in commands:
            return commands[label]
    prefix = "{}_{}_".format(csv_tool, query)
    for label, command in commands.items():
        if label.startswith(prefix):
            return command
    return None


def _build_command(commands, tool, variant):
    for label in (
        "{}_{}_r1".format(tool, variant),
        "{}_{}_r1_hot".format(tool, variant),
    ):
        if label in commands:
            return commands[label]
    prefix = "{}_{}_".format(tool, variant)
    for label, command in commands.items():
        if label.startswith(prefix):
            return command
    return None


def collect_failures(datasets):
    """Wrong answers, run failures and build failures, grouped for the page.

    A wrong answer is an ok row whose count disagrees with the reference tool
    (the same check that hatches a bar); a failure is a row that errored. Both
    are grouped by (tool, query) so several argument sets of one cause read as
    one item, and each carries the command that produced it.
    """
    display_to_csv = {label: tool for tool, label in QUERY_TOOL_LABELS.items()}
    wrong = {}
    failed = {}
    builds = {}
    skipped = set()
    for dataset in datasets:
        queries = dataset["queries"]
        reference = reference_counts(queries)
        cache = dataset.get("cache") or ""
        commands = _load_commands(dataset["source"])
        for (label, query, arg_set), value in sorted(queries["counts"].items()):
            expected = reference.get((query, arg_set))
            if not expected or expected[0] == label:
                continue
            if not count_disagrees(query, value, expected[1]):
                continue
            entry = wrong.setdefault((label, query), {
                "tool": label, "query": query, "arg_sets": [],
                "answered": value, "expected": expected[1], "ref": expected[0],
                "command": None,
            })
            if arg_set not in entry["arg_sets"]:
                entry["arg_sets"].append(arg_set)
            if entry["command"] is None:
                entry["command"] = _query_command(
                    commands, display_to_csv.get(label, label), query, arg_set,
                    cache)
        for (label, query), status in sorted(queries["states"].items()):
            if status == "fail":
                entry = failed.setdefault((label, query), {
                    "tool": label, "query": query,
                    "note": (queries["notes"].get((label, query)) or "error"),
                    "command": None,
                })
                if entry["command"] is None:
                    entry["command"] = _query_command(
                        commands, display_to_csv.get(label, label), query, "1",
                        cache)
            elif status == "skipped":
                skipped.add((label, query))
        for row in dataset.get("index_rows", []):
            status = (row.get("status") or "").strip()
            if not status or status == "ok":
                continue
            key = (row.get("tool", ""), row.get("variant", ""))
            entry = builds.setdefault(key, {
                "tool": key[0], "variant": key[1], "status": status,
                "note": (row.get("notes") or "").strip(), "command": None,
            })
            if entry["command"] is None:
                entry["command"] = _build_command(commands, key[0], key[1])
    order = {tool: index for index, tool in enumerate(QUERY_ORDER)}

    def by_deck(entry):
        return (order.get(entry["tool"], len(order)), entry.get("query", ""))

    return {
        "wrong": sorted(wrong.values(), key=by_deck),
        "failed": sorted(failed.values(), key=by_deck),
        "builds": sorted(
            builds.values(), key=lambda entry: (entry["tool"], entry["variant"])
        ),
        "skipped": len(skipped),
    }


class _SummaryPage:
    """Top-down text cursor over one or more Letter pages. Content taller than
    one page flows onto the next, so a long failure list never clips."""

    LINE = 0.020

    def __init__(self):
        self.page_w, self.page_h = LETTER_PORTRAIT
        self.left = LETTER_MARGIN / self.page_w
        self.right = 1.0 - self.left
        self.top = 1.0 - LETTER_MARGIN / self.page_h
        self.bottom = LETTER_MARGIN / self.page_h
        self.char_w = (CMD_FS * CMD_CHAR_EM / 72.0) / self.page_w
        self.figures = []
        self._new_page()

    def _new_page(self):
        self.fig = plt.figure(figsize=LETTER_PORTRAIT)
        self.figures.append(self.fig)
        self.y = self.top

    def ensure(self, height):
        if self.y - height < self.bottom:
            self._new_page()

    def gap(self, height):
        self.y -= height

    def text(self, text, indent=0.0, fs=9.0, color="#222222", weight="normal",
             style="normal", height=LINE, family=None):
        self.ensure(height)
        self.fig.text(self.left + indent, self.y, text, fontsize=fs, color=color,
                      fontweight=weight, fontstyle=style, va="top", ha="left",
                      family=family or "sans-serif")
        self.y -= height

    def paragraph(self, text, width_chars=104, **kwargs):
        for line in textwrap.wrap(text, width_chars) or [""]:
            self.text(line, **kwargs)

    def command(self, command):
        """One shell command, syntax-highlighted and wrapped to the margin."""
        x0 = self.left + 0.035
        tokens = command.split()
        lines = 1
        x = x0
        for token in tokens:
            width = (len(token) + 1) * self.char_w
            if x + width > self.right and x > x0:
                lines += 1
                x = x0
            x += width
        self.ensure(lines * self.LINE)
        x = x0
        for index, token in enumerate(tokens):
            color, weight = _cmd_token_style(token, index)
            width = (len(token) + 1) * self.char_w
            if x + width > self.right and x > x0:
                x = x0
                self.y -= self.LINE
            self.fig.text(x, self.y, token, family="monospace", fontsize=CMD_FS,
                          color=color, fontweight=weight, va="top", ha="left")
            x += width
        self.y -= self.LINE


def _failures_page(summary):
    """Render the collected failures onto one or more pages."""
    pager = _SummaryPage()
    pager.text("What didn't go well", fs=13, weight="bold", height=0.030)
    n_wrong = len(summary["wrong"])
    n_failed = len(summary["failed"])
    n_build = len(summary["builds"])
    intro = (
        "{} wrong answer{}, {} failed quer{}, {} build/index failure{}. Each "
        "is shown with the exact command that produced it, so the row can be "
        "reproduced. Separately, {} tool/query cells were skipped by design "
        "(the tool has no primitive for the question); those are expected "
        "empty cells, not problems, and are not listed."
    ).format(
        n_wrong, "" if n_wrong == 1 else "s",
        n_failed, "y" if n_failed == 1 else "ies",
        n_build, "" if n_build == 1 else "s",
        summary["skipped"],
    )
    pager.paragraph(intro, fs=9, color="#555555", height=0.018)
    pager.gap(0.010)

    if not (n_wrong or n_failed or n_build):
        pager.text(
            "Everything that ran produced the reference answer.",
            fs=10, color="#1B7A43", weight="bold", height=0.024,
        )
        return pager.figures

    if summary["wrong"]:
        pager.text("Wrong answer", fs=11, weight="bold", color="#B00020",
                   height=0.026)
        pager.paragraph(
            "The tool ran cleanly but the number is not the reference's. The "
            "timing next to it in the figures answers a different question.",
            fs=8.5, color="#555555", height=0.017)
        pager.gap(0.006)
        for entry in summary["wrong"]:
            tool, query = entry["tool"], entry["query"]
            title = QUERY_TITLES.get(query, query)
            pager.ensure(0.10)
            pager.text(
                "{} · {} — {}".format(tool, query, title),
                fs=10, weight="bold", color=COLORS.get(tool, "#222222"),
                height=0.024)
            pager.paragraph(
                "returned {:,}, but {} (the reference) reports {:,}".format(
                    entry["answered"], entry["ref"], entry["expected"]),
                indent=0.02, fs=9, height=0.019, width_chars=100)
            semantics = ANSWER_SEMANTICS.get((tool, query))
            if semantics:
                pager.paragraph(
                    "definitional, not a bug: {}".format(semantics),
                    indent=0.02, fs=8.5, color="#555555", style="italic",
                    height=0.018, width_chars=100)
            if len(entry["arg_sets"]) > 1:
                pager.text(
                    "argument sets affected: {}".format(
                        ", ".join(sorted(entry["arg_sets"]))),
                    indent=0.02, fs=8.5, color="#555555", height=0.018)
            if entry["command"]:
                pager.command(entry["command"])
            pager.gap(0.012)

    if summary["failed"]:
        pager.text("Failed to run", fs=11, weight="bold", color="#B00020",
                   height=0.026)
        pager.gap(0.004)
        for entry in summary["failed"]:
            tool, query = entry["tool"], entry["query"]
            pager.ensure(0.09)
            pager.text(
                "{} · {} — {}".format(tool, query, QUERY_TITLES.get(query, query)),
                fs=10, weight="bold", color=COLORS.get(tool, "#222222"),
                height=0.024)
            pager.paragraph(
                "the command returned an error ({})".format(entry["note"]),
                indent=0.02, fs=9, height=0.019, width_chars=100)
            if entry["command"]:
                pager.command(entry["command"])
            pager.gap(0.012)

    if summary["builds"]:
        pager.text("Build / index failed", fs=11, weight="bold",
                   color="#B00020", height=0.026)
        pager.gap(0.004)
        for entry in summary["builds"]:
            pager.ensure(0.09)
            pager.text(
                "{} · {}".format(entry["tool"], entry["variant"]),
                fs=10, weight="bold", height=0.024)
            detail = "status: {}".format(entry["status"])
            if entry["note"]:
                detail += " — {}".format(entry["note"])
            pager.paragraph(detail, indent=0.02, fs=9, height=0.019,
                            width_chars=100)
            if entry["command"]:
                pager.command(entry["command"])
            pager.gap(0.012)

    return pager.figures


def plot_failures(datasets, out_dir):
    """The 'what didn't go well' page: wrong answers and failures with their
    commands, appended last to all_charts.pdf and saved on its own too."""
    figures = _failures_page(collect_failures(datasets))
    for stale in out_dir.glob("figure8_failures*"):
        stale.unlink()
    outputs = []
    for number, fig in enumerate(figures, start=1):
        stem = "figure8_failures" if len(figures) == 1 else "figure8_failures_p{}".format(number)
        path = out_dir / (stem + ".png")
        fig.savefig(str(path), dpi=200)
        outputs.append(path)
    pdf_path = out_dir / "figure8_failures.pdf"
    with PdfPages(str(pdf_path)) as pdf:
        for fig in figures:
            pdf.savefig(fig)
    outputs.append(pdf_path)
    return outputs, figures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="+", type=Path)
    parser.add_argument(
        "--labels",
        default="",
        help="comma-separated dataset labels, in input order",
    )
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()
    labels = args.labels.split(",") if args.labels else []
    if labels and len(labels) != len(args.results):
        parser.error("--labels must provide one comma-separated label per input")

    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.labelsize": 10,
            "axes.labelcolor": "#333333",
            "xtick.color": "#555555",
            "ytick.color": "#333333",
            "axes.edgecolor": "#BBBBBB",
            "figure.facecolor": "white",
            "savefig.facecolor": "white",
        }
    )

    datasets = []
    for index, root_arg in enumerate(args.results):
        root = root_arg.resolve()
        index_csv = locate_csv(root, "index_results.csv")
        query_csv = locate_csv(root, "query_results.csv")
        if not index_csv and not query_csv:
            parser.error("{} contains no benchmark CSV files".format(root))
        index_rows = read_csv(index_csv) if index_csv else []
        query_rows = read_csv(query_csv) if query_csv else []
        env = find_kv(root, "env.txt")
        reps = env.get("reps")
        base_label = dataset_label(root, index_csv, labels[index] if labels else None)
        # A cold pass and a hot one are two measurements of two situations, so
        # they are two series -- the same machinery two input trees use. A CSV
        # written before the column existed has one unlabelled state, which keeps
        # its figures exactly as they were.
        for cache in cache_states(index_rows + query_rows):
            datasets.append(
                {
                    "label": base_label if not cache else "{} · {}".format(base_label, cache),
                    "base_label": base_label,
                    "cache": cache,
                    "conditions": run_conditions(env, reps),
                    "caveat": too_small_caveat(tree_file_count(index_csv)),
                    "file_count": tree_file_count(index_csv),
                    "index": index_metrics(rows_for_cache(index_rows, cache)),
                    "index_rows": rows_for_cache(index_rows, cache),
                    "walk": walk_metrics(rows_for_cache(index_rows, cache)),
                    "walk_floor": walk_floor(rows_for_cache(index_rows, cache)),
                    "queries": query_metrics(rows_for_cache(query_rows, cache)),
                    "source": str(root),
                }
            )

    out_dir = args.out_dir
    if out_dir is None:
        out_dir = args.results[0].resolve() / "charts"
    out_dir.mkdir(parents=True, exist_ok=True)
    index_outputs, index_figures = plot_index(datasets, out_dir)
    query_outputs, query_figures = plot_queries(datasets, out_dir)
    answer_outputs, answer_figures = plot_time_to_answer(datasets, out_dir)
    failure_outputs, failure_figures = plot_failures(datasets, out_dir)
    outputs = index_outputs + query_outputs + answer_outputs + failure_outputs
    figures = index_figures + query_figures + answer_figures + failure_figures
    if not outputs:
        sys.stderr.write("ERROR: no successful benchmark rows to plot\n")
        return 1
    # One Letter PDF with every figure in reading order, for a single print job
    # or a slide deck that should not juggle nine files.
    if figures:
        combined = out_dir / "all_charts.pdf"
        with PdfPages(str(combined)) as pdf:
            for fig in figures:
                pdf.savefig(fig)
        outputs.append(combined)
    for fig in figures:
        plt.close(fig)
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
