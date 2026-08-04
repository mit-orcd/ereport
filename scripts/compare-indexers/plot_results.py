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
            ((("ereport_index", "make"),), "trigram index"),
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
            ((("robinhood", "scan"),), "scan"),
            ((("robinhood", "indexes"),), "indexes"),
        ],
        "Robinhood (scan only)",
    ),
    ("GUFI (dir2index)", [(GUFI_DIR2INDEX, "dir2index")], None),
    (
        # An alternative to the row above, never an addition to it: rollup is a
        # second pass over the index that copies each directory's rows up into
        # its ancestors so a query opens fewer databases.
        "GUFI + rollup",
        [(GUFI_DIR2INDEX, "dir2index"), ((("gufi", "rollup_step"),), "rollup")],
        None,
    ),
    ("XDU", [((("xdu", "index"),), "index")], None),
]
INDEX_ORDER = [
    "ecrawl + ereport_index",
    "ecrawl",
    "Robinhood",
    "Robinhood (scan only)",
    "GUFI (dir2index)",
    "GUFI + rollup",
    "XDU",
]

# Walk-only rows: every one of these traverses the tree and stores nothing, so
# they are the rows that can sit beside each other with no asterisk.
# ecrawl --no-write is the suite's entry, and the reason it exists is that
# comparing ecrawl's full capture against find is not a like-for-like race.
WALK_VARIANTS = ("walk", "nowrite")
WALK_LABELS = {
    ("ecrawl", "nowrite"): "ecrawl --no-write",
    ("find", "walk"): "find",
    ("fd", "walk"): "fd",
    ("du", "walk"): "du",
    ("dua", "walk"): "dua",
}

WALK_CHART_ORDER = ["ecrawl --no-write", "fd", "find", "du", "dua"]
QUERY_ORDER = [
    "ecrawl_query",
    "ereport",
    "ereport_index",
    "Robinhood",
    "GUFI",
    "GUFI + rollup",
    "XDU",
    "find",
    "fd",
    "du",
    "dua",
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
    "ecrawl --no-write": "#0072B2",
    "ereport": "#0072B2",
    "ecrawl_query": "#0072B2",
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
}
WALKERS = ("find", "fd", "du", "dua")

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


def query_index_note(query_csv):
    """Which GUFI index answered which series. A rolled-up index costs several
    times what the plain one did to build, and it can answer the byte total the
    plain one cannot, so a GUFI query time means nothing without this."""
    if not query_csv:
        return ""
    env = kv_file(query_csv.parent / "env.txt")
    plain = env.get("gufi_index_root", "")
    rollup = env.get("gufi_rollup_index_root", "")
    if rollup:
        return "GUFI reads the plain index, GUFI + rollup the rolled-up one."
    if not plain:
        return ""
    kind = "rolled-up" if "rollup" in Path(plain).name else "plain"
    return "GUFI answered from its {} index.".format(kind)


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
        # Per-tool counts: without them the caption promises a sample size that
        # some of the bars beside it were never given.
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
    """Fastest tool that only traversed, as the reference for what a walk costs."""
    grouped = group_index(rows)
    best = None
    for (tool, variant), selected in grouped.items():
        if variant not in WALK_VARIANTS:
            continue
        times = floats(selected, "elapsed_sec")
        if not times:
            continue
        mean = statistics.mean(times)
        name = WALK_LABELS.get((tool, variant)) or "{} {}".format(tool, variant)
        if best is None or mean < best[1]:
            best = (name, mean)
    return best


def walk_metrics(rows):
    """Traversal rows: every tool here walks the whole tree and stores nothing,
    which makes them the one group that compares with no asterisk at all."""
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
    # ecrawl_query was called ecrawl_analyze until the rename; results recorded
    # under the old name draw as the same series rather than not at all.
    "ecrawl_analyze": "ecrawl_query",
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


def ordered_keys(order, datasets, key):
    """Names from `order` that any dataset has data for, plus anything unlisted,
    so a tool added to the harness still charts instead of silently vanishing."""
    present = set()
    for dataset in datasets:
        present.update(dataset.get(key, {}).keys())
    listed = [name for name in order if name in present]
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


def wrap_caption(text, fig_width_in, fontsize=8.5):
    """Fold the caption to the figure width. Captions carry the qualifications
    that keep a figure honest, so one running off the canvas loses exactly the
    part a reader most needs."""
    # DejaVu Sans averages a little over half the point size per character.
    columns = max(60, int((fig_width_in * 72.0 - 20) / (0.52 * fontsize)))
    return "\n".join(
        textwrap.fill(line, columns) if line else line for line in text.split("\n")
    )


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


def bar_label(axis, y, x, text, log, color="#222222", weight="normal", sub=None):
    """Value at the end of the bar, which beats making the reader trace gridlines."""
    offset = x * 1.12 if log else x
    axis.annotate(
        text,
        xy=(offset, y),
        xytext=(4, -4 if sub else 0),
        textcoords="offset points",
        va="center" if not sub else "bottom",
        ha="left",
        fontsize=8.5,
        color=color,
        fontweight=weight,
    )
    if sub:
        # The phase split, kept subordinate to the total it adds up to.
        axis.annotate(
            sub,
            xy=(offset, y),
            xytext=(4, -3),
            textcoords="offset points",
            va="top",
            ha="left",
            fontsize=7.5,
            color="#666666",
        )


def fits_inside(fig, axis, value, text, fontsize=7.5):
    """Whether a phase split can sit inside its bar instead of trailing it."""
    left = axis.get_xlim()[0]
    x0, x1 = axis.transData.transform([(left, 0), (value, 0)])[:, 0]
    width_pt = (x1 - x0) * 72.0 / fig.dpi
    # DejaVu Sans averages a little over half the point size per character.
    return width_pt > 0.55 * fontsize * len(text) + 14


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
    # Rates go the other way round: the best bar is the longest one, not the
    # shortest, and everything that assumes "less is better" has to be told.
    higher_better = spec.get("better") == "higher"

    tools = ordered_keys(spec["order"], datasets, key)
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
        return []
    multi = len(datasets) > 1

    # Worst at the top so the eye lands on the winner at the bottom, the way a
    # ranking reads. Rank on the worst dataset that has the tool, so one
    # dataset missing a tool does not banish it to the bottom.
    def sort_key(tool):
        means = [
            dataset[key].get(tool, {}).get(metric, (None, None))[0]
            for dataset in datasets
        ]
        means = [m for m in means if m is not None]
        if not means:
            return 0.0
        return min(means) if higher_better else -max(means)

    tools = sorted(tools, key=sort_key)
    shades = [0.0, 0.45, 0.68, 0.8][: len(datasets)]
    while len(shades) < len(datasets):
        shades.append(0.8)

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
    row_in = 0.66 if split_rows else 0.52
    # The constant is the header, the caption and the panel title, none of
    # which shrink with the number of rows; a one-row figure needs it just as
    # much as a ten-row one.
    height = max(3.9, row_in * len(tools) * max(1, len(datasets)) + 2.9)
    if multi:
        # The band the dataset key sits in, below the caption.
        height += 0.55
    if datasets[0].get("caveat"):
        height += 0.22
    fig, axis = plt.subplots(1, 1, figsize=(9.5, height))
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
            parts = [
                (name, values[part_index])
                for name, *values in entry.get("components", [])
            ] if spec.get("split", True) else []
            parts = parts if all(v is not None for _, v in parts) else []
            # A log axis cannot be read additively, so the phases go in the
            # label there instead of pretending the segments sum by length.
            if parts and not log:
                left = 0.0
                for part_no, (_, value) in enumerate(parts):
                    axis.barh(
                        y,
                        value,
                        left=left,
                        height=bar_h * 0.86,
                        color=shade(base, min(0.78, 0.42 * part_no)),
                        edgecolor="white",
                        linewidth=0.8,
                        zorder=3,
                    )
                    left += value
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
                # The total says what the index costs to keep; this says what it
                # costs per file, which is the number that carries to a tree of
                # a different size.
                note = "{} per file".format(fmt_bytes(entry["per_file"]))
            # Labels are placed once the axis limits are final, because whether
            # a split fits inside its bar depends on them.
            pending.append((y, mean, std, text, split, note))

    axis.set_yticks(positions)
    axis.set_yticklabels(tools, fontsize=10)
    # The walk-only line is annotated above the first bar, so it needs a band of
    # its own up there.
    axis.set_ylim(-(1.0 if floor else 0.7), len(tools) - 0.3)
    axis.invert_yaxis()
    axis.set_title(spec["panel_title"], fontsize=12, fontweight="bold",
                   loc="left", pad=8)
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
    if log and positive:
        low = min(positive) / 4.0
        # The floor line is the point of the panel it appears on, and on a log
        # axis it usually sits left of every bar, so it has to pull the limit
        # out with it or it is simply clipped away.
        if floor:
            low = min(low, floor[1] / 1.8)
    if log:
        axis.set_xlim(low, high * 2.6)
    else:
        axis.set_xlim(low, high * (1.55 if split_rows else 1.22))

    for y, mean, std, text, split, note in pending:
        end = mean + (std or 0)
        if note:
            bar_label(axis, y, end, text, log, sub=note)
            continue
        # On a linear panel the bar is already segmented, so the numbers go
        # underneath where they cannot fight the segment boundary.
        if split and (not log or not fits_inside(fig, axis, mean, split)):
            bar_label(axis, y, end, text, log, sub=split)
            continue
        if split:
            axis.annotate(
                split,
                xy=(0.008, y),
                xycoords=axis.get_yaxis_transform(),
                va="center",
                ha="left",
                fontsize=7.5,
                color="white",
                zorder=5,
            )
        bar_label(axis, y, end, text, log)

    if floor:
        name, value = floor
        axis.axvline(value, color="#B00020", linestyle="--", linewidth=1.1, zorder=2)
        # Above the first bar rather than beside it: the fastest indexer sits at
        # the bottom, which is exactly where this line is closest to a bar.
        axis.annotate(
            "walk only, stores nothing ({}, {})".format(name, fmt_seconds(value)),
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
    caption = "Means over repetitions; whiskers \u00b11 s.d. {} is better.".format(
        "Higher" if higher_better else "Lower"
    )
    if spec.get("caption"):
        caption += " " + spec["caption"]
    # A note about two rows belongs on a figure that drew both of them: with the
    # rollup switched off, or a results dir from before Robinhood's indexes were
    # timed, the sentence would describe a bar that is not there.
    for labels, note in spec.get("tool_notes", ()):
        if all(label in tools for label in labels):
            caption += " " + note
    if multi and metric != "rate" and multiple_trees(datasets):
        # Two trees on one axis: elapsed seconds do not survive that, so the rate
        # figures are the ones to read across them. Two passes over one tree are
        # a different matter -- comparing their seconds is the point.
        caption += " Across trees compare the rate figure, not seconds."
    caption = wrap_caption(caption, fig.get_figwidth())
    caveat = datasets[0].get("caveat", "")
    header_in = 0.95 if conditions else 0.75
    if caveat:
        header_in += 0.22
    fig.text(0.011, 1 - 0.30 / height,
             "Figure {}: {}".format(spec["number"], spec["heading"]),
             fontsize=15, fontweight="bold", va="top")
    fig.text(
        0.011,
        1 - 0.58 / height,
        subtitle + ("\n" + conditions if conditions else ""),
        fontsize=9,
        color="#555555",
        va="top",
    )
    if caveat:
        fig.text(
            0.011,
            1 - (0.58 + (0.30 if conditions else 0.15)) / height,
            caveat,
            fontsize=9,
            color="#b2182b",
            fontweight="bold",
            va="top",
        )
    # A dataset key gets a band of its own beneath the caption. The caption
    # wraps to nearly the full width and the axis runs to the bottom of the
    # plot, so there is no gap to tuck a key into; one has to be made.
    legend_in = 0.55 if multi else 0.0
    fig.text(0.011, (0.16 + legend_in) / height, caption,
             fontsize=8.5, color="#555555", va="bottom")
    # The caption grew a line for every explanation the figure needs, so reserve
    # room by counting them rather than by guessing at two cases.
    caption_in = 0.30 + 0.16 * caption.count("\n") + legend_in

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
            fontsize=9,
            loc="lower left",
            bbox_to_anchor=(0.011, 0.05 / height),
            ncol=len(proxies),
        )
    fig.tight_layout(rect=(0, caption_in / height, 1, 1 - header_in / height))
    outputs = []
    for suffix in ("png", "pdf"):
        path = out_dir / ("{}.{}".format(spec["name"], suffix))
        fig.savefig(str(path), dpi=200 if suffix == "png" else None)
        outputs.append(path)
    plt.close(fig)
    return outputs


# Two questions, each asked twice: how long did it take, and how fast is that.
# The pair is deliberate -- elapsed seconds are what actually happened and what
# the summary table prints, while files per second is the number that means
# anything on a tree of a different size -- and every figure is the same
# picture with a different column of numbers behind it.
#
# Captions carry only what the picture cannot: a bar that names its own phases,
# a legend that names its own series and a title that says what is plotted do
# not get a sentence repeating them.
WALK_CAPTION = (
    "Every row walks the whole tree and keeps nothing. find and fd read "
    "directories only; du, dua and ecrawl stat every file."
)
BUILD_CAPTION = "Unindexed tree to queryable index, end to end."
# Added only when the figure actually draws those bars: a caption explaining a
# row that is not there sends the reader looking for it.
GUFI_BUILD_NOTE = "GUFI's two rows are alternatives, not a sum."
# Robinhood needs no such line: its bar names its own two stages in place.
BUILD_TOOL_NOTES = [
    (("GUFI (dir2index)", "GUFI + rollup"), GUFI_BUILD_NOTE),
]
# What Robinhood's bar in the storage figure is a measure of. Its index lives in
# MariaDB's datadir, not in a directory of its own, so unlike every other row
# here it needs saying whether the three indexes are in the number.
SIZE_TOOL_NOTES = [
    (("Robinhood",), "Robinhood is its database with the three indexes built."),
    (("Robinhood (scan only)",), "Robinhood is its database before any index."),
]

INDEX_FIGURES = [
    {
        "name": "figure1_walk_time",
        "number": 1,
        "heading": "walking, elapsed time",
        "panel_title": "How long a full walk of the tree takes",
        "key": "walk",
        "order": WALK_CHART_ORDER,
        "metric": "time",
        "formatter": fmt_seconds,
        "xlabel": "Elapsed seconds",
        "caption": WALK_CAPTION,
    },
    {
        "name": "figure2_walk_rate",
        "number": 2,
        "heading": "walking, throughput",
        "panel_title": "Files per second, walking",
        "key": "walk",
        "order": WALK_CHART_ORDER,
        "metric": "rate",
        "better": "higher",
        "formatter": fmt_rate,
        "xlabel": "Files per second",
        "caption": WALK_CAPTION,
    },
    {
        "name": "figure3_build_time",
        "number": 3,
        "heading": "building, elapsed time",
        "panel_title": "How long it takes to build a queryable index",
        "key": "index",
        "order": INDEX_ORDER,
        "metric": "time",
        "formatter": fmt_seconds,
        "xlabel": "Elapsed seconds",
        "floor": True,
        # The dashed line explains itself where it is drawn.
        "caption": BUILD_CAPTION,
        "tool_notes": BUILD_TOOL_NOTES,
    },
    {
        "name": "figure4_build_rate",
        "number": 4,
        "heading": "building, throughput",
        "panel_title": "Files per second, building the index",
        "key": "index",
        "order": INDEX_ORDER,
        "metric": "rate",
        "better": "higher",
        "formatter": fmt_rate,
        "xlabel": "Files per second",
        # Rates do not add, so the phases stay off this one: the bar is the
        # whole pipeline's throughput, and Figure 3 is where its parts live.
        "split": False,
        "caption": BUILD_CAPTION + " Rates do not add; phases are in Figure 3.",
        "tool_notes": BUILD_TOOL_NOTES,
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
        "caption": "Walk-only tools store nothing, so they are not here.",
        "tool_notes": SIZE_TOOL_NOTES,
    },
]


def plot_index(datasets, out_dir):
    outputs = []
    for spec in INDEX_FIGURES:
        outputs.extend(index_figure(datasets, out_dir, spec))
    return outputs


def query_figure(usable, queries, rows_per_query, bounds, page, pages):
    """One page of query panels: the queries given, one row each.

    Split across pages rather than run down one tall sheet, because six panels
    of six bars is a figure nobody can put on a page and still read.
    """
    lo, hi = bounds
    heights = [rows_per_query[q] for q in queries]
    fig_height = 0.72 * len(queries) + 0.38 * sum(heights) + 2.3
    fig, axes = plt.subplots(
        len(queries),
        len(usable),
        figsize=(11.5 * len(usable), fig_height),
        # hspace belongs after tight_layout, not here: setting it on the
        # gridspec marks the axes incompatible with tight_layout, which then
        # discards the value and warns.
        gridspec_kw={"height_ratios": heights},
        sharex=True,
        squeeze=False,
    )

    any_mismatch = False
    walkers_seen = set()

    for column, dataset in enumerate(usable):
        stats = dataset["queries"]["stats"]
        states = dataset["queries"]["states"]
        notes_by_key = dataset["queries"].get("notes", {})
        reference = reference_counts(dataset["queries"])

        for row, query in enumerate(queries):
            axis = axes[row][column]
            # Fastest at the bottom, so each panel reads as a ranking.
            entries = sorted(
                (t for t in QUERY_ORDER if (t, query) in stats),
                key=lambda t: -stats[(t, query)][0],
            )
            positions = list(range(len(entries)))
            # Reason -> the tools it applies to, for the bars that missed the
            # reference because they answer a different question.
            semantics = {}
            for y, tool in zip(positions, entries):
                mean, std = stats[(tool, query)]
                mismatch = wrong_answer(dataset["queries"], reference, tool, query)
                wrong = mismatch is not None
                any_mismatch = any_mismatch or wrong
                if tool in WALKERS:
                    walkers_seen.add(tool)
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
                    answered, expected_value, ref_tool = mismatch
                    text += "   \u2717 answered {:,}, not {:,} (per {})".format(
                        answered, expected_value, ref_tool
                    )
                    # The reason goes in the panel note rather than here: the bar
                    # label already runs past the axis, and in the right-hand
                    # column another clause runs off the sheet.
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
            axis.set_yticklabels(entries, fontsize=10)
            axis.set_ylim(-0.75, max(1, len(entries)) - 0.25)
            axis.set_xscale("log")
            axis.set_xlim(lo, hi)
            style_axis(axis, None, True)
            axis.xaxis.set_major_formatter(
                FuncFormatter(lambda v, _: fmt_tick_seconds(v))
            )
            title = "{}  \u00b7  {}".format(query, QUERY_TITLES.get(query, ""))
            if len(usable) > 1:
                title = "{}   \u2014   {}".format(title, dataset["label"])
            axis.set_title(title, fontsize=11.5, fontweight="bold", loc="left", pad=6)

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
            notes = []
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
            if notes:
                # Inside the panel, top right: the bars are sorted longest at
                # the bottom, so that corner is the empty one. Above the panel
                # it collided with the title, and below it with the shared tick
                # labels of the bottom-most axis.
                # One reason per line: a Q4 panel can carry three, and strung
                # together they reach past the panel and into the next column.
                axis.annotate(
                    "\n".join(notes),
                    xy=(0.995, 0.94),
                    xycoords="axes fraction",
                    fontsize=8,
                    color="#888888",
                    va="top",
                    ha="right",
                )

        axes[-1][column].set_xlabel("Elapsed time (log scale)", fontsize=10, labelpad=8)

    conditions = usable[0].get("conditions", "")
    subtitle = subtitle_for(usable)
    caveat = usable[0].get("caveat", "")
    heading = "Figure 6: query performance"
    if pages > 1:
        heading += "  ({} of {}, {})".format(page, pages, "\u2013".join(
            [queries[0], queries[-1]] if len(queries) > 1 else [queries[0]]
        ))
    fig.text(0.011, 1 - 0.30 / fig_height, heading,
             fontsize=15, fontweight="bold", va="top")
    fig.text(
        0.011,
        1 - 0.58 / fig_height,
        subtitle + ("\n" + conditions if conditions else ""),
        fontsize=9,
        color="#555555",
        va="top",
    )
    if caveat:
        fig.text(
            0.011,
            1 - (0.58 + (0.30 if conditions else 0.15)) / fig_height,
            caveat,
            fontsize=9,
            color="#b2182b",
            fontweight="bold",
            va="top",
        )

    # Short enough to read: anything a bar, a title or a panel note already says
    # is not repeated here.
    caption = "Mean wall time; whiskers \u00b11 s.d. Lower is better."
    index_note = usable[0].get("query_index_note", "")
    if index_note:
        caption += " " + index_note
    if any_mismatch:
        caption += " Hatched bars missed the reference; the panel note says which are definitional."
    if walkers_seen:
        caption += " Grey and tan tools search live, with no index."
    caption = wrap_caption(caption, fig.get_figwidth())
    fig.text(0.011, 0.16 / fig_height, caption, fontsize=8.5, color="#555555", va="bottom")

    # Room for the two header lines plus the first panel's title.
    header_in = 1.30 if conditions else 1.05
    if caveat:
        header_in += 0.22
    fig.tight_layout(rect=(0, 0.72 / fig_height, 1, 1 - header_in / fig_height))
    # Each panel carries a title above and a note to its right, so they need
    # more air between them than tight_layout leaves.
    fig.subplots_adjust(hspace=0.42)
    return fig


# Three panels to a page. Six on one sheet is either unreadably small in print
# or a page nobody can see the bottom of on screen.
QUERIES_PER_PAGE = 3


def plot_queries(datasets, out_dir):
    usable = [d for d in datasets if d["queries"]["stats"]]
    if not usable:
        return []

    # One panel per query, each only as tall as it has bars, so a query answered
    # by three tools does not get the same space as one answered by six. With
    # several datasets each becomes a column, the paper's Set 1 / Set 2 layout.
    rows_per_query = {}
    for query in QUERIES:
        depth = max(
            len([t for t in QUERY_ORDER if (t, query) in d["queries"]["stats"]])
            for d in usable
        )
        if depth:
            rows_per_query[query] = depth
    if not rows_per_query:
        return []
    queries = [q for q in QUERIES if q in rows_per_query]

    # One x-axis for every page, or a query on page two would look faster than
    # one on page one for no reason but its own axis.
    everything = [
        m for d in usable for (m, _s) in d["queries"]["stats"].values() if m
    ]
    bounds = (min(everything) / 3.0, max(everything) * 3.0)

    pages = [
        queries[start:start + QUERIES_PER_PAGE]
        for start in range(0, len(queries), QUERIES_PER_PAGE)
    ]
    figures = [
        query_figure(usable, page, rows_per_query, bounds, number, len(pages))
        for number, page in enumerate(pages, start=1)
    ]

    outputs = []
    for number, fig in enumerate(figures, start=1):
        stem = "figure6_queries" if len(figures) == 1 else "figure6_queries_p{}".format(number)
        path = out_dir / (stem + ".png")
        fig.savefig(str(path), dpi=200)
        outputs.append(path)
    # The PDF stays one file with one page per group, which is what a reader
    # scrolling a paper expects and what a printer needs.
    pdf_path = out_dir / "figure6_queries.pdf"
    with PdfPages(str(pdf_path)) as pdf:
        for fig in figures:
            pdf.savefig(fig)
    outputs.append(pdf_path)
    for fig in figures:
        plt.close(fig)
    return outputs


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
                    "walk": walk_metrics(rows_for_cache(index_rows, cache)),
                    "walk_floor": walk_floor(rows_for_cache(index_rows, cache)),
                    "query_index_note": query_index_note(query_csv),
                    "queries": query_metrics(rows_for_cache(query_rows, cache)),
                    "source": str(root),
                }
            )

    out_dir = args.out_dir
    if out_dir is None:
        out_dir = args.results[0].resolve() / "charts"
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = plot_index(datasets, out_dir) + plot_queries(datasets, out_dir)
    if not outputs:
        sys.stderr.write("ERROR: no successful benchmark rows to plot\n")
        return 1
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
