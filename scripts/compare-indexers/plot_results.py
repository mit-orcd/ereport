#!/usr/bin/env python3
"""Create paper-style Figure 1 and Figure 3 from comparison CSV results.

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
    from matplotlib.patches import Patch
    from matplotlib.ticker import FuncFormatter, LogLocator
except ImportError:
    sys.stderr.write(
        "ERROR: matplotlib is required for charts. Rerun init.sh with "
        "INSTALL_PACKAGES=1, or install python3-matplotlib.\n"
    )
    raise SystemExit(2)


# Each indexer is one or more measured phases. Naming the phases beats a single
# opaque "suite" bar: a reader comparing against a one-shot indexer needs the
# total, and a reader tuning ecrawl needs to know which half to attack.
PIPELINES = [
    (
        "ecrawl + ereport_index",
        [(("ecrawl", "write"), "crawl"), (("ereport_index", "make"), "trigram index")],
        "ecrawl",  # label when only the first phase ran
    ),
    ("Robinhood", [(("robinhood", "scan"), "scan")], None),
    ("GUFI", [(("gufi", "plain"), "dir2index")], None),
    (
        "GUFI rollup",
        [(("gufi", "rollup_index"), "dir2index"), (("gufi", "rollup_step"), "rollup")],
        None,
    ),
    # Runs before this fix timed dir2index only and called it the rollup.
    ("GUFI rollup", [(("gufi", "rollup"), "dir2index + rollup")], None),
    ("XDU", [(("xdu", "index"), "index")], None),
]
INDEX_ORDER = [
    "ecrawl + ereport_index",
    "ecrawl",
    "Robinhood",
    "GUFI",
    "GUFI rollup",
    "XDU",
]

# Walk-only rows: every one of these traverses the tree and stores nothing, so
# they are the only rows in the run that can sit beside each other with no
# asterisk. ecrawl --no-write is the suite's entry, and the reason it exists is
# that comparing ecrawl's full capture against find is not a like-for-like race.
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
    "ecrawl_analyze",
    "ereport",
    "ereport_index",
    "Robinhood",
    "GUFI",
    "XDU",
    "find",
    "fd",
    "du",
    "dua",
]
QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
QUERY_TITLES = {
    "Q1": "exact-name lookup",
    "Q2": "name pattern across the tree",
    "Q3": "files over a size threshold",
    "Q4": "total bytes in a subtree",
    "Q5": "list files in a subtree",
}

# Indexers get saturated colour; the traditional walkers stay neutral, so the
# two classes of tool separate at a glance. Okabe-Ito, which stays legible for
# the common colour vision deficiencies and in greyscale print.
COLORS = {
    "ecrawl + ereport_index": "#0072B2",
    "ecrawl": "#0072B2",
    "ecrawl --no-write": "#0072B2",
    "ereport": "#0072B2",
    "ecrawl_analyze": "#0072B2",
    "ereport_index": "#56B4E9",
    "Robinhood": "#D55E00",
    "GUFI": "#009E73",
    "GUFI rollup": "#8FA01F",
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
        bits.append("{} repetition{}".format(reps, "" if reps == 1 else "s"))
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


def index_metrics(rows):
    """Per pipeline: the total, plus every phase that was measured separately."""
    grouped = group_index(rows)
    result = {}
    for label, phases, solo_label in PIPELINES:
        present = []
        for key, phase_name in phases:
            selected = grouped.get(key, [])
            times = floats(selected, "sec_per_1M_files")
            sizes = floats(selected, "mib_per_1M_files")
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
        entry = {
            "time": combine_stats([p[1] for p in present])
            if all(p[1] for p in present)
            else mean_std(present[0][1]),
            "size": combine_stats([p[2] for p in present])
            if all(p[2] for p in present)
            else mean_std(present[0][2]),
        }
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
        times = floats(selected, "sec_per_1M_files")
        if not times:
            continue
        mean = statistics.mean(times)
        name = WALK_LABELS.get((tool, variant)) or "{} {}".format(tool, variant)
        if best is None or mean < best[1]:
            best = (name, mean)
    return best


def walk_metrics(rows):
    """Traversal-only rows, as their own ranking rather than a single floor line."""
    grouped = group_index(rows)
    result = {}
    for (tool, variant), selected in grouped.items():
        if variant not in WALK_VARIANTS:
            continue
        times = floats(selected, "sec_per_1M_files")
        if not times:
            continue
        label = WALK_LABELS.get((tool, variant)) or "{} {}".format(tool, variant)
        result[label] = {"time": mean_std(times)}
    return result


def measured_build_only(rows):
    """The build cost of the suite with its own traversal removed, which is the
    one pipeline here where both halves are known.

    The two halves are known in different ways, and the difference matters
    enough to carry: writing the capture is fused into ecrawl's walk exactly as
    GUFI's write is fused into its own, so it can only be inferred by
    subtraction; the trigram build is a separate command reading the shards
    rather than the tree, so it is timed outright and needs no subtraction.
    """
    grouped = group_index(rows)
    write = floats(grouped.get(("ecrawl", "write"), []), "sec_per_1M_files")
    nowrite = floats(grouped.get(("ecrawl", "nowrite"), []), "sec_per_1M_files")
    if not write or not nowrite:
        return None
    store = statistics.mean(write) - statistics.mean(nowrite)
    if store <= 0:
        # The walk measured slower than the walk that also wrote, so the two
        # runs differ by less than the noise between them. On a tree small
        # enough for that to happen there is no capture cost to report, and
        # quoting a negative one as "measured" would be worse than saying
        # nothing.
        return None
    parts = [("capture write", store, "inferred")]
    trigram = floats(grouped.get(("ereport_index", "make"), []), "sec_per_1M_files")
    if trigram:
        parts.append(("trigram index", statistics.mean(trigram), "timed directly"))
    return {"total": sum(value for _, value, _ in parts), "parts": parts}


def build_only_metrics(index_entries, floor, rows):
    """Index construction with the traversal floor taken off the total.

    Only ecrawl was timed both ways, so for every other tool this is an
    estimate -- and specifically an upper bound, since subtracting the *fastest*
    walk anyone managed removes no more than that tool's own traversal did.
    The exact figure is carried alongside where it is known, so the chart can
    show what was measured next to what was inferred.
    """
    if not floor:
        return {}
    floor_value = floor[1]
    exact = measured_build_only(rows)
    result = {}
    for label, entry in index_entries.items():
        mean, std = entry.get("time", (None, None))
        if mean is None:
            continue
        remainder = mean - floor_value
        if remainder <= 0:
            # The tool indexed in less than the fastest bare walk, so the floor
            # says nothing about it; showing a zero-length bar would be a claim.
            continue
        item = {"time": (remainder, std)}
        if label.startswith("ecrawl") and exact is not None:
            item["exact"] = exact["total"]
            item["exact_parts"] = exact["parts"]
        result[label] = item
    return result


# Named after the binary that ran, so a panel says which half of the suite
# answered the query rather than crediting the pair for either one's work.
QUERY_TOOL_LABELS = {
    "ecrawl_suite": "ereport",
    "ereport_index": "ereport_index",
    "ereport": "ereport",
    "ecrawl_analyze": "ecrawl_analyze",
    "robinhood": "Robinhood",
    "gufi": "GUFI",
    "xdu": "XDU",
    "find": "find",
    "fd": "fd",
    "du": "du",
    "dua": "dua",
}


def query_metrics(rows):
    """Per (tool, query): timing, the answer returned, and why it is missing."""
    timings = {}
    counts = {}
    states = {}
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
            continue
        states[(label, query)] = "ok"
        try:
            timings.setdefault((label, query), []).append(float(row["elapsed_sec"]))
        except (KeyError, ValueError):
            pass
        try:
            counts[(label, query)] = int(row["result_count"])
        except (KeyError, ValueError, TypeError):
            pass

    stats = {key: mean_std(values) for key, values in timings.items()}
    return {"stats": stats, "counts": counts, "states": states}


def reference_counts(queries):
    """What the answer should be, taken from the tool the paper treats as truth."""
    counts = queries["counts"]
    preferred = {
        "Q1": ["find", "fd"],
        "Q2": ["find", "fd"],
        "Q3": ["find", "fd"],
        "Q4": ["du", "dua"],
        "Q5": ["find", "fd"],
    }
    reference = {}
    for query in QUERIES:
        for tool in preferred[query]:
            if (tool, query) in counts:
                reference[query] = (tool, counts[(tool, query)])
                break
    return reference


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
    tools = ordered_keys(spec["order"], datasets, key)
    if not tools:
        return []
    multi = len(datasets) > 1

    # Slowest at the top so the eye lands on the winner at the bottom, the way a
    # ranking reads. Rank on the slowest dataset that has the tool, so one
    # dataset missing a tool does not banish it to the bottom.
    def sort_key(tool):
        means = [
            dataset[key].get(tool, {}).get(metric, (None, None))[0]
            for dataset in datasets
        ]
        means = [m for m in means if m is not None]
        return -max(means) if means else 0.0

    tools = sorted(tools, key=sort_key)
    shades = [0.0, 0.45, 0.68, 0.8][: len(datasets)]
    while len(shades) < len(datasets):
        shades.append(0.8)

    # A phase split, or a measured value quoted under an estimate, adds a second
    # line of text per bar, which needs the room.
    split_rows = any(
        {"components", "exact"}.intersection(dataset[key].get(tool, {}))
        for dataset in datasets
        for tool in tools
    )
    floor = datasets[0].get("walk_floor") if spec.get("floor") else None
    row_in = 0.66 if split_rows else 0.52
    # The constant is the header, the caption and the panel title, none of
    # which shrink with the number of rows; a one-row figure needs it just as
    # much as a ten-row one.
    height = max(3.9, row_in * len(tools) * max(1, len(datasets)) + 2.9)
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
            parts = [
                (name, values[part_index])
                for name, *values in entry.get("components", [])
            ]
            parts = parts if all(v is not None for _, v in parts) else []
            known = entry.get("exact")
            if spec.get("estimated"):
                # Two tone: the bar runs out to the upper bound, and the solid
                # part marks how much of that is actually known. Unlike a phase
                # split this survives a log axis, because the meaning is carried
                # by where the boundary sits, not by how long each piece is.
                axis.barh(
                    y,
                    mean,
                    height=bar_h * 0.86,
                    color=shade(base, 0.66),
                    edgecolor="white",
                    linewidth=0.6,
                    zorder=3,
                )
                if known and known < mean:
                    axis.barh(
                        y,
                        known,
                        height=bar_h * 0.86,
                        color=base,
                        edgecolor="white",
                        linewidth=0.6,
                        zorder=3.5,
                    )
            # A log axis cannot be read additively, so the phases go in the
            # label there instead of pretending the segments sum by length.
            elif parts and not log:
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
            if spec.get("estimated"):
                # Every bar on an estimated panel is an upper bound, and saying
                # so on the bar keeps a reader from taking the panel for
                # measurement once it is separated from its caption.
                text = "\u2264 {}".format(text)
                if known:
                    # Kept outside the bar, unlike a phase split: printed inside
                    # it would read as a segment of the bound rather than as the
                    # smaller true value.
                    note = "measured: {}".format(formatter(known))
                    # How the two halves were arrived at differs, and without
                    # this the whole bar reads as inferred when half of it was
                    # timed on its own.
                    breakdown = " + ".join(
                        "{} {} ({})".format(formatter(value), name, how)
                        for name, value, how in entry.get("exact_parts", [])
                    )
                    if breakdown:
                        note += " = " + breakdown
            # Labels are placed once the axis limits are final, because whether
            # a split fits inside its bar depends on them.
            pending.append((y, mean, std, text, split, note))

    axis.set_yticks(positions)
    # A figure that has taken something out of the bar says so in the row label,
    # rather than naming commands whose cost is no longer all there.
    label_map = spec.get("label_map", {})
    axis.set_yticklabels([label_map.get(tool, tool) for tool in tools], fontsize=10)
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
        if note:
            bar_label(axis, y, mean + (std or 0), text, log, sub=note)
            continue
        # On a linear panel the bar is already segmented, so the numbers go
        # underneath where they cannot fight the segment boundary.
        if split and (not log or not fits_inside(fig, axis, mean, split)):
            bar_label(axis, y, mean + (std or 0), text, log, sub=split)
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
        bar_label(axis, y, mean + (std or 0), text, log)

    if spec.get("estimated") and any(
        "exact" in dataset[key].get(tool, {}) for dataset in datasets for tool in tools
    ):
        # Without this the two tones are just a colour, and a reader has no way
        # to know the pale part of a bar is the part nobody measured. It sits
        # above the panel rather than inside it: the free space in the plot is
        # bottom right, which is where the fastest bar's own labels go.
        axis.legend(
            handles=[
                Patch(facecolor="#555555", edgecolor="white", label="measured"),
                Patch(facecolor=shade("#555555", 0.66), edgecolor="white",
                      label="bound (not measured)"),
            ],
            loc="lower right",
            bbox_to_anchor=(1.0, 1.0),
            ncol=2,
            frameon=False,
            fontsize=8,
            handlelength=1.4,
            columnspacing=1.4,
        )

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

    if multi:
        # Neutral swatches: the bars carry the tool's own hue, so a coloured key
        # would suggest a mapping that does not exist.
        proxies = [
            Patch(facecolor=shade("#555555", shades[i]), edgecolor="white",
                  label=dataset["label"])
            for i, dataset in enumerate(datasets)
        ]
        fig.legend(
            handles=proxies,
            title="Dataset (lighter shade = later)",
            frameon=False,
            fontsize=9,
            loc="lower right",
            bbox_to_anchor=(0.995, 0.10 / height),
            ncol=len(proxies),
        )

    subtitle = datasets[0]["label"] if not multi else "; ".join(d["label"] for d in datasets)
    conditions = datasets[0].get("conditions", "")
    caption = (
        "Bars are means over repetitions; whiskers are one population standard "
        "deviation. Lower is better."
    )
    if spec.get("caption"):
        caption += "\n" + spec["caption"]
    if spec.get("caption_if_exact") and any(
        "exact" in dataset[key].get(tool, {}) for dataset in datasets for tool in tools
    ):
        caption += " " + spec["caption_if_exact"]
    # Only when phases are actually drawn: an estimated panel also carries a
    # second line per bar, but that one is a measured value, not a phase.
    has_components = any(
        "components" in dataset[key].get(tool, {})
        for dataset in datasets
        for tool in tools
    )
    if has_components:
        caption += (
            "\nPipelines built in two commands are split into their phases: the "
            "bar is segmented, and the smaller line by each value gives the parts."
        )
        # Only gloss the pipelines this run actually contains.
        glosses = {
            "ecrawl + ereport_index": (
                "ecrawl writes the capture, ereport_index --make adds the "
                "trigram index on top of it"
            ),
            "GUFI rollup": "gufi_dir2index builds the replica, gufi_rollup folds it up",
        }
        shown = [glosses[tool] for tool in tools if tool in glosses]
        if shown:
            caption += "\n" + "; ".join(shown) + "."
    # Last, so it reads as a footnote about the set rather than interrupting
    # what this particular figure is saying.
    if spec.get("decomposition"):
        caption += "\n" + DECOMPOSITION_NOTE
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
    fig.text(0.011, 0.16 / height, caption, fontsize=8.5, color="#555555", va="bottom")
    # The caption grew a line for every explanation the figure needs, so reserve
    # room by counting them rather than by guessing at two cases.
    caption_in = 0.30 + 0.16 * caption.count("\n")
    fig.tight_layout(rect=(0, caption_in / height, 1, 1 - header_in / height))
    outputs = []
    for suffix in ("png", "pdf"):
        path = out_dir / ("{}.{}".format(spec["name"], suffix))
        fig.savefig(str(path), dpi=200 if suffix == "png" else None)
        outputs.append(path)
    plt.close(fig)
    return outputs


# The three time figures are one quantity cut in two and then put back
# together, not three takes on the same chart. Saying so on each of them is the
# only thing that stops the shared phases -- the trigram build appears in both
# the total and the build-only figure -- from reading as double counting.
DECOMPOSITION_NOTE = (
    "These three time figures decompose one measurement: Figure 1 (walking) + "
    "Figure 2 (building) = Figure 3 (walking and building). They are nested, "
    "not alternatives, so a phase appearing in two of them is not counted twice."
)

# The four index figures. Same renderer, different column of numbers, so a
# reader can lay them side by side and have the bars mean the same thing.
INDEX_FIGURES = [
    {
        "name": "figure1_walk",
        "number": 1,
        "heading": "walking",
        "panel_title": "Traversal only: seeing every file, storing nothing",
        "key": "walk",
        "order": WALK_CHART_ORDER,
        "metric": "time",
        "formatter": fmt_seconds,
        "xlabel": "Seconds per 1M files",
        "caption": (
            "Every tool here traverses and stores nothing, so these are the only "
            "bars in the set that compare directly. ecrawl --no-write is the "
            "suite's walk without the capture, which is what makes it the "
            "like-for-like row against find, fd and du."
        ),
        "decomposition": True,
    },
    {
        "name": "figure2_build",
        "number": 2,
        "heading": "building",
        "panel_title": "Index construction only, with the walk taken out",
        "key": "build_only",
        "order": INDEX_ORDER,
        # The bar no longer holds everything those two commands cost, so naming
        # them would overstate it. What is left is the work after the walk.
        "label_map": {"ecrawl + ereport_index": "capture write + trigram index"},
        "metric": "time",
        "formatter": fmt_seconds,
        "xlabel": "Seconds per 1M files",
        "estimated": True,
        "caption": (
            "MOSTLY ESTIMATE: a bar runs out to the end-to-end build minus the "
            "fastest walk anyone managed. No tool traverses faster than that, so "
            "the subtraction removes no more than the tool's own walk did and "
            "the bar end is an upper bound (\u2264), not a measurement."
        ),
        "decomposition": True,
        # Only true when a pipeline was timed both ways and the two runs
        # differed by more than the noise between them.
        "caption_if_exact": (
            "ecrawl (the solid bar) was timed both with and without storing, so "
            "its build cost is measured rather than bounded; the pale remainder "
            "out to its bound is the part of its own walk that subtracting the "
            "fastest walk failed to remove."
        ),
    },
    {
        "name": "figure3_index_total",
        "number": 3,
        "heading": "walking and building",
        "panel_title": "Everything, as run against a cold tree",
        "key": "index",
        "order": INDEX_ORDER,
        "metric": "time",
        "formatter": fmt_seconds,
        "xlabel": "Seconds per 1M files",
        "floor": True,
        "caption": (
            "What each pipeline actually costs from an unindexed tree: the walk "
            "and the index build together, and the only time figure here that is "
            "measured end to end for every tool. The dashed line is the fastest "
            "walk measured, the floor no indexer can go below."
        ),
        "decomposition": True,
    },
    {
        "name": "figure4_index_size",
        "number": 4,
        "heading": "index storage",
        "panel_title": "Bytes kept on disk per 1M files",
        "key": "index",
        "order": INDEX_ORDER,
        "metric": "size",
        "formatter": fmt_mib,
        "xlabel": "MiB per 1M files",
        "caption": (
            "Walk-only tools are absent because they store nothing; that is the "
            "trade they make for the times in Figure 1."
        ),
    },
]


def plot_index(datasets, out_dir):
    outputs = []
    for spec in INDEX_FIGURES:
        outputs.extend(index_figure(datasets, out_dir, spec))
    return outputs


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

    everything = [
        m for d in usable for (m, _s) in d["queries"]["stats"].values() if m
    ]
    lo = min(everything) / 3.0
    hi = max(everything) * 3.0
    any_mismatch = False
    walkers_seen = set()

    for column, dataset in enumerate(usable):
        stats = dataset["queries"]["stats"]
        counts = dataset["queries"]["counts"]
        states = dataset["queries"]["states"]
        reference = reference_counts(dataset["queries"])

        for row, query in enumerate(queries):
            axis = axes[row][column]
            # Fastest at the bottom, so each panel reads as a ranking.
            entries = sorted(
                (t for t in QUERY_ORDER if (t, query) in stats),
                key=lambda t: -stats[(t, query)][0],
            )
            positions = list(range(len(entries)))
            for y, tool in zip(positions, entries):
                mean, std = stats[(tool, query)]
                expected = reference.get(query)
                wrong = expected is not None and count_disagrees(
                    query, counts.get((tool, query)), expected[1]
                )
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
                    text += "   \u2717 answered {:,}, not {:,} (per {})".format(
                        counts.get((tool, query), 0), expected[1], expected[0]
                    )
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
            missing = {"unsupported": [], "failed": []}
            for tool in QUERY_ORDER:
                state = states.get((tool, query))
                if state in (None, "ok"):
                    continue
                missing["failed" if state == "fail" else "unsupported"].append(tool)
            notes = []
            if missing["unsupported"]:
                notes.append("no equivalent query: " + ", ".join(missing["unsupported"]))
            if missing["failed"]:
                notes.append("failed: " + ", ".join(missing["failed"]))
            if notes:
                # Top right, opposite the title: below the panel it would land
                # on the shared tick labels of the bottom-most axis.
                axis.annotate(
                    "   \u00b7   ".join(notes),
                    xy=(1, 1),
                    xycoords="axes fraction",
                    xytext=(0, 6),
                    textcoords="offset points",
                    fontsize=8,
                    color="#888888",
                    va="bottom",
                    ha="right",
                )

        axes[-1][column].set_xlabel("Elapsed time (log scale)", fontsize=10, labelpad=8)

    conditions = usable[0].get("conditions", "")
    subtitle = (
        usable[0]["label"]
        if len(usable) == 1
        else "; ".join(d["label"] for d in usable)
    )
    caveat = usable[0].get("caveat", "")
    fig.text(0.011, 1 - 0.30 / fig_height, "Figure 5: query performance",
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

    caption = (
        "Bars are mean wall time over repetitions; whiskers are one population "
        "standard deviation. Shorter is better."
    )
    if any_mismatch:
        caption += (
            "\nHatched bars answered with a different result than the reference "
            "tool, so their timing does not measure the same work."
        )
    if walkers_seen:
        caption += (
            "\nTraditional walkers ({}) are shown in neutral grey and tan; they search live "
            "rather than from an index.".format(
                ", ".join(t for t in QUERY_ORDER if t in walkers_seen)
            )
        )
    fig.text(0.011, 0.16 / fig_height, caption, fontsize=8.5, color="#555555", va="bottom")

    # Room for the two header lines plus the first panel's title.
    header_in = 1.30 if conditions else 1.05
    if caveat:
        header_in += 0.22
    fig.tight_layout(rect=(0, 0.72 / fig_height, 1, 1 - header_in / fig_height))
    # Each panel carries a title above and a note to its right, so they need
    # more air between them than tight_layout leaves.
    fig.subplots_adjust(hspace=0.42)
    outputs = []
    for suffix in ("png", "pdf"):
        path = out_dir / ("figure5_queries." + suffix)
        fig.savefig(str(path), dpi=200 if suffix == "png" else None)
        outputs.append(path)
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
        env = find_kv(root, "env.txt")
        reps = env.get("reps")
        pipelines = index_metrics(index_rows)
        floor = walk_floor(index_rows)
        datasets.append(
            {
                "label": dataset_label(
                    root, index_csv, labels[index] if labels else None
                ),
                "conditions": run_conditions(env, reps),
                "caveat": too_small_caveat(tree_file_count(index_csv)),
                "index": pipelines,
                "walk": walk_metrics(index_rows),
                "build_only": build_only_metrics(pipelines, floor, index_rows),
                "walk_floor": floor,
                "queries": (
                    query_metrics(read_csv(query_csv))
                    if query_csv
                    else {"stats": {}, "counts": {}, "states": {}}
                ),
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
