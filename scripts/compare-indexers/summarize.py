#!/usr/bin/env python3
"""Summarize compare-indexers CSV outputs into paper-style tables.

Usage:
  python3 scripts/compare-indexers/summarize.py <results-dir> [<results-dir> ...]

Looks for index_results.csv and/or query_results.csv under each dir (or the dir itself).
Writes SUMMARY_TABLE.txt beside the first results dir (or --out PATH).

Compatible with Python 3.6+.
"""

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path


def mean_std(vals):
    vals = [v for v in vals if v is not None and not math.isnan(v)]
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], 0.0
    # statistics.fmean is 3.8+; mean works on 3.6
    return statistics.mean(vals), statistics.pstdev(vals)


def fmt(x, nd=3):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    return ("{0:." + str(nd) + "f}").format(x)


def load_csv(path):
    """Rows, each tagged with the directory it came from so that stderr lookups
    stay inside the phase that produced the row."""
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["__dir"] = str(path.parent)
    return rows


def load_kv(dirs, filename):
    """First key=value file of this name found under the results dirs."""
    for d in dirs:
        p = (d if d.is_dir() else d.parent) / filename
        if not p.is_file():
            continue
        values = {}
        with p.open() as f:
            for line in f:
                key, sep, value = line.rstrip("\n").partition("=")
                if sep:
                    values[key] = value
        if values:
            return values
    return {}


def load_env(dirs):
    """Written by run_index/run_queries at the start of each phase."""
    return load_kv(dirs, "env.txt")


# What each of the paper's queries asks, independent of tool.
QUERY_DOC = [
    ("Q1", "exact-name lookup", "find the one file with a given unique basename"),
    ("Q2", "glob search", "find files whose name matches a wildcard pattern"),
    ("Q3", "size filter", "find regular files larger than a byte threshold"),
    ("Q4", "subtree aggregate", "total bytes below one subtree (du -sb equivalent)"),
    ("Q5", "subtree file count", "count regular files below one subtree"),
]


def human_bytes(text):
    try:
        n = int(text)
    except (TypeError, ValueError):
        return None
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return "{0:.0f} {1}".format(n, unit) if unit == "B" else "{0:.1f} {1}".format(n, unit)
        n /= 1024.0
    return None


def summarize_query_defs(params):
    """Explain Q1-Q5 and the concrete parameter each one ran with."""
    lines = ["== QUERY DEFINITIONS ==", ""]
    detail = {}
    seeded = {}
    if params:
        name = params.get("q1_name")
        if name:
            detail["Q1"] = "basename '{0}'".format(name)
        glob = params.get("q2_glob")
        if glob:
            note = "pattern '{0}'".format(glob)
            term = params.get("q2_index_term") or params.get("q2_term")
            if term:
                note += ", trigram term '{0}'".format(term)
            filt = params.get("q2_exact_filter")
            if filt:
                note += ", index matches narrowed to the glob with regex {0}".format(filt)
            detail["Q2"] = note
            seeded["Q2"] = params.get("q2_expected")
        min_bytes = params.get("q3_min_bytes")
        if min_bytes:
            pretty = human_bytes(min_bytes)
            note = "larger than {0} bytes".format(min_bytes)
            if pretty:
                note += " ({0})".format(pretty)
            detail["Q3"] = note
            seeded["Q3"] = params.get("q3_expected")
        for key, q in (("q4_subtree", "Q4"), ("q5_subtree", "Q5")):
            subtree = params.get(key)
            if subtree:
                detail[q] = subtree

    for q, title, what in QUERY_DOC:
        lines.append("{0}  {1:<20} {2}".format(q, title, what))
        if q in detail:
            lines.append("    ran with: {0}".format(detail[q]))
        # A floor, not an expected result: prepare-synth.sh plants these, but the
        # generator creates matching files of its own, so a larger count is
        # normal and does not mean a tool over-reported.
        if seeded.get(q):
            lines.append(
                "    seeded matches: {0} (a floor; the generated tree can hold more)".format(seeded[q])
            )
    lines.append("")
    lines.append("The result column below is a match count for Q1-Q3 and Q5, and a byte total")
    lines.append("for Q4. Tools reach the same question differently (find -name vs a trigram")
    lines.append("search vs an SQL index), so see capability-matrix.md for the per-tool")
    lines.append("predicate used, and FAILURES.txt for why any row was skipped or failed.")
    if not params:
        lines.append("")
        lines.append("(no query_params.txt found; parameters for this run unavailable)")
    lines.append("")
    return lines


# Under this many files the walk is shorter than the time it takes to start the
# process doing it, so the elapsed columns rank start-up cost.
TIMEABLE_MIN_FILES = 100000


def summarize_scale(stats):
    """Warn, before any number is read, when the tree is too small to time."""
    try:
        count = int(stats.get("file_count", "0"))
    except ValueError:
        count = 0
    if not count or count >= TIMEABLE_MIN_FILES:
        return []
    return [
        "!! CORRECTNESS FIXTURE, NOT A BENCHMARK",
        "!! {0:,} files: process start-up outweighs the work in every elapsed".format(count),
        "!! column below. Read the result column, not the timings.",
        "",
    ]


def summarize_env(env):
    """Provenance: enough to reproduce or discredit the numbers below."""
    if not env:
        return ["(no env.txt found; run provenance unavailable)", ""]

    def get(key, default="unknown"):
        return env.get(key) or default

    lines = [
        "== RUN PROVENANCE ==",
        "{0:<16} {1}".format("when", get("timestamp")),
        "{0:<16} {1} ({2})".format("host", get("hostname"), get("os")),
        "{0:<16} {1}".format("kernel", get("kernel")),
        "{0:<16} {1}".format("libc", get("libc")),
        "{0:<16} {1}".format("compiler", get("cc")),
        "{0:<16} {1}".format("ereport commit", get("repo_commit")),
        "{0:<16} {1} cpus, reps={2}, open files={3}".format(
            "capacity", get("nproc"), get("reps"), get("nofile", "?")
        ),
        "{0:<16} {1}".format("threads", get("thread_plan", get("threads"))),
        "{0:<16} drop_caches={1} scope={2} drop_db_cache={3}".format(
            "caches",
            get("drop_caches", "0"),
            get("drop_caches_scope", "-"),
            get("drop_db_cache", "0"),
        ),
    ]
    # GUFI and Robinhood read their index, threads and credentials from a config
    # file instead of argv, so a row of theirs is only interpretable next to it.
    for label, key in (("gufi config", "gufi_config"), ("robinhood config", "rbh_config")):
        if env.get(key):
            lines.append("{0:<16} {1}".format(label, env[key]))
    fstype = env.get("cwd_fstype")
    if fstype:
        lines.append(
            "{0:<16} {1} at {2}".format(
                "results fs", fstype, get("cwd_mount", "-")
            )
        )
    if env.get("tree_fstype"):
        lines.append(
            "{0:<16} {1} at {2}".format(
                "tree fs", env["tree_fstype"], get("tree_mount", "-")
            )
        )
    if env.get("work_root"):
        # Indexes written to the filesystem being crawled compete with the walk
        # for the same spindles, so the reader needs to know which it was.
        same = env.get("work_mount") and env.get("work_mount") == env.get("tree_mount")
        lines.append(
            "{0:<16} {1} ({2} at {3}{4})".format(
                "index dir",
                env["work_root"],
                get("work_fstype", "?"),
                get("work_mount", "-"),
                ", same fs as the tree" if same else "",
            )
        )
    if env.get("tmpdir"):
        lines.append("{0:<16} {1}".format("TMPDIR", env["tmpdir"]))

    versions = [
        ("ecrawl", "version_ecrawl"),
        ("ereport", "version_ereport"),
        ("ereport_index", "version_ereport_index"),
        ("Robinhood", "version_robinhood"),
        ("MariaDB", "version_mariadb"),
        ("GUFI", "version_gufi"),
        ("XDU", "version_xdu"),
        ("find", "version_find"),
        ("fd", "version_fd"),
        ("du", "version_du"),
        ("dua", "version_dua"),
    ]
    shown = [(label, env[key]) for label, key in versions if env.get(key)]
    if shown:
        lines.append("tool versions:")
        for label, value in shown:
            lines.append("  {0:<14} {1}".format(label, value))
    lines.append("")
    return lines


def partial_marker(ok_rows):
    """Flag walks that completed but could not read every path."""
    for r in ok_rows:
        if "partial" in (r.get("notes") or ""):
            return "  # partial walk (see notes in CSV)"
    return ""


# Tools that need more than one command to produce a queryable index. Each phase
# is timed on its own, so the table above stays a record of what ran; the total
# is what compares against a one-shot indexer.
PIPELINE_PHASES = [
    ("ecrawl + ereport_index", [("ecrawl", "write"), ("ereport_index", "make")]),
    ("GUFI rollup", [("gufi", "rollup_index"), ("gufi", "rollup_step")]),
]


def pipeline_totals(groups):
    lines = []
    for label, phases in PIPELINE_PHASES:
        parts = []
        for tool, variant in phases:
            ok = [r for r in groups.get((tool, variant), []) if r.get("status") == "ok"]
            sec = [float(r["sec_per_1M_files"]) for r in ok if r.get("sec_per_1M_files")]
            mib = [float(r["mib_per_1M_files"]) for r in ok if r.get("mib_per_1M_files")]
            if not sec:
                parts = []
                break
            parts.append((variant, statistics.mean(sec), statistics.mean(mib) if mib else 0.0))
        if not parts:
            continue
        if not lines:
            lines.append("")
            lines.append("pipeline totals (tools that build in more than one command):")
        lines.append(
            "  {0:<24} {1:>10} sec/1M  {2:>10} MiB/1M   = {3}".format(
                label,
                fmt(sum(p[1] for p in parts)),
                fmt(sum(p[2] for p in parts)),
                " + ".join(
                    "{0} {1} sec/1M".format(name, fmt(sec)) for name, sec, _ in parts
                ),
            )
        )
    return lines


def summarize_index(rows):
    lines = [
        "== INDEX BUILD (paper units: sec/1M files, MiB/1M files) ==",
        f"{'tool':<16} {'variant':<12} {'n':>3} {'status':<8} {'elapsed_s':>12} {'±':>8} "
        f"{'sec/1M':>10} {'MiB/1M':>10} {'index_MiB':>10}",
        "-" * 100,
    ]
    groups = defaultdict(list)
    for r in rows:
        groups[(r["tool"], r["variant"])].append(r)

    for (tool, variant), rs in sorted(groups.items()):
        ok = [r for r in rs if r.get("status") == "ok"]
        skipped = [r for r in rs if r.get("status") == "skipped"]
        failed = [r for r in rs if r.get("status") == "fail"]
        if ok:
            el = [float(r["elapsed_sec"]) for r in ok if r.get("elapsed_sec")]
            sec = [float(r["sec_per_1M_files"]) for r in ok if r.get("sec_per_1M_files")]
            mib = [float(r["mib_per_1M_files"]) for r in ok if r.get("mib_per_1M_files")]
            nbytes = [int(r["index_bytes"]) for r in ok if r.get("index_bytes")]
            m_el, s_el = mean_std(el)
            m_sec, _ = mean_std(sec)
            m_mib, _ = mean_std(mib)
            idx_mib = (statistics.mean(nbytes) / (1024 * 1024)) if nbytes else None
            lines.append(
                f"{tool:<16} {variant:<12} {len(ok):>3} {'ok':<8} {fmt(m_el):>12} {fmt(s_el):>8} "
                f"{fmt(m_sec):>10} {fmt(m_mib):>10} {fmt(idx_mib, 2):>10}"
                f"{partial_marker(ok)}"
            )
        elif skipped:
            note = skipped[0].get("notes", "")
            lines.append(f"{tool:<16} {variant:<12} {len(skipped):>3} {'skipped':<8} {'-':>12} {'-':>8} "
                         f"{'-':>10} {'-':>10} {'-':>10}  # {note}")
        elif failed:
            lines.append(f"{tool:<16} {variant:<12} {len(failed):>3} {'fail':<8} {'-':>12} {'-':>8} "
                         f"{'-':>10} {'-':>10} {'-':>10}")

    lines.extend(pipeline_totals(groups))

    # Rows with index_MiB 0 walked the tree and stored nothing, so they measure
    # traversal alone and are not indexers. Keeping them in one table makes the
    # cost of capture visible; Figure 1 plots only the indexers.
    walkers = sorted(
        "{0}/{1}".format(tool, variant)
        for (tool, variant) in groups
        if variant in ("walk", "nowrite")
    )
    if walkers:
        lines.append("")
        lines.append("walk-only rows (store nothing, so index_MiB is 0): " + ", ".join(walkers))
        lines.append("  These measure traversal alone. ecrawl/nowrite is the like-for-like")
        lines.append("  comparison against find, fd, du and dua; ecrawl/write is that same walk")
        lines.append("  plus writing the capture, so the difference is the cost of storing it.")
    return lines


# Q4 sums bytes, and the tools disagree slightly on which inodes to count.
# A fraction of a percent is that; more is a different answer.
BYTES_TOLERANCE = 0.005
REFERENCE_TOOLS = {
    "Q1": ["find", "fd"],
    "Q2": ["find", "fd"],
    "Q3": ["find", "fd"],
    "Q4": ["du", "dua"],
    "Q5": ["find", "fd"],
}


def query_references(rows):
    """The answer each query should have, per the tool the paper treats as truth."""
    answers = {}
    for r in rows:
        if r.get("status") != "ok":
            continue
        try:
            answers[(r["tool"], r["query"])] = int(r["result_count"])
        except (KeyError, ValueError, TypeError):
            continue
    reference = {}
    for query, tools in REFERENCE_TOOLS.items():
        for tool in tools:
            if (tool, query) in answers:
                reference[query] = (tool, answers[(tool, query)])
                break
    return reference


def answer_marker(tool, query, count_text, reference):
    """Flag a tool that was fast because it answered something else entirely."""
    expected = reference.get(query)
    if not expected or expected[0] == tool:
        return ""
    try:
        value = int(count_text)
    except (TypeError, ValueError):
        return ""
    ref_tool, ref_value = expected
    if query == "Q4":
        if abs(value - ref_value) <= max(1.0, abs(ref_value) * BYTES_TOLERANCE):
            return ""
    elif value == ref_value:
        return ""
    return "  # DISAGREES: {} reports {:,}".format(ref_tool, ref_value)


def summarize_queries(rows):
    lines = [
        "",
        "== QUERIES Q1–Q5 (mean elapsed_sec ± pstdev over reps; result_count from last ok) ==",
        f"{'tool':<16} {'query':<4} {'n':>3} {'status':<8} {'elapsed_s':>12} {'±':>8} {'result':>12}",
        "-" * 72,
    ]
    groups = defaultdict(list)
    for r in rows:
        groups[(r["tool"], r["query"])].append(r)
    reference = query_references(rows)

    disagreements = 0
    for (tool, query), rs in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        ok = [r for r in rs if r.get("status") == "ok"]
        skipped = [r for r in rs if r.get("status") == "skipped"]
        failed = [r for r in rs if r.get("status") == "fail"]
        if ok:
            el = [float(r["elapsed_sec"]) for r in ok if r.get("elapsed_sec")]
            m_el, s_el = mean_std(el)
            last_count = ok[-1].get("result_count", "")
            marker = answer_marker(tool, query, last_count, reference)
            disagreements += 1 if marker else 0
            lines.append(
                f"{tool:<16} {query:<4} {len(ok):>3} {'ok':<8} {fmt(m_el, 4):>12} "
                f"{fmt(s_el, 4):>8} {last_count:>12}{partial_marker(ok)}{marker}"
            )
        elif skipped:
            note = skipped[0].get("notes", "")
            lines.append(f"{tool:<16} {query:<4} {len(skipped):>3} {'skipped':<8} {'-':>12} {'-':>8} {'-':>12}  # {note}")
        elif failed:
            lines.append(f"{tool:<16} {query:<4} {len(failed):>3} {'fail':<8} {'-':>12} {'-':>8} {'-':>12}")

    if disagreements:
        lines.append("")
        lines.append(
            "{} row(s) marked DISAGREES returned a different answer than the reference".format(
                disagreements
            )
        )
        lines.append(
            "  tool (find for Q1-Q3 and Q5, du for Q4). Their timings are not comparable:"
        )
        lines.append(
            "  a query that returns nothing is fast for reasons that have nothing to do"
        )
        lines.append("  with the index. Charts hatch these bars for the same reason.")
    return lines


STDERR_TAIL_LINES = 12


def find_stderr(row_dir, tool, label, rep):
    """Locate a run's captured stderr within the directory that produced the row.

    Names differ by phase (`_r1.stderr.txt` for index runs, `_r1.err.txt` for
    queries) and some tools omit the variant, so try exact names first and only
    then a glob that still pins tool, label and rep -- a looser glob happily
    matches another phase's file and sends you debugging the wrong command.
    """
    if not row_dir:
        return None
    base = Path(row_dir)
    if not base.is_dir():
        return None
    candidates = [
        "{0}_{1}_r{2}.stderr.txt".format(tool, label, rep),
        "{0}_{1}_r{2}.err.txt".format(tool, label, rep),
        "{0}_r{1}.stderr.txt".format(tool, rep),
        "{0}_r{1}.err.txt".format(tool, rep),
    ]
    for name in candidates:
        p = base / name
        if p.is_file() and p.stat().st_size > 0:
            return p
    for p in sorted(base.glob("{0}*{1}*_r{2}*err*.txt".format(tool, label, rep))):
        if p.is_file() and p.stat().st_size > 0:
            return p
    return None


def tail_lines(path, count=STDERR_TAIL_LINES):
    try:
        with path.open(errors="replace") as f:
            kept = f.read().splitlines()
    except OSError as exc:
        return ["(could not read {0}: {1})".format(path, exc)]
    kept = [line for line in kept if line.strip()]
    return kept[-count:]


# A row that did not finish ok is one of four different things, and only the
# first is a defect in the run. Lumping them together made an install problem
# read like a tool limitation and a tool limitation read like a bug.
WRONG = "wrong"
BROKEN = "broken"
REFUSED = "refused"
CANNOT = "cannot"
ABSENT = "absent"

CATEGORY_TITLES = [
    (
        WRONG,
        "WRONG ANSWER",
        "The tool ran, exited 0, and returned something other than what the reference"
        "\ntool found. Read this before FAILED: nothing else in the run marks these rows,"
        "\nand a query that answers nothing is fast for reasons unrelated to its index.",
    ),
    (BROKEN, "FAILED", "The tool ran and returned an error. This is the list to act on."),
    (
        REFUSED,
        "PREDICATE REFUSED BY THIS BUILD",
        "The probe pass asked first and the tool rejected the query's shape, so it was"
        "\nnot run. A parity gap of the installed build, not of the tool in principle.",
    ),
    (
        CANNOT,
        "CANNOT EXPRESS THE QUERY",
        "By design: the tool has no primitive for this question (du cannot search, find"
        "\ncannot aggregate). Expected empty cells in the paper's table too.",
    ),
    (
        ABSENT,
        "NOT AVAILABLE IN THIS RUN",
        "Not installed, not configured, or its database was unreachable. Fix the setup"
        "\nand these rows come back; nothing here says anything about the tool.",
    ),
]

# Section titles read as headings; the tally reads as a sentence, and only this
# one is a countable noun.
TALLY_LABELS = {WRONG: ("wrong answer", "wrong answers")}

# Matched against the harness note, in order.
CANNOT_MARKERS = (
    "no_aggregate_primitive",
    "no_search_predicates",
    "totals_bytes_only",
    "has_no_du_equivalent",
    "_lacks_",
    "no_such_predicate",
)
ABSENT_MARKERS = (
    "not_found",
    "not_installed",
    "missing",
    "need_",
    "no_robinhood_config",
    "mariadb_",
    "no_gufi_config",
    "no_mysql_client",
    "credentials",
    "outside_the_indexed_tree",
    "tree_mismatch",
    "db_down",
    # Both are gaps in this setup rather than in the tool: an xdu built without
    # --apparent-size indexes allocated blocks, and a Robinhood database has no
    # tables until something scans with --alter-db. A build or a scan brings the
    # rows back, which is what this section means.
    "index_holds_st_blocks",
    "database_has_no_schema",
)


def classify_row(status, note):
    """Which of the four kinds of not-ok this row is."""
    if status == "fail":
        return BROKEN
    low = (note or "").lower()
    if "probe_exit=" in low:
        return REFUSED
    for marker in CANNOT_MARKERS:
        if marker in low:
            return CANNOT
    for marker in ABSENT_MARKERS:
        if marker in low:
            return ABSENT
    return CANNOT


def wrong_answers(query_rows):
    """Query rows that finished ok and disagree with the reference tool.

    One entry per tool and query, matching the table: the disagreement is a
    property of the answer, not of the repetition that produced it.
    """
    reference = query_references(query_rows)
    groups = defaultdict(list)
    for r in query_rows:
        if r.get("status") == "ok":
            groups[(r.get("tool", "?"), r.get("query", "?"))].append(r)
    found = []
    for (tool, query), rs in sorted(groups.items()):
        last = rs[-1]
        count = last.get("result_count", "")
        if not answer_marker(tool, query, count, reference):
            continue
        ref_tool, ref_value = reference[query]
        found.append(
            {
                "tool": tool,
                "query": query,
                "value": count,
                "ref_tool": ref_tool,
                "ref_value": ref_value,
                "reps": [r.get("rep", "?") for r in rs],
                "note": last.get("notes", ""),
                "dir": last.get("__dir", ""),
            }
        )
    return found


def find_stdout(row_dir, tool, query, rep):
    """The output a disagreeing command produced. Its stderr is typically empty:
    it thought it succeeded, so what it printed is the only evidence."""
    if not row_dir:
        return None
    base = Path(row_dir)
    if not base.is_dir():
        return None
    for name in (
        "{0}_{1}_r{2}.out.txt".format(tool, query, rep),
        "{0}_{1}_r{2}.stdout.txt".format(tool, query, rep),
    ):
        p = base / name
        if p.is_file():
            return p
    return None


def render_wrong(entries):
    """Each disagreement as the two numbers that differ, with where to look."""
    lines = []
    for e in entries:
        lines.append("")
        lines.append(
            "  [QUERIES] {0} / {1} — ok, but wrong (rep {2})".format(
                e["tool"], e["query"], ",".join(e["reps"])
            )
        )
        lines.append(
            "    answered: {0}   {1} reports: {2:,}".format(e["value"], e["ref_tool"], e["ref_value"])
        )
        if e["note"]:
            lines.append("    note: {0}".format(e["note"]))
        out = find_stdout(e["dir"], e["tool"], e["query"], e["reps"][0])
        lines.append("    stdout: {0}".format(out if out else "none captured"))
    return lines


def status_tally(index_rows, query_rows):
    """One line for the summary: how many rows of each kind this run produced."""
    counts = defaultdict(int)
    # A disagreeing row is recorded ok, so count it once as wrong rather than
    # twice; every repetition of it, since the ok tally counts repetitions too.
    disagreeing = set()
    for e in wrong_answers(query_rows):
        disagreeing.add((e["tool"], e["query"]))
        counts[WRONG] += len(e["reps"])
    for rows in (index_rows, query_rows):
        for r in rows:
            status = r.get("status") or ""
            if status == "ok":
                if (r.get("tool", "?"), r.get("query", "?")) in disagreeing:
                    continue
                counts["ok"] += 1
            elif status:
                counts[classify_row(status, r.get("notes", ""))] += 1
    parts = ["{0} ok".format(counts["ok"])]
    for key, title, _ in CATEGORY_TITLES:
        n = counts[key]
        if not n:
            continue
        forms = TALLY_LABELS.get(key)
        label = title.lower() if forms is None else forms[0 if n == 1 else 1]
        parts.append("{0} {1}".format(n, label))
    return "row status: {0} — see FAILURES.txt".format(", ".join(parts))


def summarize_failures(index_rows, query_rows):
    """Every non-ok row, sorted into what kind of not-ok it is, with the note the
    harness recorded and the tool's own last words."""
    lines = [
        "Indexer comparison FAILURES AND SKIPS",
        "",
        "Every row that did not finish ok, plus every row that finished ok with the",
        "wrong answer, grouped by kind. Read WRONG ANSWER and FAILED first: the last",
        "three sections are things the run could not ask, not things that broke.",
        "",
    ]
    groups = [("INDEX", index_rows, "variant"), ("QUERIES", query_rows, "query")]
    buckets = defaultdict(list)
    for title, rows, label_key in groups:
        for r in rows:
            status = r.get("status") or ""
            if status in ("ok", ""):
                continue
            note = r.get("notes", "")
            buckets[classify_row(status, note)].append((title, label_key, r))
    wrong = wrong_answers(query_rows)

    total = 0
    for key, title, blurb in CATEGORY_TITLES:
        rows = wrong if key == WRONG else buckets.get(key, [])
        # Every section counts rows and then groups the repetitions of each into
        # one entry, so a disagreement seen in three reps is three rows here too.
        count = sum(len(e["reps"]) for e in wrong) if key == WRONG else len(rows)
        lines.append("== {0}: {1} row(s) ==".format(title, count))
        lines.append(blurb)
        if not rows:
            lines.append("  (none)")
            lines.append("")
            continue
        if key == WRONG:
            total += len(rows)
            lines.extend(render_wrong(rows))
            lines.append("")
            continue
        # One entry per phase/tool/label/note, listing which reps hit it, since a
        # 3-rep run otherwise repeats the same stderr three times.
        seen = defaultdict(list)
        row_dirs = {}
        for phase, label_key, r in rows:
            entry = (phase, r.get("tool", "?"), r.get(label_key, "?"), r.get("status", "?"), r.get("notes", ""))
            seen[entry].append(r.get("rep", "?"))
            row_dirs.setdefault(entry, r.get("__dir", ""))
        for entry in sorted(seen):
            phase, tool, label, status, note = entry
            total += 1
            reps = seen[entry]
            lines.append("")
            lines.append("  [{0}] {1} / {2} — {3} (rep {4})".format(phase, tool, label, status, ",".join(reps)))
            if note:
                lines.append("    note: {0}".format(note))
            err = find_stderr(row_dirs.get(entry, ""), tool, label, reps[0])
            if err is None:
                lines.append("    stderr: none captured")
                continue
            lines.append("    stderr: {0}".format(err))
            for line in tail_lines(err):
                lines.append("      | {0}".format(line))
        lines.append("")
    if total == 0:
        lines.append("Nothing failed, was skipped, or came back with the wrong answer.")
    return lines


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results", nargs="+", type=Path, help="results directories from run_index/run_queries")
    ap.add_argument("--out", type=Path, default=None, help="output SUMMARY_TABLE.txt path")
    ap.add_argument("--failures-out", type=Path, default=None, help="output FAILURES.txt path")
    args = ap.parse_args()

    dirs = [d.resolve() for d in args.results]
    index_rows = []
    query_rows = []
    for d in dirs:
        for p in [d / "index_results.csv", d / "query_results.csv"]:
            if p.name == "index_results.csv" and p.is_file():
                index_rows.extend(load_csv(p))
            if p.name == "query_results.csv" and p.is_file():
                query_rows.extend(load_csv(p))
        # Also accept the path being the csv itself
        if d.is_file() and d.name == "index_results.csv":
            index_rows.extend(load_csv(d))
        if d.is_file() and d.name == "query_results.csv":
            query_rows.extend(load_csv(d))

    lines = [
        "Indexer comparison SUMMARY TABLE",
        f"sources: {' '.join(str(p) for p in args.results)}",
        "",
    ]
    lines.extend(summarize_scale(load_kv(dirs, "tree_stats.txt")))
    lines.extend(summarize_env(load_env(dirs)))
    if index_rows:
        lines.extend(summarize_index(index_rows))
    else:
        lines.append("(no index_results.csv found)")
    if query_rows:
        lines.append("")
        lines.extend(summarize_query_defs(load_kv(dirs, "query_params.txt")))
        lines.extend(summarize_queries(query_rows))
    else:
        lines.append("")
        lines.append("(no query_results.csv found)")

    if index_rows or query_rows:
        lines.append("")
        lines.append(status_tally(index_rows, query_rows))

    text = "\n".join(lines) + "\n"
    out = args.out
    if out is None:
        base = args.results[0]
        out = base / "SUMMARY_TABLE.txt" if base.is_dir() else base.parent / "SUMMARY_TABLE.txt"
    out.write_text(text)
    print(text)
    print(f"wrote {out}")

    fail_text = "\n".join(summarize_failures(index_rows, query_rows)) + "\n"
    fail_out = args.failures_out or out.parent / "FAILURES.txt"
    fail_out.write_text(fail_text)
    print(f"wrote {fail_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
