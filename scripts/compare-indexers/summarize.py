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


# What each of the paper's queries asks, independent of tool. Q6 is not the
# paper's: it asks for a substring with nothing anchored at either end, which is
# the shape a B-tree on names cannot seek and a trigram index can. Marked extra
# everywhere it appears so it is never read as part of the reproduction.
QUERY_DOC = [
    ("Q1", "exact-name lookup", "find the one file with a given unique basename"),
    ("Q2", "glob search", "find files whose name matches a wildcard pattern"),
    ("Q3", "size filter", "find regular files larger than a byte threshold"),
    ("Q4", "subtree aggregate", "total bytes below one subtree (du -sb equivalent)"),
    ("Q5", "subtree file count", "count regular files below one subtree"),
    ("Q6", "substring search [extra]", "find files whose name contains a token anywhere"),
]
EXTRA_QUERIES = {"Q6"}


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


def arg_sets_used(query_rows):
    """Which argument sets were actually asked, read off the rows rather than the
    seed manifest: a tree can hold three and a cold-only run ask one, and the
    tables should describe the questions that were put."""
    seen = set()
    for row in query_rows or ():
        value = (row.get("arg_set") or "").strip()
        if not value:
            continue
        try:
            seen.add(int(value))
        except ValueError:
            continue
    return sorted(seen) or [1]


def query_arguments(params, set_no):
    """What each query actually asked, for one argument set. Keys are written
    both plain (set 1) and suffixed, so a one-set run reads the same as before."""
    def get(key):
        return params.get("{0}_{1}".format(key, set_no)) or (
            params.get(key) if set_no == 1 else None
        )

    detail = {}
    seeded = {}
    name = get("q1_name")
    if name:
        detail["Q1"] = "basename '{0}'".format(name)
    glob = get("q2_glob")
    if glob:
        note = "pattern '{0}'".format(glob)
        term = get("q2_index_term") or get("q2_term")
        if term:
            note += ", trigram term '{0}'".format(term)
        filt = get("q2_exact_filter")
        if filt:
            note += ", index matches narrowed to the glob with regex {0}".format(filt)
        detail["Q2"] = note
        seeded["Q2"] = get("q2_expected")
    min_bytes = get("q3_min_bytes")
    if min_bytes:
        pretty = human_bytes(min_bytes)
        note = "larger than {0} bytes".format(min_bytes)
        if pretty:
            note += " ({0})".format(pretty)
        detail["Q3"] = note
        seeded["Q3"] = get("q3_expected")
    for key, q in (("q4_subtree", "Q4"), ("q5_subtree", "Q5")):
        subtree = get(key)
        if subtree:
            detail[q] = subtree
    glob6 = get("q6_glob")
    if glob6:
        note = "pattern '{0}'".format(glob6)
        term = get("q6_index_term")
        if term:
            note += ", trigram term '{0}'".format(term)
        detail["Q6"] = note
        seeded["Q6"] = get("q6_expected")
    return detail, seeded


def summarize_query_defs(params, sets, queries=None):
    """Explain the queries this run asked, and what each one ran with."""
    lines = ["== QUERY DEFINITIONS ==", ""]
    per_set = [(n, query_arguments(params, n)) for n in sets] if params else []
    asked = queries if queries else {q for q, _, _ in QUERY_DOC}

    for q, title, what in QUERY_DOC:
        if q not in asked:
            continue
        lines.append("{0}  {1:<26} {2}".format(q, title, what))
        for n, (detail, seeded) in per_set:
            if q not in detail:
                continue
            label = "ran with" if len(sets) == 1 else "set {0}".format(n)
            lines.append("    {0}: {1}".format(label, detail[q]))
            # A floor, not an expected result: prepare-synth.sh plants these, but
            # the generator creates matching files of its own, so a larger count
            # is normal and does not mean a tool over-reported.
            if seeded.get(q):
                lines.append(
                    "      seeded matches: {0} (a floor; the generated tree can hold more)".format(
                        seeded[q]
                    )
                )
    lines.append("")
    if len(sets) > 1:
        lines.append(
            "{0} argument sets: the cold pass asks set 1, then the hot passes ask set 1".format(
                len(sets)
            )
        )
        lines.append(
            "again -- so the cache delta is measured on identical work -- and then the"
        )
        lines.append(
            "remaining sets, so no hot number is a second answer to a question just asked."
        )
        lines.append(
            "Each row below pools its sets, and its result column shows the last set's"
        )
        lines.append(
            "answer; correctness is still checked set by set, against that set's own"
        )
        lines.append("reference.")
        lines.append("")
    counted = [q for q in ("Q1", "Q2", "Q3", "Q5", "Q6") if q in asked]
    if len(counted) > 1:
        counted = "{0} and {1}".format(", ".join(counted[:-1]), counted[-1])
    else:
        counted = counted[0] if counted else "the search queries"
    lines.append(
        "The result column below is a match count for {0}, and a byte".format(counted)
    )
    lines.append("total for Q4. Tools reach the same question differently (find -name vs a")
    lines.append("trigram search vs an SQL index), so see capability-matrix.md for the per-tool")
    lines.append("predicate used, and FAILURES.txt for why any row was skipped or failed.")
    lines.append("")
    lines.append("Both columns are pinned to one definition, or the answers would not compare:")
    lines.append("Q4 is apparent bytes including directory inodes, which is du -sb, and a match")
    lines.append("count is per name, so a file with four hard links counts four times. A tool")
    lines.append("whose index answers a different question -- disk usage instead of apparent")
    lines.append("bytes, one row per inode instead of per name -- is marked DISAGREES with the")
    lines.append("reason beside it. Nothing is skipped for it.")
    if "Q6" in asked:
        lines.append("Q6 is this comparison's own addition, not one of the paper's five.")
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


def reps_note(env):
    """The repetition count, naming the tools that do not share it.

    Reps are per tool, so a bare "reps=3" beside a gufi row sampled once is a
    claim the run did not make.
    """
    reps = env.get("reps") or "?"
    detail = (env.get("reps_per_tool") or "").split()
    if not detail:
        return reps
    return "{0} ({1})".format(reps, ", ".join(detail))


def summarize_env(env, arg_sets=1):
    """Provenance: enough to reproduce or discredit the numbers below."""
    if not env:
        return ["(no env.txt found; run provenance unavailable)", ""]

    def get(key, default="unknown"):
        return env.get(key) or default

    # Which passes each repetition made. A CSV from before the cold/hot split has
    # one pass and no record of its cache state, which is not the same claim as
    # "cold", so it does not get to make it. The argument-set count comes from the
    # rows rather than env.txt: the index phase never reads the seed manifest, so
    # its copy of the number is the default and not what the queries asked.
    if env.get("cache_modes"):
        passes = "{0} per rep, {1} query argument set(s)".format(
            env["cache_modes"], arg_sets
        )
    else:
        passes = "one per rep (cache state not recorded), {0} query argument set(s)".format(
            arg_sets
        )

    lines = [
        "== RUN PROVENANCE ==",
        "{0:<16} {1}".format("when", get("timestamp")),
        "{0:<16} {1} ({2})".format("host", get("hostname"), get("os")),
        "{0:<16} {1}".format("kernel", get("kernel")),
        "{0:<16} {1}".format("libc", get("libc")),
        "{0:<16} {1}".format("compiler", get("cc")),
        "{0:<16} {1}".format("ereport commit", get("repo_commit")),
        "{0:<16} {1} cpus, reps={2}, open files={3}".format(
            "capacity", get("nproc"), reps_note(env), get("nofile", "?")
        ),
        "{0:<16} {1}".format("threads", get("thread_plan", get("threads"))),
        "{0:<16} drop_caches={1} scope={2} drop_db_cache={3}".format(
            "caches",
            get("drop_caches", "0"),
            get("drop_caches_scope", "-"),
            get("drop_db_cache", "0"),
        ),
        "{0:<16} {1}".format("passes", passes),
    ]
    if env.get("rbh_datadir"):
        # Robinhood's index is MariaDB's data directory. On the storage under
        # test it is comparable with the other tools' indexes; on the operating
        # system's disk it is not, and this is the line that says which.
        lines.append(
            "{0:<16} {1} ({2} at {3})".format(
                "robinhood data",
                env["rbh_datadir"],
                get("rbh_datadir_fstype", "?"),
                get("rbh_datadir_mount", "-"),
            )
        )
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
        ("dut", "version_dut"),
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
#
# gufi_dir2index runs twice per repetition -- the same command into two
# directories, so the rollup pass has its own copy to chew on -- and pooling
# both variants here is what keeps GUFI to one build row per pipeline instead
# of three rows a reader might add up.
GUFI_DIR2INDEX = [("gufi", "plain"), ("gufi", "rollup_index")]
PIPELINE_PHASES = [
    (
        "ecrawl + ereport_index",
        [("crawl", [("ecrawl", "write")]), ("trigram index", [("ereport_index", "make")])],
    ),
    ("GUFI (dir2index)", [("dir2index", GUFI_DIR2INDEX)]),
    (
        "GUFI + rollup",
        [("dir2index", GUFI_DIR2INDEX), ("rollup", [("gufi", "rollup_step")])],
    ),
    # The scan fills index-free tables, which is not a database anyone queries,
    # so what Robinhood costs is the scan plus the three CREATE INDEX statements.
    (
        "robinhood (scan + indexes)",
        [("scan", [("robinhood", "scan")]), ("indexes", [("robinhood", "indexes")])],
    ),
]

# Order the passes appear in, rather than alphabetically: cold is what the work
# costs against untouched storage, hot what it costs with the metadata already
# in memory, and warm is a cold pass the run could not actually drop caches for.
CACHE_ORDER = {"cold": 0, "warm": 1, "hot": 2}


def cache_key(name):
    return (CACHE_ORDER.get(name, 3), name)


def cache_states(rows):
    """Which passes this run made, in reading order. Empty string for a CSV
    written before the column existed, which keeps one unlabelled series."""
    return sorted({r.get("cache") or "" for r in rows}, key=cache_key)


def phase_rows(groups, keys):
    ok = []
    for key in keys:
        ok.extend(r for r in groups.get(key, []) if r.get("status") == "ok")
    return ok


def file_count(rows):
    """How many files the run walked. One number for the whole run: every tool
    was pointed at the same tree."""
    for r in rows:
        try:
            count = int(r.get("file_count") or 0)
        except ValueError:
            continue
        if count > 0:
            return count
    return 0


def pipeline_totals(rows):
    """One total per pipeline per pass. Averaging a cold build together with a hot
    one would report a number neither of them measured."""
    lines = []
    count = file_count(rows)
    shown = set()
    for cache in cache_states(rows):
        groups = defaultdict(list)
        for r in rows:
            if (r.get("cache") or "") == cache:
                groups[(r["tool"], r["variant"])].append(r)
        for label, phases in PIPELINE_PHASES:
            parts = []
            for name, keys in phases:
                ok = phase_rows(groups, keys)
                sec = [float(r["elapsed_sec"]) for r in ok if r.get("elapsed_sec")]
                nbytes = [int(r["index_bytes"]) for r in ok if r.get("index_bytes")]
                if not sec:
                    parts = []
                    break
                size = statistics.mean(nbytes) / (1024 * 1024) if nbytes else 0.0
                parts.append((name, statistics.mean(sec), size))
            if not parts:
                continue
            if not lines:
                lines.append("")
                lines.append("pipeline totals (tools that build in more than one command):")
            shown.add(label)
            total = sum(p[1] for p in parts)
            lines.append(
                "  {0:<26} {1:<5} {2:>10} s  {3:>12} files/s  {4:>10} MiB   = {5}".format(
                    label,
                    cache or "-",
                    fmt(total),
                    fmt(count / total, 0) if count else "-",
                    fmt(sum(p[2] for p in parts), 2),
                    " + ".join("{0} {1} s".format(name, fmt(sec)) for name, sec, _ in parts),
                )
            )
    # Each caveat only where it applies: a run without GUFI, or one from before
    # Robinhood's indexes were timed, should not be told how to read rows it does
    # not have.
    if any(label.startswith("GUFI") for label in shown):
        lines.append("  GUFI's two rows are alternative builds, not stages: rollup is a second")
        lines.append("  pass over the index dir2index just wrote. Do not add them together.")
    if any(label.startswith("Robinhood") for label in shown):
        lines.append("  Robinhood's two are stages: the scan crawls into index-free tables and")
        lines.append("  the indexes are what make them queryable, so both are what it costs.")
    return lines


def summarize_index(rows):
    lines = [
        "== INDEX BUILD ==",
        f"{'tool':<16} {'variant':<12} {'cache':<5} {'n':>3} {'status':<8} {'elapsed_s':>12} {'±':>8} "
        f"{'files/s':>12} {'index_MiB':>12} {'bytes/file':>10}",
        "-" * 106,
    ]
    # Cache state is part of the key, not something averaged away: a cold build
    # and a hot one are two measurements of two different situations.
    groups = defaultdict(list)
    for r in rows:
        groups[(r["tool"], r["variant"], r.get("cache") or "")].append(r)
    count = file_count(rows)

    for (tool, variant, cache), rs in sorted(
        groups.items(), key=lambda x: (x[0][0], x[0][1], cache_key(x[0][2]))
    ):
        label = cache or "-"
        ok = [r for r in rs if r.get("status") == "ok"]
        skipped = [r for r in rs if r.get("status") == "skipped"]
        failed = [r for r in rs if r.get("status") == "fail"]
        if ok:
            el = [float(r["elapsed_sec"]) for r in ok if r.get("elapsed_sec")]
            nbytes = [int(r["index_bytes"]) for r in ok if r.get("index_bytes")]
            m_el, s_el = mean_std(el)
            rate = count / m_el if count and m_el else None
            idx_mib = (statistics.mean(nbytes) / (1024 * 1024)) if nbytes else None
            per_file = (
                statistics.mean(nbytes) / count if nbytes and count else None
            )
            lines.append(
                f"{tool:<16} {variant:<12} {label:<5} {len(ok):>3} {'ok':<8} {fmt(m_el):>12} {fmt(s_el):>8} "
                f"{fmt(rate, 0):>12} {fmt(idx_mib, 2):>12} {fmt(per_file, 0):>10}"
                f"{partial_marker(ok)}"
            )
        elif skipped:
            note = skipped[0].get("notes", "")
            lines.append(f"{tool:<16} {variant:<12} {label:<5} {len(skipped):>3} {'skipped':<8} {'-':>12} {'-':>8} "
                         f"{'-':>12} {'-':>12} {'-':>10}  # {note}")
        elif failed:
            lines.append(f"{tool:<16} {variant:<12} {label:<5} {len(failed):>3} {'fail':<8} {'-':>12} {'-':>8} "
                         f"{'-':>12} {'-':>12} {'-':>10}")

    lines.extend(pipeline_totals(rows))

    # Rows with index_MiB 0 walked the tree and stored nothing, so they measure
    # traversal alone and are not indexers. Keeping them in one table is what
    # makes the cost of the capture visible: the charts put the walkers and the
    # indexers on separate figures, so this is the only place the gap between
    # ecrawl's two rows can be read directly.
    walkers = sorted(
        {
            "{0}/{1}".format(tool, variant)
            for (tool, variant, _cache) in groups
            if variant in ("walk", "nowrite")
        }
    )
    if walkers:
        lines.append("")
        lines.append("walk-only rows (store nothing, so index_MiB is 0): " + ", ".join(walkers))
        lines.append("  These measure traversal alone. ecrawl/nowrite is the like-for-like")
        lines.append("  comparison against find, fd, du, dua and dut; ecrawl/write is that same")
        lines.append("  walk plus writing the capture, so the difference is the cost of storing it.")
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
    "Q6": ["find", "fd"],
}


def row_arg_set(row):
    """Which argument set a row answered. Blank in a CSV written before the sets
    existed, which is set 1 by definition."""
    return (row.get("arg_set") or "1").strip() or "1"


def query_references(rows):
    """The answer each query should have, per the tool the paper treats as truth.

    Keyed by argument set as well as query: set 2 asks for a different name and
    a different threshold, so checking one set's answer against another's would
    report every tool as disagreeing.
    """
    answers = {}
    for r in rows:
        if r.get("status") != "ok":
            continue
        try:
            answers[(r["tool"], r["query"], row_arg_set(r))] = int(r["result_count"])
        except (KeyError, ValueError, TypeError):
            continue
    sets = {row_arg_set(r) for r in rows} or {"1"}
    reference = {}
    for query, tools in REFERENCE_TOOLS.items():
        for arg_set in sets:
            for tool in tools:
                if (tool, query, arg_set) in answers:
                    reference[(query, arg_set)] = (tool, answers[(tool, query, arg_set)])
                    break
    return reference


# Why a tool's number differs from the reference when the difference is by
# definition and not by defect. None of these rows are skipped or excused: the
# tool answered the question its index can answer, the number is right for that
# question, and this says which question it was. README.md, "Why the tools
# disagree on Q3 and Q4", has the measurements behind each line.
ANSWER_SEMANTICS = {
    # The paper's Q4 is "disk usage of large subdirectory" and rbh-du answers
    # exactly that: allocated blocks. This harness asks for apparent bytes so
    # every tool answers one question, and the seeded subtree is sparse by
    # construction, so the two differ by four orders of magnitude.
    ("robinhood", "Q4"): "rbh-du reports disk usage, not apparent bytes; the subtree is sparse",
    # gufi_du and xdu-find total file records. du -sb also counts the apparent
    # size of every directory inode, which is the whole answer on a subtree
    # whose files are empty.
    ("gufi", "Q4"): "totals file records; du -sb also counts directory inodes",
    ("gufi_rollup", "Q4"): "totals file records; du -sb also counts directory inodes",
    ("xdu", "Q4"): "totals file records; du -sb also counts directory inodes",
}
# Robinhood's ENTRIES is keyed by inode, so every count it gives is per inode
# where find's is per name.
for _query in ("Q1", "Q2", "Q3", "Q5", "Q6"):
    ANSWER_SEMANTICS[("robinhood", _query)] = (
        "one row per inode, so hard links are counted once and find counts each name"
    )


def answer_marker(tool, query, arg_set, count_text, reference):
    """Flag a tool that was fast because it answered something else entirely."""
    expected = reference.get((query, arg_set))
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


def gufi_query_indexes(dirs):
    """The GUFI indexes the run built. There are two -- what gufi_dir2index wrote
    and what gufi_rollup made of it -- and they cost very different amounts, so a
    query time means nothing until you know which one answered it."""
    found = {}
    for d in dirs:
        base = d if d.is_dir() else d.parent
        env = load_kv([base], "env.txt")
        for label, key in (("gufi", "gufi_index_root"), ("gufi_rollup", "gufi_rollup_index_root")):
            if env.get(key) and label not in found:
                found[label] = env[key]
    return found


def summarize_queries(rows, gufi_indexes=None):
    gufi_indexes = gufi_indexes or {}
    # A results directory from before Q6 existed has five queries, and titling its
    # table with six invites a hunt for a row that was never run.
    queries = {r.get("query") for r in rows}
    span = "Q1–Q6" if "Q6" in queries else "Q1–Q5"
    lines = [
        "",
        "== QUERIES {0} (mean elapsed_sec ± pstdev over reps and argument sets) ==".format(span),
        f"{'tool':<16} {'query':<4} {'cache':<5} {'n':>3} {'status':<8} {'elapsed_s':>12} {'±':>8} {'result':>12}",
        "-" * 80,
    ]
    groups = defaultdict(list)
    for r in rows:
        groups[(r["tool"], r["query"], r.get("cache") or "")].append(r)
    reference = query_references(rows)

    disagreements = 0
    explained = []
    for (tool, query, cache), rs in sorted(
        groups.items(), key=lambda x: (x[0][0], x[0][1], cache_key(x[0][2]))
    ):
        label = cache or "-"
        ok = [r for r in rs if r.get("status") == "ok"]
        skipped = [r for r in rs if r.get("status") == "skipped"]
        failed = [r for r in rs if r.get("status") == "fail"]
        if ok:
            el = [float(r["elapsed_sec"]) for r in ok if r.get("elapsed_sec")]
            m_el, s_el = mean_std(el)
            last = ok[-1]
            last_count = last.get("result_count", "")
            # Each set has its own right answer, so every row is checked against
            # the reference for the set it asked. One marker per group: they are
            # either all the same answer or the group is already suspect.
            marker = ""
            for r in ok:
                marker = answer_marker(
                    tool, query, row_arg_set(r), r.get("result_count", ""), reference
                )
                if marker:
                    last_count = r.get("result_count", "")
                    break
            disagreements += 1 if marker else 0
            if marker and (tool, query) in ANSWER_SEMANTICS:
                pair = (tool, query)
                if pair not in explained:
                    explained.append(pair)
            lines.append(
                f"{tool:<16} {query:<4} {label:<5} {len(ok):>3} {'ok':<8} {fmt(m_el, 4):>12} "
                f"{fmt(s_el, 4):>8} {last_count:>12}{partial_marker(ok)}{marker}"
            )
        elif skipped:
            note = skipped[0].get("notes", "")
            lines.append(f"{tool:<16} {query:<4} {label:<5} {len(skipped):>3} {'skipped':<8} {'-':>12} {'-':>8} {'-':>12}  # {note}")
        elif failed:
            lines.append(f"{tool:<16} {query:<4} {label:<5} {len(failed):>3} {'fail':<8} {'-':>12} {'-':>8} {'-':>12}")

    if disagreements:
        lines.append("")
        lines.append(
            "{} row(s) marked DISAGREES returned a different answer than the reference".format(
                disagreements
            )
        )
        lines.append(
            "  tool for the same argument set (find for Q1-Q3, Q5 and Q6, du for Q4)."
        )
        lines.append(
            "  Their timings are not comparable: a query that returns nothing is fast"
        )
        lines.append(
            "  for reasons that have nothing to do with the index. Charts hatch these"
        )
        lines.append("  bars for the same reason.")
    if explained:
        lines.append("")
        lines.append("Of those, these answer a different question rather than the same one")
        lines.append("  wrongly. The row stands as measured; the timing is of the question the")
        lines.append("  tool's index can answer, which is not the one the reference answered:")
        for tool, query in explained:
            lines.append(
                "  {0:<14} {1:<4} {2}".format(tool, query, ANSWER_SEMANTICS[(tool, query)])
            )
    if "Q6" in queries:
        lines.append("")
        lines.append("Q6 is extra, not one of the paper's five: an unanchored substring, which is")
        lines.append("  the shape a B-tree on names cannot seek into and a trigram index can.")
    if gufi_indexes.get("gufi_rollup"):
        lines.append("")
        lines.append("gufi is two series, because the run builds two indexes:")
        for label in ("gufi", "gufi_rollup"):
            if gufi_indexes.get(label):
                lines.append("  {0:<12} {1}".format(label, gufi_indexes[label]))
        lines.append("  gufi rows read what gufi_dir2index wrote; gufi_rollup rows read what")
        lines.append("  gufi_rollup then made of it, at several times the build cost. Q4 is")
        lines.append("  answered from treesummary rows only the rollup writes, so the plain")
        lines.append("  index has no Q4 at all -- that row is skipped, not fast.")
    elif gufi_indexes.get("gufi"):
        # A run from before the two series existed: one index answered everything,
        # and which one it was decides what its Q4 row means.
        name = Path(gufi_indexes["gufi"]).name
        lines.append("")
        lines.append(
            "gufi rows were answered from {0} ({1} index), not the other build.".format(
                name, "rolled-up" if "rollup" in name else "plain"
            )
        )
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


# A row that did not finish ok is one of several different things, and only the
# first few are defects in the run. Lumping them together made an install problem
# read like a tool limitation and a tool limitation read like a bug.
WRONG = "wrong"
BROKEN = "broken"
UNKNOWN = "unknown"
REFUSED = "refused"
CANNOT = "cannot"
ABSENT = "absent"

CATEGORY_TITLES = [
    (
        WRONG,
        "WRONG ANSWER",
        "The tool ran, exited 0, and returned something other than what the reference"
        "\ntool found. Read this before FAILED: nothing else in the run marks these rows,"
        "\nand a query that answers nothing is fast for reasons unrelated to its index."
        "\nA 'by definition' line means the tool answered a different question, not the"
        "\nsame one wrongly, and the timing is of that question.",
    ),
    (BROKEN, "FAILED", "The tool ran and returned an error. This is the list to act on."),
    (
        UNKNOWN,
        "UNRECOGNIZED SKIP REASON",
        "The row was skipped for a reason this summary has no marker for, so it could"
        "\nnot be sorted. Read it as needing attention and add the marker to"
        "\nsummarize.py: an unmatched note used to land in CANNOT EXPRESS THE QUERY,"
        "\nwhere a broken install reads as an expected empty cell.",
    ),
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
TALLY_LABELS = {
    WRONG: ("wrong answer", "wrong answers"),
    UNKNOWN: ("unrecognized skip reason", "unrecognized skip reasons"),
}

# Matched against the harness note, in order.
CANNOT_MARKERS = (
    "no_aggregate_primitive",
    "no_search_predicates",
    "totals_bytes_only",
    "has_no_du_equivalent",
    # dua aggregates bytes and counts entries, not regular files, so Q5 has no
    # dua answer. Unmatched, this landed in UNRECOGNIZED SKIP REASON, which reads
    # as something to fix rather than as the capability gap it is.
    "reports_sizes_not_file_counts",
    # dut's -f counts files and directories together and takes no type
    # predicate, so it cannot be asked Q5's question either. Its other four
    # skips already read as "totals_bytes_only".
    "no_type_filter",
    "_lacks_",
    "no_such_predicate",
    # The plain GUFI index cannot answer Q4: gufi_du reads treesummary rows, and
    # only gufi_rollup writes them. A capability of the expensive build, not a
    # fault of the cheap one, and the distinction is the point of the two series.
    "rollup_required",
    # A glob with no literal run of three characters gives a trigram index
    # nothing to look up, and one with a bracket expression does not translate to
    # a path regex. Both are limits of the query shape, not of the install.
    "glob_has_no_literal_run",
    "glob_does_not_translate",
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
    # A baseline that is installed but will not start: the file is gone, has
    # lost +x, or was built against another libc. lib.sh spells these
    # "<tool>_not_runnable: <the tool's own words>". Without this marker the
    # note matched nothing and fell through to CANNOT, where a dead dua read as
    # an expected empty cell for several runs before anyone noticed.
    "not_runnable",
    # Both are gaps in this setup rather than in the tool: an xdu built without
    # --apparent-size indexes allocated blocks, and a Robinhood database has no
    # tables until something scans with --alter-db. A build or a scan brings the
    # rows back, which is what this section means.
    "index_holds_st_blocks",
    "database_has_no_schema",
    # Q6's token is planted by prepare-synth.sh; a tree seeded before it existed
    # has no glob for it, and reseeding brings the row back.
    "no_q6_glob",
)


def classify_row(status, note):
    """Which kind of not-ok this row is.

    Unmatched notes are UNKNOWN rather than CANNOT. Defaulting to CANNOT meant
    every reason nobody had written a marker for claimed to be a by-design gap,
    which is the one bucket a reader is meant to skip.
    """
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
    return UNKNOWN


def wrong_answers(query_rows):
    """Query rows that finished ok and disagree with the reference tool.

    One entry per tool, query and argument set, matching the table: the
    disagreement is a property of the answer, not of the repetition that produced
    it -- but each set asks for something different, so a tool can be right about
    one and wrong about the next.
    """
    reference = query_references(query_rows)
    groups = defaultdict(list)
    for r in query_rows:
        if r.get("status") == "ok":
            groups[(r.get("tool", "?"), r.get("query", "?"), row_arg_set(r))].append(r)
    found = []
    for (tool, query, arg_set), rs in sorted(groups.items()):
        last = rs[-1]
        count = last.get("result_count", "")
        if not answer_marker(tool, query, arg_set, count, reference):
            continue
        ref_tool, ref_value = reference[(query, arg_set)]
        found.append(
            {
                "tool": tool,
                "query": query,
                "arg_set": arg_set,
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
    # Later passes carry the pass in the filename (_hot_a2), so the exact
    # names above only find the first one.
    for p in sorted(base.glob("{0}_{1}_r{2}_*.out.txt".format(tool, query, rep))):
        if p.is_file():
            return p
    return None


def render_wrong(entries):
    """Each disagreement as the two numbers that differ, with where to look."""
    lines = []
    for e in entries:
        lines.append("")
        lines.append(
            "  [QUERIES] {0} / {1} (argument set {2}) — ok, but wrong (rep {3})".format(
                e["tool"], e["query"], e["arg_set"], ",".join(e["reps"])
            )
        )
        lines.append(
            "    answered: {0}   {1} reports: {2:,}".format(e["value"], e["ref_tool"], e["ref_value"])
        )
        # Before the note, because it is the difference between "this tool is
        # broken" and "this tool was asked something else".
        why = ANSWER_SEMANTICS.get((e["tool"], e["query"]))
        if why:
            lines.append("    by definition: {0}".format(why))
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
        disagreeing.add((e["tool"], e["query"], e["arg_set"]))
        counts[WRONG] += len(e["reps"])
    for rows in (index_rows, query_rows):
        for r in rows:
            status = r.get("status") or ""
            if status == "ok":
                if (r.get("tool", "?"), r.get("query", "?"), row_arg_set(r)) in disagreeing:
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
        "wrong answer, grouped by kind. Read the first three sections first: the last",
        "three are things the run could not ask, not things that broke.",
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
    sets_used = arg_sets_used(query_rows)
    lines.extend(summarize_env(load_env(dirs), len(sets_used)))
    if index_rows:
        lines.extend(summarize_index(index_rows))
    else:
        lines.append("(no index_results.csv found)")
    if query_rows:
        lines.append("")
        lines.extend(
            summarize_query_defs(
                load_kv(dirs, "query_params.txt"),
                sets_used,
                {r.get("query") for r in query_rows},
            )
        )
        lines.extend(summarize_queries(query_rows, gufi_query_indexes(dirs)))
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
