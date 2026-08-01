#!/usr/bin/env python3
"""
Build one PDF from an ereport HTML directory (index.html + bucket_a*_s*.html).

Uses Chromium / Google Chrome headless for layout fidelity (heat map CSS, tables).
Requires a Chromium-based browser on PATH or set CHROME / CHROME_BIN.

With **pypdf** installed, the merge step rewrites heat-map bucket links on **every
page that came from index.html** (often one page; can be several if the index is long)
from external file URIs into **in-document GoTo** links to the merged bucket section.

Merge strategies:
  - Prefer pypdf (pip install pypdf): merges and rewrites heat-map bucket links on
    index pages to in-document GoTo links (same PDF as the appended bucket sections).
  - Otherwise: pdfunite (poppler-utils) or qpdf — no internal link rewrite (warning).

Examples:
  ./scripts/tools/ereport-html-to-pdf.py -i ./all_users -o report.pdf
  CHROME=/usr/bin/chromium-browser ./scripts/tools/ereport-html-to-pdf.py -i ./root -o root.pdf --omit-interactive
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    from urllib.parse import unquote  # py3
except ImportError:
    from urllib import unquote  # py2 fallback

BUCKET_RE = re.compile(r"^bucket_a(\d+)_s(\d+)\.html$", re.IGNORECASE)
# Match bucket HTML filename anywhere inside a printed link URI (file: or path).
BUCKET_URI_RE = re.compile(r"(?i)bucket_a(\d+)_s(\d+)\.html")


def die(msg, code=1):
    sys.stderr.write("ereport-html-to-pdf: %s\n" % msg)
    raise SystemExit(code)


def find_chrome():
    env = os.environ.get("CHROME") or os.environ.get("CHROME_BIN") or os.environ.get("GOOGLE_CHROME_BIN")
    if env and Path(env).is_file():
        return env
    for name in (
        "google-chrome-stable",
        "google-chrome",
        "chromium",
        "chromium-browser",
        "chrome",
    ):
        p = shutil.which(name)
        if p:
            return p
    die(
        "No Chromium-based browser found. Install Chrome/Chromium or set CHROME "
        "to the browser executable."
    )


def bucket_sort_key(path):
    m = BUCKET_RE.match(path.name)
    if not m:
        return (9999, 9999)
    return (int(m.group(1)), int(m.group(2)))


def collect_html_pages(report_dir):
    index = report_dir / "index.html"
    if not index.is_file():
        die("missing %s (expected ereport output directory)" % index)
    buckets = sorted(report_dir.glob("bucket_a*_s*.html"), key=bucket_sort_key)
    return [index] + buckets


def inject_omit_interactive(html):
    """Hide path search + drawer chrome for a cleaner static PDF."""
    inject = (
        "<style id=\"ereport-pdf-strip\">"
        ".path-search,.drawer,.drawer-backdrop,"
        "#ereport-badge-tip{display:none!important}"
        "</style>\n"
    )
    lower = html.lower()
    idx = lower.rfind("</head>")
    if idx >= 0:
        return html[:idx] + inject + html[idx:]
    return inject + html


def write_maybe_stripped(src, tmp_dir, omit_interactive):
    if not omit_interactive or src.name != "index.html":
        return src
    raw = src.read_text(encoding="utf-8", errors="replace")
    out = Path(tmp_dir) / "_ereport_pdf_index.html"
    out.write_text(inject_omit_interactive(raw), encoding="utf-8")
    return out


def print_html_to_pdf(chrome, html_path, pdf_path, extra_args):
    url = html_path.resolve().as_uri()
    cmd = [
        chrome,
        "--headless",
        "--disable-gpu",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        "--print-to-pdf=%s" % pdf_path,
        "--no-pdf-header-footer",
    ]
    cmd.extend(extra_args or [])
    cmd.append(url)
    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
    except OSError:
        die("chrome binary not executable: %s" % chrome)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or e.stdout or "").strip()
        die("print-to-pdf failed for %s: %s" % (html_path.name, err or str(e)))


def merge_pdfs_pypdf(parts, out):
    try:
        from pypdf import PdfWriter  # type: ignore
    except ImportError:
        raise RuntimeError("pypdf not installed")
    w = PdfWriter()
    for p in parts:
        w.append(str(p))
    with open(str(out), "wb") as f:
        w.write(f)
    w.close()


def _pdf_page_counts(parts):
    try:
        from pypdf import PdfReader  # type: ignore
    except ImportError:
        raise RuntimeError("pypdf not installed")
    counts = []
    for p in parts:
        r = PdfReader(str(p))
        counts.append(len(r.pages))
    return counts


def _deref_pdf_object(owner, obj):
    try:
        from pypdf.generic import IndirectObject  # type: ignore
    except ImportError:
        from PyPDF2.generic import IndirectObject  # type: ignore
    while isinstance(obj, IndirectObject):
        obj = owner.get_object(obj)
    return obj


def _uri_to_str(uri):
    if uri is None:
        return ""
    if isinstance(uri, bytes):
        return uri.decode("utf-8", errors="replace")
    return str(uri)


def _subtype_str(obj):
    if obj is None:
        return ""
    return str(obj)


def _bucket_stem_from_uri(uri_text):
    if not uri_text:
        return None
    try:
        decoded = unquote(uri_text)
    except Exception:
        decoded = uri_text
    m = BUCKET_URI_RE.search(decoded)
    if not m:
        return None
    return "bucket_a%s_s%s" % (m.group(1), m.group(2))


def _goto_xyz_action(writer, target_page):
    """Build a same-document GoTo action to the top of target_page."""
    try:
        from pypdf.generic import (  # type: ignore
            ArrayObject,
            DictionaryObject,
            NameObject,
            NullObject,
            NumberObject,
        )
    except ImportError:
        from PyPDF2.generic import (  # type: ignore
            ArrayObject,
            DictionaryObject,
            NameObject,
            NullObject,
            NumberObject,
        )
    ref = getattr(target_page, "indirect_reference", None)
    if ref is None:
        ref = getattr(target_page, "indirect_ref", None)
    if ref is None:
        try:
            ref = target_page.indirect_reference
        except Exception:
            ref = None
    if ref is None:
        return None
    dest = ArrayObject()
    dest.append(ref)
    dest.append(NameObject("/XYZ"))
    dest.append(NullObject())
    dest.append(NullObject())
    dest.append(NumberObject(0))
    act = DictionaryObject()
    act[NameObject("/S")] = NameObject("/GoTo")
    act[NameObject("/D")] = dest
    return act


def _rewrite_index_bucket_uri_links(writer, index_page_count, stem_to_merged_page):
    """
    On the first index_page_count pages, turn file:/…/bucket_aX_sY.html URI links
    into same-document GoTo links to the merged bucket section start page.
    """
    try:
        from pypdf.generic import NameObject  # type: ignore
    except ImportError:
        from PyPDF2.generic import NameObject  # type: ignore

    nk = NameObject
    fixed = 0
    for pi in range(min(index_page_count, len(writer.pages))):
        page = writer.pages[pi]
        page_obj = _deref_pdf_object(writer, page)
        annots = page_obj.get(nk("/Annots"))
        if not annots:
            continue
        annots = _deref_pdf_object(writer, annots)
        for ref in list(annots):
            annot = _deref_pdf_object(writer, ref)
            if _subtype_str(annot.get(nk("/Subtype"))) != "/Link":
                continue
            act = annot.get(nk("/A"))
            if act is None:
                continue
            act = _deref_pdf_object(writer, act)
            if _subtype_str(act.get(nk("/S"))) != "/URI":
                continue
            uri_s = _uri_to_str(act.get(nk("/URI")))
            stem = _bucket_stem_from_uri(uri_s)
            if not stem or stem not in stem_to_merged_page:
                continue
            tgt_idx = stem_to_merged_page[stem]
            if tgt_idx < 0 or tgt_idx >= len(writer.pages):
                continue
            tgt_page = writer.pages[tgt_idx]
            new_act = _goto_xyz_action(writer, tgt_page)
            if new_act is None:
                continue
            annot[nk("/A")] = new_act
            if nk("/Dest") in annot:
                try:
                    del annot[nk("/Dest")]
                except Exception:
                    pass
            fixed += 1
    return fixed


def merge_pdfs_pypdf_with_internal_bucket_links(parts, out):
    """Merge with pypdf and rewrite index heat-map bucket links to in-PDF destinations."""
    try:
        from pypdf import PdfWriter  # type: ignore
    except ImportError:
        raise RuntimeError("pypdf not installed")

    counts = _pdf_page_counts(parts)
    cumulative = 0
    stem_to_merged_page = {}
    for i, p in enumerate(parts):
        if i > 0:
            stem_to_merged_page[p.stem] = cumulative
        cumulative += counts[i]

    w = PdfWriter()
    for p in parts:
        w.append(str(p))

    index_pages = counts[0]
    nfix = _rewrite_index_bucket_uri_links(w, index_pages, stem_to_merged_page)
    with open(str(out), "wb") as f:
        w.write(f)
    w.close()
    return nfix


def merge_pdfs_pdfunite(parts, out):
    exe = shutil.which("pdfunite")
    if not exe:
        raise RuntimeError("pdfunite not found")
    subprocess.check_call([exe] + [str(p) for p in parts] + [str(out)])


def merge_pdfs_qpdf(parts, out):
    exe = shutil.which("qpdf")
    if not exe:
        raise RuntimeError("qpdf not found")
    pages = [str(p) for p in parts]
    subprocess.check_call([exe, "--empty", "--pages"] + pages + ["--", str(out)])


def merge_pdfs(parts, out, internal_bucket_links=True):
    errors = []
    if internal_bucket_links:
        try:
            nfix = merge_pdfs_pypdf_with_internal_bucket_links(parts, out)
            if nfix:
                sys.stderr.write(
                    "ereport-html-to-pdf: rewrote %d index heat-map link(s) to in-PDF destinations.\n"
                    % nfix
                )
            else:
                sys.stderr.write(
                    "ereport-html-to-pdf: no index /URI bucket links were rewritten "
                    "(Chromium may emit a different link style); merged with pypdf.\n"
                )
            return
        except Exception as e:  # noqa: BLE001
            errors.append("pypdf+internal links: %s" % e)

    mergers = (
        ("pypdf", merge_pdfs_pypdf),
        ("pdfunite", merge_pdfs_pdfunite),
        ("qpdf", merge_pdfs_qpdf),
    )
    for name, fn in mergers:
        try:
            fn(parts, out)
            if internal_bucket_links:
                sys.stderr.write(
                    "ereport-html-to-pdf: merged with %s; in-PDF bucket links were not "
                    "applied (install pypdf for internal links).\n" % name
                )
            return
        except Exception as e:  # noqa: BLE001 — try next backend
            errors.append("%s: %s" % (name, e))
    die(
        "Could not merge PDFs. Install one of: pip install pypdf | poppler-utils (pdfunite) | qpdf.\n"
        + "\n".join(errors)
    )


def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Combine ereport index.html and bucket HTML pages into one PDF."
    )
    p.add_argument(
        "-i",
        "--input-dir",
        required=True,
        type=Path,
        help="Directory containing index.html (and optional bucket_a*_s*.html)",
    )
    p.add_argument(
        "-o",
        "--output",
        required=True,
        type=Path,
        help="Output PDF path",
    )
    p.add_argument(
        "--omit-interactive",
        action="store_true",
        help="Strip path search UI and bucket drawer from index.html in the PDF",
    )
    p.add_argument(
        "--chrome-arg",
        action="append",
        default=[],
        metavar="ARG",
        help="Extra argument passed to Chromium (repeatable). Example: --chrome-arg=--no-sandbox",
    )
    p.add_argument(
        "--keep-temps",
        action="store_true",
        help="Keep temp directory and print its path (for debugging)",
    )
    p.add_argument(
        "--no-internal-bucket-links",
        action="store_true",
        help="Do not rewrite index bucket links to in-PDF destinations (plain merge)",
    )
    return p.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    report_dir = args.input_dir.resolve()
    if not report_dir.is_dir():
        die("not a directory: %s" % report_dir)

    pages = collect_html_pages(report_dir)
    chrome = find_chrome()
    out_pdf = args.output.expanduser().resolve()
    out_pdf.parent.mkdir(parents=True, exist_ok=True)

    tmp_pdfs = []
    keep = args.keep_temps
    if keep:
        tmp_dir = Path(tempfile.mkdtemp(prefix="ereport-pdf-"))
        tmp_ctx = None
    else:
        tmp_ctx = tempfile.TemporaryDirectory(prefix="ereport-pdf-")
        tmp_dir = Path(tmp_ctx.name)

    try:
        for html in pages:
            html_for_print = write_maybe_stripped(html, tmp_dir, args.omit_interactive)
            piece = tmp_dir / ("%s.pdf" % html.stem)
            print_html_to_pdf(chrome, html_for_print, piece, args.chrome_arg)
            tmp_pdfs.append(piece)

        merge_pdfs(tmp_pdfs, out_pdf, internal_bucket_links=not args.no_internal_bucket_links)
    finally:
        if keep:
            sys.stdout.write("Kept temp dir: %s\n" % tmp_dir)
        elif tmp_ctx is not None:
            tmp_ctx.cleanup()

    sys.stdout.write("Wrote %s (%d HTML sections)\n" % (out_pdf, len(tmp_pdfs)))


if __name__ == "__main__":
    main(sys.argv[1:])
