#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Michel Erb — see LICENSE.

"""Serve generated report files over HTTP.

Path search (uses trigram index built by ereport_index --make):

  GET /<report_dir>/search?q=<term>&skip=<n>&limit=<m>

Example (multi-user tree):  GET /milechin/search?q=foo&skip=0&limit=20

When SERVE_ROOT is the report directory itself (index.html + index/ at top level),
also accept:

  GET /search?q=...

Requires ereport_index on PATH or set EREPORT_INDEX_BIN to its absolute path.

Search index directory defaults to ./index under each report path (see URL patterns below).
Override with --index-dir DIR or EREPORT_SEARCH_INDEX_DIR (CLI wins over env): one directory
containing tri_keys.bin used for every …/search request (may be outside SERVE_ROOT).

JSON responses include X-Search-Index-Dir with the resolved index directory (compare to the DIR
you pass to ereport_index --search --index-dir … on the CLI when debugging mismatches).
"""

import argparse
import json
import os
import socket
import subprocess
from functools import partial
from http import HTTPStatus
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Optional
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, unquote, urlparse

try:
    from http.server import ThreadingHTTPServer as BaseThreadingHTTPServer
except ImportError:

    class BaseThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        pass


class ReusableThreadingHTTPServer(BaseThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def resolve_ereport_index_bin() -> str:
    override = os.environ.get('EREPORT_INDEX_BIN')
    if override:
        return override
    sibling = Path(__file__).resolve().parent / 'ereport_index'
    if sibling.is_file() and os.access(sibling, os.X_OK):
        return str(sibling)
    return 'ereport_index'


def search_term_ok(term: str) -> bool:
    t = term.strip()
    return len(t) >= 3


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', nargs='?', default='.', help='Directory to serve. Defaults to the current directory.')
    parser.add_argument('--bind', default='127.0.0.1', help='Address to bind to. Defaults to 127.0.0.1.')
    parser.add_argument('--port', type=int, default=8000, help='Port to listen on. Defaults to 8000.')
    parser.add_argument(
        '--index-dir',
        metavar='DIR',
        default=None,
        help='Trigram index directory (tri_keys.bin). Overrides default …/index layout; '
        'may be outside SERVE_ROOT. Env: EREPORT_SEARCH_INDEX_DIR.',
    )
    return parser.parse_args()


def _normalize_index_dir_string(raw: str) -> str:
    """Fix paths broken by a common Make typo: SERVE_INDEX_DIR==/abs sets value '=/abs'.

    Path('=/abs') is relative, so .resolve() becomes cwd/=/abs. Strip leading '=' run
    (from repeated ==/) until the path no longer starts with '=' or is a single '='.
    """
    s = raw.strip()
    while len(s) > 1 and s.startswith('='):
        s = s[1:]
    return s.strip()


def resolve_search_index_dir_override(cli_path: Optional[str]) -> Optional[Path]:
    """CLI --index-dir overrides EREPORT_SEARCH_INDEX_DIR."""
    raw = cli_path if cli_path else os.environ.get('EREPORT_SEARCH_INDEX_DIR')
    if not raw:
        return None
    raw = _normalize_index_dir_string(raw)
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


class ReportHTTPRequestHandler(SimpleHTTPRequestHandler):
    """Class attributes may be set before serve_forever."""

    _ereport_index_override = None  # type: Optional[str]
    _search_index_dir_override = None  # type: Optional[Path]

    def __init__(self, *args, **kwargs):
        self._ereport_index_bin = type(self)._ereport_index_override or resolve_ereport_index_bin()
        super().__init__(*args, **kwargs)

    def _send_search_json(self, status: HTTPStatus, body: bytes, index_dir: Path) -> None:
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('X-Search-Index-Dir', str(index_dir.resolve()))
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [unquote(p) for p in parsed.path.split('/') if p]
        if parts and parts[-1] == 'search':
            if self._handle_path_search(parsed, parts):
                return
        super().do_GET()

    def _handle_path_search(self, parsed, parts):
        root = Path(self.directory).resolve()
        report_rel = parts[:-1]
        idx_ov = type(self)._search_index_dir_override
        if idx_ov is not None:
            index_dir = idx_ov
        elif report_rel:
            index_dir = root.joinpath(*report_rel, 'index')
        else:
            # GET /search — serve root is already the report folder (./index/ beside index.html)
            index_dir = root / 'index'

        if idx_ov is None:
            try:
                index_dir.resolve().relative_to(root)
            except ValueError:
                self.send_error(HTTPStatus.FORBIDDEN, 'path escapes serve root')
                return True

        if not (index_dir / 'tri_keys.bin').is_file():
            hint = (
                'Check --index-dir / EREPORT_SEARCH_INDEX_DIR, or build with: ereport_index --make'
                if idx_ov is not None
                else 'Build one in this report directory with: ereport_index --make'
            )
            body = json.dumps(
                {
                    'error': 'No search index found.',
                    'hint': hint,
                }
            ).encode('utf-8')
            self._send_search_json(HTTPStatus.NOT_FOUND, body, index_dir)
            return True

        qs = parse_qs(parsed.query, keep_blank_values=True)
        term = (qs.get('q') or [''])[0]
        if not search_term_ok(term):
            body = json.dumps(
                {'error': 'search query q must be at least 3 characters (ereport_index rules)'}
            ).encode('utf-8')
            self._send_search_json(HTTPStatus.BAD_REQUEST, body, index_dir)
            return True

        try:
            skip = int((qs.get('skip') or ['0'])[0])
            limit = int((qs.get('limit') or ['50'])[0])
        except ValueError:
            self.send_error(HTTPStatus.BAD_REQUEST, 'skip and limit must be integers')
            return True
        if skip < 0:
            self.send_error(HTTPStatus.BAD_REQUEST, 'skip must be >= 0')
            return True
        if limit < 1:
            self.send_error(HTTPStatus.BAD_REQUEST, 'limit must be >= 1')
            return True
        if limit > 1_000_000:
            self.send_error(HTTPStatus.BAD_REQUEST, 'limit too large')
            return True

        cmd = [
            self._ereport_index_bin,
            '--search',
            '--index-dir',
            str(index_dir),
            term,
            '--json',
            '--skip',
            str(skip),
            '--limit',
            str(limit),
        ]
        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=600,
                cwd=str(root),
            )
        except subprocess.TimeoutExpired:
            self.send_error(HTTPStatus.GATEWAY_TIMEOUT, 'ereport_index timed out')
            return True
        except FileNotFoundError:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, 'ereport_index not found: ' + self._ereport_index_bin)
            return True

        if proc.returncode != 0:
            msg = (proc.stderr or proc.stdout or 'unknown error').strip()
            err_body = json.dumps(
                {
                    'error': 'ereport_index failed',
                    'code': proc.returncode,
                    'detail': msg[:2000],
                }
            ).encode('utf-8')
            self._send_search_json(HTTPStatus.BAD_GATEWAY, err_body, index_dir)
            return True

        out = (proc.stdout or '').strip()
        if not out:
            # Older ereport_index could exit 0 with no stdout on no-match; return empty JSON.
            empty = json.dumps(
                {'total': 0, 'skip': skip, 'limit': limit, 'paths': []}
            ).encode('utf-8')
            self._send_search_json(HTTPStatus.OK, empty, index_dir)
            return True
        try:
            json.loads(out)
        except json.JSONDecodeError:
            self.send_error(HTTPStatus.BAD_GATEWAY, 'invalid JSON from ereport_index')
            return True

        raw = out.encode('utf-8')
        self._send_search_json(HTTPStatus.OK, raw, index_dir)
        return True


def main():
    args = parse_args()
    root = Path(args.root).resolve()
    if not root.is_dir():
        raise SystemExit(f'error: not a directory: {root}')

    index_bin = resolve_ereport_index_bin()
    ReportHTTPRequestHandler._ereport_index_override = index_bin
    idx_dir = resolve_search_index_dir_override(args.index_dir)
    ReportHTTPRequestHandler._search_index_dir_override = idx_dir
    handler = partial(ReportHTTPRequestHandler, directory=str(root))
    try:
        server = ReusableThreadingHTTPServer((args.bind, args.port), handler)
    except OSError as exc:
        raise SystemExit(f'error: cannot bind {args.bind}:{args.port}: {exc.strerror}') from exc

    print(f'Serving {root}')
    print(f'ereport_index: {index_bin}')
    if idx_dir is not None:
        print(f'path search index (all GET …/search): {idx_dir}')
    else:
        print(
            'path search: no --index-dir / EREPORT_SEARCH_INDEX_DIR; '
            'each GET /<report>/search uses <SERVE_ROOT>/<report>/index '
            '(set override to match ereport_index --search --index-dir DIR on the CLI)'
        )
    if args.bind == '0.0.0.0':
        hostname = socket.gethostname()
        print(f'URL: http://127.0.0.1:{args.port}/')
        print(f'URL: http://{hostname}:{args.port}/')
    else:
        print(f'URL: http://{args.bind}:{args.port}/')
    print('Press Ctrl-C to stop.')

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print()
        print('Stopping server.')
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
