#!/usr/bin/env python3

import argparse
import socket
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from socketserver import ThreadingMixIn

try:
    from http.server import ThreadingHTTPServer as BaseThreadingHTTPServer
except ImportError:
    class BaseThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        pass


class ReusableThreadingHTTPServer(BaseThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def parse_args():
    parser = argparse.ArgumentParser(description='Serve generated report files over HTTP.')
    parser.add_argument('root', nargs='?', default='.', help='Directory to serve. Defaults to the current directory.')
    parser.add_argument('--bind', default='127.0.0.1', help='Address to bind to. Defaults to 127.0.0.1.')
    parser.add_argument('--port', type=int, default=8000, help='Port to listen on. Defaults to 8000.')
    return parser.parse_args()


def main():
    args = parse_args()
    root = Path(args.root).resolve()
    if not root.is_dir():
        raise SystemExit(f'error: not a directory: {root}')

    handler = partial(SimpleHTTPRequestHandler, directory=str(root))
    try:
        server = ReusableThreadingHTTPServer((args.bind, args.port), handler)
    except OSError as exc:
        raise SystemExit(f'error: cannot bind {args.bind}:{args.port}: {exc.strerror}') from exc

    print(f'Serving {root}')
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
