#!/usr/bin/env python3

import argparse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


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

    handler = lambda *a, **kw: SimpleHTTPRequestHandler(*a, directory=str(root), **kw)
    server = ThreadingHTTPServer((args.bind, args.port), handler)

    print(f'Serving {root}')
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
