#!/usr/bin/env python3

import argparse
import os

from http.server import BaseHTTPRequestHandler, HTTPServer


class AppMockHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/ping":
            self._handle_ping()

    def _handle_ping(self):
        marathon_id = os.getenv("MARATHON_APP_ID", "NO_MARATHON_APP_ID_SET")

        self.send_response(200)
        self.end_headers()
        self.wfile.write(bytes("Pong {0}\n".format(marathon_id), "utf-8"))


def main():
    parser = argparse.ArgumentParser(description='Marathon test helper.')
    parser.add_argument('port', type=int)

    args = parser.parse_args()

    app_mock = HTTPServer(('', args.port), AppMockHandler)
    app_mock.serve_forever()

if __name__ == '__main__':
    main()
