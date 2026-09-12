"""Gate 7 listing-only connection admission; TLS is never decrypted.

The pinned listing client must enforce HTTP/1, Connection: close and no idle pool.
This counts conservative connection reservations, not observed HTTP requests.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import select
import socket
import threading
import time
from urllib.parse import urlsplit


class ListingProxy:
    def __init__(self, authority, maximum, journal, reserve, port=0, *, loopback=False, tls_fixture=False,
                 deadline=float('inf'), expires_utc=float('inf')):
        host, separator, raw_port = authority.rpartition(':')
        if (not separator or ((loopback or tls_fixture) and host != '127.0.0.1')
                or (not loopback and not tls_fixture and raw_port != '443')):
            raise ValueError('exact listing authority required')
        if type(maximum) is not int or not 1 <= maximum <= 1_000_000:
            raise ValueError('invalid listing connection reservation')
        self.target = (host, int(raw_port))
        self.authority = authority
        self.maximum = maximum
        self.admitted = 0
        self.active = 0
        self.failure = None
        self.lock = threading.Lock()
        self.reserve = reserve
        self.journal = Path(journal).open('xb')
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *_):
                pass

            def setup(self):
                self.request.settimeout(30)
                super().setup()

            def do_CONNECT(self):
                if loopback or self.path != authority:
                    self.send_error(403); self.close_connection = True
                    return
                self.forward(None)

            def do_GET(self):
                # Credential-free fixtures only. Real S3 accepts CONNECT alone.
                url = urlsplit(self.path)
                if (not loopback or url.scheme != 'http' or url.netloc != authority
                        or self.headers.get('Connection', '').lower() != 'close'):
                    self.send_error(403); self.close_connection = True
                    return
                path = url.path + ('?' + url.query if url.query else '')
                headers = ''.join(f'{key}: {value}\r\n' for key, value in self.headers.items()
                                  if key.lower() not in ('host', 'proxy-connection'))
                self.forward(f'GET {path} HTTP/1.1\r\nHost: {authority}\r\n{headers}\r\n'.encode())

            def forward(self, request):
                self.close_connection = True
                with proxy.lock:
                    if (proxy.failure or proxy.admitted >= maximum or proxy.active >= 32
                            or time.monotonic() >= deadline or time.time() >= expires_utc):
                        proxy.failure = proxy.failure or 'listing connection/concurrency ceiling'
                        self.send_error(503)
                        return
                    data = (json.dumps(dict(kind='listing-connection-admitted',
                                           sequence=proxy.admitted + 1, authority=authority,
                                           utc=time.time(), monotonic=time.monotonic())) + '\n').encode()
                    try:
                        proxy.reserve(len(data))
                        proxy.journal.write(data); proxy.journal.flush(); os.fsync(proxy.journal.fileno())
                    except (ValueError, OSError) as error:
                        proxy.failure = str(error)
                        self.send_error(503)
                        return
                    proxy.admitted += 1
                    proxy.active += 1
                try:
                    # Admission is durable before any upstream connection exists.
                    def remaining():
                        seconds = min(deadline - time.monotonic(), expires_utc - time.time())
                        if seconds <= 0:
                            proxy.failure = 'listing deadline'
                            raise TimeoutError(proxy.failure)
                        return seconds

                    with socket.create_connection(proxy.target, timeout=min(10, remaining())) as upstream:
                        upstream.settimeout(min(30, remaining()))
                        self.connection.settimeout(min(30, remaining()))
                        if request is None:
                            self.send_response(200, 'Connection Established'); self.end_headers(); self.wfile.flush()
                        else:
                            upstream.settimeout(min(30, remaining()))
                            upstream.sendall(request)
                        relay_deadline = time.monotonic() + min(30, remaining())
                        while time.monotonic() < relay_deadline:
                            ready, _, _ = select.select([self.connection, upstream], [], [], min(0.5, remaining()))
                            remaining()
                            for source in ready:
                                source.settimeout(min(30, remaining()))
                                chunk = source.recv(65536)
                                if not chunk:
                                    return
                                destination = upstream if source is self.connection else self.connection
                                destination.settimeout(min(30, remaining()))
                                destination.sendall(chunk)
                        remaining()
                except OSError:
                    # Failed transports remain charged; the S3 adapter owns retry
                    # and publication classification. Never log tunneled contents.
                    pass
                finally:
                    with proxy.lock:
                        proxy.active -= 1

        self.server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
        self.server.daemon_threads = False
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f'http://127.0.0.1:{self.server.server_port}'

    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=5)
        self.journal.close()
        if self.thread.is_alive() or self.active:
            raise ValueError('listing proxy did not stop')
