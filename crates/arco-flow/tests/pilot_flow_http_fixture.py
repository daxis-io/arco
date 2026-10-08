import hashlib
import json
import os
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.request import Request, urlopen

ROOT = Path('/private/tmp/arco-pilot-flow-process')
PENDING = ROOT / 'pending'
DONE = ROOT / 'done'
for directory in (ROOT, PENDING, DONE):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
MODE = ROOT / 'mode'
if not MODE.exists():
    MODE.write_text('accept')


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def respond(self, status, payload=None):
        data = json.dumps(payload or {}).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == '/state':
            return self.respond(200, {
                'pending': len(list(PENDING.glob('*.json'))),
                'done': len(list(DONE.glob('*.json'))),
                'received': len(list(ROOT.glob('received-*.json'))),
            })
        self.respond(404)

    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        body = self.rfile.read(size)
        if self.path == '/accept':
            if self.headers.get('Authorization') != 'Bearer ingress-secret':
                return self.respond(403)
            task = json.loads(body)
            task_id = task['taskId']
            path = PENDING / (hashlib.sha256(task_id.encode()).hexdigest() + '.json')
            if (DONE / path.name).exists() or path.exists():
                return self.respond(409)
            mode = MODE.read_text().strip()
            if mode == 'full':
                return self.respond(429)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as file:
                file.write(body)
                file.flush()
                os.fsync(file.fileno())
            dir_fd = os.open(PENDING, os.O_RDONLY)
            os.fsync(dir_fd)
            os.close(dir_fd)
            return self.respond(500 if mode == 'uncertain' else 202)
        if self.path == '/dispatch':
            if self.headers.get('X-Arco-Dispatch-Secret') != 'local-secret':
                return self.respond(403)
            envelope = json.loads(body)
            receipt = {
                key: envelope[key]
                for key in ('dispatchId', 'runId', 'taskId', 'attemptId', 'callbackBaseUrl')
            }
            path = ROOT / ('received-' + hashlib.sha256(body).hexdigest() + '.json')
            path.write_text(json.dumps(receipt, sort_keys=True))
            return self.respond(204)
        if self.path == '/drain':
            delivered = 0
            for path in sorted(PENDING.glob('*.json')):
                task = json.loads(path.read_text())
                request = Request(
                    task['targetUrl'],
                    data=task['body'].encode(),
                    headers={'Content-Type': 'application/json', **(task.get('headers') or {})},
                    method='POST',
                )
                with urlopen(request, timeout=5) as response:
                    if response.status not in (200, 202, 204):
                        raise RuntimeError(f'worker returned {response.status}')
                os.link(path, DONE / path.name)
                os.unlink(path)
                delivered += 1
            return self.respond(200, {'delivered': delivered})
        self.respond(404)


for port in (5198, 5199):
    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
print('ARCO_FLOW_FIXTURE_READY', flush=True)
ThreadingHTTPServer(('127.0.0.1', 5200), Handler).serve_forever()
