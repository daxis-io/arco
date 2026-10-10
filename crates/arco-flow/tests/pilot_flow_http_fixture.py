import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from urllib.parse import quote
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.request import Request, urlopen

if sys.flags.optimize:
    raise SystemExit('fixture requires unoptimized Python for its validation assertions')

ROOT = Path(os.environ['ARCO_FLOW_FIXTURE_ROOT'])
PENDING = ROOT / 'pending'
DONE = ROOT / 'done'
for directory in (ROOT, PENDING, DONE):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
MODE = ROOT / 'mode'
if not MODE.exists():
    MODE.write_text('accept')


def immutable_task_hash(task):
    envelope = json.loads(task['body'])
    # Attempt credentials may renew; the execution intent and delivery metadata cannot change.
    envelope.pop('taskToken', None)
    envelope.pop('tokenExpiresAt', None)
    immutable = {**task, 'body': envelope}
    canonical = json.dumps(immutable, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    return hashlib.sha256(canonical).hexdigest()


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
            previous = DONE / path.name if (DONE / path.name).exists() else path
            if previous.exists():
                incoming = immutable_task_hash(task)
                stored = immutable_task_hash(json.loads(previous.read_text()))
                witness = {'incomingIntentSha256': incoming, 'storedIntentSha256': stored}
                (ROOT / ('duplicate-' + path.stem + '.json')).write_text(json.dumps(witness))
                return self.respond(409 if incoming == stored else 422)
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
            result = None
            if envelope.get('payload', {}).get('version') == 1:
                assert envelope['callbackBaseUrl'] == 'http://127.0.0.1:5187'
                execution = envelope['payload']['asset']['execution']['payload']
                assert execution['sql'] == 'select :value'
                parameters = execution['parameters']
                assert set(parameters) == {'value'}
                assert all(v['type'] == 'int64' and type(v['value']) is int and -(2**63) <= v['value'] < 2**63 for v in parameters.values())
                callback_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + envelope['taskToken']}
                identity = {'attempt': envelope['attempt'], 'attemptId': envelope['attemptId'], 'workerId': 'reference-worker'}
                def callback_post(suffix, payload):
                    request = Request('http://127.0.0.1:5187/api/v1/tasks/' + quote(envelope['taskId'], safe='') + suffix,
                                      data=json.dumps(payload).encode(), headers=callback_headers, method='POST')
                    with urlopen(request, timeout=10) as response:
                        assert response.status == 200
                callback_post('/started', identity)
                with closing(sqlite3.connect(':memory:')) as connection:
                    rows = connection.execute('select :value', {k: v['value'] for k, v in parameters.items()}).fetchall()
                assert len(rows) == 1 and len(rows[0]) == 1 and isinstance(rows[0][0], int)
                result = rows[0][0]
                output = json.loads(subprocess.check_output([os.environ['ARCO_FLOW_OUTPUT_SEED'], 'output', envelope['runId'], str(result)], text=True))
                callback_post('/completed', {**identity, 'outcome': 'SUCCEEDED', 'output': {'rowCount': 1, 'byteSize': output['byteSize'], 'publication': output}})
            receipt = {
                key: envelope[key]
                for key in ('dispatchId', 'runId', 'taskId', 'attemptId', 'callbackBaseUrl')
            }
            if result is not None:
                receipt['result'] = result
                receipt['manifestId'] = envelope['payload']['manifest']['manifestId']
            path = ROOT / ('received-' + hashlib.sha256(body).hexdigest() + '.json')
            path.write_text(json.dumps(receipt, sort_keys=True))
            return self.respond(204)
        if self.path == '/drain':
            delivered = 0
            for path in sorted(PENDING.glob('*.json')):
                task = json.loads(path.read_text())
                assert task['targetUrl'] == 'http://127.0.0.1:5199/dispatch'
                request = Request(
                    'http://127.0.0.1:5199/dispatch',
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


if __name__ == '__main__':
    for port in (5198, 5199):
        server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
        Thread(target=server.serve_forever, daemon=True).start()
    print('ARCO_FLOW_FIXTURE_READY', flush=True)
    ThreadingHTTPServer(('127.0.0.1', 5200), Handler).serve_forever()
