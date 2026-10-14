import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from urllib.parse import quote
from urllib.error import HTTPError
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
PUBLICATION_MODE = ROOT / 'publication-mode'
if not PUBLICATION_MODE.exists():
    PUBLICATION_MODE.write_text('normal')
COMPUTE_COUNT = ROOT / 'compute-count'
if not COMPUTE_COUNT.exists():
    COMPUTE_COUNT.write_text('0')
CALLBACK_ACK_MODE = ROOT / 'callback-ack-mode'
if not CALLBACK_ACK_MODE.exists():
    CALLBACK_ACK_MODE.write_text('normal')
NETWORK_TIMEOUT = int(os.environ.get('ARCO_FLOW_FIXTURE_NETWORK_TIMEOUT', '10'))
DRAIN_TIMEOUT = NETWORK_TIMEOUT if 'ARCO_FLOW_FIXTURE_NETWORK_TIMEOUT' in os.environ else 5
SEED_TIMEOUT = NETWORK_TIMEOUT if 'ARCO_FLOW_FIXTURE_NETWORK_TIMEOUT' in os.environ else None


def publication_claim(output, run_id, mode):
    claim = dict(output)
    # Owner evidence comes only from Arco's verifier, never from this worker fixture.
    claim.pop('ownerEvidence', None)
    return claim


def callback_record_path(task_id):
    return ROOT / ('callback-' + hashlib.sha256(task_id.encode()).hexdigest() + '.json')


def pending_task_path(task_id):
    return next((path for path in PENDING.glob('*.json')
                 if json.loads(json.loads(path.read_text())['body'])['taskId'] == task_id), None)


class CallbackAcknowledgmentDiscarded(Exception):
    pass


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
                'computeCount': int(COMPUTE_COUNT.read_text()),
                'publicationMode': PUBLICATION_MODE.read_text().strip(),
            })
        self.respond(404)

    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        body = self.rfile.read(size)
        if self.path == '/publication-mode':
            mode = json.loads(body).get('mode')
            if mode not in ('normal', 'missing', 'corrupt'):
                return self.respond(400, {'error': 'unsupported publication mode'})
            PUBLICATION_MODE.write_text(mode)
            return self.respond(200, {'mode': mode})
        if self.path == '/callback-ack-mode':
            mode = json.loads(body).get('mode')
            if mode not in ('normal', 'discard-completed'):
                return self.respond(400, {'error': 'unsupported callback acknowledgment mode'})
            CALLBACK_ACK_MODE.write_text(mode)
            return self.respond(200, {'mode': mode})
        if self.path == '/replay':
            replay = json.loads(body)
            task_id = replay.get('taskId')
            if not isinstance(task_id, str):
                return self.respond(400, {'error': 'taskId is required'})
            record_path = callback_record_path(task_id)
            if not record_path.exists():
                return self.respond(404, {'error': 'completion callback not found'})
            record = json.loads(record_path.read_text())
            payload = record['payload']
            if replay.get('changeImmutableField') == 'objectPath':
                payload = json.loads(json.dumps(payload))
                payload['output']['publication']['objectPath'] = 'outputs/tampered/result.parquet'
            request = Request(record['url'], data=json.dumps(payload).encode(),
                              headers=record['headers'], method='POST')
            try:
                with urlopen(request, timeout=NETWORK_TIMEOUT) as response:
                    assert response.status == 200
            except HTTPError as error:
                if replay.get('changeImmutableField'):
                    return self.respond(200, {'replayed': False, 'status': error.code})
                raise
            pending = pending_task_path(task_id)
            if pending is not None:
                os.link(pending, DONE / pending.name)
                os.unlink(pending)
            return self.respond(200, {'replayed': True})
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
                    with urlopen(request, timeout=NETWORK_TIMEOUT) as response:
                        assert response.status == 200
                    if suffix == '/completed' and CALLBACK_ACK_MODE.read_text().strip() == 'discard-completed':
                        raise CallbackAcknowledgmentDiscarded()
                callback_post('/started', identity)
                with closing(sqlite3.connect(':memory:')) as connection:
                    rows = connection.execute('select :value', {k: v['value'] for k, v in parameters.items()}).fetchall()
                assert len(rows) == 1 and len(rows[0]) == 1 and isinstance(rows[0][0], int)
                result = rows[0][0]
                output = json.loads(subprocess.check_output(
                    [os.environ['ARCO_FLOW_OUTPUT_SEED'], 'output', envelope['runId'], str(result)],
                    text=True, timeout=SEED_TIMEOUT))
                mode = PUBLICATION_MODE.read_text().strip()
                if mode == 'missing':
                    subprocess.check_call(
                        [os.environ['ARCO_FLOW_OUTPUT_SEED'], 'remove-output', envelope['runId']],
                        timeout=SEED_TIMEOUT)
                if mode == 'corrupt':
                    subprocess.check_call(
                        [os.environ['ARCO_FLOW_OUTPUT_SEED'], 'corrupt-output', envelope['runId']],
                        timeout=SEED_TIMEOUT)
                completion = {**identity, 'outcome': 'SUCCEEDED',
                              'output': {'rowCount': 1, 'byteSize': output['byteSize'],
                                         'publication': publication_claim(output, envelope['runId'], mode)}}
                callback_record_path(envelope['taskId']).write_text(json.dumps({
                    'url': 'http://127.0.0.1:5187/api/v1/tasks/' + quote(envelope['taskId'], safe='') + '/completed',
                    'headers': callback_headers, 'payload': completion,
                }))
                COMPUTE_COUNT.write_text(str(int(COMPUTE_COUNT.read_text()) + 1))
                try:
                    callback_post('/completed', completion)
                except CallbackAcknowledgmentDiscarded:
                    return self.respond(502, {'apiAccepted': True, 'wireLoss': False})
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
                try:
                    with urlopen(request, timeout=DRAIN_TIMEOUT) as response:
                        if response.status not in (200, 202, 204):
                            raise RuntimeError(f'worker returned {response.status}')
                except HTTPError as error:
                    if error.code == 502:
                        return self.respond(502, {
                            'delivered': delivered, 'apiAccepted': True, 'wireLoss': False,
                        })
                    raise
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
