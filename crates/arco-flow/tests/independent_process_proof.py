"""Packaged API/Flow proof against disposable loopback S3; never uses cloud credentials.

Run with the Moto/boto3 test environment and a freshly built dedicated target:
  python independent_process_proof.py <cargo-metadata-target>/debug <new-receipt-dir>
"""

import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import ProxyHandler, Request, build_opener

import boto3
from botocore.config import Config

if sys.flags.optimize:
    raise SystemExit('proof requires unoptimized Python for its evidence assertions')

BIN = Path(sys.argv[1]).resolve()
ROOT = Path(sys.argv[2]).resolve()
ROOT.mkdir(mode=0o700, parents=True, exist_ok=False)
SOURCE = Path(__file__).resolve().parents[3]
API = 'http://127.0.0.1:5187'
DISPATCHER = 'http://127.0.0.1:5188'
SWEEPER = 'http://127.0.0.1:5189'
S3 = 'http://127.0.0.1:5190'
CONTROL = 'http://127.0.0.1:5200'
JWT_SECRET = 'reference-api-secret-00000000000000000000000000000000'
TASK_SECRET = 'reference-task-secret-0000000000000000000000000000000'
processes = []
logs = []
receipt = {'status': 'failed', 'providerQualification': 'untested', 'cases': {}}
urlopen = build_opener(ProxyHandler({})).open

# Clear inherited provider and Arco configuration before assigning fake local credentials.
env = {k: v for k, v in os.environ.items()
       if not k.startswith(('ARCO_', 'AWS_', 'GOOGLE_', 'GCP_', 'AZURE_', 'CLOUDSDK_'))
       and k not in ('PORT', 'PYTHONOPTIMIZE') and k.lower() not in ('http_proxy', 'https_proxy', 'all_proxy')}
env.update({
    'AWS_ACCESS_KEY_ID': 'local-test', 'AWS_SECRET_ACCESS_KEY': 'local-test',
    'AWS_REGION': 'us-east-1', 'AWS_ENDPOINT': S3, 'AWS_ALLOW_HTTP': 'true',
    'AWS_EC2_METADATA_DISABLED': 'true', 'ARCO_STORAGE_BUCKET': 's3://arco-local-proof',
    'ARCO_TENANT_ID': 'pilot', 'ARCO_WORKSPACE_ID': 'flow',
    'ARCO_ENVIRONMENT': 'prod', 'ARCO_API_PUBLIC': 'false', 'ARCO_DEBUG': 'false',
    'ARCO_HTTP_PORT': '5187', 'ARCO_GRPC_PORT': '5191', 'ARCO_JWT_SECRET': JWT_SECRET,
    'ARCO_JWT_ISSUER': 'reference', 'ARCO_JWT_AUDIENCE': 'arco-api',
    'ARCO_CONTROL_STORE_OPERATOR_ENDPOINTS': 'true',
    'ARCO_CONTROL_STORE_OPERATOR_GROUP': 'arco-operators',
    'ARCO_TASK_TOKEN_SECRET': TASK_SECRET, 'ARCO_TASK_TOKEN_ISSUER': 'reference',
    'ARCO_TASK_TOKEN_AUDIENCE': 'arco-worker', 'ARCO_TASK_TOKEN_TTL_SECS': '3600',
    'ARCO_FLOW_TASK_TOKEN_SECRET': TASK_SECRET, 'ARCO_FLOW_TASK_TOKEN_ISSUER': 'reference',
    'ARCO_FLOW_TASK_TOKEN_AUDIENCE': 'arco-worker', 'ARCO_FLOW_TASK_TOKEN_TTL_SECS': '3600',
    'ARCO_FLOW_WORKER_TRANSPORT': 'http',
    'ARCO_FLOW_HTTP_INGRESS_URL': 'http://127.0.0.1:5198/accept',
    'ARCO_FLOW_HTTP_INGRESS_TOKEN': 'ingress-secret',
    'ARCO_FLOW_DISPATCH_TARGET_URL': 'http://127.0.0.1:5199/dispatch',
    'ARCO_FLOW_WORKER_DISPATCH_SECRET': 'local-secret',
    'ARCO_FLOW_CALLBACK_BASE_URL': API, 'ARCO_FLOW_FIXTURE_ROOT': str(ROOT / 'queue'),
    'ARCO_FLOW_OUTPUT_SEED': str(BIN / 'examples/pilot_flow_process_seed'),
})


def jwt(tenant='pilot', workspace='flow'):
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).rstrip(b'=')
    payload = {'sub': 'reference-operator', 'tenant': tenant, 'workspace': workspace,
               'groups': ['arco-operators'], 'iss': 'reference', 'aud': 'arco-api',
               'exp': int(time.time()) + 3600, 'iat': int(time.time())}
    message = encode({'alg': 'HS256', 'typ': 'JWT'}) + b'.' + encode(payload)
    signature = base64.urlsafe_b64encode(hmac.digest(JWT_SECRET.encode(), message, 'sha256')).rstrip(b'=')
    return (message + b'.' + signature).decode()


def request(url, payload=None, token=None, expected=200):
    assert urlparse(url).hostname == '127.0.0.1', 'proof must stay on loopback'
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    req = Request(url, headers=headers, data=None if payload is None else json.dumps(payload).encode())
    try:
        with urlopen(req, timeout=30) as response:
            status, data = response.status, response.read()
    except HTTPError as error:
        status, data = error.code, error.read()
    assert status == expected, (url, status, data.decode()[:2000])
    return json.loads(data) if data else None


def start(name, command, additions=None):
    log = (ROOT / (name + '.log')).open('wb')
    logs.append(log)
    process = subprocess.Popen(command, env={**env, **(additions or {})}, stdout=log, stderr=log)
    processes.append(process)
    return process


def ready(url):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        assert all(p.poll() is None for p in processes), 'process exited; inspect retained logs'
        try:
            with urlopen(url, timeout=1) as response:
                assert response.status == 200
            return
        except (URLError, TimeoutError):
            time.sleep(0.1)
    raise AssertionError('readiness deadline: ' + url)


def seed(*args):
    return subprocess.check_output([str(BIN / 'examples/pilot_flow_process_seed'), *args], env=env, text=True)


def manifest(value):
    return {'manifestVersion': '1.0', 'codeVersionId': 'code-' + str(value), 'assets': [{
        'key': {'namespace': 'analytics', 'name': 'daily'},
        'id': '019a0000-0000-7000-8000-000000000001',
        'execution': {'payload': {'sql': 'select :value', 'parameters': {'value': {'type': 'int64', 'value': value}}}},
        'resources': {'memoryMb': 64}, 'io': {'inputs': []},
    }]}


try:
    for port in (5187, 5188, 5189, 5190, 5191, 5198, 5199, 5200):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', port))
    for name in ('arco-api', 'arco_flow_dispatcher', 'arco_flow_sweeper', 'examples/pilot_flow_process_seed'):
        path = BIN / name
        receipt.setdefault('binaries', {})[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    receipt['sourceCommit'] = subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip()
    receipt['sourceDiffSha256'] = hashlib.sha256(subprocess.check_output(['git', '-C', str(SOURCE), 'diff', 'HEAD'])).hexdigest()
    receipt['fixtures'] = {str(path.relative_to(SOURCE)): hashlib.sha256(path.read_bytes()).hexdigest()
                           for path in (Path(__file__).resolve(), SOURCE / 'crates/arco-flow/tests/pilot_flow_http_fixture.py',
                                        SOURCE / 'crates/arco-flow/examples/pilot_flow_process_seed.rs')}
    start('s3', [str(Path(sys.executable).with_name('moto_server')), '-H', '127.0.0.1', '-p', '5190'])
    ready(S3)
    boto3.client('s3', endpoint_url=S3, aws_access_key_id='local-test',
                 aws_secret_access_key='local-test', region_name='us-east-1',
                 config=Config(proxies={})).create_bucket(Bucket='arco-local-proof')
    start('fixture', [sys.executable, str(SOURCE / 'crates/arco-flow/tests/pilot_flow_http_fixture.py')])
    ready(CONTROL + '/state')
    start('api', [str(BIN / 'arco-api')])
    ready(API + '/health')
    dispatcher = start('dispatcher', [str(BIN / 'arco_flow_dispatcher')], {'ARCO_FLOW_PORT': '5188'})
    start('sweeper', [str(BIN / 'arco_flow_sweeper')], {'ARCO_FLOW_PORT': '5189'})
    ready(DISPATCHER + '/health')
    ready(SWEEPER + '/health')
    token = jwt()
    request(API + '/api/v1/namespaces', {'name': 'catalog-proof'}, token, 201)
    read = request(API + '/internal/control-store/catalog-projection/urls', {}, token)
    assert read['descriptorVersion'] == 1 and len(read['files']) == 4
    assert read['scope'] == {'tenantId': 'pilot', 'workspaceId': 'flow', 'domain': 'catalog'}
    for file in read['files']:
        assert urlparse(file['url']).hostname == '127.0.0.1'
        with urlopen(file['url'], timeout=10) as response:
            data = response.read()
        assert len(data) == file['byteSize'] and hashlib.sha256(data).hexdigest() == file['checksumSha256']
        assert data[:4] == b'PAR1' and data[-4:] == b'PAR1'
        with urlopen(Request(file['url'], headers={'Range': 'bytes=0-3'}), timeout=10) as response:
            assert response.status == 206 and response.read() == b'PAR1'
    receipt['cases']['pinnedCatalogHttpRead'] = 'passed'
    manifest_a = request(API + '/api/v1/workspaces/flow/manifests', manifest(7), token, 201)
    trigger = {'selection': ['analytics.daily'], 'runKey': 'reference-frozen-a'}
    run = request(API + '/api/v1/workspaces/flow/runs', trigger, token, 201)
    (ROOT / 'queue/mode').write_text('uncertain')
    planned = request(DISPATCHER + '/run', {})
    assert planned['ready_dispatch_emitted'] == 1 and planned['dispatch_actions'] == 0
    uncertain = request(DISPATCHER + '/run', {}, expected=500)
    assert uncertain['dispatch_failed'] == 1
    assert request(CONTROL + '/state')['pending'] == 1
    request(API + '/api/v1/workspaces/flow/manifests', manifest(9), token, 201)
    dispatcher.terminate()
    dispatcher.wait(timeout=5)
    processes.remove(dispatcher)
    start('dispatcher-restarted', [str(BIN / 'arco_flow_dispatcher')], {'ARCO_FLOW_PORT': '5188'})
    ready(DISPATCHER + '/health')
    replay = request(API + '/api/v1/workspaces/flow/runs', trigger, token)
    assert replay['runId'] == run['runId']
    (ROOT / 'queue/mode').write_text('accept')
    recovered = request(DISPATCHER + '/run', {})
    assert recovered['dispatch_deduplicated'] == 1
    witness = json.loads(next((ROOT / 'queue').glob('duplicate-*.json')).read_text())
    assert witness['incomingIntentSha256'] == witness['storedIntentSha256']
    task_file = next((ROOT / 'queue/pending').glob('*.json'))
    envelope = json.loads(json.loads(task_file.read_text())['body'])
    assert request(CONTROL + '/drain', {})['delivered'] == 1
    received = json.loads(next((ROOT / 'queue').glob('received-*.json')).read_text())
    assert received['result'] == 7 and received['manifestId'] == manifest_a['manifestId']
    publication = request(API + '/api/v1/tasks/' + envelope['taskId'] + '/publication', token=envelope['taskToken'])
    assert publication['publication']['ownerEvidence'] is not None
    completed = request(API + '/api/v1/workspaces/flow/runs/' + run['runId'], token=token)
    assert completed['state'] == 'SUCCEEDED'
    receipt['cases']['frozenPayloadAfterDeployment'] = 'passed'
    receipt['cases']['uncertainAcceptanceRecovery'] = 'passed'
    receipt['cases']['dispatcherRestart'] = 'passed'
    receipt['cases']['ownerVerifiedPublication'] = 'passed'
    seed('capacity-run')
    (ROOT / 'queue/mode').write_text('full')
    full = request(DISPATCHER + '/run', {}, expected=500)
    assert full['dispatch_failed'] == 1
    assert request(CONTROL + '/state')['pending'] == 0
    receipt['cases']['capacityRefusal'] = 'passed'
    (ROOT / 'queue/mode').write_text('accept')
    seed('repair-run', '20')
    repair = request(SWEEPER + '/run', {})
    assert repair['redispatch_enqueued'] >= 1
    receipt['cases']['packagedSweeperRepair'] = 'passed'
    request(CONTROL + '/drain', {})
    seed('timer')
    refused = request(DISPATCHER + '/run', {}, expected=500)
    assert refused['timer_failed'] == 1 and refused['timer_enqueued'] == 0
    receipt['cases']['httpTimerRefusal'] = 'passed'
    receipt['status'] = 'passed'
finally:
    for process in reversed(processes):
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    for log in logs:
        log.close()
    (ROOT / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt, indent=2))
