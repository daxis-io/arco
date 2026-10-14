"""Independent API/Flow proof against loopback S3 or the dedicated R2 bucket.

Run with the Moto/boto3 test environment and a freshly built dedicated target:
  python independent_process_proof.py <cargo-metadata-target>/debug <new-receipt-dir>

The optional ``--r2`` mode is intentionally explicit. It uses the existing
dedicated qualification bucket and never creates buckets or accepts ambient
AWS profile/session configuration.
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
from urllib.parse import unquote, urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

if sys.flags.optimize:
    raise SystemExit('proof requires unoptimized Python for its evidence assertions')

args = sys.argv[1:]
R2_VALIDATION_ONLY = args == ['--validate-r2']
if not R2_VALIDATION_ONLY and (len(args) not in (2, 3) or (len(args) == 3 and args[2] != '--r2')):
    raise SystemExit('usage: independent_process_proof.py <bin-dir> <receipt-dir> [--r2]')

R2_MODE = not R2_VALIDATION_ONLY and len(args) == 3
BIN = Path(args[0]).resolve() if not R2_VALIDATION_ONLY else None
ROOT = Path(args[1]).resolve() if not R2_VALIDATION_ONLY else None
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


def r2_environment():
    def required(name):
        value = os.environ.get(name)
        if not value:
            raise SystemExit(f'R2 mode requires non-empty {name}')
        return value
    account = required('ARCO_TEST_R2_ACCOUNT_ID')
    bucket = required('ARCO_TEST_R2_BUCKET')
    access_key = required('AWS_ACCESS_KEY_ID')
    secret_key = required('AWS_SECRET_ACCESS_KEY')
    if len(account) != 32 or any(char not in '0123456789abcdef' for char in account):
        raise SystemExit('ARCO_TEST_R2_ACCOUNT_ID must be a lowercase 32-character hex account ID')
    if (not 3 <= len(bucket) <= 63 or bucket.startswith('-') or bucket.endswith('-')
            or any(char not in 'abcdefghijklmnopqrstuvwxyz0123456789-' for char in bucket)):
        raise SystemExit('ARCO_TEST_R2_BUCKET must be a lowercase DNS bucket name')
    for name in ('AWS_PROFILE', 'AWS_SESSION_TOKEN', 'AWS_DEFAULT_REGION', 'AWS_ENDPOINT_URL', 'AWS_ENDPOINT_URL_S3'):
        if name in os.environ:
            raise SystemExit(f'R2 mode rejects ambient {name}')
    endpoint = f'https://{account}.r2.cloudflarestorage.com'
    if required('AWS_ENDPOINT') != endpoint:
        raise SystemExit('R2 mode requires AWS_ENDPOINT to match the dedicated account endpoint')
    if required('AWS_REGION') != 'auto':
        raise SystemExit('R2 mode requires AWS_REGION=auto')
    if required('AWS_EC2_METADATA_DISABLED') != 'true':
        raise SystemExit('R2 mode requires AWS_EC2_METADATA_DISABLED=true')
    return account, bucket, access_key, secret_key, endpoint


if R2_VALIDATION_ONLY:
    r2_environment()
    raise SystemExit(0)

r2 = r2_environment() if R2_MODE else None
TENANT = 'pilot'
WORKSPACE = (f'flow-r2-{os.getpid()}-{int(time.time())}' if R2_MODE else 'flow')
ROOT.mkdir(mode=0o700, parents=True, exist_ok=False)

# Clear inherited provider and Arco configuration before assigning this profile.
env = {k: v for k, v in os.environ.items()
       if not k.startswith(('ARCO_', 'AWS_', 'GOOGLE_', 'GCP_', 'AZURE_', 'CLOUDSDK_'))
       and k not in ('PORT', 'PYTHONOPTIMIZE') and k.lower() not in ('http_proxy', 'https_proxy', 'all_proxy')}
env.update({
    'AWS_ACCESS_KEY_ID': 'local-test', 'AWS_SECRET_ACCESS_KEY': 'local-test',
    'AWS_REGION': 'us-east-1', 'AWS_ENDPOINT': S3, 'AWS_ALLOW_HTTP': 'true',
    'AWS_EC2_METADATA_DISABLED': 'true', 'ARCO_STORAGE_BUCKET': 's3://arco-local-proof',
    'ARCO_TENANT_ID': TENANT, 'ARCO_WORKSPACE_ID': WORKSPACE,
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
if R2_MODE:
    account, bucket, access_key, secret_key, endpoint = r2
    env.pop('AWS_ALLOW_HTTP', None)
    env.update({
        'AWS_ACCESS_KEY_ID': access_key,
        'AWS_SECRET_ACCESS_KEY': secret_key,
        'AWS_REGION': 'auto',
        'AWS_ENDPOINT': endpoint,
        'AWS_EC2_METADATA_DISABLED': 'true',
        'ARCO_STORAGE_BUCKET': f's3://{bucket}',
        'ARCO_FLOW_FIXTURE_NETWORK_TIMEOUT': '120',
    })

NETWORK_TIMEOUT = 120 if R2_MODE else 30
ARTIFACT_TIMEOUT = 120 if R2_MODE else 10
READINESS_TIMEOUT = 120 if R2_MODE else 30


class RejectRedirect(HTTPRedirectHandler):
    def redirect_request(self, _request, _fp, _code, _message, _headers, _new_url):
        raise HTTPError(_request.full_url, _code, 'artifact redirect rejected', _headers, _fp)


artifact_open = build_opener(ProxyHandler({}), RejectRedirect()).open


def jwt(tenant=TENANT, workspace=WORKSPACE):
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
        with urlopen(req, timeout=NETWORK_TIMEOUT) as response:
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
    deadline = time.monotonic() + READINESS_TIMEOUT
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
    return subprocess.check_output([str(BIN / 'examples/pilot_flow_process_seed'), *args], env=env,
                                   text=True, timeout=NETWORK_TIMEOUT if R2_MODE else None)


def manifest(value):
    return {'manifestVersion': '1.0', 'codeVersionId': 'code-' + str(value), 'assets': [{
        'key': {'namespace': 'analytics', 'name': 'daily'},
        'id': '019a0000-0000-7000-8000-000000000001',
        'execution': {'payload': {'sql': 'select :value', 'parameters': {'value': {'type': 'int64', 'value': value}}}},
        'resources': {'memoryMb': 64}, 'io': {'inputs': []},
    }]}


def assert_artifact_url(url):
    parsed = urlparse(url)
    if R2_MODE:
        expected_host = f'{r2[0]}.r2.cloudflarestorage.com'
        expected_prefix = f'/{r2[1]}/tenant={TENANT}/workspace={WORKSPACE}/'
        path = unquote(parsed.path)
        if (parsed.scheme != 'https' or parsed.netloc != expected_host
                or parsed.username is not None or parsed.password is not None
                or '\\' in path or any(segment in ('.', '..') for segment in path.split('/'))
                or not path.startswith(expected_prefix)):
            raise AssertionError('R2 artifact URL left the dedicated bucket workspace scope')
    else:
        assert parsed.hostname == '127.0.0.1', parsed


def task_jwt(task_id, run_id, attempt, attempt_id, tenant=TENANT, workspace=WORKSPACE):
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).rstrip(b'=')
    payload = {
        'taskId': task_id, 'tenantId': tenant, 'workspaceId': workspace,
        'runId': run_id, 'attempt': attempt, 'attemptId': attempt_id,
        'iss': 'reference', 'aud': 'arco-worker',
        'exp': int(time.time()) + 3600, 'iat': int(time.time()), 'nbf': int(time.time()) - 1,
    }
    message = encode({'alg': 'HS256', 'typ': 'JWT'}) + b'.' + encode(payload)
    signature = base64.urlsafe_b64encode(hmac.digest(TASK_SECRET.encode(), message, 'sha256')).rstrip(b'=')
    return (message + b'.' + signature).decode()


def queued_task_for(run_id):
    task_file = next((ROOT / 'queue/pending').glob('*.json'))
    envelope = json.loads(json.loads(task_file.read_text())['body'])
    assert envelope['runId'] == run_id
    return envelope


def drain_task_for(run_id):
    envelope = queued_task_for(run_id)
    assert request(CONTROL + '/drain', {})['delivered'] == 1
    return envelope


def dispatch_task_for(run_id):
    enqueue_task_for(run_id)
    return drain_task_for(run_id)


def enqueue_task_for(run_id):
    emitted = request(DISPATCHER + '/run', {})
    assert emitted['ready_dispatch_emitted'] == 1 and emitted['dispatch_enqueued'] == 0 and emitted['dispatch_failed'] == 0, emitted
    enqueued = request(DISPATCHER + '/run', {})
    assert enqueued['dispatch_enqueued'] == 1 and enqueued['dispatch_failed'] == 0, enqueued
    return queued_task_for(run_id)


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
                                        SOURCE / 'crates/arco-flow/examples/pilot_flow_process_seed.rs',
                                        SOURCE / 'crates/arco-flow/tests/operator_fixture_contract.py')}
    if R2_MODE:
        receipt['providerQualification'] = 'in-progress'
        receipt['provider'] = {
            'profile': 'r2-static-s3', 'accountId': r2[0], 'bucket': r2[1],
            'workspace': WORKSPACE, 'prefix': f'tenant={TENANT}/workspace={WORKSPACE}',
        }
    else:
        import boto3
        from botocore.config import Config
        start('s3', [str(Path(sys.executable).with_name('moto_server')), '-H', '127.0.0.1', '-p', '5190'])
        ready(S3)
        boto3.client('s3', endpoint_url=S3, aws_access_key_id='local-test',
                     aws_secret_access_key='local-test', region_name='us-east-1',
                     config=Config(proxies={})).create_bucket(Bucket='arco-local-proof')
    start('fixture', [sys.executable, str(SOURCE / 'crates/arco-flow/tests/pilot_flow_http_fixture.py')])
    ready(CONTROL + '/state')
    api = start('api', [str(BIN / 'arco-api')])
    ready(API + '/health')
    dispatcher = start('dispatcher', [str(BIN / 'arco_flow_dispatcher')], {'ARCO_FLOW_PORT': '5188'})
    start('sweeper', [str(BIN / 'arco_flow_sweeper')], {'ARCO_FLOW_PORT': '5189'})
    ready(DISPATCHER + '/health')
    ready(SWEEPER + '/health')
    token = jwt()
    request(API + '/api/v1/namespaces', {'name': 'catalog-proof'}, token, 201)
    read = request(API + '/internal/control-store/catalog-projection/urls', {}, token)
    assert read['descriptorVersion'] == 1 and len(read['files']) == 4
    assert read['scope'] == {'tenantId': TENANT, 'workspaceId': WORKSPACE, 'domain': 'catalog'}
    for file in read['files']:
        assert_artifact_url(file['url'])
        with artifact_open(file['url'], timeout=ARTIFACT_TIMEOUT) as response:
            data = response.read()
        assert len(data) == file['byteSize'] and hashlib.sha256(data).hexdigest() == file['checksumSha256']
        assert data[:4] == b'PAR1' and data[-4:] == b'PAR1'
        with artifact_open(Request(file['url'], headers={'Range': 'bytes=0-3'}), timeout=ARTIFACT_TIMEOUT) as response:
            assert response.status == 206 and response.read() == b'PAR1'
    receipt['cases']['pinnedCatalogHttpRead'] = 'passed'
    request(API + '/api/v1/namespaces', {'name': 'catalog-advanced-head'}, token, 201)
    for file in read['files']:
        with artifact_open(file['url'], timeout=ARTIFACT_TIMEOUT) as response:
            data = response.read()
        assert len(data) == file['byteSize'] and hashlib.sha256(data).hexdigest() == file['checksumSha256']
    receipt['cases']['retainedPinnedCatalogReadAfterAdvancedHead'] = 'passed'
    manifest_a = request(API + f'/api/v1/workspaces/{WORKSPACE}/manifests', manifest(7), token, 201)
    trigger = {'selection': ['analytics.daily'], 'runKey': 'reference-frozen-a'}
    run = request(API + f'/api/v1/workspaces/{WORKSPACE}/runs', trigger, token, 201)
    (ROOT / 'queue/mode').write_text('uncertain')
    planned = request(DISPATCHER + '/run', {})
    assert planned['ready_dispatch_emitted'] == 1 and planned['dispatch_actions'] == 0
    uncertain = request(DISPATCHER + '/run', {}, expected=500)
    assert uncertain['dispatch_failed'] == 1
    assert request(CONTROL + '/state')['pending'] == 1
    request(API + f'/api/v1/workspaces/{WORKSPACE}/manifests', manifest(9), token, 201)
    api.terminate()
    api.wait(timeout=5)
    processes.remove(api)
    api = start('api-restarted', [str(BIN / 'arco-api')])
    ready(API + '/health')
    dispatcher.terminate()
    dispatcher.wait(timeout=5)
    processes.remove(dispatcher)
    start('dispatcher-restarted', [str(BIN / 'arco_flow_dispatcher')], {'ARCO_FLOW_PORT': '5188'})
    ready(DISPATCHER + '/health')
    replay = request(API + f'/api/v1/workspaces/{WORKSPACE}/runs', trigger, token)
    assert replay['runId'] == run['runId']
    (ROOT / 'queue/mode').write_text('accept')
    recovered = request(DISPATCHER + '/run', {})
    assert recovered['dispatch_deduplicated'] == 1
    witness = json.loads(next((ROOT / 'queue').glob('duplicate-*.json')).read_text())
    assert witness['incomingIntentSha256'] == witness['storedIntentSha256']
    envelope = drain_task_for(run['runId'])
    received = json.loads(next((ROOT / 'queue').glob('received-*.json')).read_text())
    assert received['result'] == 7 and received['manifestId'] == manifest_a['manifestId']
    publication = request(API + '/api/v1/tasks/' + envelope['taskId'] + '/publication', token=envelope['taskToken'])
    assert publication['publication']['ownerEvidence'] is not None
    request(API + '/api/v1/tasks/' + envelope['taskId'] + '/publication', token='invalid-task-token', expected=401)
    request(API + '/api/v1/tasks/' + envelope['taskId'] + '/publication',
            token=task_jwt(envelope['taskId'], envelope['runId'], envelope['attempt'] + 1, 'stale-attempt'), expected=401)
    request(API + '/api/v1/tasks/' + envelope['taskId'] + '/publication',
            token=task_jwt(envelope['taskId'], envelope['runId'], envelope['attempt'], envelope['attemptId'],
                           tenant='other-tenant'), expected=404)
    request(API + '/api/v1/tasks/' + envelope['taskId'] + '/publication',
            token=task_jwt(envelope['taskId'], envelope['runId'], envelope['attempt'], envelope['attemptId'],
                           workspace='other-workspace'), expected=404)
    receipt['scopeCases'] = {
        'wrongWorkspaceRunRead': 'pending', 'crossTenantRunRead': 'pending',
        'taskWrongTenant': 'executed-404', 'taskWrongWorkspace': 'executed-404',
        'taskMetastore': 'unsupported-by-task-token-contract',
    }
    receipt['cases']['invalidAndStaleTaskAuthorization'] = 'passed'
    completed = request(API + f'/api/v1/workspaces/{WORKSPACE}/runs/' + run['runId'], token=token)
    assert completed['state'] == 'SUCCEEDED'
    request(API + f'/api/v1/workspaces/{WORKSPACE}/runs/' + run['runId'],
            token=jwt(workspace='other-workspace'), expected=404)
    receipt['scopeCases']['wrongWorkspaceRunRead'] = 'executed-404'
    request(API + f'/api/v1/workspaces/{WORKSPACE}/runs/' + run['runId'],
            token=jwt(tenant='other-tenant'), expected=404)
    receipt['scopeCases']['crossTenantRunRead'] = 'executed-404'
    receipt['cases']['frozenPayloadAfterDeployment'] = 'passed'
    receipt['cases']['uncertainAcceptanceRecovery'] = 'passed'
    receipt['cases']['dispatcherRestart'] = 'passed'
    receipt['cases']['apiRestartAfterAcceptedIntent'] = 'passed'
    receipt['cases']['ownerVerifiedPublication'] = 'passed'
    request(CONTROL + '/publication-mode', {'mode': 'missing'})
    missing_run = request(API + f'/api/v1/workspaces/{WORKSPACE}/runs', {
        'selection': ['analytics.daily'], 'runKey': 'reference-publication-missing'}, token, 201)
    missing_envelope = dispatch_task_for(missing_run['runId'])
    request(API + '/api/v1/tasks/' + missing_envelope['taskId'] + '/publication',
            token=missing_envelope['taskToken'], expected=409)
    receipt['cases']['publicationUnavailableFailsClosed'] = 'passed'
    request(CONTROL + '/publication-mode', {'mode': 'corrupt'})
    corrupt_run = request(API + f'/api/v1/workspaces/{WORKSPACE}/runs', {
        'selection': ['analytics.daily'], 'runKey': 'reference-publication-corrupt'}, token, 201)
    request(CONTROL + '/callback-ack-mode', {'mode': 'discard-completed'})
    corrupt_envelope = enqueue_task_for(corrupt_run['runId'])
    discarded_ack = request(CONTROL + '/drain', {}, expected=502)
    assert discarded_ack == {'delivered': 0, 'apiAccepted': True, 'wireLoss': False}
    request(CONTROL + '/callback-ack-mode', {'mode': 'normal'})
    request(API + '/api/v1/tasks/' + corrupt_envelope['taskId'] + '/publication',
            token=corrupt_envelope['taskToken'], expected=409)
    computes_before_replay = request(CONTROL + '/state')['computeCount']
    output_value = corrupt_envelope['payload']['asset']['execution']['payload']['parameters']['value']['value']
    callback_path = ROOT / 'queue' / ('callback-' + hashlib.sha256(corrupt_envelope['taskId'].encode()).hexdigest() + '.json')
    saved_descriptor = json.loads(callback_path.read_text())['payload']['output']['publication']
    seed('repair-output', corrupt_run['runId'], str(output_value))
    request(CONTROL + '/replay', {'taskId': corrupt_envelope['taskId']})
    assert request(CONTROL + '/state')['computeCount'] == computes_before_replay
    repaired_publication = request(API + '/api/v1/tasks/' + corrupt_envelope['taskId'] + '/publication',
                                   token=corrupt_envelope['taskToken'])
    assert repaired_publication['publication']['ownerEvidence'] is not None
    final_descriptor = dict(repaired_publication['publication'])
    final_descriptor.pop('ownerEvidence')
    assert final_descriptor == saved_descriptor
    changed_replay = request(CONTROL + '/replay', {
        'taskId': corrupt_envelope['taskId'], 'changeImmutableField': 'objectPath'})
    assert changed_replay == {'replayed': False, 'status': 409}
    unchanged_publication = request(API + '/api/v1/tasks/' + corrupt_envelope['taskId'] + '/publication',
                                    token=corrupt_envelope['taskToken'])['publication']
    assert unchanged_publication.pop('ownerEvidence') is not None
    assert unchanged_publication == saved_descriptor
    assert request(CONTROL + '/state')['computeCount'] == computes_before_replay
    receipt['cases']['publicationCorruptionRepairAndClientAckDiscardReplay'] = 'passed'
    receipt['failureModel'] = {'publicationReplay': {
        'clientBoundaryAcknowledgmentDiscarded': True, 'wireLoss': False,
    }}
    request(CONTROL + '/publication-mode', {'mode': 'normal'})
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
    if R2_MODE:
        receipt['nativeIntegrationProfile'] = 'passed'
        receipt['providerQualification'] = 'evidence-pending-review'
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
