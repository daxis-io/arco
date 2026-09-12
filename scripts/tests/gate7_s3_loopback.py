#!/usr/bin/env python3
"""Run the actual Gate 7 S3 executable against a contained local HTTP fixture.

This is protocol test support, not an S3 emulator or provider qualification.
"""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import shutil
import ssl
from pathlib import Path
import subprocess
import threading
import urllib.parse
import xml.etree.ElementTree as ET


def run(executable, evidence, provider_source=None, listing_probe=None, tls=False):
    objects = {}
    trace = []
    lock = threading.Lock()
    namespace = set()
    attempts = {}
    pending = {}
    connections = 0

    expected_tenant = None
    if provider_source is not None:
        import uuid
        expected_tenant = 'gate7-' + uuid.uuid4().hex
    if listing_probe:
        assert provider_source is not None
        objects[f'tenant={expected_tenant}/workspace=qualification-r0/conformance/proxy-probe/key'] = (b'x', '"probe"')

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def setup(self):
            nonlocal connections
            super().setup()
            with lock:
                connections += 1
                self.connection_id = connections

        def log_message(self, *_):
            pass

        def respond(self, code, body=b'', headers=None, head=False):
            self.send_response(code)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Connection', 'keep-alive' if listing_probe else 'close')
            self.end_headers()
            if not head:
                self.wfile.write(body)
            self.close_connection = not listing_probe

        def error(self, code, name):
            body = f'<Error><Code>{name}</Code><Message>loopback fixture</Message></Error>'.encode()
            self.respond(code, body, {'Content-Type': 'application/xml'})

        def dispatch(self):
            parsed = urllib.parse.urlsplit(self.path)
            path = urllib.parse.unquote(parsed.path)
            query = urllib.parse.parse_qs(parsed.query)
            key = path.removeprefix('/gate7-loopback/').lstrip('/')
            listing = self.command == 'GET' and query.get('list-type') == ['2']
            addressed = query.get('prefix', [''])[0] if listing else key
            with lock:
                allowed = (addressed.startswith('tenant=gate7-') and '/workspace=qualification/' in addressed) if provider_source is None else any(
                    addressed.startswith(f'tenant={expected_tenant}/workspace=qualification-r{r}/') for r in range(5))
                if not allowed:
                    trace.append(dict(method=self.command, path=path, query=query, rejected=True))
                    self.error(403, 'AccessDenied')
                    return
                namespace.add(addressed.split('/workspace=')[0])
                if len(namespace) != 1:
                    self.error(403, 'AccessDenied')
                    return
                trace.append(dict(method=self.command, path=path, query=query,
                                  connection=self.connection_id,
                                  alpn=self.connection.selected_alpn_protocol() if tls else None,
                                  if_match=self.headers.get('If-Match'),
                                  if_none_match=self.headers.get('If-None-Match')))
                attempt_key = (self.command, key)
                attempts[attempt_key] = attempts.get(attempt_key, 0) + 1
                attempt = attempts[attempt_key]
                fault = key.rsplit('/conformance/fault/', 1)[-1] if '/conformance/fault/' in key else None
                if self.command == 'GET' and fault == 'read-retry' and attempt == 1:
                    trace[-1]['injected_status'] = 503
                    self.error(503, 'SlowDown')
                    return
                if self.command == 'HEAD' and key in pending:
                    objects[key] = pending.pop(key)
                    trace[-1]['released_pending_application'] = True
                if self.command == 'PUT' and fault and attempt == 1:
                    body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                    names = {'403': 'AccessDenied', '404': 'NoSuchKey', '408': 'RequestTimeout',
                             '409': 'ConditionalRequestConflict', '412': 'PreconditionFailed',
                             '503': 'SlowDown'}
                    if fault in ('307', '308'):
                        trace[-1]['injected_status'] = int(fault)
                        self.respond(int(fault), headers={'Location': self.path + '?redirected=1'}); return
                    if fault in names:
                        trace[-1]['injected_status'] = int(fault)
                        self.error(int(fault), names[fault])
                        return
                    etag = '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'
                    if fault == 'lost-response':
                        objects[key] = (body, etag)
                        trace[-1]['injected_lost_response_after_application'] = True
                        self.close_connection = True
                        return
                    if fault == 'delayed-application':
                        pending[key] = (body, etag)
                        trace[-1]['injected_error_before_application'] = True
                        self.error(503, 'SlowDown')
                        return
                    # Ordinary read-retry setup has already consumed its request body.
                    objects[key] = (body, etag)
                    self.respond(200, headers={'ETag': etag})
                    return
                if listing:
                    if provider_source is not None and self.headers.get('Connection', '').lower() != 'close':
                        trace[-1]['missing_single_request_connection'] = True
                        self.error(400, 'MissingConnectionClose')
                        return
                    prefix = query['prefix'][0]
                    probe_attempt = attempts.get(('probe', prefix), 0) + 1
                    attempts[('probe', prefix)] = probe_attempt
                    if listing_probe == 'retry' and probe_attempt == 1:
                        self.error(503, 'SlowDown'); return
                    if listing_probe == 'redirect' and probe_attempt == 1:
                        self.respond(307, headers={'Location': self.path + '&redirected=1'}); return
                    if listing_probe == 'body-error':
                        self.send_response(200); self.send_header('Content-Length', '1000'); self.end_headers()
                        self.wfile.write(b'<truncated'); self.close_connection = True; return
                    after = query.get('start-after', query.get('continuation-token', ['']))[0]
                    maximum = min(1000, int(query.get('max-keys', ['1000'])[0]))
                    keys = sorted(k for k in objects if k.startswith(prefix) and k > after)
                    page = keys[:maximum]
                    empty = listing_probe == 'cap' or listing_probe == 'empty' and probe_attempt <= 3
                    if empty:
                        page = []
                    root = ET.Element('ListBucketResult', xmlns='http://s3.amazonaws.com/doc/2006-03-01/')
                    for name, value in [('Name', 'gate7-loopback'), ('Prefix', prefix),
                                        ('KeyCount', str(len(page))), ('MaxKeys', str(maximum)),
                                        ('IsTruncated', str(empty or len(keys) > len(page)).lower())]:
                        ET.SubElement(root, name).text = value
                    if empty or len(keys) > len(page):
                        ET.SubElement(root, 'NextContinuationToken').text = f'empty-{probe_attempt}' if empty else page[-1]
                    for item in page:
                        value, etag = objects[item]
                        node = ET.SubElement(root, 'Contents')
                        for name, data in [('Key', item), ('LastModified', '2026-09-10T00:00:00.000Z'),
                                           ('ETag', etag), ('Size', str(len(value))), ('StorageClass', 'STANDARD')]:
                            ET.SubElement(node, name).text = data
                    trace[-1]['returned_objects'] = len(page)
                    self.respond(200, ET.tostring(root), {'Content-Type': 'application/xml'})
                elif self.command == 'PUT':
                    length = int(self.headers.get('Content-Length', '0'))
                    if not 0 <= length <= 64 * 1024**2:
                        self.error(413, 'EntityTooLarge')
                        return
                    body = self.rfile.read(length)
                    existing = objects.get(key)
                    if self.headers.get('If-None-Match') == '*' and existing:
                        self.error(412, 'PreconditionFailed')
                    elif self.headers.get('If-Match') and not existing:
                        self.error(404, 'NoSuchKey')
                    elif self.headers.get('If-Match') and self.headers['If-Match'] != existing[1]:
                        self.error(412, 'PreconditionFailed')
                    else:
                        etag = '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'
                        objects[key] = (body, etag)
                        self.respond(200, headers={'ETag': etag})
                elif self.command == 'DELETE':
                    objects.pop(key, None)
                    self.respond(204)
                elif self.command in ('HEAD', 'GET'):
                    if key not in objects:
                        self.error(404, 'NoSuchKey')
                        return
                    body, etag = objects[key]
                    headers = {'ETag': etag, 'Last-Modified': 'Thu, 10 Sep 2026 00:00:00 GMT'}
                    code = 200
                    if self.headers.get('Range'):
                        first, last = self.headers['Range'].removeprefix('bytes=').split('-')
                        first, last = int(first), int(last)
                        if first >= len(body) or last < first:
                            self.error(416, 'InvalidRange')
                            return
                        last = min(last, len(body) - 1)
                        headers['Content-Range'] = f'bytes {first}-{last}/{len(body)}'
                        body = body[first:last+1]
                        code = 206
                    self.respond(code, body, headers, head=self.command == 'HEAD')
                else:
                    self.error(400, 'InvalidRequest')

        do_GET = do_HEAD = do_PUT = do_DELETE = dispatch

    evidence.mkdir(parents=True, exist_ok=False)
    frozen_executable = evidence / 'executable'
    shutil.copy2(executable, frozen_executable)
    frozen_executable.chmod(0o555)
    executable = frozen_executable.resolve()
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    if tls:
        assert provider_source is not None
        certificate, key = evidence / 'fixture-cert.pem', evidence / 'fixture-key.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-days', '1', '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1',
                        '-keyout', str(key), '-out', str(certificate)], check=True, capture_output=True)
        key.chmod(0o600)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.set_alpn_protocols(['h2', 'http/1.1'])
        context.load_cert_chain(certificate, key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # Do not inherit profile, web identity, metadata endpoint or real credentials.
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('AWS_', 'ARCO_')) and key.lower() not in ('http_proxy', 'https_proxy', 'all_proxy')}
    environment.update(AWS_ENDPOINT=f'http://127.0.0.1:{server.server_port}', AWS_ALLOW_HTTP='true',
                       AWS_ACCESS_KEY_ID='gate7-test', AWS_SECRET_ACCESS_KEY='gate7-test',
                       AWS_REGION='us-east-1', AWS_EC2_METADATA_DISABLED='true')
    if tls:
        environment.update(AWS_ENDPOINT=f'https://127.0.0.1:{server.server_port}', SSL_CERT_FILE=str(certificate))
    command = [str(executable), 'loopback']
    proxy = None
    if provider_source is not None:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from gate7_listing_proxy import ListingProxy
        used = 0
        def reserve(size):
            nonlocal used
            if used + size > 64 * 1024**2:
                raise ValueError('fixture proxy evidence ceiling')
            used += size
        proxy = ListingProxy(f'127.0.0.1:{server.server_port}', 3 if listing_probe == 'cap' else 100_000,
                             evidence / 'listing-connections.jsonl', reserve, loopback=not tls, tls_fixture=tls)
        command = provider_command(executable, evidence, provider_source, expected_tenant, proxy.url)
        if listing_probe:
            command[1] = 'provider-rehearsal-' + listing_probe if listing_probe.startswith('put-') else 'provider-rehearsal-listing-probe'
        environment['AWS_REGION'] = 'us-east-2'
    try:
        with (evidence / 'observations.jsonl').open('w') as output, (evidence / 'stderr.log').open('w') as errors:
            result = subprocess.run(command, env=environment, stdout=output,
                                    stderr=errors, timeout=1800)
        (evidence / 'exit-status.txt').write_text(str(result.returncode))
    finally:
        if proxy is not None:
            proxy.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        (evidence / 'transport.json').write_text(json.dumps(trace, indent=2))
    if listing_probe:
        assert not any(row.get('rejected') for row in trace)
        if tls:
            assert trace and all(row['alpn'] == 'http/1.1' for row in trace), 'qualification must negotiate HTTP/1.1'
        if listing_probe.startswith('put-'):
            assert len(trace) == 1 and trace[0]['method'] == 'PUT' and proxy.admitted == 0
        else:
            assert len(trace) <= proxy.admitted
        assert len({row['connection'] for row in trace}) == len(trace), 'SDK reused a listing connection'
        if listing_probe in ('cap', 'body-error', 'redirect', 'put-307', 'put-308'):
            assert result.returncode != 0
        else:
            assert result.returncode == 0, (evidence / 'stderr.log').read_text()
        if listing_probe == 'cap':
            assert len(trace) == proxy.admitted == 3 and proxy.failure
        if listing_probe == 'empty':
            assert len(trace) == 4
        summary = dict(probe=listing_probe, provider=False, exit_code=result.returncode,
                       tls=tls, transport_requests=len(trace), admitted=proxy.admitted, proxy_failure=proxy.failure)
        (evidence / 'summary.json').write_text(json.dumps(summary, indent=2)); print(json.dumps(summary))
        return
    assert result.returncode == 0, (evidence / 'stderr.log').read_text()
    assert trace and not any(item.get('rejected') for item in trace)
    assert all(item.get('returned_objects', 0) <= 1000 for item in trace)
    assert len(namespace) == 1
    fault_attempts = {f'{method} {key.rsplit("/conformance/fault/", 1)[1]}': count
                      for (method, key), count in attempts.items() if '/conformance/fault/' in key}
    if provider_source is None:
        for status in ('403', '404', '408', '409', '412', '503'):
            assert fault_attempts[f'PUT {status}'] == 1, fault_attempts
        assert fault_attempts['GET read-retry'] == 2, fault_attempts
    assert not pending
    (evidence / 'fault-attempts.json').write_text(json.dumps(fault_attempts, indent=2))
    summary = dict(adapter='S3StorageBackend', provider=False, transport_requests=len(trace),
                   disposable_namespace=next(iter(namespace)) + '/workspace=qualification/',
                   max_list_response=max(item.get('returned_objects', 0) for item in trace),
                   retained_objects=len(objects), object_bytes=sum(len(data) for data, _ in objects.values()))
    if provider_source is not None:
        state = json.loads((evidence / 'run/state.json').read_text())
        assert state['state'] == 'completed-evidence-pending-validation', state
        assert len(trace) <= state['counters']['requests_upper_bound'] <= 2000000
        assert state['counters']['in_flight'] == 0
        assert state['counters']['transport_attempts'] is None
        summary['requests_upper_bound'] = state['counters']['requests_upper_bound']
        summary['listing_connections_admitted'] = proxy.admitted
        assert proxy.failure is None
        assert sum(row['query'].get('list-type') == ['2'] for row in trace) <= proxy.admitted
        summary['disposable_namespaces'] = [f'tenant={expected_tenant}/workspace=qualification-r{r}/' for r in range(5)]
        summary.pop('disposable_namespace')
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from gate7_qualification import cold_summary
        rows = [json.loads(line) for line in (evidence / 'observations.jsonl').read_text().splitlines()]
        cold = [row for row in rows if row.get('kind') == 'cold-observation']
        summary['cold_summary'] = cold_summary(cold)
        assert len([row for row in rows if row.get('kind') == 'completed']) == 5
        journal = [json.loads(line) for line in (evidence / 'run/operations.jsonl').read_text().splitlines()]
        assert len([row for row in journal if row.get('kind') == 'client-publication-reconciled']) == 5
    (evidence / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary))


def provider_command(executable, evidence, source, tenant, listing_proxy):
    """Bind the actual candidate and executable for a credential-free provider rehearsal."""
    import datetime
    import stat
    source = source.resolve(strict=True)
    def sha(path):
        with path.open('rb') as stream:
            return hashlib.file_digest(stream, 'sha256').hexdigest()
    def artifact(path):
        return dict(path=str(path.resolve()), sha256=sha(path))
    files = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=source).decode().split('\0')[:-1]
    manifest = {name:dict(sha256=sha(source / name), size=(source / name).stat().st_size,
                         mode=oct(stat.S_IMODE((source / name).stat().st_mode))) for name in sorted(set(files))}
    source_manifest = evidence / 'source-manifest.json'
    source_manifest.write_text(json.dumps(manifest, sort_keys=True, indent=2))
    patch = subprocess.check_output(['git', 'diff', '--binary', 'HEAD'], cwd=source)
    new = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=source).decode().split('\0')[:-1]
    for name in new:
        result = subprocess.run(['git', 'diff', '--no-index', '--binary', '--', '/dev/null', name], cwd=source, capture_output=True)
        assert result.returncode == 1
        patch += result.stdout
    patch_file = evidence / 'candidate.patch'; patch_file.write_bytes(patch)
    snapshot = evidence / 'source'
    snapshot.mkdir()
    for name in manifest:
        target = snapshot / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / name, target)
    source = snapshot.resolve()
    build = evidence / 'build-inputs.json'
    build.write_text(json.dumps(dict(rust='1.88.0', target='credential-free-rehearsal', cargo_lock_sha256=sha(source / 'Cargo.lock'))))
    inventory = json.loads(subprocess.check_output([str(executable), 'inventory']))['provider']
    config = dict(schema=1, phase='provider', run_id=tenant, base_sha='92fd19f11a547ece5004ac94cad83a3527471812',
                  source_root=str(source), source_manifest=artifact(source_manifest), binary_patch=artifact(patch_file),
                  contract=artifact(source / 'docs' / 'plans' / '2026-09-10-state-store-vnext-gate-7.md'), build_inputs=artifact(build),
                  supervisor=artifact(source / 'scripts/gate7_provider.py'),
                  executable_sha256=sha(executable), scenarios=inventory, account='012832591253', role='arco-gate7-rehearsal',
                  instance_id='i-rehearsal', region='us-east-2', bucket='gate7-loopback', tenant=tenant, workspace='qualification',
                  evidence_dir=str((evidence / 'run').resolve()),
                  listing_proxy=listing_proxy, reserved_listing_requests=100_000,
                  expires_utc=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=6)).isoformat(),
                  ceilings=dict(elapsed_seconds=21600, requests=2000000, submitted_bytes=20*1024**3, cost_microusd=25000000, evidence_bytes=512*1024**2),
                  fixed_cost_microusd=13000000, request_cost_microusd=5, stored_byte_cost_picousd=44,
                  aws_cli=artifact(Path('/usr/bin/true')))
    path = evidence / 'provider-rehearsal.json'; path.write_text(json.dumps(config, indent=2))
    return [str(executable), 'provider-rehearsal', str(path.resolve()), sha(path)]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('executable', type=Path)
    parser.add_argument('evidence', type=Path)
    parser.add_argument('--provider-source', type=Path)
    parser.add_argument('--listing-probe', choices=['empty', 'retry', 'redirect', 'body-error', 'cap', 'put-307', 'put-308'])
    parser.add_argument('--tls', action='store_true')
    args = parser.parse_args()
    run(args.executable.resolve(strict=True), args.evidence.resolve(), args.provider_source, args.listing_probe, args.tls)
