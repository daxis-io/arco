"""Bounded-provider control checks; these do not authorize provider traffic."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import socket
import unittest
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gate7_provider as provider
import gate7_listing_proxy as listing


class ProviderTests(unittest.TestCase):
    def test_running_unit_drift_rejects_before_stop(self):
        with patch.object(provider, 'unit_contract', side_effect=ValueError('unit drift')), \
             patch.object(provider.subprocess, 'run') as calls:
            with self.assertRaisesRegex(ValueError, 'unit drift'):
                provider.verify_running_unit(dict(unit='arco-gate7-test.service',
                                                   unit_contract={'ExecStart': ['bound', 'command']}))
            calls.assert_not_called()

    def test_terminal_evidence_closure_rejects_changed_or_lost_files(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory) / 'run'; evidence.mkdir()
            supervision = evidence.with_name('run-supervision'); supervision.mkdir()
            for root, names in ((evidence, ('manifest.json', 'manifest.sha256', 'operations.jsonl', 'state.json')),
                                (supervision, ('verified-execution.json', 'telemetry.jsonl', 'listing-connections.jsonl',
                                               'stdout.jsonl', 'stderr.log', 'proxy-bound.json'))):
                for name in names:
                    (root / name).write_text('original')
            closure = provider.evidence_closure(evidence)
            self.assertEqual(provider.evidence_closure(evidence), closure)
            for path in list(evidence.iterdir()) + list(supervision.iterdir()):
                path.write_text('changed')
                self.assertNotEqual(provider.evidence_closure(evidence), closure)
                path.unlink()
                with self.assertRaises(OSError):
                    provider.evidence_closure(evidence)
                path.write_text('original')

    def test_runtime_hash_mismatch_rejects_without_invoking_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'fake-worker'; binary.write_text('unapproved executable')
            with patch.object(provider.subprocess, 'run') as calls:
                with self.assertRaisesRegex(ValueError, 'executable'):
                    provider.verify_runtime(dict(executable_sha256='0' * 64), binary)
                calls.assert_not_called()

    def test_supervisor_interpreter_path_hash_and_version_are_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'worker'; binary.write_text('reviewed binary')
            interpreter = Path(sys.executable).resolve()
            build = root / 'build.json'
            original = dict(supervisor_python=dict(path=str(interpreter), sha256=provider.digest(interpreter)),
                            supervisor_python_version=sys.version)
            config = dict(executable_sha256=provider.digest(binary),
                          supervisor=dict(path=str(Path(provider.__file__).resolve()), sha256=provider.digest(provider.__file__)))
            for key in (None, 'path', 'sha256', 'version'):
                record = copy.deepcopy(original)
                if key == 'version':
                    record['supervisor_python_version'] = '3.12.invalid'
                elif key:
                    record['supervisor_python'][key] = 'incorrect'
                build.write_text(json.dumps(record))
                config['build_inputs'] = dict(path=str(build), sha256=provider.digest(build))
                if key:
                    with self.subTest(key=key), self.assertRaises(ValueError):
                        provider.verify_runtime(config, binary)
                else:
                    provider.verify_runtime(config, binary)

    def test_cgroup_resource_counters_include_all_devices(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in {'cpu.stat': 'usage_usec 20000000\nuser_usec 15000000\n',
                                'memory.current': '150000000\n', 'pids.current': '6\n',
                                'io.stat': '8:0 rbytes=10 wbytes=20 rios=1\n8:1 rbytes=30 wbytes=40 rios=2\n'}.items():
                (root / name).write_text(value)
            self.assertEqual(provider.cgroup_sample(root), dict(cpu_seconds=20, memory_bytes=150000000,
                                                              pids=6, read_bytes=40, write_bytes=60))

    def test_listing_deadlines_rechecked_after_blocking_io(self):
        for boundary in ('connect-monotonic', 'connect-utc', 'ready-utc'):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory, socket.socket() as upstream:
                upstream.bind(('127.0.0.1', 0)); upstream.listen(); upstream.settimeout(2)
                clocks = [10.0, 100.0]
                connect = listing.socket.create_connection
                select = listing.select.select
                def late_connect(*args, **kwargs):
                    result = connect(*args, **kwargs)
                    if boundary == 'connect-monotonic':
                        clocks[0] = 20.0
                    elif boundary == 'connect-utc':
                        clocks[1] = 120.0
                    return result
                def late_ready(*args, **kwargs):
                    result = select(*args, **kwargs)
                    if result[0]:
                        clocks[1] = 120.0
                    return result
                with patch.object(listing.time, 'monotonic', side_effect=lambda: clocks[0]), \
                     patch.object(listing.time, 'time', side_effect=lambda: clocks[1]), \
                     patch.object(listing.socket, 'create_connection', side_effect=late_connect), \
                     patch.object(listing.select, 'select', side_effect=late_ready if boundary == 'ready-utc' else select):
                    authority = f'127.0.0.1:{upstream.getsockname()[1]}'
                    proxy = listing.ListingProxy(authority, 2, Path(directory) / 'admissions', lambda _: None,
                                                 tls_fixture=True, deadline=15, expires_utc=110)
                    try:
                        with socket.socket() as client:
                            client.settimeout(2); client.connect(('127.0.0.1', proxy.server.server_port))
                            client.sendall(f'CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n'.encode())
                            with upstream.accept()[0] as peer:
                                if boundary == 'ready-utc':
                                    header = b''
                                    while not header.endswith(b'\r\n\r\n'):
                                        header += client.recv(1)
                                    self.assertIn(b'200 Connection Established', header)
                                    peer.sendall(b'late response')
                                self.assertEqual(client.recv(1024), b'')
                        self.assertEqual(proxy.failure, 'listing deadline')
                        self.assertEqual(proxy.admitted, 1)
                    finally:
                        proxy.close()

    def test_every_systemd_contract_property_is_checked(self):
        values = dict(Type='exec', KillMode='mixed', Restart='no', TimeoutStopUSec='1min 15s',
                      ExecStop='', ExecStopPost='', KillSignal='15', SendSIGKILL='yes',
                      TimeoutStopFailureMode='terminate',
                      ExecCondition='', ExecStartPre='', ExecStartPost='', DropInPaths='', OnFailure='',
                      CPUAccounting='yes', IOAccounting='yes', MemoryAccounting='yes', TasksAccounting='yes',
                      NoNewPrivileges='yes', MemoryMax=str(24 * 1024**3), TasksMax='64',
                      ExecStart='{ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 supervisor.py ; ignore_errors=no ; }')
        def check(fields):
            with patch.object(provider.subprocess, 'run', return_value=SimpleNamespace(
                    stdout='\n'.join(f'{key}={value}' for key, value in fields.items()))):
                provider.unit_contract('arco-gate7-test.service', ['/usr/bin/python3', 'supervisor.py'])
        check(values)
        for key in values:
            broken = dict(values, **{key: 'invalid'})
            with self.subTest(key=key), self.assertRaises(ValueError):
                check(broken)

    def test_start_rejects_unit_hooks_before_systemctl_start(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            unit = root / 'arco-gate7-test.service'; unit.write_text('bound unit')
            manifest = root / 'manifest.json'
            config = dict(schema=1, phase='provider', run_id='gate7-test', evidence_dir=str(root / 'run'))
            manifest.write_text(json.dumps(config))
            with patch.object(provider, 'systemd_status', return_value=dict(active_state='inactive', children=[])), \
                 patch.object(provider, 'unit_contract', side_effect=ValueError('unsafe start hook')), \
                 patch.object(provider.subprocess, 'run') as calls:
                with self.assertRaisesRegex(ValueError, 'unsafe start hook'):
                    provider.start(manifest, provider.digest(manifest), root / 'binary', unit, provider.digest(unit))
                self.assertFalse(any(call.args[0][:2] == ['systemctl', 'start'] for call in calls.call_args_list))

    def test_evidence_admission_rejects_before_exceeding_partition(self):
        for size in (1024, 4 * 1024**2, 513 * 1024**2):
            with self.assertRaises(ValueError):
                provider.SupervisorEvidenceBudget(size)
        budget = provider.SupervisorEvidenceBudget(8 * 1024**2)
        budget.reserve(budget.limit)
        with self.assertRaises(ValueError):
            budget.reserve(1)
        self.assertEqual(budget.limit, budget.used)

    def test_rehearsal_identity_never_verifies_as_provider(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = dict(schema=1, phase='provider', run_id='gate7-test', reserved_listing_requests=100_000,
                          account='012832591253', role='arco-gate7-test', instance_id='i-test')
            manifest = root / 'manifest.json'; manifest.write_text(json.dumps(config))
            expected = provider.digest(manifest)
            (root / 'manifest.sha256').write_text(expected)
            (root / 'state.json').write_text(json.dumps(dict(state='completed-evidence-pending-validation', run_id='gate7-test')))
            (root / 'operations.jsonl').write_text(json.dumps(dict(kind='verified-identity', provider=False,
                                                                 identity=dict(rehearsal=True, provider=False))) + '\n')
            with self.assertRaisesRegex(ValueError, 'provider identity'):
                provider.verify(root, expected)

    def test_stale_terminal_state_cannot_acknowledge_new_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'checkpoint.json'
            state = dict(run_id='gate7-test', source_sha256='a' * 64, config_sha256='b' * 64,
                         unit='arco-gate7-test.service', sequence=9, stopped=True,
                         outstanding_operations=0, unresolved_publications=0, stop_acknowledgement='c' * 32)
            provider.durable_stop_request(path, state, state['unit'])
            request = provider.pending_stop(path, state)
            self.assertFalse(provider.stop_acknowledged(request, state))
            acknowledged = dict(state, sequence=10, stop_acknowledgement=request['nonce'])
            self.assertTrue(provider.stop_acknowledged(request, acknowledged))

    def test_configuration_and_stop_are_digest_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / 'manifest.json'
            manifest.write_text(json.dumps(dict(schema=1, phase='provider')))
            expected = provider.digest(manifest)
            provider.configuration(manifest, expected)
            provider.request_stop(root, expected)
            provider.request_stop(root, expected)
            with self.assertRaises(ValueError):
                provider.request_stop(root, 'a' * 64)
            manifest.write_text('{}')
            with self.assertRaises(ValueError):
                provider.configuration(manifest, expected)

    def test_missing_checkpoint_and_incomplete_execution_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(OSError):
                provider.read_checkpoint(root / 'checkpoint.json', 'a' * 64, 'b' * 64)
            manifest = root / 'manifest.json'
            manifest.write_text(json.dumps(dict(schema=1, phase='provider', run_id='gate7-test')))
            expected = provider.digest(manifest)
            (root / 'manifest.sha256').write_text(expected)
            for state in ('running', 'failed-recovery-required'):
                (root / 'state.json').write_text(json.dumps(dict(state=state, run_id='gate7-test')))
                with self.assertRaisesRegex(ValueError, 'incomplete'):
                    provider.verify(root, expected)

    def test_telemetry_rejects_gaps_drift_resets_and_missing_records(self):
        state = dict(start_utc=100, start_monotonic=10, end_utc=103, end_monotonic=13, pid=123, cgroup='/test')
        rows = [dict(utc=101+n, monotonic=11+n, sequence=n, pid=123,
                     cpu_seconds=n, memory_bytes=1024, pids=2, cgroup='/test', read_bytes=n, write_bytes=n,
                     evidence_bytes=100+n) for n in range(2)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'telemetry.jsonl'
            def check(records, current=state):
                path.write_text(''.join(json.dumps(row) + '\n' for row in records))
                return provider.verify_telemetry(path, current)
            self.assertEqual(2, check(rows)['samples'])
            for key, value in [('utc', 999), ('monotonic', 999), ('cpu_seconds', -1),
                               ('read_bytes', float('nan')), ('pid', 456), ('sequence', 10)]:
                broken = copy.deepcopy(rows)
                broken[1][key] = value
                with self.subTest(key=key), self.assertRaises(ValueError):
                    check(broken)
            broken = copy.deepcopy(rows)
            del broken[0]['memory_bytes']
            with self.assertRaises(ValueError):
                check(broken)
            with self.assertRaises(ValueError):
                check([])
            with self.assertRaises(ValueError):
                check(rows, dict(state, end_utc=200, end_monotonic=110))


if __name__ == '__main__':
    unittest.main()
