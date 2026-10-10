"""Prevent false evidence from changed duplicate intent or disabled assertions."""

import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock


class FixtureContract(unittest.TestCase):
    def dispatch(self, directory, callback='http://127.0.0.1:5187', sql='select :value', task_id='task-1'):
        os.environ['ARCO_FLOW_FIXTURE_ROOT'] = directory
        spec = importlib.util.spec_from_file_location('fixture', Path(__file__).with_name('pilot_flow_http_fixture.py'))
        fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture)
        envelope = {
            'dispatchId': 'dispatch-1', 'runId': 'run-1', 'taskId': task_id,
            'attempt': 1, 'attemptId': 'attempt-1', 'taskToken': 'token', 'callbackBaseUrl': callback,
            'payload': {'version': 1, 'manifest': {'manifestId': 'manifest-A'},
                        'asset': {'execution': {'payload': {'sql': sql, 'parameters': {'value': {'type': 'int64', 'value': -7}}}}}},
        }
        body = json.dumps(envelope).encode()
        handler = object.__new__(fixture.Handler)
        handler.path = '/dispatch'
        handler.headers = {'Content-Length': str(len(body)), 'X-Arco-Dispatch-Secret': 'local-secret'}
        handler.rfile = io.BytesIO(body)
        handler.respond = MagicMock()
        response = MagicMock()
        response.__enter__.return_value.status = 200
        with patch.object(fixture, 'urlopen', return_value=response) as outbound, \
             patch.object(fixture.subprocess, 'check_output', return_value=b'{"byteSize": 10}'), \
             patch.dict(os.environ, ARCO_FLOW_OUTPUT_SEED='unused-seed'):
            handler.do_POST()
        return handler, outbound

    def test_worker_rejects_another_loopback_callback_port(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises((AssertionError, ValueError)):
                self.dispatch(directory, callback='http://127.0.0.1:9999')

    def test_worker_refuses_sql_outside_the_declared_reference_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises((AssertionError, ValueError)):
                self.dispatch(directory, sql='select 99 -- changed operation')

    def test_worker_binds_the_parameter_and_encodes_the_callback_task_segment(self):
        with tempfile.TemporaryDirectory() as directory:
            handler, outbound = self.dispatch(directory, task_id='task/part?query#fragment')
            handler.respond.assert_called_once_with(204)
            requests = [call.args[0] for call in outbound.call_args_list]
            self.assertEqual([request.full_url for request in requests], [
                'http://127.0.0.1:5187/api/v1/tasks/task%2Fpart%3Fquery%23fragment/started',
                'http://127.0.0.1:5187/api/v1/tasks/task%2Fpart%3Fquery%23fragment/completed',
            ])
            received = json.loads(next(Path(directory).glob('received-*.json')).read_text())
            self.assertEqual(received['result'], -7)

    def test_duplicate_intent_allows_credentials_but_refuses_changed_payload_or_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            os.environ['ARCO_FLOW_FIXTURE_ROOT'] = directory
            spec = importlib.util.spec_from_file_location('fixture', Path(__file__).with_name('pilot_flow_http_fixture.py'))
            fixture = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(fixture)
            envelope = {'taskToken': 'old', 'tokenExpiresAt': 'old', 'payload': {'manifest': 'A', 'parameter': 1}}
            task = {'taskId': 'same', 'targetUrl': 'http://127.0.0.1:5199/dispatch', 'body': json.dumps(envelope), 'headers': {}}
            identity = fixture.immutable_task_hash(task)
            envelope.update(taskToken='renewed', tokenExpiresAt='renewed')
            renewed = {**task, 'body': json.dumps(envelope)}
            self.assertEqual(identity, fixture.immutable_task_hash(renewed))
            envelope['payload']['manifest'] = 'B'
            self.assertNotEqual(identity, fixture.immutable_task_hash({**task, 'body': json.dumps(envelope)}))
            envelope['payload']['manifest'] = 'A'
            envelope['payload']['parameter'] = 1.0
            self.assertNotEqual(identity, fixture.immutable_task_hash({**task, 'body': json.dumps(envelope)}))
            self.assertNotEqual(identity, fixture.immutable_task_hash({**task, 'headers': {'changed': 'delivery'}}))

    def test_optimized_python_refuses_before_starting_processes_or_creating_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            for name in ('pilot_flow_http_fixture.py', 'independent_process_proof.py'):
                root = Path(directory) / name
                env = dict(os.environ, PYTHONOPTIMIZE='1', ARCO_FLOW_FIXTURE_ROOT=str(root))
                result = subprocess.run([sys.executable, str(Path(__file__).with_name(name)), 'unused-bin', str(root)],
                                        env=env, capture_output=True, text=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('requires unoptimized Python', result.stderr)
                self.assertFalse(root.exists())


if __name__ == '__main__':
    unittest.main()
