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
from urllib.error import HTTPError


class FixtureContract(unittest.TestCase):
    def dispatch(self, directory, callback='http://127.0.0.1:5187', sql='select :value', task_id='task-1',
                 acknowledgment_mode='normal'):
        os.environ['ARCO_FLOW_FIXTURE_ROOT'] = directory
        spec = importlib.util.spec_from_file_location('fixture', Path(__file__).with_name('pilot_flow_http_fixture.py'))
        fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture)
        fixture.CALLBACK_ACK_MODE.write_text(acknowledgment_mode)
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

    def test_missing_publication_mode_claims_an_absent_object_without_forging_owner_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            os.environ['ARCO_FLOW_FIXTURE_ROOT'] = directory
            spec = importlib.util.spec_from_file_location('fixture', Path(__file__).with_name('pilot_flow_http_fixture.py'))
            fixture = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(fixture)
            output = {'objectPath': 'outputs/run-1/result.parquet', 'ownerEvidence': {'forged': True}}
            claim = fixture.publication_claim(output, 'run-1', 'missing')
            self.assertEqual(claim['objectPath'], 'outputs/run-1/result.parquet')
            self.assertNotIn('ownerEvidence', claim)
            self.assertEqual(output['objectPath'], 'outputs/run-1/result.parquet')

    def test_worker_discards_a_completed_callback_acknowledgment_only_after_api_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            handler, outbound = self.dispatch(directory, acknowledgment_mode='discard-completed')
            handler.respond.assert_called_once_with(502, {'apiAccepted': True, 'wireLoss': False})
            requests = [call.args[0] for call in outbound.call_args_list]
            self.assertEqual(len(requests), 2)
            self.assertTrue(requests[-1].full_url.endswith('/completed'))

    def test_replay_acknowledges_transport_envelope_using_worker_task_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            os.environ['ARCO_FLOW_FIXTURE_ROOT'] = directory
            spec = importlib.util.spec_from_file_location('fixture', Path(__file__).with_name('pilot_flow_http_fixture.py'))
            fixture = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(fixture)
            fixture.callback_record_path('worker-task').write_text(json.dumps({
                'url': 'http://127.0.0.1:5187/api/v1/tasks/worker-task/completed',
                'headers': {}, 'payload': {'attempt': 1},
            }))
            pending = fixture.PENDING / 'transport-task.json'
            pending.write_text(json.dumps({'taskId': 'transport-task', 'body': json.dumps({'taskId': 'worker-task'})}))
            body = json.dumps({'taskId': 'worker-task'}).encode()
            handler = object.__new__(fixture.Handler)
            handler.path = '/replay'
            handler.headers = {'Content-Length': str(len(body))}
            handler.rfile = io.BytesIO(body)
            handler.respond = MagicMock()
            response = MagicMock()
            response.__enter__.return_value.status = 200
            with patch.object(fixture, 'urlopen', return_value=response):
                handler.do_POST()
            self.assertFalse(pending.exists())
            self.assertTrue((fixture.DONE / pending.name).exists())

    def test_replay_reports_a_changed_immutable_descriptor_as_conflicting(self):
        with tempfile.TemporaryDirectory() as directory:
            os.environ['ARCO_FLOW_FIXTURE_ROOT'] = directory
            spec = importlib.util.spec_from_file_location('fixture', Path(__file__).with_name('pilot_flow_http_fixture.py'))
            fixture = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(fixture)
            fixture.callback_record_path('task-1').write_text(json.dumps({
                'url': 'http://127.0.0.1:5187/api/v1/tasks/task-1/completed',
                'headers': {'Authorization': 'Bearer task-token'},
                'payload': {'output': {'publication': {'objectPath': 'outputs/run-1/result.parquet'}}},
            }))
            body = json.dumps({'taskId': 'task-1', 'changeImmutableField': 'objectPath'}).encode()
            handler = object.__new__(fixture.Handler)
            handler.path = '/replay'
            handler.headers = {'Content-Length': str(len(body))}
            handler.rfile = io.BytesIO(body)
            handler.respond = MagicMock()
            with patch.object(fixture, 'urlopen', side_effect=HTTPError('http://127.0.0.1', 409, 'conflict', {}, None)):
                handler.do_POST()
            handler.respond.assert_called_once_with(200, {'replayed': False, 'status': 409})

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

    def test_r2_mode_refuses_ambient_or_incomplete_provider_configuration_before_creating_a_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'receipt'
            env = dict(os.environ,
                       ARCO_TEST_R2_ACCOUNT_ID='76d83deea87d32b454071fc35e125818',
                       ARCO_TEST_R2_BUCKET='arco-030-qualification-20261010',
                       AWS_ACCESS_KEY_ID='test-key', AWS_SECRET_ACCESS_KEY='test-secret',
                       AWS_PROFILE='ambient-profile')
            result = subprocess.run(
                [sys.executable, str(Path(__file__).with_name('independent_process_proof.py')),
                 'unused-bin', str(root), '--r2'],
                env=env, capture_output=True, text=True, timeout=10,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('AWS_PROFILE', result.stderr)
            self.assertFalse(root.exists())

    def test_r2_configuration_validator_accepts_only_the_exact_fixture_profile(self):
        account = '76d83deea87d32b454071fc35e125818'
        base = {
            'ARCO_TEST_R2_ACCOUNT_ID': account,
            'ARCO_TEST_R2_BUCKET': 'arco-030-qualification-20261010',
            'AWS_ACCESS_KEY_ID': 'test-key', 'AWS_SECRET_ACCESS_KEY': 'test-secret',
            'AWS_ENDPOINT': f'https://{account}.r2.cloudflarestorage.com',
            'AWS_REGION': 'auto', 'AWS_EC2_METADATA_DISABLED': 'true',
        }
        with tempfile.TemporaryDirectory() as directory:
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(('ARCO_', 'AWS_'))}
            env.update(base)
            result = subprocess.run(
                [sys.executable, str(Path(__file__).with_name('independent_process_proof.py')), '--validate-r2'],
                env=env, capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_r2_configuration_validator_rejects_empty_ambient_and_invalid_exact_values(self):
        account = '76d83deea87d32b454071fc35e125818'
        base = {
            'ARCO_TEST_R2_ACCOUNT_ID': account,
            'ARCO_TEST_R2_BUCKET': 'arco-030-qualification-20261010',
            'AWS_ACCESS_KEY_ID': 'test-key', 'AWS_SECRET_ACCESS_KEY': 'test-secret',
            'AWS_ENDPOINT': f'https://{account}.r2.cloudflarestorage.com',
            'AWS_REGION': 'auto', 'AWS_EC2_METADATA_DISABLED': 'true',
        }
        cases = [
            ('AWS_PROFILE', ''), ('AWS_SESSION_TOKEN', ''), ('AWS_DEFAULT_REGION', ''),
            ('AWS_ENDPOINT_URL', ''), ('AWS_ENDPOINT_URL_S3', ''),
            ('ARCO_TEST_R2_BUCKET', 'ab'), ('ARCO_TEST_R2_BUCKET', '-bucket'),
            ('ARCO_TEST_R2_BUCKET', 'bucket-'), ('AWS_ENDPOINT', ''),
            ('AWS_REGION', ''), ('AWS_EC2_METADATA_DISABLED', ''),
        ]
        for key, value in cases:
            with self.subTest(key=key, value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / 'receipt'
                env = {name: item for name, item in os.environ.items()
                       if not name.startswith(('ARCO_', 'AWS_'))}
                env.update(base)
                env[key] = value
                result = subprocess.run(
                    [sys.executable, str(Path(__file__).with_name('independent_process_proof.py')),
                     'unused-bin', str(root), '--r2'],
                    env=env, capture_output=True, text=True, timeout=10,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(root.exists())


if __name__ == '__main__':
    unittest.main()
