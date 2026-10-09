"""Prevent false evidence from changed duplicate intent or disabled assertions."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class FixtureContract(unittest.TestCase):
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
