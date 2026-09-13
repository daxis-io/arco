#!/usr/bin/env python3
"""Credential-free supervisor lifecycle probes on a disposable Linux systemd host.

Run as root with an unused absolute evidence directory. The explicitly fake worker
does no S3 I/O. Units are preserved in evidence and unloaded after stop proof.
"""
import argparse
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import subprocess
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gate7_provider as provider

WORKER = '''#!/usr/bin/python3
import json, pathlib, subprocess, sys, time
if sys.argv[1] == 'provider-validate':
    sys.exit(0)
config = json.loads(pathlib.Path(sys.argv[2]).read_text())
root = pathlib.Path(config['evidence_dir'])
root.mkdir()
(root / 'ready').write_text('fake worker; no provider traffic')
if config['run_id'].endswith('-survivor'):
    subprocess.Popen(['/bin/sleep', '300'], start_new_session='detached' in config['run_id'],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sys.exit(0)
if config['run_id'].endswith('-evidence-cap'):
    while not (root / 'stop-request').exists():
        sys.stdout.write('x' * 65536); sys.stdout.flush()
while not (root / 'stop-request').exists():
    time.sleep(0.05)
sys.exit(2)
'''


def wait_for(condition, timeout=45):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError('systemd fixture barrier timed out')
        time.sleep(0.1)


def run(root, selected=None):
    if os.geteuid() != 0 or not Path('/run/systemd/system').is_dir():
        raise ValueError('requires disposable Linux systemd host and root')
    root.mkdir(mode=0o700)
    worker = root / 'fake-worker'
    worker.write_text(WORKER); worker.chmod(0o700)
    build = root / 'build-inputs.json'
    build.write_text(json.dumps(dict(supervisor_python=dict(path=str(Path(sys.executable).resolve()),
                                                           sha256=provider.digest(Path(sys.executable).resolve())),
                                    supervisor_python_version=sys.version)))
    results = []
    for case in ('start-hook', 'stop', 'unit-drift', 'drift', 'limit', 'survivor', 'detached-survivor', 'supervisor-crash', 'evidence-cap', 'lost-checkpoint'):
        if selected and case != selected:
            continue
        run_id = f'gate7-lifecycle-{os.getpid()}-{case}'
        unit = f'arco-{run_id}.service'
        unit_path = Path('/run/systemd/system') / unit
        attempt = root / case; attempt.mkdir()
        manifest = attempt / 'manifest.json'
        evidence = attempt / 'evidence'
        config = dict(schema=1, phase='provider', run_id=run_id, region='us-east-2',
                      expires_utc=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                      executable_sha256=provider.digest(worker), evidence_dir=str(evidence),
                      supervisor=dict(path=str(Path(provider.__file__).resolve()), sha256=provider.digest(provider.__file__)),
                      build_inputs=dict(path=str(build), sha256=provider.digest(build)),
                      reserved_listing_requests=100_000,
                      source_manifest=dict(sha256='a' * 64),
                      ceilings=dict(elapsed_seconds=3 if case == 'limit' else 120,
                                    evidence_bytes=8 * 1024**2))
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            config['listing_proxy'] = f'http://127.0.0.1:{listener.getsockname()[1]}'
        manifest.write_text(json.dumps(config))
        expected = provider.digest(manifest)
        command = [sys.executable, '-B', str(Path(provider.__file__).resolve()), 'supervise',
                   str(manifest), '--expected-sha256', expected, '--executable', str(worker)]
        # Test paths must not require systemd command-line interpolation or quoting.
        if any(any(c in word for c in ' \t\n%"\\') for word in command):
            raise ValueError('simple absolute fixture paths required')
        text = ('[Unit]\nDescription=Gate 7 fake-worker lifecycle check\n[Service]\n'
                'Type=exec\nKillMode=mixed\nRestart=no\nTimeoutStopSec=75\n'
                'CPUAccounting=yes\nIOAccounting=yes\nMemoryAccounting=yes\nTasksAccounting=yes\n'
                'MemoryMax=256M\nTasksMax=16\nNoNewPrivileges=yes\nExecStart=' + ' '.join(command) + '\n')
        marker = attempt / 'unsafe-hook-executed'
        if case == 'start-hook':
            text += f'ExecStartPre=/usr/bin/touch {marker}\n'
        with unit_path.open('x') as out:
            out.write(text)
        (attempt / unit).write_text(text)
        try:
            subprocess.run(['systemctl', 'daemon-reload'], check=True)
            if case == 'start-hook':
                try:
                    provider.start(manifest, expected, worker, unit_path, provider.digest(unit_path))
                except ValueError as error:
                    assert 'contract mismatch' in str(error), error
                else:
                    raise AssertionError('unsafe start hook admitted')
                assert not marker.exists() and not evidence.exists()
                assert provider.systemd_status(unit)['active_state'] == 'inactive'
                results.append(dict(case=case, rejected_before_start=True, hook_executed=False, provider=False))
                (root / 'results.json').write_text(json.dumps(results, indent=2))
                continue
            provider.start(manifest, expected, worker, unit_path, provider.digest(unit_path))
            wait_for(lambda: (evidence / 'ready').exists())
            checkpoint = evidence.with_name('evidence-supervision') / 'checkpoint.json'
            wait_for(lambda: checkpoint.exists())
            before = provider.systemd_status(unit)
            assert before['children'], before
            if case == 'unit-drift':
                unit_path.write_text(text + 'ExecStopPost=/usr/bin/touch ' + str(marker) + '\n')
                subprocess.run(['systemctl', 'daemon-reload'], check=True)
                rejected = subprocess.run([sys.executable, str(Path(provider.__file__).resolve()), 'stop',
                                           str(manifest), '--expected-sha256', expected],
                                          capture_output=True, text=True, timeout=15)
                assert rejected.returncode != 0 and 'contract mismatch' in rejected.stderr, rejected.stderr
                assert not marker.exists()
                (attempt / 'rejected-stop.json').write_text(json.dumps(dict(code=rejected.returncode, stderr=rejected.stderr)))
                # Restore the reviewed unit before the fixture itself requests teardown.
                unit_path.write_text(text)
                subprocess.run(['systemctl', 'daemon-reload'], check=True)
                provider.request_stop(evidence, expected)
            elif case == 'drift':
                manifest.write_text(manifest.read_text() + '\n')
            elif case == 'stop':
                wait_for(lambda: provider.read_checkpoint(checkpoint, 'a' * 64, expected)['sequence'] > 0)
                samples = [json.loads(line) for line in (checkpoint.parent / 'telemetry.jsonl').read_text().splitlines()]
                assert any(row['pids'] >= 2 and row['cpu_seconds'] > row['main_process']['cpu_seconds']
                           and row['memory_bytes'] > row['main_process']['rss_bytes'] for row in samples), samples
                stopped = subprocess.run([sys.executable, str(Path(provider.__file__).resolve()), 'stop',
                                          str(manifest), '--expected-sha256', expected],
                                         capture_output=True, text=True, timeout=90)
                assert stopped.returncode == 2, stopped.stderr
                stop_proof = json.loads(stopped.stdout)
                assert stop_proof['processes_stopped'] and not stop_proof['data_reconciled'], stop_proof
                (attempt / 'stop-proof.json').write_text(stopped.stdout)
            elif case == 'supervisor-crash':
                subprocess.run(['systemctl', 'kill', '--kill-whom=main', '--signal=KILL', unit], check=True)
            elif case == 'lost-checkpoint':
                wait_for(lambda: provider.read_checkpoint(checkpoint, 'a' * 64, expected)['sequence'] > 0)
                (attempt / 'lost-checkpoint-preserved.json').write_bytes(checkpoint.read_bytes())
                checkpoint.unlink()
            wait_for(lambda: not provider.systemd_status(unit)['children'])
            subprocess.run(['systemctl', 'stop', unit], check=True, timeout=30)
            after = provider.systemd_status(unit)
            if after['active_state'] == 'failed' and not after['children']:
                (attempt / 'failed-unit-before-reset.json').write_text(json.dumps(after))
                subprocess.run(['systemctl', 'reset-failed', unit], check=True)
                after = provider.systemd_status(unit)
            assert after['active_state'] == 'inactive' and not after['children'], after
            state = provider.read_checkpoint(checkpoint, 'a' * 64, expected)
            assert state['requires_reconciliation'] is True, state
            if case == 'detached-survivor':
                assert state.get('surviving_cgroup_pids'), state
            assert sum(path.stat().st_size for directory in (evidence, checkpoint.parent)
                       for path in directory.iterdir() if path.is_file()) <= config['ceilings']['evidence_bytes']
            if case != 'supervisor-crash':
                assert state['stopped'] is True, state
            # A second invocation cannot reuse the durable attempt, even with the
            # same manifest. Running outside the bound unit also fails closed.
            replay = subprocess.run(command, capture_output=True, text=True, timeout=10)
            assert replay.returncode != 0
            results.append(dict(case=case, before=before, after=after,
                                durable_stop=state['stopped'], reconciliation_required=True,
                                restart_rejected=True, provider=False))
            (attempt / 'journal.log').write_bytes(subprocess.check_output(['journalctl', '-u', unit, '--no-pager']))
            (attempt / 'restart-stderr.txt').write_text(replay.stderr)
        finally:
            unit_path.write_text(text)
            subprocess.run(['systemctl', 'daemon-reload'], check=True)
            (attempt / 'journal.log').write_bytes(subprocess.check_output(['journalctl', '-u', unit, '--no-pager']))
            subprocess.run(['systemctl', 'stop', unit], check=True, timeout=30)
            observed = provider.systemd_status(unit)
            assert not observed['children'], observed
            unit_path.unlink()
            subprocess.run(['systemctl', 'daemon-reload'], check=True)
        (root / 'results.json').write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path)
    parser.add_argument('--case')
    args = parser.parse_args()
    run(args.evidence.resolve(), args.case)
