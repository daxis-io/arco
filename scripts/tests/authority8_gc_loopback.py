"""Actual-process GC restart/oracle checks over the existing local HTTP fixture."""
import base64
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import threading


def exercise(executable, evidence, source, tenant, proxy, environment,
             objects, trace, control, lock, command_builder):
    assert source is not None and proxy is not None
    stages = []

    def run(name, stage, seed=None, success=True, interrupt=False, cap=None, real_validate=False):
        directory = evidence / name
        directory.mkdir()
        command = command_builder(executable, directory, source, tenant, proxy.url, stage, seed)
        if cap:
            packet = Path(command[2])
            config = json.loads(packet.read_text())
            config['ceilings'][cap] = 100001 if cap == 'requests' else 1
            packet.write_text(json.dumps(config, indent=2))
            command[3] = hashlib.sha256(packet.read_bytes()).hexdigest()
        if real_validate:
            packet = Path(command[2])
            config = json.loads(packet.read_text())
            config.update(phase='provider', base_sha='92fd19f11a547ece5004ac94cad83a3527471812',
                          scenarios=json.loads(subprocess.check_output([str(executable), 'inventory']))['provider'])
            packet.write_text(json.dumps(config, indent=2))
            command[1] = 'provider-validate'
            command[3] = hashlib.sha256(packet.read_bytes()).hexdigest()
        control['stage'] = name
        start = len(trace)
        with (directory / 'stderr.log').open('w') as errors, (directory / 'observations.jsonl').open('w') as output:
            process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=errors, text=True)
            watchdog = threading.Timer(120, process.kill)
            watchdog.start()
            rows = []
            samples = []
            try:
                for line in process.stdout:
                    output.write(line)
                    row = json.loads(line)
                    rows.append(row)
                    observed = subprocess.run(['ps', '-p', str(process.pid), '-o', 'time=', '-o', 'rss='], capture_output=True, text=True)
                    fields = observed.stdout.split()
                    if observed.returncode == 0 and len(fields) == 2:
                        samples.append(dict(after_kind=row.get('kind'), cpu_seconds=sum(float(part)*60**i for i,part in enumerate(reversed(fields[0].split(':')))), rss_bytes=int(fields[1])*1024))
                    if interrupt and row.get('kind') == 'gc-interrupted':
                        process.kill()
                        break
                code = process.wait(timeout=15)
            finally:
                watchdog.cancel()
                if process.poll() is None:
                    process.kill(); process.wait(timeout=15)
        (directory / 'process-observations.json').write_text(json.dumps(samples, indent=2))
        stage_trace = trace[start:]
        result = dict(name=name, pid=process.pid, exit_code=code,
                      requests=len(stage_trace), methods=dict(Counter(row['method'] for row in stage_trace)),
                      response_body_bytes=sum(row.get('response_body_bytes', 0) for row in stage_trace),
                      response_headers_bytes=sum(row.get('response_headers_bytes', 0) for row in stage_trace),
                      max_sampled_rss_bytes=max((row['rss_bytes'] for row in samples), default=0),
                      sampled_cpu_seconds=max((row['cpu_seconds'] for row in samples), default=0))
        stages.append(result)
        (directory / 'result.json').write_text(json.dumps(result, indent=2))
        if interrupt:
            assert code != 0 and any(row.get('kind') == 'gc-interrupted' for row in rows), (directory / 'stderr.log').read_text()
        else:
            assert (code == 0) == success, (directory / 'stderr.log').read_text()
        if stage != 'fixture':
            assert not any(row['method'] == 'DELETE' for row in stage_trace)
            assert all(row['method'] != 'PUT' or '/gc-marks/v1/' in row['path'] for row in stage_trace)
            assert not any(row['method'] == 'GET' and '/segments/l1/' in row['path'] for row in stage_trace)
        if not success and name != 'changed-warm-candidate':
            assert not any(row.get('kind') == 'gc-page' for row in rows), rows
        if name == 'changed-warm-candidate':
            assert 'candidate version or size changed' in (directory / 'stderr.log').read_text()
        if real_validate:
            assert not stage_trace
            assert 'invalid/incomplete provider configuration' in (directory / 'stderr.log').read_text()
        if success and not interrupt:
            assert any(row.get('kind') == 'gc-completed' for row in rows), rows[-3:]
        return rows

    rows = run('fixture', 'fixture')
    seed = next(row['seed'] for row in rows if row.get('kind') == 'gc-completed')
    with lock:
        image = {key: dict(body_base64=base64.b64encode(body).decode(), version=version,
                           sha256=hashlib.sha256(body).hexdigest()) for key, (body, version) in objects.items()}
    (evidence / 'fixture-image.json').write_text(json.dumps(image, sort_keys=True, indent=2))
    run('killed-process', 'interrupt', seed, interrupt=True)
    control['fault'] = 'lost-completion'
    first = run('restart-1', 'sweep', seed)
    second = run('restart-2', 'sweep', seed)
    for rows in [first, second]:
        complete = next(row for row in rows if row.get('kind') == 'gc-completed')
        assert complete['candidate_count'] == 600 and complete['inventory_pages'] >= 4
        assert complete['verified_roots'] == 3
    assert len({row['pid'] for row in stages}) == len(stages)
    control['fault'] = 'candidate-version'
    run('changed-warm-candidate', 'sweep', seed, success=False)
    assert any(row.get('injected_candidate_version_change') for row in trace)
    with lock:
        candidate, original_candidate = control.pop('changed_candidate')
        objects[candidate] = original_candidate
    control['fault'] = 'unavailable'
    run('unavailable-storage', 'sweep', seed, success=False)
    control['fault'] = None
    with lock:
        page = next(key for key in objects if '/gc-marks/v1/' in key and '/pages/' in key)
        original = objects[page]
        objects[page] = (b'corrupt', '"corrupt"')
    run('corrupt-proof', 'sweep', seed, success=False)
    with lock:
        objects[page] = original
    with lock:
        del objects[page]
    run('missing-proof', 'sweep', seed, success=False)
    with lock:
        objects[page] = original
    run('reject-real-provider-mode', 'sweep', seed, success=False, real_validate=True)
    run('request-cap', 'sweep', seed, success=False, cap='requests')
    assert stages[-1]['requests'] == 0, 'request reservation must stop before sending'
    run('write-byte-cap', 'sweep', seed, success=False, cap='submitted_bytes')
    assert stages[-1]['methods'].get('PUT', 0) == 0, 'write admission must stop before sending'
    with lock:
        head = next(key for key in objects if key.endswith('/head/current.json'))
        original_head = objects[head]
        objects[head] = (original_head[0], '"same-bytes-new-version"')
    run('changed-head-version', 'sweep', seed, success=False)
    with lock:
        objects[head] = original_head
    run('recovered-again', 'sweep', seed)
    summary = dict(provider=False, adapter='S3StorageBackend', active_delete=False,
                   actual_process_restart=True, fixture_image_sha256=hashlib.sha256((evidence/'fixture-image.json').read_bytes()).hexdigest(),
                   transport_requests=len(trace), stages=stages,
                   unique_addressed_paths=len({row['path'] for row in trace}),
                   scope='Local HTTP fixture retains exact bytes/opaque ETag versions across independent processes; no S3 traffic.')
    assert not any(row.get('rejected') for row in trace)
    assert proxy.failure is None
    events = [(row['started_ns'], 1) for row in trace if row.get('range') and '/physical/descriptors/' in row['path'] and 'finished_ns' in row]
    events += [(row['finished_ns'], -1) for row in trace if row.get('range') and '/physical/descriptors/' in row['path'] and 'finished_ns' in row]
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    assert 2 <= peak <= 8, ('bounded real HTTP probe overlap', peak)
    summary['max_overlapping_descriptor_requests'] = peak
    summary['timing_scope'] = '2-ms deterministic fixture delay; sampled CPU/RSS at output boundaries; not S3 timing or exact in-future ownership.'
    (evidence / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary))
