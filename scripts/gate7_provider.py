#!/usr/bin/env python3
"""Gate 7 bounded-provider supervision and read-only evidence verification.

Run supervise inside the manifest-bound persistent systemd unit. Failed attempts
never resume automatically and never clean up provider objects.
"""
import argparse
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import selectors
import subprocess
import sys
import time
import threading

from gate7_listing_proxy import ListingProxy

from gate7_qualification import (canonical, cold_summary, digest, read_checkpoint,
                                request_stop as durable_stop_request, stop_acknowledged,
                                systemd_status, write_checkpoint as atomic_checkpoint)


def write_checkpoint(path, state, **kwargs):
    if len(canonical(state)) > 32 * 1024:
        raise ValueError('checkpoint envelope ceiling')
    atomic_checkpoint(path, state, **kwargs)


def pending_stop(checkpoint, state):
    path = Path(checkpoint).with_name('stop-request.json')
    if not path.exists():
        return None
    if path.stat().st_size > 64 * 1024:
        raise ValueError('stop envelope ceiling')
    envelope = json.loads(path.read_bytes())
    request = envelope['state']
    if (envelope['sha256'] != hashlib.sha256(canonical(request)).hexdigest()
            or any(request.get(key) != state[key] for key in ('run_id', 'source_sha256', 'config_sha256', 'unit'))
            or type(request.get('after_sequence')) is not int or not 0 <= request['after_sequence'] <= state['sequence']
            or not re.fullmatch('[0-9a-f]{32}', str(request.get('nonce')))):
        raise ValueError('stop request identity/sequence mismatch')
    return request


def configuration(path, expected):
    path = Path(path)
    if len(expected) != 64 or digest(path) != expected or path.stat().st_size > 1024**2:
        raise ValueError('provider manifest digest/size mismatch')
    value = json.loads(path.read_bytes())
    if value.get('phase') != 'provider' or value.get('schema') != 1:
        raise ValueError('provider manifest required')
    return value


def verify(evidence, expected, *, rehearsal=False):
    """Check the whole bounded journal, source binding and frozen sample cardinality."""
    evidence = Path(evidence)
    config = configuration(evidence / 'manifest.json', expected)
    if (evidence / 'manifest.sha256').read_text() != expected:
        raise ValueError('persisted manifest identity mismatch')
    state = json.loads((evidence / 'state.json').read_bytes())
    if state.get('state') != 'completed-evidence-pending-validation' or state.get('run_id') != config['run_id']:
        raise ValueError('provider run is incomplete; preserve objects and reconcile')
    intents, cold, completed, reconciled = {}, [], set(), set()
    requests = config['reserved_listing_requests']
    submitted = adapters = 0
    saw_terminal = False
    identities = 0
    with (evidence / 'operations.jsonl').open() as records:
        for line in records:
            row = json.loads(line)
            if saw_terminal:
                raise ValueError('records follow terminal result')
            kind = row.get('kind')
            if kind == 'adapter-intent':
                identity = row['id']
                if type(identity) is not int or identity != adapters + 1:
                    raise ValueError('adapter intent sequence drift')
                if row['operation'] not in ('get', 'range', 'head', 'put', 'delete', 'list_page') or any(type(row[key]) is not int or row[key] < 0 for key in ('requests_reserved', 'submitted_bytes_reserved')) or (row['requests_reserved'] == 0) != (row['operation'] == 'list_page'):
                    raise ValueError('invalid adapter reservation')
                if row['requests_reserved'] != dict(get=2, range=4, head=2, put=3, delete=2, list_page=0)[row['operation']]:
                    raise ValueError('adapter request reservation drift')
                if not any(row['path'].startswith(f"tenant={config['tenant']}/workspace={config['workspace']}-r{r}/") for r in range(5)):
                    raise ValueError('journal namespace escape')
                intents[identity] = row
                requests += row['requests_reserved']; submitted += row['submitted_bytes_reserved']; adapters += 1
            elif kind == 'adapter-outcome':
                original = intents.pop(row['id'], None)
                if not original or any(original[key] != row[key] for key in ('operation', 'path')):
                    raise ValueError('unmatched adapter outcome')
                if type(row['elapsed_ns']) is not int or row['elapsed_ns'] < 0:
                    raise ValueError('invalid operation duration')
                if original['operation'] == 'put' and row['success'] is not True:
                    raise ValueError('unresolved write failure')
            elif kind == 'cold-observation':
                cold.append(row)
            elif kind == 'completed':
                if row['tenant'] != config['tenant'] or row['scenarios'] != config['scenarios']['scenarios'] or row['workspace'] in completed:
                    raise ValueError('scenario completion identity mismatch')
                completed.add(row['workspace'])
            elif kind == 'client-publication-reconciled':
                if row['path'] in reconciled or row['fault'] != 'client-lost-response-exact-reconciliation':
                    raise ValueError('duplicate or unknown reconciliation')
                reconciled.add(row['path'])
            elif kind == 'provider-execution-completed':
                if (saw_terminal or row['repetitions'] != 5 or row['pilot_qualified'] is not False
                        or row.get('provider') is not (not rehearsal) or row.get('qualification') != 'evidence-pending-validation'):
                    raise ValueError('invalid terminal inventory')
                saw_terminal = True
            elif kind == 'verified-identity':
                identity = row['identity']
                arn = f"arn:aws:sts::{config['account']}:assumed-role/{config['role']}/{config['instance_id']}"
                if (identities or row.get('provider') is not (not rehearsal)
                        or (not rehearsal and (identity.get('Account') != config['account'] or identity.get('Arn') != arn))
                        or (rehearsal and identity != dict(rehearsal=True, provider=False))):
                    raise ValueError('missing or mismatched provider identity')
                identities += 1
    expected_workspaces = {f"{config['workspace']}-r{r}" for r in range(5)}
    expected_reconciliations = {f"tenant={config['tenant']}/workspace={workspace}/conformance/client-lost-response" for workspace in expected_workspaces}
    counts = state['counters']
    if (intents or identities != 1 or not saw_terminal or completed != expected_workspaces or reconciled != expected_reconciliations
            or counts['in_flight'] != 0 or counts['stop_reason'] is not None
            or (counts['adapter_calls'], counts['requests_upper_bound'], counts['submitted_bytes']) != (adapters, requests, submitted)):
        raise ValueError('incomplete provider operation/inventory accounting')
    if counts['evidence_bytes'] != (evidence / 'operations.jsonl').stat().st_size or any(state.get(key) is not False for key in ('automatic_resume', 'automatic_cleanup', 'pilot_qualified')):
        raise ValueError('evidence byte accounting or terminal disposition mismatch')
    limits = config['ceilings']
    cost = config['fixed_cost_microusd'] + requests * config['request_cost_microusd'] + (submitted * config['stored_byte_cost_picousd'] + 999999) // 1000000
    if (requests > limits['requests'] or submitted > limits['submitted_bytes']
            or counts['evidence_bytes'] > (limits['evidence_bytes'] - 4 * 1024**2) * 3 // 4
            or cost != state['cost_upper_microusd'] or cost > limits['cost_microusd']
            or state['elapsed_ns'] > limits['elapsed_seconds'] * 10**9):
        raise ValueError('provider resource/cost/elapsed ceiling exceeded')
    return dict(status='rehearsal-evidence-verified' if rehearsal else 'bounded-provider-evidence-verified', provider_qualified=False,
                pilot_qualified=False, requests_upper_bound=requests, adapter_calls=adapters,
                submitted_bytes_upper_bound=submitted, cost_upper_microusd=cost,
                transport_attempts=counts['transport_attempts'], cold=cold_summary(cold),
                journal_sha256=digest(evidence / 'operations.jsonl'),
                note='Requires matching supervisor telemetry and separately reviewed provider acceptance; no pilot or elapsed-retention proof.')


def verify_telemetry(path, state):
    """Reject unobserved runtime, clock drift and impossible process counters."""
    previous = dict(utc=state['start_utc'], monotonic=state['start_monotonic'],
                    cpu_seconds=0, read_bytes=0, write_bytes=0)
    count = 0
    with Path(path).open() as stream:
        for line in stream:
            row = json.loads(line)
            for key in ('utc', 'monotonic', 'cpu_seconds', 'memory_bytes', 'pids', 'read_bytes', 'write_bytes', 'evidence_bytes'):
                if type(row.get(key)) not in (int, float) or not math.isfinite(row[key]) or row[key] < 0:
                    raise ValueError('invalid/missing supervisor telemetry')
            if row['sequence'] != count or row['pid'] != state['pid'] or row['cgroup'] != state['cgroup']:
                raise ValueError('telemetry identity/sequence drift')
            utc, mono = row['utc'] - previous['utc'], row['monotonic'] - previous['monotonic']
            if not 0 <= mono <= 60 or abs(utc - mono) > 1:
                raise ValueError('unverifiable supervisor clock/coverage')
            if any(row[key] < previous[key] for key in ('cpu_seconds', 'read_bytes', 'write_bytes')):
                raise ValueError('process counter reset')
            previous = row
            count += 1
    if not count:
        raise ValueError('missing supervisor telemetry')
    utc, mono = state['end_utc'] - previous['utc'], state['end_monotonic'] - previous['monotonic']
    if not 0 <= mono <= 60 or abs(utc - mono) > 1:
        raise ValueError('unobserved supervisor closeout')
    return dict(samples=count, sha256=digest(path))


class SupervisorEvidenceBudget:
    """The fixed supervisor/proxy partition; reserve bytes before every append."""
    def __init__(self, total):
        if type(total) is not int or not 8 * 1024**2 <= total <= 512 * 1024**2:
            raise ValueError('invalid evidence allocation')
        self.limit = (total - 4 * 1024**2) // 4
        self.used = 0
        self.lock = threading.Lock()

    def reserve(self, size):
        with self.lock:
            if self.used + size > self.limit:
                raise ValueError('supervisor/proxy evidence ceiling')
            self.used += size


def unit_contract(unit, command):
    result = subprocess.run(['systemctl', 'show', unit, '--all',
                             '--property=Type,KillMode,Restart,TimeoutStopUSec,MemoryMax,TasksMax,NoNewPrivileges,ExecStart,ExecStop,ExecStopPost,KillSignal,SendSIGKILL,TimeoutStopFailureMode,ExecCondition,ExecStartPre,ExecStartPost,DropInPaths,FragmentPath,OnFailure,CPUAccounting,IOAccounting,MemoryAccounting,TasksAccounting'],
                            check=True, text=True, capture_output=True, timeout=10)
    fields = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    # systemctl omits empty command arrays even with --all (verified on systemd 255).
    for key in ('ExecCondition', 'ExecStartPre', 'ExecStartPost', 'ExecStop', 'ExecStopPost'):
        fields.setdefault(key, '')
    required = dict(Type='exec', KillMode='mixed', Restart='no', TimeoutStopUSec='1min 15s', NoNewPrivileges='yes',
                    ExecStop='', ExecStopPost='', KillSignal='15', SendSIGKILL='yes', TimeoutStopFailureMode='terminate',
                    ExecCondition='', ExecStartPre='', ExecStartPost='', DropInPaths='', OnFailure='',
                    CPUAccounting='yes', IOAccounting='yes', MemoryAccounting='yes', TasksAccounting='yes')
    arguments = re.search(r'argv\[\]=(.*?) ; ignore_errors=', fields.get('ExecStart', ''))
    if (any(fields.get(key) != value for key, value in required.items())
            or not fields.get('MemoryMax', '').isdigit() or not 0 < int(fields['MemoryMax']) <= 24 * 1024**3
            or not fields.get('TasksMax', '').isdigit() or not 0 < int(fields['TasksMax']) <= 64
            or not arguments or arguments.group(1).split() != command):
        raise ValueError('systemd execution/stop/resource contract mismatch')
    fields['ExecStart'] = command
    return fields


def verify_running_unit(state):
    current = unit_contract(state['unit'], state['unit_contract']['ExecStart'])
    if (current != state['unit_contract']
            or digest(current['FragmentPath']) != state['unit_file_sha256']):
        raise ValueError('running unit artifact/contract drift')


def start(manifest, expected, executable, unit_file, unit_sha256):
    """Admit the reviewed unit before any start hook or worker can execute."""
    config = configuration(manifest, expected)
    unit = f"arco-{config['run_id']}.service"
    observed = systemd_status(unit)
    if observed['active_state'] != 'inactive' or observed['children']:
        raise ValueError('unit must be inactive before admission')
    command = [sys.executable, '-B', str(Path(__file__).resolve()), 'supervise',
               str(manifest), '--expected-sha256', expected, '--executable', str(executable)]
    fields = unit_contract(unit, command)
    verify_runtime(config, executable)
    if (unit_file.name != unit or Path(fields['FragmentPath']).resolve() != unit_file.resolve()
            or digest(unit_file) != unit_sha256):
        raise ValueError('reviewed unit artifact mismatch')
    for directory in (Path(config['evidence_dir']), Path(config['evidence_dir'] + '-supervision')):
        if directory.exists():
            raise ValueError('new execution evidence paths required')
    subprocess.run([str(executable), 'provider-validate', str(manifest), expected],
                   env=dict(PATH='/usr/local/bin:/usr/bin:/bin', AWS_REGION=config['region']),
                   check=True, capture_output=True, timeout=120)
    if unit_contract(unit, command) != fields or digest(unit_file) != unit_sha256:
        raise ValueError('unit changed during admission')
    subprocess.run(['systemctl', 'start', unit], check=True, timeout=30)
    return 0


def verify_runtime(config, executable):
    if (not executable.is_absolute() or executable.is_symlink() or not executable.is_file()
            or digest(executable) != config['executable_sha256']):
        raise ValueError('supervisor executable drift')
    supervisor = config['supervisor']
    if (Path(supervisor['path']) != Path(__file__).resolve() or digest(__file__) != supervisor['sha256']):
        raise ValueError('supervisor source identity mismatch')
    artifact = config['build_inputs']
    if digest(artifact['path']) != artifact['sha256']:
        raise ValueError('supervisor build inputs drift')
    build = json.loads(Path(artifact['path']).read_bytes())
    interpreter = build['supervisor_python']
    if (Path(interpreter['path']) != Path(sys.executable).resolve()
            or digest(interpreter['path']) != interpreter['sha256']
            or build['supervisor_python_version'] != sys.version
            or sys.version_info[:2] != (3, 11)):
        raise ValueError('supervisor Python 3.11 identity mismatch')


def evidence_closure(evidence):
    supervision = evidence.with_name(evidence.name + '-supervision')
    result = {}
    for root, names in ((evidence, ('manifest.json', 'manifest.sha256', 'operations.jsonl', 'state.json')),
                        (supervision, ('verified-execution.json', 'telemetry.jsonl', 'listing-connections.jsonl',
                                       'stdout.jsonl', 'stderr.log', 'proxy-bound.json'))):
        for name in names:
            path = root / name
            if path.is_symlink() or not path.is_file():
                raise OSError('missing or redirected terminal evidence')
            result[f'{root.name}/{name}'] = dict(size=path.stat().st_size, sha256=digest(path))
    return result


def verify_listing(path, config, authority):
    count = 0
    with Path(path).open() as stream:
        for line in stream:
            row = json.loads(line)
            count += 1
            if (row.get('kind') != 'listing-connection-admitted' or row.get('sequence') != count
                    or row.get('authority') != authority or count > config['reserved_listing_requests']):
                raise ValueError('listing admission evidence mismatch')
    if count == 0:
        raise ValueError('missing listing admission evidence')
    return dict(connections_upper_bound=count, sha256=digest(path), actual_http_requests=None)


def proc_sample(pid):
    root = Path('/proc') / str(pid)
    fields = (root / 'stat').read_text().rsplit(') ', 1)[1].split()
    io = dict(line.split(': ', 1) for line in (root / 'io').read_text().splitlines())
    return dict(cpu_seconds=(int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
                rss_bytes=int(fields[21]) * os.sysconf('SC_PAGE_SIZE'),
                read_bytes=int(io['read_bytes']), write_bytes=int(io['write_bytes']))


def cgroup_sample(root):
    cpu = dict(line.split() for line in (root / 'cpu.stat').read_text().splitlines())
    read_bytes = write_bytes = 0
    for line in (root / 'io.stat').read_text().splitlines():
        fields = dict(item.split('=') for item in line.split()[1:])
        read_bytes += int(fields.get('rbytes', 0))
        write_bytes += int(fields.get('wbytes', 0))
    return dict(cpu_seconds=int(cpu['usage_usec']) / 1_000_000,
                memory_bytes=int((root / 'memory.current').read_text()),
                pids=int((root / 'pids.current').read_text()), read_bytes=read_bytes, write_bytes=write_bytes)


def kill_cgroup_survivors(unit):
    survivors = [int(pid) for pid in systemd_status(unit)['children'] if int(pid) != os.getpid()]
    for pid in survivors:
        # Recheck membership immediately before signaling; never signal the supervisor.
        if str(pid) in systemd_status(unit)['children']:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    deadline = time.monotonic() + 5
    while survivors and time.monotonic() < deadline:
        if systemd_status(unit)['children'] == [str(os.getpid())]:
            break
        time.sleep(0.05)
    return survivors


def request_stop(evidence, expected):
    path = Path(evidence) / 'stop-request'
    try:
        with path.open('x') as stream:
            stream.write(expected); stream.flush(); os.fsync(stream.fileno())
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except FileExistsError:
        if path.read_text() != expected:
            raise ValueError('stop request identity mismatch')


def supervise(manifest, expected, executable, unit):
    config = configuration(manifest, expected)
    verify_runtime(config, executable)
    if unit != f"arco-{config['run_id']}.service":
        raise ValueError('unit identity mismatch')
    current = systemd_status(unit)
    if str(os.getpid()) not in current['children']:
        raise ValueError('supervisor must execute inside the bound systemd unit')
    if not sys.dont_write_bytecode:
        raise ValueError('supervisor requires Python -B to preserve exact source membership')
    contract = unit_contract(unit, [sys.executable, '-B', str(Path(__file__).resolve()), *sys.argv[1:]])
    if (Path(config['supervisor']['path']).resolve() != Path(__file__).resolve()
            or digest(__file__) != config['supervisor']['sha256']):
        raise ValueError('supervisor source identity mismatch')
    budget = SupervisorEvidenceBudget(config['ceilings']['evidence_bytes'])
    environment = dict(PATH='/usr/local/bin:/usr/bin:/bin', AWS_REGION=config['region'],
                       ARCO_GATE7_SUPERVISOR_PID=str(os.getpid()), PYTHONDONTWRITEBYTECODE='1')
    subprocess.run([str(executable), 'provider-validate', str(manifest), expected],
                   env=environment, check=True, capture_output=True, timeout=120)
    evidence = Path(config['evidence_dir'])
    supervision = evidence.with_name(evidence.name + '-supervision')
    supervision.mkdir(mode=0o700)
    state = dict(run_id=config['run_id'], source_sha256=config['source_manifest']['sha256'],
                 config_sha256=expected, sequence=0, stopped=False, unit=unit,
                 boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                 cgroup=current['cgroup'], start_utc=time.time(), start_monotonic=time.monotonic(),
                 requires_reconciliation=True, exit_code=None, unit_contract=contract,
                 unit_file_sha256=digest(contract['FragmentPath']))
    checkpoint = supervision / 'checkpoint.json'
    write_checkpoint(checkpoint, state, exclusive=True)
    stopping = []
    def requested(signum, _):
        if not stopping:
            stopping.append((time.monotonic(), f'signal {signum}'))
    previous = {sig: signal.signal(sig, requested) for sig in (signal.SIGTERM, signal.SIGINT)}
    child = None
    proxy = None
    try:
        authority = f"s3.{config['region']}.amazonaws.com:443"
        port = config['listing_proxy'].removeprefix('http://127.0.0.1:')
        if not port.isdigit() or not 0 < int(port) < 65536:
            raise ValueError('fixed numeric loopback listing port required')
        proxy = ListingProxy(authority, config['reserved_listing_requests'],
                             supervision / 'listing-connections.jsonl', budget.reserve, int(port),
                             deadline=state['start_monotonic'] + config['ceilings']['elapsed_seconds'],
                             expires_utc=datetime.fromisoformat(config['expires_utc'].replace('Z', '+00:00')).timestamp())
        if proxy.url != config['listing_proxy']:
            raise ValueError('listing proxy binding mismatch')
        write_checkpoint(supervision / 'proxy-bound.json',
                         dict(pid=os.getpid(), manifest_sha256=expected, listing_proxy=proxy.url), exclusive=True)
        with (supervision / 'stdout.jsonl').open('wb') as output, (supervision / 'stderr.log').open('wb') as errors, (supervision / 'telemetry.jsonl').open('wb') as telemetry, selectors.DefaultSelector() as selector:
            child = subprocess.Popen([str(executable), 'provider', str(manifest), expected],
                                     env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            for pipe, destination in ((child.stdout, output), (child.stderr, errors)):
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, destination)
            state['pid'] = child.pid
            next_sample = 0
            child_exit_seen = None
            while child.poll() is None or selector.get_map():
                for key, _ in selector.select(timeout=0.1):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj); key.fileobj.close()
                        continue
                    try:
                        budget.reserve(len(chunk)); key.data.write(chunk)
                    except ValueError:
                        if not stopping:
                            stopping.append((time.monotonic(), 'supervisor output ceiling'))
                now = time.monotonic()
                if now < next_sample:
                    continue
                next_sample = now + 1
                if child.poll() is not None:
                    if selector.get_map():
                        child_exit_seen = child_exit_seen or now
                        if now - child_exit_seen > 5:
                            os.killpg(child.pid, signal.SIGKILL)
                            raise ValueError('surviving child holds output pipes')
                        continue
                    break
                try:
                    main_process = proc_sample(child.pid)
                    sample = cgroup_sample(Path('/sys/fs/cgroup') / state['cgroup'].lstrip('/'))
                    sample.update(cgroup=state['cgroup'], main_process=main_process)
                except FileNotFoundError:
                    if child.poll() is None:
                        raise
                    break
                total_bytes = sum(p.stat().st_size for directory in (evidence, supervision)
                                  if directory.exists() for p in directory.iterdir() if p.is_file())
                sample.update(utc=time.time(), monotonic=now, evidence_bytes=total_bytes,
                              sequence=state['sequence'], pid=child.pid)
                data = (json.dumps(sample, allow_nan=False) + '\n').encode()
                try:
                    budget.reserve(len(data)); telemetry.write(data); telemetry.flush(); os.fsync(telemetry.fileno())
                except ValueError:
                    if not stopping:
                        stopping.append((now, 'supervisor telemetry ceiling'))
                drift = digest(manifest) != expected or digest(executable) != config['executable_sha256'] or digest(__file__) != config['supervisor']['sha256']
                try:
                    verify_running_unit(state)
                except (ValueError, OSError):
                    drift = True
                if pending_stop(checkpoint, state) and not stopping:
                    stopping.append((now, 'durable stop request'))
                if not stopping and (drift or total_bytes > config['ceilings']['evidence_bytes']
                        or proxy.failure
                        or sample['memory_bytes'] > int(contract['MemoryMax'])
                        or sample['pids'] > int(contract['TasksMax'])
                        or now - state['start_monotonic'] >= config['ceilings']['elapsed_seconds']):
                    stopping.append((now, 'source/configuration/resource ceiling'))
                if stopping:
                    state['stop_reason'] = stopping[0][1]
                    if evidence.exists():
                        request_stop(evidence, expected)
                    if now - stopping[0][0] > 60:
                        os.killpg(child.pid, signal.SIGKILL)
                persisted = read_checkpoint(checkpoint, state['source_sha256'], expected)
                if persisted['sequence'] != state['sequence']:
                    raise ValueError('active checkpoint sequence drift')
                state['sequence'] += 1
                state.update(last_utc=sample['utc'], last_monotonic=now, telemetry=sample)
                write_checkpoint(checkpoint, state)
            code = child.wait()
            output.flush(); errors.flush(); os.fsync(output.fileno()); os.fsync(errors.fileno())
        # A process-group survivor invalidates completion even if the main process exited zero.
        try:
            os.killpg(child.pid, 0)
        except ProcessLookupError:
            pass
        else:
            os.killpg(child.pid, signal.SIGKILL)
            raise ValueError('surviving child process; group killed and run rejected')
        survivors = kill_cgroup_survivors(unit)
        if survivors:
            state['surviving_cgroup_pids'] = survivors
            raise ValueError('surviving cgroup process; run rejected')
        state['exit_code'] = code
        proxy.close()
        if proxy.failure:
            raise ValueError(proxy.failure)
        state['end_utc'] = time.time(); state['end_monotonic'] = time.monotonic()
        if code == 0 and not stopping:
            verdict = verify(evidence, expected)
            verdict['supervision'] = verify_telemetry(supervision / 'telemetry.jsonl', state)
            verdict['listing'] = verify_listing(supervision / 'listing-connections.jsonl', config, authority)
            data = json.dumps(verdict, indent=2)
            if len(data.encode()) > 1024**2:
                raise ValueError('verdict envelope ceiling')
            with (supervision / 'verified-execution.json').open('x') as stream:
                stream.write(data); stream.flush(); os.fsync(stream.fileno())
            total_bytes = sum(path.stat().st_size for directory in (evidence, supervision)
                              for path in directory.iterdir() if path.is_file())
            if total_bytes + 128 * 1024 > config['ceilings']['evidence_bytes']:
                raise ValueError('final evidence/envelope ceiling')
            state['evidence_closure'] = evidence_closure(evidence)
            state['requires_reconciliation'] = False
        return 0 if code == 0 and not state['requires_reconciliation'] else 2
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL); child.wait()
        if proxy is not None and not proxy.journal.closed:
            proxy.close()
        survivors = kill_cgroup_survivors(unit)
        if survivors:
            state['surviving_cgroup_pids'] = survivors
            state['requires_reconciliation'] = True
        if pending_stop(checkpoint, state) is None:
            durable_stop_request(checkpoint, state, unit)
        request = pending_stop(checkpoint, state)
        state['sequence'] += 1
        state['stopped'] = (child is None or child.poll() is not None) and systemd_status(unit)['children'] == [str(os.getpid())]
        state['stop_acknowledgement'] = request['nonce']
        state['outstanding_operations'] = int(state['requires_reconciliation'])
        state['unresolved_publications'] = int(state['requires_reconciliation'])
        state['end_utc'] = time.time(); state['end_monotonic'] = time.monotonic()
        write_checkpoint(checkpoint, state)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['start', 'supervise', 'verify', 'status', 'stop', 'recover'])
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--executable', type=Path)
    parser.add_argument('--unit-file', type=Path)
    parser.add_argument('--unit-sha256')
    args = parser.parse_args()
    config = configuration(args.manifest, args.expected_sha256)
    evidence = Path(config['evidence_dir'])
    unit = f"arco-{config['run_id']}.service"
    if args.command == 'start':
        if args.executable is None or args.unit_file is None or args.unit_sha256 is None:
            raise ValueError('approved executable, unit file and unit digest required')
        return start(args.manifest, args.expected_sha256, args.executable, args.unit_file, args.unit_sha256)
    if args.command == 'supervise':
        if args.executable is None:
            raise ValueError('verified executable path required')
        return supervise(args.manifest, args.expected_sha256, args.executable, unit)
    if args.command == 'verify':
        print(json.dumps(verify(evidence, args.expected_sha256), indent=2)); return 0
    supervision = evidence.with_name(evidence.name + '-supervision')
    state = read_checkpoint(supervision / 'checkpoint.json', config['source_manifest']['sha256'], args.expected_sha256)
    if state['unit'] != unit:
        raise ValueError('durable unit identity mismatch')
    if args.command == 'stop':
        verify_running_unit(state)
        systemd_status(unit)
        if pending_stop(supervision / 'checkpoint.json', state) is None:
            durable_stop_request(supervision / 'checkpoint.json', state, unit)
        request_stop(evidence, args.expected_sha256)
        verify_running_unit(state)
        subprocess.run(['systemctl', 'stop', unit], check=True, timeout=90)
        observed = systemd_status(unit)
        if observed['active_state'] == 'failed' and not observed['children']:
            # Preserve the failed-unit observation before resetting only systemd's
            # marker. Publication disposition remains in the durable checkpoint.
            state = read_checkpoint(supervision / 'checkpoint.json', config['source_manifest']['sha256'], args.expected_sha256)
            state['sequence'] += 1
            state['systemd_before_reset'] = observed
            write_checkpoint(supervision / 'checkpoint.json', state)
            subprocess.run(['systemctl', 'reset-failed', unit], check=True, timeout=10)
    observed = systemd_status(unit)
    state = read_checkpoint(supervision / 'checkpoint.json', config['source_manifest']['sha256'], args.expected_sha256)
    data_reconciled = not state['requires_reconciliation']
    if data_reconciled:
        try:
            data_reconciled = evidence_closure(evidence) == state.get('evidence_closure')
        except OSError:
            data_reconciled = False
    stopped = observed['active_state'] == 'inactive' and not observed['children'] and state['stopped']
    request = pending_stop(supervision / 'checkpoint.json', state)
    qualified_stop = stopped and request is not None and stop_acknowledged(request, state)
    result = dict(systemd=observed, checkpoint=state, processes_stopped=stopped,
                  stop_qualified=qualified_stop and data_reconciled, data_reconciled=data_reconciled, automatic_resume=False, automatic_cleanup=False)
    if args.command == 'recover':
        result['disposition'] = 'preserve attempt; review candidate-write intents and outcomes, reconcile before any cleanup or new run'
    print(json.dumps(result, indent=2))
    return 0 if args.command == 'status' or qualified_stop and data_reconciled else 2


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        print(f'Gate 7 provider blocked: {error}', file=sys.stderr)
        sys.exit(2)
