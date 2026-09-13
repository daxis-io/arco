#!/usr/bin/env python3
"""Fail-closed Gate 7 evidence admission and durable systemd stop/status controls.

This tool cannot turn a prepared packet into authorization or qualify a pilot
from calendar dates. Actual workload evidence must satisfy the frozen contract.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import uuid

PILOT_SECONDS = 604800
PHASE_CEILINGS = {
    'provider': dict(elapsed_seconds=21600, s3_requests=2000000, stored_bytes=20 * 1024**3, cost_usd=25),
    'pilot': dict(elapsed_seconds=195 * 3600, s3_requests=100000000, stored_bytes=512 * 1024**3, cost_usd=500),
}
BASE_SHA = '92fd19f11a547ece5004ac94cad83a3527471812'
TELEMETRY = {
    'requests_upper_bound', 'stored_bytes', 'cost_usd', 'unresolved_publications',
    'correctness_failures', 'successful_mutations', 'reads', 'cpu_seconds',
    'rss_bytes', 'projection_lag_seconds', 'reachable_l0', 'cache_ledgers',
    'maintenance_backlog', 'raw_chunk_sha256',
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def finite_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def elapsed_qualified(start_utc, end_utc, start_mono, end_mono):
    """Scalar duration check only; never sufficient for window or pilot admission."""
    values = (start_utc, end_utc, start_mono, end_mono)
    if not all(finite_number(value) for value in values):
        return False
    utc = end_utc - start_utc
    mono = end_mono - start_mono
    return min(utc, mono) >= PILOT_SECONDS and abs(utc - mono) <= 1


def _window_identity(value):
    for key in ('source_sha256', 'config_sha256', 'raw_chunk_sha256'):
        if not re.fullmatch(r'[0-9a-f]{64}', str(value.get(key))):
            raise ValueError(f'invalid window digest: {key}')
    if (type(value.get('schema')) is not int or value['schema'] != 1
            or any(type(value.get(key)) is not int or value[key] < 0
                   for key in ('sequence', 'attempt_id'))
            or not isinstance(value.get('run_id'), str) or not value['run_id']
            or not isinstance(value.get('boot_id'), str) or not value['boot_id']):
        raise ValueError('invalid window schema/run/boot/sequence/attempt')


def _window_state(state):
    _window_identity(state)
    for key in ('start_utc', 'start_monotonic', 'last_utc', 'last_monotonic', 'eligible_seconds'):
        if not finite_number(state.get(key)):
            raise ValueError(f'invalid durable window time: {key}')
    if not re.fullmatch(r'[0-9a-f]{64}', str(state.get('chain_sha256'))):
        raise ValueError('invalid durable chain head')
    if (any(type(state.get(key)) is not bool for key in
            ('baseline_valid', 'stopped', 'reset_required', 'window_complete', 'pilot_qualified'))
            or any(type(state.get(key)) is not int or state[key] < 0 for key in
                   ('outstanding_operations', 'unresolved_publications'))
            or not isinstance(state.get('prior_attempts'), list)):
        raise ValueError('invalid durable window disposition')
    elapsed = min(state['last_utc'] - state['start_utc'],
                  state['last_monotonic'] - state['start_monotonic'])
    active = state['baseline_valid'] and not state['reset_required'] and not state['stopped']
    expected = 0 if state['reset_required'] else elapsed
    if (elapsed < 0 or state['eligible_seconds'] != expected
            or state['window_complete'] != (active and expected >= PILOT_SECONDS)
            or state['pilot_qualified'] is not False
            or (not state['reset_required'] and (state['outstanding_operations'] or state['unresolved_publications']))
            or state['baseline_valid'] != (not state['reset_required'] and not state['stopped'])
            or abs((state['last_utc'] - state['start_utc'])
                   - (state['last_monotonic'] - state['start_monotonic'])) > 1):
        raise ValueError('invalid durable window accounting')
    for index, prior in enumerate(state['prior_attempts']):
        if (not isinstance(prior, dict) or prior.get('attempt_id') != index
                or any(not finite_number(prior.get(key)) for key in
                       ('start_utc', 'start_monotonic', 'last_utc', 'last_monotonic', 'eligible_seconds'))
                or prior['eligible_seconds'] < 0 or not isinstance(prior.get('reset_reason'), str)
                or not prior['reset_reason']):
            raise ValueError('invalid prior attempt summary')
        _window_identity(prior)
        if (prior['eligible_seconds'] != min(prior['last_utc'] - prior['start_utc'],
                                             prior['last_monotonic'] - prior['start_monotonic'])
                or not re.fullmatch(r'[0-9a-f]{64}', str(prior.get('chain_sha256')))):
            raise ValueError('inconsistent prior attempt accounting')
    if len(state['prior_attempts']) != state['attempt_id'] + int(state['reset_required']):
        raise ValueError('prior attempt inventory does not match current disposition')


def _continuity(state, source, config, boot, utc, mono):
    if (source, config) != (state['source_sha256'], state['config_sha256']):
        return 'source/configuration drift'
    if boot != state['boot_id']:
        return 'host boot identity changed'
    if not finite_number(utc) or not finite_number(mono):
        return 'unverifiable clock'
    wall, elapsed = utc - state['last_utc'], mono - state['last_monotonic']
    if min(wall, elapsed) <= 0 or abs(wall - elapsed) > 1:
        return 'clock discontinuity'
    if max(wall, elapsed) > 60:
        return 'coverage gap exceeds 60 seconds'
    if abs((utc - state['start_utc']) - (mono - state['start_monotonic'])) > 1:
        return 'accumulated clock discontinuity'
    return None


def _reset_window(state, reason):
    # Reject the offending heartbeat. A new labeled baseline must start the next attempt.
    prior = list(state['prior_attempts'])
    if not state['reset_required']:
        summary = {key: state[key] for key in
                   ('schema', 'run_id', 'attempt_id', 'sequence', 'source_sha256', 'config_sha256',
                    'boot_id', 'start_utc', 'start_monotonic', 'last_utc', 'last_monotonic',
                    'eligible_seconds', 'raw_chunk_sha256', 'chain_sha256')}
        prior.append(dict(summary, reset_reason=reason))
    return dict(state, reset_required=True, reset_reason=reason, baseline_valid=False,
                eligible_seconds=0, window_complete=False, prior_attempts=prior)


def advance_window(state, heartbeat, raw_chunk):
    """Pure durable-window transition. Caller persists the checkpoint and every raw input.

    Chunks are bounded JSON with an exact heartbeat header (excluding its raw digest)
    and nonempty observations. The missing workload verifier must supply the interval
    verdict. Window completion alone never qualifies a pilot or admits traffic.
    Rejected inputs do not become a baseline: the next attempt needs a new identity.
    """
    if state is not None:
        _window_state(state)
        if state['stopped']:
            raise ValueError('terminal stopped run requires a new run ID and directory')
    _window_identity(heartbeat)
    if (not all(finite_number(heartbeat.get(key)) for key in ('utc', 'monotonic'))
            or not all(type(heartbeat.get(key)) is bool for key in
                       ('interval_workload_qualified', 'remaining_workload_qualified', 'stopped'))
            or not all(type(heartbeat.get(key)) is int and heartbeat[key] >= 0
                       for key in ('outstanding_operations', 'unresolved_publications'))
            or heartbeat.get('planned_restart') not in
            (None, 'writer-0', 'writer-1', 'writer-2', 'writer-3', 'maintenance', 'projection')):
        raise ValueError('incomplete heartbeat/workload evidence')
    if not isinstance(raw_chunk, bytes) or len(raw_chunk) > 16 * 1024**2:
        raise ValueError('raw evidence chunk must be bounded bytes')
    reason = None
    try:
        chunk = json.loads(raw_chunk)
        header = {key: value for key, value in heartbeat.items() if key != 'raw_chunk_sha256'}
        if (hashlib.sha256(raw_chunk).hexdigest() != heartbeat['raw_chunk_sha256']
                or chunk['heartbeat'] != header or not isinstance(chunk['observations'], list)
                or not chunk['observations']):
            reason = 'raw chunk digest/header/observations mismatch'
    except (ValueError, KeyError, TypeError):
        reason = 'missing/corrupt raw evidence chunk'
    if state is not None:
        if heartbeat['run_id'] != state['run_id']:
            raise ValueError('different run identity requires a separate durable run directory')
        expected_attempt = state['attempt_id'] + int(state['reset_required'])
        if (heartbeat['sequence'] != state['sequence'] + 1 or heartbeat['attempt_id'] != expected_attempt):
            reason = reason or 'heartbeat sequence/attempt replay or gap'
        elif heartbeat.get('previous_chain_sha256') != state['chain_sha256']:
            reason = reason or 'broken raw chunk digest chain'
        elif not state['reset_required']:
            reason = reason or _continuity(state, heartbeat['source_sha256'], heartbeat['config_sha256'],
                                           heartbeat['boot_id'], heartbeat['utc'], heartbeat['monotonic'])
    elif heartbeat['sequence'] != 0 or heartbeat['attempt_id'] != 0 or heartbeat.get('previous_chain_sha256') is not None:
        raise ValueError('first heartbeat requires sequence/attempt zero and no predecessor')
    if (not heartbeat['interval_workload_qualified']
            or not heartbeat['remaining_workload_qualified'] or heartbeat['outstanding_operations']
            or heartbeat['unresolved_publications']):
        reason = reason or 'incomplete workload or outstanding/ambiguous operations'
    if reason:
        if state is None:
            raise ValueError('invalid initial baseline: ' + reason)
        return _reset_window(state, reason)
    fresh = state is None or state['reset_required']
    start_utc = heartbeat['utc'] if fresh else state['start_utc']
    start_mono = heartbeat['monotonic'] if fresh else state['start_monotonic']
    prior = [] if state is None else list(state['prior_attempts'])
    if (heartbeat.get('window_start_utc') != start_utc
            or heartbeat.get('window_start_monotonic') != start_mono
            or heartbeat.get('prior_attempts_sha256') != hashlib.sha256(canonical(prior)).hexdigest()):
        if state is None:
            raise ValueError('initial chunk does not bind its window baseline')
        return _reset_window(state, 'raw chunk window/attempt history mismatch')
    eligible = min(heartbeat['utc'] - start_utc, heartbeat['monotonic'] - start_mono)
    predecessor = heartbeat.get('previous_chain_sha256') or '0' * 64
    chain = hashlib.sha256(bytes.fromhex(predecessor) + canonical(heartbeat)).hexdigest()
    result = dict(schema=1, run_id=heartbeat['run_id'], sequence=heartbeat['sequence'],
                  attempt_id=heartbeat['attempt_id'], source_sha256=heartbeat['source_sha256'],
                  config_sha256=heartbeat['config_sha256'], boot_id=heartbeat['boot_id'],
                  start_utc=start_utc, start_monotonic=start_mono, last_utc=heartbeat['utc'],
                  last_monotonic=heartbeat['monotonic'], raw_chunk_sha256=heartbeat['raw_chunk_sha256'],
                  chain_sha256=chain, baseline_valid=not heartbeat['stopped'],
                  eligible_seconds=eligible, window_complete=not heartbeat['stopped'] and eligible >= PILOT_SECONDS,
                  pilot_qualified=False, stopped=heartbeat['stopped'], reset_required=False,
                  outstanding_operations=heartbeat['outstanding_operations'],
                  unresolved_publications=heartbeat['unresolved_publications'], reset_reason=None,
                  prior_attempts=prior)
    _window_state(result)
    return result


def _verify_window_head(state, raw):
    if (not isinstance(raw, bytes) or len(raw) > 16 * 1024**2
            or hashlib.sha256(raw).hexdigest() != state['raw_chunk_sha256']):
        raise ValueError('missing/corrupt checkpoint evidence chunk')
    document = json.loads(raw)
    header = document['heartbeat']
    if not isinstance(document['observations'], list) or not document['observations']:
        raise ValueError('checkpoint chunk has no observations')
    heartbeat = dict(header, raw_chunk_sha256=state['raw_chunk_sha256'])
    _window_identity(heartbeat)
    for key in ('schema', 'run_id', 'sequence', 'attempt_id', 'source_sha256', 'config_sha256',
                'boot_id', 'stopped', 'outstanding_operations', 'unresolved_publications'):
        if header[key] != state[key] or type(header[key]) is not type(state[key]):
            raise ValueError('checkpoint/chunk identity mismatch: ' + key)
    for key, state_key in (('utc', 'last_utc'), ('monotonic', 'last_monotonic'),
                           ('window_start_utc', 'start_utc'), ('window_start_monotonic', 'start_monotonic')):
        if not finite_number(header[key]) or header[key] != state[state_key]:
            raise ValueError('checkpoint/chunk time mismatch: ' + key)
    prior = state['prior_attempts'][:-1] if state['reset_required'] else state['prior_attempts']
    if (header['prior_attempts_sha256'] != hashlib.sha256(canonical(prior)).hexdigest()
            or header['interval_workload_qualified'] is not True
            or header['remaining_workload_qualified'] is not True):
        raise ValueError('checkpoint/chunk history or workload mismatch')
    predecessor = header['previous_chain_sha256']
    if predecessor is None:
        if state['sequence'] != 0 or state['attempt_id'] != 0:
            raise ValueError('missing predecessor chain')
        predecessor = '0' * 64
    if not re.fullmatch(r'[0-9a-f]{64}', str(predecessor)):
        raise ValueError('invalid predecessor chain')
    if hashlib.sha256(bytes.fromhex(predecessor) + canonical(heartbeat)).hexdigest() != state['chain_sha256']:
        raise ValueError('checkpoint/chunk chain head mismatch')


def recovery_decision(state, source, config, boot, utc, mono, last_raw_chunk):
    """Read-only assessment; no downtime credit, checkpoint mutation or worker launch."""
    if state is None:
        return dict(disposition='blocked', reason='checkpoint missing; preserve attempt and reconcile')
    try:
        _window_state(state)
    except (ValueError, KeyError, TypeError) as error:
        return dict(disposition='blocked', reason=f'invalid checkpoint: {error}')
    if state['stopped']:
        return dict(disposition='blocked', reason='terminal stopped run requires a new run ID and directory')
    reason = _continuity(state, source, config, boot, utc, mono)
    try:
        _verify_window_head(state, last_raw_chunk)
    except (ValueError, KeyError, TypeError) as error:
        reason = f'invalid checkpoint evidence: {error}'
    if (state['reset_required'] or not state['baseline_valid']
            or state['outstanding_operations'] or state['unresolved_publications']):
        reason = reason or 'unresolved workload or reset pending; reconcile and start a new labeled attempt'
    return dict(disposition='reset' if reason else 'resume', reason=reason,
                run_id=state['run_id'], attempt_id=state['attempt_id'],
                eligible_seconds=state['eligible_seconds'], downtime_credited_seconds=0,
                pilot_qualified=False, checkpoint_unchanged=True)


def capacity_verdict(mutations, rows_per_mutation, restore_row_limit):
    if any(type(value) is not int or value <= 0
           for value in (mutations, rows_per_mutation, restore_row_limit)):
        raise ValueError('capacity inputs must be positive integers')
    rows = mutations * rows_per_mutation
    return dict(eligible=rows <= restore_row_limit, retained_rows=rows,
                restore_row_limit=restore_row_limit,
                reason='retained receipt/audit rows exceed restore bound'
                if rows > restore_row_limit else 'row bound only; byte/growth proof still required')


def write_checkpoint(path, state, *, exclusive=False):
    """Flush data before rename, then flush its directory; retain prior attempts separately."""
    path = Path(path)
    payload = canonical(state)
    envelope = canonical(dict(state=state, sha256=hashlib.sha256(payload).hexdigest()))
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.checkpoint-', delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(envelope)
            stream.flush()
            os.fsync(stream.fileno())
            if exclusive:
                os.link(temporary, path)  # Atomic exclusive publication; never overwrite a worker checkpoint.
            else:
                os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)


def read_checkpoint(path, source_sha256, config_sha256):
    try:
        envelope = json.loads(Path(path).read_bytes())
        state = envelope['state']
        if envelope['sha256'] != hashlib.sha256(canonical(state)).hexdigest():
            raise ValueError('checkpoint checksum mismatch')
        if (state['source_sha256'], state['config_sha256']) != (source_sha256, config_sha256):
            raise ValueError('source/configuration drift; preserve attempt and reset window')
        if type(state['sequence']) is not int or state['sequence'] < 0 or not state['run_id']:
            raise ValueError('invalid checkpoint sequence or run identity')
        if type(state['stopped']) is not bool:
            raise ValueError('invalid durable stop state')
        return state
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError('invalid or incomplete checkpoint') from error


def validate_telemetry(sample):
    if not isinstance(sample, dict) or not TELEMETRY <= sample.keys():
        raise ValueError('missing required telemetry')
    for key in TELEMETRY - {'cache_ledgers', 'raw_chunk_sha256'}:
        if not finite_number(sample[key]) or sample[key] < 0:
            raise ValueError(f'invalid telemetry: {key}')
    if not isinstance(sample['cache_ledgers'], list):
        raise ValueError('cache ledgers must be per-handle records')
    if not re.fullmatch(r'[0-9a-f]{64}', str(sample['raw_chunk_sha256'])):
        raise ValueError('missing raw evidence digest')
    ceilings = dict(requests_upper_bound=100000000, stored_bytes=512 * 1024**3,
                    cost_usd=500, unresolved_publications=0, correctness_failures=0,
                    projection_lag_seconds=60, reachable_l0=31)
    for key, ceiling in ceilings.items():
        if sample[key] > ceiling:
            raise ValueError(f'qualification ceiling exceeded: {key}')


def cold_ledgers(row, handles):
    handle = row.get('handle_id')
    if not isinstance(handle, str) or not handle or handle in handles:
        raise ValueError('missing or reused cold handle identity')
    handles.add(handle)
    before, after = row.get('initial_cache_ledger'), row.get('cache_ledger')
    if row['mode'] == 'disabled':
        if before is not None or after is not None:
            raise ValueError('disabled cache has a ledger')
        return
    capacities = (32 * 1024**2, 128 * 1024**2) if row['mode'] == 'default' else (1024**2, 4 * 1024**2)
    counters = {'active_loads', 'coalesced', 'declined', 'demands', 'evictions', 'failures',
                'fallbacks', 'high_water_loads', 'high_water_participants',
                'high_water_participants_per_load', 'high_water_reservations', 'hits',
                'loads', 'participants', 'underestimates'}
    pool_fields = {'administration_bytes', 'capacity_bytes', 'high_water_bytes',
                   'high_water_live_evicted_bytes', 'high_water_participant_bytes',
                   'high_water_records', 'high_water_reserved_bytes', 'high_water_resident_bytes',
                   'live_evicted_bytes', 'live_records', 'participant_bytes', 'reserved_bytes', 'resident_bytes'}
    for initial, ledger in ((True, before), (False, after)):
        if not isinstance(ledger, dict) or ledger.keys() != counters | {'metadata', 'decoded'}:
            raise ValueError('incomplete cold cache ledger')
        for key in counters:
            if type(ledger[key]) is not int or ledger[key] < 0 or (initial and ledger[key] != 0):
                raise ValueError('invalid or previously used cold cache ledger')
        for pool, capacity in zip(('metadata', 'decoded'), capacities):
            values = ledger[pool]
            if not isinstance(values, dict) or values.keys() != pool_fields:
                raise ValueError('incomplete ownership ledger')
            for key, value in values.items():
                if type(value) is not int or value < 0:
                    raise ValueError('invalid or previously used ownership ledger')
            if initial:
                allowed = {'capacity_bytes', 'administration_bytes', 'high_water_bytes'}
                if (any(value != 0 for key, value in values.items() if key not in allowed)
                        or values['high_water_bytes'] != values['administration_bytes']):
                    raise ValueError('previously used initial ownership ledger')
            elif any(values[key] != 0 for key in ('reserved_bytes', 'participant_bytes', 'live_evicted_bytes')):
                raise ValueError('cold handle still owns active work')
            for key, value in values.items():
                if key.startswith('high_water_') and key not in ('high_water_bytes', 'high_water_records'):
                    current = key.removeprefix('high_water_')
                    if value < values[current]:
                        raise ValueError('ownership high water below current state')
            if values['high_water_records'] < values['live_records']:
                raise ValueError('record high water below current state')
            owned = sum(values[key] for key in ('administration_bytes', 'resident_bytes',
                                               'live_evicted_bytes', 'participant_bytes', 'reserved_bytes'))
            if (values['capacity_bytes'] != capacity or max(values['high_water_bytes'], owned) > capacity
                    or values['high_water_bytes'] < owned):
                raise ValueError('cache ownership ceiling exceeded')
    for pool in ('metadata', 'decoded'):
        if (after[pool]['administration_bytes'] != before[pool]['administration_bytes']
                or any(after[pool][key] < value for key, value in before[pool].items()
                       if key.startswith('high_water_'))):
            raise ValueError('ownership ledger decreased from initial state')
    peaks = dict(high_water_loads=8, high_water_participants=256,
                 high_water_participants_per_load=32, high_water_reservations=64 * 1024**2)
    if (any(after[key] > ceiling for key, ceiling in peaks.items())
            or after['metadata']['high_water_records'] > 1024
            or after['decoded']['high_water_records'] > 4096):
        raise ValueError('cache concurrency/ownership ceiling exceeded')
    if after['underestimates'] != 0:
        raise ValueError('cache reservation underestimated authenticated work')
    if after['active_loads'] != 0 or after['participants'] != 0:
        raise ValueError('cold handle still has active work')
    if after['demands'] == 0 or after['loads'] == 0 or after['failures'] != 0:
        raise ValueError('cold operation lacks successful authenticated load evidence')


def cold_summary(records):
    groups = {(run, mode, operation): {} for run in range(5)
              for mode in ('disabled', 'default', 'pressure')
              for operation in ('point', 'scan')}
    handles = set()
    for row in records:
        try:
            key = row['repetition'], row['mode'], row['operation']
            observation = row['observation']
            duration = row['elapsed_ns']
            if (row['kind'] != 'cold-observation' or key not in groups
                    or type(row['repetition']) is not int
                    or type(observation) is not int or not 0 <= observation < 200
                    or observation in groups[key] or row['fresh_cache'] is not True
                    or type(duration) is not int or duration < 0):
                raise ValueError('invalid, duplicate or non-cold observation')
            cold_ledgers(row, handles)
            groups[key][observation] = duration
        except (KeyError, TypeError) as error:
            raise ValueError('incomplete cold observation') from error
    summary = []
    for (run, mode, operation), samples in groups.items():
        if len(samples) != 200:
            raise ValueError('each run/mode/operation requires 200 independent cold samples')
        durations = sorted(samples.values())
        summary.append(dict(repetition=run, mode=mode, operation=operation,
                            observations=200, p50_ns=durations[99], p99_ns=durations[197]))
    return summary


def model_trace_summary(text, seed, mode, block):
    """Index both preserved annotations of each generated operation without rewriting raw evidence."""
    steps = {}
    declared = []
    layouts = []
    pattern = re.compile(r'seed=(\d+) mode=(\d+) block=(\d+) step=(\d+) family=(\d+) before_sequence=(\d+) history=([0-9a-f]{64})$')
    for number, line in enumerate(text.splitlines(), 1):
        match = pattern.fullmatch(line)
        if match:
            found_seed, found_mode, found_block, step, family = map(int, match.groups()[:5])
            if (found_seed, found_mode, found_block) != (seed, mode, block) or not 0 <= family < 10:
                raise ValueError('model trace identity/family mismatch')
            steps.setdefault(step, []).append((number, line, family))
        elif line.startswith('accepted_family_counts='):
            declared.append(json.loads(line.split('=', 1)[1]))
        elif line.startswith('physical_layout '):
            target, batches = line.split(' batches=', 1)
            if target != f'physical_layout target={block}':
                raise ValueError('physical layout identity mismatch')
            layouts.append(json.loads(batches))
    if set(steps) != set(range(128)) or len(declared) != 1 or len(layouts) != 1:
        raise ValueError('incomplete model operation/layout inventory')
    counts = [0] * 10
    index = []
    for step, annotations in sorted(steps.items()):
        if len(annotations) != 2 or annotations[0][1] != annotations[1][1]:
            raise ValueError('each generated step must have exactly two identical evidence annotations')
        family = annotations[0][2]
        counts[family] += 1
        index.append(dict(step=step, family=family, annotation_lines=[a[0] for a in annotations]))
    if counts != declared[0] or not all(counts) or sum(counts) != 128:
        raise ValueError('actual model family inventory mismatch')
    batches = layouts[0]
    if (not batches or any(type(value) is not int or value <= 0 for value in batches)
            or (block == 32768 and max(batches) <= 1)
            or (block == 262144 and any(value != 1 for value in batches))):
        raise ValueError('two physical layouts are not established')
    return dict(seed=seed, mode=mode, block=block, operations=128, actual_family_counts=counts,
                batches=batches, steps=index, raw_sha256=hashlib.sha256(text.encode()).hexdigest())


def request_stop(checkpoint, state, unit):
    request = dict(nonce=uuid.uuid4().hex, run_id=state['run_id'],
                   source_sha256=state['source_sha256'], config_sha256=state['config_sha256'],
                   after_sequence=state['sequence'], unit=unit)
    write_checkpoint(Path(checkpoint).with_name('stop-request.json'), request, exclusive=True)
    return request


def stop_acknowledged(request, state):
    return (all(state.get(key) == request[key]
                for key in ('run_id', 'source_sha256', 'config_sha256'))
            and type(state.get('sequence')) is int
            and state['sequence'] > request['after_sequence']
            and state.get('stop_acknowledgement') == request['nonce']
            and state.get('stopped') is True
            and type(state.get('outstanding_operations')) is int
            and state['outstanding_operations'] == 0
            and type(state.get('unresolved_publications')) is int
            and state['unresolved_publications'] == 0)


def stop_proven(unit_state, children, durable_stop, outstanding):
    return (unit_state == 'inactive' and type(children) is int and children == 0
            and durable_stop is True and type(outstanding) is int and outstanding == 0)


def systemd_status(unit):
    if not re.fullmatch(r'arco-gate7-[a-z0-9-]+\.service', unit):
        raise ValueError('unit must be a dedicated arco-gate7 service')
    result = subprocess.run(['systemctl', 'show', unit, '--property=ActiveState,ControlGroup,MainPID,LoadState'],
                            check=True, text=True, capture_output=True)
    fields = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    if fields.get('LoadState') != 'loaded':
        raise ValueError('systemd unit is not loaded')
    group = fields.get('ControlGroup', '')
    children = set()
    if group:
        root = Path('/sys/fs/cgroup')
        path = (root / group.lstrip('/')).resolve()
        if not path.is_relative_to(root) or path == root:
            raise ValueError('invalid cgroup identity')
        if path.exists():
            for processes in path.rglob('cgroup.procs'):
                children.update(processes.read_text().split())
    if fields.get('MainPID') not in ('0', None):
        children.add(fields['MainPID'])
    return dict(unit=unit, active_state=fields.get('ActiveState'),
                cgroup=group, children=sorted(children))


def validate_packet(packet, expected_packet_sha256):
    if (not re.fullmatch(r'[0-9a-f]{64}', str(expected_packet_sha256))
            or hashlib.sha256(canonical(packet)).hexdigest() != expected_packet_sha256):
        raise ValueError('external packet digest mismatch')
    phase = packet.get('phase')
    if phase not in PHASE_CEILINGS:
        raise ValueError('unknown execution phase')
    ceilings = packet.get('ceilings')
    maximum = PHASE_CEILINGS[phase]
    if not isinstance(ceilings, dict) or ceilings.keys() != maximum.keys():
        raise ValueError('incomplete phase ceiling schema')
    for key, limit in maximum.items():
        value = ceilings[key]
        if not finite_number(value) or value <= 0 or value > limit:
            raise ValueError(f'frozen phase ceiling exceeded: {key}')
        if key != 'cost_usd' and type(value) is not int:
            raise ValueError(f'invalid phase ceiling: {key}')
    if packet.get('status') != 'ready-for-approval':
        raise ValueError('blocked execution packet: ' + '; '.join(packet.get('blockers', ['not ready'])))
    required = ['base_sha', 'source_manifest', 'source_sha256', 'contract', 'contract_sha256',
                'binary_patch', 'binary_patch_sha256', 'executable', 'executable_sha256',
                'scenario_inventory', 'account', 'principal', 'host', 'region', 'bucket',
                'prefix', 'credentials', 'permissions', 'endpoints', 'ceilings', 'cost_calculation',
                'evidence_destination', 'abort_conditions', 'cleanup_scope', 'build_inputs']
    if any(not packet.get(key) for key in required):
        raise ValueError('incomplete execution packet')
    if packet['base_sha'] != BASE_SHA:
        raise ValueError('Gate 6 base drift')
    for field, expected in [('source_manifest', 'source_sha256'), ('contract', 'contract_sha256'),
                            ('binary_patch', 'binary_patch_sha256'), ('executable', 'executable_sha256')]:
        if digest(packet[field]) != packet[expected]:
            raise ValueError(f'identity mismatch: {field}')
    inventory = subprocess.run([packet['executable'], 'inventory'], check=True,
                               text=True, capture_output=True, timeout=30)
    if json.loads(inventory.stdout) != packet['scenario_inventory']:
        raise ValueError('discovered scenario inventory mismatch')
    raise ValueError('provider/pilot execution inventory is unavailable in this candidate; preserve blocked packet')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('capacity')
    models = commands.add_parser('models')
    models.add_argument('directory', type=Path)
    validate = commands.add_parser('validate')
    validate.add_argument('packet', type=Path)
    validate.add_argument('--expected-packet-sha256', required=True,
                          help='independently reviewed canonical JSON digest; never taken from the packet')
    recover = commands.add_parser('recover', help='read-only checkpoint/clock recovery assessment')
    recover.add_argument('checkpoint', type=Path)
    recover.add_argument('last_raw_chunk', type=Path)
    recover.add_argument('--source-sha256', required=True)
    recover.add_argument('--config-sha256', required=True)
    status = commands.add_parser('status')
    status.add_argument('unit')
    stop = commands.add_parser('stop')
    stop.add_argument('unit')
    stop.add_argument('checkpoint', type=Path)
    stop.add_argument('--source-sha256', required=True)
    stop.add_argument('--config-sha256', required=True)
    args = parser.parse_args()
    if args.command == 'models':
        records = [model_trace_summary((args.directory / f'model-{seed}-{mode}-{block}.log').read_text(), seed, mode, block)
                   for seed in range(33, 65) for mode in range(3) for block in (32768, 262144)]
        print(json.dumps(dict(cases=192, generated_operations=24576, records=records), indent=2))
        return 0
    if args.command == 'capacity':
        result = capacity_verdict(1209600, 2, 1000000)
        print(json.dumps(result, indent=2))
        return 0 if result['eligible'] else 2
    if args.command == 'recover':
        import time
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        envelope = json.loads(args.checkpoint.read_bytes())
        state = envelope['state']
        # Validate the envelope against its original identity before assessing current drift.
        state = read_checkpoint(args.checkpoint, state['source_sha256'], state['config_sha256'])
        result = recovery_decision(state, args.source_sha256, args.config_sha256,
                                   boot, time.time(), time.monotonic(), args.last_raw_chunk.read_bytes())
        print(json.dumps(result, indent=2))
        return 0 if result['disposition'] == 'resume' else 2
    if args.command == 'validate':
        validate_packet(json.loads(args.packet.read_bytes()), args.expected_packet_sha256)
    elif args.command == 'status':
        print(json.dumps(systemd_status(args.unit), indent=2))
    else:
        state = read_checkpoint(args.checkpoint, args.source_sha256, args.config_sha256)
        systemd_status(args.unit)  # Validate exact unit before mutation.
        request = request_stop(args.checkpoint, state, args.unit)
        subprocess.run(['systemctl', 'stop', args.unit], check=True, timeout=90)
        observed = systemd_status(args.unit)
        # Worker must have durably recorded disposition before exiting.
        state = read_checkpoint(args.checkpoint, args.source_sha256, args.config_sha256)
        if not (stop_acknowledged(request, state)
                and stop_proven(observed['active_state'], len(observed['children']),
                                state['stopped'], state.get('outstanding_operations'))):
            raise ValueError('stop is unproven; preserve objects and reconcile outstanding operations')
        print(json.dumps(observed, indent=2))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f'Gate 7 blocked: {error}', file=sys.stderr)
        sys.exit(2)
