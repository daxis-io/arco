"""Gate 7 verdicts must fail closed on incomplete evidence."""
import copy
import importlib.util
import pathlib
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / 'gate7_qualification.py'
spec = importlib.util.spec_from_file_location('gate7', SCRIPT)
gate7 = importlib.util.module_from_spec(spec)
if SCRIPT.exists():
    spec.loader.exec_module(gate7)


class QualificationTests(unittest.TestCase):
    def test_early_calendar_completion_is_rejected(self):
        for elapsed in [86400, 6 * 86400, 6 * 86400 + 1]:
            with self.subTest(elapsed=elapsed):
                self.assertFalse(gate7.elapsed_qualified(0, elapsed, 0, elapsed))
        self.assertTrue(gate7.elapsed_qualified(0, 604800, 0, 604800))

    def test_clock_discontinuity_is_rejected(self):
        for utc, mono in [(604800, 604799), (604800, 604860), (-1, 604800),
                          (float('nan'), 604800), (float('inf'), 604800)]:
            self.assertFalse(gate7.elapsed_qualified(0, utc, 0, mono))

    def test_checkpoint_loss_corruption_and_source_drift_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'checkpoint.json'
            with self.assertRaises((ValueError, OSError)):
                gate7.read_checkpoint(path, 'a' * 64, 'b' * 64)
            state = {'source_sha256': 'a' * 64, 'config_sha256': 'b' * 64,
                     'run_id': 'test', 'sequence': 0, 'stopped': False}
            gate7.write_checkpoint(path, state)
            self.assertEqual(state, gate7.read_checkpoint(path, 'a' * 64, 'b' * 64))
            with self.assertRaises(ValueError):
                gate7.read_checkpoint(path, 'c' * 64, 'b' * 64)
            path.write_text(path.read_text().replace('"sequence":0', '"sequence":1'))
            with self.assertRaises(ValueError):
                gate7.read_checkpoint(path, 'a' * 64, 'b' * 64)

    def test_capacity_is_a_prerequisite(self):
        verdict = gate7.capacity_verdict(1209600, 2, 1000000)
        self.assertFalse(verdict['eligible'])
        self.assertEqual(2419200, verdict['retained_rows'])
        for args in [(0, 2, 1000000), (True, 2, 1000000), (-1, 2, 1000000)]:
            with self.assertRaises(ValueError):
                gate7.capacity_verdict(*args)

    def test_telemetry_is_mandatory_and_limits_are_inclusive(self):
        sample = dict(requests_upper_bound=0, stored_bytes=0, cost_usd=0,
                      unresolved_publications=0, correctness_failures=0,
                      successful_mutations=0, reads=0, cpu_seconds=0, rss_bytes=0,
                      projection_lag_seconds=0, reachable_l0=0, cache_ledgers=[],
                      maintenance_backlog=0, raw_chunk_sha256='a' * 64)
        gate7.validate_telemetry(sample)
        for key in sample:
            broken = copy.deepcopy(sample)
            del broken[key]
            with self.subTest(key=key), self.assertRaises(ValueError):
                gate7.validate_telemetry(broken)
        for key in ['requests_upper_bound', 'stored_bytes', 'cost_usd',
                    'unresolved_publications', 'correctness_failures']:
            broken = copy.deepcopy(sample)
            broken[key] = 10**12
            with self.subTest(key=key), self.assertRaises(ValueError):
                gate7.validate_telemetry(broken)

    def test_cold_observation_inventory_and_nearest_rank(self):
        records = [dict(kind='cold-observation', repetition=run, mode=mode,
                        operation=op, observation=n, fresh_cache=True,
                        elapsed_ns=n + 1, cache_ledger=None)
                   for run in range(5) for mode in ('disabled', 'default', 'pressure')
                   for op in ('point', 'scan') for n in range(200)]
        for row in records:
            row['handle_id'] = f"{row['repetition']}-{row['mode']}-{row['operation']}-{row['observation']}"
            row['initial_cache_ledger'] = None
            if row['mode'] != 'disabled':
                pool = dict(administration_bytes=0, capacity_bytes=0, high_water_bytes=0,
                            high_water_live_evicted_bytes=0, high_water_participant_bytes=0,
                            high_water_records=0, high_water_reserved_bytes=0, high_water_resident_bytes=0,
                            live_evicted_bytes=0, live_records=0, participant_bytes=0,
                            reserved_bytes=0, resident_bytes=0)
                ledger = dict(active_loads=0, coalesced=0, declined=0, demands=0,
                              evictions=0, failures=0, fallbacks=0, high_water_loads=0,
                              high_water_participants=0, high_water_participants_per_load=0,
                              high_water_reservations=0, hits=0, loads=0, participants=0,
                              underestimates=0)
                factor = 32 if row['mode'] == 'default' else 1
                ledger['metadata'] = dict(pool, capacity_bytes=factor * 1024**2,
                                          administration_bytes=8609, high_water_bytes=8609)
                ledger['decoded'] = dict(pool, capacity_bytes=factor * 4 * 1024**2)
                row['initial_cache_ledger'] = ledger
                row['cache_ledger'] = copy.deepcopy(ledger)
                row['cache_ledger'].update(demands=1, loads=1)
        summary = gate7.cold_summary(records)
        self.assertEqual(len(summary), 30)
        self.assertTrue(all(row['p50_ns'] == 100 and row['p99_ns'] == 198 for row in summary))
        enabled = next(row for row in records if row['mode'] == 'default')
        for field in ('active_loads', 'participants'):
            damaged = copy.deepcopy(enabled)
            damaged['cache_ledger'][field] = 1
            with self.assertRaisesRegex(ValueError, 'active work'):
                gate7.cold_ledgers(damaged, set())
        damaged = copy.deepcopy(enabled)
        damaged['initial_cache_ledger']['loads'] = 1
        with self.assertRaisesRegex(ValueError, 'previously used'):
            gate7.cold_ledgers(damaged, set())
        damaged = copy.deepcopy(enabled)
        damaged['cache_ledger']['metadata']['high_water_bytes'] = 0
        with self.assertRaises(ValueError):
            gate7.cold_ledgers(damaged, set())
        for field, peak in [('high_water_loads', 9), ('high_water_participants', 257),
                            ('high_water_participants_per_load', 33),
                            ('high_water_reservations', 64 * 1024**2 + 1)]:
            damaged = copy.deepcopy(enabled)
            damaged['cache_ledger'][field] = peak
            with self.assertRaisesRegex(ValueError, 'ceiling'):
                gate7.cold_ledgers(damaged, set())
        for damaged in (records[:-1], records + [records[0]],
                        [dict(row, fresh_cache=False) if i == 0 else row
                         for i, row in enumerate(records)],
                        [dict(row, cache_ledger=None) if row['mode'] == 'default' else row for row in records],
                        [dict(row, handle_id=records[0]['handle_id']) if i == 1 else row
                         for i, row in enumerate(records)],
                        [dict(row, elapsed_ns=float('nan')) if i == 0 else row
                         for i, row in enumerate(records)]):
            with self.assertRaises(ValueError):
                gate7.cold_summary(damaged)

    def test_packet_requires_external_digest_and_frozen_phase_budgets(self):
        packet = dict(status='ready-for-approval', phase='provider',
                      ceilings=dict(elapsed_seconds=21600, s3_requests=2000000,
                                    stored_bytes=20 * 1024**3, cost_usd=25))
        for phase in ('provider', 'pilot', 'unknown'):
            damaged = dict(packet, phase=phase)
            for key in packet['ceilings']:
                damaged['ceilings'] = dict(packet['ceilings'], **{key: 10**18})
                with self.assertRaisesRegex(ValueError, 'ceiling|phase'):
                    gate7.validate_packet(damaged, gate7.hashlib.sha256(gate7.canonical(damaged)).hexdigest())
        with self.assertRaisesRegex(ValueError, 'external packet digest'):
            gate7.validate_packet(packet, 'a' * 64)

    def test_stop_requires_new_acknowledgement_and_exclusive_request(self):
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = pathlib.Path(directory) / 'checkpoint.json'
            state = dict(run_id='test', sequence=7, stopped=True,
                         outstanding_operations=0, unresolved_publications=0, source_sha256='a' * 64,
                         config_sha256='b' * 64)
            gate7.write_checkpoint(checkpoint, state)
            request = gate7.request_stop(checkpoint, state, 'arco-gate7-test.service')
            with self.assertRaises(FileExistsError):
                gate7.request_stop(checkpoint, state, 'arco-gate7-test.service')
            self.assertFalse(gate7.stop_acknowledged(request, state))
            acknowledged = dict(state, sequence=8, stop_acknowledgement=request['nonce'])
            self.assertTrue(gate7.stop_acknowledged(request, acknowledged))
            self.assertFalse(gate7.stop_acknowledged(request, dict(acknowledged, unresolved_publications=1)))
            self.assertEqual(state, gate7.read_checkpoint(checkpoint, 'a' * 64, 'b' * 64))

    def test_cold_requires_initial_ledgers_and_unique_handles(self):
        record = dict(kind='cold-observation', repetition=0, mode='default',
                      operation='point', observation=0, fresh_cache=True,
                      elapsed_ns=1, cache_ledger=None)
        with self.assertRaisesRegex(ValueError, 'ledger|handle'):
            gate7.cold_summary([record])

    def test_model_dual_annotations_count_each_operation_once(self):
        lines = ['physical_layout target=32768 batches=[5]']
        counts = [0] * 10
        for step in range(128):
            family = step % 10
            counts[family] += 1
            line = f'seed=33 mode=0 block=32768 step={step} family={family} before_sequence={step} history=' + 'a' * 64
            lines.extend([line, line])
        lines.append('accepted_family_counts=' + str(counts))
        result = gate7.model_trace_summary('\n'.join(lines), 33, 0, 32768)
        self.assertEqual(result['operations'], 128)
        self.assertEqual(len(result['steps']), 128)
        for broken in (lines[:-1], lines + [lines[1]], lines[:1] + lines[2:],
                       [line.replace('family=0', 'family=1') if i == 1 else line
                        for i, line in enumerate(lines)]):
            with self.assertRaises(ValueError):
                gate7.model_trace_summary('\n'.join(broken), 33, 0, 32768)

    def test_continuous_window_and_recovery_preserve_failed_attempts(self):
        def sample(state, seconds, **changes):
            heartbeat = dict(schema=1, run_id='window-test',
                             sequence=0 if state is None else state['sequence'] + 1,
                             attempt_id=0 if state is None else state['attempt_id'] + int(state['reset_required']),
                             source_sha256='a' * 64, config_sha256='b' * 64, boot_id='boot-one',
                             utc=seconds, monotonic=seconds,
                             previous_chain_sha256=None if state is None else state['chain_sha256'],
                             interval_workload_qualified=True, planned_restart=None,
                             remaining_workload_qualified=True, outstanding_operations=0,
                             unresolved_publications=0, stopped=False)
            heartbeat['window_start_utc'] = seconds if state is None or state['reset_required'] else state['start_utc']
            heartbeat['window_start_monotonic'] = seconds if state is None or state['reset_required'] else state['start_monotonic']
            heartbeat['prior_attempts_sha256'] = gate7.hashlib.sha256(gate7.canonical([] if state is None else state['prior_attempts'])).hexdigest()
            heartbeat.update(changes)
            raw = gate7.canonical(dict(heartbeat=heartbeat, observations=[dict(kind='fixture-only')]))
            return dict(heartbeat, raw_chunk_sha256=gate7.hashlib.sha256(raw).hexdigest()), raw
        state = None
        for sequence in range(10081):
            row, raw = sample(state, sequence * 60)
            state = gate7.advance_window(state, row, raw)
            if sequence in (1440, 8640, 10079):
                self.assertFalse(state['window_complete'])
        self.assertTrue(state['window_complete'])
        self.assertEqual(state['eligible_seconds'], 604800)
        self.assertFalse(state['pilot_qualified'])
        shifted = dict(state, start_utc=-60, start_monotonic=-60, eligible_seconds=604860)
        self.assertNotEqual(gate7.recovery_decision(shifted, 'a' * 64, 'b' * 64,
                                                  'boot-one', 604830, 604830, raw)['disposition'], 'resume')
        terminal = gate7.advance_window(state, *sample(state, 604830, stopped=True))
        self.assertGreaterEqual(terminal['eligible_seconds'], 604800)
        self.assertEqual(terminal['start_utc'], state['start_utc'])
        first, first_raw = sample(None, 0)
        base = gate7.advance_window(None, first, first_raw)
        forged = dict(base, sequence=10080, start_utc=-604800, start_monotonic=-604800,
                      eligible_seconds=604800, window_complete=True, chain_sha256='c' * 64)
        self.assertNotEqual(gate7.recovery_decision(forged, 'a' * 64, 'b' * 64,
                                                  'boot-one', 30, 30, first_raw)['disposition'], 'resume')
        self.assertEqual(gate7.advance_window(base, *sample(base, 60))['eligible_seconds'], 60)
        cases = [dict(utc=60.01, monotonic=60.01), dict(utc=3, monotonic=1),
                 dict(utc=-1, monotonic=-1), dict(boot_id='boot-two'),
                 dict(source_sha256='c' * 64), dict(config_sha256='d' * 64),
                 dict(sequence=2), dict(sequence=0), dict(previous_chain_sha256='e' * 64),
                 dict(interval_workload_qualified=False), dict(attempt_id=7),
                 dict(unresolved_publications=1), dict(outstanding_operations=1),
                 dict(planned_restart='writer-0', remaining_workload_qualified=False)]
        for change in cases:
            row, raw = sample(base, 30, **change)
            with self.subTest(change=change):
                failed = gate7.advance_window(base, row, raw)
                self.assertEqual(failed['eligible_seconds'], 0)
                self.assertEqual(len(failed['prior_attempts']), 1)
                self.assertFalse(failed['window_complete'])
                self.assertTrue(failed['reset_required'])
                self.assertEqual(failed['sequence'], base['sequence'])
                self.assertEqual(failed['attempt_id'], base['attempt_id'])
                self.assertEqual(base['prior_attempts'], [])
                next_row, next_raw = sample(failed, 90)
                restarted = gate7.advance_window(failed, next_row, next_raw)
                self.assertEqual(restarted['attempt_id'], next_row['attempt_id'])
                self.assertEqual(restarted['eligible_seconds'], 0)
                self.assertFalse(restarted['reset_required'])
        row, raw = sample(base, 30)
        for damaged_raw in (b'corrupted evidence', first_raw):
            damaged_row = dict(row, raw_chunk_sha256=gate7.hashlib.sha256(damaged_raw).hexdigest())
            failed = gate7.advance_window(base, damaged_row, damaged_raw)
            self.assertTrue(failed['reset_required'])
            self.assertIn('chunk', failed['reset_reason'])
        failed_stop = gate7.advance_window(base, *sample(base, 30, stopped=True, unresolved_publications=1))
        self.assertFalse(failed_stop['stopped'])
        self.assertTrue(failed_stop['reset_required'])
        stopped = gate7.advance_window(base, *sample(base, 30, stopped=True))
        with self.assertRaisesRegex(ValueError, 'terminal'):
            gate7.advance_window(stopped, *sample(stopped, 60))
        restarted = gate7.advance_window(base, *sample(base, 30, planned_restart='writer-0'))
        self.assertEqual(restarted['eligible_seconds'], 30)
        for gap, disposition in [(60, 'resume'), (60.01, 'reset')]:
            result = gate7.recovery_decision(base, 'a' * 64, 'b' * 64, 'boot-one', gap, gap, first_raw)
            self.assertEqual(result['disposition'], disposition)
        self.assertEqual(gate7.recovery_decision(base, 'a' * 64, 'b' * 64,
                                               'boot-two', 1, 1, first_raw)['disposition'], 'reset')
        self.assertEqual(gate7.recovery_decision(base, 'a' * 64, 'b' * 64,
                                               'boot-one', 1, 1, b'bad')['disposition'], 'reset')
        self.assertEqual(gate7.recovery_decision(None, 'a' * 64, 'b' * 64,
                                               'boot-one', 1, 1, first_raw)['disposition'], 'blocked')
        near = dict(state, sequence=10079, last_utc=604740, last_monotonic=604740,
                    eligible_seconds=604740, window_complete=False)
        self.assertFalse(gate7.advance_window(near, *sample(near, 604799))['window_complete'])
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            checkpoint = directory / 'checkpoint.json'
            chunk_path = directory / 'chunk.raw'
            chunk_path.write_bytes(first_raw)
            gate7.write_checkpoint(checkpoint, base)
            restored = gate7.read_checkpoint(checkpoint, 'a' * 64, 'b' * 64)
            gap = gate7.advance_window(restored, *sample(restored, 61))
            gate7.write_checkpoint(checkpoint, gap)
            restored = gate7.read_checkpoint(checkpoint, 'a' * 64, 'b' * 64)
            restarted = gate7.advance_window(restored, *sample(restored, 62))
            continued_row, continued_raw = sample(restarted, 92)
            continued = gate7.advance_window(restarted, continued_row, continued_raw)
            self.assertEqual(gate7.recovery_decision(continued, 'a' * 64, 'b' * 64,
                                                   'boot-one', 93, 93, continued_raw)['disposition'], 'resume')
            self.assertEqual(continued['eligible_seconds'], 30)
            self.assertEqual(continued['start_utc'], 62)
            self.assertEqual(chunk_path.read_bytes(), first_raw)
            for broken in (dict(continued, eligible_seconds=29), dict(continued, window_complete=True),
                           dict(continued, pilot_qualified=True), dict(continued, outstanding_operations=True)):
                gate7.write_checkpoint(checkpoint, broken)  # Recomputed checksum cannot hide bad semantics.
                recovered = gate7.read_checkpoint(checkpoint, 'a' * 64, 'b' * 64)
                self.assertEqual(gate7.recovery_decision(recovered, 'a' * 64, 'b' * 64,
                                                       'boot-one', 93, 93, continued_raw)['disposition'], 'blocked')

    def test_stop_requires_no_unit_no_children_and_disposition(self):
        self.assertTrue(gate7.stop_proven('inactive', 0, True, 0))
        for args in [('active', 0, True, 0), ('inactive', 1, True, 0),
                     ('inactive', 0, False, 0), ('inactive', 0, True, 1),
                     ('failed', 0, True, 0)]:
            self.assertFalse(gate7.stop_proven(*args))


if __name__ == '__main__':
    unittest.main()
