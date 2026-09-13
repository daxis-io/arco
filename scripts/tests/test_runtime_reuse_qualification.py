"""Prevent undersampled or unaccounted runtime evidence from being accepted."""
import copy
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("qualification", Path(__file__).parents[1] / "runtime_reuse_qualification.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def report():
    operations = []
    for phase in ("cold_point", "cold_scan", "warm_point", "warm_scan"):
        n = 200 if phase.startswith("cold") else 40
        operations.append({"phase": phase, "samples": n, "latency_samples_micros": list(range(1, n + 1)),
                           "latency_p50_micros": n // 2, "latency_p99_micros": (n * 99 + 99) // 100,
                           "backend": {"head_attempts": n, "block_decode_calls": 0, "metadata_decode_calls": 0, "object_reads": {}}})
    pool = {"high_water_bytes": 16, "capacity_bytes": 32, "reserved_bytes": 0}
    fixture = {"parity": True, "fixture_sha256": "frozen", "cold_fresh_starts": {"cold_point": 200, "cold_scan": 200},
               "operations": operations, "cache": [{"underestimates": 0, "active_loads": 0, "participants": 0, "metadata": pool, "decoded": pool}]}
    return [{"repetition": r, "fixtures": [copy.deepcopy(fixture) | {"tables": t} for t in (8, 64)]} for r in range(1, 6)]


class ValidationTests(unittest.TestCase):
    def test_full_sample_counts_and_independent_percentiles(self):
        self.assertEqual(len(MODULE.validated_runtime(report(), True)), 40)

    def test_rejects_singleton_cold_and_forged_percentiles(self):
        for field, value in [("samples", 1), ("latency_samples_micros", [1]), ("latency_p99_micros", 100)]:
            data = report()
            data[0]["fixtures"][0]["operations"][0][field] = value
            with self.assertRaises(AssertionError):
                MODULE.validated_runtime(data, True)

    def test_rejects_nonboolean_parity_and_duplicate_repetitions(self):
        data = report()
        data[0]["fixtures"][0]["parity"] = "true"
        with self.assertRaises(AssertionError):
            MODULE.validated_runtime(data, True)
        data = report()
        data[1]["repetition"] = 1
        with self.assertRaises(AssertionError):
            MODULE.validated_runtime(data, True)

    def test_rejects_warm_payloads_missing_heads_and_underestimates(self):
        for field, value in [("head_attempts", 0), ("block_decode_calls", 1), ("object_reads", {"directory": {"returned_bytes": 1}})]:
            data = report()
            data[0]["fixtures"][0]["operations"][2]["backend"][field] = value
            with self.assertRaises(AssertionError):
                MODULE.validated_runtime(data, True)
        data = report()
        data[0]["fixtures"][0]["cache"][0]["underestimates"] = 1
        with self.assertRaises(AssertionError):
            MODULE.validated_runtime(data, True)


if __name__ == "__main__":
    unittest.main()
