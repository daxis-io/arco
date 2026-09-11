#!/usr/bin/env python3
"""Sequential, local-only runtime reuse and projection measurements against Gate 6."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import statistics
import shutil
import subprocess
import tarfile

BASE = "92fd19f11a547ece5004ac94cad83a3527471812"
ROOT = Path(__file__).resolve().parents[1]
HELPERS = [
    "crates/arco-catalog/benches/support/control_cost.rs",
    "crates/arco-catalog/benches/support/runtime_reuse_cost.rs",
    "crates/arco-catalog/src/catalog_authority/projection_measurement.rs",
    "crates/arco-catalog/tests/runtime_reuse_cost.rs",
]


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare_baseline(path):
    path.mkdir(parents=True, exist_ok=False)
    archive = subprocess.check_output(["git", "archive", BASE], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as files:
        files.extractall(path, filter="data")
    original = {str(p.relative_to(path)): sha(p) for p in path.rglob("*") if p.is_file()}
    for relative in HELPERS:
        target = path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((ROOT / relative).read_bytes())
    authority = path / "crates/arco-catalog/src/catalog_authority.rs"
    # Test-only observability shim: the baseline has no retained registry handles.
    authority.write_text(authority.read_text() + '''
#[cfg(feature = "test-utils")]
#[doc(hidden)]
pub mod projection_measurement;
#[cfg(feature = "test-utils")]
impl CatalogAuthorityBindings {
    #[doc(hidden)]
    pub fn test_read_cache_statistics(&self) -> Vec<crate::ControlMvpReadCacheStatistics> { Vec::new() }
}
''')
    driver = path / HELPERS[-1]
    driver.write_text(driver.read_text().replace(
        "serde_json::to_value(bindings.test_read_cache_statistics()).unwrap()",
        "{ let _ = bindings; serde_json::Value::Null }"))
    changed = {relative for relative, digest in original.items() if sha(path / relative) != digest}
    assert changed <= set(HELPERS) | {str(authority.relative_to(path))}, changed
    for relative in HELPERS[:-1]:
        assert sha(path / relative) == sha(ROOT / relative)
    return {"base": BASE, "changed_base_files": sorted(changed),
            "test_overlay_sha256": {p: sha(path / p) for p in HELPERS},
            "unchanged_base_files": len(original) - len(changed)}


def validated_runtime(report, candidate):
    assert len(report) == 5
    assert [run["repetition"] for run in report] == list(range(1, 6))
    samples = {}
    for run in report:
        assert [f["tables"] for f in run["fixtures"]] == [8, 64]
        for fixture in run["fixtures"]:
            assert fixture["parity"] is True
            assert fixture["cold_fresh_starts"] == {"cold_point": 200, "cold_scan": 200}
            assert {op["phase"] for op in fixture["operations"]} == {"cold_point", "cold_scan", "warm_point", "warm_scan"}
            for op in fixture["operations"]:
                n = 200 if op["phase"].startswith("cold") else 40
                assert op["samples"] == n
                durations = op["latency_samples_micros"]
                assert len(durations) == n and sorted(durations) == durations
                assert op["latency_p50_micros"] == durations[(n * 50 + 99) // 100 - 1]
                assert op["latency_p99_micros"] == durations[(n * 99 + 99) // 100 - 1]
                if candidate and op["phase"].startswith("warm"):
                    backend = op["backend"]
                    assert backend["head_attempts"] > 0
                    assert backend["block_decode_calls"] == backend["metadata_decode_calls"] == 0
                    for kind in ("data", "directory", "transaction"):
                        assert backend["object_reads"].get(kind, {}).get("returned_bytes", 0) == 0
                samples[(run["repetition"], fixture["tables"], op["phase"])] = (fixture["fixture_sha256"], op)
            if candidate:
                assert len(fixture["cache"]) == 1
                for cache in fixture["cache"]:
                    assert cache["underestimates"] == cache["active_loads"] == cache["participants"] == 0
                    for pool in ("metadata", "decoded"):
                        assert cache[pool]["high_water_bytes"] <= cache[pool]["capacity_bytes"]
                        assert cache[pool]["reserved_bytes"] == 0
    return samples


def compare(base, candidate, projection):
    left = validated_runtime(base, False)
    right = validated_runtime(candidate, True)
    assert left.keys() == right.keys()
    for key in left:
        assert left[key][0] == right[key][0], ("fixture mismatch", key)
    ratios = []
    for tables in (8, 64):
        for phase in ("cold_point", "cold_scan", "warm_point", "warm_scan"):
            row = {"tables": tables, "phase": phase}
            for metric in ("latency_p50_micros", "latency_p99_micros"):
                b = statistics.median(left[(r, tables, phase)][1][metric] for r in range(1, 6))
                c = statistics.median(right[(r, tables, phase)][1][metric] for r in range(1, 6))
                row[metric] = {"base": b, "candidate": c, "ratio": c / b}
            b = statistics.median(left[(r, tables, phase)][1]["allocations"]["bytes"] for r in range(1, 6))
            c = statistics.median(right[(r, tables, phase)][1]["allocations"]["bytes"] for r in range(1, 6))
            row["allocation_ratio"] = c / b
            row["heads_per_operation"] = statistics.median(
                right[(r, tables, phase)][1]["backend"]["head_attempts"] / right[(r, tables, phase)][1]["samples"] for r in range(1, 6))
            if phase.startswith("cold"):
                assert row["latency_p50_micros"]["ratio"] <= 1.25, row
                assert row["latency_p99_micros"]["ratio"] <= 1.5, row
                assert row["allocation_ratio"] <= 2, row
            ratios.append(row)
    assert len(projection) == 60
    expected = {(r, t, b, i) for r in range(1, 6) for t in (8, 64) for b in (1, 8, 16) for i in (False, True)}
    seen = set()
    projection_fixtures = {}
    for run in projection:
        sample = run["sample"]
        key = run["repetition"], sample["tables"], sample["backlog"], sample["interrupted"]
        assert key not in seen
        seen.add(key)
        assert sample["parity"] is True
        fixture_key = sample["tables"], sample["backlog"]
        fingerprint = sample["fixture_sha256"]
        assert projection_fixtures.setdefault(fixture_key, fingerprint) == fingerprint
        for operation in sample["operations"]:
            backend = operation["backend"]
            for counter in ("head_attempts", "get_attempts", "range_get_attempts", "put_attempts", "read_bytes", "write_attempt_bytes"):
                assert sum(phase[counter] for phase in backend["phases"].values()) == backend[counter]
        assert len(sample["watermarks"]) == sample["backlog"]
        for watermark in sample["watermarks"]:
            assert watermark["parity"] is True
            assert watermark["tables"] == sample["tables"]
            assert watermark["columns"] == 4 * sample["tables"]
        required = {"projection-discovery", "projection-source", "projection-publication", "projection-status-ack"}
        stages = set(sample["stages"]["drain"])
        assert required <= stages <= required | {"projection-maintenance"}
        assert ("projection-maintenance" in stages) == (sample["maintenance_retries"] > 0)
        repeat = next(op for op in sample["operations"] if op["phase"] == "repeat_notification")
        assert repeat["samples"] == 3 and repeat["backend"]["put_attempts"] == 0
    assert seen == expected
    return {"passed": True, "runtime_ratios": ratios, "projection_schedules": len(seen),
            "cold_observations_per_source": 4000, "warm_observations_per_source": 800}


def run(args):
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    baseline = evidence / "baseline"
    write(evidence / "baseline-overlay.json", prepare_baseline(baseline))
    source = {str(p.relative_to(ROOT)): sha(p) for p in (ROOT / "crates").rglob("*.rs")}
    write(evidence / "source-before.json", source)
    env = os.environ | {"CARGO_INCREMENTAL": "0", "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0",
                       "ARCO_RUNTIME_FIXTURES": str(evidence / "fixtures")}
    if args.fixtures_from:
        shutil.copytree(args.fixtures_from, evidence / "fixtures")

    def execute(label, cwd, command, extra):
        write(evidence / f"{label}-command.json", {"cwd": str(cwd), "command": command, "environment": extra})
        print(f"Running {label}; log: {evidence / (label + '.log')}", flush=True)
        with (evidence / f"{label}.log").open("w") as log:
            result = subprocess.run(command, cwd=cwd, env=env | extra, stdout=log, stderr=subprocess.STDOUT)
        write(evidence / f"{label}-exit.json", {"exit_code": result.returncode, "log_sha256": sha(evidence / f"{label}.log")})
        assert result.returncode == 0, label

    # Build both sources before timing, then alternate AB/BA in five frozen pairs.
    binaries = {}
    locations = {"candidate": (ROOT, args.target.resolve()), "baseline": (baseline, evidence / "baseline-target")}
    for name, (cwd, target) in locations.items():
        label = name + "-build"
        command = ["cargo", "test", "-p", "arco-catalog", "--features", "test-utils", "--test", "runtime_reuse_cost", "--locked", "--no-run", "--message-format=json"]
        execute(label, cwd, command, {"CARGO_TARGET_DIR": str(target)})
        for line in (evidence / f"{label}.log").read_text().splitlines():
            if line.startswith("{"):
                event = json.loads(line)
                if event.get("reason") == "compiler-artifact" and event["target"]["name"] == "runtime_reuse_cost" and event.get("executable"):
                    binaries[name] = event["executable"]
        assert name in binaries
    combined = {"candidate": [], "baseline": []}
    for repetition in range(1, 6):
        for name in (("candidate", "baseline") if repetition % 2 else ("baseline", "candidate")):
            label = f"{name}-runtime-{repetition}"
            report = evidence / f"{label}.json"
            execute(label, locations[name][0], [binaries[name], "runtime_measurement", "--ignored", "--exact", "--nocapture"],
                    {"ARCO_RUNTIME_REPORT": str(report), "ARCO_RUNTIME_REPETITION": str(repetition)})
            part = json.loads(report.read_text())
            assert len(part) == 1 and part[0]["repetition"] == repetition
            combined[name].extend(part)
            write(evidence / f"{name}-runtime.json", combined[name])
    execute("projection", ROOT, [binaries["candidate"], "projection_measurement", "--ignored", "--exact", "--nocapture"],
            {"ARCO_RUNTIME_REPORT": str(evidence / "projection.json")})
    after = {str(p.relative_to(ROOT)): sha(p) for p in (ROOT / "crates").rglob("*.rs")}
    write(evidence / "source-after.json", after)
    assert source == after, "source changed during measurement"
    result = compare(*(json.loads((evidence / f"{name}.json").read_text()) for name in ("baseline-runtime", "candidate-runtime", "projection")))
    write(evidence / "acceptance.json", result)
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run")
    runner.add_argument("--evidence", type=Path, required=True, help="new evidence directory (must not exist)")
    runner.add_argument("--fixtures-from", type=Path, help="copy previously frozen input images without modifying them")
    runner.add_argument("--target", type=Path, required=True, help="dedicated candidate Cargo target")
    check = commands.add_parser("compare")
    check.add_argument("evidence", type=Path)
    args = parser.parse_args()
    if args.command == "run":
        run(args)
    else:
        result = compare(*(json.loads((args.evidence / f"{name}.json").read_text()) for name in ("baseline-runtime", "candidate-runtime", "projection")))
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
