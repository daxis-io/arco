"""Exercise hosted fixture guards with synthetic credentials and no storage adapter."""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

sys.dont_write_bytecode = True
from qualify_hosted_s3 import BUCKET, OWNER, REGION, Inventory, evidence_root, session_environment, sha256
from qualify_minio import save


def main():
    if sys.flags.optimize:
        raise SystemExit("qualification requires assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = args.output.absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    config = root / "config.json"
    save(config, {
        "tenant": "proof", "workspace": "wake", "host": {
            "permits": 1, "callbackBaseUrl": "http://127.0.0.1:8297", "claimKey": [0] * 32,
            "controlToken": "guard-fixture-control-token-000000",
            "sessions": {"worker": "guard-fixture-worker-token-0000000"},
        },
    })
    instant = datetime.now(timezone.utc)
    document = {"Credentials": {
        "AccessKeyId": "ASIA0000000000000000", "SecretAccessKey": "dummy-secret-local-guards",
        "SessionToken": "dummy-token-local-guards",
        "Expiration": (instant + timedelta(minutes=10)).isoformat(),
    }}
    base = session_environment(document, instant)
    prefix = evidence_root("bounds")
    env = dict(base, ARCO_PULL_TEST_MODE="hosted", ARCO_PULL_TEST_PREFIX=f"{prefix}/race")
    results = {}

    def scenario(name, *, replacement=None, remove=None, bucket=f"s3://{BUCKET}", action="validate-s3",
                 error=None):
        child = dict(env, **(replacement or {}))
        if remove:
            child.pop(remove)
        result = subprocess.run([str(binary), str(config), bucket, action],
                                env=child, text=True, capture_output=True, timeout=4, check=False)
        if error:
            assert result.returncode != 0 and error in result.stderr, result
        else:
            assert result.returncode == 0, result
        assert not any(value in result.stdout + result.stderr for value in [
            document["Credentials"]["AccessKeyId"], document["Credentials"]["SecretAccessKey"],
            document["Credentials"]["SessionToken"],
        ])
        results[name] = {"exit": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
        return json.loads(result.stdout) if not error else None

    positive = scenario("approved")
    assert positive["hosted"] and positive["physicalPrefix"] == f"{prefix}/race/"
    assert (positive["logicalReadLimit"], positive["logicalWriteLimit"]) == (256, 64)
    scenario("allowed-callback", action="validate-s3:/api/v1/tasks/ct1_example/completed")
    scenario("bucket-mismatch", bucket=f"s3://{BUCKET}-other", error="approved Ohio bucket")
    scenario("bucket-subpath", bucket=f"s3://{BUCKET}/new", error="approved Ohio bucket")
    scenario("region-mismatch", replacement={"AWS_REGION": "us-east-1"}, error="approved Ohio bucket")
    assert base["AWS_REGION"] == REGION
    for variable in [
        "AWS_ENDPOINT", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROXY_URL", "AWS_PROFILE",
        "AWS_DEFAULT_REGION", "AWS_S3_EXPRESS", "https_proxy",
    ]:
        scenario(f"override-{variable}", replacement={variable: "forbidden"}, error="rejects AWS and proxy overrides")
    scenario("ambient-credential-file", replacement={"AWS_CONFIG_FILE": "/tmp/untrusted"},
             error="requires isolated temporary session")
    scenario("no-session-token", remove="AWS_SESSION_TOKEN", error="requires isolated temporary session")
    scenario("long-lived-key", replacement={"AWS_ACCESS_KEY_ID": "AKIA0000000000000000"},
             error="requires isolated temporary session")
    scenario("no-prefix", remove="ARCO_PULL_TEST_PREFIX", error="needs a physical prefix")
    for variant in [
        "conformance/s3/unrelated/race", f"{prefix}/other", f"{prefix}/race/extra",
        "conformance/s3/flow-worker-pull-/race",
    ]:
        scenario(f"prefix-{variant}", replacement={"ARCO_PULL_TEST_PREFIX": variant},
                 error="invalid hosted worker-pull physical prefix")
    scenario("lock-break", action="validate-s3:break-fixture-lock", error="action is outside")
    scenario("raw-snapshot", action="validate-s3:snapshot:00000000000000000001", error="action is outside")
    scenario("crash-fault", replacement={"ARCO_PULL_TEST_FAULT": "stop-after"},
             error="invalid fixture pointer fault")
    marker = {"ARCO_PULL_TEST_MARKER": str(root / "unused.json"), "ARCO_PULL_TEST_FAULT": "lose-after"}
    scenario("race-cannot-inject", replacement=marker, error="invalid fixture pointer fault")
    scenario("response-loss-allowed", replacement=dict(marker, ARCO_PULL_TEST_PREFIX=f"{prefix}/reply-loss"))
    rivers = {"ARCO_PULL_TEST_PREFIX": f"{prefix}/rivers-retry"}
    scenario("rivers-acceptance", replacement=rivers, action="validate-s3:accept-rivers")
    scenario("rivers-acceptance-in-race", action="validate-s3:accept-rivers", error="action is outside")
    scenario("rivers-completion-response-loss", replacement=dict(rivers, **marker),
             action="validate-s3:/api/v1/tasks/ct1_example/completed")
    scenario("rivers-heartbeat-response-loss", replacement=dict(rivers, **marker),
             action="validate-s3:/api/v1/tasks/ct1_example/heartbeat", error="invalid fixture pointer fault")

    runner = Path(__file__).with_name("qualify_hosted_s3.py")
    validation = subprocess.run(
        [sys.executable, str(runner), str(binary), str(root / "runner"),
         "--run", "bounds", "--validate-only", "--expected-binary-sha256", sha256(binary)],
        input=json.dumps(document), text=True, capture_output=True, timeout=10, check=False,
    )
    assert validation.returncode == 0, validation
    receipt = json.loads((root / "runner/receipt.json").read_text())
    assert receipt["passed"] and receipt["inventoryCalls"] == 0
    assert set(receipt["cases"]) == {"race", "reply-loss"}
    expired = {"Credentials": dict(document["Credentials"], Expiration=instant.isoformat())}
    try:
        session_environment(expired, instant)
    except ValueError as error:
        assert "expires" in str(error)
    else:
        raise AssertionError("expired STS input was accepted")

    mock_root = root / "inventory-contract"
    mock_root.mkdir()
    lock_key = f"{prefix}/race/locks/orchestration.compaction.lock.json"
    pointer_key = f"{prefix}/race/tenant=proof/workspace=wake/state/orchestration/manifest.pointer.json"
    etag = '"fixture-etag"'
    version = "fixture-version-id"
    state = {f"{kind}Version": json.dumps({"e_tag": etag, "version": version})
             for kind in ("lock", "pointer")}
    state.update({"lockEtag": etag, "pointerEtag": etag})
    case = SimpleNamespace(root=mock_root, prefix=f"{prefix}/race", inspect=lambda _label: state)
    responses = [subprocess.CompletedProcess([], 0, json.dumps(value), "") for value in [
        {"Contents": []},
        {"Contents": [{"Key": key, "ETag": etag} for key in (lock_key, pointer_key)]},
        {"ETag": etag, "VersionId": version},
        {"ETag": etag, "VersionId": version},
    ]]
    with patch("qualify_hosted_s3.subprocess.run", side_effect=responses) as provider:
        inventory = Inventory("/fixture/aws", base, time.monotonic() + 5)
        assert inventory.scoped_objects(case, "before", limit=1) == []
        assert inventory.verify(case)["currentObjects"] == 2
        commands = [call.args[0] for call in provider.mock_calls]
        assert [command[command.index("s3api") + 1] for command in commands] == [
            "list-objects-v2", "list-objects-v2", "head-object", "head-object",
        ]
        assert all(command[command.index("--expected-bucket-owner") + 1] == OWNER for command in commands)
        assert all(command[command.index("--prefix") + 1] == case.prefix + "/" for command in commands[:2])
        assert all("--no-paginate" in command for command in commands[:2])
        save(mock_root / "commands.json", commands)

    save(root / "receipt.json", {
        "passed": True, "binarySha256": sha256(binary), "adapterProbes": 0,
        "inventoryCalls": 0, "guards": results,
        "runnerValidationSha256": sha256(root / "runner/receipt.json"),
        "inventoryContractSha256": sha256(mock_root / "commands.json"),
    })
    print(f"{len(results)} binary guard cases passed, plus runner dry-run, expiration, and scoped inventory checks.")


if __name__ == "__main__":
    main()
