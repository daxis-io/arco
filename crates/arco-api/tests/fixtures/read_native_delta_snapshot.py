"""Read an Arco owner bundle using delta-rs with local storage only."""

import base64
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile

from deltalake import DeltaTable


MAX_BUNDLE_BYTES = 12 * 1024 * 1024
MAX_OBJECT_BYTES = 8 * 1024 * 1024
DATA_PATH = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,119}\.parquet")


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def read_json(path):
    with path.open("rb") as stream:
        data = stream.read(MAX_BUNDLE_BYTES + 1)
    assert len(data) <= MAX_BUNDLE_BYTES
    return json.loads(data)


def read_snapshot(descriptor, bundle):
    identity = bundle["input_identity"]
    assert identity == descriptor["input_identity"]
    assert identity["format"] == "delta" and identity["bundle_version"] == 1
    version = identity["delta_version"]
    assert type(version) is int and 0 <= version < 16
    assert digest({key: value for key, value in identity.items() if key != "snapshot_id"}) == (
        identity["snapshot_id"]
    )
    logs = {f"_delta_log/{number:020}.json" for number in range(version + 1)}
    assert len(logs) <= len(bundle["objects"]) <= len(logs) + 32

    files = {}
    manifest = []
    for obj in bundle["objects"]:
        path = obj["path"]
        assert path in logs or DATA_PATH.fullmatch(path)
        assert path not in files
        contents = base64.b64decode(obj["data_base64"], validate=True)
        assert 0 < len(contents) == obj["byte_size"]
        assert hashlib.sha256(contents).hexdigest() == obj["sha256"]
        files[path] = contents
        manifest.append({key: obj[key] for key in ("path", "byte_size", "sha256")})
    assert logs <= files.keys()
    assert 0 < sum(map(len, files.values())) == identity["byte_size"] <= MAX_OBJECT_BYTES
    assert digest(manifest) == identity["manifest_sha256"]

    with tempfile.TemporaryDirectory(prefix="arco-delta-") as directory:
        root = Path(directory).resolve()
        (root / "_delta_log").mkdir()
        for path, contents in files.items():
            with (root / path).open("xb") as stream:
                stream.write(contents)
        table = DeltaTable(str(root), version=version)
        assert table.version() == version
        assert table.metadata().id == identity["delta_table_id"]
        active_files = sorted(str(Path(path).relative_to(root)) for path in table.file_uris())
        assert set(active_files) == files.keys() - logs
        if [field["name"] for field in descriptor["schema"]] == ["id", "amount"]:
            rows = table.to_pyarrow_table()
            amount_column = "amount"
        else:
            rows = table.to_pyarrow_table(filters=[
                ("status", "=", "paid"),
                ("day", ">=", date(2026, 1, 1)),
                ("day", "<", date(2026, 2, 1)),
            ])
            assert set(rows["day"].to_pylist()) == {date(2026, 1, 1)}
            amount_column = "amount_cents"
        return {
            "table_id": identity["table_id"],
            "snapshot_id": identity["snapshot_id"],
            "delta_table_id": table.metadata().id,
            "delta_version": table.version(),
            "active_files": active_files,
            "total_cents": sum(rows[amount_column].to_pylist()),
            "order_count": rows.num_rows,
        }


if __name__ == "__main__":
    descriptor_path, bundle_path = map(Path, sys.argv[1:])
    print(json.dumps(read_snapshot(read_json(descriptor_path), read_json(bundle_path)), sort_keys=True))
