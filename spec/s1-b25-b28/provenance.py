#!/usr/bin/env python3
"""Independently recompute the B27 fixture projection and provenance digests.

This verifier reads only retained raw exports and command receipts. It never
creates a store and cannot turn an authored projection into a recording.
"""
import argparse
import hashlib
import json
from pathlib import Path

FIELDS = [
    "title", "description", "status", "issue_type", "priority", "metadata",
    "labels", "spec_id", "close_reason",
]


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def rows(data: bytes) -> dict[str, dict[str, object]]:
    result = {}
    for line in data.splitlines():
        row = json.loads(line)
        identity = row["id"]
        if identity in result:
            raise ValueError(f"duplicate exported identity: {identity}")
        result[identity] = {field: row.get(field) for field in FIELDS}
    return result


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def verify(root: Path) -> dict[str, object]:
    data = root / "testdata"
    projection = {
        "source": rows((data / "run1/source-export.jsonl").read_bytes()),
        "afterImportIntoNonempty": rows((data / "run1/target-export.jsonl").read_bytes()),
    }
    encoded = canonical(projection)
    if encoded != (data / "projection.json").read_bytes():
        raise ValueError("independent projection differs from retained fixture")
    source = projection["source"]
    target = projection["afterImportIntoNonempty"]
    if len(source) != 2 or len(target) != 3 or "ref-keep" not in target:
        raise ValueError("recorded import cardinality changed")
    for identity, value in source.items():
        if target.get(identity) != value:
            raise ValueError(f"import changed source row {identity}")
    for run in ("run1", "run2"):
        commands = json.loads((data / run / "commands.json").read_bytes())
        labels = [item["label"] for item in commands]
        if labels != ["source-init", "source-import", "source-export", "target-init", "target-keep", "target-import", "target-export"]:
            raise ValueError(f"unexpected command sequence in {run}")
        if any(item["exit"] != 0 for item in commands):
            raise ValueError(f"recorded command failure in {run}")
        if rows((data / run / "source-export.jsonl").read_bytes()) != source:
            raise ValueError(f"source projection differs in {run}")
        if rows((data / run / "target-export.jsonl").read_bytes()) != target:
            raise ValueError(f"target projection differs in {run}")
    manifest = json.loads((data / "provenance.json").read_bytes())
    if manifest["SourceCommit"] != "9d6fff760b7a314b476539198504d77ce0959656":
        raise ValueError("reference source pin changed")
    if manifest["ProjectionSHA256"] != digest(encoded):
        raise ValueError("projection digest differs")
    for name, key in (("record_reference.py", "RecorderSHA256"), ("inputs.jsonl", "InputSHA256"), ("existing-target.jsonl", "ExistingTargetInputSHA256")):
        if manifest[key] != digest((root / name).read_bytes() if name == "record_reference.py" else (data / name).read_bytes()):
            raise ValueError(f"fixture digest differs: {name}")
    return {"projectionSHA256": digest(encoded), "provenanceSHA256": digest((data / "provenance.json").read_bytes())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, nargs="?", default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    print(json.dumps(verify(args.root), sort_keys=True))
