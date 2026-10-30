#!/usr/bin/env python3
"""Bind one fresh reference recording to its versioned inputs and projection."""

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    if isinstance(value, str):
        value = value.encode()
    return hashlib.sha256(value).hexdigest()


def schema_digest(root, plane):
    migrations = root / "internal/storage/schema/migrations"
    if plane == "ignored":
        migrations /= "ignored"
    files = [
        {"path": str(p.relative_to(root)), "sha256": digest(p.read_bytes())}
        for p in sorted(migrations.glob("*.up.sql"))
    ]
    if not files:
        raise ValueError("schema migration inventory is empty")
    cursor = max(int(Path(f["path"]).name.split("_", 1)[0]) for f in files)
    return cursor, digest(canonical(files))


def record(manifest, card, artifact_path, reference, binary, source):
    provenance = manifest["provenance"]
    recorder = manifest["recorders"][card]
    repository = Path(__file__).resolve().parents[2]
    commit = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "refs/tags/beads-base"], text=True
    ).strip()
    if commit != provenance["sourceCommit"]:
        raise ValueError("reference source commit differs")
    tree = subprocess.check_output(["git", "-C", str(repository), "ls-tree", "-rz", commit])
    for entry in tree.split(b"\0"):
        if not entry:
            continue
        metadata, path = entry.split(b"\t", 1)
        mode, kind, blob = metadata.split()
        if kind != b"blob":
            raise ValueError("unsupported reference entry: " + path.decode())
        file = reference / path.decode()
        data = str(file.readlink()).encode() if mode == b"120000" else file.read_bytes()
        actual = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if actual != blob.decode():
            raise ValueError("reference source differs: " + path.decode())
    if digest(binary.read_bytes()) != provenance["binary"]["sha256"]:
        raise ValueError("reference executable differs")
    if digest(Path(__file__).read_bytes()) != provenance["recorder"]["sha256"]:
        raise ValueError("provenance recorder source differs")
    source_file, = recorder["sourceFiles"]
    source_bytes = source.read_bytes()
    if source_bytes != source_file["content"].encode() or digest(source_bytes) != source_file["sha256"]:
        raise ValueError("reference recorder differs from versioned source")
    module = re.search(r"github.com/dolthub/dolt/go\s+(\S+)", (reference / "go.mod").read_text())
    if not module or "github.com/dolthub/dolt/go@" + module[1] != provenance["driver"]["version"]:
        raise ValueError("reference driver version differs")
    artifact_bytes = artifact_path.read_bytes()
    artifact = json.loads(artifact_bytes)
    vector = json.loads(artifact["rawJSON"])
    if canonical(vector) != artifact["rawJSON"] or vector != artifact["vector"]:
        raise ValueError("raw recording is not canonical or differs from vector")
    raw_digest = digest(artifact["rawJSON"])
    if raw_digest != artifact["digest"]:
        raise ValueError("raw recording digest differs")
    for plane in ("main", "ignored"):
        cursor, inventory_digest = schema_digest(reference, plane)
        if cursor != provenance["schema"][plane + "Cursor"]:
            raise ValueError("pinned schema cursor differs: " + plane)
        if card == "B17" and artifact["schemaCursors"][plane + "Cursor"] != cursor:
            raise ValueError("observed schema cursor differs: " + plane)
        if inventory_digest != provenance["schema"][plane + "Digest"]:
            raise ValueError("schema inventory differs: " + plane)
    for evidence in (provenance["configuration"], recorder["configuration"], recorder["inputs"]):
        if digest(canonical(evidence["value"])) != evidence["digest"]:
            raise ValueError("configuration or input digest differs")
    projection = canonical({path: vector[path] for path in recorder["projection"]["paths"]})
    bound = {
        "card": card,
        "sourceCommit": commit,
        "binarySha256": digest(binary.read_bytes()),
        "recorder": provenance["recorder"],
        "sources": [{"path": source_file["path"], "sha256": digest(source_bytes)}],
        "driver": provenance["driver"],
        "schema": provenance["schema"],
        "disposition": recorder["disposition"],
        "configurationDigest": recorder["configuration"]["digest"],
        "globalConfigurationDigest": provenance["configuration"]["digest"],
        "inputDigest": recorder["inputs"]["digest"],
        "rawDigest": raw_digest,
        "projectionDigest": digest(projection),
    }
    return {
        "projection": projection,
        "projectionDigest": digest(projection),
        "provenance": canonical(bound),
        "provenanceDigest": digest(canonical(bound)),
        "artifactSha256": digest(artifact_bytes),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("card", choices=("B17", "B24"))
    parser.add_argument("artifact", type=Path)
    parser.add_argument("reference", type=Path)
    parser.add_argument("binary", type=Path)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    manifest = json.loads(Path(__file__).with_name("goldens.json").read_text())
    output = record(manifest, args.card, args.artifact, args.reference, args.binary, args.source)
    args.output.write_text(canonical(output) + "\n")
