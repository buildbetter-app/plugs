#!/usr/bin/env python3
"""Record non-temporal current-state interchange from a verified pinned asset.

No compiler or build is invoked. Raw subprocess output stays in the supplied
evidence directory; only actual exports and their provenance enter testdata.
"""
import argparse
import hashlib
import io
import json
import os
import pathlib
import subprocess
import tarfile
import tempfile

PIN = "9d6fff760b7a314b476539198504d77ce0959656"
BINARY_SHA = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
BUILD_INFO_SHA = "2edc35ea3a2f0d50f708bda2271742494191f3288466ba246f6a8d0502f64d50"
ARCHIVE_SHA = "3942e1a021287e185f14b04adcc115b2a1f7c122fe7a16478ea0c5fe58fd7cba"
FIELDS = ["title", "description", "status", "issue_type", "priority", "metadata", "labels", "spec_id", "close_reason"]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def project(data):
    result = {}
    for line in data.splitlines():
        row = json.loads(line)
        assert row["id"] not in result
        result[row["id"]] = {field: row.get(field) for field in FIELDS}
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=pathlib.Path, required=True)
    p.add_argument("--source", type=pathlib.Path, required=True)
    p.add_argument("--evidence", type=pathlib.Path, required=True)
    args = p.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    assert sha(args.binary.read_bytes()) == BINARY_SHA
    archive = subprocess.check_output(["git", "archive", "--format=tar", PIN])
    assert sha(archive) == ARCHIVE_SHA
    schema = {"main": [], "ignored": []}
    verified = 0
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        for entry in source:
            if not entry.isfile() and not entry.issym():
                continue
            path = args.source / entry.name
            expected = entry.linkname.encode() if entry.issym() else source.extractfile(entry).read()
            actual = os.readlink(path).encode() if entry.issym() else path.read_bytes()
            assert actual == expected, entry.name
            verified += 1
            if entry.name.startswith("internal/storage/schema/") and entry.name.endswith(".up.sql"):
                family = "ignored" if "/ignored/" in entry.name else "main"
                schema[family].append({"path": entry.name, "sha256": sha(actual)})
    info = subprocess.check_output(["go", "version", "-m", str(args.binary)])
    assert sha(info) == BUILD_INFO_SHA
    (args.evidence / "build-info.txt").write_bytes(info)
    migrations = {}
    for family, entries in schema.items():
        entries.sort(key=lambda x: x["path"])
        migrations[family] = {"count": len(entries), "sha256": sha(canonical(entries))}
    assert migrations["main"]["count"] == 72
    assert migrations["ignored"]["count"] == 28

    stamp = "2026-01-02T03:04:05Z"
    common = {"created_at": stamp, "updated_at": stamp, "created_by": "reference-actor", "issue_type": "task", "priority": 0, "status": "open"}
    inputs = [
        {**common, "id": "ref-1", "title": "First retained row", "description": "Verbatim café", "metadata": {"nested": {"count": 7, "text": "café"}}, "labels": ["alpha", "omega"], "spec_id": "spec/one"},
        {**common, "id": "ref-2", "title": "Completed retained row", "description": "Second description", "status": "closed", "closed_at": stamp, "close_reason": "Exact close reason", "spec_id": "spec/two", "labels": ["done"], "metadata": {"flag": True}},
    ]
    keep = [{**common, "id": "ref-keep", "title": "Existing target row", "description": "Must survive import"}]
    input_bytes = b"".join(canonical(row) + b"\n" for row in inputs)
    keep_bytes = b"".join(canonical(row) + b"\n" for row in keep)
    output = pathlib.Path(__file__).resolve().parent / "testdata"
    assert not output.exists(), "retain earlier recordings; choose a new authorized run instead of overwriting"
    runs, projections = [], []
    for name in ["run1", "run2"]:
        work = pathlib.Path(tempfile.mkdtemp(prefix="cardb27-" + name + "-", dir="/tmp/plug-swarm/work"))
        raw_dir = args.evidence / name
        raw_dir.mkdir()
        env = {"PATH": os.environ["PATH"], "LANG": "C.UTF-8", "USER": "reference-actor", "CI": "true", "DO_NOT_TRACK": "1", "BEADS_NO_DAEMON": "1", "BEADS_ACTOR": "reference-actor", "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}
        for directory in ["home", "config", "source", "target"]:
            (work / directory).mkdir()
        env.update(HOME=str(work / "home"), XDG_CONFIG_HOME=str(work / "config"))
        commands = []

        def run(label, store, argv, stdin=None):
            result = subprocess.run([str(args.binary)] + argv, input=stdin, cwd=work / store, env=env, capture_output=True, timeout=60)
            (raw_dir / (label + ".stdout")).write_bytes(result.stdout)
            (raw_dir / (label + ".stderr")).write_bytes(result.stderr)
            (raw_dir / (label + ".exit")).write_text(str(result.returncode) + "\n")
            commands.append({"label": label, "store": store, "argv": argv, "stdinSHA256": sha(stdin) if stdin is not None else None, "exit": result.returncode})
            if result.returncode:
                raise RuntimeError(label + ": " + result.stderr.decode(errors="replace"))
            return result.stdout

        init = ["init", "--prefix", "ref", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive"]
        run("source-init", "source", init)
        run("source-import", "source", ["import", "-", "--json"], input_bytes)
        source_export = run("source-export", "source", ["export", "--all"])
        run("target-init", "target", init)
        run("target-keep", "target", ["import", "-", "--json"], keep_bytes)
        run("target-import", "target", ["import", "-", "--json"], source_export)
        target_export = run("target-export", "target", ["export", "--all"])
        projection = canonical({"source": project(source_export), "afterImportIntoNonempty": project(target_export)})
        assert len(project(source_export)) == 2
        assert len(project(target_export)) == 3
        assert "ref-keep" in project(target_export)
        for key, value in project(source_export).items():
            assert project(target_export)[key] == value
        destination = output / name
        destination.mkdir(parents=True)
        (destination / "source-export.jsonl").write_bytes(source_export)
        (destination / "target-export.jsonl").write_bytes(target_export)
        (raw_dir / "commands.json").write_bytes(canonical(commands) + b"\n")
        (raw_dir / "workspace.txt").write_text(str(work) + "\n")
        runs.append({"SourceExportSHA256": sha(source_export), "TargetExportSHA256": sha(target_export), "CommandsSHA256": sha(canonical(commands) + b"\n")})
        projections.append(projection)
    assert projections[0] == projections[1], "independently initialized stores disagree"
    provenance = {
        "SourceCommit": PIN,
        "SourceTree": subprocess.check_output(["git", "rev-parse", PIN + "^{tree}"], text=True).strip(),
        "SourceArchiveSHA256": ARCHIVE_SHA,
        "VerifiedSourceFiles": verified,
        "BinarySHA256": BINARY_SHA,
        "BuildInfoSHA256": BUILD_INFO_SHA,
        "RecorderSHA256": sha(pathlib.Path(__file__).read_bytes()),
        "InputSHA256": sha(input_bytes),
        "ExistingTargetInputSHA256": sha(keep_bytes),
        "Driver": "embedded-dolt",
        "SchemaEvidence": "pinned source migration inventory; not a runtime SQL census",
        "Migrations": migrations,
        "ProjectionFields": FIELDS,
        "Omitted": ["timestamps", "source-local revision tokens", "source-local epoch", "content hash", "non-issue tables", "full history", "configuration union", "relations and comments"],
        "ProjectionSHA256": sha(projections[0]),
        "Runs": runs,
    }
    (output / "inputs.jsonl").write_bytes(input_bytes)
    (output / "existing-target.jsonl").write_bytes(keep_bytes)
    (output / "projection.json").write_bytes(projections[0])
    (output / "provenance.json").write_bytes(canonical(provenance) + b"\n")
    (args.evidence / "provenance.json").write_bytes(canonical(provenance) + b"\n")
    print(json.dumps({"projectionSHA256": sha(projections[0]), "runs": runs}, indent=2))


if __name__ == "__main__":
    main()
