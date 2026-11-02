#!/usr/bin/env python3
"""Record two independent head-state interchange runs; never synthesize output."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

PIN = "9d6fff760b7a314b476539198504d77ce0959656"
BINARY_SHA256 = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
WORKLOAD = {
    "id": "b19-snapshot",
    "initial_title": "Archive before update",
    "title": "Archive current state",
    "description": "Retain this current description",
    "priority": 2,
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def projection(row):
    return {key: row[key] for key in ("title", "description", "status", "priority")}


def record(binary, evidence, destination):
    binary = binary.resolve()
    assert digest(binary.read_bytes()) == BINARY_SHA256, "reference executable changed"
    evidence.mkdir(parents=True, exist_ok=False)
    runs = []
    for number in (1, 2):
        workspace = Path(tempfile.mkdtemp(prefix=f"cardb19-reference-{number}-", dir="/tmp/plug-swarm/work"))
        home = workspace / "isolated-home"
        home.mkdir()
        env = {key: os.environ[key] for key in ("PATH", "TMPDIR") if key in os.environ}
        env.update(HOME=str(home), XDG_CONFIG_HOME=str(home / "config"), CI="true", DO_NOT_TRACK="1", BEADS_NO_DAEMON="1", BEADS_ACTOR="cardb19-recorder", GIT_AUTHOR_NAME="cardb19-recorder", GIT_AUTHOR_EMAIL="cardb19@example.invalid", GIT_COMMITTER_NAME="cardb19-recorder", GIT_COMMITTER_EMAIL="cardb19@example.invalid", LC_ALL="C", TZ="UTC")
        raw = evidence / f"recording-{number}"
        raw.mkdir()
        commands = []

        def invoke(cwd, label, args, expected=0):
            argv = [str(binary), *args]
            result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, timeout=120)
            (raw / f"{label}.stdout").write_bytes(result.stdout)
            (raw / f"{label}.stderr").write_bytes(result.stderr)
            commands.append(dict(cwd=str(cwd), argv=argv, exit_code=result.returncode, stdout_sha256=digest(result.stdout), stderr_sha256=digest(result.stderr)))
            (raw / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
            assert result.returncode == expected, (label, result.returncode, result.stdout.decode(errors="replace"), result.stderr.decode(errors="replace"))
            return result.stdout

        source = workspace / "source"
        target = workspace / "target"
        source.mkdir()
        target.mkdir()
        init = ["init", "--prefix", "b19", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"]
        invoke(source, "source-init", init)
        invoke(source, "create", ["create", WORKLOAD["initial_title"], "--id", WORKLOAD["id"], "--description", WORKLOAD["description"], "--priority", str(WORKLOAD["priority"]), "--json"])
        invoke(source, "update", ["update", WORKLOAD["id"], "--title", WORKLOAD["title"], "--json"])
        exported = invoke(source, "export", ["export"])
        rows = [json.loads(line) for line in exported.splitlines() if line.strip()]
        assert len(rows) == 1 and rows[0]["id"] == WORKLOAD["id"]
        export_path = raw / "export.jsonl"
        export_path.write_bytes(exported)
        invoke(target, "target-init", init)
        invoke(target, "import", ["import", str(export_path), "--json"])
        imported = json.loads(invoke(target, "show-imported", ["show", WORKLOAD["id"], "--json"]))
        assert len(imported) == 1
        state = projection(rows[0])
        assert projection(imported[0]) == state, "JSONL did not preserve the selected state"
        invoke(target, "show-unknown", ["show", "b19-absent", "--json"], expected=1)
        retained = destination / "testdata" / f"recording-{number}"
        retained.mkdir(parents=True, exist_ok=False)
        (retained / "export.jsonl").write_bytes(exported)
        shown = (raw / "show-imported.stdout").read_bytes()
        (retained / "imported.json").write_bytes(shown)
        runs.append(dict(workspace=str(workspace), evidence=str(raw), export_sha256=digest(exported), imported_sha256=digest(shown), projection=state, commands_sha256=digest((raw / "commands.json").read_bytes())))
    assert runs[0]["workspace"] != runs[1]["workspace"]
    assert runs[0]["projection"] == runs[1]["projection"], "independent projections differ"
    result = dict(source_commit=PIN, binary_sha256=BINARY_SHA256, workload=WORKLOAD, projection_fields=["title", "description", "status", "priority"], recordings=runs, limits="Two fresh source/target pairs. Current issue JSONL only; no native archive, history export, removed-version producer, migration or cutover certificate.")
    (destination / "reference.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"recordings": len(runs), "evidence": str(evidence)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    record(args.binary, args.evidence.resolve(), Path(__file__).resolve().parent)
