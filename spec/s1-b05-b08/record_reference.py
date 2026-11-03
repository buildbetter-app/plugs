#!/usr/bin/env python3
"""Record B05-B08 from a pinned public reference executable.

The script is intentionally fail-closed: it refuses to emit a recording until
the caller supplies the exact clean source pin, a verified executable, and
generated Go+SQL clock evidence. It never copies Plug output into a reference
vector.
"""

import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from reference_inputs import verify_clock_proof, verify_source


COMMIT = "9d6fff760b7a314b476539198504d77ce0959656"
TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
CARDS = ("B05", "B06", "B07", "B08")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def require_pin(binary_digest):
    if os.environ.get("PLUG_REFERENCE_SOURCE_COMMIT") != COMMIT:
        raise RuntimeError("PLUG_REFERENCE_SOURCE_COMMIT must equal the pinned reference commit")
    if os.environ.get("PLUG_REFERENCE_SOURCE_TREE") != TREE:
        raise RuntimeError("PLUG_REFERENCE_SOURCE_TREE must equal the verified source tree")
    if os.environ.get("PLUG_REFERENCE_CLOCK_MODE") != "go-sql-controlled":
        raise RuntimeError("PLUG_REFERENCE_CLOCK_MODE=go-sql-controlled is required")
    source_path = os.environ.get("PLUG_REFERENCE_SOURCE_DIR")
    if not source_path:
        raise RuntimeError("PLUG_REFERENCE_SOURCE_DIR must identify the clean pinned source checkout")
    verify_source(Path(source_path))
    proof_path = os.environ.get("PLUG_REFERENCE_CLOCK_PROOF")
    if not proof_path:
        raise RuntimeError("PLUG_REFERENCE_CLOCK_PROOF must identify generated Go+SQL clock evidence")
    return verify_clock_proof(proof_path, binary_digest)


def run(binary, root, commands, args, expected=None, env=None):
    merged = {"PATH": os.environ["PATH"], "TZ": "UTC", "LANG": "C.UTF-8"}
    merged.update(env or {})
    result = subprocess.run(
        [str(binary), *args], cwd=root, env=merged,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
    )
    if expected is not None and result.returncode != expected:
        raise RuntimeError(f"unexpected exit for {args!r}: {result.returncode}")
    if expected is None and result.returncode != 0:
        raise RuntimeError(f"reference command failed {args!r}: {result.returncode}")
    outcome = {
        "argv": list(args),
        "exitCode": result.returncode,
        "stdoutSha256": sha(result.stdout),
        "stderrSha256": sha(result.stderr),
        "stdoutBase64": base64.b64encode(result.stdout).decode("ascii"),
        "stderrBase64": base64.b64encode(result.stderr).decode("ascii"),
    }
    try:
        payload = json.loads(result.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        outcome["errorCode"] = payload["error"].get("code")
    if payload is not None:
        outcome["stdout"] = payload
    commands.append(outcome)
    return result.stdout


def b07_import_fixture(root, ids, actor):
    """Write a deterministic public-import fixture for an opaque B07 edge.

    The pinned CLI deliberately restricts ``dep add --type`` to its
    well-known vocabulary, while the pinned import surface accepts the open
    dependency column and preserves opaque values.  Import is therefore the
    executable public seam for the B07 custom-kind observation; no SQL or
    authored Plug bytes are used.
    """
    stamp = "2026-01-01T00:00:00Z"
    source, target = ids
    rows = [
        {
            "id": source,
            "title": "Source",
            "issue_type": "task",
            "status": "open",
            "priority": 2,
            "created_at": stamp,
            "updated_at": stamp,
            "dependencies": [
                {
                    "depends_on_id": target,
                    "type": "custom-edge",
                    "created_by": actor,
                    "created_at": stamp,
                }
            ],
        },
        {
            "id": target,
            "title": "Target",
            "issue_type": "task",
            "status": "open",
            "priority": 2,
            "created_at": stamp,
            "updated_at": stamp,
        },
    ]
    fixture = root / "b07-reference.jsonl"
    fixture.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    return fixture.name, sha(fixture.read_bytes())


def dependency_projection(stdout, source, target):
    try:
        payload = json.loads(stdout.decode("utf-8"))
        row = payload[0]
        return [
            {"from": source, "to": edge["id"], "kind": edge["dependency_type"]}
            for edge in row.get("dependencies", [])
            if edge.get("id") == target
        ]
    except (KeyError, IndexError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("B07 show output did not contain a decodable dependency projection") from exc


def record(card, binary, root, binary_digest, clock_proof_digest):
    prefix = card.lower()
    ids = [f"{prefix}-source", f"{prefix}-target"]
    commands = []
    actor = f"{prefix}-reference"
    env = {
        "BEADS_ACTOR": actor,
        "BEADS_DOLT_AUTO_START": "1",
        "CI": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": actor,
        "GIT_AUTHOR_EMAIL": f"{prefix}@reference.invalid",
        "GIT_COMMITTER_NAME": actor,
        "GIT_COMMITTER_EMAIL": f"{prefix}@reference.invalid",
        "PLUG_REFERENCE_SOURCE_COMMIT": COMMIT,
        "PLUG_REFERENCE_SOURCE_TREE": TREE,
        "PLUG_REFERENCE_CLOCK_MODE": "go-sql-controlled",
        "PLUG_REFERENCE_SOURCE_DIR": os.environ["PLUG_REFERENCE_SOURCE_DIR"],
        "PLUG_REFERENCE_CLOCK_PROOF": os.environ["PLUG_REFERENCE_CLOCK_PROOF"],
    }
    run(binary, root, commands, ["init", "--non-interactive", "--quiet", "--skip-hooks", "--skip-agents", "--prefix", prefix], env=env)
    version = run(binary, root, commands, ["version"], env=env)
    fixture_name = None
    fixture_digest = None
    if card == "B07":
        fixture_name, fixture_digest = b07_import_fixture(root, ids, actor)
        probe_start = len(commands)
        run(binary, root, commands, ["import", fixture_name, "--json"], env=env)
        run(binary, root, commands, ["import", fixture_name, "--json"], env=env)
    else:
        for issue, title in zip(ids, ("Source", "Target")):
            run(binary, root, commands, ["create", "--id", issue, "--type", "task", "--title", title], env=env)
        probe_start = len(commands)
    if card == "B05":
        # The pinned executable exposes claim as the atomic update flag.  The
        # older standalone `bd claim` verb is not present at this source pin;
        # invoking it would make an otherwise valid disposable recording fail
        # before any B05 observation is retained.
        run(binary, root, commands, ["update", ids[0], "--claim", "--json"], env=env)
        run(binary, root, commands, ["heartbeat", ids[0], "--json"], env=env)
        run(binary, root, commands, ["show", ids[0], "--json"], env=env)
        observations = {"probeResults": commands[probe_start:]}
    elif card == "B06":
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "blocks"], env=env)
        run(binary, root, commands, ["dep", "add", ids[1], ids[0], "--type", "blocks"], expected=1, env=env)
        run(binary, root, commands, ["dep", "cycles", "--json"], env=env)
        observations = {"probeResults": commands[probe_start:]}
    elif card == "B07":
        before = run(binary, root, commands, ["show", ids[0], "--json"], env=env)
        before_edges = dependency_projection(before, ids[0], ids[1])
        if before_edges != [{"from": ids[0], "to": ids[1], "kind": "custom-edge"}]:
            raise RuntimeError(f"B07 import did not retain the opaque edge: {before_edges!r}")
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "blocks"], expected=1, env=env)
        after = run(binary, root, commands, ["show", ids[0], "--json"], env=env)
        after_edges = dependency_projection(after, ids[0], ids[1])
        if after_edges != before_edges:
            raise RuntimeError(f"B07 retype refusal changed the opaque edge: {before_edges!r} -> {after_edges!r}")
        observations = {
            "probeResults": commands[probe_start:],
            "opaqueEdge": {"before": before_edges, "after": after_edges},
            "fixtureSha256": fixture_digest,
        }
    else:
        run(binary, root, commands, ["dep", "add", ids[1], ids[0], "--type", "parent-child"], env=env)
        run(binary, root, commands, ["close", ids[0], "--json"], expected=1, env=env)
        run(binary, root, commands, ["close", ids[1], "--json"], env=env)
        run(binary, root, commands, ["close", ids[0], "--json"], env=env)
        run(binary, root, commands, ["reopen", ids[0], "--json"], env=env)
        observations = {"probeResults": commands[probe_start:]}
    if not observations["probeResults"] or not any("stdout" in item or "stdoutBase64" in item for item in observations["probeResults"]):
        raise RuntimeError(f"{card} did not retain probe outputs")
    return {
        "card": card,
        "sourceCommit": COMMIT,
        "sourceTree": TREE,
        "engine": "embedded-dolt",
        "configuration": {
            "prefix": prefix,
            "actor": actor,
            "clock": "go-sql-controlled",
            "clockProofSha256": clock_proof_digest,
        },
        "binary": {"sha256": binary_digest, "versionDigest": sha(version)},
        "ids": ids,
        "commands": commands,
        "observations": observations,
    }


def project(vectors):
    projection = []
    for vector in vectors:
        projection.append({
            "card": vector["card"],
            "ids": vector["ids"],
            "prefix": vector["configuration"]["prefix"],
            "commands": [
                {k: command[k] for k in ("argv", "exitCode", "errorCode") if k in command}
                for command in vector["commands"]
            ],
            "observations": vector["observations"],
        })
    return projection


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: record_reference.py REFERENCE_EXECUTABLE OUTPUT")
    binary = Path(sys.argv[1]).resolve()
    output = Path(sys.argv[2]).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("reference executable is missing or not executable")
    binary_digest = sha(binary.read_bytes())
    _, clock_proof_digest = require_pin(binary_digest)
    runs = []
    for index in ("a", "b"):
        vectors = []
        for card in CARDS:
            root = Path(tempfile.mkdtemp(prefix=f"plug-b05-b08-{index}-{card.lower()}-", dir="/tmp/plug-swarm/work"))
            try:
                vectors.append(record(card, binary, root, binary_digest, clock_proof_digest))
            finally:
                shutil.rmtree(root, ignore_errors=True)
        raw = canonical(vectors).encode()
        projected = canonical(project(vectors)).encode()
        runs.append({"run": index, "raw": vectors, "rawSha256": sha(raw), "projectionSha256": sha(projected)})
    if runs[0]["raw"] != runs[1]["raw"]:
        raise RuntimeError("independent reference twins differ")
    result = {
        "schema": "plug.s1-b05-b08.reference.v1",
        "sourceCommit": COMMIT,
        "sourceTree": TREE,
        "binarySha256": binary_digest,
        "clockProofSha256": clock_proof_digest,
        "recordings": runs,
        "status": "recorded",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
