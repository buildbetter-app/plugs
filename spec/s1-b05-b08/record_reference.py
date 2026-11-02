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
    source = Path(source_path)
    try:
        actual_commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
        actual_tree = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"], text=True).strip()
        dirty = subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"], text=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(f"cannot verify pinned reference source: {exc}") from exc
    if actual_commit != COMMIT or actual_tree != TREE or dirty:
        raise RuntimeError("reference source is not the exact clean pinned commit/tree")
    proof_path = os.environ.get("PLUG_REFERENCE_CLOCK_PROOF")
    if not proof_path:
        raise RuntimeError("PLUG_REFERENCE_CLOCK_PROOF must identify generated Go+SQL clock evidence")
    try:
        proof_bytes = Path(proof_path).read_bytes()
        proof = json.loads(proof_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid PLUG_REFERENCE_CLOCK_PROOF: {exc}") from exc
    if proof.get("sourceCommit") != COMMIT or proof.get("sourceTree") != TREE:
        raise RuntimeError("clock proof is not bound to the pinned source")
    if proof.get("binarySha256") != binary_digest:
        raise RuntimeError("clock proof is not bound to the reference executable")
    if proof.get("mode") != "go-sql-controlled":
        raise RuntimeError("clock proof does not declare Go+SQL control")
    for name in ("go", "sql"):
        clock = proof.get(name)
        reads = clock.get("controlledReads", 0) if isinstance(clock, dict) else 0
        if not isinstance(reads, int) or reads <= 0:
            raise RuntimeError(f"clock proof has no controlled {name} clock reads")
    schedule_digest = proof.get("scheduleSha256", "")
    if len(schedule_digest) != 64 or any(c not in "0123456789abcdef" for c in schedule_digest):
        raise RuntimeError("clock proof has no schedule digest")
    return proof, sha(proof_bytes)


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
    }
    try:
        payload = json.loads(result.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        outcome["errorCode"] = payload["error"].get("code")
    if payload is not None:
        outcome["stdout"] = payload
    else:
        outcome["stdoutBase64"] = base64.b64encode(result.stdout).decode("ascii")
    if result.stderr:
        outcome["stderrBase64"] = base64.b64encode(result.stderr).decode("ascii")
    commands.append(outcome)
    return result.stdout


def record(card, binary, root, binary_digest, clock_proof_digest):
    prefix = card.lower()
    ids = [f"{prefix}-source", f"{prefix}-target"]
    commands = []
    env = {
        "BEADS_ACTOR": f"{prefix}-reference",
        "PLUG_REFERENCE_SOURCE_COMMIT": COMMIT,
        "PLUG_REFERENCE_SOURCE_TREE": TREE,
        "PLUG_REFERENCE_CLOCK_MODE": "go-sql-controlled",
        "PLUG_REFERENCE_SOURCE_DIR": os.environ["PLUG_REFERENCE_SOURCE_DIR"],
        "PLUG_REFERENCE_CLOCK_PROOF": os.environ["PLUG_REFERENCE_CLOCK_PROOF"],
    }
    run(binary, root, commands, ["init", "--non-interactive", "--quiet", "--skip-hooks", "--skip-agents", "--prefix", prefix], env=env)
    version = run(binary, root, commands, ["version"], env=env)
    for issue, title in zip(ids, ("Source", "Target")):
        run(binary, root, commands, ["create", "--id", issue, "--type", "task", "--title", title], env=env)
    if card == "B05":
        run(binary, root, commands, ["claim", ids[0], "--json"], env=env)
        run(binary, root, commands, ["heartbeat", ids[0], "--json"], env=env)
        run(binary, root, commands, ["show", ids[0], "--json"], env=env)
        observations = {"probeResults": commands[4:]}
    elif card == "B06":
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "blocks"], env=env)
        run(binary, root, commands, ["dep", "add", ids[1], ids[0], "--type", "blocks"], expected=1, env=env)
        run(binary, root, commands, ["dep", "cycles", "--json"], env=env)
        observations = {"probeResults": commands[4:]}
    elif card == "B07":
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "custom-edge"], env=env)
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "custom-edge"], env=env)
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "blocks"], expected=1, env=env)
        run(binary, root, commands, ["show", ids[0], "--json"], env=env)
        observations = {"probeResults": commands[4:]}
    else:
        run(binary, root, commands, ["dep", "add", ids[1], ids[0], "--type", "parent-child"], env=env)
        run(binary, root, commands, ["close", ids[0], "--json"], expected=1, env=env)
        run(binary, root, commands, ["close", ids[1], "--json"], env=env)
        run(binary, root, commands, ["close", ids[0], "--json"], env=env)
        run(binary, root, commands, ["reopen", ids[0], "--json"], env=env)
        observations = {"probeResults": commands[4:]}
    if not observations["probeResults"] or not any("stdout" in item or "stdoutBase64" in item for item in observations["probeResults"]):
        raise RuntimeError(f"{card} did not retain probe outputs")
    return {
        "card": card,
        "sourceCommit": COMMIT,
        "sourceTree": TREE,
        "engine": "embedded-dolt",
        "configuration": {
            "prefix": prefix,
            "actor": env["BEADS_ACTOR"],
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
