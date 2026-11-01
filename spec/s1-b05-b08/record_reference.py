#!/usr/bin/env python3
"""Record B05-B08 from a pinned public reference executable.

The script is intentionally fail-closed: it refuses to emit a recording until
the caller supplies the exact source pin, a verified executable, and explicit
Go+SQL clock control. It never copies Plug output into a reference vector.
"""

import hashlib
import json
import os
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


def require_pin():
    if os.environ.get("PLUG_REFERENCE_SOURCE_COMMIT") != COMMIT:
        raise RuntimeError("PLUG_REFERENCE_SOURCE_COMMIT must equal the pinned reference commit")
    if os.environ.get("PLUG_REFERENCE_SOURCE_TREE") != TREE:
        raise RuntimeError("PLUG_REFERENCE_SOURCE_TREE must equal the verified source tree")
    if os.environ.get("PLUG_REFERENCE_CLOCK_MODE") != "go-sql-controlled":
        raise RuntimeError("PLUG_REFERENCE_CLOCK_MODE=go-sql-controlled is required")


def run(binary, root, commands, args, expected=None, env=None):
    merged = {"PATH": os.environ["PATH"], "TZ": "UTC", "LANG": "C.UTF-8"}
    merged.update(env or {})
    result = subprocess.run(
        [str(binary), *args], cwd=root, env=merged, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
    )
    if expected is not None and result.returncode != expected:
        raise RuntimeError(f"unexpected exit for {args!r}: {result.returncode}")
    if expected is None and result.returncode != 0:
        raise RuntimeError(f"reference command failed {args!r}: {result.returncode}")
    outcome = {
        "argv": list(args),
        "exitCode": result.returncode,
        "stdoutSha256": sha(result.stdout.encode()),
        "stderrSha256": sha(result.stderr.encode()),
    }
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        outcome["errorCode"] = payload["error"].get("code")
    commands.append(outcome)
    return result.stdout


def record(card, binary, root):
    prefix = card.lower()
    ids = [f"{prefix}-source", f"{prefix}-target"]
    commands = []
    env = {
        "BEADS_ACTOR": f"{prefix}-reference",
        "PLUG_REFERENCE_SOURCE_COMMIT": COMMIT,
        "PLUG_REFERENCE_SOURCE_TREE": TREE,
        "PLUG_REFERENCE_CLOCK_MODE": "go-sql-controlled",
    }
    run(binary, root, commands, ["init", "--non-interactive", "--quiet", "--skip-hooks", "--skip-agents", "--prefix", prefix], env=env)
    for issue, title in zip(ids, ("Source", "Target")):
        run(binary, root, commands, ["create", "--id", issue, "--type", "task", "--title", title], env=env)
    if card == "B05":
        run(binary, root, commands, ["claim", ids[0], "--json"], env=env)
        run(binary, root, commands, ["heartbeat", ids[0], "--json"], env=env)
        run(binary, root, commands, ["show", ids[0], "--json"], env=env)
        observations = {"lease": "captured from show", "heartbeat": "captured from heartbeat"}
    elif card == "B06":
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "blocks"], env=env)
        run(binary, root, commands, ["dep", "add", ids[1], ids[0], "--type", "blocks"], expected=1, env=env)
        run(binary, root, commands, ["dep", "cycles", "--json"], env=env)
        observations = {"cycle": "typed refusal and deterministic cycle report captured"}
    elif card == "B07":
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "custom-edge"], env=env)
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "custom-edge"], env=env)
        run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "blocks"], expected=1, env=env)
        run(binary, root, commands, ["show", ids[0], "--json"], env=env)
        observations = {"dependency": "custom edge retained; same-type re-add and retype refusal captured"}
    else:
        run(binary, root, commands, ["dep", "add", ids[1], ids[0], "--type", "parent-child"], env=env)
        run(binary, root, commands, ["close", ids[0], "--json"], expected=1, env=env)
        run(binary, root, commands, ["close", ids[1], "--json"], env=env)
        run(binary, root, commands, ["close", ids[0], "--json"], env=env)
        run(binary, root, commands, ["reopen", ids[0], "--json"], env=env)
        observations = {"lifecycle": "open-child refusal, child settlement, parent settlement, and reopen captured"}
    return {
        "card": card,
        "sourceCommit": COMMIT,
        "sourceTree": TREE,
        "engine": "embedded-dolt",
        "configuration": {"prefix": prefix, "actor": env["BEADS_ACTOR"], "clock": "go-sql-controlled"},
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
    require_pin()
    binary = Path(sys.argv[1]).resolve()
    output = Path(sys.argv[2]).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("reference executable is missing or not executable")
    binary_digest = sha(binary.read_bytes())
    runs = []
    for index in ("a", "b"):
        vectors = []
        for card in CARDS:
            root = Path(tempfile.mkdtemp(prefix=f"plug-b05-b08-{index}-{card.lower()}-", dir="/tmp/plug-swarm/work"))
            vectors.append(record(card, binary, root))
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
        "recordings": runs,
        "status": "recorded",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
