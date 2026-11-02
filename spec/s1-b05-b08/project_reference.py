#!/usr/bin/env python3
"""Independently project a recorded reference twin without changing it."""

import hashlib
import json
import sys


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def project(vectors):
    projection = []
    for vector in vectors:
        observations = vector.get("observations", {})
        probes = observations.get("probeResults", [])
        if not probes or not any("stdout" in command or "stdoutBase64" in command for command in probes):
            raise RuntimeError(f"{vector.get('card', '<unknown>')} has no retained probe output")
        projection.append({
            "card": vector["card"],
            "ids": vector["ids"],
            "prefix": vector["configuration"]["prefix"],
            "commands": [
                {k: command[k] for k in ("argv", "exitCode", "errorCode") if k in command}
                for command in vector["commands"]
            ],
            "observations": observations,
        })
    return projection


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: project_reference.py RECORDING.json")
    value = json.loads(open(sys.argv[1], encoding="utf-8").read())
    if value.get("status") != "recorded" or not value.get("recordings"):
        raise RuntimeError("reference recording is absent or typed blocked")
    first = value["recordings"][0]
    projection = project(first["raw"])
    encoded = canonical(projection).encode()
    print(json.dumps({"projection": projection, "projectionSha256": hashlib.sha256(encoded).hexdigest()}, indent=2))


if __name__ == "__main__":
    main()
