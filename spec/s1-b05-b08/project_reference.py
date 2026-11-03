#!/usr/bin/env python3
"""Independently project a recorded reference twin without changing it."""

import base64
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


def verify_recordings(value):
    runs = value.get("recordings")
    if value.get("status") != "recorded" or not isinstance(runs, list) or len(runs) != 2:
        raise RuntimeError("two actual reference recordings are required")
    projected = []
    for expected, recording in zip(("a", "b"), runs):
        if recording.get("run") != expected:
            raise RuntimeError("reference recording identity differs")
        vectors = recording["raw"]
        if [vector.get("card") for vector in vectors] != ["B05", "B06", "B07", "B08"]:
            raise RuntimeError("reference recording card coverage differs")
        for vector in vectors:
            for command in vector["commands"]:
                for stream in ("stdout", "stderr"):
                    raw = base64.b64decode(command[stream + "Base64"], validate=True)
                    if hashlib.sha256(raw).hexdigest() != command[stream + "Sha256"]:
                        raise RuntimeError("reference stream digest differs from its retained bytes")
                if "stdout" in command:
                    actual = json.loads(base64.b64decode(command["stdoutBase64"]).decode("utf-8"))
                    if actual != command["stdout"]:
                        raise RuntimeError("reference parsed output differs from its retained bytes")
        raw_digest = hashlib.sha256(canonical(vectors).encode()).hexdigest()
        projection = project(vectors)
        projection_digest = hashlib.sha256(canonical(projection).encode()).hexdigest()
        if recording.get("rawSha256") != raw_digest or recording.get("projectionSha256") != projection_digest:
            raise RuntimeError("reference raw/projection digest differs")
        projected.append(projection)
    if runs[0]["raw"] != runs[1]["raw"] or projected[0] != projected[1]:
        raise RuntimeError("reference twins differ")
    return projected[0]


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: project_reference.py RECORDING.json")
    value = json.loads(open(sys.argv[1], encoding="utf-8").read())
    projection = verify_recordings(value)
    encoded = canonical(projection).encode()
    print(json.dumps({"projection": projection, "projectionSha256": hashlib.sha256(encoded).hexdigest()}, indent=2))


if __name__ == "__main__":
    main()
