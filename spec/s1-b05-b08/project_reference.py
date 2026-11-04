#!/usr/bin/env python3
"""Independently project a recorded reference twin without changing it."""

import base64
import hashlib
import json
import re
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


def verify_b06(vector):
    """Validate retained native observations independently of recorder claims."""
    observations = vector.get("observations", {})
    coverage = observations.get("cycleProbeCoverage", {})
    if (coverage.get("status") != "partial"
            or coverage.get("supported") != ["self-blocks-refusal", "self-tracks-refusal", "ancestor-blocker-refusal", "descendant-blocker-refusal",
                                              "public-refusal-conservation", "nonempty-tracks-report", "lowest-id-rotation", "report-order", "pure-tracks-exclusion",
                                              "unchanged-report-bytes", "same-type-readd", "native-json-uncapped", "native-human-limit"]
            or coverage.get("blocked") != ["blocking-only-cycle-public-fixture", "parallel-rows-public-fixture", "unreadable-cycle-public-fixture"]):
        raise RuntimeError("B06 must retain the supported probe scope and unresolved public fixture dependencies")
    paths = [["b06-alpha", "b06-middle", "b06-zulu"], ["b06-beta", "b06-november", "b06-yankee"]]
    specifications = [
        ("acyclic-control", ["--json"], []),
        ("pure-tracks-control", ["--include-tracks", "--json"], []),
        ("default-excludes-tracks", ["--json"], []),
        ("tracks-expanded", ["--include-tracks", "--json"], paths),
        ("unchanged-repeat", ["--include-tracks", "--json"], paths),
        ("same-edge-readd", ["--include-tracks", "--json"], paths),
        ("json-limit-uncapped", ["--include-tracks", "--limit", "1", "--json"], paths),
        ("human-limit", ["--include-tracks", "--limit", "1"], 1),
        ("human-unlimited", ["--include-tracks", "--limit", "0"], 2),
    ]
    probes = observations.get("cycleProbes", [])
    if len(probes) != len(specifications):
        raise RuntimeError("B06 native cycle probe coverage is incomplete")
    commands = vector["commands"]
    # Bind every creation and edge, including their order and exits. A valid
    # report must not cover for a dropped fixture operation or a bypass flag.
    transcript = [
        (["init", "--non-interactive", "--quiet", "--skip-hooks", "--skip-agents", "--prefix", "b06"], 0),
        (["version"], 0),
        (["create", "--id", "b06-source", "--type", "task", "--title", "Source"], 0),
        (["create", "--id", "b06-target", "--type", "task", "--title", "Target"], 0),
    ]
    expected_indices = []
    expected_ids = ["b06-source", "b06-target"]
    expected_refusals = []

    def append_probe(number):
        expected_indices.append(len(transcript))
        transcript.append((["dep", "cycles", *specifications[number][1]], 0))

    def append_create(identifier):
        expected_ids.append(identifier)
        transcript.append((["create", "--id", identifier, "--type", "task", "--title", identifier], 0))

    def append_refusal(label, source, target, kind, members, reason):
        before = []
        for identifier, _ in members:
            before.append(len(transcript))
            transcript.append((["show", identifier, "--json"], 0))
        index = len(transcript)
        transcript.append((["dep", "add", source, target, "--type", kind], 1))
        after = []
        for identifier, _ in members:
            after.append(len(transcript))
            transcript.append((["show", identifier, "--json"], 0))
        expected_refusals.append((
            {"case": label, "commandIndex": index, "beforeIndices": before, "afterIndices": after}, members, reason))

    for kind in ("blocks", "tracks"):
        append_refusal("self-" + kind, "b06-source", "b06-source", kind,
                       [("b06-source", None)], rb"self|itself")
    for identifier in ("b06-ancestor", "b06-child", "b06-descendant"):
        append_create(identifier)
    transcript.extend([
        (["dep", "add", "b06-child", "b06-ancestor", "--type", "parent-child"], 0),
        (["dep", "add", "b06-descendant", "b06-child", "--type", "parent-child"], 0),
    ])
    members = [("b06-ancestor", None), ("b06-child", "b06-ancestor"), ("b06-descendant", "b06-child")]
    for label, source, target in (("ancestor-blocker", "b06-ancestor", "b06-descendant"),
                                  ("descendant-blocker", "b06-descendant", "b06-ancestor")):
        append_refusal(label, source, target, "blocks", members, rb"ancestor|descendant|hierarch|parent|child")
    scheduling_start = len(transcript)
    transcript.extend([
        (["dep", "add", "b06-source", "b06-target", "--type", "blocks"], 0),
        (["dep", "add", "b06-target", "b06-source", "--type", "blocks"], 1),
    ])
    append_probe(0)
    for identifier in ("b06-pure-a", "b06-pure-z"):
        append_create(identifier)
    transcript.extend([
        (["dep", "add", "b06-pure-a", "b06-pure-z", "--type", "tracks"], 0),
        (["dep", "add", "b06-pure-z", "b06-pure-a", "--type", "tracks"], 0),
    ])
    append_probe(1)
    for low, middle, high in (paths[1], paths[0]):
        for identifier in (low, middle, high):
            append_create(identifier)
        transcript.extend([
            (["dep", "add", high, low, "--type", "blocks"], 0),
            (["dep", "add", low, middle, "--type", "tracks"], 0),
            (["dep", "add", middle, high, "--type", "tracks"], 0),
        ])
    for number in (2, 3, 4):
        append_probe(number)
    transcript.append((["dep", "add", "b06-zulu", "b06-alpha", "--type", "blocks"], 0))
    for number in (5, 6, 7, 8):
        append_probe(number)
    if len(commands) != len(transcript) or vector.get("ids") != expected_ids:
        raise RuntimeError("B06 lost a public fixture operation or its exact created IDs")
    for command, (arguments, exit_code) in zip(commands, transcript):
        if (command.get("argv") != arguments or type(command.get("exitCode")) is not int
                or command.get("exitCode") != exit_code):
            raise RuntimeError("B06 fixture must retain every ordinary guarded command and its exit")
    if observations.get("guardRefusals") != [item[0] for item in expected_refusals]:
        raise RuntimeError("B06 self and hierarchy refusals lost their exact public command bindings")
    for refusal, members, reason in expected_refusals:
        command = commands[refusal["commandIndex"]]
        diagnostic = (base64.b64decode(command["stdoutBase64"], validate=True)
                      + base64.b64decode(command["stderrBase64"], validate=True)).lower()
        if (not re.search(reason, diagnostic)
                or re.search(rb"unknown command|unknown flag|usage:|unrecognized", diagnostic)):
            raise RuntimeError("B06 self or hierarchy refusal did not identify its guard")
        for before, after, (identifier, parent) in zip(refusal["beforeIndices"], refusal["afterIndices"], members):
            raw = base64.b64decode(commands[before]["stdoutBase64"], validate=True)
            if raw != base64.b64decode(commands[after]["stdoutBase64"], validate=True):
                raise RuntimeError("B06 refused edge changed a retained public member")
            rows = json.loads(raw)
            if not isinstance(rows, list) or len(rows) != 1 or rows[0].get("id") != identifier:
                raise RuntimeError("B06 refusal conservation lost its public member")
            edges = rows[0].get("dependencies") or []
            expected = [] if parent is None else [(parent, "parent-child")]
            if not isinstance(edges, list) or [(edge.get("id"), edge.get("dependency_type")) for edge in edges] != expected:
                raise RuntimeError("B06 refusal baseline lost its exact parent-child fixture")
    refusal = commands[scheduling_start + 1]
    if b"cycle" not in (base64.b64decode(refusal["stdoutBase64"], validate=True)
                        + base64.b64decode(refusal["stderrBase64"], validate=True)).lower():
        raise RuntimeError("B06 reverse-edge refusal did not identify a cycle")
    indices = []
    expanded_bytes = None
    for probe, (label, arguments, expected) in zip(probes, specifications):
        index = probe.get("commandIndex")
        if type(index) is not int or not 0 <= index < len(commands) or probe.get("case") != label:
            raise RuntimeError("B06 native probe command binding differs")
        indices.append(index)
        command = commands[index]
        if command.get("argv") != ["dep", "cycles", *arguments] or command.get("exitCode") != 0:
            raise RuntimeError("B06 native probe argv or exit differs")
        raw = base64.b64decode(command["stdoutBase64"], validate=True)
        if isinstance(expected, list):
            report = json.loads(raw.decode("utf-8"))
            if not isinstance(report, list):
                raise RuntimeError("B06 native cycle report is not an array")
            actual = []
            for cycle in report:
                if not isinstance(cycle, dict):
                    raise RuntimeError("B06 native cycle must be an object")
                members = cycle.get("members")
                if cycle.get("partial") is not False or not isinstance(members, list) or not members:
                    raise RuntimeError("B06 supported public fixture lost complete members")
                path = []
                for member in members:
                    if not isinstance(member, dict) or not isinstance(member.get("issue"), dict):
                        raise RuntimeError("B06 native member must retain its hydrated row")
                    identifier = member.get("id")
                    if not isinstance(identifier, str) or not identifier or member.get("issue", {}).get("id") != identifier:
                        raise RuntimeError("B06 native member differs from its hydrated row")
                    path.append(identifier)
                actual.append(path)
            if actual != expected or probe.get("paths") != actual:
                raise RuntimeError("B06 native paths are empty, unordered, incomplete, or differ from retained observations")
            if expected:
                if expanded_bytes is None:
                    expanded_bytes = raw
                elif raw != expanded_bytes:
                    raise RuntimeError("B06 unchanged/readd/JSON-limit native bytes differ")
        else:
            text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", raw.decode("utf-8"))
            headings = re.findall(r"^\d+\. Cycle involving:\s*$", text, re.MULTILINE)
            members = re.findall(r"^   - ([^:]+):", text, re.MULTILINE)
            if ("Found 2 dependency cycles:" not in text or len(headings) != expected
                    or members != [identifier for path in paths[:expected] for identifier in path]
                    or ("... and 1 more not shown." in text) != (expected == 1)
                    or (expected == 2 and "more not shown" in text)
                    or type(probe.get("total")) is not int or type(probe.get("shown")) is not int
                    or probe.get("total") != 2 or probe.get("shown") != expected):
                raise RuntimeError("B06 native human limit changed the whole total or member projection")
    if indices != expected_indices:
        raise RuntimeError("B06 probes must bind their exact commands in recorded order")
    if observations.get("probeResults") != commands[4:]:
        raise RuntimeError("B06 probe observations are not the retained command suffix")


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
            if vector.get("card") == "B06":
                verify_b06(vector)
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
