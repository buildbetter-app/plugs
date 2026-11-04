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
import re
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


def b06_paths(stdout, expected, label):
    """Check the actual native report without repairing its path or order."""
    try:
        report = json.loads(stdout.decode("utf-8"))
        if not isinstance(report, list):
            raise ValueError("report must be an array")
        paths = []
        for cycle in report:
            if cycle["partial"] is not False or not cycle["members"]:
                raise ValueError("these public fixtures have complete members")
            path = []
            for member in cycle["members"]:
                identifier = member["id"]
                if not isinstance(identifier, str) or not identifier or member["issue"]["id"] != identifier:
                    raise ValueError("member hydration differs from its edge ID")
                path.append(identifier)
            if len(set(path)) != len(path) or path[0] != min(path):
                raise ValueError("path must be distinct and start at its lowest ID")
            paths.append(path)
        if paths != expected:
            raise ValueError(f"paths {paths!r} differ from {expected!r}")
        return paths
    except (KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
        raise RuntimeError(f"B06 {label} report failed: {exc}") from exc


def b06_rendered(stdout, expected_paths, shown):
    # Keep the original streams; remove terminal decoration only for assertions.
    rendered = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", stdout.decode("utf-8"))
    total = len(expected_paths)
    headings = re.findall(r"^\d+\. Cycle involving:\s*$", rendered, re.MULTILINE)
    members = re.findall(r"^   - ([^:]+):", rendered, re.MULTILINE)
    expected_members = [identifier for path in expected_paths[:shown] for identifier in path]
    omitted = f"... and {total - shown} more not shown."
    if (f"Found {total} dependency cycles:" not in rendered or len(headings) != shown
            or members != expected_members or (omitted in rendered) != (shown < total)
            or (shown == total and "more not shown" in rendered)):
        raise RuntimeError("B06 native human limit changed the total, order, or rendered members")
    return {"total": total, "shown": shown}


def record_b06(binary, root, commands, env, ids, probe_start):
    """Use ordinary guarded CLI operations, never legacy-row or SQL injection.

    A blocks-only cycle cannot be made through the traced add/import routes:
    even bulk --no-cycle-check retains a final graph gate, and import records
    skipped cycle edges. Tracks can legitimately close a diagnostic path while
    leaving the scheduling graph acyclic. That supported case is useful real
    coverage, with the remaining native fixture dependencies stated explicitly.
    """
    probes = []
    refusals = []

    def create(identifier):
        run(binary, root, commands, ["create", "--id", identifier, "--type", "task", "--title", identifier], env=env)
        ids.append(identifier)

    def refuse(label, arguments, members, reason):
        before_indices = []
        before = []
        for identifier, parent in members:
            before_indices.append(len(commands))
            output = run(binary, root, commands, ["show", identifier, "--json"], env=env)
            row = json.loads(output)[0]
            if row.get("id") != identifier:
                raise RuntimeError("B06 refusal baseline differs from its public member")
            if parent is not None and dependency_projection(output, identifier, parent) != [
                    {"from": identifier, "to": parent, "kind": "parent-child"}]:
                raise RuntimeError("B06 refusal fixture lost its public parent-child edge")
            before.append(output)
        index = len(commands)
        output = run(binary, root, commands, arguments, expected=1, env=env)
        diagnostic = (output + base64.b64decode(commands[-1]["stderrBase64"])).lower()
        if (not re.search(reason, diagnostic)
                or re.search(rb"unknown command|unknown flag|usage:|unrecognized", diagnostic)):
            raise RuntimeError(f"B06 {label} did not identify its guard refusal")
        after_indices = []
        for (identifier, _), original in zip(members, before):
            after_indices.append(len(commands))
            if run(binary, root, commands, ["show", identifier, "--json"], env=env) != original:
                raise RuntimeError(f"B06 {label} changed a public member after refusal")
        refusals.append({"case": label, "commandIndex": index,
                         "beforeIndices": before_indices, "afterIndices": after_indices})

    def report(label, arguments, expected):
        index = len(commands)
        output = run(binary, root, commands, ["dep", "cycles", *arguments], env=env)
        probes.append({"case": label, "commandIndex": index, "paths": b06_paths(output, expected, label)})
        return output

    for kind in ("blocks", "tracks"):
        refuse("self-" + kind, ["dep", "add", ids[0], ids[0], "--type", kind],
               [(ids[0], None)], rb"self|itself")
    ancestor, child, descendant = ("b06-ancestor", "b06-child", "b06-descendant")
    for identifier in (ancestor, child, descendant):
        create(identifier)
    for source, target in ((child, ancestor), (descendant, child)):
        run(binary, root, commands, ["dep", "add", source, target, "--type", "parent-child"], env=env)
    members = [(ancestor, None), (child, ancestor), (descendant, child)]
    for label, source, target in (("ancestor-blocker", ancestor, descendant),
                                  ("descendant-blocker", descendant, ancestor)):
        refuse(label, ["dep", "add", source, target, "--type", "blocks"], members,
               rb"ancestor|descendant|hierarch|parent|child")

    run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "blocks"], env=env)
    refusal = run(binary, root, commands, ["dep", "add", ids[1], ids[0], "--type", "blocks"], expected=1, env=env)
    if b"cycle" not in (refusal + base64.b64decode(commands[-1]["stderrBase64"])).lower():
        raise RuntimeError("B06 reverse-edge refusal did not identify a cycle")
    report("acyclic-control", ["--json"], [])

    prefix = "b06"
    pure = [f"{prefix}-pure-a", f"{prefix}-pure-z"]
    rings = [[f"{prefix}-alpha", f"{prefix}-middle", f"{prefix}-zulu"],
             [f"{prefix}-beta", f"{prefix}-november", f"{prefix}-yankee"]]

    for identifier in pure:
        create(identifier)
    for source, target in (pure, list(reversed(pure))):
        run(binary, root, commands, ["dep", "add", source, target, "--type", "tracks"], env=env)
    report("pure-tracks-control", ["--include-tracks", "--json"], [])

    # Write the higher ring first, and each scheduling edge from its highest
    # member, so a correct answer must order and rotate the actual report.
    for low, middle, high in reversed(rings):
        for identifier in (low, middle, high):
            create(identifier)
        for source, target, kind in ((high, low, "blocks"), (low, middle, "tracks"), (middle, high, "tracks")):
            run(binary, root, commands, ["dep", "add", source, target, "--type", kind], env=env)
    report("default-excludes-tracks", ["--json"], [])
    expanded = report("tracks-expanded", ["--include-tracks", "--json"], rings)
    repeated = report("unchanged-repeat", ["--include-tracks", "--json"], rings)
    if repeated != expanded:
        raise RuntimeError("B06 unchanged native cycle report bytes differ")
    run(binary, root, commands, ["dep", "add", rings[0][2], rings[0][0], "--type", "blocks"], env=env)
    if report("same-edge-readd", ["--include-tracks", "--json"], rings) != expanded:
        raise RuntimeError("B06 same-type re-add changed the native report")
    # Native JSON returns before the renderer's cap; Plug's result envelope
    # has its own explicit count/truncated/items projection instead.
    if report("json-limit-uncapped", ["--include-tracks", "--limit", "1", "--json"], rings) != expanded:
        raise RuntimeError("B06 native JSON limit truncated or changed the report")
    for label, limit, shown in (("human-limit", "1", 1), ("human-unlimited", "0", 2)):
        index = len(commands)
        output = run(binary, root, commands, ["dep", "cycles", "--include-tracks", "--limit", limit], env=env)
        probes.append({"case": label, "commandIndex": index, **b06_rendered(output, rings, shown)})
    return {
        "probeResults": commands[probe_start:],
        "guardRefusals": refusals,
        "cycleProbes": probes,
        "cycleProbeCoverage": {
            "status": "partial",
            "supported": ["self-blocks-refusal", "self-tracks-refusal", "ancestor-blocker-refusal", "descendant-blocker-refusal",
                          "public-refusal-conservation", "nonempty-tracks-report", "lowest-id-rotation", "report-order", "pure-tracks-exclusion",
                          "unchanged-report-bytes", "same-type-readd", "native-json-uncapped", "native-human-limit"],
            "blocked": ["blocking-only-cycle-public-fixture", "parallel-rows-public-fixture", "unreadable-cycle-public-fixture"],
        },
    }


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
        observations = record_b06(binary, root, commands, env, ids, probe_start)
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
