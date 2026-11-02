#!/usr/bin/env python3
"""Record the bounded reference CLI in two independent disposable stores."""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path


PIN = "9d6fff760b7a314b476539198504d77ce0959656"
BINARY = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
ORDERED = (
    "update b21-anchor title=middle\n"
    "create task 2 created-second\n"
    "update b21-anchor title=declared-last\n"
    "update b21-anchor priority=1\n"
)
REFUSED = (
    "update b21-anchor title=must-rollback\n"
    "create task 2 must-not-exist\n"
    "update b21-missing title=refused-last\n"
)
CLOSE = (
    "update b21-anchor title=close-in-batch\n"
    "create task 2 close-created\n"
    "close b21-anchor complete\n"
)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(data):
    return hashlib.sha256(data if isinstance(data, bytes) else data.encode()).hexdigest()


def project(raw):
    by_step = {row["step"]: row for row in raw}

    def result(step):
        row = by_step[step]
        if row["exit"]:
            raise ValueError("reference command refused: " + step)
        return json.loads(row["stdout"])

    ordered = result("orderedBatch")
    anchor, = result("orderedAnchor")
    created_id = ordered["results"][1]["target"]
    created, = [row for row in result("afterOrderedList") if row["id"] == created_id]
    targets = [row["target"] for row in ordered["results"]]
    if targets != ["b21-anchor", created_id, "b21-anchor", "b21-anchor"]:
        raise ValueError("reference results lost declaration order")
    closed, = result("closedAnchor")
    history_before = result("beforeOrderedHistory")
    history_ordered = result("afterOrderedHistory")
    history_refused = result("afterRefusedHistory")
    return {
        "orderedAnchorTitle": anchor["title"],
        "orderedAnchorPriority": anchor["priority"],
        "orderedCreatedTitle": created["title"],
        "orderedItemCount": ordered["operations"],
        "orderedResultRoles": ["anchor", "created", "anchor", "anchor"],
        "orderedRowDelta": len(result("afterOrderedList")) - len(result("beforeOrderedList")),
        "orderedHistoryDelta": len(history_ordered) - len(history_before),
        "refusedBatchExitNonzero": by_step["refusedBatch"]["exit"] != 0,
        "refusedBatchChangedState": result("afterRefusedList") != result("afterOrderedList"),
        "refusedHistoryChanged": history_refused != history_ordered,
        "referenceBatchCloseStatus": closed["status"],
        "referenceBatchCloseHistoryDelta": len(result("afterCloseHistory")) - len(history_refused),
    }


def record(binary, evidence, name):
    workspace = Path(tempfile.mkdtemp(prefix="cardb21-" + name + "-", dir="/tmp/plug-swarm/work"))
    subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("BEADS_", "BD_", "DOLT_"))}
    env.update({"CI": "true", "NO_COLOR": "1"})
    commands = []

    def call(step, args, stdin=None, refusal=False):
        argv = [str(binary), "--sandbox", "--actor", "cardb21", "--json", *args]
        result = subprocess.run(argv, input=stdin, cwd=workspace, env=env, text=True, capture_output=True, timeout=90)
        row = {"step": step, "argv": argv, "stdin": stdin, "exit": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
        commands.append(row)
        (evidence / (name + ".partial.json")).write_text(canonical(commands) + "\n")
        if bool(result.returncode) != refusal:
            raise ValueError("unexpected reference result: " + canonical(row))

    call("init", ["init", "--prefix", "b21", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"])
    call("anchor", ["create", "--id", "b21-anchor", "--title", "anchor", "--type", "task"])
    call("beforeOrderedList", ["list", "--all", "--limit", "0"])
    call("beforeOrderedHistory", ["history", "b21-anchor"])
    call("orderedBatch", ["batch"], ORDERED)
    call("orderedAnchor", ["show", "b21-anchor"])
    call("afterOrderedList", ["list", "--all", "--limit", "0"])
    call("afterOrderedHistory", ["history", "b21-anchor"])
    call("refusedBatch", ["batch"], REFUSED, refusal=True)
    call("afterRefusedList", ["list", "--all", "--limit", "0"])
    call("afterRefusedHistory", ["history", "b21-anchor"])
    call("closeBatch", ["batch"], CLOSE)
    call("closedAnchor", ["show", "b21-anchor"])
    call("afterCloseHistory", ["history", "b21-anchor"])
    projection = project(commands)
    assert projection["orderedHistoryDelta"] == 1 and projection["orderedRowDelta"] == 1
    assert not projection["refusedBatchChangedState"] and not projection["refusedHistoryChanged"]
    assert projection["referenceBatchCloseStatus"] == "closed" and projection["referenceBatchCloseHistoryDelta"] == 1
    raw = canonical(commands)
    record = {"workspace": str(workspace), "rawJSON": raw, "rawSha256": digest(raw), "projection": projection, "projectionSha256": digest(canonical(projection))}
    (evidence / (name + ".json")).write_text(json.dumps(record, indent=2) + "\n")
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("verification", type=Path)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    verification_bytes = args.verification.read_bytes()
    verified = json.loads(verification_bytes)
    assert verified["sourcePin"] == PIN and verified["verification"] == "PASS"
    assert verified["verifiedBlobsAndModes"] == 5087 and verified["sourceExtras"] == []
    assert verified["binarySha256"] == BINARY and digest(args.binary.read_bytes()) == BINARY
    args.evidence.mkdir(parents=True, exist_ok=False)
    runs = [record(args.binary, args.evidence, name) for name in ("first", "repeat")]
    assert runs[0]["projection"] == runs[1]["projection"]
    inputs = {"ordered": ORDERED, "refused": REFUSED, "close": CLOSE, "referenceType": "task", "plugType": "issue", "referenceAnchor": "b21-anchor", "plugAnchor": "b21-11"}
    manifest = {
        "sourcePin": PIN,
        "binarySha256": BINARY,
        "sourceArchiveSha256": verified["sourceArchiveSha256"],
        "buildInfoSha256": verified["buildinfoSha256"],
        "sourceVerificationSha256": digest(verification_bytes),
        "buildCommandSha256": digest(canonical(verified["buildCommand"])),
        "recorderSha256": digest(Path(__file__).read_bytes()),
        "inputs": inputs,
        "inputSha256": digest(canonical(inputs)),
        "projectionPaths": sorted(runs[0]["projection"]),
        "projectionOmissions": ["timestamps", "commit hashes", "minted IDs", "owner and reference-only schema fields"],
        "unsupported": {
            "plugAtomicClose": "UNRUN: admitted ChangeSet has no close op; generic status patches require a lifecycle verb",
            "referenceTypedConcurrency": "UNRUN: CLI has no typed concurrent transaction receipt contract",
            "referenceStoredCommitReplay": "UNRUN: CLI has no Plug idempotency receipt or post-subscriber protocol",
            "referenceAllocatorState": "UNRUN: CLI does not expose the durable allocator contract",
            "serverHTTP": "UNRUN: no server or HTTP runtime installed or launched",
        },
        "runs": runs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    print(canonical({"output": str(args.output), "workspaces": [run["workspace"] for run in runs], "projection": runs[0]["projection"]}))


if __name__ == "__main__":
    main()
