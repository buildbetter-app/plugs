#!/usr/bin/env python3
"""Execute the B07/B25 reference twins; record observations, never Plug output."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


CARDS = {
    "B07": {
        "actor": "build50-recorder",
        "ids": ["b07-source", "b07-target"],
        "sourceTests": [
            "cmd/bd/protocol/dep_identity_test.go::TestProtocol_DepReAddSameTypeIsIdempotent",
        ],
        "graph": {"seed": "explicit-pair", "nodes": 2, "edges": 1},
    },
    "B25": {
        "actor": "build50-recorder",
        "ids": ["b25-source", "b25-target"],
        "sourceTests": [
            "cmd/bd/protocol/preservation_test.go::TestProtocol_LabelsPreservedAcrossUpdate",
            "cmd/bd/protocol/preservation_test.go::TestProtocol_DepsPreservedAcrossUpdate",
            "cmd/bd/protocol/preservation_test.go::TestProtocol_CommentsPreservedAcrossUpdate",
            "cmd/bd/protocol/preservation_test.go::TestProtocol_ScalarUpdatePreservesRelationalData",
        ],
        "graph": {"seed": "explicit-pair", "nodes": 2, "edges": 1},
    },
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def configuration(card):
    return {
        "actor": CARDS[card]["actor"],
        "owner": "build50@example.invalid",
        "engine": "embedded-dolt",
        "prefix": card.lower(),
        "environment": {
            "BEADS_DOLT_AUTO_START": "1",
            "BEADS_ACTOR": CARDS[card]["actor"],
            "CI": "1",
            "TZ": "UTC",
            "LANG": "C.UTF-8",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_EMAIL": "build50@example.invalid",
        },
        "ambientEnvironment": "PATH only; no inherited application configuration",
        "initFlags": ["--non-interactive", "--quiet", "--skip-agents", "--skip-hooks"],
        "history": "fresh-store defaults; no history assertion in this slice",
    }


def inputs(card):
    return {
        "clock": {
            "mode": "runtime-non-temporal-slice",
            "schedule": "no clock advances or time predicate; runtime Go/SQL stamps excluded",
            "fieldsOmitted": ["timestamps", "revision hashes", "generated relation IDs"],
        },
        "ids": {
            "prefix": card.lower(),
            "explicit": CARDS[card]["ids"],
            "collisionState": "fresh disposable store",
            "counterState": "unused; explicit top-level IDs, no child or hash allocation",
        },
        "graph": CARDS[card]["graph"],
        "sequence": (
            "create pair; add blocks three times; remove; add relates-to"
            if card == "B07" else
            "create pair; add ordered labels, block and two comments; patch title, priority and description"
        ),
    }


def run(binary, root, config, commands, *args):
    env = {"PATH": os.environ["PATH"], **config["environment"]}
    result = subprocess.run(
        [str(binary), *args], cwd=root, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
    )
    if audit_dir := os.environ.get("BUILD50_AUDIT_DIR"):
        audit = Path(audit_dir)
        audit.mkdir(parents=True, exist_ok=True)
        stem = f"{len(commands):02d}"
        (audit / (stem + ".stdout")).write_text(result.stdout, encoding="utf-8")
        (audit / (stem + ".stderr")).write_text(result.stderr, encoding="utf-8")
        (audit / (stem + ".command.json")).write_text(json.dumps({
            "executable": str(binary), "cwd": str(root), "args": list(args),
            "environment": env, "exit": result.returncode,
        }, indent=2) + "\n", encoding="utf-8")
    commands.append({"args": list(args), "exit": result.returncode})
    if result.returncode != 0:
        raise RuntimeError(f"reference command {args!r}: {result.returncode}\n{result.stdout}\n{result.stderr}")
    return result.stdout


def show(binary, root, config, commands, issue):
    result = json.loads(run(binary, root, config, commands, "show", issue, "--include-comments", "--json"))
    if not isinstance(result, list) or len(result) != 1 or result[0]["id"] != issue:
        raise ValueError(f"unexpected reference detail: {result!r}")
    return result[0]


def edges(row):
    return [
        {"from": row["id"], "to": edge["id"], "kind": edge["dependency_type"]}
        for edge in row.get("dependencies", [])
    ]


def preserved(row):
    # The reference returns labels in lexical order and comments in thread order.
    # Preserve both arrays exactly as observed; do not sort semantic output.
    return {
        "labels": row["labels"],
        "dependencies": edges(row),
        "comments": [comment["text"] for comment in row["comments"]],
        "commentCount": row["comment_count"],
    }


def record(card, binary, root):
    config = configuration(card)
    commands = []
    call = lambda *args: run(binary, root, config, commands, *args)
    call("init", "--prefix", card.lower(), *config["initFlags"])
    source, target = CARDS[card]["ids"]
    for issue, title in ((source, "Source"), (target, "Target")):
        call("create", "--id", issue, "--type", "task", "--title", title, "--priority", "2")
    call("dep", "add", source, target, "--type", "blocks")
    if card == "B07":
        call("dep", "add", source, target, "--type", "blocks")
        call("dep", "add", source, target, "--type", "blocks")
        repeated = edges(show(binary, root, config, commands, source))
        if repeated != [{"from": source, "to": target, "kind": "blocks"}]:
            raise ValueError(f"duplicate re-add changed the edge set: {repeated!r}")
        call("dep", "remove", source, target)
        removed = edges(show(binary, root, config, commands, source))
        if removed:
            raise ValueError(f"removed edge remains: {removed!r}")
        call("dep", "add", source, target, "--type", "relates-to")
        readded = edges(show(binary, root, config, commands, source))
        if readded != [{"from": source, "to": target, "kind": "relates-to"}]:
            raise ValueError(f"explicit remove/re-add did not land: {readded!r}")
        observations = {"afterSameTypeReadds": repeated, "afterRemove": removed, "afterExplicitRetype": readded}
    else:
        for label in ("alpha", "beta"):
            call("label", "add", source, label)
        for body in ("Design review notes", "Implementation started"):
            call("comments", "add", source, body)
        before = preserved(show(binary, root, config, commands, source))
        states = []
        for flag, value in (("--title", "Renamed source"), ("--priority", "0"), ("--description", "Updated description")):
            call("update", source, flag, value)
            row = show(binary, root, config, commands, source)
            relations = preserved(row)
            if relations != before:
                raise ValueError(f"scalar update {flag} lost relations: {before!r} -> {relations!r}")
            states.append({"operation": flag[2:], "relations": relations})
        observations = {
            "before": before, "afterEachUpdate": states,
            "finalFields": {"title": row["title"], "priority": row["priority"], "description": row["description"]},
        }
    vector = {
        "card": card, "engine": "embedded-dolt", "sourceTests": CARDS[card]["sourceTests"],
        "configuration": config, "inputs": inputs(card), "commands": commands, "observations": observations,
    }
    raw = canonical(vector)
    return {"digest": hashlib.sha256(raw.encode()).hexdigest(), "rawJSON": raw, "vector": vector}


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in CARDS:
        raise SystemExit("usage: build50_record.py B07|B25 REFERENCE_EXECUTABLE OUTPUT")
    card, executable, output = sys.argv[1:]
    binary = Path(executable).resolve()
    root = Path(tempfile.mkdtemp(prefix="build50-" + card.lower() + "-", dir="/tmp/plug-swarm/work"))
    if audit_dir := os.environ.get("BUILD50_AUDIT_DIR"):
        audit = Path(audit_dir)
        audit.mkdir(parents=True, exist_ok=True)
        (audit / "workspace.txt").write_text(str(root) + "\n", encoding="utf-8")
    result = record(card, binary, root)
    Path(output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(card + " rawDigest=" + result["digest"])


if __name__ == "__main__":
    main()
