#!/usr/bin/env python3
"""Record the bounded non-temporal B01 projection from pinned Beads."""

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path


PINNED_COMMIT = "9d6fff760b7a314b476539198504d77ce0959656"


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest_bytes(value):
    return hashlib.sha256(value).hexdigest()


def digest(value):
    return digest_bytes(value.encode())


def run(command, cwd, env):
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout


def fixture_rows():
    stamp = "2026-01-01T00:00:00Z"

    def row(issue_id, title, status, priority, **extra):
        value = {
            "id": issue_id,
            "title": title,
            "status": status,
            "priority": priority,
            "issue_type": "task",
            "created_at": stamp,
            "updated_at": stamp,
            "created_by": "b01-reference-recorder",
        }
        value.update(extra)
        return value

    return [
        row("b01-p0", "priority zero", "open", 0),
        row("b01-tie-a", "tie a", "open", 1),
        row("b01-tie-b", "tie b", "open", 1),
        row("b01-deferred", "deferred", "deferred", 2),
        row("b01-closed", "closed", "closed", 2, closed_at=stamp),
        row("b01-pinned", "pinned", "pinned", 2, pinned=True),
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-binary", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.workspace.exists():
        raise SystemExit(f"workspace already exists: {args.workspace}")
    args.workspace.mkdir(parents=True)
    source_commit = run(["git", "rev-parse", "HEAD"], args.source, os.environ).strip()
    if source_commit != PINNED_COMMIT:
        raise SystemExit(f"source is {source_commit}, expected {PINNED_COMMIT}")
    source_tree = run(["git", "rev-parse", "HEAD^{tree}"], args.source, os.environ).strip()
    if run(["git", "status", "--porcelain"], args.source, os.environ) != "":
        raise SystemExit("reference source is not an exact clean pinned tree")

    env = dict(os.environ)
    env.update(
        {
            "BD_NON_INTERACTIVE": "1",
            "BEADS_ACTOR": "b01-reference-recorder",
            "CI": "true",
            "NO_COLOR": "1",
        }
    )
    run(["git", "init", "-q"], args.workspace, env)
    run(["git", "config", "user.name", "B01 Recorder"], args.workspace, env)
    run(["git", "config", "user.email", "b01-recorder@example.invalid"], args.workspace, env)
    run(
        [
            str(args.reference_binary),
            "init",
            "--non-interactive",
            "--prefix",
            "b01",
            "--skip-hooks",
            "--skip-agents",
            "--quiet",
        ],
        args.workspace,
        env,
    )

    rows = fixture_rows()
    fixture = "".join(canonical(value) + "\n" for value in rows)
    fixture_path = args.workspace / "fixture.jsonl"
    fixture_path.write_text(fixture)
    run([str(args.reference_binary), "import", str(fixture_path), "--json"], args.workspace, env)
    raw_stdout = run(
        [str(args.reference_binary), "ready", "--json", "--sort", "priority", "--limit", "0"],
        args.workspace,
        env,
    )
    raw = json.loads(raw_stdout)
    projection = [
        {
            "createdAt": item["created_at"],
            "id": item["id"],
            "priority": item["priority"],
            "status": item["status"],
        }
        for item in raw
    ]
    expected_ids = ["b01-p0", "b01-tie-a", "b01-tie-b"]
    if [item["id"] for item in projection] != expected_ids:
        raise SystemExit(f"unexpected ready projection: {projection}")
    if any(item["id"] in expected_ids for item in rows[3:]):
        raise SystemExit("excluded fixture identity overlaps the ready set")

    script_bytes = Path(__file__).read_bytes()
    version = run([str(args.reference_binary), "version"], args.workspace, env).strip()
    raw_json = canonical(raw)
    projection_json = canonical(projection)
    record = {
        "card": "B01",
        "scope": "non-temporal-ready-membership-priority-id-tie",
        "engine": "embedded-dolt",
        "sourceCommit": source_commit,
        "sourceTree": source_tree,
        "binary": {
            "sha256": digest_bytes(args.reference_binary.read_bytes()),
            "versionDigest": digest(version),
            "buildTags": ["gms_pure_go"],
        },
        "recorderSha256": digest_bytes(script_bytes),
        "configuration": {"prefix": "b01", "sort": "priority", "limit": 0},
        "fixtureDigest": digest(fixture),
        "raw": raw,
        "rawDigest": digest(raw_json),
        "projection": projection,
        "projectionDigest": digest(projection_json),
        "excludedIDs": ["b01-closed", "b01-deferred", "b01-pinned"],
        "limits": [
            "No hybrid 48-hour cutoff or timing bound measured.",
            "No filters, dependency planes, wisps, or parent propagation claimed.",
            "Reference ordering uses explicit --sort priority and equal created_at for the ID tie.",
        ],
    }
    args.output.write_text(canonical(record) + "\n")


if __name__ == "__main__":
    main()
