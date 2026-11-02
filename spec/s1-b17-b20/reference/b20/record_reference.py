#!/usr/bin/env python3
"""Record the stable B20 Beads journal projection from the pinned bd binary."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


WORK_ROOT = Path("/tmp/plug-swarm/work")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run(argv: list[str], cwd: Path, env: dict[str, str]) -> tuple[bytes, bytes]:
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(argv)}\n"
            f"stdout:\n{result.stdout.decode(errors='replace')}\n"
            f"stderr:\n{result.stderr.decode(errors='replace')}"
        )
    return result.stdout, result.stderr


def journal(stdout: bytes) -> list[dict[str, object]]:
    return [json.loads(line) for line in stdout.decode().splitlines() if line.strip()]


def projection(records: list[dict[str, object]]) -> list[dict[str, object]]:
    projected: list[dict[str, object]] = []
    for record in records:
        issue = record.get("issue")
        if not isinstance(issue, dict):
            raise RuntimeError(f"record has no issue projection: {record!r}")
        selected: dict[str, object] = {
            "seq": record["seq"],
            "op": record["op"],
            "issueId": record["issue_id"],
            "actor": record["actor"],
            "title": issue["title"],
            "status": issue["status"],
        }
        if "close_reason" in issue:
            selected["closeReason"] = issue["close_reason"]
        projected.append(selected)
    return projected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bd", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--recording", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--keep-workspace", action="store_true")
    args = parser.parse_args()

    binary = args.bd.resolve()
    if not binary.is_file():
        parser.error(f"bd binary does not exist: {binary}")
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix="cardb20-beads-", dir=WORK_ROOT))
    env = os.environ.copy()
    env.update(
        {
            "BD_EVENTS_JOURNAL": "1",
            "BD_EVENTS_JOURNAL_AUTO_PRUNE": "0",
            "NO_COLOR": "1",
        }
    )
    commands: list[dict[str, object]] = []

    def recorded(argv: list[str]) -> bytes:
        stdout, stderr = run(argv, workspace, env)
        commands.append(
            {
                "argv": ["bd" if value == str(binary) else value for value in argv],
                "stdoutSha256": digest(stdout),
                "stderrSha256": digest(stderr),
            }
        )
        return stdout

    try:
        run(["git", "init", "-q"], workspace, env)
        recorded(
            [
                str(binary),
                "init",
                "--non-interactive",
                "--skip-hooks",
                "--skip-agents",
                "--prefix",
                "b20",
            ]
        )
        prefix = [str(binary), "--actor", "b20-recorder"]
        recorded(prefix + ["create", "--id", "b20-1", "--title", "journal anchor", "--silent"])
        recorded(prefix + ["update", "b20-1", "--title", "journal edited", "--json"])
        before_noop = recorded(prefix + ["events", "export", "--json"])
        recorded(prefix + ["update", "b20-1", "--title", "journal edited", "--json"])
        after_noop = recorded(prefix + ["events", "export", "--json"])
        recorded(prefix + ["close", "b20-1", "--reason", "complete", "--json"])
        final_raw = recorded(prefix + ["events", "export", "--json"])

        before_records = journal(before_noop)
        after_records = journal(after_noop)
        final_records = journal(final_raw)
        stable = projection(final_records)
        if [row["seq"] for row in stable] != [1, 2, 3]:
            raise RuntimeError(f"journal is not gapless: {stable!r}")
        if [row["op"] for row in stable] != ["create", "update", "close"]:
            raise RuntimeError(f"unexpected journal operations: {stable!r}")
        if len(before_records) != 2 or len(after_records) != 2:
            raise RuntimeError("same-value update changed the journal length")

        payload = {
            "schema": "plug.cardb20.beads-reference.v1",
            "recording": args.recording,
            "sourceCommit": args.source_commit,
            "recordedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "binarySha256": digest(binary.read_bytes()),
            "commands": commands,
            "observations": {
                "beforeNoOpRows": len(before_records),
                "afterNoOpRows": len(after_records),
                "finalRows": len(final_records),
                "rawFinalJournalSha256": digest(final_raw),
                "projectionSha256": digest(
                    json.dumps(stable, sort_keys=True, separators=(",", ":")).encode()
                ),
            },
            "projection": stable,
        }
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    finally:
        if not args.keep_workspace:
            shutil.rmtree(workspace)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
