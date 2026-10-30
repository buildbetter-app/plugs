#!/usr/bin/env python3
"""Record stable B28 post-commit callback facts from the pinned reference."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


PIN = "9d6fff760b7a314b476539198504d77ce0959656"
SOURCE_PATHS = [
    "internal/hooks/hooks.go",
    "internal/hooks/hooks_unix.go",
    "internal/storage/hook_decorator.go",
    "internal/storage/hook_decorator_internal_test.go",
    "internal/storage/uow/notifying.go",
    "internal/storage/uow/notifying_test.go",
]
SOURCE_TESTS = [
    "internal/storage/uow/notifying_test.go:TestNotifyingUOWFiresNothingUntilCommit",
    "internal/storage/uow/notifying_test.go:TestNotifyingUOWRollbackFiresNothing",
    "internal/storage/uow/notifying_test.go:TestNotifyingUOWDropsAWorkThatCommitsNothing",
    "internal/storage/uow/notifying_test.go:TestNotifyingUOWOpToHookEventMapping",
    "internal/storage/hook_decorator_internal_test.go:TestHookFiringStoreUpdateIssueCheckedFiresOnSuccessOnly",
]
WORK_ROOT = Path("/tmp/plug-swarm/work")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def run(argv: list[str], cwd: Path, env: dict[str, str]) -> tuple[int, bytes, bytes]:
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, check=False)
    return result.returncode, result.stdout, result.stderr


def wait_for_lines(path: Path, count: int) -> list[str]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if path.exists():
            lines = path.read_text().splitlines()
            if len(lines) >= count:
                return lines
        time.sleep(0.02)
    return path.read_text().splitlines() if path.exists() else []


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--recording", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.source_commit != PIN:
        parser.error(f"source commit {args.source_commit} differs from the B28 pin")
    if not args.binary.is_file():
        parser.error("reference executable is missing")
    source_files = []
    for relative in SOURCE_PATHS:
        content = (args.source / relative).read_bytes()
        source_files.append({"path": relative, "sha256": digest(content)})

    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix="cardb28-reference-", dir=WORK_ROOT))
    env = os.environ.copy()
    env.update({"NO_COLOR": "1", "CI": "true", "BEADS_ACTOR": "card-b28-reference"})
    commands: list[dict[str, object]] = []

    def recorded(argv: list[str]) -> tuple[int, bytes, bytes]:
        code, stdout, stderr = run(argv, workspace, env)
        commands.append(
            {
                "argv": ["reference" if value == str(args.binary) else value for value in argv],
                "exitCode": code,
                "stdoutSha256": digest(stdout),
                "stderrSha256": digest(stderr),
            }
        )
        return code, stdout, stderr

    try:
        code, _, stderr = run(["git", "init", "-q"], workspace, env)
        if code != 0:
            raise RuntimeError(stderr.decode(errors="replace"))
        code, _, stderr = recorded(
            [str(args.binary), "init", "--non-interactive", "--skip-hooks", "--skip-agents", "--prefix", "b28", "--quiet"]
        )
        if code != 0:
            raise RuntimeError(stderr.decode(errors="replace"))
        callback_dir = workspace / ".beads" / "hooks"
        callback_dir.mkdir(parents=True, exist_ok=True)
        log = workspace / "callbacks.log"
        env["B28_REFERENCE_LOG"] = str(log)
        script = "#!/bin/sh\nprintf '%s|%s\\n' \"$2\" \"$1\" >> \"$B28_REFERENCE_LOG\"\n"
        for name in ("on_create", "on_update", "on_close"):
            path = callback_dir / name
            path.write_text(script)
            path.chmod(0o755)

        prefix = [str(args.binary), "--actor", "card-b28-reference"]
        code, _, stderr = recorded(prefix + ["create", "--id", "b28-1", "--title", "anchor", "--silent"])
        if code != 0 or wait_for_lines(log, 1) != ["create|b28-1"]:
            raise RuntimeError(f"create callback mismatch: {wait_for_lines(log, 1)!r} {stderr!r}")
        code, _, stderr = recorded(prefix + ["update", "b28-1", "--title", "edited", "--json"])
        if code != 0 or wait_for_lines(log, 2) != ["create|b28-1", "update|b28-1"]:
            raise RuntimeError(f"update callback mismatch: {wait_for_lines(log, 2)!r} {stderr!r}")

        before_refusal = list(wait_for_lines(log, 2))
        code, _, _ = recorded(prefix + ["update", "b28-1", "--title", "refused", "--expect-version", "0", "--json"])
        after_refusal = wait_for_lines(log, 2)
        if code == 0 or after_refusal != before_refusal:
            raise RuntimeError(f"guarded refusal callback mismatch: code={code}, lines={after_refusal!r}")

        code, _, stderr = recorded(prefix + ["close", "b28-1", "--reason", "complete", "--json"])
        final = wait_for_lines(log, 3)
        if code != 0 or final != ["create|b28-1", "update|b28-1", "close|b28-1"]:
            raise RuntimeError(f"close callback mismatch: {final!r} {stderr!r}")

        projection = [
            {"sequence": index + 1, "event": line.split("|", 1)[0], "issueId": line.split("|", 1)[1]}
            for index, line in enumerate(final)
        ]
        payload = {
            "schema": "plug.cardb28.reference.v1",
            "recording": args.recording,
            "sourceCommit": args.source_commit,
            "sourceFiles": source_files,
            "sourceTests": SOURCE_TESTS,
            "binarySha256": digest(args.binary.read_bytes()),
            "recorderSha256": digest(Path(__file__).read_bytes()),
            "commands": commands,
            "observations": {
                "callbacksBeforeGuardedRefusal": len(before_refusal),
                "callbacksAfterGuardedRefusal": len(after_refusal),
                "projectionSha256": digest(canonical(projection)),
            },
            "projection": projection,
            "limits": [
                "Selected stable event order and identity only; no timestamps retained.",
                "The guarded refusal is expected to fail and add no callback.",
                "Reference script callbacks are evidence, not Plug subscriber bindings.",
            ],
        }
        args.output.write_bytes(canonical(payload) + b"\n")
    finally:
        shutil.rmtree(workspace)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
