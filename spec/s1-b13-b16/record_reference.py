#!/usr/bin/env python3
"""Record two fresh B13-B16 Beads runs from the pinned public source.

This runner never supplies an authored expected projection.  It refuses to
write an artifact unless the source tree and executable digest were supplied
and verified, then stores the raw command receipts and the values returned by
the executable.  Run it on RunsOn with disposable workspaces only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


PINNED_COMMIT = "9d6fff760b7a314b476539198504d77ce0959656"
PINNED_TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def verify_source(source: Path) -> tuple[str, str]:
    if not source.is_dir():
        raise RuntimeError(f"reference source is missing: {source}")
    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()

    commit = git("rev-parse", "HEAD")
    tree = git("rev-parse", "HEAD^{tree}")
    if commit != PINNED_COMMIT or tree != PINNED_TREE:
        raise RuntimeError(f"reference source is {commit}/{tree}, expected {PINNED_COMMIT}/{PINNED_TREE}")
    if git("status", "--porcelain"):
        raise RuntimeError("reference source is not an exact clean pinned tree")
    return commit, tree


def invoke(binary: Path, cwd: Path, env: dict[str, str], evidence: Path, label: str, args: list[str], commands: list[dict[str, object]]) -> bytes:
    result = subprocess.run([str(binary), *args], cwd=cwd, env=env, capture_output=True, check=False, timeout=180)
    (evidence / f"{label}.stdout").write_bytes(result.stdout)
    (evidence / f"{label}.stderr").write_bytes(result.stderr)
    commands.append({
        "label": label,
        "argv": ["reference" if value == str(binary) else value for value in [str(binary), *args]],
        "exitCode": result.returncode,
        "stdoutSha256": digest(result.stdout),
        "stderrSha256": digest(result.stderr),
    })
    if result.returncode != 0:
        raise RuntimeError(f"reference command {label} failed with exit {result.returncode}")
    return result.stdout


def parse_json(stdout: bytes, label: str) -> object:
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"reference command {label} did not return JSON") from exc


def one_run(binary: Path, root: Path, number: int, env: dict[str, str]) -> dict[str, object]:
    workspace = root / f"run-{number}" / "store"
    evidence = root / f"run-{number}" / "evidence"
    workspace.mkdir(parents=True)
    evidence.mkdir()
    commands: list[dict[str, object]] = []

    subprocess.run(["git", "init", "-q"], cwd=workspace, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    init_args = ["init", "--non-interactive", "--skip-hooks", "--skip-agents", "--prefix", "s1", "--quiet"]
    invoke(binary, workspace, env, evidence, "init", init_args, commands)

    explicit = invoke(binary, workspace, env, evidence, "b13-explicit", ["remember", "--key", "s1-explicit", "Alpha body", "--json"], commands)
    derived = invoke(binary, workspace, env, evidence, "b13-derived", ["remember", "Always run tests with -race flag and preserve exact body", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b13-recall", ["recall", "s1-explicit", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b13-list", ["memories", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b13-forget", ["forget", "s1-explicit", "--json"], commands)

    invoke(binary, workspace, env, evidence, "b14-subject", ["create", "s1 gate subject", "--id", "s1-gate-subject", "--json"], commands)
    gate = parse_json(invoke(binary, workspace, env, evidence, "b14-create", ["gate", "create", "--type", "human", "--blocks", "s1-gate-subject", "--json"], commands), "b14-create")
    gate_id = gate.get("id") if isinstance(gate, dict) else None
    if not isinstance(gate_id, str) or not gate_id:
        raise RuntimeError("reference gate create did not return an id")
    invoke(binary, workspace, env, evidence, "b14-resolve", ["gate", "resolve", gate_id, "--json"], commands)
    invoke(binary, workspace, env, evidence, "b14-show", ["gate", "show", gate_id, "--json"], commands)

    invoke(binary, workspace, env, evidence, "b15-create", ["create", "s1 comment anchor", "--id", "s1-comment-anchor", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b15-comment-one", ["comment", "s1-comment-anchor", "  literal body  ", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b15-comment-two", ["comment", "s1-comment-anchor", "second comment", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b15-list", ["comments", "s1-comment-anchor", "--json"], commands)

    invoke(binary, workspace, env, evidence, "b16-create", ["create", "s1 label anchor", "--id", "s1-label-anchor", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b16-add", ["label", "add", "s1-label-anchor", "alpha,beta", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b16-remove", ["label", "remove", "s1-label-anchor", "beta", "--json"], commands)
    invoke(binary, workspace, env, evidence, "b16-show", ["show", "s1-label-anchor", "--json"], commands)

    projection = {
        "explicit": parse_json(explicit, "b13-explicit"),
        "derived": parse_json(derived, "b13-derived"),
        "gateId": gate_id,
        "commands": [{"label": item["label"], "exitCode": item["exitCode"]} for item in commands],
    }
    (evidence / "commands.json").write_bytes(canonical(commands) + b"\n")
    return {
        "workspace": str(workspace),
        "evidence": str(evidence),
        "commandsSha256": digest((evidence / "commands.json").read_bytes()),
        "projection": projection,
        "projectionSha256": digest(canonical(projection)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-binary", type=Path, required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.workspace.exists():
        raise SystemExit(f"refusing to reuse workspace: {args.workspace}")
    binary = args.reference_binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise SystemExit(f"reference executable is missing or not executable: {binary}")
    actual_binary = digest(binary.read_bytes())
    if actual_binary != args.binary_sha256:
        raise SystemExit(f"reference executable digest {actual_binary} differs from supplied verified digest")
    source_commit, source_tree = verify_source(args.source.resolve())
    args.workspace.mkdir(parents=True, mode=0o700)
    try:
        env = {key: value for key, value in os.environ.items() if key in ("PATH", "TMPDIR")}
        env.update({
            "HOME": str(args.workspace / "home"),
            "XDG_CONFIG_HOME": str(args.workspace / "home" / "config"),
            "CI": "true",
            "DO_NOT_TRACK": "1",
            "BEADS_NO_DAEMON": "1",
            "BEADS_ACTOR": "s1-reference-recorder",
            "GIT_AUTHOR_NAME": "s1-reference-recorder",
            "GIT_AUTHOR_EMAIL": "s1-reference@example.invalid",
            "GIT_COMMITTER_NAME": "s1-reference-recorder",
            "GIT_COMMITTER_EMAIL": "s1-reference@example.invalid",
            "LC_ALL": "C",
            "TZ": "UTC",
        })
        Path(env["HOME"]).mkdir(parents=True)
        runs = [one_run(binary, args.workspace, number, env) for number in (1, 2)]
        if runs[0]["projection"] != runs[1]["projection"]:
            raise SystemExit("independent reference projections differ")
        record = {
            "schema": "plug.s1-b13-b16.reference.v1",
            "cards": ["B13", "B14", "B15", "B16"],
            "sourceCommit": source_commit,
            "sourceTree": source_tree,
            "binarySha256": actual_binary,
            "recorderSha256": digest(Path(__file__).read_bytes()),
            "recordings": runs,
            "limits": [
                "Two fresh embedded Beads stores; no shared or production store is touched.",
                "The recorded values are executable stdout, not authored expected projections.",
                "No full-card, Postgres, staging, reverse-import, or B1 qualification is implied.",
                "Any unavailable command or producer aborts before an artifact is written.",
            ],
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(canonical(record) + b"\n")
        print(json.dumps({"recordings": len(runs), "output": str(args.output)}))
        return 0
    finally:
        shutil.rmtree(args.workspace, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
