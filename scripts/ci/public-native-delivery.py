#!/usr/bin/env python3
"""Finite native artifact addressing and direct-command binding primitives.

Only artifact-env is a CLI entry point. The dispatch/collection functions are
for a separately authorized controller unit AFTER its exact live source/ref
and absent-lease guards. This module is not a producer authorization or a
replacement for those guards. No list lookup can manufacture an acknowledgement.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

REPOSITORY = "buildbetter-app/plugs"
WORKFLOW_ID = 403922586
PROFILE = "plug-public-native/1"
ACTOR = "nikhil-s-buildbetter-remote-dev[bot]"
GH_APP = "/tmp/plug-swarm/bin/gh-app"
PAUSE = Path("/tmp/plug-swarm/PAUSE")
OLD_RUN = 41480350151
OLD_ARTIFACTS = {13158698976, 13158584449}


class Stop(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise Stop(message)


def native_names(label: str) -> dict[str, str]:
    require(isinstance(label, str) and re.fullmatch(r"[A-Za-z0-9._-]{1,48}", label) is not None,
            "invalid literal native label")
    return {"bundle": "plug-public-bundle-" + label,
            "verification": "plug-public-bundle-verification-" + label}


def exact_query(name: str) -> list[str]:
    # A literal documented name equality filter, never a prefix/glob query.
    return [GH_APP, "api", "--include", "--method", "GET",
            "repos/" + REPOSITORY + "/actions/artifacts",
            "-H", "Accept: application/vnd.github+json",
            "-H", "X-GitHub-Api-Version: 2022-11-28",
            "-f", "name=" + name, "-F", "per_page=100", "-F", "page=1"]


def strict_json(payload: bytes) -> object:
    """Reject ambiguous members in every object before consuming any field."""
    def unique_members(pairs: list[tuple[str, object]]) -> dict:
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON object member in API reply")
            result[key] = value
        return result

    def invalid_constant(value: str) -> object:
        raise Stop("non-standard JSON constant in API reply")

    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=unique_members,
                          parse_constant=invalid_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise Stop("malformed API JSON reply") from error


def one_response(raw: bytes) -> tuple[dict[str, list[str]], object]:
    # Preserve raw first. Parsing permits normal HTTP CRLF or CLI LF headers.
    head, separator, body = raw.replace(b"\r\n", b"\n").partition(b"\n\n")
    require(bool(separator), "missing raw HTTP headers/body separator")
    lines = head.decode("ascii").splitlines()
    require(bool(lines) and re.fullmatch(r"HTTP/[0-9.]+ 200(?: .*)?", lines[0]) is not None,
            "metadata response is not HTTP 200")
    headers: dict[str, list[str]] = {}
    for line in lines[1:]:
        key, separator, value = line.partition(":")
        require(bool(separator), "ambiguous HTTP header")
        headers.setdefault(key.lower(), []).append(value.strip())
    return headers, strict_json(body)


def exact_page(raw: bytes, name: str, expected_count: int) -> list[dict]:
    headers, body = one_response(raw)
    require("link" not in headers, "not a complete single-response result; no pagination allowed")
    require(isinstance(body, dict), "artifact result is not an object")
    count, records = body.get("total_count"), body.get("artifacts")
    require(type(count) is int and 0 <= count <= 100 and isinstance(records, list),
            "missing/invalid/beyond-page artifact count")
    require(len(records) == count, "truncated or inconsistent single-response result")
    require(all(isinstance(item, dict) and item.get("name") == name for item in records),
            "name filter response includes an ambiguous/out-of-scope record")
    require(count == expected_count, "native address collision, absence or ambiguity")
    return records


def write_new(path: Path, payload: bytes) -> None:
    require(not PAUSE.exists(), "PAUSE before evidence mutation")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def recorded_call(directory: Path, stem: str, argv: list[str], timeout: int) -> dict:
    """Same argv object goes to Popen and to lossless evidence; never shell=True."""
    require(not PAUSE.exists(), "PAUSE before command")
    argv_bytes = b"\0".join(os.fsencode(arg) for arg in argv) + b"\0"
    write_new(directory / (stem + ".argv.nul"), argv_bytes)
    write_new(directory / (stem + ".argv.json"), canonical(argv))
    started = datetime.now(timezone.utc).isoformat()
    write_new(directory / (stem + ".started.json"), canonical({"started_at": started}))
    env = os.environ.copy()
    # gh-app mints a command-scoped App token. Never record the environment.
    env.pop("GH_DEBUG", None)
    env.pop("GH_FORCE_TTY", None)
    env.update({"NO_COLOR": "1", "CLICOLOR": "0", "GH_PAGER": "cat"})
    require(not PAUSE.exists(), "PAUSE immediately before command")
    try:
        process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    except OSError as error:
        write_new(directory / (stem + ".spawn-error.json"),
                  canonical({"type": type(error).__name__, "errno": error.errno}))
        raise Stop("command spawn failed; dispatch budget remains consumed") from error
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        process.kill()
        stdout, stderr = process.communicate()
    ended = datetime.now(timezone.utc).isoformat()
    write_new(directory / (stem + ".stdout"), stdout)
    write_new(directory / (stem + ".stderr"), stderr)
    receipt = {"argv_sha256": hashlib.sha256(argv_bytes).hexdigest(),
               "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
               "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
               "started_at": started, "ended_at": ended,
               "exit_code": process.returncode, "timed_out": timed_out,
               "identity_requirement": ACTOR}
    payload = canonical(receipt)
    write_new(directory / (stem + ".receipt.json"), payload)
    write_new(directory / (stem + ".receipt.sha256"),
              (hashlib.sha256(payload).hexdigest() + "\n").encode())
    require(not timed_out and process.returncode == 0, "command failed/timed out; STOP, no retry")
    return {"receipt": receipt, "stdout": stdout, "stderr": stderr}


def collect_exact(directory: Path, phase: str, label: str, expected_count: int) -> dict[str, list[dict]]:
    # Exactly two independent equality requests, one response each, no retries.
    result = {}
    for role, name in native_names(label).items():
        raw = recorded_call(directory, phase + "-" + role, exact_query(name), 120)
        result[role] = exact_page(raw["stdout"], name, expected_count)
    return result


def direct_ack(stdout: bytes, stderr: bytes) -> tuple[int, str]:
    # Local gh 2.95.0 help promises a URL only "if available". Do not infer one.
    urls = re.findall(rb"https?://[^\s<>\"']+", stdout + b"\n" + stderr)
    require(len(urls) == 1 and urls[0] in stdout,
            "missing/ambiguous actual dispatch URL; no run lookup or fallback")
    match = re.fullmatch(rb"https://github\.com/buildbetter-app/plugs/actions/runs/([1-9][0-9]*)", urls[0])
    require(match is not None, "dispatch did not directly acknowledge an exact repository run URL")
    run_id = int(match[1])
    require(run_id != OLD_RUN, "historical blocked run is not a successor")
    return run_id, urls[0].decode("ascii")


def dispatch_once(directory: Path, branch: str, label: str) -> tuple[int, str, dict]:
    """Call only after the separately released unit's complete live guards.

    Creating dispatch.spent consumes this unit's one dispatch irreversibly,
    including spawn failure, timeout or missing acknowledgement. Never retry.
    """
    native_names(label)
    require(branch == "codex/b1c-native-producer-d70c-20261102" and label == "b1c-native-d70c-20261102",
            "ref/label differs from this prospective contract")
    # Reserve before any per-attempt file: retries fail specifically at the
    # exclusive spent marker even after spawn/command/acknowledgement failure.
    write_new(directory / "dispatch.spent", b"one dispatch consumed; never remove or reuse\n")
    write_new(directory / "dispatch.inputs.json", canonical({"ref": branch,
              "inputs": {"profile": PROFILE, "label": label}}))
    argv = [GH_APP, "workflow", "run", str(WORKFLOW_ID), "--repo", REPOSITORY,
            "--ref", branch, "-f", "profile=" + PROFILE, "-f", "label=" + label]
    result = recorded_call(directory, "dispatch", argv, 120)
    run_id, url = direct_ack(result["stdout"], result["stderr"])
    return run_id, url, result["receipt"]


def positive_id(value: object, description: str) -> int:
    # bool, float, string, missing and null must never become equal identities.
    require(type(value) is int and value > 0, description + " is not a positive integer ID")
    return value


def repository_ids(run: dict) -> tuple[int, int]:
    require(isinstance(run, dict), "run reply is not an object")
    identities = []
    for key in ("repository", "head_repository"):
        repository = run.get(key)
        require(isinstance(repository, dict) and repository.get("full_name") == REPOSITORY,
                "repository identity mismatch")
        identities.append(positive_id(repository.get("id"), key + ".id"))
    # Both named objects are the same repository, not a fork or unknown origin.
    require(identities[0] == identities[1], "repository/head_repository IDs disagree")
    return identities[0], identities[1]


def bind_run(run: dict, run_id: int, control_sha: str, branch: str, *, terminal: bool) -> None:
    repository_ids(run)
    require(positive_id(run.get("id"), "run.id") == positive_id(run_id, "acknowledged run") and
            run_id != OLD_RUN, "run does not equal actual direct acknowledgement")
    positive_id(run.get("workflow_id"), "run.workflow_id")
    positive_id(run.get("run_attempt"), "run.run_attempt")
    expected = {"workflow_id": WORKFLOW_ID, "head_sha": control_sha,
                "head_branch": branch, "run_attempt": 1, "event": "workflow_dispatch",
                "html_url": f"https://github.com/{REPOSITORY}/actions/runs/{run_id}"}
    require(all(run.get(key) == value for key, value in expected.items()), "run identity mismatch")
    require(run.get("actor", {}).get("login") == ACTOR and
            run.get("triggering_actor", {}).get("login") == ACTOR, "App identity mismatch")
    if terminal:
        require(run.get("status") == "completed" and run.get("conclusion") == "success",
                "producer is not completed/success; retain first failure and STOP")


def bind_artifacts(pages: dict[str, list[dict]], run: dict, label: str) -> dict[str, dict]:
    # Revalidate here too; an unchecked run must not compare null == null.
    repository_id, head_repository_id = repository_ids(run)
    run_id = positive_id(run.get("id"), "run.id")
    selected: dict[str, dict] = {}
    for role, name in native_names(label).items():
        records = pages.get(role)
        require(isinstance(records, list) and len(records) == 1, "native address is not singular")
        item = records[0]
        require(isinstance(item, dict), "artifact record is not an object")
        require(item.get("name") == name and type(item.get("id")) is int and
                item["id"] > 0 and item["id"] not in OLD_ARTIFACTS, "artifact identity mismatch")
        require(item.get("expired") is False and type(item.get("size_in_bytes")) is int and
                item["size_in_bytes"] > 0 and
                isinstance(item.get("digest"), str) and
                re.fullmatch(r"sha256:[0-9a-f]{64}", item["digest"]) is not None,
                "artifact has no usable official byte binding")
        origin = item.get("workflow_run")
        require(isinstance(origin, dict), "artifact has no workflow_run object")
        origin_id = positive_id(origin.get("id"), "artifact.workflow_run.id")
        origin_repository_id = positive_id(origin.get("repository_id"), "artifact.repository_id")
        origin_head_repository_id = positive_id(origin.get("head_repository_id"), "artifact.head_repository_id")
        require(origin_id == run_id and origin.get("head_sha") == run["head_sha"] and
                origin.get("head_branch") == run["head_branch"] and
                origin_repository_id == repository_id and
                origin_head_repository_id == head_repository_id,
                "artifact belongs to another run/control/repository")
        selected[role] = item
    require(len({item["id"] for item in selected.values()}) == 2, "artifact IDs are not distinct")
    return selected


def verify_official_zip(item: dict, payload: bytes) -> None:
    require(len(payload) == item["size_in_bytes"] and
            "sha256:" + hashlib.sha256(payload).hexdigest() == item["digest"],
            "official ZIP size/digest mismatch")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    names = sub.add_parser("artifact-env")
    names.add_argument("--label", required=True)
    args = parser.parse_args()
    values = native_names(args.label)
    print("BUNDLE_ARTIFACT_NAME=" + values["bundle"])
    print("VERIFICATION_ARTIFACT_NAME=" + values["verification"])


if __name__ == "__main__":
    main()
