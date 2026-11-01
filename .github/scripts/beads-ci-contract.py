#!/usr/bin/env python3
"""Validate and execute the beads PR-core gate from an immutable CI checkout."""

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tarfile
import xml.etree.ElementTree as ET


FOCUSED = [
    "//internal/plugdump:plugdump_test",
    "//cmd/bd:bd_test",
    "//scripts:scripts_test",
    "//internal/storage/dolt:dolt_test",
    "//tools/bazel:dolt_version_test",
]
PROFILE = [
    "--@rules_go//go/config:race",
    "--@rules_go//go/config:tags=gms_pure_go",
    "--lockfile_mode=error",
    "--test_arg=-test.short",
    "--test_arg=-test.parallel=4",
    "--test_arg=-test.skip=^TestEmbedded",
    "--test_env=BEADS_TEST_SKIP=dolt",
    "--test_env=BEADS_TEST_REQUIRE_EXCLUDE_PERMISSION=1",
    "--test_env=GO_TEST_WRAP_TESTV=1",
    "--test_tag_filters=-dolt-server,-dolt-server-proxied,-dolt-server-integration,-dolt-server-cmd,-embedded,-manual,-integration-only",
    "--run_under=//tools/bazel:test_env",
    "--nocache_test_results",
    "--test_output=errors",
    "--test_timeout=900",
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def immutable(value):
    require(re.fullmatch(r"[0-9a-f]{40}", value) is not None, "reference must be a full lowercase commit SHA")


def code(text):
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def carrier(harness_sha):
    immutable(harness_sha)
    return f"""name: Plug Bazel

on:
  pull_request:
    branches: [beads]

permissions:
  contents: read

concurrency:
  group: beads-bazel-${{{{ github.event.pull_request.number }}}}
  cancel-in-progress: true

jobs:
  bazel:
    uses: buildbetter-app/plugs/.github/workflows/plug-beads-bazel.yml@{harness_sha}
    with:
      harness_sha: {harness_sha}
"""


def check_carrier(text, harness_sha):
    # This deliberately small carrier has no extension points for another job,
    # trigger, permission or input. A change needs a reviewed contract update.
    require(code(text).strip() == carrier(harness_sha).strip(), "carrier must match the minimal immutable reviewed workflow contract")


def check_workflow(text):
    text = code(text)
    require(re.findall(r"(?m)^([a-z][a-z_-]*):", text) == ["name", "on", "permissions", "jobs"], "unexpected reusable workflow top-level key")
    require(re.search(r"(?m)^on:\n  workflow_call:\n", text), "validator must be reusable-only")
    require(not re.search(r"(?m)^  (?:push|pull_request|schedule|workflow_dispatch):", text), "validator must not register another trigger")
    require("pull_request_target" not in text and "secrets" not in text, "validator must not use privileged events or secrets")
    require(re.search(r"(?m)^permissions:\n  contents: read$", text), "validator permissions must be contents: read")
    require(not re.search(r":\s*(?:write|inherit)\s*$", text, re.M), "validator must not grant write/inherited permissions")
    actions = re.findall(r"(?m)^\s+uses: (\S+)", text)
    require(len(actions) == 4 and all(re.fullmatch(r"(?:actions|runs-on)/[a-z/-]+@[0-9a-f]{40}", x) for x in actions), "validator actions must retain immutable pins")
    for fragment in (
        "github.event.pull_request.head.repo.full_name == github.repository",
        "github.actor != 'dependabot[bot]'",
        "format('runs-on={0}/runner=bb-x64-16', github.run_id)",
        "'ubuntu-latest'", "if: env.RUNSON_TRUSTED == 'true'",
        "ref: ${{ github.event.pull_request.head.sha }}",
        "ref: ${{ inputs.harness_sha }}", "path: source", "path: ci-harness",
        '[[ "$GITHUB_EVENT_NAME" == pull_request ]]',
        '[[ "$BASE_REF" == beads ]]',
        "--nosystem_rc", "--nohome_rc", "--noworkspace_rc",
        "common --remote_cache=", "common --remote_executor=", "common --repo_contents_cache=",
        "BAZELISK_VERIFY_SHA256", "sha256sum --check --status",
        "ci-harness/.github/scripts/beads-ci-contract.py run",
        '--event-name "$GITHUB_EVENT_NAME"',
        "if-no-files-found: error", "if: ${{ always() }}",
    ):
        require(fragment in text, f"validator lost required contract: {fragment}")
    require(text.count("persist-credentials: false") == 2, "both checkouts must discard credentials")


def clean_env():
    names = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "BAZELISK_HOME", "BAZELISK_VERIFY_SHA256")
    env = {name: os.environ[name] for name in names if name in os.environ}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    return env


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], env=clean_env(), text=True).strip()


def checkpoint(source, sha):
    require(git(source, "rev-parse", "HEAD") == sha, "source HEAD changed")
    require(not git(source, "status", "--porcelain=v1", "--untracked-files=all"), "source checkout is dirty")


def log_snapshot(source):
    root = source / "bazel-testlogs"
    return {str(path): (path.stat().st_ino, path.stat().st_mtime_ns, path.stat().st_size)
            for path in root.rglob("*") if path.is_file() and path.name in {"test.log", "test.xml"}}


def archive_logs(source, destination, previous):
    root = source / "bazel-testlogs"
    require(root.is_dir(), "Bazel produced no test logs")
    xml_count = cases = errors = 0
    with tarfile.open(destination, "w:gz", dereference=True) as archive:
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.name not in {"test.log", "test.xml"}:
                continue
            stat = path.stat()
            if previous.get(str(path)) == (stat.st_ino, stat.st_mtime_ns, stat.st_size):
                continue
            archive.add(path, arcname=str(path.relative_to(root)), recursive=False)
            if path.name == "test.xml":
                xml_count += 1
                document = ET.fromstring(path.read_bytes())
                cases += sum(1 for item in document.iter("testcase") if item.find("skipped") is None)
                errors += sum(1 for item in document.iter() if item.tag in {"failure", "error"})
    require(xml_count > 0 and cases > 0, "Bazel produced no executed test cases")
    return {"xml_files": xml_count, "executed_cases": cases, "xml_errors": errors}


def run(args):
    source, evidence = Path(args.source).resolve(), Path(args.evidence).resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    startup = [str(Path(args.bazel).resolve()), "--nosystem_rc", "--nohome_rc", "--noworkspace_rc", "--bazelrc=" + str(Path(args.rc).resolve())]
    result = {"sha": args.expected_sha, "harness_sha": args.harness_sha, "profile": "beads-prcore", "phases": {}, "success": False}
    status = 0
    started = False
    try:
        immutable(args.expected_sha)
        immutable(args.harness_sha)
        require(not evidence.is_relative_to(source), "evidence must remain outside the source checkout")
        require(args.event_name == "pull_request", "only pull_request events may run this gate")
        event = json.loads(Path(args.event).read_text())
        pr = event["pull_request"]
        require(args.repository == "buildbetter-app/plugs", "unexpected repository")
        require(pr["base"]["ref"] == "beads" and pr["base"]["repo"]["full_name"] == args.repository, "event is not a beads PR in this repository")
        require(pr["head"]["sha"] == args.expected_sha, "event head does not match requested source")
        harness = Path(args.harness).resolve()
        checkpoint(harness, args.harness_sha)
        require(not harness.is_relative_to(source) and not source.is_relative_to(harness), "source and harness must be separate checkouts")
        check_carrier((source / ".github/workflows/plug-bazel.yml").read_text(), args.harness_sha)
        require(re.search(r"(?m)^module github\.com/steveyegge/beads\s*$", (source / "go.mod").read_text()), "source is not the beads module")
        require((source / ".bazelversion").read_text().strip() == "9.3.0", "unsupported source Bazel pin")
        checkpoint(source, args.expected_sha)
        tree = git(source, "rev-parse", "HEAD^{tree}")
        result.update(tree=tree, harness_tree=git(harness, "rev-parse", "HEAD^{tree}"), source_clean_before=True)
        for name, value in (("sha", args.expected_sha), ("tree", tree), ("harness-sha", args.harness_sha)):
            (evidence / (name + ".txt")).write_text(value + "\n")
        runtime = evidence.parent / (evidence.name + "-runtime")
        for directory in (runtime / "home", runtime / "tmp"):
            directory.mkdir(parents=True, exist_ok=True)
        execution_env = dict(clean_env(), HOME=str(runtime / "home"), TMPDIR=str(runtime / "tmp"), USE_BAZEL_VERSION="9.3.0")
        started = True
        for name, targets in (("focused", FOCUSED), ("whole", ["//..."])):
            checkpoint(source, args.expected_sha)
            previous = log_snapshot(source)
            phase = evidence / name
            phase.mkdir()
            command = [*startup, "test", *targets, *PROFILE]
            (phase / "command.txt").write_text(shlex.join(command) + "\n")
            with (phase / "bazel.log").open("w") as log:
                process = subprocess.Popen(command, cwd=source, env=execution_env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in process.stdout:
                    print(line, end="", flush=True)
                    log.write(line)
                rc = process.wait()
            (phase / "exit_code").write_text(str(rc) + "\n")
            phase_result = {"exit_code": rc, **archive_logs(source, phase / "testlogs.tgz", previous)}
            result["phases"][name] = phase_result
            if rc or phase_result["xml_errors"]:
                status = status or rc or 1
            checkpoint(source, args.expected_sha)
            checkpoint(harness, args.harness_sha)
        result["source_clean_after"] = True
        result["head_unchanged"] = True
        result["harness_clean_after"] = True
    except (ValueError, OSError, KeyError, ET.ParseError, subprocess.SubprocessError) as error:
        result["error"] = str(error)
        status = status or 1
    finally:
        if started:
            try:
                shutdown = subprocess.run([*startup, "shutdown"], cwd=source, env=execution_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                (evidence / "shutdown.log").write_text(shutdown.stdout)
                result["shutdown_exit_code"] = shutdown.returncode
                status = status or shutdown.returncode
                checkpoint(source, args.expected_sha)
                checkpoint(harness, args.harness_sha)
            except (ValueError, OSError, subprocess.SubprocessError) as error:
                result["shutdown_error"] = str(error)
                status = status or 1
        result["success"] = status == 0 and len(result["phases"]) == 2
        (evidence / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        (evidence / "exit_code").write_text(str(status) + "\n")
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    workflow = commands.add_parser("check-workflow")
    workflow.add_argument("path")
    carrier = commands.add_parser("check-carrier")
    carrier.add_argument("path")
    carrier.add_argument("--harness-sha", required=True)
    execute = commands.add_parser("run")
    for name in ("source", "evidence", "expected-sha", "harness-sha", "harness", "bazel", "rc", "event", "event-name", "repository"):
        execute.add_argument("--" + name, required=True)
    args = parser.parse_args()
    try:
        if args.command == "check-workflow":
            check_workflow(Path(args.path).read_text())
        elif args.command == "check-carrier":
            check_carrier(Path(args.path).read_text(), args.harness_sha)
        else:
            return run(args)
        return 0
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        print(f"beads-ci: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
