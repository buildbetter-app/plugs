#!/usr/bin/env bash
# Deterministic run-bound delivery and failure propagation; no real Bazel runs.
set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
python3 - "$repo_root" <<'PY'
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

source = Path(sys.argv[1])
with tempfile.TemporaryDirectory(prefix="plug-bazel-delivery-test.") as temporary:
    fixture = Path(temporary)
    repo = fixture / "repo"
    repo.mkdir()
    env = {"PATH": "/usr/bin:/bin", "HOME": str(fixture), "LANG": "C.UTF-8"}

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=repo, env=env, stderr=subprocess.DEVNULL).decode().strip()

    git("init", "-q")
    git("config", "user.name", "Plug CI test")
    git("config", "user.email", "plug-ci-test@example.invalid")
    (repo / "scripts/ci").mkdir(parents=True)
    for name in ("plug-bazel-delivery.py", "plug-bazel-targets.sh"):
        shutil.copyfile(source / "scripts/ci" / name, repo / "scripts/ci" / name)
    (repo / "pkg").mkdir()
    (repo / "pkg/file.go").write_text("package pkg\n")
    (repo / "MODULE.bazel.lock").write_text("{}\n")
    (repo / ".bazelversion").write_text("9.3.0\n")
    git("add", ".")
    git("commit", "-qm", "base")
    base = git("rev-parse", "HEAD")
    (repo / "pkg/file.go").write_text("package pkg\n// changed\n")
    git("add", ".")
    git("commit", "-qm", "change")
    head = git("rev-parse", "HEAD")

    # Only the fake sees this fixture control file. The production collector
    # intentionally does not forward arbitrary environment variables to Bazel.
    fake = fixture / "bazel"
    fake.write_text(r'''#!/usr/bin/python3
import json
import os
from pathlib import Path
import sys
root = Path(__file__).parent
config = json.loads((root / "control.json").read_text())
args = sys.argv[1:]
assert not any(k in os.environ for k in ("GH_TOKEN", "DATABASE_URL", "ACTIONS_RUNTIME_TOKEN"))
with (root / "calls.jsonl").open("a") as out:
    out.write(json.dumps(args) + "\n")
command = "--version" if "--version" in args else next(x for x in args if not x.startswith("--"))
execution = root / "execution"
if command == "--version" or "--version" in args:
    print("bazel 9.3.0")
elif command == "info":
    print(execution)
elif command == "run":
    print("go version go1.26.10 linux/amd64")
elif command == "query":
    if config.get("query_exit"):
        print("query failed", file=sys.stderr)
        sys.exit(config["query_exit"])
    expression = args[args.index("query") + 1]
    if '.*_library rule' in expression:
        print("//pkg:pkg")
    elif 'set(//pkg:pkg)' in expression:
        print("//libraryconsumer:library_test")
    elif 'rdeps(' in expression:
        print("//consumer:consumer_test")
    elif 'tests(set(' in expression:
        print("\n".join(("//pkg:pkg_test", "//consumer:consumer_test", "//libraryconsumer:library_test", "//tools/lexicon:lexicon_gate")))
    else:
        print("//pkg:pkg_test")
elif command == "test":
    labels = ("//pkg:pkg_test", "//consumer:consumer_test", "//libraryconsumer:library_test", "//tools/lexicon:lexicon_gate")
    events = [{"id": {"configuration": {"id": "fixture"}}, "configuration": {"cpu": "k8", "platformName": "host"}}]
    for i, label in enumerate(labels):
        logs = execution / "bazel-out/k8-fastbuild/testlogs" / str(i)
        logs.mkdir(parents=True, exist_ok=True)
        (logs / "test.log").write_text("fixture test output " + label)
        case = '<testcase name="skip"><skipped/></testcase>' if i == 0 else '<testcase name="pass"/>'
        if config.get("test_exit") and i == 1:
            case = '<testcase name="fail"><failure message="expected fixture failure"/></testcase>'
        (logs / "test.xml").write_text('<testsuite tests="1">' + case + '</testsuite>')
        outputs = [{"name": name, "uri": (logs / name).as_uri()} for name in ("test.log", "test.xml")]
        if config.get("missing_xml") and i == 0:
            outputs.pop()
        if config.get("escape") and i == 0:
            outputs[0]["uri"] = (root / "outside.log").as_uri()
        status = "FAILED" if config.get("test_exit") and i == 1 else "PASSED"
        events.append({"id": {"testResult": {"label": label, "run": 1, "shard": 0, "attempt": 1}}, "testResult": {"status": status, "cachedLocally": i == 2, "testActionOutput": outputs}})
        events.append({"id": {"testSummary": {"label": label}}, "testSummary": {"overallStatus": status, "totalRunCount": 1}})
    events.append({"finished": {"exitCode": {"code": config.get("test_exit", 0)}}})
    events.append({"lastMessage": True, "buildMetrics": {"actionSummary": {"actionsExecuted": 3}}})
    bep = Path(next(x.split("=", 1)[1] for x in args if x.startswith("--build_event_json_file=")))
    if config.get("malformed_bep"):
        events[0] = None
    if config.get("incomplete_bep"):
        events.pop()
    bep.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    sys.exit(config.get("test_exit", 0))
elif command == "shutdown":
    sys.exit(config.get("shutdown_exit", 0))
else:
    raise AssertionError(args)
''')
    fake.chmod(0o755)
    # Marker values are fake and must never reach a child process or artifacts.
    env.update(BAZEL_BIN=str(fake), GH_TOKEN="fixture-secret-marker",
               DATABASE_URL="fixture-dsn-marker", ACTIONS_RUNTIME_TOKEN="fixture-capability-marker")
    collector = repo / "scripts/ci/plug-bazel-delivery.py"

    def phase(name, out, expected=0, *extra):
        result = subprocess.run(["/usr/bin/python3", str(collector), name, "--output", str(out), *extra],
                                cwd=repo, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert result.returncode == expected, (name, result.returncode, expected, result.stdout.decode())

    def exercise(name, config, run_exit=0, finish_exit=0, corrupt_state=False):
        out = fixture / name
        (fixture / "control.json").write_text(json.dumps(config))
        (fixture / "calls.jsonl").write_text("")
        phase("begin", out, 0, "--base", base, "--head", head)
        phase("run", out, run_exit)
        if corrupt_state:
            (out / "state.json").write_text("broken state\n")
        phase("finish", out, finish_exit)
        phase("verify", out)
        result = json.loads((out / "result.json").read_text())
        assert result["exit"] == finish_exit
        assert result["result"] == ("PASS" if finish_exit == 0 else "FAIL")
        assert result["shutdown"]["exit"] == config.get("shutdown_exit", 0)
        assert result["post"]["head"] == head and result["post"]["clean"]
        assert any("shutdown" in json.loads(line) for line in (fixture / "calls.jsonl").read_text().splitlines())
        for path in out.rglob("*"):
            if path.is_file():
                assert not any(marker in path.read_bytes() for marker in
                               (b"fixture-secret-marker", b"fixture-dsn-marker", b"fixture-capability-marker"))
        return out, result

    out, result = exercise("pass", {})
    assert result["source_unchanged"] and result["pre"]["tree"] == result["post"]["tree"]
    assert json.loads((out / "preflight.json").read_text())["tools"]["nix"]["state"] == "unavailable"
    assert set((out / "selection/command-targets.txt").read_text().splitlines()) == {
        "//:lexicon_gate", "//pkg:pkg_test", "//consumer:consumer_test", "//libraryconsumer:library_test"}
    accounting = json.loads((out / "tests.json").read_text())["accounting"]
    assert accounting["executed_attempts"] == 3 and accounting["cached_attempts"] == 1
    assert accounting["executed_xml_skips"] == 1
    assert len(list((out / "tests").glob("*/test.xml"))) == 4
    args = json.loads((out / "commands/tests/argv.json").read_text())
    assert all(flag in args for flag in ("--nocache_test_results", "--lockfile_mode=error",
               "--jobs=2", "--test_timeout=900", "--local_resources=memory=8192"))
    (out / "tests.json").write_text("tampered")
    phase("verify", out, 86)

    out, result = exercise("query-failure", {"query_exit": 37}, 37, 37)
    assert result["first_failure"]["stage"] == "target-resolution"
    assert list((out / "selection/queries").glob("*/exit.txt"))[0].read_text().strip() == "37"
    assert json.loads((out / "tests.json").read_text())["state"] == "not-run"
    out, result = exercise("test-failure", {"test_exit": 3}, 3, 3)
    assert result["first_failure"]["stage"] == "tests"
    tests = json.loads((out / "tests.json").read_text())
    assert tests["accounting"]["target_status_counts"]["FAILED"] == 1
    assert sum(x["xml"]["failures"] for x in tests["attempts"]) == 1
    exercise("collector-failure", {"missing_xml": True}, 0, 86)
    exercise("malformed-bep", {"malformed_bep": True}, 0, 86)
    exercise("incomplete-bep", {"incomplete_bep": True}, 0, 86)
    exercise("escaped-log", {"escape": True}, 0, 86)
    exercise("shutdown-failure", {"shutdown_exit": 42}, 0, 42)
    out, result = exercise("test-collector-shutdown", {"test_exit": 3, "missing_xml": True, "shutdown_exit": 42}, 3, 3)
    assert {x["stage"] for x in result["errors"]} >= {"tests", "collector", "shutdown"}
    out, result = exercise("query-state-shutdown", {"query_exit": 37, "shutdown_exit": 42}, 37, 37, True)
    assert {x["stage"] for x in result["errors"]} >= {"collector-state", "shutdown", "post-source"}

    # A failure while sealing must invalidate a previous PASS and still stop
    # Bazel. This leaves a deliberately unsealed, uploadable partial payload.
    (fixture / "control.json").write_text("{}")
    out = fixture / "seal-failure"
    phase("begin", out, 0, "--base", base, "--head", head)
    phase("run", out)
    (out / "bad-link").symlink_to(fixture / "control.json")
    phase("finish", out, 86)
    result = json.loads((out / "result.json").read_text())
    assert result["result"] == "FAIL" and result["shutdown"]["exit"] == 0
    assert not (out / "manifest.sha256").exists()
    phase("verify", out, 86)

    # Setup failure before a run still yields a failed sealed bundle/shutdown.
    out = fixture / "setup-failure"
    phase("begin", out, 0, "--base", base, "--head", head)
    phase("finish", out, 1, "--job-status", "failure")
    phase("verify", out)
    assert json.loads((out / "result.json").read_text())["first_failure"]["stage"] == "upstream-workflow"
    # Changed tracked bytes and dirty state are captured and rejected even
    # though the immutable commit SHA itself did not change.
    out = fixture / "source-mutation"
    phase("begin", out, 0, "--base", base, "--head", head)
    phase("run", out)
    original = (repo / "pkg/file.go").read_text()
    (repo / "pkg/file.go").write_text(original + "// unexpected mutation\n")
    phase("finish", out, 86)
    phase("verify", out)
    result = json.loads((out / "result.json").read_text())
    assert not result["source_unchanged"] and not result["post"]["clean"]
    assert (out / "post-status.txt").read_text().strip()
    (repo / "pkg/file.go").write_text(original)
    phase("begin", repo / "inside-source", 2)
    assert not (repo / "inside-source").exists()

print("plug-ci-delivery: bundle, accounting, secrets exclusion, tamper/escape, query/test/collector/shutdown first-failure and cleanup fixtures passed")
PY
