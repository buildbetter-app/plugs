#!/usr/bin/env bash
# Deterministic run-bound delivery and failure propagation; no real Bazel runs.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
python3 - "$repo_root" <<'PY'
import json
import hashlib
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
    (repo / "pkg/cmd/nested").mkdir(parents=True)
    (repo / "pkg/file.go").write_text("package pkg\n")
    (repo / "pkg/cmd/nested/main.go").write_text("package main\nfunc main() {}\n")
    (repo / "pkg/BUILD.bazel").write_text('go_library(name="pkg", srcs=["file.go"])\ngo_binary(name="nested", srcs=["cmd/nested/main.go"])\n')
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
import re
import sys
root = Path(__file__).parent
config = json.loads((root / "control.json").read_text())
args = sys.argv[1:]
assert not any(k in os.environ for k in ("GH_TOKEN", "DATABASE_URL", "ACTIONS_RUNTIME_TOKEN"))
assert "PLUG_PUBLIC_BEADS_EXTRA" not in os.environ
with (root / "calls.jsonl").open("a") as out:
    out.write(json.dumps(args) + "\n")
# Bazel accepts the standalone launcher --version form only before startup
# flags; the collector must use the actual version subcommand after them.
assert "--version" not in args
command = next(x for x in args if not x.startswith("--"))
execution = root / "execution"
cp7 = ("//internal/integration/cp7latency:cp7_correctness_test",
       "//internal/integration/cp7latency:cp7_latency_sqlite_test",
       "//internal/integration/cp7latency:cp7_latency_postgres_test") if config.get("cp7") else ()
public_target = ("//tools/roundtrip:public_beads_runtime_test",) if config.get("public_runtime") else ()
ordinary_policy = ("//pkg:manual_policy_test",) if config.get("manual_candidates") else ()
count_capture = "//spec/s1-b21-b24:ordinary_count_capture_test"
manual_targets = (count_capture, "//pkg:manual_capture_test") if config.get("manual_candidates") else ()
tags = {count_capture: ["local", "manual"], "//pkg:manual_capture_test": ["manual", "local"],
        "//tools/roundtrip:public_beads_runtime_test": ["local", "manual"],
        "//pkg:manual_policy_test": ["manual-policy"]}
test_labels = ("//pkg:pkg_test", "//consumer:consumer_test", "//libraryconsumer:library_test", "//tools/lexicon:lexicon_gate", *ordinary_policy, *cp7, *public_target)
if command == "version":
    assert "--gnu_format" in args
    if not config.get("direct_bazel"):
        print("Bazelisk v1.29.0")
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
    scope, pattern = expression, None
    boundary = ') except (attr("tags", '
    if expression.startswith("(") and boundary in expression:
        scope, tail = expression[1:].split(boundary, 1)
        pattern, end = json.JSONDecoder().raw_decode(tail)
        assert tail[end:] == ', ' + scope + ') except set(//tools/roundtrip:public_beads_runtime_test))', expression
    if scope == 'kind("source file", deps(//...))':
        result = ["//pkg:file.go", "//pkg:cmd/nested/main.go"]
    elif '.*_library rule' in scope:
        # A directory without a BUILD is not a Bazel package. Reproduce the
        # real owning-libraries failure if the selector invents one.
        assert scope == 'kind(".*_library rule", set(//pkg:all))', scope
        result = ["//pkg:pkg"]
    elif 'set(//pkg:pkg)' in scope:
        result = ["//libraryconsumer:library_test", *manual_targets, *public_target]
    elif 'rdeps(' in scope:
        result = ["//consumer:consumer_test", *manual_targets, *public_target]
    elif 'tests(set(' in scope:
        result = test_labels
    elif scope == 'kind(".*_test rule", //...)':
        result = ["//pkg:pkg_test", "//consumer:consumer_test", *ordinary_policy, *manual_targets, *public_target, *cp7]
    else:
        result = ["//pkg:pkg_test", *ordinary_policy, *manual_targets]
    if pattern is not None:
        matcher = re.compile(pattern)
        result = [label for label in result if label == "//tools/roundtrip:public_beads_runtime_test"
                  or not matcher.search("[" + ", ".join(tags.get(label, [])) + "]")]
    print("\n".join(result))
elif command == "test":
    assert "--test_env=GO_TEST_WRAP_TESTV=1" in args
    assert [arg for arg in args if arg.startswith("--test_timeout=")] == ["--test_timeout=900,900,900,9000"], args
    assert not any(arg.startswith(("--test_tag_filters", "--test_size_filters", "--test_timeout_filters", "--test_filter", "--build_tag_filters")) for arg in args), args
    if cp7:
        assert set(cp7).issubset(args), args
    if config.get("manual_candidates"):
        assert not set(manual_targets).intersection(args), args
        assert set(ordinary_policy).issubset(args), args
    if public_target:
        assert set(public_target).issubset(args), args
    public_args = [arg for arg in args if arg.startswith("--test_env=PLUG_PUBLIC_BEADS_")]
    assert public_args == config.get("public_env", []), (public_args, config.get("public_env", []))
    labels = test_labels
    events = [{"id": {"configuration": {"id": "fixture"}}, "configuration": {"cpu": "k8", "platformName": "host"}}]
    for i, label in enumerate(labels):
        logs = execution / "bazel-out/k8-fastbuild/testlogs" / str(i)
        logs.mkdir(parents=True, exist_ok=True)
        (logs / "test.log").write_text("fixture test output " + label)
        case = '<testcase name="skip"><skipped/></testcase>' if i == 0 else '<testcase name="pass"/>'
        if config.get("test_exit") and i == 1:
            case = '<testcase name="fail"><failure message="expected fixture failure"/></testcase>'
        xml = '<testsuite tests="1">' + case + '</testsuite>'
        if config.get("empty_xml") and i == 0:
            xml = '<testsuites></testsuites>'
        (logs / "test.xml").write_text(xml)
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
               DATABASE_URL="fixture-dsn-marker", ACTIONS_RUNTIME_TOKEN="fixture-capability-marker",
               PLUG_PUBLIC_BEADS_EXTRA="fixture-extra-marker")
    collector = repo / "scripts/ci/plug-bazel-delivery.py"

    def phase(name, out, expected=0, *extra):
        result = subprocess.run(["/usr/bin/python3", str(collector), name, "--output", str(out), *extra],
                                cwd=repo, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert result.returncode == expected, (name, result.returncode, expected, result.stdout.decode())

    public_descriptor = "41398730774:13120521766:66de7c070d365bbd7fc2bd39ed822e5bf72c21efabe0add9a5e26d07bf170a62"
    public_names = ("PLUG_PUBLIC_BEADS_BINARY", "PLUG_PUBLIC_BEADS_SHA256",
                    "PLUG_PUBLIC_BEADS_BUILD_RUN", "PLUG_PUBLIC_BEADS_MANIFEST")

    def exercise(name, config, run_exit=0, finish_exit=0, corrupt_state=False, public_mode=None):
        out = fixture / name
        original_public = {key: env.get(key) for key in public_names}
        (fixture / "control.json").write_text(json.dumps(config))
        (fixture / "calls.jsonl").write_text("")
        try:
            phase("begin", out, 0, "--base", base, "--head", head)
            phase("selection", out, config.get("selection_exit", config.get("query_exit", 0)))
            if public_mode in {"valid", "malformed"}:
                runtime = fixture / (name + "-runtime")
                (runtime / "bin").mkdir(parents=True)
                binary = runtime / "bin/bd-export"
                binary.write_bytes(b"fixture public exporter\n")
                binary.chmod(0o755)
                manifest = runtime / "manifest.json"
                manifest.write_text("{}\n")
                values = {
                    "PLUG_PUBLIC_BEADS_BINARY": str(binary),
                    "PLUG_PUBLIC_BEADS_SHA256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                    "PLUG_PUBLIC_BEADS_BUILD_RUN": "41398730774",
                    "PLUG_PUBLIC_BEADS_MANIFEST": str(manifest),
                }
                (out / "public-bundle.json").write_text(json.dumps({
                    "status": "PASS", "descriptor": public_descriptor,
                    "manifest_sha256": public_descriptor.rsplit(":", 1)[1],
                    "environment": values,
                }) + "\n")
                env.update(values)
                config["public_env"] = [f"--test_env={key}={values[key]}" for key in public_names]
                if public_mode == "malformed":
                    env["PLUG_PUBLIC_BEADS_SHA256"] = "0" * 64
            elif public_mode == "stray":
                env.update({key: "fixture-stray-value" for key in public_names})
            (fixture / "control.json").write_text(json.dumps(config))
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
                                   (b"fixture-secret-marker", b"fixture-dsn-marker", b"fixture-capability-marker",
                                    b"fixture-extra-marker"))
            return out, result
        finally:
            for key, value in original_public.items():
                if value is None:
                    env.pop(key, None)
                else:
                    env[key] = value

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
               "--jobs=2", "--test_timeout=900,900,900,9000", "--local_resources=memory=8192", "--test_env=GO_TEST_WRAP_TESTV=1"))
    (out / "tests.json").write_text("tampered")
    phase("verify", out, 86)

    exercise("direct-bazel", {"direct_bazel": True})
    out, result = exercise("query-failure", {"query_exit": 37, "selection_exit": 37}, 37, 37)
    assert result["first_failure"]["stage"] == "target-resolution"
    assert list((out / "selection/queries").glob("*/exit.txt"))[0].read_text().strip() == "37"
    assert json.loads((out / "tests.json").read_text())["state"] == "not-run"
    out, result = exercise("test-failure", {"test_exit": 3}, 3, 3)
    assert result["first_failure"]["stage"] == "tests"
    tests = json.loads((out / "tests.json").read_text())
    assert tests["accounting"]["target_status_counts"]["FAILED"] == 1
    assert sum(x["xml"]["failures"] for x in tests["attempts"]) == 1
    exercise("collector-failure", {"missing_xml": True}, 0, 86)
    exercise("empty-xml", {"empty_xml": True}, 0, 86)
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
    phase("selection", out)
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
    phase("selection", out)
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

    def assert_automatic_manual_boundary(out, prepared_runtime):
        count = "//spec/s1-b21-b24:ordinary_count_capture_test"
        manual = "//pkg:manual_capture_test"
        runtime = "//tools/roundtrip:public_beads_runtime_test"
        ordinary = {"//pkg:pkg_test", "//pkg:manual_policy_test",
                    "//consumer:consumer_test", "//libraryconsumer:library_test"}
        selected = set((out / "selection/command-targets.txt").read_text().splitlines())
        assert selected == {"//:lexicon_gate", *ordinary, *({runtime} if prepared_runtime else set())}, selected
        resolved = set((out / "selection/resolved-test-targets.txt").read_text().splitlines())
        assert resolved == {"//tools/lexicon:lexicon_gate", *ordinary, *({runtime} if prepared_runtime else set())}, resolved
        for name in ("touched-targets", "reverse-targets", "owning-reverse-targets"):
            assert not {count, manual}.intersection((out / ("selection/" + name + ".txt")).read_text().splitlines()), name
        argv = json.loads((out / "commands/tests/argv.json").read_text())
        assert selected.issubset(argv) and not {count, manual}.intersection(argv), argv

    # Source/package, source reverse and delivery's owning-library closure all
    # contain manual candidates. A reference env claim cannot opt them in.
    env["PLUG_PUBLIC_REFERENCE"] = "caller-claim"
    try:
        out, result = exercise("manual-source-ordinary", {"manual_candidates": True})
        assert_automatic_manual_boundary(out, False)
    finally:
        env.pop("PLUG_PUBLIC_REFERENCE")
    out, result = exercise("manual-source-prepared", {"manual_candidates": True, "public_runtime": True}, public_mode="valid")
    assert_automatic_manual_boundary(out, True)
    out, result = exercise("manual-source-unprepared-runtime", {"manual_candidates": True, "public_runtime": True}, 86, 86)
    assert result["first_failure"]["stage"] == "collector-run"
    assert not (out / "commands/tests").exists()
    out, result = exercise("manual-source-mismatched-runtime", {"manual_candidates": True, "public_runtime": True}, 86, 86, public_mode="malformed")
    assert result["first_failure"]["stage"] == "collector-run"
    assert not (out / "commands/tests").exists()

    # Keep the package-only delivery/failure cases above and separately cover
    # the graph-plus-nested-source path that failed in the hosted workflow.
    base = head
    (repo / "pkg/cmd/nested/main.go").write_text("package main\nfunc main() {}\n// changed\n")
    with (repo / "pkg/BUILD.bazel").open("a") as out:
        out.write("# graph change with nested source\n")
    git("add", ".")
    git("commit", "-qm", "nested-source-graph-change")
    head = git("rev-parse", "HEAD")
    out, result = exercise("nested-graph-pass", {})
    assert (out / "selection/packages.txt").read_text().splitlines() == ["//pkg"]
    assert (out / "selection/full-graph.txt").read_text().strip() == "1"
    assert (out / "selection/seeds.txt").read_text().splitlines() == ["//pkg:cmd/nested/main.go"]

    # Model the new harness BUILD change through the real full-graph resolver.
    # The ordinary delivery command must retain both eternal latency targets
    # and default-class correctness without manual/size/tag/timeout filters.
    out, result = exercise("cp7-graph-pass", {"cp7": True})
    cp7 = {"//internal/integration/cp7latency:" + name for name in
           ("cp7_correctness_test", "cp7_latency_sqlite_test", "cp7_latency_postgres_test")}
    selected = set((out / "selection/command-targets.txt").read_text().splitlines())
    assert selected == {"//:lexicon_gate", "//pkg:pkg_test", "//consumer:consumer_test", "//libraryconsumer:library_test", *cp7}
    assert set((out / "selection/resolved-test-targets.txt").read_text().splitlines()) == {
        "//tools/lexicon:lexicon_gate", "//pkg:pkg_test", "//consumer:consumer_test", "//libraryconsumer:library_test", *cp7}
    tests = json.loads((out / "tests.json").read_text())
    assert not tests["unreported_tests"] and tests["accounting"]["executed_attempts"] == 6
    assert cp7.issubset({attempt["identity"]["label"] for attempt in tests["attempts"] if not attempt["cached"]})

    public_config = {"public_runtime": True}
    out, result = exercise("public-runtime", public_config, public_mode="valid")
    public_args = [arg for arg in json.loads((out / "commands/tests/argv.json").read_text())
                   if arg.startswith("--test_env=PLUG_PUBLIC_BEADS_")]
    assert public_args == public_config["public_env"]
    out, result = exercise("public-missing", {"public_runtime": True}, 86, 86, public_mode="missing")
    assert result["first_failure"]["stage"] == "collector-run"
    out, result = exercise("public-malformed", {"public_runtime": True}, 86, 86, public_mode="malformed")
    assert result["first_failure"]["stage"] == "collector-run"
    out, result = exercise("public-stray", {}, 86, 86, public_mode="stray")
    assert result["first_failure"]["stage"] == "collector-run"

    # BUILD/fullgraph selection must not reintroduce the unprepared count
    # capture, and must retain the deliberately prepared manual runtime.
    out, result = exercise("manual-graph-ordinary", {"manual_candidates": True})
    assert_automatic_manual_boundary(out, False)
    out, result = exercise("manual-graph-prepared", {"manual_candidates": True, "public_runtime": True}, public_mode="valid")
    assert_automatic_manual_boundary(out, True)

print("plug-ci-delivery: manual preparation boundary, ordinary coverage, prepared runtime, bundle/accounting/refusal and CP7 timeout-map fixtures passed")
PY
