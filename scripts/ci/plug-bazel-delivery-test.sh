#!/usr/bin/env bash
# Deterministic run-bound delivery and failure propagation; no real Bazel runs.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
python3 - "$repo_root" <<'PY'
import json
import hashlib
import ast
from pathlib import Path
import shutil
import shlex
from urllib.parse import unquote, urlparse
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
    # Synthetic provenance for this fixture only. Patch only the copied
    # collector before its fixture commit; never claim official bytes or pins.
    public_payload = b"fixture public exporter\n"
    public_manifest = {
        "schema": "plug-public-companion/1", "build": {"run_id": "123"},
        "workflow": {"sha": "a" * 40},
        "source": {"beads": {"commit": "1" * 40}, "plug": {"commit": "2" * 40}},
        "targets": {"beads_exporter": {"label": "//cmd/bd:bd", "binary": "bin/bd-export"}},
        "artifacts": {"bin/bd-export": {"mode": "0o755", "size": len(public_payload),
                                       "sha256": hashlib.sha256(public_payload).hexdigest()}},
        "archive": {"name": "bundle.tar", "size": 10240, "sha256": "b" * 64},
    }
    public_manifest_bytes = (json.dumps(public_manifest, sort_keys=True) + "\n").encode()
    public_descriptor = "123:456:" + hashlib.sha256(public_manifest_bytes).hexdigest()
    (repo / "scripts/ci").mkdir(parents=True)
    for name in ("plug-bazel-delivery.py", "plug-bazel-targets.sh"):
        shutil.copyfile(source / "scripts/ci" / name, repo / "scripts/ci" / name)
    fixture_collector = repo / "scripts/ci/plug-bazel-delivery.py"
    fixture_source = fixture_collector.read_text()
    assignment = next(node for node in ast.parse(fixture_source).body if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == "PUBLIC_BUNDLE_DESCRIPTOR" for target in node.targets))
    source_lines = fixture_source.splitlines(keepends=True)
    source_lines[assignment.lineno - 1:assignment.end_lineno] = ["PUBLIC_BUNDLE_DESCRIPTOR = " + repr(public_descriptor) + "\n"]
    fixture_collector.write_text("".join(source_lines))
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
import io
import json
import os
import zipfile
from pathlib import Path
import re
import sys
root = Path(__file__).parent
config = json.loads((root / "control.json").read_text())
args = sys.argv[1:]
assert not any(k in os.environ for k in ("GH_TOKEN", "DATABASE_URL", "ACTIONS_RUNTIME_TOKEN"))
assert "PLUG_PUBLIC_BEADS_EXTRA" not in os.environ
# Exercise Capture.command's real child environment and the trusted wrapper.
# These values are synthetic; existing credential exclusions stay mandatory.
expected_binding = {
    "BAZEL_BIN": str(root / "bazel-wrapper"), "BAZEL_RC": str(root / "fixture.bazelrc"),
    "GITHUB_RUN_ID": "123456", "GITHUB_RUN_ATTEMPT": "2",
    "GITHUB_SHA": "4" * 40,
    "GITHUB_WORKFLOW_REF": "buildbetter-app/plugs/.github/workflows/plug-bazel.yml@refs/heads/main",
}
assert {key: os.environ.get(key) for key in expected_binding} == expected_binding
assert args[:4] == ["--nosystem_rc", "--nohome_rc", "--noworkspace_rc",
                    "--bazelrc=" + expected_binding["BAZEL_RC"]], args
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
public_target = (("//tools/roundtrip:public_beads_runtime_test",) if config.get("public_runtime") else ()) + (
    ("//spec/s1-b25-b28:public_recording_test",) if config.get("public_recording") else ())
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
    assert [arg for arg in args if arg.startswith("--flaky_test_attempts=")] == ["--flaky_test_attempts=1"], args
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
    if config.get("output_attempts"):
        events.append({"id": {"configuration": {"id": "other-fixture"}}, "configuration": {"cpu": "k8", "platformName": "host"}})
    for i, label in enumerate(labels):
        identity = {"label": label, "run": 1, "shard": 0, "attempt": 1}
        if config.get("action_outputs"):
            identity["configuration"] = {"id": "fixture"}
        identities = [identity]
        if config.get("output_attempts") and i == 0:
            identities += [{**identity, "attempt": 2}, {**identity, "run": 2, "shard": 1},
                           {**identity, "configuration": {"id": "other-fixture"}}]
        for identity in identities:
            logs = execution / "bazel-out/k8-fastbuild/testlogs" / str(i)
            if config.get("action_outputs"):
                logs = logs / identity["configuration"]["id"] / ("run-%s-shard-%s-attempt-%s" % (identity["run"], identity["shard"], identity["attempt"]))
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
            if config.get("action_outputs"):
                identity_bytes = json.dumps(identity, sort_keys=True).encode()
                raw = io.BytesIO()
                with zipfile.ZipFile(raw, "w") as archive:
                    archive.writestr("source-bytes.bin", identity_bytes + b"\x00\xff\r\n")
                    archive.writestr("target-bytes.bin", identity_bytes + b"\xff\x00\n")
                extras = (
                    ("test.outputs", "test.outputs/outputs.zip", raw.getvalue()),
                    ("test.outputs_manifest", "test.outputs_manifest/MANIFEST", identity_bytes + b"\x00\xff\n"),
                    ("test.outputs/nested/.opaque.bin", "test.outputs/nested/.opaque.bin", b"\x00\xff\r\n" + identity_bytes),
                    ("test.outputs", "test.outputs/second/outputs.zip", b"not a valid ZIP\x00\xff\n" + identity_bytes),
                    ("../../logical-only", "test.outputs/metadata-name.bin", b"name is metadata\n" + identity_bytes),
                )
                for name, relative, data in extras:
                    payload = logs / relative
                    payload.parent.mkdir(parents=True, exist_ok=True)
                    payload.write_bytes(data)
                    outputs.append({"name": name, "uri": payload.as_uri(), "length": str(len(data)),
                                    "pathPrefix": ["preserved", "BEP", "metadata"]})
                if i == 0 and identity == identities[0]:
                    fault = config.get("output_fault")
                    payload = logs / "test.outputs/outputs.zip"
                    if fault in {"escaped-source", "symlink-escape"}:
                        outside = root / "outside-output.bin"
                        outside.write_bytes(b"outside testlogs: must not be copied\n")
                        if fault == "escaped-source":
                            outputs[2]["uri"] = outside.as_uri()
                        else:
                            payload.unlink()
                            payload.symlink_to(outside)
                    elif fault == "missing":
                        payload.unlink()
                    elif fault == "nonlocal":
                        outputs[2]["uri"] = "https://invalid.example/test-output"
                    elif fault == "nonregular":
                        payload.unlink()
                        payload.mkdir()
                    elif fault == "duplicate-required":
                        outputs.append(dict(outputs[0]))
                    elif fault == "required-name-mismatch":
                        wrong = logs / "other.log"
                        wrong.write_bytes(b"wrong required basename\n")
                        outputs[0]["uri"] = wrong.as_uri()
                    elif fault == "missing-xml-alias":
                        outputs = [item for item in outputs if item["name"] != "test.xml"]
                        alias = logs / "test.outputs/nested/test.xml"
                        alias.write_text(xml)
                        outputs.append({"name": "test.outputs/nested/test.xml", "uri": alias.as_uri()})
                    elif fault == "malformed-xml":
                        (logs / "test.xml").write_bytes(b"not XML\x00\xff\n")
            status = "FAILED" if config.get("test_exit") and i == 1 else "PASSED"
            events.append({"id": {"testResult": identity}, "testResult": {"status": status, "cachedLocally": i == 2, "testActionOutput": outputs}})
        events.append({"id": {"testSummary": {"label": label}}, "testSummary": {"overallStatus": status, "totalRunCount": len(identities)}})
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
    (fixture / "fixture.bazelrc").write_text("# synthetic trusted runner rc\n")
    wrapper = fixture / "bazel-wrapper"
    wrapper.write_text("#!/bin/sh\nset -eu\nexec " + shlex.quote(str(fake))
                       + " --nosystem_rc --nohome_rc --noworkspace_rc --bazelrc="
                       + shlex.quote(str(fixture / "fixture.bazelrc")) + ' "$@"\n')
    wrapper.chmod(0o755)
    env.update(BAZEL_RC=str(fixture / "fixture.bazelrc"),
               GITHUB_RUN_ID="123456", GITHUB_RUN_ATTEMPT="2", GITHUB_SHA="4" * 40,
               GITHUB_WORKFLOW_REF="buildbetter-app/plugs/.github/workflows/plug-bazel.yml@refs/heads/main")
    # Marker values are fake and must never reach a child process or artifacts.
    env.update(BAZEL_BIN=str(wrapper), GH_TOKEN="fixture-secret-marker",
               DATABASE_URL="fixture-dsn-marker", ACTIONS_RUNTIME_TOKEN="fixture-capability-marker",
               PLUG_PUBLIC_BEADS_EXTRA="fixture-extra-marker")
    collector = repo / "scripts/ci/plug-bazel-delivery.py"

    def phase(name, out, expected=0, *extra, copy_fault=None):
        argv = ["/usr/bin/python3", str(collector), name, "--output", str(out), *extra]
        if copy_fault is not None:
            # Controlled I/O failures exercise the real finish/main path. No
            # production option or environment-controlled bypass is added.
            inject = r'''import os, runpy, sys
from pathlib import Path
fault, script = sys.argv[1:3]
original_open, original_stat, original_resolve = Path.open, Path.stat, Path.resolve
original_descriptor_open = os.open
fired = False
def open_file(self, mode="r", *args, **kwargs):
    global fired
    if fault == "error" and not fired and self.name == "payload" and mode == "xb":
        fired = True
        raise OSError("fixture copy error")
    return original_open(self, mode, *args, **kwargs)
def stat_file(self, *args, **kwargs):
    global fired
    value = original_stat(self, *args, **kwargs)
    if fault == "corrupt" and not fired and self.name == "payload":
        fired = True
        with original_open(self, "ab") as stream:
            stream.write(b"\x00changed")
        value = original_stat(self, *args, **kwargs)
    return value
def resolve_file(self, *args, **kwargs):
    global fired
    admitted = original_resolve(self, *args, **kwargs)
    if fault == "replace" and not fired and self.name == "outputs.zip":
        fired = True
        outside = Path(script).parents[3] / "replacement-sentinel.bin"
        outside.write_bytes(b"OUTSIDE-REPLACEMENT-SENTINEL-DO-NOT-RETAIN\x00\xff")
        self.unlink()
        self.symlink_to(outside)
    return admitted
def descriptor_open(path, flags, *args, **kwargs):
    global fired
    descriptor = original_descriptor_open(path, flags, *args, **kwargs)
    if fault == "replace-admitted" and not fired and path == "outputs.zip":
        fired = True
        outside = Path(script).parents[3] / "replacement-sentinel.bin"
        outside.write_bytes(b"OUTSIDE-REPLACEMENT-SENTINEL-DO-NOT-RETAIN\x00\xff")
        reference = outside.with_name("admitted-output-before-replacement.bin")
        reference.write_bytes(os.pread(descriptor, os.fstat(descriptor).st_size, 0))
        os.unlink(path, dir_fd=kwargs["dir_fd"])
        os.symlink(str(outside), path, dir_fd=kwargs["dir_fd"])
    return descriptor
Path.open, Path.stat, Path.resolve, os.open = open_file, stat_file, resolve_file, descriptor_open
sys.argv = sys.argv[2:]
runpy.run_path(script, run_name="__main__")
'''
            argv = ["/usr/bin/python3", "-c", inject, copy_fault, *argv[1:]]
        result = subprocess.run(argv, cwd=repo, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert result.returncode == expected, (name, result.returncode, expected, result.stdout.decode())
        return result.stdout.decode().strip()

    public_names = ("PLUG_PUBLIC_BEADS_BINARY", "PLUG_PUBLIC_BEADS_SHA256",
                    "PLUG_PUBLIC_BEADS_BUILD_RUN", "PLUG_PUBLIC_BEADS_MANIFEST")

    def exercise(name, config, run_exit=0, finish_exit=0, corrupt_state=False, public_mode=None):
        # Each fake execution owns fresh temporary files; fault-created symlinks
        # and directories must not contaminate the next independent control.
        execution = fixture / "execution"
        if execution.exists():
            shutil.rmtree(execution)
        out = fixture / name
        original_public = {key: env.get(key) for key in public_names}
        (fixture / "control.json").write_text(json.dumps(config))
        (fixture / "calls.jsonl").write_text("")
        try:
            phase("begin", out, 0, "--base", base, "--head", head)
            phase("selection", out, config.get("selection_exit", config.get("query_exit", 0)))
            if not config.get("selection_exit", config.get("query_exit", 0)):
                selected_public = phase("public-selection", out)
                assert selected_public == ("true" if config.get("public_runtime") or config.get("public_recording") else "false")
            if public_mode and public_mode not in {"missing", "stray"}:
                runtime = fixture / (name + "-runtime")
                (runtime / "bin").mkdir(parents=True)
                binary = runtime / "bin/bd-export"
                binary.write_bytes(public_payload)
                binary.chmod(0o755)
                manifest = runtime / "manifest.json"
                manifest.write_bytes(public_manifest_bytes)
                values = {
                    "PLUG_PUBLIC_BEADS_BINARY": str(binary),
                    "PLUG_PUBLIC_BEADS_SHA256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                    "PLUG_PUBLIC_BEADS_BUILD_RUN": "123",
                    "PLUG_PUBLIC_BEADS_MANIFEST": str(manifest),
                }
                receipt = {
                    "status": "PASS", "descriptor": public_descriptor,
                    "manifest_sha256": public_descriptor.rsplit(":", 1)[1],
                    "producer_run": "123", "artifact_id": "456", "producer_workflow_sha": "a" * 40,
                    "artifact_digest": "sha256:" + "c" * 64,
                    **{field: public_manifest[field] for field in ("schema", "source", "targets", "artifacts", "archive")},
                    "environment": dict(values),
                }
                env.update(values)
                config["public_env"] = [f"--test_env={key}={values[key]}" for key in public_names]
                if public_mode == "malformed":
                    env["PLUG_PUBLIC_BEADS_SHA256"] = "0" * 64
                elif public_mode.startswith("missing-value:"):
                    env.pop(public_mode.split(":", 1)[1], None)
                elif public_mode == "manifest-bytes":
                    manifest.write_bytes(public_manifest_bytes + b" ")
                elif public_mode == "binary-mode":
                    binary.chmod(0o644)
                elif public_mode == "binary-link":
                    binary.unlink()
                    binary.symlink_to(manifest)
                elif public_mode == "receipt-run":
                    receipt["producer_run"] = "999"
                elif public_mode == "receipt-artifact":
                    receipt["artifact_id"] = "999"
                elif public_mode == "receipt-workflow":
                    receipt["producer_workflow_sha"] = "b" * 40
                elif public_mode == "receipt-schema":
                    receipt["schema"] = "plug-public-native/1"
                elif public_mode == "receipt-digest":
                    receipt["artifact_digest"] = "invalid"
                elif public_mode == "receipt-descriptor":
                    receipt["descriptor"] = "999:456:" + public_descriptor.rsplit(":", 1)[1]
                elif public_mode == "receipt-status":
                    receipt["status"] = "WAIT"
                elif public_mode == "receipt-environment":
                    receipt["environment"]["PLUG_PUBLIC_BEADS_EXTRA"] = "untrusted"
                elif public_mode == "receipt-source":
                    receipt["source"] = {}
                elif public_mode == "receipt-archive":
                    receipt["archive"] = {}
                if public_mode != "receipt-missing":
                    (out / "public-bundle.json").write_text(json.dumps(receipt) + "\n")
            elif public_mode == "stray":
                env.update({key: "fixture-stray-value" for key in public_names})
            (fixture / "control.json").write_text(json.dumps(config))
            phase("run", out, run_exit)
            if corrupt_state:
                (out / "state.json").write_text("broken state\n")
            phase("finish", out, finish_exit, copy_fault=config.get("copy_fault"))
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
    assert all(flag in args for flag in ("--nocache_test_results", "--lockfile_mode=error", "--flaky_test_attempts=1",
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

    def inspect_action_outputs(out, complete, admitted=None):
        events = [json.loads(line) for line in (out / "raw.bep.json").read_text().splitlines()]
        expected = {hashlib.sha256(json.dumps(event["id"]["testResult"], sort_keys=True).encode()).hexdigest(): event
                    for event in events if "testResult" in event}
        manifest = json.loads((out / "manifest.json").read_text())["files"]
        receipts = sorted((out / "tests").glob("*/test-action-outputs.json"))
        assert receipts and (not complete or len(receipts) == len(expected))
        observed = []
        for path in receipts:
            inventory = json.loads(path.read_text())
            event = expected[path.parent.name]
            assert inventory["identity"] == event["id"]["testResult"]
            records = inventory["outputs"]
            assert [record["bep"] for record in records] == event["testResult"]["testActionOutput"]
            assert [record["index"] for record in records] == list(range(len(records)))
            for record in records:
                assert record["state"] in {"copied", "error"}, record
                if record["state"] == "error":
                    assert record["error"] and "sha256" not in record and "size" not in record
                    if "path" in record:
                        assert not (out / record["path"]).exists(), record
                    continue
                target = out / record["path"]
                assert target.resolve().is_relative_to(path.parent)
                selected = Path(unquote(urlparse(record["bep"]["uri"]).path))
                data = target.read_bytes()
                if admitted is not None and record["bep"]["uri"] == admitted[0]:
                    assert record["source"] == str(selected) and selected.is_symlink()
                    assert data == admitted[1], record
                else:
                    source = selected.resolve()
                    assert record["source"] == str(source)
                    assert data == source.read_bytes(), record
                assert record["sha256"] == hashlib.sha256(data).hexdigest()
                assert record["size"] == len(data)
                assert manifest[record["path"]] == {"sha256": record["sha256"], "size": len(data)}
                if record["bep"]["name"] not in {"test.log", "test.xml"}:
                    assert target.name == "payload" and not any(part.startswith(".") for part in target.relative_to(out).parts)
            observed.append(inventory)
        if complete:
            tests = json.loads((out / "tests.json").read_text())
            assert {attempt["output_inventory"] for attempt in tests["attempts"]} == {
                str(path.relative_to(out)) for path in receipts}
            assert all(record["state"] == "copied" for inventory in observed for record in inventory["outputs"])
        return observed

    # Opaque bytes and duplicate logical container names retain their original
    # identities through the real CLI phases, for passed and failed processes.
    for case, test_exit in (("action-outputs-pass", 0), ("action-outputs-failed-tests", 3)):
        out, result = exercise(case, {"action_outputs": True, "output_attempts": True, "test_exit": test_exit}, test_exit, test_exit)
        inventories = inspect_action_outputs(out, True)
        assert len(inventories) == 7
        assert len({json.dumps(row["identity"], sort_keys=True) for row in inventories}) == 7
        tests = json.loads((out / "tests.json").read_text())
        assert tests["accounting"]["executed_attempts"] == 6 and tests["accounting"]["cached_attempts"] == 1
        assert tests["accounting"]["executed_xml_skips"] == 4
        assert sum(row["xml"]["failures"] for row in tests["attempts"]) == int(bool(test_exit))
        assert result["hosted_delivery"] == "unverified"
        if test_exit:
            assert result["first_failure"]["stage"] == "tests"
        else:
            # Independent hash and size tampering must invalidate the seal.
            payload = out / inventories[0]["outputs"][2]["path"]
            original = payload.read_bytes()
            payload.write_bytes(bytes([original[0] ^ 1]) + original[1:])
            phase("verify", out, 86)
            payload.write_bytes(original + b"\x00")
            phase("verify", out, 86)
        print("hosted-output-oracle: " + case + " PASS")

    faults = ("missing", "escaped-source", "symlink-escape", "nonlocal", "nonregular",
              "duplicate-required", "required-name-mismatch", "missing-xml-alias", "malformed-xml")
    for fault in faults:
        out, result = exercise("action-output-" + fault, {"action_outputs": True, "output_fault": fault}, 0, 86)
        inventories = inspect_action_outputs(out, False)
        assert (out / "tests-incomplete.json").is_file() and not (out / "tests.json").exists()
        assert result["first_failure"]["stage"] == "collector"
        # A rejected/missing output must not prevent the later local opaque
        # payloads for this attempt from being retained and hash-bound.
        assert sum(record["state"] == "copied" and record["bep"]["name"] not in {"test.log", "test.xml"}
                   for record in inventories[0]["outputs"]) >= 4
        assert not any(path.name == "outside-output.bin" for path in out.rglob("*"))
        print("hosted-output-oracle: " + fault + " PASS")

    for fault in ("error", "corrupt"):
        out, result = exercise("action-output-copy-" + fault, {"action_outputs": True, "copy_fault": fault}, 0, 86)
        inventories = inspect_action_outputs(out, False)
        assert inventories[0]["outputs"][2]["state"] == "error"
        if fault == "corrupt":
            assert inventories[0]["outputs"][2]["partial_removed"]
        assert all(row["state"] == "copied" for row in inventories[0]["outputs"][3:])
        assert (out / "tests-incomplete.json").is_file() and result["first_failure"]["stage"] == "collector"
        print("hosted-output-oracle: copy-" + fault + " PASS")

    # A deterministic temporary-file replacement happens after the selected
    # source resolution, before the collector can open it. No outside bytes
    # may appear even in a correctly sealed incomplete/failing bundle.
    for test_exit in (0, 3):
        out, result = exercise("action-output-replacement-" + str(test_exit),
                               {"action_outputs": True, "copy_fault": "replace", "test_exit": test_exit, "shutdown_exit": 42},
                               test_exit, test_exit or 86)
        inventories = inspect_action_outputs(out, False)
        assert inventories[0]["outputs"][2]["state"] == "error"
        assert all(row["state"] == "copied" for row in inventories[0]["outputs"][3:])
        assert (out / "tests-incomplete.json").is_file() and not (out / "tests.json").exists()
        sentinel = b"OUTSIDE-REPLACEMENT-SENTINEL-DO-NOT-RETAIN\x00\xff"
        assert all(sentinel not in path.read_bytes() for path in out.rglob("*") if path.is_file())
        assert result["first_failure"]["stage"] == ("tests" if test_exit else "collector")
        assert {row["stage"] for row in result["errors"]} >= {"collector", "shutdown", *({"tests"} if test_exit else set())}
    # Once admitted, the original descriptor remains the only byte source even
    # when that pathname is replaced. Check against bytes saved from the open
    # fixture inode, with the replacement sentinel absent from the whole bundle.
    for test_exit in (0, 3):
        out, result = exercise("action-output-admitted-" + str(test_exit),
                               {"action_outputs": True, "copy_fault": "replace-admitted", "test_exit": test_exit,
                                "shutdown_exit": 42 if test_exit else 0}, test_exit, test_exit)
        events = [json.loads(line) for line in (out / "raw.bep.json").read_text().splitlines()]
        first = next(event for event in events if "testResult" in event)
        replaced = first["testResult"]["testActionOutput"][2]["uri"]
        original = (fixture / "admitted-output-before-replacement.bin").read_bytes()
        inventories = inspect_action_outputs(out, True, (replaced, original))
        assert original and not (out / "tests-incomplete.json").exists()
        assert not any(row["stage"] == "collector" for row in result["errors"])
        sentinel = b"OUTSIDE-REPLACEMENT-SENTINEL-DO-NOT-RETAIN\x00\xff"
        assert all(sentinel not in path.read_bytes() for path in out.rglob("*") if path.is_file())
        if test_exit:
            assert result["first_failure"]["stage"] == "tests"
            assert {row["stage"] for row in result["errors"]} >= {"tests", "shutdown"}
    print("hosted-output-oracle: source-replacement PASS")

    out, result = exercise("action-output-first-exit", {"action_outputs": True, "output_fault": "missing", "test_exit": 3, "shutdown_exit": 42}, 3, 3)
    inspect_action_outputs(out, False)
    assert result["first_failure"]["stage"] == "tests" and result["first_failure"]["exit"] == 3
    assert {row["stage"] for row in result["errors"]} >= {"tests", "collector", "shutdown"}
    print("hosted-output-oracle: original-test-exit PASS")

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

    # Both complete resolved consumer selections use exactly the same workflow
    # predicate and four-value receipt check; no leaf is filtered or waived.
    runtime_target = "//tools/roundtrip:public_beads_runtime_test"
    recording_target = "//spec/s1-b25-b28:public_recording_test"
    for name, config, mandatory in (
        ("pr290-only", {"public_runtime": True}, {runtime_target}),
        ("pr295-only", {"public_recording": True}, {recording_target}),
        ("both", {"public_runtime": True, "public_recording": True}, {runtime_target, recording_target}),
        ("neither", {}, set()),
    ):
        out, result = exercise(name, config, public_mode="valid" if mandatory else None)
        actual_resolved = set((out / "selection/resolved-test-targets.txt").read_text().splitlines())
        assert actual_resolved.intersection({runtime_target, recording_target}) == mandatory
        actual_args = json.loads((out / "commands/tests/argv.json").read_text())
        assert mandatory.issubset(actual_args)
        public_args = [arg for arg in actual_args if arg.startswith("--test_env=PLUG_PUBLIC_BEADS_")]
        assert public_args == config.get("public_env", [])
        assert len(public_args) == (4 if mandatory else 0)
        if mandatory:
            for missing in ("missing", *("missing-value:" + key for key in public_names)):
                out, result = exercise(name + "-" + missing.replace(":", "-"), config.copy(), 86, 86, public_mode=missing)
                assert result["first_failure"]["stage"] == "collector-run"
                assert not (out / "commands/tests").exists()

    for mode in ("malformed", "manifest-bytes", "binary-mode", "binary-link", "receipt-run",
                 "receipt-artifact", "receipt-workflow", "receipt-schema", "receipt-digest",
                 "receipt-descriptor", "receipt-status", "receipt-environment", "receipt-source",
                 "receipt-archive", "receipt-missing"):
        out, result = exercise("public-" + mode, {"public_recording": True}, 86, 86, public_mode=mode)
        assert result["first_failure"]["stage"] == "collector-run"
        assert not (out / "commands/tests").exists()
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
