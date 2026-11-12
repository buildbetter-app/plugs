#!/usr/bin/env python3
"""Run-bound local Bazel evidence. No hosted upload/retention claim is made here.

begin runs before bootstrap; selection resolves the complete union of the
existing lanes; run verifies that selection and executes it; finish runs
unconditionally before upload (including after failed setup).
Only explicitly selected platform/tool facts are read, never the environment or
credential files. Failure bundles retain the first real process exit separately
from later collection and shutdown failures.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import stat
import subprocess
import sys
import time
from urllib.parse import unquote, urlparse
import uuid
import xml.etree.ElementTree as ET


CAPTURE_ERROR = 86
# Existing automatically prepared manual exception; other captures stay opt-in.
PUBLIC_RUNTIME_TARGET = "//tools/roundtrip:public_beads_runtime_test"
PUBLIC_RUNTIME_TARGETS = frozenset({
    "//tools/roundtrip:public_beads_runtime_test",
    "//spec/s1-b25-b28:public_recording_test",
})
# Native adoption replaces this and the descriptor only after official-byte
# review and trusted-helper admission. Keep the current companion delivery.
PUBLIC_BUNDLE_PROFILE = "plug-public-companion/1"
PUBLIC_BUNDLE_DESCRIPTOR = (
    "41398730774:13120521766:"
    "66de7c070d365bbd7fc2bd39ed822e5bf72c21efabe0add9a5e26d07bf170a62"
)
PUBLIC_ENV_NAMES = (
    "PLUG_PUBLIC_BEADS_BINARY",
    "PLUG_PUBLIC_BEADS_SHA256",
    "PLUG_PUBLIC_BEADS_BUILD_RUN",
    "PLUG_PUBLIC_BEADS_MANIFEST",
)
SAFE_ENV = (
    "PATH", "HOME", "USER", "TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL", "TZ",
    "BAZELISK_HOME", "BAZELISK_VERIFY_SHA256", "USE_BAZEL_VERSION",
    # Public run identity and trusted tool paths required by source-built native tests.
    "BAZEL_BIN", "BAZEL_RC", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT",
    "GITHUB_SHA", "GITHUB_WORKFLOW_REF",
)


def child_env():
    # Do not pass ambient tokens, DSNs, test credentials, or action capabilities
    # into probes/tests. Workflow job permissions and event guards stay unchanged.
    return {key: os.environ[key] for key in SAFE_ENV if key in os.environ}


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def git(*args):
    return subprocess.check_output(["git", *args], env=child_env()).decode().strip()


def snapshot(output, phase):
    source = {}
    generators = {}
    entries = subprocess.check_output(["git", "ls-files", "-s", "-z"], env=child_env())
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        meta, raw_name = entry.split(b"\t", 1)
        name = os.fsdecode(raw_name)
        path = Path(name)
        if not path.is_file() or path.is_symlink():
            raise ValueError("non-regular tracked source: " + name)
        source[name] = {"sha256": digest(path), "mode": meta.decode().split()[0]}
        if (path.name in {"BUILD", "BUILD.bazel", "MODULE.bazel", "MODULE.bazel.lock", "go.mod", "go.sum"}
                or path.suffix == ".bzl" or "gen" in name or name.startswith("scripts/ci/")):
            generators[name] = source[name]
    status = subprocess.check_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], env=child_env())
    (output / (phase + "-status.txt")).write_bytes(status)
    write_json(output / (phase + "-sources.json"), source)
    write_json(output / (phase + "-generator-inputs.json"), {
        "meaning": "source inputs/definitions, not proof that a generator ran",
        "files": generators,
    })
    value = {"head": git("rev-parse", "HEAD"), "tree": git("rev-parse", "HEAD^{tree}"),
             "clean": not status, "sources_sha256": digest(output / (phase + "-sources.json")),
             "lock_sha256": digest(Path("MODULE.bazel.lock")), "observed_at": time.time()}
    write_json(output / (phase + "-source.json"), value)
    return value


class Capture:
    def __init__(self, output):
        self.output = output
        self.state_path = output / "state.json"
        self.state = {
            "schema": 1, "run_id": str(uuid.uuid4()), "started_at": time.time(),
            "commands": [], "first_failure": None, "errors": [],
        }
        invalid = None
        if self.state_path.exists():
            try:
                state = json.loads(self.state_path.read_text())
                if (state["schema"] != 1 or not isinstance(state["commands"], list)
                        or not isinstance(state["errors"], list) or not state["run_id"]
                        or "first_failure" not in state):
                    raise ValueError("invalid state schema")
                self.state = state
            except (ValueError, KeyError, TypeError) as error:
                shutil.copyfile(self.state_path, output / "invalid-state.json")
                invalid = str(error)
        # This independent receipt survives a damaged state/manifest. Never
        # replace the first process failure with a later collection error.
        first = output / "first-failure.json"
        if first.exists():
            try:
                failure = json.loads(first.read_text())
                if not isinstance(failure["exit"], int) or not 1 <= failure["exit"] <= 255:
                    raise ValueError("invalid first failure exit")
                self.state["first_failure"] = failure
            except (ValueError, KeyError, TypeError) as error:
                invalid = "invalid independent failure receipt: " + str(error)
        if invalid:
            self.fail("collector-state", CAPTURE_ERROR, invalid)

    def save(self):
        write_json(self.state_path, self.state)

    def fail(self, stage, code, message):
        failure = {"stage": stage, "exit": code, "message": message}
        self.state["errors"].append(failure)
        if self.state["first_failure"] is None:
            self.state["first_failure"] = failure
            write_json(self.output / "first-failure.json", failure)
        self.save()

    def command(self, name, argv, *, env=None, required=True):
        if not re.fullmatch(r"[a-z0-9-]+", name):
            raise ValueError("invalid command receipt name")
        directory = self.output / "commands" / name
        directory.mkdir(parents=True, exist_ok=False)
        write_json(directory / "argv.json", argv)
        (directory / "command.txt").write_text(shlex.join(argv) + "\n")
        start = time.time()
        with (directory / "stdout.log").open("wb") as stdout, (directory / "stderr.log").open("wb") as stderr:
            try:
                code = subprocess.run(argv, stdout=stdout, stderr=stderr,
                                      env=child_env() if env is None else env).returncode
                code = 128 - code if code < 0 else code
            except OSError as error:
                stderr.write((str(error) + "\n").encode())
                code = 127
        (directory / "exit.txt").write_text(str(code) + "\n")
        self.state["commands"].append({"name": name, "exit": code,
                                        "started_at": start, "finished_at": time.time()})
        if code and required:
            self.fail(name, code, "process failed; see command receipt")
        else:
            self.save()
        print(f"delivery: {name}: exit {code}", flush=True)
        return code, (directory / "stdout.log").read_text(errors="replace").strip()

    def code(self):
        failure = self.state["first_failure"]
        return failure["exit"] if failure else 0


def bazel_command():
    binary = shutil.which(os.environ.get("BAZEL_BIN", "bazel"))
    if binary is None:
        raise ValueError("Bazel executable is unavailable")
    args = [binary, "--nosystem_rc", "--nohome_rc", "--noworkspace_rc"]
    if os.environ.get("BAZEL_RC"):
        args.append("--bazelrc=" + os.environ["BAZEL_RC"])
    return args


def begin(capture, args):
    if any(capture.output.iterdir()):
        raise ValueError("begin requires a new empty evidence directory")
    capture.state["requested"] = {"base": args.base, "head": args.head}
    capture.state["pre"] = snapshot(capture.output, "pre")
    capture.save()
    pre = capture.state["pre"]
    if not pre["clean"] or (args.head and args.head != pre["head"]):
        capture.fail("pre-source", CAPTURE_ERROR, "source must be clean at the requested immutable head")
    resolved_base = None
    # Mirror the resolver's fallback but pin it now, before any queries/tests.
    for revision in (args.base, "origin/main"):
        if not revision:
            continue
        try:
            resolved_base = git("rev-parse", "--verify", revision + "^{commit}")
            capture.state["base_selection"] = revision
            break
        except subprocess.CalledProcessError:
            continue
    capture.state["base"] = resolved_base
    capture.state["merge_base"] = git("merge-base", resolved_base, pre["head"]) if resolved_base else None
    facts = {"platform": {"system": platform.system(), "release": platform.release(),
                           "machine": platform.machine(), "cpu_count": os.cpu_count(),
                           "memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")}, "tools": {}}
    try:
        release = platform.freedesktop_os_release()
        facts["platform"]["os_release"] = {key: release[key] for key in ("ID", "VERSION_ID", "PRETTY_NAME") if key in release}
    except OSError:
        facts["platform"]["os_release"] = "unavailable"
    # These keys are non-secret runner identifiers; no environment dump.
    facts["runner"] = {key: os.environ.get(key, "unavailable") for key in
                       ("RUNNER_NAME", "RUNNER_OS", "RUNNER_ARCH", "ImageOS", "ImageVersion",
                        "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_JOB")}
    for tool, flag in (("python3", "--version"), ("git", "--version"), ("go", "version"), ("nix", "--version")):
        binary = shutil.which(tool)
        if binary:
            code, version = capture.command("ambient-" + tool, [binary, flag], required=False)
            facts["tools"][tool] = {"path": binary, "exit": code, "version": version,
                                      "state": "available" if code == 0 else "probe-failed"}
        else:
            facts["tools"][tool] = {"state": "unavailable"}
    write_json(capture.output / "preflight.json", facts)
    capture.save()


def labels(value):
    result = sorted(set(value.splitlines()))
    if any(not re.fullmatch(r"//[^\s]+", label) for label in result):
        raise ValueError("invalid or external test label")
    return result


def setup_toolchain(capture):
    if capture.code():
        return None
    if "pre" not in capture.state:
        raise ValueError("begin must precede run")
    bazel = bazel_command()
    tool = {"launcher": bazel[0], "launcher_sha256": digest(Path(bazel[0]).resolve()),
            "bazel_pin": Path(".bazelversion").read_text().strip(),
            "rc_sha256": digest(Path(os.environ["BAZEL_RC"])) if os.environ.get("BAZEL_RC") else None}
    code, version = capture.command("bazel-version", [*bazel, "version", "--gnu_format"])
    # Bazelisk prepends its own version to the version subcommand's output;
    # direct Bazel prints only the second line. Retain the entire real report.
    versions = [line for line in version.splitlines() if re.fullmatch(r"bazel \S+", line)]
    tool["version_report"] = version
    tool["resolved_bazel"] = versions[0] if len(versions) == 1 else None
    write_json(capture.output / "toolchain.json", tool)
    if code:
        return
    if versions != ["bazel 9.3.0"] or tool["bazel_pin"] != "9.3.0":
        raise ValueError("Bazel pin and resolved binary must be 9.3.0")
    code, root = capture.command("execution-root", [*bazel, "info", "execution_root", "--lockfile_mode=error"])
    if code:
        return
    capture.state["execution_root"] = root
    capture.save()
    code, sdk = capture.command("resolved-go-sdk", [*bazel, "run", "--lockfile_mode=error", "--jobs=2", "--local_resources=memory=8192", "@rules_go//go", "--", "version"])
    tool["resolved_go_sdk"] = sdk
    write_json(capture.output / "toolchain.json", tool)
    if code:
        return
    if not re.search(r"go version go1\.26\.10\s", sdk):
        raise ValueError("resolved Bazel Go SDK does not match the source pin")
    capture.state["toolchain_ready"] = True
    capture.save()
    return bazel


def selection_files(target_dir):
    return sorted(path for path in target_dir.rglob("*") if path.is_file())


def selection_binding(capture, target_dir, selected, resolved):
    return {
        "head": git("rev-parse", "HEAD"),
        "tree": git("rev-parse", "HEAD^{tree}"),
        "base": capture.state.get("base"),
        "merge_base": capture.state.get("merge_base"),
        "selected_targets": selected,
        "resolved_tests": resolved,
        "files": {
            str(path.relative_to(target_dir)): digest(path)
            for path in selection_files(target_dir)
        },
    }


def automatic_test_query(tests):
    # Match a complete item in Bazel's rendered tag list, preserving ordinary
    # tags such as manual-policy. Only the hosted prepared runtime is implicit;
    # reference captures use their existing explicit preparation contract.
    # The first/last characters are the list brackets; no query escapes needed.
    manual = "(^.|, )manual(,|.$)"
    return ('(' + tests + ') except (attr("tags", "' + manual + '", ' + tests
            + ') except set(' + PUBLIC_RUNTIME_TARGET + '))')


def resolve_selection(capture, bazel):
    target_dir = capture.output / "selection"
    resolver_env = child_env()
    resolver_env["BAZEL_BIN"] = bazel[0]
    if os.environ.get("BAZEL_RC"):
        resolver_env["BAZEL_RC"] = os.environ["BAZEL_RC"]
    code, _ = capture.command("target-resolution", ["bash", "scripts/ci/plug-bazel-targets.sh",
        "--base", capture.state["base"] or "", "--head", capture.state["pre"]["head"],
        "--output", str(target_dir)], env=resolver_env)
    if code:
        return
    # Preserve file/resource selection and supplement with the owning libraries'
    # full reverse closure. CI-only fallback remains the required lexicon gate.
    packages = (target_dir / "packages.txt").read_text().splitlines()
    libraries = []
    reverse = []
    if packages:
        expression = 'kind(".*_library rule", set(' + " ".join(p + ":all" for p in packages) + '))'
        code, value = capture.command("owning-libraries", [*bazel, "query", expression, "--lockfile_mode=error", "--output=label"])
        if code:
            return
        libraries = labels(value)
        if libraries:
            expression = automatic_test_query(
                'kind(".*_test rule", rdeps(//..., set(' + " ".join(libraries) + ')))')
            code, value = capture.command("owning-reverse", [*bazel, "query", expression, "--lockfile_mode=error", "--output=label"])
            if code:
                return
            reverse = labels(value)
    (target_dir / "owning-libraries.txt").write_text("".join(x + "\n" for x in libraries))
    (target_dir / "owning-reverse-targets.txt").write_text("".join(x + "\n" for x in reverse))
    selected = {"//:lexicon_gate", *reverse}
    for name in ("touched-targets.txt", "reverse-targets.txt"):
        selected.update(labels((target_dir / name).read_text().strip()))
    selected = sorted(selected)
    (target_dir / "command-targets.txt").write_text("\n".join(selected) + "\n")
    expression = "tests(set(" + " ".join(selected) + "))"
    code, value = capture.command("resolved-test-targets", [*bazel, "query", expression, "--lockfile_mode=error", "--output=label"])
    if code:
        return
    resolved = labels(value)
    if not resolved:
        raise ValueError("target selection resolved to no tests")
    (target_dir / "resolved-test-targets.txt").write_text("\n".join(resolved) + "\n")
    capture.state["selected_targets"] = selected
    capture.state["resolved_tests"] = resolved
    capture.state["selection_ready"] = True
    capture.state["selection_binding"] = selection_binding(capture, target_dir, selected, resolved)
    capture.save()


def selection(capture):
    bazel = setup_toolchain(capture)
    if bazel is None:
        return
    resolve_selection(capture, bazel)


def verify_selection(capture):
    if not capture.state.get("selection_ready") or "selection_binding" not in capture.state:
        raise ValueError("selection phase must succeed before run")
    target_dir = capture.output / "selection"
    binding = capture.state["selection_binding"]
    current = selection_binding(capture, target_dir, capture.state.get("selected_targets", []),
                                capture.state.get("resolved_tests", []))
    if current != binding:
        raise ValueError("source, base or resolved selection changed after selection")
    if subprocess.check_output(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            env=child_env()):
        raise ValueError("source became dirty after selection")
    resolved = (target_dir / "resolved-test-targets.txt").read_text().splitlines()
    if resolved != capture.state["resolved_tests"]:
        raise ValueError("resolved test target file changed after selection")


def public_selection(capture):
    # The workflow uses exactly the collector's predicate, after the complete
    # touched/reverse/owning closure has been resolved and bound to source.
    verify_selection(capture)
    print("true" if PUBLIC_RUNTIME_TARGETS.intersection(capture.state["resolved_tests"]) else "false")


def public_test_env(capture):
    resolved = set(capture.state.get("resolved_tests", ()))
    present = {name: os.environ.get(name) for name in PUBLIC_ENV_NAMES if os.environ.get(name) is not None}
    if not PUBLIC_RUNTIME_TARGETS.intersection(resolved):
        if present:
            raise ValueError("public executable values supplied without either mandatory public consumer target")
        return []
    values = {name: os.environ.get(name) for name in PUBLIC_ENV_NAMES}
    if any(value is None or value == "" for value in values.values()):
        raise ValueError("mandatory public consumer selection lacks prepared executable values")
    if any("\n" in value or "\r" in value for value in values.values()):
        raise ValueError("prepared public executable value contains a newline")
    binary = Path(values["PLUG_PUBLIC_BEADS_BINARY"])
    manifest = Path(values["PLUG_PUBLIC_BEADS_MANIFEST"])
    if (not binary.is_absolute() or binary.is_symlink() or not binary.is_file()
            or stat.S_IMODE(binary.stat().st_mode) != 0o755):
        raise ValueError("prepared public executable path is not an absolute regular 0755 file")
    if re.fullmatch(r"[0-9a-f]{64}", values["PLUG_PUBLIC_BEADS_SHA256"]) is None:
        raise ValueError("prepared public executable SHA256 is malformed")
    if digest(binary) != values["PLUG_PUBLIC_BEADS_SHA256"]:
        raise ValueError("prepared public executable SHA256 does not match the file")
    if (not manifest.is_absolute() or manifest.is_symlink() or not manifest.is_file()):
        raise ValueError("prepared public manifest path is not an absolute regular file")
    if re.fullmatch(r"[1-9][0-9]{0,19}", values["PLUG_PUBLIC_BEADS_BUILD_RUN"]) is None:
        raise ValueError("prepared public producer run is not a positive canonical decimal")
    receipt_path = capture.output / "public-bundle.json"
    if not receipt_path.is_file() or receipt_path.is_symlink():
        raise ValueError("prepared public bundle receipt is missing")
    try:
        receipt = json.loads(receipt_path.read_text())
    except (OSError, ValueError) as error:
        raise ValueError("prepared public bundle receipt is invalid") from error
    descriptor = re.fullmatch(r"([1-9][0-9]{0,19}):([1-9][0-9]{0,19}):([0-9a-f]{64})", PUBLIC_BUNDLE_DESCRIPTOR)
    if descriptor is None:
        raise ValueError("fixed public descriptor is malformed")
    run_id, artifact_id, manifest_sha256 = descriptor.groups()
    if digest(manifest) != manifest_sha256:
        raise ValueError("prepared public manifest bytes differ from the fixed descriptor")
    try:
        sealed = json.loads(manifest.read_text())
    except (OSError, ValueError) as error:
        raise ValueError("prepared public manifest is invalid") from error
    if not isinstance(receipt, dict) or not isinstance(sealed, dict):
        raise ValueError("prepared public receipt and manifest must be objects")
    if (receipt.get("status") != "PASS" or receipt.get("descriptor") != PUBLIC_BUNDLE_DESCRIPTOR
            or receipt.get("manifest_sha256") != manifest_sha256
            or receipt.get("producer_run") != run_id or receipt.get("artifact_id") != artifact_id
            or values["PLUG_PUBLIC_BEADS_BUILD_RUN"] != run_id
            or sealed.get("build", {}).get("run_id") != run_id
            or receipt.get("schema") != PUBLIC_BUNDLE_PROFILE
            or receipt.get("environment") != values
            or re.fullmatch(r"sha256:[0-9a-f]{64}", str(receipt.get("artifact_digest"))) is None
            or receipt.get("producer_workflow_sha") != sealed.get("workflow", {}).get("sha")
            or re.fullmatch(r"[0-9a-f]{40}", str(receipt.get("producer_workflow_sha"))) is None
            or any(receipt.get(field) != sealed.get(field)
                   for field in ("schema", "source", "targets", "artifacts", "archive"))):
        raise ValueError("prepared public bundle receipt is not bound to the fixed descriptor and manifest")
    record = sealed.get("artifacts", {}).get("bin/bd-export", {})
    if (record.get("sha256") != values["PLUG_PUBLIC_BEADS_SHA256"]
            or record.get("mode") != "0o755" or record.get("size") != binary.stat().st_size
            or binary != manifest.parent / "bin/bd-export"
            or any(part.is_symlink() for part in (*binary.parents, *manifest.parents))):
        raise ValueError("prepared public executable and manifest do not share the verified bundle")
    return [f"--test_env={name}={values[name]}" for name in PUBLIC_ENV_NAMES]


def run(capture):
    if capture.code():
        return
    if "pre" not in capture.state:
        raise ValueError("begin must precede run")
    verify_selection(capture)
    if not capture.state.get("toolchain_ready"):
        raise ValueError("toolchain and selection phase must precede run")
    bazel = bazel_command()
    selected = capture.state["selected_targets"]
    args = [*bazel, "test", *selected, "--lockfile_mode=error", "--nocache_test_results", "--flaky_test_attempts=1",
            # rules_go needs verbose wrapper events to emit passing/skipped
            # test cases in XML rather than an empty <testsuites> stub.
            "--test_env=GO_TEST_WRAP_TESTV=1",
            # Preserve short/moderate/long budgets; CP7's eternal tests need 150m.
            "--test_output=all", "--test_timeout=900,900,900,9000", "--jobs=2", "--local_resources=memory=8192",
            "--remote_cache=", "--remote_executor=", "--repo_contents_cache=",
            "--build_event_publish_all_actions", "--build_event_json_file=" + str(capture.output / "raw.bep.json")]
    if os.environ.get("PLUG_BAZEL_DISK_CACHE"):
        args.append("--disk_cache=" + os.environ["PLUG_BAZEL_DISK_CACHE"])
    args.extend(public_test_env(capture))
    capture.state["test_started"] = True
    capture.save()
    capture.command("tests", args)


def open_confined_test_output(allowed, source):
    # Anchor every component, including the allowed-root path, before opening
    # the selected file. A replacement symlink is never followed at read time.
    parts = allowed.parts[1:] + source.relative_to(allowed).parts
    directory = os.open(allowed.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    descriptor = None
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("test output is not a regular file")
        return os.fdopen(descriptor, "rb")
    except Exception:
        if descriptor is not None:
            os.close(descriptor)
        raise
    finally:
        os.close(directory)


def copy_test_outputs(capture, identity, result, destination):
    # Keep each BEP declaration and attempt identity, even when copying fails.
    inventory = {"identity": identity, "outputs": [
        {"index": index, "bep": item, "state": "pending"}
        for index, item in enumerate(result.get("testActionOutput", []))]}
    receipt = destination / "test-action-outputs.json"
    write_json(receipt, inventory)
    copied, errors = [], []
    allowed = (Path(capture.state["execution_root"]) / "bazel-out").resolve()
    for record in inventory["outputs"]:
        created, target = False, None
        try:
            item = record["bep"]
            name = item.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError("test output lacks its BEP name")
            uri = urlparse(item.get("uri", ""))
            if uri.scheme != "file" or uri.netloc not in {"", "localhost"}:
                raise ValueError("test output is not a local file; cannot deliver complete evidence")
            source = Path(unquote(uri.path)).resolve(strict=True)
            if not source.is_relative_to(allowed) or "testlogs" not in source.relative_to(allowed).parts:
                raise ValueError("test output escapes this run's Bazel testlogs")
            required = name in {"test.log", "test.xml"}
            if required and source.name != name:
                raise ValueError("test output name mismatch")
            # Logical names and URI basenames are metadata, never destination
            # paths. Fixed visible payload names also survive hosted upload's
            # default exclusion of hidden files, without changing that policy.
            target = destination / name if required else destination / "outputs" / str(record["index"]) / "payload"
            if target.exists():
                raise ValueError("duplicate required test output: " + name)
            target.parent.mkdir(parents=True, exist_ok=True)
            record.update(source=str(source), path=str(target.relative_to(capture.output)))
            with open_confined_test_output(allowed, source) as snapshot:
                before = os.fstat(snapshot.fileno())
                h, size = hashlib.sha256(), 0
                with target.open("xb") as output:
                    created = True
                    for block in iter(lambda: snapshot.read(1024 * 1024), b""):
                        output.write(block)
                        h.update(block)
                        size += len(block)
                after = os.fstat(snapshot.fileno())
                if (size != before.st_size or
                        any(getattr(before, key) != getattr(after, key) for key in
                            ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"))):
                    raise ValueError("test output changed during the admitted descriptor snapshot")
            facts = {"size": size, "sha256": h.hexdigest()}
            if target.stat().st_size != size or digest(target) != facts["sha256"]:
                raise ValueError("test output bytes changed while copying")
            record.update(state="copied", **facts)
            copied.append(record["path"])
        except Exception as error:
            if created:
                try:
                    target.unlink(missing_ok=True)
                    record["partial_removed"] = True
                except OSError as cleanup:
                    error = ValueError(str(error) + "; incomplete output cleanup failed: " + str(cleanup))
            record.update(state="error", error=str(error))
            errors.append(str(error))
        write_json(receipt, inventory)
    if errors:
        raise ValueError("test output retention incomplete for " + identity["label"] + ": " + "; ".join(errors))
    return copied


def collect_tests(capture):
    if not capture.state.get("test_started"):
        write_json(capture.output / "tests.json", {"state": "not-run", "reason": capture.state["first_failure"]})
        return
    events = [json.loads(line) for line in (capture.output / "raw.bep.json").read_text().splitlines() if line]
    finished = [e["finished"] for e in events if "finished" in e]
    if len(finished) != 1 or not events[-1].get("lastMessage"):
        raise ValueError("BEP is incomplete: missing build finish or last message")
    test_exit = next(x["exit"] for x in capture.state["commands"] if x["name"] == "tests")
    if finished[0].get("exitCode", {}).get("code", 0) != test_exit:
        raise ValueError("BEP exit disagrees with the real test process exit")
    summaries = []
    attempts = []
    generated = []
    configurations = []
    for event in events:
        if "configuration" in event:
            configurations.append({"id": event["id"], "facts": event["configuration"]})
        if "namedSetOfFiles" in event:
            for item in event["namedSetOfFiles"].get("files", []):
                if "digest" in item:
                    generated.append({key: item[key] for key in ("name", "digest", "length") if key in item})
        if "testSummary" in event:
            summaries.append({"label": event["id"]["testSummary"]["label"], **event["testSummary"]})
        if "testResult" not in event:
            continue
        identity = event["id"]["testResult"]
        result = event["testResult"]
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        destination = capture.output / "tests" / key
        destination.mkdir(parents=True, exist_ok=False)
        copied = copy_test_outputs(capture, identity, result, destination)
        counts = None
        # Retain all opaque outputs for this attempt before XML accounting.
        if (destination / "test.xml").is_file():
            root = ET.parse(destination / "test.xml").getroot()
            cases = list(root.iter("testcase"))
            counts = {"cases": len(cases), "skipped": sum(x.find("skipped") is not None for x in cases),
                      "failures": sum(x.find("failure") is not None for x in cases),
                      "errors": sum(x.find("error") is not None for x in cases)}
            if not cases:
                suites = list(root.iter("testsuite"))
                if not suites or any("tests" not in x.attrib for x in suites):
                    raise ValueError("XML lacks test-case or explicit suite accounting for " + identity["label"])
                counts = {key: sum(int(x.get(attr, 0)) for x in suites) for key, attr in
                          (("cases", "tests"), ("skipped", "skipped"), ("failures", "failures"), ("errors", "errors"))}
        cached = bool(result.get("cachedLocally") or result.get("executionInfo", {}).get("cachedRemotely"))
        attempts.append({"identity": identity, "status": result.get("status", "UNKNOWN"),
                         "cached": cached, "xml": counts, "files": copied,
                         "output_inventory": str((destination / "test-action-outputs.json").relative_to(capture.output))})
        # Failed-to-build targets may have no test artifacts. Never label them
        # executed or skipped; the summary/process records the real failure.
        required = {str((destination / name).relative_to(capture.output)) for name in ("test.log", "test.xml")}
        if result.get("status") in {"PASSED", "FAILED", "FLAKY"} and not required.issubset(copied):
            raise ValueError("test log/XML missing for " + identity["label"])
    reported = {x["label"] for x in summaries}
    unreported = sorted(set(capture.state["resolved_tests"]) - reported)
    statuses = {}
    for summary in summaries:
        status = summary.get("overallStatus", "UNKNOWN")
        statuses[status] = statuses.get(status, 0) + 1
    metrics = [e["buildMetrics"] for e in events if "buildMetrics" in e]
    write_json(capture.output / "tests.json", {
        "state": "collected", "summaries": summaries, "attempts": attempts, "unreported_tests": unreported,
        "accounting": {"target_status_counts": statuses, "executed_attempts": sum(not x["cached"] for x in attempts),
                       "cached_attempts": sum(x["cached"] for x in attempts),
                       "executed_xml_skips": sum((x["xml"] or {}).get("skipped", 0) for x in attempts if not x["cached"]),
                       "cached_xml_skips": sum((x["xml"] or {}).get("skipped", 0) for x in attempts if x["cached"]),
                       "meaning": "attempt counts and XML skip elements; missing targets are unreported, not skipped"},
        "build_metrics": metrics,
    })
    write_json(capture.output / "bep-output-digests.json", {
        "meaning": "BEP-reported output digests, not reconstructed generator execution history", "outputs": generated})
    write_json(capture.output / "resolved-platforms.json", configurations)
    attempted = {x["identity"]["label"] for x in attempts}
    if test_exit == 0 and (unreported or reported != set(capture.state["resolved_tests"])
            or attempted != reported or any(x.get("overallStatus") != "PASSED" for x in summaries)
            or any((x["xml"] or {}).get("failures", 0) or (x["xml"] or {}).get("errors", 0) for x in attempts)):
        raise ValueError("successful process lacks complete passing per-target evidence")


def finish(capture, job_status):
    if capture.state.get("finished_at"):
        raise ValueError("bundle is already finalized")
    if job_status != "success" and not capture.code():
        capture.fail("upstream-workflow", 1, "upstream step failed/cancelled; exact exit unavailable to this step")
    try:
        collect_tests(capture)
        if not capture.state.get("test_started") and not capture.code():
            raise ValueError("test process never ran")
    except Exception as error:
        # Malformed event/XML schemas must not bypass cleanup either.
        capture.fail("collector", CAPTURE_ERROR, str(error))
        write_json(capture.output / "tests-incomplete.json", {"state": "incomplete", "error": str(error),
            "meaning": "raw events and copied outputs retained; no complete accounting claimed"})
    # Deliberately outside collection's try block: cleanup still runs after a
    # malformed BEP, missing XML, failed query, or failed bootstrap.
    try:
        code, _ = capture.command("shutdown", [*bazel_command(), "shutdown"])
        capture.state["shutdown"] = {"state": "ran", "exit": code}
    except Exception as error:
        capture.state["shutdown"] = {"state": "unavailable", "exit": None}
        capture.fail("shutdown", 127, str(error))
    try:
        post = snapshot(capture.output, "post")
        capture.state["post"] = post
        pre = capture.state.get("pre", {})
        same = all(pre.get(key) == post[key] for key in ("head", "tree", "sources_sha256", "lock_sha256"))
        capture.state["source_unchanged"] = same and post["clean"] and pre.get("clean", False)
        if not capture.state["source_unchanged"]:
            raise ValueError("source/head/tree/lock/clean state changed during capture")
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        capture.fail("post-source", CAPTURE_ERROR, str(error))
    capture.state["finished_at"] = time.time()
    capture.save()
    write_result(capture)
    files = {}
    for path in sorted(capture.output.rglob("*")):
        if path.is_symlink():
            raise ValueError("symlink in delivery bundle")
        if path.is_file() and path not in {capture.output / "manifest.json", capture.output / "manifest.sha256"}:
            files[str(path.relative_to(capture.output))] = {"sha256": digest(path), "size": path.stat().st_size}
    write_json(capture.output / "manifest.json", {"schema": 1, "run_id": capture.state["run_id"], "files": files})
    (capture.output / "manifest.sha256").write_text(digest(capture.output / "manifest.json") + "\n")
    verify(capture.output)


def write_result(capture):
    write_json(capture.output / "result.json", {**capture.state, "exit": capture.code(),
        "result": "PASS" if capture.code() == 0 else "FAIL", "hosted_delivery": "unverified"})


def verify(output):
    manifest = json.loads((output / "manifest.json").read_text())
    if digest(output / "manifest.json") != (output / "manifest.sha256").read_text().strip():
        raise ValueError("evidence manifest hash mismatch")
    paths = list(output.rglob("*"))
    if any(p.is_symlink() for p in paths):
        raise ValueError("symlink in delivery bundle")
    present = {str(p.relative_to(output)) for p in paths if p.is_file()
               and p not in {output / "manifest.json", output / "manifest.sha256"}}
    if present != set(manifest["files"]):
        raise ValueError("evidence inventory mismatch")
    for name, facts in manifest["files"].items():
        path = output / name
        if path.is_symlink() or not path.resolve().is_relative_to(output) or digest(path) != facts["sha256"] or path.stat().st_size != facts["size"]:
            raise ValueError("evidence digest/size mismatch: " + name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("begin", "selection", "public-selection", "run", "finish", "verify"))
    parser.add_argument("--output", required=True)
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--job-status", choices=("success", "failure", "cancelled"), default="success")
    args = parser.parse_args()
    output = Path(args.output).resolve()
    root = Path(git("rev-parse", "--show-toplevel")).resolve()
    if output.is_relative_to(root):
        parser.error("evidence must remain outside source")
    os.chdir(root)
    output.mkdir(parents=True, exist_ok=True)
    if args.phase == "verify":
        try:
            verify(output)
            return 0
        except (OSError, ValueError, KeyError) as error:
            print("delivery: verification failed: " + str(error), file=sys.stderr)
            return CAPTURE_ERROR
    capture = Capture(output)
    try:
        if args.phase == "begin":
            begin(capture, args)
        elif args.phase == "selection":
            selection(capture)
        elif args.phase == "public-selection":
            public_selection(capture)
        elif args.phase == "run":
            run(capture)
        else:
            finish(capture, args.job_status)
    except Exception as error:
        capture.fail("collector-" + args.phase, CAPTURE_ERROR, str(error))
        if args.phase == "finish":
            # Invalidate any stale seal and PASS if final hashing/verification
            # failed. Upload still receives the raw partial failure payload.
            (output / "manifest.sha256").unlink(missing_ok=True)
            write_result(capture)
        print("delivery: collection failed; partial payload retained", file=sys.stderr)
    return capture.code()


if __name__ == "__main__":
    sys.exit(main())
