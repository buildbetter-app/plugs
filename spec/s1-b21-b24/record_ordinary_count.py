#!/usr/bin/env python3
"""Capture two fresh public B23 ordinary-count projections.

The executable and managed Dolt archive are same-run prepared inputs.  The
recorder authenticates both before execution, creates a private runtime per
fresh workspace, retains every raw stream (including timeout/failure bytes),
and emits one framed packet.  It never substitutes the retained PR288 twins.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from typing import Any

from ordinary_count_profile import (
    BD_GLOBAL_ARGS, COMMANDS, COUNT_VECTOR, ENVIRONMENT, GIT_CONFIGURATION,
    INPUTS, MODULES, PROFILE, SUPPORT_FILES,
)


WORK_ROOT = Path("/tmp/plug-swarm/work")
GIT = "/usr/bin/git"
SETUP_STREAMS: list[dict[str, Any]] = []
RUNTIME_STAMP = re.compile(r"20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z")
WORKSPACE_STAMP = re.compile(r"/tmp/plug-swarm/work/[^/\\\s]+")


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value: Any) -> str:
    data = value if isinstance(value, (bytes, bytearray)) else canonical(value)
    return hashlib.sha256(data).hexdigest()


def encoded(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def decoded(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"), validate=True)


def safe_path(value: str, *, directory: bool = False) -> Path:
    if not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("prepared input path is missing or malformed")
    path = Path(value)
    if not path.is_absolute() or str(path) != value or path.resolve(strict=True) != path:
        raise ValueError("prepared input path must be absolute and symlink-free")
    if directory and not path.is_dir():
        raise ValueError("prepared input is not a directory")
    if not directory and not path.is_file():
        raise ValueError("prepared input is not a file")
    return path


def isolated_environment(workspace: Path, runtime: Path) -> dict[str, str]:
    directory = workspace / ".recorder-env"
    directory.mkdir(mode=0o700)
    env = dict(ENVIRONMENT)
    for key, name in (("HOME", "home"), ("TMPDIR", "tmp"),
                      ("XDG_CONFIG_HOME", "config"), ("XDG_CACHE_HOME", "cache"),
                      ("XDG_DATA_HOME", "data"), ("XDG_STATE_HOME", "state")):
        path = directory / name
        path.mkdir(mode=0o700)
        env[key] = str(path)
    env.update({"PATH": f"{runtime.parent}:/usr/bin:/bin", "DOLT_ROOT_PATH": env["HOME"],
                "DOLT_DISABLE_EVENT_FLUSH": "1", "DOLT_CLI_PAGER": "cat", "PAGER": "cat"})
    env["GIT_CONFIG_COUNT"] = str(len(GIT_CONFIGURATION))
    for index, (key, value) in enumerate(GIT_CONFIGURATION.items()):
        env[f"GIT_CONFIG_KEY_{index}"] = key
        env[f"GIT_CONFIG_VALUE_{index}"] = value
    return env


def git(root: Path, *args: str) -> bytes:
    env = {
        "PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C",
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0",
    }
    return run_process(Path(GIT), ["-C", str(root), *args], root, env,
                       "source-git", [], SETUP_STREAMS, timeout=30)


def verify_source(root: Path) -> dict[str, Any]:
    if git(root, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching"):
        raise ValueError("prepared public source checkout is dirty")
    commit = git(root, "rev-parse", "HEAD").decode().strip()
    tree = git(root, "rev-parse", "HEAD^{tree}").decode().strip()
    if commit != PROFILE["source"]["commit"] or tree != PROFILE["source"]["tree"]:
        raise ValueError(f"public source pin differs: {commit} {tree}")
    manifest = git(root, "ls-tree", "-r", "--full-tree", "HEAD")
    if digest(manifest) != PROFILE["source"]["manifestSha256"] or len(manifest.splitlines()) != PROFILE["source"]["manifestEntries"]:
        raise ValueError("public source manifest differs from the reviewed pin")
    if any(line[:2] != b"H " for line in git(root, "ls-files", "-v").splitlines()):
        raise ValueError("public source has hidden index entries")
    blobs = []
    for name, checksum in PROFILE["runtimeSource"].items():
        data = git(root, "cat-file", "blob", commit + ":" + name)
        if digest(data) != checksum or (root / name).read_bytes() != data:
            raise ValueError("pinned runtime/count source differs: " + name)
        blobs.append({"path": name, "sha256": digest(data), "bytesBase64": encoded(data)})
    return {**PROFILE["source"], "cleanWorkingTree": True,
            "manifestBase64": encoded(manifest), "evidenceBlobs": blobs}


def candidate_identity() -> dict[str, Any]:
    root = safe_path(os.environ.get(PROFILE["capture"]["sourceRootEnv"], ""), directory=True)
    commit = git(root, "rev-parse", "HEAD").decode().strip()
    tree = git(root, "rev-parse", "HEAD^{tree}").decode().strip()
    modules = {}
    for name in MODULES:
        path = f"spec/s1-b21-b24/{name}"
        pinned = git(root, "cat-file", "blob", f"{commit}:{path}")
        actual = Path(__file__).with_name(name).read_bytes() if Path(__file__).with_name(name).exists() else None
        if actual is None or actual != pinned:
            raise ValueError(f"capture module differs from candidate: {name}")
        modules[name] = digest(pinned)
    support = {}
    for name in SUPPORT_FILES:
        data = git(root, "cat-file", "blob", f"{commit}:spec/s1-b21-b24/{name}")
        if (root / "spec/s1-b21-b24" / name).read_bytes() != data:
            raise ValueError("candidate harness source differs: " + name)
        support[name] = digest(data)
    return {"commit": commit, "tree": tree, "modules": modules, "support": support}


def safe_name(value: str) -> bool:
    path = PurePosixPath(value)
    return (bool(value) and str(path) == value and not path.is_absolute()
            and ".." not in path.parts and "\\" not in value
            and not any(ord(char) < 32 or ord(char) == 127 for char in value))


def consume_public_bundle(root: Path) -> tuple[Path, dict[str, Any], Path]:
    descriptor = os.environ.get(PROFILE["capture"]["publicDescriptorEnv"])
    build_run = os.environ.get(PROFILE["capture"]["publicBuildRunEnv"])
    if descriptor != PROFILE["descriptor"] or build_run != PROFILE["producer"]["run"]:
        raise ValueError("same-run public descriptor/build identity is absent")
    zip_input = safe_path(os.environ.get(PROFILE["capture"]["publicZipEnv"], ""))
    manifest_input = safe_path(os.environ.get(PROFILE["capture"]["publicManifestEnv"], ""))
    source = safe_path(os.environ.get(PROFILE["capture"]["publicSourceEnv"], ""), directory=True)
    bundle = root / "public.zip"
    shutil.copyfile(zip_input, bundle)
    expected = PROFILE["publicBundle"]
    if zip_input.stat().st_size != expected["zipBytes"]:
        raise ValueError("prepared public ZIP size differs")
    data = bundle.read_bytes()
    if len(data) != expected["zipBytes"] or digest(data) != expected["zipSha256"]:
        raise ValueError("prepared public ZIP identity differs")
    with zipfile.ZipFile(bundle) as archive:
        members = archive.infolist()
        names = [item.filename for item in members]
        files = {"bin/bd", "bin/plug", "bundle.tar", "manifest.json", "receipt.json"}
        if (len(names) != len(set(names)) or {item.filename for item in members if not item.is_dir()} != files
                or any(not safe_name(item.filename.rstrip("/"))
                       or (item.is_dir() and item.filename != "bin/")
                       or stat.S_IFMT(item.external_attr >> 16) not in {0, stat.S_IFDIR if item.is_dir() else stat.S_IFREG}
                       for item in members)):
            raise ValueError("prepared public ZIP member set differs")
        manifest = archive.read("manifest.json")
        receipt = archive.read("receipt.json")
        if digest(manifest) != expected["manifestSha256"] or manifest_input.read_bytes() != manifest or json.loads(receipt) != json.loads(manifest):
            raise ValueError("prepared public manifest/receipt differs")
        record = json.loads(manifest)
        from extract_ordinary_count_packet import validate_manifest
        validate_manifest(record)
        for name, identity in record["artifacts"].items():
            member = archive.read(name)
            if len(member) != identity["size"] or digest(member) != identity["sha256"]:
                raise ValueError("loose public binary differs: " + name)
        tar_bytes = archive.read("bundle.tar")
        if len(tar_bytes) != expected["archiveBytes"] or digest(tar_bytes) != expected["archiveSha256"]:
            raise ValueError("prepared public archive differs")
    tar_path = root / "bundle.tar"
    tar_path.write_bytes(tar_bytes)
    payload = None
    seen = set()
    with tarfile.open(tar_path, "r:") as archive:
        for item in archive:
            if item.name not in record["artifacts"] or item.name in seen:
                raise ValueError("public tar member set differs")
            seen.add(item.name)
            identity = record["artifacts"][item.name]
            if not item.isreg() or item.mode != 0o755 or item.size != identity["size"]:
                raise ValueError("public executable member is duplicate, nonregular, wrong mode or size")
            stream = archive.extractfile(item)
            if stream is None:
                raise ValueError("public executable payload is missing")
            member = stream.read()
            if len(member) != identity["size"] or digest(member) != identity["sha256"]:
                raise ValueError("public tar member differs: " + item.name)
            if item.name == expected["member"]:
                payload = member
    if seen != set(record["artifacts"]):
        raise ValueError("public tar member is missing")
    if payload is None or digest(payload) != expected["binarySha256"]:
        raise ValueError("public executable digest differs")
    binary = root / "bd"
    with binary.open("xb") as stream:
        stream.write(payload)
    binary.chmod(0o755)
    return binary, {"identity": expected, "manifestBase64": encoded(manifest),
                    "receiptBase64": encoded(receipt), "manifestSha256": digest(manifest),
                    "receiptSha256": digest(receipt), "binaryPath": str(binary),
                    "binarySha256": digest(binary.read_bytes()), "binaryMode": oct(binary.stat().st_mode & 0o777)}, source


def consume_dolt(root: Path) -> tuple[Path, dict[str, Any]]:
    archive_input = safe_path(os.environ.get(PROFILE["capture"]["doltArchiveEnv"], ""))
    archive_bytes = archive_input.read_bytes()
    expected = PROFILE["runtime"]
    if len(archive_bytes) != expected["archiveBytes"] or digest(archive_bytes) != expected["archiveSha256"]:
        raise ValueError("prepared Dolt archive identity differs")
    archive_path = root / "dolt.tar.gz"
    archive_path.write_bytes(archive_bytes)
    payload = None
    with tarfile.open(archive_path, "r:gz") as archive:
        for item in archive:
            if item.name != expected["member"]:
                continue
            if payload is not None or not item.isreg() or item.size != expected["binaryBytes"]:
                raise ValueError("Dolt member is duplicate, nonregular or empty")
            stream = archive.extractfile(item)
            if stream is None:
                raise ValueError("Dolt member is missing")
            payload = stream.read()
    if payload is None or len(payload) != expected["binaryBytes"] or digest(payload) != expected["binarySha256"]:
        raise ValueError("Dolt executable digest differs")
    runtime_dir = root / "runtime-bin"
    runtime_dir.mkdir(mode=0o700)
    runtime = runtime_dir / "dolt"
    with runtime.open("xb") as stream:
        stream.write(payload)
    runtime.chmod(0o700)
    return runtime, {"mode": expected["mode"], "version": expected["version"], "archive": {"url": expected["archiveURL"], "bytes": len(archive_bytes), "sha256": digest(archive_bytes)}, "binary": {"path": str(runtime), "member": expected["member"], "bytes": len(payload), "sha256": digest(runtime.read_bytes()), "mode": oct(runtime.stat().st_mode & 0o777)}}


def write_config(env: dict[str, str]) -> dict[str, Any]:
    home = Path(env["HOME"])
    directory = home / ".dolt"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    path = directory / "config_global.json"
    data = PROFILE["runtime"]["configBytes"].encode()
    with path.open("xb") as stream:
        stream.write(data)
    path.chmod(0o600)
    if path.read_bytes() != data:
        raise ValueError("private Dolt configuration changed after write")
    return {"relativePath": ".dolt/config_global.json", "rootPath": str(home), "path": str(path), "directoryMode": oct(directory.stat().st_mode & 0o777), "fileMode": oct(path.stat().st_mode & 0o777), "bytesBase64": encoded(data), "sha256": digest(data), "environment": {"DOLT_ROOT_PATH": env["DOLT_ROOT_PATH"], "DOLT_DISABLE_EVENT_FLUSH": env["DOLT_DISABLE_EVENT_FLUSH"]}}


def run_process(program: Path, args: list[str], cwd: Path, env: dict[str, str], label: str, commands: list[dict[str, Any]], streams: list[dict[str, Any]], timeout: int = 120, expected: int | None = 0) -> bytes:
    argv = [str(program), *args]
    outcome, launch_error = "exited", None
    try:
        process = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, check=False, timeout=timeout)
        stdout, stderr, code = process.stdout, process.stderr, process.returncode
    except subprocess.TimeoutExpired as error:
        stdout, stderr, code, outcome = error.stdout or b"", error.stderr or b"", None, "timeout"
    except OSError as error:
        stdout, stderr, code, outcome = b"", b"", None, "launch-error"
        launch_error = {"type": type(error).__name__, "message": str(error)}
    stream = {"label": label, "argv": argv, "cwd": str(cwd), "environmentSha256": digest(env),
              "exitCode": code, "outcome": outcome, "launchError": launch_error,
              "stdoutBase64": encoded(stdout), "stderrBase64": encoded(stderr),
              "stdoutBytes": len(stdout), "stderrBytes": len(stderr),
              "stdoutSha256": digest(stdout), "stderrSha256": digest(stderr), "timeoutSeconds": timeout}
    streams.append(stream)
    stdout_text, stderr_text = stdout.decode(errors="replace"), stderr.decode(errors="replace")
    commands.append({"step": label, "argv": argv, "exit": code, "outcome": stream["outcome"], "stdout": stdout_text, "stderr": stderr_text})
    if expected is not None and code != expected:
        raise RuntimeError(f"{label} failed with exit={code}; raw streams retained")
    return stdout


def normalize_text(value: str) -> str:
    value = RUNTIME_STAMP.sub("<runtime-time>", value)
    return WORKSPACE_STAMP.sub("<workspace>", value)


def json_output(data: bytes, step: str) -> Any:
    try:
        return json.loads(data)
    except json.JSONDecodeError as error:
        raise ValueError(f"{step} did not return JSON") from error


def count_value(data: bytes, step: str) -> int:
    value = json_output(data, step)
    if not isinstance(value, dict) or type(value.get("count")) is not int:
        raise ValueError(f"{step} did not return a count object")
    return value["count"]


def grouped_value(data: bytes) -> int:
    value = json_output(data, "open-task-by-status")
    if value.get("total") != 4 or value.get("groups") != [{"count": 4, "group": "open"}]:
        raise ValueError("open-task-by-status returned an unexpected grouped vector")
    return value["total"]


def capture_one(binary: Path, runtime: Path, name: str, result: dict[str, Any]) -> None:
    workspace = Path(tempfile.mkdtemp(prefix=f"s1-b23-{name}-", dir=WORK_ROOT))
    commands: list[dict[str, Any]] = []
    streams: list[dict[str, Any]] = []
    result.update({"recording": name, "workspace": str(workspace), "commands": commands,
                   "streams": streams, "workspaceRetained": True})
    env = None
    shutdown_verified, init_attempted = False, False
    failure = None
    projection = None
    try:
        env = isolated_environment(workspace, runtime)
        result["environment"] = env
        result["runtimeConfiguration"] = write_config(env)
        outputs = {}
        for label, program, suffix in COMMANDS[:-1]:
            executable = {"git": Path(GIT), "dolt": runtime, "bd": binary}[program]
            args = [*BD_GLOBAL_ARGS, *suffix] if program == "bd" else suffix
            if label == "init":
                init_attempted = True
            data = run_process(executable, args, workspace, env, label, commands, streams)
            outputs[label] = data
            if label in {"dolt-config-probe", "dolt-version"}:
                wanted = PROFILE["runtime"]["configProbeStdout" if label == "dolt-config-probe" else "versionStdout"]
                if data != wanted.encode() or streams[-1]["stderrBytes"] != 0:
                    raise ValueError(label + " did not return exact pinned output; full streams retained")
        projection = {
            key: count_value(outputs[step], step) for key, step in (
                ("openTaskCount", "open-task-count"), ("openNeedleTaskCount", "open-needle-count"),
                ("closedTaskCount", "closed-task-count"), ("absentNeedleTaskCount", "absent-needle-count"),
                ("afterUpdateOpenNeedleTaskCount", "after-update-needle-count"),
                ("afterUpdateChangedTaskCount", "after-update-changed-count"))
        }
        projection["openTaskStatusTotal"] = grouped_value(outputs["open-task-by-status"])
        if projection != COUNT_VECTOR:
            raise ValueError(f"ordinary count vector differs: {projection}")
    except Exception as error:
        failure = error
    finally:
        if init_attempted:
            try:
                stop = run_process(binary, [*BD_GLOBAL_ARGS, *COMMANDS[-1][2]], workspace, env,
                                   "cleanup-stop", commands, streams)
                value = json_output(stop, "cleanup-stop")
                if value.get("stopped") is not True or value.get("verified") is not True:
                    raise ValueError("owned Dolt shutdown was not verified")
                shutdown_verified = True
            except Exception as error:
                result["cleanupError"] = {"type": type(error).__name__, "message": str(error)}
                if failure is None:
                    failure = error
    result.update({
        "schema": "plug.s1-b23.ordinary-count-reference.v2",
        "recorderSha256": digest(Path(__file__).read_bytes()),
        "card": "B23", "recording": name, "engine": "managed-proxied-dolt",
        "source": {"commit": PROFILE["source"]["commit"], "tree": PROFILE["source"]["tree"], "descriptor": PROFILE["descriptor"], "profile": PROFILE["id"], "binarySha256": PROFILE["publicBundle"]["binarySha256"], "binaryMode": PROFILE["publicBundle"]["binaryMode"], "doltVersion": PROFILE["runtime"]["version"], "doltSha256": PROFILE["runtime"]["binarySha256"], "doltMode": PROFILE["runtime"]["binaryMode"], "runtimeMode": PROFILE["runtime"]["mode"]},
        "commands": commands, "streams": streams,
        "workspace": str(workspace), "shutdownVerified": shutdown_verified,
        "projection": projection, "projectionSha256": digest(projection) if projection is not None else None,
        "rawStreamsSha256": digest(streams),
        "rawJSON": canonical(commands).decode(), "rawSha256": digest(canonical(commands)),
        "inputs": INPUTS,
        "limitations": PROFILE["limitations"],
        "failure": {"type": type(failure).__name__, "message": str(failure)} if failure is not None else None,
    })
    result["inputSha256"] = digest(result["inputs"])
    result["normalizedTranscriptSha256"] = digest(normalize_text(canonical(commands).decode()).encode())
    if shutdown_verified:
        try:
            shutil.rmtree(workspace)
            result["workspaceRetained"] = False
        except OSError as error:
            result["failure"] = {"type": type(error).__name__, "message": str(error)}


def emit(packet: dict[str, Any]) -> None:
    payload = canonical(packet)
    # Never truncate failure or timeout evidence. The offline consumer may
    # refuse an oversized packet, but the original archived log keeps it all.
    identity = f"{len(payload)} {digest(payload)}".encode()
    begin = PROFILE["capture"]["frameBegin"].encode()
    end = PROFILE["capture"]["frameEnd"].encode()
    sys.stdout.buffer.write(begin + b" " + identity + b"\n" + payload + b"\n" + end + b" " + identity + b"\n")
    sys.stdout.buffer.flush()


def main() -> int:
    if len(sys.argv) != 1:
        raise SystemExit("capture accepts only fixed same-run prepared inputs")
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    SETUP_STREAMS.clear()
    packet: dict[str, Any] = {"schema": "plug.s1-b23.ordinary-count-capture.v2", "status": "BLOCK", "profile": PROFILE, "recordings": [], "setupStreams": SETUP_STREAMS}
    root = Path(tempfile.mkdtemp(prefix="s1-b23-count-capture-", dir=WORK_ROOT))
    try:
        try:
            packet["candidate"] = candidate_identity()
            binary, packet["publicBundle"], public_source = consume_public_bundle(root)
            packet["publicSource"] = verify_source(public_source)
            runtime, packet["runtime"] = consume_dolt(root)
            packet["configuration"] = {"inheritAmbient": False, "environment": ENVIRONMENT, "gitConfiguration": GIT_CONFIGURATION}
            for name in ("first", "repeat"):
                result = {"recording": name}
                packet["recordings"].append(result)
                capture_one(binary, runtime, name, result)
                if result.get("failure") or not result.get("shutdownVerified") or result.get("workspaceRetained"):
                    raise ValueError("count recording failed; full streams retained: " + name)
            first, repeat = packet["recordings"]
            if first.get("failure") or repeat.get("failure") or not first["shutdownVerified"] or not repeat["shutdownVerified"]:
                raise ValueError("one or more count recordings failed")
            if first["projection"] != repeat["projection"] or first["projection"] != COUNT_VECTOR:
                raise ValueError("fresh ordinary count projections differ")
            packet["pair"] = {"sameProjection": True, "rawStreamsEqual": first["streams"] == repeat["streams"], "normalizedTranscriptsEqual": first["normalizedTranscriptSha256"] == repeat["normalizedTranscriptSha256"], "projectionsEqual": first["projection"] == repeat["projection"], "independentWorkspaces": [first["workspace"], repeat["workspace"]]}
            packet["status"] = "PASS"
            from extract_ordinary_count_packet import validate_packet
            validate_packet(packet, packet["candidate"])
        except Exception as error:
            packet["status"] = "BLOCK"
            packet["error"] = {"type": type(error).__name__, "message": str(error)}
        packet["runtimeRetained"] = any(item.get("workspaceRetained", True) for item in packet["recordings"])
        packet["captureRoot"] = str(root)
        emit(packet)
    finally:
        if not packet.get("runtimeRetained", True):
            shutil.rmtree(root, ignore_errors=True)
    return 0 if packet["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
