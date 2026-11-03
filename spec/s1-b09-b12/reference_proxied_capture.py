#!/usr/bin/env python3
"""Explicit managed-proxied B09-B11 capture; never embedded or B12 proof."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

import record_reference as recorder
import reference_capture as original
from public_reference_profile import SOURCE_TESTS
from proxied_reference_profile import (
    CARDS, ENGINE, MODULES, PACKET_SCHEMA, PROFILE, PUBLIC_BUILD, QUALIFICATION,
    RUN_SCHEMA, RUNTIME, RUNTIME_CONFIGURATION, RUNTIME_SOURCE, UNEXECUTED,
)


def candidate_identity():
    root = original.prepared_path(os.environ.get("PLUG_SOURCE_MODULE_ROOT", ""), directory=True)
    commit = recorder.source_git(root, "rev-parse", "HEAD").decode().strip()
    tree = recorder.source_git(root, "rev-parse", "HEAD^{tree}").decode().strip()
    modules = {}
    for name in MODULES:
        data = recorder.source_git(root, "cat-file", "blob", commit + ":spec/s1-b09-b12/" + name)
        if Path(__file__).with_name(name).read_bytes() != data:
            raise ValueError("executed proxied module differs from candidate: " + name)
        modules[name] = original.sha256(data)
    if modules["record_reference.py"] != PUBLIC_BUILD["reviewedRecorderSha256"]:
        raise ValueError("legacy recorder changed")
    return {"commit": commit, "tree": tree, "modules": modules}


def runtime_source(source):
    result = []
    for path, checksum in RUNTIME_SOURCE.items():
        data = recorder.source_git(source, "cat-file", "blob", recorder.PIN + ":" + path)
        if original.sha256(data) != checksum:
            raise ValueError("pinned runtime source differs: " + path)
        result.append({"path": path, "sha256": checksum, "bytesBase64": original.encoded(data)})
    return result


def configure_runtime(environment):
    # Original9d6 localdoltserver.go and test_env.sh use these exact supported
    # private config keys. Write them before spawning Dolt, so even its first
    # command cannot perform the release check or fork an event-flush child.
    home = Path(environment["HOME"])
    directory = home / ".dolt"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    path = home / RUNTIME_CONFIGURATION["relativePath"]
    with path.open("xb") as output:
        output.write(RUNTIME_CONFIGURATION["contents"].encode())
    path.chmod(0o600)
    environment.update({"DOLT_ROOT_PATH": str(home), "DOLT_DISABLE_EVENT_FLUSH": "1"})
    data = path.read_bytes()
    return {
        "operation": RUNTIME_CONFIGURATION["operation"],
        "rootPath": str(home), "path": str(path),
        "relativePath": RUNTIME_CONFIGURATION["relativePath"],
        "directoryMode": oct(directory.stat().st_mode & 0o777),
        "fileMode": oct(path.stat().st_mode & 0o777),
        "bytesBase64": original.encoded(data), "sha256": original.sha256(data),
        "environment": {key: environment[key] for key in RUNTIME_CONFIGURATION["environment"]},
    }


def runtime_probe(binary, args, environment):
    try:
        process = subprocess.run([str(binary), *args], env=environment, capture_output=True, timeout=30)
        stdout, stderr, code = process.stdout, process.stderr, process.returncode
    except subprocess.TimeoutExpired as error:
        stdout, stderr, code = error.stdout or b"", error.stderr or b"", None
    return {
        "argv": ["verified-runtime", *args], "exitCode": code,
        "stdoutBase64": original.encoded(stdout), "stderrBase64": original.encoded(stderr),
        "stdoutSha256": original.sha256(stdout), "stderrSha256": original.sha256(stderr),
    }


def require_probe(probe, expected_stdout, detail):
    if (probe["exitCode"] != 0 or original.encoded(expected_stdout.encode()) != probe["stdoutBase64"]
            or probe["stderrBase64"]):
        raise ValueError(detail + "; exact streams retained")


def consume_runtime(root, progress):
    # Only the spec-local Go provisioner supplies this same-VM path. The entire
    # fixed compressed archive is authenticated again before any parsing or chmod.
    supplied = original.prepared_path(os.environ.get("S1_B09_B12_DOLT_ARCHIVE", ""))
    if not 0 < supplied.stat().st_size <= 256 * 1024 * 1024:
        raise ValueError("pinned runtime archive size is invalid")
    archive = root / "dolt-runtime.tar.gz"
    shutil.copyfile(supplied, archive)
    archive_data = archive.read_bytes()
    if original.sha256(archive_data) != RUNTIME["archiveSha256"]:
        raise ValueError("pinned runtime archive digest differs")
    payload = None
    with tarfile.open(archive, "r:gz") as stream:
        for item in stream:
            if item.name != RUNTIME["member"]:
                continue
            if payload is not None or not item.isreg() or not 0 < item.size <= 256 * 1024 * 1024:
                raise ValueError("runtime executable is duplicate, nonregular, empty or oversized")
            handle = stream.extractfile(item)
            if handle is None:
                raise ValueError("runtime executable payload missing")
            payload = handle.read()
            if len(payload) != item.size:
                raise ValueError("runtime executable payload is truncated")
    if payload is None:
        raise ValueError("fixed runtime executable member is missing")
    directory = root / "runtime-bin"
    directory.mkdir(mode=0o700)
    binary = directory / "dolt"
    with binary.open("xb") as output:
        output.write(payload)
    binary.chmod(0o700)
    progress.update({
        "mode": RUNTIME["mode"], "version": RUNTIME["version"],
        "archive": {"sha256": original.sha256(archive_data), "bytes": len(archive_data), "url": RUNTIME["url"]},
        "binary": {"sha256": original.sha256(payload), "bytes": len(payload), "mode": "0o700", "member": RUNTIME["member"]},
    })
    environment = recorder.isolated_environment(root / "version-environment", "s1-runtime-version")
    progress["configuration"] = configure_runtime(environment)
    progress["configurationProbe"] = runtime_probe(binary, RUNTIME["configurationProbe"], environment)
    require_probe(progress["configurationProbe"], RUNTIME["configurationStdout"], "fixed runtime configuration probe failed")
    progress["versionProbe"] = runtime_probe(binary, ["version"], environment)
    require_probe(progress["versionProbe"], RUNTIME["versionStdout"], "fixed runtime version probe failed")
    return binary


class ProxiedRecorder(original.LosslessRecorder):
    def __init__(self, binary, source, card, runtime_binary):
        if card not in CARDS:
            raise ValueError("card is not in the selected proxied profile")
        super().__init__(binary, source, card)
        self.env["PATH"] = str(runtime_binary.parent) + ":/usr/bin:/bin"
        self.runtime_configuration = configure_runtime(self.env)

    def init(self):
        self.run("init", [*RUNTIME["init"], "--prefix", self.card.lower(), "--quiet"])

    def selection(self):
        subprocess.run([recorder.GIT, "init", "-q"], cwd=self.workspace,
                       env=self.env, check=True, capture_output=True)
        self.init()
        return getattr(self, "card_" + self.card.lower())()


def capture_one(card, binary, source, runtime_binary, progress):
    current = ProxiedRecorder(binary, source, card, runtime_binary)
    progress.update({"card": card, "workspaceId": current.workspace.name, "engine": ENGINE,
                     "runtimeConfiguration": current.runtime_configuration})
    failure, selected, stopped = None, None, False
    try:
        selected = current.selection()
    except Exception as error:
        failure = error
    finally:
        # Public lifecycle command, scoped by cwd and the same private env. Never
        # remove an owned workspace until shutdown is verified from raw output.
        try:
            shutdown = current.json("cleanup-stop", RUNTIME["shutdown"])
            if shutdown.get("stopped") is not True or shutdown.get("verified") is not True:
                raise ValueError("owned proxied runtime shutdown was not verified")
            stopped = True
        except Exception as error:
            progress["cleanupError"] = {"type": type(error).__name__, "message": str(error)}
            if failure is None:
                failure = error
        progress.update({"commands": current.commands, "streams": current.streams,
                         "shutdownVerified": stopped, "workspaceRetained": not stopped})
        if stopped:
            shutil.rmtree(current.workspace)
    if failure is not None:
        raise failure
    result = {
        "schema": RUN_SCHEMA, "card": card, "engine": ENGINE,
        "sourceCommit": recorder.PIN, "sourceTree": recorder.TREE,
        "binarySha256": original.sha256(binary.read_bytes()),
        "runtimeBinarySha256": original.sha256(runtime_binary.read_bytes()),
        "runtimeConfiguration": current.runtime_configuration,
        "workspaceId": current.workspace.name, "shutdownVerified": True,
        "commands": current.commands, "streams": current.streams,
        "observations": selected["observations"], "projection": selected["projection"],
        "limits": selected["limits"], "sourceTests": SOURCE_TESTS[card],
        "sourceRefs": recorder.source_evidence(source, card),
    }
    result["rawStreamsSha256"] = recorder.digest(result["streams"])
    result["normalizedTranscriptSha256"] = recorder.digest({"commands": result["commands"], "observations": result["observations"]})
    result["projectionSha256"] = recorder.digest(result["projection"])
    return result


def main():
    if len(sys.argv) != 1:
        raise SystemExit("capture accepts only the fixed same-run prepared inputs")
    recorder.WORK_ROOT.mkdir(parents=True, exist_ok=True)
    packet = {
        "schema": PACKET_SCHEMA, "status": "BLOCK", "profile": PROFILE,
        "profileSha256": recorder.digest(PROFILE), "qualification": QUALIFICATION,
        "selectedCards": list(CARDS), "unexecuted": UNEXECUTED,
        "recordings": [], "inProgress": {}, "runtime": {},
    }
    root = Path(tempfile.mkdtemp(prefix="s1-proxied-capture-", dir=recorder.WORK_ROOT))
    try:
        try:
            packet["candidate"] = candidate_identity()
            binary, source, manifest, packet["bundle"] = original.consume_bundle(root)
            packet["source"] = original.source_packet(source, manifest)
            packet["runtimeSource"] = runtime_source(source)
            runtime_binary = consume_runtime(root, packet["runtime"])
            packet["configuration"] = {
                "inheritAmbient": False, "environment": recorder.ENVIRONMENT,
                "gitConfiguration": recorder.GIT_CONFIGURATION,
                "runtimePath": "verified private runtime-bin prepended to /usr/bin:/bin",
                "runtimeConfiguration": RUNTIME_CONFIGURATION,
                "workspaces": "two new private Git-backed managed-proxied stores per selected card",
            }
            packet["configurationSha256"] = recorder.digest(packet["configuration"])
            for card in CARDS:
                result = {"card": card}
                packet["recordings"].append(result)
                for name in ("first", "repeat"):
                    packet["inProgress"] = {"card": card, "run": name}
                    result[name] = capture_one(card, binary, source, runtime_binary, packet["inProgress"])
                    packet["inProgress"] = {}
                result["equality"] = recorder.compare_runs(result["first"], result["repeat"])
                result["disposition"] = {"status": "keep", "source": "engdocs/design/plug/spec/behaviors/content.md#" + card}
            packet["status"] = "PASS"
            from extract_proxied_reference_packet import validate_packet
            validate_packet(packet, packet["candidate"])
        except Exception as error:
            packet["status"] = "BLOCK"
            packet["error"] = {"type": type(error).__name__, "message": str(error)}
        original.emit_packet(packet)
    finally:
        # A failed public shutdown retains its owned workspace and the verified
        # executable it still needs; the disposable RunsOn VM is the boundary.
        if not packet["inProgress"].get("workspaceRetained", False):
            shutil.rmtree(root, ignore_errors=True)
    return 0 if packet["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
