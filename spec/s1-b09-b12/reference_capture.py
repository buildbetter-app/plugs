#!/usr/bin/env python3
"""Manual RunsOn consumer of this run's fixed, prepared public9d6 inputs.

No downloads, user-selected hashes, legacy-binary substitution or host fallback.
The original reviewed recorder is reused without changing its legacy main().
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile

import record_reference as recorder
from public_reference_profile import (
    BEGIN, CARDS, END, MAX_PACKET_BYTES, MODULES, PACKET_SCHEMA, PROFILE,
    QUALIFICATION, SOURCE_TESTS,
)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def encoded(data):
    return base64.b64encode(data).decode("ascii")


def safe_name(value):
    path = PurePosixPath(value)
    return (bool(value) and str(path) == value and not path.is_absolute()
            and all(part not in {"", ".", ".."} for part in path.parts)
            and not any(ord(char) < 32 or ord(char) == 127 for char in value)
            and "\\" not in value)


def prepared_path(value, *, directory=False):
    if not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("prepared input is missing or malformed")
    path = Path(value)
    if (not path.is_absolute() or str(path) != value or path.resolve(strict=True) != path
            or (not path.is_dir() if directory else not path.is_file())):
        raise ValueError("prepared input must be an absolute ordinary path without symlinks")
    return path


def candidate_identity():
    root = prepared_path(os.environ.get("PLUG_SOURCE_MODULE_ROOT", ""), directory=True)
    commit = recorder.source_git(root, "rev-parse", "HEAD").decode().strip()
    tree = recorder.source_git(root, "rev-parse", "HEAD^{tree}").decode().strip()
    modules = {}
    for name in MODULES:
        path = "spec/s1-b09-b12/" + name
        pinned = recorder.source_git(root, "cat-file", "blob", commit + ":" + path)
        # Go embeds the capture files and writes them into a private directory.
        # Compare those actual executed bytes with the runner's pinned candidate.
        if Path(__file__).with_name(name).read_bytes() != pinned:
            raise ValueError("capture module differs from the pinned candidate: " + name)
        modules[name] = sha256(pinned)
    if modules["record_reference.py"] != PROFILE["reviewedRecorderSha256"]:
        raise ValueError("reviewed recorder changed")
    return {"commit": commit, "tree": tree, "modules": modules}


def snapshot_source(supplied, root):
    source = root / "source"
    if root.resolve().is_relative_to(supplied):
        raise ValueError("prepared source contains the private capture directory")
    shutil.copytree(supplied, source, symlinks=True)
    git_dir = source / ".git"
    if (git_dir.is_symlink() or not git_dir.is_dir()
            or any(path.is_symlink() or not (path.is_dir() or path.is_file()) for path in git_dir.rglob("*"))
            or any((git_dir / name).exists() for name in (
                "commondir", "gitdir", "FETCH_HEAD", "logs",
                "objects/info/alternates", "objects/info/http-alternates",
            ))):
        raise ValueError("source snapshot contains external or unsupported Git state")
    config = recorder.source_git(source, "config", "--local", "--no-includes", "--list").decode().splitlines()
    allowed = {"core.repositoryformatversion", "core.filemode", "core.bare", "core.logallrefupdates"}
    if (any(line.split("=", 1)[0].lower() not in allowed for line in config)
            or "core.filemode=true" not in config or "core.bare=false" not in config):
        raise ValueError("source snapshot configuration is not the prepared configuration")
    recorder.verify_source(source)
    if recorder.source_git(source, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching"):
        raise ValueError("source snapshot contains ignored or changed files")
    if any(line[:2] != b"H " for line in recorder.source_git(source, "ls-files", "-v").splitlines()):
        raise ValueError("source snapshot contains hidden index entries")
    manifest = recorder.source_git(source, "ls-tree", "-r", "--full-tree", "HEAD")
    if (sha256(manifest) != PROFILE["source"]["manifestSha256"]
            or len(manifest.splitlines()) != PROFILE["source"]["manifestEntries"]):
        raise ValueError("source snapshot manifest differs from the fixed source")
    entries = recorder.source_git(source, "ls-tree", "-rz", "--full-tree", "HEAD")
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.split()
        relative = os.fsdecode(name)
        if kind != b"blob" or not safe_name(relative):
            raise ValueError("unsupported source tree entry")
        path = source / relative
        actual = path.lstat().st_mode
        if not path.resolve(strict=True).is_relative_to(source.resolve()):
            raise ValueError("source link escapes the private snapshot")
        if mode in {b"100644", b"100755"} and stat.S_ISREG(actual):
            if bool(actual & 0o111) != (mode == b"100755"):
                raise ValueError("source executable mode differs from its tree")
            contents = path.read_bytes()
        elif mode == b"120000" and stat.S_ISLNK(actual):
            contents = os.fsencode(os.readlink(path))
        else:
            raise ValueError("source file type differs from its tree")
        if hashlib.sha1(b"blob " + str(len(contents)).encode() + b"\0" + contents).hexdigest().encode() != oid:
            raise ValueError("source bytes differ from the pinned object")
    return source, manifest


def consume_bundle(root):
    if (os.environ.get("PLUG_PUBLIC_REFERENCE") != PROFILE["descriptor"]
            or os.environ.get("PLUG_PUBLIC_REFERENCE_BUILD_RUN") != PROFILE["producer"]["run"]):
        raise ValueError("fixed same-run preparation descriptor/build identity is absent")
    original = prepared_path(os.environ.get("PLUG_PUBLIC_REFERENCE_ZIP", ""))
    manifest_input = prepared_path(os.environ.get("PLUG_PUBLIC_REFERENCE_MANIFEST", ""))
    supplied = prepared_path(os.environ.get("PLUG_PUBLIC_REFERENCE_SOURCE", ""), directory=True)
    if original.stat().st_size != PROFILE["zip"]["size"]:
        raise ValueError("prepared ZIP size differs from the fixed public build")
    archive = root / "artifact.zip"
    shutil.copyfile(original, archive)
    if sha256(archive.read_bytes()) != PROFILE["zip"]["sha256"]:
        raise ValueError("prepared ZIP digest differs from the fixed public build")
    with zipfile.ZipFile(archive) as bundle:
        members = bundle.infolist()
        names = [item.filename for item in members]
        files = {"bin/bd", "bin/plug", "manifest.json", "receipt.json", "bundle.tar"}
        if (len(names) != len(set(names)) or {item.filename for item in members if not item.is_dir()} != files
                or any(not safe_name(item.filename.rstrip("/"))
                       or (item.is_dir() and item.filename != "bin/")
                       or stat.S_IFMT(item.external_attr >> 16) not in {0, stat.S_IFDIR if item.is_dir() else stat.S_IFREG}
                       for item in members)):
            raise ValueError("prepared ZIP members are not the fixed ordinary member set")
        manifest_bytes = bundle.read("manifest.json")
        receipt_bytes = bundle.read("receipt.json")
        if sha256(manifest_bytes) != PROFILE["manifestSha256"] or manifest_input.read_bytes() != manifest_bytes:
            raise ValueError("prepared manifest differs from the fixed public manifest")
        manifest = json.loads(manifest_bytes)
        from extract_reference_packet import validate_manifest
        validate_manifest(manifest)
        if json.loads(receipt_bytes) != manifest:
            raise ValueError("prepared receipt and manifest differ")
        for name, record in manifest["artifacts"].items():
            data = bundle.read(name)
            if len(data) != record["size"] or sha256(data) != record["sha256"]:
                raise ValueError("prepared loose executable differs from its manifest")
        tar_bytes = bundle.read("bundle.tar")
        if len(tar_bytes) != PROFILE["archive"]["size"] or sha256(tar_bytes) != PROFILE["archive"]["sha256"]:
            raise ValueError("prepared tar differs from the fixed public archive")
        tar_path = root / "bundle.tar"
        tar_path.write_bytes(tar_bytes)
    # Source and archive validation finish before any execute permission exists.
    source, source_manifest = snapshot_source(supplied, root)
    binary = root / "bd"
    seen = set()
    with tarfile.open(tar_path, "r:") as stream:
        for item in stream:
            if item.name not in manifest["artifacts"] or item.name in seen:
                raise ValueError("canonical archive member set differs")
            seen.add(item.name)
            expected = manifest["artifacts"][item.name]
            if not item.isreg() or item.mode != 0o755 or item.size != expected["size"]:
                raise ValueError("canonical executable type/mode/size differs")
            payload = stream.extractfile(item)
            if payload is None:
                raise ValueError("canonical executable payload missing")
            data = payload.read()
            if sha256(data) != expected["sha256"] or len(data) != expected["size"]:
                raise ValueError("canonical executable digest differs")
            if item.name == "bin/bd":
                binary.write_bytes(data)
                binary.chmod(0o600)
    if seen != set(manifest["artifacts"]):
        raise ValueError("canonical executable missing")
    binary.chmod(0o755)
    return binary, source, source_manifest, {
        "manifestBase64": encoded(manifest_bytes), "receiptBase64": encoded(receipt_bytes),
        "receiptSha256": sha256(receipt_bytes), "zip": PROFILE["zip"],
        "binary": PROFILE["binary"], "archive": PROFILE["archive"],
    }


def source_packet(source, manifest):
    paths = {item.split("::", 1)[0] for tests in SOURCE_TESTS.values() for item in tests}
    migrations = recorder.source_git(
        source, "ls-tree", "-r", "--name-only", recorder.PIN, "--", "internal/storage/schema/migrations",
    ).decode().splitlines()
    paths.update(path for path in migrations if path.endswith(".up.sql") and str(Path(path).parent) in {
        "internal/storage/schema/migrations", "internal/storage/schema/migrations/ignored",
    })
    blobs = []
    for path in sorted(paths):
        contents = recorder.source_git(source, "cat-file", "blob", recorder.PIN + ":" + path)
        blobs.append({"path": path, "sha256": sha256(contents), "bytesBase64": encoded(contents)})
    return {
        **PROFILE["source"], "cleanWorkingTree": True,
        "manifestBase64": encoded(manifest), "evidenceBlobs": blobs,
        "schemaEvidence": recorder.schema_evidence(source),
    }


class LosslessRecorder(recorder.Recorder):
    """Also retain partial output when a source-engine command times out."""

    def run(self, label, args, expected=0, stdin=None):
        try:
            return super().run(label, args, expected, stdin)
        except subprocess.TimeoutExpired as error:
            stdout, stderr = error.stdout or b"", error.stderr or b""
            self.streams.append({
                "label": label, "stdoutBase64": encoded(stdout), "stderrBase64": encoded(stderr),
                "stdoutSha256": sha256(stdout), "stderrSha256": sha256(stderr),
            })
            self.commands.append({
                "label": label, "argv": recorder.normalize(["reference-engine", *args], self.card),
                "exitCode": None, "timedOut": True,
                "stdout": recorder.normalize(stdout.decode(errors="replace"), self.card),
                "stderr": recorder.normalize(stderr.decode(errors="replace"), self.card),
            })
            raise RuntimeError("source-engine command timed out: " + label) from None


def capture_one(card, binary, source, progress):
    current = LosslessRecorder(binary, source, card)
    progress.update({"card": card, "workspaceId": current.workspace.name})
    try:
        result = current.record()
        result["workspaceId"] = current.workspace.name
        result["sourceTests"] = SOURCE_TESTS[card]
        result["sourceRefs"] = recorder.source_evidence(source, card)
        return result
    except Exception:
        # record() and json() may fail after an actual engine command. Keep its
        # original streams, including nonzero exits, in the BLOCK packet.
        progress.update({"commands": current.commands, "streams": current.streams})
        raise
    finally:
        shutil.rmtree(current.workspace, ignore_errors=True)


def emit_packet(packet):
    payload = canonical(packet)
    if len(payload) > MAX_PACKET_BYTES:
        raise ValueError("lossless packet exceeds the retention bound")
    identity = str(len(payload)).encode("ascii") + b" " + sha256(payload).encode("ascii")
    sys.stdout.buffer.write(BEGIN + b" " + identity + b"\n" + payload + b"\n" + END + b" " + identity + b"\n")
    sys.stdout.buffer.flush()


def main():
    if len(sys.argv) != 1:
        raise SystemExit("capture accepts only the fixed prepared environment, without arguments")
    recorder.WORK_ROOT.mkdir(parents=True, exist_ok=True)
    packet = {
        "schema": PACKET_SCHEMA, "status": "BLOCK", "profile": PROFILE,
        "profileSha256": sha256(canonical(PROFILE)), "qualification": QUALIFICATION,
        "recordings": [], "inProgress": {},
    }
    with tempfile.TemporaryDirectory(prefix="s1-b09-b12-capture-", dir=recorder.WORK_ROOT) as directory:
        root = Path(directory)
        try:
            if recorder.SOURCE_TESTS != SOURCE_TESTS:
                raise ValueError("source-test selection differs from the reviewed recorder")
            packet["candidate"] = candidate_identity()
            binary, source, manifest, bundle = consume_bundle(root)
            packet["bundle"] = bundle
            packet["source"] = source_packet(source, manifest)
            packet["configuration"] = {
                "inheritAmbient": False, "environment": recorder.ENVIRONMENT,
                "gitConfiguration": recorder.GIT_CONFIGURATION,
                "workspaces": "two new private Git-backed stores per card",
            }
            packet["configurationSha256"] = sha256(canonical(packet["configuration"]))
            for card in CARDS:
                result = {"card": card}
                packet["recordings"].append(result)
                for name in ("first", "repeat"):
                    packet["inProgress"] = {"card": card, "run": name}
                    result[name] = capture_one(card, binary, source, packet["inProgress"])
                    packet["inProgress"] = {}
                result["equality"] = recorder.compare_runs(result["first"], result["repeat"])
                result["disposition"] = {
                    "status": "deviate" if card == "B12" else "keep",
                    "source": "engdocs/design/plug/spec/behaviors/content.md#" + card,
                }
            packet["status"] = "PASS"
            # A separate implementation rederives transcripts and projections
            # from bytes before the packet can be labelled successful.
            from extract_reference_packet import validate_packet
            validate_packet(packet, packet["candidate"])
        except Exception as error:
            packet["status"] = "BLOCK"
            packet["error"] = {"type": type(error).__name__, "message": str(error)}
        emit_packet(packet)
    return 0 if packet["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
