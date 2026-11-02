#!/usr/bin/env python3
"""Prepare one pinned public companion artifact for an opt-in RunsOn test.

The privileged entry point is fetched from the workflow-control commit. It
uses the job-local read token only through gh; downloaded executables are
never run here and no credential is written or forwarded to Bazel.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tarfile
import zipfile


REPOSITORY = "buildbetter-app/plugs"
WORKFLOW = ".github/workflows/plug-public-bundle.yml"
PROFILE = "plug-public-companion/1"
MARKER = "--test_env=PLUG_PUBLIC_BUNDLE="
DESCRIPTOR = re.compile(r"([1-9][0-9]{0,19}):([1-9][0-9]{0,19}):([0-9a-f]{64})\Z")
BEADS_COMMIT = "682ca798dab448803ea9b74f7da3715c21d8c3ae"
BEADS_TREE = "9d026de307ee915953b247cf42758d0c53303574"
PLUG_COMMIT = "83fe6cd8efdbbc6a06194d3fc8b675a4b713baa1"
PLUG_TREE = "79bff38788a8470d1bee30043884ca37c095307b"
SOURCES = {
    "beads": (BEADS_COMMIT, BEADS_TREE, "778d1227069b60685f1f40b2934d8155a954ebc452dd25bc465d5d2bf1649194"),
    "plug": (PLUG_COMMIT, PLUG_TREE, "cd9f25478d0d023982d6065cb0b3186ccaa45b85621961fb95eeeb360aafccef"),
}
SOURCE_REFS = {
    "beads": "refs/remotes/origin/beads",
    "plug": "codex/b1c-roundtrip-source-83fe6cd8-20261102",
}
TARGETS = {
    "beads_exporter": {"label": "//cmd/bd:bd", "binary": "bin/bd-export"},
    "plug_roundtrip": {"label": "//tools/roundtrip:roundtrip", "binary": "bin/roundtrip"},
}
BINARIES = {"bin/bd-export", "bin/roundtrip"}
FILES = BINARIES | {"manifest.json", "receipt.json", "bundle.tar", "source/beads.manifest", "source/plug.manifest"}
MAX_FILE_BYTES = 1024 * 1024 * 1024
MAX_ARTIFACT_BYTES = 3 * MAX_FILE_BYTES


def parse_descriptor(value: str) -> tuple[str, str, str]:
    match = DESCRIPTOR.fullmatch(value)
    if match is None:
        raise ValueError("public bundle must be RUN_ID:ARTIFACT_ID:lowercase_MANIFEST_SHA256")
    return match.groups()


def descriptor_from_args(value: str) -> str:
    arguments = value.split()
    candidates = [arg for arg in arguments if "PLUG_PUBLIC_BUNDLE" in arg]
    if not candidates:
        return ""
    if len(candidates) != 1 or not candidates[0].startswith(MARKER):
        raise ValueError("public bundle marker must occur exactly once in its fixed test_env form")
    descriptor = candidates[0][len(MARKER):]
    parse_descriptor(descriptor)
    command = next((arg for arg in arguments if not arg.startswith("-")), "")
    if command not in {"test", "coverage"}:
        raise ValueError("public executable artifacts are available only to test or coverage")
    if any(arg.startswith("--test_env=PLUG_PUBLIC_BEADS_") for arg in arguments):
        raise ValueError("prepared public executable environment cannot be supplied by the caller")
    return descriptor


def digest_stream(stream) -> str:
    result = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b""):
        result.update(block)
    return result.hexdigest()


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return digest_stream(stream)


def safe_name(value: str) -> bool:
    path = PurePosixPath(value)
    return bool(value) and not path.is_absolute() and str(path) == value and all(part not in {"", ".", ".."} for part in path.parts)


def validate_metadata(run: dict, artifact: dict, run_id: str, artifact_id: str) -> None:
    if (str(run.get("id")) != run_id or run.get("status") != "completed"
            or run.get("conclusion") != "success" or run.get("event") != "workflow_dispatch"
            or run.get("path") != WORKFLOW or run.get("repository", {}).get("full_name") != REPOSITORY
            or run.get("head_repository", {}).get("full_name") != REPOSITORY
            or re.fullmatch(r"[0-9a-f]{40}", str(run.get("head_sha"))) is None):
        raise ValueError("public bundle producer is not the successful fixed same-repository workflow")
    binding = artifact.get("workflow_run", {})
    if (str(artifact.get("id")) != artifact_id or artifact.get("expired") is not False
            or str(binding.get("id")) != run_id or binding.get("head_sha") != run["head_sha"]
            or re.fullmatch(r"plug-public-bundle-[A-Za-z0-9._-]{1,48}-" + run_id, str(artifact.get("name"))) is None
            or not isinstance(artifact.get("size_in_bytes"), int)
            or not 0 < artifact["size_in_bytes"] <= MAX_ARTIFACT_BYTES
            or re.fullmatch(r"sha256:[0-9a-f]{64}", str(artifact.get("digest"))) is None):
        raise ValueError("public bundle artifact is not bound to the immutable producer run")


def validate_manifest(manifest: dict, run: dict, run_id: str) -> None:
    if manifest.get("schema") != PROFILE or manifest.get("targets") != TARGETS or manifest.get("source_refs") != SOURCE_REFS:
        raise ValueError("public companion profile, targets or source refs differ from selected pins")
    if manifest.get("workflow") != {"ref": "refs/heads/" + run["head_branch"], "sha": run["head_sha"], "profile": PROFILE}:
        raise ValueError("public companion workflow-control binding mismatch")
    if manifest.get("build", {}).get("run_id") != run_id:
        raise ValueError("public companion build run mismatch")
    sources = manifest.get("source")
    if not isinstance(sources, dict) or set(sources) != set(SOURCES):
        raise ValueError("public companion canonical source set mismatch")
    for name, (commit, tree, source_digest) in SOURCES.items():
        record = sources[name]
        if (not isinstance(record, dict) or record.get("commit") != commit or record.get("tree") != tree
                or record.get("manifest_sha256") != source_digest
                or not isinstance(record.get("manifest_entries"), int) or record["manifest_entries"] <= 0):
            raise ValueError("public companion canonical source SHA/tree/manifest mismatch: " + name)
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != BINARIES:
        raise ValueError("public companion executable set is not exact")
    for record in artifacts.values():
        if (not isinstance(record, dict) or set(record) != {"sha256", "size", "mode"}
                or record.get("mode") != "0o755" or re.fullmatch(r"[0-9a-f]{64}", str(record.get("sha256"))) is None
                or not isinstance(record.get("size"), int) or not 0 < record["size"] <= MAX_FILE_BYTES):
            raise ValueError("public companion executable digest/size/mode record is invalid")
    archive = manifest.get("archive", {})
    if (archive.get("name") != "bundle.tar" or not isinstance(archive.get("size"), int)
            or not 0 < archive["size"] <= MAX_ARTIFACT_BYTES
            or re.fullmatch(r"[0-9a-f]{64}", str(archive.get("sha256"))) is None):
        raise ValueError("public companion archive digest/size record is invalid")
    version = manifest.get("resolution", {}).get("bd_export_version", {})
    if version.get("commit") != BEADS_COMMIT or version.get("build") != "public-" + BEADS_COMMIT:
        raise ValueError("public companion sealed executable identity mismatch")


def prepare_archive(archive: Path, output: Path, manifest_sha256: str, run: dict, run_id: str) -> dict:
    """Validate both containers before exposing canonical regular executables."""
    output.mkdir(parents=True, exist_ok=False)
    with zipfile.ZipFile(archive) as stream:
        members = stream.infolist()
        names = [member.filename for member in members if not member.is_dir()]
        if len(members) != len({member.filename for member in members}) or len(names) != len(set(names)) or set(names) != FILES:
            raise ValueError("public artifact ZIP file set is not exact")
        for member in members:
            name = member.filename.rstrip("/") if member.is_dir() else member.filename
            mode = member.external_attr >> 16
            kind = stat.S_IFMT(mode)
            if (not safe_name(name) or (member.is_dir() and name not in {"bin", "source"})
                    or kind not in {0, stat.S_IFDIR if member.is_dir() else stat.S_IFREG}
                    or member.file_size > MAX_ARTIFACT_BYTES):
                raise ValueError("public artifact ZIP contains an unsafe member")
        if sum(member.file_size for member in members) > MAX_ARTIFACT_BYTES:
            raise ValueError("public artifact ZIP expands beyond the public artifact bound")
        if stream.getinfo("manifest.json").file_size > 1024 * 1024 or stream.getinfo("receipt.json").file_size > 1024 * 1024:
            raise ValueError("public artifact provenance exceeds the metadata bound")
        manifest_bytes = stream.read("manifest.json")
        if hashlib.sha256(manifest_bytes).hexdigest() != manifest_sha256:
            raise ValueError("public artifact manifest checksum mismatch")
        manifest = json.loads(manifest_bytes)
        if not isinstance(manifest, dict):
            raise ValueError("public artifact manifest must be an object")
        validate_manifest(manifest, run, run_id)
        if json.loads(stream.read("receipt.json")) != manifest:
            raise ValueError("public artifact receipt differs from the pinned manifest")
        for name in BINARIES:
            record = manifest["artifacts"][name]
            if stream.getinfo(name).file_size != record["size"]:
                raise ValueError("public artifact loose executable size mismatch")
            with stream.open(name) as payload:
                if digest_stream(payload) != record["sha256"]:
                    raise ValueError("public artifact loose executable digest mismatch")
        for name in sorted(FILES - BINARIES):
            destination = output / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            with stream.open(name) as payload, destination.open("xb") as target:
                shutil.copyfileobj(payload, target, 1024 * 1024)

    for name, (_, _, expected_digest) in SOURCES.items():
        path = output / "source" / f"{name}.manifest"
        entries = sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line)
        if digest(path) != expected_digest or entries != manifest["source"][name]["manifest_entries"]:
            raise ValueError("public artifact canonical source manifest mismatch")
    canonical = output / "bundle.tar"
    if digest(canonical) != manifest["archive"]["sha256"] or canonical.stat().st_size != manifest["archive"]["size"]:
        raise ValueError("public artifact canonical archive checksum/size mismatch")
    with tarfile.open(canonical, "r:") as stream:
        members = stream.getmembers()
        if len(members) != 2 or {member.name for member in members} != BINARIES:
            raise ValueError("public artifact canonical archive member set is not exact")
        for member in members:
            record = manifest["artifacts"][member.name]
            if (not safe_name(member.name) or not member.isreg() or stat.S_IMODE(member.mode) != 0o755
                    or member.size != record["size"]):
                raise ValueError("public artifact canonical executable is unsafe or has the wrong size/mode")
            payload = stream.extractfile(member)
            if payload is None:
                raise ValueError("public artifact canonical executable has no payload")
            destination = output / member.name
            destination.parent.mkdir(exist_ok=True)
            with payload, destination.open("xb") as target:
                shutil.copyfileobj(payload, target, 1024 * 1024)
            if digest(destination) != record["sha256"]:
                raise ValueError("public artifact canonical executable digest mismatch")
            destination.chmod(0o755)
    return manifest


def gh_json(path: str) -> dict:
    response = subprocess.run(["gh", "api", path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if response.returncode != 0:
        raise ValueError("public artifact metadata API request failed")
    value = json.loads(response.stdout)
    if not isinstance(value, dict):
        raise ValueError("public artifact metadata API response is not an object")
    return value


def prepare(descriptor: str, output: Path, github_env: Path, evidence: Path) -> None:
    run_id, artifact_id, manifest_sha256 = parse_descriptor(descriptor)
    run = gh_json(f"repos/{REPOSITORY}/actions/runs/{run_id}")
    artifact = gh_json(f"repos/{REPOSITORY}/actions/artifacts/{artifact_id}")
    validate_metadata(run, artifact, run_id, artifact_id)
    output = output.resolve()
    archive = output.with_suffix(".zip")
    with archive.open("xb") as target:
        response = subprocess.run(
            ["gh", "api", f"repos/{REPOSITORY}/actions/artifacts/{artifact_id}/zip"],
            stdout=target, stderr=subprocess.PIPE, check=False,
        )
    if response.returncode != 0:
        raise ValueError("public artifact download failed")
    if archive.stat().st_size != artifact["size_in_bytes"] or archive.stat().st_size > MAX_ARTIFACT_BYTES or "sha256:" + digest(archive) != artifact["digest"]:
        raise ValueError("public artifact download digest mismatch")
    manifest = prepare_archive(archive, output, manifest_sha256, run, run_id)
    values = {
        "PLUG_PUBLIC_BEADS_BINARY": str(output / "bin/bd-export"),
        "PLUG_PUBLIC_BEADS_SHA256": manifest["artifacts"]["bin/bd-export"]["sha256"],
        "PLUG_PUBLIC_BEADS_BUILD_RUN": run_id,
        "PLUG_PUBLIC_BEADS_MANIFEST": str(output / "manifest.json"),
    }
    if any("\n" in value or "\r" in value for value in values.values()):
        raise ValueError("public artifact runner path is not a single environment value")
    receipt = {
        "status": "PASS", "descriptor": descriptor, "producer_run": run_id,
        "producer_workflow_sha": run["head_sha"], "artifact_id": artifact_id,
        "artifact_digest": artifact["digest"], "manifest_sha256": manifest_sha256,
        "schema": manifest["schema"], "source": manifest["source"],
        "targets": manifest["targets"], "artifacts": manifest["artifacts"],
        "archive": manifest["archive"], "environment": values,
    }
    evidence.write_text(json.dumps(receipt, sort_keys=True) + "\n", encoding="utf-8")
    # Publication occurs only after every metadata, source and payload check.
    with github_env.open("a", encoding="utf-8") as stream:
        for name, value in values.items():
            stream.write(name + "=" + value + "\n")
    print(json.dumps({"status": "PASS", "producer_run": run_id, "artifact_id": artifact_id, "manifest_sha256": manifest_sha256}))


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    request = commands.add_parser("request")
    request.add_argument("--args", required=True)
    make = commands.add_parser("prepare")
    make.add_argument("--descriptor", required=True)
    make.add_argument("--output", type=Path, required=True)
    make.add_argument("--github-env", type=Path, required=True)
    make.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "request":
        print(descriptor_from_args(args.args))
    else:
        prepare(args.descriptor, args.output, args.github_env, args.evidence)


if __name__ == "__main__":
    main()
