#!/usr/bin/env python3
"""Create and verify the public B1c executable bundle.

The bundle contains only the two public executables and a provenance manifest.
Verification extracts the archive into a temporary directory and refuses path
traversal, links, special files, duplicate members, unexpected members, or
hash/size/mode mismatches before a caller can use either executable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import stat
import tarfile
import tempfile


SCHEMA = "plug-public-bundle/1"
ARTIFACTS = {"bin/bd", "bin/plug"}


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def safe_member(name: str) -> bool:
    path = PurePosixPath(name)
    return (
        bool(name)
        and not path.is_absolute()
        and str(path) == name
        and all(part not in {"", ".", ".."} for part in path.parts)
    )


def contained(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    root_resolved = root.resolve()
    if candidate != root_resolved and root_resolved not in candidate.parents:
        raise ValueError("path escapes extraction root")
    return candidate


def artifact_record(path: Path) -> dict[str, object]:
    mode = stat.S_IMODE(path.stat().st_mode)
    if path.is_symlink() or not path.is_file() or mode != 0o755:
        raise ValueError(f"artifact is not a regular executable: {path}")
    return {"sha256": digest(path), "size": path.stat().st_size, "mode": oct(mode)}


def verify_artifact(path: Path, relative: str, record: object) -> None:
    if not isinstance(record, dict):
        raise ValueError(f"artifact record is not an object: {relative}")
    if record.get("mode") != "0o755":
        raise ValueError(f"artifact mode is not the fixed executable mode: {relative}")
    actual = artifact_record(path)
    if actual != record:
        raise ValueError(f"artifact digest, size, or mode mismatch: {relative}")


def create(args: argparse.Namespace) -> None:
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "bin").mkdir()
    for relative in sorted(ARTIFACTS):
        source = Path(args.bd if relative.endswith("bd") else args.plug).resolve()
        destination = output / relative
        destination.write_bytes(source.read_bytes())
        destination.chmod(0o755)

    metadata = read_json(Path(args.metadata))
    if not isinstance(metadata, dict):
        raise ValueError("metadata must be an object")
    source_manifests: dict[str, dict[str, object]] = {}
    for name, commit, tree, source_manifest in (
        ("beads", args.beads_commit, args.beads_tree, args.beads_manifest),
        ("plug", args.plug_commit, args.plug_tree, args.plug_manifest),
    ):
        manifest_path = Path(source_manifest)
        source_manifests[name] = {
            "commit": commit,
            "tree": tree,
            "manifest_sha256": digest(manifest_path),
            "manifest_entries": sum(1 for line in manifest_path.read_text(encoding="utf-8").splitlines() if line),
        }

    manifest: dict[str, object] = {
        "schema": SCHEMA,
        "source": source_manifests,
        "targets": {
            "beads_importer": {"label": "./cmd/bd", "binary": "bin/bd"},
            "plug_runtime": {"label": args.plug_target, "binary": "bin/plug"},
        },
        "build": metadata.get("build", {}),
        "toolchain": metadata.get("toolchain", {}),
        "resolution": metadata.get("resolution", {}),
        "artifacts": {relative: artifact_record(output / relative) for relative in sorted(ARTIFACTS)},
    }

    archive = output / "bundle.tar"
    with tarfile.open(archive, "w", format=tarfile.GNU_FORMAT) as stream:
        for relative in sorted(ARTIFACTS):
            source = output / relative
            info = stream.gettarinfo(str(source), arcname=relative)
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            info.mode = 0o755
            with source.open("rb") as payload:
                stream.addfile(info, payload)
    manifest["archive"] = {"name": archive.name, "sha256": digest(archive), "size": archive.stat().st_size}
    write_json(output / "manifest.json", manifest)
    write_json(
        output / "receipt.json",
        {
            "schema": SCHEMA,
            "source": source_manifests,
            "targets": manifest["targets"],
            "build": manifest["build"],
            "toolchain": manifest["toolchain"],
            "resolution": manifest["resolution"],
            "artifacts": manifest["artifacts"],
            "archive": manifest["archive"],
        },
    )


def verify(args: argparse.Namespace) -> None:
    root = Path(args.directory).resolve()
    manifest_path = root / "manifest.json"
    archive = root / "bundle.tar"
    manifest = read_json(manifest_path)
    if not isinstance(manifest, dict) or manifest.get("schema") != SCHEMA:
        raise ValueError("unsupported bundle manifest")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != ARTIFACTS:
        raise ValueError("artifact set is not exact")
    for relative in sorted(ARTIFACTS):
        verify_artifact(contained(root, relative), relative, artifacts[relative])
    archive_record = manifest.get("archive")
    if not isinstance(archive_record, dict) or archive_record.get("name") != archive.name:
        raise ValueError("archive record is missing")
    if digest(archive) != archive_record.get("sha256") or archive.stat().st_size != archive_record.get("size"):
        raise ValueError("archive digest or size mismatch")

    with tempfile.TemporaryDirectory(prefix="plug-public-bundle-") as temporary:
        extraction = Path(temporary)
        with tarfile.open(archive, "r") as stream:
            members = stream.getmembers()
            names = [member.name for member in members]
            if len(names) != len(set(names)) or set(names) != ARTIFACTS:
                raise ValueError("archive members are not exact")
            for member in members:
                if not safe_member(member.name) or not member.isreg():
                    raise ValueError("archive contains an unsafe member")
                record = artifacts[member.name]
                if not isinstance(record, dict) or stat.S_IMODE(member.mode) != 0o755:
                    raise ValueError("archive member mode is not the fixed executable mode")
                destination = contained(extraction, member.name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                payload = stream.extractfile(member)
                if payload is None:
                    raise ValueError("archive member has no payload")
                destination.write_bytes(payload.read())
                destination.chmod(member.mode & 0o777)

        for relative, record in artifacts.items():
            verify_artifact(contained(extraction, relative), relative, record)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    subparsers = root.add_subparsers(dest="command", required=True)
    make = subparsers.add_parser("create")
    make.add_argument("--output", type=Path, required=True)
    make.add_argument("--bd", required=True)
    make.add_argument("--plug", required=True)
    make.add_argument("--metadata", required=True)
    make.add_argument("--beads-commit", required=True)
    make.add_argument("--beads-tree", required=True)
    make.add_argument("--beads-manifest", required=True)
    make.add_argument("--plug-commit", required=True)
    make.add_argument("--plug-tree", required=True)
    make.add_argument("--plug-manifest", required=True)
    make.add_argument("--plug-target", required=True)
    make.set_defaults(function=create)
    check = subparsers.add_parser("verify")
    check.add_argument("--directory", type=Path, required=True)
    check.set_defaults(function=verify)
    return root


if __name__ == "__main__":
    arguments = parser().parse_args()
    arguments.function(arguments)
