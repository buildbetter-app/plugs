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
COMPANION_SCHEMA = "plug-public-companion/1"

# Keep the original importer/runtime profile byte-for-byte compatible at the
# artifact boundary.  The companion profile is deliberately a separate
# schema and has its own exact two-member contract.
PROFILE_ARTIFACTS = {
    SCHEMA: {"bin/bd", "bin/plug"},
    COMPANION_SCHEMA: {"bin/bd-export", "bin/roundtrip"},
}
ARTIFACTS = PROFILE_ARTIFACTS[SCHEMA]


def profile_config(profile: str, plug_target: str) -> dict[str, object]:
    if profile == SCHEMA:
        return {
            "artifacts": PROFILE_ARTIFACTS[SCHEMA],
            "inputs": {"bin/bd": "bd", "bin/plug": "plug"},
            "targets": {
                "beads_importer": {"label": "./cmd/bd", "binary": "bin/bd"},
                "plug_runtime": {"label": plug_target, "binary": "bin/plug"},
            },
        }
    if profile == COMPANION_SCHEMA:
        if plug_target != "//tools/roundtrip:roundtrip":
            raise ValueError("companion target must be the canonical roundtrip executable")
        return {
            "artifacts": PROFILE_ARTIFACTS[COMPANION_SCHEMA],
            "inputs": {"bin/bd-export": "bd_export", "bin/roundtrip": "roundtrip"},
            "targets": {
                "beads_exporter": {"label": "//cmd/bd:bd", "binary": "bin/bd-export"},
                "plug_roundtrip": {"label": plug_target, "binary": "bin/roundtrip"},
            },
        }
    raise ValueError(f"unsupported bundle profile: {profile}")


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
    profile = getattr(args, "profile", SCHEMA)
    config = profile_config(profile, args.plug_target)
    artifacts = config["artifacts"]
    inputs = config["inputs"]
    output.mkdir(parents=True, exist_ok=False)
    (output / "bin").mkdir()
    for relative in sorted(artifacts):
        input_name = inputs[relative]
        source_name = getattr(args, input_name, None)
        if not isinstance(source_name, str) or not source_name:
            raise ValueError(f"missing executable input: {relative}")
        source = Path(source_name).resolve()
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
        if profile == COMPANION_SCHEMA:
            (output / "source").mkdir(exist_ok=True)
            (output / "source" / f"{name}.manifest").write_bytes(manifest_path.read_bytes())

    manifest: dict[str, object] = {
        "schema": profile,
        "source": source_manifests,
        "targets": config["targets"],
        "build": metadata.get("build", {}),
        "toolchain": metadata.get("toolchain", {}),
        "resolution": metadata.get("resolution", {}),
        "workflow": metadata.get("workflow", {}),
        "source_refs": metadata.get("source_refs", {}),
        "artifacts": {relative: artifact_record(output / relative) for relative in sorted(artifacts)},
    }

    archive = output / "bundle.tar"
    with tarfile.open(archive, "w", format=tarfile.GNU_FORMAT) as stream:
        for relative in sorted(artifacts):
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
            "schema": manifest["schema"],
            "source": source_manifests,
            "targets": manifest["targets"],
            "build": manifest["build"],
            "toolchain": manifest["toolchain"],
            "resolution": manifest["resolution"],
            "workflow": manifest["workflow"],
            "source_refs": manifest["source_refs"],
            "artifacts": manifest["artifacts"],
            "archive": manifest["archive"],
        },
    )


def verify_provenance(manifest: dict[str, object], expected_path: Path) -> None:
    """Compare downloaded provenance with the dispatch's independent pins."""
    expected = read_json(expected_path)
    fields = {"schema", "source", "targets", "workflow", "source_refs"}
    if not isinstance(expected, dict) or set(expected) != fields:
        raise ValueError("expected provenance must name every source binding")
    for field in fields - {"source"}:
        if manifest.get(field) != expected[field]:
            raise ValueError(f"bundle provenance mismatch: {field}")
    sources = expected["source"]
    actual = manifest.get("source")
    if not isinstance(sources, dict) or set(sources) != {"beads", "plug"} or not isinstance(actual, dict):
        raise ValueError("expected provenance must name both canonical sources")
    for name, record in sources.items():
        if not isinstance(record, dict) or set(record) != {"commit", "tree"}:
            raise ValueError("expected source must contain commit and tree")
        if not isinstance(actual.get(name), dict) or any(actual[name].get(key) != value for key, value in record.items()):
            raise ValueError(f"bundle source binding mismatch: {name}")


def verify(args: argparse.Namespace) -> None:
    root = Path(args.directory).resolve()
    manifest_path = root / "manifest.json"
    archive = root / "bundle.tar"
    manifest = read_json(manifest_path)
    if not isinstance(manifest, dict):
        raise ValueError("unsupported bundle manifest")
    profile = manifest.get("schema")
    if not isinstance(profile, str) or profile not in PROFILE_ARTIFACTS:
        raise ValueError("unsupported bundle manifest")
    if profile == COMPANION_SCHEMA:
        if manifest.get("targets") != profile_config(profile, "//tools/roundtrip:roundtrip")["targets"]:
            raise ValueError("companion executable targets are not exact")
        sources = manifest.get("source")
        if not isinstance(sources, dict) or set(sources) != {"beads", "plug"}:
            raise ValueError("companion canonical source manifests are missing")
        for name, record in sources.items():
            path = contained(root, f"source/{name}.manifest")
            if not isinstance(record, dict) or digest(path) != record.get("manifest_sha256"):
                raise ValueError(f"canonical source manifest digest mismatch: {name}")
            entries = sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line)
            if entries != record.get("manifest_entries"):
                raise ValueError(f"canonical source manifest entry count mismatch: {name}")
    expected_provenance = getattr(args, "expected_provenance", None)
    if expected_provenance is not None:
        verify_provenance(manifest, Path(expected_provenance))
    artifacts_expected = PROFILE_ARTIFACTS[profile]
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != artifacts_expected:
        raise ValueError("artifact set is not exact")
    for relative in sorted(artifacts_expected):
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
            if len(names) != len(set(names)) or set(names) != artifacts_expected:
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
    make.add_argument("--profile", choices=tuple(PROFILE_ARTIFACTS), default=SCHEMA)
    make.add_argument("--output", type=Path, required=True)
    make.add_argument("--bd")
    make.add_argument("--plug")
    make.add_argument("--bd-export")
    make.add_argument("--roundtrip")
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
    check.add_argument("--expected-provenance", type=Path)
    check.set_defaults(function=verify)
    return root


if __name__ == "__main__":
    arguments = parser().parse_args()
    arguments.function(arguments)
