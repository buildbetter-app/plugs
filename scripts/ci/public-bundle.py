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
import re
import stat
import tarfile
import tempfile


SCHEMA = "plug-public-bundle/1"
COMPANION_SCHEMA = "plug-public-companion/1"
NATIVE_SCHEMA = "plug-public-native/1"
NATIVE_BEADS_COMMIT = "305fec3afac83224f46c54f982959fd99db136fa"
NATIVE_BEADS_TREE = "48e3545b7c5c6a5cdc0119e34dfa379354456a6e"
NATIVE_BEADS_SOURCE_REF = "refs/remotes/origin/codex/refill-beads-native-inventory-20261102-0610"
NATIVE_BEADS_MANIFEST_ENTRIES = 5141
NATIVE_BEADS_MANIFEST_SHA256 = "6c2e549b54b472790d57a27370821d69159d4d6dc817e5bf38dbc56c856c8420"

# Keep the original importer/runtime profile byte-for-byte compatible at the
# artifact boundary.  The companion profile is deliberately a separate
# schema and has its own exact two-member contract.
PROFILE_ARTIFACTS = {
    SCHEMA: {"bin/bd", "bin/plug"},
    COMPANION_SCHEMA: {"bin/bd-export", "bin/roundtrip"},
    NATIVE_SCHEMA: {"bin/bd-export"},
}
ARTIFACTS = PROFILE_ARTIFACTS[SCHEMA]


def profile_config(profile: str, plug_target: str | None) -> dict[str, object]:
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
    if profile == NATIVE_SCHEMA:
        if plug_target is not None:
            raise ValueError("native profile does not accept a Plug target")
        return {
            "artifacts": PROFILE_ARTIFACTS[NATIVE_SCHEMA],
            "inputs": {"bin/bd-export": "bd_export"},
            "targets": {
                "beads_exporter": {"label": "//cmd/bd:bd", "binary": "bin/bd-export"},
            },
        }
    raise ValueError(f"unsupported bundle profile: {profile}")


def verify_native_manifest_contract(manifest: dict[str, object]) -> None:
    expected_keys = {
        "schema", "source", "targets", "build", "toolchain", "resolution",
        "workflow", "source_refs", "artifacts", "archive",
    }
    if set(manifest) != expected_keys or manifest.get("schema") != NATIVE_SCHEMA:
        raise ValueError("native manifest keys or profile are not exact")
    source = manifest.get("source")
    if not isinstance(source, dict) or set(source) != {"beads"}:
        raise ValueError("native manifest must contain only Beads source")
    beads = source.get("beads")
    if not isinstance(beads, dict) or set(beads) != {"commit", "tree", "manifest_sha256", "manifest_entries"}:
        raise ValueError("native Beads source record is not exact")
    if beads.get("commit") != NATIVE_BEADS_COMMIT or beads.get("tree") != NATIVE_BEADS_TREE:
        raise ValueError("native Beads source identity is not exact")
    if beads.get("manifest_sha256") != NATIVE_BEADS_MANIFEST_SHA256:
        raise ValueError("native Beads source manifest digest is not the pinned manifest")
    if beads.get("manifest_entries") != NATIVE_BEADS_MANIFEST_ENTRIES:
        raise ValueError("native Beads source manifest count is not the pinned manifest")
    if manifest.get("source_refs") != {"beads": NATIVE_BEADS_SOURCE_REF}:
        raise ValueError("native source references are not exact")
    if manifest.get("targets") != {
        "beads_exporter": {"label": "//cmd/bd:bd", "binary": "bin/bd-export"},
    }:
        raise ValueError("native target binding is not exact")
    workflow = manifest.get("workflow")
    if not isinstance(workflow, dict) or set(workflow) != {"ref", "sha", "profile"} or workflow.get("profile") != NATIVE_SCHEMA:
        raise ValueError("native workflow provenance is not exact")
    if not isinstance(workflow.get("ref"), str) or not workflow["ref"] or not isinstance(workflow.get("sha"), str) or not re.fullmatch(r"[0-9a-fA-F]{40}", workflow["sha"]):
        raise ValueError("native workflow provenance is invalid")
    build = manifest.get("build")
    if not isinstance(build, dict) or set(build) != {"beads", "run_id"}:
        raise ValueError("native build provenance is not exact")
    if not isinstance(build.get("beads"), str) or not build["beads"]:
        raise ValueError("native build command is invalid")
    run_id = build.get("run_id")
    if not isinstance(run_id, str) or not re.fullmatch(r"[1-9][0-9]*", run_id):
        raise ValueError("native build.run_id must be a positive decimal string")
    toolchain = manifest.get("toolchain")
    if not isinstance(toolchain, dict) or set(toolchain) != {"go", "bazel", "bazelisk_sha256"}:
        raise ValueError("native toolchain provenance is not exact")
    if not all(isinstance(toolchain.get(key), str) and toolchain[key] for key in ("go", "bazel")):
        raise ValueError("native toolchain provenance is invalid")
    if not isinstance(toolchain.get("bazelisk_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", toolchain["bazelisk_sha256"]):
        raise ValueError("native Bazelisk provenance is invalid")
    resolution = manifest.get("resolution")
    resolution_keys = {
        "bd_export_help_sha256", "bd_export_help_exit", "bd_export_version",
        "bd_export_version_sha256", "bd_init_help_sha256", "bd_import_help_sha256",
    }
    if not isinstance(resolution, dict) or set(resolution) != resolution_keys or resolution.get("bd_export_help_exit") != 0:
        raise ValueError("native resolution provenance is not exact")
    version = resolution.get("bd_export_version")
    if not isinstance(version, dict) or version.get("commit") != NATIVE_BEADS_COMMIT or version.get("build") != "public-" + NATIVE_BEADS_COMMIT:
        raise ValueError("native executable identity is not exact")
    for key in ("bd_export_help_sha256", "bd_export_version_sha256", "bd_init_help_sha256", "bd_import_help_sha256"):
        if not isinstance(resolution.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", resolution[key]):
            raise ValueError("native probe provenance is invalid: " + key)
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != {"bin/bd-export"}:
        raise ValueError("native artifact set is not exact")


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
    plug_target = getattr(args, "plug_target", None)
    if profile == NATIVE_SCHEMA:
        if any(getattr(args, name, None) is not None for name in (
            "bd", "plug", "roundtrip", "plug_commit", "plug_tree", "plug_manifest", "plug_target",
        )):
            raise ValueError("native profile rejects Plug and importer executable inputs")
    else:
        required = {
            SCHEMA: ("bd", "plug", "plug_commit", "plug_tree", "plug_manifest", "plug_target"),
            COMPANION_SCHEMA: ("bd_export", "roundtrip", "plug_commit", "plug_tree", "plug_manifest", "plug_target"),
        }[profile]
        missing = [name for name in required if not isinstance(getattr(args, name, None), str) or not getattr(args, name)]
        if missing:
            raise ValueError("missing profile inputs: " + ", ".join(missing))
    config = profile_config(profile, plug_target)
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
    if profile == NATIVE_SCHEMA:
        if args.beads_commit != NATIVE_BEADS_COMMIT or args.beads_tree != NATIVE_BEADS_TREE:
            raise ValueError("native Beads source identity is not exact")
        if set(metadata) != {"workflow", "source_refs", "toolchain", "resolution", "build"}:
            raise ValueError("native metadata keys are not exact")
        workflow = metadata["workflow"]
        if not isinstance(workflow, dict) or set(workflow) != {"ref", "sha", "profile"} or workflow.get("profile") != NATIVE_SCHEMA:
            raise ValueError("native workflow metadata is not exact")
        if not isinstance(workflow.get("ref"), str) or not workflow["ref"] or not isinstance(workflow.get("sha"), str) or not re.fullmatch(r"[0-9a-fA-F]{40}", workflow["sha"]):
            raise ValueError("native workflow provenance is invalid")
        source_refs = metadata["source_refs"]
        if source_refs != {"beads": NATIVE_BEADS_SOURCE_REF}:
            raise ValueError("native source references are not exact")
        toolchain = metadata["toolchain"]
        if not isinstance(toolchain, dict) or set(toolchain) != {"go", "bazel", "bazelisk_sha256"}:
            raise ValueError("native toolchain metadata is not exact")
        if not all(isinstance(toolchain.get(key), str) and toolchain[key] for key in ("go", "bazel")):
            raise ValueError("native toolchain metadata is invalid")
        if not isinstance(toolchain.get("bazelisk_sha256"), str) or len(toolchain["bazelisk_sha256"]) != 64:
            raise ValueError("native Bazelisk provenance is invalid")
        build = metadata["build"]
        if not isinstance(build, dict) or set(build) != {"beads", "run_id"}:
            raise ValueError("native build metadata is not exact")
        if not isinstance(build.get("beads"), str) or not build["beads"]:
            raise ValueError("native build command is invalid")
        run_id = build.get("run_id")
        if not isinstance(run_id, str) or not re.fullmatch(r"[1-9][0-9]*", run_id):
            raise ValueError("native build.run_id must be a positive decimal string")
        resolution = metadata["resolution"]
        resolution_keys = {
            "bd_export_help_sha256", "bd_export_help_exit", "bd_export_version",
            "bd_export_version_sha256", "bd_init_help_sha256", "bd_import_help_sha256",
        }
        if not isinstance(resolution, dict) or set(resolution) != resolution_keys:
            raise ValueError("native resolution metadata is not exact")
        if resolution.get("bd_export_help_exit") != 0:
            raise ValueError("native exporter help probe did not pass")
        version = resolution.get("bd_export_version")
        if not isinstance(version, dict) or version.get("commit") != NATIVE_BEADS_COMMIT or version.get("build") != "public-" + NATIVE_BEADS_COMMIT:
            raise ValueError("native executable identity is not linked to the source commit")
        for key in ("bd_export_help_sha256", "bd_export_version_sha256", "bd_init_help_sha256", "bd_import_help_sha256"):
            if not isinstance(resolution.get(key), str) or len(resolution[key]) != 64:
                raise ValueError("native probe provenance is invalid: " + key)
    source_manifests: dict[str, dict[str, object]] = {}
    sources = [("beads", args.beads_commit, args.beads_tree, args.beads_manifest)]
    if profile != NATIVE_SCHEMA:
        sources.append(("plug", args.plug_commit, args.plug_tree, args.plug_manifest))
    for name, commit, tree, source_manifest in sources:
        manifest_path = Path(source_manifest)
        source_manifests[name] = {
            "commit": commit,
            "tree": tree,
            "manifest_sha256": digest(manifest_path),
            "manifest_entries": sum(1 for line in manifest_path.read_text(encoding="utf-8").splitlines() if line),
        }
        if profile in {COMPANION_SCHEMA, NATIVE_SCHEMA}:
            (output / "source").mkdir(exist_ok=True)
            (output / "source" / f"{name}.manifest").write_bytes(manifest_path.read_bytes())
    if profile == NATIVE_SCHEMA:
        native_source = source_manifests["beads"]
        if native_source["manifest_sha256"] != NATIVE_BEADS_MANIFEST_SHA256 or native_source["manifest_entries"] != NATIVE_BEADS_MANIFEST_ENTRIES:
            raise ValueError("native Beads source manifest is not the pinned manifest")

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
    if not isinstance(sources, dict) or not isinstance(actual, dict):
        raise ValueError("expected provenance must name canonical sources")
    if set(sources) != set(actual):
        raise ValueError("expected provenance source set does not match the profile")
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
    if profile in {COMPANION_SCHEMA, NATIVE_SCHEMA}:
        if profile == NATIVE_SCHEMA:
            verify_native_manifest_contract(manifest)
        expected_target = "//tools/roundtrip:roundtrip" if profile == COMPANION_SCHEMA else None
        if manifest.get("targets") != profile_config(profile, expected_target)["targets"]:
            raise ValueError("profile executable targets are not exact")
        sources = manifest.get("source")
        expected_sources = {"beads", "plug"} if profile == COMPANION_SCHEMA else {"beads"}
        if not isinstance(sources, dict) or set(sources) != expected_sources:
            raise ValueError("canonical source manifests are missing or contain an unexpected source")
        source_dir = root / "source"
        if profile == NATIVE_SCHEMA:
            names = {path.name for path in source_dir.iterdir()} if source_dir.is_dir() else set()
            if names != {"beads.manifest"}:
                raise ValueError("native source manifest set is not exact")
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
    make.add_argument("--plug-commit")
    make.add_argument("--plug-tree")
    make.add_argument("--plug-manifest")
    make.add_argument("--plug-target")
    make.set_defaults(function=create)
    check = subparsers.add_parser("verify")
    check.add_argument("--directory", type=Path, required=True)
    check.add_argument("--expected-provenance", type=Path)
    check.set_defaults(function=verify)
    return root


if __name__ == "__main__":
    arguments = parser().parse_args()
    arguments.function(arguments)
