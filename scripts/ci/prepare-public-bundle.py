#!/usr/bin/env python3
"""Prepare one pinned public companion or native artifact for an opt-in RunsOn test.

The privileged entry point is fetched from independently pinned trusted source. It
uses the job-local read token only through gh; downloaded executables are
never run here and no credential is written or forwarded to Bazel.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tarfile
import zipfile
from typing import NamedTuple


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


NATIVE_PROFILE = "plug-public-native/1"
NATIVE_BEADS_COMMIT = "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724"
NATIVE_SOURCES = {
    "beads": (NATIVE_BEADS_COMMIT, "930405e863284a3955cbd35e73a6097b606b91a5",
              "e01a584042b4a2c32ea3cbcbdd55069f7c23bc2b853f8734857c5082e828f15d"),
}
NATIVE_SOURCE_ENTRIES = 5141
NATIVE_SOURCE_REFS = {
    "beads": "refs/remotes/origin/swarm/native-producer-census-dedup-20261104",
}
NATIVE_TARGETS = {"beads_exporter": {"label": "//cmd/bd:bd", "binary": "bin/bd-export"}}
NATIVE_BINARIES = {"bin/bd-export"}
NATIVE_FILES = NATIVE_BINARIES | {"manifest.json", "receipt.json", "bundle.tar", "source/beads.manifest"}
NATIVE_BUILD = (
    "bazel build --jobs=2 --local_resources=memory=8192 "
    "--@rules_go//go/config:tags=gms_pure_go "
    "--@rules_go//go/config:gc_linkopts=-X=main.Commit=" + NATIVE_BEADS_COMMIT
    + ",-X=main.Build=public-" + NATIVE_BEADS_COMMIT + " //cmd/bd:bd"
)
NATIVE_TOOLCHAIN = {
    "go": "go version go1.26.10 linux/amd64", "bazel": "bazel 9.3.0",
    "bazelisk_sha256": "5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992",
}
PUBLIC_ENV_NAMES = (
    "PLUG_PUBLIC_BEADS_BINARY", "PLUG_PUBLIC_BEADS_SHA256",
    "PLUG_PUBLIC_BEADS_BUILD_RUN", "PLUG_PUBLIC_BEADS_MANIFEST",
)


class BundleProfile(NamedTuple):
    schema: str
    sources: dict
    source_refs: dict
    targets: dict
    binaries: set[str]
    files: set[str]
    beads_commit: str


class NativeAdmission(NamedTuple):
    # Filled only in reviewed trusted source from the replacement producer's
    # exact command/run/official-byte disposition. No CLI/env override exists.
    descriptor: str
    workflow_sha: str
    workflow_branch: str
    label: str
    artifact_digest: str
    artifact_size: int


# Trusted admission for the independently reviewed official R41800330346 bytes.
# Descriptor/profile adoption and real consumer qualification remain separate.
NATIVE_ADMISSION: NativeAdmission | None = NativeAdmission(
    descriptor="41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a",
    workflow_sha="d758fdf42a426e9e2a4b707dbcebb22ec68b264e",
    workflow_branch="swarm/native-producer-census-control-c8eb-20261104",
    label="native-census-c8eb-20261104",
    artifact_digest="sha256:28609d7c072c281a748cb53a361d9a3ffada826d6f2e8866a3b1e62b23d5ffc4",
    artifact_size=111646601,
)


def bundle_profile(schema: str) -> BundleProfile:
    if schema == PROFILE:
        return BundleProfile(PROFILE, SOURCES, SOURCE_REFS, TARGETS, BINARIES, FILES, BEADS_COMMIT)
    if schema == NATIVE_PROFILE:
        return BundleProfile(NATIVE_PROFILE, NATIVE_SOURCES, NATIVE_SOURCE_REFS,
                             NATIVE_TARGETS, NATIVE_BINARIES, NATIVE_FILES, NATIVE_BEADS_COMMIT)
    raise ValueError("unsupported public executable profile")


def strict_json(payload: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate public provenance key")
            result[key] = value
        return result

    def constant(value):
        raise ValueError("non-finite public provenance value")

    value = json.loads(payload, object_pairs_hook=pairs, parse_constant=constant)
    if not isinstance(value, dict):
        raise ValueError("public provenance must be an object")
    return value


def physical_path(path: Path) -> Path:
    path = path.absolute()
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("public preparation path contains a link")
    return path.resolve()


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
    if any("PLUG_PUBLIC_BEADS_" in arg for arg in arguments):
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


def validate_metadata(run: dict, artifact: dict, run_id: str, artifact_id: str,
                      *, profile: str = PROFILE, manifest_sha256: str = "") -> None:
    if (str(run.get("id")) != run_id or run.get("status") != "completed"
            or run.get("conclusion") != "success" or run.get("event") != "workflow_dispatch"
            or run.get("path") != WORKFLOW or run.get("repository", {}).get("full_name") != REPOSITORY
            or run.get("head_repository", {}).get("full_name") != REPOSITORY
            or re.fullmatch(r"[0-9a-f]{40}", str(run.get("head_sha"))) is None):
        raise ValueError("public bundle producer is not the successful fixed same-repository workflow")
    if profile == PROFILE:
        name_pattern = r"plug-public-bundle-[A-Za-z0-9._-]{1,48}-" + run_id
    elif profile == NATIVE_PROFILE:
        # Accepted successor contract: native names are finite label-only
        # addresses. Exact admitted label/run/official-byte checks still follow.
        name_pattern = r"plug-public-bundle-[A-Za-z0-9._-]{1,48}"
    else:
        raise ValueError("unsupported public metadata profile")
    binding = artifact.get("workflow_run", {})
    if not isinstance(binding, dict):
        raise ValueError("public artifact workflow binding must be an object")
    if (str(artifact.get("id")) != artifact_id or artifact.get("expired") is not False
            or str(binding.get("id")) != run_id or binding.get("head_sha") != run["head_sha"]
            or re.fullmatch(name_pattern, str(artifact.get("name"))) is None
            or type(artifact.get("size_in_bytes")) is not int
            or not 0 < artifact["size_in_bytes"] <= MAX_ARTIFACT_BYTES
            or re.fullmatch(r"sha256:[0-9a-f]{64}", str(artifact.get("digest"))) is None):
        raise ValueError("public bundle artifact is not bound to the immutable producer run")


    if profile == NATIVE_PROFILE:
        admission = NATIVE_ADMISSION
        if admission is None:
            raise ValueError("native delivery has no trusted official-byte admission")
        descriptor = run_id + ":" + artifact_id + ":" + manifest_sha256
        if (descriptor != admission.descriptor or run["head_sha"] != admission.workflow_sha
                or run.get("head_branch") != admission.workflow_branch
                or run.get("workflow_id") != 403922586 or type(run.get("run_attempt")) is not int
                or run["run_attempt"] != 1
                or run.get("actor", {}).get("login") != "nikhil-s-buildbetter-remote-dev[bot]"
                or run.get("triggering_actor", {}).get("login") != "nikhil-s-buildbetter-remote-dev[bot]"
                or artifact.get("name") != "plug-public-bundle-" + admission.label
                or artifact["digest"] != admission.artifact_digest
                or artifact["size_in_bytes"] != admission.artifact_size):
            raise ValueError("native metadata differs from the reviewed command/run/official bytes")


def validate_native_manifest(manifest: dict) -> None:
    if set(manifest) != {"schema", "source", "targets", "build", "toolchain", "resolution",
                         "workflow", "source_refs", "artifacts", "archive"}:
        raise ValueError("native manifest top-level keys are not exact")
    source = manifest["source"]["beads"]
    if (set(source) != {"commit", "tree", "manifest_sha256", "manifest_entries"}
            or type(source["manifest_entries"]) is not int
            or source["manifest_entries"] != NATIVE_SOURCE_ENTRIES):
        raise ValueError("native source manifest record/count is not exact")
    if manifest["build"] != {"beads": NATIVE_BUILD, "run_id": manifest["build"].get("run_id")}:
        raise ValueError("native build command or sole-source build keys differ")
    if manifest.get("toolchain") != NATIVE_TOOLCHAIN:
        raise ValueError("native toolchain differs from the canonical producer contract")
    resolution = manifest.get("resolution")
    hashes = {"bd_export_help_sha256", "bd_export_version_sha256", "bd_init_help_sha256", "bd_import_help_sha256"}
    if (not isinstance(resolution, dict) or set(resolution) != hashes | {"bd_export_help_exit", "bd_export_version"}
            or type(resolution.get("bd_export_help_exit")) is not int or resolution["bd_export_help_exit"] != 0):
        raise ValueError("native help/version/init/import probe fields are not exact")
    if any(not isinstance(resolution.get(key), str)
           or re.fullmatch(r"[0-9a-f]{64}", resolution[key]) is None for key in hashes):
        raise ValueError("native probe digest is invalid")
    if set(manifest["archive"]) != {"name", "sha256", "size"}:
        raise ValueError("native archive keys are not exact")


def validate_manifest(manifest: dict, run: dict, run_id: str) -> BundleProfile:
    profile = bundle_profile(manifest.get("schema"))
    if manifest.get("targets") != profile.targets or manifest.get("source_refs") != profile.source_refs:
        raise ValueError("public profile targets or source refs differ from selected pins")
    if manifest.get("workflow") != {"ref": "refs/heads/" + run["head_branch"], "sha": run["head_sha"], "profile": profile.schema}:
        raise ValueError("public workflow-control binding mismatch")
    build = manifest.get("build")
    if not isinstance(build, dict) or build.get("run_id") != run_id:
        raise ValueError("public build run mismatch")
    sources = manifest.get("source")
    if not isinstance(sources, dict) or set(sources) != set(profile.sources):
        raise ValueError("public canonical source set mismatch")
    for name, (commit, tree, source_digest) in profile.sources.items():
        record = sources[name]
        if (not isinstance(record, dict) or record.get("commit") != commit or record.get("tree") != tree
                or record.get("manifest_sha256") != source_digest
                or type(record.get("manifest_entries")) is not int or record["manifest_entries"] <= 0):
            raise ValueError("public canonical source SHA/tree/manifest mismatch: " + name)
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != profile.binaries:
        raise ValueError("public executable set is not exact")
    for record in artifacts.values():
        if (not isinstance(record, dict) or set(record) != {"sha256", "size", "mode"}
                or record.get("mode") != "0o755" or not isinstance(record.get("sha256"), str) or re.fullmatch(r"[0-9a-f]{64}", str(record.get("sha256"))) is None
                or type(record.get("size")) is not int or not 0 < record["size"] <= MAX_FILE_BYTES):
            raise ValueError("public executable digest/size/mode record is invalid")
    archive = manifest.get("archive")
    if (not isinstance(archive, dict) or archive.get("name") != "bundle.tar" or type(archive.get("size")) is not int
            or not 0 < archive["size"] <= MAX_ARTIFACT_BYTES
            or not isinstance(archive.get("sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", archive["sha256"]) is None):
        raise ValueError("public archive digest/size record is invalid")
    resolution = manifest.get("resolution")
    version = resolution.get("bd_export_version") if isinstance(resolution, dict) else None
    if (not isinstance(version, dict) or version.get("commit") != profile.beads_commit
            or version.get("build") != "public-" + profile.beads_commit):
        raise ValueError("public sealed executable identity mismatch")
    if profile.schema == NATIVE_PROFILE:
        validate_native_manifest(manifest)
    return profile


def prepare_archive(archive: Path, output: Path, manifest_sha256: str, run: dict, run_id: str,
                    *, artifact: dict | None = None, expected_profile: str | None = None) -> dict:
    """Validate both containers before exposing canonical regular executables."""
    output = physical_path(output)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    with zipfile.ZipFile(archive) as stream:
        members = stream.infolist()
        names = [member.filename for member in members if not member.is_dir()]
        if len(members) != len({member.filename for member in members}) or len(names) != len(set(names)):
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
        if not {"manifest.json", "receipt.json"}.issubset(names):
            raise ValueError("public artifact provenance members are missing")
        if stream.getinfo("manifest.json").file_size > 1024 * 1024 or stream.getinfo("receipt.json").file_size > 1024 * 1024:
            raise ValueError("public artifact provenance exceeds the metadata bound")
        manifest_bytes = stream.read("manifest.json")
        if hashlib.sha256(manifest_bytes).hexdigest() != manifest_sha256:
            raise ValueError("public artifact manifest checksum mismatch")
        manifest = strict_json(manifest_bytes)
        if expected_profile is not None and manifest.get("schema") != expected_profile:
            raise ValueError("public artifact profile differs from trusted preparation selection")
        profile = validate_manifest(manifest, run, run_id)
        if set(names) != profile.files:
            raise ValueError("public artifact ZIP file set is not exact for its profile")
        if profile.schema == NATIVE_PROFILE:
            if artifact is None:
                raise ValueError("native preparation requires official artifact metadata")
            validate_metadata(run, artifact, run_id, str(artifact.get("id")),
                              profile=profile.schema, manifest_sha256=manifest_sha256)
        if strict_json(stream.read("receipt.json")) != manifest:
            raise ValueError("public artifact receipt differs from the pinned manifest")
        for name in profile.binaries:
            record = manifest["artifacts"][name]
            if stream.getinfo(name).file_size != record["size"]:
                raise ValueError("public artifact loose executable size mismatch")
            with stream.open(name) as payload:
                if digest_stream(payload) != record["sha256"]:
                    raise ValueError("public artifact loose executable digest mismatch")
        for name in sorted(profile.files - profile.binaries):
            destination = output / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            with stream.open(name) as payload, destination.open("xb") as target:
                shutil.copyfileobj(payload, target, 1024 * 1024)

    for name, (_, _, expected_digest) in profile.sources.items():
        path = output / "source" / f"{name}.manifest"
        entries = sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line)
        if digest(path) != expected_digest or entries != manifest["source"][name]["manifest_entries"]:
            raise ValueError("public artifact canonical source manifest mismatch")
    canonical = output / "bundle.tar"
    if digest(canonical) != manifest["archive"]["sha256"] or canonical.stat().st_size != manifest["archive"]["size"]:
        raise ValueError("public artifact canonical archive checksum/size mismatch")
    with zipfile.ZipFile(archive) as loose, tarfile.open(canonical, "r:") as stream:
        members = stream.getmembers()
        if len(members) != len(profile.binaries) or {member.name for member in members} != profile.binaries:
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
            with payload, loose.open(member.name) as original, destination.open("xb") as target:
                for block in iter(lambda: payload.read(1024 * 1024), b""):
                    if block != original.read(len(block)):
                        raise ValueError("public loose and canonical executable bytes differ")
                    target.write(block)
                if original.read(1):
                    raise ValueError("public loose executable has trailing bytes")
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
    if any(name.startswith("PLUG_PUBLIC_BEADS_") for name in os.environ):
        raise ValueError("prepared executable environment cannot be supplied by the caller")
    output, github_env, evidence = map(physical_path, (output, github_env, evidence))
    if (output.exists() or output.with_suffix(".zip").exists() or evidence.exists()
            or (github_env.exists() and not github_env.is_file())):
        raise ValueError("public preparation requires new helper-owned output and receipt")
    run = gh_json(f"repos/{REPOSITORY}/actions/runs/{run_id}")
    artifact = gh_json(f"repos/{REPOSITORY}/actions/artifacts/{artifact_id}")
    # A descriptor admitted in trusted source alone selects native metadata
    # rules before download. Caller input, artifact name and manifest cannot
    # grant native authority; companion remains the default fixed profile.
    profile = (NATIVE_PROFILE if NATIVE_ADMISSION is not None
               and descriptor == NATIVE_ADMISSION.descriptor else PROFILE)
    validate_metadata(run, artifact, run_id, artifact_id,
                      profile=profile, manifest_sha256=manifest_sha256)
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
    manifest = prepare_archive(archive, output, manifest_sha256, run, run_id,
                               artifact=artifact, expected_profile=profile)
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
    with evidence.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(receipt, sort_keys=True) + "\n")
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
