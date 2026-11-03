#!/usr/bin/env python3
"""Prepare the fixed public9d6 Go reference and source without executing it."""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import zipfile


# Both files are retrieved from the same trusted workflow-control commit.
spec = importlib.util.spec_from_file_location(
    "trusted_public_bundle", Path(__file__).with_name("prepare-public-bundle.py"),
)
bundle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bundle)

MARKER = "--test_env=PLUG_PUBLIC_REFERENCE="
CAPTURE_TARGET = "//spec/s1-b13-b16:reference_capture"
ENVIRONMENT_NAMES = (
    "PLUG_PUBLIC_REFERENCE_ZIP", "PLUG_PUBLIC_REFERENCE_SOURCE",
    "PLUG_PUBLIC_REFERENCE_MANIFEST", "PLUG_PUBLIC_REFERENCE_BUILD_RUN",
)
RUN = "41399124974"
ARTIFACT = "13121145530"
MANIFEST_SHA256 = "8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2"
DESCRIPTOR = RUN + ":" + ARTIFACT + ":" + MANIFEST_SHA256
WORKFLOW_SHA = "c761c579a91c38263187afd5d30fdfd38eadd97c"
WORKFLOW_BRANCH = "codex/v4-b1c-public-companion-20261102"
ZIP_SHA256 = "c8ace91c3494db905a4c64dbd4e733e5ebb863b1987d66b4907e8d21a0ba3b4c"
ZIP_SIZE = 119364059
PROFILE = "plug-public-bundle/1"
SOURCE_SHA = "9d6fff760b7a314b476539198504d77ce0959656"
SOURCE_TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
SOURCE_MANIFEST_SHA256 = "23d84a56302ad19c50d9d25af733b2da50f9137aaf9308b273cab039b6023ae3"
SOURCE_ENTRIES = 5087
SOURCE_URL = "https://github.com/buildbetter-app/plugs.git"
SOURCES = {
    "beads": {"commit": SOURCE_SHA, "tree": SOURCE_TREE,
              "manifest_sha256": SOURCE_MANIFEST_SHA256, "manifest_entries": SOURCE_ENTRIES},
    "plug": {"commit": "cb74bf48e6a196b8f3862186b35a5c01cb6346f9",
             "tree": "b1394d8b1fa25510f0f79f942b302d0db11e1e0f",
             "manifest_sha256": "9dca92b7e51e8130c28a8095a4a0afc0b675eff2ab4d0307983f4ad6952b84b5",
             "manifest_entries": 1052},
}
TARGETS = {
    "beads_importer": {"label": "./cmd/bd", "binary": "bin/bd"},
    "plug_runtime": {"label": "//cmd/plug:plug", "binary": "bin/plug"},
}


class GitAuthenticationError(ValueError):
    """The fixed Git operation failed because authentication was rejected."""


def authentication_failure(stderr: bytes) -> bool:
    message = stderr.lower()
    return any(marker in message for marker in (
        b"authentication failed",
        b"could not read username",
        b"invalid username or password",
        b"the requested url returned error: 401",
    ))


BINARIES = {
    "bin/bd": {"mode": "0o755", "size": 112341314,
               "sha256": "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"},
    "bin/plug": {"mode": "0o755", "size": 19980553,
                 "sha256": "fc4e17ed68b111e599cb2be49ba7f82c42ab612752b0546716cf34112eefe417"},
}
ARCHIVE = {"name": "bundle.tar", "size": 132331520,
           "sha256": "e4b67e5456f91419ec9c1290ca3cc620ef27f71812a821677a3ea5647a70d412"}
FILES = set(BINARIES) | {"manifest.json", "receipt.json", "bundle.tar"}
MAX_BYTES = 512 * 1024 * 1024


def descriptor_from_args(value: str) -> str:
    arguments = value.split()
    candidates = [arg for arg in arguments if "PLUG_PUBLIC_REFERENCE" in arg]
    if not candidates:
        return ""
    if (len(candidates) != 1 or not candidates[0].startswith(MARKER)
            or any("PLUG_PUBLIC_BUNDLE" in arg for arg in arguments)):
        raise ValueError("reference marker must be unique, fixed and separate from the companion marker")
    descriptor = candidates[0][len(MARKER):]
    bundle.parse_descriptor(descriptor)
    if descriptor != DESCRIPTOR:
        raise ValueError("reference descriptor differs from the fixed public9d6 delivery")
    command = next((arg for arg in arguments if not arg.startswith("-")), "")
    if command == "run":
        capture_build_arguments(arguments)
    elif command not in {"test", "coverage"}:
        raise ValueError("public reference preparation requires test, coverage or the exact capture run")
    return descriptor


def capture_build_arguments(arguments: list[str]) -> list[str]:
    if arguments.count("--") > 1:
        raise ValueError("capture run has ambiguous program separators")
    separator = arguments.index("--") if "--" in arguments else len(arguments)
    build = arguments[:separator]
    if arguments[separator + 1:]:
        raise ValueError("capture program arguments must come only from verified preparation")
    if [arg for arg in build if not arg.startswith("-")] != ["run", CAPTURE_TARGET]:
        raise ValueError("public reference run requires exactly the fixed capture binary target")
    if (MARKER + DESCRIPTOR not in build
            or any(arg.startswith(("--prepared-", "--test_arg", "--test_env"))
                   and arg != MARKER + DESCRIPTOR for arg in build)):
        raise ValueError("capture prepared paths and environment cannot be supplied by the caller")
    return [arg for arg in build if arg != MARKER + DESCRIPTOR]


def route_arguments(value: str, environment: dict[str, str]) -> list[str]:
    arguments = value.split()
    if not descriptor_from_args(value):
        return arguments
    values = {name: environment.get(name, "") for name in ENVIRONMENT_NAMES}
    if any(not value or any(char in value for char in "\r\n\0") for value in values.values()):
        raise ValueError("verified public reference environment is incomplete or malformed")
    if (any(not Path(values[name]).is_absolute() for name in ENVIRONMENT_NAMES[:3])
            or values["PLUG_PUBLIC_REFERENCE_BUILD_RUN"] != RUN):
        raise ValueError("verified public reference paths or build run differ from preparation")
    command = next(arg for arg in arguments if not arg.startswith("-"))
    if command == "run":
        return capture_build_arguments(arguments) + [
            "--", "--prepared-artifact", values["PLUG_PUBLIC_REFERENCE_ZIP"],
            "--prepared-source", values["PLUG_PUBLIC_REFERENCE_SOURCE"],
        ]
    return arguments + ["--test_env=" + name + "=" + values[name] for name in ENVIRONMENT_NAMES]


def validate_metadata(run: dict, artifact: dict) -> None:
    bundle.validate_metadata(run, artifact, RUN, ARTIFACT)
    if (run.get("head_sha") != WORKFLOW_SHA or run.get("head_branch") != WORKFLOW_BRANCH
            or artifact.get("digest") != "sha256:" + ZIP_SHA256
            or artifact.get("size_in_bytes") != ZIP_SIZE):
        raise ValueError("reference producer metadata differs from the fixed public delivery")


def validate_manifest(manifest: dict) -> None:
    expected = {
        "schema": PROFILE, "source": SOURCES, "targets": TARGETS,
        "artifacts": BINARIES, "archive": ARCHIVE,
        "source_refs": {"beads": "refs/remotes/origin/beads",
                        "plug": "codex/b1c-runtime-source-cb74bf48-20261102"},
        "workflow": {"ref": "refs/heads/" + WORKFLOW_BRANCH,
                     "sha": WORKFLOW_SHA, "profile": PROFILE},
        "build": {"run_id": RUN,
                  "beads": "CGO_ENABLED=0 go build -tags gms_pure_go -trimpath ./cmd/bd",
                  "plug": "bazel build --jobs=2 --local_resources=memory=8192 //cmd/plug:plug"},
        "resolution": {"beads_version_sha256": "affb43505816fa697a866678b7f0b22024508c6f78cb97ce2d4b6c6369fcacb6",
                       "plug_help_exit": 0,
                       "plug_help_sha256": "6168f272cf1bc8e6a9437ff8859eaf54294153b7976f15c9795af9dd48c9d0f5"},
        "toolchain": {"bazel": "bazel 9.3.0", "go": "go version go1.26.10 linux/amd64",
                      "bazelisk_sha256": "5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992"},
    }
    if manifest != expected:
        raise ValueError("reference profile/source/tree/target/hash/build provenance mismatch")


def validate_zip(archive: Path, manifest_path: Path) -> dict:
    if archive.stat().st_size != ZIP_SIZE or bundle.digest(archive) != ZIP_SHA256:
        raise ValueError("reference ZIP differs from the independently pinned bytes")
    with zipfile.ZipFile(archive) as stream:
        members = stream.infolist()
        files = [member.filename for member in members if not member.is_dir()]
        if (len(members) != len({member.filename for member in members})
                or set(files) != FILES or len(files) != len(FILES)):
            raise ValueError("reference ZIP member set is not exact")
        for member in members:
            name = member.filename.rstrip("/") if member.is_dir() else member.filename
            kind = stat.S_IFMT(member.external_attr >> 16)
            if (not bundle.safe_name(name) or member.file_size > MAX_BYTES
                    or kind not in {0, stat.S_IFDIR if member.is_dir() else stat.S_IFREG}
                    or (member.is_dir() and name != "bin")):
                raise ValueError("reference ZIP contains an unsafe member")
        if sum(member.file_size for member in members) > 2 * MAX_BYTES:
            raise ValueError("reference ZIP expansion exceeds the public bound")
        if any(stream.getinfo(name).file_size > 1024 * 1024 for name in ("manifest.json", "receipt.json")):
            raise ValueError("reference metadata exceeds its bound")
        manifest_bytes = stream.read("manifest.json")
        if hashlib.sha256(manifest_bytes).hexdigest() != MANIFEST_SHA256:
            raise ValueError("reference manifest checksum mismatch")
        manifest = json.loads(manifest_bytes)
        validate_manifest(manifest)
        if json.loads(stream.read("receipt.json")) != manifest:
            raise ValueError("reference receipt differs from the fixed manifest")
        for name, record in BINARIES.items():
            with stream.open(name) as payload:
                if stream.getinfo(name).file_size != record["size"] or bundle.digest_stream(payload) != record["sha256"]:
                    raise ValueError("reference loose executable size/hash mismatch")
        with stream.open("bundle.tar") as payload:
            if stream.getinfo("bundle.tar").file_size != ARCHIVE["size"] or bundle.digest_stream(payload) != ARCHIVE["sha256"]:
                raise ValueError("reference canonical archive size/hash mismatch")
        seen = set()
        with stream.open("bundle.tar") as payload, tarfile.open(fileobj=payload, mode="r|") as canonical:
            for member in canonical:
                if member.name not in BINARIES or member.name in seen:
                    raise ValueError("reference canonical member set is not exact")
                seen.add(member.name)
                record = BINARIES[member.name]
                if not member.isreg() or member.mode != 0o755 or member.size != record["size"]:
                    raise ValueError("reference canonical executable type/mode/size mismatch")
                contents = canonical.extractfile(member)
                if contents is None or bundle.digest_stream(contents) != record["sha256"]:
                    raise ValueError("reference canonical executable digest mismatch")
        if seen != set(BINARIES):
            raise ValueError("reference canonical executable is missing")
    # Keep the raw ZIP intact; the capture consumer retains its own extraction guards.
    manifest_path.write_bytes(manifest_bytes)
    return manifest


def git_environment() -> dict[str, str]:
    # No job token, Git credential helper, global config or caller Git options.
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8",
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0",
    }


def git(source: Path, arguments: list[str], environment: dict | None = None) -> bytes:
    result = subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
         "-c", "core.logAllRefUpdates=false", "-c", "gc.auto=0",
         "-c", "credential.helper=", "-C", str(source), *arguments],
        env=git_environment() if environment is None else environment,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if result.returncode != 0:
        # Git's captured output is never printed. Only explicit authentication
        # failures may trigger the credential-free public retry.
        if authentication_failure(result.stderr):
            raise GitAuthenticationError()
        raise ValueError("fixed public reference Git operation failed")
    return result.stdout


def validate_source(source: Path) -> dict:
    if source.is_symlink() or not (source / ".git").is_dir() or (source / ".git").is_symlink():
        raise ValueError("reference source must be a separate ordinary Git checkout")
    if git(source, ["rev-parse", "HEAD"]).decode().strip() != SOURCE_SHA:
        raise ValueError("reference source HEAD differs from the fixed9d6 source")
    if git(source, ["rev-parse", "HEAD^{tree}"]).decode().strip() != SOURCE_TREE:
        raise ValueError("reference source tree differs from the fixed9d6 tree")
    allowed = {"core.repositoryformatversion", "core.filemode", "core.bare", "core.logallrefupdates"}
    config = git(source, ["config", "--local", "--list"]).decode()
    if any(line.split("=", 1)[0] not in allowed for line in config.splitlines()):
        raise ValueError("reference source retains unexpected Git configuration")
    if "core.filemode=true" not in config.splitlines() or "core.bare=false" not in config.splitlines():
        raise ValueError("reference source cannot hide file modes or omit its worktree")
    if any((source / ".git" / name).exists() for name in ("FETCH_HEAD", "objects/info/alternates", "logs")):
        raise ValueError("reference source retains fetch metadata or external object references")
    if git(source, ["status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching"]):
        raise ValueError("reference source worktree/index is not clean")
    if any(line[:2] != b"H " for line in git(source, ["ls-files", "-v"]).splitlines()):
        raise ValueError("reference source index contains hidden or sparse entries")
    manifest = git(source, ["ls-tree", "-r", "--full-tree", "HEAD"])
    if hashlib.sha256(manifest).hexdigest() != SOURCE_MANIFEST_SHA256 or len(manifest.splitlines()) != SOURCE_ENTRIES:
        raise ValueError("reference source canonical manifest differs from the selected source")
    return {"commit": SOURCE_SHA, "tree": SOURCE_TREE,
            "manifest_sha256": SOURCE_MANIFEST_SHA256, "manifest_entries": SOURCE_ENTRIES}


def fetch_source(source: Path) -> dict:
    source.mkdir(parents=True, exist_ok=False)
    git(source, ["init", "--quiet", "--template="])
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        raise ValueError("reference preparation requires its step-local read token")
    fetch_environment = git_environment()
    basic_credentials = base64.b64encode(("x-access-token:" + token).encode("utf-8")).decode("ascii")
    fetch_environment.update({
        "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
        "GIT_CONFIG_VALUE_0": "AUTHORIZATION: Basic " + basic_credentials,
    })
    # Retrieve only the exact pinned object. A trusted workflow token gets the
    # first attempt; a public repository may reject that authenticated
    # request, so retry the identical URL/SHA without any credentials.
    fetch_arguments = ["fetch", "--quiet", "--depth=1", "--no-tags", "--no-write-fetch-head",
                       "--no-auto-maintenance", SOURCE_URL, SOURCE_SHA]
    try:
        git(source, fetch_arguments, fetch_environment)
    except GitAuthenticationError:
        # Retry the identical URL/SHA request without any credentials. The
        # source may be public, and no bearer or Basic header may cross this
        # boundary.
        git(source, fetch_arguments, git_environment())
    finally:
        del fetch_environment
        del token
    # Checkout and all source inspection run without the retrieval credential.
    git(source, ["checkout", "--quiet", "--detach", SOURCE_SHA])
    return validate_source(source)


def prepare(descriptor: str, output: Path, github_env: Path, evidence: Path) -> None:
    if descriptor != DESCRIPTOR:
        raise ValueError("reference descriptor differs from the fixed public9d6 delivery")
    run = bundle.gh_json(f"repos/{bundle.REPOSITORY}/actions/runs/{RUN}")
    artifact = bundle.gh_json(f"repos/{bundle.REPOSITORY}/actions/artifacts/{ARTIFACT}")
    validate_metadata(run, artifact)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    archive = output / "artifact.zip"
    with archive.open("xb") as target:
        result = subprocess.run(
            ["gh", "api", f"repos/{bundle.REPOSITORY}/actions/artifacts/{ARTIFACT}/zip"],
            stdout=target, stderr=subprocess.PIPE, check=False,
        )
    if result.returncode != 0:
        raise ValueError("fixed public reference artifact download failed")
    manifest_path = output / "manifest.json"
    manifest = validate_zip(archive, manifest_path)
    source = output / "beads-source"
    source_record = fetch_source(source)
    values = {
        "PLUG_PUBLIC_REFERENCE_ZIP": str(archive),
        "PLUG_PUBLIC_REFERENCE_SOURCE": str(source),
        "PLUG_PUBLIC_REFERENCE_MANIFEST": str(manifest_path),
        "PLUG_PUBLIC_REFERENCE_BUILD_RUN": RUN,
    }
    if any("\r" in value or "\n" in value for value in values.values()):
        raise ValueError("reference runner path is not a single environment value")
    receipt = {
        "status": "PASS", "descriptor": descriptor, "producer_run": RUN,
        "producer_workflow_sha": WORKFLOW_SHA, "artifact_id": ARTIFACT,
        "artifact_sha256": ZIP_SHA256, "manifest_sha256": MANIFEST_SHA256,
        "profile": PROFILE, "build_profile": "original Go-built9d6; not canonical Bazel",
        "source": manifest["source"], "prepared_source": source_record,
        "targets": manifest["targets"], "artifacts": manifest["artifacts"],
        "archive": manifest["archive"], "environment": values,
    }
    evidence.write_text(json.dumps(receipt, sort_keys=True) + "\n", encoding="utf-8")
    with github_env.open("a", encoding="utf-8") as stream:
        for name, value in values.items():
            stream.write(name + "=" + value + "\n")
    print(json.dumps({"status": "PASS", "producer_run": RUN, "artifact_id": ARTIFACT,
                      "manifest_sha256": MANIFEST_SHA256}))


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    request = commands.add_parser("request")
    request.add_argument("--args", required=True)
    route = commands.add_parser("route")
    route.add_argument("--args", required=True)
    make = commands.add_parser("prepare")
    make.add_argument("--descriptor", required=True)
    make.add_argument("--output", type=Path, required=True)
    make.add_argument("--github-env", type=Path, required=True)
    make.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "request":
        print(descriptor_from_args(args.args))
    elif args.command == "route":
        # NUL-delimited argv preserves runner paths without shell evaluation.
        for argument in route_arguments(args.args, os.environ):
            sys.stdout.buffer.write(os.fsencode(argument) + b"\0")
    else:
        prepare(args.descriptor, args.output, args.github_env, args.evidence)


if __name__ == "__main__":
    main()
