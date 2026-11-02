#!/usr/bin/env python3
"""Record the bounded B18 reference vector from the admitted pinned binary."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Any


SOURCE_COMMIT = "9d6fff760b7a314b476539198504d77ce0959656"
SOURCE_TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
BINARY_SHA256 = "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3"
BUILD_INFO_SHA256 = "2edc35ea3a2f0d50f708bda2271742494191f3288466ba246f6a8d0502f64d50"
METADATA_SHA256 = "8e748f6f69c62a2a04cc505ede4202d7d346018c7ae78ef7b7d4f27626720a9b"
BUILD_COMMAND_SHA256 = "9ad766be28a628287fb305ada6a038cb930f96f591da2e611fb35b8841c531af"
TOOLCHAIN_SHA256 = "19e5d35abb16621313ab2c3b9c107578eb3bf6d9f4a82bc9a00a592de9b6a000"
VERSION_SHA256 = "089850c5597ef244242b3d8952a41baf3e4990853314ba385a2c622c04868229"

SOURCE_SYMBOLS = [
    ("internal/storage/issueops/version_history.go", "63ffe88e5f8c70e710cc1f36a7fd39b1dafa63691304bca553ddea46aacfb7e7"),
    ("internal/storage/issueops/asof_read.go", "e5b4376f456b34ed3dfd90b37ef4f80ce415e97f8e31723234be1c596eb352ff"),
    ("internal/storage/issueops/as_of.go", "fdc275261773f253d607c56282dedd574237115bdfc0c767b3e546e8e249ec49"),
    ("internal/storage/issueops/version_list.go", "5c64c58c8ac225708f45f76ded6ab126f626003499f890921cf2061cc6b5c68a"),
    ("cmd/bd/versions.go", "883406960294b0dfb19026238d4afa570d700d0f9ec0cea9f1667599495872ef"),
    ("cmd/bd/versions_test.go", "49022069284ca889cdfda7430dcdf8324eb5edb72ab6968af77e44e65926a590"),
    ("cmd/bd/show.go", "cdccb0df4c22c14a50061653119f3f8a129610f0357d9e7340fe08bb19c45342"),
    ("internal/storage/leg_contract_wiring_test.go", "0f71f6ca0335d8830d11a23df3c8fe3e7ccb92a08048df21d99eb0326a9b756a"),
]


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def validate_asset(engine: pathlib.Path) -> dict[str, Any]:
    asset = engine.parent
    files = {
        "metadata.json": METADATA_SHA256,
        "build-command.json": BUILD_COMMAND_SHA256,
        "build-info.stdout": BUILD_INFO_SHA256,
        "toolchain.stdout": TOOLCHAIN_SHA256,
        "version.stdout": VERSION_SHA256,
    }
    require(engine.is_file(), f"missing reference engine: {engine}")
    require(sha256(engine) == BINARY_SHA256, "reference engine digest mismatch")
    for name, expected in files.items():
        require(sha256(asset / name) == expected, f"{name} digest mismatch")

    metadata = json.loads((asset / "metadata.json").read_text())
    build_command = json.loads((asset / "build-command.json").read_text())
    build_info = (asset / "build-info.stdout").read_text()
    require(metadata["sourceCommit"] == SOURCE_COMMIT, "metadata source pin mismatch")
    require(metadata["sha256"] == BINARY_SHA256, "metadata binary digest mismatch")
    require(pathlib.Path(metadata["binary"]).resolve() == engine.resolve(), "metadata binary path mismatch")
    require(metadata["version"] == "bd version 1.3.0 (dev)", "metadata version mismatch")
    require(build_command["argv"][:4] == ["go", "build", "-tags", "gms_pure_go"], "build tag mismatch")
    require(build_command["argv"][-1] == "./cmd/bd", "build package mismatch")
    require("\tbuild\t-tags=gms_pure_go\n" in build_info, "build-info lacks gms_pure_go")
    require("\tpath\tgithub.com/steveyegge/beads/cmd/bd\n" in build_info, "build-info module path mismatch")

    source = pathlib.Path(metadata["source"])
    require(source.is_dir(), f"missing bound source archive: {source}")
    symbols = []
    for relative, expected in SOURCE_SYMBOLS:
        actual = sha256(source / relative)
        require(actual == expected, f"source digest mismatch: {relative}")
        symbols.append({"path": relative, "sha256": actual})
    return {
        "root": str(asset),
        "binary": str(engine),
        "binarySha256": BINARY_SHA256,
        "metadataSha256": METADATA_SHA256,
        "buildCommandSha256": BUILD_COMMAND_SHA256,
        "buildInfoSha256": BUILD_INFO_SHA256,
        "toolchainSha256": TOOLCHAIN_SHA256,
        "versionSha256": VERSION_SHA256,
        "source": str(source),
        "sourceCommit": SOURCE_COMMIT,
        "sourceTree": SOURCE_TREE,
        "sourceSymbols": symbols,
        "buildTag": "gms_pure_go",
        "version": "bd version 1.3.0 (dev)",
    }


def decode(stdout: str, name: str) -> Any:
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{name} did not return JSON: {error}") from error


def one_issue(stdout: str, name: str) -> dict[str, Any]:
    value = decode(stdout, name)
    require(isinstance(value, list) and len(value) == 1, f"{name} did not return one issue")
    return value[0]


def record(engine: pathlib.Path, output: pathlib.Path) -> None:
    asset = validate_asset(engine)
    workspace = pathlib.Path(tempfile.mkdtemp(prefix="b18-reference-", dir="/tmp/plug-swarm/work"))
    (workspace / "tmp").mkdir()
    environment = {
        "PATH": os.environ["PATH"],
        "BEADS_ACTOR": "b18-reference-recorder",
        "BEADS_DOLT_AUTO_START": "1",
        "CI": "1",
        "GIT_AUTHOR_EMAIL": "b18@example.invalid",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "LANG": "C.UTF-8",
        "TMPDIR": str(workspace / "tmp"),
        "TZ": "UTC",
    }
    commands: list[dict[str, Any]] = []

    def run(name: str, *arguments: str) -> dict[str, Any]:
        argv = [str(engine), *arguments]
        completed = subprocess.run(
            argv,
            cwd=workspace,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        outcome = {
            "name": name,
            "args": list(arguments),
            "exit": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        commands.append(outcome)
        require(completed.returncode == 0, f"{name} exited {completed.returncode}: {completed.stderr}")
        return outcome

    run("init", "init", "--prefix", "b18", "--non-interactive", "--quiet", "--skip-agents", "--skip-hooks")
    run("enable-history", "config", "set", "versioned-history.enabled", "true")
    run("create", "create", "--id", "b18-reference", "--type", "task", "--title", "original", "--priority", "2", "--json")
    created = one_issue(run("show-created", "show", "b18-reference", "--json")["stdout"], "show-created")
    versions_created = decode(run("versions-created", "versions", "b18-reference", "--json")["stdout"], "versions-created")
    run("noop-update", "update", "b18-reference", "--title", "original", "--json")
    after_noop = one_issue(run("show-noop", "show", "b18-reference", "--json")["stdout"], "show-noop")
    versions_noop = decode(run("versions-noop", "versions", "b18-reference", "--json")["stdout"], "versions-noop")
    history_noop = decode(run("history-noop", "history", "b18-reference", "--json")["stdout"], "history-noop")
    require(isinstance(history_noop, list) and len(history_noop) == 1, "no-op changed Dolt history")
    original_commit = history_noop[0]["CommitHash"]
    require(re.fullmatch(r"[0-9a-v]{32}", original_commit) is not None, "history returned a non-full Dolt hash")
    run("update-second", "update", "b18-reference", "--title", "second", "--json")
    run("update-third", "update", "b18-reference", "--title", "third", "--json")
    history_final = decode(run("history-final", "history", "b18-reference", "--json")["stdout"], "history-final")
    current = one_issue(run("show-current", "show", "b18-reference", "--json")["stdout"], "show-current")
    versions_final = decode(run("versions-final", "versions", "b18-reference", "--json")["stdout"], "versions-final")
    historical = one_issue(run("show-as-of-create", "show", "b18-reference", "--as-of", original_commit, "--json")["stdout"], "show-as-of-create")

    created_versions = versions_created["versions"]
    noop_versions = versions_noop["versions"]
    final_versions = versions_final["versions"]
    require(versions_created["recording"] and versions_noop["recording"] and versions_final["recording"], "version recording is off")
    require([len(created_versions), len(noop_versions), len(final_versions)] == [1, 1, 3], "unexpected version counts")
    require(created["revision"] == after_noop["revision"], "same-value update changed issue revision")
    require(current["revision"] != created["revision"], "changing updates did not change issue revision")
    require([row["local_revision"] for row in final_versions] == [3, 2, 1], "unexpected local revision sequence")
    require(all("revision" not in row for row in final_versions), "versions payload conflated local and issue revision")
    require([row["Issue"]["title"] for row in history_final] == ["third", "second", "original"], "unexpected history titles")
    require(historical["title"] == "original" and current["title"] == "third", "as-of read substituted current content")

    projection = {
        "format": "plug-b18-reference-projection/1",
        "card": "B18",
        "engine": "embedded-dolt",
        "observations": {
            "versionCounts": {"afterCreate": 1, "afterSameValue": 1, "afterTwoChanges": 3},
            "localRevisionsNewestFirst": [3, 2, 1],
            "issueRevision": {
                "present": True,
                "sameValuePreserved": True,
                "changingUpdateChanged": True,
                "relationToLocalRevision": "separate-identities",
            },
            "asOf": {
                "selector": "full-dolt-commit-hash",
                "historicalTitle": "original",
                "currentTitle": "third",
                "retainedAcrossLaterWrites": True,
            },
            "sameStoreAcrossProcesses": {
                "independentCLIInvocations": len(commands),
                "finalTitle": "third",
                "retainedVersionCount": 3,
            },
        },
    }
    provenance = {
        "format": "plug-b18-reference-provenance/1",
        "card": "B18",
        "recordedAt": datetime.now(timezone.utc).isoformat(),
        "workspace": str(workspace),
        "referenceAsset": asset,
        "assetValidation": {
            "fullSourceComparison": "PASS",
            "evidence": "/tmp/plug-swarm/ci-local/B18-REFERENCE-CONTINUATION-f52e85b235864137be001d5dda00d441c4d4366b/asset-validation",
            "comparison": "local beads-base archive and bound extracted source matched all regular paths, bytes, and symlink targets",
        },
        "recorder": {
            "path": "spec/cardb18/reference/record.py",
            "sha256": sha256(pathlib.Path(__file__)),
        },
        "configuration": {
            "actor": environment["BEADS_ACTOR"],
            "owner": environment["GIT_AUTHOR_EMAIL"],
            "prefix": "b18",
            "history": "store setting versioned-history.enabled=true before create",
            "ambient": "PATH only; no inherited application configuration",
            "engine": "embedded-dolt",
        },
        "inputs": {
            "id": "b18-reference",
            "titles": ["original", "original", "second", "third"],
            "clock": "runtime; timestamps excluded from semantic comparison",
            "allocator": "explicit ID; historical allocator limits not exercised",
        },
        "disposition": {
            "status": "deviate",
            "decisions": ["P6", "P7", "P30"],
            "reference": "per-record local revisions plus Dolt commit-addressed CLI as-of",
            "plug": "global changeset revision identity plus driver-log history.query",
        },
        "limits": [
            "no internal issue_versions as-of substitute: the pinned source states that API has no public wire",
            "no temporal selector or performance qualification",
            "no historical allocator qualification",
            "no full B18 command/card parity",
            "reference persistence is embedded Dolt across processes, not SQLite parity",
        ],
    }
    provenance["rawCommandsSha256"] = canonical_digest(commands)
    provenance["projectionSha256"] = canonical_digest(projection)
    artifact = {
        "format": "plug-b18-reference-run/1",
        "card": "B18",
        "status": "RECORDED",
        "commands": commands,
        "projection": projection,
        "provenance": provenance,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({
        "output": str(output),
        "workspace": str(workspace),
        "rawCommandsSha256": provenance["rawCommandsSha256"],
        "projectionSha256": provenance["projectionSha256"],
        "artifactSha256": sha256(output),
    }, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("engine", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    arguments = parser.parse_args()
    record(arguments.engine.resolve(), arguments.output.resolve())


if __name__ == "__main__":
    main()
