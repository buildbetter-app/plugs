#!/usr/bin/env python3
"""Validate the trusted public-reference handoff without inventing clock proof.

The remote workflow owns ZIP/source preparation.  This adapter independently
checks the paths and pinned bytes, then records an explicit blocked boundary
when the prepared route has no generated Go+SQL clock proof.  It invokes the
real B05-B08 recorder only when such proof is supplied by a separately
reviewed producer; a missing proof never becomes a synthetic recording.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import zipfile


DESCRIPTOR = "41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2"
RUN = "41399124974"
COMMIT = "9d6fff760b7a314b476539198504d77ce0959656"
TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
BINARY_SHA256 = "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_file(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def required_path(name: str) -> Path:
    value = os.environ.get(name, "")
    if not value or any(char in value for char in "\r\n\0"):
        raise RuntimeError(f"{name} is missing or malformed")
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise RuntimeError(f"{name} is not an absolute ordinary file")
    return path


def prepared_paths() -> tuple[Path, Path, Path]:
    archive = required_path("PLUG_PUBLIC_REFERENCE_ZIP")
    source = Path(os.environ.get("PLUG_PUBLIC_REFERENCE_SOURCE", ""))
    manifest_path = required_path("PLUG_PUBLIC_REFERENCE_MANIFEST")
    if not source.is_absolute() or source.is_symlink() or not source.is_dir():
        raise RuntimeError("PLUG_PUBLIC_REFERENCE_SOURCE is not an absolute ordinary directory")
    if os.environ.get("PLUG_PUBLIC_REFERENCE_BUILD_RUN") != RUN:
        raise RuntimeError("prepared public reference run differs from the fixed descriptor")
    return archive, source, manifest_path


def verify_prepared(archive: Path, source: Path, manifest_path: Path, output: Path) -> dict:
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    beads = manifest.get("source", {}).get("beads", {})
    if beads.get("commit") != COMMIT or beads.get("tree") != TREE:
        raise RuntimeError("prepared manifest is not bound to the pinned Beads source")
    artifact = manifest.get("artifacts", {}).get("bin/bd", {})
    if artifact.get("sha256") != BINARY_SHA256 or artifact.get("mode") != "0o755":
        raise RuntimeError("prepared manifest has an unexpected reference executable")
    if not (source / ".git").is_dir():
        raise RuntimeError("prepared source checkout has no ordinary .git directory")
    source_commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    source_tree = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"], text=True).strip()
    if (source_commit, source_tree) != (COMMIT, TREE):
        raise RuntimeError("prepared source checkout differs from the pinned commit/tree")
    if subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"], text=True):
        raise RuntimeError("prepared source checkout is dirty")

    with zipfile.ZipFile(archive) as bundle:
        names = {item.filename for item in bundle.infolist() if not item.is_dir()}
        required = {"bin/bd", "bin/plug", "manifest.json", "receipt.json", "bundle.tar", "bd-version.json", "plug-help.txt"}
        if names != required:
            raise RuntimeError(f"prepared public ZIP member set is not exact: {sorted(names)}")
        embedded_manifest = bundle.read("manifest.json")
        if embedded_manifest != manifest_bytes:
            raise RuntimeError("prepared ZIP manifest differs from the routed manifest")
        if json.loads(bundle.read("receipt.json")) != manifest:
            raise RuntimeError("prepared ZIP receipt differs from its manifest")
        binary_bytes = bundle.read("bin/bd")
        if digest_bytes(binary_bytes) != BINARY_SHA256:
            raise RuntimeError("prepared reference executable digest differs from the pinned digest")

    output.mkdir(parents=True, exist_ok=True)
    return {
        "descriptor": DESCRIPTOR,
        "producerRun": RUN,
        "archiveSha256": digest_file(archive),
        "archiveSize": archive.stat().st_size,
        "manifestSha256": digest_bytes(manifest_bytes),
        "binarySha256": BINARY_SHA256,
        "sourceCommit": source_commit,
        "sourceTree": source_tree,
        "sourceStatus": "clean",
    }


def extract_binary(archive: Path, root: Path) -> Path:
    binary = root / "bd"
    with zipfile.ZipFile(archive) as bundle, bundle.open("bin/bd") as source:
        binary.write_bytes(source.read())
    binary.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    if digest_file(binary) != BINARY_SHA256:
        raise RuntimeError("extracted reference executable digest changed")
    return binary


def write_evidence(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    archive, source, manifest = prepared_paths()
    evidence_dir = Path(os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR", tempfile.mkdtemp(prefix="s1-b05-b08-prepared-")))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    verified = verify_prepared(archive, source, manifest, evidence_dir)
    proof = os.environ.get("PLUG_REFERENCE_CLOCK_PROOF", "")
    if not proof:
        # The trusted preparation route deliberately does not claim a generated
        # Go+SQL clock proof.  Retain the exact missing capability as a typed
        # handoff and do not invoke the recorder or write a fake recording.
        write_evidence(evidence_dir / "prepared-reference.json", {
            **verified,
            "status": "blocked",
            "captureDisposition": "BLOCKED_MISSING_GO_SQL_CLOCK_PROOF",
            "recordings": [],
            "missing": "PLUG_REFERENCE_CLOCK_PROOF generated by the pinned reference producer, bound to this executable digest",
        })
        print(json.dumps({"status": "blocked", "descriptor": DESCRIPTOR, "recordings": 0}))
        return 0

    with tempfile.TemporaryDirectory(prefix="s1-b05-b08-prepared-", dir="/tmp") as temporary:
        root = Path(temporary)
        binary = extract_binary(archive, root)
        output = evidence_dir / "recordings.json"
        recorder = Path(__file__).with_name("record_reference.py")
        env = {key: value for key, value in os.environ.items()}
        env.update({
            "PLUG_REFERENCE_SOURCE_COMMIT": COMMIT,
            "PLUG_REFERENCE_SOURCE_TREE": TREE,
            "PLUG_REFERENCE_CLOCK_MODE": "go-sql-controlled",
            "PLUG_REFERENCE_SOURCE_DIR": str(source),
            "PLUG_REFERENCE_CLOCK_PROOF": proof,
        })
        Path("/tmp/plug-swarm/work").mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            [sys.executable, str(recorder), str(binary), str(output)],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            check=False,
        )
        (evidence_dir / "recorder.stdout").write_text(result.stdout, encoding="utf-8")
        (evidence_dir / "recorder.stderr").write_text(result.stderr, encoding="utf-8")
        if result.returncode != 0 or not output.is_file():
            raise RuntimeError(f"prepared reference recorder failed with exit {result.returncode}")
        recording = json.loads(output.read_text(encoding="utf-8"))
        if recording.get("status") != "recorded" or len(recording.get("recordings", [])) != 2:
            raise RuntimeError("prepared recorder did not retain two recordings")
        write_evidence(evidence_dir / "prepared-reference.json", {
            **verified,
            "status": "recorded",
            "captureDisposition": "RECORDED",
            "recordings": recording["recordings"],
            "clockProof": proof,
        })
    print(json.dumps({"status": "recorded", "descriptor": DESCRIPTOR, "recordings": 2}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
