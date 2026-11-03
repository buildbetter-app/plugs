#!/usr/bin/env python3
"""Manual fixed-profile consumer. Missing real clock evidence is a failure.

This consumes only the trusted same-run preparation interface. It never
replaces an artifact, synthesizes clock evidence, or records a blocked
boundary as paired evidence. Unit fixtures use a separate target.
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import zipfile

from reference_inputs import COMMIT, TREE, neutral_environment, ordinary_path, verify_clock_proof, verify_source


RUN = "41399124974"
MANIFEST_SHA256 = "8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2"
DESCRIPTOR = RUN + ":13121145530:" + MANIFEST_SHA256
ZIP_SHA256 = "c8ace91c3494db905a4c64dbd4e733e5ebb863b1987d66b4907e8d21a0ba3b4c"
ZIP_SIZE = 119364059
BINARY_SHA256 = "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"
BINARY_SIZE = 112341314
FILES = {"bin/bd", "bin/plug", "manifest.json", "receipt.json", "bundle.tar"}


def digest_file(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def prepared_paths():
    if (os.environ.get("PLUG_PUBLIC_REFERENCE") != DESCRIPTOR
            or os.environ.get("PLUG_PUBLIC_REFERENCE_BUILD_RUN") != RUN):
        raise RuntimeError("prepared descriptor/run differs from the fixed public profile")
    return (
        ordinary_path(os.environ.get("PLUG_PUBLIC_REFERENCE_ZIP", "")),
        ordinary_path(os.environ.get("PLUG_PUBLIC_REFERENCE_SOURCE", ""), directory=True),
        ordinary_path(os.environ.get("PLUG_PUBLIC_REFERENCE_MANIFEST", "")),
    )


def verify_archive(archive, manifest_path):
    if archive.stat().st_size != ZIP_SIZE or digest_file(archive) != ZIP_SHA256:
        raise RuntimeError("prepared ZIP differs from the fixed public profile")
    manifest_bytes = manifest_path.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != MANIFEST_SHA256:
        raise RuntimeError("prepared manifest differs from the fixed public profile")
    # The whole ZIP and manifest are pinned above; checks below independently
    # bind the extracted loose executable to that same fixed profile.
    with zipfile.ZipFile(archive) as bundle:
        members = bundle.infolist()
        names = [item.filename for item in members if not item.is_dir()]
        if (len(members) != len({item.filename for item in members})
                or set(names) != FILES or len(names) != len(FILES)):
            raise RuntimeError("prepared ZIP member set is not exact")
        for item in members:
            kind = stat.S_IFMT(item.external_attr >> 16)
            if ((item.is_dir() and item.filename != "bin/")
                    or kind not in {0, stat.S_IFDIR if item.is_dir() else stat.S_IFREG}):
                raise RuntimeError("prepared ZIP contains an unsupported member type")
        if bundle.read("manifest.json") != manifest_bytes:
            raise RuntimeError("prepared ZIP and routed manifest differ")
        manifest = json.loads(manifest_bytes)
        if json.loads(bundle.read("receipt.json")) != manifest:
            raise RuntimeError("prepared ZIP receipt differs from its manifest")
        source = manifest.get("source", {}).get("beads", {})
        binary = manifest.get("artifacts", {}).get("bin/bd", {})
        if (source.get("commit") != COMMIT or source.get("tree") != TREE
                or binary != {"mode": "0o755", "size": BINARY_SIZE, "sha256": BINARY_SHA256}):
            raise RuntimeError("prepared source/executable identity differs from the pin")
        data = bundle.read("bin/bd")
        if len(data) != BINARY_SIZE or hashlib.sha256(data).hexdigest() != BINARY_SHA256:
            raise RuntimeError("prepared executable bytes differ from the pin")
    return data


def consume_prepared(root, archive, source, manifest, proof):
    if root.resolve().is_relative_to(source):
        raise RuntimeError("source cannot contain the private capture directory")
    # Consume only private copies. Original inputs can never replace already
    # admitted bytes between validation and recorder use.
    private_archive, private_manifest = root / "artifact.zip", root / "manifest.json"
    private_proof, private_source = root / "clock-proof.json", root / "source"
    shutil.copyfile(archive, private_archive)
    shutil.copyfile(manifest, private_manifest)
    shutil.copyfile(proof, private_proof)
    _, proof_digest = verify_clock_proof(private_proof, BINARY_SHA256)
    data = verify_archive(private_archive, private_manifest)
    shutil.copytree(source, private_source, symlinks=True)
    source_record = verify_source(private_source)
    # Do not grant execute permission until every admission check succeeds.
    binary = root / "bd"
    binary.write_bytes(data)
    binary.chmod(0o500)
    return binary, private_source, private_proof, {
        **source_record, "descriptor": DESCRIPTOR, "producerRun": RUN,
        "archiveSha256": ZIP_SHA256, "archiveSize": ZIP_SIZE,
        "manifestSha256": MANIFEST_SHA256, "binarySha256": BINARY_SHA256,
        "clockProofSha256": proof_digest,
        "buildProfile": "original-Go-gms_pure_go", "canonicalBazel": False,
    }


def recorder_environment(source, proof):
    return {
        **neutral_environment(), "PLUG_REFERENCE_SOURCE_COMMIT": COMMIT,
        "PLUG_REFERENCE_SOURCE_TREE": TREE, "PLUG_REFERENCE_CLOCK_MODE": "go-sql-controlled",
        "PLUG_REFERENCE_SOURCE_DIR": str(source), "PLUG_REFERENCE_CLOCK_PROOF": str(proof),
    }


def main():
    # Trusted preparation supplies no generated clock proof. Refuse before
    # output creation or extraction, rather than returning a green capture.
    proof = os.environ.get("PLUG_REFERENCE_CLOCK_PROOF", "")
    if not proof:
        raise RuntimeError("BLOCKED_MISSING_GO_SQL_CLOCK_PROOF: pinned generated evidence is required")
    proof = ordinary_path(proof)
    verify_clock_proof(proof, BINARY_SHA256)
    archive, source, manifest = prepared_paths()
    output_value = os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR", "")
    if not output_value:
        raise RuntimeError("manual capture requires TEST_UNDECLARED_OUTPUTS_DIR")
    evidence_dir = ordinary_path(output_value, directory=True)
    output = evidence_dir / "recordings.json"
    if output.exists() or (evidence_dir / "prepared-reference.json").exists():
        raise RuntimeError("capture output already exists; refusing to replace evidence")
    with tempfile.TemporaryDirectory(prefix="s1-b05-b08-prepared-") as temporary:
        root = Path(temporary)
        binary, private_source, private_proof, verified = consume_prepared(root, archive, source, manifest, proof)
        Path("/tmp/plug-swarm/work").mkdir(parents=True, exist_ok=True)
        recorder = Path(__file__).with_name("record_reference.py")
        result = subprocess.run(
            [sys.executable, "-B", str(recorder), str(binary), str(output)],
            env=recorder_environment(private_source, private_proof),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        (evidence_dir / "recorder.stdout").write_bytes(result.stdout)
        (evidence_dir / "recorder.stderr").write_bytes(result.stderr)
        if result.returncode != 0 or not output.is_file():
            raise RuntimeError(f"prepared reference recorder failed with exit {result.returncode}")
        recording = json.loads(output.read_bytes())
        if (recording.get("status") != "recorded" or len(recording.get("recordings", [])) != 2
                or recording.get("sourceCommit") != COMMIT or recording.get("sourceTree") != TREE
                or recording.get("binarySha256") != BINARY_SHA256
                or recording.get("clockProofSha256") != verified["clockProofSha256"]):
            raise RuntimeError("prepared recorder did not retain matching pinned twins")
        projector = Path(__file__).with_name("project_reference.py")
        projection = subprocess.run(
            [sys.executable, "-B", str(projector), str(output)], env=neutral_environment(),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        (evidence_dir / "projector.stderr").write_bytes(projection.stderr)
        if projection.returncode:
            raise RuntimeError("independent projection rejected the reference twins")
        (evidence_dir / "projection.json").write_bytes(projection.stdout)
        verified.update({"status": "recorded", "recordingsSha256": digest_file(output)})
        (evidence_dir / "prepared-reference.json").write_text(json.dumps(verified, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"status": "recorded", "descriptor": DESCRIPTOR, "recordings": 2}))


if __name__ == "__main__":
    main()
