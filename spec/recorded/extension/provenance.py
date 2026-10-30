#!/usr/bin/env python3
"""Validate the pinned source and independently project one fresh recording."""

import hashlib
import json
import platform
import re
import subprocess
import sys
from pathlib import Path


PIN = "9d6fff760b7a314b476539198504d77ce0959656"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(value.encode() if isinstance(value, str) else value).hexdigest()


def evidence(value):
    return {"value": value, "digest": digest(canonical(value))}


def schema(root):
    result = {"cursorSource": "pinned migration inventory; CLI does not expose embedded SQL"}
    for plane in ("main", "ignored"):
        migrations = root / "internal/storage/schema/migrations"
        if plane == "ignored":
            migrations /= "ignored"
        files = [{"path": str(p.relative_to(root)), "sha256": digest(p.read_bytes())}
                 for p in sorted(migrations.glob("*.up.sql"))]
        if len(files) != (72 if plane == "main" else 28):
            raise ValueError("incomplete schema inventory: " + plane)
        result[plane + "Cursor"] = max(int(Path(p["path"]).name.split("_", 1)[0]) for p in files)
        result[plane + "Digest"] = digest(canonical(files))
    return result


def record(card, artifact_path, reference, binary, repository, recorder_source):
    commit = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "refs/tags/beads-base"], text=True,
    ).strip()
    if commit != PIN:
        raise ValueError("reference tag differs")
    source_tree = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", PIN + "^{tree}"], text=True,
    ).strip()
    tree = subprocess.check_output(["git", "-C", str(repository), "ls-tree", "-rz", PIN])
    for entry in tree.split(b"\0"):
        if not entry:
            continue
        metadata, path = entry.split(b"\t", 1)
        mode, kind, blob = metadata.split()
        if kind != b"blob":
            raise ValueError("unsupported reference object")
        source = reference / path.decode()
        data = str(source.readlink()).encode() if mode == b"120000" else source.read_bytes()
        actual = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if actual != blob.decode():
            raise ValueError("pinned source differs: " + path.decode())
    artifact_bytes = artifact_path.read_bytes()
    artifact = json.loads(artifact_bytes)
    vector = json.loads(artifact["rawJSON"])
    if canonical(vector) != artifact["rawJSON"] or vector != artifact["vector"]:
        raise ValueError("raw recording is not canonical or differs from vector")
    if vector["card"] != card or vector["engine"] != "embedded-dolt":
        raise ValueError("raw recording has wrong identity")
    raw_digest = digest(artifact["rawJSON"])
    if raw_digest != artifact["digest"] or not vector["commands"] or any(c["exit"] != 0 for c in vector["commands"]):
        raise ValueError("raw digest or command execution differs")
    source_refs = [
        {"path": path, "sha256": digest((reference / path).read_bytes())}
        for path in sorted({name.split("::", 1)[0] for name in vector["sourceTests"]})
    ]
    for name in vector["sourceTests"]:
        path, symbol = name.split("::", 1)
        if not re.search(r"\bfunc " + re.escape(symbol) + r"\(", (reference / path).read_text()):
            raise ValueError("pinned source test missing: " + name)
    match = re.search(r"github.com/dolthub/dolt/go\s+(\S+)", (reference / "go.mod").read_text())
    if not match:
        raise ValueError("reference driver version missing")
    projection = canonical({"card": card, "engine": vector["engine"], "observations": vector["observations"]})
    build_info = subprocess.check_output(["go", "version", "-m", str(binary)], text=True)
    go_version = re.search(r": (go\S+)$", build_info.splitlines()[0])
    if not go_version:
        raise ValueError("reference compiler identity missing")
    settings = dict(re.findall(r"^\s*build\s+(GOOS|GOARCH|CGO_ENABLED)=(\S+)$", build_info, re.MULTILINE))
    if set(settings) != {"GOOS", "GOARCH", "CGO_ENABLED"}:
        raise ValueError("reference binary build settings missing")
    toolchain = {
        "goVersion": go_version[1], "pythonVersion": platform.python_version(),
        "buildInfoSHA256": digest(build_info), "settings": settings,
    }
    recorder = {
        "identity": "build50-reference-recorder", "version": "1",
        "source": "spec/recorded/extension/build50_record.py", "sha256": digest(recorder_source.read_bytes()),
    }
    projector = {
        "identity": "build50-reference-provenance", "version": "1",
        "source": "spec/recorded/extension/provenance.py", "sha256": digest(Path(__file__).read_bytes()),
    }
    disposition = {
        "status": "keep", "source": "engdocs/design/plug/spec/behaviors/" + ("work" if card == "B07" else "surfaces") + ".md#" + card,
        "scope": "recorded-reference-vector",
    }
    bound = {
        "card": card, "sourceCommit": PIN, "sourceTree": source_tree,
        "binarySha256": digest(binary.read_bytes()), "toolchain": toolchain,
        "recorder": recorder, "projector": projector, "sources": source_refs,
        "driver": {"name": "embedded-dolt", "version": "github.com/dolthub/dolt/go@" + match[1]},
        "schema": schema(reference), "disposition": disposition,
        "configurationDigest": digest(canonical(vector["configuration"])),
        "inputDigest": digest(canonical(vector["inputs"])),
        "sourceTests": vector["sourceTests"], "commands": vector["commands"],
        "rawDigest": raw_digest, "projectionDigest": digest(projection),
    }
    return {
        "sourceRefs": source_refs, "disposition": disposition,
        "configuration": evidence(vector["configuration"]), "inputs": evidence(vector["inputs"]),
        "projection": {"format": "plug-recorded-projection/1", "paths": ["card", "engine", "observations"],
                       "json": projection, "digest": digest(projection)},
        "binding": bound, "provenance": canonical(bound), "provenanceDigest": digest(canonical(bound)),
        "artifactSha256": digest(artifact_bytes),
    }


if __name__ == "__main__":
    if len(sys.argv) != 8 or sys.argv[1] not in ("B07", "B25"):
        raise SystemExit("usage: provenance.py B07|B25 ARTIFACT REFERENCE_SOURCE EXECUTABLE REPOSITORY RECORDER_SOURCE OUTPUT")
    card, artifact, reference, binary, repository, source, output = sys.argv[1:]
    result = record(card, Path(artifact), Path(reference), Path(binary), Path(repository), Path(source))
    Path(output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
