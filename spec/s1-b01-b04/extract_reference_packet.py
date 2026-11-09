#!/usr/bin/env python3
"""Independently validate an exact public packet from archived test.log.

No capture/recorder imports, product execution, network, or output repair.
BLOCK bytes require explicit diagnostic retention and never become twins.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import re

BEGIN = b"S1_B01_B04_RAW_PACKET_BEGIN"
END = b"S1_B01_B04_RAW_PACKET_END"
MAX_PACKET_BYTES = 32 * 1024 * 1024
FILES = {
    "reference-recording.json", "raw-commands.jsonl", "capture-runtime.json",
    "capture-profile.json", "public-reference-manifest.json", "record_reference.py",
    "recorder.stdout", "recorder.stderr", "dolt-version.stdout", "dolt-version.stderr",
    "runtime-commands.jsonl",
    "dolt-config.json", "dolt-config-source.go", "dolt-env-source.sh",
    "dolt-identity-source.go",
}
COMMIT = "9d6fff760b7a314b476539198504d77ce0959656"
TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
BINARY = "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"
DESCRIPTOR = "41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2"
DOLT_ARCHIVE = "1f7ad8c2622995789420a3fb0f2d16b4aa7430000a825dd91d5938f36480cbf6"
DOLT_URL = "https://github.com/dolthub/dolt/releases/download/v2.2.0/dolt-linux-amd64.tar.gz"
DOLT_MEMBER = "dolt-linux-amd64/bin/dolt"
DOLT_CONFIG_SOURCE = "2515e188b89e1676921d3a7dd215b8fa824f1206a72aa81da9aa85e491f6f3df"
DOLT_ENV_SOURCE = "8bb3e5f57e2d8816753a4e0e9f08be5d64e51b89bde61d0d799f3bf04e1dd3d0"
DOLT_IDENTITY_SOURCE = "17e65f9062e2dc9207f3c9291686fe66ec2756df5ea452bc99530b4dbeec83e8"
DOLT_IDENTITY = {"user.name": "beads", "user.email": "beads@localhost"}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def no_duplicate_keys(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate JSON key")
        value[key] = item
    return value


def reject_constant(value):
    raise ValueError("nonfinite JSON constant: " + value)


def load(data):
    return json.loads(data, object_pairs_hook=no_duplicate_keys, parse_constant=reject_constant)


def decode(value):
    require(isinstance(value, str), "base64 bytes are not text")
    data = base64.b64decode(value, validate=True)
    require(base64.b64encode(data).decode("ascii") == value, "noncanonical base64 bytes")
    return data


def unframe(log):
    # Same strict LF framing convention as the independent B09-B12 lane.
    lines = log.split(b"\n")
    starts = [i for i, line in enumerate(lines) if line.startswith(BEGIN)]
    ends = [i for i, line in enumerate(lines) if line.startswith(END)]
    require(len(starts) == len(ends) == 1, "packet framing is absent or ambiguous")
    first, last = starts[0], ends[0]
    require(last == first + 2 and last < len(lines) - 1, "packet frame is partial or has extra lines")
    match = re.fullmatch(re.escape(BEGIN) + rb" ([1-9][0-9]*) ([0-9a-f]{64})", lines[first])
    require(match is not None, "packet header is malformed")
    length, checksum = match.groups()
    require(lines[last] == END + b" " + length + b" " + checksum, "packet footer differs")
    payload = lines[first + 1]
    require(len(payload) == int(length) <= MAX_PACKET_BYTES, "packet byte length differs or exceeds bound")
    require(digest(payload).encode() == checksum, "packet SHA256 differs")
    packet = load(payload)
    require(canonical(packet) == payload, "packet is not exact canonical JSON")
    return packet, payload


def validate_packet(packet):
    require(isinstance(packet, dict) and packet.get("format") == "plug.s1-b01-b04.raw-packet.v1",
            "packet format differs")
    require(packet.get("status") in {"BLOCK", "CAPTURED"}, "packet outcome is absent")
    files = packet.get("files")
    require(isinstance(files, dict) and set(files) == FILES, "packet file inventory differs")
    decoded = {}
    for name, entry in files.items():
        require(isinstance(entry, dict) and type(entry.get("present")) is bool
                and type(entry.get("size")) is int and entry["size"] >= 0, "file presence/size is malformed")
        require(set(entry) <= {"present", "size", "bytesBase64", "sha256", "error"}, "unknown file metadata")
        require("error" not in entry or isinstance(entry["error"], str), "file error is malformed")
        if entry["present"]:
            data = decode(entry.get("bytesBase64"))
            require(len(data) == entry["size"] and digest(data) == entry.get("sha256"), "file bytes differ: " + name)
            decoded[name] = data
        else:
            require(entry.get("bytesBase64") is None and entry["size"] == 0 and "sha256" not in entry,
                    "missing file has invented bytes")
    return decoded


def accept_capture(packet, files):
    require(packet["status"] == "CAPTURED", "BLOCK packet is diagnostic only")
    require(set(files) == FILES and not any("error" in entry for entry in packet["files"].values()),
            "completed capture has missing files or retention errors")
    profile = load(files["capture-profile.json"])
    require(profile.get("format") == "plug.s1-b01-b04.capture-profile.v1"
            and profile.get("publicDescriptor") == DESCRIPTOR
            and profile.get("sourceCommit") == COMMIT and profile.get("sourceTree") == TREE
            and profile.get("binarySHA256") == BINARY
            and profile.get("recorderSHA256") == digest(files["record_reference.py"])
            and profile.get("mode") == "managed-proxied-server"
            and profile.get("doltVersion") == "2.2.0" and profile.get("doltArchiveSHA256") == DOLT_ARCHIVE
            and profile.get("doltVersionCheckDisabled") is True
            and profile.get("doltMetricsDisabled") is True and profile.get("doltEventFlushDisabled") is True
            and profile.get("doltConfigSourceSHA256") == digest(files["dolt-config-source.go"]) == DOLT_CONFIG_SOURCE
            and profile.get("doltEnvSourceSHA256") == digest(files["dolt-env-source.sh"]) == DOLT_ENV_SOURCE
            and profile.get("doltIdentitySourceSHA256") == digest(files["dolt-identity-source.go"]) == DOLT_IDENTITY_SOURCE
            and profile.get("doltIdentity") == DOLT_IDENTITY
            and profile.get("doltArchiveURL") == DOLT_URL and profile.get("doltMember") == DOLT_MEMBER,
            "capture profile/source/recorder identity differs")
    manifest = load(files["public-reference-manifest.json"])
    source = manifest.get("source", {}).get("beads", {})
    require(source.get("commit") == COMMIT and source.get("tree") == TREE
            and any(item.get("sha256") == BINARY for item in manifest.get("artifacts", {}).values()),
            "public executable manifest/source differs")
    runtime = load(files["capture-runtime.json"])
    require(runtime.get("mode") == "managed-proxied-server" and runtime.get("version") == "2.2.0"
            and runtime.get("archiveSHA256") == DOLT_ARCHIVE
            and runtime.get("archiveURL") == DOLT_URL and runtime.get("member") == DOLT_MEMBER
            and re.fullmatch(r"[0-9a-f]{64}", runtime.get("binarySHA256", "")) is not None
            and type(runtime.get("binaryBytes")) is int and runtime["binaryBytes"] > 0
            and type(runtime.get("archiveBytes")) is int and runtime["archiveBytes"] > 0
            and files["dolt-version.stdout"].strip() == b"dolt version 2.2.0"
            and runtime.get("versionStdoutSHA256") == digest(files["dolt-version.stdout"])
            and runtime.get("versionStderrSHA256") == digest(files["dolt-version.stderr"]),
            "actual runtime profile/stream identity differs")
    require(runtime.get("versionCheckDisabled") is True
            and runtime.get("metricsDisabled") is True and runtime.get("eventFlushDisabled") is True
            and runtime.get("privateDoltRoot") is True
            and runtime.get("configFileMode") == "0600" and runtime.get("configDirectoryMode") == "0700"
            and runtime.get("configSHA256") == digest(files["dolt-config.json"])
            and runtime.get("configSourceSHA256") == DOLT_CONFIG_SOURCE
            and runtime.get("envSourceSHA256") == DOLT_ENV_SOURCE
            and runtime.get("identitySourceSHA256") == DOLT_IDENTITY_SOURCE and runtime.get("identity") == DOLT_IDENTITY
            and runtime.get("runtimeCommandsSHA256") == digest(files["runtime-commands.jsonl"]),
            "runtime configuration trace identity differs")
    require(load(files["dolt-config.json"]) == {"metrics.disabled": "true", "versioncheck.disabled": "true", **DOLT_IDENTITY},
            "private runtime configuration bytes differ")
    runtime_events = [load(line) for line in files["runtime-commands.jsonl"].splitlines()]
    require([event.get("argv") for event in runtime_events] == [
        ["dolt", "config", "--global", "--add", "metrics.disabled", "true"],
        ["dolt", "config", "--global", "--add", "versioncheck.disabled", "true"],
        ["dolt", "config", "--global", "--add", "user.name", "beads"],
        ["dolt", "config", "--global", "--add", "user.email", "beads@localhost"], ["dolt", "version"]],
        "runtime setup/version command order differs")
    for event in runtime_events:
        require(event.get("outcome") == "exited" and event.get("exitCode") == 0 and "error" not in event,
                "runtime setup/version command failed")
        require(event.get("privateHome") is True and event.get("doltRootMatchesHome") is True
                and event.get("eventFlushDisabled") is True, "runtime command environment is not private")
        for stream in ("stdout", "stderr"):
            data = decode(event.get(stream + "Base64"))
            require(len(data) == event.get(stream + "Bytes") and digest(data) == event.get(stream + "SHA256"),
                    "runtime setup/version stream bytes differ")
    require(decode(runtime_events[4]["stdoutBase64"]) == files["dolt-version.stdout"]
            and decode(runtime_events[4]["stderrBase64"]) == files["dolt-version.stderr"],
            "runtime version event and retained streams differ")
    recording = load(files["reference-recording.json"])
    require(recording.get("workspaceClass") == "fresh-disposable"
            and recording.get("runtimeProfile") == "managed-proxied-server"
            and recording.get("recorderSHA256") == profile["recorderSHA256"], "recording profile differs")
    vector_bytes = recording.get("rawJSON", "").encode()
    vector = load(vector_bytes)
    require(digest(vector_bytes) == recording.get("digest") and vector == recording.get("vector"),
            "recording vector bytes/digest differ")
    require(vector.get("source", {}).get("commit") == COMMIT and vector["source"].get("tree") == TREE
            and vector.get("binary", {}).get("sha256") == BINARY
            and vector.get("driver", {}).get("runtime") == runtime
            and set(vector.get("cards", {})) == {"B01", "B02", "B03", "B04"},
            "recorded source/runtime/card inventory differs")
    events = [load(line) for line in files["raw-commands.jsonl"].splitlines()]
    require(bool(events), "per-command raw transcript is empty")
    for event in events:
        require(event.get("outcome") == "exited" and type(event.get("exitCode")) is int,
                "completed capture has a timeout or missing exit")
        decode(event.get("stdoutBase64"))
        decode(event.get("stderrBase64"))
    require([{key: event.get(key) for key in ("label", "argv", "exitCode", "outcome")} for event in events]
            == [{key: event.get(key) for key in ("label", "argv", "exitCode", "outcome")}
                for event in vector.get("commands", [])], "recorded/raw command sequence differs")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--retain-block", action="store_true", help="retain diagnostic bytes; never accept as a twin")
    args = parser.parse_args()
    packet, payload = unframe(args.log.read_bytes())
    files = validate_packet(packet)
    if packet["status"] == "BLOCK" and args.retain_block:
        classification = "diagnostic-unqualified"
    else:
        accept_capture(packet, files)
        classification = "raw-capture-unqualified"
    # Validation precedes exclusive output creation. Never replace prior raw evidence.
    args.output.mkdir(mode=0o700)
    (args.output / "packet.json").write_bytes(payload)
    for name, data in files.items():
        (args.output / name).write_bytes(data)
    summary = {"status": packet["status"], "classification": classification,
               "packetBytes": len(payload), "packetSHA256": digest(payload),
               "logBytes": args.log.stat().st_size, "logSHA256": digest(args.log.read_bytes()),
               "files": {name: {"size": len(data), "sha256": digest(data)} for name, data in files.items()}}
    (args.output / "extraction.json").write_bytes(canonical(summary) + b"\n")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
