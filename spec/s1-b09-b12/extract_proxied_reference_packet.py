#!/usr/bin/env python3
"""Independent offline validator for the distinct three-card proxied packet.

Imports neither recorder nor capture code. Historical four-card extraction is
unchanged. Runtime executable derivation additionally requires the exact-source
RunsOn receipt; retained digests alone do not prove that extraction executed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

import extract_reference_packet as common
from public_reference_profile import SOURCE_TESTS
from proxied_reference_profile import (
    CARDS, ENGINE, ENVIRONMENT_SHA256, GIT_CONFIGURATION_SHA256, MODULES,
    PACKET_SCHEMA, PROFILE, PUBLIC_BUILD, QUALIFICATION,
    RUN_SCHEMA, RUNTIME, RUNTIME_SOURCE, UNEXECUTED,
)

require, decode, digest = common.require, common.decode, common.digest


def validate_runtime(runtime, source, proofs):
    require(runtime.get("mode") == RUNTIME["mode"] and runtime.get("version") == RUNTIME["version"],
            "actual external runtime mode/version mismatch")
    archive, binary = runtime["archive"], runtime["binary"]
    require(archive.get("sha256") == RUNTIME["archiveSha256"] and archive.get("url") == RUNTIME["url"]
            and type(archive.get("bytes")) is int and 0 < archive["bytes"] <= 256 * 1024 * 1024,
            "fixed external runtime archive mismatch")
    require(binary.get("member") == RUNTIME["member"] and binary.get("mode") == "0o700"
            and re.fullmatch(r"[0-9a-f]{64}", binary.get("sha256", "")) is not None
            and type(binary.get("bytes")) is int and 0 < binary["bytes"] <= 256 * 1024 * 1024,
            "measured runtime executable identity missing")
    probe = runtime["versionProbe"]
    stdout, stderr = decode(probe["stdoutBase64"]), decode(probe["stderrBase64"])
    require(probe.get("argv") == ["verified-runtime", "version"]
            and type(probe.get("exitCode")) is int and probe["exitCode"] == 0
            and stdout == RUNTIME["versionStdout"].encode() and stderr == b""
            and digest(stdout) == probe["stdoutSha256"] and digest(stderr) == probe["stderrSha256"],
            "actual runtime version streams mismatch")
    manifest = decode(source["manifestBase64"])
    entries = {line.split(b"\t", 1)[1].decode(): line.split(b"\t", 1)[0].split()[2].decode()
               for line in manifest.splitlines()}
    checked = set()
    for proof in proofs:
        path, data = proof["path"], decode(proof["bytesBase64"])
        require(path in RUNTIME_SOURCE and path not in checked
                and digest(data) == proof["sha256"] == RUNTIME_SOURCE[path], "runtime source proof mismatch")
        oid = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        require(entries.get(path) == oid, "runtime source is not the pinned Git blob")
        checked.add(path)
    require(checked == set(RUNTIME_SOURCE), "runtime source proofs incomplete")


def validate_run(run, card, refs, runtime_binary_sha256):
    require(card in CARDS and run["schema"] == RUN_SCHEMA and run["card"] == card
            and run["engine"] == ENGINE, "proxied run schema/card/engine mismatch")
    require(run["sourceCommit"] == PUBLIC_BUILD["source"]["commit"]
            and run["sourceTree"] == PUBLIC_BUILD["source"]["tree"]
            and run["binarySha256"] == PUBLIC_BUILD["binary"]["sha256"]
            and run["runtimeBinarySha256"] == runtime_binary_sha256, "run source/executable/runtime mismatch")
    require(run["sourceTests"] == SOURCE_TESTS[card] and run["sourceRefs"] == refs,
            "run source evidence mismatch")
    labels = common.LABELS[card] + ["cleanup-stop"]
    require([item["label"] for item in run["commands"]] == labels
            and [item["label"] for item in run["streams"]] == labels, "proxied command/stream set mismatch")
    outputs, commands, raw = {}, {}, []
    for command, stream in zip(run["commands"], run["streams"]):
        label = command["label"]
        require(type(command["exitCode"]) is int and command["exitCode"] == (1 if label in common.REFUSALS else 0),
                "unexpected source-engine exit")
        argv = command["argv"]
        require(isinstance(argv, list) and len(argv) >= 2 and argv[0] == "reference-engine"
                and all(isinstance(arg, str) for arg in argv), "command argv provenance missing")
        if label == "init":
            require(argv == ["reference-engine", *RUNTIME["init"], "--prefix", card.lower(), "--quiet"],
                    "managed proxied init argv mismatch")
        if label == "cleanup-stop":
            require(argv == ["reference-engine", *RUNTIME["shutdown"]], "owned shutdown argv mismatch")
        stdout, stderr = decode(stream["stdoutBase64"]), decode(stream["stderrBase64"])
        require(digest(stdout) == stream["stdoutSha256"] and digest(stderr) == stream["stderrSha256"],
                "stream checksum mismatch")
        text = stdout.decode(errors="replace")
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            value = text
        require(command["stdout"] == common.normalized(value, card)
                and command["stderr"] == common.normalized(stderr.decode(errors="replace"), card),
                "normalized transcript differs from exact streams")
        outputs[label], commands[label] = value, command
        raw.append((label, stdout, stderr))
    shutdown = outputs["cleanup-stop"]
    require(isinstance(shutdown, dict) and shutdown.get("stopped") is True
            and shutdown.get("verified") is True and run.get("shutdownVerified") is True,
            "owned runtime shutdown was not verified")
    projection, observations = common.projected(card, outputs, commands)
    require(run["projection"] == projection and run["observations"] == observations,
            "selected projection differs from raw streams")
    require(run["rawStreamsSha256"] == digest(run["streams"])
            and run["normalizedTranscriptSha256"] == digest({"commands": run["commands"], "observations": observations})
            and run["projectionSha256"] == digest(projection), "run digest summary mismatch")
    return raw


def validate_card_set(packet):
    require(packet.get("selectedCards") == list(CARDS) and packet.get("unexecuted") == UNEXECUTED
            and [item["card"] for item in packet["recordings"]] == list(CARDS),
            "selected three-card set or explicit B12 omission mismatch")


def validate_packet(packet, expected_candidate):
    require(packet.get("schema") == PACKET_SCHEMA and packet.get("status") == "PASS",
            "proxied packet is BLOCK or has wrong schema")
    require(packet.get("profile") == PROFILE and packet.get("profileSha256") == digest(PROFILE),
            "fixed distinct proxied profile mismatch")
    require(packet.get("qualification") == QUALIFICATION and packet.get("inProgress") == {},
            "qualification boundary or completion mismatch")
    require(packet.get("candidate") == expected_candidate and set(expected_candidate["modules"]) == set(MODULES)
            and expected_candidate["modules"]["record_reference.py"] == PUBLIC_BUILD["reviewedRecorderSha256"],
            "candidate/module binding mismatch")
    validate_card_set(packet)
    bundle = packet["bundle"]
    manifest_bytes, receipt_bytes = decode(bundle["manifestBase64"]), decode(bundle["receiptBase64"])
    require(digest(manifest_bytes) == PUBLIC_BUILD["manifestSha256"] and digest(receipt_bytes) == bundle["receiptSha256"],
            "public-build metadata checksum mismatch")
    manifest = json.loads(manifest_bytes, object_pairs_hook=common.no_duplicate_keys)
    require(json.loads(receipt_bytes, object_pairs_hook=common.no_duplicate_keys) == manifest, "public receipt differs")
    common.validate_manifest(manifest)
    require(all(bundle[key] == PUBLIC_BUILD[key] for key in ("zip", "binary", "archive")), "public byte identity mismatch")
    refs = common.validate_source(packet["source"])
    validate_runtime(packet["runtime"], packet["source"], packet["runtimeSource"])
    config = packet["configuration"]
    require(config.get("inheritAmbient") is False and packet["configurationSha256"] == digest(config)
            and digest(config["environment"]) == ENVIRONMENT_SHA256
            and digest(config["gitConfiguration"]) == GIT_CONFIGURATION_SHA256
            and config["gitConfiguration"].get("credential.helper") == ""
            and config["environment"].get("GIT_CONFIG_GLOBAL") == "/dev/null"
            and config["environment"].get("PATH") == "/usr/bin:/bin"
            and config.get("runtimePath") == "verified private runtime-bin prepended to /usr/bin:/bin"
            and not any("TOKEN" in key or key.endswith("_KEY") or key in {"SSH_AUTH_SOCK", "LD_PRELOAD", "GIT_DIR", "BEADS_DIR", "DOLT_ROOT_PATH"}
                        for key in config["environment"]), "configuration/isolation mismatch")
    workspaces = set()
    for item in packet["recordings"]:
        card, first, repeat = item["card"], item["first"], item["repeat"]
        for run in (first, repeat):
            workspace = run["workspaceId"]
            require(isinstance(workspace, str) and workspace.startswith("s1-" + card.lower() + "-")
                    and workspace not in workspaces, "fresh workspace identities missing or reused")
            workspaces.add(workspace)
        runtime_sha256 = packet["runtime"]["binary"]["sha256"]
        left = validate_run(first, card, refs[card], runtime_sha256)
        right = validate_run(repeat, card, refs[card], runtime_sha256)
        equality = common.measured_equality(first, repeat, left, right)
        require(item["equality"] == equality and equality["normalizedTranscriptsEqual"] and equality["projectionsEqual"],
                "measured raw/normalized/projection pair binding mismatch")
        require(item["disposition"] == {"status": "keep", "source": "engdocs/design/plug/spec/behaviors/content.md#" + card},
                "selected disposition mismatch")


def git_candidate(root, commit, tree):
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None
            and re.fullmatch(r"[0-9a-f]{40}", tree) is not None, "candidate SHA/tree must be full object IDs")
    environment = {
        "PATH": "/usr/bin:/bin", "LANG": "C", "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0", "GIT_NO_LAZY_FETCH": "1",
    }

    def git(*args):
        return subprocess.check_output([
            "/usr/bin/git", "-c", "credential.helper=", "-c", "core.hooksPath=/dev/null",
            "-c", "core.fsmonitor=false", "-c", "protocol.allow=never", "-C", str(root), *args,
        ], env=environment, stderr=subprocess.PIPE)

    require(git("rev-parse", commit + "^{tree}").decode().strip() == tree, "candidate tree mismatch")
    modules = {name: digest(git("cat-file", "blob", commit + ":spec/s1-b09-b12/" + name)) for name in MODULES}
    for name in ("extract_proxied_reference_packet.py", "proxied_reference_profile.py",
                 "extract_reference_packet.py", "public_reference_profile.py"):
        require(digest(Path(__file__).with_name(name).read_bytes()) == modules[name],
                "extractor/profile is not the requested candidate")
    return {"commit": commit, "tree": tree, "modules": modules}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    packet, payload = common.unframe(args.log.read_bytes())
    validate_packet(packet, git_candidate(args.source_root, args.candidate, args.tree))
    with args.output.open("xb") as output:
        output.write(payload)
    print(json.dumps({"status": "PASS", "bytes": len(payload), "sha256": digest(payload),
                      "candidate": args.candidate, "profile": PROFILE["id"],
                      "selectedCards": list(CARDS), "unexecuted": UNEXECUTED,
                      "equality": {item["card"]: item["equality"] for item in packet["recordings"]}}, sort_keys=True))


if __name__ == "__main__":
    main()
