#!/usr/bin/env python3
"""Offline, independent validator for the exact packet retained in test.log.

Imports no recorder/capture code and executes neither product nor network work.
Missing, truncated, duplicated, altered or BLOCK packets never yield evidence.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess

from public_reference_profile import (
    BEGIN, CARDS, END, MAX_PACKET_BYTES, MODULES, PACKET_SCHEMA, PROFILE,
    QUALIFICATION, SOURCE_TESTS,
)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else canonical(value)).hexdigest()


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def decode(value):
    require(isinstance(value, str), "base64 value is not text")
    data = base64.b64decode(value, validate=True)
    require(base64.b64encode(data).decode("ascii") == value, "noncanonical base64")
    return data


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def unframe(log):
    # Match complete literal lines. No stripping, whitespace repair, CRLF
    # conversion, prefix removal, fallback to normalized output or partial JSON.
    lines = log.split(b"\n")
    starts = [index for index, line in enumerate(lines) if line.startswith(BEGIN)]
    ends = [index for index, line in enumerate(lines) if line.startswith(END)]
    require(len(starts) == len(ends) == 1, "packet framing is absent or ambiguous")
    first, last = starts[0], ends[0]
    require(last == first + 2 and last < len(lines) - 1, "packet frame is truncated or has extra lines")
    header = re.fullmatch(re.escape(BEGIN) + rb" ([1-9][0-9]*) ([0-9a-f]{64})", lines[first])
    require(header is not None, "packet frame header is malformed")
    length, checksum = header.groups()
    require(lines[last] == END + b" " + length + b" " + checksum, "packet frame footer differs")
    payload = lines[first + 1]
    require(len(payload) == int(length) <= MAX_PACKET_BYTES, "packet byte length differs or exceeds the bound")
    require(digest(payload).encode() == checksum, "packet byte checksum differs")
    packet = json.loads(payload, object_pairs_hook=no_duplicate_keys)
    require(canonical(packet) == payload, "packet is not exact canonical JSON")
    return packet, payload


def validate_manifest(manifest):
    source = manifest.get("source", {}).get("beads")
    expected_source = {
        "commit": PROFILE["source"]["commit"], "tree": PROFILE["source"]["tree"],
        "manifest_sha256": PROFILE["source"]["manifestSha256"],
        "manifest_entries": PROFILE["source"]["manifestEntries"],
    }
    require(manifest.get("schema") == "plug-public-bundle/1" and source == expected_source,
            "manifest source/profile mismatch")
    require(manifest.get("artifacts", {}).get("bin/bd") == PROFILE["binary"], "manifest executable mismatch")
    require(set(manifest["artifacts"]) == {"bin/bd", "bin/plug"}, "manifest executable set mismatch")
    require(manifest.get("archive") == PROFILE["archive"], "manifest canonical archive mismatch")
    require(manifest.get("build", {}).get("run_id") == PROFILE["producer"]["run"]
            and manifest["build"].get("beads") == PROFILE["build"]["command"]
            and manifest.get("toolchain", {}).get("go") == PROFILE["build"]["go"], "manifest build mismatch")
    require(manifest.get("workflow") == {
        "ref": "refs/heads/" + PROFILE["producer"]["branch"],
        "sha": PROFILE["producer"]["workflowSha"], "profile": "plug-public-bundle/1",
    }, "manifest producer workflow mismatch")


def validate_source(source):
    require(all(source.get(key) == value for key, value in PROFILE["source"].items())
            and source.get("cleanWorkingTree") is True, "source identity or cleanliness mismatch")
    raw_manifest = decode(source["manifestBase64"])
    require(digest(raw_manifest) == PROFILE["source"]["manifestSha256"], "source manifest checksum mismatch")
    lines = raw_manifest.splitlines()
    require(len(lines) == PROFILE["source"]["manifestEntries"], "source manifest entry count mismatch")
    entries = {}
    for line in lines:
        found = re.fullmatch(rb"(100644|100755|120000) blob ([0-9a-f]{40})\t(.+)", line)
        require(found is not None, "source manifest entry is unsupported")
        mode, oid, name = found.groups()
        path = name.decode("utf-8")
        relative = PurePosixPath(path)
        require(not relative.is_absolute() and str(relative) == path and ".." not in relative.parts
                and path not in entries, "source manifest path is invalid or duplicate")
        entries[path] = oid.decode()
    schema_paths = {path for path in entries if path.endswith(".up.sql") and str(PurePosixPath(path).parent) in {
        "internal/storage/schema/migrations", "internal/storage/schema/migrations/ignored",
    }}
    selected = {test.split("::", 1)[0] for tests in SOURCE_TESTS.values() for test in tests} | schema_paths
    blobs = {}
    for item in source["evidenceBlobs"]:
        path = item["path"]
        require(path in selected and path not in blobs, "unselected or duplicate source evidence")
        data = decode(item["bytesBase64"])
        require(digest(data) == item["sha256"], "source evidence SHA256 mismatch")
        oid = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        require(oid == entries[path], "source evidence differs from pinned Git blob")
        blobs[path] = data
    require(set(blobs) == selected, "source evidence is incomplete")
    schema = {}
    for plane, directory in (("main", "internal/storage/schema/migrations"),
                             ("ignored", "internal/storage/schema/migrations/ignored")):
        files = [{"path": path, "sha256": digest(blobs[path])} for path in sorted(schema_paths)
                 if str(PurePosixPath(path).parent) == directory]
        require(bool(files), "migration evidence is empty")
        schema[plane] = {
            "cursor": max(int(PurePosixPath(item["path"]).name.split("_", 1)[0]) for item in files),
            "files": len(files), "digest": digest(files),
        }
    require(source["schemaEvidence"] == schema, "migration evidence digest mismatch")
    refs = {}
    for card, tests in SOURCE_TESTS.items():
        refs[card] = []
        for test in tests:
            path, symbol = test.split("::", 1)
            require(re.search(rb"\bfunc " + re.escape(symbol.encode()) + rb"\(", blobs[path]) is not None,
                    "pinned source symbol missing")
            refs[card].append({"path": path, "symbol": symbol, "sha256": digest(blobs[path])})
    return refs


OMITTED_FIELDS = {
    "created_at", "updated_at", "closed_at", "defer_until", "heartbeat_at", "started_at",
    "revision", "schema_version", "owner", "lease_expires_at", "leaseExpiresAt",
    "createdAt", "updatedAt", "closedAt", "commit", "hash", "CommitHash", "CommitDate",
    "revision_hash", "revisionHash",
}
LABELS = {
    "B09": ["init", "hash-first", "hash-second", "hash-collision-refusal", "hash-list"],
    "B10": ["init", "create-b10-parent", "create-b10-parent.8", "child-9", "child-10", "delete-10",
            "child-11", "close-11", "child-12", "delete-live-children", "child-13", "children"],
    "B11": ["init", "create-b11-source", "create-b11-target", "add-blocks", "readd-blocks",
            "show-before-retype", "retype-refusal", "show-after-retype", "remove-blocks",
            "add-relates", "show-after-readd", "remove-relates", "show-final"],
    "B12": ["init", "create-b12-source", "close-source", "admin-compact-analyze",
            "admin-compact-apply", "restore-read", "restore-apply"],
}
REFUSALS = {"hash-collision-refusal", "retype-refusal", "admin-compact-apply", "restore-read", "restore-apply"}


def normalized(value, card):
    if isinstance(value, dict):
        return {key: normalized(item, card) for key, item in value.items() if key not in OMITTED_FIELDS}
    if isinstance(value, list):
        return [normalized(item, card) for item in value]
    if isinstance(value, str):
        value = re.sub(r"20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", "<runtime-time>", value)
        value = re.sub(r"/tmp/plug-swarm/work/[^/\s]+", "<workspace>", value)
        if card == "B09":
            value = re.sub(r"\bb09-[a-z0-9]{3,8}\b", "<generated-id>", value)
    return value


def projected(card, outputs, commands):
    """Derive each selected semantic result directly from decoded raw stdout."""
    if card == "B09":
        first, second, rows = (outputs[label] for label in ("hash-first", "hash-second", "hash-list"))
        projection = {
            "createdIDsDistinct": first["id"] != second["id"],
            "createdIDPattern": all(re.fullmatch(r"b09-[a-z0-9]{3,8}", row["id"]) is not None for row in (first, second)),
            "sameSemanticFields": all(first.get(key) == second.get(key) for key in ("title", "description", "issue_type", "status")),
            "collisionRefused": commands["hash-collision-refusal"]["exitCode"] != 0,
            "rowsAfterCollision": len(rows),
        }
        require(projection == {"createdIDsDistinct": True, "createdIDPattern": True,
                               "sameSemanticFields": True, "collisionRefused": True, "rowsAfterCollision": 2},
                "B09 selected behavior failed")
        observations = {"first": first, "second": second, "rows": rows}
    elif card == "B10":
        explicit, children = outputs["create-b10-parent.8"], outputs["children"]
        ids = [explicit["id"]] + [outputs["child-" + str(number)]["id"] for number in range(9, 14)]
        projection = {
            "explicitHighWater": ids[0] == "b10-parent.8", "mintedAfterImported": ids[1] == "b10-parent.9",
            "sequentialContinuation": ids[2:] == ["b10-parent." + str(number) for number in range(10, 14)],
            "noReuseAfterDeleteAndClose": ids[-1] == "b10-parent.13",
            "childRowsAfterCleanup": sorted(row["id"] for row in children) == ["b10-parent.13"],
            "observedIDs": ids,
        }
        require(all(value is True for key, value in projection.items() if key != "observedIDs"), "B10 selected behavior failed")
        observations = {"explicit": explicit, "children": children}
    elif card == "B11":
        before, refused, readded, final = (outputs[label] for label in (
            "show-before-retype", "show-after-retype", "show-after-readd", "show-final"))

        def edge(rows):
            deps = rows[0].get("dependencies", []) if rows else []
            return (deps[0].get("id"), deps[0].get("dependency_type")) if deps else None

        projection = {
            "sameTypeReaddAccepted": commands["readd-blocks"]["exitCode"] == 0,
            "typedRetypeRefused": commands["retype-refusal"]["exitCode"] != 0,
            "retypeNoWrite": edge(before) == edge(refused) == ("b11-target", "blocks"),
            "unlinkThenDifferentTypeAccepted": edge(readded) == ("b11-target", "relates-to"),
            "unlinkConservesEndpoints": final[0].get("dependency_count", 0) == 0,
        }
        require(all(value is True for value in projection.values()), "B11 selected behavior failed")
        observations = {"before": before, "afterRefusal": refused, "afterReadd": readded, "final": final}
    else:
        analyze = outputs["admin-compact-analyze"]
        apply_error = "Error: 'bd admin compact' is not yet supported in embedded mode\n"
        restore_error = "Error: issue b12-source is not compacted\nHint: only compacted issues need restoration\n"
        projection = {
            "analyzeSucceeded": isinstance(analyze, dict),
            "analyzeCandidateCount": analyze.get("summary", {}).get("total_candidates", -1),
            "applyRefusedInEmbeddedMode": commands["admin-compact-apply"]["exitCode"] == 1 and commands["admin-compact-apply"]["stderr"] == apply_error,
            "restoreReadRefusedWithoutSnapshot": commands["restore-read"]["exitCode"] == 1 and commands["restore-read"]["stderr"] == restore_error,
            "restoreApplyRefusedWithoutSnapshot": commands["restore-apply"]["exitCode"] == 1 and commands["restore-apply"]["stderr"] == restore_error,
        }
        require(projection == {"analyzeSucceeded": True, "analyzeCandidateCount": 0, "applyRefusedInEmbeddedMode": True,
                "restoreReadRefusedWithoutSnapshot": True, "restoreApplyRefusedWithoutSnapshot": True},
                "B12 embedded apply/restore boundary is unsupported or changed; remains unqualified")
        observations = {"analyze": analyze}
    return projection, normalized(observations, card)


def validate_run(run, card, refs):
    require(run["schema"] == "plug.s1-b09-b12.beads-reference.v2" and run["card"] == card
            and run["engine"] == "embedded-dolt", "run schema/card/engine mismatch")
    require(run["sourceCommit"] == PROFILE["source"]["commit"] and run["sourceTree"] == PROFILE["source"]["tree"]
            and run["binarySha256"] == PROFILE["binary"]["sha256"], "run source/binary mismatch")
    require(run["sourceTests"] == SOURCE_TESTS[card] and run["sourceRefs"] == refs, "run source evidence mismatch")
    require([command["label"] for command in run["commands"]] == LABELS[card]
            and [stream["label"] for stream in run["streams"]] == LABELS[card], "run stream/command set mismatch")
    outputs, commands, raw_streams = {}, {}, []
    for command, stream in zip(run["commands"], run["streams"]):
        label = command["label"]
        require(type(command["exitCode"]) is int and command["exitCode"] == (1 if label in REFUSALS else 0),
                "unexpected source-engine exit")
        require(isinstance(command["argv"], list) and len(command["argv"]) >= 2
                and command["argv"][0] == "reference-engine" and all(isinstance(arg, str) for arg in command["argv"]),
                "command argv provenance missing")
        stdout, stderr = decode(stream["stdoutBase64"]), decode(stream["stderrBase64"])
        require(digest(stdout) == stream["stdoutSha256"] and digest(stderr) == stream["stderrSha256"], "stream checksum mismatch")
        decoded = stdout.decode(errors="replace")
        try:
            value = json.loads(decoded)
        except json.JSONDecodeError:
            value = decoded
        outputs[label] = value
        require(command["stdout"] == normalized(value, card)
                and command["stderr"] == normalized(stderr.decode(errors="replace"), card),
                "normalized transcript differs from exact stream bytes")
        commands[label] = command
        raw_streams.append((label, stdout, stderr))
    projection, observations = projected(card, outputs, commands)
    require(run["projection"] == projection and run["observations"] == observations, "projection/observations differ from raw streams")
    require(run["rawStreamsSha256"] == digest(run["streams"])
            and run["normalizedTranscriptSha256"] == digest({"commands": run["commands"], "observations": observations})
            and run["projectionSha256"] == digest(projection), "run digest summary mismatch")
    return raw_streams


def measured_equality(first, repeat, left, right):
    require(len(left) == len(right) and [item[0] for item in left] == [item[0] for item in right], "unpaired raw streams")
    commands = [{"label": a[0], "stdoutEqual": a[1] == b[1], "stderrEqual": a[2] == b[2]} for a, b in zip(left, right)]
    return {
        "rawStreamsEqual": all(item["stdoutEqual"] and item["stderrEqual"] for item in commands),
        "normalizedTranscriptsEqual": first["commands"] == repeat["commands"] and first["observations"] == repeat["observations"],
        "projectionsEqual": first["projection"] == repeat["projection"], "perCommand": commands,
    }


def validate_packet(packet, expected_candidate):
    require(packet.get("schema") == PACKET_SCHEMA and packet.get("status") == "PASS", "packet is BLOCK or has wrong schema")
    require(packet.get("profile") == PROFILE and packet.get("profileSha256") == digest(PROFILE), "fixed profile digest mismatch")
    require(packet.get("qualification") == QUALIFICATION and packet.get("inProgress") == {}, "qualification boundary or completion mismatch")
    require(packet.get("candidate") == expected_candidate and set(expected_candidate["modules"]) == set(MODULES)
            and expected_candidate["modules"]["record_reference.py"] == PROFILE["reviewedRecorderSha256"], "candidate/module binding mismatch")
    bundle = packet["bundle"]
    manifest_bytes, receipt_bytes = decode(bundle["manifestBase64"]), decode(bundle["receiptBase64"])
    require(digest(manifest_bytes) == PROFILE["manifestSha256"] and digest(receipt_bytes) == bundle["receiptSha256"], "bundle metadata checksum mismatch")
    manifest = json.loads(manifest_bytes, object_pairs_hook=no_duplicate_keys)
    require(json.loads(receipt_bytes, object_pairs_hook=no_duplicate_keys) == manifest, "bundle receipt differs")
    validate_manifest(manifest)
    require(all(bundle[key] == PROFILE[key] for key in ("zip", "binary", "archive")), "bundle byte identity mismatch")
    refs = validate_source(packet["source"])
    config = packet["configuration"]
    require(config.get("inheritAmbient") is False and packet["configurationSha256"] == digest(config), "configuration digest/isolation mismatch")
    require(config["gitConfiguration"].get("credential.helper") == ""
            and config["environment"].get("GIT_CONFIG_GLOBAL") == "/dev/null"
            and config["environment"].get("GIT_ASKPASS") == "/bin/false"
            and not any("TOKEN" in key or key.endswith("_KEY") or key in {"SSH_AUTH_SOCK", "GIT_DIR", "BEADS_DIR", "DOLT_ROOT_PATH"}
                        for key in config["environment"]), "ambient credential/configuration appeared")
    require([item["card"] for item in packet["recordings"]] == list(CARDS), "packet card set/order mismatch")
    workspaces = set()
    for item in packet["recordings"]:
        card, first, repeat = item["card"], item["first"], item["repeat"]
        for run in (first, repeat):
            workspace = run["workspaceId"]
            require(isinstance(workspace, str) and workspace.startswith("s1-" + card.lower() + "-")
                    and workspace not in workspaces, "fresh workspace identities are missing or reused")
            workspaces.add(workspace)
        left, right = validate_run(first, card, refs[card]), validate_run(repeat, card, refs[card])
        equality = measured_equality(first, repeat, left, right)
        require(item["equality"] == equality, "reported raw/normalized/projection equality differs from measured equality")
        require(equality["normalizedTranscriptsEqual"] and equality["projectionsEqual"], "selected normalized cases disagree")
        require(item["disposition"] == {"status": "deviate" if card == "B12" else "keep",
                                          "source": "engdocs/design/plug/spec/behaviors/content.md#" + card}, "card disposition mismatch")


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
    # The independent extractor/profile executed locally must themselves be the
    # reviewed versions at the requested candidate, not a later repaired tool.
    for name in ("extract_reference_packet.py", "public_reference_profile.py"):
        require(digest(Path(__file__).with_name(name).read_bytes()) == modules[name], "extractor/profile is not the requested candidate")
    return {"commit": commit, "tree": tree, "modules": modules}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    packet, payload = unframe(args.log.read_bytes())
    validate_packet(packet, git_candidate(args.source_root, args.candidate, args.tree))
    # Refuse accidental overwrite of historical recordings or an earlier packet.
    with args.output.open("xb") as output:
        output.write(payload)
    print(json.dumps({"status": "PASS", "bytes": len(payload), "sha256": digest(payload),
                      "candidate": args.candidate, "profile": PROFILE["id"],
                      "equality": {item["card"]: item["equality"] for item in packet["recordings"]}}, sort_keys=True))


if __name__ == "__main__":
    main()
