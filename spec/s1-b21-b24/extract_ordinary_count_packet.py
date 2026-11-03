#!/usr/bin/env python3
"""Independent validator for a B23 ordinary-count capture packet.

This module intentionally does not import the recorder or run a source
engine. It validates framed bytes, source/profile identity, raw stream hashes,
private runtime configuration and the paired count projection offline.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import subprocess

from ordinary_count_profile import (
    BD_GLOBAL_ARGS, COMMANDS, COUNT_STEPS, COUNT_VECTOR, ENVIRONMENT,
    GIT_CONFIGURATION, INPUTS, MODULES, PROFILE, SUPPORT_FILES,
)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    data = value if isinstance(value, (bytes, bytearray)) else canonical(value)
    return hashlib.sha256(data).hexdigest()


def decode(value):
    data = base64.b64decode(value.encode("ascii"), validate=True)
    require(base64.b64encode(data).decode("ascii") == value, "noncanonical base64")
    return data


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def parse_json(data):
    return json.loads(data, object_pairs_hook=no_duplicate_keys,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite JSON")))


def unframe(data: bytes, *, outer=False):
    begin = PROFILE["capture"]["outerFrameBegin" if outer else "frameBegin"].encode()
    end = PROFILE["capture"]["outerFrameEnd" if outer else "frameEnd"].encode()
    lines = data.split(b"\n")
    starts = [i for i, line in enumerate(lines) if line.startswith(begin)]
    ends = [i for i, line in enumerate(lines) if line.startswith(end)]
    require(len(starts) == len(ends) == 1, "ordinary-count packet has duplicate or missing frame")
    first, last = starts[0], ends[0]
    require(last == first + 2 and last < len(lines) - 1, "truncated or multiline frame")
    header = re.fullmatch(re.escape(begin) + rb" ([1-9][0-9]*) ([0-9a-f]{64})", lines[first])
    require(header is not None, "malformed frame header")
    size_text, sha = header.groups()
    payload = lines[first + 1]
    require(lines[last] == end + b" " + size_text + b" " + sha, "frame identities differ")
    require(int(size_text) == len(payload) and len(payload) <= 64 * 1024 * 1024, "frame length differs or exceeds reviewed bound")
    require(digest(payload).encode() == sha, "frame digest differs")
    packet = parse_json(payload)
    if not outer:  # Go's outer JSON uses struct field order; exact bytes are hashed.
        require(canonical(packet) == payload, "packet is not canonical JSON")
    return packet, payload


def validate_manifest(manifest):
    source, public = PROFILE["source"], PROFILE["publicBundle"]
    require(manifest.get("schema") == "plug-public-bundle/1", "public manifest schema differs")
    require(manifest.get("source", {}).get("beads") == {
        "commit": source["commit"], "tree": source["tree"],
        "manifest_sha256": source["manifestSha256"], "manifest_entries": source["manifestEntries"]}, "manifest source differs")
    artifacts = manifest.get("artifacts", {})
    require(set(artifacts) == {"bin/bd", "bin/plug"} and artifacts["bin/bd"] == {
        "sha256": public["binarySha256"], "size": public["binaryBytes"], "mode": public["binaryMode"]}, "manifest executable differs")
    require(manifest.get("archive") == {"name": "bundle.tar", "sha256": public["archiveSha256"],
                                        "size": public["archiveBytes"]}, "manifest archive differs")
    require(manifest.get("build", {}).get("run_id") == PROFILE["producer"]["run"]
            and manifest["build"].get("beads") == "CGO_ENABLED=0 go build -tags gms_pure_go -trimpath ./cmd/bd"
            and manifest.get("toolchain", {}).get("go") == "go version go1.26.10 linux/amd64", "manifest build differs")
    require(manifest.get("workflow") == {"ref": "refs/heads/" + PROFILE["producer"]["branch"],
            "sha": PROFILE["producer"]["workflowSha"], "profile": "plug-public-bundle/1"}, "manifest workflow differs")


def validate_source(source):
    require(all(source.get(k) == v for k, v in PROFILE["source"].items())
            and source.get("cleanWorkingTree") is True, "public source identity differs")
    manifest = decode(source["manifestBase64"])
    require(digest(manifest) == PROFILE["source"]["manifestSha256"]
            and len(manifest.splitlines()) == PROFILE["source"]["manifestEntries"], "public source manifest differs")
    entries = {}
    for line in manifest.splitlines():
        found = re.fullmatch(rb"(100644|100755|120000) blob ([0-9a-f]{40})\t(.+)", line)
        require(found is not None, "unsupported source entry")
        _, oid, name = found.groups()
        require(name.decode() not in entries, "duplicate source entry")
        entries[name.decode()] = oid.decode()
    seen = set()
    for blob in source["evidenceBlobs"]:
        name = blob["path"]
        require(name in PROFILE["runtimeSource"] and name not in seen, "unexpected source proof")
        seen.add(name)
        data = decode(blob["bytesBase64"])
        require(digest(data) == blob["sha256"] == PROFILE["runtimeSource"][name], "source proof digest differs")
        require(hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest() == entries[name], "source proof differs from Git manifest")
    require(seen == set(PROFILE["runtimeSource"]), "source proofs are incomplete")


def candidate_identity(root: Path, commit: str | None = None, tree: str | None = None):
    env = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0"}
    def git(*args):
        return subprocess.check_output(["/usr/bin/git", "-C", str(root), *args], env=env, stderr=subprocess.PIPE)
    actual_commit = git("rev-parse", "HEAD").decode().strip()
    actual_tree = git("rev-parse", "HEAD^{tree}").decode().strip()
    require(commit is None or actual_commit == commit, "candidate commit differs")
    require(tree is None or actual_tree == tree, "candidate tree differs")
    modules = {}
    for name in MODULES:
        data = git("cat-file", "blob", f"{actual_commit}:spec/s1-b21-b24/{name}")
        path = Path(__file__).with_name(name)
        require(path.read_bytes() == data, f"executed capture module differs: {name}")
        modules[name] = digest(data)
    support = {}
    for name in SUPPORT_FILES:
        data = git("cat-file", "blob", f"{actual_commit}:spec/s1-b21-b24/{name}")
        require((root / "spec/s1-b21-b24" / name).read_bytes() == data, "harness source differs: " + name)
        support[name] = digest(data)
    return {"commit": actual_commit, "tree": actual_tree, "modules": modules, "support": support}


def validate_runtime(result):
    runtime = result["runtimeConfiguration"]
    expected = PROFILE["runtime"]
    data = decode(runtime["bytesBase64"])
    require(runtime["relativePath"] == ".dolt/config_global.json" and data == expected["configBytes"].encode(), "private Dolt config bytes differ")
    require(runtime["sha256"] == digest(data) and runtime["directoryMode"] == expected["configDirectoryMode"] and runtime["fileMode"] == expected["configFileMode"], "private Dolt config mode/digest differs")
    require(runtime["environment"] == {"DOLT_ROOT_PATH": runtime["rootPath"], "DOLT_DISABLE_EVENT_FLUSH": "1"}, "Dolt environment is not private")
    require(runtime["rootPath"] == result["environment"]["HOME"]
            and runtime["path"] == runtime["rootPath"] + "/.dolt/config_global.json", "private config path differs")


def validate_stream(stream):
    stdout, stderr = decode(stream["stdoutBase64"]), decode(stream["stderrBase64"])
    require(digest(stdout) == stream["stdoutSha256"] and digest(stderr) == stream["stderrSha256"]
            and len(stdout) == stream["stdoutBytes"] and len(stderr) == stream["stderrBytes"], "raw stream length/digest differs")
    require(stream["exitCode"] == 0 and stream["outcome"] == "exited" and stream["launchError"] is None, "command did not exit successfully")
    return stdout, stderr


def derive_projection(outputs):
    projection = {}
    for key, label in (
        ("openTaskCount", "open-task-count"), ("openNeedleTaskCount", "open-needle-count"),
        ("closedTaskCount", "closed-task-count"), ("absentNeedleTaskCount", "absent-needle-count"),
        ("afterUpdateOpenNeedleTaskCount", "after-update-needle-count"),
        ("afterUpdateChangedTaskCount", "after-update-changed-count")):
        value = parse_json(outputs[label])
        require(isinstance(value, dict) and type(value.get("count")) is int and value["count"] >= 0, "raw count is not a nonnegative integer")
        projection[key] = value["count"]
    grouped = parse_json(outputs["open-task-by-status"])
    require(type(grouped.get("total")) is int and grouped["groups"] == [{"count": 4, "group": "open"}], "raw grouped count differs")
    projection["openTaskStatusTotal"] = grouped["total"]
    return projection


def normalized_digest(commands):
    text = canonical(commands).decode()
    text = re.sub(r"20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", "<runtime-time>", text)
    text = re.sub(r"/tmp/plug-swarm/work/[^/\\\s]+", "<workspace>", text)
    return digest(text.encode())


def validate_result(result, binary_path, runtime_path):
    require(result["schema"] == "plug.s1-b23.ordinary-count-reference.v2" and result["card"] == "B23" and result["engine"] == "managed-proxied-dolt", "ordinary-count recording schema differs")
    require(result["source"] == {
        "commit": PROFILE["source"]["commit"], "tree": PROFILE["source"]["tree"],
        "descriptor": PROFILE["descriptor"], "profile": PROFILE["id"],
        "binarySha256": PROFILE["publicBundle"]["binarySha256"], "binaryMode": "0o755",
        "doltVersion": "2.2.0", "doltSha256": PROFILE["runtime"]["binarySha256"],
        "doltMode": "0o700", "runtimeMode": "managed-proxied-server"}, "ordinary-count source identity differs")
    workspace = result["workspace"]
    require(re.fullmatch(r"/tmp/plug-swarm/work/s1-b23-(first|repeat)-[A-Za-z0-9_-]+", workspace) is not None, "workspace is not owned/fresh")
    environment = dict(ENVIRONMENT)
    for key, name in (("HOME", "home"), ("TMPDIR", "tmp"), ("XDG_CONFIG_HOME", "config"),
                      ("XDG_CACHE_HOME", "cache"), ("XDG_DATA_HOME", "data"), ("XDG_STATE_HOME", "state")):
        environment[key] = workspace + "/.recorder-env/" + name
    environment.update({"PATH": str(Path(runtime_path).parent) + ":/usr/bin:/bin",
        "DOLT_ROOT_PATH": environment["HOME"], "DOLT_DISABLE_EVENT_FLUSH": "1", "DOLT_CLI_PAGER": "cat", "PAGER": "cat",
        "GIT_CONFIG_COUNT": str(len(GIT_CONFIGURATION))})
    for index, (key, value) in enumerate(GIT_CONFIGURATION.items()):
        environment[f"GIT_CONFIG_KEY_{index}"] = key
        environment[f"GIT_CONFIG_VALUE_{index}"] = value
    require(result["environment"] == environment, "effective environment differs from private profile")
    validate_runtime(result)
    commands, streams = result["commands"], result["streams"]
    require([item["step"] for item in commands] == COUNT_STEPS and [item["label"] for item in streams] == COUNT_STEPS, "ordinary-count command labels differ")
    require(len(commands) == len(streams), "ordinary-count command and stream counts differ")
    outputs = {}
    for command, stream, (label, program, args) in zip(commands, streams, COMMANDS):
        executable = {"git": "/usr/bin/git", "dolt": runtime_path, "bd": binary_path}[program]
        argv = [executable, *(BD_GLOBAL_ARGS if program == "bd" else []), *args]
        require(command["step"] == stream["label"] == label and command["exit"] == 0 and command["outcome"] == "exited", "ordinary-count command failed")
        require(command["argv"] == stream["argv"] == argv and stream["cwd"] == workspace
                and stream["environmentSha256"] == digest(environment) and stream["timeoutSeconds"] == 120, "command argv/environment/cwd/timeout differs")
        stdout, stderr = validate_stream(stream)
        require(command["stdout"] == stdout.decode(errors="replace") and command["stderr"] == stderr.decode(errors="replace"), "decoded command differs from raw bytes")
        outputs[label] = stdout
        if label in {"dolt-config-probe", "dolt-version"}:
            require(not stderr and stdout == (b"true\n" if label == "dolt-config-probe" else b"dolt version 2.2.0\n"), "exact runtime probe differs")
    projection = derive_projection(outputs)
    require(projection == COUNT_VECTOR == result["projection"] and result["projectionSha256"] == digest(projection), "projection differs from raw count outputs")
    shutdown = parse_json(outputs["cleanup-stop"])
    require(shutdown.get("stopped") is True and shutdown.get("verified") is True, "raw shutdown not verified")
    require(result["inputs"] == INPUTS and result["inputSha256"] == digest(INPUTS), "input vector differs")
    require(result["rawJSON"] == canonical(commands).decode() and result["normalizedTranscriptSha256"] == normalized_digest(commands), "transcript differs from raw command records")
    require(result["rawStreamsSha256"] == digest(streams) and result["rawSha256"] == digest(result["rawJSON"].encode()), "ordinary-count transcript digest differs")
    require(result["shutdownVerified"] is True and result["failure"] is None and result["workspaceRetained"] is False, "ordinary-count shutdown/cleanup was not verified")
    require(result["limitations"] == PROFILE["limitations"], "qualification limits differ")


def validate_packet(packet, expected_candidate=None):
    require(packet.get("schema") == "plug.s1-b23.ordinary-count-capture.v2" and packet.get("status") == "PASS", "ordinary-count packet is BLOCK or has wrong schema")
    require(packet.get("profile") == PROFILE, "ordinary-count profile changed")
    require(packet.get("candidate") == expected_candidate if expected_candidate is not None else isinstance(packet.get("candidate"), dict), "ordinary-count candidate identity differs")
    bundle = packet["publicBundle"]
    manifest_bytes, receipt_bytes = decode(bundle["manifestBase64"]), decode(bundle["receiptBase64"])
    require(digest(manifest_bytes) == bundle["manifestSha256"] == PROFILE["publicBundle"]["manifestSha256"]
            and digest(receipt_bytes) == bundle["receiptSha256"], "public bundle manifest differs")
    require(bundle["identity"] == PROFILE["publicBundle"] and parse_json(manifest_bytes) == parse_json(receipt_bytes), "bundle/receipt differs")
    validate_manifest(parse_json(manifest_bytes))
    validate_source(packet["publicSource"])
    require(bundle["binarySha256"] == PROFILE["publicBundle"]["binarySha256"] and bundle["binaryMode"] == "0o755", "measured public executable differs")
    runtime = packet.get("runtime", {})
    require(runtime.get("mode") == PROFILE["runtime"]["mode"] and runtime.get("version") == PROFILE["runtime"]["version"], "managed runtime identity differs")
    require(runtime.get("archive", {}).get("sha256") == PROFILE["runtime"]["archiveSha256"] and runtime.get("binary", {}).get("sha256") == PROFILE["runtime"]["binarySha256"], "managed runtime digest differs")
    require(runtime["archive"] == {"url": PROFILE["runtime"]["archiveURL"], "bytes": PROFILE["runtime"]["archiveBytes"], "sha256": PROFILE["runtime"]["archiveSha256"]}, "runtime archive binding differs")
    require(all(runtime["binary"].get(k) == v for k, v in {
        "bytes": PROFILE["runtime"]["binaryBytes"], "mode": "0o700", "member": PROFILE["runtime"]["member"]}.items()), "runtime executable member differs")
    binary_path, runtime_path = bundle["binaryPath"], runtime["binary"]["path"]
    require(re.fullmatch(r"/tmp/plug-swarm/work/s1-b23-count-capture-[A-Za-z0-9_-]+/bd", binary_path) is not None
            and runtime_path == str(Path(binary_path).parent / "runtime-bin/dolt"), "runtime paths differ")
    require(packet["configuration"] == {"inheritAmbient": False, "environment": ENVIRONMENT, "gitConfiguration": GIT_CONFIGURATION}, "configuration declaration differs")
    require(bool(packet["setupStreams"]), "setup command evidence is absent")
    for stream in packet["setupStreams"]:
        validate_stream(stream)
    recordings = packet.get("recordings", [])
    require([item.get("recording") for item in recordings] == ["first", "repeat"], "ordinary-count pair is incomplete")
    for result in recordings:
        require(result["recorderSha256"] == packet["candidate"]["modules"]["record_ordinary_count.py"], "recording source differs from candidate")
        validate_result(result, binary_path, runtime_path)
    first, repeat = recordings
    require(first["workspace"] != repeat["workspace"] and first["projection"] == repeat["projection"], "ordinary-count pair is not independent/equal")
    pair = packet.get("pair", {})
    require(pair == {"sameProjection": True, "rawStreamsEqual": first["streams"] == repeat["streams"],
        "normalizedTranscriptsEqual": first["normalizedTranscriptSha256"] == repeat["normalizedTranscriptSha256"],
        "projectionsEqual": True, "independentWorkspaces": [first["workspace"], repeat["workspace"]]}, "ordinary-count pair metadata differs")
    return packet


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--twins-dir", type=Path)
    parser.add_argument("--retain-block", action="store_true", help="diagnostic retention only; never acceptance")
    args = parser.parse_args()
    outer, _ = unframe(args.log.read_bytes(), outer=True)
    require(outer["schema"] == "plug.s1-b23.ordinary-count-outer.v1", "wrong launcher envelope")
    stdout, stderr = decode(outer["stdoutBase64"]), decode(outer["stderrBase64"])
    require(len(stdout) == outer["stdoutBytes"] and digest(stdout) == outer["stdoutSha256"]
            and len(stderr) == outer["stderrBytes"] and digest(stderr) == outer["stderrSha256"], "outer stream length/digest differs")
    packet, payload = unframe(stdout)
    candidate = candidate_identity(args.source_root, args.candidate, args.tree)
    if args.retain_block:
        require(args.twins_dir is None and packet.get("status") == "BLOCK", "diagnostic mode requires a BLOCK packet without twins")
        with args.output.open("xb") as stream:
            stream.write(payload)
        print(json.dumps({"status": "BLOCK_RETAINED", "bytes": len(payload), "sha256": digest(payload)}))
        return
    require(outer["exitCode"] == 0 and outer["outcome"] == "exited", "outer recorder failed")
    validate_packet(packet, candidate)
    with args.output.open("xb") as stream:
        stream.write(payload)
    if args.twins_dir is not None:
        args.twins_dir.mkdir(exist_ok=False)
        for result in packet["recordings"]:
            with (args.twins_dir / ("count-reference-" + result["recording"] + ".json")).open("xb") as stream:
                stream.write(canonical(result) + b"\n")
    print(json.dumps({"status": "PASS", "bytes": len(payload), "sha256": digest(payload), "candidate": candidate, "projection": COUNT_VECTOR}, sort_keys=True))


if __name__ == "__main__":
    main()
